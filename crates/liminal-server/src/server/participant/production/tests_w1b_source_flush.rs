use std::cell::Cell;
use std::error::Error;
use std::sync::Arc;

use liminal::durability::{DurableStore, bridge::block_on, open_ephemeral};
use liminal_protocol::wire::{
    AttachAttemptToken, ClientRequest, ConnectionIncarnation, CredentialAttachRequest,
    EnrollmentRequest, EnrollmentToken, Generation, ServerValue,
};

use crate::server::participant::{
    ConnectionFateClass, ConnectionFateWorkItem, ParticipantConnectionConversations,
};

use super::ProductionParticipantHandler;
use super::log::{
    DecodedStoredOperation, OperationLog, OperationLogError, StoredOperation,
    StoredOrdinaryTerminalSource, StoredTerminalDisposition,
};
use super::state::DurableAppend;
use super::tests::{dispatch_tracked, test_participant_config};

const OBSERVER_STREAM_KEY: &str = "liminal:participant-observer-recovery";

struct SourceCutAppender<'a> {
    log: &'a OperationLog,
    source_flushed: Cell<bool>,
}

impl DurableAppend for SourceCutAppender<'_> {
    fn append(
        &self,
        operation: &StoredOperation,
        expected_sequence: u64,
    ) -> Result<(), OperationLogError> {
        if self.source_flushed.replace(true) {
            let actual = expected_sequence
                .checked_add(1)
                .ok_or(OperationLogError::CorruptRow {
                    sequence: expected_sequence,
                })?;
            return Err(OperationLogError::Sequence {
                expected: expected_sequence,
                actual,
            });
        }
        block_on(self.log.append(operation, expected_sequence))?
    }
}

fn observer_row_count(store: &Arc<dyn DurableStore>) -> Result<usize, Box<dyn Error>> {
    Ok(block_on(store.read_from(OBSERVER_STREAM_KEY, 0, 256))??.len())
}

fn require_source_cut(
    class: ConnectionFateClass,
    completion: Result<(), super::state::StateError>,
) -> Result<(), Box<dyn Error>> {
    match class {
        ConnectionFateClass::ConnectionLost => {
            assert!(completion.is_err(), "the cut must stop the specific fate");
            Ok(())
        }
        ConnectionFateClass::CleanDisconnect => completion.map_err(Into::into),
        ConnectionFateClass::ServerShutdown | ConnectionFateClass::ProtocolError => {
            Err("source-cut fixture received an unsupported class".into())
        }
    }
}

fn committed_source_progress(
    class: ConnectionFateClass,
    operation: DecodedStoredOperation,
) -> Result<u64, Box<dyn Error>> {
    let disposition = match (class, operation) {
        (
            ConnectionFateClass::ConnectionLost,
            DecodedStoredOperation::V3(StoredOperation::Died { row }),
        ) => row.disposition,
        (
            ConnectionFateClass::CleanDisconnect,
            DecodedStoredOperation::V3(StoredOperation::Detached { row }),
        ) => row.disposition,
        (_, operation) => {
            return Err(format!("source-cut selected the wrong operation: {operation:?}").into());
        }
    };
    match disposition {
        StoredTerminalDisposition::Committed { terminal_seq } => Ok(terminal_seq),
        StoredTerminalDisposition::Pending => {
            Err("source-cut unexpectedly selected Pending".into())
        }
    }
}

fn assert_single_ordinary_completion(
    log: &OperationLog,
    died_source_sequence: u64,
) -> Result<(), Box<dyn Error>> {
    let ordinary_sequence = died_source_sequence
        .checked_add(1)
        .ok_or("ordinary source sequence overflow")?;
    let Some(specific) = block_on(log.read_at(ordinary_sequence))?? else {
        return Err("cold repair omitted the named Ordinary completion".into());
    };
    let DecodedStoredOperation::V3(StoredOperation::Ordinary { row, .. }) = specific.operation
    else {
        return Err("cold repair appended the wrong specific fate class".into());
    };
    assert!(matches!(
        row.terminal_source,
        StoredOrdinaryTerminalSource::DiedCommitted {
            died_source_sequence: source
        } if source == died_source_sequence
    ));
    let after_specific = ordinary_sequence
        .checked_add(1)
        .ok_or("post-specific sequence overflow")?;
    assert!(block_on(log.read_at(after_specific))??.is_none());
    Ok(())
}

fn source_flush_precedes_advance(
    class: ConnectionFateClass,
    connection_ordinal: u64,
    conversation_id: u64,
    enrollment_token_byte: u8,
) -> Result<(), Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), test_participant_config())?;
    let connection_incarnation = ConnectionIncarnation::new(67, connection_ordinal);
    let mut conversations = ParticipantConnectionConversations::default();
    let enrolled = dispatch_tracked(
        &handler,
        connection_incarnation,
        &mut conversations,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([enrollment_token_byte; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(receipt) = enrolled else {
        return Err(format!("source-cut enrollment did not bind: {enrolled:?}").into());
    };
    let attached = dispatch_tracked(
        &handler,
        connection_incarnation,
        &mut conversations,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id,
            participant_id: receipt.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: receipt.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([enrollment_token_byte; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    if !matches!(attached, ServerValue::AttachBound(_)) {
        return Err(format!("source-cut ordinary attach did not bind: {attached:?}").into());
    }
    let rows_before_source = observer_row_count(&store)?;
    let cell = handler.cell(conversation_id)?;
    let mut owner = cell
        .lock()
        .map_err(|_| "source-cut owner lock was poisoned")?;
    let authority = owner.as_mut().ok_or("source-cut owner was unavailable")?;
    let observer_before = authority.observer_progress;
    let source_sequence = authority.next_log_sequence;
    let log = OperationLog::new(Arc::clone(&store), conversation_id);
    let transaction = authority.prepare_connection_fate_transaction(&ConnectionFateWorkItem {
        open_sequence: 31,
        connection_incarnation,
        class,
        tracked_conversations: conversations.tracked_conversations(),
    });
    let appender = SourceCutAppender {
        log: &log,
        source_flushed: Cell::new(false),
    };
    let completion = transaction.complete(authority, &appender);
    require_source_cut(class, completion)?;
    drop(owner);

    assert_eq!(
        observer_row_count(&store)?,
        rows_before_source,
        "the durable source barrier must not imply an observer Advance"
    );
    let Some(source) = block_on(log.read_at(source_sequence))?? else {
        return Err("source-cut row was not durable".into());
    };
    let projected_progress = committed_source_progress(class, source.operation)?;
    assert!(projected_progress > observer_before);

    let cold = ProductionParticipantHandler::new(Arc::clone(&store), test_participant_config())?;
    let replayed = cold.replay_and_repair(conversation_id, &log)?;
    let repaired_progress = cold
        .observer
        .lock()
        .map_err(|_| "observer owner lock was poisoned")?
        .as_ref()
        .and_then(|observer| observer.aggregate.observer_progress(conversation_id));
    assert_eq!(repaired_progress, Some(projected_progress));
    if class == ConnectionFateClass::ConnectionLost {
        assert_single_ordinary_completion(&log, source_sequence)?;
    }
    let rows_after_repair = observer_row_count(&store)?;
    assert_eq!(
        rows_after_repair,
        rows_before_source
            .checked_add(1)
            .ok_or("observer row count overflow")?
    );
    let repeated = cold.replay_and_repair(conversation_id, &log)?;
    assert_eq!(repeated.observer_progress, replayed.observer_progress);
    assert_eq!(observer_row_count(&store)?, rows_after_repair);
    assert!(replayed.slots.contains_key(&receipt.participant_id()));
    Ok(())
}

#[test]
fn died_source_flush_precedes_observer_advance_and_cold_repair() -> Result<(), Box<dyn Error>> {
    source_flush_precedes_advance(ConnectionFateClass::ConnectionLost, 5, 31, 41)
}

#[test]
fn detached_source_flush_precedes_observer_advance_and_cold_repair() -> Result<(), Box<dyn Error>> {
    source_flush_precedes_advance(ConnectionFateClass::CleanDisconnect, 6, 33, 42)
}

#[test]
fn died_specific_fate_intent_completes_after_source_only_crash() -> Result<(), Box<dyn Error>> {
    source_flush_precedes_advance(ConnectionFateClass::ConnectionLost, 7, 35, 43)
}
