use std::cell::Cell;
use std::error::Error;
use std::sync::Arc;

use liminal::durability::{DurableStore, bridge::block_on, open_ephemeral};
use liminal_protocol::lifecycle::{BindingState, PendingFinalization};
use liminal_protocol::wire::{
    AttachAttemptToken, BindingEpoch, ClientRequest, ConnectionIncarnation,
    CredentialAttachRequest, EnrollmentRequest, EnrollmentToken, Generation, RecordAdmission,
    RecordAdmissionAttemptToken, ServerValue,
};

use crate::server::participant::{
    ConnectionFateClass, ConnectionFateWorkItem, ParticipantConnectionConversations,
};

use super::ProductionParticipantHandler;
use super::log::{
    DecodedStoredOperation, OperationLog, OperationLogError, StoredDiedCause, StoredOperation,
    StoredSpecificFateIntent, StoredTerminalDisposition,
};
use super::state::DurableAppend;
use super::tests::{dispatch_tracked, test_participant_config};

struct SourceOnlyAppender<'a> {
    log: &'a OperationLog,
    source_flushed: Cell<bool>,
}

impl DurableAppend for SourceOnlyAppender<'_> {
    fn append(
        &self,
        operation: &StoredOperation,
        expected_sequence: u64,
    ) -> Result<(), OperationLogError> {
        if self.source_flushed.replace(true) {
            return Err(OperationLogError::FateReplayDrift {
                sequence: expected_sequence,
            });
        }
        block_on(self.log.append(operation, expected_sequence))?
    }
}

struct BoundDebtFixture {
    handler: ProductionParticipantHandler,
    conversations: ParticipantConnectionConversations,
    connection: ConnectionIncarnation,
    conversation_id: u64,
    participant_id: u64,
    binding_epoch: BindingEpoch,
}

struct PendingRestartFixture {
    handler: ProductionParticipantHandler,
    log: OperationLog,
    conversation_id: u64,
    participant_id: u64,
    binding_epoch: BindingEpoch,
    died_source_sequence: u64,
    specific_sequence: u64,
    terminal_order: u64,
    next_terminal_sequence: u64,
}

fn bound_debt_fixture() -> Result<BoundDebtFixture, Box<dyn Error>> {
    let conversation_id = 67;
    let connection = ConnectionIncarnation::new(97, 3);
    let peer_connection = ConnectionIncarnation::new(97, 4);
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let mut config = test_participant_config();
    config.max_retained_record_rows = 4;
    let handler = ProductionParticipantHandler::new(store, config)?;
    let mut conversations = ParticipantConnectionConversations::default();
    let enrolled = dispatch_tracked(
        &handler,
        connection,
        &mut conversations,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([83; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(receipt) = enrolled else {
        return Err(format!("pending-Died restart fixture did not enroll: {enrolled:?}").into());
    };
    let attached = dispatch_tracked(
        &handler,
        connection,
        &mut conversations,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id,
            participant_id: receipt.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: receipt.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([85; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    let ServerValue::AttachBound(attached) = attached else {
        return Err(format!("pending-Died ordinary attach did not bind: {attached:?}").into());
    };
    let mut peer_conversations = ParticipantConnectionConversations::default();
    let peer = dispatch_tracked(
        &handler,
        peer_connection,
        &mut peer_conversations,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([87; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(peer) = peer else {
        return Err(format!("pending-Died peer did not enroll: {peer:?}").into());
    };
    assert_ne!(peer.participant_id(), receipt.participant_id());
    let record = dispatch_tracked(
        &handler,
        connection,
        &mut conversations,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id,
            participant_id: receipt.participant_id(),
            capability_generation: attached.origin_binding_epoch().capability_generation,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([89; 16]),
            payload: vec![91],
        }),
    )?;
    if !matches!(record, ServerValue::RecordCommitted(_)) {
        return Err(format!("pending-Died debt record did not commit: {record:?}").into());
    }
    Ok(BoundDebtFixture {
        handler,
        conversations,
        connection,
        conversation_id,
        participant_id: receipt.participant_id(),
        binding_epoch: attached.origin_binding_epoch(),
    })
}

fn pending_restart_fixture() -> Result<PendingRestartFixture, Box<dyn Error>> {
    let setup = bound_debt_fixture()?;
    let cell = setup.handler.cell(setup.conversation_id)?;
    let mut owner = cell
        .lock()
        .map_err(|_| "pending-Died restart owner lock was poisoned")?;
    let authority = owner
        .as_mut()
        .ok_or("pending-Died restart owner was unavailable")?;
    let died_source_sequence = authority.next_log_sequence;
    let next_terminal_sequence = authority.next_seq;
    let log = OperationLog::new(Arc::clone(&setup.handler.store), setup.conversation_id);
    authority
        .prepare_connection_fate_transaction(&ConnectionFateWorkItem {
            open_sequence: 37,
            connection_incarnation: setup.connection,
            class: ConnectionFateClass::ConnectionLost,
            tracked_conversations: setup.conversations.tracked_conversations(),
        })
        .complete(
            authority,
            &SourceOnlyAppender {
                log: &log,
                source_flushed: Cell::new(false),
            },
        )?;
    drop(owner);
    let Some(source) = block_on(log.read_at(died_source_sequence))?? else {
        return Err("pending-Died source-only cut omitted Died".into());
    };
    let DecodedStoredOperation::V3(StoredOperation::Died { row }) = source.operation else {
        return Err("pending-Died source-only cut appended the wrong row".into());
    };
    assert_eq!(row.cause, StoredDiedCause::ConnectionLost);
    assert_eq!(row.disposition, StoredTerminalDisposition::Pending);
    assert!(matches!(
        row.specific_fate_intent,
        Some(StoredSpecificFateIntent::Ordinary { .. })
    ));
    let specific_sequence = died_source_sequence
        .checked_add(1)
        .ok_or("pending-Died specific sequence overflow")?;
    assert!(block_on(log.read_at(specific_sequence))??.is_none());
    Ok(PendingRestartFixture {
        handler: setup.handler,
        log,
        conversation_id: setup.conversation_id,
        participant_id: setup.participant_id,
        binding_epoch: setup.binding_epoch,
        died_source_sequence,
        specific_sequence,
        terminal_order: row.terminal_order,
        next_terminal_sequence,
    })
}

#[test]
fn pending_died_restart_restores_cause_epoch_order_without_refinish() -> Result<(), Box<dyn Error>>
{
    let fixture = pending_restart_fixture()?;
    let replayed = fixture
        .handler
        .replay_aggregate_reference(fixture.conversation_id, &fixture.log)?;
    let replayed_slot = replayed
        .slots
        .get(&fixture.participant_id)
        .ok_or("pending-Died replay omitted its participant")?;
    let BindingState::PendingFinalization(PendingFinalization::Died(pending)) =
        replayed_slot.binding
    else {
        return Err("pending-Died replay did not restore Pending Died".into());
    };
    assert_eq!(pending.binding_epoch(), fixture.binding_epoch);
    assert_eq!(
        pending.cause(),
        liminal_protocol::wire::DiedCause::ConnectionLost
    );
    assert_eq!(
        pending.admission_order().transaction_order(),
        fixture.terminal_order
    );
    assert_eq!(replayed.next_seq, fixture.next_terminal_sequence);
    let open_intent = replayed
        .pending_specific_fates
        .get(&fixture.participant_id)
        .ok_or("pending-Died replay omitted its open specific intent")?;
    assert_eq!(
        open_intent.died_source_sequence,
        fixture.died_source_sequence
    );
    assert!(open_intent.terminal.is_none());
    assert!(replayed_slot.binding_fate.is_some());

    let repeated = fixture
        .handler
        .replay_aggregate_reference(fixture.conversation_id, &fixture.log)?;
    assert_eq!(repeated.next_seq, replayed.next_seq);
    assert_eq!(repeated.next_order, replayed.next_order);
    assert_eq!(repeated.next_log_sequence, replayed.next_log_sequence);
    assert!(block_on(fixture.log.read_at(fixture.specific_sequence))??.is_none());
    Ok(())
}
