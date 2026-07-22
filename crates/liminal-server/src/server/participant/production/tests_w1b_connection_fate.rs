use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use liminal::durability::{DurableStore, bridge::block_on, open_ephemeral};
use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, DetachAttemptToken, DetachRequest, EnrollmentRequest,
    EnrollmentToken, Generation, ParticipantId, ServerValue,
};

use crate::server::connection::ReadyWaker;
use crate::server::participant::{
    ConnectionFateClass, ConnectionFateWorkItem, InstalledParticipantService,
    ParticipantConnectionContext, ParticipantConnectionConversations, ParticipantSemanticHandler,
    ParticipantServiceFatal,
};

use super::ProductionParticipantHandler;
use super::log::{
    DecodedStoredOperation, OperationLog, StoredDetachedCause, StoredDetachedSource,
    StoredDiedCause, StoredOperation, StoredTerminalDisposition,
};
use super::tests::{dispatch_tracked, test_participant_config};

fn read_operation(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
    sequence: u64,
) -> Result<StoredOperation, Box<dyn Error>> {
    let log = OperationLog::new(Arc::clone(&handler.store), conversation_id);
    let Some(decoded) = block_on(log.read_at(sequence))?? else {
        return Err(format!("operation {sequence} was not durably appended").into());
    };
    let DecodedStoredOperation::V3(operation) = decoded.operation else {
        return Err(format!("operation {sequence} did not use schema v3").into());
    };
    Ok(operation)
}

#[test]
fn connection_lost_appends_died_source_before_transport_teardown() -> Result<(), Box<dyn Error>> {
    let store = Arc::new(open_ephemeral(1)?);
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    let connection_incarnation = ConnectionIncarnation::new(41, 7);
    let conversation_id = 19;
    let mut conversations = ParticipantConnectionConversations::default();
    let enrolled = dispatch_tracked(
        &handler,
        connection_incarnation,
        &mut conversations,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([31; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(receipt) = enrolled else {
        return Err(format!("enrollment did not bind: {enrolled:?}").into());
    };
    let work_item = ConnectionFateWorkItem {
        open_sequence: 3,
        connection_incarnation,
        class: ConnectionFateClass::ConnectionLost,
        tracked_conversations: conversations.tracked_conversations(),
    };

    let cell = handler.cell(conversation_id)?;
    let owner = cell
        .lock()
        .map_err(|_| "connection-fate test owner lock was poisoned")?;
    let authority = owner
        .as_ref()
        .ok_or("connection-fate test owner was unavailable")?;
    let prepared = authority.prepare_connection_fate_transaction(&work_item);
    assert_eq!(prepared.targets().len(), 1);
    assert_eq!(
        prepared.targets()[0].participant_id,
        receipt.participant_id()
    );
    assert_eq!(
        prepared.targets()[0].binding_epoch.connection_incarnation,
        connection_incarnation
    );
    drop(owner);

    handler.handle_connection_fate(work_item.clone())?;
    let StoredOperation::Died { row } = read_operation(&handler, conversation_id, 2)? else {
        return Err("ConnectionLost did not append a Died row".into());
    };
    assert_eq!(row.participant_id, receipt.participant_id());
    assert_eq!(
        row.binding_epoch.to_epoch()?,
        receipt.origin_binding_epoch()
    );
    assert_eq!(row.cause, StoredDiedCause::ConnectionLost);
    assert_eq!(
        row.connection_intent_sequence,
        Some(work_item.open_sequence)
    );
    assert!(row.specific_fate_intent.is_none());
    assert!(matches!(
        row.disposition,
        StoredTerminalDisposition::Committed { .. }
    ));
    let owner = cell
        .lock()
        .map_err(|_| "connection-fate test owner lock was poisoned after append")?;
    let authority = owner
        .as_ref()
        .ok_or("connection-fate test owner was unavailable after append")?;
    let slot = authority
        .slots
        .get(&receipt.participant_id())
        .ok_or("connection-fate target slot disappeared after append")?;
    assert!(matches!(
        slot.binding,
        liminal_protocol::lifecycle::BindingState::Detached
    ));
    drop(owner);
    Ok(())
}

fn enroll_for_fate(
    handler: &ProductionParticipantHandler,
    connection_incarnation: ConnectionIncarnation,
    conversation_id: u64,
    token_byte: u8,
) -> Result<(ParticipantId, Vec<u64>), Box<dyn Error>> {
    let mut conversations = ParticipantConnectionConversations::default();
    let enrolled = dispatch_tracked(
        handler,
        connection_incarnation,
        &mut conversations,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([token_byte; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(receipt) = enrolled else {
        return Err(format!("enrollment did not bind: {enrolled:?}").into());
    };
    Ok((
        receipt.participant_id(),
        conversations.tracked_conversations(),
    ))
}

#[test]
fn clean_disconnect_appends_detached_source_before_transport_teardown() -> Result<(), Box<dyn Error>>
{
    let store = Arc::new(open_ephemeral(1)?);
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    let connection_incarnation = ConnectionIncarnation::new(71, 3);
    let conversation_id = 37;
    let (participant_id, tracked_conversations) =
        enroll_for_fate(&handler, connection_incarnation, conversation_id, 43)?;
    let work_item = ConnectionFateWorkItem {
        open_sequence: 13,
        connection_incarnation,
        class: ConnectionFateClass::CleanDisconnect,
        tracked_conversations,
    };

    handler.handle_connection_fate(work_item.clone())?;
    let StoredOperation::Detached { row } = read_operation(&handler, conversation_id, 2)? else {
        return Err("CleanDisconnect did not append a Detached row".into());
    };
    assert_eq!(row.participant_id, participant_id);
    assert_eq!(row.cause, StoredDetachedCause::CleanDeregister);
    assert!(matches!(
        row.source,
        StoredDetachedSource::ConnectionClose {
            connection_intent_sequence
        } if connection_intent_sequence == work_item.open_sequence
    ));
    Ok(())
}

#[test]
fn server_force_close_appends_shutdown_detached_source_before_release() -> Result<(), Box<dyn Error>>
{
    let store = Arc::new(open_ephemeral(1)?);
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    let connection_incarnation = ConnectionIncarnation::new(73, 4);
    let conversation_id = 41;
    let (participant_id, tracked_conversations) =
        enroll_for_fate(&handler, connection_incarnation, conversation_id, 47)?;
    let work_item = ConnectionFateWorkItem {
        open_sequence: 17,
        connection_incarnation,
        class: ConnectionFateClass::ServerShutdown,
        tracked_conversations,
    };

    handler.handle_connection_fate(work_item.clone())?;
    let StoredOperation::Detached { row } = read_operation(&handler, conversation_id, 2)? else {
        return Err("ServerShutdown did not append a Detached row".into());
    };
    assert_eq!(row.participant_id, participant_id);
    assert_eq!(row.cause, StoredDetachedCause::ServerShutdown);
    assert!(matches!(
        row.source,
        StoredDetachedSource::ConnectionClose {
            connection_intent_sequence
        } if connection_intent_sequence == work_item.open_sequence
    ));
    Ok(())
}

#[test]
fn protocol_error_appends_died_source_only_for_bound_terminal_refusal() -> Result<(), Box<dyn Error>>
{
    let store = Arc::new(open_ephemeral(1)?);
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    let connection_incarnation = ConnectionIncarnation::new(79, 5);
    let conversation_id = 43;
    let (participant_id, tracked_conversations) =
        enroll_for_fate(&handler, connection_incarnation, conversation_id, 53)?;
    let work_item = ConnectionFateWorkItem {
        open_sequence: 19,
        connection_incarnation,
        class: ConnectionFateClass::ProtocolError,
        tracked_conversations,
    };

    handler.handle_connection_fate(work_item.clone())?;
    let StoredOperation::Died { row } = read_operation(&handler, conversation_id, 2)? else {
        return Err("bound ProtocolError did not append a Died row".into());
    };
    assert_eq!(row.participant_id, participant_id);
    assert_eq!(row.cause, StoredDiedCause::ProtocolError);
    assert_eq!(
        row.connection_intent_sequence,
        Some(work_item.open_sequence)
    );

    let absent_conversation = 47;
    handler.handle_connection_fate(ConnectionFateWorkItem {
        tracked_conversations: vec![absent_conversation],
        ..work_item
    })?;
    assert_eq!(handler.registry_len(), 1);
    Ok(())
}

#[test]
fn unclean_restart_appends_prior_incarnation_died_before_owner_publication()
-> Result<(), Box<dyn Error>> {
    let store = Arc::new(open_ephemeral(1)?);
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    let connection_incarnation = ConnectionIncarnation::new(83, 2);
    let conversation_id = 53;
    let (participant_id, _) =
        enroll_for_fate(&handler, connection_incarnation, conversation_id, 59)?;

    handler.repair_unclean_server_restart(
        connection_incarnation
            .server_incarnation
            .checked_add(1)
            .ok_or("restart incarnation overflow")?,
    )?;
    let StoredOperation::Died { row } = read_operation(&handler, conversation_id, 2)? else {
        return Err("unclean restart did not append a Died row".into());
    };
    assert_eq!(row.participant_id, participant_id);
    assert_eq!(
        row.cause,
        StoredDiedCause::UncleanServerRestart {
            prior_server_incarnation: connection_incarnation.server_incarnation,
        }
    );
    assert_eq!(row.connection_intent_sequence, None);
    Ok(())
}

#[test]
fn production_connection_fate_handler_completes_listed_conversation_without_matching_slot()
-> Result<(), Box<dyn Error>> {
    let store = Arc::new(open_ephemeral(1)?);
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    handler.handle_connection_fate(ConnectionFateWorkItem {
        open_sequence: 5,
        connection_incarnation: ConnectionIncarnation::new(43, 2),
        class: ConnectionFateClass::ServerShutdown,
        tracked_conversations: vec![23],
    })?;
    assert_eq!(handler.registry_len(), 0);
    Ok(())
}

#[test]
fn protocol_fate_classification_requires_current_bound_authority() -> Result<(), Box<dyn Error>> {
    let store = Arc::new(open_ephemeral(1)?);
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    let connection_incarnation = ConnectionIncarnation::new(47, 3);
    let other_incarnation = ConnectionIncarnation::new(47, 4);
    let conversation_id = 29;
    let mut conversations = ParticipantConnectionConversations::default();
    let enrolled = dispatch_tracked(
        &handler,
        connection_incarnation,
        &mut conversations,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([37; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(receipt) = enrolled else {
        return Err(format!("enrollment did not bind: {enrolled:?}").into());
    };
    let tracked = conversations.tracked_conversations();
    assert!(handler.connection_has_bound_participant(connection_incarnation, &tracked)?);
    assert!(!handler.connection_has_bound_participant(other_incarnation, &tracked)?);

    let detached = dispatch_tracked(
        &handler,
        connection_incarnation,
        &mut conversations,
        ClientRequest::Detach(DetachRequest {
            conversation_id,
            participant_id: receipt.participant_id(),
            capability_generation: Generation::ONE,
            detach_attempt_token: DetachAttemptToken::new([41; 16]),
        }),
    )?;
    if !matches!(detached, ServerValue::DetachCommitted(_)) {
        return Err(format!("detach did not commit: {detached:?}").into());
    }
    assert!(
        !handler.connection_has_bound_participant(connection_incarnation, &tracked)?,
        "tracked-but-detached state must not select ProtocolError"
    );
    Ok(())
}

#[test]
fn participant_service_fatal_blocks_obligation_dispatch() -> Result<(), Box<dyn Error>> {
    const OPEN_SEQUENCE: u64 = 11;
    const CONVERSATION_ID: u64 = 17;
    const LATER_OPEN_SEQUENCE: u64 = 19;
    const LATER_CONVERSATION_ID: u64 = 23;

    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let config = test_participant_config();
    let handler = Arc::new(ProductionParticipantHandler::new(
        Arc::clone(&store),
        config,
    )?);
    let service = InstalledParticipantService::new(handler.clone(), store, config.wire_frame_limit)
        .map_err(|error| format!("participant service configuration failed: {error:?}"))?;
    let incarnation = ConnectionIncarnation::new(53, 1);
    let inbox = service.new_publication_inbox();
    let wakes = Arc::new(AtomicU64::new(0));
    service.publication_registry().register(
        incarnation,
        &inbox,
        ReadyWaker::for_test(Arc::clone(&wakes)),
    )?;

    let selected =
        service.latch_connection_fate_intent_incomplete(OPEN_SEQUENCE, CONVERSATION_ID)?;
    assert_eq!(
        selected,
        ParticipantServiceFatal::ConnectionFateIntentIncomplete {
            open_sequence: OPEN_SEQUENCE,
            conversation_id: CONVERSATION_ID,
        }
    );
    assert_eq!(
        service
            .latch_connection_fate_intent_incomplete(LATER_OPEN_SEQUENCE, LATER_CONVERSATION_ID,)?,
        selected,
        "the installed wrapper must preserve the inner handler's first fatal"
    );
    assert_eq!(service.service_fatal()?, Some(selected.clone()));
    let work_before = handler.obligation_dispatch_work_snapshot();
    let ready_fires_before = service.publication_registry().ready_fire_count();

    let publication = service.next_publication(incarnation, CONVERSATION_ID, None);
    assert!(matches!(
        publication,
        Err(crate::server::participant::ParticipantSemanticError::ServiceFatal(fatal))
            if fatal == selected
    ));
    let request = service.handle(
        ParticipantConnectionContext::new(incarnation),
        &mut ParticipantConnectionConversations::default(),
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION_ID,
            enrollment_token: EnrollmentToken::new([0x26; 16]),
        }),
    );
    assert!(matches!(
        request,
        Err(crate::server::participant::ParticipantSemanticError::ServiceFatal(fatal))
            if fatal == selected
    ));
    assert_eq!(handler.obligation_dispatch_work_snapshot(), work_before);
    assert_eq!(
        service.publication_registry().ready_fire_count(),
        ready_fires_before
    );
    assert_eq!(wakes.load(Ordering::SeqCst), 0);
    assert!(!inbox.has_pending()?);

    let stopped = service.handle_connection_fate(ConnectionFateWorkItem {
        open_sequence: LATER_OPEN_SEQUENCE,
        connection_incarnation: ConnectionIncarnation::new(53, 1),
        class: ConnectionFateClass::ConnectionLost,
        tracked_conversations: Vec::new(),
    });
    assert!(matches!(
        stopped,
        Err(crate::server::participant::ParticipantSemanticError::ServiceFatal(fatal))
            if fatal == selected
    ));
    Ok(())
}

#[test]
fn process_killed_has_no_production_participant_binding_emitter() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/trybuild/production_connection_fate_cannot_select_process_killed.rs");
}
