use std::error::Error;
use std::sync::Arc;

use liminal::durability::{DurableStore, open_ephemeral};
use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, DetachAttemptToken, DetachRequest, EnrollmentRequest,
    EnrollmentToken, Generation, ServerValue,
};

use crate::server::participant::{
    ConnectionFateClass, ConnectionFateWorkItem, InstalledParticipantService,
    ParticipantConnectionConversations, ParticipantSemanticHandler, ParticipantServiceFatal,
};

use super::ProductionParticipantHandler;
use super::tests::{dispatch_tracked, test_participant_config};

#[test]
fn production_connection_fate_handler_records_authority_selected_targets()
-> Result<(), Box<dyn Error>> {
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

    handler.handle_connection_fate(work_item)?;
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
fn installed_participant_service_delegates_connection_fate_fatal_latch()
-> Result<(), Box<dyn Error>> {
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
    let service = InstalledParticipantService::new(handler, store, config.wire_frame_limit)
        .map_err(|error| format!("participant service configuration failed: {error:?}"))?;

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
