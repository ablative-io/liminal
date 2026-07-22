use std::error::Error;
use std::sync::Arc;

use liminal::durability::{DurableStore, open_ephemeral};
use liminal_protocol::wire::{
    AttachAttemptToken, ClientRequest, ConnectionIncarnation, CredentialAttachRequest,
    EnrollmentRequest, EnrollmentToken, Generation, ParticipantAck, ServerValue,
};

use super::{RecordingSink, service_participant_publications};
use crate::server::connection::state::ConnectionProcessState;
use crate::server::participant::{
    ConnectionFateClass, ConnectionFateWorkItem, InstalledParticipantService,
    ParticipantConnectionContext, ParticipantConnectionConversations, ParticipantSemanticHandler,
};
use crate::server::participant::{ProductionParticipantHandler, test_participant_config};

fn production_service(
    retained_capacity_entries: u64,
) -> Result<
    (
        Arc<ProductionParticipantHandler>,
        InstalledParticipantService,
    ),
    Box<dyn Error>,
> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let mut config = test_participant_config();
    config.retained_capacity_entries = retained_capacity_entries;
    let handler = Arc::new(ProductionParticipantHandler::new(
        Arc::clone(&store),
        config,
    )?);
    let semantic: Arc<dyn ParticipantSemanticHandler> = handler.clone();
    let service = InstalledParticipantService::new(semantic, store, config.wire_frame_limit)
        .map_err(|error| format!("production service configuration failed: {error:?}"))?;
    Ok((handler, service))
}

fn enroll_bound(
    service: &InstalledParticipantService,
    incarnation: ConnectionIncarnation,
    conversation_id: u64,
    token: u8,
) -> Result<liminal_protocol::wire::EnrollBound, Box<dyn Error>> {
    let value = service.handle(
        ParticipantConnectionContext::new(incarnation),
        &mut ParticipantConnectionConversations::default(),
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([token; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(bound) = value else {
        return Err(format!("production enrollment did not bind: {value:?}").into());
    };
    Ok(bound)
}

fn enroll(
    service: &InstalledParticipantService,
    incarnation: ConnectionIncarnation,
    conversation_id: u64,
    token: u8,
) -> Result<u64, Box<dyn Error>> {
    Ok(enroll_bound(service, incarnation, conversation_id, token)?.participant_id())
}

fn ready_state(
    service: &InstalledParticipantService,
    incarnation: ConnectionIncarnation,
    conversation_id: u64,
) -> Result<ConnectionProcessState, Box<dyn Error>> {
    let inbox = service.new_publication_inbox();
    inbox.requeue([conversation_id])?;
    Ok(ConnectionProcessState {
        connection_incarnation: Some(incarnation),
        participant_publication: Some(inbox),
        ..ConnectionProcessState::default()
    })
}

#[test]
fn held_obligation_revalidates_binding_and_debt_before_offer() -> Result<(), Box<dyn Error>> {
    let conversation_id = 0xD3_04;
    let held_incarnation = ConnectionIncarnation::new(0xD3, 1);
    let peer_incarnation = ConnectionIncarnation::new(0xD3, 2);
    let (handler, service) = production_service(12)?;
    let held_participant = enroll(&service, held_incarnation, conversation_id, 0x41)?;
    let peer_participant = enroll(&service, peer_incarnation, conversation_id, 0x42)?;
    assert_ne!(held_participant, peer_participant);

    let selected = service
        .next_publication(held_incarnation, conversation_id, None)?
        .ok_or("production fixture had no obligation to hold")?;
    let held_endpoint = selected.delivery_seq();
    let work_before_hold = handler.obligation_dispatch_work_snapshot();
    let mut state = ready_state(&service, held_incarnation, conversation_id)?;
    let mut sink = RecordingSink::new(4096);
    sink.fill_current_room();
    assert_eq!(
        service_participant_publications(&mut state, &service, &mut sink, 1)?,
        0
    );
    let work_after_hold = handler.obligation_dispatch_work_snapshot();
    assert!(work_after_hold.selector_calls > work_before_hold.selector_calls);
    assert_eq!(state.held_pushes.participant_len(), 1);
    assert!(state.participant_offered.is_empty());

    let ack = service.handle(
        ParticipantConnectionContext::new(held_incarnation),
        &mut ParticipantConnectionConversations::default(),
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id,
            participant_id: held_participant,
            capability_generation: Generation::ONE,
            through_seq: held_endpoint,
        }),
    )?;
    assert!(matches!(ack, ServerValue::AckCommitted(_)));

    sink.writable();
    assert_eq!(
        service_participant_publications(&mut state, &service, &mut sink, 1)?,
        0,
        "the stale held verdict must not survive the committed debt/cursor change"
    );
    let work_after_revalidation = handler.obligation_dispatch_work_snapshot();
    assert!(work_after_revalidation.selector_calls > work_after_hold.selector_calls);
    assert!(state.held_pushes.is_empty());
    assert!(state.participant_offered.is_empty());
    assert!(sink.frames.is_empty());
    Ok(())
}

#[test]
fn connection_fate_drops_stale_head_and_replays_after_reconciled_cursor()
-> Result<(), Box<dyn Error>> {
    let conversation_id = 0xD3_18;
    let old_incarnation = ConnectionIncarnation::new(0xD3, 18);
    let peer_incarnation = ConnectionIncarnation::new(0xD3, 19);
    let new_incarnation = ConnectionIncarnation::new(0xD3, 20);
    let (_handler, service) = production_service(12)?;
    let bound = enroll_bound(&service, old_incarnation, conversation_id, 0x81)?;
    enroll(&service, peer_incarnation, conversation_id, 0x82)?;

    let selected = service
        .next_publication(old_incarnation, conversation_id, None)?
        .ok_or("fate fixture had no durable obligation")?;
    let endpoint = selected.delivery_seq();
    let mut state = ready_state(&service, old_incarnation, conversation_id)?;
    let mut sink = RecordingSink::new(4096);
    sink.fill_current_room();
    assert_eq!(
        service_participant_publications(&mut state, &service, &mut sink, 1)?,
        0
    );
    assert_eq!(state.held_pushes.participant_len(), 1);

    service.handle_connection_fate(ConnectionFateWorkItem {
        open_sequence: 18,
        connection_incarnation: old_incarnation,
        class: ConnectionFateClass::CleanDisconnect,
        tracked_conversations: vec![conversation_id],
    })?;
    sink.writable();
    assert_eq!(
        service_participant_publications(&mut state, &service, &mut sink, 1)?,
        0
    );
    assert!(state.held_pushes.is_empty());
    assert!(state.participant_offered.is_empty());
    assert!(sink.frames.is_empty());

    let attached = service.handle(
        ParticipantConnectionContext::new(new_incarnation),
        &mut ParticipantConnectionConversations::default(),
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id,
            participant_id: bound.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: bound.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([0x83; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    assert!(matches!(attached, ServerValue::AttachBound(_)));
    let replayed = service
        .next_publication(new_incarnation, conversation_id, None)?
        .ok_or("post-fate binding did not replay the surviving obligation")?;
    assert_eq!(replayed.delivery_seq(), endpoint);
    Ok(())
}

#[test]
fn deferred_debt_does_not_self_requeue_or_create_debt_wakes() -> Result<(), Box<dyn Error>> {
    let conversation_id = 0xD3_07;
    let incarnation = ConnectionIncarnation::new(0xD3, 7);
    let (handler, service) = production_service(12)?;
    enroll(&service, incarnation, conversation_id, 0x71)?;
    let mut state = ready_state(&service, incarnation, conversation_id)?;
    let mut sink = RecordingSink::new(4096);
    let ready_fires_before = service.publication_registry().ready_fire_count();

    assert_eq!(
        service_participant_publications(&mut state, &service, &mut sink, 1)?,
        0
    );
    let after_defer = handler.obligation_dispatch_work_snapshot();
    assert!(state.held_pushes.is_empty());
    assert!(
        !state
            .participant_publication
            .as_ref()
            .ok_or("production publication inbox was absent")?
            .has_pending()?
    );
    assert!(state.participant_offered.is_empty());
    assert!(sink.frames.is_empty());

    assert_eq!(
        service_participant_publications(&mut state, &service, &mut sink, 1)?,
        0
    );
    assert_eq!(handler.obligation_dispatch_work_snapshot(), after_defer);
    assert_eq!(
        service.publication_registry().ready_fire_count(),
        ready_fires_before
    );
    Ok(())
}
