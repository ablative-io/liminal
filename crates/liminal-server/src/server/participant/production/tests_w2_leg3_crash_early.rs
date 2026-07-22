use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use liminal::durability::{DurableStore, open_ephemeral};
use liminal_protocol::wire::{
    AttachAttemptToken, ClientRequest, ConnectionIncarnation, CredentialAttachRequest,
    EnrollmentRequest, EnrollmentToken, Generation, ParticipantAck, ServerValue,
};

use super::ProductionParticipantHandler;
use super::tests::test_participant_config;
use super::tests_outbox_barrier_fixture::{OutboxBarrierKind, OutboxBarrierStore};
use crate::server::connection::ReadyWaker;
use crate::server::participant::dispatch_impact::DispatchImpact;
use crate::server::participant::{
    InstalledParticipantService, ParticipantConnectionContext, ParticipantConnectionConversations,
    ParticipantSemanticHandler,
};

pub(super) const CONVERSATION: u64 = 0xD3_20;

pub(super) fn handler(
    store: Arc<dyn DurableStore>,
) -> Result<Arc<ProductionParticipantHandler>, Box<dyn Error>> {
    let mut config = test_participant_config();
    config.retained_capacity_entries = 12;
    Ok(Arc::new(ProductionParticipantHandler::new(store, config)?))
}

pub(super) fn apply_without_tell(
    handler: &ProductionParticipantHandler,
    incarnation: ConnectionIncarnation,
    request: ClientRequest,
) -> Result<(ServerValue, DispatchImpact), Box<dyn Error>> {
    let outcome = handler.handle_with_impact(
        ParticipantConnectionContext::new(incarnation),
        &mut ParticipantConnectionConversations::default(),
        request,
    );
    let (result, impact) = outcome.into_parts();
    Ok((result?, impact))
}

pub(super) fn enroll_without_tell(
    handler: &ProductionParticipantHandler,
    incarnation: ConnectionIncarnation,
    token: u8,
) -> Result<liminal_protocol::wire::EnrollBound, Box<dyn Error>> {
    let (value, _) = apply_without_tell(
        handler,
        incarnation,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new([token; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(bound) = value else {
        return Err(format!("crash fixture enrollment did not bind: {value:?}").into());
    };
    Ok(bound)
}

pub(super) fn installed(
    handler: &Arc<ProductionParticipantHandler>,
    store: Arc<dyn DurableStore>,
) -> Result<InstalledParticipantService, Box<dyn Error>> {
    let semantic: Arc<dyn ParticipantSemanticHandler> = handler.clone();
    InstalledParticipantService::new(semantic, store, test_participant_config().wire_frame_limit)
        .map_err(|error| format!("participant service configuration failed: {error:?}").into())
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct DispatchSnapshot {
    pub(super) authority: String,
    pub(super) selected_endpoint: Option<u64>,
}

pub(super) fn dispatch_snapshot(
    handler: &ProductionParticipantHandler,
    incarnation: ConnectionIncarnation,
) -> Result<DispatchSnapshot, Box<dyn Error>> {
    let cell = handler.cell(CONVERSATION)?;
    let owner = cell
        .lock()
        .map_err(|_| "crash fixture conversation lock was poisoned")?;
    let authority = format!(
        "{:?}",
        owner.as_ref().ok_or("conversation authority was absent")?
    );
    drop(owner);
    let selected_endpoint = handler
        .next_publication(incarnation, CONVERSATION, None)?
        .map(|publication| publication.delivery_seq());
    Ok(DispatchSnapshot {
        authority,
        selected_endpoint,
    })
}

#[test]
fn crash_before_debt_flush_restores_prior_dispatch_state() -> Result<(), Box<dyn Error>> {
    let inner: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let barriers = Arc::new(OutboxBarrierStore::new(Arc::clone(&inner)));
    let gated_store: Arc<dyn DurableStore> = barriers.clone();
    let live = handler(Arc::clone(&gated_store))?;
    let sender_incarnation = ConnectionIncarnation::new(0xD3, 19);
    let peer_incarnation = ConnectionIncarnation::new(0xD3, 20);
    let sender = enroll_without_tell(&live, sender_incarnation, 0x19)?;
    let peer = enroll_without_tell(&live, peer_incarnation, 0x20)?;
    assert_ne!(sender.participant_id(), peer.participant_id());
    let before = dispatch_snapshot(&live, sender_incarnation)?;
    let candidate_endpoint = before
        .selected_endpoint
        .ok_or("pre-flush fixture had no selected obligation to acknowledge")?;

    let service = installed(&live, Arc::clone(&gated_store))?;
    let inbox = service.new_publication_inbox();
    let wake_count = Arc::new(AtomicU64::new(0));
    service.publication_registry().register(
        sender_incarnation,
        &inbox,
        ReadyWaker::for_test(Arc::clone(&wake_count)),
    )?;
    barriers.fail_next(OutboxBarrierKind::OperationAppend)?;
    let failed = service.handle(
        ParticipantConnectionContext::new(sender_incarnation),
        &mut ParticipantConnectionConversations::default(),
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: CONVERSATION,
            participant_id: sender.participant_id(),
            capability_generation: Generation::ONE,
            through_seq: candidate_endpoint,
        }),
    );
    assert!(
        failed.is_err(),
        "the pre-flush append cut must fail the candidate"
    );
    assert_eq!(wake_count.load(Ordering::SeqCst), 0);
    assert!(!inbox.has_pending()?);

    drop(service);
    drop(live);
    drop(barriers);
    let cold = handler(inner)?;
    assert_eq!(dispatch_snapshot(&cold, sender_incarnation)?, before);
    Ok(())
}

#[test]
fn crash_after_debt_flush_before_tell_rebind_replays_ready_work() -> Result<(), Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let old = handler(Arc::clone(&store))?;
    let old_incarnation = ConnectionIncarnation::new(0xD3, 30);
    let peer_incarnation = ConnectionIncarnation::new(0xD3, 31);
    let rebound_incarnation = ConnectionIncarnation::new(0xD3, 32);
    let first = enroll_without_tell(&old, old_incarnation, 0x30)?;
    let (_peer, committed_impact) = {
        let (value, impact) = apply_without_tell(
            &old,
            peer_incarnation,
            ClientRequest::Enrollment(EnrollmentRequest {
                conversation_id: CONVERSATION,
                enrollment_token: EnrollmentToken::new([0x31; 16]),
            }),
        )?;
        let ServerValue::EnrollBound(peer) = value else {
            return Err(format!("second crash fixture enrollment did not bind: {value:?}").into());
        };
        (peer, impact)
    };
    assert!(!committed_impact.target_union().is_empty());
    let precrash_endpoint = old
        .next_publication(old_incarnation, CONVERSATION, None)?
        .ok_or("durable change produced no pre-crash obligation")?
        .delivery_seq();
    drop(old);

    let cold = handler(Arc::clone(&store))?;
    let service = installed(&cold, store)?;
    let inbox = service.new_publication_inbox();
    let wake_count = Arc::new(AtomicU64::new(0));
    let work_before_registration = cold.obligation_dispatch_work_snapshot();
    service.publication_registry().register(
        rebound_incarnation,
        &inbox,
        ReadyWaker::for_test(Arc::clone(&wake_count)),
    )?;
    assert_eq!(
        cold.obligation_dispatch_work_snapshot(),
        work_before_registration
    );
    assert_eq!(wake_count.load(Ordering::SeqCst), 0);
    assert!(!inbox.has_pending()?);

    let rebound = service.handle(
        ParticipantConnectionContext::new(rebound_incarnation),
        &mut ParticipantConnectionConversations::default(),
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: CONVERSATION,
            participant_id: first.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: first.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([0x32; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    assert!(matches!(rebound, ServerValue::AttachBound(_)));
    assert_eq!(wake_count.load(Ordering::SeqCst), 1);
    assert_eq!(inbox.take_ready()?.conversations, vec![CONVERSATION]);
    let replayed_endpoint = service
        .next_publication(rebound_incarnation, CONVERSATION, None)?
        .ok_or("committed rebind did not select cold-replayed work")?
        .delivery_seq();
    assert_eq!(replayed_endpoint, precrash_endpoint);
    Ok(())
}
