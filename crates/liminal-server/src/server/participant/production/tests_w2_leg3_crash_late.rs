use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use liminal::durability::{DurableStore, open_ephemeral};
use liminal_protocol::wire::{
    AttachAttemptToken, ClientRequest, ConnectionIncarnation, CredentialAttachRequest,
    EnrollmentRequest, EnrollmentToken, Generation, ParticipantAck, ServerValue,
};

use super::e2e_cold_all_shapes_fixture::decoded_history_from_store;
use super::log_v3::StoredOperationV3;
use super::tests_outbox_barrier_fixture::{OutboxBarrierKind, OutboxBarrierStore};
use super::tests_w2_leg3_crash_early::{CONVERSATION, enroll_without_tell, handler, installed};
use crate::server::connection::ReadyWaker;
use crate::server::participant::{
    InstalledParticipantService, ParticipantConnectionContext, ParticipantConnectionConversations,
    ParticipantSemanticHandler,
};

fn apply(
    service: &InstalledParticipantService,
    incarnation: ConnectionIncarnation,
    request: ClientRequest,
) -> Result<ServerValue, Box<dyn Error>> {
    service
        .handle(
            ParticipantConnectionContext::new(incarnation),
            &mut ParticipantConnectionConversations::default(),
            request,
        )
        .map_err(Into::into)
}

fn enroll(
    service: &InstalledParticipantService,
    incarnation: ConnectionIncarnation,
    token: u8,
) -> Result<liminal_protocol::wire::EnrollBound, Box<dyn Error>> {
    let value = apply(
        service,
        incarnation,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new([token; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(bound) = value else {
        return Err(format!("late crash fixture enrollment did not bind: {value:?}").into());
    };
    Ok(bound)
}

fn rebind(
    service: &InstalledParticipantService,
    incarnation: ConnectionIncarnation,
    bound: &liminal_protocol::wire::EnrollBound,
    token: u8,
) -> Result<ServerValue, Box<dyn Error>> {
    apply(
        service,
        incarnation,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: CONVERSATION,
            participant_id: bound.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: bound.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([token; 16]),
            accept_marker_delivery_seq: None,
        }),
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AckSnapshot {
    protocol_cursor: u64,
    episode_cursor: Option<u64>,
    outbox_ack_through: u64,
}

fn ack_snapshot(
    handler: &super::ProductionParticipantHandler,
    participant_id: u64,
) -> Result<AckSnapshot, Box<dyn Error>> {
    let cell = handler.cell(CONVERSATION)?;
    let owner = cell
        .lock()
        .map_err(|_| "late crash owner lock was poisoned")?;
    let authority = owner.as_ref().ok_or("late crash authority was absent")?;
    let protocol_cursor = authority
        .slots
        .get(&participant_id)
        .ok_or("late crash participant slot was absent")?
        .member
        .cursor();
    let episode_cursor = authority
        .obligation_debt_dispatch()
        .and_then(|state| state.participant(participant_id).map(|(_, cursor)| cursor));
    let outbox_ack_through = authority
        .outbox
        .as_ref()
        .ok_or("late crash outbox was absent")?
        .ack_through(participant_id);
    drop(owner);
    Ok(AckSnapshot {
        protocol_cursor,
        episode_cursor,
        outbox_ack_through,
    })
}

#[test]
fn crash_after_tell_before_enqueue_rebind_replays_same_obligation() -> Result<(), Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let live = handler(Arc::clone(&store))?;
    let service = installed(&live, Arc::clone(&store))?;
    let old_incarnation = ConnectionIncarnation::new(0xD3, 41);
    let peer_incarnation = ConnectionIncarnation::new(0xD3, 42);
    let rebound_incarnation = ConnectionIncarnation::new(0xD3, 43);
    let first = enroll(&service, old_incarnation, 0x41)?;

    let old_inbox = service.new_publication_inbox();
    let old_wakes = Arc::new(AtomicU64::new(0));
    service.publication_registry().register(
        old_incarnation,
        &old_inbox,
        ReadyWaker::for_test(Arc::clone(&old_wakes)),
    )?;
    let peer = enroll(&service, peer_incarnation, 0x42)?;
    assert_ne!(peer.participant_id(), first.participant_id());
    assert_eq!(old_wakes.load(Ordering::SeqCst), 1);
    assert!(old_inbox.has_pending()?);
    let precrash_endpoint = service
        .next_publication(old_incarnation, CONVERSATION, None)?
        .ok_or("tell did not correspond to a durable obligation")?
        .delivery_seq();
    drop(old_inbox);
    drop(service);
    drop(live);

    let cold = handler(Arc::clone(&store))?;
    let cold_service = installed(&cold, store)?;
    let rebound_inbox = cold_service.new_publication_inbox();
    let rebound_wakes = Arc::new(AtomicU64::new(0));
    cold_service.publication_registry().register(
        rebound_incarnation,
        &rebound_inbox,
        ReadyWaker::for_test(Arc::clone(&rebound_wakes)),
    )?;
    assert_eq!(rebound_wakes.load(Ordering::SeqCst), 0);
    assert!(!rebound_inbox.has_pending()?);
    assert!(matches!(
        rebind(&cold_service, rebound_incarnation, &first, 0x43)?,
        ServerValue::AttachBound(_)
    ));
    assert_eq!(rebound_wakes.load(Ordering::SeqCst), 1);
    let replayed = cold_service
        .next_publication(rebound_incarnation, CONVERSATION, None)?
        .ok_or("committed rebind did not replay the lost-tell obligation")?;
    assert_eq!(replayed.delivery_seq(), precrash_endpoint);
    Ok(())
}

#[test]
fn crash_after_enqueue_before_ack_reoffers_and_accepts_endpoint() -> Result<(), Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let live = handler(Arc::clone(&store))?;
    let old_incarnation = ConnectionIncarnation::new(0xD3, 51);
    let peer_incarnation = ConnectionIncarnation::new(0xD3, 52);
    let rebound_incarnation = ConnectionIncarnation::new(0xD3, 53);
    let third_incarnation = ConnectionIncarnation::new(0xD3, 54);
    let first = enroll_without_tell(&live, old_incarnation, 0x51)?;
    let peer = enroll_without_tell(&live, peer_incarnation, 0x52)?;
    let third = enroll_without_tell(&live, third_incarnation, 0x54)?;
    assert_ne!(peer.participant_id(), first.participant_id());
    assert_ne!(third.participant_id(), first.participant_id());
    let service = installed(&live, Arc::clone(&store))?;
    let enqueued = service
        .next_publication(old_incarnation, CONVERSATION, None)?
        .ok_or("enqueue cut fixture had no obligation")?;
    service.record_publication_offer(&enqueued)?;
    let enqueued_endpoint = enqueued.delivery_seq();
    drop(service);
    drop(live);

    let cold = handler(Arc::clone(&store))?;
    let cold_service = installed(&cold, store)?;
    let rebound = rebind(&cold_service, rebound_incarnation, &first, 0x53)?;
    let ServerValue::AttachBound(rebound_bound) = rebound else {
        return Err(format!("cut-4 rebind did not commit: {rebound:?}").into());
    };
    let reoffered = cold_service
        .next_publication(rebound_incarnation, CONVERSATION, None)?
        .ok_or("restart did not reoffer the unacked enqueued obligation")?;
    assert_eq!(reoffered.delivery_seq(), enqueued_endpoint);
    let before_ack = ack_snapshot(&cold, first.participant_id())?;
    if before_ack.protocol_cursor != before_ack.episode_cursor.unwrap_or_default() {
        return Err(
            format!("rebind left cursor authority mixed before ack: {before_ack:?}").into(),
        );
    }
    let ack = apply(
        &cold_service,
        rebound_incarnation,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: CONVERSATION,
            participant_id: first.participant_id(),
            capability_generation: rebound_bound.capability_generation(),
            through_seq: enqueued_endpoint,
        }),
    )?;
    let ServerValue::AckCommitted(_) = ack else {
        return Err(format!("reoffered endpoint was not committed: {ack:?}").into());
    };
    let committed = ack_snapshot(&cold, first.participant_id())?;
    assert_eq!(committed.protocol_cursor, enqueued_endpoint);
    assert_eq!(committed.episode_cursor, Some(enqueued_endpoint));
    assert_eq!(committed.outbox_ack_through, enqueued_endpoint);
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BarrierRestartOutcome {
    Old,
    CoupledCommit,
    LoudStartupFailure,
}

fn run_nonzero_barrier_cut(
    cut: OutboxBarrierKind,
    ordinal: u64,
) -> Result<BarrierRestartOutcome, Box<dyn Error>> {
    let inner: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let barriers = Arc::new(OutboxBarrierStore::new(Arc::clone(&inner)));
    let gated: Arc<dyn DurableStore> = barriers.clone();
    let live = handler(Arc::clone(&gated))?;
    let recipient_incarnation = ConnectionIncarnation::new(0xD3, ordinal);
    let peer_incarnation = ConnectionIncarnation::new(
        0xD3,
        ordinal
            .checked_add(1)
            .ok_or("barrier-cut connection ordinal overflowed")?,
    );
    let recipient = enroll_without_tell(&live, recipient_incarnation, ordinal.to_le_bytes()[0])?;
    let peer = enroll_without_tell(
        &live,
        peer_incarnation,
        peer_incarnation.connection_ordinal.to_le_bytes()[0],
    )?;
    assert_ne!(recipient.participant_id(), peer.participant_id());
    let endpoint = live
        .next_publication(recipient_incarnation, CONVERSATION, None)?
        .ok_or("barrier cut fixture had no nonzero obligation")?
        .delivery_seq();
    let before = ack_snapshot(&live, recipient.participant_id())?;
    let service = installed(&live, Arc::clone(&gated))?;
    barriers.fail_next(cut)?;
    let result = service.handle(
        ParticipantConnectionContext::new(recipient_incarnation),
        &mut ParticipantConnectionConversations::default(),
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: CONVERSATION,
            participant_id: recipient.participant_id(),
            capability_generation: Generation::ONE,
            through_seq: endpoint,
        }),
    );
    assert!(
        result.is_err(),
        "the armed durability barrier must cut the ack"
    );
    drop(service);
    drop(live);
    drop(barriers);

    let cold = match handler(Arc::clone(&inner)) {
        Ok(cold) => cold,
        Err(error) => {
            assert!(!error.to_string().is_empty());
            return Ok(BarrierRestartOutcome::LoudStartupFailure);
        }
    };
    let after = ack_snapshot(&cold, recipient.participant_id())?;
    let (rows, _) = decoded_history_from_store(inner, CONVERSATION)?;
    let nonzero_rows = rows
        .iter()
        .filter(|(_, row)| matches!(row, StoredOperationV3::NonzeroDebtAck { .. }))
        .count();
    if after == before {
        assert_eq!(nonzero_rows, 0);
        return Ok(BarrierRestartOutcome::Old);
    }
    let coupled = after.protocol_cursor == endpoint
        && after.episode_cursor == Some(endpoint)
        && after.outbox_ack_through == endpoint;
    if !coupled {
        return Err(format!(
            "mixed nonzero ack authority after {cut:?}: before={before:?}, after={after:?}"
        )
        .into());
    }
    assert_eq!(nonzero_rows, 1);
    Ok(BarrierRestartOutcome::CoupledCommit)
}

#[test]
fn crash_between_nonzero_ack_barriers_reconciles_one_coupled_commit() -> Result<(), Box<dyn Error>>
{
    let cuts = [
        OutboxBarrierKind::OperationAppend,
        OutboxBarrierKind::OperationFlush,
        OutboxBarrierKind::OutboxAppend,
        OutboxBarrierKind::OutboxFlush,
    ];
    let mut outcomes = Vec::with_capacity(cuts.len());
    for (index, cut) in cuts.into_iter().enumerate() {
        let ordinal = u64::try_from(index)
            .ok()
            .and_then(|index| index.checked_add(61))
            .ok_or("barrier-cut ordinal conversion overflowed")?;
        outcomes.push(run_nonzero_barrier_cut(cut, ordinal)?);
    }
    assert_eq!(outcomes.len(), cuts.len());
    assert!(outcomes.iter().all(|outcome| matches!(
        outcome,
        BarrierRestartOutcome::Old
            | BarrierRestartOutcome::CoupledCommit
            | BarrierRestartOutcome::LoudStartupFailure
    )));
    Ok(())
}
