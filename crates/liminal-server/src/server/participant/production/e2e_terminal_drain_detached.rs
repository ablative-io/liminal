//! Live-socket Detached-flavor candidate-lane terminal drain, live resume of
//! the drained victim, and real unclean-restart replay (S-18 pin 9, with the
//! decisive pin-5 replay assertion riding the resume leg).
//!
//! Built in the same buildable ordering the Died e2e pinned: the residence is
//! minted LIVE (the ruled restore-then-publish ordering stays structurally
//! unbuildable at this base — the pending residence requires retention at its
//! cap, and at-cap attach/enrollment is hard-refused after a restart, so no
//! publisher could exist). The pending-Detached residence is minted by a
//! clean client `Disconnect` at the retention cap — the `CleanDisconnect`
//! connection fate, cause `CleanDeregister`. The pin text names the
//! `ServerShutdown` mint; a real server shutdown fates EVERY live connection,
//! and with the publisher's connection also fated there is no live socket
//! left to publish the drain, so the socket leg exercises the clean-close
//! mint of the same pending-Detached residence class while the dispatch
//! suite (`tests_restore_window_detached.rs`) covers the `ServerShutdown`
//! cause end to end. Reported, not silent — the tear seat sizes this leg.

use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

use liminal::durability::DurableStore;
use liminal::durability::bridge::block_on;
use liminal_protocol::lifecycle::BindingState;
use liminal_protocol::wire::{
    AttachAttemptToken, ClientRequest, CredentialAttachRequest, EnrollBound, EnrollmentRequest,
    EnrollmentToken, Generation, ParticipantAck, ParticipantRecord, RecordAdmission,
    RecordAdmissionAttemptToken, ServerPush, ServerValue,
};

use super::ProductionParticipantHandler;
use super::e2e_tests::{SocketFixture, SocketPeer};
use super::log::{
    DecodedStoredOperation, OperationLog, StoredDetachedCause, StoredDetachedSource,
    StoredOperation, StoredTerminalDisposition,
};
use super::tests::test_participant_config;

const CONVERSATION: u64 = 607;

fn small_retention_config() -> crate::config::types::ParticipantConfig {
    let mut config = test_participant_config();
    config.max_retained_record_rows = 4;
    config
}

fn enroll_bound(
    value: Result<ServerValue, Box<dyn Error>>,
    what: &str,
) -> Result<EnrollBound, Box<dyn Error>> {
    match value? {
        ServerValue::EnrollBound(receipt) => Ok(receipt),
        other => Err(format!("{what} did not bind: {other:?}").into()),
    }
}

fn committed_seq(
    value: Result<ServerValue, Box<dyn Error>>,
    what: &str,
) -> Result<u64, Box<dyn Error>> {
    match value? {
        ServerValue::RecordCommitted(record) => Ok(record.delivery_seq()),
        other => Err(format!("{what} did not commit: {other:?}").into()),
    }
}

/// Polls the durable operation log until the victim's Detached source row
/// rests with a Pending disposition — the live clean-close connection fate
/// has minted the residence.
fn wait_for_pending_detached_residence(
    store: &Arc<dyn DurableStore>,
    participant_id: u64,
    capability_generation: u64,
) -> Result<(), Box<dyn Error>> {
    let log = OperationLog::new(Arc::clone(store), CONVERSATION);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut sequence = 0_u64;
        while let Some(entry) = block_on(log.read_at(sequence))?? {
            if let DecodedStoredOperation::V3(StoredOperation::Detached { row }) = entry.operation {
                if row.participant_id == participant_id
                    && row.disposition == StoredTerminalDisposition::Pending
                    && row.cause == StoredDetachedCause::CleanDeregister
                    && row.binding_epoch.to_epoch()?.capability_generation.get()
                        == capability_generation
                {
                    return Ok(());
                }
            }
            sequence = sequence
                .checked_add(1)
                .ok_or("durable log sequence overflowed")?;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "victim's pending Detached residence (generation {capability_generation}) \
                 never became durable"
            )
            .into());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Counts the victim's durable Drained-sourced committed Detached drain rows.
fn drained_row_count(
    store: &Arc<dyn DurableStore>,
    participant_id: u64,
) -> Result<usize, Box<dyn Error>> {
    let log = OperationLog::new(Arc::clone(store), CONVERSATION);
    let mut count = 0;
    let mut sequence = 0_u64;
    while let Some(entry) = block_on(log.read_at(sequence))?? {
        if let DecodedStoredOperation::V3(StoredOperation::Detached { row }) = entry.operation {
            if row.participant_id == participant_id
                && matches!(row.source, StoredDetachedSource::Drained { .. })
                && matches!(row.disposition, StoredTerminalDisposition::Committed { .. })
            {
                count += 1;
            }
        }
        sequence = sequence
            .checked_add(1)
            .ok_or("durable log sequence overflowed")?;
    }
    Ok(count)
}

fn publish(peer: u64, token: u8, payload: Vec<u8>) -> ClientRequest {
    ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: CONVERSATION,
        participant_id: peer,
        capability_generation: Generation::ONE,
        record_admission_attempt_token: RecordAdmissionAttemptToken::new([token; 16]),
        payload,
    })
}

/// Reads every obligation the resumed session replays, in order, until the
/// push stream is momentarily empty (bounded socket-deadline reads). Returns
/// the delivered ordinary-record sequences.
fn drain_replay(resumed: &mut SocketPeer) -> Vec<u64> {
    let mut sequences = Vec::new();
    while let Ok(ServerPush::ParticipantDelivery(delivery)) = resumed.read_push() {
        if let ParticipantRecord::OrdinaryRecord { .. } = delivery.record {
            sequences.push(delivery.delivery_seq);
        }
    }
    sequences
}

/// One live-minted residence: the victim's identity facts and the peer's, as
/// the drain publish will meet them.
struct MintedResidence {
    victim_id: u64,
    victim_generation: Generation,
    victim_secret: liminal_protocol::wire::AttachSecret,
    peer_id: u64,
}

/// Builds the live residence over real sockets: the victim enrolls, attaches,
/// and publishes the debt record that rests retention at its cap; the peer
/// enrolls on the fixture connection; both pre-crash acks land (the victim
/// through its contiguous window, the peer through its obligation endpoint
/// r0) so the later drain publish's admission can prune the fully-acked
/// prefix and the resume attach's record row stays admissible without any
/// capacity waiver; then the victim's clean `Disconnect` bow-out pends its
/// Detached terminal — the live pending-Detached residence, durable with no
/// finalizer.
fn mint_live_residence(server: &mut SocketFixture) -> Result<MintedResidence, Box<dyn Error>> {
    let mut victim_socket = server.spawn_peer()?;
    let victim = enroll_bound(
        victim_socket.request(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x81; 16]),
        })),
        "victim enrollment",
    )?;
    let attached =
        victim_socket.request(ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: CONVERSATION,
            participant_id: victim.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: victim.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([0x82; 16]),
            accept_marker_delivery_seq: None,
        }))?;
    let ServerValue::AttachBound(attached) = attached else {
        return Err(format!("victim attach did not bind: {attached:?}").into());
    };
    let victim_generation = attached.origin_binding_epoch().capability_generation;
    let peer = enroll_bound(
        server.request(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x85; 16]),
        })),
        "peer enrollment",
    )?;
    let r0_seq = committed_seq(
        victim_socket.request(ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: CONVERSATION,
            participant_id: victim.participant_id(),
            capability_generation: victim_generation,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([0x83; 16]),
            payload: vec![0x84],
        })),
        "victim debt record r0",
    )?;
    let victim_ack = victim_socket.request(ClientRequest::ParticipantAck(ParticipantAck {
        conversation_id: CONVERSATION,
        participant_id: victim.participant_id(),
        capability_generation: victim_generation,
        through_seq: r0_seq
            .checked_sub(1)
            .ok_or("victim debt record committed at the sequence origin")?,
    }))?;
    if !matches!(victim_ack, ServerValue::AckCommitted(_)) {
        return Err(format!("victim pre-crash ack did not commit: {victim_ack:?}").into());
    }
    let peer_ack = server.request(ClientRequest::ParticipantAck(ParticipantAck {
        conversation_id: CONVERSATION,
        participant_id: peer.participant_id(),
        capability_generation: Generation::ONE,
        through_seq: r0_seq,
    }))?;
    if !matches!(peer_ack, ServerValue::AckCommitted(_)) {
        return Err(format!("peer pre-crash ack did not commit: {peer_ack:?}").into());
    }
    victim_socket.disconnect()?;
    victim_socket.shutdown_transport()?;
    drop(victim_socket);
    wait_for_pending_detached_residence(
        &server.durable_store(),
        victim.participant_id(),
        victim_generation.get(),
    )?;
    Ok(MintedResidence {
        victim_id: victim.participant_id(),
        victim_generation,
        victim_secret: attached.attach_secret(),
        peer_id: peer.participant_id(),
    })
}

/// The drained bytes replay through the production restore path with the
/// faithful finalization intact: victim slot PRESENT at committed Detached,
/// enrollment token still mapped, no residual candidate.
fn assert_cold_replay_faithful(
    store: Arc<dyn DurableStore>,
    victim_id: u64,
) -> Result<(), Box<dyn Error>> {
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), small_retention_config())?;
    let replay_log = OperationLog::new(store, CONVERSATION);
    let replayed = handler.replay_aggregate_reference(CONVERSATION, &replay_log)?;
    let victim_slot = replayed
        .slots
        .get(&victim_id)
        .ok_or("cold replay erased the drained victim's slot")?;
    if !matches!(victim_slot.binding, BindingState::Detached) {
        return Err(format!(
            "cold replay did not settle the victim at committed Detached: {:?}",
            victim_slot.binding
        )
        .into());
    }
    if !replayed.tokens.values().any(|mapped| *mapped == victim_id) {
        return Err("cold replay unmapped the drained victim's enrollment token".into());
    }
    let residual = replayed
        .frontier()
        .ok_or("cold replay lost its frontier")?
        .frontiers()
        .sequence()
        .immutable_candidates()
        .len();
    if residual != 0 {
        return Err(format!("cold replay kept {residual} residual candidates").into());
    }
    Ok(())
}

#[test]
fn live_socket_publish_drains_pending_detached_then_victim_resumes_and_restart_serves()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let mut server = SocketFixture::start_with_config(&data_dir, small_retention_config())?;
    let minted = mint_live_residence(&mut server)?;
    let victim_id = minted.victim_id;
    let victim_generation = minted.victim_generation;
    let victim_secret = minted.victim_secret;
    let peer_id = minted.peer_id;

    // LIVE-SOCKET publish from the still-bound peer: the candidate-lane
    // Detached drain runs and the client observes RecordCommitted on the
    // wire — no refusal, no connection close.
    let drain_publish_seq = committed_seq(
        server.request(publish(peer_id, 0x87, vec![0x91, 0x92])),
        "residence publish",
    )?;
    if drained_row_count(&server.durable_store(), victim_id)? != 1 {
        return Err("drain transaction left no durable Drained Detached row".into());
    }
    assert_cold_replay_faithful(server.durable_store(), victim_id)?;

    // LIVE RESUME (the decisive pin-5 assertion): a fresh socket reattaches
    // the drained victim with its exact secret, and the replay redelivers the
    // PARKED while-drained publication.
    let mut resumed = server.spawn_peer()?;
    let resumed_bound =
        resumed.request(ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: CONVERSATION,
            participant_id: victim_id,
            capability_generation: victim_generation,
            attach_secret: victim_secret,
            attach_attempt_token: AttachAttemptToken::new([0x86; 16]),
            accept_marker_delivery_seq: None,
        }))?;
    let ServerValue::AttachBound(resumed_receipt) = resumed_bound else {
        return Err(format!(
            "drained victim's exact-secret attach did not bind on the wire: {resumed_bound:?}"
        )
        .into());
    };
    let replayed_seqs = drain_replay(&mut resumed);
    if !replayed_seqs.contains(&drain_publish_seq) {
        return Err(format!(
            "parked publication {drain_publish_seq} did not replay to the resumed victim: \
             {replayed_seqs:?}"
        )
        .into());
    }

    // The resumed victim acknowledges the replayed publication, and a repeat
    // publish commits on the wire — the server keeps serving and retention
    // keeps releasing through the real floor.
    let resumed_ack = resumed.request(ClientRequest::ParticipantAck(ParticipantAck {
        conversation_id: CONVERSATION,
        participant_id: victim_id,
        capability_generation: resumed_receipt.origin_binding_epoch().capability_generation,
        through_seq: drain_publish_seq,
    }))?;
    if !matches!(resumed_ack, ServerValue::AckCommitted(_)) {
        return Err(format!("resumed victim's ack did not commit: {resumed_ack:?}").into());
    }
    let repeated = server.request(publish(peer_id, 0x88, vec![0x93]))?;
    if !matches!(repeated, ServerValue::RecordCommitted(_)) {
        return Err(format!("repeat publish after drain did not commit: {repeated:?}").into());
    }

    // The resumed victim bows out cleanly again. Retention has refilled to
    // its cap, so this second clean-close fate PENDS a fresh Detached
    // residence at the rotated epoch — and one more live publish drains it
    // too: the Detached drain is repeatable for the same participant across
    // epochs. The victim then rests at committed Detached with only the
    // peer Bound — the one-Bound shape a real unclean restart's startup
    // repair admits (two still-Bound participants at an at-cap restart is
    // the same structural sole-candidate boundary the Died e2e documented).
    resumed.disconnect()?;
    resumed.shutdown_transport()?;
    drop(resumed);
    wait_for_pending_detached_residence(
        &server.durable_store(),
        victim_id,
        resumed_receipt
            .origin_binding_epoch()
            .capability_generation
            .get(),
    )?;
    let second_drain = server.request(publish(peer_id, 0x8A, vec![0x94]))?;
    if !matches!(second_drain, ServerValue::RecordCommitted(_)) {
        return Err(format!("second residence publish did not commit: {second_drain:?}").into());
    }
    if drained_row_count(&server.durable_store(), victim_id)? != 2 {
        return Err("second drain left no second durable Drained Detached row".into());
    }

    // REAL unclean restart: the server is abandoned without any graceful
    // shutdown and reopened on the same durable bytes — bytes that contain
    // BOTH Drained Detached rows. The reopened server serves fresh
    // connections.
    drop(server);
    let reopened = SocketFixture::start_with_config(&data_dir, small_retention_config())?;
    let mut fresh = reopened.spawn_peer()?;
    enroll_bound(
        fresh.request(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: 608,
            enrollment_token: EnrollmentToken::new([0x89; 16]),
        })),
        "post-restart enrollment",
    )?;
    drop(fresh);
    reopened.stop();
    Ok(())
}
