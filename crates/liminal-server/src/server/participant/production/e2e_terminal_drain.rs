//! Live-socket candidate-lane terminal drain and real unclean-restart replay.
//!
//! The S-4 socket pin, built in the buildable ordering: a real-socket client
//! publish drains the live pending Died residence and observes
//! `RecordCommitted` on the wire with its connection intact and the server
//! still serving; the server is then killed WITHOUT any graceful shutdown and
//! reopened on the same durable bytes, replaying the drain row through the
//! production restore path. The ruled restore-then-publish ordering is
//! structurally unbuildable at this base — the pending residence requires
//! retention at its cap, a real restart must startup-fate every still-Bound
//! participant (refused while the candidate occupies the sole-candidate
//! lane), and at-cap attach/enrollment is hard-refused, so no publisher can
//! exist after such a restart — see the lane report.

use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

use liminal::durability::DurableStore;
use liminal::durability::bridge::block_on;
use liminal_protocol::wire::{
    AttachAttemptToken, ClientRequest, CredentialAttachRequest, EnrollBound, EnrollmentRequest,
    EnrollmentToken, Generation, RecordAdmission, RecordAdmissionAttemptToken, ServerValue,
};

use super::ProductionParticipantHandler;
use super::e2e_tests::SocketFixture;
use super::log::{
    DecodedStoredOperation, OperationLog, StoredOperation, StoredTerminalDisposition,
};
use super::tests::test_participant_config;

const CONVERSATION: u64 = 601;

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

/// Polls the durable operation log until the victim's Died source row rests
/// with a Pending disposition — the live connection fate has minted the
/// residence.
fn wait_for_pending_died_residence(
    store: &Arc<dyn DurableStore>,
    participant_id: u64,
) -> Result<(), Box<dyn Error>> {
    let log = OperationLog::new(Arc::clone(store), CONVERSATION);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut sequence = 0_u64;
        while let Some(entry) = block_on(log.read_at(sequence))?? {
            if let DecodedStoredOperation::V3(StoredOperation::Died { row }) = entry.operation {
                if row.participant_id == participant_id
                    && row.disposition == StoredTerminalDisposition::Pending
                    && row.drained.is_none()
                {
                    return Ok(());
                }
            }
            sequence = sequence
                .checked_add(1)
                .ok_or("durable log sequence overflowed")?;
        }
        if Instant::now() >= deadline {
            return Err("victim's pending Died residence never became durable".into());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Reads the drained terminal row for the victim, if one is durable.
fn drained_row_present(
    store: &Arc<dyn DurableStore>,
    participant_id: u64,
) -> Result<bool, Box<dyn Error>> {
    let log = OperationLog::new(Arc::clone(store), CONVERSATION);
    let mut sequence = 0_u64;
    while let Some(entry) = block_on(log.read_at(sequence))?? {
        if let DecodedStoredOperation::V3(StoredOperation::Died { row }) = entry.operation {
            if row.participant_id == participant_id && row.drained.is_some() {
                return Ok(true);
            }
        }
        sequence = sequence
            .checked_add(1)
            .ok_or("durable log sequence overflowed")?;
    }
    Ok(false)
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

/// Builds the live residence over real sockets: the victim (on its own real
/// connection) enrolls, attaches, and publishes so retention rests at its
/// cap; the peer enrolls on the fixture connection while the hard cap still
/// admits its row; the victim's transport then dies and the live connection
/// fate pends its terminal. Returns `(victim, peer)` participant ids.
fn mint_live_residence(server: &mut SocketFixture) -> Result<(u64, u64), Box<dyn Error>> {
    let mut victim_socket = server.spawn_peer()?;
    let victim = enroll_bound(
        victim_socket.request(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x61; 16]),
        })),
        "victim enrollment",
    )?;
    let attached =
        victim_socket.request(ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: CONVERSATION,
            participant_id: victim.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: victim.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([0x62; 16]),
            accept_marker_delivery_seq: None,
        }))?;
    let ServerValue::AttachBound(attached) = attached else {
        return Err(format!("victim attach did not bind: {attached:?}").into());
    };
    let peer = enroll_bound(
        server.request(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x65; 16]),
        })),
        "peer enrollment",
    )?;
    let record = victim_socket.request(ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: CONVERSATION,
        participant_id: victim.participant_id(),
        capability_generation: attached.origin_binding_epoch().capability_generation,
        record_admission_attempt_token: RecordAdmissionAttemptToken::new([0x63; 16]),
        payload: vec![0x64],
    }))?;
    if !matches!(record, ServerValue::RecordCommitted(_)) {
        return Err(format!("victim record did not commit: {record:?}").into());
    }
    victim_socket.shutdown_transport()?;
    drop(victim_socket);
    wait_for_pending_died_residence(&server.durable_store(), victim.participant_id())?;
    Ok((victim.participant_id(), peer.participant_id()))
}

#[test]
fn live_socket_publish_drains_pending_terminal_then_unclean_restart_serves()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let mut server = SocketFixture::start_with_config(&data_dir, small_retention_config())?;
    let (victim, peer) = mint_live_residence(&mut server)?;

    // LIVE-SOCKET publish from the still-bound peer: the candidate-lane
    // terminal drain runs and the client observes RecordCommitted on the
    // wire — no refusal, no connection close.
    let committed = server.request(publish(peer, 0xA7, vec![0xB1, 0xB2]))?;
    if !matches!(committed, ServerValue::RecordCommitted(_)) {
        return Err(format!("residence publish did not commit on the wire: {committed:?}").into());
    }
    if !drained_row_present(&server.durable_store(), victim)? {
        return Err("drain transaction left no durable drained terminal row".into());
    }

    // The connection stayed open and the server keeps serving: a repeat
    // publish commits on the SAME connection, and a fresh connection binds an
    // unrelated conversation.
    let repeated = server.request(publish(peer, 0xA8, vec![0xB3]))?;
    if !matches!(repeated, ServerValue::RecordCommitted(_)) {
        return Err(format!("repeat publish after drain did not commit: {repeated:?}").into());
    }
    let mut other_connection = server.spawn_peer()?;
    enroll_bound(
        other_connection.request(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: 602,
            enrollment_token: EnrollmentToken::new([0x71; 16]),
        })),
        "post-drain enrollment on another connection",
    )?;

    // REAL unclean restart: the server is abandoned without any graceful
    // shutdown — no force-close, no server-stop fates — and reopened on the
    // same durable bytes.
    drop(other_connection);
    drop(server);
    let reopened = SocketFixture::start_with_config(&data_dir, small_retention_config())?;

    // The reopened server serves fresh connections.
    let mut fresh = reopened.spawn_peer()?;
    enroll_bound(
        fresh.request(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: 603,
            enrollment_token: EnrollmentToken::new([0x72; 16]),
        })),
        "post-restart enrollment",
    )?;

    // And the drained conversation's durable truth replays with the drain
    // intact: the victim's slot is gone and no candidate survives.
    let handler =
        ProductionParticipantHandler::new(reopened.durable_store(), small_retention_config())?;
    let log = OperationLog::new(reopened.durable_store(), CONVERSATION);
    let replayed = handler.replay_aggregate_reference(CONVERSATION, &log)?;
    if replayed.slots.contains_key(&victim) {
        return Err("restart replay resurrected the drained victim's slot".into());
    }
    let victim_candidates = replayed
        .frontier()
        .ok_or("restart replay lost its frontier")?
        .frontiers()
        .sequence()
        .immutable_candidates()
        .iter()
        .filter(|candidate| {
            matches!(
                candidate,
                liminal_protocol::lifecycle::ImmutableSequenceCandidate::BindingTerminal {
                    owner,
                    ..
                } if owner.participant_index == victim
            )
        })
        .count();
    if victim_candidates != 0 {
        return Err("restart replay resurrected the drained candidate".into());
    }
    drop(fresh);
    reopened.stop();
    Ok(())
}
