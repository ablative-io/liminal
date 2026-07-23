//! PIN B1 (e2e socket) — a publish accepted while a participant is FIN-dead-but-
//! resumable must reach the resumed session's replay.
//!
//! WHY THIS PIN EXISTS. liminal's suite never raced a peer publish against a
//! subscriber's FIN-fold at socket speed, so a latent concurrent defect — present
//! since before 0.3.2 — had no in-repo detector. A downstream storeless consumer
//! that dropped a subscriber's connection and kept publishing was the first to
//! observe the while-dead publish silently never reaching the resumed session:
//! the publish is accepted (a committed record — a `PublishAck`) yet mints no
//! obligation for the dead-but-resumable subscriber, because `produced()`'s
//! recipient snapshot admits only live-`Bound` slots. This pin makes liminal its
//! own first detector.
//!
//! OBSERVED STATE (recorded for the fold — see the build report): driving the
//! subscriber's fate fold to completion before e3, the subscriber settles at
//! `BindingState::Detached` (a committed connection-lost Died terminal), NOT
//! `PendingFinalization::Died`. It is nonetheless resumable — a fresh
//! `CredentialAttach` reattaches it and replays its retained unacked obligation
//! (e2). e3 admitted while the slot is `Detached` is excluded by `produced()`, so
//! the resumed replay yields only (e2); e3 is lost. This is the ruled contract
//! breach (accepted-then-lost), reproduced deterministically.

use std::error::Error;

use liminal_protocol::wire::{
    AttachAttemptToken, ClientRequest, CredentialAttachRequest, EnrollBound, EnrollmentRequest,
    EnrollmentToken, Generation, LeaveAttemptToken, LeaveRequest, ParticipantAck, ParticipantId,
    ParticipantRecord, RecordAdmission, RecordAdmissionAttemptToken, ServerPush, ServerValue,
};

use super::e2e_tests::{SocketFixture, SocketPeer};
use super::tests_outbox_barrier_fixture::OutboxBarrierKind;

const CONV: u64 = 0xB1_2E;

fn expect_record(value: ServerValue, label: &str) -> Result<u64, Box<dyn Error>> {
    match value {
        ServerValue::RecordCommitted(record) => Ok(record.delivery_seq()),
        other => Err(format!("{label} did not commit: {other:?}").into()),
    }
}

/// Uniform request entry over the primary fixture connection and its spawned
/// peers — both expose the identical inherent `request`, but as distinct types.
trait RequestPeer {
    fn issue(&mut self, request: ClientRequest) -> Result<ServerValue, Box<dyn Error>>;
}

impl RequestPeer for SocketFixture {
    fn issue(&mut self, request: ClientRequest) -> Result<ServerValue, Box<dyn Error>> {
        self.request(request)
    }
}

impl RequestPeer for SocketPeer {
    fn issue(&mut self, request: ClientRequest) -> Result<ServerValue, Box<dyn Error>> {
        self.request(request)
    }
}

/// Enrolls `peer` under `token` and returns its bound receipt.
fn enroll<P: RequestPeer>(
    peer: &mut P,
    token: [u8; 16],
    label: &str,
) -> Result<EnrollBound, Box<dyn Error>> {
    let ServerValue::EnrollBound(bound) =
        peer.issue(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONV,
            enrollment_token: EnrollmentToken::new(token),
        }))?
    else {
        return Err(format!("{label} enroll did not bind").into());
    };
    Ok(bound)
}

/// Admits one record from `peer` and returns its committed delivery sequence.
fn admit_record<P: RequestPeer>(
    peer: &mut P,
    participant_id: ParticipantId,
    capability_generation: Generation,
    attempt_token: [u8; 16],
    payload: u8,
    label: &str,
) -> Result<u64, Box<dyn Error>> {
    expect_record(
        peer.issue(ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: CONV,
            participant_id,
            capability_generation,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new(attempt_token),
            payload: vec![payload],
        }))?,
        label,
    )
}

#[test]
fn while_dead_publish_reaches_resumed_replay() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let mut publisher = SocketFixture::start_with_barriers(&data_dir)?;
    let mut subscriber = publisher.spawn_peer()?;

    // Publisher and subscriber both enroll (enrollment binds).
    let pub_bound = enroll(&mut publisher, [0x01; 16], "publisher")?;
    let sub_bound = enroll(&mut subscriber, [0x02; 16], "subscriber")?;
    let sub_pid = sub_bound.participant_id();

    // Subscriber attaches (presenting binding) and admits one record — the debt a
    // downstream subscriber participant carries in practice.
    let ServerValue::AttachBound(sub_attached) =
        subscriber.request(ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: CONV,
            participant_id: sub_pid,
            capability_generation: Generation::ONE,
            attach_secret: sub_bound.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([0x03; 16]),
            accept_marker_delivery_seq: None,
        }))?
    else {
        return Err("subscriber attach did not bind".into());
    };
    let sub_gen = sub_attached.capability_generation();
    let sub_secret = sub_attached.attach_secret();
    let _r0 = admit_record(
        &mut subscriber,
        sub_pid,
        sub_gen,
        [0x04; 16],
        0x00,
        "subscriber record r0",
    )?;

    // Publisher admits e1; the subscriber receives it and acknowledges it.
    let e1_seq = admit_record(
        &mut publisher,
        pub_bound.participant_id(),
        Generation::ONE,
        [0x05; 16],
        0x01,
        "e1",
    )?;
    let _ = subscriber.read_push()?;
    let acked = subscriber.request(ClientRequest::ParticipantAck(ParticipantAck {
        conversation_id: CONV,
        participant_id: sub_pid,
        capability_generation: sub_gen,
        through_seq: e1_seq,
    }))?;
    if !matches!(acked, ServerValue::AckCommitted(_)) {
        return Err(format!("subscriber ack of e1 did not commit: {acked:?}").into());
    }

    // Publisher admits e2; the subscriber receives it but does NOT acknowledge.
    let e2_seq = admit_record(
        &mut publisher,
        pub_bound.participant_id(),
        Generation::ONE,
        [0x06; 16],
        0x02,
        "e2",
    )?;
    let _ = subscriber.read_push()?;

    // FIN-drop the subscriber and DETERMINISTICALLY drive its fate fold to
    // completion (arm the operation-flush barrier, drop, wait for the fold to
    // reach it, release) so the subscriber is FIN-dead-but-resumable before e3 is
    // admitted.
    publisher.arm_outbox_barriers([OutboxBarrierKind::OperationFlush])?;
    subscriber.shutdown_transport()?;
    publisher.wait_for_outbox_barrier(OutboxBarrierKind::OperationFlush)?;
    publisher.release_outbox_barrier(OutboxBarrierKind::OperationFlush)?;

    // The peer publishes e3 WHILE the subscriber is FIN-dead-but-resumable. The
    // publish is accepted (committed — a PublishAck), so the ruled contract
    // requires it to reach the resumed session's replay.
    let e3_seq = admit_record(
        &mut publisher,
        pub_bound.participant_id(),
        Generation::ONE,
        [0x07; 16],
        0x03,
        "while-dead publish e3",
    )?;

    // Resume with grant: a fresh socket reattaches the subscriber; the replay
    // redelivers its unacked obligations as pushes, in delivery order.
    let mut resumed = publisher.spawn_peer()?;
    let ServerValue::AttachBound(_) =
        resumed.request(ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: CONV,
            participant_id: sub_pid,
            capability_generation: sub_gen,
            attach_secret: sub_secret,
            attach_attempt_token: AttachAttemptToken::new([0x08; 16]),
            accept_marker_delivery_seq: None,
        }))?
    else {
        return Err("subscriber resume did not reattach".into());
    };
    let replayed = drain_replay(&mut resumed);
    publisher.stop();

    // The ruled contract: replay must yield exactly (e2, e3) in order. The
    // while-dead publish e3 minted no obligation for the dead-but-resumable
    // subscriber, so the resumed replay yields only e2.
    assert_eq!(
        replayed,
        vec![e2_seq, e3_seq],
        "resumed replay must redeliver exactly (e2={e2_seq}, e3={e3_seq}) in order; \
         a missing e3 is the while-dead publish lost from the resumed session"
    );
    Ok(())
}

/// EXCLUSION COMPANION — a permanently-departed (cleanly Left) participant must
/// NOT be named by a subsequent publish's recipient snapshot.
///
/// WHY THIS PIN IS LOAD-BEARING (not hygiene). The while-dead fix widens
/// `produced()`'s recipient predicate from `Bound(_)` to `Bound(_) | Detached`
/// so a connection-lost-but-resumable subscriber keeps receiving. The discriminator
/// that keeps this SAFE is map presence: clean Leave is the SOLE path that removes
/// a slot from `authority.slots` (`ops_leave.rs` — no reinsert; the identity becomes a
/// retired tombstone), so a departed participant is absent from the iteration and can
/// never be named, while every resumable terminal (connection-lost Died,
/// detach-by-request, deregistration) RETAINS the slot at `Detached`. Naming a
/// departed participant would mint obligation debt that can never discharge (it has
/// no session to resume and ack, and only `ProducedSourceKind::Left` permanently
/// retires outbox obligations) — a standing W2-liveness defect strictly worse than
/// the delivery loss the widening repairs. This pin proves the widening does not
/// over-mint: it is expected GREEN pre-fix (map absence already excludes the
/// departed) and MUST stay green post-fix — an anti-over-minting regression guard.
#[test]
fn clean_leave_departed_mints_no_obligation() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let mut publisher = SocketFixture::start_with_barriers(&data_dir)?;
    let mut departing = publisher.spawn_peer()?;
    let mut observer = publisher.spawn_peer()?;

    // Publisher, the departing participant, and a still-bound observer all enroll.
    let pub_bound = enroll(&mut publisher, [0x11; 16], "publisher")?;
    let dep_bound = enroll(&mut departing, [0x12; 16], "departing")?;
    let obs_bound = enroll(&mut observer, [0x13; 16], "observer")?;
    let dep_pid = dep_bound.participant_id();
    let obs_pid = obs_bound.participant_id();

    // The departing participant attaches, then admits e1 so it carries a live
    // obligation before it leaves (the debt a departure must discharge).
    let ServerValue::AttachBound(dep_attached) =
        departing.request(ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: CONV,
            participant_id: dep_pid,
            capability_generation: Generation::ONE,
            attach_secret: dep_bound.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([0x14; 16]),
            accept_marker_delivery_seq: None,
        }))?
    else {
        return Err("departing attach did not bind".into());
    };

    // Publisher admits e1; both the departing participant and the observer are
    // Bound recipients, so each accrues a live obligation.
    let _e1 = admit_record(
        &mut publisher,
        pub_bound.participant_id(),
        Generation::ONE,
        [0x15; 16],
        0xE1,
        "e1",
    )?;
    assert!(
        publisher
            .outbox_owner_facts(CONV, dep_pid)?
            .next_live_obligation
            .is_some(),
        "departing participant should carry e1's live obligation before leaving"
    );

    // The departing participant cleanly LEAVES — the sole path that removes the slot
    // from authority.slots and permanently discharges its obligations.
    let left = departing.request(ClientRequest::Leave(LeaveRequest {
        conversation_id: CONV,
        participant_id: dep_pid,
        capability_generation: dep_attached.capability_generation(),
        attach_secret: dep_attached.attach_secret(),
        leave_attempt_token: LeaveAttemptToken::new([0x16; 16]),
    }))?;
    if !matches!(left, ServerValue::LeaveCommitted(_)) {
        return Err(format!("clean Leave did not commit: {left:?}").into());
    }
    assert_eq!(
        publisher
            .outbox_owner_facts(CONV, dep_pid)?
            .next_live_obligation,
        None,
        "clean Leave must discharge the departed participant's obligations"
    );

    // Publisher admits e2 AFTER the departure. It must mint an obligation for the
    // still-Bound observer but NONE for the departed participant.
    let _e2 = admit_record(
        &mut publisher,
        pub_bound.participant_id(),
        Generation::ONE,
        [0x17; 16],
        0xE2,
        "e2",
    )?;
    assert!(
        publisher
            .outbox_owner_facts(CONV, obs_pid)?
            .next_live_obligation
            .is_some(),
        "e2 must mint an obligation for the still-Bound observer (publish is not a no-op)"
    );
    assert_eq!(
        publisher
            .outbox_owner_facts(CONV, dep_pid)?
            .next_live_obligation,
        None,
        "e2 must mint NO obligation for the permanently-departed participant; a Some \
         here is undischargeable obligation debt — a W2 standing-liveness defect"
    );

    publisher.stop();
    Ok(())
}

/// Reads every obligation the resumed session replays, in order, until the push
/// stream is momentarily empty (a bounded socket-deadline read — no wall-clock
/// sampling). Returns the delivered sequences.
fn drain_replay(resumed: &mut SocketPeer) -> Vec<u64> {
    let mut sequences = Vec::new();
    while let Ok(ServerPush::ParticipantDelivery(delivery)) = resumed.read_push() {
        if let ParticipantRecord::OrdinaryRecord { .. } = delivery.record {
            sequences.push(delivery.delivery_seq);
        }
    }
    sequences
}
