//! #195 — killed-mid-attach identity orphan, demonstrated and then healed over
//! a real socket through the natural embedder flow.
//!
//! Every client here enters through `RemoteParticipantHandle`. No test builds an
//! inbound value, applies client state directly, or reaches into the aggregate:
//! the orphan and its cure are both asserted at the transport, because that is
//! the only place the field failure is visible (`docs/design/P195-ORPHAN-DIAGNOSIS.md`).
//!
//! # The kill shape
//!
//! [`orphan_a_client_mid_attach`] is the whole defect in one function: a client
//! issues a `CredentialAttach`, the server commits it — rotating the credential,
//! with the `AttachBound` response as the SOLE carrier of the rotation — and the
//! client dies before consuming that response. Its `LPCR` bytes survive holding
//! an ISSUED attach; the rotated credential does not survive anywhere on the
//! client side. Everything downstream of that function is about what a restored
//! client can and cannot then do.
//!
//! The commit is observed rather than slept for. A witness participant enrolled
//! in the same conversation receives the `Attached` push the rotation appends,
//! so "the server committed" is a machine observation on the witness's socket,
//! not a wall-clock guess about the victim's.

use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use liminal_protocol::wire::{
    AttachAttemptToken, AttachBound, AttachSecret, ClientRequest, CredentialAttachRequest,
    EnrollBound, EnrollmentRequest, EnrollmentToken, Generation, ParticipantDelivery,
    ParticipantRecord, ReceiptExpiryReason, ReceiptReplay, ServerPush, ServerValue,
};
use liminal_sdk::{
    ConnectionPoolConfig, CredentialAttachReissueReason, LostCredentialAttachRefusalReason,
    ParticipantResumeStore, RemoteConfig, RemoteCredentialAttachRecovery,
    RemoteOperationRecordOutcome, RemoteParticipantHandle, RemoteParticipantInbound,
    RemoteParticipantSendOutcome, SdkError,
};

use super::super::tests::test_participant_config;
use super::SdkSocketFixture;

const CONVERSATION: u64 = 0x19_50;

/// Pinned wall-clock base for the deadline pins. Any fixed reading works; this
/// one is far enough from zero that no window arithmetic underflows.
const BASE_MS: u64 = 1_770_000_000_000;
/// Receipt window used by the deadline pins.
const RECEIPT_TTL_MS: u64 = 60_000;
/// Provenance window used by the deadline pins. Must be at least the receipt
/// TTL: provenance explains the receipt, so it cannot expire first.
const PROVENANCE_TTL_MS: u64 = 600_000;

/// Resume store whose committed bytes are readable from OUTSIDE the handle that
/// wrote them, and which can be made to STOP recording at a chosen write.
///
/// A process kill destroys the handle and keeps the durable record, so the
/// bytes must outlive the writer. The second half is what models WHEN the
/// process died: after [`Self::die_after_next_write`], one further checkpoint is
/// accepted and every later one is discarded — which is not the store lying but
/// the process being gone. Without it, a test that wants the checkpoint written
/// midway through `send_operation` can only reach it by racing another thread or
/// by indexing into a version list, and both of those assert a position rather
/// than a moment.
#[derive(Clone, Debug)]
struct SharedResumeStore {
    bytes: Arc<Mutex<Vec<u8>>>,
    writes: Arc<AtomicUsize>,
    accept_through: Arc<AtomicUsize>,
}

impl Default for SharedResumeStore {
    fn default() -> Self {
        Self {
            bytes: Arc::new(Mutex::new(Vec::new())),
            writes: Arc::new(AtomicUsize::new(0)),
            accept_through: Arc::new(AtomicUsize::new(usize::MAX)),
        }
    }
}

impl SharedResumeStore {
    fn snapshot(&self) -> Result<Vec<u8>, Box<dyn Error>> {
        self.bytes
            .lock()
            .map(|bytes| bytes.clone())
            .map_err(|_| "shared resume store lock poisoned".into())
    }

    /// Accepts exactly one more checkpoint, then behaves as a store whose
    /// process no longer exists.
    fn die_after_next_write(&self) {
        let seen = self.writes.load(Ordering::SeqCst);
        self.accept_through.store(seen + 1, Ordering::SeqCst);
    }
}

impl ParticipantResumeStore for SharedResumeStore {
    fn persist(&mut self, canonical_lpcr: &[u8]) -> Result<(), SdkError> {
        let seen = self.writes.fetch_add(1, Ordering::SeqCst);
        if seen >= self.accept_through.load(Ordering::SeqCst) {
            return Ok(());
        }
        let mut bytes = self.bytes.lock().map_err(|_| SdkError::Store {
            description: "shared resume store lock poisoned".to_string(),
        })?;
        bytes.clear();
        bytes.extend_from_slice(canonical_lpcr);
        drop(bytes);
        Ok(())
    }
}

type SdkParticipant = RemoteParticipantHandle<SharedResumeStore>;

fn client_config(address: SocketAddr) -> Result<RemoteConfig, Box<dyn Error>> {
    Ok(RemoteConfig::new(
        address.to_string(),
        "p195-orphan-recovery",
        CONVERSATION.to_string(),
        ConnectionPoolConfig::new(1, 1, 8),
    )?
    .connect_tcp()?)
}

fn connect(address: SocketAddr) -> Result<(SdkParticipant, SharedResumeStore), Box<dyn Error>> {
    let store = SharedResumeStore::default();
    let config = client_config(address)?;
    let handle = RemoteParticipantHandle::new(&config, store.clone())?;
    Ok((handle, store))
}

fn expect_applied(inbound: RemoteParticipantInbound) -> Result<ServerValue, Box<dyn Error>> {
    match inbound {
        RemoteParticipantInbound::Applied { value, .. } => Ok(value),
        other => Err(format!("expected an SDK-applied server value, got {other:?}").into()),
    }
}

/// Records, sends, and receives one request, refusing to paper over any typed
/// refusal on the way.
fn exchange(
    participant: &SdkParticipant,
    request: ClientRequest,
) -> Result<ServerValue, Box<dyn Error>> {
    issue(participant, request)?;
    expect_applied(participant.receive()?)
}

/// Records and sends one request WITHOUT consuming its answer.
///
/// This is the half of `exchange` the kill shape needs: after it returns, the
/// operation is durably committed as ISSUED and the response is in flight with
/// nobody waiting for it.
fn issue(participant: &SdkParticipant, request: ClientRequest) -> Result<(), Box<dyn Error>> {
    let operation = match participant.record_operation(request)? {
        RemoteOperationRecordOutcome::Recorded(operation)
        | RemoteOperationRecordOutcome::Continuous(operation) => operation,
        RemoteOperationRecordOutcome::Refused { request, reason } => {
            return Err(format!("SDK refused outbound {request:?}: {reason:?}").into());
        }
    };
    match participant.send_operation(operation)? {
        RemoteParticipantSendOutcome::Sent { .. } => Ok(()),
        RemoteParticipantSendOutcome::TransportLost { error, .. } => {
            Err(format!("SDK transport lost while sending: {error}").into())
        }
    }
}

fn enroll(
    participant: &SdkParticipant,
    token: [u8; 16],
) -> Result<EnrollBound, Box<dyn Error>> {
    let value = exchange(
        participant,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new(token),
        }),
    )?;
    match value {
        ServerValue::EnrollBound(bound) => Ok(bound),
        other => Err(format!("enrollment did not bind: {other:?}").into()),
    }
}

fn attach_request(
    participant_id: u64,
    generation: Generation,
    attach_secret: AttachSecret,
    token: [u8; 16],
) -> ClientRequest {
    ClientRequest::CredentialAttach(CredentialAttachRequest {
        conversation_id: CONVERSATION,
        participant_id,
        capability_generation: generation,
        attach_secret,
        attach_attempt_token: AttachAttemptToken::new(token),
        accept_marker_delivery_seq: None,
    })
}

/// Drains the witness's socket until it observes the attach commit for
/// `participant_id`, and returns the committed binding epoch.
///
/// This is the deterministic "the server committed" signal. The rotation
/// appends an `Attached` record to the conversation, so the witness's push is
/// evidence produced BY the commit rather than an inference from elapsed time.
fn await_attach_commit(
    witness: &SdkParticipant,
    participant_id: u64,
) -> Result<(), Box<dyn Error>> {
    for _ in 0..64 {
        match witness.receive()? {
            RemoteParticipantInbound::Push {
                value:
                    ServerPush::ParticipantDelivery(ParticipantDelivery {
                        record:
                            ParticipantRecord::Attached {
                                affected_participant_id,
                                ..
                            },
                        ..
                    }),
                ..
            } if affected_participant_id == participant_id => return Ok(()),
            RemoteParticipantInbound::Push { .. } => {}
            other => {
                return Err(format!("witness expected only pushes, got {other:?}").into());
            }
        }
    }
    Err("witness never observed the attach commit".into())
}

/// The victim's state after the kill: the exact bytes a restore must work from,
/// plus the identity facts an uninformed embedder would still believe.
struct KilledMidAttach {
    checkpoint: Vec<u8>,
    participant_id: u64,
    /// The generation the client still believes it holds. The commit has already
    /// moved past it; nothing told the client.
    retained_generation: Generation,
    /// The credential the client still holds. The commit invalidated it.
    retained_secret: AttachSecret,
}

/// Enrolls a victim, attaches it once so a rotation has demonstrably happened,
/// then issues a SECOND attach and kills the client before it consumes the
/// answer — the exact field shape of #195.
fn orphan_a_client_mid_attach(
    address: SocketAddr,
    witness: &SdkParticipant,
    enrollment_token: [u8; 16],
) -> Result<KilledMidAttach, Box<dyn Error>> {
    let (victim, store) = connect(address)?;
    let bound = enroll(&victim, enrollment_token)?;
    let participant_id = bound.participant_id();
    await_attach_commit(witness, participant_id)?;

    // First attach: an ordinary, fully consumed rotation. It establishes that
    // the client's credential really does move on every attach commit, so the
    // orphan below cannot be read as an artefact of a never-rotated identity.
    let first = exchange(
        &victim,
        attach_request(
            participant_id,
            bound.capability_generation(),
            bound.attach_secret(),
            [0xA1; 16],
        ),
    )?;
    let ServerValue::AttachBound(first_bound) = first else {
        return Err(format!("first attach did not bind: {first:?}").into());
    };
    await_attach_commit(witness, participant_id)?;
    assert_ne!(
        first_bound.attach_secret(),
        bound.attach_secret(),
        "the attach commit must rotate the credential"
    );

    // Second attach: issued, committed by the server, and never consumed by the
    // client. The response carrying the NEXT rotation is written to a socket the
    // victim is about to stop existing behind.
    let retained_generation = first_bound.capability_generation();
    let retained_secret = first_bound.attach_secret();
    issue(
        &victim,
        attach_request(
            participant_id,
            retained_generation,
            retained_secret,
            [0xB2; 16],
        ),
    )?;
    await_attach_commit(witness, participant_id)?;

    // THE KILL. The durable record is snapshotted exactly as the dying process
    // left it; the handle and its transport are dropped without ever reading the
    // committed answer.
    let checkpoint = store.snapshot()?;
    drop(victim);
    drop(store);

    Ok(KilledMidAttach {
        checkpoint,
        participant_id,
        retained_generation,
        retained_secret,
    })
}

/// #195 AT BASE — a client killed mid-attach cannot re-attach by any means an
/// uninformed embedder has.
///
/// This is the field shape, asserted at the transport. The restored client
/// consumes its crash testimony exactly as the SDK's documented recovery path
/// says to, and then does the one thing left available to it: form a fresh
/// attach from the credential it retained. The server refuses it as
/// `StaleAuthority`, and correctly so — that credential and generation were
/// invalidated by the commit whose answer the client never saw.
///
/// The refusal is not the bug. The bug is that this is the END of the road: the
/// server is at that moment still holding the committed outcome, inside a
/// receipt window, ready to replay the rotated credential to anyone who
/// re-presents the ORIGINAL token — and no SDK surface does.
#[test]
fn a_client_killed_mid_attach_cannot_re_attach_from_its_retained_credential()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let server = SdkSocketFixture::start(&home.path().join("p195-orphan"))?;
    let address = server.address()?;

    let (witness, _witness_store) = connect(address)?;
    enroll(&witness, [0x01; 16])?;

    let killed = orphan_a_client_mid_attach(address, &witness, [0x02; 16])?;

    // Fresh process: restore from the bytes the kill left behind.
    let config = client_config(address)?;
    let restored = RemoteParticipantHandle::restore(
        &config,
        SharedResumeStore::default(),
        &killed.checkpoint,
    )?;

    // The documented recovery path. It hands back the exact retained envelope as
    // DATA and terminalizes the operation; nothing is re-presented, and the
    // receipt window the server is holding open goes unspent.
    let resolution = restored.resolve_lost_operation_authority()?;
    let liminal_sdk::RemoteLostOperationResolution::Recorded { request, testimony } = resolution
    else {
        return Err(format!("a killed issued attach must testify a lost authority: {resolution:?}")
            .into());
    };
    assert_eq!(
        testimony,
        liminal_protocol::client::LostAuthorityKind::IssuedOperationCorrelation
    );
    assert!(
        matches!(request, ClientRequest::CredentialAttach(_)),
        "the retained envelope must be the exact issued credential attach, got {request:?}"
    );

    // What an uninformed embedder does next: re-attach with the credential it
    // still holds and a FRESH attempt token.
    let refused = exchange(
        &restored,
        attach_request(
            killed.participant_id,
            killed.retained_generation,
            killed.retained_secret,
            [0xC3; 16],
        ),
    )?;
    let ServerValue::StaleAuthority(liminal_protocol::wire::StaleAuthority::Live {
        current_generation,
        ..
    }) = refused
    else {
        return Err(format!("the orphaned re-attach must be refused as stale: {refused:?}").into());
    };
    assert!(
        current_generation.get() > killed.retained_generation.get(),
        "the server must be strictly ahead of the credential the client retained: \
         current {current_generation:?} vs retained {:?}",
        killed.retained_generation
    );

    drop(restored);
    drop(witness);
    server.stop()?;
    Ok(())
}

/// Restores a killed client and drives the recovery in one step, which is the
/// whole embedder-facing sequence the fix adds.
fn restore_and_recover(
    address: SocketAddr,
    checkpoint: &[u8],
) -> Result<(SdkParticipant, SharedResumeStore, RemoteCredentialAttachRecovery), Box<dyn Error>> {
    let store = SharedResumeStore::default();
    let config = client_config(address)?;
    let restored = RemoteParticipantHandle::restore(&config, store.clone(), checkpoint)?;
    let recovery = restored.recover_lost_credential_attach()?;
    Ok((restored, store, recovery))
}

/// Reads the rotated credential out of whichever receipt replay the server sent.
///
/// `Bound` and `UnboundReceipt` are the same healing with different liveness
/// claims about the receipt's ORIGIN binding, and which one arrives depends on
/// whether the server has already reaped the victim's dead connection. Both
/// carry the successor generation and the newly minted secret, so a pin that
/// demanded one of them would be pinning a race rather than the cure.
fn healed_credential(value: &ServerValue) -> Result<&AttachBound, Box<dyn Error>> {
    match value {
        ServerValue::Bound(ReceiptReplay::CredentialAttach(bound))
        | ServerValue::UnboundReceipt(ReceiptReplay::CredentialAttach(bound)) => Ok(bound),
        other => Err(format!("expected a credential-attach receipt replay, got {other:?}").into()),
    }
}

/// PIN (a) — the healing act, over a real socket.
///
/// The restored client re-presents its retained envelope, the server replays the
/// committed receipt, and the rotated credential the orphan could never learn is
/// now held. The proof that it is genuinely held is not an inspection of client
/// state but an operation: a fresh attach formed from the recovered generation
/// and secret is ACCEPTED by the server, which is exactly what the orphaned
/// client could not achieve by any means in the base test above.
#[test]
fn a_lost_credential_attach_heals_from_the_servers_receipt_replay() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let server = SdkSocketFixture::start(&home.path().join("p195-heal"))?;
    let address = server.address()?;

    let (witness, _witness_store) = connect(address)?;
    enroll(&witness, [0x11; 16])?;
    let killed = orphan_a_client_mid_attach(address, &witness, [0x12; 16])?;

    let (restored, _store, recovery) = restore_and_recover(address, &killed.checkpoint)?;
    let RemoteCredentialAttachRecovery::HealedFromReceipt { value, .. } = &recovery else {
        return Err(format!("the receipt window must heal the orphan, got {recovery:?}").into());
    };
    let rotated = healed_credential(value)?;

    // The rotation the client could never otherwise learn.
    assert_ne!(
        rotated.attach_secret(),
        killed.retained_secret,
        "the replayed receipt must carry the ROTATED secret, not the presented one"
    );
    assert!(
        rotated.capability_generation().get() > killed.retained_generation.get(),
        "the replayed receipt must carry the successor generation: got {:?} against retained {:?}",
        rotated.capability_generation(),
        killed.retained_generation
    );

    // And it is genuinely current: the server accepts an attach formed from it.
    let operated = exchange(
        &restored,
        attach_request(
            killed.participant_id,
            rotated.capability_generation(),
            rotated.attach_secret(),
            [0xD4; 16],
        ),
    )?;
    assert!(
        matches!(operated, ServerValue::AttachBound(_)),
        "the healed client must be able to operate on its recovered credential, got {operated:?}"
    );

    drop(restored);
    drop(witness);
    server.stop()?;
    Ok(())
}

/// PIN (e) — restart parity after healing.
///
/// A heal that could not survive the next restart would only have moved the
/// orphan one process along. The healed client is checkpointed, killed again,
/// and restored: the restore must accept the bytes, testify no lost authority
/// (there is none — the operation was answered), and still be able to operate.
#[test]
fn a_healed_client_round_trips_through_one_more_restart() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let server = SdkSocketFixture::start(&home.path().join("p195-parity"))?;
    let address = server.address()?;

    let (witness, _witness_store) = connect(address)?;
    enroll(&witness, [0x21; 16])?;
    let killed = orphan_a_client_mid_attach(address, &witness, [0x22; 16])?;

    let (healed, healed_store, recovery) = restore_and_recover(address, &killed.checkpoint)?;
    let RemoteCredentialAttachRecovery::HealedFromReceipt { value, .. } = &recovery else {
        return Err(format!("the receipt window must heal the orphan, got {recovery:?}").into());
    };
    let rotated_generation = healed_credential(value)?.capability_generation();
    let rotated_secret = healed_credential(value)?.attach_secret();

    // Second kill: the healed state, and only the healed state, goes durable.
    let checkpoint = healed_store.snapshot()?;
    drop(healed);
    drop(healed_store);

    let config = client_config(address)?;
    let reborn =
        RemoteParticipantHandle::restore(&config, SharedResumeStore::default(), &checkpoint)?;

    // Nothing is owed: the answered operation left no testimony behind, so the
    // driver has nothing to drive and says so rather than re-probing.
    let idle = reborn.recover_lost_credential_attach()?;
    assert!(
        matches!(
            idle,
            RemoteCredentialAttachRecovery::NotPending {
                reason: LostCredentialAttachRefusalReason::NoPendingTestimony
            }
        ),
        "a healed restore owes no recovery, got {idle:?}"
    );

    // And the credential really did survive the round trip.
    let operated = exchange(
        &reborn,
        attach_request(
            killed.participant_id,
            rotated_generation,
            rotated_secret,
            [0xE5; 16],
        ),
    )?;
    assert!(
        matches!(operated, ServerValue::AttachBound(_)),
        "the restarted healed client must still operate, got {operated:?}"
    );

    drop(reborn);
    drop(witness);
    server.stop()?;
    Ok(())
}

/// PIN (b) — the never-committed case commits fresh.
///
/// The kill can also land in the window BEFORE the server commits: the client
/// checkpointed its attach as issued and then died, and the request never
/// arrived. Nothing was lost, so nothing needs replaying — the re-presentation
/// is simply the first time the server ever sees this token, and it commits it.
///
/// The server is stopped before the send and restarted on the SAME data
/// directory, so "the server never committed this token" is a fact of the run
/// rather than a timing assumption, while the identity itself survives in the
/// durable store exactly as a real server restart would leave it.
#[test]
fn a_never_committed_credential_attach_commits_fresh_on_re_presentation()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("p195-fresh");
    let server = SdkSocketFixture::start(&data_dir)?;
    let address = server.address()?;

    let (witness, _witness_store) = connect(address)?;
    enroll(&witness, [0x31; 16])?;

    let (victim, store) = connect(address)?;
    let bound = enroll(&victim, [0x32; 16])?;
    let participant_id = bound.participant_id();
    await_attach_commit(&witness, participant_id)?;

    let first = exchange(
        &victim,
        attach_request(
            participant_id,
            bound.capability_generation(),
            bound.attach_secret(),
            [0xA1; 16],
        ),
    )?;
    let ServerValue::AttachBound(first_bound) = first else {
        return Err(format!("first attach did not bind: {first:?}").into());
    };
    await_attach_commit(&witness, participant_id)?;
    let retained_generation = first_bound.capability_generation();
    let retained_secret = first_bound.attach_secret();

    // The server goes away BEFORE the attach is recorded, so the token below can
    // never have been committed.
    drop(witness);
    server.stop()?;

    let unseen = attach_request(
        participant_id,
        retained_generation,
        retained_secret,
        [0xF6; 16],
    );
    let operation = match victim.record_operation(unseen)? {
        RemoteOperationRecordOutcome::Recorded(operation)
        | RemoteOperationRecordOutcome::Continuous(operation) => operation,
        RemoteOperationRecordOutcome::Refused { request, reason } => {
            return Err(format!("SDK refused the unseen attach {request:?}: {reason:?}").into());
        }
    };
    // The victim dies with the ISSUED checkpoint as its last durable act.
    // `send_operation` persists the issued state before it touches the socket,
    // so accepting exactly one more write lands the process squarely in the real
    // crash window between the durable write and the wire write.
    store.die_after_next_write();
    // Whether the write lands in a dead socket or fails outright is not this
    // pin's business; either way the server never saw it and the checkpoint above
    // is what the process left behind.
    let _ = victim.send_operation(operation)?;
    let checkpoint = store.snapshot()?;
    drop(victim);
    drop(store);

    // Same durable identity, new process.
    let server = SdkSocketFixture::start(&data_dir)?;
    let address = server.address()?;
    let (restored, _restored_store, recovery) = restore_and_recover(address, &checkpoint)?;

    let RemoteCredentialAttachRecovery::CommittedFresh { value, .. } = &recovery else {
        return Err(
            format!("a never-committed attach must commit on re-presentation, got {recovery:?}")
                .into(),
        );
    };
    let ServerValue::AttachBound(committed) = value else {
        return Err(format!("a fresh commit must answer AttachBound, got {value:?}").into());
    };
    assert!(
        committed.capability_generation().get() > retained_generation.get(),
        "the fresh commit must rotate forward: got {:?} against retained {:?}",
        committed.capability_generation(),
        retained_generation
    );

    drop(restored);
    server.stop()?;
    Ok(())
}

/// Config for the deadline pins: explicit receipt and provenance windows, driven
/// by a pinned clock rather than waited out.
fn deadline_config() -> crate::config::types::ParticipantConfig {
    let mut config = test_participant_config();
    config.attach_receipt_ttl_ms = RECEIPT_TTL_MS;
    config.receipt_provenance_ttl_ms = PROVENANCE_TTL_MS;
    config
}

/// Builds an orphan on a clock-pinned server, then steps the clock to `now_ms`
/// and drives the recovery there.
///
/// Every commit in the setup is stamped at [`BASE_MS`], so the receipt and
/// provenance windows are known exactly and the step is arithmetic rather than a
/// sleep. This matters more than convenience here: the two windows this pins are
/// adjacent, and a sleep long enough to clear one reliably is long enough to
/// overshoot into the other.
fn orphan_then_step_clock_to(
    data_dir: &Path,
    now_ms: u64,
) -> Result<RemoteCredentialAttachRecovery, Box<dyn Error>> {
    let server = SdkSocketFixture::start_with_config(data_dir, deadline_config())?;
    server.pin_clock_ms(BASE_MS);
    let address = server.address()?;

    let (witness, _witness_store) = connect(address)?;
    enroll(&witness, [0x41; 16])?;
    let killed = orphan_a_client_mid_attach(address, &witness, [0x42; 16])?;

    server.pin_clock_ms(now_ms);
    let (restored, _store, recovery) = restore_and_recover(address, &killed.checkpoint)?;

    drop(restored);
    drop(witness);
    server.stop()?;
    Ok(recovery)
}

/// PIN (c) — past the receipt window, the driver surfaces the typed re-issue
/// terminal.
///
/// The receipt window is the healing window and it never re-opens. Once it has
/// closed, provenance still explains the commit, so the server can name the
/// generation the lost attach produced — and the driver reports that as
/// `ReissueRequired` rather than as a generic refusal, because "your identity
/// moved to generation N and you cannot reach it" is a state an embedder acts
/// on, while "refused" is one it retries.
#[test]
fn an_expired_receipt_surfaces_the_typed_reissue_terminal() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let recovery = orphan_then_step_clock_to(
        &home.path().join("p195-expired"),
        BASE_MS + RECEIPT_TTL_MS + 1,
    )?;

    let RemoteCredentialAttachRecovery::ReissueRequired {
        result_generation,
        current_generation,
        reason,
        ..
    } = &recovery
    else {
        return Err(
            format!("an expired receipt must terminate in re-issue, got {recovery:?}").into(),
        );
    };
    assert_eq!(
        *reason,
        CredentialAttachReissueReason::ReceiptExpired(ReceiptExpiryReason::Deadline),
        "the receipt closed on its own deadline, not by supersession"
    );
    let result_generation =
        result_generation.ok_or("an expired receipt still proves WHICH generation it committed")?;
    assert_eq!(
        result_generation, *current_generation,
        "nothing else moved the identity, so the lost commit's result is still current"
    );
    Ok(())
}

/// PIN (d) — past provenance too, the terminal is the same but the claim is
/// weaker.
///
/// Once provenance has expired the server deliberately makes no commit claim at
/// all: exact-old and unknown tokens are indistinguishable from there. The
/// driver reports the same actionable terminal, and reports the missing
/// generation as MISSING rather than inventing one.
#[test]
fn a_stale_or_unknown_receipt_surfaces_the_same_terminal_without_a_commit_claim()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let recovery = orphan_then_step_clock_to(
        &home.path().join("p195-stale"),
        BASE_MS + PROVENANCE_TTL_MS + 1,
    )?;

    let RemoteCredentialAttachRecovery::ReissueRequired {
        result_generation,
        reason,
        ..
    } = &recovery
    else {
        return Err(
            format!("a stale receipt must terminate in re-issue, got {recovery:?}").into(),
        );
    };
    assert_eq!(*reason, CredentialAttachReissueReason::StaleOrUnknownReceipt);
    assert!(
        result_generation.is_none(),
        "a server claiming no commit proof must not be reported as having named a generation: \
         got {result_generation:?}"
    );
    Ok(())
}

/// The driver refuses to spend testimony it does not own.
///
/// A detach's testimony belongs to the replay machinery, which is correct and
/// untouched by this lane. The driver must therefore report that it has nothing
/// to do WITHOUT consuming the take-once atom the detach path still needs —
/// consuming it to discover the fact would destroy the resolution it was
/// checking for.
#[test]
fn the_driver_leaves_a_detachs_testimony_untouched() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let server = SdkSocketFixture::start(&home.path().join("p195-detach"))?;
    let address = server.address()?;

    let (witness, _witness_store) = connect(address)?;
    enroll(&witness, [0x51; 16])?;

    let (victim, store) = connect(address)?;
    let bound = enroll(&victim, [0x52; 16])?;
    await_attach_commit(&witness, bound.participant_id())?;

    // Issue a detach and die without consuming its answer.
    issue(
        &victim,
        ClientRequest::Detach(liminal_protocol::wire::DetachRequest {
            conversation_id: CONVERSATION,
            participant_id: bound.participant_id(),
            capability_generation: bound.capability_generation(),
            detach_attempt_token: liminal_protocol::wire::DetachAttemptToken::new([0x77; 16]),
        }),
    )?;
    let checkpoint = store.snapshot()?;
    drop(victim);
    drop(store);

    let config = client_config(address)?;
    let restored =
        RemoteParticipantHandle::restore(&config, SharedResumeStore::default(), &checkpoint)?;

    let verdict = restored.recover_lost_credential_attach()?;
    assert!(
        matches!(
            verdict,
            RemoteCredentialAttachRecovery::NotPending {
                reason: LostCredentialAttachRefusalReason::NotAnIssuedCredentialAttach
            }
        ),
        "a detach is not this driver's case, got {verdict:?}"
    );

    // THE POINT: the testimony the driver declined is still there for the path
    // that owns it.
    let resolution = restored.resolve_lost_operation_authority()?;
    assert!(
        matches!(
            resolution,
            liminal_sdk::RemoteLostOperationResolution::DetachParked { .. }
        ),
        "the detach's own resolution must still be available, got {resolution:?}"
    );

    drop(restored);
    drop(witness);
    server.stop()?;
    Ok(())
}
