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
use std::sync::{Arc, Mutex};

use liminal_protocol::wire::{
    AttachAttemptToken, AttachSecret, ClientRequest, CredentialAttachRequest, EnrollBound,
    EnrollmentRequest, EnrollmentToken, Generation, ParticipantDelivery, ParticipantRecord,
    ServerPush, ServerValue,
};
use liminal_sdk::{
    ConnectionPoolConfig, ParticipantResumeStore, RemoteConfig, RemoteOperationRecordOutcome,
    RemoteParticipantHandle, RemoteParticipantInbound, RemoteParticipantSendOutcome, SdkError,
};

use super::SdkSocketFixture;

const CONVERSATION: u64 = 0x19_50;

/// Resume store whose committed bytes are readable from OUTSIDE the handle that
/// wrote them.
///
/// A process kill destroys the handle and keeps the durable record. Modelling
/// that needs exactly this shape: the bytes outlive the writer, so the test can
/// snapshot what the dying process had actually committed and hand those exact
/// bytes to the restore.
#[derive(Clone, Debug, Default)]
struct SharedResumeStore {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl SharedResumeStore {
    fn snapshot(&self) -> Result<Vec<u8>, Box<dyn Error>> {
        self.bytes
            .lock()
            .map(|bytes| bytes.clone())
            .map_err(|_| "shared resume store lock poisoned".into())
    }
}

impl ParticipantResumeStore for SharedResumeStore {
    fn persist(&mut self, canonical_lpcr: &[u8]) -> Result<(), SdkError> {
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
