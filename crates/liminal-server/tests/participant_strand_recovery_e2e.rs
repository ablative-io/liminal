//! The strand the churn arm named and deliberately left unpinned (task #62).
//!
//! `participant_churn_convergence_e2e.rs` proves that 24 torn attach exchanges
//! cost the CONVERSATION nothing. What it explicitly does NOT assert is that the
//! churned participant can re-attach, and its `enroll_attach_and_commit` doc
//! says why: a torn attach that bound rotates the capability generation, and a
//! client restored from the pre-tear checkpoint can only present the generation
//! it knew. Pinning that green there would have been pinning a defect.
//!
//! This gate is that defect's own lane, end to end over real sockets.
//!
//! # The recipe
//!
//! Enroll, detach, checkpoint. Tear exactly ONE attach — `record_operation` ->
//! `send_operation` -> pause so the server certainly acts -> drop the handle and
//! its transport without ever calling `receive`. The server binds, rotates the
//! capability generation to 2, mints a fresh attach secret, and writes an answer
//! into a socket nobody will read.
//!
//! Then, on a clean connection restored from the SAME pre-tear checkpoint, the
//! client re-presents the SAME attempt token. That is what a correct retry of an
//! unanswered request looks like, and it is what makes recovery possible at all:
//! the server answers a committed attempt by replaying its receipt, and that
//! receipt carries BOTH values the client is missing.
//!
//! # What is actually being measured
//!
//! One thing the protocol-level pins in `p0_62_stranded_handle_tests` cannot
//! reach: whether the SERVER accepts the attach the recovered client can now
//! form. The protocol pins prove the client adopts the rotated credential and
//! that `record_operation` stops refusing it. They stop at the client's edge.
//! Here the re-formed attach goes onto a real socket and has to be honoured by a
//! real server holding real state — and then the recovered participant has to
//! commit an ordinary record, which is the only evidence that what it recovered
//! was a WORKING binding rather than a well-formed request.
//!
//! Two facts are established before the recovery is asked for, so a pass cannot
//! be vacuous: the tear must actually have bound (measured by the pre-tear
//! credential being refused as `StaleAuthority`, not assumed), and the receipt
//! replay must actually carry a ROTATED generation (measured against the
//! pre-tear generation, not merely `>= 1`).
//!
//! This lives in an integration test for the reason `tests/loopback_sdk_e2e.rs`
//! documents: `liminal-server` dev-depends on `liminal-sdk` and the SDK depends
//! back, so only an integration test sees ONE `EmbeddedServer` type.

use std::error::Error;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use liminal_protocol::wire::{
    AttachAttemptToken, AttachSecret, ClientRequest, CredentialAttachRequest, DetachAttemptToken,
    DetachRequest, EnrollmentRequest, EnrollmentToken, Generation, ParticipantId, ReceiptReplay,
    RecordAdmission, RecordAdmissionAttemptToken, ServerValue,
};
use liminal_sdk::{
    ConnectionPoolConfig, ParticipantResumeStore, RemoteConfig, RemoteOperationRecordOutcome,
    RemoteParticipantError, RemoteParticipantHandle, RemoteParticipantInbound,
    RemoteParticipantSendOutcome, SdkError,
};
use liminal_server::config::types::ParticipantConfig;
use liminal_server::config::{LimitsConfig, ServerConfig, ServicesConfig};
use liminal_server::server::connection::{
    ConnectionServices, ConnectionSupervisor, LiminalConnectionServices,
};
use liminal_server::server::listener::ServerListener;

/// The one conversation this gate runs in.
const STRAND_CONVERSATION: u64 = 0x_57_2A;

/// Frames read while demultiplexing one correlated response.
const MAX_DEMUX_FRAMES: usize = 64;

/// Pause after the send so the server has certainly bound and written a reply
/// into the socket the tear is about to close.
const LET_THE_SERVER_BIND: Duration = Duration::from_millis(64);

/// The attempt token the torn attach uses and the recovery re-presents. ONE
/// attempt, retried — re-presenting it is what entitles the client to the
/// receipt replay that carries the rotated credential.
const STRAND_TOKEN: [u8; 16] = [0x5A; 16];

/// A token no attempt has used, so a request carrying it is a NEW attempt and
/// cannot be answered by replaying a receipt.
const FRESH_TOKEN: [u8; 16] = [0x5B; 16];

/// Deployment-shaped participant configuration. Every field is a deployment
/// owner's decision — the type carries no defaults on purpose.
const fn participant_config() -> ParticipantConfig {
    ParticipantConfig {
        wire_frame_limit: 65_536,
        attach_receipt_ttl_ms: 60_000,
        receipt_provenance_ttl_ms: 600_000,
        live_receipt_server_report_threshold: 1_024,
        max_live_attach_receipts_per_participant: 8,
        receipt_provenance_server_report_threshold: 4_096,
        receipt_provenance_per_conversation_report_threshold: 256,
        max_receipt_provenance_per_participant: 64,
        max_retired_identity_slots_server: 1_024,
        identity_slots: 4,
        observer_recovery_max_entries: 64,
        max_semantic_conversations_per_connection: 32,
        max_ordinary_record_entries: 1,
        max_ordinary_record_bytes: 131_072,
        max_generated_marker_entries: 1,
        max_generated_marker_bytes: 4_096,
        mandatory_transaction_bound_entries: 4,
        mandatory_transaction_bound_bytes: 16_384,
        full_recovery_claim_entries: 4,
        full_recovery_claim_bytes: 16_384,
        retained_capacity_entries: 2_048,
        retained_capacity_bytes: 16_777_216,
        max_retained_record_rows: 1_024,
        closure_episode_churn_limit: 1_024,
    }
}

/// The resume store the SDK participant checkpoints into, shared so the
/// checkpoint is readable from outside the handle that wrote it.
#[derive(Debug, Default, Clone)]
struct SharedResumeStore {
    canonical: Arc<Mutex<Vec<u8>>>,
}

impl SharedResumeStore {
    fn snapshot(&self) -> Result<Vec<u8>, Box<dyn Error>> {
        let guard = self
            .canonical
            .lock()
            .map_err(|_| "the resume store lock was poisoned")?;
        if guard.is_empty() {
            return Err("the participant never persisted a checkpoint".into());
        }
        Ok(guard.clone())
    }
}

impl ParticipantResumeStore for SharedResumeStore {
    fn persist(&mut self, canonical_lpcr: &[u8]) -> Result<(), SdkError> {
        let mut guard = self.canonical.lock().map_err(|_| SdkError::Store {
            description: "the resume store lock was poisoned".to_owned(),
        })?;
        guard.clear();
        guard.extend_from_slice(canonical_lpcr);
        drop(guard);
        Ok(())
    }
}

type SdkParticipant = RemoteParticipantHandle<SharedResumeStore>;

fn server_config(store_dir: &Path) -> Result<ServerConfig, Box<dyn Error>> {
    Ok(ServerConfig {
        listen_address: "127.0.0.1:0".parse()?,
        health_listen_address: "127.0.0.1:0".parse()?,
        drain_timeout_ms: 30_000,
        channels: Vec::new(),
        routing_rules: Vec::new(),
        persistence_path: Some(store_dir.to_path_buf()),
        cluster: None,
        auth: None,
        services: ServicesConfig::default(),
        limits: LimitsConfig::default(),
        websocket: None,
        participant: Some(participant_config()),
    })
}

const fn pool() -> ConnectionPoolConfig {
    ConnectionPoolConfig::new(1, 1, 8)
}

/// A bound TCP server with the participant protocol live.
struct StrandServer {
    listener: Option<ServerListener>,
    supervisor: ConnectionSupervisor,
    address: String,
}

impl StrandServer {
    fn start(store_dir: &Path) -> Result<Self, Box<dyn Error>> {
        // The haematite engine creates its directory exactly one level below a
        // pre-existing parent it can fence.
        std::fs::create_dir_all(store_dir)?;
        let config = server_config(store_dir)?;
        let services = Arc::new(LiminalConnectionServices::from_config(&config)?);
        let supervisor =
            ConnectionSupervisor::with_services(services as Arc<dyn ConnectionServices>)?;
        let listener = ServerListener::bind(&config, supervisor.clone())?;
        let address = listener.local_addr().to_string();
        Ok(Self {
            listener: Some(listener),
            supervisor,
            address,
        })
    }

    /// A fresh connected transport config — one new socket per call.
    fn dial(&self, channel: &str) -> Result<RemoteConfig, Box<dyn Error>> {
        Ok(RemoteConfig::new(
            self.address.clone(),
            channel,
            STRAND_CONVERSATION.to_string(),
            pool(),
        )?
        .connect_tcp()?)
    }

    fn shutdown(mut self) -> Result<(), Box<dyn Error>> {
        if let Some(listener) = self.listener.take() {
            listener.shutdown()?;
        }
        self.supervisor.shutdown();
        Ok(())
    }
}

/// Records and sends one request, naming a local refusal as the strand it is.
fn send_operation(
    participant: &SdkParticipant,
    request: ClientRequest,
) -> Result<(), Box<dyn Error>> {
    let operation = match participant.record_operation(request)? {
        RemoteOperationRecordOutcome::Recorded(operation)
        | RemoteOperationRecordOutcome::Continuous(operation) => operation,
        RemoteOperationRecordOutcome::Refused { request, reason } => {
            return Err(format!(
                "#62 REPRODUCED: the SDK refused to FORM the request its own server just \
                 authorized — the stranded handle. {reason:?} on {request:?}"
            )
            .into());
        }
    };
    match participant.send_operation(operation)? {
        RemoteParticipantSendOutcome::Sent { .. } => Ok(()),
        RemoteParticipantSendOutcome::TransportLost { error, .. } => {
            Err(format!("SDK transport lost while sending participant operation: {error}").into())
        }
    }
}

/// Sends one request and reads until its correlated response arrives.
fn exchange(
    participant: &SdkParticipant,
    request: ClientRequest,
) -> Result<ServerValue, Box<dyn Error>> {
    send_operation(participant, request)?;
    for _ in 0..MAX_DEMUX_FRAMES {
        match participant.receive()? {
            RemoteParticipantInbound::Applied { value, .. } => return Ok(value),
            RemoteParticipantInbound::Push { .. } => {}
            refused @ RemoteParticipantInbound::Refused { .. } => {
                return Err(format!("expected an applied server value, got {refused:?}").into());
            }
        }
    }
    Err(format!("no response arrived within {MAX_DEMUX_FRAMES} inbound frames").into())
}

/// One credential attach with the credentials the caller currently holds.
const fn attach_request(
    participant_id: ParticipantId,
    attach_secret: AttachSecret,
    capability_generation: Generation,
    attach_attempt_token: [u8; 16],
) -> ClientRequest {
    ClientRequest::CredentialAttach(CredentialAttachRequest {
        conversation_id: STRAND_CONVERSATION,
        participant_id,
        capability_generation,
        attach_secret,
        attach_attempt_token: AttachAttemptToken::new(attach_attempt_token),
        accept_marker_delivery_seq: None,
    })
}

/// The pre-tear state: identity, live attach credential, and the checkpoint
/// bytes that put a fresh handle back into the detached state.
struct DetachedParticipant {
    participant_id: ParticipantId,
    attach_secret: AttachSecret,
    checkpoint: Vec<u8>,
}

/// Enrolls a participant and detaches it, leaving it attachable.
///
/// The detach is what makes an attach legal: a freshly enrolled participant is
/// already bound to its connection, so a credential attach only has work to do
/// once the origin binding has been given up.
fn enroll_then_detach(server: &StrandServer) -> Result<DetachedParticipant, Box<dyn Error>> {
    let config = server.dial("strand-origin")?;
    let store = SharedResumeStore::default();
    let participant = RemoteParticipantHandle::new(&config, store.clone())?;

    let enrolled = exchange(
        &participant,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: STRAND_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x51; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(bound) = enrolled else {
        return Err(format!("enrollment did not bind: {enrolled:?}").into());
    };
    let participant_id = bound.participant_id();
    let attach_secret = bound.attach_secret();

    let detached = exchange(
        &participant,
        ClientRequest::Detach(DetachRequest {
            conversation_id: STRAND_CONVERSATION,
            participant_id,
            capability_generation: Generation::ONE,
            detach_attempt_token: DetachAttemptToken::new([0x52; 16]),
        }),
    )?;
    if !matches!(detached, ServerValue::DetachCommitted(_)) {
        return Err(format!("the origin detach did not commit: {detached:?}").into());
    }

    let checkpoint = store.snapshot()?;
    drop(participant);
    drop(config);
    Ok(DetachedParticipant {
        participant_id,
        attach_secret,
        checkpoint,
    })
}

/// Tears exactly one attach: restore, record, send, pause, drop without ever
/// reading the reply.
///
/// The handle and its transport config are both dropped here and nowhere else.
/// `RemoteConfig` holds the `Arc` the handle clones, so the socket closes only
/// when the last of the two goes — dropping just the handle would leave the
/// connection open and the tear would not be a tear.
fn tear_one_bound_attach(
    server: &StrandServer,
    detached: &DetachedParticipant,
) -> Result<(), Box<dyn Error>> {
    let config = server.dial("strand-tear")?;
    let store = SharedResumeStore::default();
    let participant = RemoteParticipantHandle::restore(&config, store, &detached.checkpoint)?;

    send_operation(
        &participant,
        attach_request(
            detached.participant_id,
            detached.attach_secret,
            Generation::ONE,
            STRAND_TOKEN,
        ),
    )?;

    std::thread::sleep(LET_THE_SERVER_BIND);
    drop(participant);
    drop(config);
    Ok(())
}

/// A fresh connection carrying a handle restored from the pre-tear checkpoint.
fn clean_session(
    server: &StrandServer,
    channel: &str,
    checkpoint: &[u8],
) -> Result<(RemoteConfig, SdkParticipant), Box<dyn Error>> {
    let config = server.dial(channel)?;
    let store = SharedResumeStore::default();
    let participant = RemoteParticipantHandle::restore(&config, store, checkpoint)
        .map_err(|error| assert_not_bricked("restoring the pre-tear checkpoint", &error))?;
    Ok((config, participant))
}

/// Names the two permanent-brick error shapes explicitly.
fn assert_not_bricked(step: &str, error: &RemoteParticipantError) -> String {
    match error {
        RemoteParticipantError::StateUnavailable { .. } => format!(
            "{step}: the client aggregate is permanently unavailable (StateUnavailable) — the \
             tear bricked the handle"
        ),
        RemoteParticipantError::ResumeEncode(source) => format!(
            "{step}: the checkpoint no longer re-encodes (ResumeEncode: {source:?}) — the tear \
             bricked the resume record"
        ),
        other => format!("{step}: {other:?}"),
    }
}

/// RED AT THE PARENT OF ITS FIX. A torn-but-bound attach must not strand the
/// client: re-presenting the lost attempt's token recovers the rotated
/// credential, the server honours the attach formed from it, and the recovered
/// participant commits an ordinary record.
///
/// The parent discarded the receipt replay (`ServerValue::UnboundReceipt(_)` in
/// `apply_correlated_value`'s no-op arm), so the recovery attach was refused
/// locally as `BindingMismatch` and this gate failed at `send_operation` with
/// the "#62 REPRODUCED" message before a byte reached the wire.
#[test]
fn a_torn_but_bound_attach_recovers_instead_of_stranding_the_client() -> Result<(), Box<dyn Error>>
{
    let store_dir = tempfile::tempdir()?;
    let server = StrandServer::start(&store_dir.path().join("strand"))?;
    let detached = enroll_then_detach(&server)?;

    tear_one_bound_attach(&server, &detached)?;

    // TEETH. The tear must really have bound, or every later step is vacuous:
    // presenting the pre-tear credential under a token no attempt used must now
    // be refused as stale. This measures what ARRIVED at the server, rather than
    // assuming the tear landed.
    let (config, participant) = clean_session(&server, "strand-teeth", &detached.checkpoint)?;
    let stale = exchange(
        &participant,
        attach_request(
            detached.participant_id,
            detached.attach_secret,
            Generation::ONE,
            FRESH_TOKEN,
        ),
    )?;
    let ServerValue::StaleAuthority(_) = stale else {
        return Err(format!(
            "the pre-tear credential is still current, so the tear never bound and this gate \
             would prove nothing: {stale:?}"
        )
        .into());
    };
    drop(participant);
    drop(config);

    // THE RECOVERY. A clean session restored from the SAME pre-tear checkpoint
    // re-presents the SAME attempt token, and the server replays the receipt for
    // the attach it committed.
    let (config, participant) = clean_session(&server, "strand-recover", &detached.checkpoint)?;
    let replayed = exchange(
        &participant,
        attach_request(
            detached.participant_id,
            detached.attach_secret,
            Generation::ONE,
            STRAND_TOKEN,
        ),
    )?;
    let receipt = match replayed {
        ServerValue::UnboundReceipt(ReceiptReplay::CredentialAttach(receipt))
        | ServerValue::Bound(ReceiptReplay::CredentialAttach(receipt))
        | ServerValue::AttachBound(receipt) => receipt,
        other => {
            return Err(format!(
                "re-presenting the torn attempt's token was not answered with its receipt: {other:?}"
            )
            .into());
        }
    };
    assert!(
        receipt.capability_generation() > Generation::ONE,
        "the receipt must carry a ROTATED generation — without a rotation there is no strand to \
         recover from, and this gate would be measuring nothing"
    );

    // THE CLAIM. The client adopted the rotated credential, so it can now FORM
    // an attach at that generation — and the server, holding the real state,
    // honours it. The parent could not get past `record_operation` here.
    let recovered = exchange(
        &participant,
        attach_request(
            detached.participant_id,
            receipt.attach_secret(),
            receipt.capability_generation(),
            FRESH_TOKEN,
        ),
    )?;
    let ServerValue::AttachBound(bound) = recovered else {
        return Err(format!(
            "the server refused the attach formed from the credential it had just replayed: \
             {recovered:?}"
        )
        .into());
    };
    assert!(
        bound.capability_generation() > receipt.capability_generation(),
        "a fresh attach must rotate the generation again, proving this was a real binding rather \
         than a replayed receipt"
    );

    // The only evidence that what was recovered is a WORKING binding rather than
    // a well-formed request: the recovered participant commits an ordinary
    // record, under its own identity, on this connection.
    let committed = exchange(
        &participant,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: STRAND_CONVERSATION,
            participant_id: detached.participant_id,
            capability_generation: bound.capability_generation(),
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([0x5C; 16]),
            payload: vec![0x57, 0x2A, 0x11, 0xED],
        }),
    )?;
    assert!(
        matches!(committed, ServerValue::RecordCommitted(_)),
        "the recovered participant could not commit an ordinary record, so what it recovered was \
         not a working binding: {committed:?}"
    );

    drop(participant);
    drop(config);
    server.shutdown()?;
    Ok(())
}
