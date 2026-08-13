//! The churn arm of the re-consume gate: attach exchanges torn mid-flight, many
//! times, against a live server over real sockets — then the session must still
//! converge.
//!
//! # What "torn mid-flight" means here, exactly
//!
//! `record_operation` -> `send_operation` -> drop the handle and its transport
//! without ever calling `receive`. The request is on the wire and the server
//! acts on it; the client never learns the outcome and the socket dies under the
//! reply. That is the 2026-08-10 outage's client-side shape reduced to a recipe:
//! `fill_buffer` read a closed receive window as a lost peer and abandoned the
//! socket with the answer still in flight, so every slow admission became one of
//! these tears.
//!
//! Each iteration restores from the SAME checkpoint bytes, so every attempt is
//! the same client making the same attach attempt again, having learned nothing
//! — not a sequence of different clients. That is what a retry after an
//! unanswered request actually looks like.
//!
//! # What convergence means, and why it is scoped where it is
//!
//! A torn exchange is allowed to fail. What it is not allowed to do is leave
//! anything permanently unusable. So: the checkpoint must still restore (never
//! into [`RemoteParticipantError::StateUnavailable`], never into a
//! `ResumeEncode` refusal), the torn attempt must still get a typed answer
//! carrying a live credential when it is retried, and the conversation must
//! still take a fresh participant's attach and commit its record.
//!
//! What is deliberately NOT asserted is that the CHURNED participant can
//! re-attach. A torn attach that bound rotates the capability generation, and a
//! client restored from the pre-tear checkpoint can only present the generation
//! it knew: the aggregate refuses to form an attach at the rotated generation
//! (`BindingMismatch`) and the server refuses one at the old generation
//! (`StaleAuthority { current_generation: 2 }`). That is a real gap in client
//! recovery from a lost attach answer. It is not what this lane fixes, and
//! pinning it green here would be pinning a defect, so it is named and left to
//! its own lane rather than dressed up as convergence.
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
const CHURN_CONVERSATION: u64 = 0x_C0_17;

/// How many attach exchanges are torn before convergence is asked for.
///
/// Large enough that a per-iteration leak (a retained identity slot, a live
/// attach receipt, a stranded binding) crosses a configured bound rather than
/// hiding under it: [`participant_config`] allows 8 live attach receipts per
/// participant and 4 identity slots, so 24 tears is three times the tightest
/// bound and six times the loosest.
const CHURN_ITERATIONS: usize = 24;

/// Pause before the tear on alternating iterations, so the churn covers both
/// sides of the server's reply: torn before it is written, and torn while or
/// after it is written into a socket nobody will read.
const LET_THE_SERVER_ACT: Duration = Duration::from_millis(8);

/// Frame-count bound on the response demultiplex loop. A step that reaches it
/// has failed.
const MAX_DEMUX_FRAMES: usize = 64;

/// Deployment-shaped participant configuration. Every field is a deployment
/// owner's decision — the type carries no defaults on purpose — so a fixture
/// has to state all of them, exactly as a real `[participant]` section does.
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

/// The resume store the SDK participant checkpoints into.
///
/// Shared rather than owned, because the checkpoint a churn iteration restores
/// from has to be readable from outside the handle that wrote it — the handle
/// owns its store and hands it back only through this Arc. The bytes are the
/// SDK's own canonical LPCR, not a fixture's reconstruction of one.
#[derive(Debug, Default, Clone)]
struct SharedResumeStore {
    canonical: Arc<Mutex<Vec<u8>>>,
}

impl SharedResumeStore {
    /// The latest checkpoint bytes, or an error if none was ever persisted.
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

/// A bound TCP server with the participant protocol live, plus the listener and
/// supervisor its teardown needs.
struct ChurnServer {
    listener: Option<ServerListener>,
    supervisor: ConnectionSupervisor,
    address: String,
}

impl ChurnServer {
    fn start(store_dir: &Path) -> Result<Self, Box<dyn Error>> {
        // The haematite engine creates its directory exactly one level below a
        // pre-existing parent it can fence, so the fixture creates the parent as
        // a deployment's operator would.
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
            CHURN_CONVERSATION.to_string(),
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

/// Records and sends one request, returning nothing about its outcome.
fn send_operation(
    participant: &SdkParticipant,
    request: ClientRequest,
) -> Result<(), Box<dyn Error>> {
    let operation = match participant.record_operation(request)? {
        RemoteOperationRecordOutcome::Recorded(operation)
        | RemoteOperationRecordOutcome::Continuous(operation) => operation,
        RemoteOperationRecordOutcome::Refused { request, reason } => {
            return Err(format!("SDK refused outbound request {request:?}: {reason:?}").into());
        }
    };
    match participant.send_operation(operation)? {
        RemoteParticipantSendOutcome::Sent { .. } => Ok(()),
        RemoteParticipantSendOutcome::TransportLost { error, .. } => {
            Err(format!("SDK transport lost while sending participant operation: {error}").into())
        }
    }
}

/// Sends one request and reads until its correlated response arrives,
/// discarding unsolicited pushes exactly as the SDK's own e2e tests do.
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

/// The state a churn iteration and the convergence step both start from: the
/// participant's identity, its live attach credential, and the checkpoint bytes
/// that put a fresh handle back into the detached state.
struct DetachedParticipant {
    participant_id: ParticipantId,
    attach_secret: AttachSecret,
    checkpoint: Vec<u8>,
}

/// Enrolls a participant and detaches it, leaving it attachable, and returns the
/// checkpoint every later attach attempt is restored from.
///
/// The detach is what makes an attach legal: a freshly enrolled participant is
/// already bound to its connection, so a credential attach only has work to do
/// once the origin binding has been given up.
fn enroll_then_detach(server: &ChurnServer) -> Result<DetachedParticipant, Box<dyn Error>> {
    let config = server.dial("churn-origin")?;
    let store = SharedResumeStore::default();
    let participant = RemoteParticipantHandle::new(&config, store.clone())?;

    let enrolled = exchange(
        &participant,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CHURN_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0xC1; 16]),
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
            conversation_id: CHURN_CONVERSATION,
            participant_id,
            capability_generation: Generation::ONE,
            detach_attempt_token: DetachAttemptToken::new([0xC2; 16]),
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

/// The attempt token every torn iteration and the first convergence attempt
/// present. One attempt, retried — not a new attempt each time.
const CHURN_ATTEMPT_TOKEN: [u8; 16] = [0xC3; 16];

/// A token no torn iteration used, so a request carrying it is a NEW attempt
/// and cannot be answered by replaying a receipt.
const FRESH_ATTEMPT_TOKEN: [u8; 16] = [0xC5; 16];

/// One credential attach, with the credentials the caller currently holds.
const fn attach_request(
    participant_id: ParticipantId,
    attach_secret: AttachSecret,
    capability_generation: Generation,
    attach_attempt_token: [u8; 16],
) -> ClientRequest {
    ClientRequest::CredentialAttach(CredentialAttachRequest {
        conversation_id: CHURN_CONVERSATION,
        participant_id,
        capability_generation,
        attach_secret,
        attach_attempt_token: AttachAttemptToken::new(attach_attempt_token),
        accept_marker_delivery_seq: None,
    })
}

/// The request every torn iteration presents: the participant's enrollment
/// credential under [`CHURN_ATTEMPT_TOKEN`].
const fn churn_attach_request(detached: &DetachedParticipant) -> ClientRequest {
    attach_request(
        detached.participant_id,
        detached.attach_secret,
        Generation::ONE,
        CHURN_ATTEMPT_TOKEN,
    )
}

/// The outcome of one torn attach exchange, kept so the census below can report
/// what the churn actually exercised rather than asserting it by construction.
#[derive(Debug, Default)]
struct ChurnCensus {
    /// Iterations whose request reached `send_operation` and was written.
    sent: usize,
    /// Iterations the SDK refused before the request reached the wire.
    refused_locally: Vec<String>,
    /// Iterations whose transport failed during the send itself.
    lost_in_send: Vec<String>,
}

/// Tears one attach exchange: restore, record, send, drop without ever reading
/// the reply.
///
/// The handle and its transport config are both dropped here and nowhere else.
/// `RemoteConfig` holds the `Arc` the handle clones, so the socket closes only
/// when the last of the two goes — dropping just the handle would leave the
/// connection open and the tear would not be a tear.
fn tear_one_attach(
    server: &ChurnServer,
    detached: &DetachedParticipant,
    census: &mut ChurnCensus,
    iteration: usize,
) -> Result<(), Box<dyn Error>> {
    let config = server.dial("churn-tear")?;
    let store = SharedResumeStore::default();
    let participant = RemoteParticipantHandle::restore(&config, store, &detached.checkpoint)
        .map_err(|error| {
            format!("iteration {iteration}: restoring the checkpoint failed: {error:?}")
        })?;

    let request = churn_attach_request(detached);
    let operation = match participant.record_operation(request)? {
        RemoteOperationRecordOutcome::Recorded(operation)
        | RemoteOperationRecordOutcome::Continuous(operation) => operation,
        RemoteOperationRecordOutcome::Refused { reason, .. } => {
            census
                .refused_locally
                .push(format!("iteration {iteration}: {reason:?}"));
            return Ok(());
        }
    };
    match participant.send_operation(operation)? {
        RemoteParticipantSendOutcome::Sent { .. } => census.sent += 1,
        RemoteParticipantSendOutcome::TransportLost { error, .. } => {
            census
                .lost_in_send
                .push(format!("iteration {iteration}: {error}"));
        }
    }

    if iteration % 2 == 1 {
        std::thread::sleep(LET_THE_SERVER_ACT);
    }
    drop(participant);
    drop(config);
    Ok(())
}

/// THE CHURN ARM. Twenty-four attach exchanges torn mid-flight against a live
/// server over real sockets, then the convergence the tears must not have cost.
///
/// The tears are the deterministic recipe, not a race: `record_operation` ->
/// `send_operation` -> drop, with no `receive` anywhere in the loop. Half of
/// them pause first so the server has certainly written a reply into a socket
/// nobody will ever read; the other half do not, so the tear lands while the
/// server is still deciding. Both are the same client-side event — an answer
/// that never arrives — and the outage produced both.
///
/// Four things are asserted, in the order a failure would matter:
///
/// 1. every tear reached the wire, so the churn exercised the server rather
///    than being absorbed by a client-side refusal;
/// 2. the checkpoint the tears were made from still restores, and never into
///    [`RemoteParticipantError::StateUnavailable`] or a `ResumeEncode` refusal;
/// 3. the torn attempt, retried once on a clean connection, gets a TYPED ANSWER
///    carrying live credentials — the server neither lost the attempt nor
///    terminated over it;
/// 4. the conversation itself converges: a fresh participant attaches and
///    commits an ordinary record on a clean connection, so 24 tears left no
///    exhausted identity slot, no leaked attach receipt, and no stuck binding.
#[test]
fn a_conversation_converges_after_attach_exchanges_torn_mid_flight() -> Result<(), Box<dyn Error>> {
    let store_dir = tempfile::tempdir()?;
    let server = ChurnServer::start(&store_dir.path().join("churn-store"))?;
    let detached = enroll_then_detach(&server)?;

    let mut census = ChurnCensus::default();
    for iteration in 0..CHURN_ITERATIONS {
        tear_one_attach(&server, &detached, &mut census, iteration)?;
    }

    assert!(
        census.refused_locally.is_empty(),
        "the SDK refused a torn-and-retried attach before it reached the wire, so the churn \
         stopped exercising the server: {:?}",
        census.refused_locally
    );
    assert_eq!(
        census.sent, CHURN_ITERATIONS,
        "the churn did not exercise what it claims: {} of {CHURN_ITERATIONS} attach requests \
         reached the wire, transport lost during send on {:?}",
        census.sent, census.lost_in_send
    );

    // The tears really landed: presenting the pre-churn credential under an
    // attempt token no tear used must now be refused as STALE. That is only
    // true if a torn attach reached the server, bound, and rotated the
    // authority — so this is what stops the gate passing on a churn that never
    // touched the server at all. It is the census's discriminator, not a
    // restatement of it: the census counts what left the client, this measures
    // what arrived.
    let (config, participant) = clean_session(&server, "churn-teeth", &detached.checkpoint)?;
    let stale = exchange(
        &participant,
        attach_request(
            detached.participant_id,
            detached.attach_secret,
            Generation::ONE,
            FRESH_ATTEMPT_TOKEN,
        ),
    )?;
    assert!(
        matches!(stale, ServerValue::StaleAuthority(_)),
        "the pre-churn credential is still current, so no torn attach reached the server far \
         enough to bind — the churn proved nothing: {stale:?}"
    );
    drop(participant);
    drop(config);

    // The torn attempt, retried once, on a clean connection.
    let (config, participant) = clean_session(&server, "churn-retry", &detached.checkpoint)?;
    let retried = exchange(&participant, churn_attach_request(&detached)).map_err(|error| {
        format!("the clean retry of the torn attach after {CHURN_ITERATIONS} tears failed: {error}")
    })?;
    let credential = match retried {
        // The retried attempt is the one that never got an answer, so the server
        // is entitled to answer it either way: it binds here if no tear landed
        // after the server bound, and replays the receipt for the attempt that
        // did otherwise. Which one arrives depends on where the tears fell, so
        // neither is asserted — what is asserted is that an answer arrives, and
        // that it carries a live credential rather than a refusal or a terminal.
        ServerValue::AttachBound(bound) => bound,
        ServerValue::UnboundReceipt(ReceiptReplay::CredentialAttach(receipt)) => receipt,
        other => {
            return Err(format!(
                "the clean retry after {CHURN_ITERATIONS} tears was neither a binding nor a \
                 receipt replay: {other:?}"
            )
            .into());
        }
    };
    assert_eq!(
        credential.participant_id(),
        detached.participant_id,
        "the answer to the retried attempt named a different participant"
    );
    assert!(
        credential.capability_generation() >= Generation::ONE,
        "the answer to the retried attempt carried no usable capability generation"
    );
    drop(participant);
    drop(config);

    // The conversation converges: a fresh participant, a clean connection, a
    // committed record.
    let converged = enroll_attach_and_commit(&server)?;
    assert!(
        matches!(converged, ServerValue::RecordCommitted(_)),
        "a fresh participant could not commit an ordinary record after {CHURN_ITERATIONS} torn \
         attach exchanges: {converged:?}"
    );

    server.shutdown()?;
    Ok(())
}

/// Enrolls a fresh participant on a clean connection and commits one ordinary
/// record with it, returning the server's answer to the admission.
///
/// The participant is fresh rather than the churned one for a reason this gate
/// does not hide: a torn attach that reached the server far enough to BIND
/// rotates the capability generation, and the client's checkpoint — restored
/// from before the tear — can only ever present the generation it knew. The
/// retry above gets a receipt replay carrying the rotated credential, but the
/// client aggregate refuses to form an attach at that generation
/// (`BindingMismatch`) and the server refuses one at the old generation
/// (`StaleAuthority { current_generation: 2 }`). That is a real gap in the
/// client's recovery from a lost attach answer, it is NOT what this lane fixes,
/// and pinning it green here would be pinning a defect — so it is named and
/// left to its own lane. What this step proves is the separable claim: the
/// tears cost the CONVERSATION nothing.
fn enroll_attach_and_commit(server: &ChurnServer) -> Result<ServerValue, Box<dyn Error>> {
    let config = server.dial("churn-fresh")?;
    let store = SharedResumeStore::default();
    let participant = RemoteParticipantHandle::new(&config, store)?;

    let enrolled = exchange(
        &participant,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CHURN_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0xC6; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(bound) = enrolled else {
        return Err(
            format!("the fresh enrollment did not bind after the churn: {enrolled:?}").into(),
        );
    };

    let committed = exchange(
        &participant,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: CHURN_CONVERSATION,
            participant_id: bound.participant_id(),
            capability_generation: bound.capability_generation(),
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([0xC4; 16]),
            payload: vec![0x0C, 0x17, 0xA5, 0x5A],
        }),
    )?;
    drop(participant);
    drop(config);
    Ok(committed)
}

/// A fresh connection carrying a handle restored from `checkpoint`.
///
/// The config is returned alongside the handle because it holds the `Arc` the
/// handle clones: dropping only the handle would leave the socket open.
fn clean_session(
    server: &ChurnServer,
    channel: &str,
    checkpoint: &[u8],
) -> Result<(RemoteConfig, SdkParticipant), Box<dyn Error>> {
    let config = server.dial(channel)?;
    let store = SharedResumeStore::default();
    let participant = RemoteParticipantHandle::restore(&config, store, checkpoint)
        .map_err(|error| assert_not_bricked("restoring the checkpoint after the churn", &error))?;
    Ok((config, participant))
}

/// Names the two permanent-brick error shapes explicitly, so a convergence
/// failure says WHICH failure it is rather than "an error happened".
fn assert_not_bricked(step: &str, error: &RemoteParticipantError) -> String {
    match error {
        RemoteParticipantError::StateUnavailable { .. } => format!(
            "{step}: the client aggregate is permanently unavailable after the churn \
             (StateUnavailable) — a torn exchange bricked the handle"
        ),
        RemoteParticipantError::ResumeEncode(source) => format!(
            "{step}: the checkpoint no longer re-encodes after the churn \
             (ResumeEncode: {source:?}) — a torn exchange bricked the resume record"
        ),
        other => format!("{step}: {other:?}"),
    }
}
