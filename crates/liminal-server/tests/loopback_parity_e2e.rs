//! The discriminating parity gate: one participant scenario, driven twice, once
//! through the in-process loopback and once through a real TCP socket, asserting
//! byte-identical record outcomes
//! (`docs/design/IN-PROCESS-TRANSPORT.md` §6, §10).
//!
//! This is the acceptance test the design names. It joins the parity family
//! whose existing member is `ws_and_tcp_connect_responses_are_byte_identical`
//! (`tests/ws_transport_e2e.rs`), as the third column: the loopback and the
//! socket carry the identical framed wire image through the identical preflight,
//! participant gate and `apply_frame` seam, so what lands on the record must be
//! the same bytes.
//!
//! Three surfaces are compared, all as bytes:
//!
//! 1. **The durable transition-input log rows.** Each server writes one
//!    append-only stream per conversation whose entries carry the exact
//!    operation inputs; the rows are read back entry by entry off the same
//!    `DurableStore` handle the production handler wrote through.
//! 2. **The canonical shell event bytes.** They ride inside the event-bearing
//!    rows as the `event` field, and are additionally decoded through
//!    [`ConversationEvent::decode_canonical`] and compared as decoded events.
//! 3. **The response frames the client received**, re-encoded through the
//!    participant codec at each step, plus the unsolicited pushes.
//!
//! **The nondeterminism rule, and what it cost here.** Nothing is normalized
//! silently. Every input the two drives control is identical by construction:
//! the same conversation id, the same enrollment token, the same attach, detach
//! and record-admission attempt tokens, the same payload bytes, the same
//! sequence of operations. What remains is handled two ways, never by masking:
//!
//! - **Durable rows** are decoded at their own structure (they are `serde_json`
//!   documents) and walked in lockstep, so a difference is reported at its exact
//!   path. An exempt field stops the descent and is instead held to being
//!   EQUAL-SHAPED — same JSON type, same array length, same position.
//! - **Binary images** (canonical shell event bytes, response frames, pushes)
//!   are proven by SUBSTITUTION: the exempt value is read off the drive's own
//!   decoded surface, swapped into the other drive's image, and the two must
//!   then be byte-identical for their whole length. A substitution proof says
//!   "these named bytes and nothing else differ", which is strictly stronger
//!   than skipping a field.
//!
//! The complete exempt list, with reasons, is [`EXEMPT_FIELDS`]. It has three
//! entries and every one of them is a fact the SERVER produces from outside the
//! request — entropy, the wall clock, and the publication schedule.
//!
//! **The mount divergence is deliberate and is NOT in these bytes.** The
//! loopback drive's connection is stamped `MountKind::Loopback` at spawn and the
//! socket drive's `MountKind::Tcp`; the handler context reports each door's
//! stamp for every frame it carries (pinned in-crate at
//! `server/connection/process_tests.rs`, `a_connection_reports_its_own_mount_…`).
//! Per design §10 liminal's own durable rows carry NO mount field, so the row
//! parity asserted below is the behavioral proof of that absence: if the mount
//! had leaked into a row, these two drives would differ and this test would be
//! red. That is why the mount assertion and the row assertion are the same
//! assertion here.
//!
//! These pins live in an integration test for the reason
//! `tests/loopback_sdk_e2e.rs` documents: `liminal-server` dev-depends on
//! `liminal-sdk` with the `embedded` feature and the SDK depends back, so only
//! an integration test sees ONE `EmbeddedServer` type.

use std::error::Error;
use std::path::Path;
use std::sync::Arc;

use liminal::durability::bridge::block_on;
use liminal::durability::{DurableStore, StoredEntry};
use liminal_protocol::lifecycle::ConversationEvent;
use liminal_protocol::wire::{
    AttachAttemptToken, AttachSecret, ClientRequest, CredentialAttachRequest, DetachAttemptToken,
    DetachRequest, EnrollmentRequest, EnrollmentToken, Generation, ParticipantAck,
    ParticipantFrame, RecordAdmission, RecordAdmissionAttemptToken, ServerValue, encode,
    encoded_len,
};
use liminal_sdk::{
    ConnectionPoolConfig, ParticipantResumeStore, RemoteConfig, RemoteOperationRecordOutcome,
    RemoteParticipantHandle, RemoteParticipantInbound, RemoteParticipantSendOutcome, SdkError,
};
use liminal_server::config::types::ParticipantConfig;
use liminal_server::config::{LimitsConfig, ServerConfig, ServicesConfig};
use liminal_server::server::connection::{
    ConnectionServices, ConnectionSupervisor, LiminalConnectionServices,
};
use liminal_server::server::embedded::EmbeddedServer;
use liminal_server::server::listener::ServerListener;

/// The one conversation both drives run in.
const PARITY_CONVERSATION: u64 = 0x50_05;

/// Stream-key prefix of the production participant transition-input log.
///
/// Spelled out here because the constant itself is `pub(super)` inside the
/// production module (`server/participant/production/log.rs`, `STREAM_PREFIX`).
/// A wrong literal cannot pass silently: [`read_transition_log`] refuses an
/// empty stream, and the row census below names every operation it expects.
const TRANSITION_LOG_PREFIX: &str = "liminal:participant-production:";

/// Page size for the durable read-back. Larger than any row count this scenario
/// can produce, so a short page means the stream ended.
const TRANSITION_LOG_PAGE: usize = 256;

/// The complete list of bytes this test could NOT make identical across the two
/// drives, with the reason each one is outside a client's control.
///
/// Both are minted by the SERVER from outside the request, so no arrangement of
/// deterministic inputs can align them; both are handled by substitution (see
/// [`assert_parity_after_substitution`]), never by masking.
///
/// 1. `attach_secret` — 32 bytes read from `/dev/urandom` at every enrollment
///    and at every credential rotation (`production/facts.rs`,
///    `mint_secret_bytes`). A predictable attach secret must never be issued, so
///    this one is nondeterministic ON PURPOSE.
/// 2. `receipt_expires_at` / `provenance_expires_at` / `admitted_now_ms` —
///    wall-clock reads (`production/facts.rs`, `now_unix_millis`) plus the
///    configured TTLs. Two runs a few milliseconds apart stamp different
///    deadlines.
/// 3. The INTERLEAVE POSITION and REPEAT COUNT of an unsolicited push — not a
///    field but a schedule. Participant publication is at-least-once and
///    server-driven, so when a push lands relative to a response, and how many
///    times an unacked obligation is re-offered, is decided by the publication
///    scan rather than by the door. Measured on both mounts across repeated runs
///    of this scenario (see [`assert_push_parity`]), so it is not a loopback
///    property. The push CONTENT is still asserted byte-identical.
///
/// Everything else — every request field, every allocated identity, epoch,
/// transaction order, delivery sequence, charge, every response frame in request
/// order, and every canonical shell event byte around the substituted secret —
/// is asserted equal verbatim.
const EXEMPT_FIELDS: &[&str] = &[
    "attach_secret (32 bytes of /dev/urandom entropy, minted per enrollment and per rotation)",
    "receipt_expires_at / provenance_expires_at / admitted_now_ms (wall-clock reads plus TTL)",
    "unsolicited-push interleave position and repeat count (at-least-once publication \
     scheduling, observed on BOTH mounts; push content is still compared byte for byte)",
];

/// Deployment-shaped participant configuration; every field is a deployment
/// owner's decision, so a fixture states all of them.
const fn participant_config() -> ParticipantConfig {
    ParticipantConfig {
        wire_frame_limit: 65_536,
        attach_receipt_ttl_ms: 60_000,
        receipt_provenance_ttl_ms: 600_000,
        max_live_attach_receipts_server: 1_024,
        max_live_attach_receipts_per_participant: 8,
        max_receipt_provenance_server: 4_096,
        max_receipt_provenance_per_conversation: 256,
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
#[derive(Debug, Default)]
struct MemoryResumeStore {
    canonical: Vec<u8>,
}

impl ParticipantResumeStore for MemoryResumeStore {
    fn persist(&mut self, canonical_lpcr: &[u8]) -> Result<(), SdkError> {
        self.canonical.clear();
        self.canonical.extend_from_slice(canonical_lpcr);
        Ok(())
    }
}

type SdkParticipant = RemoteParticipantHandle<MemoryResumeStore>;

/// One drive's complete observation: what the client saw and what the server
/// wrote.
struct DriveOutcome {
    /// Durable transition-input log rows, in stream order, as stored bytes.
    rows: Vec<Vec<u8>>,
    /// Response frames the client received, one per request, in request order,
    /// re-encoded through the participant codec.
    responses: Vec<Vec<u8>>,
    /// Unsolicited server pushes the client received, in arrival order,
    /// re-encoded through the participant codec. May contain the same delivery
    /// more than once — see [`assert_push_parity`].
    pushes: Vec<Vec<u8>>,
    /// Every server-minted attach secret this drive saw, in mint order: the two
    /// enrollment secrets, then the secret the credential rotation returned.
    secrets: Vec<[u8; 32]>,
    /// Every server-stamped receipt/provenance deadline this drive saw, in the
    /// order the responses carried them.
    deadlines: Vec<u128>,
}

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

/// The full production service stack plus the durable store handle the
/// production handler writes through.
type ProductionStack = (Arc<LiminalConnectionServices>, Arc<dyn DurableStore>);

/// Builds the full production service stack rooted at `store_dir`, keeping the
/// durable store handle the production handler will write through.
///
/// `LiminalConnectionServices::from_config` is the same construction
/// `build_connection_services` performs for the `Full` profile; taking it
/// directly is what makes the store readable without widening any surface —
/// `EmbeddedServer` deliberately exposes no store, and nothing here asks it to.
fn production_services(store_dir: &Path) -> Result<ProductionStack, Box<dyn Error>> {
    // The haematite engine creates its directory exactly one level below a
    // pre-existing parent it can fence, so the fixture creates the parent as a
    // deployment's operator would.
    std::fs::create_dir_all(store_dir)?;
    let config = server_config(store_dir)?;
    let services = Arc::new(LiminalConnectionServices::from_config(&config)?);
    let store = services.durable_store();
    Ok((services, store))
}

const fn pool() -> ConnectionPoolConfig {
    ConnectionPoolConfig::new(1, 1, 8)
}

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

/// Re-encodes one received participant frame back into its exact wire bytes.
///
/// The SDK hands an application the DECODED value, not the inbound buffer, and
/// giving it a raw-bytes accessor to satisfy a test would widen the client
/// surface this design keeps narrow. Re-encoding is faithful because the
/// participant codec is canonical — the same encoder minted these bytes on the
/// server — so a frame that differed on the wire differs here too.
fn frame_bytes(frame: &ParticipantFrame) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut bytes = vec![0_u8; encoded_len(frame).map_err(|error| format!("{error:?}"))?];
    let written = encode(frame, &mut bytes).map_err(|error| format!("{error:?}"))?;
    bytes.truncate(written);
    Ok(bytes)
}

/// Frame-count bound on the response demultiplex loop; mirrors the in-tree
/// `MAX_DEMUX_FRAMES`. A step that reaches it has failed.
const MAX_DEMUX_FRAMES: usize = 64;

/// Sends one request and reads until its response arrives, recording every
/// inbound frame's wire bytes — responses in request order, pushes in arrival
/// order.
///
/// An unsolicited `ServerPush` may reach the client ahead of a response on a
/// shared connection — the participant contract permits per-conversation
/// interleave — so the reader demultiplexes exactly as the SDK's own e2e tests
/// do. Pushes are not skipped: they are kept, in arrival order, for the separate
/// comparison [`assert_push_parity`] performs.
fn step(
    participant: &SdkParticipant,
    responses: &mut Vec<Vec<u8>>,
    pushes: &mut Vec<Vec<u8>>,
    request: ClientRequest,
) -> Result<ServerValue, Box<dyn Error>> {
    send_operation(participant, request)?;
    for _ in 0..MAX_DEMUX_FRAMES {
        match participant.receive()? {
            RemoteParticipantInbound::Applied { value, .. } => {
                responses.push(frame_bytes(&ParticipantFrame::ServerValue(value.clone()))?);
                return Ok(value);
            }
            RemoteParticipantInbound::Push { value, .. } => {
                pushes.push(frame_bytes(&ParticipantFrame::ServerPush(value))?);
            }
            // Named rather than wildcarded: a refusal is a real outcome this
            // scenario must never reach, and a future inbound variant should
            // fail to compile here rather than be swallowed as "not applied".
            refused @ RemoteParticipantInbound::Refused { .. } => {
                return Err(format!("expected an applied server value, got {refused:?}").into());
            }
        }
    }
    Err(format!("no response arrived within {MAX_DEMUX_FRAMES} inbound frames").into())
}

/// The scenario, identical on both mounts: connect (already done by the caller's
/// transport), enroll, enroll a peer, drain the peer's arrival push, detach,
/// attach, commit two ordinary records, ack, detach.
///
/// The first detach is what makes the attach legal — a freshly enrolled
/// participant is already bound to its connection, so a credential attach only
/// has work to do once the origin binding has been given up.
///
/// The peer is what makes the ACK real. A lone participant is excluded from its
/// own records, so it holds no delivery obligation and an acknowledgement over
/// an empty debt is answered `AckNoOp` and writes no durable row — a step that
/// would prove nothing about the record. The peer enrolls, which lands a genuine
/// recipient obligation at delivery sequence 2 on the primary, and the ack below
/// discharges exactly that. The peer then stays silent for the rest of the run,
/// so the primary's inbound stream is deterministic: one push, then one response
/// per request.
///
/// Every token and every payload below is a fixed literal, so both drives
/// present byte-identical request bodies at every step.
fn run_scenario(
    participant: &SdkParticipant,
    peer: &SdkParticipant,
    store: &Arc<dyn DurableStore>,
) -> Result<DriveOutcome, Box<dyn Error>> {
    let mut responses = Vec::new();
    let mut pushes = Vec::new();
    let mut secrets = Vec::new();
    let mut deadlines = Vec::new();

    let enrolled = step(
        participant,
        &mut responses,
        &mut pushes,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: PARITY_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x5A; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(bound) = enrolled else {
        return Err(format!("enrollment did not bind: {enrolled:?}").into());
    };
    let participant_id = bound.participant_id();
    let enrollment_secret = bound.attach_secret();
    secrets.push(enrollment_secret.into_bytes());
    deadlines.push(bound.receipt_expires_at());
    deadlines.push(bound.provenance_expires_at());

    let peer_enrolled = step(
        peer,
        &mut responses,
        &mut pushes,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: PARITY_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x6A; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(peer_bound) = peer_enrolled else {
        return Err(format!("the peer enrollment did not bind: {peer_enrolled:?}").into());
    };
    secrets.push(peer_bound.attach_secret().into_bytes());
    deadlines.push(peer_bound.receipt_expires_at());
    deadlines.push(peer_bound.provenance_expires_at());

    let detached = step(
        participant,
        &mut responses,
        &mut pushes,
        ClientRequest::Detach(DetachRequest {
            conversation_id: PARITY_CONVERSATION,
            participant_id,
            capability_generation: Generation::ONE,
            detach_attempt_token: DetachAttemptToken::new([0x5B; 16]),
        }),
    )?;
    if !matches!(detached, ServerValue::DetachCommitted(_)) {
        return Err(format!("the origin detach did not commit: {detached:?}").into());
    }

    let attached = step(
        participant,
        &mut responses,
        &mut pushes,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: PARITY_CONVERSATION,
            participant_id,
            capability_generation: Generation::ONE,
            attach_secret: enrollment_secret,
            attach_attempt_token: AttachAttemptToken::new([0x5C; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    let ServerValue::AttachBound(attach_bound) = attached else {
        return Err(format!("the credential attach did not bind: {attached:?}").into());
    };
    secrets.push(attach_bound.attach_secret().into_bytes());
    deadlines.push(attach_bound.receipt_expires_at());
    deadlines.push(attach_bound.provenance_expires_at());
    let rotated = Generation::new(2).ok_or("generation two is nonzero")?;

    for (token, payload) in [
        ([0x5D_u8; 16], vec![0x00_u8, 0xFF, 0x50, 0x05, 0xA5]),
        ([0x5E_u8; 16], vec![0x11_u8, 0x22, 0x33, 0x44, 0x55, 0x66]),
    ] {
        let committed = step(
            participant,
            &mut responses,
            &mut pushes,
            ClientRequest::RecordAdmission(RecordAdmission {
                conversation_id: PARITY_CONVERSATION,
                participant_id,
                capability_generation: rotated,
                record_admission_attempt_token: RecordAdmissionAttemptToken::new(token),
                payload,
            }),
        )?;
        if !matches!(committed, ServerValue::RecordCommitted(_)) {
            return Err(format!("an ordinary record did not commit: {committed:?}").into());
        }
    }

    let acked = step(
        participant,
        &mut responses,
        &mut pushes,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: PARITY_CONVERSATION,
            participant_id,
            capability_generation: rotated,
            through_seq: 2,
        }),
    )?;
    if !matches!(acked, ServerValue::AckCommitted(_)) {
        return Err(format!("the acknowledgement did not commit: {acked:?}").into());
    }

    let final_detach = step(
        participant,
        &mut responses,
        &mut pushes,
        ClientRequest::Detach(DetachRequest {
            conversation_id: PARITY_CONVERSATION,
            participant_id,
            capability_generation: rotated,
            detach_attempt_token: DetachAttemptToken::new([0x5F; 16]),
        }),
    )?;
    if !matches!(final_detach, ServerValue::DetachCommitted(_)) {
        return Err(format!("the final detach did not commit: {final_detach:?}").into());
    }

    let rows = read_transition_log(store)?;
    Ok(DriveOutcome {
        rows,
        responses,
        pushes,
        secrets,
        deadlines,
    })
}

/// Reads the whole transition-input log for [`PARITY_CONVERSATION`] as stored
/// bytes, in stream order.
fn read_transition_log(store: &Arc<dyn DurableStore>) -> Result<Vec<Vec<u8>>, Box<dyn Error>> {
    let stream_key = format!("{TRANSITION_LOG_PREFIX}{PARITY_CONVERSATION}");
    let mut rows: Vec<Vec<u8>> = Vec::new();
    loop {
        let head = u64::try_from(rows.len())?;
        let page: Vec<StoredEntry> =
            block_on(store.read_from(&stream_key, head, TRANSITION_LOG_PAGE))??;
        let read = page.len();
        for (offset, entry) in page.into_iter().enumerate() {
            let expected = head + u64::try_from(offset)?;
            if entry.sequence != expected {
                return Err(format!(
                    "the transition-input log is not contiguous: expected sequence {expected}, \
                     read {}",
                    entry.sequence
                )
                .into());
            }
            rows.push(entry.payload);
        }
        if read < TRANSITION_LOG_PAGE {
            break;
        }
    }
    if rows.is_empty() {
        return Err(format!(
            "no durable rows were found at {stream_key} — a parity assertion over an empty \
             stream would prove nothing"
        )
        .into());
    }
    Ok(rows)
}

/// Drives the scenario over the in-process loopback.
fn drive_loopback(store_dir: &Path) -> Result<DriveOutcome, Box<dyn Error>> {
    let (services, store) = production_services(store_dir)?;
    let server = Arc::new(EmbeddedServer::with_services(
        services as Arc<dyn ConnectionServices>,
    )?);
    let config = RemoteConfig::new(
        "in-process",
        "loopback-parity",
        PARITY_CONVERSATION.to_string(),
        pool(),
    )?
    .connect_loopback(Arc::clone(&server))?;
    let peer_config = RemoteConfig::new(
        "in-process",
        "loopback-parity-peer",
        PARITY_CONVERSATION.to_string(),
        pool(),
    )?
    .connect_loopback(Arc::clone(&server))?;
    let participant = RemoteParticipantHandle::new(&config, MemoryResumeStore::default())?;
    let peer = RemoteParticipantHandle::new(&peer_config, MemoryResumeStore::default())?;
    let outcome = run_scenario(&participant, &peer, &store)?;
    drop(participant);
    drop(peer);
    drop(config);
    drop(peer_config);
    Ok(outcome)
}

/// Drives the identical scenario over a real bound TCP listener with the real
/// SDK socket transport.
fn drive_tcp(store_dir: &Path) -> Result<DriveOutcome, Box<dyn Error>> {
    let (services, store) = production_services(store_dir)?;
    let config = server_config(store_dir)?;
    let supervisor = ConnectionSupervisor::with_services(services as Arc<dyn ConnectionServices>)?;
    let listener = ServerListener::bind(&config, supervisor.clone())?;
    let address = listener.local_addr();
    let remote = RemoteConfig::new(
        address.to_string(),
        "loopback-parity",
        PARITY_CONVERSATION.to_string(),
        pool(),
    )?
    .connect_tcp()?;
    let peer_remote = RemoteConfig::new(
        address.to_string(),
        "loopback-parity-peer",
        PARITY_CONVERSATION.to_string(),
        pool(),
    )?
    .connect_tcp()?;
    let participant = RemoteParticipantHandle::new(&remote, MemoryResumeStore::default())?;
    let peer = RemoteParticipantHandle::new(&peer_remote, MemoryResumeStore::default())?;
    let outcome = run_scenario(&participant, &peer, &store)?;
    drop(participant);
    drop(peer);
    drop(remote);
    drop(peer_remote);
    listener.shutdown()?;
    supervisor.shutdown();
    Ok(outcome)
}

/// Durable row FIELD NAMES the two drives cannot make identical, each named
/// with the reason it is outside a client's control.
///
/// Matched by leaf field name during the structural walk below, so an exemption
/// applies wherever that field appears in the row grammar and nowhere else. A
/// field not on this list that differs fails the comparison with its full path.
const EXEMPT_ROW_FIELDS: &[(&str, &str)] = &[
    (
        "attach_secret",
        "32 bytes read from /dev/urandom per enrollment and per credential rotation \
         (production/facts.rs, mint_secret_bytes); a predictable attach secret must never \
         be issued, so this is nondeterministic on purpose",
    ),
    (
        "receipt_expires_at",
        "a wall-clock read plus the configured attach-receipt TTL \
         (production/facts.rs, now_unix_millis)",
    ),
    (
        "provenance_expires_at",
        "a wall-clock read plus the configured receipt-provenance TTL",
    ),
    (
        "admitted_now_ms",
        "the wall-clock millisecond the attach was admitted at",
    ),
];

/// What a structural row comparison found.
#[derive(Debug, Default)]
struct RowComparison {
    /// Full paths at which an exempt field was reached, whether or not its
    /// values happened to differ.
    exempt_paths: Vec<String>,
    /// Canonical shell event byte pairs pulled out for the separate byte-level
    /// substitution proof, keyed by path.
    events: Vec<(String, Vec<u8>, Vec<u8>)>,
}

/// Walks two decoded rows in lockstep and asserts every non-exempt leaf is
/// equal, reporting the exact path of the first difference.
///
/// This is the nondeterminism rule's "decode/split at the row structure" — the
/// rows are `serde_json` documents, so the split is the document's own shape and
/// nothing is matched by pattern over a blob. At an exempt field the walk stops
/// descending and instead asserts the two values are EQUAL-SHAPED: same JSON
/// type, and for the byte arrays the same length. Position is guaranteed because
/// the walk only ever compares the same path in both documents.
///
/// A row's `event` field — the canonical shell event bytes — is neither compared
/// here nor waved through: it is handed back for the byte-level substitution
/// proof, which is strictly stronger than an element-wise compare because it
/// proves the ONLY differing bytes are the named secret.
fn compare_row_structure(
    path: &str,
    left: &serde_json::Value,
    right: &serde_json::Value,
    report: &mut RowComparison,
) -> Result<(), Box<dyn Error>> {
    use serde_json::Value;
    match (left, right) {
        (Value::Object(left_map), Value::Object(right_map)) => {
            let mut left_keys: Vec<&String> = left_map.keys().collect();
            let mut right_keys: Vec<&String> = right_map.keys().collect();
            left_keys.sort_unstable();
            right_keys.sort_unstable();
            if left_keys != right_keys {
                return Err(format!(
                    "{path}: the two mounts wrote different row FIELDS — loopback \
                     {left_keys:?}, socket {right_keys:?}"
                )
                .into());
            }
            for (key, left_value) in left_map {
                let child = format!("{path}/{key}");
                let right_value = right_map
                    .get(key)
                    .ok_or_else(|| format!("{child}: missing on the socket mount"))?;
                if key == "event" {
                    let left_bytes: Vec<u8> = serde_json::from_value(left_value.clone())?;
                    let right_bytes: Vec<u8> = serde_json::from_value(right_value.clone())?;
                    report.events.push((child, left_bytes, right_bytes));
                    continue;
                }
                if let Some((_, reason)) = EXEMPT_ROW_FIELDS.iter().find(|(name, _)| name == key) {
                    assert_eq!(
                        shape_of(left_value),
                        shape_of(right_value),
                        "{child}: an exempt field must still be EQUAL-SHAPED across the two \
                         mounts ({reason})"
                    );
                    report.exempt_paths.push(child);
                    continue;
                }
                compare_row_structure(&child, left_value, right_value, report)?;
            }
            Ok(())
        }
        (Value::Array(left_items), Value::Array(right_items)) => {
            if left_items.len() != right_items.len() {
                return Err(format!(
                    "{path}: the two mounts wrote arrays of different length ({} vs {})",
                    left_items.len(),
                    right_items.len()
                )
                .into());
            }
            for (index, (left_item, right_item)) in
                left_items.iter().zip(right_items.iter()).enumerate()
            {
                compare_row_structure(&format!("{path}/{index}"), left_item, right_item, report)?;
            }
            Ok(())
        }
        _ => {
            if left == right {
                return Ok(());
            }
            Err(format!(
                "{path}: the two mounts wrote different values — loopback {left}, socket \
                 {right}. This field is NOT on the exempt list, so it is a genuine \
                 divergence between the in-process and socket record paths."
            )
            .into())
        }
    }
}

/// A JSON value's shape: its type, and an array's length.
///
/// Used to hold an exempt field to "same length/type/position" rather than
/// letting it vary freely.
fn shape_of(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_owned(),
        serde_json::Value::Bool(_) => "bool".to_owned(),
        serde_json::Value::Number(_) => "number".to_owned(),
        serde_json::Value::String(text) => format!("string[{}]", text.len()),
        serde_json::Value::Array(items) => format!("array[{}]", items.len()),
        serde_json::Value::Object(map) => format!("object[{}]", map.len()),
    }
}

/// One exempt value as the two drives minted it: the loopback's bytes and the
/// socket's bytes, always the same width.
type ExemptPair = (Vec<u8>, Vec<u8>);

/// Rewrites every occurrence of each `from` byte string in `image` with the
/// matching `to`, returning the substituted image and how many substitutions it
/// made.
///
/// Every pair is equal-width, so a substitution never changes the image's
/// length — which is what lets the caller assert on length separately and read a
/// length difference as a genuine structural divergence.
fn substitute(image: &[u8], pairs: &[ExemptPair]) -> (Vec<u8>, usize) {
    let mut out = image.to_vec();
    let mut count = 0;
    for (from, to) in pairs {
        assert_eq!(
            from.len(),
            to.len(),
            "an exempt substitution pair must be equal-width"
        );
        let mut cursor = 0;
        while let Some(found) = out
            .get(cursor..)
            .and_then(|tail| tail.windows(from.len()).position(|w| w == from.as_slice()))
        {
            let at = cursor + found;
            if let Some(slot) = out.get_mut(at..at + to.len()) {
                slot.copy_from_slice(to);
            }
            cursor = at + to.len();
            count += 1;
        }
    }
    (out, count)
}

/// Asserts two byte images are identical once — and ONLY once — the named
/// exempt secrets have been substituted.
///
/// This is the nondeterminism rule's shape: nothing is skipped, nothing is
/// masked. The exempt values are read off each drive's own decoded surface
/// (`EnrollBound::attach_secret` and `AttachBound::attach_secret`), swapped into
/// the loopback image, and the result must then match the socket image for its
/// whole length. Anything else that differed survives the substitution and
/// fails the comparison.
fn assert_parity_after_substitution(what: &str, loopback: &[u8], tcp: &[u8], pairs: &[ExemptPair]) {
    assert_eq!(
        loopback.len(),
        tcp.len(),
        "{what}: the two mounts produced different image LENGTHS ({} vs {}), which no \
         fixed-width exempt field can explain",
        loopback.len(),
        tcp.len()
    );
    if loopback == tcp {
        return;
    }
    let (substituted, replacements) = substitute(loopback, pairs);
    assert_eq!(
        substituted,
        tcp.to_vec(),
        "{what}: the two mounts differ in bytes that are NOT one of the {} exempt \
         server-minted secrets ({replacements} substitution(s) applied). \
         loopback={loopback:02x?} tcp={tcp:02x?}",
        pairs.len()
    );
}

/// The durable operation tag of a row, e.g. `"record_admission"`.
fn row_operation(row: &[u8]) -> Result<String, Box<dyn Error>> {
    let json: serde_json::Value = serde_json::from_slice(row)?;
    json["operation"]["operation"]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| "a durable row carried no operation tag".into())
}

#[test]
fn a_loopback_drive_and_a_socket_drive_leave_byte_identical_records() -> Result<(), Box<dyn Error>>
{
    let home = tempfile::tempdir()?;
    let loopback = drive_loopback(&home.path().join("loopback"))?;
    let tcp = drive_tcp(&home.path().join("socket"))?;

    // The exempt substitutions, paired in the order the two drives minted them.
    // Both drives mint exactly three secrets — two enrollments and the
    // rotation's successor — and six deadlines, two per bound response, so a
    // count mismatch here would mean the two drives did not run the same
    // scenario at all.
    assert_eq!(
        loopback.secrets.len(),
        tcp.secrets.len(),
        "the two drives minted different numbers of attach secrets"
    );
    assert_eq!(
        loopback.secrets.len(),
        3,
        "the scenario mints three secrets: two enrollments and one rotation"
    );
    assert_eq!(
        loopback.deadlines.len(),
        tcp.deadlines.len(),
        "the two drives stamped different numbers of deadlines"
    );
    assert_eq!(
        loopback.deadlines.len(),
        6,
        "the scenario stamps a receipt and a provenance deadline on each of the three \
         bound responses"
    );

    let mut pairs: Vec<ExemptPair> = loopback
        .secrets
        .iter()
        .zip(tcp.secrets.iter())
        .map(|(left, right)| (left.to_vec(), right.to_vec()))
        .collect();
    // A secret that repeated across the two drives would make its substitution a
    // no-op, so the exemption would be proving nothing. 32 bytes of entropy
    // colliding is not a real possibility; asserting it is how the test stays
    // honest if the mint is ever replaced by something weaker.
    for (loopback_secret, tcp_secret) in &pairs {
        assert_ne!(
            loopback_secret, tcp_secret,
            "the two drives minted the SAME attach secret, so its substitution would be \
             a no-op and would prove nothing"
        );
    }
    // Deadlines are wall-clock u128s, carried big-endian on the wire. Unlike the
    // secrets they MAY legitimately coincide — two runs can land in the same
    // millisecond — so no inequality is asserted here; an equal pair simply
    // substitutes to itself and the surrounding bytes still have to match.
    pairs.extend(
        loopback
            .deadlines
            .iter()
            .zip(tcp.deadlines.iter())
            .map(|(left, right)| (left.to_be_bytes().to_vec(), right.to_be_bytes().to_vec())),
    );

    // ---- the durable op-log rows ----
    assert_eq!(
        loopback.rows.len(),
        tcp.rows.len(),
        "the two mounts wrote different numbers of durable rows"
    );
    let census: Vec<String> = loopback
        .rows
        .iter()
        .map(|row| row_operation(row))
        .collect::<Result<_, _>>()?;
    assert_eq!(
        census,
        vec![
            "genesis".to_owned(),
            "enrolled".to_owned(),
            "enrolled".to_owned(),
            "detached".to_owned(),
            "attached".to_owned(),
            "record_admission".to_owned(),
            "record_admission".to_owned(),
            "zero_debt_ack".to_owned(),
            "detached".to_owned(),
        ],
        "the scenario did not write the operations it claims to exercise, so a parity \
         assertion over these rows would not cover the record path it names"
    );

    // Entry by entry, decoded at the row's own structure. Row bytes are a
    // `serde_json` document whose numbers are decimal text, so two different
    // random secrets make two different row LENGTHS — which is exactly why the
    // rule says split at the structure rather than compare a blob.
    let mut report = RowComparison::default();
    for (index, (loopback_row, tcp_row)) in loopback.rows.iter().zip(tcp.rows.iter()).enumerate() {
        assert_eq!(
            row_operation(loopback_row)?,
            row_operation(tcp_row)?,
            "durable row {index} is a different operation on the two mounts"
        );
        let loopback_json: serde_json::Value = serde_json::from_slice(loopback_row)?;
        let tcp_json: serde_json::Value = serde_json::from_slice(tcp_row)?;
        compare_row_structure(
            &format!("row[{index}]"),
            &loopback_json,
            &tcp_json,
            &mut report,
        )?;
    }

    // The exemption machinery must have been EXERCISED, or the row comparison
    // above proves nothing about it.
    assert!(
        report
            .exempt_paths
            .iter()
            .any(|path| path.ends_with("/attach_secret")),
        "no attach-secret exemption fired, so the exempt path was never taken and the \
         rows above may not contain the nondeterminism this test claims to handle"
    );
    assert!(
        report
            .exempt_paths
            .iter()
            .any(|path| path.ends_with("/receipt_expires_at")),
        "no wall-clock exemption fired"
    );

    // ---- the canonical shell event bytes ----
    //
    // Pulled out of the rows by the walk and proven at the BYTE level, which is
    // stronger than the element-wise compare the walk would have done: the
    // loopback's own secrets are substituted for the socket's and the images
    // must then be identical for their whole length, so any byte that is not one
    // of those named secrets survives and fails.
    assert!(
        report.events.len() >= 4,
        "only {} canonical shell events were compared; the scenario mints one per \
         lifecycle operation, so a low count means the walk did not reach them",
        report.events.len()
    );
    for (path, loopback_event, tcp_event) in &report.events {
        assert_parity_after_substitution(
            &format!("canonical shell event bytes at {path}"),
            loopback_event,
            tcp_event,
            &pairs,
        );
        // Decoded as well as raw: the canonical envelope must name the same
        // conversation and the same ordinal on both mounts, so a hypothetical
        // encoder that produced equal bytes for unequal events could not pass.
        let loopback_decoded = ConversationEvent::decode_canonical(loopback_event)
            .map_err(|error| format!("loopback shell event did not decode: {error:?}"))?;
        let tcp_decoded = ConversationEvent::decode_canonical(tcp_event)
            .map_err(|error| format!("socket shell event did not decode: {error:?}"))?;
        assert_eq!(
            loopback_decoded.conversation_id(),
            tcp_decoded.conversation_id(),
            "the shell event at {path} names a different conversation on the two mounts"
        );
        assert_eq!(
            loopback_decoded.ordinal(),
            tcp_decoded.ordinal(),
            "the shell event at {path} carries a different ordinal on the two mounts"
        );
    }

    // ---- the response frames ----
    assert_eq!(
        loopback.responses.len(),
        tcp.responses.len(),
        "the two mounts answered with different numbers of response frames"
    );
    assert_eq!(
        loopback.responses.len(),
        8,
        "the scenario expects exactly one response per request: two enrollments, \
         detach, attach, two records, ack, detach"
    );
    for (index, (loopback_frame, tcp_frame)) in loopback
        .responses
        .iter()
        .zip(tcp.responses.iter())
        .enumerate()
    {
        assert_parity_after_substitution(
            &format!("response frame {index}"),
            loopback_frame,
            tcp_frame,
            &pairs,
        );
    }

    // ---- the unsolicited pushes ----
    assert_push_parity(&loopback.pushes, &tcp.pushes, &pairs);

    // The exempt list is part of the verdict, not a footnote: a future change
    // that needs another exemption has to come here and say so.
    assert_eq!(
        EXEMPT_FIELDS.len(),
        3,
        "the exempted-field list changed without the parity assertions changing with it"
    );
    Ok(())
}

/// Asserts the two mounts pushed the same DELIVERIES, by bytes.
///
/// **Why this one compares content rather than arrival positions, stated
/// openly.** Participant publication is at-least-once and server-driven: the
/// position of an unsolicited push in the interleave, and the number of times an
/// unacked obligation is re-offered, are decided by when the connection's
/// publication scan runs, not by which door admitted it. Measured over repeated
/// runs of this very scenario, BOTH mounts produced the delivery-sequence-2 push
/// at two different interleave positions and BOTH produced it twice on some
/// runs — the loopback on one run, the socket on another. So an assertion on
/// push COUNT or push POSITION would not be measuring the mount; it would be
/// measuring the scheduler, and would be flaky on either door. This is design
/// §7's named timing item, and it is recorded in the exempt list rather than
/// absorbed.
///
/// What IS asserted, and is exact: the deduplicated sequence of push images —
/// first-arrival order preserved — must be byte-identical across the two mounts,
/// and every repeat must be byte-identical to its first arrival, so a re-offer
/// can never smuggle in a different record.
fn assert_push_parity(loopback: &[Vec<u8>], tcp: &[Vec<u8>], pairs: &[ExemptPair]) {
    fn distinct(pushes: &[Vec<u8>]) -> Vec<Vec<u8>> {
        let mut out: Vec<Vec<u8>> = Vec::new();
        for push in pushes {
            if !out.iter().any(|seen| seen == push) {
                out.push(push.clone());
            }
        }
        out
    }

    let loopback_distinct = distinct(loopback);
    let tcp_distinct = distinct(tcp);
    assert_eq!(
        loopback_distinct.len(),
        1,
        "the loopback drive received {} distinct pushes; the scenario delivers exactly \
         one (the peer's arrival at delivery sequence 2), so anything else means the \
         two drives did not run the same scenario",
        loopback_distinct.len()
    );
    assert_eq!(
        tcp_distinct.len(),
        1,
        "the socket drive received {} distinct pushes; the scenario delivers exactly one",
        tcp_distinct.len()
    );
    for (index, (loopback_push, tcp_push)) in loopback_distinct
        .iter()
        .zip(tcp_distinct.iter())
        .enumerate()
    {
        assert_parity_after_substitution(
            &format!("distinct push {index}"),
            loopback_push,
            tcp_push,
            pairs,
        );
    }
}

/// A control for the assertion above: the substitution is a real discriminator,
/// not a rewrite that makes any two images match.
///
/// Without this, `assert_parity_after_substitution` passing would be consistent
/// with a substitution so aggressive it erased genuine differences.
#[test]
fn the_parity_substitution_does_not_absorb_an_unrelated_difference() {
    let left = [1_u8, 2, 3, 4, 5];
    let mut right = left;
    right[3] = 0xFF;
    let pairs: [ExemptPair; 1] = [([0xAA_u8; 32].to_vec(), [0xBB_u8; 32].to_vec())];
    let (substituted, replacements) = substitute(&left, &pairs);
    assert_eq!(replacements, 0, "no exempt value occurs in this image");
    assert_ne!(
        substituted,
        right.to_vec(),
        "substitution must leave an unrelated differing byte differing"
    );

    // And the positive half: an image that differs ONLY by an exempt value is
    // reconciled, so the discriminator is proven in both directions.
    let mut exempt_left = vec![9_u8, 9];
    exempt_left.extend_from_slice(&[0xAA_u8; 32]);
    exempt_left.push(7);
    let mut exempt_right = vec![9_u8, 9];
    exempt_right.extend_from_slice(&[0xBB_u8; 32]);
    exempt_right.push(7);
    let (reconciled, replaced) = substitute(&exempt_left, &pairs);
    assert_eq!(replaced, 1, "the exempt value occurs exactly once");
    assert_eq!(
        reconciled, exempt_right,
        "substituting the exempt value must reconcile the two images exactly"
    );
}

/// Keeps [`AttachSecret`]'s width and the substitution width in agreement.
///
/// The substitution above works on fixed 32-byte windows. If the protocol's
/// attach secret ever changed width, every substitution would silently stop
/// matching and the parity test would start failing for a reason that looked
/// like a divergence.
#[test]
fn the_exempt_attach_secret_is_thirty_two_bytes_wide() {
    let secret = AttachSecret::new([0x11; 32]);
    assert_eq!(
        secret.into_bytes().len(),
        32,
        "the substitution window must match the protocol's attach-secret width"
    );
}
