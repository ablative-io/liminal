//! THE FIELD SHAPE (p0-63): a seat that boots by attach -> drain -> ready must
//! not spend a reply-owed deadline on the drain.
//!
//! # The regression this pins, exactly
//!
//! #61 made `receive_within(RESPONSE_DEADLINE = 60 s)` the semantics of every
//! read on the request/response connection. That is the 2026-08-10 outage's fix
//! and it is CORRECT for every read whose caller is owed a reply. It is wrong
//! for the participant PUMP: a consumer looping to collect whatever the server
//! pushes next is owed nothing, and silence there is a normal state.
//!
//! In the field a registry seat boots by attaching mailboxes, draining the
//! backlog, then declaring itself ready. Its quiet-stream drain read used to
//! return after one 5 s socket window; after #61 it blocked for 60 s and blew a
//! 30 s boot gate. This runs that sequence against a live server and holds the
//! drain to a budget a 30 s gate cannot notice.
//!
//! # Why all three mounts
//!
//! The pump door has to be answered honestly by every transport or the same
//! boot gate blows on one mount and not another — and a default implementation
//! that delegated to the blocking read would give exactly that, silently. TCP,
//! WebSocket, and the in-process loopback each get the full arm: the same
//! sequence, the same budget, the same reply-owed exchange afterwards proving
//! the bounded read left the connection usable.
//!
//! # What is deliberately NOT asserted
//!
//! That the drain finds nothing. The point is that it TERMINATES on silence
//! within a bounded time, whatever it did or did not collect first — so the
//! census reports how many frames each mount drained rather than pinning a
//! number that would make the gate a statement about server push timing.
//!
//! This lives in an integration test for the reason `tests/loopback_sdk_e2e.rs`
//! documents: `liminal-server` dev-depends on `liminal-sdk` and the SDK depends
//! back, so only an integration test sees ONE `EmbeddedServer` type.

use std::error::Error;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use liminal_protocol::wire::{
    ClientRequest, EnrollmentRequest, EnrollmentToken, RecordAdmission,
    RecordAdmissionAttemptToken, ServerValue,
};
use liminal_sdk::{
    ConnectionPoolConfig, PARTICIPANT_PUMP_WINDOW, ParticipantResumeStore, RemoteConfig,
    RemoteOperationRecordOutcome, RemoteParticipantHandle, RemoteParticipantInbound,
    RemoteParticipantSendOutcome, SdkError,
};
use liminal_server::config::types::ParticipantConfig;
use liminal_server::config::{LimitsConfig, ServerConfig, ServicesConfig, WebSocketConfig};
use liminal_server::server::connection::{
    ConnectionServices, ConnectionSupervisor, LiminalConnectionServices, WebSocketListener,
};
use liminal_server::server::embedded::EmbeddedServer;
use liminal_server::server::listener::ServerListener;

/// The one conversation these arms run in.
const PUMP_CONVERSATION: u64 = 0x_63_02;

/// WebSocket path the listener serves and the SDK dials.
const WS_PATH: &str = "/liminal";

/// The drain's budget.
///
/// The field gate is 30 s and the drain is only part of a boot, so this is a
/// third of it. It is also comfortably below `RESPONSE_DEADLINE` = 60 s: a
/// drain read that inherited the reply-owed deadline cannot pass this, which is
/// the whole discrimination.
const BOOT_BUDGET: Duration = Duration::from_secs(10);

/// Frames one drain will collect before it stops calling itself a drain.
const MAX_DRAIN_FRAMES: usize = 64;

/// Frame-count bound on a reply-owed demultiplex. A step that reaches it failed.
const MAX_DEMUX_FRAMES: usize = 64;

/// Deployment-shaped participant configuration. Every field is a deployment
/// owner's decision — the type carries no defaults on purpose — so a fixture
/// has to state all of them, exactly as a real `[participant]` section does.
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
#[derive(Debug, Default, Clone)]
struct PumpStore {
    canonical: Arc<Mutex<Vec<u8>>>,
}

impl ParticipantResumeStore for PumpStore {
    fn persist(&mut self, canonical_lpcr: &[u8]) -> Result<(), SdkError> {
        let mut guard = self.canonical.lock().map_err(|_| SdkError::Store {
            description: "the pump resume store lock was poisoned".to_owned(),
        })?;
        guard.clear();
        guard.extend_from_slice(canonical_lpcr);
        drop(guard);
        Ok(())
    }
}

type SdkParticipant = RemoteParticipantHandle<PumpStore>;

/// The three real mounts a participant connection can ride.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mount {
    /// Real sockets through the TCP listener.
    Tcp,
    /// Real sockets through the WebSocket listener, same supervisor.
    WebSocket,
    /// The in-memory duplex against a listenerless embedded server.
    Loopback,
}

impl Mount {
    const fn label(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::WebSocket => "websocket",
            Self::Loopback => "loopback",
        }
    }
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
        // The one field that activates participant framing, and it is
        // transport-agnostic: the supervisor installs the service, and both
        // listeners share the supervisor.
        participant: Some(participant_config()),
    })
}

const fn pool() -> ConnectionPoolConfig {
    ConnectionPoolConfig::new(1, 1, 8)
}

/// Whatever has to stay alive for the mount under test to keep serving.
///
/// One struct of optionals rather than a per-mount enum: the socket mounts hold
/// listeners plus the supervisor they share, the in-process mount holds only the
/// embedded server (whose teardown is `Drop`), and every field is simply absent
/// for the mounts that do not use it.
#[derive(Default)]
struct ServerHolder {
    tcp: Option<ServerListener>,
    ws: Option<WebSocketListener>,
    supervisor: Option<ConnectionSupervisor>,
    embedded: Option<Arc<EmbeddedServer>>,
}

impl ServerHolder {
    fn shutdown(mut self) -> Result<(), Box<dyn Error>> {
        if let Some(listener) = self.tcp.take() {
            listener.shutdown()?;
        }
        if let Some(listener) = self.ws.take() {
            listener.shutdown()?;
        }
        if let Some(supervisor) = self.supervisor.take() {
            supervisor.shutdown();
        }
        drop(self.embedded.take());
        Ok(())
    }
}

/// Starts the server for `mount` and returns it with a connected client config.
///
/// The config is returned alongside the holder because it owns the `Arc` the
/// participant handle clones: dropping only the handle would leave the
/// connection open.
fn open(mount: Mount, store_dir: &Path) -> Result<(ServerHolder, RemoteConfig), Box<dyn Error>> {
    // The haematite engine creates its directory exactly one level below a
    // pre-existing parent it can fence, so the fixture creates the parent as a
    // deployment's operator would.
    std::fs::create_dir_all(store_dir)?;
    let config = server_config(store_dir)?;

    match mount {
        Mount::Loopback => {
            let services = Arc::new(LiminalConnectionServices::from_config(&config)?);
            let server = Arc::new(EmbeddedServer::with_services(
                services as Arc<dyn ConnectionServices>,
            )?);
            let remote = RemoteConfig::new(
                "in-process",
                "quiet-pump",
                PUMP_CONVERSATION.to_string(),
                pool(),
            )?
            .connect_loopback(Arc::clone(&server))?;
            Ok((
                ServerHolder {
                    embedded: Some(server),
                    ..ServerHolder::default()
                },
                remote,
            ))
        }
        Mount::Tcp | Mount::WebSocket => {
            let supervisor = ConnectionSupervisor::from_config(&config)?;
            let tcp = ServerListener::bind(&config, supervisor.clone())?;
            let ws_config = WebSocketConfig {
                listen_address: "127.0.0.1:0".parse()?,
                path: WS_PATH.to_owned(),
                allowed_origins: Vec::new(),
                ping_interval_ms: None,
            };
            let ws = WebSocketListener::bind(&ws_config, supervisor.clone())?;
            let address = match mount {
                Mount::WebSocket => format!("ws://{}{WS_PATH}", ws.local_addr()),
                Mount::Tcp | Mount::Loopback => tcp.local_addr().to_string(),
            };
            let remote =
                RemoteConfig::new(address, "quiet-pump", PUMP_CONVERSATION.to_string(), pool())?;
            let remote = match mount {
                Mount::WebSocket => remote.connect_websocket()?,
                Mount::Tcp | Mount::Loopback => remote.connect_tcp()?,
            };
            Ok((
                ServerHolder {
                    tcp: Some(tcp),
                    ws: Some(ws),
                    supervisor: Some(supervisor),
                    embedded: None,
                },
                remote,
            ))
        }
    }
}

/// Sends one request and reads until its correlated response arrives, through
/// the REPLY-OWED door. Unchanged from every other participant e2e: this is the
/// path #61 protects and this lane must not touch.
fn exchange(
    participant: &SdkParticipant,
    request: ClientRequest,
) -> Result<ServerValue, Box<dyn Error>> {
    let operation = match participant.record_operation(request)? {
        RemoteOperationRecordOutcome::Recorded(operation)
        | RemoteOperationRecordOutcome::Continuous(operation) => operation,
        RemoteOperationRecordOutcome::Refused { request, reason } => {
            return Err(format!("SDK refused outbound request {request:?}: {reason:?}").into());
        }
    };
    match participant.send_operation(operation)? {
        RemoteParticipantSendOutcome::Sent { .. } => {}
        RemoteParticipantSendOutcome::TransportLost { error, .. } => {
            return Err(format!("SDK transport lost while sending: {error}").into());
        }
    }
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

/// What one mount's drain actually did, reported rather than asserted.
#[derive(Debug)]
struct DrainCensus {
    /// Frames the drain collected before silence.
    drained: usize,
    /// Wall-clock time the whole drain loop took.
    elapsed: Duration,
}

/// Drains the connection through the PUMP door until it reports silence.
///
/// This is manifold's registry-seat drain reduced to a recipe: loop
/// [`RemoteParticipantHandle::try_receive`] until it answers `Ok(None)`. A
/// transport error is a real failure here and is propagated — the point of the
/// typed silence is that a quiet connection never has to be reported as one.
fn drain_until_quiet(participant: &SdkParticipant) -> Result<DrainCensus, Box<dyn Error>> {
    let started = Instant::now();
    let mut drained = 0_usize;
    while drained < MAX_DRAIN_FRAMES {
        match participant.try_receive()? {
            None => {
                return Ok(DrainCensus {
                    drained,
                    elapsed: started.elapsed(),
                });
            }
            Some(RemoteParticipantInbound::Push { .. }) => drained += 1,
            Some(other) => {
                return Err(format!(
                    "the drain read a correlated frame it never asked for: {other:?}"
                )
                .into());
            }
        }
    }
    Err(format!("the drain never reached silence within {MAX_DRAIN_FRAMES} frames").into())
}

/// One mount's full arm: attach, drain under the boot budget, declare ready,
/// then prove the connection still carries a reply-owed exchange.
fn boot_a_seat(mount: Mount) -> Result<(), Box<dyn Error>> {
    let store_dir = tempfile::tempdir()?;
    let (holder, config) = open(mount, &store_dir.path().join("pump-store"))?;
    let participant = RemoteParticipantHandle::new(&config, PumpStore::default())?;

    // Step 1 — attach mailboxes. A genuine reply-owed exchange through the 60 s
    // door, which this lane leaves exactly as #61 left it.
    let enrolled = exchange(
        &participant,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: PUMP_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x63; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(bound) = enrolled else {
        return Err(format!("[{}] enrollment did not bind: {enrolled:?}", mount.label()).into());
    };

    // Step 2 — drain the backlog. Nothing is owed and the connection is quiet.
    // THIS IS THE MEASUREMENT.
    let census = drain_until_quiet(&participant)
        .map_err(|error| format!("[{}] the drain failed: {error}", mount.label()))?;

    // Step 3 — declare ready, within the gate.
    println!(
        "[{}] drained {} frames, quiet after {:.3}s",
        mount.label(),
        census.drained,
        census.elapsed.as_secs_f64()
    );
    assert!(
        census.elapsed < BOOT_BUDGET,
        "[{}] the quiet drain took {:.3}s against a {:.3}s boot budget — a pump read is \
         waiting out the reply-owed response deadline",
        mount.label(),
        census.elapsed.as_secs_f64(),
        BOOT_BUDGET.as_secs_f64()
    );

    // The bounded read left the connection usable: a reply-owed exchange still
    // completes on it. A pump read that failed to restore the steady-state read
    // window, or that consumed a frame it reported as silence, fails here.
    let committed = exchange(
        &participant,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: PUMP_CONVERSATION,
            participant_id: bound.participant_id(),
            capability_generation: bound.capability_generation(),
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([0x64; 16]),
            payload: vec![0x63, 0x02, 0xA5, 0x5A],
        }),
    )
    .map_err(|error| {
        format!(
            "[{}] the connection did not survive the bounded drain: {error}",
            mount.label()
        )
    })?;
    assert!(
        matches!(committed, ServerValue::RecordCommitted(_)),
        "[{}] the post-drain record did not commit: {committed:?}",
        mount.label()
    );

    drop(participant);
    drop(config);
    holder.shutdown()?;
    Ok(())
}

/// THE FIELD SHAPE over real sockets.
#[test]
fn a_seat_boots_within_its_gate_after_a_quiet_tcp_drain() -> Result<(), Box<dyn Error>> {
    boot_a_seat(Mount::Tcp)
}

/// THE FIELD SHAPE over the WebSocket mount — parity, for the reason #61's own
/// WS arm exists: a fix that lands on one transport and not the other leaves
/// the same outage live on half the deployments.
#[test]
fn a_seat_boots_within_its_gate_after_a_quiet_websocket_drain() -> Result<(), Box<dyn Error>> {
    boot_a_seat(Mount::WebSocket)
}

/// THE FIELD SHAPE over the in-process mount. An embedded consumer has no
/// socket at all, and the in-process design forbids it having its own
/// semantics: its drain must end on the same silence within the same budget.
#[test]
fn a_seat_boots_within_its_gate_after_a_quiet_loopback_drain() -> Result<(), Box<dyn Error>> {
    boot_a_seat(Mount::Loopback)
}

/// The pump window the drain loop rides must be shorter than the boot budget it
/// has to fit inside, or the arms above would be passing on a coincidence.
///
/// This is the arms' own precondition made explicit: `try_receive` waits one
/// [`PARTICIPANT_PUMP_WINDOW`] before reporting silence, so a drain that
/// collects nothing costs exactly that.
#[test]
fn the_pump_window_fits_inside_the_boot_budget() {
    assert!(
        PARTICIPANT_PUMP_WINDOW < BOOT_BUDGET,
        "the pump window ({PARTICIPANT_PUMP_WINDOW:?}) does not fit inside the boot budget \
         ({BOOT_BUDGET:?}), so a single quiet drain read cannot meet the gate"
    );
}
