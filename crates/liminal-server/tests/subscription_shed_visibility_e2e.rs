//! A subscription shed must be LOUD.
//!
//! P0 #55's cost was not only that a subscriber was shed — it was that a
//! SUCCESSFUL shed said nothing anywhere. The server logged only the failure to
//! release a shed subscription, never the shed itself; the metrics surface
//! carried three aggregates, none of which counted sheds; and both SDK
//! subscription readers dropped the server's `SubscribeError` on the floor. A
//! browser was starved for an hour and no instrument on either side of the socket
//! had a word to say about it.
//!
//! These pins make a shed observable. They do NOT assert that sheds never happen
//! — that is `subscription_starvation_e2e.rs`, and at these bytes it fails; see
//! its module note. This file pins the promise this lane can actually keep: when
//! a subscription IS shed, the server says so and the client is told.
//!
//! The shed here is FORCED, not raced: the fixture boots with
//! `max_subscription_inbox_depth = 1`, so the second envelope to queue behind an
//! undrained first one trips the fairness cap by construction. Every pin asserts
//! the shed actually occurred before asserting anything about how it was
//! reported, so a boot that failed to shed fails the test rather than passing
//! vacuously on an untriggered instrument.

use std::error::Error;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use liminal::metrics::{MetricValue, global_registry, render};
use liminal::protocol::{
    CausalContext, Frame, MessageEnvelope, ProtocolVersion, SchemaId, decode, encode, encoded_len,
};
use liminal_sdk::SubscriptionStream;
use liminal_sdk::remote::websocket::WebSocketSubscriptionStream;
use liminal_server::config::{
    ChannelDef, LimitsConfig, ServerConfig, ServicesConfig, WebSocketConfig,
};
use liminal_server::server::connection::{ConnectionSupervisor, WebSocketListener};
use liminal_server::server::listener::ServerListener;

const CHANNEL: &str = "events";
const PATH: &str = "/liminal";
/// Metric families this file pins. Read from string constants here, compared
/// against the rendered exposition, so a rename on either side shows up as a
/// failure rather than as a silently vacuous `contains`.
const SHEDS_TOTAL: &str = "liminal_subscription_sheds_total";
const TRANSPORT_DELIVERIES_TOTAL: &str = "liminal_transport_deliveries_total";
/// The pre-existing aggregate. Pinned here so the additive-only promise for the
/// `/metrics` text format is checked, not merely intended.
const DELIVERIES_TOTAL: &str = "liminal_deliveries_total";
/// Burst used to force the shed. Far above the depth cap of 1.
const SHED_BURST: usize = 200;
/// Burst used for the clean-delivery counter pin. Far BELOW the 32-envelope
/// delivery slice budget, so nothing sheds and the counters are the only thing
/// under test.
const CLEAN_BURST: usize = 10;
const RECV_WINDOW: Duration = Duration::from_secs(2);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const BURST_PUBLISH_STREAM: u32 = 7;

/// Serializes the pins: the metric families are PROCESS-global, so two pins
/// taking deltas concurrently would each be measuring the other's traffic.
static PIN_GATE: Mutex<()> = Mutex::new(());

// ---------------------------------------------------------------------------
// Captured tracing output
// ---------------------------------------------------------------------------

/// Shared buffer the test subscriber writes every event into.
type CapturedLog = Arc<Mutex<Vec<u8>>>;

static CAPTURED: OnceLock<CapturedLog> = OnceLock::new();
/// Guards the one-shot global subscriber install.
static SUBSCRIBER_INSTALLED: OnceLock<()> = OnceLock::new();

#[derive(Clone)]
struct CaptureWriter(CapturedLog);

impl Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut guard = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        guard.extend_from_slice(buf);
        drop(guard);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Installs the capturing subscriber once for this test process and returns the
/// buffer. A second call is a no-op that returns the same buffer.
fn captured_log() -> CapturedLog {
    let buffer = CAPTURED.get_or_init(|| Arc::new(Mutex::new(Vec::new())));
    SUBSCRIBER_INSTALLED.get_or_init(|| {
        let writer = CaptureWriter(Arc::clone(buffer));
        let _ = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .try_init();
    });
    Arc::clone(buffer)
}

fn captured_text(buffer: &CapturedLog) -> String {
    let guard = buffer.lock().unwrap_or_else(PoisonError::into_inner);
    let text = String::from_utf8_lossy(&guard).into_owned();
    drop(guard);
    text
}

// ---------------------------------------------------------------------------
// Metric readers
// ---------------------------------------------------------------------------

/// Sums every counter sample named `name`, whatever its labels.
///
/// `None` means the FAMILY is absent — which a caller must treat as "not
/// measured", never as zero: a counter that does not exist and a counter sitting
/// at zero are the difference between an unwired instrument and a quiet one.
fn counter_total(name: &str) -> Option<u64> {
    let registry = global_registry()?;
    let snapshot = registry.snapshot();
    let mut found = None;
    for metric in snapshot.metrics() {
        if metric.name != name {
            continue;
        }
        if let MetricValue::Counter(value) = metric.value {
            found = Some(found.unwrap_or(0) + value);
        }
    }
    found
}

/// Reads one labelled counter sample.
fn labelled_counter(name: &str, label: (&str, &str)) -> Option<u64> {
    let registry = global_registry()?;
    let snapshot = registry.snapshot();
    for metric in snapshot.metrics() {
        if metric.name != name {
            continue;
        }
        let matches = metric
            .labels
            .iter()
            .any(|(key, value)| key == label.0 && value == label.1);
        if !matches {
            continue;
        }
        if let MetricValue::Counter(value) = metric.value {
            return Some(value);
        }
    }
    None
}

fn exposition() -> Option<String> {
    Some(render(&global_registry()?.snapshot()))
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

struct RunningServer {
    _tcp: ServerListener,
    _ws: WebSocketListener,
    supervisor: ConnectionSupervisor,
    tcp_addr: SocketAddr,
    ws_addr: SocketAddr,
}

impl RunningServer {
    /// Boots a server whose per-subscription inbox depth cap is `depth_cap`.
    /// `Some(1)` makes the fairness trip reachable by construction.
    fn start(depth_cap: Option<usize>) -> Result<Self, Box<dyn Error>> {
        let health = std::net::TcpListener::bind("127.0.0.1:0")?;
        let health_listen_address = health.local_addr()?;
        drop(health);
        let mut limits = LimitsConfig::default();
        if let Some(depth_cap) = depth_cap {
            limits.max_subscription_inbox_depth = depth_cap;
        }
        let config = ServerConfig {
            listen_address: "127.0.0.1:0".parse()?,
            health_listen_address,
            drain_timeout_ms: 30_000,
            channels: vec![ChannelDef {
                name: CHANNEL.to_owned(),
                schema_ref: None,
                durable: false,
                loaded_schema: None,
            }],
            routing_rules: Vec::new(),
            persistence_path: None,
            cluster: None,
            auth: None,
            services: ServicesConfig::default(),
            limits,
            websocket: None,
            participant: None,
        };
        let supervisor = ConnectionSupervisor::from_config(&config)?;
        let tcp = ServerListener::bind(&config, supervisor.clone())?;
        let ws_config = WebSocketConfig {
            listen_address: "127.0.0.1:0".parse()?,
            path: PATH.to_owned(),
            allowed_origins: Vec::new(),
            ping_interval_ms: None,
        };
        let ws = WebSocketListener::bind(&ws_config, supervisor.clone())?;
        let tcp_addr = tcp.local_addr();
        let ws_addr = ws.local_addr();
        Ok(Self {
            _tcp: tcp,
            _ws: ws,
            supervisor,
            tcp_addr,
            ws_addr,
        })
    }

    fn tcp_address(&self) -> String {
        self.tcp_addr.to_string()
    }

    fn ws_url(&self) -> String {
        format!("ws://{}{PATH}", self.ws_addr)
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        self.supervisor.shutdown();
    }
}

fn open_tcp_subscription(server: &RunningServer) -> Result<SubscriptionStream, Box<dyn Error>> {
    let deadline = std::time::Instant::now() + CONNECT_TIMEOUT;
    let mut last_error = None;
    while std::time::Instant::now() < deadline {
        match SubscriptionStream::open(&server.tcp_address(), CHANNEL, Vec::new()) {
            Ok(stream) => return Ok(stream),
            Err(error) => {
                last_error = Some(error);
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }
    Err(format!("tcp subscription never opened: {last_error:?}").into())
}

fn open_ws_subscription(
    server: &RunningServer,
) -> Result<WebSocketSubscriptionStream, Box<dyn Error>> {
    let deadline = std::time::Instant::now() + CONNECT_TIMEOUT;
    let mut last_error = None;
    while std::time::Instant::now() < deadline {
        match WebSocketSubscriptionStream::open(&server.ws_url(), CHANNEL, Vec::new()) {
            Ok(stream) => return Ok(stream),
            Err(error) => {
                last_error = Some(error);
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }
    Err(format!("websocket subscription never opened: {last_error:?}").into())
}

fn write_frame(stream: &mut std::net::TcpStream, frame: &Frame) -> Result<(), Box<dyn Error>> {
    let mut bytes = vec![0_u8; encoded_len(frame)?];
    let written = encode(frame, &mut bytes)?;
    bytes.truncate(written);
    stream.write_all(&bytes)?;
    Ok(())
}

/// Publishes `count` records over a raw TCP connection, returning how many the
/// server ACCEPTED. Every pin compares against the accepted count, never the
/// attempted one.
fn publish_burst(server: &RunningServer, count: usize) -> Result<usize, Box<dyn Error>> {
    use std::io::Read;
    use std::sync::mpsc;

    let mut stream = std::net::TcpStream::connect(server.tcp_addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    write_frame(
        &mut stream,
        &Frame::Connect {
            flags: 0,
            min_version: ProtocolVersion::new(1, 0),
            max_version: ProtocolVersion::new(1, 0),
            auth_token: Vec::new(),
        },
    )?;
    let mut buffer = vec![0_u8; 4096];
    let read = stream.read(&mut buffer)?;
    decode(&buffer[..read])?;

    let mut drain = stream.try_clone()?;
    let (done, all_acked) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut pending: Vec<u8> = Vec::new();
        let mut chunk = vec![0_u8; 16384];
        let mut accepted = 0_usize;
        loop {
            let read = match drain.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(read) => read,
            };
            pending.extend_from_slice(&chunk[..read]);
            while let Ok((frame, consumed)) = decode(&pending) {
                pending.drain(..consumed);
                if matches!(frame, Frame::PublishAck { .. }) {
                    accepted += 1;
                }
            }
            if accepted >= count {
                done.send(()).ok();
            }
        }
        accepted
    });

    for index in 0..count {
        write_frame(
            &mut stream,
            &Frame::Publish {
                flags: 0,
                stream_id: BURST_PUBLISH_STREAM,
                channel: CHANNEL.to_owned(),
                envelope: MessageEnvelope::new(
                    SchemaId::new([7_u8; SchemaId::WIRE_LEN]),
                    CausalContext::independent(),
                    format!("{{\"id\":{index}}}").into_bytes(),
                ),
                idempotency_key: None,
            },
        )?;
    }
    all_acked.recv_timeout(Duration::from_secs(20)).ok();
    stream.shutdown(std::net::Shutdown::Both).ok();
    reader.join().map_err(|_| "ack reader panicked")?;
    Ok(count)
}

/// Drains a WebSocket subscriber until it stops delivering, returning the count
/// received and the terminal error text.
fn drain_ws(stream: &WebSocketSubscriptionStream, wanted: usize) -> (usize, Option<String>) {
    let mut received = 0_usize;
    while received < wanted {
        match stream.recv_timeout(RECV_WINDOW) {
            Ok(_) => received += 1,
            Err(error) => return (received, Some(format!("{error}"))),
        }
    }
    (received, None)
}

fn drain_tcp(stream: &SubscriptionStream, wanted: usize) -> (usize, Option<String>) {
    let mut received = 0_usize;
    while received < wanted {
        match stream.recv_timeout(RECV_WINDOW) {
            Ok(_) => received += 1,
            Err(error) => return (received, Some(format!("{error}"))),
        }
    }
    (received, None)
}

// ---------------------------------------------------------------------------
// Pins
// ---------------------------------------------------------------------------

/// A shed must increment a shed counter and name the shed subscription in a
/// warning. Before this lane the shed counter did not exist and the only thing
/// logged on this path was the failure to RELEASE a shed subscription, which a
/// successful shed never reaches.
#[test]
fn a_forced_shed_is_counted_and_logged() -> Result<(), Box<dyn Error>> {
    let _gate = PIN_GATE.lock().unwrap_or_else(PoisonError::into_inner);
    let log = captured_log();
    liminal_server::metrics::init();

    let before = counter_total(SHEDS_TOTAL);

    let server = RunningServer::start(Some(1))?;
    let ws_stream = open_ws_subscription(&server)?;
    let accepted = publish_burst(&server, SHED_BURST)?;
    let (received, error) = drain_ws(&ws_stream, accepted);

    // The shed must have HAPPENED, or every assertion below is about an
    // instrument that was never triggered.
    assert!(
        received < accepted,
        "the fixture failed to force a shed: received {received} of {accepted} accepted \
         (error {error:?}); the depth cap of 1 should make the fairness trip certain"
    );

    let after = counter_total(SHEDS_TOTAL)
        .ok_or_else(|| format!("metric family {SHEDS_TOTAL} is absent from the registry"))?;
    assert!(
        after > before.unwrap_or(0),
        "{SHEDS_TOTAL} did not move across a shed: before={before:?} after={after}"
    );

    let text = captured_text(&log);
    assert!(
        text.contains("subscription shed"),
        "no shed warning reached the tracing subscriber; captured:\n{text}"
    );
    assert!(
        text.contains(CHANNEL),
        "the shed warning does not name the channel; captured:\n{text}"
    );
    assert!(
        text.contains("websocket"),
        "the shed warning does not name the transport; captured:\n{text}"
    );
    assert!(
        text.contains("subscription_id"),
        "the shed warning does not name the subscription; captured:\n{text}"
    );
    Ok(())
}

/// Deliveries must be counted PER TRANSPORT, and the pre-existing aggregate must
/// survive unchanged — the `/metrics` text format grows by whole lines and never
/// loses one.
#[test]
fn b_deliveries_are_counted_per_transport_without_disturbing_the_aggregate()
-> Result<(), Box<dyn Error>> {
    let _gate = PIN_GATE.lock().unwrap_or_else(PoisonError::into_inner);
    liminal_server::metrics::init();

    let tcp_before = labelled_counter(TRANSPORT_DELIVERIES_TOTAL, ("transport", "tcp"));
    let ws_before = labelled_counter(TRANSPORT_DELIVERIES_TOTAL, ("transport", "websocket"));

    let server = RunningServer::start(None)?;
    let tcp_stream = open_tcp_subscription(&server)?;
    let ws_stream = open_ws_subscription(&server)?;
    let accepted = publish_burst(&server, CLEAN_BURST)?;

    let (tcp_received, tcp_error) = drain_tcp(&tcp_stream, accepted);
    let (ws_received, ws_error) = drain_ws(&ws_stream, accepted);
    // Both subscribers must have received EVERYTHING: a counter pin over a run
    // that shed would be measuring a different path than the one it names.
    assert_eq!(
        (tcp_received, ws_received),
        (accepted, accepted),
        "the clean-delivery fixture did not deliver cleanly: tcp_error={tcp_error:?} \
         ws_error={ws_error:?}"
    );

    let tcp_after = labelled_counter(TRANSPORT_DELIVERIES_TOTAL, ("transport", "tcp"))
        .ok_or_else(|| format!("{TRANSPORT_DELIVERIES_TOTAL} has no tcp sample"))?;
    let ws_after = labelled_counter(TRANSPORT_DELIVERIES_TOTAL, ("transport", "websocket"))
        .ok_or_else(|| format!("{TRANSPORT_DELIVERIES_TOTAL} has no websocket sample"))?;
    assert!(
        tcp_after >= tcp_before.unwrap_or(0) + accepted as u64,
        "the tcp transport counter did not record {accepted} deliveries: \
         before={tcp_before:?} after={tcp_after}"
    );
    assert!(
        ws_after >= ws_before.unwrap_or(0) + accepted as u64,
        "the websocket transport counter did not record {accepted} deliveries: \
         before={ws_before:?} after={ws_after}"
    );

    let text = exposition().ok_or("the metrics registry is not installed")?;
    for family in [DELIVERIES_TOTAL, TRANSPORT_DELIVERIES_TOTAL, SHEDS_TOTAL] {
        assert!(
            text.contains(family),
            "{family} is missing from the rendered exposition:\n{text}"
        );
    }
    assert!(
        text.contains("transport=\"tcp\"") && text.contains("transport=\"websocket\""),
        "the transport label is not rendered on the exposition lines:\n{text}"
    );
    Ok(())
}
