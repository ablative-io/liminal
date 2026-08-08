//! P0 MEASUREMENT lane: does a WebSocket subscriber stop receiving channel
//! deliveries when its connection has been idle (parked) since subscribe?
//!
//! The field failure (manifold estate, liminal 596b894 embedded): browser
//! WebSocket subscribers receive ZERO deliveries while TCP subscribers on the
//! same channels receive everything. The ws clients CAN publish and DO get
//! frame responses, so the socket is alive in both directions — only
//! channel-actor deliveries never arrive. The failure is boot-dependent: the
//! same binary delivered on one boot and starved on the next, so this harness
//! builds a FRESH server (fresh supervisor, fresh scheduler, fresh listeners,
//! fresh connections) per iteration and repeats.
//!
//! Three arms, each iterated:
//!
//! * A (control) — the existing pin's shape: subscribe both transports, publish
//!   IMMEDIATELY. The connection has almost certainly never parked.
//! * B (park window) — subscribe both transports, idle 5s so the WebSocket
//!   connection process reaches `NativeOutcome::Wait`, then publish from a
//!   third connection.
//! * C (ask shape) — the closest analogue of the field: ONE raw WebSocket
//!   connection subscribes, idles, then PUBLISHES on itself (the browser's
//!   re-ask, which forces an inbound-triggered slice), and a second connection
//!   then publishes records the ws connection must receive.
//!
//! This file REPRODUCES; it does not fix. Every arm reports per-iteration
//! outcomes and fails with the full tally so a flaky arm is distinguishable
//! from a deterministic one.

use std::error::Error;
use std::io::ErrorKind;
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use liminal::protocol::{
    CausalContext, Frame, MessageEnvelope, ProtocolVersion, SchemaId, decode, encode, encoded_len,
};
use liminal_sdk::remote::websocket::WebSocketSubscriptionStream;
use liminal_sdk::{
    ChannelHandle, ConnectionPoolConfig, RemoteChannelHandle, RemoteConfig, SchemaMetadata,
    SchemaValidate, SubscriptionStream,
};
use liminal_server::config::{
    ChannelDef, LimitsConfig, ServerConfig, ServicesConfig, WebSocketConfig,
};
use liminal_server::server::connection::{ConnectionSupervisor, WebSocketListener};
use liminal_server::server::listener::ServerListener;

use serde::{Deserialize, Serialize};
use tungstenite::Message;
use tungstenite::protocol::WebSocket;

const CHANNEL: &str = "events";
const PATH: &str = "/liminal";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RECV_TIMEOUT: Duration = Duration::from_secs(10);
/// Idle window for arm B: long enough that the WebSocket connection process has
/// drained every source and selected `Wait` well before the publish lands.
const PARK_WINDOW: Duration = Duration::from_secs(5);
/// Idle window for arm C before the ws client's own "re-ask" publish.
const ASK_IDLE: Duration = Duration::from_secs(3);
/// Client-side subscribe stream id for the raw ws client (arm C).
const RAW_SUBSCRIBE_STREAM: u32 = 3;
/// Client-side publish stream id for the raw ws client (arm C).
const RAW_PUBLISH_STREAM: u32 = 7;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
struct OrderPlaced {
    id: u64,
}

impl SchemaValidate for OrderPlaced {
    fn schema_metadata() -> SchemaMetadata {
        SchemaMetadata::new("orders.placed", "1", br#"{"type":"object"}"#.as_slice())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransportKind {
    Tcp,
    Ws,
}

/// Iteration count for an arm, overridable so a slow arm can be driven hard
/// without changing the committed default.
fn iterations(default: usize) -> usize {
    std::env::var("LIMINAL_WS_REPRO_ITERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

/// Holds both live listeners plus the supervisor, so a dropped fixture releases
/// its scheduler threads instead of leaking them across iterations.
struct RunningServer {
    _tcp: ServerListener,
    _ws: WebSocketListener,
    supervisor: ConnectionSupervisor,
    tcp_addr: SocketAddr,
    ws_addr: SocketAddr,
}

impl RunningServer {
    fn start() -> Result<Self, Box<dyn Error>> {
        let health = std::net::TcpListener::bind("127.0.0.1:0")?;
        let health_listen_address = health.local_addr()?;
        drop(health);
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
            limits: LimitsConfig::default(),
            websocket: None,
            participant: None,
        };
        let supervisor = ConnectionSupervisor::from_config(&config)?;
        let tcp = ServerListener::bind(&config, supervisor.clone())?;
        let ws_config = WebSocketConfig {
            listen_address: "127.0.0.1:0".parse()?,
            path: PATH.to_owned(),
            allowed_origins: Vec::new(),
            // The field estate runs with NO keepalive timer, so the connection
            // has no periodic self-wake: pinned here so the harness cannot
            // accidentally supply the wake the field lacks.
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

    fn address(&self, kind: TransportKind) -> String {
        match kind {
            TransportKind::Tcp => self.tcp_addr.to_string(),
            TransportKind::Ws => format!("ws://{}{PATH}", self.ws_addr),
        }
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

/// Connects the selected SDK transport, retrying while the listener warms up.
fn connect(
    server: &RunningServer,
    kind: TransportKind,
) -> Result<RemoteConfig, Box<dyn Error>> {
    let deadline = Instant::now() + CONNECT_TIMEOUT;
    let mut last_error = None;
    while Instant::now() < deadline {
        let config = RemoteConfig::new(
            server.address(kind),
            CHANNEL,
            "repro.conversation".to_owned(),
            ConnectionPoolConfig::new(1, 10, 16),
        )?;
        let attempt = match kind {
            TransportKind::Tcp => config.connect_tcp(),
            TransportKind::Ws => config.connect_websocket(),
        };
        match attempt {
            Ok(connected) => return Ok(connected),
            Err(error) => {
                last_error = Some(error);
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }
    Err(last_error.map_or_else(
        || "client never connected within timeout".into(),
        |error| format!("client never connected within timeout: {error}").into(),
    ))
}

/// Opens a TCP subscription, retrying while the listener warms up.
fn open_tcp_subscription(server: &RunningServer) -> Result<SubscriptionStream, Box<dyn Error>> {
    let deadline = Instant::now() + CONNECT_TIMEOUT;
    let mut last_error = None;
    while Instant::now() < deadline {
        match SubscriptionStream::open(&server.address(TransportKind::Tcp), CHANNEL, Vec::new()) {
            Ok(stream) => return Ok(stream),
            Err(error) => {
                last_error = Some(error);
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }
    Err(format!("tcp subscription never opened: {last_error:?}").into())
}

/// Opens a WebSocket subscription, retrying while the acceptor warms up.
fn open_ws_subscription(
    server: &RunningServer,
) -> Result<WebSocketSubscriptionStream, Box<dyn Error>> {
    let deadline = Instant::now() + CONNECT_TIMEOUT;
    let mut last_error = None;
    while Instant::now() < deadline {
        match WebSocketSubscriptionStream::open(
            &server.address(TransportKind::Ws),
            CHANNEL,
            Vec::new(),
        ) {
            Ok(stream) => return Ok(stream),
            Err(error) => {
                last_error = Some(error);
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }
    Err(format!("websocket subscription never opened: {last_error:?}").into())
}

fn publish_over_tcp(server: &RunningServer, id: u64) -> Result<(), Box<dyn Error>> {
    let config = connect(server, TransportKind::Tcp)?;
    let handle = RemoteChannelHandle::new(&config)?;
    handle.publish(OrderPlaced { id })?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Raw WebSocket client (arm C): one connection that subscribes AND publishes,
// which is the browser's shape and which no SDK type offers.
// ---------------------------------------------------------------------------

struct RawWsClient {
    socket: WebSocket<TcpStream>,
    subscription_id: u64,
}

impl RawWsClient {
    /// Performs the RFC 6455 upgrade, the liminal `Connect`, and a `Subscribe`.
    fn open(server: &RunningServer) -> Result<Self, Box<dyn Error>> {
        let deadline = Instant::now() + CONNECT_TIMEOUT;
        let mut last: Option<String> = None;
        while Instant::now() < deadline {
            match Self::try_open(server) {
                Ok(client) => return Ok(client),
                Err(error) => {
                    last = Some(error.to_string());
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
        }
        Err(format!("raw websocket client never opened: {last:?}").into())
    }

    fn try_open(server: &RunningServer) -> Result<Self, Box<dyn Error>> {
        let stream = TcpStream::connect(server.ws_addr)?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        let (mut socket, _response) = tungstenite::client::client(server.ws_url(), stream)?;

        send_frame(
            &mut socket,
            &Frame::Connect {
                flags: 0,
                min_version: ProtocolVersion::new(1, 0),
                max_version: ProtocolVersion::new(1, 0),
                auth_token: Vec::new(),
            },
        )?;
        match read_frame(&mut socket, Duration::from_secs(5))? {
            Some(Frame::ConnectAck { .. }) => {}
            other => return Err(format!("expected ConnectAck, got {other:?}").into()),
        }

        send_frame(
            &mut socket,
            &Frame::Subscribe {
                flags: 0,
                stream_id: RAW_SUBSCRIBE_STREAM,
                channel: CHANNEL.to_owned(),
                accepted_schemas: Vec::new(),
                max_in_flight: 16,
            },
        )?;
        let subscription_id = match read_frame(&mut socket, Duration::from_secs(5))? {
            Some(Frame::SubscribeAck {
                subscription_id, ..
            }) => subscription_id,
            other => return Err(format!("expected SubscribeAck, got {other:?}").into()),
        };
        Ok(Self {
            socket,
            subscription_id,
        })
    }

    fn publish(&mut self, payload: &[u8]) -> Result<(), Box<dyn Error>> {
        send_frame(
            &mut self.socket,
            &Frame::Publish {
                flags: 0,
                stream_id: RAW_PUBLISH_STREAM,
                channel: CHANNEL.to_owned(),
                envelope: MessageEnvelope::new(
                    SchemaId::new([7_u8; SchemaId::WIRE_LEN]),
                    CausalContext::independent(),
                    payload.to_vec(),
                ),
                idempotency_key: None,
            },
        )?;
        Ok(())
    }

    /// Collects delivered payloads until `deadline`, returning every non-Deliver
    /// frame seen alongside them so a refusal is never silently read as silence.
    fn collect_until(&mut self, deadline: Instant) -> (Vec<Vec<u8>>, Vec<String>) {
        let mut payloads = Vec::new();
        let mut others = Vec::new();
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match read_frame(&mut self.socket, remaining.max(Duration::from_millis(50))) {
                Ok(Some(Frame::Deliver { envelope, .. })) => payloads.push(envelope.payload),
                Ok(Some(other)) => others.push(format!("{other:?}")),
                Ok(None) => {}
                Err(error) => {
                    others.push(format!("READ ERROR: {error}"));
                    break;
                }
            }
        }
        (payloads, others)
    }
}

fn send_frame(socket: &mut WebSocket<TcpStream>, frame: &Frame) -> Result<(), Box<dyn Error>> {
    let mut bytes = vec![0_u8; encoded_len(frame)?];
    let written = encode(frame, &mut bytes)?;
    bytes.truncate(written);
    socket.send(Message::Binary(bytes.into()))?;
    Ok(())
}

/// Reads one liminal frame, returning `Ok(None)` when the read window elapsed
/// with no complete message (a timeout is not an error here — silence is the
/// measurement).
fn read_frame(
    socket: &mut WebSocket<TcpStream>,
    window: Duration,
) -> Result<Option<Frame>, Box<dyn Error>> {
    socket.get_ref().set_read_timeout(Some(window))?;
    loop {
        match socket.read() {
            Ok(Message::Binary(bytes)) => {
                let (frame, _consumed) = decode(&bytes)?;
                return Ok(Some(frame));
            }
            Ok(Message::Ping(_) | Message::Pong(_)) => continue,
            Ok(other) => return Err(format!("unexpected websocket message: {other:?}").into()),
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted
                ) =>
            {
                return Ok(None);
            }
            Err(error) => return Err(Box::new(error)),
        }
    }
}

// ---------------------------------------------------------------------------
// Arm A: control — immediate publish after subscribe (the existing pin's shape).
// ---------------------------------------------------------------------------

fn arm_a_once() -> Result<(bool, bool, String), Box<dyn Error>> {
    let server = RunningServer::start()?;
    let tcp_stream = open_tcp_subscription(&server)?;
    let ws_stream = open_ws_subscription(&server)?;
    publish_over_tcp(&server, 9)?;

    let tcp_got = tcp_stream.recv_timeout(RECV_TIMEOUT);
    let ws_got = ws_stream.recv_timeout(RECV_TIMEOUT);
    let detail = format!("tcp={tcp_got:?} ws={ws_got:?}");
    Ok((tcp_got.is_ok(), ws_got.is_ok(), detail))
}

#[test]
fn arm_a_immediate_publish_reaches_both_transports() -> Result<(), Box<dyn Error>> {
    let iterations = iterations(10);
    let mut failures = Vec::new();
    for index in 0..iterations {
        let (tcp_ok, ws_ok, detail) = arm_a_once()?;
        eprintln!("ARM A iteration {index}: tcp_ok={tcp_ok} ws_ok={ws_ok} :: {detail}");
        if !tcp_ok || !ws_ok {
            failures.push(format!("iteration {index}: tcp_ok={tcp_ok} ws_ok={ws_ok} :: {detail}"));
        }
    }
    assert!(
        failures.is_empty(),
        "ARM A (control, immediate publish) failed {}/{iterations} iterations:\n{}",
        failures.len(),
        failures.join("\n")
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Arm B: park window — subscribe, idle, then publish from a third connection.
// ---------------------------------------------------------------------------

fn arm_b_once() -> Result<(bool, bool, String), Box<dyn Error>> {
    let server = RunningServer::start()?;
    let tcp_stream = open_tcp_subscription(&server)?;
    let ws_stream = open_ws_subscription(&server)?;

    // Let both connection processes drain every source and park.
    std::thread::sleep(PARK_WINDOW);

    publish_over_tcp(&server, 21)?;

    let tcp_got = tcp_stream.recv_timeout(RECV_TIMEOUT);
    let ws_got = ws_stream.recv_timeout(RECV_TIMEOUT);
    let detail = format!("tcp={tcp_got:?} ws={ws_got:?}");
    Ok((tcp_got.is_ok(), ws_got.is_ok(), detail))
}

#[test]
fn arm_b_publish_after_park_window_reaches_both_transports() -> Result<(), Box<dyn Error>> {
    let iterations = iterations(3);
    let mut failures = Vec::new();
    for index in 0..iterations {
        let (tcp_ok, ws_ok, detail) = arm_b_once()?;
        eprintln!("ARM B iteration {index}: tcp_ok={tcp_ok} ws_ok={ws_ok} :: {detail}");
        if !tcp_ok || !ws_ok {
            failures.push(format!("iteration {index}: tcp_ok={tcp_ok} ws_ok={ws_ok} :: {detail}"));
        }
    }
    assert!(
        failures.is_empty(),
        "ARM B (publish after a {PARK_WINDOW:?} park window) failed {}/{iterations} iterations:\n{}",
        failures.len(),
        failures.join("\n")
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Arm C: ask shape — one raw ws connection subscribes, idles, publishes on
// itself (forcing an inbound-triggered slice), then a second connection
// publishes three records the ws connection must receive.
// ---------------------------------------------------------------------------

fn arm_c_once() -> Result<(bool, String), Box<dyn Error>> {
    let server = RunningServer::start()?;
    let mut client = RawWsClient::open(&server)?;
    let _subscription_id = client.subscription_id;

    // Idle: the ws connection process parks with a live subscription.
    std::thread::sleep(ASK_IDLE);

    // The browser's re-ask: a publish on the ws connection ITSELF, which forces
    // an inbound-triggered slice on that connection. The channel validates JSON,
    // so the ask carries a real record (a refused ask would still force a slice,
    // but it would not be a delivery this arm can count).
    client.publish(br#"{"id":42}"#)?;
    std::thread::sleep(Duration::from_millis(100));

    // A second connection publishes the records the ws subscriber must receive.
    let expected: Vec<String> = (0..3_u64)
        .map(|index| format!("{{\"id\":{}}}", 100 + index))
        .collect();
    let config = connect(&server, TransportKind::Tcp)?;
    let handle = RemoteChannelHandle::new(&config)?;
    for index in 0..3_u64 {
        handle.publish(OrderPlaced { id: 100 + index })?;
    }

    let deadline = Instant::now() + RECV_TIMEOUT;
    let (payloads, others) = client.collect_until(deadline);
    let seen: Vec<String> = payloads
        .iter()
        .map(|bytes| String::from_utf8_lossy(bytes).to_string())
        .collect();
    let missing: Vec<&String> = expected
        .iter()
        .filter(|wanted| !seen.contains(wanted))
        .collect();
    let detail = format!(
        "late_missing={missing:?} own_ask_delivered={} seen={seen:?} other_frames={others:?}",
        seen.iter().any(|payload| payload == r#"{"id":42}"#)
    );
    Ok((missing.is_empty(), detail))
}

#[test]
fn arm_c_ask_shape_ws_connection_receives_later_publishes() -> Result<(), Box<dyn Error>> {
    let iterations = iterations(3);
    let mut failures = Vec::new();
    for index in 0..iterations {
        let (all_late_received, detail) = arm_c_once()?;
        eprintln!("ARM C iteration {index}: all_late_received={all_late_received} :: {detail}");
        if !all_late_received {
            failures.push(format!("iteration {index}: {detail}"));
        }
    }
    assert!(
        failures.is_empty(),
        "ARM C (ask shape) failed {}/{iterations} iterations:\n{}",
        failures.len(),
        failures.join("\n")
    );
    Ok(())
}
