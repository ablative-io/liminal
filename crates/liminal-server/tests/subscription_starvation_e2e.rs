//! P0 #55 pins: a subscription that falls behind a burst must never be starved
//! into a permanent shed.
//!
//! The defect these pin (measured at `4ed0562`): the subscription inbox fires its
//! wake notifier ONLY on the empty→non-empty edge
//! (`liminal/src/channel/subscription.rs`), while the delivery pump drains at most
//! `DELIVERY_SLICE_BUDGET` = 32 envelopes per slice
//! (`liminal-server/src/server/connection/delivery.rs`). A connection more than 32
//! envelopes behind therefore never empties its inbox, never earns another edge,
//! and is left to the self-requeue path — which re-enters far slower than a wake
//! under contention. The queue ratchets one way to the 256-envelope depth cap
//! (`liminal-server/src/config/types.rs`), the sticky overflow marker trips, and
//! the delivery pump SHEDS the subscription permanently: one `SubscribeError`
//! frame, then removal at the connection AND release at the channel actor. The
//! socket stays open and publishes keep acking, so the failure presents in the
//! field as a subscriber that has simply gone quiet forever.
//!
//! Both pins run several FRESH server boots. The pre-fix failure is a race and
//! shows up on roughly 60% of boots (measured: 6/10 on the repro harness's SDK
//! burst arm), so a single boot would be a coin flip; repeating over independent
//! boots is what makes the red reliable and the green meaningful.
//!
//! Two disciplines carried from the repro harness, both load-bearing:
//!
//! * The burst publisher counts `PublishAck`s and every subscriber is compared
//!   against what the server ACCEPTED, never against what was attempted — a
//!   publisher that died mid-burst must not read as a delivery defect.
//! * `accepted == burst` is asserted FIRST. Without it an iteration whose
//!   publisher got no acks at all scores `0 == 0` and passes vacuously.
//!
//! Diagnostics stay slice-level (counts and totals). Per-envelope probes perturb
//! the very race under measurement.

use std::error::Error;
use std::net::SocketAddr;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

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

/// Channel every arm publishes and subscribes on.
const CHANNEL: &str = "events";
/// WebSocket mount path.
const PATH: &str = "/liminal";
/// Envelopes per burst. Must comfortably exceed the 256-envelope inbox depth cap,
/// or the pre-fix ratchet never reaches the shed it is being pinned against.
const BURST: usize = 400;
/// Fresh server boots per pin. The pre-fix failure is per-boot and probabilistic;
/// this is what turns it into a reliable red.
const ITERATIONS: usize = 6;
/// Per-message receive window while draining a burst.
const RECV_WINDOW: Duration = Duration::from_secs(2);
/// Bound on the synchronous listener warm-up retry loop.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Client-side publish stream id for the raw TCP burst publisher.
const BURST_PUBLISH_STREAM: u32 = 7;

/// Serializes the pins in this binary against each other.
///
/// Both pins measure a scheduling race whose outcome depends on how promptly the
/// connection scheduler picks a woken connection back up. Two of them racing each
/// other inside one test binary would make each pin's verdict partly a statement
/// about the other, in BOTH directions — a pre-fix red that owed something to the
/// neighbour's load, and a post-fix green that had to survive it. Serializing here
/// keeps each verdict about the mechanism.
static PIN_GATE: Mutex<()> = Mutex::new(());

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
            // No keepalive timer: the field estate runs without one, so the
            // fixture must not hand the connection a periodic self-wake that the
            // field never supplies.
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

/// Opens a TCP subscription, retrying while the listener warms up.
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

/// Opens a WebSocket subscription, retrying while the acceptor warms up.
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
    use std::io::Write;

    let mut bytes = vec![0_u8; encoded_len(frame)?];
    let written = encode(frame, &mut bytes)?;
    bytes.truncate(written);
    stream.write_all(&bytes)?;
    Ok(())
}

/// Publishes `count` records to `CHANNEL` as fast as one raw TCP connection can
/// write them, returning how many the server ACCEPTED (`PublishAck`) plus any
/// other frames it answered with.
///
/// A concurrent ack reader is what keeps the publisher's own outbound from
/// backing up, and waiting for every ack BEFORE the shutdown is what keeps a
/// socket closed over still-buffered publishes from being mistaken for a
/// delivery defect.
fn publish_burst_raw_tcp(
    server: &RunningServer,
    count: usize,
) -> Result<(usize, Vec<String>), Box<dyn Error>> {
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
        let mut others: Vec<String> = Vec::new();
        loop {
            let read = match drain.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(read) => read,
            };
            pending.extend_from_slice(&chunk[..read]);
            while let Ok((frame, consumed)) = decode(&pending) {
                pending.drain(..consumed);
                match frame {
                    Frame::PublishAck { .. } => accepted += 1,
                    other => others.push(format!("{other:?}")),
                }
            }
            if accepted >= count {
                done.send(()).ok();
            }
        }
        (accepted, others)
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
    let (accepted, others) = reader.join().map_err(|_| "ack reader panicked")?;
    Ok((accepted, others))
}

/// Drains a WebSocket subscriber until `wanted` messages have arrived or it goes
/// quiet, returning the count received and the terminal error (if any).
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

/// Drains a TCP subscriber until `wanted` messages have arrived or it goes quiet.
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

/// One boot of the burst pin: a TCP subscriber and a WebSocket subscriber on the
/// same channel of the same fresh server take one burst.
fn burst_once() -> Result<(bool, String), Box<dyn Error>> {
    let server = RunningServer::start()?;
    let tcp_stream = open_tcp_subscription(&server)?;
    let ws_stream = open_ws_subscription(&server)?;

    let (accepted, publisher_frames) = publish_burst_raw_tcp(&server, BURST)?;

    let (tcp_received, tcp_error) = drain_tcp(&tcp_stream, accepted);
    let (ws_received, ws_error) = drain_ws(&ws_stream, accepted);

    let detail = format!(
        "burst={BURST} accepted={accepted} tcp_received={tcp_received} tcp_error={tcp_error:?} \
         ws_received={ws_received} ws_error={ws_error:?} publisher_frames={publisher_frames:?}"
    );
    Ok((
        accepted == BURST && tcp_received == accepted && ws_received == accepted,
        detail,
    ))
}

/// The burst pin. Every envelope the server ACCEPTED must reach both subscribers,
/// on every boot.
#[test]
fn a_burst_larger_than_the_delivery_slice_never_starves_a_subscriber()
-> Result<(), Box<dyn Error>> {
    let _gate = PIN_GATE.lock().unwrap_or_else(PoisonError::into_inner);
    let mut failures = Vec::new();
    for index in 0..ITERATIONS {
        let (ok, detail) = burst_once()?;
        eprintln!("BURST PIN iteration {index}: ok={ok} :: {detail}");
        if !ok {
            failures.push(format!("iteration {index}: {detail}"));
        }
    }
    assert!(
        failures.is_empty(),
        "the burst pin lost a subscriber on {}/{ITERATIONS} fresh boots:\n{}",
        failures.len(),
        failures.join("\n")
    );
    Ok(())
}

/// One boot of the mixed-fate pin: TWO WebSocket subscriptions established on ONE
/// server boot take ONE burst.
fn mixed_fate_once() -> Result<(bool, String), Box<dyn Error>> {
    let server = RunningServer::start()?;
    let first = open_ws_subscription(&server)?;
    let second = open_ws_subscription(&server)?;

    let (accepted, publisher_frames) = publish_burst_raw_tcp(&server, BURST)?;

    let (first_received, first_error) = drain_ws(&first, accepted);
    let (second_received, second_error) = drain_ws(&second, accepted);

    let both_ok = first_received == accepted && second_received == accepted;
    let mixed = (first_received == accepted) != (second_received == accepted);
    let detail = format!(
        "burst={BURST} accepted={accepted} first_received={first_received} \
         first_error={first_error:?} second_received={second_received} \
         second_error={second_error:?} mixed_fate={mixed} \
         publisher_frames={publisher_frames:?}"
    );
    Ok((accepted == BURST && both_ok, detail))
}

/// The mixed-fate pin, field-derived: two WebSocket subscribers on one boot, one
/// burst, and one of them survived while the other was starved into a permanent
/// shed. Nothing about the two differs except which one the scheduler happened to
/// re-enter first, which is exactly why "it worked for me" and "it is dead" were
/// both true reports of the same estate. After the fix BOTH must receive
/// everything the server accepted.
#[test]
fn b_two_websocket_subscribers_on_one_boot_share_the_same_fate() -> Result<(), Box<dyn Error>> {
    let _gate = PIN_GATE.lock().unwrap_or_else(PoisonError::into_inner);
    let mut failures = Vec::new();
    for index in 0..ITERATIONS {
        let (ok, detail) = mixed_fate_once()?;
        eprintln!("MIXED-FATE PIN iteration {index}: ok={ok} :: {detail}");
        if !ok {
            failures.push(format!("iteration {index}: {detail}"));
        }
    }
    assert!(
        failures.is_empty(),
        "the mixed-fate pin lost a subscriber on {}/{ITERATIONS} fresh boots:\n{}",
        failures.len(),
        failures.join("\n")
    );
    Ok(())
}
