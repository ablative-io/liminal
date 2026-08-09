//! P0 #55 pins: a subscription that falls behind a burst must never be starved
//! into a permanent shed.
//!
//! The OUTCOME these pin (measured at `4ed0562`, when the depth cap was 256): a
//! burst outruns a subscription inbox to its envelope depth cap
//! (`liminal-server/src/config/types.rs`), the
//! sticky overflow marker trips, and the delivery pump SHEDS the subscription
//! permanently — one `SubscribeError` frame, then removal at the connection AND
//! release at the channel actor. The socket stays open and publishes keep acking,
//! so the failure presents in the field as a subscriber that has simply gone quiet
//! forever.
//!
//! These pin the OUTCOME, deliberately, and name no mechanism in their assertions:
//! every envelope the server accepted must reach every live subscriber. That is
//! what let them falsify the mechanism this lane was dispatched to fix — see the
//! 2x2 below. A pin written against `was_empty` would have gone green on a change
//! that, measured, does nothing.
//!
//! Both pins run several FRESH server boots, because the failure is per-boot and
//! lands on roughly half of them; a single boot would be a coin flip.
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
//!
//! # These pins were `#[ignore]`d, and are not any more
//!
//! Part 1 landed them RED and ignored: they are honest measurements of a real P0,
//! and no change inside part 1's authority made them pass. Part 2 made the ruling
//! they were waiting on — `LimitsConfig::DEFAULT_MAX_SUBSCRIPTION_INBOX_DEPTH`
//! 256 -> 4096 — and they now gate.
//!
//! Read the fix at the constant's doc, not here; the one-line version is that the
//! subscription inbox is bounded by BYTES and by COUNT, and the count bound was
//! tripping at roughly 1% of the bytes the connection was already permitted. A
//! replay burst is thousands of SMALL records, so it hit the crude bound while the
//! real one sat idle. Nothing about the scheduling changed. What changed is how far
//! behind a subscriber is allowed to fall before the server kills it, and 256
//! envelopes was never a memory decision.
//!
//! The full 2x2 that RULED OUT the other candidate, each cell a run of THIS binary
//! under `gate-logs/p0-55/run-pins.sh`, iterations counted across the whole cell:
//!
//! | inbox wake        | pump slice budget | boots that lost a subscriber |
//! |-------------------|-------------------|------------------------------|
//! | edge (`was_empty`)| 32 (stock)        | 62/120 (51.7%)               |
//! | level (every admit)| 32 (stock)       | 64/120 (53.3%)               |
//! | edge (`was_empty`)| 256              | 0/72                         |
//! | level (every admit)| 256             | 0/120                        |
//!
//! Read the rows, not the story: the wake rule does not move the outcome and the
//! slice budget decides it completely. The mechanism is R6 coalescing — N wake
//! markers are drained before ONE slice runs, so firing more of them cannot buy
//! the connection another slice, and slices are what drain the queue. A 400
//! burst needs 13 slices at 32/slice and 2 at 256/slice, and each slice boundary
//! is a scheduling round trip whose latency is bimodal: a probe of the WebSocket
//! connection process measured ~0.09 ms between slices on surviving boots and
//! 6-24 ms on losing ones, with every one of ~750 wake fires per burst returning
//! `true` from `enqueue_atom_message`. The wakes arrive; the slices do not.
//!
//! The slice budget is a cross-connection FAIRNESS bound — it exists so one fast
//! producer cannot starve other connections sharing a scheduler thread — so
//! raising it trades one starvation for another. The ruling did NOT raise it: the
//! 2x2 above ran two or three connections, so the peer starvation a raise would
//! CAUSE was unobservable by construction. That table shows the knob MOVES the
//! outcome; it does not show that moving it is safe. The budget became an operator
//! knob (`limits.delivery_slice_budget`, default 32) and the depth cap took the
//! fix.
//!
//! # Running a census
//!
//! Both pins run [`ITERATIONS`] fresh boots on the gate. To measure a loss RATE at
//! some bytes, raise the boot count instead of running the binary repeatedly, so
//! the denominator is the test's rather than a runner script's:
//!
//! ```text
//! LIMINAL_STARVATION_ITERS=40 cargo test -p liminal-server \
//!   --test subscription_starvation_e2e -- --nocapture
//! ```
//!
//! Every boot prints one `ok=` line, so losses are counted from the per-iteration
//! lines, never from the summary.
//!
//! What this lane DID close is pinned elsewhere and does gate: a shed is now loud
//! server-side (`liminal-server/src/server/connection/delivery.rs` warn +
//! `liminal_subscription_sheds_total`) and typed client-side on both transports,
//! so the next occurrence is diagnosable in seconds rather than an hour. See
//! `subscription_shed_visibility_e2e.rs`.

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
/// Envelopes per burst.
///
/// Chosen to exceed the depth cap AS IT WAS (256) so these pins were genuinely red
/// before the fix — a burst under that cap could never have reached the shed, and
/// the pins would have been green against a broken server.
///
/// It stays 400 after the ruling raised the cap to 4096, and the gap is the point:
/// the burst still comfortably outruns the 32-envelope slice budget, so the
/// subscriber still falls far behind on every boot. What changed is that falling
/// behind is no longer fatal. Raising BURST past 4096 would re-create the shed at
/// the new cap and turn these back into reds — that is a DIFFERENT measurement
/// (where is the new cliff), not this one (is a live subscriber killed by an
/// ordinary burst).
const BURST: usize = 400;
/// Fresh server boots per pin. The pre-fix failure is per-boot and probabilistic;
/// this is what turns it into a reliable red.
const ITERATIONS: usize = 6;
/// Environment key that raises [`ITERATIONS`] for a census run.
///
/// The gate always runs [`ITERATIONS`] boots. A census — "how many boots out of N
/// lose a subscriber at these bytes" — wants far more, and running the binary
/// seven times to accumulate 42 boots makes the denominator an artefact of the
/// runner script rather than something the test states. This lets the census name
/// its own N while the gate's N stays a constant in the source.
const ITERATIONS_ENV: &str = "LIMINAL_STARVATION_ITERS";
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
    /// Boots with the SHIPPED defaults — the configuration the field runs, which
    /// is the only one the two starvation pins may measure.
    fn start() -> Result<Self, Box<dyn Error>> {
        Self::start_with(LimitsConfig::default())
    }

    fn start_with(limits: LimitsConfig) -> Result<Self, Box<dyn Error>> {
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

/// Boots to run this invocation: [`ITERATIONS`], or the census override.
///
/// An unparseable or zero value falls back to [`ITERATIONS`] rather than failing:
/// a typo in a census command must not be able to turn a gate test into a red that
/// says nothing about the server.
fn iterations() -> usize {
    std::env::var(ITERATIONS_ENV)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(ITERATIONS)
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
    let boots = iterations();
    let mut failures = Vec::new();
    for index in 0..boots {
        let (ok, detail) = burst_once()?;
        eprintln!("BURST PIN iteration {index}: ok={ok} :: {detail}");
        if !ok {
            failures.push(format!("iteration {index}: {detail}"));
        }
    }
    assert!(
        failures.is_empty(),
        "the burst pin lost a subscriber on {}/{boots} fresh boots:\n{}",
        failures.len(),
        failures.join("\n")
    );
    Ok(())
}

/// The floor of the new operator knob.
///
/// Part 2 turned the delivery slice budget into config, and validation forbids
/// only ZERO. That makes 1 the smallest value an operator can now set, and 1 is
/// the worst case for this pump: a burst needs one scheduler round trip PER
/// ENVELOPE, so anything that made progress depend on draining more than one
/// envelope per slice would stall here and nowhere else.
///
/// What this pin does and does not claim: it is a LIVENESS pin for the configured
/// floor, not a proof that the connection reads the configured value. No
/// deterministic functional discriminator exists for that — the slice budget's
/// only observable effects are timing and shed PROBABILITY, so a test that
/// distinguished a threaded value from a hardcoded 32 would have to be a race. The
/// threading itself is covered structurally (neither pump call site can name a
/// constant any more) and by the config pins in `config::file`.
///
/// The burst is deliberately smaller than [`BURST`]: at one envelope per slice the
/// cost is scheduling round trips, and this pin is about whether progress happens
/// at all, not how fast.
#[test]
fn c_the_smallest_configurable_slice_budget_still_delivers_everything()
-> Result<(), Box<dyn Error>> {
    const SMALL_BURST: usize = 120;

    let _gate = PIN_GATE.lock().unwrap_or_else(PoisonError::into_inner);
    let limits = LimitsConfig {
        delivery_slice_budget: 1,
        ..LimitsConfig::default()
    };
    let server = RunningServer::start_with(limits)?;
    let tcp_stream = open_tcp_subscription(&server)?;
    let ws_stream = open_ws_subscription(&server)?;

    let (accepted, publisher_frames) = publish_burst_raw_tcp(&server, SMALL_BURST)?;
    assert_eq!(
        accepted, SMALL_BURST,
        "the publisher must be acked for every record before delivery is judged; \
         otherwise a dead publisher scores 0 == 0 and this pin passes vacuously \
         (publisher frames: {publisher_frames:?})"
    );

    let (tcp_received, tcp_error) = drain_tcp(&tcp_stream, accepted);
    let (ws_received, ws_error) = drain_ws(&ws_stream, accepted);

    assert_eq!(
        tcp_received, accepted,
        "a budget of 1 must still drain the TCP subscriber: {tcp_error:?}"
    );
    assert_eq!(
        ws_received, accepted,
        "a budget of 1 must still drain the WebSocket subscriber: {ws_error:?}"
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
    let boots = iterations();
    let mut failures = Vec::new();
    for index in 0..boots {
        let (ok, detail) = mixed_fate_once()?;
        eprintln!("MIXED-FATE PIN iteration {index}: ok={ok} :: {detail}");
        if !ok {
            failures.push(format!("iteration {index}: {detail}"));
        }
    }
    assert!(
        failures.is_empty(),
        "the mixed-fate pin lost a subscriber on {}/{boots} fresh boots:\n{}",
        failures.len(),
        failures.join("\n")
    );
    Ok(())
}
