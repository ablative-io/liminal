//! Teardown-window delivery-loss pins (DEFECT A).
//!
//! WHY THESE PINS EXIST. liminal's own test suite never published a burst of
//! fire-and-forget events into its *own* embedded shutdown, so a latent teardown
//! race — present since before 0.3.2 — had no in-repo detector. A downstream
//! storeless consumer that embeds the server exactly as [`RunningServer`] does
//! below (both listeners, a `durable = false` channel, no persistence, no
//! `[participant]` config) was the first to observe events vanishing when it
//! published immediately before calling [`run_shutdown_sequence`]. These pins
//! reproduce that consumer's shape in-tree, making liminal its own first
//! detector of the two unfenced teardown windows:
//!
//!   * PIN A1 (shutdown-flush): accepted publishes that have not yet fanned out
//!     to a parked subscriber are overtaken by the shutdown `Disconnect`, because
//!     `run_shutdown_sequence` notified subscribers with no barrier draining the
//!     in-flight fan-out first.
//!   * PIN A2 (parked-wake): a `PushClient` dropped immediately after a
//!     fire-and-forget burst RSTs its socket (unread `PublishAck`s), and the
//!     server discards the not-yet-read publish frames, so a *parked* subscriber
//!     never receives the tail of the burst.
//!
//! Both assert delivery through a TOLD socket read deadline
//! ([`SubscriptionStream::recv_timeout`]), never a wall-clock sample.

use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

use liminal_server::config::types::WebSocketConfig;
use liminal_server::config::{ChannelDef, LimitsConfig, ServerConfig, ServicesConfig};
use liminal_server::server::connection::{LiminalConnectionServices, WebSocketListener};
use liminal_server::server::shutdown::run_shutdown_sequence;
use liminal_server::server::{ConnectionSupervisor, ServerListener};
use liminal_sdk::remote::{PushClient, SubscriptionStream};

const CHANNEL: &str = "app.events";
/// Bound on connection-count setup waits and on each delivery read.
const DEADLINE: Duration = Duration::from_secs(5);
/// The drain budget handed to the shutdown sequence. Generous enough that a
/// correct flush barrier always completes inside it on loopback.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(4);

/// Storeless embedding of the server: both listeners over one shared supervisor,
/// a single `durable = false` channel, no persistence, no `[participant]` config
/// — the exact shape the downstream consumer that first detected DEFECT A used.
struct RunningServer {
    tcp: ServerListener,
    ws: Option<WebSocketListener>,
    supervisor: ConnectionSupervisor,
    addr: String,
}

impl RunningServer {
    fn start() -> Result<Self, Box<dyn Error>> {
        let ws_config = WebSocketConfig {
            listen_address: "127.0.0.1:0".parse()?,
            path: "/liminal".to_owned(),
            allowed_origins: vec!["http://127.0.0.1:1".to_owned()],
            ping_interval_ms: None,
        };
        let config = ServerConfig {
            listen_address: "127.0.0.1:0".parse()?,
            health_listen_address: "127.0.0.1:0".parse()?,
            drain_timeout_ms: 4_000,
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
            participant: None,
            websocket: Some(ws_config.clone()),
        };
        let services = Arc::new(LiminalConnectionServices::from_config(&config)?);
        let supervisor = ConnectionSupervisor::with_services_auth_and_limits(
            services,
            None,
            config.limits.clone(),
        )?;
        let tcp = ServerListener::bind(&config, supervisor)?;
        let supervisor = tcp.supervisor();
        let ws = WebSocketListener::bind(&ws_config, supervisor.clone())?;
        let addr = tcp.local_addr().to_string();
        Ok(Self {
            tcp,
            ws: Some(ws),
            supervisor,
            addr,
        })
    }

    /// Blocks (bounded) until exactly `expected` connections are tracked. Setup
    /// scaffolding only — the delivery assertions below use TOLD socket deadlines.
    fn wait_for_active(&self, expected: usize) -> Result<(), Box<dyn Error>> {
        let deadline = Instant::now() + DEADLINE;
        loop {
            if self.supervisor.active_connection_count() == expected {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "expected {expected} active connections, observed {}",
                    self.supervisor.active_connection_count()
                )
                .into());
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

/// A fixed-size payload distinct per index so a lost or reordered delivery is
/// detectable, not just a count mismatch. The channel applies a permissive
/// any-valid-JSON schema, so the payload is a quoted JSON string; an invalid body
/// would be rejected at publish and never fan out (masking the teardown race).
fn payload(index: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(1024);
    bytes.push(b'"');
    bytes.extend_from_slice(format!("event-{index:04}-").as_bytes());
    bytes.resize(1023, b'x');
    bytes.push(b'"');
    bytes
}

/// Drains exactly `count` deliveries from `subscriber`, each bounded by a TOLD
/// socket read deadline. A stream that ends early (server `Disconnect` overtook
/// the deliveries) surfaces as the reader-stopped error, which is the DEFECT-A
/// red signal.
fn collect_deliveries(
    subscriber: &SubscriptionStream,
    count: usize,
) -> Result<Vec<Vec<u8>>, Box<dyn Error>> {
    let mut received = Vec::with_capacity(count);
    for index in 0..count {
        let message = subscriber.recv_timeout(DEADLINE).map_err(|error| {
            format!("delivery {index} of {count} never arrived before the stream ended: {error}")
        })?;
        received.push(message.payload().to_vec());
    }
    Ok(received)
}

/// PIN A1 — shutdown-flush barrier.
///
/// A subscriber is parked on a `durable = false` channel; a still-connected
/// `PushClient` fires `BURST` payloads fire-and-forget; within the ≤5 ms teardown
/// window the embedder calls [`run_shutdown_sequence`]. Every published payload
/// was accepted (the publisher stays connected, so no RST discards them), so a
/// correct server must flush the accepted-but-unfanned-out deliveries to the
/// subscriber BEFORE broadcasting the shutdown `Disconnect`. Pre-fix, the
/// `Disconnect` overtakes the still-in-flight fan-out and the subscriber's reader
/// exits having seen fewer than `BURST` deliveries.
///
/// The scenario is repeated across fresh servers to reproduce the analysis's
/// deterministic red; a single surviving iteration that loses a delivery fails
/// the pin.
#[test]
fn pin_a1_shutdown_flushes_accepted_publishes_before_disconnect() -> Result<(), Box<dyn Error>> {
    const BURST: usize = 8;
    const ITERATIONS: usize = 5;

    for iteration in 0..ITERATIONS {
        let mut server = RunningServer::start()?;
        let subscriber = SubscriptionStream::open(&server.addr, CHANNEL, Vec::new())?;
        server.wait_for_active(1)?;

        let publisher = PushClient::connect(&server.addr)?;
        server.wait_for_active(2)?;

        // Fire the burst, then — within the teardown window, with no settle that
        // would let the fan-out complete — hand the server to the shutdown
        // sequence. The publisher stays connected across the shutdown so this pin
        // isolates the flush barrier (A-ii), not the drop-RST window (A-i).
        for index in 0..BURST {
            publisher.publish(CHANNEL, payload(index))?;
        }

        let supervisor = server.supervisor.clone();
        let mut tcp = server.tcp;
        let mut ws = server.ws.take().ok_or("websocket listener missing")?;
        let shutdown = std::thread::spawn(move || {
            run_shutdown_sequence(&mut tcp, Some(&mut ws), &supervisor, DRAIN_TIMEOUT)
        });

        let received = collect_deliveries(&subscriber, BURST)
            .map_err(|error| format!("iteration {iteration}: {error}"))?;
        for (index, bytes) in received.iter().enumerate() {
            assert_eq!(
                bytes.as_slice(),
                payload(index).as_slice(),
                "iteration {iteration}: delivery {index} payload mismatch"
            );
        }

        shutdown
            .join()
            .map_err(|_| "shutdown worker panicked")?
            .map_err(|error| format!("iteration {iteration}: shutdown sequence failed: {error}"))?;
        drop(publisher);
        drop(subscriber);
    }
    Ok(())
}

/// PIN A2 — parked-subscriber drop-RST.
///
/// A subscriber is left parked idle well past any wake latency, then a
/// `PushClient` fires a fire-and-forget burst and is dropped immediately. Pre-fix
/// [`PushClient`]'s drop stops and joins its ack-reader before the socket closes,
/// leaving the server's `PublishAck`s unread; the close emits a kernel RST and the
/// server discards the not-yet-read publish frames, tearing the publisher slice
/// down as a connection loss. The parked subscriber therefore never receives the
/// tail of the burst. A correct drop drains the pending acks first, so every
/// accepted publish reaches the parked subscriber within the TOLD read deadline.
#[test]
fn pin_a2_parked_subscriber_receives_burst_after_publisher_drop() -> Result<(), Box<dyn Error>> {
    const BURST: usize = 64;
    const ITERATIONS: usize = 3;

    for iteration in 0..ITERATIONS {
        let server = RunningServer::start()?;
        let subscriber = SubscriptionStream::open(&server.addr, CHANNEL, Vec::new())?;
        server.wait_for_active(1)?;

        // Park the subscriber genuinely idle — well past the observed 8–131 ms
        // fan-out wake latency — so the delivery below is a true parked wake, not
        // a coalesced busy-loop pump.
        std::thread::sleep(Duration::from_millis(300));

        {
            let publisher = PushClient::connect(&server.addr)?;
            server.wait_for_active(2)?;
            for index in 0..BURST {
                publisher.publish(CHANNEL, payload(index))?;
            }
            // Drop the publisher immediately after the last fire-and-forget
            // publish, with unread PublishAcks still queued — the RST window.
            drop(publisher);
        }

        let received = collect_deliveries(&subscriber, BURST)
            .map_err(|error| format!("iteration {iteration}: {error}"))?;
        for (index, bytes) in received.iter().enumerate() {
            assert_eq!(
                bytes.as_slice(),
                payload(index).as_slice(),
                "iteration {iteration}: delivery {index} payload mismatch"
            );
        }
        drop(subscriber);
        server.tcp.shutdown()?;
    }
    Ok(())
}
