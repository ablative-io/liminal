//! End-to-end proof that the SDK's real TCP transport talks to a live server.
//!
//! This test starts a real `liminal-server` listener bound to an ephemeral
//! loopback port, constructs a real SDK client configured with [`RemoteConfig`]
//! pointed at that port, and performs genuine request/response round trips over
//! the socket: the SDK's `connect_tcp` drives `Connect` -> `ConnectAck`, then
//! `publish`/`subscribe` drive `Publish` -> `PublishAck` and
//! `Subscribe` -> `SubscribeAck`.
//!
//! These assertions would FAIL against the old `black_box` mock transport: the
//! mock never opens a socket and synthesises an `Accept` locally, so it could not
//! observe a server bound to an ephemeral port (the SDK never learns the port until
//! runtime) and could not produce the subscribe acknowledgement that only the real
//! server emits. Here the SDK is handed the live port and the bytes travel the wire.

use std::error::Error;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use futures_core::Stream;
use std::sync::Arc;
use std::sync::Mutex;

use liminal::protocol::WorkerRegistration;
use liminal_sdk::{
    ChannelHandle, ConnectionPoolConfig, ConversationHandle, DeliveryAck, PressureResponse,
    PushClient, RemoteChannelHandle, RemoteConfig, RemoteConversationHandle, SchemaMetadata,
    SchemaValidate, SdkConfig, build_channel_handle, build_conversation_handle,
};
use liminal_server::ServerError;
use liminal_server::config::{AuthConfig, ChannelDef, LoadedSchema, ServerConfig};
use liminal_server::server::connection::notifier::ConnectionNotifier;
use liminal_server::server::connection::{ConnectionSupervisor, LiminalConnectionServices};
use liminal_server::server::listener::ServerListener;

use serde::{Deserialize, Serialize};

const CHANNEL: &str = "events";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Serialize, Deserialize)]
struct OrderPlaced {
    id: u64,
}

impl SchemaValidate for OrderPlaced {
    fn schema_metadata() -> SchemaMetadata {
        SchemaMetadata::new("orders.placed", "1", br#"{"type":"object"}"#.as_slice())
    }
}

#[derive(Serialize, Deserialize)]
struct ChatMessage {
    text: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
struct DispatchRequest {
    activity: String,
}

impl SchemaValidate for DispatchRequest {
    fn schema_metadata() -> SchemaMetadata {
        SchemaMetadata::new("dispatch.request", "1", br#"{"type":"object"}"#.as_slice())
    }
}

/// The JSON Schema written to disk and referenced by a channel's `schema_ref` in
/// the real-schema e2e test: an object requiring an integer `id` and forbidding
/// any other property.
const REAL_SCHEMA: &[u8] = br#"{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"],"additionalProperties":false}"#;

/// A payload that SATISFIES [`REAL_SCHEMA`] (`{"id":1}`).
#[derive(Serialize, Deserialize)]
struct SchemaValidEvent {
    id: u64,
}

impl SchemaValidate for SchemaValidEvent {
    fn schema_metadata() -> SchemaMetadata {
        SchemaMetadata::new("events.real", "1", REAL_SCHEMA)
    }
}

/// A payload that VIOLATES [`REAL_SCHEMA`] (`{"name":"..."}`: missing the required
/// `id` and carrying a forbidden extra property).
#[derive(Serialize, Deserialize)]
struct SchemaInvalidEvent {
    name: String,
}

impl SchemaValidate for SchemaInvalidEvent {
    fn schema_metadata() -> SchemaMetadata {
        SchemaMetadata::new("events.real", "1", REAL_SCHEMA)
    }
}

/// Drives a `ReadyResult`-style future to completion. The TCP transport completes
/// the round trip synchronously inside the call that builds the future, so a
/// single poll resolves it; a `Pending` would mean the synchronous contract was
/// violated and is surfaced as a test failure.
fn block_on<F: Future>(future: F) -> Result<F::Output, Box<dyn Error>> {
    let mut future = pin!(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => Ok(value),
        Poll::Pending => Err("synchronous transport future parked unexpectedly".into()),
    }
}

/// Builds a TCP-connected `RemoteConfig` directly (not wrapped in `SdkConfig`), so
/// the test can construct concrete remote handles and reach the request-reply
/// surface. Mirrors `connect_client`'s connect-retry loop.
fn connect_remote_config(
    address: SocketAddr,
    channel: &str,
    conversation: &str,
) -> Result<RemoteConfig, Box<dyn Error>> {
    let deadline = Instant::now() + CONNECT_TIMEOUT;
    let mut last_error = None;
    while Instant::now() < deadline {
        let config = RemoteConfig::new(
            address.to_string(),
            channel,
            conversation.to_owned(),
            ConnectionPoolConfig::new(1, 10, 16),
        )?;
        match config.connect_tcp() {
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

/// Holds the running listener so it stays bound for the lifetime of a test.
struct RunningServer {
    listener: Option<ServerListener>,
    supervisor: ConnectionSupervisor,
    address: SocketAddr,
}

impl RunningServer {
    fn start() -> Result<Self, Box<dyn Error>> {
        let config = ServerConfig {
            listen_address: "127.0.0.1:0".parse()?,
            health_listen_address: reserve_loopback_port()?,
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
            drain_timeout_ms: 30_000,
        };
        let supervisor = ConnectionSupervisor::from_config(&config)?;
        let listener = ServerListener::bind(&config, supervisor)?;
        let supervisor = listener.supervisor();
        let address = listener.local_addr();
        Ok(Self {
            listener: Some(listener),
            supervisor,
            address,
        })
    }

    /// Starts a server whose `Connect` handshake is gated by `token`: every client
    /// must present a matching `auth_token` or the server closes the connection.
    fn start_with_auth_token(token: &str) -> Result<Self, Box<dyn Error>> {
        let config = ServerConfig {
            listen_address: "127.0.0.1:0".parse()?,
            health_listen_address: reserve_loopback_port()?,
            channels: vec![ChannelDef {
                name: CHANNEL.to_owned(),
                schema_ref: None,
                durable: false,
                loaded_schema: None,
            }],
            routing_rules: Vec::new(),
            persistence_path: None,
            cluster: None,
            auth: Some(AuthConfig {
                token: token.to_owned(),
            }),
            drain_timeout_ms: 30_000,
        };
        let supervisor = ConnectionSupervisor::from_config(&config)?;
        let listener = ServerListener::bind(&config, supervisor)?;
        let supervisor = listener.supervisor();
        let address = listener.local_addr();
        Ok(Self {
            listener: Some(listener),
            supervisor,
            address,
        })
    }

    /// Starts a server whose connection supervisor carries `notifier`, so worker
    /// registration over a real socket exercises the application hook.
    fn start_with_notifier(notifier: Arc<dyn ConnectionNotifier>) -> Result<Self, Box<dyn Error>> {
        let config = ServerConfig {
            listen_address: "127.0.0.1:0".parse()?,
            health_listen_address: reserve_loopback_port()?,
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
            drain_timeout_ms: 30_000,
        };
        let services = Arc::new(LiminalConnectionServices::from_config(&config)?);
        let supervisor = ConnectionSupervisor::with_services_and_notifier(services, notifier)?;
        let listener = ServerListener::bind(&config, supervisor)?;
        let supervisor = listener.supervisor();
        let address = listener.local_addr();
        Ok(Self {
            listener: Some(listener),
            supervisor,
            address,
        })
    }

    /// Starts a server whose single channel carries a real JSON Schema loaded from
    /// `schema_path`. The schema bytes are read from that file and parsed into a
    /// [`LoadedSchema`] exactly as config validation would, then the channel is
    /// built with that real schema. (The listener binds to an ephemeral port, which
    /// full config `validate` deliberately rejects, so the load step is exercised
    /// directly here; the validation-driven load path is covered by the config unit
    /// tests.)
    fn start_with_schema(schema_path: &std::path::Path) -> Result<Self, Box<dyn Error>> {
        let bytes = std::fs::read(schema_path)?;
        let document = serde_json::from_slice(&bytes)?;
        let config = ServerConfig {
            listen_address: "127.0.0.1:0".parse()?,
            health_listen_address: reserve_loopback_port()?,
            channels: vec![ChannelDef {
                name: CHANNEL.to_owned(),
                schema_ref: Some(schema_path.to_path_buf()),
                durable: false,
                loaded_schema: Some(LoadedSchema { bytes, document }),
            }],
            routing_rules: Vec::new(),
            persistence_path: None,
            cluster: None,
            auth: None,
            drain_timeout_ms: 30_000,
        };
        let supervisor = ConnectionSupervisor::from_config(&config)?;
        let listener = ServerListener::bind(&config, supervisor)?;
        let supervisor = listener.supervisor();
        let address = listener.local_addr();
        Ok(Self {
            listener: Some(listener),
            supervisor,
            address,
        })
    }

    const fn address(&self) -> SocketAddr {
        self.address
    }

    /// Waits until the server has accepted at least one live connection, bounded
    /// by a deadline so the test cannot hang. Proves a real socket reached the
    /// server, which the in-process mock transport could never cause.
    fn wait_for_connection(&self) -> Result<(), Box<dyn Error>> {
        let deadline = Instant::now() + CONNECT_TIMEOUT;
        while Instant::now() < deadline {
            if self.supervisor.active_connection_count() >= 1 {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        Err("server never observed a live client connection".into())
    }

    /// Waits until exactly one connection is tracked and returns its beamr pid, so
    /// a test can address a server-initiated push at that specific connection.
    fn single_connection_pid(&self) -> Result<u64, Box<dyn Error>> {
        let deadline = Instant::now() + CONNECT_TIMEOUT;
        while Instant::now() < deadline {
            let pids = self.supervisor.active_connection_pids();
            if let [pid] = pids.as_slice() {
                return Ok(*pid);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        Err("server never observed exactly one live client connection".into())
    }

    fn shutdown(mut self) -> Result<(), Box<dyn Error>> {
        if let Some(listener) = self.listener.take() {
            listener.shutdown()?;
        }
        Ok(())
    }
}

fn reserve_loopback_port() -> Result<SocketAddr, Box<dyn Error>> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    drop(listener);
    Ok(address)
}

/// Builds an SDK channel handle whose transport is a live TCP connection.
///
/// `connect_tcp` performs the handshake eagerly, so this also proves the
/// `Connect` -> `ConnectAck` round trip succeeded over the socket. The loop is
/// guarded by a deadline so the test cannot hang if the listener never accepts.
fn connect_client(address: SocketAddr) -> Result<SdkConfig, Box<dyn Error>> {
    let deadline = Instant::now() + CONNECT_TIMEOUT;
    let mut last_error = None;
    while Instant::now() < deadline {
        let config = RemoteConfig::new(
            address.to_string(),
            CHANNEL,
            "conversation",
            ConnectionPoolConfig::new(1, 10, 16),
        )?;
        match config.connect_tcp() {
            Ok(connected) => return Ok(SdkConfig::remote(connected)),
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

#[test]
fn sdk_tcp_client_publishes_over_real_socket() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start()?;
    let client = connect_client(server.address())?;

    // The server observed a real accepted connection; the mock never opens a
    // socket so this count would stay at zero under the old transport.
    server.wait_for_connection()?;

    // A successful publish proves the Publish frame crossed the socket and the
    // server replied with PublishAck (mapped to Accept). The mock never opened a
    // socket, so it could not reach a server on a port chosen at runtime.
    let handle = build_channel_handle(&client)?;
    let response = handle.publish(OrderPlaced { id: 1 })?;
    assert_eq!(response, PressureResponse::Accept);

    server.shutdown()?;
    Ok(())
}

#[test]
fn sdk_tcp_client_subscribes_over_real_socket() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start()?;
    let client = connect_client(server.address())?;

    // subscribe() returns a Stream that surfaces any setup error as its first
    // item. The real transport only yields an error-free subscription after the
    // server answers SubscribeAck over the socket, so polling the stream once
    // returns None (no pending error). A SubscribeError or a dead socket would
    // instead surface Some(Err(..)). The mock returned an empty stream without
    // sending any bytes; here the empty result is the product of a live round trip.
    let handle = build_channel_handle(&client)?;
    let subscription = handle.subscribe::<OrderPlaced>();
    let mut subscription = pin!(subscription);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match subscription.as_mut().poll_next(&mut context) {
        Poll::Ready(None) => {}
        Poll::Ready(Some(Err(error))) => {
            return Err(format!("subscribe surfaced a setup error: {error}").into());
        }
        Poll::Ready(Some(Ok(_))) => {
            return Err("subscribe unexpectedly yielded a buffered message".into());
        }
        Poll::Pending => return Err("subscribe stream parked unexpectedly".into()),
    }

    // The same live connection still serves publishes afterwards.
    let response = handle.publish(OrderPlaced { id: 2 })?;
    assert_eq!(response, PressureResponse::Accept);

    server.shutdown()?;
    Ok(())
}

/// The mock transport never opens a socket, so it would "succeed" against a dead
/// port. The real transport must surface a connection error, which is the
/// sharpest distinction between a genuine TCP transport and the old `black_box` mock.
#[test]
fn sdk_tcp_connect_to_closed_port_fails() -> Result<(), Box<dyn Error>> {
    // Reserve and immediately release a port so nothing is listening on it.
    let dead_address = reserve_loopback_port()?;
    let config = RemoteConfig::new(
        dead_address.to_string(),
        CHANNEL,
        "conversation",
        ConnectionPoolConfig::new(1, 10, 16),
    )?;

    let result = config.connect_tcp();

    assert!(
        result.is_err(),
        "connecting the real TCP transport to a closed port must fail; \
         a mock that never touches the network would have succeeded"
    );
    Ok(())
}

/// A conversation message must actually reach the server (`ConversationOpen` +
/// `ConversationMessage`), report a true outcome instead of silently dropping, and
/// leave the shared connection in sync so a following publish still works.
///
/// Against the old fire-and-forget path this would either: (a) report success
/// while the server replied `ConversationError` (silent drop), and (b) leave that
/// error frame undrained so the next publish read a stale `ConversationError` as
/// its response and failed -- a head-of-line desync. Both are exercised here.
#[test]
fn sdk_tcp_conversation_send_then_publish_stays_in_sync() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start()?;
    let client = connect_client(server.address())?;
    server.wait_for_connection()?;

    // The transport opens the conversation, sends the message, and drains any
    // error reply. open_conversation always succeeds server-side, so this must
    // return Ok -- proving the frames crossed the socket and were accepted.
    let conversation = build_conversation_handle(&client)?;
    conversation.send(ChatMessage {
        text: "hello".to_owned(),
    })?;

    // Sending a second message on the now-open conversation must also succeed
    // without re-opening (no duplicate open, no desync).
    conversation.send(ChatMessage {
        text: "world".to_owned(),
    })?;

    // The shared connection is still in sync: a publish round trip reads its own
    // PublishAck, not a stale conversation reply. With the old undrained-error
    // path this publish would have consumed a leftover frame and failed.
    let channel = build_channel_handle(&client)?;
    let response = channel.publish(OrderPlaced { id: 3 })?;
    assert_eq!(response, PressureResponse::Accept);

    server.shutdown()?;
    Ok(())
}

/// 13-L0 load-bearing proof: a remote `request_reply` carries a CORRELATED reply
/// back through the SDK over the real TCP socket.
///
/// The client opens a conversation, sends a request frame tagged with the
/// reply-requested flag, the server forwards it to the echo participant, drains
/// the participant's reply, and sends a `ConversationMessage` reply carrying the
/// same `conversation_id`; the client read path matches that response by
/// conversation id and returns the deserialized payload. Against the old stub this
/// returned `Err("remote request/reply awaits protocol response integration")`
/// without ever reaching the socket.
#[test]
fn sdk_tcp_request_reply_returns_correlated_response() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start()?;
    let config = connect_remote_config(server.address(), CHANNEL, "rr-conversation")?;
    server.wait_for_connection()?;

    let handle = RemoteChannelHandle::new(&config)?;
    let request = DispatchRequest {
        activity: "charge-card".to_owned(),
    };
    let reply: DispatchRequest = block_on(handle.request_reply(request))??;

    // The echo participant returns the request payload verbatim, so a correlated
    // round trip yields back exactly what was sent. A miscorrelated or dropped
    // reply would fail to deserialize or return the wrong value.
    assert_eq!(
        reply,
        DispatchRequest {
            activity: "charge-card".to_owned(),
        }
    );

    server.shutdown()?;
    Ok(())
}

/// 13-L0: the conversation `request` + `receive` pair also carries a correlated
/// reply over the socket, proving the aion `send`-then-`receive` dispatch shape.
///
/// `request` performs the round trip (send with reply flag, block for the
/// correlated reply) and buffers the answer; `receive` deserializes it. Against
/// the old stub `receive` returned `Err("remote receive awaits protocol inbox
/// integration")`.
#[test]
fn sdk_tcp_conversation_request_then_receive_correlates() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start()?;
    let config = connect_remote_config(server.address(), CHANNEL, "dispatch-conversation")?;
    server.wait_for_connection()?;

    let conversation = RemoteConversationHandle::new(&config);
    conversation.request(DispatchRequest {
        activity: "ship-order".to_owned(),
    })?;
    let reply: DispatchRequest = block_on(conversation.receive())??;

    assert_eq!(
        reply,
        DispatchRequest {
            activity: "ship-order".to_owned(),
        }
    );

    server.shutdown()?;
    Ok(())
}

/// 13-L1 load-bearing proof over the real socket: a publish carrying an
/// idempotency key returns a GENUINE delivery ack the caller can observe, and a
/// duplicate of the same key returns a non-ack (dedup-on-delivery suppressed it).
///
/// A subscriber is registered first so a genuine delivery is observable. The first
/// keyed publish is a fresh dedup claim with a live subscriber, so its ack reports
/// `is_accepted() == true`; the duplicate is suppressed, so its ack reports
/// `is_accepted() == false`. Against the pre-13-L1 path there was no delivery ack
/// at all (only the backpressure `Accept`), so this distinction could not be made.
#[test]
fn sdk_tcp_publish_with_idempotency_key_reports_genuine_delivery_ack() -> Result<(), Box<dyn Error>>
{
    let server = RunningServer::start()?;
    let config = connect_remote_config(server.address(), CHANNEL, "delivery-ack")?;
    server.wait_for_connection()?;

    let handle = RemoteChannelHandle::new(&config)?;

    // Register a subscriber on the channel so a delivery is genuinely observable.
    // Polling the subscription once drives the Subscribe -> SubscribeAck round trip
    // (the server adds a subscriber to the channel fan-out).
    let subscription = handle.subscribe::<OrderPlaced>();
    let mut subscription = pin!(subscription);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match subscription.as_mut().poll_next(&mut context) {
        Poll::Ready(None) => {}
        Poll::Ready(Some(Err(error))) => {
            return Err(format!("subscribe surfaced a setup error: {error}").into());
        }
        Poll::Ready(Some(Ok(_))) => {
            return Err("subscribe unexpectedly yielded a buffered message".into());
        }
        Poll::Pending => return Err("subscribe stream parked unexpectedly".into()),
    }

    // First keyed publish: fresh claim + a live subscriber => genuine delivery.
    let first: DeliveryAck =
        handle.publish_with_idempotency_key(&OrderPlaced { id: 1 }, "dispatch-1")?;
    assert!(
        first.is_accepted(),
        "first keyed publish with a subscriber must report a genuine delivery ack"
    );

    // Duplicate of the SAME key: dedup suppresses fan-out => a non-ack the caller
    // can observe (distinct from a backpressure decision).
    let duplicate: DeliveryAck =
        handle.publish_with_idempotency_key(&OrderPlaced { id: 1 }, "dispatch-1")?;
    assert!(
        !duplicate.is_accepted(),
        "a duplicate idempotency key must report a non-delivery ack"
    );

    server.shutdown()?;
    Ok(())
}

/// 13-L1: a keyed publish with NO subscriber returns a non-ack over the socket,
/// so a caller can distinguish "a subscriber received it" from "it was accepted
/// by the bus but reached no one" -- the load-bearing distinction the aion outbox
/// needs to decide whether a send is genuinely done.
#[test]
fn sdk_tcp_publish_with_no_subscriber_reports_non_delivery() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start()?;
    let config = connect_remote_config(server.address(), CHANNEL, "no-subscriber")?;
    server.wait_for_connection()?;

    let handle = RemoteChannelHandle::new(&config)?;
    let ack: DeliveryAck =
        handle.publish_with_idempotency_key(&OrderPlaced { id: 2 }, "lonely-1")?;
    assert!(
        !ack.is_accepted(),
        "a keyed publish that reaches no subscriber must report a non-delivery ack"
    );

    server.shutdown()?;
    Ok(())
}

/// Bound on the server-side wait for the client's correlated push reply.
const PUSH_REPLY_TIMEOUT: Duration = Duration::from_secs(5);
/// Bound on the client-side wait for the server's pushed frame.
const PUSH_RECV_TIMEOUT: Duration = Duration::from_secs(5);

/// LSUB-0 load-bearing proof over the real socket: the server PUSHES a frame to a
/// specific connected client on that client's existing connection, the client's
/// background reader receives it, the client replies with the same correlation id,
/// and the server matches the correlated reply back to its push.
///
/// This is the capability liminal did not have: every prior frame was
/// client-initiated request/response. Here the server originates the frame
/// (`push_to_connection`) and gets a correlated answer, end to end over TCP.
#[test]
fn server_push_to_client_returns_correlated_reply() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start()?;
    let push_client = PushClient::connect(&server.address().to_string())?;
    let pid = server.single_connection_pid()?;

    // The server pushes an opaque payload to that specific connection and gets back
    // an awaiter keyed by a fresh correlation id.
    let awaiter = server
        .supervisor
        .push_to_connection(pid, b"dispatch-activity".to_vec())?;

    // The client's background reader surfaces the pushed frame (no outstanding
    // client request drove this read — the server originated it).
    let pushed = push_client.recv_timeout(PUSH_RECV_TIMEOUT)?;
    assert_eq!(pushed.payload(), b"dispatch-activity");
    assert_eq!(pushed.correlation_id(), awaiter.correlation_id());

    // The client answers, echoing the correlation id; the server matches the reply
    // to its originating push.
    push_client.reply(pushed.correlation_id(), b"activity-done".to_vec())?;
    let reply = awaiter.receive(PUSH_REPLY_TIMEOUT)?;
    assert_eq!(reply, b"activity-done");

    drop(push_client);
    server.shutdown()?;
    Ok(())
}

/// LSUB-0 regression guard: the server-push path is additive and does not disturb
/// the pre-existing client-initiated request/reply path. A normal request/reply
/// round trip still works on a separate connection while the push machinery exists.
#[test]
fn server_push_does_not_regress_request_reply() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start()?;

    // A push client connects (registers the push machinery on the server side)...
    let push_client = PushClient::connect(&server.address().to_string())?;
    let pid = server.single_connection_pid()?;
    let awaiter = server
        .supervisor
        .push_to_connection(pid, b"ping".to_vec())?;
    let pushed = push_client.recv_timeout(PUSH_RECV_TIMEOUT)?;
    push_client.reply(pushed.correlation_id(), b"pong".to_vec())?;
    assert_eq!(awaiter.receive(PUSH_REPLY_TIMEOUT)?, b"pong");

    // ...and a separate ordinary client still completes a request/reply round trip
    // unchanged, proving the request->response path is unregressed.
    let config = connect_remote_config(server.address(), CHANNEL, "rr-after-push")?;
    let handle = RemoteChannelHandle::new(&config)?;
    let reply: DispatchRequest = block_on(handle.request_reply(DispatchRequest {
        activity: "still-works".to_owned(),
    }))??;
    assert_eq!(
        reply,
        DispatchRequest {
            activity: "still-works".to_owned(),
        }
    );

    drop(push_client);
    server.shutdown()?;
    Ok(())
}

/// A2 load-bearing proof over the real socket: a channel wired with a real
/// `schema_ref` (loaded from disk during config validation) actually ENFORCES that
/// schema on the publish path. A schema-invalid publish is rejected by the server
/// (surfacing as a `PublishError` the SDK returns as `Err`), and a schema-valid
/// publish is accepted and genuinely delivered to a subscriber.
///
/// Against the pre-A2 server every channel was built with an empty `json!({})`
/// schema, so the "invalid" publish would have been accepted — this test would
/// fail to observe a rejection.
#[test]
fn sdk_tcp_real_schema_rejects_invalid_and_delivers_valid() -> Result<(), Box<dyn Error>> {
    let schema_file = tempfile::Builder::new()
        .prefix("liminal-e2e-schema")
        .suffix(".json")
        .tempfile()?;
    std::fs::write(schema_file.path(), REAL_SCHEMA)?;

    let server = RunningServer::start_with_schema(schema_file.path())?;
    let config = connect_remote_config(server.address(), CHANNEL, "schema-conversation")?;
    server.wait_for_connection()?;

    let handle = RemoteChannelHandle::new(&config)?;

    // Register a subscriber so a genuine delivery of the valid publish is
    // observable through the delivery ack.
    let subscription = handle.subscribe::<SchemaValidEvent>();
    let mut subscription = pin!(subscription);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match subscription.as_mut().poll_next(&mut context) {
        Poll::Ready(None) => {}
        Poll::Ready(Some(Err(error))) => {
            return Err(format!("subscribe surfaced a setup error: {error}").into());
        }
        Poll::Ready(Some(Ok(_))) => {
            return Err("subscribe unexpectedly yielded a buffered message".into());
        }
        Poll::Pending => return Err("subscribe stream parked unexpectedly".into()),
    }

    // A schema-invalid publish must be rejected by the server's channel schema and
    // surface as an error to the SDK caller.
    let rejected = handle.publish_with_idempotency_key(
        &SchemaInvalidEvent {
            name: "no-id-here".to_owned(),
        },
        "invalid-1",
    );
    assert!(
        rejected.is_err(),
        "a schema-invalid publish must be rejected by the real channel schema, got: {rejected:?}"
    );

    // A schema-valid publish is accepted and delivered to the live subscriber.
    let accepted: DeliveryAck =
        handle.publish_with_idempotency_key(&SchemaValidEvent { id: 1 }, "valid-1")?;
    assert!(
        accepted.is_accepted(),
        "a schema-valid publish with a subscriber must report a genuine delivery ack"
    );

    server.shutdown()?;
    Ok(())
}

/// Shared secret used by the auth-gate e2e tests.
const AUTH_TOKEN: &str = "correct-horse-battery-staple";

/// Builds a TCP-connected `RemoteConfig` whose handshake carries `token`, retrying
/// the connect until the listener is accepting (mirroring `connect_remote_config`).
/// A persistent auth rejection is surfaced as the returned error after the deadline.
fn connect_remote_config_with_auth(
    address: SocketAddr,
    channel: &str,
    conversation: &str,
    token: &[u8],
) -> Result<RemoteConfig, Box<dyn Error>> {
    let deadline = Instant::now() + CONNECT_TIMEOUT;
    let mut last_error = None;
    while Instant::now() < deadline {
        let config = RemoteConfig::new(
            address.to_string(),
            channel,
            conversation.to_owned(),
            ConnectionPoolConfig::new(1, 10, 16),
        )?;
        match config.connect_tcp_with_auth(token) {
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

/// A client presenting the correct token connects over the real socket and can
/// publish: the auth gate lets an authenticated client through unchanged.
#[test]
fn sdk_tcp_correct_token_connects_and_publishes() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start_with_auth_token(AUTH_TOKEN)?;
    let config = connect_remote_config_with_auth(
        server.address(),
        CHANNEL,
        "auth-ok",
        AUTH_TOKEN.as_bytes(),
    )?;
    server.wait_for_connection()?;

    let handle = RemoteChannelHandle::new(&config)?;
    let response = handle.publish(OrderPlaced { id: 1 })?;
    assert_eq!(response, PressureResponse::Accept);

    server.shutdown()?;
    Ok(())
}

/// A client presenting the WRONG token is rejected by the server's `ConnectError`
/// and the connection is closed, so the handshake (`connect_tcp_with_auth`) fails
/// and the client never reaches a state where it could publish.
#[test]
fn sdk_tcp_wrong_token_is_rejected() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start_with_auth_token(AUTH_TOKEN)?;

    // Confirm the listener is accepting first (a correct-token connect that we then
    // drop), so a subsequent wrong-token failure is unambiguously the auth gate and
    // not a listener-still-starting connection-refused.
    let warmup = connect_remote_config_with_auth(
        server.address(),
        CHANNEL,
        "auth-warmup",
        AUTH_TOKEN.as_bytes(),
    )?;
    drop(warmup);

    let config = RemoteConfig::new(
        server.address().to_string(),
        CHANNEL,
        "auth-wrong",
        ConnectionPoolConfig::new(1, 10, 16),
    )?;
    let result = config.connect_tcp_with_auth(b"not-the-token");
    assert!(
        result.is_err(),
        "a wrong auth token must be rejected by the server handshake"
    );

    server.shutdown()?;
    Ok(())
}

/// A client presenting NO token (the pre-auth open handshake) against a gated
/// server is rejected exactly like a wrong token: absence is not a bypass.
#[test]
fn sdk_tcp_missing_token_is_rejected() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start_with_auth_token(AUTH_TOKEN)?;

    // Confirm the listener is accepting first, as in the wrong-token test, so a
    // missing-token failure is the auth gate and not a startup race.
    let warmup = connect_remote_config_with_auth(
        server.address(),
        CHANNEL,
        "auth-warmup",
        AUTH_TOKEN.as_bytes(),
    )?;
    drop(warmup);

    let config = RemoteConfig::new(
        server.address().to_string(),
        CHANNEL,
        "auth-missing",
        ConnectionPoolConfig::new(1, 10, 16),
    )?;
    // `connect_tcp` sends an empty auth token — the open-access handshake.
    let result = config.connect_tcp();
    assert!(
        result.is_err(),
        "a missing auth token against a gated server must be rejected"
    );

    server.shutdown()?;
    Ok(())
}

/// The auth gate is opt-in: a server with no `[auth]` section behaves exactly as
/// before — a plain `connect_tcp` (empty token) connects and publishes. This is the
/// byte-identical-to-today guarantee for the un-gated deployment.
#[test]
fn sdk_tcp_no_auth_section_behaves_as_before() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start()?;
    let client = connect_client(server.address())?;
    server.wait_for_connection()?;

    let handle = build_channel_handle(&client)?;
    let response = handle.publish(OrderPlaced { id: 2 })?;
    assert_eq!(response, PressureResponse::Accept);

    server.shutdown()?;
    Ok(())
}

/// Records the worker-registration lifecycle calls a server-side notifier
/// observes, so an end-to-end test can assert the exact `(pid, registration)` the
/// hook was handed and the matching deregistration on close.
#[derive(Debug)]
struct RecordingNotifier {
    registered: Mutex<Vec<(u64, WorkerRegistration)>>,
    unregistered: Mutex<Vec<u64>>,
    reject_with: Option<String>,
}

impl RecordingNotifier {
    const fn accepting() -> Self {
        Self {
            registered: Mutex::new(Vec::new()),
            unregistered: Mutex::new(Vec::new()),
            reject_with: None,
        }
    }

    fn rejecting(reason: &str) -> Self {
        Self {
            registered: Mutex::new(Vec::new()),
            unregistered: Mutex::new(Vec::new()),
            reject_with: Some(reason.to_owned()),
        }
    }

    fn registered_calls(&self) -> Result<Vec<(u64, WorkerRegistration)>, Box<dyn Error>> {
        Ok(self
            .registered
            .lock()
            .map_err(|error| format!("registration recorder poisoned: {error}"))?
            .clone())
    }

    fn unregistered_calls(&self) -> Result<Vec<u64>, Box<dyn Error>> {
        Ok(self
            .unregistered
            .lock()
            .map_err(|error| format!("deregistration recorder poisoned: {error}"))?
            .clone())
    }
}

impl ConnectionNotifier for RecordingNotifier {
    fn on_worker_registered(
        &self,
        pid: u64,
        registration: &WorkerRegistration,
    ) -> Result<(), ServerError> {
        self.registered
            .lock()
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("registration recorder poisoned: {error}"),
            })?
            .push((pid, registration.clone()));
        self.reject_with.as_ref().map_or(Ok(()), |reason| {
            Err(ServerError::ListenerAccept {
                message: reason.clone(),
            })
        })
    }

    fn on_worker_unregistered(&self, pid: u64) {
        if let Ok(mut unregistered) = self.unregistered.lock() {
            unregistered.push(pid);
        }
    }
}

fn worker_registration() -> WorkerRegistration {
    WorkerRegistration {
        namespaces: vec!["default".to_owned(), "billing".to_owned()],
        task_queue: "payments".to_owned(),
        node: Some("node-a".to_owned()),
        activity_types: vec!["charge".to_owned(), "refund".to_owned()],
        identity: "worker-7".to_owned(),
    }
}

/// LSUB-L2 Stage 1: a worker registers over its existing connection. The server
/// associates the registration with the connection pid, surfaces it to the
/// notifier with matching dimensions, acks Accepted (so `connect_with_registration`
/// returns Ok), and fires `on_worker_unregistered` with the SAME pid on close.
#[test]
fn worker_registration_round_trips_and_deregisters_on_close() -> Result<(), Box<dyn Error>> {
    let notifier = Arc::new(RecordingNotifier::accepting());
    let server = RunningServer::start_with_notifier(Arc::clone(&notifier) as Arc<_>)?;

    let registration = worker_registration();
    let push_client =
        PushClient::connect_with_registration(&server.address().to_string(), registration.clone())?;
    let pid = server.single_connection_pid()?;

    // The notifier observed exactly one registration, for this connection pid,
    // carrying the dimensions the worker announced.
    let deadline = Instant::now() + CONNECT_TIMEOUT;
    loop {
        let calls = notifier.registered_calls()?;
        if let [(observed_pid, observed)] = calls.as_slice() {
            assert_eq!(*observed_pid, pid);
            assert_eq!(*observed, registration);
            break;
        }
        if Instant::now() >= deadline {
            return Err("notifier never observed the worker registration".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    // Closing the worker connection deregisters it, with the same pid.
    drop(push_client);
    let deadline = Instant::now() + CONNECT_TIMEOUT;
    loop {
        // The supervisor reaps externally-closed connections lazily; drive it so a
        // socket close that bypassed the handler's finish path is still observed.
        let _reaped = server.supervisor.reap_crashed_connections();
        if notifier.unregistered_calls()?.contains(&pid) {
            break;
        }
        if Instant::now() >= deadline {
            return Err("notifier never observed the worker deregistration".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    server.shutdown()?;
    Ok(())
}

/// A rejecting notifier makes the server ack Rejected; the SDK
/// `connect_with_registration` returns a typed `Err` carrying the reason.
#[test]
fn worker_registration_rejected_returns_typed_error() -> Result<(), Box<dyn Error>> {
    let notifier = Arc::new(RecordingNotifier::rejecting("task queue not served"));
    let server = RunningServer::start_with_notifier(Arc::clone(&notifier) as Arc<_>)?;

    let result =
        PushClient::connect_with_registration(&server.address().to_string(), worker_registration());

    let error = result.err().ok_or("expected registration to be rejected")?;
    let message = error.to_string();
    assert!(
        message.contains("task queue not served"),
        "rejection error should carry the server reason, got: {message}"
    );

    server.shutdown()?;
    Ok(())
}

/// Standalone liminal (no notifier configured) still accepts a worker
/// registration over a real socket: `connect_with_registration` returns Ok and the
/// connection is live, proving the hook is purely additive.
#[test]
fn worker_registration_without_notifier_is_accepted() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start()?;

    let push_client = PushClient::connect_with_registration(
        &server.address().to_string(),
        worker_registration(),
    )?;
    let _pid = server.single_connection_pid()?;

    drop(push_client);
    server.shutdown()?;
    Ok(())
}
