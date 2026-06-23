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
use std::net::SocketAddr;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use futures_core::Stream;
use liminal_sdk::{
    ChannelHandle, ConnectionPoolConfig, ConversationHandle, PressureResponse, RemoteConfig,
    SchemaMetadata, SchemaValidate, SdkConfig, build_channel_handle, build_conversation_handle,
};
use liminal_server::config::{ChannelDef, ServerConfig};
use liminal_server::server::connection::ConnectionSupervisor;
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
                schema_ref: "schemas/events.json".to_owned(),
                durable: false,
            }],
            routing_rules: Vec::new(),
            persistence_path: None,
            cluster: None,
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
