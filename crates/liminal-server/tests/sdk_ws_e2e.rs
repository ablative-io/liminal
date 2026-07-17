//! R2.1 end-to-end proof: the SDK's WebSocket transport talks to the real
//! sibling acceptor (LP-WS-TRANSPORT R1) exactly like the TCP transport talks
//! to the TCP listener.
//!
//! One server instance runs both listeners against one supervisor; the same
//! handle-surface fixtures run once over `connect_tcp` and once over
//! `connect_websocket`, so behavioral parity is proven against real sibling
//! ownership, not a mock.

use std::error::Error;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use liminal_sdk::remote::websocket::WebSocketSubscriptionStream;
use liminal_sdk::{
    ChannelHandle, ConnectionPoolConfig, PressureResponse, RemoteChannelHandle, RemoteConfig,
    SchemaMetadata, SchemaValidate, SdkError, SubscriptionStream,
};
use liminal_server::config::{
    AuthConfig, ChannelDef, LimitsConfig, ServerConfig, ServicesConfig, WebSocketConfig,
};
use liminal_server::server::connection::{ConnectionSupervisor, WebSocketListener};
use liminal_server::server::listener::ServerListener;

use serde::{Deserialize, Serialize};

const CHANNEL: &str = "events";
const PATH: &str = "/liminal";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RECV_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
struct OrderPlaced {
    id: u64,
}

impl SchemaValidate for OrderPlaced {
    fn schema_metadata() -> SchemaMetadata {
        SchemaMetadata::new("orders.placed", "1", br#"{"type":"object"}"#.as_slice())
    }
}

/// Which SDK transport a fixture leg selects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransportKind {
    Tcp,
    Ws,
}

/// Holds both live listeners so they stay bound for the test's lifetime.
struct RunningServer {
    _tcp: ServerListener,
    _ws: WebSocketListener,
    tcp_addr: SocketAddr,
    ws_addr: SocketAddr,
}

impl RunningServer {
    fn start(auth_token: Option<&str>) -> Result<Self, Box<dyn Error>> {
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
            auth: auth_token.map(|token| AuthConfig {
                token: token.to_owned(),
            }),
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
            ping_interval_ms: None,
        };
        let ws = WebSocketListener::bind(&ws_config, supervisor)?;
        let tcp_addr = tcp.local_addr();
        let ws_addr = ws.local_addr();
        Ok(Self {
            _tcp: tcp,
            _ws: ws,
            tcp_addr,
            ws_addr,
        })
    }

    /// SDK-facing address for the selected transport.
    fn address(&self, kind: TransportKind) -> String {
        match kind {
            TransportKind::Tcp => self.tcp_addr.to_string(),
            TransportKind::Ws => format!("ws://{}{PATH}", self.ws_addr),
        }
    }
}

/// Connects the selected SDK transport, retrying while the listener warms up.
fn connect(
    server: &RunningServer,
    kind: TransportKind,
    auth_token: &[u8],
) -> Result<RemoteConfig, Box<dyn Error>> {
    let deadline = Instant::now() + CONNECT_TIMEOUT;
    let mut last_error = None;
    while Instant::now() < deadline {
        let config = RemoteConfig::new(
            server.address(kind),
            CHANNEL,
            "e2e.conversation".to_owned(),
            ConnectionPoolConfig::new(1, 10, 16),
        )?;
        let attempt = match kind {
            TransportKind::Tcp => config.connect_tcp_with_auth(auth_token),
            TransportKind::Ws => config.connect_websocket_with_auth(auth_token),
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

#[test]
fn sdk_publish_parity_over_real_acceptor() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start(None)?;
    for kind in [TransportKind::Tcp, TransportKind::Ws] {
        let config = connect(&server, kind, &[])?;
        let handle = RemoteChannelHandle::new(&config)?;
        let response = handle.publish(OrderPlaced { id: 7 })?;
        assert_eq!(
            response,
            PressureResponse::Accept,
            "{kind:?} publish must be accepted by the real server"
        );
    }
    Ok(())
}

#[test]
fn sdk_subscription_delivery_parity_over_real_acceptor() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start(None)?;

    // One subscriber per transport, both on the same channel of one server.
    let tcp_stream =
        SubscriptionStream::open(&server.address(TransportKind::Tcp), CHANNEL, Vec::new())?;
    let ws_stream =
        WebSocketSubscriptionStream::open(&server.address(TransportKind::Ws), CHANNEL, Vec::new())?;

    // A keyed publish over TCP reports a genuine delivery ack because the two
    // real subscribers exist, and BOTH transports receive the delivery.
    let config = connect(&server, TransportKind::Tcp, &[])?;
    let handle = RemoteChannelHandle::new(&config)?;
    let ack = handle.publish_with_idempotency_key(&OrderPlaced { id: 9 }, "parity-9")?;
    assert!(ack.is_accepted(), "delivery ack must be genuine");

    let tcp_message = tcp_stream.recv_timeout(RECV_TIMEOUT)?;
    let ws_message = ws_stream.recv_timeout(RECV_TIMEOUT)?;
    assert_eq!(tcp_message.payload(), ws_message.payload());
    assert_eq!(tcp_message.delivery_seq(), ws_message.delivery_seq());
    Ok(())
}

#[test]
fn sdk_auth_parity_over_real_acceptor() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start(Some("right-token"))?;
    for kind in [TransportKind::Tcp, TransportKind::Ws] {
        // The wrong token is refused with a typed connection error on both
        // transports. The shared connect helper retries within its window (it
        // cannot distinguish a refusal from listener warmup), so this leg
        // proves the refusal by exhausting that window without ever
        // connecting; the right-token leg below then proves the same warm
        // listener accepts.
        let refused = connect(&server, kind, b"wrong-token");
        assert!(
            refused.is_err(),
            "{kind:?} wrong token must be refused by the real server"
        );

        let config = connect(&server, kind, b"right-token")?;
        let handle = RemoteChannelHandle::new(&config)?;
        let response = handle.publish(OrderPlaced { id: 11 })?;
        assert_eq!(response, PressureResponse::Accept);
    }
    Ok(())
}

#[test]
fn sdk_ws_wrong_path_is_typed_refusal() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start(None)?;
    let config = RemoteConfig::new(
        format!("ws://{}/not-liminal", server.ws_addr),
        CHANNEL,
        "e2e.conversation".to_owned(),
        ConnectionPoolConfig::new(1, 10, 16),
    )?;
    let result = config.connect_websocket();
    assert!(
        matches!(result, Err(SdkError::Connection { .. })),
        "an unconfigured path must surface as a typed connection error"
    );
    Ok(())
}
