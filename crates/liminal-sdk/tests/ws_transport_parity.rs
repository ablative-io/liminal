//! R2.1 native WebSocket transport parity tests (LP-WS-TRANSPORT, folded r1.1).
//!
//! One `RemoteConversationHandle`/channel surface runs the same fixtures once
//! with TCP and once with WebSocket, against scripted in-test servers that
//! speak the canonical liminal wire protocol. The scripted servers give the
//! client leg hermetic control over adversarial cases (auth refusal, malformed
//! bytes, oversize declarations, byte capture) that the real server never
//! produces; parity against the real R1 acceptor is proven separately in
//! `crates/liminal-server/tests/sdk_ws_e2e.rs`.

use std::net::{TcpListener, TcpStream};
use std::thread::JoinHandle;
use std::time::Duration;

use liminal::protocol::{
    Frame, FrameType, PUBLISH_DELIVERED_FLAG, ProtocolError, ProtocolVersion, decode, encode,
    encoded_len,
};
use liminal_protocol::wire::FRAME_MAX;
use liminal_sdk::remote::websocket::liminal_ws_message_bound;
use liminal_sdk::{
    ChannelHandle, ConnectionPoolConfig, ConversationHandle, DeliveryAck, PressureResponse,
    RemoteConfig, SchemaMetadata, SchemaValidate, SdkError, SubscriptionStream,
};

type TestResult<T = ()> = Result<T, String>;

/// Drives a `ReadyResult`-style future to completion; the synchronous
/// transports resolve it in one poll.
fn block_on<F: core::future::Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let waker = std::task::Waker::noop();
    let mut context = std::task::Context::from_waker(waker);
    match future.as_mut().poll(&mut context) {
        std::task::Poll::Ready(value) => value,
        std::task::Poll::Pending => unreachable!("synchronous transport future parked"),
    }
}

const AUTH_TOKEN: &[u8] = b"parity-secret";
const CHANNEL: &str = "parity.events";
const CONVERSATION: &str = "parity.conversation";
const RECV_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
struct ParityMessage {
    value: u64,
}

impl SchemaValidate for ParityMessage {
    fn schema_metadata() -> SchemaMetadata {
        SchemaMetadata::new("parity.message", "1", br#"{"type":"object"}"#.as_slice())
    }
}

/// Which transport a fixture leg runs over.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransportKind {
    Tcp,
    Ws,
}

/// Transport-neutral server side of one scripted connection.
///
/// `read_frame` captures the exact canonical bytes each inbound frame consumed
/// so fixtures can assert cross-transport byte identity.
trait ServerLink {
    fn read_frame(&mut self) -> TestResult<Frame>;
    fn write_frame(&mut self, frame: &Frame) -> TestResult<()>;
    /// Writes raw transport payload bytes (for malformed-input fixtures).
    fn write_raw_message(&mut self, bytes: &[u8]) -> TestResult<()>;
    fn close(&mut self) -> TestResult<()>;
    fn captured_frames(&self) -> &[Vec<u8>];
}

fn encode_frame_bytes(frame: &Frame) -> TestResult<Vec<u8>> {
    let len = encoded_len(frame).map_err(|error| format!("encoded_len failed: {error}"))?;
    let mut bytes = vec![0_u8; len];
    let written = encode(frame, &mut bytes).map_err(|error| format!("encode failed: {error}"))?;
    bytes.truncate(written);
    Ok(bytes)
}

/// Scripted TCP server link: blocking byte stream plus a decode buffer.
struct TcpServerLink {
    stream: TcpStream,
    buffer: Vec<u8>,
    captured: Vec<Vec<u8>>,
}

impl TcpServerLink {
    fn new(stream: TcpStream) -> TestResult<Self> {
        stream
            .set_read_timeout(Some(RECV_TIMEOUT))
            .map_err(|error| format!("script read timeout failed: {error}"))?;
        Ok(Self {
            stream,
            buffer: Vec::new(),
            captured: Vec::new(),
        })
    }
}

impl ServerLink for TcpServerLink {
    fn read_frame(&mut self) -> TestResult<Frame> {
        use std::io::Read;
        loop {
            match decode(&self.buffer) {
                Ok((frame, consumed)) => {
                    let bytes: Vec<u8> = self.buffer.drain(..consumed).collect();
                    self.captured.push(bytes);
                    return Ok(frame);
                }
                Err(
                    ProtocolError::IncompleteHeader { .. } | ProtocolError::TruncatedPayload { .. },
                ) => {
                    let mut chunk = [0_u8; 4096];
                    let read = self
                        .stream
                        .read(&mut chunk)
                        .map_err(|error| format!("script socket read failed: {error}"))?;
                    if read == 0 {
                        return Err("client closed before a full frame arrived".to_string());
                    }
                    self.buffer.extend_from_slice(&chunk[..read]);
                }
                Err(error) => return Err(format!("script decode failed: {error}")),
            }
        }
    }

    fn write_frame(&mut self, frame: &Frame) -> TestResult<()> {
        self.write_raw_message(&encode_frame_bytes(frame)?)
    }

    fn write_raw_message(&mut self, bytes: &[u8]) -> TestResult<()> {
        use std::io::Write;
        self.stream
            .write_all(bytes)
            .map_err(|error| format!("script socket write failed: {error}"))?;
        self.stream
            .flush()
            .map_err(|error| format!("script socket flush failed: {error}"))
    }

    fn close(&mut self) -> TestResult<()> {
        self.stream
            .shutdown(std::net::Shutdown::Both)
            .map_err(|error| format!("script socket shutdown failed: {error}"))
    }

    fn captured_frames(&self) -> &[Vec<u8>] {
        &self.captured
    }
}

/// Scripted WebSocket server link: one binary message per canonical frame.
struct WsServerLink {
    socket: tungstenite::WebSocket<TcpStream>,
    captured: Vec<Vec<u8>>,
}

impl WsServerLink {
    fn accept(stream: TcpStream) -> TestResult<Self> {
        stream
            .set_read_timeout(Some(RECV_TIMEOUT))
            .map_err(|error| format!("ws script read timeout failed: {error}"))?;
        let socket = tungstenite::accept(stream)
            .map_err(|error| format!("ws script accept failed: {error}"))?;
        Ok(Self {
            socket,
            captured: Vec::new(),
        })
    }
}

impl ServerLink for WsServerLink {
    fn read_frame(&mut self) -> TestResult<Frame> {
        loop {
            let message = self
                .socket
                .read()
                .map_err(|error| format!("ws script read failed: {error}"))?;
            match message {
                tungstenite::Message::Binary(bytes) => {
                    let bytes = bytes.to_vec();
                    let (frame, consumed) = decode(&bytes)
                        .map_err(|error| format!("ws script decode failed: {error}"))?;
                    if consumed != bytes.len() {
                        return Err(format!(
                            "one WS message must hold exactly one frame: {consumed} of {}",
                            bytes.len()
                        ));
                    }
                    self.captured.push(bytes);
                    return Ok(frame);
                }
                tungstenite::Message::Ping(_) | tungstenite::Message::Pong(_) => {}
                other => return Err(format!("ws script expected binary, got {other:?}")),
            }
        }
    }

    fn write_frame(&mut self, frame: &Frame) -> TestResult<()> {
        self.write_raw_message(&encode_frame_bytes(frame)?)
    }

    fn write_raw_message(&mut self, bytes: &[u8]) -> TestResult<()> {
        self.socket
            .send(tungstenite::Message::Binary(bytes.to_vec().into()))
            .map_err(|error| format!("ws script send failed: {error}"))
    }

    fn close(&mut self) -> TestResult<()> {
        match self.socket.close(None) {
            Ok(()) => {}
            Err(tungstenite::Error::ConnectionClosed) => return Ok(()),
            Err(error) => return Err(format!("ws script close failed: {error}")),
        }
        // Drive the close handshake until the connection reports closed.
        loop {
            match self.socket.read() {
                Ok(_) => {}
                Err(_) => return Ok(()),
            }
        }
    }

    fn captured_frames(&self) -> &[Vec<u8>] {
        &self.captured
    }
}

/// Handles the canonical liminal handshake, verifying the expected auth token.
fn script_handshake(link: &mut dyn ServerLink, expected_token: &[u8]) -> TestResult<()> {
    let frame = link.read_frame()?;
    let Frame::Connect { auth_token, .. } = frame else {
        return Err(format!("expected Connect, got {frame:?}"));
    };
    if auth_token != expected_token {
        link.write_frame(&Frame::ConnectError {
            flags: 0,
            reason_code: 1,
            message: Some("bad token".to_string()),
        })?;
        return Ok(());
    }
    link.write_frame(&Frame::ConnectAck {
        flags: 0,
        selected_version: ProtocolVersion::new(1, 0),
        capabilities: 0,
    })
}

type Script = Box<dyn FnOnce(&mut dyn ServerLink) -> TestResult<CapturedFrames> + Send>;
/// Canonical frame byte images a script captured from the client.
type CapturedFrames = Vec<Vec<u8>>;
/// Join handle for a running script thread.
type ScriptHandle = JoinHandle<TestResult<CapturedFrames>>;

/// Spawns a one-connection scripted server, returning its client address for
/// `kind` and a handle joining to the frames the script captured.
fn spawn_script(kind: TransportKind, script: Script) -> TestResult<(String, ScriptHandle)> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .map_err(|error| format!("script listener bind failed: {error}"))?;
    let port = listener
        .local_addr()
        .map_err(|error| format!("script listener addr failed: {error}"))?
        .port();
    let address = match kind {
        TransportKind::Tcp => format!("127.0.0.1:{port}"),
        TransportKind::Ws => format!("ws://127.0.0.1:{port}/liminal"),
    };
    let handle = std::thread::spawn(move || -> TestResult<CapturedFrames> {
        let (stream, _) = listener
            .accept()
            .map_err(|error| format!("script accept failed: {error}"))?;
        match kind {
            TransportKind::Tcp => {
                let mut link = TcpServerLink::new(stream)?;
                script(&mut link)
            }
            TransportKind::Ws => {
                let mut link = WsServerLink::accept(stream)?;
                script(&mut link)
            }
        }
    });
    Ok((address, handle))
}

fn remote_config(address: &str) -> TestResult<RemoteConfig> {
    RemoteConfig::new(
        address,
        CHANNEL,
        CONVERSATION,
        ConnectionPoolConfig::new(1, 10, 16),
    )
    .map_err(|error| format!("remote config failed: {error}"))
}

/// Connects the configured transport for `kind` with `AUTH_TOKEN`.
fn connect(kind: TransportKind, address: &str) -> Result<RemoteConfig, SdkError> {
    let config = RemoteConfig::new(
        address,
        CHANNEL,
        CONVERSATION,
        ConnectionPoolConfig::new(1, 10, 16),
    )?;
    match kind {
        TransportKind::Tcp => config.connect_tcp_with_auth(AUTH_TOKEN),
        TransportKind::Ws => config.connect_websocket_with_auth(AUTH_TOKEN),
    }
}

fn join_script(handle: ScriptHandle) -> TestResult<CapturedFrames> {
    handle
        .join()
        .map_err(|_| "script thread panicked".to_string())?
}

#[test]
fn frame_bound_is_the_named_product_limit() -> TestResult {
    let bound = liminal_ws_message_bound().map_err(|error| format!("bound failed: {error}"))?;
    let expected =
        usize::try_from(FRAME_MAX).map_err(|error| format!("bound must fit usize: {error}"))?;
    assert_eq!(bound, expected);
    Ok(())
}

#[test]
fn deliver_discriminant_matches_canonical_registry() {
    // The transport-neutral driver classifies deliveries from the frame-type
    // byte; this pins its constant against the canonical registry.
    assert_eq!(u8::from(FrameType::Deliver), 0x19);
}

#[test]
fn auth_success_parity() -> TestResult {
    for kind in [TransportKind::Tcp, TransportKind::Ws] {
        let (address, handle) = spawn_script(
            kind,
            Box::new(|link| {
                script_handshake(link, AUTH_TOKEN)?;
                Ok(link.captured_frames().to_vec())
            }),
        )?;
        connect(kind, &address).map_err(|error| format!("{kind:?} connect failed: {error}"))?;
        join_script(handle)?;
    }
    Ok(())
}

#[test]
fn auth_refusal_parity() -> TestResult {
    for kind in [TransportKind::Tcp, TransportKind::Ws] {
        let (address, handle) = spawn_script(
            kind,
            Box::new(|link| {
                script_handshake(link, b"a-different-token")?;
                Ok(Vec::new())
            }),
        )?;
        let result = connect(kind, &address);
        assert!(
            matches!(result, Err(SdkError::Connection { .. })),
            "{kind:?} auth refusal must be a typed connection error"
        );
        join_script(handle)?;
    }
    Ok(())
}

#[test]
fn publish_parity_with_cross_transport_byte_identity() -> TestResult {
    let mut captured_by_kind = Vec::new();
    for kind in [TransportKind::Tcp, TransportKind::Ws] {
        let (address, handle) = spawn_script(
            kind,
            Box::new(|link| {
                script_handshake(link, AUTH_TOKEN)?;
                let frame = link.read_frame()?;
                let Frame::Publish { stream_id, .. } = frame else {
                    return Err(format!("expected Publish, got {frame:?}"));
                };
                link.write_frame(&Frame::PublishAck {
                    flags: 0,
                    stream_id,
                    message_id: 42,
                })?;
                Ok(link.captured_frames().to_vec())
            }),
        )?;
        let config = connect(kind, &address).map_err(|error| format!("connect: {error}"))?;
        let handle_channel = liminal_sdk::RemoteChannelHandle::new(&config)
            .map_err(|error| format!("handle: {error}"))?;
        let response = handle_channel
            .publish(ParityMessage { value: 7 })
            .map_err(|error| format!("{kind:?} publish failed: {error}"))?;
        assert_eq!(response, PressureResponse::Accept);
        captured_by_kind.push(join_script(handle)?);
    }
    // The same `Frame` produced identical canonical bytes over both transports
    // (index 0 is the Connect frame, index 1 the Publish frame).
    assert_eq!(captured_by_kind[0], captured_by_kind[1]);
    Ok(())
}

#[test]
fn delivered_ack_parity() -> TestResult {
    for kind in [TransportKind::Tcp, TransportKind::Ws] {
        let (address, handle) = spawn_script(
            kind,
            Box::new(|link| {
                script_handshake(link, AUTH_TOKEN)?;
                let frame = link.read_frame()?;
                let Frame::Publish { stream_id, .. } = frame else {
                    return Err(format!("expected Publish, got {frame:?}"));
                };
                link.write_frame(&Frame::PublishAck {
                    flags: PUBLISH_DELIVERED_FLAG,
                    stream_id,
                    message_id: 43,
                })?;
                Ok(Vec::new())
            }),
        )?;
        let config = connect(kind, &address).map_err(|error| format!("connect: {error}"))?;
        let handle_channel = liminal_sdk::RemoteChannelHandle::new(&config)
            .map_err(|error| format!("handle: {error}"))?;
        let ack: DeliveryAck = handle_channel
            .publish_with_idempotency_key(&ParityMessage { value: 9 }, "key-9")
            .map_err(|error| format!("{kind:?} keyed publish failed: {error}"))?;
        assert!(ack.is_accepted(), "{kind:?} delivered ack must be genuine");
        join_script(handle)?;
    }
    Ok(())
}

/// Script serving one conversation request/reply exchange.
fn conversation_reply_script(link: &mut dyn ServerLink) -> TestResult<Vec<Vec<u8>>> {
    script_handshake(link, AUTH_TOKEN)?;
    let frame = link.read_frame()?;
    let Frame::ConversationOpen { .. } = frame else {
        return Err(format!("expected ConversationOpen, got {frame:?}"));
    };
    let frame = link.read_frame()?;
    let Frame::ConversationMessage {
        conversation_id,
        stream_id,
        envelope,
        ..
    } = frame
    else {
        return Err(format!("expected ConversationMessage, got {frame:?}"));
    };
    let reply = liminal::protocol::MessageEnvelope::new(
        envelope.schema_id,
        liminal::protocol::CausalContext::independent(),
        br#"{"value":11}"#.to_vec(),
    );
    link.write_frame(&Frame::ConversationMessage {
        flags: 0,
        stream_id,
        conversation_id,
        envelope: reply,
    })?;
    Ok(Vec::new())
}

#[test]
fn request_reply_parity() -> TestResult {
    for kind in [TransportKind::Tcp, TransportKind::Ws] {
        let (address, handle) = spawn_script(kind, Box::new(conversation_reply_script))?;
        let config = connect(kind, &address).map_err(|error| format!("connect: {error}"))?;
        let conversation = liminal_sdk::RemoteConversationHandle::new(&config);
        conversation
            .request(ParityMessage { value: 10 })
            .map_err(|error| format!("{kind:?} request failed: {error}"))?;
        let reply: ParityMessage = block_on(conversation.receive())
            .map_err(|error| format!("{kind:?} receive failed: {error}"))?;
        assert_eq!(reply, ParityMessage { value: 11 });
        join_script(handle)?;
    }
    Ok(())
}

#[test]
fn silent_send_parity() -> TestResult {
    for kind in [TransportKind::Tcp, TransportKind::Ws] {
        let (address, handle) = spawn_script(
            kind,
            Box::new(|link| {
                script_handshake(link, AUTH_TOKEN)?;
                let frame = link.read_frame()?;
                let Frame::ConversationOpen { .. } = frame else {
                    return Err(format!("expected ConversationOpen, got {frame:?}"));
                };
                let frame = link.read_frame()?;
                let Frame::ConversationMessage { .. } = frame else {
                    return Err(format!("expected ConversationMessage, got {frame:?}"));
                };
                // Success is silence on the conversation path: hold the
                // connection open through the client's brief error-drain
                // window so the silence is genuine, not a socket teardown.
                std::thread::sleep(Duration::from_millis(600));
                Ok(Vec::new())
            }),
        )?;
        let config = connect(kind, &address).map_err(|error| format!("connect: {error}"))?;
        let conversation = liminal_sdk::RemoteConversationHandle::new(&config);
        conversation
            .send(ParityMessage { value: 12 })
            .map_err(|error| format!("{kind:?} silent send failed: {error}"))?;
        join_script(handle)?;
    }
    Ok(())
}

#[test]
fn server_close_parity() -> TestResult {
    for kind in [TransportKind::Tcp, TransportKind::Ws] {
        let (address, handle) = spawn_script(
            kind,
            Box::new(|link| {
                script_handshake(link, AUTH_TOKEN)?;
                link.close()?;
                Ok(Vec::new())
            }),
        )?;
        let config = connect(kind, &address).map_err(|error| format!("connect: {error}"))?;
        let handle_channel = liminal_sdk::RemoteChannelHandle::new(&config)
            .map_err(|error| format!("handle: {error}"))?;
        join_script(handle)?;
        let result = handle_channel.publish(ParityMessage { value: 13 });
        assert!(
            matches!(result, Err(SdkError::Connection { .. })),
            "{kind:?} publish after server close must be a typed connection error, got {result:?}"
        );
    }
    Ok(())
}

#[test]
fn malformed_message_parity() -> TestResult {
    // An unknown frame type in the response position must be a typed protocol
    // failure on both transports.
    let garbage: Vec<u8> = {
        let mut bytes = vec![0xFF, 0x00];
        bytes.extend_from_slice(&1_u32.to_be_bytes());
        bytes.extend_from_slice(&0_u32.to_be_bytes());
        bytes
    };
    for kind in [TransportKind::Tcp, TransportKind::Ws] {
        let garbage = garbage.clone();
        let (address, handle) = spawn_script(
            kind,
            Box::new(move |link| {
                script_handshake(link, AUTH_TOKEN)?;
                let _publish = link.read_frame()?;
                link.write_raw_message(&garbage)?;
                Ok(Vec::new())
            }),
        )?;
        let config = connect(kind, &address).map_err(|error| format!("connect: {error}"))?;
        let handle_channel = liminal_sdk::RemoteChannelHandle::new(&config)
            .map_err(|error| format!("handle: {error}"))?;
        let result = handle_channel.publish(ParityMessage { value: 14 });
        assert!(
            matches!(result, Err(SdkError::Protocol { .. })),
            "{kind:?} malformed response must be a typed protocol error, got {result:?}"
        );
        join_script(handle)?;
    }
    Ok(())
}

/// Script serving one subscription with two deliveries.
fn subscription_script(link: &mut dyn ServerLink) -> TestResult<Vec<Vec<u8>>> {
    script_handshake(link, &[])?;
    let frame = link.read_frame()?;
    let Frame::Subscribe { stream_id, .. } = frame else {
        return Err(format!("expected Subscribe, got {frame:?}"));
    };
    link.write_frame(&Frame::SubscribeAck {
        flags: 0,
        stream_id,
        subscription_id: 77,
        selected_schema: liminal::protocol::SchemaId::new([1; 32]),
    })?;
    for (seq, payload) in [(1_u64, b"first".to_vec()), (2, b"second".to_vec())] {
        let envelope = liminal::protocol::MessageEnvelope::new(
            liminal::protocol::SchemaId::new([1; 32]),
            liminal::protocol::CausalContext::independent(),
            payload,
        );
        link.write_frame(&Frame::Deliver {
            flags: 0,
            stream_id,
            delivery_seq: seq,
            envelope,
        })?;
    }
    Ok(Vec::new())
}

#[test]
fn subscription_delivery_parity() -> TestResult {
    let mut deliveries_by_kind: Vec<Vec<(u64, Vec<u8>)>> = Vec::new();
    for kind in [TransportKind::Tcp, TransportKind::Ws] {
        let (address, handle) = spawn_script(kind, Box::new(subscription_script))?;
        let received = match kind {
            TransportKind::Tcp => {
                let stream = SubscriptionStream::open(&address, CHANNEL, Vec::new())
                    .map_err(|error| format!("tcp subscription open failed: {error}"))?;
                assert_eq!(stream.subscription_id(), 77);
                let mut received = Vec::new();
                for _ in 0..2 {
                    let message = stream
                        .recv_timeout(RECV_TIMEOUT)
                        .map_err(|error| format!("tcp delivery receive failed: {error}"))?;
                    received.push((message.delivery_seq(), message.into_payload()));
                }
                received
            }
            TransportKind::Ws => {
                let stream = liminal_sdk::remote::websocket::WebSocketSubscriptionStream::open(
                    &address,
                    CHANNEL,
                    Vec::new(),
                )
                .map_err(|error| format!("ws subscription open failed: {error}"))?;
                assert_eq!(stream.subscription_id(), 77);
                let mut received = Vec::new();
                for _ in 0..2 {
                    let message = stream
                        .recv_timeout(RECV_TIMEOUT)
                        .map_err(|error| format!("ws delivery receive failed: {error}"))?;
                    received.push((message.delivery_seq(), message.into_payload()));
                }
                received
            }
        };
        deliveries_by_kind.push(received);
        join_script(handle)?;
    }
    assert_eq!(deliveries_by_kind[0], deliveries_by_kind[1]);
    Ok(())
}

#[test]
fn resume_parity() -> TestResult {
    // Neither v1 transport supports the resume path over the wire; both must
    // surface the same typed refusal class rather than silently succeeding.
    for kind in [TransportKind::Tcp, TransportKind::Ws] {
        let (address, handle) = spawn_script(
            kind,
            Box::new(|link| {
                script_handshake(link, AUTH_TOKEN)?;
                Ok(Vec::new())
            }),
        )?;
        let config = connect(kind, &address).map_err(|error| format!("connect: {error}"))?;
        join_script(handle)?;
        let handle_channel = liminal_sdk::RemoteChannelHandle::new(&config)
            .map_err(|error| format!("handle: {error}"))?;
        let subscription_id = handle_channel
            .track_subscription()
            .map_err(|error| format!("track failed: {error}"))?;
        handle_channel
            .acknowledge(subscription_id, 5)
            .map_err(|error| format!("acknowledge failed: {error}"))?;
        let mut jitter = liminal_sdk::remote::NoJitter;
        handle_channel
            .reconnect(&mut jitter)
            .map_err(|error| format!("lifecycle reconnect failed: {error}"))?;
        let result = handle_channel.connected();
        assert!(
            matches!(result, Err(SdkError::Protocol { .. })),
            "{kind:?} resume must be the typed unsupported refusal, got {result:?}"
        );
    }
    Ok(())
}

#[test]
fn ws_reconnect_traverses_typed_permit_path() -> TestResult {
    use liminal_protocol::outcome::ReconnectState;

    // Phase one: connect and observe the server close the connection.
    let (address, handle) = spawn_script(
        TransportKind::Ws,
        Box::new(|link| {
            script_handshake(link, AUTH_TOKEN)?;
            link.close()?;
            Ok(Vec::new())
        }),
    )?;
    let config = connect(TransportKind::Ws, &address)
        .map_err(|error| format!("ws connect failed: {error}"))?;
    let transport = config
        .websocket_transport()
        .ok_or("a websocket-connected config must expose its transport")?;
    let channel = liminal_sdk::RemoteChannelHandle::new(&config)
        .map_err(|error| format!("handle failed: {error}"))?;
    assert_eq!(transport.reconnect_state(), ReconnectState::Online);
    join_script(handle)?;

    // The loss surfaces on the next exchange as a typed error, and the client
    // unit holds exactly one retained permit — no timer, no automatic retry.
    let probe = channel.publish(ParityMessage { value: 30 });
    assert!(
        matches!(probe, Err(SdkError::Connection { .. })),
        "exchange after server close must be a typed connection error, got {probe:?}"
    );
    assert_eq!(
        transport.reconnect_state(),
        ReconnectState::PermitOutstanding
    );

    // Phase two: a fresh scripted server on the same port cannot be guaranteed,
    // so reconnect against a new address is refused — the permit is bound to
    // this transport's address; instead restart a listener script and point the
    // same transport at it through its recorded address.
    let listener = TcpListener::bind(
        address
            .trim_start_matches("ws://")
            .split('/')
            .next()
            .ok_or("address must have an authority")?,
    );
    let Ok(listener) = listener else {
        // The ephemeral port can be reused only when free; when the OS refuses,
        // the typed-permit observation above has already proven the R2.2 path,
        // and redemption is covered by the binding tests.
        return Ok(());
    };
    let script_handle = std::thread::spawn(move || -> TestResult<()> {
        let (stream, _) = listener
            .accept()
            .map_err(|error| format!("script accept failed: {error}"))?;
        let mut link = WsServerLink::accept(stream)?;
        script_handshake(&mut link, AUTH_TOKEN)?;
        Ok(())
    });
    transport
        .reconnect()
        .map_err(|error| format!("typed reconnect failed: {error}"))?;
    assert_eq!(transport.reconnect_state(), ReconnectState::Online);
    script_handle
        .join()
        .map_err(|_| "reconnect script panicked".to_string())??;
    Ok(())
}

#[test]
fn f2_oversize_declared_message_fails_at_pinned_bound() -> TestResult {
    use std::io::Write;

    // The server declares a WebSocket binary message longer than the liminal
    // frame bound. The client must fail from the DECLARED length at the pinned
    // bound — never after allocating the library's 64 MiB default buffer. The
    // declared body is never sent, so a pass proves the refusal used the
    // declaration alone.
    let listener = TcpListener::bind("127.0.0.1:0")
        .map_err(|error| format!("oversize listener bind failed: {error}"))?;
    let port = listener
        .local_addr()
        .map_err(|error| format!("oversize listener addr failed: {error}"))?
        .port();
    let address = format!("ws://127.0.0.1:{port}/liminal");
    let script = std::thread::spawn(move || -> TestResult<()> {
        let (stream, _) = listener
            .accept()
            .map_err(|error| format!("oversize accept failed: {error}"))?;
        let mut link = WsServerLink::accept(stream)?;
        script_handshake(&mut link, AUTH_TOKEN)?;
        let _publish = link.read_frame()?;
        // Raw RFC 6455 frame head: FIN + binary opcode, unmasked, 64-bit
        // extended length declaring one byte more than the liminal frame bound.
        let mut head = vec![0x82_u8, 127];
        head.extend_from_slice(&(FRAME_MAX + 1).to_be_bytes());
        let stream = link.socket.get_mut();
        stream
            .write_all(&head)
            .map_err(|error| format!("oversize head write failed: {error}"))?;
        stream
            .flush()
            .map_err(|error| format!("oversize head flush failed: {error}"))?;
        // Hold the socket open until the client refuses, so the failure cannot
        // be a connection teardown racing the declared length.
        std::thread::sleep(Duration::from_millis(500));
        Ok(())
    });

    let config = connect(TransportKind::Ws, &address).map_err(|error| format!("{error}"))?;
    let handle_channel =
        liminal_sdk::RemoteChannelHandle::new(&config).map_err(|error| format!("{error}"))?;
    let result = handle_channel.publish(ParityMessage { value: 1 });
    assert!(
        matches!(result, Err(SdkError::Protocol { .. })),
        "oversize-declared message must be a typed protocol failure at the bound, got {result:?}"
    );
    script
        .join()
        .map_err(|_| "oversize script panicked".to_string())??;
    Ok(())
}

#[test]
fn handle_traverses_trait_object_for_both_transports() -> TestResult {
    fn exercise<H: ChannelHandle>(handle: &H) -> Result<PressureResponse, SdkError> {
        handle.publish(ParityMessage { value: 21 })
    }

    for kind in [TransportKind::Tcp, TransportKind::Ws] {
        let (address, handle) = spawn_script(
            kind,
            Box::new(|link| {
                script_handshake(link, AUTH_TOKEN)?;
                let frame = link.read_frame()?;
                let Frame::Publish { stream_id, .. } = frame else {
                    return Err(format!("expected Publish, got {frame:?}"));
                };
                link.write_frame(&Frame::PublishAck {
                    flags: 0,
                    stream_id,
                    message_id: 44,
                })?;
                Ok(Vec::new())
            }),
        )?;
        let config = connect(kind, &address).map_err(|error| format!("connect: {error}"))?;
        let channel = liminal_sdk::RemoteChannelHandle::new(&config)
            .map_err(|error| format!("handle: {error}"))?;
        let response =
            exercise(&channel).map_err(|error| format!("{kind:?} publish failed: {error}"))?;
        assert_eq!(response, PressureResponse::Accept);
        join_script(handle)?;
    }
    Ok(())
}

#[test]
fn non_ws_scheme_is_typed_refusal() -> TestResult {
    let config = remote_config("wss://127.0.0.1:1/liminal")?;
    let result = config.connect_websocket();
    assert!(
        matches!(result, Err(SdkError::Connection { .. })),
        "wss must be refused typed (TLS is owned by the named proxy), got an unexpected result"
    );
    let config = remote_config("127.0.0.1:1")?;
    let result = config.connect_websocket();
    assert!(
        matches!(result, Err(SdkError::Connection { .. })),
        "a non-ws scheme must be refused typed"
    );
    Ok(())
}

// ---- LP-WS-TRANSPORT integration pass: participant transport parity ----

use liminal_protocol::wire::{
    AttachSecret, BindingEpoch, ClientRequest, ConnectionIncarnation, EnrollBound,
    EnrollmentRequest, EnrollmentToken, Generation, PARTICIPANT_FRAME_TYPE, ParticipantFrame,
    ReceiverDirection, ServerValue,
};
use liminal_sdk::{
    ParticipantResumeStore, RemoteOperationRecordOutcome, RemoteParticipantHandle,
    RemoteParticipantInbound, RemoteParticipantSendOutcome, RemoteReconnectAttemptOutcome,
    RemoteReconnectPermitOutcome,
};

const PARTICIPANT_CONVERSATION: u64 = 21;
const PARTICIPANT_ID: u64 = 22;

/// Minimal durable store for participant fixtures: retains the latest bytes.
#[derive(Debug, Default)]
struct MemoryResumeStore {
    committed: Vec<u8>,
}

impl ParticipantResumeStore for MemoryResumeStore {
    fn persist(&mut self, canonical_lpcr: &[u8]) -> Result<(), SdkError> {
        self.committed = canonical_lpcr.to_vec();
        Ok(())
    }
}

/// Encodes one participant frame into its complete canonical byte image.
fn participant_frame_bytes(frame: &ParticipantFrame) -> TestResult<Vec<u8>> {
    let needed = liminal_protocol::wire::encoded_len(frame)
        .map_err(|error| format!("participant encoded_len failed: {error:?}"))?;
    let mut bytes = vec![0_u8; needed];
    let written = liminal_protocol::wire::encode(frame, &mut bytes)
        .map_err(|error| format!("participant encode failed: {error:?}"))?;
    bytes.truncate(written);
    Ok(bytes)
}

/// Server side of one participant enrollment exchange: verifies the request
/// and answers with the correlated `EnrollBound`.
fn participant_enrollment_script(link: &mut dyn ServerLink) -> TestResult<Vec<Vec<u8>>> {
    script_handshake(link, AUTH_TOKEN)?;
    let frame = link.read_frame()?;
    let Frame::Unknown {
        type_id, payload, ..
    } = frame
    else {
        return Err(format!("expected a participant frame, got {frame:?}"));
    };
    if type_id != PARTICIPANT_FRAME_TYPE {
        return Err(format!("expected participant type 0x1A, got {type_id:#x}"));
    }
    let mut complete = vec![type_id, 0];
    complete.extend_from_slice(&0_u32.to_be_bytes());
    let length = u32::try_from(payload.len()).map_err(|_| "payload length".to_string())?;
    complete.extend_from_slice(&length.to_be_bytes());
    complete.extend_from_slice(&payload);
    let decoded = liminal_protocol::wire::decode(&complete, ReceiverDirection::Server)
        .map_err(|error| format!("participant request decode failed: {error:?}"))?;
    let ParticipantFrame::ClientRequest(ClientRequest::Enrollment(request)) = decoded else {
        return Err(format!("expected an enrollment request, got {decoded:?}"));
    };
    let generation = Generation::new(1).ok_or("generation 1 must construct")?;
    let epoch = BindingEpoch::new(ConnectionIncarnation::new(3, 4), generation);
    let bound = EnrollBound::new(
        request.conversation_id,
        request.enrollment_token,
        PARTICIPANT_ID,
        AttachSecret::new([9; 32]),
        epoch,
        100,
        200,
    )
    .ok_or("generation-1 enroll bound must construct")?;
    let response = participant_frame_bytes(&ParticipantFrame::ServerValue(ServerValue::EnrollBound(
        bound,
    )))?;
    link.write_raw_message(&response)?;
    Ok(link.captured_frames().to_vec())
}

#[test]
fn participant_send_receive_parity() -> TestResult {
    let mut captured_by_kind = Vec::new();
    for kind in [TransportKind::Tcp, TransportKind::Ws] {
        let (address, handle) = spawn_script(kind, Box::new(participant_enrollment_script))?;
        let config = connect(kind, &address).map_err(|error| format!("connect: {error}"))?;
        let participant = RemoteParticipantHandle::new(&config, MemoryResumeStore::default())
            .map_err(|error| format!("participant handle failed: {error}"))?;

        let request = ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: PARTICIPANT_CONVERSATION,
            enrollment_token: EnrollmentToken::new([7; 16]),
        });
        let RemoteOperationRecordOutcome::Recorded(operation) = participant
            .record_operation(request)
            .map_err(|error| format!("record failed: {error}"))?
        else {
            return Err(format!("{kind:?} enrollment must be recorded"));
        };
        let outcome = participant
            .send_operation(operation)
            .map_err(|error| format!("send failed: {error}"))?;
        let RemoteParticipantSendOutcome::Sent { provenance } = outcome else {
            return Err(format!("{kind:?} participant send must succeed: {outcome:?}"));
        };
        assert_eq!(provenance.connection_id(), 1, "{kind:?} first connection");
        assert_eq!(provenance.attempt_id(), 1, "{kind:?} first attempt");

        let inbound = participant
            .receive()
            .map_err(|error| format!("{kind:?} participant receive failed: {error}"))?;
        let RemoteParticipantInbound::Applied { value, provenance } = inbound else {
            return Err(format!(
                "{kind:?} correlated enroll bound must apply: {inbound:?}"
            ));
        };
        assert!(matches!(value, ServerValue::EnrollBound(_)));
        assert_eq!(provenance.connection_id(), 1);
        assert_eq!(provenance.attempt_id(), 1);
        captured_by_kind.push(join_script(handle)?);
    }
    // The same participant request produced identical canonical bytes over
    // both transports (index 0 Connect, index 1 the participant frame).
    assert_eq!(captured_by_kind[0], captured_by_kind[1]);
    Ok(())
}

/// Runs one accepted connection's handshake for the reconnect fixture.
fn reconnect_phase(kind: TransportKind, listener: TcpListener, close: bool) -> ScriptHandle {
    std::thread::spawn(move || -> TestResult<CapturedFrames> {
        let (stream, _) = listener
            .accept()
            .map_err(|error| format!("phase accept failed: {error}"))?;
        drop(listener);
        match kind {
            TransportKind::Tcp => {
                let mut link = TcpServerLink::new(stream)?;
                script_handshake(&mut link, AUTH_TOKEN)?;
                if close {
                    link.close()?;
                }
                Ok(Vec::new())
            }
            TransportKind::Ws => {
                let mut link = WsServerLink::accept(stream)?;
                script_handshake(&mut link, AUTH_TOKEN)?;
                if close {
                    link.close()?;
                }
                Ok(Vec::new())
            }
        }
    })
}

#[test]
fn participant_reconnect_provenance_parity() -> TestResult {
    for kind in [TransportKind::Tcp, TransportKind::Ws] {
        let listener = TcpListener::bind("127.0.0.1:0")
            .map_err(|error| format!("listener bind failed: {error}"))?;
        let local = listener
            .local_addr()
            .map_err(|error| format!("listener addr failed: {error}"))?;
        let address = match kind {
            TransportKind::Tcp => local.to_string(),
            TransportKind::Ws => format!("ws://{local}/liminal"),
        };
        let phase_one = reconnect_phase(kind, listener, true);
        let config = connect(kind, &address).map_err(|error| format!("connect: {error}"))?;
        phase_one
            .join()
            .map_err(|_| "phase-one script panicked".to_string())??;

        let participant = RemoteParticipantHandle::new(&config, MemoryResumeStore::default())
            .map_err(|error| format!("participant handle failed: {error}"))?;
        let loss = participant
            .record_established_transport_loss()
            .map_err(|error| format!("loss record failed: {error}"))?;
        let RemoteReconnectPermitOutcome::Permitted { permit, .. } = loss.reconnect else {
            return Err(format!(
                "{kind:?} transport loss must mint one reconnect permit"
            ));
        };

        // The transports dial their recorded address, so the second listener
        // rebinds the same port before the typed reconnect runs.
        let listener = TcpListener::bind(local)
            .map_err(|error| format!("phase-two rebind failed: {error}"))?;
        let phase_two = reconnect_phase(kind, listener, false);
        let outcome = participant
            .reconnect(permit)
            .map_err(|error| format!("reconnect failed: {error}"))?;
        let RemoteReconnectAttemptOutcome::Connected { provenance } = outcome else {
            return Err(format!("{kind:?} typed reconnect must connect: {outcome:?}"));
        };
        assert_eq!(provenance.connection_id(), 2, "{kind:?} second connection");
        assert_eq!(provenance.attempt_id(), 2, "{kind:?} second attempt");
        phase_two
            .join()
            .map_err(|_| "phase-two script panicked".to_string())??;
    }
    Ok(())
}
