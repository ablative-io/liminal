//! LP-WS-TRANSPORT R1 end-to-end evidence: the sibling WebSocket acceptor
//! carries the canonical liminal wire protocol with byte identity and
//! behavioral parity against the TCP reference transport.
//!
//! Covers: F1 (extension offers declined, never negotiated), F2 (declared
//! lengths beyond the pinned liminal frame bound refused pre-allocation), F6
//! (the four origin cases, fail-closed allow-list), the one-binary-message-
//! one-canonical-frame contract pins (text, empty, trailing, concatenated,
//! truncated, fragmented-binary reassembly), Q-A keepalive (named config,
//! absent means disabled, transport pings mint nothing), shared §5 admission,
//! push/reply, subscribe/deliver, auth refusal parity, client close cleanup,
//! and graceful drain/forced shutdown through the shared supervisor.

use std::error::Error;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use liminal::protocol::{
    CausalContext, Frame, MessageEnvelope, ProtocolVersion, SchemaId, decode, encode, encoded_len,
};
use liminal_server::config::{
    AuthConfig, ChannelDef, LimitsConfig, ServerConfig, ServicesConfig, WebSocketConfig,
};
use liminal_server::server::connection::{ConnectionSupervisor, WebSocketListener};
use liminal_server::server::listener::ServerListener;
use liminal_server::server::shutdown::run_shutdown_sequence;

use tungstenite::Message;
use tungstenite::client::IntoClientRequest;
use tungstenite::protocol::WebSocket;

const CHANNEL: &str = "events";
const PATH: &str = "/liminal";
const ALLOWED_ORIGIN: &str = "https://app.example.com";
const DEADLINE: Duration = Duration::from_secs(5);

// ---- harness ----

struct RunningServer {
    tcp: ServerListener,
    ws: Option<WebSocketListener>,
    supervisor: ConnectionSupervisor,
    tcp_addr: SocketAddr,
    ws_addr: SocketAddr,
}

struct ServerOptions {
    auth_token: Option<String>,
    max_connections: usize,
    ping_interval_ms: Option<u64>,
    allowed_origins: Vec<String>,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            auth_token: None,
            max_connections: LimitsConfig::default().max_connections,
            ping_interval_ms: None,
            allowed_origins: vec![ALLOWED_ORIGIN.to_owned()],
        }
    }
}

impl RunningServer {
    fn start(options: ServerOptions) -> Result<Self, Box<dyn Error>> {
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
            auth: options.auth_token.clone().map(|token| AuthConfig { token }),
            services: ServicesConfig::default(),
            limits: LimitsConfig {
                max_connections: options.max_connections,
                ..LimitsConfig::default()
            },
            websocket: None,
            participant: None,
        };
        let supervisor = ConnectionSupervisor::from_config(&config)?;
        let tcp = ServerListener::bind(&config, supervisor.clone())?;
        let ws_config = WebSocketConfig {
            listen_address: "127.0.0.1:0".parse()?,
            path: PATH.to_owned(),
            allowed_origins: options.allowed_origins,
            ping_interval_ms: options.ping_interval_ms,
        };
        let ws = WebSocketListener::bind(&ws_config, supervisor.clone())?;
        let tcp_addr = tcp.local_addr();
        let ws_addr = ws.local_addr();
        Ok(Self {
            tcp,
            ws: Some(ws),
            supervisor,
            tcp_addr,
            ws_addr,
        })
    }

    fn wait_for_active(&self, expected: usize) -> Result<(), Box<dyn Error>> {
        let deadline = Instant::now() + DEADLINE;
        loop {
            let _ = self.supervisor.reap_crashed_connections();
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
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

fn encode_frame(frame: &Frame) -> Result<Vec<u8>, Box<dyn Error>> {
    let len = encoded_len(frame).map_err(|error| format!("encoded_len: {error}"))?;
    let mut bytes = vec![0_u8; len];
    let written = encode(frame, &mut bytes).map_err(|error| format!("encode: {error}"))?;
    bytes.truncate(written);
    Ok(bytes)
}

fn connect_frame(token: &[u8]) -> Frame {
    Frame::Connect {
        flags: 0,
        min_version: ProtocolVersion::new(1, 0),
        max_version: ProtocolVersion::new(1, 0),
        auth_token: token.to_vec(),
    }
}

const fn envelope(payload: Vec<u8>) -> MessageEnvelope {
    MessageEnvelope::new(
        SchemaId::new([0_u8; SchemaId::WIRE_LEN]),
        CausalContext::independent(),
        payload,
    )
}

fn json_payload(byte: u8, size: usize) -> Vec<u8> {
    let mut payload = Vec::with_capacity(size + 2);
    payload.push(b'"');
    payload.resize(size + 1, byte);
    payload.push(b'"');
    payload
}

// ---- raw TCP client (byte-capture reference) ----

fn tcp_send_and_capture_response(
    address: SocketAddr,
    frame: &Frame,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut stream = TcpStream::connect(address)?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_millis(200)))?;
    stream.write_all(&encode_frame(frame)?)?;
    stream.flush()?;
    read_one_frame_bytes(&mut stream)
}

/// Reads exactly one complete canonical frame's bytes off a raw TCP socket.
fn read_one_frame_bytes(stream: &mut TcpStream) -> Result<Vec<u8>, Box<dyn Error>> {
    let deadline = Instant::now() + DEADLINE;
    let mut buffer: Vec<u8> = Vec::new();
    loop {
        if let Ok((_, consumed)) = decode(&buffer) {
            return Ok(buffer.drain(..consumed).collect());
        }
        if Instant::now() >= deadline {
            return Err("timed out reading a frame".into());
        }
        let mut chunk = [0_u8; 8192];
        match stream.read(&mut chunk) {
            Ok(0) => return Err("connection closed while reading a frame".into()),
            Ok(read) => buffer.extend_from_slice(chunk.get(..read).unwrap_or(&[])),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(error) => return Err(error.into()),
        }
    }
}

// ---- WebSocket client helpers ----

fn ws_connect(
    address: SocketAddr,
    origin: Option<&str>,
) -> Result<WebSocket<TcpStream>, Box<dyn Error>> {
    let stream = TcpStream::connect(address)?;
    stream.set_nodelay(true)?;
    // Generous timeout for the handshake; tightened to a poll quantum after.
    stream.set_read_timeout(Some(DEADLINE))?;
    let mut request = format!("ws://{address}{PATH}").into_client_request()?;
    if let Some(origin) = origin {
        request.headers_mut().insert("Origin", origin.parse()?);
    }
    let (socket, response) = tungstenite::client::client(request, stream)
        .map_err(|error| format!("websocket client handshake failed: {error}"))?;
    socket
        .get_ref()
        .set_read_timeout(Some(Duration::from_millis(200)))?;
    // F1: the response must never carry a negotiated extension or subprotocol.
    if response.headers().get("Sec-WebSocket-Extensions").is_some() {
        return Err("server negotiated a websocket extension".into());
    }
    if response.headers().get("Sec-WebSocket-Protocol").is_some() {
        return Err("server negotiated a websocket subprotocol".into());
    }
    Ok(socket)
}

/// Reads the next BINARY message, skipping transport Ping/Pong control.
fn ws_read_binary(socket: &mut WebSocket<TcpStream>) -> Result<Vec<u8>, Box<dyn Error>> {
    let deadline = Instant::now() + DEADLINE;
    loop {
        match socket.read() {
            Ok(Message::Binary(bytes)) => return Ok(bytes.to_vec()),
            Ok(Message::Ping(_) | Message::Pong(_)) => {}
            Ok(other) => return Err(format!("unexpected websocket message: {other:?}").into()),
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                if Instant::now() >= deadline {
                    return Err("timed out reading a websocket message".into());
                }
            }
            Err(error) => return Err(format!("websocket read failed: {error}").into()),
        }
    }
}

fn ws_send_frame(socket: &mut WebSocket<TcpStream>, frame: &Frame) -> Result<(), Box<dyn Error>> {
    socket.send(Message::Binary(encode_frame(frame)?.into()))?;
    Ok(())
}

/// Completes the liminal Connect handshake over an upgraded websocket.
fn ws_liminal_connect(
    socket: &mut WebSocket<TcpStream>,
    token: &[u8],
) -> Result<(), Box<dyn Error>> {
    ws_send_frame(socket, &connect_frame(token))?;
    let bytes = ws_read_binary(socket)?;
    let (frame, consumed) = decode(&bytes)?;
    if consumed != bytes.len() {
        return Err("connect ack message carried trailing bytes".into());
    }
    match frame {
        Frame::ConnectAck { .. } => Ok(()),
        other => Err(format!("expected ConnectAck, got {:?}", other.frame_type()).into()),
    }
}

/// Asserts the server tears the websocket down (close frame, EOF, or reset).
fn ws_expect_server_close(socket: &mut WebSocket<TcpStream>) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + DEADLINE;
    loop {
        match socket.read() {
            Ok(Message::Close(_)) | Err(tungstenite::Error::ConnectionClosed) => return Ok(()),
            Ok(Message::Ping(_) | Message::Pong(_)) => {}
            Ok(other) => {
                return Err(format!("expected close, got message: {other:?}").into());
            }
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                if Instant::now() >= deadline {
                    return Err("timed out waiting for the server to close".into());
                }
            }
            Err(tungstenite::Error::Protocol(_) | tungstenite::Error::Io(_)) => return Ok(()),
            Err(error) => return Err(format!("unexpected websocket error: {error}").into()),
        }
    }
}

// ---- raw upgrade helpers (handshake-level assertions) ----

fn raw_upgrade_request(address: SocketAddr, path: &str, extra_headers: &str) -> String {
    format!(
        "GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\
         Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
         {extra_headers}\r\n"
    )
}

/// Sends raw bytes and returns the full HTTP response head the server wrote.
fn raw_http_exchange(address: SocketAddr, request: &str) -> Result<String, Box<dyn Error>> {
    let mut stream = TcpStream::connect(address)?;
    stream.set_read_timeout(Some(DEADLINE))?;
    stream.write_all(request.as_bytes())?;
    stream.flush()?;
    let mut response = Vec::new();
    let mut chunk = [0_u8; 4096];
    let deadline = Instant::now() + DEADLINE;
    while !response.windows(4).any(|window| window == b"\r\n\r\n") {
        if Instant::now() >= deadline {
            return Err("timed out reading the upgrade response".into());
        }
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(read) => response.extend_from_slice(chunk.get(..read).unwrap_or(&[])),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(String::from_utf8_lossy(&response).into_owned())
}

/// Reads an HTTP response head (through the blank line) from a raw stream.
fn read_response_head(stream: &mut TcpStream) -> Result<String, Box<dyn Error>> {
    let mut response = Vec::new();
    let mut chunk = [0_u8; 4096];
    let deadline = Instant::now() + DEADLINE;
    while !response.windows(4).any(|window| window == b"\r\n\r\n") {
        if Instant::now() >= deadline {
            return Err("timed out reading the response head".into());
        }
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(read) => response.extend_from_slice(chunk.get(..read).unwrap_or(&[])),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(String::from_utf8_lossy(&response).into_owned())
}

// ---- R1.2: cross-transport byte identity ----

#[test]
fn ws_and_tcp_connect_responses_are_byte_identical() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start(ServerOptions::default())?;

    let tcp_bytes = tcp_send_and_capture_response(server.tcp_addr, &connect_frame(&[]))?;

    let mut socket = ws_connect(server.ws_addr, None)?;
    ws_send_frame(&mut socket, &connect_frame(&[]))?;
    let ws_bytes = ws_read_binary(&mut socket)?;

    assert_eq!(
        tcp_bytes, ws_bytes,
        "the same Connect frame must produce byte-identical responses on both transports"
    );
    Ok(())
}

#[test]
fn ws_and_tcp_auth_refusals_are_byte_identical_and_both_close() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start(ServerOptions {
        auth_token: Some("correct-horse".to_owned()),
        ..ServerOptions::default()
    })?;

    let tcp_bytes = tcp_send_and_capture_response(server.tcp_addr, &connect_frame(b"wrong"))?;
    let (tcp_frame, _) = decode(&tcp_bytes)?;
    assert!(
        matches!(tcp_frame, Frame::ConnectError { .. }),
        "expected a ConnectError over TCP, got {:?}",
        tcp_frame.frame_type()
    );

    let mut socket = ws_connect(server.ws_addr, None)?;
    ws_send_frame(&mut socket, &connect_frame(b"wrong"))?;
    let ws_bytes = ws_read_binary(&mut socket)?;
    assert_eq!(
        tcp_bytes, ws_bytes,
        "auth refusal bytes must match across transports"
    );
    ws_expect_server_close(&mut socket)?;
    server.wait_for_active(0)?;
    Ok(())
}

// ---- R1.3: same application seam, subscribe/deliver/publish parity ----

#[test]
fn ws_subscriber_receives_tcp_publish_as_one_binary_message() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start(ServerOptions::default())?;

    let mut subscriber = ws_connect(server.ws_addr, Some(ALLOWED_ORIGIN))?;
    ws_liminal_connect(&mut subscriber, &[])?;
    ws_send_frame(
        &mut subscriber,
        &Frame::Subscribe {
            flags: 0,
            stream_id: 1,
            channel: CHANNEL.to_owned(),
            accepted_schemas: Vec::new(),
            max_in_flight: 1024,
        },
    )?;
    let ack_bytes = ws_read_binary(&mut subscriber)?;
    let (ack, _) = decode(&ack_bytes)?;
    assert!(
        matches!(ack, Frame::SubscribeAck { .. }),
        "expected SubscribeAck, got {:?}",
        ack.frame_type()
    );

    let payload = json_payload(b'w', 64);
    let mut publisher = TcpStream::connect(server.tcp_addr)?;
    publisher.set_read_timeout(Some(Duration::from_millis(200)))?;
    publisher.write_all(&encode_frame(&connect_frame(&[]))?)?;
    let _connect_ack = read_one_frame_bytes(&mut publisher)?;
    publisher.write_all(&encode_frame(&Frame::new_publish(
        1,
        CHANNEL,
        envelope(payload.clone()),
    )?)?)?;
    let publish_ack = read_one_frame_bytes(&mut publisher)?;
    let (publish_ack, _) = decode(&publish_ack)?;
    assert!(
        matches!(publish_ack, Frame::PublishAck { .. }),
        "expected PublishAck, got {:?}",
        publish_ack.frame_type()
    );

    let deliver_bytes = ws_read_binary(&mut subscriber)?;
    let (deliver, consumed) = decode(&deliver_bytes)?;
    assert_eq!(
        consumed,
        deliver_bytes.len(),
        "a Deliver must arrive as exactly one canonical frame per binary message"
    );
    match deliver {
        Frame::Deliver {
            delivery_seq,
            envelope: delivered,
            ..
        } => {
            assert_eq!(delivery_seq, 1);
            assert_eq!(delivered.payload, payload);
        }
        other => return Err(format!("expected Deliver, got {:?}", other.frame_type()).into()),
    }
    Ok(())
}

#[test]
fn ws_liminal_ping_gets_liminal_pong_not_transport_pong() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start(ServerOptions::default())?;
    let mut socket = ws_connect(server.ws_addr, None)?;
    ws_liminal_connect(&mut socket, &[])?;
    ws_send_frame(&mut socket, &Frame::new_ping(0)?)?;
    let bytes = ws_read_binary(&mut socket)?;
    let (frame, _) = decode(&bytes)?;
    assert!(
        matches!(frame, Frame::Pong { .. }),
        "liminal Ping must be answered by a liminal Pong frame in a binary message"
    );
    Ok(())
}

// ---- R1.3: server push / correlated reply over the shared supervisor ----

#[test]
fn ws_connection_serves_server_push_and_correlated_reply() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start(ServerOptions::default())?;
    let mut socket = ws_connect(server.ws_addr, None)?;
    ws_liminal_connect(&mut socket, &[])?;
    server.wait_for_active(1)?;

    let pids = server.supervisor.active_connection_pids();
    let pid = *pids.first().ok_or("no active connection pid")?;
    let awaiter = server
        .supervisor
        .push_to_connection(pid, b"push-payload".to_vec())?;

    let push_bytes = ws_read_binary(&mut socket)?;
    let (push, _) = decode(&push_bytes)?;
    let correlation_id = match push {
        Frame::Push {
            correlation_id,
            payload,
            ..
        } => {
            assert_eq!(payload, b"push-payload");
            correlation_id
        }
        other => return Err(format!("expected Push, got {:?}", other.frame_type()).into()),
    };
    ws_send_frame(
        &mut socket,
        &Frame::PushReply {
            flags: 0,
            stream_id: 1,
            correlation_id,
            payload: b"reply-payload".to_vec(),
        },
    )?;
    let reply = awaiter.receive(DEADLINE)?;
    assert_eq!(reply, b"reply-payload");
    Ok(())
}

// ---- non-negotiable transport contract pins ----

fn assert_contract_violation_closes(
    build: impl FnOnce(&mut WebSocket<TcpStream>) -> Result<(), Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start(ServerOptions::default())?;
    let mut socket = ws_connect(server.ws_addr, None)?;
    ws_liminal_connect(&mut socket, &[])?;
    server.wait_for_active(1)?;
    build(&mut socket)?;
    ws_expect_server_close(&mut socket)?;
    server.wait_for_active(0)?;
    Ok(())
}

#[test]
fn ws_text_message_is_a_typed_protocol_failure_that_closes() -> Result<(), Box<dyn Error>> {
    assert_contract_violation_closes(|socket| {
        socket.send(Message::Text("not binary".into()))?;
        Ok(())
    })
}

#[test]
fn ws_empty_binary_message_closes() -> Result<(), Box<dyn Error>> {
    assert_contract_violation_closes(|socket| {
        socket.send(Message::Binary(Vec::new().into()))?;
        Ok(())
    })
}

#[test]
fn ws_nine_byte_header_closes() -> Result<(), Box<dyn Error>> {
    assert_contract_violation_closes(|socket| {
        // One byte short of the ten-byte canonical header.
        socket.send(Message::Binary(vec![0_u8; 9].into()))?;
        Ok(())
    })
}

#[test]
fn ws_truncated_declared_body_closes() -> Result<(), Box<dyn Error>> {
    assert_contract_violation_closes(|socket| {
        let mut bytes = encode_frame(&Frame::new_ping(0)?)?;
        // Declare one more payload byte than the message carries.
        let declared = u32::from_be_bytes([bytes[6], bytes[7], bytes[8], bytes[9]]);
        bytes[6..10].copy_from_slice(&(declared + 1).to_be_bytes());
        socket.send(Message::Binary(bytes.into()))?;
        Ok(())
    })
}

#[test]
fn ws_trailing_bytes_close() -> Result<(), Box<dyn Error>> {
    assert_contract_violation_closes(|socket| {
        let mut bytes = encode_frame(&Frame::new_ping(0)?)?;
        bytes.push(0xAB);
        socket.send(Message::Binary(bytes.into()))?;
        Ok(())
    })
}

#[test]
fn ws_two_concatenated_frames_close() -> Result<(), Box<dyn Error>> {
    assert_contract_violation_closes(|socket| {
        let mut bytes = encode_frame(&Frame::new_ping(0)?)?;
        bytes.extend(encode_frame(&Frame::new_ping(0)?)?);
        socket.send(Message::Binary(bytes.into()))?;
        Ok(())
    })
}

// ---- F2: oversize-declared message refused at the pinned bound ----

#[test]
fn ws_oversize_declared_length_is_refused_before_allocation() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start(ServerOptions::default())?;
    // Manual upgrade so raw frame bytes can be written post-handshake.
    let mut stream = TcpStream::connect(server.ws_addr)?;
    stream.set_read_timeout(Some(DEADLINE))?;
    stream.write_all(raw_upgrade_request(server.ws_addr, PATH, "").as_bytes())?;
    let head = read_response_head(&mut stream)?;
    assert!(head.starts_with("HTTP/1.1 101"), "upgrade failed: {head}");

    // A masked client binary frame DECLARING an 8 GiB payload — far beyond the
    // pinned liminal frame bound — with no body. The refusal must come from
    // the declared length alone.
    let mut frame = vec![0x82_u8, 0xFF];
    frame.extend(0x0002_0000_0000_u64.to_be_bytes());
    frame.extend([0_u8; 4]); // zero mask key
    stream.write_all(&frame)?;
    stream.flush()?;

    // The server closes; reading eventually yields EOF/reset (a WS close frame
    // may arrive first).
    let deadline = Instant::now() + DEADLINE;
    let mut chunk = [0_u8; 1024];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(_) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                if Instant::now() >= deadline {
                    return Err("server did not close after an oversize declaration".into());
                }
            }
            Err(_) => break,
        }
    }
    server.wait_for_active(0)?;
    Ok(())
}

// ---- fragmentation: library reassembly delivers exactly once ----

#[test]
fn ws_fragmented_binary_is_reassembled_and_applied_once() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start(ServerOptions::default())?;
    let mut stream = TcpStream::connect(server.ws_addr)?;
    stream.set_read_timeout(Some(DEADLINE))?;
    stream.write_all(raw_upgrade_request(server.ws_addr, PATH, "").as_bytes())?;
    let head = read_response_head(&mut stream)?;
    assert!(head.starts_with("HTTP/1.1 101"), "upgrade failed: {head}");

    // Liminal Connect then Ping, each fragmented into two masked frames
    // (zero mask key keeps payload bytes verbatim).
    for frame in [connect_frame(&[]), Frame::new_ping(0)?] {
        let bytes = encode_frame(&frame)?;
        let split = bytes.len() / 2;
        let (first, second) = bytes.split_at(split);
        let mut fragment_one = vec![0x02_u8]; // FIN=0, opcode=binary
        push_masked_payload(&mut fragment_one, first);
        let mut fragment_two = vec![0x80_u8]; // FIN=1, opcode=continuation
        push_masked_payload(&mut fragment_two, second);
        stream.write_all(&fragment_one)?;
        stream.flush()?;
        stream.write_all(&fragment_two)?;
        stream.flush()?;
    }

    // Server answers ConnectAck then exactly one Pong, each as one unmasked
    // binary websocket message wrapping one canonical frame.
    let mut ws_buffer: Vec<u8> = Vec::new();
    let first = read_server_binary_ws_message(&mut stream, &mut ws_buffer)?;
    let (ack, _) = decode(&first)?;
    assert!(matches!(ack, Frame::ConnectAck { .. }));
    let second = read_server_binary_ws_message(&mut stream, &mut ws_buffer)?;
    let (pong, consumed) = decode(&second)?;
    assert_eq!(consumed, second.len());
    assert!(matches!(pong, Frame::Pong { .. }));
    Ok(())
}

fn push_masked_payload(frame: &mut Vec<u8>, payload: &[u8]) {
    if payload.len() < 126 {
        frame.push(0x80 | u8::try_from(payload.len()).unwrap_or(125));
    } else {
        frame.push(0x80 | 126);
        frame.extend(
            u16::try_from(payload.len())
                .unwrap_or(u16::MAX)
                .to_be_bytes(),
        );
    }
    frame.extend([0_u8; 4]); // zero mask key: payload rides verbatim
    frame.extend_from_slice(payload);
}

/// Reads one server->client binary websocket message (unmasked, small frames).
/// `buffer` persists across calls so a TCP segment carrying several websocket
/// frames loses none of them.
fn read_server_binary_ws_message(
    stream: &mut TcpStream,
    buffer: &mut Vec<u8>,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let deadline = Instant::now() + DEADLINE;
    loop {
        if buffer.len() >= 2 {
            let opcode = buffer[0] & 0x0F;
            let len = usize::from(buffer[1] & 0x7F);
            let (header_len, payload_len) = if len < 126 {
                (2, len)
            } else if len == 126 && buffer.len() >= 4 {
                (4, usize::from(u16::from_be_bytes([buffer[2], buffer[3]])))
            } else {
                (0, 0)
            };
            if header_len > 0 && buffer.len() >= header_len + payload_len {
                let payload: Vec<u8> = buffer[header_len..header_len + payload_len].to_vec();
                buffer.drain(..header_len + payload_len);
                match opcode {
                    0x2 => return Ok(payload),
                    0x9 | 0xA => continue, // transport ping/pong: skip
                    other => {
                        return Err(format!("unexpected websocket opcode {other:#x}").into());
                    }
                }
            }
        }
        if Instant::now() >= deadline {
            return Err("timed out reading a server websocket message".into());
        }
        let mut chunk = [0_u8; 4096];
        match stream.read(&mut chunk) {
            Ok(0) => return Err("server closed mid-message".into()),
            Ok(read) => buffer.extend_from_slice(chunk.get(..read).unwrap_or(&[])),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(error) => return Err(error.into()),
        }
    }
}

// ---- F1 + F6: handshake surface ----

#[test]
fn ws_upgrade_declines_permessage_deflate_offer_without_negotiating() -> Result<(), Box<dyn Error>>
{
    let server = RunningServer::start(ServerOptions::default())?;
    let request = raw_upgrade_request(
        server.ws_addr,
        PATH,
        &format!(
            "Origin: {ALLOWED_ORIGIN}\r\nSec-WebSocket-Extensions: permessage-deflate; \
             client_max_window_bits\r\n"
        ),
    );
    let head = raw_http_exchange(server.ws_addr, &request)?;
    assert!(head.starts_with("HTTP/1.1 101"), "upgrade refused: {head}");
    assert!(
        !head
            .to_ascii_lowercase()
            .contains("sec-websocket-extensions"),
        "F1: the response must not carry Sec-WebSocket-Extensions: {head}"
    );
    Ok(())
}

#[test]
fn ws_offered_subprotocol_is_refused() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start(ServerOptions::default())?;
    let request = raw_upgrade_request(
        server.ws_addr,
        PATH,
        "Sec-WebSocket-Protocol: liminal-nonexistent\r\n",
    );
    let head = raw_http_exchange(server.ws_addr, &request)?;
    assert!(
        head.starts_with("HTTP/1.1 400"),
        "expected 400, got: {head}"
    );
    Ok(())
}

#[test]
fn ws_origin_cases_enumerated() -> Result<(), Box<dyn Error>> {
    // Case 1+2: configured list — listed passes, unlisted refused typed.
    let server = RunningServer::start(ServerOptions::default())?;
    let listed = raw_http_exchange(
        server.ws_addr,
        &raw_upgrade_request(
            server.ws_addr,
            PATH,
            &format!("Origin: {ALLOWED_ORIGIN}\r\n"),
        ),
    )?;
    assert!(
        listed.starts_with("HTTP/1.1 101"),
        "listed origin refused: {listed}"
    );
    let unlisted = raw_http_exchange(
        server.ws_addr,
        &raw_upgrade_request(server.ws_addr, PATH, "Origin: https://evil.example.com\r\n"),
    )?;
    assert!(
        unlisted.starts_with("HTTP/1.1 403"),
        "expected 403, got: {unlisted}"
    );

    // Case 3+4: EMPTY allow-list — Origin-bearing refused (fail closed), while
    // a native client sending no Origin passes.
    let closed = RunningServer::start(ServerOptions {
        allowed_origins: Vec::new(),
        ..ServerOptions::default()
    })?;
    let origin_bearing = raw_http_exchange(
        closed.ws_addr,
        &raw_upgrade_request(
            closed.ws_addr,
            PATH,
            &format!("Origin: {ALLOWED_ORIGIN}\r\n"),
        ),
    )?;
    assert!(
        origin_bearing.starts_with("HTTP/1.1 403"),
        "empty allow-list must fail closed: {origin_bearing}"
    );
    let native = raw_http_exchange(
        closed.ws_addr,
        &raw_upgrade_request(closed.ws_addr, PATH, ""),
    )?;
    assert!(
        native.starts_with("HTTP/1.1 101"),
        "native no-Origin refused: {native}"
    );
    Ok(())
}

#[test]
fn ws_ordinary_http_and_wrong_route_receive_fixed_refusals() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start(ServerOptions::default())?;
    // Plain HTTP GET (no upgrade headers) to the configured path: the
    // fixed non-success upgrade refusal.
    let plain = raw_http_exchange(
        server.ws_addr,
        &format!("GET {PATH} HTTP/1.1\r\nHost: {}\r\n\r\n", server.ws_addr),
    )?;
    assert!(plain.starts_with("HTTP/1.1 400"), "plain HTTP: {plain}");
    assert!(plain.to_ascii_lowercase().contains("connection: close"));
    // Plain HTTP GET elsewhere: the fixed not-found refusal.
    let plain_root = raw_http_exchange(
        server.ws_addr,
        &format!("GET / HTTP/1.1\r\nHost: {}\r\n\r\n", server.ws_addr),
    )?;
    assert!(
        plain_root.starts_with("HTTP/1.1 404"),
        "plain HTTP root: {plain_root}"
    );
    // Wrong method.
    let post = raw_http_exchange(
        server.ws_addr,
        &raw_upgrade_request(server.ws_addr, PATH, "").replacen("GET", "POST", 1),
    )?;
    assert!(post.starts_with("HTTP/1.1 400"), "POST: {post}");
    // Wrong version.
    let old = raw_http_exchange(
        server.ws_addr,
        &raw_upgrade_request(server.ws_addr, PATH, "").replacen("HTTP/1.1", "HTTP/1.0", 1),
    )?;
    assert!(old.starts_with("HTTP/1.1 400"), "HTTP/1.0: {old}");
    // Wrong path.
    let wrong_path = raw_http_exchange(
        server.ws_addr,
        &raw_upgrade_request(server.ws_addr, "/nope", ""),
    )?;
    assert!(
        wrong_path.starts_with("HTTP/1.1 404"),
        "wrong path: {wrong_path}"
    );
    // Query on the exact path.
    let query = raw_http_exchange(
        server.ws_addr,
        &raw_upgrade_request(server.ws_addr, &format!("{PATH}?x=1"), ""),
    )?;
    assert!(query.starts_with("HTTP/1.1 404"), "query path: {query}");
    // Oversized request head.
    let oversized = raw_http_exchange(
        server.ws_addr,
        &raw_upgrade_request(
            server.ws_addr,
            PATH,
            &format!("X-Filler: {}\r\n", "a".repeat(10_000)),
        ),
    )?;
    assert!(
        oversized.starts_with("HTTP/1.1 431"),
        "oversized head: {oversized}"
    );
    // Malformed request line.
    let garbage = raw_http_exchange(server.ws_addr, "NONSENSE\u{7f}\r\n\r\n")?;
    assert!(garbage.starts_with("HTTP/1.1 400"), "garbage: {garbage}");
    Ok(())
}

// ---- shared §5 admission across transports ----

#[test]
fn ws_and_tcp_share_one_max_connections_bound() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start(ServerOptions {
        max_connections: 1,
        ..ServerOptions::default()
    })?;

    // TCP takes the single slot.
    let mut tcp_client = TcpStream::connect(server.tcp_addr)?;
    tcp_client.set_read_timeout(Some(Duration::from_millis(200)))?;
    tcp_client.write_all(&encode_frame(&connect_frame(&[]))?)?;
    let _ack = read_one_frame_bytes(&mut tcp_client)?;
    server.wait_for_active(1)?;

    // The WS upgrade completes at HTTP level but admission refuses the spawn:
    // the socket closes without ever serving a liminal frame.
    let mut refused = ws_connect(server.ws_addr, None)?;
    ws_send_frame(&mut refused, &connect_frame(&[]))?;
    let outcome = ws_read_binary(&mut refused);
    assert!(
        outcome.is_err(),
        "an over-cap websocket connection must not be served: {outcome:?}"
    );
    server.wait_for_active(1)?;

    // Freeing the TCP slot admits a fresh WS connection.
    drop(tcp_client);
    server.wait_for_active(0)?;
    let mut admitted = ws_connect(server.ws_addr, None)?;
    ws_liminal_connect(&mut admitted, &[])?;
    Ok(())
}

// ---- Q-A keepalive ----

#[test]
fn ws_keepalive_sends_transport_pings_on_the_named_interval() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start(ServerOptions {
        ping_interval_ms: Some(80),
        ..ServerOptions::default()
    })?;
    let mut socket = ws_connect(server.ws_addr, None)?;
    ws_liminal_connect(&mut socket, &[])?;

    // Bounded event observation: read until two transport pings arrive.
    let deadline = Instant::now() + DEADLINE;
    let mut pings = 0_u32;
    while pings < 2 {
        match socket.read() {
            Ok(Message::Ping(_)) => pings += 1,
            Ok(Message::Binary(_) | Message::Pong(_)) => {
                return Err("keepalive must not mint application traffic".into());
            }
            Ok(other) => return Err(format!("unexpected message: {other:?}").into()),
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                if Instant::now() >= deadline {
                    return Err(format!("only {pings} keepalive pings arrived").into());
                }
            }
            Err(error) => return Err(format!("websocket read failed: {error}").into()),
        }
    }
    Ok(())
}

#[test]
fn ws_keepalive_absent_means_no_pings() -> Result<(), Box<dyn Error>> {
    // No ping_interval_ms: after a full liminal exchange, a bounded window —
    // several multiples of the enabled test's 80ms interval — sees no
    // transport Ping.
    let server = RunningServer::start(ServerOptions::default())?;
    let mut socket = ws_connect(server.ws_addr, None)?;
    ws_liminal_connect(&mut socket, &[])?;
    ws_send_frame(&mut socket, &Frame::new_ping(0)?)?;
    let _pong = ws_read_binary(&mut socket)?;
    let deadline = Instant::now() + Duration::from_millis(400);
    while Instant::now() < deadline {
        match socket.read() {
            Ok(Message::Ping(_)) => {
                return Err("keepalive pings arrived despite absent configuration".into());
            }
            Ok(other) => return Err(format!("unexpected message: {other:?}").into()),
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(error) => return Err(format!("websocket read failed: {error}").into()),
        }
    }
    Ok(())
}

// ---- close, drain, and forced shutdown cleanup ----

#[test]
fn ws_client_close_removes_connection_resources() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start(ServerOptions::default())?;
    let mut socket = ws_connect(server.ws_addr, None)?;
    ws_liminal_connect(&mut socket, &[])?;
    server.wait_for_active(1)?;
    socket.close(None)?;
    // Drive the close handshake from the client side.
    let deadline = Instant::now() + DEADLINE;
    loop {
        match socket.read() {
            Err(tungstenite::Error::ConnectionClosed) | Ok(Message::Close(_)) => break,
            Ok(_) => {}
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                if Instant::now() >= deadline {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    server.wait_for_active(0)?;
    Ok(())
}

#[test]
fn ws_graceful_shutdown_notifies_subscribers_and_drains_both_transports()
-> Result<(), Box<dyn Error>> {
    let mut server = RunningServer::start(ServerOptions::default())?;

    let mut subscriber = ws_connect(server.ws_addr, None)?;
    ws_liminal_connect(&mut subscriber, &[])?;
    ws_send_frame(
        &mut subscriber,
        &Frame::Subscribe {
            flags: 0,
            stream_id: 1,
            channel: CHANNEL.to_owned(),
            accepted_schemas: Vec::new(),
            max_in_flight: 8,
        },
    )?;
    let _ack = ws_read_binary(&mut subscriber)?;
    server.wait_for_active(1)?;

    let supervisor = server.supervisor.clone();
    let mut ws_listener = server.ws.take().ok_or("websocket listener missing")?;
    let shutdown_worker = std::thread::spawn(move || {
        run_shutdown_sequence(
            &mut server.tcp,
            Some(&mut ws_listener),
            &supervisor,
            Duration::from_millis(1500),
        )
    });

    // The drain notification arrives as a canonical liminal Disconnect frame
    // in one binary message; the forced close then tears the socket down.
    let disconnect_bytes = ws_read_binary(&mut subscriber)?;
    let (disconnect, _) = decode(&disconnect_bytes)?;
    assert!(
        matches!(disconnect, Frame::Disconnect { .. }),
        "expected Disconnect, got {:?}",
        disconnect.frame_type()
    );
    ws_expect_server_close(&mut subscriber)?;

    shutdown_worker
        .join()
        .map_err(|_| "shutdown worker panicked")??;
    Ok(())
}

// ---- R1.1 acceptance 4: dependency inspection ----

/// The websocket route added ONE direct dependency (`tungstenite`, sync) and
/// no async runtime or web framework: no tokio-tungstenite, hyper, axum, warp,
/// router, or middleware stack anywhere in the workspace graph.
#[test]
fn ws_dependency_adds_no_async_runtime_or_http_framework() -> Result<(), Box<dyn Error>> {
    let lock_path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../Cargo.lock");
    let lock = std::fs::read_to_string(lock_path)?;
    for forbidden in [
        "tokio-tungstenite",
        "hyper",
        "axum",
        "warp",
        "actix-web",
        "tide",
        "rocket",
        "tower",
    ] {
        assert!(
            !lock.contains(&format!("name = \"{forbidden}\"")),
            "the workspace graph must not contain {forbidden}"
        );
    }
    assert!(
        lock.contains("name = \"tungstenite\""),
        "the pinned sync tungstenite dependency must be present"
    );
    Ok(())
}
