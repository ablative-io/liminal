//! End-to-end proof of the H1 server->client delivery pump + G4 outbound writer.
//!
//! A real `liminal-server` fans a published message out to a real subscriber
//! reached over TCP by the SDK's [`SubscriptionStream`]. These tests exercise the
//! full path — `Subscribe` -> server fan-out -> delivery pump -> `Deliver` frame
//! -> SDK reader -> `recv_timeout` — including the load-bearing cases the design
//! calls out: large-payload delivery (the inverted G4 regression), a slow reader
//! that must not wedge the connection process, and an outbound overflow that tears
//! down only the offending connection.

use std::error::Error;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use liminal::protocol::{
    CausalContext, Frame, MessageEnvelope, ProtocolVersion, SchemaId, decode, encode, encoded_len,
};
use liminal_sdk::SubscriptionStream;
use liminal_server::config::{ChannelDef, ServerConfig, ServicesConfig};
use liminal_server::server::connection::ConnectionSupervisor;
use liminal_server::server::listener::ServerListener;

const CHANNEL: &str = "events";
const RECV_TIMEOUT: Duration = Duration::from_secs(5);
const DEADLINE: Duration = Duration::from_secs(5);

/// Holds the running listener so it stays bound for the lifetime of a test.
struct RunningServer {
    listener: Option<ServerListener>,
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
            services: ServicesConfig::default(),
        };
        let supervisor = ConnectionSupervisor::from_config(&config)?;
        let listener = ServerListener::bind(&config, supervisor)?;
        let address = listener.local_addr();
        Ok(Self {
            listener: Some(listener),
            address,
        })
    }

    const fn address(&self) -> SocketAddr {
        self.address
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

/// A JSON string payload of `size` bytes of `byte`, so it satisfies the channel's
/// permissive `{}` schema (any valid JSON) while giving an exact, checkable body.
fn json_string_payload(byte: u8, size: usize) -> Vec<u8> {
    let mut payload = Vec::with_capacity(size + 2);
    payload.push(b'"');
    payload.resize(size + 1, byte);
    payload.push(b'"');
    payload
}

// ---- raw wire helpers (a minimal client for publishing and silent-subscribing) ----

fn write_frame(stream: &mut TcpStream, frame: &Frame) -> Result<(), Box<dyn Error>> {
    let len = encoded_len(frame).map_err(|error| format!("encoded_len: {error}"))?;
    let mut bytes = vec![0_u8; len];
    let written = encode(frame, &mut bytes).map_err(|error| format!("encode: {error}"))?;
    stream.write_all(bytes.get(..written).unwrap_or(&[]))?;
    stream.flush()?;
    Ok(())
}

fn read_frame(stream: &mut TcpStream, buffer: &mut Vec<u8>) -> Result<Frame, Box<dyn Error>> {
    let deadline = Instant::now() + DEADLINE;
    loop {
        match decode(buffer) {
            Ok((frame, consumed)) => {
                buffer.drain(..consumed);
                return Ok(frame);
            }
            Err(error) if is_incomplete(&error) => {
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
            Err(error) => return Err(format!("decode: {error}").into()),
        }
    }
}

const fn is_incomplete(error: &liminal::protocol::ProtocolError) -> bool {
    matches!(
        error,
        liminal::protocol::ProtocolError::IncompleteHeader { .. }
            | liminal::protocol::ProtocolError::TruncatedPayload { .. }
    )
}

/// Opens a raw client socket and completes the `Connect` -> `ConnectAck` handshake.
fn raw_connect(address: SocketAddr) -> Result<TcpStream, Box<dyn Error>> {
    let mut stream = TcpStream::connect(address)?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_millis(200)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    write_frame(
        &mut stream,
        &Frame::Connect {
            flags: 0,
            min_version: ProtocolVersion::new(1, 0),
            max_version: ProtocolVersion::new(1, 0),
            auth_token: Vec::new(),
        },
    )?;
    let mut buffer = Vec::new();
    match read_frame(&mut stream, &mut buffer)? {
        Frame::ConnectAck { .. } => Ok(stream),
        other => Err(format!("expected ConnectAck, got {:?}", other.frame_type()).into()),
    }
}

/// Publishes `payload` to `CHANNEL` and reads the `PublishAck`, confirming the
/// server accepted and fanned it out.
fn raw_publish(
    stream: &mut TcpStream,
    buffer: &mut Vec<u8>,
    payload: Vec<u8>,
) -> Result<(), Box<dyn Error>> {
    let envelope = MessageEnvelope::new(
        SchemaId::new([0_u8; SchemaId::WIRE_LEN]),
        CausalContext::independent(),
        payload,
    );
    let frame = Frame::new_publish(1, CHANNEL, envelope)?;
    write_frame(stream, &frame)?;
    match read_frame(stream, buffer)? {
        Frame::PublishAck { .. } => Ok(()),
        Frame::PublishError { message, .. } => Err(format!("publish rejected: {message:?}").into()),
        other => Err(format!("expected PublishAck, got {:?}", other.frame_type()).into()),
    }
}

/// Subscribes to `CHANNEL` on a raw socket and reads the `SubscribeAck`, then
/// returns the socket WITHOUT reading any further — a "silent" subscriber whose
/// deliveries pile up in the server's outbound buffer.
fn raw_silent_subscribe(address: SocketAddr) -> Result<TcpStream, Box<dyn Error>> {
    let mut stream = raw_connect(address)?;
    write_frame(
        &mut stream,
        &Frame::Subscribe {
            flags: 0,
            stream_id: 1,
            channel: CHANNEL.to_owned(),
            accepted_schemas: Vec::new(),
            max_in_flight: 1024,
        },
    )?;
    let mut buffer = Vec::new();
    match read_frame(&mut stream, &mut buffer)? {
        Frame::SubscribeAck { .. } => Ok(stream),
        other => Err(format!("expected SubscribeAck, got {:?}", other.frame_type()).into()),
    }
}

/// (a) A published message is delivered to a remote subscriber with payload
/// fidelity, and successive publishes arrive in order with `delivery_seq` 1, 2, 3.
#[test]
fn subscription_receives_published_messages_in_order() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start()?;
    let subscription =
        SubscriptionStream::open(&server.address().to_string(), CHANNEL, Vec::new())?;

    let mut publisher = raw_connect(server.address())?;
    let mut ack_buffer = Vec::new();
    let payloads = [
        json_string_payload(b'a', 8),
        json_string_payload(b'b', 8),
        json_string_payload(b'c', 8),
    ];
    for payload in &payloads {
        raw_publish(&mut publisher, &mut ack_buffer, payload.clone())?;
    }

    for (index, payload) in payloads.iter().enumerate() {
        let message = subscription.recv_timeout(RECV_TIMEOUT)?;
        assert_eq!(
            message.delivery_seq(),
            u64::try_from(index)? + 1,
            "delivery_seq is monotonic from 1"
        );
        assert_eq!(
            message.payload(),
            payload.as_slice(),
            "payload is delivered verbatim"
        );
    }

    drop(subscription);
    server.shutdown()?;
    Ok(())
}

/// (b) The inverted G4 regression: a payload well above the ~64 KiB socket send
/// buffer is delivered INTACT. Under the old `write_all` on a non-blocking socket
/// this frame would ghost the connection; the outbound writer streams it out
/// across slices instead.
#[test]
fn large_payload_is_delivered_intact() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start()?;
    let subscription =
        SubscriptionStream::open(&server.address().to_string(), CHANNEL, Vec::new())?;

    let mut publisher = raw_connect(server.address())?;
    let mut ack_buffer = Vec::new();
    // 128 KiB payload — twice a typical send buffer, so the Deliver frame cannot be
    // written in one syscall and must survive a WouldBlock mid-drain.
    let payload = json_string_payload(b'z', 128 * 1024);
    raw_publish(&mut publisher, &mut ack_buffer, payload.clone())?;

    let message = subscription.recv_timeout(RECV_TIMEOUT)?;
    assert_eq!(message.delivery_seq(), 1);
    assert_eq!(
        message.payload().len(),
        payload.len(),
        "the full large payload length is delivered"
    );
    assert_eq!(
        message.into_payload(),
        payload,
        "the large payload is delivered byte-for-byte intact"
    );

    drop(subscription);
    server.shutdown()?;
    Ok(())
}

/// (c) A slow reader that never drains its socket does not wedge the connection
/// process or other connections: a healthy subscriber on the same channel keeps
/// receiving, and publishes keep succeeding, while the silent subscriber's
/// deliveries merely queue on its own connection.
#[test]
fn slow_reader_does_not_wedge_other_connections() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start()?;

    // A healthy subscriber that we DO consume.
    let healthy = SubscriptionStream::open(&server.address().to_string(), CHANNEL, Vec::new())?;
    // A silent subscriber that never reads its socket (a slow/stalled reader).
    let _silent = raw_silent_subscribe(server.address())?;

    let mut publisher = raw_connect(server.address())?;
    let mut ack_buffer = Vec::new();
    // A modest volume, comfortably under the 4 MiB outbound cap, so the silent
    // reader's connection is stalled but not torn down.
    let total = 12;
    for index in 0..total {
        raw_publish(
            &mut publisher,
            &mut ack_buffer,
            json_string_payload(b'q', 64 + index),
        )?;
    }

    // The healthy subscriber receives every message despite the silent peer.
    for expected_seq in 1..=total {
        let message = healthy.recv_timeout(RECV_TIMEOUT)?;
        assert_eq!(message.delivery_seq(), u64::try_from(expected_seq)?);
    }

    // A publish round trip still completes: the connection processes are not wedged.
    raw_publish(
        &mut publisher,
        &mut ack_buffer,
        json_string_payload(b'r', 4),
    )?;
    let after = healthy.recv_timeout(RECV_TIMEOUT)?;
    assert_eq!(after.delivery_seq(), u64::try_from(total)? + 1);

    drop(healthy);
    server.shutdown()?;
    Ok(())
}

/// (d) An outbound-buffer overflow tears down ONLY the offending connection. Since
/// the headroom-aware pump holds sub-capacity frames back rather than overflowing,
/// the one remaining overflow is a SINGLE frame larger than the whole 4 MiB buffer:
/// it can never be queued, so it is fatal. A silent subscriber is sent one such
/// oversized delivery; its connection is closed by the server, while a fresh
/// subscriber and publisher on other connections keep working.
#[test]
fn outbound_overflow_tears_down_only_the_offending_connection() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start()?;

    let mut silent = raw_silent_subscribe(server.address())?;

    // A single message whose Deliver frame exceeds the 4 MiB outbound cap: it cannot
    // fit even an empty buffer, so the pump falls through to the fatal Overflow (the
    // spec-inherent per-frame bound) instead of holding it back. A pipelined burst of
    // sub-capacity frames would NOT overflow — it rides out across slices — so the
    // oversized single frame is the case that still tears the connection down.
    let mut publisher = raw_connect(server.address())?;
    let mut ack_buffer = Vec::new();
    raw_publish(
        &mut publisher,
        &mut ack_buffer,
        json_string_payload(b'x', 4 * 1024 * 1024 + 64 * 1024),
    )?;

    // The server tears the silent connection down: its socket is closed. We now read
    // it (the point is that it did NOT read DURING the flood) and expect EOF within
    // the deadline.
    silent.set_read_timeout(Some(Duration::from_millis(200)))?;
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut closed = false;
    let mut scratch = vec![0_u8; 65536];
    while Instant::now() < deadline {
        match silent.read(&mut scratch) {
            Ok(0) => {
                closed = true;
                break;
            }
            Ok(_) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(_) => {
                closed = true;
                break;
            }
        }
    }
    assert!(
        closed,
        "the overflowing connection must be torn down by the server"
    );

    // Other connections are unaffected: a fresh subscriber still receives a fresh
    // publish, proving only the offending connection died.
    let healthy = SubscriptionStream::open(&server.address().to_string(), CHANNEL, Vec::new())?;
    raw_publish(
        &mut publisher,
        &mut ack_buffer,
        json_string_payload(b'y', 16),
    )?;
    let message = healthy.recv_timeout(RECV_TIMEOUT)?;
    assert_eq!(message.payload(), json_string_payload(b'y', 16).as_slice());

    drop(healthy);
    server.shutdown()?;
    Ok(())
}
