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
use socket2::{Domain, Socket, Type};

use liminal_server::config::{ChannelDef, LimitsConfig, ServerConfig, ServicesConfig};
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
            limits: LimitsConfig::default(),
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

/// A subscriber is parked after its `SubscribeAck`; later publishes fire the inbox
/// notifier, wake it, and deliver payloads in order with sequences 1, 2, 3.
#[test]
fn subscription_publish_wakes_parked_connection_in_order() -> Result<(), Box<dyn Error>> {
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

/// (d) §5 inbox-overflow SHED (behaviour change from R3, flagged): an envelope that
/// cannot fit the connection's 4 MiB shared inbox byte budget is shed at the inbox —
/// the OFFENDING SUBSCRIPTION is released with a typed error frame — rather than
/// buffered until the outbound buffer overflows and the whole connection is torn
/// down. Before R3, a single Deliver frame larger than the 4 MiB outbound buffer was
/// the fatal per-frame overflow that tore the connection down; now the same oversized
/// delivery is refused admission to the inbox first (it exceeds the whole 4 MiB
/// inbox budget), so the subscription is shed and the CONNECTION SURVIVES. This is
/// the §5 policy — "a slow consumer sheds its own subscription; it cannot grow server
/// memory without bound" — superseding the old connection-teardown for this class.
/// Other connections remain unaffected either way.
#[test]
fn inbox_overflow_sheds_the_offending_subscription_without_tearing_down_the_connection()
-> Result<(), Box<dyn Error>> {
    let server = RunningServer::start()?;

    let mut silent = raw_silent_subscribe(server.address())?;

    // A single message whose serialized envelope exceeds the 4 MiB shared inbox byte
    // budget: it can never be admitted to the inbox, so the subscription is shed with
    // a typed SubscribeError and the envelope dropped — memory never grows past the
    // §5 bound.
    let mut publisher = raw_connect(server.address())?;
    let mut ack_buffer = Vec::new();
    raw_publish(
        &mut publisher,
        &mut ack_buffer,
        json_string_payload(b'x', 4 * 1024 * 1024 + 64 * 1024),
    )?;

    // The silent connection is NOT torn down: it survives the shed. We read it (it did
    // NOT read DURING the flood) and must observe live bytes (the shed SubscribeError),
    // never an EOF, within the window.
    silent.set_read_timeout(Some(Duration::from_millis(200)))?;
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut closed = false;
    let mut saw_bytes = false;
    let mut scratch = vec![0_u8; 65536];
    while Instant::now() < deadline {
        match silent.read(&mut scratch) {
            Ok(0) => {
                closed = true;
                break;
            }
            Ok(_) => {
                saw_bytes = true;
                break;
            }
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
        !closed,
        "the connection must SURVIVE the inbox-overflow shed (§5), not be torn down"
    );
    assert!(
        saw_bytes,
        "the shed subscription must receive a typed error frame on its connection"
    );

    // Other connections are unaffected: a fresh subscriber still receives a fresh
    // publish, proving the shed was confined to the offending subscription.
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

// ---- G4 oversize-frame WouldBlock-boundary regression ----

/// Client receive-buffer size for the G4 regression subscriber. Shrunk to 16 KiB
/// BEFORE connect so a subscriber that never reads during the flood advertises a
/// tiny, never-reopening receive window. The server can then only park a bounded
/// amount of the oversize delivery in the kernel (its own send buffer plus this
/// window) before `write()` returns `WouldBlock` with the frame still partly
/// queued — the exact G4 partial-write condition, made certain by construction
/// (an integration test cannot observe the server's private `DrainOutcome`, so the
/// socket configuration is what guarantees the boundary is crossed).
const G4_CLIENT_RCVBUF: usize = 16 * 1024;

/// Total wire size of the oversize G4 delivery payload (512 KiB). This is 5x+ the
/// ~96 KiB empirical old-loss boundary and 2x the 256 KiB task floor, yet
/// comfortably under both the 4 MiB outbound-writer capacity and the 4 MiB §5
/// inbox byte budget (so the frame is admitted and buffered, never shed or
/// overflow-torn-down). Larger than any default TCP send buffer, so with the
/// shrunk client window above the server's drain cannot complete without a
/// `WouldBlock`-with-residue.
const G4_OVERSIZE_PAYLOAD: usize = 512 * 1024;

/// Subscribes on a raw socket whose `SO_RCVBUF` is shrunk to [`G4_CLIENT_RCVBUF`]
/// before connect, then returns the socket WITHOUT reading past the
/// `SubscribeAck` — a silent subscriber with a deliberately tiny receive window.
///
/// This mirrors [`raw_silent_subscribe`] but constructs the socket via `socket2`
/// so the receive buffer can be set prior to connect (needed for the window to be
/// negotiated small). The shrink is the mechanism that makes the server-side
/// outbound `WouldBlock`-with-residue certain for the oversize delivery.
fn raw_silent_subscribe_small_rcvbuf(address: SocketAddr) -> Result<TcpStream, Box<dyn Error>> {
    let socket = Socket::new(Domain::for_address(address), Type::STREAM, None)?;
    socket.set_recv_buffer_size(G4_CLIENT_RCVBUF)?;
    socket.connect(&address.into())?;
    let mut stream: TcpStream = socket.into();
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_millis(200)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    // Connect -> ConnectAck.
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
        Frame::ConnectAck { .. } => {}
        other => return Err(format!("expected ConnectAck, got {:?}", other.frame_type()).into()),
    }

    // Subscribe -> SubscribeAck, then read nothing further: deliveries pile up on
    // this connection's server-side outbound buffer.
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
    match read_frame(&mut stream, &mut buffer)? {
        // Nothing is published until this returns, so `buffer` holds no trailing
        // bytes past the SubscribeAck and can be discarded safely.
        Frame::SubscribeAck { .. } => Ok(stream),
        other => Err(format!("expected SubscribeAck, got {:?}", other.frame_type()).into()),
    }
}

/// Extracts `(delivery_seq, payload)` from a `Deliver` frame, or fails the test
/// with the unexpected frame type — a truncated/desynced stream surfaces here as a
/// decode error or a non-`Deliver` frame rather than a silent wrong answer.
fn expect_deliver(frame: Frame) -> Result<(u64, Vec<u8>), Box<dyn Error>> {
    match frame {
        Frame::Deliver {
            delivery_seq,
            envelope,
            ..
        } => Ok((delivery_seq, envelope.payload)),
        other => Err(format!("expected Deliver, got {:?}", other.frame_type()).into()),
    }
}

/// G4 regression (docs/stack-review/liminal-ledger.md §G4): the pre-fix
/// `write_frame` called `write_all` on the NON-BLOCKING connection socket, so a
/// server-originated frame larger than the free kernel send buffer (~73 KiB
/// delivered / ~96 KiB lost, empirically — the boundary moved with buffer state)
/// was truncated at the first `WouldBlock` after a partial write. The client
/// decoder then blocked on the declared `payload_length` and consumed every
/// SUBSEQUENT frame as continuation bytes — a permanent, silent desync until the
/// "worker lost" timeout swept the connection. Pushes were where it bit.
///
/// The fix (2026-07-07, e1b847d, H1 outbound writer:
/// crates/liminal-server/src/server/connection/outbound.rs) buffers each frame in
/// a bounded per-connection `OutboundWriter` and drains it cooperatively across
/// scheduler slices, so a mid-frame `WouldBlock` merely re-queues the residue for
/// the next slice. This boundary had NO regression coverage; this test pins it.
///
/// Construction of the WouldBlock-with-residue (see
/// [`raw_silent_subscribe_small_rcvbuf`]): the subscriber shrinks its receive
/// buffer to 16 KiB and never reads during the flood, so the total in-flight
/// window is capped ~2 orders of magnitude below the 512 KiB delivery — the
/// server's drain CANNOT complete the frame in one pass and is guaranteed to hit
/// `WouldBlock` with residue queued. The test then reads everything and asserts
/// (a) the oversize frame decodes byte-exact and (b) a subsequent normal frame on
/// the same connection also decodes cleanly — the no-desync half, which is the
/// half that made G4 catastrophic. On the pre-fix code the oversize `Deliver`
/// would be truncated on the wire and this test would fail by construction: (a)
/// the first frame never completes (decode blocks / times out) and (b) the
/// follow-up frame is swallowed as continuation bytes.
#[test]
fn oversize_frame_survives_wouldblock_boundary_and_no_desync() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start()?;

    // Silent subscriber with a 16 KiB receive window that it never drains during
    // the flood: this is what forces the server-side outbound WouldBlock-with-residue.
    let mut subscriber = raw_silent_subscribe_small_rcvbuf(server.address())?;

    let mut publisher = raw_connect(server.address())?;
    let mut ack_buffer = Vec::new();

    // The oversize server-originated frame: 512 KiB, well past the ~96 KiB old-loss
    // boundary and the 256 KiB floor, under the 4 MiB caps.
    let oversize = json_string_payload(b'Z', G4_OVERSIZE_PAYLOAD);
    raw_publish(&mut publisher, &mut ack_buffer, oversize.clone())?;

    // A SECOND, small publish enqueued strictly AFTER the oversize delivery on the
    // same subscriber connection, BEFORE the subscriber reads anything. Under the
    // old truncating path its bytes would be consumed as continuation of the
    // truncated oversize frame — the head-of-line desync G4 caused.
    let follow = json_string_payload(b'y', 32);
    raw_publish(&mut publisher, &mut ack_buffer, follow.clone())?;

    // Now drain the subscriber. read_frame reads in 8 KiB chunks and blocks on a
    // complete frame, reopening the shrunk window a little at a time so the whole
    // 512 KiB streams out across many cooperative drain slices.
    let mut sub_buffer = Vec::new();

    // (a) The oversize frame decodes byte-exact: full length and identical bytes,
    // proving the frame survived the WouldBlock-with-residue drain intact.
    let (seq, payload) = expect_deliver(read_frame(&mut subscriber, &mut sub_buffer)?)?;
    assert_eq!(seq, 1, "the oversize delivery is delivery_seq 1");
    assert_eq!(
        payload.len(),
        oversize.len(),
        "the full oversize payload length survives the WouldBlock boundary"
    );
    assert_eq!(
        payload, oversize,
        "the oversize payload is delivered byte-for-byte intact across the WouldBlock boundary"
    );

    // (b) The subsequent normal frame decodes cleanly on the SAME connection: the
    // stream stayed framed, no continuation-byte desync. This is the half that made
    // G4 catastrophic.
    let (seq2, payload2) = expect_deliver(read_frame(&mut subscriber, &mut sub_buffer)?)?;
    assert_eq!(
        seq2, 2,
        "the follow-up delivery is delivery_seq 2 — the stream stayed in frame"
    );
    assert_eq!(
        payload2, follow,
        "the follow-up frame decodes byte-exact — no head-of-line desync after the oversize frame"
    );

    drop(subscriber);
    drop(publisher);
    server.shutdown()?;
    Ok(())
}
