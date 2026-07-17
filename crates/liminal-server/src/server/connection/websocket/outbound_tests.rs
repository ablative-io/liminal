//! R1.2 outbound pins: every server frame is canonically encoded ONCE and
//! drained as exactly one binary WebSocket message, under the same bounded
//! 4 MiB / fatal-overflow discipline as the TCP byte queue.

use liminal::protocol::{Frame, ProtocolVersion, decode, encode, encoded_len};

use super::super::super::outbound::{DEFAULT_OUTBOUND_CAPACITY, DrainOutcome, OutboundError};
use super::WebSocketOutbound;

fn encode_frame(frame: &Frame) -> Result<Vec<u8>, String> {
    let len = encoded_len(frame).map_err(|error| format!("encoded_len: {error}"))?;
    let mut bytes = vec![0_u8; len];
    let written = encode(frame, &mut bytes).map_err(|error| format!("encode: {error}"))?;
    bytes.truncate(written);
    Ok(bytes)
}

#[test]
fn one_enqueued_frame_becomes_one_canonical_message() -> Result<(), String> {
    let mut outbound = WebSocketOutbound::new();
    let pong = Frame::Pong { flags: 0 };
    let ack = Frame::ConnectAck {
        flags: 0,
        selected_version: ProtocolVersion::new(1, 0),
        capabilities: 1,
    };
    outbound
        .enqueue_frame(&pong)
        .map_err(|error| error.to_string())?;
    outbound
        .enqueue_frame(&ack)
        .map_err(|error| error.to_string())?;
    let messages = outbound.take_messages();
    assert_eq!(messages.len(), 2, "one frame per message, never batched");
    assert_eq!(messages[0], encode_frame(&pong)?);
    assert_eq!(messages[1], encode_frame(&ack)?);
    Ok(())
}

#[test]
fn default_capacity_matches_the_shared_outbound_bound() {
    let outbound = WebSocketOutbound::new();
    assert_eq!(outbound.capacity(), DEFAULT_OUTBOUND_CAPACITY);
}

#[test]
fn overflow_is_a_typed_fatal_error() -> Result<(), String> {
    let mut outbound = WebSocketOutbound::with_capacity(16);
    // A Pong is exactly 10 bytes: the first fits, the second overflows.
    outbound
        .enqueue_frame(&Frame::Pong { flags: 0 })
        .map_err(|error| error.to_string())?;
    match outbound.enqueue_frame(&Frame::Pong { flags: 0 }) {
        Err(OutboundError::Overflow {
            queued,
            needed,
            capacity,
        }) => {
            assert_eq!(queued, 10);
            assert_eq!(needed, 10);
            assert_eq!(capacity, 16);
            Ok(())
        }
        other => Err(format!("expected a typed overflow, got {other:?}")),
    }
}

#[test]
fn has_room_accounts_for_queued_bytes() -> Result<(), String> {
    let mut outbound = WebSocketOutbound::with_capacity(16);
    assert!(outbound.has_room(10));
    outbound
        .enqueue_frame(&Frame::Pong { flags: 0 })
        .map_err(|error| error.to_string())?;
    assert!(outbound.has_room(6));
    assert!(!outbound.has_room(7));
    assert_eq!(outbound.queued_len(), 10);
    Ok(())
}

/// Drains queued frames over a real loopback pair and asserts the client-side
/// websocket receives each canonical frame byte-exact — the transport-level
/// half of the R1.2 identity fixture (the frame-category matrix lives in
/// `process_tests`; this proves the wire hop preserves it).
#[test]
fn drain_delivers_each_frame_as_one_byte_exact_binary_message() -> Result<(), String> {
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").map_err(|error| format!("bind: {error}"))?;
    let address = listener
        .local_addr()
        .map_err(|error| format!("addr: {error}"))?;
    let client_stream =
        std::net::TcpStream::connect(address).map_err(|error| format!("connect: {error}"))?;
    client_stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .map_err(|error| format!("timeout: {error}"))?;
    let (server_stream, _) = listener
        .accept()
        .map_err(|error| format!("accept: {error}"))?;
    server_stream
        .set_nonblocking(true)
        .map_err(|error| format!("nonblocking: {error}"))?;

    let bound = super::super::liminal_ws_message_bound().map_err(|error| error.to_string())?;
    let mut server = tungstenite::protocol::WebSocket::from_raw_socket(
        server_stream,
        tungstenite::protocol::Role::Server,
        Some(super::super::pinned_protocol_config(bound)),
    );
    let mut client = tungstenite::protocol::WebSocket::from_raw_socket(
        client_stream,
        tungstenite::protocol::Role::Client,
        None,
    );

    let frames = vec![
        Frame::Pong { flags: 0 },
        Frame::ConnectAck {
            flags: 0,
            selected_version: ProtocolVersion::new(1, 0),
            capabilities: 0,
        },
        Frame::Disconnect { flags: 0 },
    ];
    let mut outbound = WebSocketOutbound::new();
    for frame in &frames {
        outbound
            .enqueue_frame(frame)
            .map_err(|error| error.to_string())?;
    }

    // Drain to completion (the loopback buffer easily holds three tiny
    // frames, but WouldBlock residue is retried to keep the loop honest).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match outbound.drain(&mut server, None) {
            Ok(DrainOutcome::Drained) => break,
            Ok(DrainOutcome::Progress | DrainOutcome::WouldBlockWithResidue) => {
                if std::time::Instant::now() >= deadline {
                    return Err("drain did not complete".to_owned());
                }
            }
            Err(error) => return Err(format!("drain failed: {error}")),
        }
    }
    assert_eq!(outbound.queued_len(), 0, "all bytes accounted as flushed");

    for expected in &frames {
        let message = client.read().map_err(|error| format!("read: {error}"))?;
        let tungstenite::Message::Binary(bytes) = message else {
            return Err(format!("expected a binary message, got {message:?}"));
        };
        assert_eq!(bytes.as_ref(), encode_frame(expected)?.as_slice());
        let (decoded, consumed) = decode(&bytes).map_err(|error| format!("decode: {error}"))?;
        assert_eq!(consumed, bytes.len());
        assert_eq!(&decoded, expected);
    }
    Ok(())
}
