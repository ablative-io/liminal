use super::*;
use liminal::protocol::{CausalContext, MessageEnvelope};
use std::net::TcpListener;

/// Encodes one frame into a fresh byte vector for the fake server below.
fn encode_frame(frame: &Frame) -> Result<Vec<u8>, SdkError> {
    let len = encoded_len(frame).map_err(|error| protocol_error(&error))?;
    let mut bytes = vec![0_u8; len];
    let written = encode(frame, &mut bytes).map_err(|error| protocol_error(&error))?;
    bytes.truncate(written);
    Ok(bytes)
}

/// Blocks reading `socket` until one complete frame decodes, discarding it. Used
/// by the fake server to consume the client's `Connect`/`Subscribe` frames.
fn read_and_discard_one(socket: &mut TcpStream, buffer: &mut Vec<u8>) -> Result<(), SdkError> {
    loop {
        match decode(buffer) {
            Ok((_, consumed)) => {
                buffer.drain(..consumed);
                return Ok(());
            }
            Err(
                ProtocolError::IncompleteHeader { .. } | ProtocolError::TruncatedPayload { .. },
            ) => {
                let mut chunk = [0_u8; 512];
                let read = socket
                    .read(&mut chunk)
                    .map_err(|source| SdkError::Connection {
                        description: format!("fake server read failed: {source}"),
                    })?;
                if read == 0 {
                    return Err(SdkError::Connection {
                        description: "fake server: client closed before a full frame".to_string(),
                    });
                }
                buffer.extend_from_slice(chunk.get(..read).unwrap_or(&[]));
            }
            Err(error) => return Err(protocol_error(&error)),
        }
    }
}

#[test]
fn delivered_message_exposes_seq_and_payload() {
    let message = DeliveredMessage {
        delivery_seq: 3,
        schema_id: SchemaId::new([1; SchemaId::WIRE_LEN]),
        payload: vec![9, 8, 7],
    };
    assert_eq!(message.delivery_seq(), 3);
    assert_eq!(message.payload(), &[9, 8, 7]);
    assert_eq!(message.schema_id(), SchemaId::new([1; SchemaId::WIRE_LEN]));
    assert_eq!(message.into_payload(), vec![9, 8, 7]);
}

#[test]
fn deliver_frame_round_trips_through_codec() -> Result<(), SdkError> {
    // The exact frame the reader decodes: a Deliver carrying delivery_seq and a
    // MessageEnvelope whose payload the reader surfaces verbatim.
    let envelope = MessageEnvelope::new(
        SchemaId::new([2; SchemaId::WIRE_LEN]),
        CausalContext::independent(),
        vec![4, 5, 6],
    );
    let frame = Frame::new_deliver(SUBSCRIPTION_STREAM_ID, 1, envelope)
        .map_err(|error| protocol_error(&error))?;
    let len = encoded_len(&frame).map_err(|error| protocol_error(&error))?;
    let mut bytes = vec![0_u8; len];
    let written = encode(&frame, &mut bytes).map_err(|error| protocol_error(&error))?;
    let (decoded, consumed) = decode(&bytes[..written]).map_err(|error| protocol_error(&error))?;
    assert_eq!(consumed, written);
    let Frame::Deliver {
        delivery_seq,
        envelope,
        ..
    } = decoded
    else {
        return Err(SdkError::Protocol {
            description: "expected a Deliver frame".to_string(),
        });
    };
    assert_eq!(delivery_seq, 1);
    assert_eq!(envelope.payload, vec![4, 5, 6]);
    Ok(())
}

/// Repro for the setup-residue drop: a server that coalesces `SubscribeAck` with
/// the first `Deliver` frames into one TCP segment must not lose those
/// deliveries. Before the fix, the throwaway subscribe buffer discarded the bytes
/// read past the ack and the reader started on an empty buffer, so both deliveries
/// vanished; now the residue threads into the reader and surfaces.
#[test]
fn open_preserves_deliveries_coalesced_with_the_subscribe_ack() -> Result<(), SdkError> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|source| SdkError::Connection {
        description: format!("failed to bind fake server: {source}"),
    })?;
    let address = listener
        .local_addr()
        .map_err(|source| SdkError::Connection {
            description: format!("failed to read fake server address: {source}"),
        })?
        .to_string();

    let server = std::thread::spawn(move || -> Result<(), SdkError> {
        let (mut socket, _peer) = listener.accept().map_err(|source| SdkError::Connection {
            description: format!("fake server accept failed: {source}"),
        })?;
        let mut buffer = Vec::new();
        // Consume the client's Connect, then ack the handshake on its own write.
        read_and_discard_one(&mut socket, &mut buffer)?;
        write_frame(
            &mut socket,
            &Frame::ConnectAck {
                flags: 0,
                selected_version: CLIENT_MAX_VERSION,
                capabilities: 0,
            },
        )?;
        // Consume the client's Subscribe, then coalesce the SubscribeAck and two
        // Deliver frames into ONE segment — the exact hot-channel repro.
        read_and_discard_one(&mut socket, &mut buffer)?;
        let schema = SchemaId::new([7; SchemaId::WIRE_LEN]);
        let ack = Frame::SubscribeAck {
            flags: 0,
            stream_id: SUBSCRIPTION_STREAM_ID,
            subscription_id: 42,
            selected_schema: schema,
        };
        let first = Frame::new_deliver(
            SUBSCRIPTION_STREAM_ID,
            1,
            MessageEnvelope::new(schema, CausalContext::independent(), vec![1, 1, 1]),
        )
        .map_err(|error| protocol_error(&error))?;
        let second = Frame::new_deliver(
            SUBSCRIPTION_STREAM_ID,
            2,
            MessageEnvelope::new(schema, CausalContext::independent(), vec![2, 2, 2]),
        )
        .map_err(|error| protocol_error(&error))?;
        let mut segment = Vec::new();
        segment.extend_from_slice(&encode_frame(&ack)?);
        segment.extend_from_slice(&encode_frame(&first)?);
        segment.extend_from_slice(&encode_frame(&second)?);
        socket
            .write_all(&segment)
            .map_err(|source| SdkError::Connection {
                description: format!("fake server write failed: {source}"),
            })?;
        socket.flush().map_err(|source| SdkError::Connection {
            description: format!("fake server flush failed: {source}"),
        })?;
        // Hold the socket open so the client reader does not hit EOF before it has
        // surfaced both buffered deliveries.
        std::thread::sleep(Duration::from_millis(500));
        Ok(())
    });

    let subscription = SubscriptionStream::open(&address, "orders", Vec::new())?;
    assert_eq!(subscription.subscription_id(), 42);
    let first = subscription.recv_timeout(Duration::from_secs(2))?;
    assert_eq!(first.delivery_seq(), 1);
    assert_eq!(first.payload(), &[1, 1, 1]);
    let second = subscription.recv_timeout(Duration::from_secs(2))?;
    assert_eq!(second.delivery_seq(), 2);
    assert_eq!(second.payload(), &[2, 2, 2]);
    drop(subscription);
    server.join().ok();
    Ok(())
}

/// The named setup deadline SDK-010 installs: 5 s, the estate's already-ratified
/// constant generalized, never a new default.
const NAMED_SETUP_DEADLINE: Duration = Duration::from_secs(5);

/// A control-frame reply that is slow but well-behaved: past the retired 100 ms
/// `READER_POLL_TIMEOUT` cadence, far inside the named 5 s deadline. The delay is
/// the fixture's INTENDED slow reply, not a proof device.
const SLOW_BUT_ANSWERED: Duration = Duration::from_millis(250);

/// Runs a fake server that answers the handshake and the subscribe, optionally
/// delaying each control reply, then holds the socket until the client tears it
/// down. Returns the dialable address and the server's join handle.
fn spawn_setup_server(
    reply_delay: Duration,
) -> Result<(String, std::thread::JoinHandle<Result<(), SdkError>>), SdkError> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|source| SdkError::Connection {
        description: format!("failed to bind fake server: {source}"),
    })?;
    let address = listener
        .local_addr()
        .map_err(|source| SdkError::Connection {
            description: format!("failed to read fake server address: {source}"),
        })?
        .to_string();
    let server = std::thread::spawn(move || -> Result<(), SdkError> {
        let (mut socket, _peer) = listener.accept().map_err(|source| SdkError::Connection {
            description: format!("fake server accept failed: {source}"),
        })?;
        let mut buffer = Vec::new();
        read_and_discard_one(&mut socket, &mut buffer)?;
        std::thread::sleep(reply_delay);
        write_frame(
            &mut socket,
            &Frame::ConnectAck {
                flags: 0,
                selected_version: CLIENT_MAX_VERSION,
                capabilities: 0,
            },
        )?;
        read_and_discard_one(&mut socket, &mut buffer)?;
        std::thread::sleep(reply_delay);
        write_frame(
            &mut socket,
            &Frame::SubscribeAck {
                flags: 0,
                stream_id: SUBSCRIPTION_STREAM_ID,
                subscription_id: 42,
                selected_schema: SchemaId::new([7; SchemaId::WIRE_LEN]),
            },
        )?;
        // Read to the client's teardown so the socket outlives the assertions.
        let mut scratch = [0_u8; 512];
        while socket.read(&mut scratch).unwrap_or(0) > 0 {}
        Ok(())
    });
    Ok((address, server))
}

/// RED PIN (SDK-010 R2, direction (b)) — the disarm, pinned behaviorally.
///
/// `connect_socket` arms the retired 100 ms `READER_POLL_TIMEOUT` on this socket
/// and never takes it off, so the steady-state reader wakes ten times a second
/// forever on a quiet subscription — family F9 in the wiring ledger.
/// `read_timeout()` reads the live `SO_RCVTIMEO` off the very kernel socket the
/// reader thread blocks on (the writer handle and the reader's handle are
/// `try_clone` siblings), so this observes behaviour, not source.
///
/// The assertion is `None`, not "longer than before": a leaked setup deadline
/// would report `Some(5s)` and fail exactly as `Some(100ms)` does.
#[test]
fn the_subscription_reader_carries_no_read_window_in_steady_state() -> Result<(), SdkError> {
    let (address, server) = spawn_setup_server(Duration::ZERO)?;
    let subscription = SubscriptionStream::open(&address, "orders", Vec::new())?;
    let observed = subscription
        .writer
        .read_timeout()
        .map_err(|source| SdkError::Connection {
            description: format!("failed to read the subscription socket read timeout: {source}"),
        })?;
    assert_eq!(
        observed, None,
        "the subscription reader must block with no read window once the control \
         exchange is over; a Some(_) here is a cadence, whatever its period"
    );
    drop(subscription);
    server.join().ok();
    Ok(())
}

/// PARITY PIN (SDK-010 R3) — the same slow-reply fixture the push reader is red
/// against, aimed at the subscription reader.
///
/// This one is GREEN before the change as well as after: `subscription.rs`'s
/// `read_one_frame` already samples the 100 ms cadence against a 5 s
/// `SETUP_TIMEOUT` total deadline instead of dying on the first tick, so a
/// 250 ms reply already survives here. It is pinned so the two TCP readers are
/// held to ONE answer after the generalization, and so a regression toward the
/// push reader's fatal-on-first-timeout shape is caught.
#[test]
fn open_survives_control_replies_slower_than_the_retired_poll_cadence() -> Result<(), SdkError> {
    let (address, server) = spawn_setup_server(SLOW_BUT_ANSWERED)?;
    let subscription = SubscriptionStream::open(&address, "orders", Vec::new())?;
    assert_eq!(subscription.subscription_id(), 42);
    assert!(
        NAMED_SETUP_DEADLINE > SLOW_BUT_ANSWERED,
        "the fixture's slow reply must sit inside the named deadline"
    );
    drop(subscription);
    server.join().ok();
    Ok(())
}

/// TOMBSTONE (SDK-010 R5) — the retired reader poll family must not reappear in
/// this reader's production source.
#[test]
fn subscription_source_has_no_retired_reader_poll_family() {
    const SOURCE: &str = include_str!("subscription.rs");
    let production = SOURCE.split("#[cfg(test)]").next().unwrap_or(SOURCE);
    for forbidden in [
        "READER_POLL_TIMEOUT",
        "AtomicBool",
        "stop.load",
        "stop.store",
        "re-check the stop flag",
        "poll the stop flag",
    ] {
        assert!(
            !production.contains(forbidden),
            "retired subscription-reader poll-family source `{forbidden}` reappeared"
        );
    }
}
