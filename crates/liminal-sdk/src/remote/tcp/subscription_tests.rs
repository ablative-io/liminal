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
