use super::{RECEIVE_PREFIX_LEN, SUBSCRIBE_MAX_IN_FLIGHT, receive, send, subscribe, unsubscribe};
use crate::protocol::{
    CausalContext, Frame, MessageEnvelope, ProtocolError, SchemaId, decode, encode, encoded_len,
};

#[test]
fn subscribe_encodes_canonical_frame() -> Result<(), ProtocolError> {
    let first = [0x11; SchemaId::WIRE_LEN];
    let second = [0x22; SchemaId::WIRE_LEN];
    let mut accepted = first.to_vec();
    accepted.extend_from_slice(&second);

    let encoded = wasm_result(subscribe(7, "frame.demo.graph-view", &accepted))?;
    let (frame, consumed) = decode(&encoded)?;

    assert_eq!(consumed, encoded.len());
    assert!(matches!(
        frame,
        Frame::Subscribe {
            stream_id: 7,
            channel,
            accepted_schemas,
            max_in_flight: SUBSCRIBE_MAX_IN_FLIGHT,
            ..
        } if channel == "frame.demo.graph-view"
            && accepted_schemas == vec![SchemaId::new(first), SchemaId::new(second)]
    ));
    Ok(())
}

#[test]
fn unsubscribe_encodes_server_subscription_id() -> Result<(), ProtocolError> {
    let encoded = wasm_result(unsubscribe(9, 42))?;
    let (frame, consumed) = decode(&encoded)?;

    assert_eq!(consumed, encoded.len());
    assert!(matches!(
        frame,
        Frame::Unsubscribe {
            stream_id: 9,
            subscription_id: 42,
            ..
        }
    ));
    Ok(())
}

#[test]
fn receive_surfaces_deliver_type_and_exact_payload() -> Result<(), ProtocolError> {
    let payload = br#"{"componentId":"demo","kind":"snapshot"}"#.to_vec();
    let envelope = MessageEnvelope::new(
        SchemaId::new([0; SchemaId::WIRE_LEN]),
        CausalContext::independent(),
        payload.clone(),
    );
    let frame = Frame::new_deliver(3, 8, envelope)?;
    let encoded = encode_test_frame(&frame)?;

    let decoded = wasm_result(receive(&encoded))?;
    let encoded_len = u32::try_from(encoded.len())
        .map_err(|_| ProtocolError::codec("test frame length exceeded u32"))?;

    assert_eq!(read_u32(&decoded[0..4])?, encoded_len);
    assert_eq!(decoded[4], 0x19);
    assert_eq!(read_u32(&decoded[5..9])?, 3);
    assert_eq!(read_u64(&decoded[9..17])?, 0);
    assert_eq!(u16::from_be_bytes([decoded[17], decoded[18]]), 0);
    assert_eq!(&decoded[RECEIVE_PREFIX_LEN..], payload);
    Ok(())
}

#[test]
fn receive_surfaces_subscribe_ack_id() -> Result<(), ProtocolError> {
    let frame = Frame::SubscribeAck {
        flags: 0,
        stream_id: 5,
        subscription_id: 99,
        selected_schema: SchemaId::new([0xAB; SchemaId::WIRE_LEN]),
    };
    let encoded = encode_test_frame(&frame)?;

    let decoded = wasm_result(receive(&encoded))?;

    assert_eq!(decoded[4], 0x06);
    assert_eq!(read_u32(&decoded[5..9])?, 5);
    assert_eq!(read_u64(&decoded[9..17])?, 99);
    assert_eq!(decoded.len(), RECEIVE_PREFIX_LEN);
    Ok(())
}

#[test]
fn send_encodes_canonical_publish_frame() -> Result<(), ProtocolError> {
    let schema = [0x33; SchemaId::WIRE_LEN];
    let payload = br#"{"kind":"delta","seq":4}"#.to_vec();

    let encoded = wasm_result(send(1, "frame.demo.graph-view", &schema, &payload))?;
    let (frame, consumed) = decode(&encoded)?;

    assert_eq!(consumed, encoded.len());
    assert!(matches!(
        frame,
        Frame::Publish {
            stream_id: 1,
            channel,
            envelope,
            idempotency_key: None,
            ..
        } if channel == "frame.demo.graph-view"
            && envelope.schema_id == SchemaId::new(schema)
            && envelope.payload == payload
    ));
    Ok(())
}

#[test]
fn receive_surfaces_publish_ack_message_id() -> Result<(), ProtocolError> {
    let frame = Frame::PublishAck {
        flags: 0,
        stream_id: 1,
        message_id: 7_777,
    };
    let encoded = encode_test_frame(&frame)?;

    let decoded = wasm_result(receive(&encoded))?;

    assert_eq!(decoded[4], 0x0A);
    assert_eq!(read_u32(&decoded[5..9])?, 1);
    assert_eq!(read_u64(&decoded[9..17])?, 7_777);
    assert_eq!(u16::from_be_bytes([decoded[17], decoded[18]]), 0);
    assert_eq!(decoded.len(), RECEIVE_PREFIX_LEN);
    Ok(())
}

fn encode_test_frame(frame: &Frame) -> Result<Vec<u8>, ProtocolError> {
    let mut bytes = vec![0; encoded_len(frame)?];
    let written = encode(frame, &mut bytes)?;
    bytes.truncate(written);
    Ok(bytes)
}

fn wasm_result<T>(result: Result<T, wasm_bindgen::JsValue>) -> Result<T, ProtocolError> {
    result.map_err(|_| ProtocolError::codec("wasm bridge rejected a valid test frame"))
}

fn read_u32(bytes: &[u8]) -> Result<u32, ProtocolError> {
    let value = bytes
        .try_into()
        .map_err(|_| ProtocolError::codec("test slice was not four bytes"))?;
    Ok(u32::from_be_bytes(value))
}

fn read_u64(bytes: &[u8]) -> Result<u64, ProtocolError> {
    let value = bytes
        .try_into()
        .map_err(|_| ProtocolError::codec("test slice was not eight bytes"))?;
    Ok(u64::from_be_bytes(value))
}
