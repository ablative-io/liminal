#[path = "../../../../crates/liminal/src/protocol/mod.rs"]
pub mod protocol;

mod frame_bridge;

#[cfg(test)]
mod tests;

use frame_bridge::{RECEIVE_PREFIX_LEN, read_schema_ids, receive_parts};
use protocol::{
    CausalContext, Frame, MessageEnvelope, ProtocolError, ProtocolVersion, SchemaId, decode,
    encode, encoded_len,
};
use wasm_bindgen::prelude::*;

const SCHEMA_ID_LEN: usize = SchemaId::WIRE_LEN;
const SUBSCRIBE_MAX_IN_FLIGHT: u32 = 1024;

/// Encode a canonical liminal connect frame for the protocol control stream.
///
/// # Errors
///
/// Returns a JavaScript error when the canonical Rust protocol encoder rejects
/// the frame or cannot represent its encoded length.
#[wasm_bindgen]
pub fn connect(auth_token: &[u8]) -> Result<Vec<u8>, JsValue> {
    let frame = Frame::Connect {
        flags: 0,
        min_version: ProtocolVersion::new(1, 0),
        max_version: ProtocolVersion::new(1, 0),
        auth_token: auth_token.to_vec(),
    };
    encode_frame(&frame)
}

/// Encode a canonical liminal publish frame carrying a typed message payload.
///
/// # Errors
///
/// Returns a JavaScript error when the schema id is not 32 bytes, the stream id
/// violates protocol invariants, or the canonical Rust protocol encoder rejects
/// the frame.
#[wasm_bindgen]
pub fn send(
    stream_id: u32,
    channel: &str,
    schema_id: &[u8],
    payload: &[u8],
) -> Result<Vec<u8>, JsValue> {
    let schema_id = read_schema_id(schema_id)?;
    let envelope = MessageEnvelope::new(schema_id, CausalContext::independent(), payload.to_vec());
    let frame =
        Frame::new_publish(stream_id, channel, envelope).map_err(|error| protocol_error(&error))?;
    encode_frame(&frame)
}

/// Encode a canonical liminal subscribe frame.
///
/// `accepted_schemas` is the concatenation of zero or more 32-byte schema ids.
/// An empty slice uses the protocol's explicit schema-enforcement opt-out. The
/// in-flight bound matches the native liminal SDK subscription transport.
///
/// # Errors
///
/// Returns a JavaScript error when an accepted schema id is incomplete, the
/// stream id violates protocol invariants, or canonical encoding fails.
#[wasm_bindgen]
pub fn subscribe(
    stream_id: u32,
    channel: &str,
    accepted_schemas: &[u8],
) -> Result<Vec<u8>, JsValue> {
    let frame = Frame::Subscribe {
        flags: 0,
        stream_id,
        channel: channel.to_owned(),
        accepted_schemas: read_schema_ids(accepted_schemas).map_err(js_error)?,
        max_in_flight: SUBSCRIBE_MAX_IN_FLIGHT,
    };
    encode_frame(&frame)
}

/// Encode a canonical liminal unsubscribe frame.
///
/// # Errors
///
/// Returns a JavaScript error when the stream id violates protocol invariants
/// or canonical encoding fails.
#[wasm_bindgen]
pub fn unsubscribe(stream_id: u32, subscription_id: u64) -> Result<Vec<u8>, JsValue> {
    encode_frame(&Frame::Unsubscribe {
        flags: 0,
        stream_id,
        subscription_id,
    })
}

/// Decode one canonical liminal protocol frame and return typed metadata plus
/// its byte-preserved message payload.
///
/// The returned bytes use a fixed, no-serialization prefix:
///
/// - bytes 0..4: decoded frame length, big-endian `u32`
/// - byte 4: stable protocol frame-type discriminant
/// - bytes 5..9: stream id, big-endian `u32`
/// - bytes 9..17: `SubscribeAck` subscription id, big-endian `u64` (zero otherwise)
/// - bytes 17..19: protocol error reason, big-endian `u16` (zero otherwise)
/// - bytes 19..: exact envelope payload for message frames, or UTF-8 error detail
///
/// `Deliver` is deliberately message-bearing here: subscribers need its nested
/// envelope payload byte-for-byte, without a JSON decode/re-encode round trip.
///
/// # Errors
///
/// Returns a JavaScript error when the canonical Rust protocol decoder rejects
/// the frame bytes or when the decoded length cannot fit in a JavaScript-facing
/// `u32` prefix.
#[wasm_bindgen]
pub fn receive(bytes: &[u8]) -> Result<Vec<u8>, JsValue> {
    let (frame, consumed) = decode(bytes).map_err(|error| protocol_error(&error))?;
    let frame_type = u8::from(frame.frame_type());
    let stream_id = frame.stream_id();
    let (subscription_id, reason_code, payload) = receive_parts(frame);
    let mut decoded = Vec::with_capacity(RECEIVE_PREFIX_LEN + payload.len());
    let consumed =
        u32::try_from(consumed).map_err(|_| js_error("decoded frame length exceeded u32"))?;
    decoded.extend_from_slice(&consumed.to_be_bytes());
    decoded.push(frame_type);
    decoded.extend_from_slice(&stream_id.to_be_bytes());
    decoded.extend_from_slice(&subscription_id.to_be_bytes());
    decoded.extend_from_slice(&reason_code.to_be_bytes());
    decoded.extend_from_slice(&payload);
    Ok(decoded)
}

/// Encode a canonical liminal disconnect frame for the protocol control stream.
///
/// # Errors
///
/// Returns a JavaScript error when the canonical Rust protocol encoder rejects
/// the frame or cannot represent its encoded length.
#[wasm_bindgen]
pub fn close() -> Result<Vec<u8>, JsValue> {
    encode_frame(&Frame::Disconnect { flags: 0 })
}

fn encode_frame(frame: &Frame) -> Result<Vec<u8>, JsValue> {
    let len = encoded_len(frame).map_err(|error| protocol_error(&error))?;
    let mut bytes = vec![0; len];
    let written = encode(frame, &mut bytes).map_err(|error| protocol_error(&error))?;
    bytes.truncate(written);
    Ok(bytes)
}

fn read_schema_id(bytes: &[u8]) -> Result<SchemaId, JsValue> {
    let schema_bytes: [u8; SCHEMA_ID_LEN] = bytes
        .try_into()
        .map_err(|_| js_error("schema id must be exactly 32 bytes"))?;
    Ok(SchemaId::new(schema_bytes))
}

fn protocol_error(error: &ProtocolError) -> JsValue {
    let message = error.to_string();
    js_error(message.as_str())
}

fn js_error(message: &str) -> JsValue {
    JsValue::from_str(message)
}
