#[path = "../../../../crates/liminal/src/protocol/mod.rs"]
pub mod protocol;

use protocol::{
    CausalContext, Frame, MessageEnvelope, ProtocolError, ProtocolVersion, SchemaId, decode,
    encode, encoded_len,
};
use wasm_bindgen::prelude::*;

const SCHEMA_ID_LEN: usize = SchemaId::WIRE_LEN;

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

/// Decode one canonical liminal protocol frame and return its message payload.
///
/// The returned bytes are prefixed with the decoded frame length as a big-endian
/// `u32`, followed by the payload bytes for message-bearing frames. Non-message
/// frames return only the length prefix.
///
/// # Errors
///
/// Returns a JavaScript error when the canonical Rust protocol decoder rejects
/// the frame bytes or when the decoded length cannot fit in a JavaScript-facing
/// `u32` prefix.
#[wasm_bindgen]
pub fn receive(bytes: &[u8]) -> Result<Vec<u8>, JsValue> {
    let (frame, consumed) = decode(bytes).map_err(|error| protocol_error(&error))?;
    let payload = match frame {
        Frame::Publish { envelope, .. } | Frame::ConversationMessage { envelope, .. } => {
            envelope.payload
        }
        Frame::Unknown { payload, .. } => payload,
        _ => Vec::new(),
    };
    let mut decoded = Vec::with_capacity(4 + payload.len());
    let consumed =
        u32::try_from(consumed).map_err(|_| js_error("decoded frame length exceeded u32"))?;
    decoded.extend_from_slice(&consumed.to_be_bytes());
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
