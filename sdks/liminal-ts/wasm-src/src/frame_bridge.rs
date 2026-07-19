use crate::protocol::{Frame, SchemaId};

pub const RECEIVE_PREFIX_LEN: usize = 19;

pub fn read_schema_ids(bytes: &[u8]) -> Result<Vec<SchemaId>, &'static str> {
    const INVALID_SCHEMA_LIST: &str =
        "accepted schemas must be a concatenation of 32-byte schema ids";
    if bytes.len() % SchemaId::WIRE_LEN != 0 {
        return Err(INVALID_SCHEMA_LIST);
    }
    bytes
        .chunks_exact(SchemaId::WIRE_LEN)
        .map(|chunk| {
            let schema_bytes: [u8; SchemaId::WIRE_LEN] =
                chunk.try_into().map_err(|_| INVALID_SCHEMA_LIST)?;
            Ok(SchemaId::new(schema_bytes))
        })
        .collect()
}

pub fn receive_parts(frame: Frame) -> (u64, u16, Vec<u8>) {
    match frame {
        Frame::Publish { envelope, .. }
        | Frame::ConversationMessage { envelope, .. }
        | Frame::Deliver { envelope, .. } => (0, 0, envelope.payload),
        Frame::SubscribeAck {
            subscription_id, ..
        } => (subscription_id, 0, Vec::new()),
        Frame::PublishAck { message_id, .. } => (message_id, 0, Vec::new()),
        Frame::ConnectError {
            reason_code,
            message,
            ..
        }
        | Frame::SubscribeError {
            reason_code,
            message,
            ..
        }
        | Frame::PublishError {
            reason_code,
            message,
            ..
        }
        | Frame::ConversationError {
            reason_code,
            message,
            ..
        } => (
            0,
            reason_code,
            message.map_or_else(Vec::new, String::into_bytes),
        ),
        Frame::Unknown { payload, .. } => (0, 0, payload),
        _ => (0, 0, Vec::new()),
    }
}
