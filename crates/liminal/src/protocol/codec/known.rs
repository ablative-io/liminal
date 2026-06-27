use crate::protocol::{
    Frame, FrameType, MessageEnvelope, MessageId, ProtocolError, ProtocolVersion, StreamPressure,
    frame::PUBLISH_IDEMPOTENCY_KEY_FLAG,
};

use super::payload::PayloadReader;

pub(super) fn decode_known_payload(
    frame_type: FrameType,
    flags: u8,
    stream_id: u32,
    payload: &[u8],
) -> Result<Frame, ProtocolError> {
    let mut reader = PayloadReader::new(payload);
    let frame = match frame_type {
        FrameType::Connect
        | FrameType::ConnectAck
        | FrameType::ConnectError
        | FrameType::Disconnect
        | FrameType::Ping
        | FrameType::Pong => decode_control_payload(frame_type, flags, &mut reader)?,
        FrameType::Subscribe
        | FrameType::SubscribeAck
        | FrameType::SubscribeError
        | FrameType::Unsubscribe => {
            decode_subscription_payload(frame_type, flags, stream_id, &mut reader)?
        }
        FrameType::Publish | FrameType::PublishAck | FrameType::PublishError => {
            decode_publish_payload(frame_type, flags, stream_id, &mut reader)?
        }
        FrameType::ConversationOpen
        | FrameType::ConversationMessage
        | FrameType::ConversationClose
        | FrameType::ConversationError => {
            decode_conversation_payload(frame_type, flags, stream_id, &mut reader)?
        }
        FrameType::Accept | FrameType::Defer | FrameType::Reject => {
            decode_pressure_payload(frame_type, flags, stream_id, &mut reader)?
        }
        FrameType::Unknown(type_id) => Frame::Unknown {
            type_id,
            flags,
            stream_id,
            payload: payload.to_vec(),
        },
    };
    reader.finish()?;
    Ok(frame)
}

fn read_protocol_version(reader: &mut PayloadReader<'_>) -> Result<ProtocolVersion, ProtocolError> {
    Ok(ProtocolVersion::new(reader.read_u16()?, reader.read_u16()?))
}

fn decode_control_payload(
    frame_type: FrameType,
    flags: u8,
    reader: &mut PayloadReader<'_>,
) -> Result<Frame, ProtocolError> {
    match frame_type {
        FrameType::Connect => Ok(Frame::Connect {
            flags,
            min_version: read_protocol_version(reader)?,
            max_version: read_protocol_version(reader)?,
            auth_token: reader.read_bytes_field()?,
        }),
        FrameType::ConnectAck => Ok(Frame::ConnectAck {
            flags,
            selected_version: read_protocol_version(reader)?,
            capabilities: reader.read_u32()?,
        }),
        FrameType::ConnectError => Ok(Frame::ConnectError {
            flags,
            reason_code: reader.read_u16()?,
            message: reader.read_optional_string()?,
        }),
        FrameType::Disconnect => Ok(Frame::Disconnect { flags }),
        FrameType::Ping => Ok(Frame::Ping { flags }),
        FrameType::Pong => Ok(Frame::Pong { flags }),
        _ => Err(ProtocolError::codec("frame type was not a control frame")),
    }
}

fn decode_subscription_payload(
    frame_type: FrameType,
    flags: u8,
    stream_id: u32,
    reader: &mut PayloadReader<'_>,
) -> Result<Frame, ProtocolError> {
    match frame_type {
        FrameType::Subscribe => {
            let channel = reader.read_string_field()?;
            let accepted_schemas = reader.read_schema_ids_field()?;
            let max_in_flight = reader.read_u32()?;
            StreamPressure::new(max_in_flight)?;
            Ok(Frame::Subscribe {
                flags,
                stream_id,
                channel,
                accepted_schemas,
                max_in_flight,
            })
        }
        FrameType::SubscribeAck => Ok(Frame::SubscribeAck {
            flags,
            stream_id,
            subscription_id: reader.read_u64()?,
            selected_schema: reader.read_schema_id()?,
        }),
        FrameType::SubscribeError => Ok(Frame::SubscribeError {
            flags,
            stream_id,
            reason_code: reader.read_u16()?,
            message: reader.read_optional_string()?,
        }),
        FrameType::Unsubscribe => Ok(Frame::Unsubscribe {
            flags,
            stream_id,
            subscription_id: reader.read_u64()?,
        }),
        _ => Err(ProtocolError::codec(
            "frame type was not a subscription frame",
        )),
    }
}

fn decode_publish_payload(
    frame_type: FrameType,
    flags: u8,
    stream_id: u32,
    reader: &mut PayloadReader<'_>,
) -> Result<Frame, ProtocolError> {
    match frame_type {
        FrameType::Publish => {
            let channel = reader.read_string_field()?;
            let envelope = MessageEnvelope::deserialize(&reader.read_bytes_field()?)?;
            // The trailing idempotency-key field is present ONLY when the publisher
            // set PUBLISH_IDEMPOTENCY_KEY_FLAG; otherwise the payload ends here and
            // `reader.finish()` confirms no trailing bytes, exactly as before.
            let idempotency_key = if flags & PUBLISH_IDEMPOTENCY_KEY_FLAG == 0 {
                None
            } else {
                Some(reader.read_string_field()?)
            };
            Ok(Frame::Publish {
                flags,
                stream_id,
                channel,
                envelope,
                idempotency_key,
            })
        }
        FrameType::PublishAck => Ok(Frame::PublishAck {
            flags,
            stream_id,
            message_id: reader.read_u64()?,
        }),
        FrameType::PublishError => Ok(Frame::PublishError {
            flags,
            stream_id,
            reason_code: reader.read_u16()?,
            message: reader.read_optional_string()?,
        }),
        _ => Err(ProtocolError::codec("frame type was not a publish frame")),
    }
}

fn decode_conversation_payload(
    frame_type: FrameType,
    flags: u8,
    stream_id: u32,
    reader: &mut PayloadReader<'_>,
) -> Result<Frame, ProtocolError> {
    let conversation_id = reader.read_u64()?;
    match frame_type {
        FrameType::ConversationOpen => Ok(Frame::ConversationOpen {
            flags,
            stream_id,
            conversation_id,
            subject: reader.read_string_field()?,
        }),
        FrameType::ConversationMessage => Ok(Frame::ConversationMessage {
            flags,
            stream_id,
            conversation_id,
            envelope: MessageEnvelope::deserialize(&reader.read_bytes_field()?)?,
        }),
        FrameType::ConversationClose => Ok(Frame::ConversationClose {
            flags,
            stream_id,
            conversation_id,
            reason_code: reader.read_optional_u16()?,
            message: reader.read_optional_string()?,
        }),
        FrameType::ConversationError => Ok(Frame::ConversationError {
            flags,
            stream_id,
            conversation_id,
            reason_code: reader.read_u16()?,
            message: reader.read_optional_string()?,
        }),
        _ => Err(ProtocolError::codec(
            "frame type was not a conversation frame",
        )),
    }
}

fn decode_pressure_payload(
    frame_type: FrameType,
    flags: u8,
    stream_id: u32,
    reader: &mut PayloadReader<'_>,
) -> Result<Frame, ProtocolError> {
    match frame_type {
        FrameType::Accept => Ok(Frame::Accept {
            flags,
            stream_id,
            referenced_message_id: MessageId::new(reader.read_string_field()?),
        }),
        FrameType::Defer => Ok(Frame::Defer {
            flags,
            stream_id,
            referenced_message_id: MessageId::new(reader.read_string_field()?),
            reason: reader.read_optional_string()?,
        }),
        FrameType::Reject => Ok(Frame::Reject {
            flags,
            stream_id,
            referenced_message_id: MessageId::new(reader.read_string_field()?),
            reason: reader.read_optional_string()?,
        }),
        _ => Err(ProtocolError::codec("frame type was not a pressure frame")),
    }
}
