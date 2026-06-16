use crate::protocol::{Frame, FrameType, ProtocolError, ProtocolVersion};

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
        FrameType::Subscribe => Ok(Frame::Subscribe {
            flags,
            stream_id,
            channel: reader.read_string_field()?,
            schema: reader.read_optional_string()?,
        }),
        FrameType::SubscribeAck => Ok(Frame::SubscribeAck {
            flags,
            stream_id,
            subscription_id: reader.read_u64()?,
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
        FrameType::Publish => Ok(Frame::Publish {
            flags,
            stream_id,
            channel: reader.read_string_field()?,
            payload: reader.read_bytes_field()?,
        }),
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
            payload: reader.read_bytes_field()?,
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
            credit: reader.read_u32()?,
        }),
        FrameType::Defer => Ok(Frame::Defer {
            flags,
            stream_id,
            retry_after_ms: reader.read_u32()?,
        }),
        FrameType::Reject => Ok(Frame::Reject {
            flags,
            stream_id,
            reason_code: reader.read_u16()?,
            message: reader.read_optional_string()?,
        }),
        _ => Err(ProtocolError::codec("frame type was not a pressure frame")),
    }
}
