mod known;
mod payload;

use super::error::ProtocolError;
use super::frame::{Frame, FrameType, HEADER_LEN, validate_stream};
use super::version::ProtocolVersion;
use known::decode_known_payload;
use payload::{
    PayloadReader, PayloadWriter, U16_LEN, U32_LEN, U64_LEN, bytes_field_len, checked_u32_len,
    option_string_len, option_u16_len, string_field_len, sum_lengths,
};

/// Return the number of bytes needed to encode a frame.
///
/// # Errors
///
/// Returns [`ProtocolError`] when the frame violates stream invariants or its
/// payload cannot fit in the protocol's `u32` length fields.
pub fn encoded_len(frame: &Frame) -> Result<usize, ProtocolError> {
    frame.validate()?;
    let payload_len = encoded_payload_len(frame)?;
    HEADER_LEN
        .checked_add(payload_len)
        .ok_or_else(|| ProtocolError::codec("encoded frame length overflowed usize"))
}

/// Encode a frame into the provided byte buffer, returning bytes written.
///
/// The buffer must be at least [`encoded_len`] bytes long. Encoding writes the
/// fixed 10-byte header followed by the serialized payload and performs no heap
/// allocation.
///
/// # Errors
///
/// Returns [`ProtocolError`] when the frame violates stream invariants, its
/// payload cannot fit in the protocol's length fields, or the provided buffer is
/// too small.
pub fn encode(frame: &Frame, buffer: &mut [u8]) -> Result<usize, ProtocolError> {
    frame.validate()?;
    let payload_len = encoded_payload_len(frame)?;
    let payload_length = u32::try_from(payload_len)
        .map_err(|_| ProtocolError::codec("payload length exceeded u32::MAX"))?;
    let total_len = HEADER_LEN
        .checked_add(payload_len)
        .ok_or_else(|| ProtocolError::codec("encoded frame length overflowed usize"))?;

    if buffer.len() < total_len {
        return Err(ProtocolError::codec("output buffer is too small"));
    }

    let Some(header) = buffer.get_mut(..HEADER_LEN) else {
        return Err(ProtocolError::codec(
            "output buffer is too small for header",
        ));
    };
    write_header(frame, payload_length, header)?;

    let Some(payload) = buffer.get_mut(HEADER_LEN..total_len) else {
        return Err(ProtocolError::codec(
            "output buffer is too small for payload",
        ));
    };
    write_payload(frame, payload)?;

    Ok(total_len)
}

/// Decode one complete frame from a byte buffer.
///
/// Returns the decoded frame and the number of bytes consumed. Unknown frame
/// types are length-delimited and returned as [`Frame::Unknown`] without
/// producing an error.
///
/// # Errors
///
/// Returns [`ProtocolError::IncompleteHeader`] for buffers shorter than the
/// fixed header, [`ProtocolError::TruncatedPayload`] when the declared payload
/// is not fully present, and [`ProtocolError`] for malformed known-frame
/// payloads or invalid stream placement.
pub fn decode(buffer: &[u8]) -> Result<(Frame, usize), ProtocolError> {
    if buffer.len() < HEADER_LEN {
        return Err(ProtocolError::IncompleteHeader {
            message: Some("buffer shorter than fixed frame header".to_owned()),
        });
    }

    let Some(header) = buffer.get(..HEADER_LEN) else {
        return Err(ProtocolError::IncompleteHeader {
            message: Some("buffer shorter than fixed frame header".to_owned()),
        });
    };
    let mut header_reader = PayloadReader::new(header);
    let type_id = header_reader.read_u8()?;
    let flags = header_reader.read_u8()?;
    let stream_id = header_reader.read_u32()?;
    let payload_length = header_reader.read_u32()?;
    header_reader.finish()?;

    let payload_len = usize::try_from(payload_length)
        .map_err(|_| ProtocolError::codec("payload length cannot fit usize"))?;
    let total_len = HEADER_LEN
        .checked_add(payload_len)
        .ok_or_else(|| ProtocolError::codec("decoded frame length overflowed usize"))?;

    if buffer.len() < total_len {
        return Err(ProtocolError::TruncatedPayload {
            message: Some("buffer shorter than declared payload length".to_owned()),
        });
    }

    let Some(payload) = buffer.get(HEADER_LEN..total_len) else {
        return Err(ProtocolError::TruncatedPayload {
            message: Some("buffer shorter than declared payload length".to_owned()),
        });
    };

    let frame_type = FrameType::from(type_id);
    let frame = decode_payload(frame_type, flags, stream_id, payload)?;
    Ok((frame, total_len))
}

fn write_header(
    frame: &Frame,
    payload_length: u32,
    buffer: &mut [u8],
) -> Result<(), ProtocolError> {
    let mut writer = PayloadWriter::new(buffer);
    writer.write_u8(u8::from(frame.frame_type()))?;
    writer.write_u8(frame.flags())?;
    writer.write_u32(frame.stream_id())?;
    writer.write_u32(payload_length)?;
    writer.finish()
}

fn encoded_payload_len(frame: &Frame) -> Result<usize, ProtocolError> {
    match frame {
        Frame::Connect { auth_token, .. } => sum_lengths(&[
            ProtocolVersion::WIRE_LEN,
            ProtocolVersion::WIRE_LEN,
            bytes_field_len(auth_token)?,
        ]),
        Frame::ConnectAck { .. } => sum_lengths(&[ProtocolVersion::WIRE_LEN, U32_LEN]),
        Frame::ConnectError { message, .. }
        | Frame::SubscribeError { message, .. }
        | Frame::PublishError { message, .. }
        | Frame::Reject { message, .. } => {
            sum_lengths(&[U16_LEN, option_string_len(message.as_deref())?])
        }
        Frame::Disconnect { .. } | Frame::Ping { .. } | Frame::Pong { .. } => Ok(0),
        Frame::Subscribe {
            channel, schema, ..
        } => sum_lengths(&[
            string_field_len(channel)?,
            option_string_len(schema.as_deref())?,
        ]),
        Frame::SubscribeAck { .. } | Frame::Unsubscribe { .. } | Frame::PublishAck { .. } => {
            Ok(U64_LEN)
        }
        Frame::Publish {
            channel, payload, ..
        } => sum_lengths(&[string_field_len(channel)?, bytes_field_len(payload)?]),
        Frame::ConversationOpen { subject, .. } => {
            sum_lengths(&[U64_LEN, string_field_len(subject)?])
        }
        Frame::ConversationMessage { payload, .. } => {
            sum_lengths(&[U64_LEN, bytes_field_len(payload)?])
        }
        Frame::ConversationClose {
            reason_code,
            message,
            ..
        } => sum_lengths(&[
            U64_LEN,
            option_u16_len(*reason_code),
            option_string_len(message.as_deref())?,
        ]),
        Frame::ConversationError { message, .. } => {
            sum_lengths(&[U64_LEN, U16_LEN, option_string_len(message.as_deref())?])
        }
        Frame::Accept { .. } | Frame::Defer { .. } => Ok(U32_LEN),
        Frame::Unknown { payload, .. } => checked_u32_len(payload.len()).map(|()| payload.len()),
    }
}

fn write_handshake_payload(
    frame: &Frame,
    writer: &mut PayloadWriter<'_>,
) -> Result<(), ProtocolError> {
    match frame {
        Frame::Connect {
            min_version,
            max_version,
            auth_token,
            ..
        } => {
            writer.write_slice(&min_version.to_wire_bytes())?;
            writer.write_slice(&max_version.to_wire_bytes())?;
            writer.write_bytes_field(auth_token)
        }
        Frame::ConnectAck {
            selected_version,
            capabilities,
            ..
        } => {
            writer.write_slice(&selected_version.to_wire_bytes())?;
            writer.write_u32(*capabilities)
        }
        _ => Err(ProtocolError::codec("frame type was not a handshake frame")),
    }
}

fn write_payload(frame: &Frame, buffer: &mut [u8]) -> Result<(), ProtocolError> {
    let mut writer = PayloadWriter::new(buffer);
    match frame {
        Frame::Connect { .. } | Frame::ConnectAck { .. } => {
            write_handshake_payload(frame, &mut writer)?;
        }
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
        | Frame::Reject {
            reason_code,
            message,
            ..
        } => {
            writer.write_u16(*reason_code)?;
            writer.write_optional_string(message.as_deref())?;
        }
        Frame::Disconnect { .. } | Frame::Ping { .. } | Frame::Pong { .. } => {}
        Frame::Subscribe {
            channel, schema, ..
        } => {
            writer.write_string_field(channel)?;
            writer.write_optional_string(schema.as_deref())?;
        }
        Frame::SubscribeAck {
            subscription_id, ..
        }
        | Frame::Unsubscribe {
            subscription_id, ..
        } => writer.write_u64(*subscription_id)?,
        Frame::Publish {
            channel, payload, ..
        } => {
            writer.write_string_field(channel)?;
            writer.write_bytes_field(payload)?;
        }
        Frame::PublishAck { message_id, .. } => writer.write_u64(*message_id)?,
        Frame::ConversationOpen {
            conversation_id,
            subject,
            ..
        } => {
            writer.write_u64(*conversation_id)?;
            writer.write_string_field(subject)?;
        }
        Frame::ConversationMessage {
            conversation_id,
            payload,
            ..
        } => {
            writer.write_u64(*conversation_id)?;
            writer.write_bytes_field(payload)?;
        }
        Frame::ConversationClose {
            conversation_id,
            reason_code,
            message,
            ..
        } => {
            writer.write_u64(*conversation_id)?;
            writer.write_optional_u16(*reason_code)?;
            writer.write_optional_string(message.as_deref())?;
        }
        Frame::ConversationError {
            conversation_id,
            reason_code,
            message,
            ..
        } => {
            writer.write_u64(*conversation_id)?;
            writer.write_u16(*reason_code)?;
            writer.write_optional_string(message.as_deref())?;
        }
        Frame::Accept { credit, .. } => writer.write_u32(*credit)?,
        Frame::Defer { retry_after_ms, .. } => writer.write_u32(*retry_after_ms)?,
        Frame::Unknown { payload, .. } => writer.write_slice(payload)?,
    }
    writer.finish()
}

fn decode_payload(
    frame_type: FrameType,
    flags: u8,
    stream_id: u32,
    payload: &[u8],
) -> Result<Frame, ProtocolError> {
    if let FrameType::Unknown(type_id) = frame_type {
        return Ok(Frame::Unknown {
            type_id,
            flags,
            stream_id,
            payload: payload.to_vec(),
        });
    }

    validate_stream(frame_type, stream_id)?;
    decode_known_payload(frame_type, flags, stream_id, payload)
}

#[cfg(test)]
mod tests;
