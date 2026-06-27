mod known;
mod payload;

#[cfg(test)]
mod tests_support;

use super::causal::MessageId;
use super::envelope::SchemaId;
use super::error::ProtocolError;
use super::frame::{Frame, FrameType, HEADER_LEN, validate_stream};
use super::version::ProtocolVersion;
use known::decode_known_payload;
use payload::{
    PayloadReader, PayloadWriter, U16_LEN, U32_LEN, U64_LEN, bytes_field_len, checked_u32_len,
    option_string_len, option_u16_len, schema_ids_field_len, string_field_len, sum_lengths,
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
        | Frame::PublishError { message, .. } => {
            sum_lengths(&[U16_LEN, option_string_len(message.as_deref())?])
        }
        Frame::Disconnect { .. } | Frame::Ping { .. } | Frame::Pong { .. } => Ok(0),
        Frame::Subscribe {
            channel,
            accepted_schemas,
            ..
        } => sum_lengths(&[
            string_field_len(channel)?,
            schema_ids_field_len(accepted_schemas)?,
            U32_LEN,
        ]),
        Frame::SubscribeAck { .. } => sum_lengths(&[U64_LEN, SchemaId::WIRE_LEN]),
        Frame::Unsubscribe { .. } | Frame::PublishAck { .. } => Ok(U64_LEN),
        Frame::Publish {
            channel,
            envelope,
            idempotency_key,
            ..
        } => {
            let mut parts = vec![
                string_field_len(channel)?,
                envelope_bytes_field_len(envelope.encoded_len()?)?,
            ];
            if let Some(key) = idempotency_key {
                parts.push(string_field_len(key)?);
            }
            sum_lengths(&parts)
        }
        Frame::ConversationOpen { subject, .. } => {
            sum_lengths(&[U64_LEN, string_field_len(subject)?])
        }
        Frame::ConversationMessage { envelope, .. } => {
            sum_lengths(&[U64_LEN, envelope_bytes_field_len(envelope.encoded_len()?)?])
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
        Frame::Accept {
            referenced_message_id,
            ..
        } => message_id_field_len(referenced_message_id),
        Frame::Defer {
            referenced_message_id,
            reason,
            ..
        }
        | Frame::Reject {
            referenced_message_id,
            reason,
            ..
        } => sum_lengths(&[
            message_id_field_len(referenced_message_id)?,
            option_string_len(reason.as_deref())?,
        ]),
        Frame::Unknown { payload, .. } => checked_u32_len(payload.len()).map(|()| payload.len()),
    }
}

fn envelope_bytes_field_len(envelope_len: usize) -> Result<usize, ProtocolError> {
    checked_u32_len(envelope_len)?;
    sum_lengths(&[U32_LEN, envelope_len])
}

fn message_id_field_len(message_id: &MessageId) -> Result<usize, ProtocolError> {
    string_field_len(message_id.as_str())
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

fn write_pressure_payload(
    frame: &Frame,
    writer: &mut PayloadWriter<'_>,
) -> Result<(), ProtocolError> {
    match frame {
        Frame::Accept {
            referenced_message_id,
            ..
        } => writer.write_string_field(referenced_message_id.as_str()),
        Frame::Defer {
            referenced_message_id,
            reason,
            ..
        }
        | Frame::Reject {
            referenced_message_id,
            reason,
            ..
        } => {
            writer.write_string_field(referenced_message_id.as_str())?;
            writer.write_optional_string(reason.as_deref())
        }
        _ => Err(ProtocolError::codec("frame type was not a pressure frame")),
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
        } => {
            writer.write_u16(*reason_code)?;
            writer.write_optional_string(message.as_deref())?;
        }
        Frame::Disconnect { .. } | Frame::Ping { .. } | Frame::Pong { .. } => {}
        Frame::Subscribe {
            channel,
            accepted_schemas,
            max_in_flight,
            ..
        } => {
            writer.write_string_field(channel)?;
            writer.write_schema_ids_field(accepted_schemas)?;
            writer.write_u32(*max_in_flight)?;
        }
        Frame::SubscribeAck {
            subscription_id,
            selected_schema,
            ..
        } => {
            writer.write_u64(*subscription_id)?;
            writer.write_schema_id(*selected_schema)?;
        }
        Frame::Unsubscribe {
            subscription_id, ..
        } => writer.write_u64(*subscription_id)?,
        Frame::Publish {
            channel,
            envelope,
            idempotency_key,
            ..
        } => {
            writer.write_string_field(channel)?;
            writer.write_bytes_field(&envelope.serialize()?)?;
            // The trailing idempotency-key field is written ONLY when present, so
            // a no-key publish stays byte-identical to the pre-13-L1 layout. The
            // PUBLISH_IDEMPOTENCY_KEY_FLAG bit (set on construction) tells the
            // decoder whether to read it back.
            if let Some(key) = idempotency_key {
                writer.write_string_field(key)?;
            }
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
            envelope,
            ..
        } => {
            writer.write_u64(*conversation_id)?;
            writer.write_bytes_field(&envelope.serialize()?)?;
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
        Frame::Accept { .. } | Frame::Defer { .. } | Frame::Reject { .. } => {
            write_pressure_payload(frame, &mut writer)?;
        }
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
