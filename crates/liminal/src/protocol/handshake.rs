use super::{Frame, FrameType, ProtocolError, ProtocolVersion, validate_stream};

/// Construct a `Connect` frame on the connection-control stream.
///
/// The authentication token is opaque to the protocol and is carried without
/// validation or interpretation.
///
/// # Errors
///
/// Returns [`ProtocolError::InvalidStream`] when `stream_id` is not stream 0.
pub fn connect_frame(
    stream_id: u32,
    min_version: ProtocolVersion,
    max_version: ProtocolVersion,
    auth_token: impl Into<Vec<u8>>,
) -> Result<Frame, ProtocolError> {
    validate_stream(FrameType::Connect, stream_id)?;
    Ok(Frame::Connect {
        flags: 0,
        min_version,
        max_version,
        auth_token: auth_token.into(),
    })
}

/// Construct a `ConnectAck` frame on the connection-control stream.
///
/// Capabilities are an opaque server-defined bitfield at this layer.
///
/// # Errors
///
/// Returns [`ProtocolError::InvalidStream`] when `stream_id` is not stream 0.
pub fn connect_ack_frame(
    stream_id: u32,
    selected_version: ProtocolVersion,
    capabilities: u32,
) -> Result<Frame, ProtocolError> {
    validate_stream(FrameType::ConnectAck, stream_id)?;
    Ok(Frame::ConnectAck {
        flags: 0,
        selected_version,
        capabilities,
    })
}

/// Construct a `ConnectError` frame on the connection-control stream.
///
/// # Errors
///
/// Returns [`ProtocolError::InvalidStream`] when `stream_id` is not stream 0.
pub fn connect_error_frame(stream_id: u32, error: &ProtocolError) -> Result<Frame, ProtocolError> {
    validate_stream(FrameType::ConnectError, stream_id)?;
    Ok(Frame::ConnectError {
        flags: 0,
        reason_code: error.reason_code(),
        message: error.message().map(str::to_owned),
    })
}

#[cfg(test)]
mod tests {
    use super::{connect_ack_frame, connect_error_frame, connect_frame};
    use crate::protocol::{Frame, ProtocolError, ProtocolVersion, decode, encode, encoded_len};

    #[test]
    fn connect_constructor_builds_stream_zero_frame_with_opaque_token() -> Result<(), ProtocolError>
    {
        let token = vec![0, 159, 146, 150, 255];
        let frame = connect_frame(
            0,
            ProtocolVersion::new(1, 0),
            ProtocolVersion::new(2, 0),
            token.clone(),
        )?;

        assert_eq!(frame.stream_id(), 0);
        assert!(matches!(
            frame,
            Frame::Connect {
                min_version,
                max_version,
                auth_token,
                ..
            } if min_version == ProtocolVersion::new(1, 0)
                && max_version == ProtocolVersion::new(2, 0)
                && auth_token == token
        ));
        Ok(())
    }

    #[test]
    fn connect_constructor_rejects_non_zero_stream() {
        let result = connect_frame(
            1,
            ProtocolVersion::new(1, 0),
            ProtocolVersion::new(2, 0),
            [1_u8, 2, 3],
        );

        assert!(matches!(result, Err(ProtocolError::InvalidStream { .. })));
    }

    #[test]
    fn connect_frame_round_trips_through_codec() -> Result<(), ProtocolError> {
        let frame = connect_frame(
            0,
            ProtocolVersion::new(1, 0),
            ProtocolVersion::new(2, 1),
            [0_u8, 1, 2, 255],
        )?;

        assert_eq!(round_trip(&frame)?, frame);
        Ok(())
    }

    #[test]
    fn connect_ack_constructor_builds_stream_zero_frame() -> Result<(), ProtocolError> {
        let frame = connect_ack_frame(0, ProtocolVersion::new(2, 0), 0xA5A5_0001)?;

        assert_eq!(frame.stream_id(), 0);
        assert!(matches!(
            frame,
            Frame::ConnectAck {
                selected_version,
                capabilities,
                ..
            } if selected_version == ProtocolVersion::new(2, 0)
                && capabilities == 0xA5A5_0001
        ));
        Ok(())
    }

    #[test]
    fn connect_ack_constructor_rejects_non_zero_stream() {
        let result = connect_ack_frame(1, ProtocolVersion::new(1, 0), 0);

        assert!(matches!(result, Err(ProtocolError::InvalidStream { .. })));
    }

    #[test]
    fn connect_ack_frame_round_trips_through_codec() -> Result<(), ProtocolError> {
        let frame = connect_ack_frame(0, ProtocolVersion::new(3, 4), 0xCAFE_BABE)?;

        assert_eq!(round_trip(&frame)?, frame);
        Ok(())
    }

    #[test]
    fn connect_error_frame_uses_error_reason_code_and_message() -> Result<(), ProtocolError> {
        let error = ProtocolError::VersionMismatch {
            message: Some("no mutual version".to_owned()),
        };
        let frame = connect_error_frame(0, &error)?;

        assert_eq!(frame.stream_id(), 0);
        assert!(matches!(
            frame,
            Frame::ConnectError {
                reason_code,
                message,
                ..
            } if reason_code == ProtocolError::VERSION_MISMATCH_CODE
                && message.as_deref() == Some("no mutual version")
        ));
        Ok(())
    }

    #[test]
    fn connect_error_constructor_rejects_non_zero_stream() {
        let error = ProtocolError::VersionMismatch { message: None };
        let result = connect_error_frame(1, &error);

        assert!(matches!(result, Err(ProtocolError::InvalidStream { .. })));
    }

    fn round_trip(frame: &Frame) -> Result<Frame, ProtocolError> {
        let mut buffer = vec![0_u8; encoded_len(frame)?];
        let written = encode(frame, &mut buffer)?;
        let Some(encoded) = buffer.get(..written) else {
            return Err(ProtocolError::codec("encoded length exceeded test buffer"));
        };
        let (decoded, consumed) = decode(encoded)?;
        assert_eq!(consumed, written);
        Ok(decoded)
    }
}
