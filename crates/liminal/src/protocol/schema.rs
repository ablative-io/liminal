use super::{
    envelope::SchemaId,
    error::ProtocolError,
    frame::{Frame, FrameType, validate_stream},
};

/// Select the schema that will be used for a subscription stream.
///
/// An empty accepted-schema list is an explicit opt-out from schema enforcement
/// and accepts the channel's declared schema. Otherwise matching is exact
/// [`SchemaId`] equality against the channel schema hash.
///
/// # Errors
///
/// Returns [`ProtocolError::SchemaIncompatible`] when the subscriber provided a
/// non-empty accepted-schema list that does not contain the channel schema.
pub fn negotiate_schema(
    channel_schema: SchemaId,
    accepted_schemas: &[SchemaId],
) -> Result<SchemaId, ProtocolError> {
    if accepted_schemas.is_empty() || accepted_schemas.contains(&channel_schema) {
        Ok(channel_schema)
    } else {
        Err(ProtocolError::SchemaIncompatible {
            message: Some("subscriber does not accept channel schema".to_owned()),
        })
    }
}

/// Construct a `SubscribeError` frame for a failed subscription negotiation.
///
/// This keeps schema incompatibility visible on the subscription stream as an
/// explicit protocol error frame with the stable numeric reason code carried by
/// [`ProtocolError::reason_code`].
///
/// # Errors
///
/// Returns [`ProtocolError::InvalidStream`] when `stream_id` is not an
/// application stream.
pub fn subscribe_error_frame(
    stream_id: u32,
    error: &ProtocolError,
) -> Result<Frame, ProtocolError> {
    validate_stream(FrameType::SubscribeError, stream_id)?;
    Ok(Frame::SubscribeError {
        flags: 0,
        stream_id,
        reason_code: error.reason_code(),
        message: error.message().map(str::to_owned),
    })
}

#[cfg(test)]
mod tests {
    use super::{negotiate_schema, subscribe_error_frame};
    use crate::protocol::{Frame, ProtocolError, SchemaId};

    #[test]
    fn negotiation_selects_channel_schema_when_accepted() -> Result<(), ProtocolError> {
        let hash_a = schema(0xA0);
        let hash_b = schema(0xB0);

        let selected = negotiate_schema(hash_a, &[hash_a, hash_b])?;

        assert_eq!(selected, hash_a);
        Ok(())
    }

    #[test]
    fn negotiation_reports_schema_incompatible_with_distinct_reason_code() {
        let hash_a = schema(0xA0);
        let hash_b = schema(0xB0);
        let hash_c = schema(0xC0);

        let result = negotiate_schema(hash_a, &[hash_b, hash_c]);

        assert!(matches!(
            &result,
            Err(ProtocolError::SchemaIncompatible { .. })
        ));
        let reason_code = result.err().map_or(0, |error| error.reason_code());
        assert_eq!(reason_code, ProtocolError::SCHEMA_INCOMPATIBLE_CODE);
        assert_ne!(reason_code, ProtocolError::CODEC_ERROR_CODE);
    }

    #[test]
    fn negotiation_accepts_empty_list_as_opt_out() -> Result<(), ProtocolError> {
        let hash_a = schema(0xA0);

        let selected = negotiate_schema(hash_a, &[])?;

        assert_eq!(selected, hash_a);
        Ok(())
    }

    #[test]
    fn negotiation_requires_exact_schema_id_equality() {
        let channel_schema = SchemaId::new([0xAB; SchemaId::WIRE_LEN]);
        let mut near_match = [0xAB; SchemaId::WIRE_LEN];
        near_match[SchemaId::WIRE_LEN - 1] = 0xAC;

        let result = negotiate_schema(channel_schema, &[SchemaId::new(near_match)]);

        assert!(matches!(
            result,
            Err(ProtocolError::SchemaIncompatible { .. })
        ));
    }

    #[test]
    fn schema_incompatible_error_builds_subscribe_error_frame() -> Result<(), ProtocolError> {
        let hash_a = schema(0xA0);
        let hash_b = schema(0xB0);
        let Err(error) = negotiate_schema(hash_a, &[hash_b]) else {
            return Err(ProtocolError::codec(
                "schema negotiation unexpectedly succeeded",
            ));
        };

        let frame = subscribe_error_frame(5, &error)?;

        assert!(matches!(
            frame,
            Frame::SubscribeError {
                stream_id: 5,
                reason_code,
                message,
                ..
            } if reason_code == ProtocolError::SCHEMA_INCOMPATIBLE_CODE
                && message.as_deref() == Some("subscriber does not accept channel schema")
        ));
        Ok(())
    }

    fn schema(seed: u8) -> SchemaId {
        SchemaId::new([seed; SchemaId::WIRE_LEN])
    }
}
