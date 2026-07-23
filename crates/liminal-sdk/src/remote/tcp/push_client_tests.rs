use super::*;
use liminal::protocol::FrameType;

#[test]
fn pushed_frame_exposes_correlation_and_payload() {
    let frame = PushedFrame {
        correlation_id: 7,
        payload: vec![1, 2, 3],
    };
    assert_eq!(frame.correlation_id(), 7);
    assert_eq!(frame.payload(), &[1, 2, 3]);
    assert_eq!(frame.into_payload(), vec![1, 2, 3]);
}

#[test]
fn publish_frame_round_trips_through_codec() -> Result<(), SdkError> {
    // The observability publish frame the drain leg writes: a Publish on the
    // reserved channel carrying opaque payload bytes verbatim.
    let envelope = MessageEnvelope::new(
        SchemaId::new([0_u8; SchemaId::WIRE_LEN]),
        CausalContext::independent(),
        vec![9, 9, 9],
    );
    let frame = Frame::new_publish(APPLICATION_STREAM_ID, OBSERVABILITY_CHANNEL, envelope)
        .map_err(|error| protocol_error(&error))?;
    let len = encoded_len(&frame).map_err(|error| protocol_error(&error))?;
    let mut bytes = vec![0_u8; len];
    let written = encode(&frame, &mut bytes).map_err(|error| protocol_error(&error))?;
    let (decoded, consumed) = decode(&bytes[..written]).map_err(|error| protocol_error(&error))?;
    assert_eq!(consumed, written);
    assert_eq!(decoded.frame_type(), FrameType::Publish);
    let Frame::Publish {
        channel, envelope, ..
    } = decoded
    else {
        return Err(SdkError::Protocol {
            description: "expected a Publish frame".to_string(),
        });
    };
    assert_eq!(channel, OBSERVABILITY_CHANNEL);
    assert_eq!(envelope.payload, vec![9, 9, 9]);
    Ok(())
}

#[test]
fn reply_frame_round_trips_through_codec() -> Result<(), SdkError> {
    let frame = Frame::new_push_reply(APPLICATION_STREAM_ID, 9, vec![4, 5])
        .map_err(|error| protocol_error(&error))?;
    let len = encoded_len(&frame).map_err(|error| protocol_error(&error))?;
    let mut bytes = vec![0_u8; len];
    let written = encode(&frame, &mut bytes).map_err(|error| protocol_error(&error))?;
    let (decoded, consumed) = decode(&bytes[..written]).map_err(|error| protocol_error(&error))?;
    assert_eq!(consumed, written);
    assert_eq!(decoded.frame_type(), FrameType::PushReply);
    Ok(())
}
