use super::{decode, encode, encoded_len};
use crate::protocol::{
    CausalContext, Frame, FrameType, MessageEnvelope, MessageId, ProtocolError, SchemaId,
    extract_causal_context,
};

use super::tests_support::{
    pressure_frames, publish_envelope_bytes, round_trip, sample_envelope, sample_frames,
    sample_schema,
};

#[test]
fn round_trips_all_named_frame_types() -> Result<(), ProtocolError> {
    for frame in sample_frames() {
        let len = encoded_len(&frame)?;
        let mut buffer = vec![0_u8; len];
        let written = encode(&frame, &mut buffer)?;
        assert_eq!(written, len);
        assert_eq!(
            usize::from(buffer[0]),
            usize::from(u8::from(frame.frame_type()))
        );

        let (decoded, consumed) = decode(&buffer)?;
        assert_eq!(consumed, len);
        assert_eq!(decoded, frame);
    }
    Ok(())
}

#[test]
fn encode_writes_header_fields_in_wire_order() -> Result<(), ProtocolError> {
    let frame = Frame::Publish {
        flags: 0xA5,
        stream_id: 0x0102_0304,
        channel: "orders".to_owned(),
        envelope: sample_envelope(vec![0xDE, 0xAD, 0xBE, 0xEF]),
    };
    let mut buffer = vec![0_u8; encoded_len(&frame)?];
    let written = encode(&frame, &mut buffer)?;
    let Ok(payload_len) = u32::try_from(written - 10) else {
        return Err(ProtocolError::codec("test payload length exceeded u32"));
    };

    assert_eq!(written, buffer.len());
    assert_eq!(buffer[0], u8::from(FrameType::Publish));
    assert_eq!(buffer[1], 0xA5);
    assert_eq!(&buffer[2..6], &0x0102_0304_u32.to_be_bytes());
    assert_eq!(&buffer[6..10], &payload_len.to_be_bytes());
    Ok(())
}

#[test]
fn message_frames_preserve_envelope_payload_bytes() -> Result<(), ProtocolError> {
    let publish_envelope = sample_envelope(vec![0, 1, 2, 3, 255]);
    let publish = Frame::Publish {
        flags: 0,
        stream_id: 7,
        channel: "payloads".to_owned(),
        envelope: publish_envelope.clone(),
    };
    let decoded_publish = round_trip(&publish)?;
    assert!(matches!(
        decoded_publish,
        Frame::Publish { envelope, .. } if envelope == publish_envelope
    ));

    let conversation_envelope = sample_envelope(vec![9, 8, 7, 6, 5]);
    let conversation = Frame::ConversationMessage {
        flags: 1,
        stream_id: 8,
        conversation_id: 42,
        envelope: conversation_envelope.clone(),
    };
    let decoded_conversation = round_trip(&conversation)?;
    assert!(matches!(
        decoded_conversation,
        Frame::ConversationMessage { envelope, .. } if envelope == conversation_envelope
    ));
    Ok(())
}

#[test]
fn subscription_schema_fields_round_trip_and_remain_accessible() -> Result<(), ProtocolError> {
    let hash_a = sample_schema(0xA1);
    let hash_b = sample_schema(0xB2);
    let decoded = round_trip(&Frame::Subscribe {
        flags: 4,
        stream_id: 9,
        channel: "orders".to_owned(),
        accepted_schemas: vec![hash_a, hash_b],
        max_in_flight: 100,
    })?;
    assert!(
        matches!(decoded, Frame::Subscribe { accepted_schemas, max_in_flight, .. }
        if accepted_schemas == vec![hash_a, hash_b] && max_in_flight == 100)
    );

    let decoded = round_trip(&Frame::Subscribe {
        flags: 4,
        stream_id: 9,
        channel: "orders".to_owned(),
        accepted_schemas: Vec::new(),
        max_in_flight: 100,
    })?;
    assert!(
        matches!(decoded, Frame::Subscribe { accepted_schemas, max_in_flight, .. }
        if accepted_schemas.is_empty() && max_in_flight == 100)
    );

    let selected_schema = sample_schema(0xC3);
    let decoded = round_trip(&Frame::SubscribeAck {
        flags: 5,
        stream_id: 9,
        subscription_id: 101,
        selected_schema,
    })?;
    assert!(matches!(
        decoded,
        Frame::SubscribeAck { selected_schema: decoded_schema, .. } if decoded_schema == selected_schema
    ));
    Ok(())
}

#[test]
fn pressure_frames_preserve_referenced_message_ids_and_reasons() -> Result<(), ProtocolError> {
    for frame in pressure_frames() {
        assert_eq!(round_trip(&frame)?, frame);
    }
    assert!(matches!(
        encoded_len(&Frame::Subscribe {
            flags: 4,
            stream_id: 9,
            channel: "orders".to_owned(),
            accepted_schemas: Vec::new(),
            max_in_flight: 0,
        }),
        Err(ProtocolError::CodecError { .. })
    ));
    Ok(())
}

#[test]
fn causal_context_extracts_from_publish_frame_envelope_bytes() -> Result<(), ProtocolError> {
    let causal_context = CausalContext {
        parent_id: Some(MessageId::from("publish-parent")),
        vector_clock_entry: Some(77),
    };
    let envelope = MessageEnvelope::new(
        SchemaId::new([0x33; 32]),
        causal_context.clone(),
        vec![0xCA, 0xFE, 0xBA, 0xBE],
    );
    let frame = Frame::Publish {
        flags: 0,
        stream_id: 7,
        channel: "payloads".to_owned(),
        envelope,
    };
    let mut buffer = vec![0_u8; encoded_len(&frame)?];
    let written = encode(&frame, &mut buffer)?;
    let envelope_bytes = publish_envelope_bytes(&buffer[..written], "payloads")?;

    assert_eq!(extract_causal_context(envelope_bytes)?, causal_context);
    Ok(())
}

#[test]
fn no_payload_frames_round_trip_as_header_only() -> Result<(), ProtocolError> {
    for frame in [
        Frame::Disconnect { flags: 0 },
        Frame::Ping { flags: 0 },
        Frame::Pong { flags: 0 },
    ] {
        let mut buffer = vec![0_u8; encoded_len(&frame)?];
        let written = encode(&frame, &mut buffer)?;
        assert_eq!(written, 10);
        assert_eq!(&buffer[6..10], &0_u32.to_be_bytes());
        let (decoded, consumed) = decode(&buffer)?;
        assert_eq!(consumed, 10);
        assert_eq!(decoded, frame);
    }
    Ok(())
}

#[test]
fn conversation_close_optional_reason_code_round_trips() -> Result<(), ProtocolError> {
    for reason_code in [None, Some(0x0100)] {
        let frame = Frame::ConversationClose {
            flags: 13,
            stream_id: 3,
            conversation_id: 303,
            reason_code,
            message: Some("done".to_owned()),
        };

        assert_eq!(round_trip(&frame)?, frame);
    }
    Ok(())
}

#[test]
fn decode_short_header_returns_incomplete_header() {
    let result = decode(&[0_u8; 9]);
    assert!(matches!(
        result,
        Err(ProtocolError::IncompleteHeader { .. })
    ));
}

#[test]
fn decode_truncated_payload_returns_truncated_payload() {
    let input = [
        u8::from(FrameType::Publish),
        0,
        0,
        0,
        0,
        1,
        0,
        0,
        0,
        4,
        0xAA,
        0xBB,
    ];
    let result = decode(&input);
    assert!(matches!(
        result,
        Err(ProtocolError::TruncatedPayload { .. })
    ));
}

#[test]
fn decode_unknown_frame_type_returns_unknown_and_consumes_payload() -> Result<(), ProtocolError> {
    let input = [0xFE, 0x7F, 0, 0, 0, 9, 0, 0, 0, 3, 0xCA, 0xFE, 0xBA];
    let (frame, consumed) = decode(&input)?;

    assert_eq!(consumed, input.len());
    assert_eq!(
        frame,
        Frame::Unknown {
            type_id: 0xFE,
            flags: 0x7F,
            stream_id: 9,
            payload: vec![0xCA, 0xFE, 0xBA],
        }
    );
    Ok(())
}

#[test]
fn decode_rejects_invalid_stream_without_panicking() {
    let input = [u8::from(FrameType::Ping), 0, 0, 0, 0, 1, 0, 0, 0, 0];
    let result = decode(&input);
    assert!(matches!(result, Err(ProtocolError::InvalidStream { .. })));
}

#[test]
fn decode_handles_garbage_inputs_without_panicking() {
    let cases: &[&[u8]] = &[
        &[],
        &[0xFF],
        &[0xFF; 9],
        &[
            u8::from(FrameType::ConnectAck),
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            7,
            0,
        ],
        &[u8::from(FrameType::Subscribe), 0, 0, 0, 0, 1, 0, 0, 0, 0],
        &[u8::from(FrameType::Ping), 0, 0, 0, 0, 0, 0, 0, 0, 2, 1, 2],
    ];

    for input in cases {
        let _ = decode(input);
    }
}
