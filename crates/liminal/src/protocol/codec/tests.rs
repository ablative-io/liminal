use super::{decode, encode, encoded_len};
use crate::protocol::{Frame, FrameType, ProtocolError};

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
        payload: vec![0xDE, 0xAD, 0xBE, 0xEF],
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
fn payload_bearing_frames_preserve_payload_bytes() -> Result<(), ProtocolError> {
    let publish_payload = vec![0, 1, 2, 3, 255];
    let publish = Frame::Publish {
        flags: 0,
        stream_id: 7,
        channel: "payloads".to_owned(),
        payload: publish_payload.clone(),
    };
    let decoded_publish = round_trip(&publish)?;
    assert!(matches!(
        decoded_publish,
        Frame::Publish { payload, .. } if payload == publish_payload
    ));

    let conversation_payload = vec![9, 8, 7, 6, 5];
    let conversation = Frame::ConversationMessage {
        flags: 1,
        stream_id: 8,
        conversation_id: 42,
        payload: conversation_payload.clone(),
    };
    let decoded_conversation = round_trip(&conversation)?;
    assert!(matches!(
        decoded_conversation,
        Frame::ConversationMessage { payload, .. } if payload == conversation_payload
    ));
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
        &[u8::from(FrameType::ConnectAck), 0, 0, 0, 0, 0, 0, 0, 0, 1],
        &[u8::from(FrameType::Subscribe), 0, 0, 0, 0, 1, 0, 0, 0, 0],
        &[u8::from(FrameType::Ping), 0, 0, 0, 0, 0, 0, 0, 0, 2, 1, 2],
    ];

    for input in cases {
        let _ = decode(input);
    }
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

fn sample_frames() -> Vec<Frame> {
    let mut frames = Vec::new();
    frames.extend(control_frames());
    frames.extend(subscription_frames());
    frames.extend(publish_frames());
    frames.extend(conversation_frames());
    frames.extend(pressure_frames());
    frames
}

fn control_frames() -> [Frame; 6] {
    [
        Frame::Connect {
            flags: 0,
            min_version: 1,
            max_version: 3,
            auth_token: vec![1, 2, 3, 4],
        },
        Frame::ConnectAck {
            flags: 1,
            version: 3,
        },
        Frame::ConnectError {
            flags: 2,
            reason_code: ProtocolError::AUTHENTICATION_FAILURE_CODE,
            message: Some("bad token".to_owned()),
        },
        Frame::Disconnect { flags: 3 },
        Frame::Ping { flags: 18 },
        Frame::Pong { flags: 19 },
    ]
}

fn subscription_frames() -> [Frame; 4] {
    [
        Frame::Subscribe {
            flags: 4,
            stream_id: 1,
            channel: "orders".to_owned(),
            schema: Some("application/json".to_owned()),
        },
        Frame::SubscribeAck {
            flags: 5,
            stream_id: 1,
            subscription_id: 101,
        },
        Frame::SubscribeError {
            flags: 6,
            stream_id: 1,
            reason_code: ProtocolError::SCHEMA_INCOMPATIBLE_CODE,
            message: Some("schema rejected".to_owned()),
        },
        Frame::Unsubscribe {
            flags: 7,
            stream_id: 1,
            subscription_id: 101,
        },
    ]
}

fn publish_frames() -> [Frame; 3] {
    [
        Frame::Publish {
            flags: 8,
            stream_id: 2,
            channel: "orders".to_owned(),
            payload: vec![0x10, 0x20, 0x30],
        },
        Frame::PublishAck {
            flags: 9,
            stream_id: 2,
            message_id: 202,
        },
        Frame::PublishError {
            flags: 10,
            stream_id: 2,
            reason_code: ProtocolError::CODEC_ERROR_CODE,
            message: Some("publish rejected".to_owned()),
        },
    ]
}

fn conversation_frames() -> [Frame; 4] {
    [
        Frame::ConversationOpen {
            flags: 11,
            stream_id: 3,
            conversation_id: 303,
            subject: "support".to_owned(),
        },
        Frame::ConversationMessage {
            flags: 12,
            stream_id: 3,
            conversation_id: 303,
            payload: vec![0xAB, 0xCD],
        },
        Frame::ConversationClose {
            flags: 13,
            stream_id: 3,
            conversation_id: 303,
            reason_code: None,
            message: Some("done".to_owned()),
        },
        Frame::ConversationError {
            flags: 14,
            stream_id: 3,
            conversation_id: 303,
            reason_code: ProtocolError::INVALID_STATE_TRANSITION_CODE,
            message: Some("bad state".to_owned()),
        },
    ]
}

fn pressure_frames() -> [Frame; 3] {
    [
        Frame::Accept {
            flags: 15,
            stream_id: 4,
            credit: 16,
        },
        Frame::Defer {
            flags: 16,
            stream_id: 4,
            retry_after_ms: 250,
        },
        Frame::Reject {
            flags: 17,
            stream_id: 4,
            reason_code: ProtocolError::CODEC_ERROR_CODE,
            message: Some("over capacity".to_owned()),
        },
    ]
}
