use crate::protocol::{
    CausalContext, Frame, MessageEnvelope, MessageId, ProtocolError, ProtocolVersion, SchemaId,
    WorkerRegisterOutcome, WorkerRegistration, decode, encode, encoded_len,
};

pub(super) fn round_trip(frame: &Frame) -> Result<Frame, ProtocolError> {
    let mut buffer = vec![0_u8; encoded_len(frame)?];
    let written = encode(frame, &mut buffer)?;
    let Some(encoded) = buffer.get(..written) else {
        return Err(ProtocolError::codec("encoded length exceeded test buffer"));
    };
    let (decoded, consumed) = decode(encoded)?;
    assert_eq!(consumed, written);
    Ok(decoded)
}

pub(super) fn sample_frames() -> Vec<Frame> {
    let mut frames = Vec::new();
    frames.extend(control_frames());
    frames.extend(subscription_frames());
    frames.extend(publish_frames());
    frames.extend(conversation_frames());
    frames.extend(pressure_frames());
    frames.extend(push_frames());
    frames.extend(worker_register_frames());
    frames.extend(deliver_frames());
    frames
}

pub(super) fn deliver_frames() -> [Frame; 2] {
    [
        Frame::Deliver {
            flags: 0,
            stream_id: 6,
            delivery_seq: 1,
            envelope: sample_envelope(vec![0xD0, 0xD1, 0xD2]),
        },
        Frame::Deliver {
            flags: 0x24,
            stream_id: 6,
            delivery_seq: 0xFFFF_FFFF_FFFF_FFFF,
            envelope: sample_envelope(Vec::new()),
        },
    ]
}

pub(super) fn worker_register_frames() -> [Frame; 4] {
    [
        // node = Some
        Frame::WorkerRegister {
            flags: 22,
            registration: WorkerRegistration {
                namespaces: vec!["default".to_owned(), "billing".to_owned()],
                task_queue: "payments".to_owned(),
                node: Some("node-a".to_owned()),
                activity_types: vec!["charge".to_owned(), "refund".to_owned()],
                identity: "worker-1".to_owned(),
            },
        },
        // node = None (and empty vecs) — exercises the optional-node presence byte
        // and zero-count string vectors.
        Frame::WorkerRegister {
            flags: 0,
            registration: WorkerRegistration {
                namespaces: Vec::new(),
                task_queue: "payments".to_owned(),
                node: None,
                activity_types: Vec::new(),
                identity: "worker-2".to_owned(),
            },
        },
        Frame::WorkerRegisterAck {
            flags: 23,
            outcome: WorkerRegisterOutcome::Accepted,
        },
        Frame::WorkerRegisterAck {
            flags: 0,
            outcome: WorkerRegisterOutcome::Rejected {
                reason: "task queue not served".to_owned(),
            },
        },
    ]
}

fn push_frames() -> [Frame; 2] {
    [
        Frame::Push {
            flags: 20,
            stream_id: 4,
            correlation_id: 404,
            payload: vec![0xDE, 0xAD, 0xBE, 0xEF],
        },
        Frame::PushReply {
            flags: 21,
            stream_id: 4,
            correlation_id: 404,
            payload: vec![0xCA, 0xFE],
        },
    ]
}

fn control_frames() -> [Frame; 6] {
    [
        Frame::Connect {
            flags: 0,
            min_version: ProtocolVersion::new(1, 0),
            max_version: ProtocolVersion::new(3, 0),
            auth_token: vec![1, 2, 3, 4],
        },
        Frame::ConnectAck {
            flags: 1,
            selected_version: ProtocolVersion::new(3, 0),
            capabilities: 0x0000_0005,
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
            accepted_schemas: vec![sample_schema(0xA1), sample_schema(0xB2)],
            max_in_flight: 100,
        },
        Frame::SubscribeAck {
            flags: 5,
            stream_id: 1,
            subscription_id: 101,
            selected_schema: sample_schema(0xA1),
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
            envelope: sample_envelope(vec![0x10, 0x20, 0x30]),
            idempotency_key: None,
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
            envelope: sample_envelope(vec![0xAB, 0xCD]),
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

pub(super) fn pressure_frames() -> [Frame; 3] {
    [
        Frame::Accept {
            flags: 15,
            stream_id: 4,
            referenced_message_id: MessageId::from("accepted-message"),
        },
        Frame::Defer {
            flags: 16,
            stream_id: 4,
            referenced_message_id: MessageId::from("deferred-message"),
            reason: Some("buffered".to_owned()),
        },
        Frame::Reject {
            flags: 17,
            stream_id: 4,
            referenced_message_id: MessageId::from("rejected-message"),
            reason: Some("over capacity".to_owned()),
        },
    ]
}

pub(super) fn sample_envelope(payload: Vec<u8>) -> MessageEnvelope {
    MessageEnvelope::new(
        sample_schema(0x11),
        CausalContext {
            parent_id: Some(MessageId::from("parent-1")),
            vector_clock_entry: Some(99),
        },
        payload,
    )
}

pub(super) fn sample_schema(seed: u8) -> SchemaId {
    SchemaId::new([seed; SchemaId::WIRE_LEN])
}

pub(super) fn publish_envelope_bytes<'a>(
    encoded_frame: &'a [u8],
    expected_channel: &str,
) -> Result<&'a [u8], ProtocolError> {
    let payload = read_slice(encoded_frame, 10, encoded_frame.len() - 10, "frame payload")?;
    let mut offset = 0;
    let channel_len = read_u32_as_usize(payload, &mut offset, "channel length")?;
    let channel_bytes = read_slice(payload, offset, channel_len, "channel bytes")?;
    offset = offset
        .checked_add(channel_len)
        .ok_or_else(|| ProtocolError::codec("test channel offset overflowed usize"))?;
    if channel_bytes != expected_channel.as_bytes() {
        return Err(ProtocolError::codec("test channel bytes did not match"));
    }

    let envelope_len = read_u32_as_usize(payload, &mut offset, "envelope length")?;
    read_slice(payload, offset, envelope_len, "envelope bytes")
}

fn read_u32_as_usize(
    bytes: &[u8],
    offset: &mut usize,
    field: &str,
) -> Result<usize, ProtocolError> {
    let bytes = read_slice(bytes, *offset, 4, field)?;
    *offset = offset
        .checked_add(4)
        .ok_or_else(|| ProtocolError::codec("test u32 offset overflowed usize"))?;
    let [b0, b1, b2, b3] = bytes else {
        return Err(ProtocolError::codec("test u32 field was truncated"));
    };
    usize::try_from(u32::from_be_bytes([*b0, *b1, *b2, *b3]))
        .map_err(|_| ProtocolError::codec(format!("{field} cannot fit usize")))
}

fn read_slice<'a>(
    bytes: &'a [u8],
    offset: usize,
    len: usize,
    field: &str,
) -> Result<&'a [u8], ProtocolError> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| ProtocolError::codec(format!("{field} offset overflowed usize")))?;
    bytes
        .get(offset..end)
        .ok_or_else(|| ProtocolError::codec(format!("{field} exceeded available bytes")))
}
