//! R1.2 message-contract pins for the WebSocket process: one binary message is
//! exactly one canonical liminal frame, byte-fixtured across every existing
//! frame category, with the boundary/violation cases pinned, and the shared
//! `apply_frame` seam exercised scheduler-free.

use std::sync::Arc;

use liminal::protocol::{
    CausalContext, Frame, MessageEnvelope, MessageId, ProtocolVersion, SchemaId,
    WorkerRegisterOutcome, WorkerRegistration, encode, encoded_len,
};

use super::super::super::services::LiminalConnectionServices;
use super::super::super::state::ProcessStatus;
use super::super::super::supervisor::ConnectionRuntime;
use super::super::{WsInboundViolation, decode_ws_binary};
use super::WebSocketConnectionProcess;
use crate::server::participant::ConnectionFateClass;

const TEST_PID: u64 = 1;

fn encode_frame(frame: &Frame) -> Result<Vec<u8>, String> {
    let len = encoded_len(frame).map_err(|error| format!("encoded_len: {error}"))?;
    let mut bytes = vec![0_u8; len];
    let written = encode(frame, &mut bytes).map_err(|error| format!("encode: {error}"))?;
    bytes.truncate(written);
    Ok(bytes)
}

fn sample_envelope() -> MessageEnvelope {
    MessageEnvelope::new(
        SchemaId::new([7_u8; SchemaId::WIRE_LEN]),
        CausalContext::independent(),
        b"{\"k\":1}".to_vec(),
    )
}

/// One representative frame per existing category (both worker-ack outcomes
/// and the forward-compatible Unknown shape included).
fn every_frame_category() -> Vec<Frame> {
    let mut frames = connection_and_channel_frames();
    frames.extend(conversation_and_worker_frames());
    frames
}

fn connection_and_channel_frames() -> Vec<Frame> {
    vec![
        Frame::Connect {
            flags: 0,
            min_version: ProtocolVersion::new(1, 0),
            max_version: ProtocolVersion::new(1, 0),
            auth_token: b"token".to_vec(),
        },
        Frame::ConnectAck {
            flags: 0,
            selected_version: ProtocolVersion::new(1, 0),
            capabilities: 3,
        },
        Frame::ConnectError {
            flags: 0,
            reason_code: 7,
            message: Some("refused".to_owned()),
        },
        Frame::Disconnect { flags: 0 },
        Frame::Subscribe {
            flags: 0,
            stream_id: 1,
            channel: "events".to_owned(),
            accepted_schemas: vec![SchemaId::new([1_u8; SchemaId::WIRE_LEN])],
            max_in_flight: 16,
        },
        Frame::SubscribeAck {
            flags: 0,
            stream_id: 1,
            subscription_id: 42,
            selected_schema: SchemaId::new([1_u8; SchemaId::WIRE_LEN]),
        },
        Frame::SubscribeError {
            flags: 0,
            stream_id: 1,
            reason_code: 2,
            message: None,
        },
        Frame::Unsubscribe {
            flags: 0,
            stream_id: 1,
            subscription_id: 42,
        },
        Frame::Publish {
            flags: 0,
            stream_id: 1,
            channel: "events".to_owned(),
            envelope: sample_envelope(),
            idempotency_key: None,
        },
        Frame::PublishAck {
            flags: 0,
            stream_id: 1,
            message_id: 9,
        },
        Frame::PublishError {
            flags: 0,
            stream_id: 1,
            reason_code: 3,
            message: Some("bad".to_owned()),
        },
    ]
}

fn conversation_and_worker_frames() -> Vec<Frame> {
    vec![
        Frame::ConversationOpen {
            flags: 0,
            stream_id: 2,
            conversation_id: 11,
            subject: "subject".to_owned(),
        },
        Frame::ConversationMessage {
            flags: 0,
            stream_id: 2,
            conversation_id: 11,
            envelope: sample_envelope(),
        },
        Frame::ConversationClose {
            flags: 0,
            stream_id: 2,
            conversation_id: 11,
            reason_code: Some(1),
            message: Some("done".to_owned()),
        },
        Frame::ConversationError {
            flags: 0,
            stream_id: 2,
            conversation_id: 11,
            reason_code: 4,
            message: None,
        },
        Frame::Accept {
            flags: 0,
            stream_id: 1,
            referenced_message_id: MessageId::new("message-1"),
        },
        Frame::Defer {
            flags: 0,
            stream_id: 1,
            referenced_message_id: MessageId::new("message-2"),
            reason: Some("later".to_owned()),
        },
        Frame::Reject {
            flags: 0,
            stream_id: 1,
            referenced_message_id: MessageId::new("message-3"),
            reason: None,
        },
        Frame::Ping { flags: 0 },
        Frame::Pong { flags: 0 },
        Frame::Push {
            flags: 0,
            stream_id: 1,
            correlation_id: 77,
            payload: b"push".to_vec(),
        },
        Frame::PushReply {
            flags: 0,
            stream_id: 1,
            correlation_id: 77,
            payload: b"reply".to_vec(),
        },
        Frame::WorkerRegister {
            flags: 0,
            registration: WorkerRegistration {
                namespaces: vec!["ns".to_owned()],
                task_queue: "queue".to_owned(),
                node: Some("node-a".to_owned()),
                activity_types: vec!["activity".to_owned()],
                identity: "worker-1".to_owned(),
            },
        },
        Frame::WorkerRegisterAck {
            flags: 0,
            outcome: WorkerRegisterOutcome::Accepted,
        },
        Frame::WorkerRegisterAck {
            flags: 0,
            outcome: WorkerRegisterOutcome::Rejected {
                reason: "no".to_owned(),
            },
        },
        Frame::Deliver {
            flags: 0,
            stream_id: 1,
            delivery_seq: 1,
            envelope: sample_envelope(),
        },
        Frame::Unknown {
            type_id: 0xEE,
            flags: 1,
            stream_id: 5,
            payload: b"future".to_vec(),
        },
    ]
}

#[test]
fn ws_decode_round_trips_every_frame_category_byte_exact() -> Result<(), String> {
    for frame in every_frame_category() {
        let bytes = encode_frame(&frame)?;
        let decoded = decode_ws_binary(&bytes)
            .map_err(|violation| format!("{:?} refused: {violation}", frame.frame_type()))?;
        assert_eq!(
            decoded, frame,
            "canonical bytes must decode to the identical frame"
        );
        let re_encoded = encode_frame(&decoded)?;
        assert_eq!(re_encoded, bytes, "re-encode must be byte-identical");
    }
    Ok(())
}

#[test]
fn ws_decode_refuses_empty_message() {
    assert!(matches!(
        decode_ws_binary(&[]),
        Err(WsInboundViolation::MalformedFrame(_))
    ));
}

#[test]
fn ws_decode_refuses_nine_byte_header() {
    assert!(matches!(
        decode_ws_binary(&[0_u8; 9]),
        Err(WsInboundViolation::MalformedFrame(_))
    ));
}

#[test]
fn ws_decode_accepts_exact_ten_byte_control_frame() -> Result<(), String> {
    let bytes = encode_frame(&Frame::Pong { flags: 0 })?;
    assert_eq!(bytes.len(), 10, "a bodyless frame is exactly the header");
    let decoded = decode_ws_binary(&bytes).map_err(|violation| violation.to_string())?;
    assert_eq!(decoded, Frame::Pong { flags: 0 });
    Ok(())
}

#[test]
fn ws_decode_refuses_declared_body_short_by_one() -> Result<(), String> {
    let mut bytes = encode_frame(&Frame::Ping { flags: 0 })?;
    let declared = u32::from_be_bytes([bytes[6], bytes[7], bytes[8], bytes[9]]);
    bytes[6..10].copy_from_slice(&(declared + 1).to_be_bytes());
    assert!(matches!(
        decode_ws_binary(&bytes),
        Err(WsInboundViolation::MalformedFrame(_))
    ));
    Ok(())
}

#[test]
fn ws_decode_refuses_trailing_byte() -> Result<(), String> {
    let mut bytes = encode_frame(&Frame::Ping { flags: 0 })?;
    bytes.push(0xAB);
    assert!(matches!(
        decode_ws_binary(&bytes),
        Err(WsInboundViolation::TrailingBytes {
            consumed: 10,
            length: 11
        })
    ));
    Ok(())
}

#[test]
fn ws_decode_refuses_two_concatenated_frames() -> Result<(), String> {
    let mut bytes = encode_frame(&Frame::Ping { flags: 0 })?;
    bytes.extend(encode_frame(&Frame::Ping { flags: 0 })?);
    assert!(matches!(
        decode_ws_binary(&bytes),
        Err(WsInboundViolation::TrailingBytes {
            consumed: 10,
            length: 20
        })
    ));
    Ok(())
}

// ---- apply_binary: the same semantic seam, scheduler-free ----

/// A scheduler-free process over a real loopback socket pair. The returned
/// client end must stay alive for the process's lifetime.
fn scheduler_free_process() -> Result<(WebSocketConnectionProcess, std::net::TcpStream), String> {
    let services =
        Arc::new(LiminalConnectionServices::empty().map_err(|error| format!("services: {error}"))?);
    let runtime = Arc::new(ConnectionRuntime::for_tests(services));
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").map_err(|error| format!("bind: {error}"))?;
    let address = listener
        .local_addr()
        .map_err(|error| format!("addr: {error}"))?;
    let client =
        std::net::TcpStream::connect(address).map_err(|error| format!("connect: {error}"))?;
    let (server_side, _) = listener
        .accept()
        .map_err(|error| format!("accept: {error}"))?;
    let message_bound =
        super::super::liminal_ws_message_bound().map_err(|error| error.to_string())?;
    let socket = tungstenite::protocol::WebSocket::from_raw_socket(
        server_side,
        tungstenite::protocol::Role::Server,
        Some(super::super::pinned_protocol_config(message_bound)),
    );
    let holder = Arc::new(std::sync::Mutex::new(Some(socket)));
    let settings = Arc::new(super::super::AcceptorSettings {
        path: "/liminal".to_owned(),
        allowed_origins: Vec::new(),
        ping_interval: None,
        message_bound,
    });
    Ok((
        WebSocketConnectionProcess::from_holder(runtime, None, &holder, None, &settings),
        client,
    ))
}

#[test]
fn ws_apply_binary_runs_the_shared_apply_frame_seam() -> Result<(), String> {
    let (mut process, _client) = scheduler_free_process()?;
    let connect = encode_frame(&Frame::Connect {
        flags: 0,
        min_version: ProtocolVersion::new(1, 0),
        max_version: ProtocolVersion::new(1, 0),
        auth_token: Vec::new(),
    })?;
    let status = process
        .apply_binary(TEST_PID, &connect)
        .map_err(|error| format!("apply connect: {error}"))?;
    assert_eq!(status, ProcessStatus::Continue);
    let messages = process.outbound.take_messages();
    assert_eq!(
        messages.len(),
        1,
        "one response frame => one queued message"
    );
    let ack = decode_ws_binary(&messages[0]).map_err(|violation| format!("ack: {violation}"))?;
    assert!(matches!(ack, Frame::ConnectAck { .. }));
    Ok(())
}

#[test]
fn ws_apply_binary_disconnect_closes() -> Result<(), String> {
    let (mut process, _client) = scheduler_free_process()?;
    let disconnect = encode_frame(&Frame::Disconnect { flags: 0 })?;
    let status = process
        .apply_binary(TEST_PID, &disconnect)
        .map_err(|error| format!("apply disconnect: {error}"))?;
    assert_eq!(
        status,
        ProcessStatus::CloseWithFate(ConnectionFateClass::CleanDisconnect)
    );
    Ok(())
}

#[test]
fn ws_apply_binary_refuses_trailing_bytes_as_typed_error() -> Result<(), String> {
    let (mut process, _client) = scheduler_free_process()?;
    let mut bytes = encode_frame(&Frame::Ping { flags: 0 })?;
    bytes.push(0x00);
    let result = process.apply_binary(TEST_PID, &bytes);
    assert!(result.is_err(), "trailing bytes must be a typed failure");
    Ok(())
}

#[test]
fn ws_apply_binary_refuses_truncated_body_as_typed_error() -> Result<(), String> {
    let (mut process, _client) = scheduler_free_process()?;
    let mut bytes = encode_frame(&Frame::Ping { flags: 0 })?;
    let declared = u32::from_be_bytes([bytes[6], bytes[7], bytes[8], bytes[9]]);
    bytes[6..10].copy_from_slice(&(declared + 5).to_be_bytes());
    let result = process.apply_binary(TEST_PID, &bytes);
    assert!(
        result.is_err(),
        "a truncated declared body must be a typed failure"
    );
    Ok(())
}
