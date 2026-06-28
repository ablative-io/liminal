use super::{Frame, FrameType, validate_stream};
use crate::protocol::{CausalContext, MessageEnvelope, ProtocolError, SchemaId};

#[test]
fn frame_type_discriminants_round_trip() {
    let values = [
        (0x01, FrameType::Connect),
        (0x02, FrameType::ConnectAck),
        (0x03, FrameType::ConnectError),
        (0x04, FrameType::Disconnect),
        (0x05, FrameType::Subscribe),
        (0x06, FrameType::SubscribeAck),
        (0x07, FrameType::SubscribeError),
        (0x08, FrameType::Unsubscribe),
        (0x09, FrameType::Publish),
        (0x0A, FrameType::PublishAck),
        (0x0B, FrameType::PublishError),
        (0x0C, FrameType::ConversationOpen),
        (0x0D, FrameType::ConversationMessage),
        (0x0E, FrameType::ConversationClose),
        (0x0F, FrameType::ConversationError),
        (0x10, FrameType::Accept),
        (0x11, FrameType::Defer),
        (0x12, FrameType::Reject),
        (0x13, FrameType::Ping),
        (0x14, FrameType::Pong),
        (0x15, FrameType::Push),
        (0x16, FrameType::PushReply),
        (0x17, FrameType::WorkerRegister),
        (0x18, FrameType::WorkerRegisterAck),
    ];

    for (wire, frame_type) in values {
        assert_eq!(FrameType::from(wire), frame_type);
        assert_eq!(u8::from(frame_type), wire);
    }
    assert_eq!(FrameType::from(0x80), FrameType::Unknown(0x80));
    assert_eq!(u8::from(FrameType::Unknown(0x80)), 0x80);
}

#[test]
fn constructors_validate_streams() {
    assert!(Frame::new_ping(0).is_ok());
    assert!(matches!(
        Frame::new_ping(1),
        Err(ProtocolError::InvalidStream { .. })
    ));
    let envelope = sample_envelope();
    assert!(Frame::new_publish(1, "orders", envelope.clone()).is_ok());
    assert!(matches!(
        Frame::new_publish(0, "orders", envelope),
        Err(ProtocolError::InvalidStream { .. })
    ));
    assert!(validate_stream(FrameType::Accept, 2).is_ok());
}

fn sample_envelope() -> MessageEnvelope {
    MessageEnvelope::new(
        SchemaId::new([0x55; 32]),
        CausalContext::independent(),
        vec![1, 2, 3],
    )
}
