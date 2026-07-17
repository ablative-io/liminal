mod constants;
mod participant;
mod payload;

use constants::{
    APPLICATION_STREAM_ID, FRAME_TYPE_ACCEPT, FRAME_TYPE_CONVERSATION_MESSAGE, FRAME_TYPE_DEFER,
    FRAME_TYPE_PUBLISH, FRAME_TYPE_REJECT, FRAME_TYPE_RESUME, FRAME_TYPE_SUBSCRIBE,
    WIRE_HEADER_LEN,
};
pub use participant::{ParticipantRemoteTransport, ParticipantTransportFrame};
pub(super) use payload::{deserialize_payload, serialize_payload};

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::fmt;
use core::time::Duration;

use serde::Serialize;

use crate::{
    ConversationId, DeliveryAck, PressureResponse, ResumeRequest, SchemaMetadata, SchemaValidate,
    SdkError, SubscriptionId,
};

use super::ServerAddress;

pub(super) trait RemoteTransport:
    ParticipantRemoteTransport + fmt::Debug + Send + Sync
{
    fn publish(
        &self,
        server_address: &ServerAddress,
        request: &WirePublishRequest,
    ) -> Result<PressureResponse, SdkError>;

    /// Publishes and reports a genuine delivery ack (and optional dedup key).
    ///
    /// Returns a [`DeliveryAck`] whose `is_accepted` reflects whether a subscriber
    /// genuinely received the message. The default in-process transport cannot
    /// observe a real subscriber, so it reports a protocol error rather than
    /// faking acceptance; the TCP transport reads the delivery flag the server
    /// sets on the publish ack.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] when the publish cannot be sent or no genuine delivery
    /// ack can be produced.
    fn publish_with_delivery(
        &self,
        server_address: &ServerAddress,
        request: &WirePublishRequest,
    ) -> Result<DeliveryAck, SdkError>;

    fn subscribe(
        &self,
        server_address: &ServerAddress,
        request: &WireSubscribeRequest,
    ) -> Result<(), SdkError>;

    fn send_conversation(
        &self,
        server_address: &ServerAddress,
        request: &WireConversationRequest,
    ) -> Result<(), SdkError>;

    /// Sends a conversation request and blocks for its correlated reply payload.
    ///
    /// Returns the serialized reply bytes the server delivered for this
    /// conversation. The default in-process transport has no socket, so it
    /// reports a protocol error; the real TCP transport performs the round trip.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] when the request cannot be sent, no correlated reply
    /// arrives, or the server rejects the conversation.
    fn request_reply_conversation(
        &self,
        server_address: &ServerAddress,
        request: &WireConversationRequest,
    ) -> Result<Vec<u8>, SdkError>;

    fn resume(
        &self,
        server_address: &ServerAddress,
        request: &WireResumeRequest,
    ) -> Result<(), SdkError>;
}

/// SDK-internal protocol transport boundary. Wire frames never appear in public APIs.
#[derive(Clone, Debug, Default)]
pub(super) struct ProtocolRemoteTransport;

impl RemoteTransport for ProtocolRemoteTransport {
    fn publish(
        &self,
        server_address: &ServerAddress,
        request: &WirePublishRequest,
    ) -> Result<PressureResponse, SdkError> {
        let endpoint = server_address.as_str();
        let frame = request.to_frame();
        let encoded = encode_frame(&frame)?;
        core::hint::black_box((endpoint, encoded));
        decode_backpressure(FRAME_TYPE_ACCEPT, Duration::ZERO, String::new()).map(map_backpressure)
    }

    fn publish_with_delivery(
        &self,
        server_address: &ServerAddress,
        request: &WirePublishRequest,
    ) -> Result<DeliveryAck, SdkError> {
        let endpoint = server_address.as_str();
        let frame = request.to_frame();
        let encoded = encode_frame(&frame)?;
        core::hint::black_box((endpoint, encoded));
        // The in-process transport never opens a socket, so it cannot observe a
        // genuine subscriber delivery. Report this honestly rather than
        // synthesising an acceptance the way the backpressure-only `publish` does;
        // a true delivery ack is the TCP transport's job.
        Err(SdkError::Protocol {
            description: "delivery ack requires the TCP transport; the in-process transport \
                          cannot observe a genuine subscriber delivery"
                .to_string(),
        })
    }

    fn subscribe(
        &self,
        server_address: &ServerAddress,
        request: &WireSubscribeRequest,
    ) -> Result<(), SdkError> {
        let endpoint = server_address.as_str();
        let frame = request.to_frame();
        let encoded = encode_frame(&frame)?;
        core::hint::black_box((endpoint, encoded));
        Ok(())
    }

    fn send_conversation(
        &self,
        server_address: &ServerAddress,
        request: &WireConversationRequest,
    ) -> Result<(), SdkError> {
        let endpoint = server_address.as_str();
        let frame = request.to_frame();
        let encoded = encode_frame(&frame)?;
        core::hint::black_box((endpoint, encoded));
        Ok(())
    }

    fn request_reply_conversation(
        &self,
        server_address: &ServerAddress,
        request: &WireConversationRequest,
    ) -> Result<Vec<u8>, SdkError> {
        let endpoint = server_address.as_str();
        let frame = request.to_frame();
        let encoded = encode_frame(&frame)?;
        core::hint::black_box((endpoint, encoded));
        // The in-process transport never opens a socket, so it cannot carry a
        // correlated reply back. Report this honestly rather than synthesising a
        // fake reply; the real round trip is the TCP transport's job.
        Err(SdkError::Protocol {
            description: "request/reply requires the TCP transport; the in-process transport \
                          cannot carry a correlated reply"
                .to_string(),
        })
    }

    fn resume(
        &self,
        server_address: &ServerAddress,
        request: &WireResumeRequest,
    ) -> Result<(), SdkError> {
        let endpoint = server_address.as_str();
        let frame = request.to_frame();
        let encoded = encode_frame(&frame)?;
        core::hint::black_box((endpoint, encoded));
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct WirePublishRequest {
    channel: String,
    schema: SchemaMetadata,
    payload: Vec<u8>,
    idempotency_key: Option<String>,
}

impl WirePublishRequest {
    pub(super) fn new<M>(channel: &str, message: &M) -> Result<Self, SdkError>
    where
        M: Serialize + SchemaValidate,
    {
        Ok(Self {
            channel: channel.to_string(),
            schema: M::schema_metadata(),
            payload: serialize_payload(message)?,
            idempotency_key: None,
        })
    }

    /// Builds a publish request carrying an idempotency key for dedup-on-delivery.
    pub(super) fn with_idempotency_key<M>(
        channel: &str,
        message: &M,
        idempotency_key: &str,
    ) -> Result<Self, SdkError>
    where
        M: Serialize + SchemaValidate,
    {
        Ok(Self {
            channel: channel.to_string(),
            schema: M::schema_metadata(),
            payload: serialize_payload(message)?,
            idempotency_key: Some(idempotency_key.to_string()),
        })
    }

    /// Idempotency key carried with this publish, when dedup-on-delivery is requested.
    #[cfg(feature = "std")]
    pub(super) fn idempotency_key(&self) -> Option<&str> {
        self.idempotency_key.as_deref()
    }

    fn to_frame(&self) -> WireFrame {
        WireFrame::Publish {
            channel: self.channel.clone(),
            schema_name: self.schema.name.to_string(),
            schema_version: self.schema.version.to_string(),
            payload: self.payload.clone(),
        }
    }

    /// Application channel this publish targets.
    #[cfg(feature = "std")]
    pub(super) fn channel(&self) -> &str {
        &self.channel
    }

    /// Schema metadata declared for the published message type.
    #[cfg(feature = "std")]
    pub(super) const fn schema(&self) -> &SchemaMetadata {
        &self.schema
    }

    /// Serialized application payload bytes.
    #[cfg(feature = "std")]
    pub(super) fn payload(&self) -> &[u8] {
        &self.payload
    }
}

#[derive(Debug)]
pub(super) struct WireSubscribeRequest {
    channel: String,
    subscription_id: SubscriptionId,
    stream_id: u32,
}

impl WireSubscribeRequest {
    pub(super) fn new(
        channel: &str,
        subscription_id: SubscriptionId,
        connection_slot: usize,
    ) -> Result<Self, SdkError> {
        let stream_slot = connection_slot
            .checked_add(1)
            .ok_or_else(|| SdkError::Protocol {
                description: "pooled connection id cannot advance to application stream id"
                    .to_string(),
            })?;
        let stream_id = u32::try_from(stream_slot).map_err(|source| SdkError::Protocol {
            description: format!("pooled connection id cannot fit protocol stream id: {source}"),
        })?;
        Ok(Self {
            channel: channel.to_string(),
            subscription_id,
            stream_id,
        })
    }

    fn to_frame(&self) -> WireFrame {
        WireFrame::Subscribe {
            channel: self.channel.clone(),
            subscription_id: self.subscription_id.get(),
            stream_id: self.stream_id,
        }
    }

    /// Application channel this subscription targets.
    #[cfg(feature = "std")]
    pub(super) fn channel(&self) -> &str {
        &self.channel
    }

    /// Protocol stream id allocated for this subscription.
    #[cfg(feature = "std")]
    pub(super) const fn stream_id(&self) -> u32 {
        self.stream_id
    }
}

#[derive(Debug)]
pub(super) struct WireConversationRequest {
    conversation_id: ConversationId,
    message_type: &'static str,
    payload: Vec<u8>,
}

impl WireConversationRequest {
    pub(super) fn new<M>(conversation_id: &ConversationId, message: &M) -> Result<Self, SdkError>
    where
        M: Serialize,
    {
        Ok(Self {
            conversation_id: conversation_id.clone(),
            message_type: core::any::type_name::<M>(),
            payload: serialize_payload(message)?,
        })
    }

    fn to_frame(&self) -> WireFrame {
        WireFrame::ConversationMessage {
            conversation_id: self.conversation_id.as_str().to_string(),
            message_type: self.message_type,
            payload: self.payload.clone(),
        }
    }

    /// Application conversation identifier this message belongs to.
    #[cfg(feature = "std")]
    pub(super) const fn conversation_id(&self) -> &ConversationId {
        &self.conversation_id
    }

    /// Serialized application payload bytes.
    #[cfg(feature = "std")]
    pub(super) fn payload(&self) -> &[u8] {
        &self.payload
    }
}

#[derive(Debug)]
pub(super) struct WireResumeRequest {
    subscription_id: SubscriptionId,
    from_sequence: u64,
}

impl WireResumeRequest {
    #[must_use]
    pub(super) const fn new(request: ResumeRequest) -> Self {
        Self {
            subscription_id: request.subscription_id,
            from_sequence: request.from_sequence,
        }
    }

    const fn to_frame(&self) -> WireFrame {
        WireFrame::Resume {
            subscription_id: self.subscription_id.get(),
            from_sequence: self.from_sequence,
        }
    }

    /// Subscription this resume request reopens.
    #[cfg(feature = "std")]
    pub(super) const fn subscription_id(&self) -> SubscriptionId {
        self.subscription_id
    }

    /// Sequence number the consumer wants delivery to resume from.
    #[cfg(feature = "std")]
    pub(super) const fn resume_from_sequence(&self) -> u64 {
        self.from_sequence
    }
}

#[derive(Debug)]
enum WireFrame {
    Publish {
        channel: String,
        schema_name: String,
        schema_version: String,
        payload: Vec<u8>,
    },
    Subscribe {
        channel: String,
        subscription_id: u64,
        stream_id: u32,
    },
    ConversationMessage {
        conversation_id: String,
        message_type: &'static str,
        payload: Vec<u8>,
    },
    Resume {
        subscription_id: u64,
        from_sequence: u64,
    },
}

impl WireFrame {
    const fn frame_type(&self) -> u8 {
        match self {
            Self::Publish { .. } => FRAME_TYPE_PUBLISH,
            Self::Subscribe { .. } => FRAME_TYPE_SUBSCRIBE,
            Self::ConversationMessage { .. } => FRAME_TYPE_CONVERSATION_MESSAGE,
            Self::Resume { .. } => FRAME_TYPE_RESUME,
        }
    }

    const fn stream_id(&self) -> u32 {
        match self {
            Self::Subscribe { stream_id, .. } => *stream_id,
            Self::Publish { .. } | Self::ConversationMessage { .. } | Self::Resume { .. } => {
                APPLICATION_STREAM_ID
            }
        }
    }
}

#[derive(Debug)]
enum WireBackpressure {
    Accept { credit: u32 },
    Defer { retry_after: Duration },
    Reject { reason: String },
}

fn encode_frame(frame: &WireFrame) -> Result<Vec<u8>, SdkError> {
    let mut payload = Vec::new();
    encode_payload(frame, &mut payload)?;
    let payload_len = u32::try_from(payload.len()).map_err(|source| SdkError::Protocol {
        description: format!("wire payload exceeds protocol length: {source}"),
    })?;

    let mut bytes = Vec::with_capacity(WIRE_HEADER_LEN.saturating_add(payload.len()));
    bytes.push(frame.frame_type());
    bytes.push(0);
    bytes.extend_from_slice(&frame.stream_id().to_be_bytes());
    bytes.extend_from_slice(&payload_len.to_be_bytes());
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

fn encode_payload(frame: &WireFrame, bytes: &mut Vec<u8>) -> Result<(), SdkError> {
    match frame {
        WireFrame::Publish {
            channel,
            schema_name,
            schema_version,
            payload,
        } => {
            push_field(bytes, channel.as_bytes())?;
            push_field(bytes, schema_name.as_bytes())?;
            push_field(bytes, schema_version.as_bytes())?;
            push_field(bytes, payload)?;
        }
        WireFrame::Subscribe {
            channel,
            subscription_id,
            stream_id,
        } => {
            push_field(bytes, channel.as_bytes())?;
            bytes.extend_from_slice(&subscription_id.to_be_bytes());
            bytes.extend_from_slice(&stream_id.to_be_bytes());
        }
        WireFrame::ConversationMessage {
            conversation_id,
            message_type,
            payload,
        } => {
            push_field(bytes, conversation_id.as_bytes())?;
            push_field(bytes, message_type.as_bytes())?;
            push_field(bytes, payload)?;
        }
        WireFrame::Resume {
            subscription_id,
            from_sequence,
        } => {
            bytes.extend_from_slice(&subscription_id.to_be_bytes());
            bytes.extend_from_slice(&from_sequence.to_be_bytes());
        }
    }
    Ok(())
}

fn push_field(bytes: &mut Vec<u8>, field: &[u8]) -> Result<(), SdkError> {
    let len = u32::try_from(field.len()).map_err(|source| SdkError::Protocol {
        description: format!("wire field exceeds protocol length: {source}"),
    })?;
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(field);
    Ok(())
}

fn map_backpressure(backpressure: WireBackpressure) -> PressureResponse {
    match backpressure {
        WireBackpressure::Accept { credit } => {
            core::hint::black_box(credit);
            PressureResponse::Accept
        }
        WireBackpressure::Defer { retry_after } => PressureResponse::Defer { delay: retry_after },
        WireBackpressure::Reject { reason } => PressureResponse::Reject { reason },
    }
}

fn decode_backpressure(
    kind: u8,
    retry_after: Duration,
    reason: String,
) -> Result<WireBackpressure, SdkError> {
    match kind {
        FRAME_TYPE_ACCEPT => Ok(WireBackpressure::Accept { credit: 1 }),
        FRAME_TYPE_DEFER => Ok(WireBackpressure::Defer { retry_after }),
        FRAME_TYPE_REJECT => Ok(WireBackpressure::Reject { reason }),
        _ => Err(SdkError::Protocol {
            description: format!("unknown backpressure frame kind {kind}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SchemaMetadata, SchemaValidate};

    #[derive(serde::Serialize)]
    struct TestMessage {
        id: u64,
    }

    impl SchemaValidate for TestMessage {
        fn schema_metadata() -> SchemaMetadata {
            SchemaMetadata::new("remote.message", "1", br#"{"type":"object"}"#.as_slice())
        }
    }

    #[test]
    fn publish_frame_uses_protocol_header() -> Result<(), SdkError> {
        let request = WirePublishRequest::new("events", &TestMessage { id: 7 })?;
        let frame = request.to_frame();
        let encoded = encode_frame(&frame)?;

        assert_eq!(encoded[0], FRAME_TYPE_PUBLISH);
        assert_eq!(encoded[1], 0);
        assert_eq!(
            u32::from_be_bytes([encoded[2], encoded[3], encoded[4], encoded[5]]),
            1
        );
        assert_eq!(encoded.len(), WIRE_HEADER_LEN + payload_len(&encoded)?);
        Ok(())
    }

    #[test]
    fn subscription_connection_slot_zero_maps_to_application_stream_one() -> Result<(), SdkError> {
        let request = WireSubscribeRequest::new("events", SubscriptionId::new(42), 0)?;
        let frame = request.to_frame();
        let encoded = encode_frame(&frame)?;

        assert_eq!(encoded[0], FRAME_TYPE_SUBSCRIBE);
        assert_eq!(
            u32::from_be_bytes([encoded[2], encoded[3], encoded[4], encoded[5]]),
            1
        );
        Ok(())
    }

    #[test]
    fn subscribe_and_resume_use_distinct_frame_types() {
        assert_ne!(FRAME_TYPE_SUBSCRIBE, FRAME_TYPE_RESUME);
        let subscribe = WireFrame::Subscribe {
            channel: String::new(),
            subscription_id: 1,
            stream_id: 1,
        };
        let resume = WireFrame::Resume {
            subscription_id: 1,
            from_sequence: 0,
        };
        assert_ne!(subscribe.frame_type(), resume.frame_type());
    }

    fn payload_len(encoded: &[u8]) -> Result<usize, SdkError> {
        let len = u32::from_be_bytes([encoded[6], encoded[7], encoded[8], encoded[9]]);
        usize::try_from(len).map_err(|source| SdkError::Protocol {
            description: format!("test payload length cannot fit usize: {source}"),
        })
    }
}
