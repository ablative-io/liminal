use super::{
    causal::MessageId,
    envelope::{MessageEnvelope, SchemaId},
    error::ProtocolError,
    version::ProtocolVersion,
};

/// Number of bytes in every serialized frame header.
pub const HEADER_LEN: usize = 10;

/// Frame-flag bit set on a [`Frame::ConversationMessage`] to request a correlated
/// reply.
///
/// A client sets this bit on the request frame of a request-reply round trip. The
/// server, after delivering the message to the conversation participant, drains
/// the participant's reply and sends it back as a `ConversationMessage` carrying
/// the same `conversation_id` (the correlation key) and this same flag bit. A
/// `ConversationMessage` WITHOUT this bit keeps the pre-existing fire-and-forget
/// semantics: the server stays silent on success. The bit travels in the frame
/// header's `flags` byte, which the codec already round-trips, so no wire-format
/// change is required.
pub const CONVERSATION_REPLY_REQUESTED_FLAG: u8 = 0x01;

/// Frame-flag bit set on a [`Frame::Publish`] to declare that the frame body
/// carries a trailing idempotency-key string field (the dedup-on-delivery key).
///
/// A publisher sets this bit when it wants the server to consult its dedup cache
/// keyed by the trailing idempotency key, delivering the message to subscribers
/// AT MOST ONCE across re-publishes of the same key. A `Publish` frame WITHOUT
/// this bit keeps the pre-existing wire layout EXACTLY: no trailing field is
/// written and none is read, so a no-key publish is byte-identical to before.
/// The bit travels in the frame header's `flags` byte, which the codec already
/// round-trips, so no header-format change is required (the 13-L0 precedent).
pub const PUBLISH_IDEMPOTENCY_KEY_FLAG: u8 = 0x02;

/// Frame-flag bit set on a [`Frame::PublishAck`] to report a GENUINE delivery
/// ack: the published message was accepted by at least one live subscriber on
/// this publish.
///
/// This is distinct from the backpressure `Accept`/`Defer`/`Reject` signal: a
/// `PublishAck` always means the publish was processed without error, but only a
/// `PublishAck` carrying this bit means a subscriber actually received the
/// message. An ack WITHOUT this bit means the publish succeeded but reached no
/// subscriber (an empty channel, or a duplicate suppressed by dedup-on-delivery),
/// so a caller that needs a true delivery ack can observe the difference. The bit
/// rides the existing `flags` byte, so no wire-format change is required.
pub const PUBLISH_DELIVERED_FLAG: u8 = 0x01;

/// Status byte prefixing a [`Frame::WorkerRegisterAck`] payload that signals the
/// registration was accepted (no further payload follows).
pub(crate) const WORKER_REGISTER_ACK_ACCEPTED: u8 = 0x00;

/// Status byte prefixing a [`Frame::WorkerRegisterAck`] payload that signals the
/// registration was rejected (a length-prefixed reason string follows).
pub(crate) const WORKER_REGISTER_ACK_REJECTED: u8 = 0x01;

/// Protocol frame categories and their stable wire discriminants.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameType {
    /// Connection request.
    Connect,
    /// Successful connection response.
    ConnectAck,
    /// Failed connection response.
    ConnectError,
    /// Connection close notification.
    Disconnect,
    /// Channel subscription request.
    Subscribe,
    /// Successful subscription response.
    SubscribeAck,
    /// Failed subscription response.
    SubscribeError,
    /// Channel unsubscription request.
    Unsubscribe,
    /// Channel publish request.
    Publish,
    /// Successful publish response.
    PublishAck,
    /// Failed publish response.
    PublishError,
    /// Conversation lifecycle open.
    ConversationOpen,
    /// Conversation message delivery.
    ConversationMessage,
    /// Conversation lifecycle close.
    ConversationClose,
    /// Conversation processing error.
    ConversationError,
    /// In-band backpressure acceptance.
    Accept,
    /// In-band backpressure deferral.
    Defer,
    /// In-band backpressure rejection.
    Reject,
    /// Connection keepalive ping.
    Ping,
    /// Connection keepalive pong.
    Pong,
    /// Server-initiated push of an opaque payload to a connected client.
    Push,
    /// Client-initiated correlated reply to a server push.
    PushReply,
    /// Worker self-registration announcing identity and routing dimensions.
    WorkerRegister,
    /// Server acknowledgement of a worker registration (accepted or rejected).
    WorkerRegisterAck,
    /// Forward-compatible frame type not known to this implementation.
    Unknown(u8),
}

impl FrameType {
    /// Return true when this frame type must appear on stream 0.
    #[must_use]
    pub const fn is_control(self) -> bool {
        matches!(
            self,
            Self::Connect
                | Self::ConnectAck
                | Self::ConnectError
                | Self::Disconnect
                | Self::Ping
                | Self::Pong
                | Self::WorkerRegister
                | Self::WorkerRegisterAck
        )
    }
}

impl From<u8> for FrameType {
    fn from(value: u8) -> Self {
        match value {
            0x01 => Self::Connect,
            0x02 => Self::ConnectAck,
            0x03 => Self::ConnectError,
            0x04 => Self::Disconnect,
            0x05 => Self::Subscribe,
            0x06 => Self::SubscribeAck,
            0x07 => Self::SubscribeError,
            0x08 => Self::Unsubscribe,
            0x09 => Self::Publish,
            0x0A => Self::PublishAck,
            0x0B => Self::PublishError,
            0x0C => Self::ConversationOpen,
            0x0D => Self::ConversationMessage,
            0x0E => Self::ConversationClose,
            0x0F => Self::ConversationError,
            0x10 => Self::Accept,
            0x11 => Self::Defer,
            0x12 => Self::Reject,
            0x13 => Self::Ping,
            0x14 => Self::Pong,
            0x15 => Self::Push,
            0x16 => Self::PushReply,
            0x17 => Self::WorkerRegister,
            0x18 => Self::WorkerRegisterAck,
            unknown => Self::Unknown(unknown),
        }
    }
}

impl From<FrameType> for u8 {
    fn from(value: FrameType) -> Self {
        match value {
            FrameType::Connect => 0x01,
            FrameType::ConnectAck => 0x02,
            FrameType::ConnectError => 0x03,
            FrameType::Disconnect => 0x04,
            FrameType::Subscribe => 0x05,
            FrameType::SubscribeAck => 0x06,
            FrameType::SubscribeError => 0x07,
            FrameType::Unsubscribe => 0x08,
            FrameType::Publish => 0x09,
            FrameType::PublishAck => 0x0A,
            FrameType::PublishError => 0x0B,
            FrameType::ConversationOpen => 0x0C,
            FrameType::ConversationMessage => 0x0D,
            FrameType::ConversationClose => 0x0E,
            FrameType::ConversationError => 0x0F,
            FrameType::Accept => 0x10,
            FrameType::Defer => 0x11,
            FrameType::Reject => 0x12,
            FrameType::Ping => 0x13,
            FrameType::Pong => 0x14,
            FrameType::Push => 0x15,
            FrameType::PushReply => 0x16,
            FrameType::WorkerRegister => 0x17,
            FrameType::WorkerRegisterAck => 0x18,
            FrameType::Unknown(type_id) => type_id,
        }
    }
}

/// Fixed-size frame prefix: type, flags, stream identifier, and payload length.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameHeader {
    /// Frame type read from or written to byte 0.
    pub frame_type: FrameType,
    /// Frame flags read from or written to byte 1.
    pub flags: u8,
    /// Stream identifier read from or written to bytes 2..6.
    pub stream_id: u32,
    /// Payload length read from or written to bytes 6..10.
    pub payload_length: u32,
}

impl FrameHeader {
    /// Serialized header length in bytes.
    pub const WIRE_LEN: usize = HEADER_LEN;
}

/// Self-describing worker registration carried by [`Frame::WorkerRegister`].
///
/// A worker announces the routing dimensions it serves plus a stable identity so
/// the server can associate the worker with its connection and the application can
/// route work to it. `node` is optional locality (the routing model treats node as
/// an optional dimension); the codec encodes it with a presence byte rather than
/// flattening `None` to an empty string.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerRegistration {
    /// Namespaces this worker serves.
    pub namespaces: Vec<String>,
    /// Task queue this worker pulls from.
    pub task_queue: String,
    /// Optional node locality; `None` when the worker declares no node affinity.
    pub node: Option<String>,
    /// Activity types this worker can execute.
    pub activity_types: Vec<String>,
    /// Stable worker identity.
    pub identity: String,
}

/// Outcome of a worker registration, carried by [`Frame::WorkerRegisterAck`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerRegisterOutcome {
    /// The server accepted the registration.
    Accepted,
    /// The server rejected the registration, with a human-readable reason.
    Rejected {
        /// Human-readable rejection reason surfaced to the worker.
        reason: String,
    },
}

/// A typed protocol frame body plus the header metadata required to encode it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    /// Connection request carrying a supported version range and opaque auth token.
    Connect {
        flags: u8,
        min_version: ProtocolVersion,
        max_version: ProtocolVersion,
        auth_token: Vec<u8>,
    },
    /// Connection success carrying the negotiated protocol version and server capabilities.
    ConnectAck {
        flags: u8,
        selected_version: ProtocolVersion,
        capabilities: u32,
    },
    /// Connection failure carrying a numeric reason and optional message.
    ConnectError {
        flags: u8,
        reason_code: u16,
        message: Option<String>,
    },
    /// Connection close notification with no payload.
    Disconnect { flags: u8 },
    /// Channel subscription request carrying a channel and accepted schema hashes.
    Subscribe {
        flags: u8,
        stream_id: u32,
        channel: String,
        accepted_schemas: Vec<SchemaId>,
        max_in_flight: u32,
    },
    /// Channel subscription success carrying a subscription id and selected schema.
    SubscribeAck {
        flags: u8,
        stream_id: u32,
        subscription_id: u64,
        selected_schema: SchemaId,
    },
    /// Channel subscription failure carrying a numeric reason and optional message.
    SubscribeError {
        flags: u8,
        stream_id: u32,
        reason_code: u16,
        message: Option<String>,
    },
    /// Channel unsubscription request carrying the subscription id.
    Unsubscribe {
        flags: u8,
        stream_id: u32,
        subscription_id: u64,
    },
    /// Publish request carrying a channel and typed message envelope.
    ///
    /// `idempotency_key` is `Some` only when the [`PUBLISH_IDEMPOTENCY_KEY_FLAG`]
    /// flag bit is set; it is the dedup-on-delivery key the server feeds to its
    /// dedup cache. When `None` (and the flag clear) the frame is byte-identical
    /// on the wire to a pre-13-L1 publish.
    Publish {
        flags: u8,
        stream_id: u32,
        channel: String,
        envelope: MessageEnvelope,
        idempotency_key: Option<String>,
    },
    /// Publish success carrying the accepted message id.
    PublishAck {
        flags: u8,
        stream_id: u32,
        message_id: u64,
    },
    /// Publish failure carrying a numeric reason and optional message.
    PublishError {
        flags: u8,
        stream_id: u32,
        reason_code: u16,
        message: Option<String>,
    },
    /// Conversation open carrying a conversation id and subject.
    ConversationOpen {
        flags: u8,
        stream_id: u32,
        conversation_id: u64,
        subject: String,
    },
    /// Conversation message carrying a conversation id and typed message envelope.
    ConversationMessage {
        flags: u8,
        stream_id: u32,
        conversation_id: u64,
        envelope: MessageEnvelope,
    },
    /// Conversation close carrying a conversation id and optional reason.
    ConversationClose {
        flags: u8,
        stream_id: u32,
        conversation_id: u64,
        reason_code: Option<u16>,
        message: Option<String>,
    },
    /// Conversation failure carrying a conversation id, numeric reason, and optional message.
    ConversationError {
        flags: u8,
        stream_id: u32,
        conversation_id: u64,
        reason_code: u16,
        message: Option<String>,
    },
    /// Backpressure acceptance for a delivered message.
    Accept {
        flags: u8,
        stream_id: u32,
        referenced_message_id: MessageId,
    },
    /// Backpressure deferral for a buffered message.
    Defer {
        flags: u8,
        stream_id: u32,
        referenced_message_id: MessageId,
        reason: Option<String>,
    },
    /// Backpressure rejection for a shed message.
    Reject {
        flags: u8,
        stream_id: u32,
        referenced_message_id: MessageId,
        reason: Option<String>,
    },
    /// Connection keepalive ping.
    Ping { flags: u8 },
    /// Connection keepalive pong.
    Pong { flags: u8 },
    /// Server-initiated push carrying a correlation id and an opaque payload.
    ///
    /// A server writes this frame to a connected client over the client's existing
    /// connection (server-to-client, the inverse of every other request frame). The
    /// `correlation_id` is the key the server uses to match the client's later
    /// [`Frame::PushReply`] back to this push; the `payload` is opaque application
    /// bytes the server hands the client. This is an application-stream frame, so
    /// `stream_id` is non-zero like a publish or conversation message.
    Push {
        flags: u8,
        stream_id: u32,
        correlation_id: u64,
        payload: Vec<u8>,
    },
    /// Client-initiated correlated reply to a [`Frame::Push`].
    ///
    /// After handling a pushed frame the client writes this back on the same
    /// connection, echoing the push's `correlation_id` so the server can match the
    /// reply to the originating push. The `payload` is the client's opaque answer.
    PushReply {
        flags: u8,
        stream_id: u32,
        correlation_id: u64,
        payload: Vec<u8>,
    },
    /// Worker self-registration over an established connection.
    ///
    /// A worker sends this control frame (stream 0) after the connection
    /// handshake to announce its identity and routing dimensions. The server
    /// associates the registration with the connection's process id and surfaces
    /// it to the application via the connection-notifier hook, then answers with a
    /// [`Frame::WorkerRegisterAck`]. `node` is optional locality and is encoded
    /// with a presence byte, never flattened to an empty string.
    WorkerRegister {
        flags: u8,
        registration: WorkerRegistration,
    },
    /// Server acknowledgement of a [`Frame::WorkerRegister`].
    ///
    /// Carries the registration outcome: [`WorkerRegisterOutcome::Accepted`] when
    /// the server (and any configured notifier) accepted the worker, or
    /// [`WorkerRegisterOutcome::Rejected`] carrying a human-readable reason when it
    /// did not. The acknowledgement is synchronous so a worker never believes it is
    /// registered when the application rejected it.
    WorkerRegisterAck {
        flags: u8,
        outcome: WorkerRegisterOutcome,
    },
    /// Forward-compatible frame preserved after length-delimited skipping.
    Unknown {
        type_id: u8,
        flags: u8,
        stream_id: u32,
        payload: Vec<u8>,
    },
}

impl Frame {
    /// Construct a ping frame, enforcing the stream-0 control-frame invariant.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::InvalidStream`] when `stream_id` is not zero.
    pub fn new_ping(stream_id: u32) -> Result<Self, ProtocolError> {
        validate_stream(FrameType::Ping, stream_id)?;
        Ok(Self::Ping { flags: 0 })
    }

    /// Construct a publish frame, enforcing the non-zero application-stream invariant.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::InvalidStream`] when `stream_id` is zero.
    pub fn new_publish(
        stream_id: u32,
        channel: impl Into<String>,
        envelope: MessageEnvelope,
    ) -> Result<Self, ProtocolError> {
        validate_stream(FrameType::Publish, stream_id)?;
        Ok(Self::Publish {
            flags: 0,
            stream_id,
            channel: channel.into(),
            envelope,
            idempotency_key: None,
        })
    }

    /// Construct a publish frame carrying an idempotency key for dedup-on-delivery.
    ///
    /// The returned frame has [`PUBLISH_IDEMPOTENCY_KEY_FLAG`] set and serializes
    /// the trailing key field, so the server consults its dedup cache for this key.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::InvalidStream`] when `stream_id` is zero.
    pub fn new_publish_with_idempotency_key(
        stream_id: u32,
        channel: impl Into<String>,
        envelope: MessageEnvelope,
        idempotency_key: impl Into<String>,
    ) -> Result<Self, ProtocolError> {
        validate_stream(FrameType::Publish, stream_id)?;
        Ok(Self::Publish {
            flags: PUBLISH_IDEMPOTENCY_KEY_FLAG,
            stream_id,
            channel: channel.into(),
            envelope,
            idempotency_key: Some(idempotency_key.into()),
        })
    }

    /// Construct a server-to-client push frame, enforcing the non-zero
    /// application-stream invariant.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::InvalidStream`] when `stream_id` is zero.
    pub fn new_push(
        stream_id: u32,
        correlation_id: u64,
        payload: Vec<u8>,
    ) -> Result<Self, ProtocolError> {
        validate_stream(FrameType::Push, stream_id)?;
        Ok(Self::Push {
            flags: 0,
            stream_id,
            correlation_id,
            payload,
        })
    }

    /// Construct a client-to-server push reply frame, echoing the correlation id of
    /// the originating push, and enforcing the non-zero application-stream invariant.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::InvalidStream`] when `stream_id` is zero.
    pub fn new_push_reply(
        stream_id: u32,
        correlation_id: u64,
        payload: Vec<u8>,
    ) -> Result<Self, ProtocolError> {
        validate_stream(FrameType::PushReply, stream_id)?;
        Ok(Self::PushReply {
            flags: 0,
            stream_id,
            correlation_id,
            payload,
        })
    }

    /// Return the frame type represented by this frame body.
    #[must_use]
    pub const fn frame_type(&self) -> FrameType {
        match self {
            Self::Connect { .. } => FrameType::Connect,
            Self::ConnectAck { .. } => FrameType::ConnectAck,
            Self::ConnectError { .. } => FrameType::ConnectError,
            Self::Disconnect { .. } => FrameType::Disconnect,
            Self::Subscribe { .. } => FrameType::Subscribe,
            Self::SubscribeAck { .. } => FrameType::SubscribeAck,
            Self::SubscribeError { .. } => FrameType::SubscribeError,
            Self::Unsubscribe { .. } => FrameType::Unsubscribe,
            Self::Publish { .. } => FrameType::Publish,
            Self::PublishAck { .. } => FrameType::PublishAck,
            Self::PublishError { .. } => FrameType::PublishError,
            Self::ConversationOpen { .. } => FrameType::ConversationOpen,
            Self::ConversationMessage { .. } => FrameType::ConversationMessage,
            Self::ConversationClose { .. } => FrameType::ConversationClose,
            Self::ConversationError { .. } => FrameType::ConversationError,
            Self::Accept { .. } => FrameType::Accept,
            Self::Defer { .. } => FrameType::Defer,
            Self::Reject { .. } => FrameType::Reject,
            Self::Ping { .. } => FrameType::Ping,
            Self::Pong { .. } => FrameType::Pong,
            Self::Push { .. } => FrameType::Push,
            Self::PushReply { .. } => FrameType::PushReply,
            Self::WorkerRegister { .. } => FrameType::WorkerRegister,
            Self::WorkerRegisterAck { .. } => FrameType::WorkerRegisterAck,
            Self::Unknown { type_id, .. } => FrameType::Unknown(*type_id),
        }
    }

    /// Return the frame flags stored in the fixed header.
    #[must_use]
    pub const fn flags(&self) -> u8 {
        match self {
            Self::Connect { flags, .. }
            | Self::ConnectAck { flags, .. }
            | Self::ConnectError { flags, .. }
            | Self::Disconnect { flags, .. }
            | Self::Subscribe { flags, .. }
            | Self::SubscribeAck { flags, .. }
            | Self::SubscribeError { flags, .. }
            | Self::Unsubscribe { flags, .. }
            | Self::Publish { flags, .. }
            | Self::PublishAck { flags, .. }
            | Self::PublishError { flags, .. }
            | Self::ConversationOpen { flags, .. }
            | Self::ConversationMessage { flags, .. }
            | Self::ConversationClose { flags, .. }
            | Self::ConversationError { flags, .. }
            | Self::Accept { flags, .. }
            | Self::Defer { flags, .. }
            | Self::Reject { flags, .. }
            | Self::Ping { flags }
            | Self::Pong { flags }
            | Self::Push { flags, .. }
            | Self::PushReply { flags, .. }
            | Self::WorkerRegister { flags, .. }
            | Self::WorkerRegisterAck { flags, .. }
            | Self::Unknown { flags, .. } => *flags,
        }
    }

    /// Return the stream id stored in the fixed header.
    #[must_use]
    pub const fn stream_id(&self) -> u32 {
        match self {
            Self::Connect { .. }
            | Self::ConnectAck { .. }
            | Self::ConnectError { .. }
            | Self::Disconnect { .. }
            | Self::Ping { .. }
            | Self::Pong { .. }
            | Self::WorkerRegister { .. }
            | Self::WorkerRegisterAck { .. } => 0,
            Self::Subscribe { stream_id, .. }
            | Self::SubscribeAck { stream_id, .. }
            | Self::SubscribeError { stream_id, .. }
            | Self::Unsubscribe { stream_id, .. }
            | Self::Publish { stream_id, .. }
            | Self::PublishAck { stream_id, .. }
            | Self::PublishError { stream_id, .. }
            | Self::ConversationOpen { stream_id, .. }
            | Self::ConversationMessage { stream_id, .. }
            | Self::ConversationClose { stream_id, .. }
            | Self::ConversationError { stream_id, .. }
            | Self::Accept { stream_id, .. }
            | Self::Defer { stream_id, .. }
            | Self::Reject { stream_id, .. }
            | Self::Push { stream_id, .. }
            | Self::PushReply { stream_id, .. }
            | Self::Unknown { stream_id, .. } => *stream_id,
        }
    }

    /// Validate the stream invariant for this frame.
    pub(crate) fn validate(&self) -> Result<(), ProtocolError> {
        validate_stream(self.frame_type(), self.stream_id())?;

        if let Self::Subscribe { max_in_flight, .. } = self {
            if *max_in_flight == 0 {
                return Err(ProtocolError::codec(
                    "max_in_flight must be greater than zero",
                ));
            }
        }

        Ok(())
    }
}

/// Validate stream placement for a known frame type.
///
/// # Errors
///
/// Returns [`ProtocolError::InvalidStream`] when `stream_id` is invalid for
/// `frame_type`.
pub fn validate_stream(frame_type: FrameType, stream_id: u32) -> Result<(), ProtocolError> {
    if matches!(frame_type, FrameType::Unknown(_)) {
        return Ok(());
    }

    let valid = if frame_type.is_control() {
        stream_id == 0
    } else {
        stream_id >= 1
    };

    if valid {
        Ok(())
    } else {
        Err(ProtocolError::invalid_stream(frame_type, stream_id))
    }
}

#[cfg(test)]
mod tests;
