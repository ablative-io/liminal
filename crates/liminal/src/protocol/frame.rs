use super::error::ProtocolError;

/// Number of bytes in every serialized frame header.
pub const HEADER_LEN: usize = 10;

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

/// A typed protocol frame body plus the header metadata required to encode it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    /// Connection request carrying a supported version range and opaque auth token.
    Connect {
        flags: u8,
        min_version: u16,
        max_version: u16,
        auth_token: Vec<u8>,
    },
    /// Connection success carrying the negotiated protocol version.
    ConnectAck { flags: u8, version: u16 },
    /// Connection failure carrying a numeric reason and optional message.
    ConnectError {
        flags: u8,
        reason_code: u16,
        message: Option<String>,
    },
    /// Connection close notification with no payload.
    Disconnect { flags: u8 },
    /// Channel subscription request carrying a channel and optional schema reference.
    Subscribe {
        flags: u8,
        stream_id: u32,
        channel: String,
        schema: Option<String>,
    },
    /// Channel subscription success carrying a server-visible subscription id.
    SubscribeAck {
        flags: u8,
        stream_id: u32,
        subscription_id: u64,
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
    /// Publish request carrying a channel and opaque application payload bytes.
    Publish {
        flags: u8,
        stream_id: u32,
        channel: String,
        payload: Vec<u8>,
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
    /// Conversation message carrying a conversation id and opaque payload bytes.
    ConversationMessage {
        flags: u8,
        stream_id: u32,
        conversation_id: u64,
        payload: Vec<u8>,
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
    /// Backpressure acceptance carrying additional message credit.
    Accept {
        flags: u8,
        stream_id: u32,
        credit: u32,
    },
    /// Backpressure deferral carrying a retry delay in milliseconds.
    Defer {
        flags: u8,
        stream_id: u32,
        retry_after_ms: u32,
    },
    /// Backpressure rejection carrying a numeric reason and optional message.
    Reject {
        flags: u8,
        stream_id: u32,
        reason_code: u16,
        message: Option<String>,
    },
    /// Connection keepalive ping.
    Ping { flags: u8 },
    /// Connection keepalive pong.
    Pong { flags: u8 },
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
        payload: impl Into<Vec<u8>>,
    ) -> Result<Self, ProtocolError> {
        validate_stream(FrameType::Publish, stream_id)?;
        Ok(Self::Publish {
            flags: 0,
            stream_id,
            channel: channel.into(),
            payload: payload.into(),
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
            | Self::Pong { .. } => 0,
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
            | Self::Unknown { stream_id, .. } => *stream_id,
        }
    }

    /// Validate the stream invariant for this frame.
    pub(crate) fn validate(&self) -> Result<(), ProtocolError> {
        validate_stream(self.frame_type(), self.stream_id())
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
mod tests {
    use super::{Frame, FrameType, validate_stream};
    use crate::protocol::ProtocolError;

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
        assert!(Frame::new_publish(1, "orders", [1_u8, 2, 3]).is_ok());
        assert!(matches!(
            Frame::new_publish(0, "orders", [1_u8, 2, 3]),
            Err(ProtocolError::InvalidStream { .. })
        ));
        assert!(validate_stream(FrameType::Accept, 2).is_ok());
    }
}
