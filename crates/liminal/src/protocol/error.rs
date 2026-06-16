use super::{FrameType, lifecycle::ConnectionState};

/// Protocol-level failures with stable numeric reason codes for error frames and metrics.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ProtocolError {
    /// A frame header did not contain all fixed-size header bytes.
    #[error("incomplete frame header")]
    IncompleteHeader { message: Option<String> },

    /// A frame header declared more payload bytes than were present.
    #[error("truncated frame payload")]
    TruncatedPayload { message: Option<String> },

    /// An unknown frame type was observed for logging or metrics.
    #[error("unknown frame type {type_id}")]
    UnknownFrameType {
        type_id: u8,
        message: Option<String>,
    },

    /// A frame appeared on a stream that is invalid for its type.
    #[error("invalid stream {stream_id} for {frame_type:?}")]
    InvalidStream {
        frame_type: FrameType,
        stream_id: u32,
        message: Option<String>,
    },

    /// A frame would move the protocol connection through an invalid state transition.
    #[error("invalid protocol state transition from {current_state:?} with {frame_type:?}")]
    InvalidStateTransition {
        current_state: ConnectionState,
        frame_type: FrameType,
        message: Option<String>,
    },

    /// Authentication failed during connection setup.
    #[error("authentication failure")]
    AuthenticationFailure { message: Option<String> },

    /// The peers could not agree on a compatible protocol version.
    #[error("protocol version mismatch")]
    VersionMismatch { message: Option<String> },

    /// A requested schema is incompatible with this connection or stream.
    #[error("schema incompatible")]
    SchemaIncompatible { message: Option<String> },

    /// Bytes could not be encoded or decoded according to the frame format.
    #[error("protocol codec error")]
    CodecError { message: Option<String> },
}

impl ProtocolError {
    /// Malformed header reason code.
    pub const INCOMPLETE_HEADER_CODE: u16 = 0x0001;
    /// Truncated payload reason code.
    pub const TRUNCATED_PAYLOAD_CODE: u16 = 0x0002;
    /// Unknown frame type reason code.
    pub const UNKNOWN_FRAME_TYPE_CODE: u16 = 0x0003;
    /// Invalid stream reason code.
    pub const INVALID_STREAM_CODE: u16 = 0x0004;
    /// Invalid state transition reason code.
    pub const INVALID_STATE_TRANSITION_CODE: u16 = 0x0005;
    /// Authentication failure reason code.
    pub const AUTHENTICATION_FAILURE_CODE: u16 = 0x0006;
    /// Version mismatch reason code.
    pub const VERSION_MISMATCH_CODE: u16 = 0x0007;
    /// Schema incompatibility reason code.
    pub const SCHEMA_INCOMPATIBLE_CODE: u16 = 0x0008;
    /// Codec error reason code.
    pub const CODEC_ERROR_CODE: u16 = 0x0009;

    /// Return the stable numeric reason code for this error.
    #[must_use]
    pub const fn reason_code(&self) -> u16 {
        match self {
            Self::IncompleteHeader { .. } => Self::INCOMPLETE_HEADER_CODE,
            Self::TruncatedPayload { .. } => Self::TRUNCATED_PAYLOAD_CODE,
            Self::UnknownFrameType { .. } => Self::UNKNOWN_FRAME_TYPE_CODE,
            Self::InvalidStream { .. } => Self::INVALID_STREAM_CODE,
            Self::InvalidStateTransition { .. } => Self::INVALID_STATE_TRANSITION_CODE,
            Self::AuthenticationFailure { .. } => Self::AUTHENTICATION_FAILURE_CODE,
            Self::VersionMismatch { .. } => Self::VERSION_MISMATCH_CODE,
            Self::SchemaIncompatible { .. } => Self::SCHEMA_INCOMPATIBLE_CODE,
            Self::CodecError { .. } => Self::CODEC_ERROR_CODE,
        }
    }

    /// Return the optional human-readable message carried by this error.
    #[must_use]
    pub fn message(&self) -> Option<&str> {
        match self {
            Self::IncompleteHeader { message }
            | Self::TruncatedPayload { message }
            | Self::UnknownFrameType { message, .. }
            | Self::InvalidStream { message, .. }
            | Self::InvalidStateTransition { message, .. }
            | Self::AuthenticationFailure { message }
            | Self::VersionMismatch { message }
            | Self::SchemaIncompatible { message }
            | Self::CodecError { message } => message.as_deref(),
        }
    }

    #[must_use]
    pub(crate) fn invalid_stream(frame_type: FrameType, stream_id: u32) -> Self {
        Self::InvalidStream {
            frame_type,
            stream_id,
            message: Some("stream id violates frame type invariants".to_owned()),
        }
    }

    #[must_use]
    pub(crate) fn invalid_state_transition(
        current_state: ConnectionState,
        frame_type: FrameType,
    ) -> Self {
        Self::InvalidStateTransition {
            current_state,
            frame_type,
            message: Some("frame is invalid for the current connection state".to_owned()),
        }
    }

    #[must_use]
    pub(crate) fn codec(message: impl Into<String>) -> Self {
        Self::CodecError {
            message: Some(message.into()),
        }
    }
}
