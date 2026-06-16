use super::{Frame, FrameType, ProtocolError, validate_stream};

/// Connection-level lifecycle states enforced by the protocol.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionState {
    /// Waiting for the client Connect frame.
    Connecting,
    /// Waiting for the server `ConnectAck` or `ConnectError` frame.
    Authenticating,
    /// Handshake complete; application and keepalive frames may flow.
    Active,
    /// Disconnect has been sent; waiting for the peer's Disconnect acknowledgement.
    Closing,
    /// Connection is closed and accepts no further frames.
    Closed,
}

impl ConnectionState {
    /// Validate that a frame type is accepted in this state.
    ///
    /// Unknown frame types are forward-compatible and are allowed to pass through
    /// lifecycle validation after codec length-delimited skipping.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::InvalidStateTransition`] when `frame_type` is not
    /// accepted by this state.
    pub fn validate_frame_type(self, frame_type: FrameType) -> Result<(), ProtocolError> {
        if self.accepts(frame_type) {
            Ok(())
        } else {
            Err(ProtocolError::invalid_state_transition(self, frame_type))
        }
    }

    /// Validate a frame and return the next lifecycle state.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::InvalidStream`] for invalid stream placement and
    /// [`ProtocolError::InvalidStateTransition`] for out-of-sequence frames.
    pub fn transition(self, frame: &Frame) -> Result<Self, ProtocolError> {
        validate_stream(frame.frame_type(), frame.stream_id())?;
        self.transition_frame_type(frame.frame_type())
    }

    /// Validate a frame type and return the next lifecycle state.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::InvalidStateTransition`] when `frame_type` is not
    /// accepted by this state.
    pub fn transition_frame_type(self, frame_type: FrameType) -> Result<Self, ProtocolError> {
        self.validate_frame_type(frame_type)?;
        Ok(self.next_state(frame_type))
    }

    /// Validate and handle a frame, returning the next state and any protocol response.
    ///
    /// Active Ping frames produce a Pong response on stream 0. This function does
    /// not perform transport I/O.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::InvalidStream`] for invalid stream placement and
    /// [`ProtocolError::InvalidStateTransition`] for out-of-sequence frames.
    pub fn handle_frame(self, frame: &Frame) -> Result<LifecycleAction, ProtocolError> {
        let next_state = self.transition(frame)?;
        let response = if self == Self::Active && frame.frame_type() == FrameType::Ping {
            Some(Frame::Pong { flags: 0 })
        } else {
            None
        };

        Ok(LifecycleAction {
            state: next_state,
            response,
        })
    }

    const fn accepts(self, frame_type: FrameType) -> bool {
        if matches!(self, Self::Closed) {
            return false;
        }

        if matches!(frame_type, FrameType::Unknown(_)) {
            return true;
        }

        match self {
            Self::Connecting => matches!(frame_type, FrameType::Connect),
            Self::Authenticating => {
                matches!(frame_type, FrameType::ConnectAck | FrameType::ConnectError)
            }
            Self::Active => Self::accepts_active(frame_type),
            Self::Closing => matches!(frame_type, FrameType::Disconnect),
            Self::Closed => false,
        }
    }

    const fn accepts_active(frame_type: FrameType) -> bool {
        matches!(
            frame_type,
            FrameType::Disconnect
                | FrameType::Subscribe
                | FrameType::SubscribeAck
                | FrameType::SubscribeError
                | FrameType::Unsubscribe
                | FrameType::Publish
                | FrameType::PublishAck
                | FrameType::PublishError
                | FrameType::ConversationOpen
                | FrameType::ConversationMessage
                | FrameType::ConversationClose
                | FrameType::ConversationError
                | FrameType::Accept
                | FrameType::Defer
                | FrameType::Reject
                | FrameType::Ping
                | FrameType::Pong
        )
    }

    const fn next_state(self, frame_type: FrameType) -> Self {
        match (self, frame_type) {
            (Self::Connecting, FrameType::Connect) => Self::Authenticating,
            (Self::Authenticating, FrameType::ConnectAck) => Self::Active,
            (Self::Authenticating, FrameType::ConnectError)
            | (Self::Closing, FrameType::Disconnect) => Self::Closed,
            (Self::Active, FrameType::Disconnect) => Self::Closing,
            _ => self,
        }
    }
}

/// Result of lifecycle frame handling.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LifecycleAction {
    /// State after accepting the frame.
    pub state: ConnectionState,
    /// Optional protocol response generated by handling the frame.
    pub response: Option<Frame>,
}

#[cfg(test)]
mod tests {
    use super::ConnectionState;
    use crate::protocol::{Frame, FrameType, ProtocolError, ProtocolVersion, validate_stream};

    #[test]
    fn connecting_rejects_publish_with_state_and_frame_type() {
        let result = ConnectionState::Connecting.validate_frame_type(FrameType::Publish);

        assert!(matches!(
            result,
            Err(ProtocolError::InvalidStateTransition {
                current_state: ConnectionState::Connecting,
                frame_type: FrameType::Publish,
                ..
            })
        ));
    }

    #[test]
    fn active_accepts_subscribe() -> Result<(), ProtocolError> {
        ConnectionState::Active.validate_frame_type(FrameType::Subscribe)
    }

    #[test]
    fn connecting_rejects_ping_with_state_and_frame_type() {
        let result = ConnectionState::Connecting.validate_frame_type(FrameType::Ping);

        assert!(matches!(
            result,
            Err(ProtocolError::InvalidStateTransition {
                current_state: ConnectionState::Connecting,
                frame_type: FrameType::Ping,
                ..
            })
        ));
    }

    #[test]
    fn connect_transitions_to_authenticating() -> Result<(), ProtocolError> {
        let frame = Frame::Connect {
            flags: 0,
            min_version: ProtocolVersion::new(1, 0),
            max_version: ProtocolVersion::new(2, 0),
            auth_token: vec![1, 2, 3],
        };

        assert_eq!(
            ConnectionState::Connecting.transition(&frame)?,
            ConnectionState::Authenticating
        );
        Ok(())
    }

    #[test]
    fn connect_ack_transitions_authenticating_to_active() -> Result<(), ProtocolError> {
        let frame = Frame::ConnectAck {
            flags: 0,
            selected_version: ProtocolVersion::new(1, 0),
            capabilities: 0,
        };

        assert_eq!(
            ConnectionState::Authenticating.transition(&frame)?,
            ConnectionState::Active
        );
        Ok(())
    }

    #[test]
    fn disconnect_ack_in_closing_transitions_to_closed() -> Result<(), ProtocolError> {
        let frame = Frame::Disconnect { flags: 0 };

        assert_eq!(
            ConnectionState::Closing.transition(&frame)?,
            ConnectionState::Closed
        );
        Ok(())
    }

    #[test]
    fn closed_accepts_nothing() {
        for frame_type in [FrameType::Disconnect, FrameType::Unknown(0xFE)] {
            let result = ConnectionState::Closed.validate_frame_type(frame_type);

            assert!(matches!(
                result,
                Err(ProtocolError::InvalidStateTransition {
                    current_state: ConnectionState::Closed,
                    frame_type: rejected,
                    ..
                }) if rejected == frame_type
            ));
        }
    }

    #[test]
    fn ping_on_stream_zero_in_active_is_valid() -> Result<(), ProtocolError> {
        let frame = Frame::Ping { flags: 0 };

        assert_eq!(
            ConnectionState::Active.transition(&frame)?,
            ConnectionState::Active
        );
        Ok(())
    }

    #[test]
    fn ping_on_stream_one_in_active_returns_invalid_stream() {
        let result = validate_stream(FrameType::Ping, 1);

        assert!(matches!(result, Err(ProtocolError::InvalidStream { .. })));
    }

    #[test]
    fn ping_in_connecting_returns_invalid_state_transition() {
        let frame = Frame::Ping { flags: 0 };
        let result = ConnectionState::Connecting.transition(&frame);

        assert!(matches!(
            result,
            Err(ProtocolError::InvalidStateTransition {
                current_state: ConnectionState::Connecting,
                frame_type: FrameType::Ping,
                ..
            })
        ));
    }

    #[test]
    fn ping_generates_pong_response_on_stream_zero() -> Result<(), ProtocolError> {
        let frame = Frame::Ping { flags: 7 };
        let action = ConnectionState::Active.handle_frame(&frame)?;

        assert_eq!(action.state, ConnectionState::Active);
        assert_eq!(action.response, Some(Frame::Pong { flags: 0 }));
        assert_eq!(action.response.as_ref().map(Frame::stream_id), Some(0));
        Ok(())
    }

    #[test]
    fn pong_frames_carry_no_payload_semantically() -> Result<(), ProtocolError> {
        let frame = Frame::Pong { flags: 0 };
        let action = ConnectionState::Active.handle_frame(&frame)?;

        assert_eq!(action.state, ConnectionState::Active);
        assert_eq!(action.response, None);
        Ok(())
    }
}
