use liminal::protocol::Frame;
use liminal_protocol::wire::{
    AuthenticationState, ClientRequest, CodecError, InboundGateContext, InboundGateError,
    NegotiatedParticipantCapability, PARTICIPANT_FRAME_TYPE, ParticipantCapabilityState,
    ParticipantFrame, ParticipantTransportRejected, ReceiverDirection, ServerValue,
    ValidatedFrameLimit, encode, encoded_len, gate_inbound,
};

/// Bit advertised in the existing connection acknowledgement for `participant-v1`.
///
/// The legacy conversation frames remain available only for the Phase C SDK
/// migration window. Participant lifecycle traffic itself uses the shared
/// protocol's assigned outer frame exclusively.
pub const PARTICIPANT_CAPABILITY_BIT: u32 = 1;

/// Complete participant-frame limit selected for one server connection.
const PARTICIPANT_MAX_FRAME_BYTES: u64 = 4 * 1024 * 1024;

/// Participant capability negotiated on one authenticated connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParticipantSession {
    capability: ParticipantCapabilityState,
}

impl Default for ParticipantSession {
    fn default() -> Self {
        Self {
            capability: ParticipantCapabilityState::Missing,
        }
    }
}

impl ParticipantSession {
    /// Stores the server's supported v1 capability after a successful handshake.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::InvalidFrameLimit`] if the server limit ever drifts
    /// outside the shared protocol's accepted range.
    pub fn negotiate_v1(&mut self) -> Result<(), CodecError> {
        let limit = ValidatedFrameLimit::new(PARTICIPANT_MAX_FRAME_BYTES)?;
        self.capability =
            ParticipantCapabilityState::Negotiated(NegotiatedParticipantCapability::v1(limit));
        Ok(())
    }

    const fn gate_context(self, authenticated: bool) -> InboundGateContext {
        InboundGateContext {
            receiver: ReceiverDirection::Server,
            authentication: if authenticated {
                AuthenticationState::Authenticated
            } else {
                AuthenticationState::Unauthenticated
            },
            participant_capability: self.capability,
        }
    }
}

/// Result of classifying one preserved generic frame through the shared gate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParticipantIngress {
    /// The generic frame is not assigned to participant traffic.
    NotParticipant,
    /// A structurally valid and authorized client request.
    Request(ClientRequest),
    /// Exact pre-semantic rejection selected by `liminal-protocol`.
    Rejected(ParticipantTransportRejected),
    /// A locally fabricated generic frame could not be represented canonically.
    InvalidGenericFrame,
}

/// Applies the shared participant gate to one generic frame preserved by
/// `liminal`'s forward-compatible decoder.
#[must_use]
pub fn gate_generic_frame(
    frame: &Frame,
    authenticated: bool,
    session: ParticipantSession,
) -> ParticipantIngress {
    let Frame::Unknown {
        type_id,
        flags,
        stream_id,
        payload,
    } = frame
    else {
        return ParticipantIngress::NotParticipant;
    };
    if *type_id != PARTICIPANT_FRAME_TYPE {
        return ParticipantIngress::NotParticipant;
    }
    let Ok(payload_length) = u32::try_from(payload.len()) else {
        return ParticipantIngress::InvalidGenericFrame;
    };
    let Some(capacity) = 10_usize.checked_add(payload.len()) else {
        return ParticipantIngress::InvalidGenericFrame;
    };
    let mut complete = Vec::with_capacity(capacity);
    complete.push(*type_id);
    complete.push(*flags);
    complete.extend_from_slice(&stream_id.to_be_bytes());
    complete.extend_from_slice(&payload_length.to_be_bytes());
    complete.extend_from_slice(payload);

    match gate_inbound(&complete, session.gate_context(authenticated)) {
        Ok(ParticipantFrame::ClientRequest(request)) => ParticipantIngress::Request(request),
        Ok(ParticipantFrame::ServerValue(_) | ParticipantFrame::ServerPush(_)) => {
            ParticipantIngress::InvalidGenericFrame
        }
        Err(InboundGateError::NotParticipantFrame { .. }) => ParticipantIngress::NotParticipant,
        Err(InboundGateError::ParticipantRejected(rejection)) => {
            ParticipantIngress::Rejected(rejection)
        }
    }
}

/// Encodes one crate-produced semantic value into the existing generic frame.
///
/// # Errors
///
/// Returns the shared codec error if the typed value cannot be encoded.
pub fn encode_server_value(value: ServerValue) -> Result<Frame, CodecError> {
    let participant = ParticipantFrame::ServerValue(value);
    let needed = encoded_len(&participant)?;
    let mut complete = vec![0_u8; needed];
    let written = encode(&participant, &mut complete)?;
    complete.truncate(written);

    let payload = complete
        .get(10..)
        .ok_or(CodecError::LengthOverflow)?
        .to_vec();
    Ok(Frame::Unknown {
        type_id: PARTICIPANT_FRAME_TYPE,
        flags: 0,
        stream_id: 0,
        payload,
    })
}
