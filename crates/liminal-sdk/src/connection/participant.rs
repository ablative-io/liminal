//! Participant receive binding for the shared protocol inbound gate.
//!
//! This module deliberately owns no participant lifecycle state. It preserves
//! the protocol crate's exhaustive [`ParticipantFrame`] and [`InboundGateError`]
//! types so callers receive the crate's exact typed value without an SDK-local
//! transition table, generation comparison, token match, or fallback arm.

use liminal_protocol::wire::{
    InboundGateContext, InboundGateError, ParticipantFrame, gate_inbound,
};

/// Applies the protocol-owned inbound gate to one complete participant frame.
///
/// The returned value is the crate's exhaustive frame type: semantic server
/// values, server pushes, and directionally invalid client requests remain
/// distinguishable without an SDK-owned classification layer.
///
/// This is a structural receive boundary only. Correlating a semantic value to
/// an expected request and applying it to durable client lifecycle state require
/// protocol-owned authorities not represented by this adapter.
///
/// # Errors
///
/// Returns the exact [`InboundGateError`] selected by the protocol crate for a
/// non-participant outer frame or a participant transport rejection.
pub fn receive_participant_frame(
    bytes: &[u8],
    context: InboundGateContext,
) -> Result<ParticipantFrame, InboundGateError> {
    gate_inbound(bytes, context)
}

#[cfg(test)]
mod tests {
    use alloc::vec;

    use liminal_protocol::wire::{
        AckCommitted, AckGap, AckNoOp, AckRegression, AuthenticationState, Generation,
        InboundGateContext, NegotiatedParticipantCapability, ParticipantAckEnvelope,
        ParticipantCapabilityState, ParticipantFrame, ReceiverDirection, ServerValue,
        ValidatedFrameLimit, encode, encoded_len,
    };

    use super::receive_participant_frame;

    const CONVERSATION_ID: u64 = 34;
    const PARTICIPANT_ID: u64 = 3;
    const CURRENT_CURSOR: u64 = 10;
    const OFFERED_THROUGH: u64 = 12;

    fn envelope(through_seq: u64) -> ParticipantAckEnvelope {
        ParticipantAckEnvelope {
            conversation_id: CONVERSATION_ID,
            participant_id: PARTICIPANT_ID,
            capability_generation: Generation::ONE,
            through_seq,
        }
    }

    fn client_context() -> Result<InboundGateContext, &'static str> {
        let limit = ValidatedFrameLimit::new(1_048_576)
            .map_err(|_| "the protocol pre-capability ceiling must be a valid negotiated limit")?;
        Ok(InboundGateContext {
            receiver: ReceiverDirection::Client,
            authentication: AuthenticationState::Authenticated,
            participant_capability: ParticipantCapabilityState::Negotiated(
                NegotiatedParticipantCapability::v1(limit),
            ),
        })
    }

    fn receive(value: ServerValue) -> Result<ParticipantFrame, &'static str> {
        let frame = ParticipantFrame::ServerValue(value);
        let len = encoded_len(&frame)
            .map_err(|_| "typed test value must have a canonical encoded length")?;
        let mut bytes = vec![0_u8; len];
        let written =
            encode(&frame, &mut bytes).map_err(|_| "typed test value must encode canonically")?;
        assert_eq!(written, len);
        receive_participant_frame(&bytes, client_context()?)
            .map_err(|_| "canonical authenticated value must pass the SDK inbound boundary")
    }

    #[test]
    fn canonical_c34_ack_selector_values_cross_the_sdk_receive_boundary_intact()
    -> Result<(), &'static str> {
        let regression_request = envelope(9);
        let no_op_request = envelope(10);
        let committed_request = envelope(OFFERED_THROUGH);
        let gap_request = envelope(14);

        let regression = AckRegression::new(regression_request.clone(), CURRENT_CURSOR)
            .ok_or("9 must be below cursor 10")?;
        let no_op = AckNoOp::participant_ack(no_op_request.clone());
        let committed = AckCommitted::new(committed_request.clone());
        let gap = AckGap::new(gap_request.clone(), CURRENT_CURSOR)
            .ok_or("14 must be above cursor 10 and offered-through 12")?;

        assert_eq!(
            receive(ServerValue::AckRegression(regression.clone()))?,
            ParticipantFrame::ServerValue(ServerValue::AckRegression(regression.clone()))
        );
        assert_eq!(regression.request(), &regression_request);
        assert_eq!(regression.current_cursor(), CURRENT_CURSOR);

        assert_eq!(
            receive(ServerValue::AckNoOp(no_op.clone()))?,
            ParticipantFrame::ServerValue(ServerValue::AckNoOp(no_op.clone()))
        );
        assert_eq!(no_op, AckNoOp::ParticipantAck(no_op_request));
        assert_eq!(no_op.current_cursor(), CURRENT_CURSOR);

        assert_eq!(
            receive(ServerValue::AckCommitted(committed.clone()))?,
            ParticipantFrame::ServerValue(ServerValue::AckCommitted(committed.clone()))
        );
        assert_eq!(committed.request(), &committed_request);
        assert_eq!(committed.current_cursor(), OFFERED_THROUGH);

        assert_eq!(
            receive(ServerValue::AckGap(gap.clone()))?,
            ParticipantFrame::ServerValue(ServerValue::AckGap(gap.clone()))
        );
        assert_eq!(gap.request(), &gap_request);
        assert_eq!(gap.current_cursor(), CURRENT_CURSOR);
        Ok(())
    }
}
