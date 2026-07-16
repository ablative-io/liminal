//! Total marker-proof selection after participant and binding authority.
//!
//! The selector is pure: it consumes an operation-specific request view and
//! borrows durable marker facts. Only the success arm creates an opaque permit
//! retaining the exact operation, expected marker, proof epoch, and typed
//! marker-backed cursor provenance needed by a later `MarkerAck` or fenced attach
//! commit.

use crate::wire::{
    AckNoOp, AttachMarkerProof, BindingEpoch, CredentialAttachRequest, DeliverySeq, MarkerAck,
    MarkerAckEnvelope, MarkerAckProof, MarkerMismatch, MarkerMismatchBody, MarkerNotDelivered,
    MarkerNotDeliveredReason, MarkerProofRequest, ParticipantId,
};

use super::super::edge::ParticipantCursorProgress;

/// Operation-specific marker-proof input after common authority validation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MarkerProofInput {
    /// Credential attach presenting an explicit marker.
    CredentialAttach(AttachMarkerProof),
    /// Explicit marker acknowledgement.
    MarkerAck(MarkerAckProof),
}

impl MarkerProofInput {
    /// Derives attach proof input only when the request presents `Some(marker)`.
    #[must_use]
    pub const fn credential_attach(request: &CredentialAttachRequest) -> Option<Self> {
        let Some(requested_marker_delivery_seq) = request.accept_marker_delivery_seq else {
            return None;
        };
        Some(Self::CredentialAttach(AttachMarkerProof {
            conversation_id: request.conversation_id,
            token: request.attach_attempt_token,
            participant_id: request.participant_id,
            capability_generation: request.capability_generation,
            requested_marker_delivery_seq,
        }))
    }

    /// Derives marker-ack proof input without changing its operation envelope.
    #[must_use]
    pub const fn marker_ack(request: &MarkerAck) -> Self {
        Self::MarkerAck(MarkerAckProof {
            conversation_id: request.conversation_id,
            participant_id: request.participant_id,
            capability_generation: request.capability_generation,
            requested_marker_delivery_seq: request.marker_delivery_seq,
        })
    }

    /// Returns the exact requested marker sequence.
    #[must_use]
    pub const fn requested_marker_delivery_seq(&self) -> DeliverySeq {
        match self {
            Self::CredentialAttach(request) => request.requested_marker_delivery_seq,
            Self::MarkerAck(request) => request.requested_marker_delivery_seq,
        }
    }

    /// Returns the permanent participant named by the operation.
    #[must_use]
    pub const fn participant_id(&self) -> ParticipantId {
        match self {
            Self::CredentialAttach(request) => request.participant_id,
            Self::MarkerAck(request) => request.participant_id,
        }
    }

    const fn capability_generation(&self) -> crate::wire::Generation {
        match self {
            Self::CredentialAttach(request) => request.capability_generation,
            Self::MarkerAck(request) => request.capability_generation,
        }
    }

    const fn is_marker_ack(&self) -> bool {
        matches!(self, Self::MarkerAck(_))
    }

    const fn into_wire_request(self) -> MarkerProofRequest {
        match self {
            Self::CredentialAttach(request) => MarkerProofRequest::CredentialAttach(request),
            Self::MarkerAck(request) => MarkerProofRequest::MarkerAck(request),
        }
    }

    const fn marker_ack_envelope(&self) -> Option<MarkerAckEnvelope> {
        let Self::MarkerAck(request) = self else {
            return None;
        };
        Some(MarkerAckEnvelope {
            conversation_id: request.conversation_id,
            participant_id: request.participant_id,
            capability_generation: request.capability_generation,
            marker_delivery_seq: request.requested_marker_delivery_seq,
        })
    }
}

/// Durable participant facts read by the total marker-proof selector.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarkerProofState {
    current_cursor: DeliverySeq,
    accepted_marker_at_cursor: bool,
    expected_marker_delivery_seq: Option<DeliverySeq>,
    proof_binding_epoch: BindingEpoch,
    delivered_to_proof_epoch: Option<ParticipantCursorProgress>,
}

impl MarkerProofState {
    /// Creates one immutable snapshot of the durable marker proof facts.
    ///
    /// The final field requires a cursor-progress witness. A planned, still
    /// undelivered [`crate::lifecycle::MarkerDelivery`] cannot be supplied as
    /// proof by construction.
    ///
    /// ```compile_fail
    /// use liminal_protocol::{
    ///     lifecycle::{MarkerDelivery, MarkerProofState},
    ///     wire::BindingEpoch,
    /// };
    ///
    /// fn undelivered_is_not_proof(epoch: BindingEpoch, delivery: MarkerDelivery) {
    ///     let _ = MarkerProofState::new(0, false, Some(1), epoch, Some(delivery));
    /// }
    /// ```
    #[must_use]
    pub const fn new(
        current_cursor: DeliverySeq,
        accepted_marker_at_cursor: bool,
        expected_marker_delivery_seq: Option<DeliverySeq>,
        proof_binding_epoch: BindingEpoch,
        delivered_to_proof_epoch: Option<ParticipantCursorProgress>,
    ) -> Self {
        Self {
            current_cursor,
            accepted_marker_at_cursor,
            expected_marker_delivery_seq,
            proof_binding_epoch,
            delivered_to_proof_epoch,
        }
    }

    /// Returns the participant's durable cumulative cursor.
    #[must_use]
    pub const fn current_cursor(self) -> DeliverySeq {
        self.current_cursor
    }

    /// Returns whether the record at the cursor is this participant's accepted marker.
    #[must_use]
    pub const fn accepted_marker_at_cursor(self) -> bool {
        self.accepted_marker_at_cursor
    }

    /// Returns the currently expected marker anchor, if any.
    #[must_use]
    pub const fn expected_marker_delivery_seq(self) -> Option<DeliverySeq> {
        self.expected_marker_delivery_seq
    }

    /// Returns the exact binding epoch against which delivery must be proven.
    #[must_use]
    pub const fn proof_binding_epoch(self) -> BindingEpoch {
        self.proof_binding_epoch
    }

    /// Returns the marker-backed cursor witness proving exact delivery, if present.
    #[must_use]
    pub const fn delivered_to_proof_epoch(self) -> Option<ParticipantCursorProgress> {
        self.delivered_to_proof_epoch
    }
}

/// Opaque authority for an exact delivered marker and originating operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarkerProofPermit {
    operation: MarkerProofInput,
    expected_marker_delivery_seq: DeliverySeq,
    proof_binding_epoch: BindingEpoch,
    progress: ParticipantCursorProgress,
}

impl MarkerProofPermit {
    /// Returns the exact operation authorized by this proof.
    #[must_use]
    pub const fn operation(&self) -> &MarkerProofInput {
        &self.operation
    }

    /// Returns the exact expected and delivered marker sequence.
    #[must_use]
    pub const fn expected_marker_delivery_seq(&self) -> DeliverySeq {
        self.expected_marker_delivery_seq
    }

    /// Returns the binding epoch against which delivery was proven.
    #[must_use]
    pub const fn proof_binding_epoch(&self) -> BindingEpoch {
        self.proof_binding_epoch
    }

    /// Returns the retained marker-backed cursor provenance.
    #[must_use]
    pub const fn progress(&self) -> ParticipantCursorProgress {
        self.progress
    }
}

/// Exhaustive result of marker-proof selection after common authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MarkerProofDecision {
    /// Exact replay of an already-accepted marker acknowledgement.
    AckNoOp(AckNoOp),
    /// Requested marker conflicts with cursor or anchor state.
    MarkerMismatch(MarkerMismatch),
    /// Exact expected marker lacks delivery to the proof epoch.
    MarkerNotDelivered(MarkerNotDelivered),
    /// Exact expected marker was delivered to the exact proof epoch.
    Permit(MarkerProofPermit),
}

/// Applies the frozen total marker-proof selector in its exact precedence order.
#[must_use]
pub fn select_marker_proof(
    state: &MarkerProofState,
    input: MarkerProofInput,
) -> MarkerProofDecision {
    let requested = input.requested_marker_delivery_seq();
    if requested < state.current_cursor {
        return MarkerProofDecision::MarkerMismatch(MarkerMismatch {
            request: input.into_wire_request(),
            mismatch: MarkerMismatchBody::BelowCursor {
                current_cursor: state.current_cursor,
            },
        });
    }

    if requested == state.current_cursor && state.accepted_marker_at_cursor && input.is_marker_ack()
    {
        if let Some(envelope) = input.marker_ack_envelope() {
            return MarkerProofDecision::AckNoOp(AckNoOp::marker_ack(envelope));
        }
    }

    let Some(expected) = state.expected_marker_delivery_seq else {
        return MarkerProofDecision::MarkerMismatch(MarkerMismatch {
            request: input.into_wire_request(),
            mismatch: MarkerMismatchBody::NoMarkerExpected,
        });
    };
    if requested != expected {
        return MarkerProofDecision::MarkerMismatch(MarkerMismatch {
            request: input.into_wire_request(),
            mismatch: MarkerMismatchBody::ExpectedDifferentMarker {
                expected_marker_delivery_seq: expected,
            },
        });
    }

    let Some(progress) = state.delivered_to_proof_epoch else {
        return MarkerProofDecision::MarkerNotDelivered(MarkerNotDelivered {
            request: input.into_wire_request(),
            reason: MarkerNotDeliveredReason::NotDeliveredToProofEpoch,
            expected_marker_delivery_seq: expected,
        });
    };
    if progress.participant_id() != input.participant_id()
        || progress.binding_epoch() != state.proof_binding_epoch
        || progress.through_seq() != expected
        || progress.marker_delivery_seq() != Some(expected)
        || state.proof_binding_epoch.capability_generation != input.capability_generation()
    {
        return MarkerProofDecision::MarkerNotDelivered(MarkerNotDelivered {
            request: input.into_wire_request(),
            reason: MarkerNotDeliveredReason::NotDeliveredToProofEpoch,
            expected_marker_delivery_seq: expected,
        });
    }
    MarkerProofDecision::Permit(MarkerProofPermit {
        operation: input,
        expected_marker_delivery_seq: expected,
        proof_binding_epoch: state.proof_binding_epoch,
        progress,
    })
}
