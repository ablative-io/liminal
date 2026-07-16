//! Response authority bound to `ClientRequest::CredentialAttach` (`0x0002`).

use alloc::boxed::Box;

use super::super::{
    AttachBound, AttachEnvelope, AttachMarkerProof, AttemptConflict, AttemptTokenBodyConflict,
    CommonStaleAuthorityEnvelope, ConnectionConversationBindingOccupied,
    ConnectionConversationCapacityExceeded, ConversationOrderExhausted,
    ConversationSequenceExhausted, DeliverySeq, Generation, MarkerClosureCapacityExceeded,
    MarkerMismatch, MarkerMismatchBody, MarkerNotDelivered, MarkerNotDeliveredReason,
    MarkerProofRequest, ObserverBackpressure, ObserverBackpressureState,
    ParticipantReferenceEnvelope, ParticipantUnknown, ReceiptCapacityExceeded,
    ReceiptCapacityScope, ReceiptExpired, ReceiptExpiryReason, ReceiptReplay, ResponseEnvelope,
    Retired, SequenceAllocatingEnvelope, SequenceBudget, ServerDiscriminant, ServerValue,
    StaleAuthority, StaleOrUnknownReceipt,
};
use crate::wire::closure::{ClosureCheckedEnvelope, ClosureRefusalReason, ClosureSnapshot};

/// Server response bound to one credential-attach request.
///
/// Constructors exist only for the outcomes the frozen R-D1 register admits
/// for credential attach; every other pairing is a compile error by
/// construction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CredentialAttachResponse {
    value: ServerValue,
}

impl CredentialAttachResponse {
    /// Verified exact live receipt with a changed canonical non-secret body;
    /// generation is tested before the marker sequence (register row 5639).
    #[must_use]
    pub const fn attempt_token_body_conflict(
        request: &AttachEnvelope,
        conflict: AttemptConflict,
    ) -> Self {
        Self {
            value: ServerValue::AttemptTokenBodyConflict(
                AttemptTokenBodyConflict::CredentialAttach {
                    token: request.attach_attempt_token,
                    conversation_id: request.conversation_id,
                    presented_participant_id: request.participant_id,
                    presented_generation: request.capability_generation,
                    presented_marker_delivery_seq: request.accept_marker_delivery_seq,
                    conflict,
                },
            ),
        }
    }

    /// First decoded semantic operation for an untracked conversation
    /// exceeded the connection-conversation limit (register row 5641).
    #[must_use]
    pub const fn connection_conversation_capacity_exceeded(
        request: AttachEnvelope,
        limit: u64,
    ) -> Self {
        Self {
            value: ServerValue::ConnectionConversationCapacityExceeded(
                ConnectionConversationCapacityExceeded::SemanticRequest {
                    request: ResponseEnvelope::CredentialAttach(request),
                    limit,
                },
            ),
        }
    }

    /// Credential-attach binding attempt found an occupied slot owned by a
    /// different participant (register row 5643).
    #[must_use]
    pub const fn connection_conversation_binding_occupied(request: &AttachEnvelope) -> Self {
        Self {
            value: ServerValue::ConnectionConversationBindingOccupied(
                ConnectionConversationBindingOccupied::CredentialAttach {
                    conversation_id: request.conversation_id,
                    participant_id: request.participant_id,
                    capability_generation: request.capability_generation,
                    attach_attempt_token: request.attach_attempt_token,
                    accept_marker_delivery_seq: request.accept_marker_delivery_seq,
                },
            ),
        }
    }

    /// Credential attach required an unreserved `transaction_order` major and
    /// the conversation order is exhausted (register row 5644).
    #[must_use]
    pub fn conversation_order_exhausted(
        request: AttachEnvelope,
        high: u64,
        order_remaining: u128,
        reserved_claims: u128,
        resulting_order_remaining: u128,
        resulting_reserved_claims: u128,
    ) -> Self {
        Self {
            value: ServerValue::ConversationOrderExhausted(Box::new(
                ConversationOrderExhausted::new(
                    super::super::OrderAllocatingEnvelope::CredentialAttach(request),
                    high,
                    order_remaining,
                    reserved_claims,
                    resulting_order_remaining,
                    resulting_reserved_claims,
                ),
            )),
        }
    }

    /// Presented participant is unknown (register row 5645).
    #[must_use]
    pub const fn participant_unknown(request: AttachEnvelope) -> Self {
        Self {
            value: ServerValue::ParticipantUnknown(ParticipantUnknown {
                request: ParticipantReferenceEnvelope::CredentialAttach(request),
            }),
        }
    }

    /// Live generation or secret authority is stale (register rows 5647,
    /// 5660, 5665).
    #[must_use]
    pub const fn stale_authority(request: AttachEnvelope, current_generation: Generation) -> Self {
        Self {
            value: ServerValue::StaleAuthority(StaleAuthority::Live {
                request: CommonStaleAuthorityEnvelope::CredentialAttach(request),
                current_generation,
            }),
        }
    }

    /// Presented id or exact token resolved to a tombstone (register rows
    /// 5648, 5659, 5667).
    #[must_use]
    pub const fn retired(request: AttachEnvelope, retired_generation: Generation) -> Self {
        Self {
            value: ServerValue::Retired(Retired::Participant {
                request: ParticipantReferenceEnvelope::CredentialAttach(request),
                retired_generation,
            }),
        }
    }

    /// Closure-checked attach admission exceeded marker-closure capacity
    /// (register rows 5649, 5662).
    #[must_use]
    pub fn marker_closure_capacity_exceeded(
        request: AttachEnvelope,
        snapshot: ClosureSnapshot,
        reason: ClosureRefusalReason,
    ) -> Self {
        Self {
            value: ServerValue::MarkerClosureCapacityExceeded(Box::new(
                MarkerClosureCapacityExceeded {
                    request: ClosureCheckedEnvelope::CredentialAttach(request),
                    snapshot,
                    reason,
                },
            )),
        }
    }

    /// Successful credential attach (register row 5658).
    #[must_use]
    pub const fn attach_bound(value: AttachBound) -> Self {
        Self {
            value: ServerValue::AttachBound(value),
        }
    }

    /// Exact credential-attach provenance window response; the flattened
    /// request-echo fields are derived from the request's own envelope
    /// (register rows 5659, 5664).
    #[must_use]
    pub const fn receipt_expired(
        request: &AttachEnvelope,
        result_generation: Generation,
        current_generation: Generation,
        reason: ReceiptExpiryReason,
    ) -> Self {
        Self {
            value: ServerValue::ReceiptExpired(ReceiptExpired::CredentialAttach {
                conversation_id: request.conversation_id,
                token: request.attach_attempt_token,
                participant_id: request.participant_id,
                presented_generation: request.capability_generation,
                presented_marker_delivery_seq: request.accept_marker_delivery_seq,
                result_generation,
                current_generation,
                reason,
            }),
        }
    }

    /// Post-provenance ambiguity: the receipt is no longer known (register
    /// rows 5659, 5666).
    #[must_use]
    pub const fn stale_or_unknown_receipt(value: StaleOrUnknownReceipt) -> Self {
        Self {
            value: ServerValue::StaleOrUnknownReceipt(value),
        }
    }

    /// Fenced attach named a marker that was never delivered to the proof
    /// epoch (register row 5661).
    #[must_use]
    pub const fn marker_not_delivered(
        proof: AttachMarkerProof,
        reason: MarkerNotDeliveredReason,
        expected_marker_delivery_seq: DeliverySeq,
    ) -> Self {
        Self {
            value: ServerValue::MarkerNotDelivered(MarkerNotDelivered {
                request: MarkerProofRequest::CredentialAttach(proof),
                reason,
                expected_marker_delivery_seq,
            }),
        }
    }

    /// Fenced attach named a marker that mismatches current marker state
    /// (register row 5661).
    #[must_use]
    pub const fn marker_mismatch(proof: AttachMarkerProof, mismatch: MarkerMismatchBody) -> Self {
        Self {
            value: ServerValue::MarkerMismatch(MarkerMismatch {
                request: MarkerProofRequest::CredentialAttach(proof),
                mismatch,
            }),
        }
    }

    /// The first full scope in the exact five-scope receipt/provenance order
    /// (register row 5662).
    #[must_use]
    pub const fn receipt_capacity_exceeded(
        request: AttachEnvelope,
        scope: ReceiptCapacityScope,
        limit: u64,
        occupied: u64,
    ) -> Self {
        Self {
            value: ServerValue::ReceiptCapacityExceeded(
                ReceiptCapacityExceeded::CredentialAttach {
                    request,
                    scope,
                    limit,
                    occupied,
                },
            ),
        }
    }

    /// Hard-observer retention refused the attach append (register row 5662).
    #[must_use]
    pub const fn observer_backpressure(
        request: AttachEnvelope,
        state: ObserverBackpressureState,
    ) -> Self {
        Self {
            value: ServerValue::ObserverBackpressure(ObserverBackpressure::CredentialAttach {
                request,
                state,
            }),
        }
    }

    /// Canonical resulting sequence-reserve check failed (register row 5662).
    #[must_use]
    pub fn conversation_sequence_exhausted(
        request: AttachEnvelope,
        sequence_budget: SequenceBudget,
    ) -> Self {
        Self {
            value: ServerValue::ConversationSequenceExhausted(Box::new(
                ConversationSequenceExhausted {
                    request: SequenceAllocatingEnvelope::CredentialAttach(request),
                    sequence_budget,
                },
            )),
        }
    }

    /// Byte-identical receipt replay whose exact binding epoch still occupies
    /// its origin slot (register row 5663).
    #[must_use]
    pub const fn bound(value: AttachBound) -> Self {
        Self {
            value: ServerValue::Bound(ReceiptReplay::CredentialAttach(value)),
        }
    }

    /// Byte-identical receipt replay whose origin slot is empty, replaced, or
    /// at a later epoch (register row 5663).
    #[must_use]
    pub const fn unbound_receipt(value: AttachBound) -> Self {
        Self {
            value: ServerValue::UnboundReceipt(ReceiptReplay::CredentialAttach(value)),
        }
    }

    /// Borrows the bound wire value for encoding or inspection.
    #[must_use]
    pub const fn server_value(&self) -> &ServerValue {
        &self.value
    }

    /// Returns the bound value's exact wire discriminant.
    #[must_use]
    pub const fn discriminant(&self) -> ServerDiscriminant {
        self.value.discriminant()
    }

    /// Moves the bound wire value out for transmission.
    #[must_use]
    pub fn into_server_value(self) -> ServerValue {
        self.value
    }
}
