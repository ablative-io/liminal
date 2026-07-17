//! Response authorities bound to `ClientRequest::ParticipantAck` (`0x0004`)
//! and `ClientRequest::MarkerAck` (`0x0006`).
//!
//! Per the register's closing rule (contract lines 5699-5701), normal and
//! marker acks never return `ObserverBackpressure`,
//! `MarkerClosureCapacityExceeded`, or `ConversationOrderExhausted`: they
//! append no record, allocate no major, and may relieve retention pressure.
//! `ConversationSequenceExhausted` is excluded separately by the register's
//! reachability rule (contract lines 5691-5696): that outcome remains
//! reachable only for optional enrollment, attach, supersession, ordinary,
//! or floor-triggering candidates. No constructor for any of those outcomes
//! exists here.

use super::super::{
    AckCommitted, AckGap, AckNoOp, AckRegression, ConnectionConversationCapacityExceeded,
    MarkerAckCommitted, MarkerAckEnvelope, MarkerMismatch, MarkerNotDelivered, NoBinding,
    ParticipantAckEnvelope, ParticipantUnknown, ResponseEnvelope, Retired, ServerDiscriminant,
    ServerValue, StaleAuthority,
};

/// Server response bound to one continuous cumulative acknowledgement.
///
/// Constructors exist only for the outcomes the frozen R-D1 register admits
/// for normal ack; every other pairing is a compile error by construction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParticipantAckResponse {
    value: ServerValue,
}

impl ParticipantAckResponse {
    /// First decoded semantic operation for an untracked conversation
    /// exceeded the connection-conversation limit (register row 5641).
    #[must_use]
    pub const fn connection_conversation_capacity_exceeded(
        request: ParticipantAckEnvelope,
        limit: u64,
    ) -> Self {
        Self {
            value: ServerValue::ConnectionConversationCapacityExceeded(
                ConnectionConversationCapacityExceeded::SemanticRequest {
                    request: ResponseEnvelope::ParticipantAck(request),
                    limit,
                },
            ),
        }
    }

    /// Presented participant is unknown (register row 5645).
    ///
    /// The payload is minted only by `lookup_binding_required` for this
    /// exact request.
    pub(crate) const fn from_participant_unknown(value: ParticipantUnknown) -> Self {
        Self {
            value: ServerValue::ParticipantUnknown(value),
        }
    }

    /// Exact-binding lookup missed (register row 5646).
    ///
    /// The payload is minted only by `lookup_binding_required` for this
    /// exact request.
    pub(crate) const fn from_no_binding(value: NoBinding) -> Self {
        Self {
            value: ServerValue::NoBinding(value),
        }
    }

    /// Live generation authority is stale (register row 5647).
    ///
    /// The payload is minted only by `lookup_binding_required` for this
    /// exact request.
    pub(crate) const fn from_stale_authority(value: StaleAuthority) -> Self {
        Self {
            value: ServerValue::StaleAuthority(value),
        }
    }

    /// Presented id has a tombstone (register row 5677).
    ///
    /// The payload is minted only by `lookup_binding_required` for this
    /// exact request.
    pub(crate) const fn from_retired(value: Retired) -> Self {
        Self {
            value: ServerValue::Retired(value),
        }
    }

    /// The acknowledgement advanced the cursor (register row 5674).
    #[must_use]
    pub const fn ack_committed(value: AckCommitted) -> Self {
        Self {
            value: ServerValue::AckCommitted(value),
        }
    }

    /// Idempotent confirmation at the unchanged cursor (register row 5675).
    #[must_use]
    pub const fn ack_no_op(request: ParticipantAckEnvelope) -> Self {
        Self {
            value: ServerValue::AckNoOp(AckNoOp::participant_ack(request)),
        }
    }

    /// Idempotent confirmation minted by the nonzero-debt cumulative-ack
    /// episode selector for this exact request (register row 5675).
    pub(crate) const fn from_ack_no_op(value: AckNoOp) -> Self {
        Self {
            value: ServerValue::AckNoOp(value),
        }
    }

    /// The requested boundary crossed a gap (register row 5676).
    #[must_use]
    pub const fn ack_gap(value: AckGap) -> Self {
        Self {
            value: ServerValue::AckGap(value),
        }
    }

    /// The requested boundary regressed below the cursor (register row 5676).
    #[must_use]
    pub const fn ack_regression(value: AckRegression) -> Self {
        Self {
            value: ServerValue::AckRegression(value),
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

/// Server response bound to one explicit marker acknowledgement.
///
/// Constructors exist only for the outcomes the frozen R-D1 register admits
/// for marker ack; every other pairing is a compile error by construction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarkerAckResponse {
    value: ServerValue,
}

impl MarkerAckResponse {
    /// First decoded semantic operation for an untracked conversation
    /// exceeded the connection-conversation limit (register row 5641).
    #[must_use]
    pub const fn connection_conversation_capacity_exceeded(
        request: MarkerAckEnvelope,
        limit: u64,
    ) -> Self {
        Self {
            value: ServerValue::ConnectionConversationCapacityExceeded(
                ConnectionConversationCapacityExceeded::SemanticRequest {
                    request: ResponseEnvelope::MarkerAck(request),
                    limit,
                },
            ),
        }
    }

    /// Presented participant is unknown (register row 5645).
    ///
    /// The payload is minted only by `lookup_binding_required` for this
    /// exact request.
    pub(crate) const fn from_participant_unknown(value: ParticipantUnknown) -> Self {
        Self {
            value: ServerValue::ParticipantUnknown(value),
        }
    }

    /// Exact-binding lookup missed (register row 5646).
    ///
    /// The payload is minted only by `lookup_binding_required` for this
    /// exact request.
    pub(crate) const fn from_no_binding(value: NoBinding) -> Self {
        Self {
            value: ServerValue::NoBinding(value),
        }
    }

    /// Live generation authority is stale (register row 5647).
    ///
    /// The payload is minted only by `lookup_binding_required` for this
    /// exact request.
    pub(crate) const fn from_stale_authority(value: StaleAuthority) -> Self {
        Self {
            value: ServerValue::StaleAuthority(value),
        }
    }

    /// Presented id has a tombstone (register row 5684).
    ///
    /// The payload is minted only by `lookup_binding_required` for this
    /// exact request.
    pub(crate) const fn from_retired(value: Retired) -> Self {
        Self {
            value: ServerValue::Retired(value),
        }
    }

    /// The marker acknowledgement advanced the cursor (register row 5682).
    #[must_use]
    pub const fn marker_ack_committed(value: MarkerAckCommitted) -> Self {
        Self {
            value: ServerValue::MarkerAckCommitted(value),
        }
    }

    /// Idempotent confirmation at the unchanged marker cursor (register row
    /// 5682).
    ///
    /// The payload is minted only by the shared marker-proof selector
    /// invoked with this request's own proof fields.
    pub(crate) const fn from_ack_no_op(value: AckNoOp) -> Self {
        Self {
            value: ServerValue::AckNoOp(value),
        }
    }

    /// The named marker was not delivered to the proof epoch (register row
    /// 5683).
    ///
    /// The payload is minted only by the shared marker-proof selector
    /// invoked with this request's own proof fields.
    pub(crate) const fn from_marker_not_delivered(value: MarkerNotDelivered) -> Self {
        Self {
            value: ServerValue::MarkerNotDelivered(value),
        }
    }

    /// The named marker mismatches current marker state (register row 5683).
    ///
    /// The payload is minted only by the shared marker-proof selector
    /// invoked with this request's own proof fields.
    pub(crate) const fn from_marker_mismatch(value: MarkerMismatch) -> Self {
        Self {
            value: ServerValue::MarkerMismatch(value),
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
