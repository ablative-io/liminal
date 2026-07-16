//! Response authorities bound to `ClientRequest::RecordAdmission` (`0x0007`)
//! and `ClientRequest::ObserverRecovery` (`0x0008`).

use alloc::boxed::Box;

use super::super::{
    ConnectionConversationCapacityExceeded, ConversationId, ConversationOrderExhausted,
    ConversationSequenceExhausted, InvalidObserverEpoch, InvalidObserverEpochList,
    MarkerClosureCapacityExceeded, NoBinding, ObserverBackpressure, ObserverRecoveryAccepted,
    ParticipantUnknown, RecordAdmissionEnvelope, RecordCommitted, RecordTooLarge, ResponseEnvelope,
    Retired, ServerDiscriminant, ServerValue, StaleAuthority,
};

/// Server response bound to one ordinary record admission.
///
/// Constructors exist only for the outcomes the frozen R-D1 register admits
/// for ordinary admission; every other pairing is a compile error by
/// construction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordAdmissionResponse {
    value: ServerValue,
}

impl RecordAdmissionResponse {
    /// First decoded semantic operation for an untracked conversation
    /// exceeded the connection-conversation limit (register row 5641).
    #[must_use]
    pub const fn connection_conversation_capacity_exceeded(
        request: RecordAdmissionEnvelope,
        limit: u64,
    ) -> Self {
        Self {
            value: ServerValue::ConnectionConversationCapacityExceeded(
                ConnectionConversationCapacityExceeded::SemanticRequest {
                    request: ResponseEnvelope::RecordAdmission(request),
                    limit,
                },
            ),
        }
    }

    /// Ordinary admission required an unreserved `transaction_order` major
    /// and the conversation order is exhausted (register row 5644).
    ///
    /// The payload is minted only by the shared order allocator invoked with
    /// this request's own envelope.
    pub(crate) const fn from_conversation_order_exhausted(
        value: Box<ConversationOrderExhausted>,
    ) -> Self {
        Self {
            value: ServerValue::ConversationOrderExhausted(value),
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

    /// Presented id has a tombstone (register row 5648).
    ///
    /// The payload is minted only by `lookup_binding_required` for this
    /// exact request.
    pub(crate) const fn from_retired(value: Retired) -> Self {
        Self {
            value: ServerValue::Retired(value),
        }
    }

    /// Closure-checked ordinary admission exceeded marker-closure capacity
    /// (register rows 5649, 5686).
    ///
    /// The payload is minted only by the shared remaining-closure selector
    /// invoked with this request's own envelope.
    pub(crate) const fn from_marker_closure_capacity_exceeded(
        value: Box<MarkerClosureCapacityExceeded>,
    ) -> Self {
        Self {
            value: ServerValue::MarkerClosureCapacityExceeded(value),
        }
    }

    /// The ordinary record committed (register row 5685).
    #[must_use]
    pub const fn record_committed(value: RecordCommitted) -> Self {
        Self {
            value: ServerValue::RecordCommitted(value),
        }
    }

    /// The record exceeds the configured entry or byte maximum (register row
    /// 5686).
    #[must_use]
    pub const fn record_too_large(value: RecordTooLarge) -> Self {
        Self {
            value: ServerValue::RecordTooLarge(value),
        }
    }

    /// Canonical resulting sequence-reserve check failed (register row 5686).
    ///
    /// The payload is minted only by the shared sequence allocator invoked
    /// with this request's own envelope.
    pub(crate) const fn from_conversation_sequence_exhausted(
        value: Box<ConversationSequenceExhausted>,
    ) -> Self {
        Self {
            value: ServerValue::ConversationSequenceExhausted(value),
        }
    }

    /// Hard-observer retention refused the ordinary append (register row
    /// 5687).
    ///
    /// The payload is minted only by the shared observer-floor selector
    /// invoked with this request's own envelope.
    pub(crate) const fn from_observer_backpressure(value: ObserverBackpressure) -> Self {
        Self {
            value: ServerValue::ObserverBackpressure(value),
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

/// Server response bound to one observer-recovery handshake batch.
///
/// The register admits exactly four outcomes for the one-shot recovery batch
/// (rows 5642, 5688, 5689); the contract's routing rule (lines 5780-5782)
/// marks all four as already request-specific, so they carry no
/// `originating_request` echo. Every other pairing is a compile error by
/// construction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObserverRecoveryResponse {
    value: ServerValue,
}

impl ObserverRecoveryResponse {
    /// Batch preflight found an untracked conversation that would exceed the
    /// signed connection-conversation limit (register row 5642, wire
    /// `0x0124`).
    #[must_use]
    pub const fn connection_capacity_exceeded(conversation_id: ConversationId, limit: u64) -> Self {
        Self {
            value: ServerValue::ConnectionConversationCapacityExceeded(
                ConnectionConversationCapacityExceeded::ObserverRecovery {
                    conversation_id,
                    limit,
                },
            ),
        }
    }

    /// Whole-batch success with request-ordered statuses (register row 5688).
    #[must_use]
    pub const fn accepted(value: ObserverRecoveryAccepted) -> Self {
        Self {
            value: ServerValue::ObserverRecoveryAccepted(value),
        }
    }

    /// Whole-batch unknown-conversation or ahead-epoch refusal (register row
    /// 5689).
    #[must_use]
    pub const fn invalid_observer_epoch(value: InvalidObserverEpoch) -> Self {
        Self {
            value: ServerValue::InvalidObserverEpoch(value),
        }
    }

    /// Whole-batch list-shape refusal (register row 5689).
    #[must_use]
    pub const fn invalid_observer_epoch_list(value: InvalidObserverEpochList) -> Self {
        Self {
            value: ServerValue::InvalidObserverEpochList(value),
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
