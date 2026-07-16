//! Response authority bound to `ClientRequest::Enrollment` (`0x0001`).

use alloc::boxed::Box;

use super::super::{
    ConnectionConversationBindingOccupied, ConnectionConversationCapacityExceeded,
    ConversationOrderExhausted, ConversationSequenceExhausted, EnrollBound, EnrollmentEnvelope,
    EnrollmentKnown, EnrollmentReceiptCapacityScope, IdentityCapacityExceeded,
    MarkerClosureCapacityExceeded, ObserverBackpressure, ObserverBackpressureState,
    ReceiptCapacityExceeded, ReceiptExpired, ReceiptReplay, ResponseEnvelope, Retired,
    ServerDiscriminant, ServerValue,
};

/// Server response bound to one enrollment request.
///
/// Constructors exist only for the outcomes the frozen R-D1 register admits
/// for enrollment; every other pairing is a compile error by construction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnrollmentResponse {
    value: ServerValue,
}

impl EnrollmentResponse {
    /// First decoded semantic operation for an untracked conversation
    /// exceeded the connection-conversation limit (register row 5641).
    #[must_use]
    pub const fn connection_conversation_capacity_exceeded(
        request: EnrollmentEnvelope,
        limit: u64,
    ) -> Self {
        Self {
            value: ServerValue::ConnectionConversationCapacityExceeded(
                ConnectionConversationCapacityExceeded::SemanticRequest {
                    request: ResponseEnvelope::Enrollment(request),
                    limit,
                },
            ),
        }
    }

    /// Enrollment binding attempt found an occupied connection/conversation
    /// slot (register row 5643).
    #[must_use]
    pub const fn connection_conversation_binding_occupied(request: &EnrollmentEnvelope) -> Self {
        Self {
            value: ServerValue::ConnectionConversationBindingOccupied(
                ConnectionConversationBindingOccupied::Enrollment {
                    conversation_id: request.conversation_id,
                    enrollment_token: request.enrollment_token,
                },
            ),
        }
    }

    /// Enrollment required an unreserved `transaction_order` major and the
    /// conversation order is exhausted (register row 5644).
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

    /// Enrollment token mapping resolved to a tombstone (register row 5653).
    ///
    /// The payload is minted only by `lookup_enrollment` for this exact
    /// request.
    pub(crate) const fn from_retired(value: Retired) -> Self {
        Self {
            value: ServerValue::Retired(value),
        }
    }

    /// Closure-checked enrollment admission exceeded marker-closure capacity
    /// (register row 5649).
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

    /// Successful enrollment attach (register row 5650).
    #[must_use]
    pub const fn enroll_bound(value: EnrollBound) -> Self {
        Self {
            value: ServerValue::EnrollBound(value),
        }
    }

    /// Post-provenance replay for a live non-retired mapped identity
    /// (register row 5651).
    #[must_use]
    pub const fn enrollment_known(value: EnrollmentKnown) -> Self {
        Self {
            value: ServerValue::EnrollmentKnown(value),
        }
    }

    /// Exact enrollment provenance window response (register row 5652).
    ///
    /// The payload is minted only by `lookup_enrollment` for this exact
    /// request.
    pub(crate) const fn from_receipt_expired(value: ReceiptExpired) -> Self {
        Self {
            value: ServerValue::ReceiptExpired(value),
        }
    }

    /// One of the three receipt/provenance scopes reachable before identity
    /// mint is full (register row 5654).
    #[must_use]
    pub const fn receipt_capacity_exceeded(
        request: EnrollmentEnvelope,
        scope: EnrollmentReceiptCapacityScope,
        limit: u64,
        occupied: u64,
    ) -> Self {
        Self {
            value: ServerValue::ReceiptCapacityExceeded(ReceiptCapacityExceeded::Enrollment {
                request,
                scope,
                limit,
                occupied,
            }),
        }
    }

    /// Server or conversation identity capacity is full (register row 5655).
    #[must_use]
    pub const fn identity_capacity_exceeded(value: IdentityCapacityExceeded) -> Self {
        Self {
            value: ServerValue::IdentityCapacityExceeded(value),
        }
    }

    /// Hard-observer retention refused the enrollment append (register row
    /// 5656).
    #[must_use]
    pub const fn observer_backpressure(
        request: EnrollmentEnvelope,
        state: ObserverBackpressureState,
    ) -> Self {
        Self {
            value: ServerValue::ObserverBackpressure(ObserverBackpressure::Enrollment {
                request,
                state,
            }),
        }
    }

    /// Hard-observer retention refusal minted by the shared observer-floor
    /// selector invoked with this request's own envelope (register row 5656).
    pub(crate) const fn from_observer_backpressure(value: ObserverBackpressure) -> Self {
        Self {
            value: ServerValue::ObserverBackpressure(value),
        }
    }

    /// Canonical resulting sequence-reserve check failed (register row 5657).
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

    /// Byte-identical receipt replay whose exact binding epoch still occupies
    /// its origin slot (register row 5663).
    #[must_use]
    pub const fn bound(value: EnrollBound) -> Self {
        Self {
            value: ServerValue::Bound(ReceiptReplay::Enrollment(value)),
        }
    }

    /// Byte-identical receipt replay whose origin slot is empty, replaced, or
    /// at a later epoch (register row 5663).
    #[must_use]
    pub const fn unbound_receipt(value: EnrollBound) -> Self {
        Self {
            value: ServerValue::UnboundReceipt(ReceiptReplay::Enrollment(value)),
        }
    }

    /// Byte-identical live-receipt replay minted by `lookup_enrollment`
    /// (register row 5663).
    pub(crate) const fn from_bound(value: ReceiptReplay) -> Self {
        Self {
            value: ServerValue::Bound(value),
        }
    }

    /// Byte-identical unbound-receipt replay minted by `lookup_enrollment`
    /// (register row 5663).
    pub(crate) const fn from_unbound_receipt(value: ReceiptReplay) -> Self {
        Self {
            value: ServerValue::UnboundReceipt(value),
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
