//! Response authorities bound to `ClientRequest::Detach` (`0x0003`) and
//! `ClientRequest::Leave` (`0x0005`).

use super::super::{
    AttemptTokenBodyConflict, BindingEpoch, BindingRequiredEnvelope, ClosureCheckedEnvelope,
    ClosureRefusalReason, ClosureSnapshot, ConnectionConversationCapacityExceeded, ConversationId,
    DetachCommitted, DetachEnvelope, DetachInProgress, DetachStaleAuthority, Generation,
    LeaveAttemptToken, LeaveCommitted, LeaveEnvelope, LeaveStaleAuthority,
    MarkerClosureCapacityExceeded, NoBinding, ObserverBackpressure, ObserverBackpressureState,
    ParticipantId, ParticipantReferenceEnvelope, ParticipantUnknown, ResponseEnvelope, Retired,
    ServerDiscriminant, ServerValue, StaleAuthority,
};

use alloc::boxed::Box;

/// Server response bound to one explicit detach request.
///
/// Constructors exist only for the outcomes the frozen R-D1 register admits
/// for detach; every other pairing is a compile error by construction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DetachResponse {
    value: ServerValue,
}

impl DetachResponse {
    /// First decoded semantic operation for an untracked conversation
    /// exceeded the connection-conversation limit (register row 5641).
    #[must_use]
    pub const fn connection_conversation_capacity_exceeded(
        request: DetachEnvelope,
        limit: u64,
    ) -> Self {
        Self {
            value: ServerValue::ConnectionConversationCapacityExceeded(
                ConnectionConversationCapacityExceeded::SemanticRequest {
                    request: ResponseEnvelope::Detach(request),
                    limit,
                },
            ),
        }
    }

    /// Presented participant is unknown (register row 5645).
    #[must_use]
    pub const fn participant_unknown(request: DetachEnvelope) -> Self {
        Self {
            value: ServerValue::ParticipantUnknown(ParticipantUnknown {
                request: ParticipantReferenceEnvelope::Detach(request),
            }),
        }
    }

    /// New detach with no Pending cell found no current binding (register
    /// row 5646).
    #[must_use]
    pub const fn no_binding(request: DetachEnvelope) -> Self {
        Self {
            value: ServerValue::NoBinding(NoBinding {
                request: BindingRequiredEnvelope::Detach(request),
            }),
        }
    }

    /// Detach-specific stale authority: live mismatch or a verified exact old
    /// token resolved to a terminalized detach cell (register rows 5647,
    /// 5671).
    #[must_use]
    pub const fn stale_authority(value: DetachStaleAuthority) -> Self {
        Self {
            value: ServerValue::StaleAuthority(StaleAuthority::Detach(value)),
        }
    }

    /// Presented id has a tombstone after Leave (register rows 5648, 5672).
    #[must_use]
    pub const fn retired(request: DetachEnvelope, retired_generation: Generation) -> Self {
        Self {
            value: ServerValue::Retired(Retired::Participant {
                request: ParticipantReferenceEnvelope::Detach(request),
                retired_generation,
            }),
        }
    }

    /// Stable committed detach result (register row 5668).
    #[must_use]
    pub const fn detach_committed(value: DetachCommitted) -> Self {
        Self {
            value: ServerValue::DetachCommitted(value),
        }
    }

    /// A different detach token encountered an existing Pending cell
    /// (register row 5670).
    #[must_use]
    pub const fn detach_in_progress(value: DetachInProgress) -> Self {
        Self {
            value: ServerValue::DetachInProgress(value),
        }
    }

    /// Detach append is blocked or an exact-token Pending replay returned its
    /// current cell epoch (register rows 5669, 5673).
    #[must_use]
    pub const fn observer_backpressure(
        request: DetachEnvelope,
        committed_binding_epoch: BindingEpoch,
        state: ObserverBackpressureState,
    ) -> Self {
        Self {
            value: ServerValue::ObserverBackpressure(ObserverBackpressure::Detach {
                request,
                committed_binding_epoch,
                state,
            }),
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

/// Server response bound to one terminal Leave request.
///
/// Constructors exist only for the outcomes the frozen R-D1 register admits
/// for Leave; every other pairing is a compile error by construction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaveResponse {
    value: ServerValue,
}

impl LeaveResponse {
    /// Exact committed Leave token with verified secret but a changed
    /// canonical non-secret body; Leave can select only the generation
    /// conflict (register row 5639).
    #[must_use]
    pub const fn attempt_token_body_conflict(
        token: LeaveAttemptToken,
        conversation_id: ConversationId,
        presented_participant_id: ParticipantId,
        presented_generation: Generation,
    ) -> Self {
        Self {
            value: ServerValue::AttemptTokenBodyConflict(AttemptTokenBodyConflict::Leave {
                token,
                conversation_id,
                presented_participant_id,
                presented_generation,
            }),
        }
    }

    /// First decoded semantic operation for an untracked conversation
    /// exceeded the connection-conversation limit (register row 5641).
    #[must_use]
    pub const fn connection_conversation_capacity_exceeded(
        request: LeaveEnvelope,
        limit: u64,
    ) -> Self {
        Self {
            value: ServerValue::ConnectionConversationCapacityExceeded(
                ConnectionConversationCapacityExceeded::SemanticRequest {
                    request: ResponseEnvelope::Leave(request),
                    limit,
                },
            ),
        }
    }

    /// Presented participant is unknown (register row 5645).
    #[must_use]
    pub const fn participant_unknown(request: LeaveEnvelope) -> Self {
        Self {
            value: ServerValue::ParticipantUnknown(ParticipantUnknown {
                request: ParticipantReferenceEnvelope::Leave(request),
            }),
        }
    }

    /// Leave while a different live binding epoch exists (register row 5646).
    #[must_use]
    pub const fn no_binding(request: LeaveEnvelope) -> Self {
        Self {
            value: ServerValue::NoBinding(NoBinding {
                request: BindingRequiredEnvelope::Leave(request),
            }),
        }
    }

    /// Leave-specific stale authority: live mismatch or the exact committed
    /// Leave token with a wrong secret (register rows 5647, 5680).
    #[must_use]
    pub const fn stale_authority(value: LeaveStaleAuthority) -> Self {
        Self {
            value: ServerValue::StaleAuthority(StaleAuthority::Leave(value)),
        }
    }

    /// Presented id has a tombstone under a different token (register rows
    /// 5648, 5680).
    #[must_use]
    pub const fn retired(request: LeaveEnvelope, retired_generation: Generation) -> Self {
        Self {
            value: ServerValue::Retired(Retired::Participant {
                request: ParticipantReferenceEnvelope::Leave(request),
                retired_generation,
            }),
        }
    }

    /// Closure-checked Leave admission exceeded marker-closure capacity
    /// (register row 5649).
    #[must_use]
    pub fn marker_closure_capacity_exceeded(
        request: LeaveEnvelope,
        snapshot: ClosureSnapshot,
        reason: ClosureRefusalReason,
    ) -> Self {
        Self {
            value: ServerValue::MarkerClosureCapacityExceeded(Box::new(
                MarkerClosureCapacityExceeded {
                    request: ClosureCheckedEnvelope::Leave(request),
                    snapshot,
                    reason,
                },
            )),
        }
    }

    /// Terminal Leave success for the bound or detached exact-secret arms
    /// (register rows 5678, 5679).
    #[must_use]
    pub const fn leave_committed(value: LeaveCommitted) -> Self {
        Self {
            value: ServerValue::LeaveCommitted(value),
        }
    }

    /// Hard-observer retention refused the Leave append (register row 5681).
    #[must_use]
    pub const fn observer_backpressure(
        request: LeaveEnvelope,
        state: ObserverBackpressureState,
        prior_terminal_cell_exists: bool,
    ) -> Self {
        Self {
            value: ServerValue::ObserverBackpressure(ObserverBackpressure::Leave {
                request,
                state,
                prior_terminal_cell_exists,
            }),
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
