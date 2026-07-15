use crate::wire::{BindingEpoch, CloseCause, ConversationId, ParticipantId, ParticipantIndex};

/// Stable admission ordering key for one lifecycle candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct AdmissionOrder {
    /// Conversation transaction order.
    pub transaction_order: u64,
    /// Candidate phase within that transaction.
    pub candidate_phase: u8,
    /// Permanent participant index tie-breaker.
    pub participant_index: ParticipantIndex,
}

/// Active binding authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ActiveBinding {
    /// Bound participant.
    pub participant_id: ParticipantId,
    /// Bound conversation.
    pub conversation_id: ConversationId,
    /// Immutable current binding epoch.
    pub binding_epoch: BindingEpoch,
}

/// Binding-terminal lifecycle record kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindingTerminalKind {
    /// Clean/supersession/shutdown Detached record.
    Detached,
    /// Unexpected Died record.
    Died,
}

/// Binding authority ended, but its terminal record awaits durable append.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PendingFinalization {
    /// Participant whose authority has already ended.
    pub participant_id: ParticipantId,
    /// Conversation whose binding slot is finalizing.
    pub conversation_id: ConversationId,
    /// Ended binding epoch.
    pub binding_epoch: BindingEpoch,
    /// Original close cause retained across backpressure/restart.
    pub original_cause: CloseCause,
    /// Detached or Died record class.
    pub event_kind: BindingTerminalKind,
    /// Immutable admission ordering key.
    pub admission_order: AdmissionOrder,
}

/// Binding-slot state; pending finalization carries no live authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindingState {
    /// No binding or pending terminal exists.
    Detached,
    /// Current live binding authority.
    Bound(ActiveBinding),
    /// Authority ended and a terminal record is pending.
    PendingFinalization(PendingFinalization),
}
