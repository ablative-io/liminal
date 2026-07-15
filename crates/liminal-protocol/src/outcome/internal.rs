use crate::lifecycle::AdmissionOrder;
use crate::wire::{
    BindingEpoch, CloseCause, ConversationId, DeliverySeq, ParticipantId, ParticipantIndex,
    RepaymentEdge, TransactionOrder,
};

/// The only close cause admitted by startup binding recovery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UncleanServerRestartCause {
    /// Server incarnation that previously owned the recovered binding.
    pub prior_server_incarnation: u64,
}

impl UncleanServerRestartCause {
    /// Returns the corresponding shared close-cause value.
    #[must_use]
    pub const fn as_close_cause(self) -> CloseCause {
        CloseCause::UncleanServerRestart {
            prior_server_incarnation: self.prior_server_incarnation,
        }
    }
}

/// Durable finalization selected by startup binding recovery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindingRecoveryFinalization {
    /// Binding terminal was appended in the recovery transaction.
    Appended {
        /// Assigned durable delivery sequence.
        delivery_seq: DeliverySeq,
    },
    /// Capacity-backed terminal remains pending for later append.
    Pending {
        /// Immutable ordering key retained by pending finalization.
        admission_order: AdmissionOrder,
    },
}

/// Internal durable success from server-startup binding recovery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BindingRecoveryCommitted {
    /// Participant whose dead binding was recovered.
    pub participant_id: ParticipantId,
    /// Conversation that owned the recovered binding.
    pub conversation_id: ConversationId,
    /// Exact old binding epoch terminalized by recovery.
    pub recovered_binding_epoch: BindingEpoch,
    /// Exact unclean-restart cause and prior incarnation.
    pub cause: UncleanServerRestartCause,
    /// Assigned conversation transaction-order major.
    pub assigned_transaction_order: TransactionOrder,
    /// Appended or bounded-pending terminalization.
    pub finalization: BindingRecoveryFinalization,
    /// Exact stored closure-debt repayment edge after recovery.
    pub repayment_edge: RepaymentEdge,
}

/// Canonical candidate phase in transaction ordering.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum CandidatePhase {
    /// Pending or direct binding terminal.
    BindingTerminal = 0,
    /// Membership exit record.
    MembershipExit = 1,
    /// Attached lifecycle record.
    AttachLifecycle = 2,
    /// Admitted ordinary application record.
    OrdinaryRecord = 3,
    /// Induced history-compaction marker.
    CompactionMarker = 4,
}

/// Counter whose movable-claim frontier is invalid.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimCounter {
    /// Conversation delivery-sequence frontier.
    DeliverySeq,
    /// Conversation transaction-order frontier.
    TransactionOrder,
}

/// Exact retained-state corruption reasons that remain after Fix 2.
///
/// `docs/design/LP-EXTRACTION-GOAL.md` Fix 2 removes fixed occurrence slots,
/// so occurrence-array placement, churn-block, duplicate-successor, and
/// unbacked-occurrence reasons are intentionally not constructible here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantStateCorruptReason {
    /// Two live candidates share one complete ordering key.
    DuplicateCandidateKey {
        /// Candidate transaction-order major.
        transaction_order: TransactionOrder,
        /// Candidate phase within the major.
        candidate_phase: CandidatePhase,
        /// Permanent participant-index tie-breaker.
        participant_index: ParticipantIndex,
    },
    /// Numeric or logical movable-claim frontier is malformed.
    ClaimFrontierInvalid {
        /// First counter in fixed validation order that failed.
        counter: ClaimCounter,
        /// Deterministic checked-u128 position of the first fault.
        first_bad_position: u128,
    },
}

/// Candidate validation or startup decoding found corrupt participant state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParticipantStateCorrupt {
    /// Conversation whose durable bytes remain preserved and fail closed.
    pub conversation_id: ConversationId,
    /// First exact corruption reason in validation order.
    pub reason: ParticipantStateCorruptReason,
}
