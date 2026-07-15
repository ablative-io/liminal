use crate::outcome::CandidatePhase;
use crate::wire::{
    BindingEpoch, CloseCause, ConversationId, DeliverySeq, DetachedCause, DiedCause, ParticipantId,
    ParticipantIndex, TransactionOrder,
};

/// Stable admission ordering key for one lifecycle candidate.
///
/// The phase is the canonical typed protocol phase rather than a free integer.
/// Binding-terminal transitions below additionally fix it to
/// [`CandidatePhase::BindingTerminal`] and derive the participant index from
/// the participant identifier, which is exactly that permanent index in v1.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct AdmissionOrder {
    transaction_order: TransactionOrder,
    candidate_phase: CandidatePhase,
    participant_index: ParticipantIndex,
}

impl AdmissionOrder {
    /// Creates an order for a typed lifecycle candidate.
    #[must_use]
    pub const fn new(
        transaction_order: TransactionOrder,
        candidate_phase: CandidatePhase,
        participant_index: ParticipantIndex,
    ) -> Self {
        Self {
            transaction_order,
            candidate_phase,
            participant_index,
        }
    }

    /// Creates the only order shape valid for a binding terminal.
    #[must_use]
    pub const fn binding_terminal(
        transaction_order: TransactionOrder,
        participant_id: ParticipantId,
    ) -> Self {
        Self::new(
            transaction_order,
            CandidatePhase::BindingTerminal,
            participant_id,
        )
    }

    /// Returns the conversation transaction-order major.
    #[must_use]
    pub const fn transaction_order(self) -> TransactionOrder {
        self.transaction_order
    }

    /// Returns the canonical candidate phase.
    #[must_use]
    pub const fn candidate_phase(self) -> CandidatePhase {
        self.candidate_phase
    }

    /// Returns the permanent participant-index tie-breaker.
    #[must_use]
    pub const fn participant_index(self) -> ParticipantIndex {
        self.participant_index
    }
}

/// Active binding authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ActiveBinding {
    /// Bound participant; in v1 this value is also its permanent participant index.
    pub participant_id: ParticipantId,
    /// Bound conversation.
    pub conversation_id: ConversationId,
    /// Immutable current binding epoch.
    pub binding_epoch: BindingEpoch,
}

/// A durable binding-terminal record was appended in the fate transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommittedBindingTerminalPosition {
    transaction_order: TransactionOrder,
    delivery_seq: DeliverySeq,
}

impl CommittedBindingTerminalPosition {
    /// Creates the assigned major and delivery key for an appended terminal.
    #[must_use]
    pub const fn new(transaction_order: TransactionOrder, delivery_seq: DeliverySeq) -> Self {
        Self {
            transaction_order,
            delivery_seq,
        }
    }

    /// Returns the assigned conversation transaction order.
    #[must_use]
    pub const fn transaction_order(self) -> TransactionOrder {
        self.transaction_order
    }

    /// Returns the committed terminal record's delivery sequence.
    #[must_use]
    pub const fn delivery_seq(self) -> DeliverySeq {
        self.delivery_seq
    }
}

/// A binding fate was durably accepted but its terminal append remains pending.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PendingBindingTerminalPosition {
    transaction_order: TransactionOrder,
}

impl PendingBindingTerminalPosition {
    /// Creates the immutable major reserved for a pending binding terminal.
    #[must_use]
    pub const fn new(transaction_order: TransactionOrder) -> Self {
        Self { transaction_order }
    }

    /// Returns the assigned conversation transaction order.
    #[must_use]
    pub const fn transaction_order(self) -> TransactionOrder {
        self.transaction_order
    }
}

/// Durable placement selected by an unrefusable binding-fate transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindingTerminalDisposition {
    /// The terminal record committed in the source transaction.
    Committed(CommittedBindingTerminalPosition),
    /// The exact terminal remains in the bounded pending binding slot.
    Pending(PendingBindingTerminalPosition),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BindingTerminalIdentity {
    participant_id: ParticipantId,
    conversation_id: ConversationId,
    binding_epoch: BindingEpoch,
    admission_order: AdmissionOrder,
}

impl BindingTerminalIdentity {
    const fn from_active(binding: ActiveBinding, transaction_order: TransactionOrder) -> Self {
        Self {
            participant_id: binding.participant_id,
            conversation_id: binding.conversation_id,
            binding_epoch: binding.binding_epoch,
            admission_order: AdmissionOrder::binding_terminal(
                transaction_order,
                binding.participant_id,
            ),
        }
    }
}

/// Appended `Detached` terminal with a cause valid for that record class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommittedDetachedTerminal {
    identity: BindingTerminalIdentity,
    cause: DetachedCause,
    delivery_seq: DeliverySeq,
}

impl CommittedDetachedTerminal {
    /// Returns the permanent participant identifier/index.
    #[must_use]
    pub const fn participant_id(self) -> ParticipantId {
        self.identity.participant_id
    }

    /// Returns the owning conversation.
    #[must_use]
    pub const fn conversation_id(self) -> ConversationId {
        self.identity.conversation_id
    }

    /// Returns the exact ended binding epoch.
    #[must_use]
    pub const fn binding_epoch(self) -> BindingEpoch {
        self.identity.binding_epoch
    }

    /// Returns the type-restricted `Detached` cause.
    #[must_use]
    pub const fn cause(self) -> DetachedCause {
        self.cause
    }

    /// Returns the typed binding-terminal admission position.
    #[must_use]
    pub const fn admission_order(self) -> AdmissionOrder {
        self.identity.admission_order
    }

    /// Returns the committed lifecycle delivery sequence.
    #[must_use]
    pub const fn delivery_seq(self) -> DeliverySeq {
        self.delivery_seq
    }
}

/// Appended `Died` terminal with a cause valid for that record class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommittedDiedTerminal {
    identity: BindingTerminalIdentity,
    cause: DiedCause,
    delivery_seq: DeliverySeq,
}

impl CommittedDiedTerminal {
    /// Returns the permanent participant identifier/index.
    #[must_use]
    pub const fn participant_id(self) -> ParticipantId {
        self.identity.participant_id
    }

    /// Returns the owning conversation.
    #[must_use]
    pub const fn conversation_id(self) -> ConversationId {
        self.identity.conversation_id
    }

    /// Returns the exact ended binding epoch.
    #[must_use]
    pub const fn binding_epoch(self) -> BindingEpoch {
        self.identity.binding_epoch
    }

    /// Returns the type-restricted `Died` cause.
    #[must_use]
    pub const fn cause(self) -> DiedCause {
        self.cause
    }

    /// Returns the typed binding-terminal admission position.
    #[must_use]
    pub const fn admission_order(self) -> AdmissionOrder {
        self.identity.admission_order
    }

    /// Returns the committed lifecycle delivery sequence.
    #[must_use]
    pub const fn delivery_seq(self) -> DeliverySeq {
        self.delivery_seq
    }
}

/// Cause-partitioned durable binding-terminal record.
///
/// The variant determines the allowed cause domain, so no independent record
/// kind can disagree with the stored cause.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommittedBindingTerminal {
    /// An appended `Detached` terminal.
    Detached(CommittedDetachedTerminal),
    /// An appended `Died` terminal.
    Died(CommittedDiedTerminal),
}

impl CommittedBindingTerminal {
    /// Returns the permanent participant identifier/index.
    #[must_use]
    pub const fn participant_id(self) -> ParticipantId {
        match self {
            Self::Detached(value) => value.participant_id(),
            Self::Died(value) => value.participant_id(),
        }
    }

    /// Returns the owning conversation.
    #[must_use]
    pub const fn conversation_id(self) -> ConversationId {
        match self {
            Self::Detached(value) => value.conversation_id(),
            Self::Died(value) => value.conversation_id(),
        }
    }

    /// Returns the exact ended binding epoch.
    #[must_use]
    pub const fn binding_epoch(self) -> BindingEpoch {
        match self {
            Self::Detached(value) => value.binding_epoch(),
            Self::Died(value) => value.binding_epoch(),
        }
    }

    /// Returns the typed binding-terminal admission position.
    #[must_use]
    pub const fn admission_order(self) -> AdmissionOrder {
        match self {
            Self::Detached(value) => value.admission_order(),
            Self::Died(value) => value.admission_order(),
        }
    }

    /// Returns the committed lifecycle delivery sequence.
    #[must_use]
    pub const fn delivery_seq(self) -> DeliverySeq {
        match self {
            Self::Detached(value) => value.delivery_seq(),
            Self::Died(value) => value.delivery_seq(),
        }
    }

    /// Returns the record class derived from the cause-partitioned variant.
    #[must_use]
    pub const fn kind(self) -> BindingTerminalKind {
        match self {
            Self::Detached(_) => BindingTerminalKind::Detached,
            Self::Died(_) => BindingTerminalKind::Died,
        }
    }

    /// Returns the shared seven-class close-cause view.
    #[must_use]
    pub const fn close_cause(self) -> CloseCause {
        match self {
            Self::Detached(value) => value.cause().close_cause(),
            Self::Died(value) => value.cause().close_cause(),
        }
    }

    /// Returns the restricted `Detached` cause, or `None` for a `Died` record.
    #[must_use]
    pub const fn detached_cause(self) -> Option<DetachedCause> {
        match self {
            Self::Detached(value) => Some(value.cause()),
            Self::Died(_) => None,
        }
    }

    /// Returns the restricted `Died` cause, or `None` for a `Detached` record.
    #[must_use]
    pub const fn died_cause(self) -> Option<DiedCause> {
        match self {
            Self::Detached(_) => None,
            Self::Died(value) => Some(value.cause()),
        }
    }
}

impl From<CommittedDetachedTerminal> for CommittedBindingTerminal {
    fn from(value: CommittedDetachedTerminal) -> Self {
        Self::Detached(value)
    }
}

impl From<CommittedDiedTerminal> for CommittedBindingTerminal {
    fn from(value: CommittedDiedTerminal) -> Self {
        Self::Died(value)
    }
}

/// Pending `Detached` terminal with no possible `Died`-only cause.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PendingDetachedFinalization {
    identity: BindingTerminalIdentity,
    cause: DetachedCause,
}

impl PendingDetachedFinalization {
    /// Returns the permanent participant identifier/index.
    #[must_use]
    pub const fn participant_id(self) -> ParticipantId {
        self.identity.participant_id
    }

    /// Returns the owning conversation.
    #[must_use]
    pub const fn conversation_id(self) -> ConversationId {
        self.identity.conversation_id
    }

    /// Returns the exact ended binding epoch.
    #[must_use]
    pub const fn binding_epoch(self) -> BindingEpoch {
        self.identity.binding_epoch
    }

    /// Returns the type-restricted `Detached` cause.
    #[must_use]
    pub const fn cause(self) -> DetachedCause {
        self.cause
    }

    /// Returns the immutable binding-terminal admission position.
    #[must_use]
    pub const fn admission_order(self) -> AdmissionOrder {
        self.identity.admission_order
    }

    /// Commits the exact pending terminal at its assigned delivery sequence.
    #[must_use]
    pub const fn commit(self, delivery_seq: DeliverySeq) -> CommittedDetachedTerminal {
        CommittedDetachedTerminal {
            identity: self.identity,
            cause: self.cause,
            delivery_seq,
        }
    }
}

/// Pending `Died` terminal with no possible `Detached`-only cause.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PendingDiedFinalization {
    identity: BindingTerminalIdentity,
    cause: DiedCause,
}

impl PendingDiedFinalization {
    /// Returns the permanent participant identifier/index.
    #[must_use]
    pub const fn participant_id(self) -> ParticipantId {
        self.identity.participant_id
    }

    /// Returns the owning conversation.
    #[must_use]
    pub const fn conversation_id(self) -> ConversationId {
        self.identity.conversation_id
    }

    /// Returns the exact ended binding epoch.
    #[must_use]
    pub const fn binding_epoch(self) -> BindingEpoch {
        self.identity.binding_epoch
    }

    /// Returns the type-restricted `Died` cause.
    #[must_use]
    pub const fn cause(self) -> DiedCause {
        self.cause
    }

    /// Returns the immutable binding-terminal admission position.
    #[must_use]
    pub const fn admission_order(self) -> AdmissionOrder {
        self.identity.admission_order
    }

    /// Commits the exact pending terminal at its assigned delivery sequence.
    #[must_use]
    pub const fn commit(self, delivery_seq: DeliverySeq) -> CommittedDiedTerminal {
        CommittedDiedTerminal {
            identity: self.identity,
            cause: self.cause,
            delivery_seq,
        }
    }
}

/// Binding-terminal lifecycle record kind, derived from cause-partitioned state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindingTerminalKind {
    /// Clean/supersession/shutdown `Detached` record.
    Detached,
    /// Unexpected `Died` record.
    Died,
}

/// Binding authority ended, but its terminal record awaits durable append.
///
/// Both variants have private, transition-derived fields. Consequently a
/// `Detached` finalization cannot carry `ConnectionLost`, `ProcessKilled`,
/// `ProtocolError`, or `UncleanServerRestart`, and a `Died` finalization cannot
/// carry `CleanDeregister`, `Superseded`, or `ServerShutdown`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PendingFinalization {
    /// A pending `Detached` record and its restricted cause.
    Detached(PendingDetachedFinalization),
    /// A pending `Died` record and its restricted cause.
    Died(PendingDiedFinalization),
}

impl PendingFinalization {
    /// Returns the permanent participant identifier/index.
    #[must_use]
    pub const fn participant_id(self) -> ParticipantId {
        match self {
            Self::Detached(value) => value.participant_id(),
            Self::Died(value) => value.participant_id(),
        }
    }

    /// Returns the owning conversation.
    #[must_use]
    pub const fn conversation_id(self) -> ConversationId {
        match self {
            Self::Detached(value) => value.conversation_id(),
            Self::Died(value) => value.conversation_id(),
        }
    }

    /// Returns the exact ended binding epoch.
    #[must_use]
    pub const fn binding_epoch(self) -> BindingEpoch {
        match self {
            Self::Detached(value) => value.binding_epoch(),
            Self::Died(value) => value.binding_epoch(),
        }
    }

    /// Returns the immutable binding-terminal admission position.
    #[must_use]
    pub const fn admission_order(self) -> AdmissionOrder {
        match self {
            Self::Detached(value) => value.admission_order(),
            Self::Died(value) => value.admission_order(),
        }
    }

    /// Returns the record class derived from the cause-partitioned variant.
    #[must_use]
    pub const fn kind(self) -> BindingTerminalKind {
        match self {
            Self::Detached(_) => BindingTerminalKind::Detached,
            Self::Died(_) => BindingTerminalKind::Died,
        }
    }

    /// Returns the shared seven-class close-cause view.
    #[must_use]
    pub const fn close_cause(self) -> CloseCause {
        match self {
            Self::Detached(value) => value.cause().close_cause(),
            Self::Died(value) => value.cause().close_cause(),
        }
    }

    /// Returns the restricted `Detached` cause, or `None` for a `Died` state.
    #[must_use]
    pub const fn detached_cause(self) -> Option<DetachedCause> {
        match self {
            Self::Detached(value) => Some(value.cause()),
            Self::Died(_) => None,
        }
    }

    /// Returns the restricted `Died` cause, or `None` for a `Detached` state.
    #[must_use]
    pub const fn died_cause(self) -> Option<DiedCause> {
        match self {
            Self::Detached(_) => None,
            Self::Died(value) => Some(value.cause()),
        }
    }

    /// Commits the exact pending terminal without changing cause, epoch, or order.
    #[must_use]
    pub const fn commit(self, delivery_seq: DeliverySeq) -> CommittedBindingTerminal {
        match self {
            Self::Detached(value) => CommittedBindingTerminal::Detached(value.commit(delivery_seq)),
            Self::Died(value) => CommittedBindingTerminal::Died(value.commit(delivery_seq)),
        }
    }
}

impl From<PendingDetachedFinalization> for PendingFinalization {
    fn from(value: PendingDetachedFinalization) -> Self {
        Self::Detached(value)
    }
}

impl From<PendingDiedFinalization> for PendingFinalization {
    fn from(value: PendingDiedFinalization) -> Self {
        Self::Died(value)
    }
}

/// Result of a fate that records a `Detached` lifecycle terminal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DetachedBindingTransition {
    /// Terminal record committed with its durable delivery key.
    Committed(CommittedDetachedTerminal),
    /// Authority ended and the exact terminal remains bounded-pending.
    Pending(PendingDetachedFinalization),
}

impl DetachedBindingTransition {
    /// Returns the post-transition binding slot.
    #[must_use]
    pub const fn binding_state(self) -> BindingState {
        match self {
            Self::Committed(_) => BindingState::Detached,
            Self::Pending(value) => {
                BindingState::PendingFinalization(PendingFinalization::Detached(value))
            }
        }
    }
}

/// Result of a fate that records a `Died` lifecycle terminal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiedBindingTransition {
    /// Terminal record committed with its durable delivery key.
    Committed(CommittedDiedTerminal),
    /// Authority ended and the exact terminal remains bounded-pending.
    Pending(PendingDiedFinalization),
}

impl DiedBindingTransition {
    /// Returns the post-transition binding slot.
    #[must_use]
    pub const fn binding_state(self) -> BindingState {
        match self {
            Self::Committed(_) => BindingState::Detached,
            Self::Pending(value) => {
                BindingState::PendingFinalization(PendingFinalization::Died(value))
            }
        }
    }
}

impl ActiveBinding {
    const fn finish_detached(
        self,
        cause: DetachedCause,
        disposition: BindingTerminalDisposition,
    ) -> DetachedBindingTransition {
        match disposition {
            BindingTerminalDisposition::Committed(position) => {
                DetachedBindingTransition::Committed(CommittedDetachedTerminal {
                    identity: BindingTerminalIdentity::from_active(
                        self,
                        position.transaction_order,
                    ),
                    cause,
                    delivery_seq: position.delivery_seq,
                })
            }
            BindingTerminalDisposition::Pending(position) => {
                DetachedBindingTransition::Pending(PendingDetachedFinalization {
                    identity: BindingTerminalIdentity::from_active(
                        self,
                        position.transaction_order,
                    ),
                    cause,
                })
            }
        }
    }

    const fn finish_died(
        self,
        cause: DiedCause,
        disposition: BindingTerminalDisposition,
    ) -> DiedBindingTransition {
        match disposition {
            BindingTerminalDisposition::Committed(position) => {
                DiedBindingTransition::Committed(CommittedDiedTerminal {
                    identity: BindingTerminalIdentity::from_active(
                        self,
                        position.transaction_order,
                    ),
                    cause,
                    delivery_seq: position.delivery_seq,
                })
            }
            BindingTerminalDisposition::Pending(position) => {
                DiedBindingTransition::Pending(PendingDiedFinalization {
                    identity: BindingTerminalIdentity::from_active(
                        self,
                        position.transaction_order,
                    ),
                    cause,
                })
            }
        }
    }

    /// Ends authority for explicit detach or clean protocol Disconnect.
    #[must_use]
    pub const fn clean_deregister(
        self,
        disposition: BindingTerminalDisposition,
    ) -> DetachedBindingTransition {
        self.finish_detached(DetachedCause::CleanDeregister, disposition)
    }

    /// Commits an explicit detach terminal at its assigned durable position.
    #[must_use]
    pub const fn commit_clean_deregister(
        self,
        position: CommittedBindingTerminalPosition,
    ) -> CommittedDetachedTerminal {
        CommittedDetachedTerminal {
            identity: BindingTerminalIdentity::from_active(self, position.transaction_order),
            cause: DetachedCause::CleanDeregister,
            delivery_seq: position.delivery_seq,
        }
    }

    /// Ends explicit-detach authority while retaining its exact pending terminal.
    #[must_use]
    pub const fn pending_clean_deregister(
        self,
        position: PendingBindingTerminalPosition,
    ) -> PendingDetachedFinalization {
        PendingDetachedFinalization {
            identity: BindingTerminalIdentity::from_active(self, position.transaction_order),
            cause: DetachedCause::CleanDeregister,
        }
    }

    /// Ends every binding still active when a clean protocol Disconnect arrives.
    #[must_use]
    pub const fn clean_disconnect(
        self,
        disposition: BindingTerminalDisposition,
    ) -> DetachedBindingTransition {
        self.clean_deregister(disposition)
    }

    /// Commits the old terminal in an authorized superseding attach handoff.
    ///
    /// Supersession is an optional new-major producer and cannot leave only the
    /// old terminal pending; the enclosing attach transaction either appends
    /// the ordered `Detached(Superseded)`/`Attached` pair or commits nothing.
    #[must_use]
    pub const fn superseded(
        self,
        position: CommittedBindingTerminalPosition,
    ) -> CommittedDetachedTerminal {
        CommittedDetachedTerminal {
            identity: BindingTerminalIdentity::from_active(self, position.transaction_order),
            cause: DetachedCause::Superseded,
            delivery_seq: position.delivery_seq,
        }
    }

    /// Ends authority during an orderly server shutdown.
    #[must_use]
    pub const fn server_shutdown(
        self,
        disposition: BindingTerminalDisposition,
    ) -> DetachedBindingTransition {
        self.finish_detached(DetachedCause::ServerShutdown, disposition)
    }

    /// Ends authority after TCP/keepalive/read/write connection loss.
    #[must_use]
    pub const fn connection_lost(
        self,
        disposition: BindingTerminalDisposition,
    ) -> DiedBindingTransition {
        self.finish_died(DiedCause::ConnectionLost, disposition)
    }

    /// Ends authority after trapped linked-EXIT or known forced termination.
    #[must_use]
    pub const fn process_killed(
        self,
        disposition: BindingTerminalDisposition,
    ) -> DiedBindingTransition {
        self.finish_died(DiedCause::ProcessKilled, disposition)
    }

    /// Ends authority after a terminating decode or protocol-state refusal.
    #[must_use]
    pub const fn protocol_error(
        self,
        disposition: BindingTerminalDisposition,
    ) -> DiedBindingTransition {
        self.finish_died(DiedCause::ProtocolError, disposition)
    }

    /// Recovers a durably active epoch owned by a prior server incarnation.
    ///
    /// The prior incarnation in the `Died` cause is derived from the exact old
    /// binding epoch, so callers cannot pair the state with a different value.
    #[must_use]
    pub const fn unclean_server_restart(
        self,
        disposition: BindingTerminalDisposition,
    ) -> DiedBindingTransition {
        self.finish_died(
            DiedCause::UncleanServerRestart {
                prior_server_incarnation: self
                    .binding_epoch
                    .connection_incarnation
                    .server_incarnation,
            },
            disposition,
        )
    }
}

/// Binding-slot state; pending finalization carries no live authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindingState {
    /// No binding or pending terminal exists.
    Detached,
    /// Current live binding authority.
    Bound(ActiveBinding),
    /// Authority ended and a cause-partitioned terminal record is pending.
    PendingFinalization(PendingFinalization),
}
