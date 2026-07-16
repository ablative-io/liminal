//! Public durable-fact vocabulary for the six lifecycle operations recorded
//! by the participant-conversation shell.
//!
//! Each operation payload is constructible only from the typed commit values
//! the crate itself produces (`docs/design/LP-GAP-CLOSURE-GOAL.md` item A1), so
//! a storage binding cannot mint a lifecycle event from raw caller-authored
//! values. Decoded events rebuild these payloads through crate-private
//! constructors that re-validate every canonical field invariant.

use crate::wire::{
    BindingEpoch, ConversationId, DeliverySeq, DetachAttemptToken, DetachedCause, Generation,
    LeaveCommitted, ParticipantId, TransactionOrder,
};

use super::{
    AttachedLifecycleRecord, CommittedDetachedTerminal, NonzeroParticipantAckCommit,
    OrdinaryBindingFate, RecoveredBindingFate,
};

/// Durable facts of one committed enrollment, as recorded by the shell.
///
/// The only public producer consumes the crate's own enrollment commit output
/// ([`AttachedLifecycleRecord`] is not publicly constructible), so an event can
/// exist only downstream of [`super::commit_enrollment`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EnrolledOperation {
    conversation_id: ConversationId,
    participant_id: ParticipantId,
    binding_epoch: BindingEpoch,
    attached_transaction_order: TransactionOrder,
    attached_delivery_seq: DeliverySeq,
}

impl EnrolledOperation {
    /// Creates the enrollment event body from the commit's `Attached` record.
    ///
    /// Returns `None` unless the record carries the mandatory generation-one
    /// origin epoch (the invariant [`super::commit_enrollment`] enforces).
    #[must_use]
    pub fn new(attached: AttachedLifecycleRecord) -> Option<Self> {
        Self::from_decoded(
            attached.conversation_id(),
            attached.participant_id(),
            attached.binding_epoch(),
            attached.admission_order().transaction_order(),
            attached.delivery_seq(),
        )
    }

    pub(super) fn from_decoded(
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
        attached_transaction_order: TransactionOrder,
        attached_delivery_seq: DeliverySeq,
    ) -> Option<Self> {
        if binding_epoch.capability_generation != Generation::ONE {
            return None;
        }
        Some(Self {
            conversation_id,
            participant_id,
            binding_epoch,
            attached_transaction_order,
            attached_delivery_seq,
        })
    }

    /// Returns the conversation that enrolled the participant.
    #[must_use]
    pub const fn conversation_id(self) -> ConversationId {
        self.conversation_id
    }

    /// Returns the permanent participant identifier/index.
    #[must_use]
    pub const fn participant_id(self) -> ParticipantId {
        self.participant_id
    }

    /// Returns the generation-one origin binding epoch.
    #[must_use]
    pub const fn binding_epoch(self) -> BindingEpoch {
        self.binding_epoch
    }

    /// Returns the `Attached` record's immutable transaction-order major.
    #[must_use]
    pub const fn attached_transaction_order(self) -> TransactionOrder {
        self.attached_transaction_order
    }

    /// Returns the `Attached` record's committed delivery sequence.
    #[must_use]
    pub const fn attached_delivery_seq(self) -> DeliverySeq {
        self.attached_delivery_seq
    }
}

/// Durable facts of one committed credential attach, as recorded by the shell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AttachedOperation {
    conversation_id: ConversationId,
    participant_id: ParticipantId,
    binding_epoch: BindingEpoch,
    attached_transaction_order: TransactionOrder,
    attached_delivery_seq: DeliverySeq,
}

impl AttachedOperation {
    /// Creates the attach event body from the commit's `Attached` record.
    #[must_use]
    pub const fn new(attached: AttachedLifecycleRecord) -> Self {
        Self {
            conversation_id: attached.conversation_id(),
            participant_id: attached.participant_id(),
            binding_epoch: attached.binding_epoch(),
            attached_transaction_order: attached.admission_order().transaction_order(),
            attached_delivery_seq: attached.delivery_seq(),
        }
    }

    pub(super) const fn from_decoded(
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
        attached_transaction_order: TransactionOrder,
        attached_delivery_seq: DeliverySeq,
    ) -> Self {
        Self {
            conversation_id,
            participant_id,
            binding_epoch,
            attached_transaction_order,
            attached_delivery_seq,
        }
    }

    /// Returns the conversation whose participant attached.
    #[must_use]
    pub const fn conversation_id(self) -> ConversationId {
        self.conversation_id
    }

    /// Returns the permanent participant identifier/index.
    #[must_use]
    pub const fn participant_id(self) -> ParticipantId {
        self.participant_id
    }

    /// Returns the newly committed authoritative binding epoch.
    #[must_use]
    pub const fn binding_epoch(self) -> BindingEpoch {
        self.binding_epoch
    }

    /// Returns the `Attached` record's immutable transaction-order major.
    #[must_use]
    pub const fn attached_transaction_order(self) -> TransactionOrder {
        self.attached_transaction_order
    }

    /// Returns the `Attached` record's committed delivery sequence.
    #[must_use]
    pub const fn attached_delivery_seq(self) -> DeliverySeq {
        self.attached_delivery_seq
    }
}

/// Durable facts of one committed clean detach, as recorded by the shell.
///
/// The atomic detach transaction (terminal append, floor transition, cell
/// replacement, and binding release) is summarized by its committed
/// `Detached(CleanDeregister)` terminal plus the replayable attempt token.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DetachedOperation {
    detach_attempt_token: DetachAttemptToken,
    conversation_id: ConversationId,
    participant_id: ParticipantId,
    committed_binding_epoch: BindingEpoch,
    detached_transaction_order: TransactionOrder,
    detached_delivery_seq: DeliverySeq,
}

impl DetachedOperation {
    /// Creates the detach event body from the exact committed terminal.
    ///
    /// Returns `None` unless the terminal's cause is `CleanDeregister`:
    /// supersession terminals belong to the attach event that committed them,
    /// never to a standalone detach event.
    #[must_use]
    pub fn new(
        terminal: CommittedDetachedTerminal,
        detach_attempt_token: DetachAttemptToken,
    ) -> Option<Self> {
        if terminal.cause() != DetachedCause::CleanDeregister {
            return None;
        }
        Some(Self {
            detach_attempt_token,
            conversation_id: terminal.conversation_id(),
            participant_id: terminal.participant_id(),
            committed_binding_epoch: terminal.binding_epoch(),
            detached_transaction_order: terminal.admission_order().transaction_order(),
            detached_delivery_seq: terminal.delivery_seq(),
        })
    }

    pub(super) const fn from_decoded(
        detach_attempt_token: DetachAttemptToken,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        committed_binding_epoch: BindingEpoch,
        detached_transaction_order: TransactionOrder,
        detached_delivery_seq: DeliverySeq,
    ) -> Self {
        Self {
            detach_attempt_token,
            conversation_id,
            participant_id,
            committed_binding_epoch,
            detached_transaction_order,
            detached_delivery_seq,
        }
    }

    /// Returns the stable detach attempt token retained for replay.
    #[must_use]
    pub const fn detach_attempt_token(self) -> DetachAttemptToken {
        self.detach_attempt_token
    }

    /// Returns the conversation whose participant detached.
    #[must_use]
    pub const fn conversation_id(self) -> ConversationId {
        self.conversation_id
    }

    /// Returns the permanent participant identifier/index.
    #[must_use]
    pub const fn participant_id(self) -> ParticipantId {
        self.participant_id
    }

    /// Returns the binding epoch ended by the detach.
    #[must_use]
    pub const fn committed_binding_epoch(self) -> BindingEpoch {
        self.committed_binding_epoch
    }

    /// Returns the terminal's immutable transaction-order major.
    #[must_use]
    pub const fn detached_transaction_order(self) -> TransactionOrder {
        self.detached_transaction_order
    }

    /// Returns the committed `Detached` record's delivery sequence.
    #[must_use]
    pub const fn detached_delivery_seq(self) -> DeliverySeq {
        self.detached_delivery_seq
    }
}

/// Durable facts of one permanent Leave, as recorded by the shell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeftOperation {
    committed: LeaveCommitted,
    left_transaction_order: TransactionOrder,
}

impl LeftOperation {
    /// Creates the leave event body from the canonical permanent result.
    ///
    /// [`LeaveCommitted`] validates the epoch-generation pairing and terminal
    /// ordering at its own construction, so no invariant is re-stated here.
    #[must_use]
    pub const fn new(committed: LeaveCommitted, left_transaction_order: TransactionOrder) -> Self {
        Self {
            committed,
            left_transaction_order,
        }
    }

    /// Borrows the canonical permanent Leave result.
    #[must_use]
    pub const fn committed(&self) -> &LeaveCommitted {
        &self.committed
    }

    /// Returns the immutable transaction-order major of the `Left` record.
    #[must_use]
    pub const fn left_transaction_order(&self) -> TransactionOrder {
        self.left_transaction_order
    }
}

/// Durable facts of one observed binding fate (crash/death), as recorded by
/// the shell.
///
/// Both fate authorities are private-field types constructible only through
/// the crate's own transitions, so this event cannot assert a fate that never
/// committed. Fate authorities are conversation-scoped by the aggregate that
/// owns them; the shell stamps its own conversation into the event header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BindingFateOperation {
    participant_id: ParticipantId,
    last_dead_binding_epoch: BindingEpoch,
    resulting_floor: DeliverySeq,
}

impl BindingFateOperation {
    /// Creates the fate event body from an ordinary binding death.
    #[must_use]
    pub const fn from_ordinary(fate: &OrdinaryBindingFate) -> Self {
        Self {
            participant_id: fate.participant_id(),
            last_dead_binding_epoch: fate.last_dead_binding_epoch(),
            resulting_floor: fate.resulting_floor(),
        }
    }

    /// Creates the fate event body from a recovered-epoch binding death.
    #[must_use]
    pub const fn from_recovered(fate: &RecoveredBindingFate) -> Self {
        Self {
            participant_id: fate.participant_id(),
            last_dead_binding_epoch: fate.last_dead_binding_epoch(),
            resulting_floor: fate.resulting_floor(),
        }
    }

    pub(super) const fn from_decoded(
        participant_id: ParticipantId,
        last_dead_binding_epoch: BindingEpoch,
        resulting_floor: DeliverySeq,
    ) -> Self {
        Self {
            participant_id,
            last_dead_binding_epoch,
            resulting_floor,
        }
    }

    /// Returns the participant whose binding died.
    #[must_use]
    pub const fn participant_id(self) -> ParticipantId {
        self.participant_id
    }

    /// Returns the exact dead binding epoch whose fate was observed.
    #[must_use]
    pub const fn last_dead_binding_epoch(self) -> BindingEpoch {
        self.last_dead_binding_epoch
    }

    /// Returns the floor measured in the binding-fate transaction.
    #[must_use]
    pub const fn resulting_floor(self) -> DeliverySeq {
        self.resulting_floor
    }
}

/// Durable facts of one nonzero-debt participant cursor acknowledgement, as
/// recorded by the shell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NonzeroDebtAckOperation {
    conversation_id: ConversationId,
    participant_id: ParticipantId,
    capability_generation: Generation,
    through_seq: DeliverySeq,
}

impl NonzeroDebtAckOperation {
    /// Creates the ack event body from the crate's nonzero-debt ack commit.
    #[must_use]
    pub const fn new(commit: &NonzeroParticipantAckCommit) -> Self {
        let request = commit.outcome().request();
        Self {
            conversation_id: request.conversation_id,
            participant_id: request.participant_id,
            capability_generation: request.capability_generation,
            through_seq: request.through_seq,
        }
    }

    pub(super) const fn from_decoded(
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        capability_generation: Generation,
        through_seq: DeliverySeq,
    ) -> Self {
        Self {
            conversation_id,
            participant_id,
            capability_generation,
            through_seq,
        }
    }

    /// Returns the conversation whose participant acknowledged.
    #[must_use]
    pub const fn conversation_id(self) -> ConversationId {
        self.conversation_id
    }

    /// Returns the acknowledging participant.
    #[must_use]
    pub const fn participant_id(self) -> ParticipantId {
        self.participant_id
    }

    /// Returns the presented capability generation.
    #[must_use]
    pub const fn capability_generation(self) -> Generation {
        self.capability_generation
    }

    /// Returns the committed cumulative cursor boundary.
    #[must_use]
    pub const fn through_seq(self) -> DeliverySeq {
        self.through_seq
    }
}

/// One of the six lifecycle operations recordable by the conversation shell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConversationOperation {
    /// A participant enrolled with its generation-one origin binding.
    Enrolled(EnrolledOperation),
    /// A participant committed a credential attach.
    Attached(AttachedOperation),
    /// A participant committed a clean detach.
    Detached(DetachedOperation),
    /// A participant permanently left the conversation.
    Left(LeftOperation),
    /// A binding's crash/death fate was durably observed.
    BindingFate(BindingFateOperation),
    /// A participant advanced its cursor during a nonzero-debt episode.
    NonzeroDebtAck(NonzeroDebtAckOperation),
}

impl ConversationOperation {
    /// Returns the conversation named by the operation's provenance, when the
    /// producing commit carries one.
    pub(super) const fn conversation_id(&self) -> Option<ConversationId> {
        match self {
            Self::Enrolled(operation) => Some(operation.conversation_id()),
            Self::Attached(operation) => Some(operation.conversation_id()),
            Self::Detached(operation) => Some(operation.conversation_id()),
            Self::Left(operation) => Some(operation.committed().conversation_id()),
            Self::BindingFate(_) => None,
            Self::NonzeroDebtAck(operation) => Some(operation.conversation_id()),
        }
    }
}
