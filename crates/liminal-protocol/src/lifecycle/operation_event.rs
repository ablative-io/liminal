//! Public durable-fact vocabulary for the six lifecycle operations recorded
//! by the participant-conversation shell.
//!
//! Every public producer consumes one of the crate's own sealed commit values
//! (`docs/design/LP-GAP-CLOSURE-GOAL.md` item A1): enrollment facts come from
//! [`EnrollmentCommit`] and attach facts from [`AttachCommit`] (both carry
//! private fields, so only [`super::commit_enrollment`] and
//! [`super::commit_attach`] mint them, and the recorded fact kind is bound to
//! the producing commit kind); detach facts consume the whole atomic
//! [`CommittedDetachTransition`], whose committed replay cell — born in the
//! same detach commit as its terminal — supplies the recorded attempt token;
//! leave
//! facts come from the permanent [`RetiredIdentity`] tombstone, which carries
//! both the canonical [`LeaveCommitted`] result and the congruence-checked
//! `Left` transaction order; fate facts come from the two private-field fate
//! authorities, which carry the conversation validated at their minting
//! transitions; and ack facts come from the nonzero-debt ack commit. The one
//! raw promotion path into any of those inputs is validated whole-participant
//! cold restore ([`super::ParticipantLifecycleRestore::restore`] and the
//! sealed stored-edge restores), each of which re-validates provenance before
//! minting. These payloads describe committed operations for the ordering
//! shell; they are not executable lifecycle authority, and no path exists from
//! a decoded or constructed payload to a typed lifecycle state, stored edge,
//! or binding origin. Decoded events rebuild payloads through crate-private
//! constructors that re-validate every canonical field invariant.

use crate::wire::{
    BindingEpoch, ConversationId, DeliverySeq, DetachAttemptToken, DetachedCause, Generation,
    LeaveCommitted, ParticipantId, TransactionOrder,
};

use super::{
    AttachCommit, CommittedDetachTransition, EnrollmentCommit, NonzeroParticipantAckCommit,
    OrdinaryBindingFate, RecoveredBindingFate, RetiredIdentity,
};

/// Durable facts of one committed enrollment, as recorded by the shell.
///
/// The only public producer consumes the crate's own enrollment commit
/// ([`EnrollmentCommit`] carries a private field, so only
/// [`super::commit_enrollment`] mints it), so an event can exist only
/// downstream of a real enrollment commit and can never be minted from an
/// attach commit's record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EnrolledOperation {
    conversation_id: ConversationId,
    participant_id: ParticipantId,
    binding_epoch: BindingEpoch,
    attached_transaction_order: TransactionOrder,
    attached_delivery_seq: DeliverySeq,
}

impl EnrolledOperation {
    /// Creates the enrollment event body from the enrollment commit itself.
    ///
    /// [`super::commit_enrollment`] refuses any non-generation-one origin
    /// epoch before constructing [`EnrollmentCommit`], so the canonical
    /// generation-one body invariant re-validated on decode holds for every
    /// value this producer can observe.
    #[must_use]
    pub const fn new<F>(commit: &EnrollmentCommit<F>) -> Self {
        let attached = commit.attached;
        Self {
            conversation_id: attached.conversation_id(),
            participant_id: attached.participant_id(),
            binding_epoch: attached.binding_epoch(),
            attached_transaction_order: attached.admission_order().transaction_order(),
            attached_delivery_seq: attached.delivery_seq(),
        }
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
///
/// The only public producer consumes the crate's own attach commit
/// ([`AttachCommit`] carries private fields, so only [`super::commit_attach`]
/// mints it), so an enrollment's generation-one record cannot be relabeled as
/// an attach fact.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AttachedOperation {
    conversation_id: ConversationId,
    participant_id: ParticipantId,
    binding_epoch: BindingEpoch,
    attached_transaction_order: TransactionOrder,
    attached_delivery_seq: DeliverySeq,
}

impl AttachedOperation {
    /// Creates the attach event body from the attach commit itself.
    ///
    /// The producer consumes the whole sealed commit rather than the shared
    /// `Attached` record, so an enrollment commit cannot be relabeled as a
    /// credential-attach fact:
    ///
    /// ```compile_fail
    /// use liminal_protocol::lifecycle::{AttachedOperation, EnrollmentCommit};
    ///
    /// fn relabel(commit: &EnrollmentCommit<[u8; 4]>) -> AttachedOperation {
    ///     AttachedOperation::new(commit)
    /// }
    /// ```
    #[must_use]
    pub const fn new<F, V>(commit: &AttachCommit<F, V>) -> Self {
        let attached = commit.attached;
        Self {
            conversation_id: attached.conversation_id(),
            participant_id: attached.participant_id(),
            binding_epoch: attached.binding_epoch(),
            attached_transaction_order: attached.admission_order().transaction_order(),
            attached_delivery_seq: attached.delivery_seq(),
        }
    }

    pub(super) fn from_decoded(
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
        attached_transaction_order: TransactionOrder,
        attached_delivery_seq: DeliverySeq,
    ) -> Option<Self> {
        // Every attach commit increments the member generation, so a
        // committed attach epoch is generation two or later; a generation-one
        // "attach" is an enrollment fact relabeled on the wire and is refused
        // exactly as the enrolled decoder refuses non-generation-one bodies.
        if binding_epoch.capability_generation == Generation::ONE {
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
/// `Detached(CleanDeregister)` terminal plus the replayable attempt token
/// retained inside the committed replay cell.
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
    /// Creates the detach event body from the whole atomic detach transition.
    ///
    /// The producer consumes the crate's own sealed commit
    /// ([`CommittedDetachTransition`] has no public constructor, so only
    /// [`super::commit_detach`] and [`super::complete_pending_detach`] mint
    /// it), exactly like the other five producers: the recorded terminal and
    /// the replay cell supplying the recorded attempt token were born in one
    /// detach commit with full conversation context, so pairing a terminal
    /// with a cell from another detach — or another conversation whose
    /// participant, epoch, and delivery-sequence fields collide — is
    /// unrepresentable:
    ///
    /// ```compile_fail
    /// use liminal_protocol::lifecycle::{CommittedDetachTransition, DetachedOperation};
    ///
    /// fn mispair(
    ///     conversation_a: &CommittedDetachTransition<(), [u8; 32]>,
    ///     conversation_b: &CommittedDetachTransition<(), [u8; 32]>,
    /// ) -> Option<DetachedOperation> {
    ///     DetachedOperation::new(conversation_a.terminal(), conversation_b.cell())
    /// }
    /// ```
    ///
    /// A standalone terminal cannot be presented either, so a supersession
    /// terminal still belongs only to the attach event that committed it:
    ///
    /// ```compile_fail
    /// use liminal_protocol::lifecycle::{CommittedDetachedTerminal, DetachedOperation};
    ///
    /// fn relabel(terminal: &CommittedDetachedTerminal) -> Option<DetachedOperation> {
    ///     DetachedOperation::new(terminal)
    /// }
    /// ```
    ///
    /// Returns `None` unless the transition's terminal cause is
    /// `CleanDeregister` and its cell names that terminal's exact
    /// participant, ended binding epoch, and committed delivery sequence —
    /// re-validation of invariants the detach commit constructors already
    /// guarantee, surfaced by the aggregate commit as a typed pairing fault
    /// rather than a recorded lie.
    #[must_use]
    pub fn new<EF, V>(transition: &CommittedDetachTransition<EF, V>) -> Option<Self> {
        let terminal = transition.terminal();
        let cell = transition.cell();
        if terminal.cause() != DetachedCause::CleanDeregister {
            return None;
        }
        let participant_matches = cell.participant_id() == terminal.participant_id();
        let epoch_matches = cell.committed_binding_epoch() == terminal.binding_epoch();
        let delivery_matches = cell.detached_delivery_seq() == terminal.delivery_seq();
        if !(participant_matches && epoch_matches && delivery_matches) {
            return None;
        }
        Some(Self {
            detach_attempt_token: cell.token(),
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
    /// Creates the leave event body from the permanent retirement tombstone.
    ///
    /// [`RetiredIdentity`] carries both the canonical [`LeaveCommitted`]
    /// result and the congruence-checked `Left` admission order, so the
    /// recorded fact and its transaction order both come from a committed
    /// leave ([`super::commit_leave`] or [`super::commit_pending_leave`])
    /// rather than from caller-chosen raw values. The one raw promotion path
    /// into [`RetiredIdentity`] is validated whole-participant cold restore
    /// ([`super::ParticipantLifecycleRestore::restore`]), which re-validates
    /// every stored field against the stored result before minting the
    /// tombstone.
    ///
    /// A leave fact cannot be minted from wire-constructible raw values:
    ///
    /// ```compile_fail
    /// use liminal_protocol::lifecycle::LeftOperation;
    /// use liminal_protocol::wire::LeaveCommitted;
    ///
    /// fn fabricate(committed: LeaveCommitted) -> LeftOperation {
    ///     LeftOperation::new(committed, 17)
    /// }
    /// ```
    #[must_use]
    pub fn new<EF, V, LF>(retired: &RetiredIdentity<EF, V, LF>) -> Self {
        Self {
            committed: retired.committed_result().clone(),
            left_transaction_order: retired.left_admission_order().transaction_order(),
        }
    }

    pub(super) const fn from_decoded(
        committed: LeaveCommitted,
        left_transaction_order: TransactionOrder,
    ) -> Self {
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
/// committed. Each authority carries the conversation validated at its
/// minting transition (the committed `Died` terminal for ordinary fate, the
/// frontier-validated marker provenance for recovered fate), so the shell's
/// conversation-congruence refusal applies to this arm exactly as it does to
/// the other five.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BindingFateOperation {
    conversation_id: ConversationId,
    participant_id: ParticipantId,
    last_dead_binding_epoch: BindingEpoch,
    resulting_floor: DeliverySeq,
}

impl BindingFateOperation {
    /// Creates the fate event body from an ordinary binding death.
    #[must_use]
    pub const fn from_ordinary(fate: &OrdinaryBindingFate) -> Self {
        Self {
            conversation_id: fate.conversation_id(),
            participant_id: fate.participant_id(),
            last_dead_binding_epoch: fate.last_dead_binding_epoch(),
            resulting_floor: fate.resulting_floor(),
        }
    }

    /// Creates the fate event body from a recovered-epoch binding death.
    #[must_use]
    pub const fn from_recovered(fate: &RecoveredBindingFate) -> Self {
        Self {
            conversation_id: fate.conversation_id(),
            participant_id: fate.participant_id(),
            last_dead_binding_epoch: fate.last_dead_binding_epoch(),
            resulting_floor: fate.resulting_floor(),
        }
    }

    pub(super) const fn from_decoded(
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        last_dead_binding_epoch: BindingEpoch,
        resulting_floor: DeliverySeq,
    ) -> Self {
        Self {
            conversation_id,
            participant_id,
            last_dead_binding_epoch,
            resulting_floor,
        }
    }

    /// Returns the conversation whose binding fate was observed.
    #[must_use]
    pub const fn conversation_id(self) -> ConversationId {
        self.conversation_id
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
    /// Returns the conversation named by the operation's provenance.
    pub(super) const fn conversation_id(&self) -> ConversationId {
        match self {
            Self::Enrolled(operation) => operation.conversation_id(),
            Self::Attached(operation) => operation.conversation_id(),
            Self::Detached(operation) => operation.conversation_id(),
            Self::Left(operation) => operation.committed().conversation_id(),
            Self::BindingFate(operation) => operation.conversation_id(),
            Self::NonzeroDebtAck(operation) => operation.conversation_id(),
        }
    }
}
