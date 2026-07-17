//! Total aggregate-level commits for the six lifecycle operations.
//!
//! `docs/design/LP-GAP-CLOSURE-GOAL.md` item A3: every producer here consumes
//! one of the crate's existing public typed commit results — no operation
//! logic is re-derived — mints the exact A1 event body from that consumed
//! value, and selects the shell event under the [`ConversationCommit`]
//! durability barrier. The consumed operation value travels inside the
//! barrier: it becomes reachable again only together with the advanced shell
//! (after a confirmed durable append) or together with the byte-for-byte
//! unchanged shell (after a failed append), so a crash between event
//! selection and durable append leaves the shell unadvanced and loses no
//! operation authority.
//!
//! Atomicity laws recorded by these commits:
//!
//! * **Detach** is ONE event: the consumed [`CommittedDetachTransition`]
//!   carries the terminal append, the floor transition, the replay-cell
//!   replacement, and the binding release as one non-decomposable value, and
//!   the single `Detached` event summarizes exactly that transaction.
//! * **Attach** records Fix 1: the consumed [`AttachCommit`] already
//!   terminalized its `Committed` detach cell atomically with the credential
//!   rotation, so the one `Attached` event covers the
//!   `Committed → Terminalized` cell transition.
//! * **Leave** consumes the whole [`LeaveCommit`] WITH its claim frontiers;
//!   the tombstone is never split from the frontier authority committed in
//!   the same Leave transaction.
//! * **The nonzero-debt ack** records Fix 2's per-participant cursor-fact
//!   accounting: the event body's `(participant, through_seq)` pair is the
//!   exact `(participant_index, boundary)` cursor-fact key committed into the
//!   consumed commit's resulting episode.
//!
//! Record admission is deliberately absent from this surface: it is the one
//! conversation mutation that mints no shell event. Its aggregate feed is the
//! moved [`RecordAdmissionPersistenceParts`](super::RecordAdmissionPersistenceParts)
//! payload (item A2), persisted by the durable writer in one atomic
//! transaction without advancing the shell's event log.

use super::operation_event::{
    AttachedOperation, BindingFateOperation, ConversationOperation, DetachedOperation,
    EnrolledOperation, LeftOperation, NonzeroDebtAckOperation,
};
use super::{
    AttachCommit, CommittedDetachTransition, ConversationCommit, ConversationDecision,
    ConversationEvent, ConversationRefusal, ConversationRefusalReason, EnrollmentCommit,
    IdentityState, LeaveCommit, NonzeroParticipantAckCommit, OrdinaryBindingFate,
    ParticipantConversation, RecoveredBindingFate,
};

use alloc::boxed::Box;

/// Aggregate durability barrier pairing one selected shell event with the
/// consumed typed operation commit.
///
/// Both fields are private: while the event is speculative, neither the shell
/// nor the consumed operation authority is reachable, so nothing can be
/// persisted or executed from a not-yet-durable decision.
///
/// ```compile_fail
/// use liminal_protocol::lifecycle::AggregateOperationCommit;
///
/// fn leak<T>(commit: AggregateOperationCommit<T>) {
///     let _ = commit.operation;
/// }
/// ```
#[derive(Debug, PartialEq, Eq)]
pub struct AggregateOperationCommit<T> {
    shell: ConversationCommit,
    operation: T,
}

impl<T> AggregateOperationCommit<T> {
    /// Borrows the exact event that must be durably appended before commit.
    #[must_use]
    pub const fn event(&self) -> &ConversationEvent {
        self.shell.event()
    }

    /// Consumes the barrier after a confirmed durable append.
    ///
    /// Returns the advanced shell together with the intact typed operation
    /// commit, so the caller persists the operation's own resulting state in
    /// the same durable transaction as the appended event.
    #[must_use]
    pub fn commit(self) -> (ParticipantConversation, T) {
        (self.shell.commit(), self.operation)
    }

    /// Cancels a failed durable append.
    ///
    /// Returns the byte-for-byte unchanged shell pre-state together with the
    /// intact typed operation commit; nothing advanced and no authority was
    /// dropped.
    #[must_use]
    pub fn abort(self) -> (ParticipantConversation, T) {
        (self.shell.abort(), self.operation)
    }
}

/// Refused aggregate decision retaining the unchanged shell and the intact
/// consumed operation commit.
#[derive(Debug, PartialEq, Eq)]
pub struct AggregateOperationRefusal<T> {
    refusal: ConversationRefusal,
    operation: T,
}

impl<T> AggregateOperationRefusal<T> {
    /// Returns the shell's stable refusal reason.
    #[must_use]
    pub const fn reason(&self) -> ConversationRefusalReason {
        self.refusal.reason()
    }

    /// Recovers the unchanged shell and the intact operation commit.
    #[must_use]
    pub fn into_parts(self) -> (ParticipantConversation, T) {
        (self.refusal.into_conversation(), self.operation)
    }
}

/// Total aggregate decision for one lifecycle operation.
#[derive(Debug, PartialEq, Eq)]
pub enum AggregateOperationDecision<T> {
    /// Append the selected event, then consume the barrier into usable state.
    Commit(AggregateOperationCommit<T>),
    /// The shell refused the event; shell and operation are both recoverable.
    Refused(AggregateOperationRefusal<T>),
}

/// Reason a consumed typed commit could not repeat itself as an event body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregateOperationFaultReason {
    /// The detach transition's committed terminal and replay cell did not
    /// name one detach commit.
    DetachTerminalCellMismatch,
    /// The leave commit's identity result was not a permanent tombstone.
    LeaveIdentityNotRetired,
}

/// Fail-loud invariant report: a consumed typed commit disagreed with itself.
///
/// The crate's own commit constructors make both reasons unreachable; this
/// arm exists so a violated internal pairing invariant surfaces as a typed
/// error carrying the unchanged shell and the intact operation value, never
/// as a silently recorded lie and never as a panic.
#[derive(Debug, PartialEq, Eq)]
pub struct AggregateOperationFault<T> {
    shell: ParticipantConversation,
    operation: T,
    reason: AggregateOperationFaultReason,
}

impl<T> AggregateOperationFault<T> {
    /// Returns the exact violated pairing invariant.
    #[must_use]
    pub const fn reason(&self) -> AggregateOperationFaultReason {
        self.reason
    }

    /// Recovers the unchanged shell and the intact operation commit.
    #[must_use]
    pub fn into_parts(self) -> (ParticipantConversation, T) {
        (self.shell, self.operation)
    }
}

/// Total aggregate decision, or the fail-loud pairing fault for the two
/// producers whose event bodies revalidate their consumed commit.
pub type AggregateOperationResult<T> =
    Result<AggregateOperationDecision<T>, Box<AggregateOperationFault<T>>>;

const fn decide<T>(
    conversation: ParticipantConversation,
    operation: ConversationOperation,
    payload: T,
) -> AggregateOperationDecision<T> {
    match conversation.decide_operation(operation) {
        ConversationDecision::Commit(shell) => {
            AggregateOperationDecision::Commit(AggregateOperationCommit {
                shell,
                operation: payload,
            })
        }
        ConversationDecision::Refused(refusal) => {
            AggregateOperationDecision::Refused(AggregateOperationRefusal {
                refusal,
                operation: payload,
            })
        }
    }
}

/// Selects the aggregate `Enrolled` event for one consumed enrollment commit.
///
/// The event body repeats the exact committed `Attached` record carried by
/// [`super::commit_enrollment`]'s result; the commit itself returns intact
/// through the durability barrier.
#[must_use]
pub const fn decide_enrolled_operation<F>(
    conversation: ParticipantConversation,
    commit: EnrollmentCommit<F>,
) -> AggregateOperationDecision<EnrollmentCommit<F>> {
    let body = EnrolledOperation::new(&commit);
    decide(conversation, ConversationOperation::Enrolled(body), commit)
}

/// Selects the aggregate `Attached` event for one consumed attach commit.
///
/// Fix 1 rides inside the consumed [`AttachCommit`]: its detach cell was
/// terminalized `Committed → Terminalized` atomically with the rotation, so
/// this single event records that whole transaction.
#[must_use]
pub const fn decide_attached_operation<F, V>(
    conversation: ParticipantConversation,
    commit: AttachCommit<F, V>,
) -> AggregateOperationDecision<AttachCommit<F, V>> {
    let body = AttachedOperation::new(&commit);
    decide(conversation, ConversationOperation::Attached(body), commit)
}

/// Selects the aggregate `Detached` event for one consumed detach transition.
///
/// Detach is ONE event: the consumed [`CommittedDetachTransition`] carries
/// the terminal append, floor transition, cell replacement, and binding
/// release together, and the event summarizes exactly that committed
/// transaction, recording the replay cell's own attempt token.
///
/// # Errors
///
/// Returns [`AggregateOperationFault`] with
/// [`AggregateOperationFaultReason::DetachTerminalCellMismatch`] if the
/// transition's terminal and cell fail their pairing revalidation — an
/// internal invariant [`super::commit_detach`] makes unreachable. The shell
/// and transition return unchanged.
pub fn decide_detached_operation<EF, V>(
    conversation: ParticipantConversation,
    transition: CommittedDetachTransition<EF, V>,
) -> AggregateOperationResult<CommittedDetachTransition<EF, V>> {
    let Some(body) = DetachedOperation::new(&transition) else {
        return Err(Box::new(AggregateOperationFault {
            shell: conversation,
            operation: transition,
            reason: AggregateOperationFaultReason::DetachTerminalCellMismatch,
        }));
    };
    Ok(decide(
        conversation,
        ConversationOperation::Detached(body),
        transition,
    ))
}

/// Selects the aggregate `Left` event for one consumed leave commit.
///
/// The whole [`LeaveCommit`] — tombstone AND the claim frontiers committed in
/// the same Leave transaction — is consumed and returned intact through the
/// barrier, so the frontier authority can never be split from the event that
/// records its Leave.
///
/// # Errors
///
/// Returns [`AggregateOperationFault`] with
/// [`AggregateOperationFaultReason::LeaveIdentityNotRetired`] if the commit's
/// identity result is not a permanent tombstone — an internal invariant
/// [`super::commit_leave`] and [`super::commit_pending_leave`] make
/// unreachable. The shell and commit return unchanged.
pub fn decide_left_operation<EF, V, LF>(
    conversation: ParticipantConversation,
    commit: LeaveCommit<EF, V, LF>,
) -> AggregateOperationResult<LeaveCommit<EF, V, LF>> {
    let body = match commit.identity() {
        IdentityState::Retired(retired) => LeftOperation::new(retired),
        IdentityState::Live(_) => {
            return Err(Box::new(AggregateOperationFault {
                shell: conversation,
                operation: commit,
                reason: AggregateOperationFaultReason::LeaveIdentityNotRetired,
            }));
        }
    };
    Ok(decide(
        conversation,
        ConversationOperation::Left(body),
        commit,
    ))
}

/// Selects the aggregate `BindingFate` event for one consumed ordinary fate.
///
/// The fate authority returns intact through the barrier so the caller can
/// apply it to the stored closure edge in the same durable transaction.
#[must_use]
pub const fn decide_ordinary_binding_fate_operation(
    conversation: ParticipantConversation,
    fate: OrdinaryBindingFate,
) -> AggregateOperationDecision<OrdinaryBindingFate> {
    let body = BindingFateOperation::from_ordinary(&fate);
    decide(conversation, ConversationOperation::BindingFate(body), fate)
}

/// Selects the aggregate `BindingFate` event for one consumed recovered fate.
///
/// The non-cloneable fate authority returns intact through the barrier so
/// the caller can consume it through the exact stored edge's
/// `apply_recovered_binding_fate` transition in the same durable transaction.
#[must_use]
pub const fn decide_recovered_binding_fate_operation(
    conversation: ParticipantConversation,
    fate: RecoveredBindingFate,
) -> AggregateOperationDecision<RecoveredBindingFate> {
    let body = BindingFateOperation::from_recovered(&fate);
    decide(conversation, ConversationOperation::BindingFate(body), fate)
}

/// Selects the aggregate `NonzeroDebtAck` event for one consumed ack commit.
///
/// Fix 2's per-participant cursor-fact accounting rides with the consumed
/// commit: the event body's `(participant, through_seq)` pair is the exact
/// `(participant_index, boundary)` cursor-fact key recorded in the commit's
/// resulting episode, which returns intact through the barrier for the same
/// durable transaction as the event append.
#[must_use]
pub fn decide_nonzero_debt_ack_operation(
    conversation: ParticipantConversation,
    commit: Box<NonzeroParticipantAckCommit>,
) -> AggregateOperationDecision<Box<NonzeroParticipantAckCommit>> {
    let body = NonzeroDebtAckOperation::new(&commit);
    decide(
        conversation,
        ConversationOperation::NonzeroDebtAck(body),
        commit,
    )
}
