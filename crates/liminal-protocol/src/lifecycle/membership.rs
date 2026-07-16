use crate::outcome::CandidatePhase;
use crate::wire::{
    AttachSecret, BindingEpoch, ConversationId, DeliverySeq, DetachedCause, Generation,
    LeaveAttemptToken, LeaveCommitted, LeaveRequest, ParticipantId, TransactionOrder,
};

use super::{
    AdmissionOrder, BindingState, ClaimFrontiers, CommittedBindingTerminal, DetachCell,
    PendingFinalization, detach::validate_pending_pair, lookup::AttachSecretProof,
};

/// Consuming-layer enrollment-token fingerprint with no protocol-invented width.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnrollmentFingerprint<F>(F);

impl<F> EnrollmentFingerprint<F> {
    /// Wraps the consuming cryptographic layer's canonical fingerprint value.
    #[must_use]
    pub const fn new(value: F) -> Self {
        Self(value)
    }

    /// Borrows the consuming-layer fingerprint.
    #[must_use]
    pub const fn value(&self) -> &F {
        &self.0
    }

    /// Consumes the wrapper and returns the fingerprint value.
    #[must_use]
    pub fn into_inner(self) -> F {
        self.0
    }
}

/// Consuming-layer canonical Leave-request fingerprint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaveFingerprint<F>(F);

impl<F> LeaveFingerprint<F> {
    /// Wraps the consuming cryptographic layer's canonical fingerprint value.
    #[must_use]
    pub const fn new(value: F) -> Self {
        Self(value)
    }

    /// Borrows the consuming-layer fingerprint.
    #[must_use]
    pub const fn value(&self) -> &F {
        &self.0
    }

    /// Consumes the wrapper and returns the fingerprint value.
    #[must_use]
    pub fn into_inner(self) -> F {
        self.0
    }
}

/// Complete persistence input for restoring one live member.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveMemberRestore<F> {
    /// Permanent participant identity/index.
    pub participant_id: ParticipantId,
    /// Owning conversation.
    pub conversation_id: ConversationId,
    /// Current credential generation.
    pub generation: Generation,
    /// Current attach secret.
    pub attach_secret: AttachSecret,
    /// Durable cumulative participant cursor.
    pub cursor: DeliverySeq,
    /// Permanent enrollment-token fingerprint.
    pub enrollment_fingerprint: EnrollmentFingerprint<F>,
    /// Most recent committed binding terminal, if any.
    pub latest_terminal: Option<CommittedBindingTerminal>,
}

/// Invalid durable membership/history combination.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MembershipInvariantError {
    /// Retained terminal names another participant or conversation.
    TerminalIdentity,
    /// Retained terminal belongs to a generation newer than the current credential.
    TerminalGeneration,
}

/// Live participant membership plus permanent enrollment and terminal history.
///
/// Fields are private so a committed binding terminal cannot drift away from
/// the identity that owns it. Persistence restoration must pass [`Self::restore`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveMember<F> {
    participant_id: ParticipantId,
    conversation_id: ConversationId,
    generation: Generation,
    attach_secret: AttachSecret,
    cursor: DeliverySeq,
    enrollment_fingerprint: EnrollmentFingerprint<F>,
    latest_terminal: Option<CommittedBindingTerminal>,
}

/// Crate-owned proof of one exact cumulative cursor transition.
///
/// Fields and construction remain crate-private so consuming servers can apply
/// only an update emitted by a typed protocol operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct LiveMemberCursorUpdate {
    conversation_id: ConversationId,
    participant_id: ParticipantId,
    generation: Generation,
    from_cursor: DeliverySeq,
    resulting_cursor: DeliverySeq,
}

impl LiveMemberCursorUpdate {
    /// Captures the complete identity and old/new cursor prestate.
    pub(super) const fn new(
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        generation: Generation,
        from_cursor: DeliverySeq,
        resulting_cursor: DeliverySeq,
    ) -> Self {
        Self {
            conversation_id,
            participant_id,
            generation,
            from_cursor,
            resulting_cursor,
        }
    }
}

/// Rejection while applying an opaque cursor update to durable membership.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LiveMemberCursorUpdateError {
    /// Conversation differs from the commit proof.
    Conversation {
        expected: ConversationId,
        actual: ConversationId,
    },
    /// Participant differs from the commit proof.
    Participant {
        expected: ParticipantId,
        actual: ParticipantId,
    },
    /// Credential generation differs from the commit proof.
    Generation {
        expected: Generation,
        actual: Generation,
    },
    /// Proposed update is not a strict cumulative advance.
    NonAdvancing {
        from_cursor: DeliverySeq,
        resulting_cursor: DeliverySeq,
    },
    /// Member cursor is neither the old nor already-committed new prestate.
    CursorPrestate {
        expected_from_cursor: DeliverySeq,
        resulting_cursor: DeliverySeq,
        actual_cursor: DeliverySeq,
    },
}

impl<F> LiveMember<F> {
    /// Restores a durable member after checking retained terminal identity and generation.
    ///
    /// # Errors
    ///
    /// Returns [`MembershipInvariantError`] when the terminal belongs to another
    /// identity/conversation or a generation newer than the restored credential.
    pub fn restore(state: LiveMemberRestore<F>) -> Result<Self, MembershipInvariantError> {
        validate_terminal(
            state.participant_id,
            state.conversation_id,
            state.generation,
            state.latest_terminal,
        )?;
        Ok(Self {
            participant_id: state.participant_id,
            conversation_id: state.conversation_id,
            generation: state.generation,
            attach_secret: state.attach_secret,
            cursor: state.cursor,
            enrollment_fingerprint: state.enrollment_fingerprint,
            latest_terminal: state.latest_terminal,
        })
    }

    pub(crate) const fn from_enrollment(
        participant_id: ParticipantId,
        conversation_id: ConversationId,
        attach_secret: AttachSecret,
        enrollment_fingerprint: EnrollmentFingerprint<F>,
    ) -> Self {
        Self {
            participant_id,
            conversation_id,
            generation: Generation::ONE,
            attach_secret,
            cursor: 0,
            enrollment_fingerprint,
            latest_terminal: None,
        }
    }

    /// Returns the permanent participant identity/index.
    #[must_use]
    pub const fn participant_id(&self) -> ParticipantId {
        self.participant_id
    }

    /// Returns the owning conversation.
    #[must_use]
    pub const fn conversation_id(&self) -> ConversationId {
        self.conversation_id
    }

    /// Returns the current credential generation.
    #[must_use]
    pub const fn generation(&self) -> Generation {
        self.generation
    }

    /// Returns the current attach secret.
    #[must_use]
    pub const fn attach_secret(&self) -> AttachSecret {
        self.attach_secret
    }

    /// Returns the durable cumulative cursor.
    #[must_use]
    pub const fn cursor(&self) -> DeliverySeq {
        self.cursor
    }

    /// Borrows the permanent enrollment-token fingerprint.
    #[must_use]
    pub const fn enrollment_fingerprint(&self) -> &EnrollmentFingerprint<F> {
        &self.enrollment_fingerprint
    }

    /// Returns the most recent committed binding terminal.
    #[must_use]
    pub const fn latest_terminal(&self) -> Option<CommittedBindingTerminal> {
        self.latest_terminal
    }

    /// Applies an exact old/new-prestate cursor proof without permitting regression.
    pub(super) fn apply_cursor_update(
        &mut self,
        update: LiveMemberCursorUpdate,
    ) -> Result<(), LiveMemberCursorUpdateError> {
        if self.conversation_id != update.conversation_id {
            return Err(LiveMemberCursorUpdateError::Conversation {
                expected: update.conversation_id,
                actual: self.conversation_id,
            });
        }
        if self.participant_id != update.participant_id {
            return Err(LiveMemberCursorUpdateError::Participant {
                expected: update.participant_id,
                actual: self.participant_id,
            });
        }
        if self.generation != update.generation {
            return Err(LiveMemberCursorUpdateError::Generation {
                expected: update.generation,
                actual: self.generation,
            });
        }
        if update.resulting_cursor <= update.from_cursor {
            return Err(LiveMemberCursorUpdateError::NonAdvancing {
                from_cursor: update.from_cursor,
                resulting_cursor: update.resulting_cursor,
            });
        }
        if self.cursor == update.resulting_cursor {
            return Ok(());
        }
        if self.cursor != update.from_cursor {
            return Err(LiveMemberCursorUpdateError::CursorPrestate {
                expected_from_cursor: update.from_cursor,
                resulting_cursor: update.resulting_cursor,
                actual_cursor: self.cursor,
            });
        }
        self.cursor = update.resulting_cursor;
        Ok(())
    }

    /// Replaces the latest binding terminal after checking its identity domain.
    ///
    /// # Errors
    ///
    /// Returns [`MembershipInvariantError`] for mismatched identity or generation.
    pub fn with_committed_terminal(
        mut self,
        terminal: CommittedBindingTerminal,
    ) -> Result<Self, MembershipInvariantError> {
        validate_terminal(
            self.participant_id,
            self.conversation_id,
            self.generation,
            Some(terminal),
        )?;
        self.latest_terminal = Some(terminal);
        Ok(self)
    }

    pub(super) fn rotate(
        mut self,
        generation: Generation,
        attach_secret: AttachSecret,
        cursor: DeliverySeq,
        terminal: Option<CommittedBindingTerminal>,
    ) -> Result<Self, MembershipInvariantError> {
        let latest_terminal = terminal.or(self.latest_terminal);
        validate_terminal(
            self.participant_id,
            self.conversation_id,
            generation,
            latest_terminal,
        )?;
        self.generation = generation;
        self.attach_secret = attach_secret;
        self.cursor = cursor;
        self.latest_terminal = latest_terminal;
        Ok(self)
    }
}

fn validate_terminal(
    participant_id: ParticipantId,
    conversation_id: ConversationId,
    generation: Generation,
    terminal: Option<CommittedBindingTerminal>,
) -> Result<(), MembershipInvariantError> {
    let Some(terminal) = terminal else {
        return Ok(());
    };
    if terminal.participant_id() != participant_id || terminal.conversation_id() != conversation_id
    {
        return Err(MembershipInvariantError::TerminalIdentity);
    }
    if terminal.binding_epoch().capability_generation > generation {
        return Err(MembershipInvariantError::TerminalGeneration);
    }
    Ok(())
}

/// Permanent retired identity tombstone.
///
/// The tombstone retains generic non-reversible fingerprints/verifier but no
/// attach secret or request body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetiredIdentity<EF, V, LF> {
    participant_id: ParticipantId,
    conversation_id: ConversationId,
    retired_generation: Generation,
    enrollment_fingerprint: EnrollmentFingerprint<EF>,
    leave_attempt_token: LeaveAttemptToken,
    leave_request_verifier: V,
    leave_fingerprint: LeaveFingerprint<LF>,
    left_admission_order: AdmissionOrder,
    committed_result: LeaveCommitted,
}

impl<EF, V, LF> RetiredIdentity<EF, V, LF> {
    /// Permanent participant id.
    #[must_use]
    pub const fn participant_id(&self) -> ParticipantId {
        self.participant_id
    }

    /// Conversation containing the tombstone.
    #[must_use]
    pub const fn conversation_id(&self) -> ConversationId {
        self.conversation_id
    }

    /// Permanent retired generation.
    #[must_use]
    pub const fn retired_generation(&self) -> Generation {
        self.retired_generation
    }

    /// Permanent committed Leave token.
    #[must_use]
    pub const fn leave_attempt_token(&self) -> LeaveAttemptToken {
        self.leave_attempt_token
    }

    /// Stored complete Leave result for exact replay.
    #[must_use]
    pub const fn committed_result(&self) -> &LeaveCommitted {
        &self.committed_result
    }

    /// Returns the immutable causal key of the permanent `Left` record.
    #[must_use]
    pub const fn left_admission_order(&self) -> AdmissionOrder {
        self.left_admission_order
    }

    /// Stored non-reversible secret-proof verifier.
    #[must_use]
    pub const fn leave_request_verifier(&self) -> &V {
        &self.leave_request_verifier
    }

    /// Permanent enrollment mapping fingerprint.
    #[must_use]
    pub const fn enrollment_fingerprint(&self) -> &EnrollmentFingerprint<EF> {
        &self.enrollment_fingerprint
    }

    /// Permanent canonical Leave fingerprint.
    #[must_use]
    pub const fn leave_fingerprint(&self) -> &LeaveFingerprint<LF> {
        &self.leave_fingerprint
    }

    #[allow(clippy::too_many_arguments)]
    #[cfg(test)]
    pub(super) fn restore(
        participant_id: ParticipantId,
        conversation_id: ConversationId,
        retired_generation: Generation,
        enrollment_fingerprint: EnrollmentFingerprint<EF>,
        leave_attempt_token: LeaveAttemptToken,
        leave_request_verifier: V,
        leave_fingerprint: LeaveFingerprint<LF>,
        left_transaction_order: TransactionOrder,
        committed_result: LeaveCommitted,
    ) -> Result<Self, RetirementError> {
        if committed_result.conversation_id() != conversation_id {
            return Err(RetirementError::Conversation);
        }
        if committed_result.participant_id() != participant_id {
            return Err(RetirementError::Participant);
        }
        if committed_result.presented_generation() != retired_generation {
            return Err(RetirementError::Generation);
        }
        if committed_result.retired_generation() != retired_generation {
            return Err(RetirementError::RetiredGeneration);
        }
        if committed_result.leave_attempt_token() != leave_attempt_token {
            return Err(RetirementError::Token);
        }
        let left_admission_order = AdmissionOrder::new(
            left_transaction_order,
            CandidatePhase::MembershipExit,
            participant_id,
        );
        Ok(Self {
            participant_id,
            conversation_id,
            retired_generation,
            enrollment_fingerprint,
            leave_attempt_token,
            leave_request_verifier,
            leave_fingerprint,
            left_admission_order,
            committed_result,
        })
    }
}

/// Present participant identity state; absence is represented outside this enum.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdentityState<EF, V, LF> {
    /// Live membership, whether bound or detached.
    Live(LiveMember<EF>),
    /// Permanent Leave tombstone.
    Retired(RetiredIdentity<EF, V, LF>),
}

/// One indivisible committed Leave state update.
///
/// The identity tombstone and the claim frontiers resulting from the prepared
/// Leave transition are deliberately carried together and are not cloneable.
/// A durable binding must persist both parts in the same transaction; exposing
/// only the tombstone would discard the consumed and relayed frontier authority.
#[derive(Debug, PartialEq, Eq)]
pub struct LeaveCommit<EF, V, LF> {
    identity: IdentityState<EF, V, LF>,
    frontiers: ClaimFrontiers,
}

impl<EF, V, LF> LeaveCommit<EF, V, LF> {
    /// Borrows the permanent identity result committed by Leave.
    #[must_use]
    pub const fn identity(&self) -> &IdentityState<EF, V, LF> {
        &self.identity
    }

    /// Borrows the exact claim frontiers committed with the identity result.
    #[must_use]
    pub const fn frontiers(&self) -> &ClaimFrontiers {
        &self.frontiers
    }

    /// Consumes the atomic commit for one durable transaction.
    #[must_use]
    pub fn into_parts(self) -> (IdentityState<EF, V, LF>, ClaimFrontiers) {
        (self.identity, self.frontiers)
    }
}

/// Mismatch between a live member and proposed stored Leave result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetirementError {
    /// Result names another conversation.
    Conversation,
    /// Result names another participant.
    Participant,
    /// Result's presented generation differs from current generation.
    Generation,
    /// Result's retired generation differs from the current live generation.
    RetiredGeneration,
    /// Result's Leave token differs from the committing token.
    Token,
    /// Stored `Left` order is not phase 1 for the retired participant.
    LeftAdmissionOrder,
}

/// Failure while proving a live member's exact Leave request authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaveVerificationError {
    /// Request names another conversation.
    Conversation,
    /// Request names another participant.
    Participant,
    /// Presented generation is not the current live generation.
    Generation,
    /// Presented attach secret failed the consuming layer's constant-time proof.
    Secret,
}

/// Exact Leave request authority proven against one live member.
pub struct VerifiedLeaveRequest<V, LF> {
    conversation_id: ConversationId,
    participant_id: ParticipantId,
    generation: Generation,
    leave_attempt_token: LeaveAttemptToken,
    leave_request_verifier: V,
    leave_fingerprint: LeaveFingerprint<LF>,
}

/// Allocation fields for settled bound or detached Leave.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaveCommitParameters {
    /// Assigned terminal `Left` record sequence.
    pub left_delivery_seq: DeliverySeq,
}

/// Proof bound to one exact pending finalization and later `Left` major.
#[derive(Debug, PartialEq, Eq)]
struct NoInterveningTupleProof {
    pending_order: AdmissionOrder,
    left_transaction_order: TransactionOrder,
}

impl NoInterveningTupleProof {
    fn matches(&self, pending: PendingFinalization) -> bool {
        self.pending_order == pending.admission_order()
            && self.left_transaction_order > self.pending_order.transaction_order()
    }
}

/// Linear order-lane authority for one exact settled or positional Leave.
///
/// Construction consumes a completely validated [`ClaimFrontiers`] snapshot,
/// removes and relays the exact `X` order handle, and seals the resulting lane
/// inside this value. The frontier cannot be recovered by a caller or reused
/// after a successful commit. Leave returns that authority only inside the
/// indivisible [`LeaveCommit`] beside the resulting tombstone.
///
/// External code cannot implement a planner proof or initialize this authority
/// from raw majors:
///
/// ```compile_fail
/// use liminal_protocol::lifecycle::NoInterveningTuplePlannerProof;
/// ```
///
/// ```compile_fail
/// use liminal_protocol::lifecycle::PreparedLeaveAuthority;
///
/// let _ = PreparedLeaveAuthority { left_transaction_order: 7 };
/// ```
#[derive(Debug, PartialEq, Eq)]
pub struct PreparedLeaveAuthority {
    frontiers: ClaimFrontiers,
    conversation_id: ConversationId,
    participant_id: ParticipantId,
    kind: PreparedLeaveKind,
}

#[derive(Debug, PartialEq, Eq)]
enum PreparedLeaveKind {
    Settled {
        ended_binding_epoch: Option<BindingEpoch>,
        left_transaction_order: TransactionOrder,
    },
    Pending {
        binding_epoch: BindingEpoch,
        no_intervening: NoInterveningTupleProof,
    },
}

impl PreparedLeaveAuthority {
    pub(super) const fn settled(
        frontiers: ClaimFrontiers,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        ended_binding_epoch: Option<BindingEpoch>,
        left_transaction_order: TransactionOrder,
    ) -> Self {
        Self {
            frontiers,
            conversation_id,
            participant_id,
            kind: PreparedLeaveKind::Settled {
                ended_binding_epoch,
                left_transaction_order,
            },
        }
    }

    pub(super) const fn pending(
        frontiers: ClaimFrontiers,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
        pending_order: AdmissionOrder,
        left_transaction_order: TransactionOrder,
    ) -> Self {
        Self {
            frontiers,
            conversation_id,
            participant_id,
            kind: PreparedLeaveKind::Pending {
                binding_epoch,
                no_intervening: NoInterveningTupleProof {
                    pending_order,
                    left_transaction_order,
                },
            },
        }
    }

    fn consume_settled(
        self,
        member_conversation_id: ConversationId,
        member_participant_id: ParticipantId,
        ended_binding_epoch: Option<BindingEpoch>,
    ) -> Result<(ClaimFrontiers, TransactionOrder), LeaveCommitError> {
        let Self {
            frontiers,
            conversation_id,
            participant_id,
            kind,
        } = self;
        let PreparedLeaveKind::Settled {
            ended_binding_epoch: authorized_epoch,
            left_transaction_order,
        } = kind
        else {
            return Err(LeaveCommitError::PreparedAuthority);
        };
        if conversation_id != member_conversation_id
            || participant_id != member_participant_id
            || authorized_epoch != ended_binding_epoch
        {
            return Err(LeaveCommitError::PreparedAuthority);
        }
        Ok((frontiers, left_transaction_order))
    }

    fn consume_pending(
        self,
        pending: PendingFinalization,
    ) -> Result<(ClaimFrontiers, TransactionOrder), LeaveCommitError> {
        let Self {
            frontiers,
            conversation_id,
            participant_id,
            kind,
        } = self;
        let PreparedLeaveKind::Pending {
            binding_epoch,
            no_intervening,
        } = kind
        else {
            return Err(LeaveCommitError::PreparedAuthority);
        };
        if conversation_id != pending.conversation_id()
            || participant_id != pending.participant_id()
            || binding_epoch != pending.binding_epoch()
            || !no_intervening.matches(pending)
        {
            return Err(LeaveCommitError::PreparedAuthority);
        }
        Ok((frontiers, no_intervening.left_transaction_order))
    }
}

/// Allocation and ordering proof for positional pending-terminal Leave.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PendingLeaveCommitParameters {
    /// Real sequence allocated to the pending binding terminal.
    pub terminal_delivery_seq: DeliverySeq,
    /// Real sequence allocated to the following `Left` record.
    pub left_delivery_seq: DeliverySeq,
}

/// Failure while applying an already-authorized Leave transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaveCommitError {
    /// Prepared order authority belongs to another state or Leave mode.
    PreparedAuthority,
    /// Verified request authority was minted for another live member.
    VerifiedAuthority,
    /// Bound or pending-finalization state belongs to another member/generation.
    BindingAuthority,
    /// Settled Leave was called while a binding terminal remains pending.
    PendingTerminalRequiresComposition,
    /// Positional proof does not cover the supplied pending finalization.
    NoInterveningTuple,
    /// A pending detach cell is not paired with explicit-detach finalization.
    PendingDetachState,
    /// Detach cell and retained committed terminal disagree.
    TerminalHistory,
    /// Prior terminal sequence is not strictly before the new `Left` sequence.
    TerminalSequenceOrder,
    /// The supplied record positions do not consume the next gap-free sequence values.
    SequenceAuthority,
    /// Consuming and relaying Leave claims could not produce a valid frontier.
    ResultingFrontier,
    /// Internal tombstone construction rejected an inconsistent result.
    RetirementInvariant(RetirementError),
}

impl<F> LiveMember<F> {
    /// Verifies an exact Leave request against current live credential authority.
    ///
    /// # Errors
    ///
    /// Returns [`LeaveVerificationError`] at the first mismatching authority component.
    pub fn verify_leave_request<V, LF>(
        &self,
        request: &LeaveRequest,
        secret_proof: AttachSecretProof,
        leave_request_verifier: V,
        leave_fingerprint: LeaveFingerprint<LF>,
    ) -> Result<VerifiedLeaveRequest<V, LF>, LeaveVerificationError> {
        if request.conversation_id != self.conversation_id {
            return Err(LeaveVerificationError::Conversation);
        }
        if request.participant_id != self.participant_id {
            return Err(LeaveVerificationError::Participant);
        }
        if request.capability_generation != self.generation {
            return Err(LeaveVerificationError::Generation);
        }
        if secret_proof == AttachSecretProof::Mismatch {
            return Err(LeaveVerificationError::Secret);
        }
        Ok(VerifiedLeaveRequest {
            conversation_id: request.conversation_id,
            participant_id: request.participant_id,
            generation: request.capability_generation,
            leave_attempt_token: request.leave_attempt_token,
            leave_request_verifier,
            leave_fingerprint,
        })
    }

    fn retire<V, LF>(
        self,
        leave_attempt_token: LeaveAttemptToken,
        leave_request_verifier: V,
        leave_fingerprint: LeaveFingerprint<LF>,
        left_admission_order: AdmissionOrder,
        committed_result: LeaveCommitted,
    ) -> Result<RetiredIdentity<F, V, LF>, RetirementError> {
        if committed_result.conversation_id() != self.conversation_id {
            return Err(RetirementError::Conversation);
        }
        if committed_result.participant_id() != self.participant_id {
            return Err(RetirementError::Participant);
        }
        if committed_result.presented_generation() != self.generation {
            return Err(RetirementError::Generation);
        }
        if committed_result.retired_generation() != self.generation {
            return Err(RetirementError::RetiredGeneration);
        }
        if committed_result.leave_attempt_token() != leave_attempt_token {
            return Err(RetirementError::Token);
        }
        if left_admission_order.candidate_phase() != CandidatePhase::MembershipExit
            || left_admission_order.participant_index() != self.participant_id
        {
            return Err(RetirementError::LeftAdmissionOrder);
        }
        Ok(RetiredIdentity {
            participant_id: self.participant_id,
            conversation_id: self.conversation_id,
            retired_generation: committed_result.retired_generation(),
            enrollment_fingerprint: self.enrollment_fingerprint,
            leave_attempt_token,
            leave_request_verifier,
            leave_fingerprint,
            left_admission_order,
            committed_result,
        })
    }
}

/// Commits bound or already-detached Leave, deriving all optional result fields.
///
/// # Errors
///
/// Returns [`LeaveCommitError`] when authority, binding, history/cell, or order
/// is inconsistent. Pending finalization must use [`commit_pending_leave`].
pub fn commit_leave<EF, V, LF, D>(
    member: LiveMember<EF>,
    binding_state: BindingState,
    detach_cell: DetachCell<D>,
    verified: VerifiedLeaveRequest<V, LF>,
    authority: PreparedLeaveAuthority,
    parameters: LeaveCommitParameters,
) -> Result<LeaveCommit<EF, V, LF>, LeaveCommitError> {
    validate_verified(&member, &verified)?;
    let ended_binding_epoch = match binding_state {
        BindingState::Detached => None,
        BindingState::Bound(active) => {
            validate_active(
                &member,
                active.participant_id,
                active.conversation_id,
                active.binding_epoch,
            )?;
            Some(active.binding_epoch)
        }
        BindingState::PendingFinalization(_) => {
            return Err(LeaveCommitError::PendingTerminalRequiresComposition);
        }
    };
    validate_settled_cell(&member, binding_state, &detach_cell)?;
    let (frontiers, left_transaction_order) = authority.consume_settled(
        member.conversation_id,
        member.participant_id,
        ended_binding_epoch,
    )?;
    let prior_terminal_delivery_seq = member
        .latest_terminal
        .map(CommittedBindingTerminal::delivery_seq);
    validate_sequence_order(prior_terminal_delivery_seq, parameters.left_delivery_seq)?;
    finish_leave(
        member,
        verified,
        ended_binding_epoch,
        prior_terminal_delivery_seq,
        None,
        left_transaction_order,
        parameters.left_delivery_seq,
        detach_cell,
        frontiers,
    )
}

/// Positionally commits one pending binding terminal immediately before `Left`.
///
/// A separately drained terminal must first update [`LiveMember`] through its
/// committed terminal and then use ordinary [`commit_leave`].
///
/// # Errors
///
/// Returns [`LeaveCommitError`] when the planner proof, pending state/cell,
/// authority, or allocated sequence order is inconsistent.
pub fn commit_pending_leave<EF, V, LF, D>(
    member: LiveMember<EF>,
    pending: PendingFinalization,
    detach_cell: DetachCell<D>,
    verified: VerifiedLeaveRequest<V, LF>,
    authority: PreparedLeaveAuthority,
    parameters: PendingLeaveCommitParameters,
) -> Result<LeaveCommit<EF, V, LF>, LeaveCommitError> {
    let PendingLeaveCommitParameters {
        terminal_delivery_seq,
        left_delivery_seq,
    } = parameters;
    validate_verified(&member, &verified)?;
    validate_pending(&member, pending)?;
    let (frontiers, left_transaction_order) = authority.consume_pending(pending)?;
    validate_pending_cell(member.conversation_id, pending, &detach_cell)?;
    if terminal_delivery_seq >= left_delivery_seq {
        return Err(LeaveCommitError::TerminalSequenceOrder);
    }
    let committed_terminal = pending.commit(terminal_delivery_seq);
    validate_terminal(
        member.participant_id,
        member.conversation_id,
        member.generation,
        Some(committed_terminal),
    )
    .map_err(|_| LeaveCommitError::BindingAuthority)?;
    finish_leave(
        member,
        verified,
        None,
        Some(committed_terminal.delivery_seq()),
        Some(committed_terminal),
        left_transaction_order,
        left_delivery_seq,
        detach_cell,
        frontiers,
    )
}

fn validate_verified<EF, V, LF>(
    member: &LiveMember<EF>,
    verified: &VerifiedLeaveRequest<V, LF>,
) -> Result<(), LeaveCommitError> {
    if verified.conversation_id != member.conversation_id
        || verified.participant_id != member.participant_id
        || verified.generation != member.generation
    {
        return Err(LeaveCommitError::VerifiedAuthority);
    }
    Ok(())
}

fn validate_active<EF>(
    member: &LiveMember<EF>,
    participant_id: ParticipantId,
    conversation_id: ConversationId,
    binding_epoch: BindingEpoch,
) -> Result<(), LeaveCommitError> {
    if participant_id != member.participant_id
        || conversation_id != member.conversation_id
        || binding_epoch.capability_generation != member.generation
    {
        return Err(LeaveCommitError::BindingAuthority);
    }
    Ok(())
}

fn validate_pending<EF>(
    member: &LiveMember<EF>,
    pending: PendingFinalization,
) -> Result<(), LeaveCommitError> {
    validate_active(
        member,
        pending.participant_id(),
        pending.conversation_id(),
        pending.binding_epoch(),
    )
}

fn validate_sequence_order(
    prior: Option<DeliverySeq>,
    left: DeliverySeq,
) -> Result<(), LeaveCommitError> {
    if prior.is_some_and(|sequence| sequence >= left) {
        return Err(LeaveCommitError::TerminalSequenceOrder);
    }
    Ok(())
}

fn validate_settled_cell<EF, D>(
    member: &LiveMember<EF>,
    binding_state: BindingState,
    detach_cell: &DetachCell<D>,
) -> Result<(), LeaveCommitError> {
    match detach_cell {
        DetachCell::Empty(_) => Ok(()),
        DetachCell::Pending(_) => Err(LeaveCommitError::PendingDetachState),
        DetachCell::Committed(cell) => {
            if binding_state != BindingState::Detached
                || cell.participant_id() != member.participant_id
                || cell.request_generation() != member.generation
            {
                return Err(LeaveCommitError::TerminalHistory);
            }
            let Some(terminal) = member.latest_terminal else {
                return Err(LeaveCommitError::TerminalHistory);
            };
            if terminal.detached_cause() != Some(DetachedCause::CleanDeregister)
                || terminal.binding_epoch() != cell.committed_binding_epoch()
                || terminal.delivery_seq() != cell.detached_delivery_seq()
            {
                return Err(LeaveCommitError::TerminalHistory);
            }
            Ok(())
        }
        DetachCell::Terminalized(cell) => {
            if cell.participant_id() != member.participant_id || member.latest_terminal.is_none() {
                return Err(LeaveCommitError::TerminalHistory);
            }
            Ok(())
        }
    }
}

fn validate_pending_cell<D>(
    conversation_id: ConversationId,
    pending: PendingFinalization,
    detach_cell: &DetachCell<D>,
) -> Result<(), LeaveCommitError> {
    match detach_cell {
        DetachCell::Pending(cell) => validate_pending_pair(
            BindingState::PendingFinalization(pending),
            cell,
            Some(conversation_id),
        )
        .map(|_| ())
        .map_err(|_| LeaveCommitError::PendingDetachState),
        DetachCell::Committed(_) => Err(LeaveCommitError::TerminalHistory),
        DetachCell::Terminalized(cell) if cell.participant_id() != pending.participant_id() => {
            Err(LeaveCommitError::TerminalHistory)
        }
        DetachCell::Empty(_) | DetachCell::Terminalized(_) => Ok(()),
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the final Leave constructor keeps every verified authority and both atomic result halves explicit"
)]
fn finish_leave<EF, V, LF, D>(
    member: LiveMember<EF>,
    verified: VerifiedLeaveRequest<V, LF>,
    ended_binding_epoch: Option<BindingEpoch>,
    prior_terminal_delivery_seq: Option<DeliverySeq>,
    committed_terminal: Option<CommittedBindingTerminal>,
    left_transaction_order: TransactionOrder,
    left_delivery_seq: DeliverySeq,
    detach_cell: DetachCell<D>,
    frontiers: ClaimFrontiers,
) -> Result<LeaveCommit<EF, V, LF>, LeaveCommitError> {
    let VerifiedLeaveRequest {
        conversation_id,
        participant_id,
        generation,
        leave_attempt_token,
        leave_request_verifier,
        leave_fingerprint,
    } = verified;
    if generation != member.generation {
        return Err(LeaveCommitError::VerifiedAuthority);
    }
    let Some(committed_result) = LeaveCommitted::new(
        conversation_id,
        leave_attempt_token,
        participant_id,
        member.generation,
        ended_binding_epoch,
        prior_terminal_delivery_seq,
        left_delivery_seq,
    ) else {
        return Err(LeaveCommitError::TerminalSequenceOrder);
    };
    let frontiers = frontiers.finish_leave_claims(
        participant_id,
        ended_binding_epoch,
        committed_terminal,
        left_delivery_seq,
        left_transaction_order,
    )?;
    let retired = member
        .retire(
            leave_attempt_token,
            leave_request_verifier,
            leave_fingerprint,
            AdmissionOrder::new(
                left_transaction_order,
                CandidatePhase::MembershipExit,
                participant_id,
            ),
            committed_result,
        )
        .map_err(LeaveCommitError::RetirementInvariant)?;
    let _detach_cell_replaced_by_tombstone = detach_cell;
    Ok(LeaveCommit {
        identity: IdentityState::Retired(retired),
        frontiers,
    })
}
