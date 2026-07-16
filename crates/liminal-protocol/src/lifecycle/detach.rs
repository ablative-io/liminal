use crate::wire::{
    BindingEpoch, BindingStateView, ConversationId, DeliverySeq, DetachAttemptToken,
    DetachCommitted, DetachEnvelope, DetachInProgress, DetachRequest, DetachedCause, Generation,
    ObserverBackpressure, ObserverBackpressureState, ObserverEpoch, ParticipantId,
    TerminalizedDetachCell,
};

use super::binding::{
    ActiveBinding, AdmissionOrder, BindingState, CommittedBindingTerminal,
    CommittedBindingTerminalPosition, CommittedDetachedTerminal, PendingBindingTerminalPosition,
    PendingFinalization,
};
use super::membership::{LiveMember, MembershipInvariantError};

/// Empty detach replay cell.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EmptyDetach;

/// Pending detach replay cell whose terminal record is observer-blocked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PendingDetach<V> {
    token: DetachAttemptToken,
    participant_id: ParticipantId,
    request_generation: Generation,
    request_verifier: V,
    committed_binding_epoch: BindingEpoch,
    admission_order: AdmissionOrder,
    refused_epoch: ObserverEpoch,
}

/// Committed detach replay cell with its real Detached record sequence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommittedDetach<V> {
    token: DetachAttemptToken,
    participant_id: ParticipantId,
    request_generation: Generation,
    request_verifier: V,
    committed_binding_epoch: BindingEpoch,
    detached_delivery_seq: DeliverySeq,
}

/// Terminalized detach replay cell retained after a successful attach.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TerminalizedDetach<V> {
    token: DetachAttemptToken,
    participant_id: ParticipantId,
    request_generation: Generation,
    request_verifier: V,
    committed_binding_epoch: BindingEpoch,
}

/// Exact four-variant detach cell mandated by the extraction brief.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DetachCell<V> {
    /// No detach replay state.
    Empty(EmptyDetach),
    /// Accepted detach awaits terminal append.
    Pending(PendingDetach<V>),
    /// Detach committed and is replayable.
    Committed(CommittedDetach<V>),
    /// A later attach consumed binding authority but retained old detach data.
    Terminalized(TerminalizedDetach<V>),
}

impl<V> Default for DetachCell<V> {
    fn default() -> Self {
        Self::Empty(EmptyDetach)
    }
}

pub(super) fn restore_pending_detach<V>(
    token: DetachAttemptToken,
    participant_id: ParticipantId,
    request_generation: Generation,
    request_verifier: V,
    committed_binding_epoch: BindingEpoch,
    admission_order: AdmissionOrder,
    refused_epoch: ObserverEpoch,
) -> Option<PendingDetach<V>> {
    if committed_binding_epoch.capability_generation != request_generation
        || admission_order.participant_index() != participant_id
        || !matches!(
            admission_order.candidate_phase(),
            crate::outcome::CandidatePhase::BindingTerminal
        )
    {
        return None;
    }
    Some(PendingDetach {
        token,
        participant_id,
        request_generation,
        request_verifier,
        committed_binding_epoch,
        admission_order,
        refused_epoch,
    })
}

pub(super) fn restore_committed_detach<V>(
    token: DetachAttemptToken,
    participant_id: ParticipantId,
    request_generation: Generation,
    request_verifier: V,
    committed_binding_epoch: BindingEpoch,
    detached_delivery_seq: DeliverySeq,
) -> Option<CommittedDetach<V>> {
    if committed_binding_epoch.capability_generation != request_generation {
        return None;
    }
    Some(CommittedDetach {
        token,
        participant_id,
        request_generation,
        request_verifier,
        committed_binding_epoch,
        detached_delivery_seq,
    })
}

pub(super) fn restore_terminalized_detach<V>(
    token: DetachAttemptToken,
    participant_id: ParticipantId,
    request_generation: Generation,
    request_verifier: V,
    committed_binding_epoch: BindingEpoch,
) -> Option<TerminalizedDetach<V>> {
    if committed_binding_epoch.capability_generation != request_generation {
        return None;
    }
    Some(TerminalizedDetach {
        token,
        participant_id,
        request_generation,
        request_verifier,
        committed_binding_epoch,
    })
}

impl<V> DetachCell<V> {
    /// Extracts the mandated fourth variant without exposing its fields.
    #[must_use]
    pub fn into_terminalized(self) -> Option<TerminalizedDetach<V>> {
        match self {
            Self::Terminalized(cell) => Some(cell),
            Self::Empty(_) | Self::Pending(_) | Self::Committed(_) => None,
        }
    }
}

impl<V> PendingDetach<V> {
    pub(crate) const fn participant_id(&self) -> ParticipantId {
        self.participant_id
    }

    pub(crate) const fn request_generation(&self) -> Generation {
        self.request_generation
    }

    pub(crate) const fn committed_binding_epoch(&self) -> BindingEpoch {
        self.committed_binding_epoch
    }

    pub(crate) const fn admission_order(&self) -> AdmissionOrder {
        self.admission_order
    }

    pub(crate) const fn refused_epoch(&self) -> ObserverEpoch {
        self.refused_epoch
    }
}

impl<V> CommittedDetach<V> {
    pub(crate) const fn token(&self) -> DetachAttemptToken {
        self.token
    }

    pub(crate) const fn participant_id(&self) -> ParticipantId {
        self.participant_id
    }

    pub(crate) const fn request_generation(&self) -> Generation {
        self.request_generation
    }

    pub(crate) const fn committed_binding_epoch(&self) -> BindingEpoch {
        self.committed_binding_epoch
    }

    pub(crate) const fn detached_delivery_seq(&self) -> DeliverySeq {
        self.detached_delivery_seq
    }
}

/// Authority mismatch between an active binding and detach request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DetachVerificationError {
    /// Request names another conversation.
    Conversation,
    /// Request names another participant.
    Participant,
    /// Request generation differs from the binding epoch.
    Generation,
}

/// Exact-token replay verification failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DetachReplayError {
    /// Token differs from the stored cell token.
    Token,
    /// Participant differs from the stored cell participant.
    Participant,
    /// Generation differs from the stored canonical request.
    Generation,
    /// Canonical non-secret verifier differs.
    RequestVerifier,
    /// Pending-finalization state and detach cell do not describe one commit.
    StatePair,
    /// Live membership does not own the binding terminal being completed.
    MembershipAuthority,
    /// Committed terminal history violates the live membership invariant.
    TerminalHistory(MembershipInvariantError),
}

/// Invalid previous-cell state for accepting a new detach.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DetachCommitError {
    /// An accepted pending detach must be replayed or drained, never replaced.
    PendingCell,
    /// A committed detach cannot coexist with the verified current binding.
    CommittedCell,
    /// Retained terminalized state belongs to another participant or is not older.
    TerminalizedCellAuthority,
    /// Live membership does not own the verified active binding.
    MembershipAuthority,
    /// Committed terminal history violates the live membership invariant.
    TerminalHistory(MembershipInvariantError),
}

/// Result of the mandatory ordered drain attempt on advanced observer progress.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PendingDrainDecision {
    /// Progress equals the stored refusal epoch, so no drain was attempted.
    NotAttempted,
    /// The ordered candidate remains blocked after the required drain attempt.
    StillBlocked,
    /// The ordered candidate committed at this real delivery sequence.
    Committed {
        /// Assigned Detached record sequence.
        detached_delivery_seq: DeliverySeq,
    },
}

/// Invalid pending replay input or paired state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PendingReplayError {
    /// Observer progress regressed below the cell's stored refusal epoch.
    ObserverProgressRegression,
    /// Greater progress was supplied without first attempting the ordered drain.
    DrainRequired,
    /// Equal progress incorrectly claimed that a drain was attempted.
    UnexpectedDrain,
    /// Binding finalization and detach replay cell do not describe one commit.
    StatePair,
    /// Live membership does not own the binding terminal being replayed.
    MembershipAuthority,
    /// Committed terminal history violates the live membership invariant.
    TerminalHistory(MembershipInvariantError),
}

/// Atomic durable result of an immediately committed or drained detach.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedDetachTransition<EF, V> {
    member: LiveMember<EF>,
    terminal: CommittedDetachedTerminal,
    binding_state: BindingState,
    cell: CommittedDetach<V>,
    outcome: DetachCommitted,
}

impl<EF, V> CommittedDetachTransition<EF, V> {
    /// Borrows live membership with the exact binding terminal persisted.
    #[must_use]
    pub const fn member(&self) -> &LiveMember<EF> {
        &self.member
    }

    /// Returns the exact committed `Detached(CleanDeregister)` terminal.
    #[must_use]
    pub const fn terminal(&self) -> CommittedDetachedTerminal {
        self.terminal
    }

    /// Returns the detached post-transition binding slot.
    #[must_use]
    pub const fn binding_state(&self) -> BindingState {
        self.binding_state
    }

    /// Borrows the stable committed detach replay cell.
    #[must_use]
    pub const fn cell(&self) -> &CommittedDetach<V> {
        &self.cell
    }

    /// Borrows the exact canonical committed-detach response.
    #[must_use]
    pub const fn outcome(&self) -> &DetachCommitted {
        &self.outcome
    }

    /// Consumes the atomic result into its persistence and response values.
    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        LiveMember<EF>,
        CommittedDetachedTerminal,
        BindingState,
        CommittedDetach<V>,
        DetachCommitted,
    ) {
        (
            self.member,
            self.terminal,
            self.binding_state,
            self.cell,
            self.outcome,
        )
    }
}

/// Atomic durable result of accepting an observer-blocked detach.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingDetachTransition<EF, V> {
    member: LiveMember<EF>,
    binding_state: BindingState,
    cell: PendingDetach<V>,
    outcome: ObserverBackpressure,
}

impl<EF, V> PendingDetachTransition<EF, V> {
    /// Borrows live membership retained unchanged until the terminal drains.
    #[must_use]
    pub const fn member(&self) -> &LiveMember<EF> {
        &self.member
    }

    /// Returns the cause-partitioned pending-finalization binding slot.
    #[must_use]
    pub const fn binding_state(&self) -> BindingState {
        self.binding_state
    }

    /// Borrows the exact token-bearing pending detach replay cell.
    #[must_use]
    pub const fn cell(&self) -> &PendingDetach<V> {
        &self.cell
    }

    /// Borrows the exact initial observer-backpressure response.
    #[must_use]
    pub const fn outcome(&self) -> &ObserverBackpressure {
        &self.outcome
    }

    /// Consumes the atomic result into its persistence and response values.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        LiveMember<EF>,
        BindingState,
        PendingDetach<V>,
        ObserverBackpressure,
    ) {
        (self.member, self.binding_state, self.cell, self.outcome)
    }
}

/// Atomic durable result of exact-token pending replay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PendingReplay<EF, V> {
    /// Candidate remains pending, either unchanged or rewritten to a newer epoch.
    Pending {
        /// Live membership retained unchanged while the terminal remains pending.
        member: LiveMember<EF>,
        /// Pending-finalization binding state retained atomically.
        binding_state: BindingState,
        /// Complete replacement pending cell.
        cell: PendingDetach<V>,
        /// Observer refusal carrying the cell's current epoch.
        outcome: ObserverBackpressure,
    },
    /// Candidate drained and the cell committed with its real sequence.
    Committed {
        /// Live membership with the exact committed terminal persisted.
        member: LiveMember<EF>,
        /// Cause-partitioned terminal committed by the ordered drain.
        terminal: CommittedDetachedTerminal,
        /// Detached binding state after terminal append.
        binding_state: BindingState,
        /// Stable committed replay cell.
        cell: CommittedDetach<V>,
        /// Stable committed detach response.
        outcome: DetachCommitted,
    },
}

/// Detach request proven to match one active binding.
#[derive(Clone, Debug)]
pub struct VerifiedDetachRequest<V> {
    binding: ActiveBinding,
    request: DetachRequest,
    request_verifier: V,
}

impl ActiveBinding {
    /// Verifies a detach request against this exact binding epoch.
    ///
    /// # Errors
    ///
    /// Returns [`DetachVerificationError`] for the first mismatching authority
    /// component. The caller supplies the canonical request verifier computed
    /// by the consuming server's cryptographic layer.
    pub fn verify_detach_request<V>(
        &self,
        request: DetachRequest,
        request_verifier: V,
    ) -> Result<VerifiedDetachRequest<V>, DetachVerificationError> {
        if request.conversation_id != self.conversation_id {
            return Err(DetachVerificationError::Conversation);
        }
        if request.participant_id != self.participant_id {
            return Err(DetachVerificationError::Participant);
        }
        if request.capability_generation != self.binding_epoch.capability_generation {
            return Err(DetachVerificationError::Generation);
        }
        Ok(VerifiedDetachRequest {
            binding: *self,
            request,
            request_verifier,
        })
    }
}

/// Commits an immediate detach atomically with binding release.
///
/// # Errors
///
/// Returns [`DetachCommitError::PendingCell`] when an accepted pending detach
/// already occupies the identity slot.
pub fn commit_detach<EF, V: Copy + Eq>(
    member: LiveMember<EF>,
    verified: VerifiedDetachRequest<V>,
    previous_cell: DetachCell<V>,
    position: CommittedBindingTerminalPosition,
) -> Result<CommittedDetachTransition<EF, V>, DetachCommitError> {
    validate_previous_cell(&member, &previous_cell)?;
    let VerifiedDetachRequest {
        binding,
        request,
        request_verifier,
    } = verified;
    validate_member_authority(&member, binding)
        .map_err(|_| DetachCommitError::MembershipAuthority)?;
    let terminal = binding.commit_clean_deregister(position);
    let member = member
        .with_committed_terminal(CommittedBindingTerminal::Detached(terminal))
        .map_err(DetachCommitError::TerminalHistory)?;
    let cell = CommittedDetach {
        token: request.detach_attempt_token,
        participant_id: request.participant_id,
        request_generation: request.capability_generation,
        request_verifier,
        committed_binding_epoch: binding.binding_epoch,
        detached_delivery_seq: position.delivery_seq(),
    };
    let outcome = cell.outcome(request.conversation_id);
    Ok(CommittedDetachTransition {
        member,
        terminal,
        binding_state: BindingState::Detached,
        cell,
        outcome,
    })
}

/// Starts an observer-blocked detach with terminal authority already ended.
///
/// The stored refusal epoch is derived from `observer_progress` in this same
/// transition. There is deliberately no independent epoch input, so an initial
/// refusal cannot arm an epoch different from its observed progress baseline.
///
/// # Errors
///
/// Returns [`DetachCommitError::PendingCell`] when an accepted pending detach
/// already occupies the identity slot.
pub fn start_blocked_detach<EF, V: Copy + Eq>(
    member: LiveMember<EF>,
    verified: VerifiedDetachRequest<V>,
    previous_cell: DetachCell<V>,
    position: PendingBindingTerminalPosition,
    observer_progress: DeliverySeq,
) -> Result<PendingDetachTransition<EF, V>, DetachCommitError> {
    validate_previous_cell(&member, &previous_cell)?;
    let VerifiedDetachRequest {
        binding,
        request,
        request_verifier,
    } = verified;
    validate_member_authority(&member, binding)
        .map_err(|_| DetachCommitError::MembershipAuthority)?;
    let pending = binding.pending_clean_deregister(position);
    let admission_order = pending.admission_order();
    let finalization = PendingFinalization::Detached(pending);
    let cell = PendingDetach {
        token: request.detach_attempt_token,
        participant_id: request.participant_id,
        request_generation: request.capability_generation,
        request_verifier,
        committed_binding_epoch: binding.binding_epoch,
        admission_order,
        refused_epoch: observer_progress,
    };
    let outcome = cell.backpressure(binding.conversation_id, observer_progress);
    Ok(PendingDetachTransition {
        member,
        binding_state: BindingState::PendingFinalization(finalization),
        cell,
        outcome,
    })
}

/// Completes one paired pending finalization and detach replay cell.
///
/// # Errors
///
/// Returns [`DetachReplayError::StatePair`] when the two durable states do not
/// describe the same participant, binding epoch, and admission order.
pub fn complete_pending_detach<EF, V: Copy + Eq>(
    member: LiveMember<EF>,
    binding_state: BindingState,
    cell: PendingDetach<V>,
    detached_delivery_seq: DeliverySeq,
) -> Result<CommittedDetachTransition<EF, V>, DetachReplayError> {
    let finalization = validate_pending_pair(binding_state, &cell, None)?;
    validate_member_finalization(&member, finalization)?;
    let PendingFinalization::Detached(pending) = finalization else {
        return Err(DetachReplayError::StatePair);
    };
    let terminal = pending.commit(detached_delivery_seq);
    let member = member
        .with_committed_terminal(CommittedBindingTerminal::Detached(terminal))
        .map_err(DetachReplayError::TerminalHistory)?;
    let committed = cell.commit(detached_delivery_seq);
    let outcome = committed.outcome(finalization.conversation_id());
    Ok(CommittedDetachTransition {
        member,
        terminal,
        binding_state: BindingState::Detached,
        cell: committed,
        outcome,
    })
}

impl<V: Copy + Eq> PendingDetach<V> {
    /// Verifies exact replay against the stored token, request fields, and verifier.
    ///
    /// # Errors
    ///
    /// Returns [`DetachReplayError`] at the first mismatch.
    pub fn verify_exact(
        &self,
        request: &DetachRequest,
        request_verifier: V,
    ) -> Result<VerifiedPendingDetach<'_, V>, DetachReplayError> {
        verify_stored_request(
            self.token,
            self.participant_id,
            self.request_generation,
            &self.request_verifier,
            request,
            &request_verifier,
        )?;
        Ok(VerifiedPendingDetach { state: self })
    }

    /// Produces the different-token result without exposing the stored token.
    #[must_use]
    pub const fn competing_attempt(
        &self,
        conversation_id: ConversationId,
        presented_token: DetachAttemptToken,
        presented_generation: Generation,
    ) -> DetachInProgress {
        DetachInProgress {
            conversation_id,
            participant_id: self.participant_id,
            presented_token,
            presented_generation,
            committed_binding_epoch: self.committed_binding_epoch,
        }
    }

    const fn backpressure(
        &self,
        conversation_id: ConversationId,
        observer_progress: DeliverySeq,
    ) -> ObserverBackpressure {
        ObserverBackpressure::Detach {
            request: DetachEnvelope {
                conversation_id,
                participant_id: self.participant_id(),
                capability_generation: self.request_generation(),
                detach_attempt_token: self.token,
            },
            committed_binding_epoch: self.committed_binding_epoch(),
            state: ObserverBackpressureState::initial(observer_progress),
        }
    }

    const fn commit(self, detached_delivery_seq: DeliverySeq) -> CommittedDetach<V> {
        CommittedDetach {
            token: self.token,
            participant_id: self.participant_id,
            request_generation: self.request_generation,
            request_verifier: self.request_verifier,
            committed_binding_epoch: self.committed_binding_epoch,
            detached_delivery_seq,
        }
    }

    pub(crate) const fn terminalize_after_attach(self) -> TerminalizedDetach<V> {
        TerminalizedDetach {
            token: self.token,
            participant_id: self.participant_id,
            request_generation: self.request_generation,
            request_verifier: self.request_verifier,
            committed_binding_epoch: self.committed_binding_epoch,
        }
    }
}

/// Exact verified view of a pending detach cell.
#[derive(Clone, Copy, Debug)]
pub struct VerifiedPendingDetach<'a, V> {
    state: &'a PendingDetach<V>,
}

impl<V: Copy + Eq> VerifiedPendingDetach<'_, V> {
    /// Prepares exact pending replay after request-verifier matching.
    #[must_use]
    pub const fn prepare_replay(
        self,
        conversation_id: ConversationId,
        binding_state: BindingState,
        observer_progress: DeliverySeq,
    ) -> PendingReplayRequest<V> {
        PendingReplayRequest {
            conversation_id,
            binding_state,
            cell: *self.state,
            observer_progress,
        }
    }
}

/// Verified command that forces pending replay through the progress/drain rule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingReplayRequest<V> {
    conversation_id: ConversationId,
    binding_state: BindingState,
    cell: PendingDetach<V>,
    observer_progress: DeliverySeq,
}

impl<V: Copy + Eq> PendingReplayRequest<V> {
    /// Applies the exact equality-or-greater observer-progress transition.
    ///
    /// # Errors
    ///
    /// Returns [`PendingReplayError`] when progress regresses, the caller skips
    /// the required drain, claims a drain at equal progress, or supplies a
    /// mismatched binding finalization.
    pub fn apply<EF>(
        self,
        member: LiveMember<EF>,
        decision: PendingDrainDecision,
    ) -> Result<PendingReplay<EF, V>, PendingReplayError> {
        let finalization =
            validate_pending_pair(self.binding_state, &self.cell, Some(self.conversation_id))
                .map_err(|_| PendingReplayError::StatePair)?;
        validate_member_finalization(&member, finalization)
            .map_err(|_| PendingReplayError::MembershipAuthority)?;
        if self.observer_progress < self.cell.refused_epoch() {
            return Err(PendingReplayError::ObserverProgressRegression);
        }
        if self.observer_progress == self.cell.refused_epoch() {
            if decision != PendingDrainDecision::NotAttempted {
                return Err(PendingReplayError::UnexpectedDrain);
            }
            let outcome = self
                .cell
                .backpressure(self.conversation_id, self.observer_progress);
            return Ok(PendingReplay::Pending {
                member,
                binding_state: self.binding_state,
                cell: self.cell,
                outcome,
            });
        }

        match decision {
            PendingDrainDecision::NotAttempted => Err(PendingReplayError::DrainRequired),
            PendingDrainDecision::StillBlocked => {
                let mut cell = self.cell;
                cell.refused_epoch = self.observer_progress;
                let outcome = cell.backpressure(self.conversation_id, self.observer_progress);
                Ok(PendingReplay::Pending {
                    member,
                    binding_state: self.binding_state,
                    cell,
                    outcome,
                })
            }
            PendingDrainDecision::Committed {
                detached_delivery_seq,
            } => complete_pending_detach(
                member,
                self.binding_state,
                self.cell,
                detached_delivery_seq,
            )
            .map(|transition| PendingReplay::Committed {
                member: transition.member,
                terminal: transition.terminal,
                binding_state: transition.binding_state,
                cell: transition.cell,
                outcome: transition.outcome,
            })
            .map_err(map_pending_replay_error),
        }
    }
}

const fn map_pending_replay_error(error: DetachReplayError) -> PendingReplayError {
    match error {
        DetachReplayError::MembershipAuthority => PendingReplayError::MembershipAuthority,
        DetachReplayError::TerminalHistory(error) => PendingReplayError::TerminalHistory(error),
        DetachReplayError::Token
        | DetachReplayError::Participant
        | DetachReplayError::Generation
        | DetachReplayError::RequestVerifier
        | DetachReplayError::StatePair => PendingReplayError::StatePair,
    }
}

pub(super) fn validate_pending_pair<V>(
    binding_state: BindingState,
    cell: &PendingDetach<V>,
    expected_conversation_id: Option<ConversationId>,
) -> Result<PendingFinalization, DetachReplayError> {
    let BindingState::PendingFinalization(finalization) = binding_state else {
        return Err(DetachReplayError::StatePair);
    };
    let conversation_mismatch = expected_conversation_id
        .is_some_and(|conversation_id| finalization.conversation_id() != conversation_id);
    let participant_mismatch = finalization.participant_id() != cell.participant_id();
    let epoch_mismatch = finalization.binding_epoch() != cell.committed_binding_epoch();
    let order_mismatch = finalization.admission_order() != cell.admission_order();
    let cause_mismatch = finalization.detached_cause() != Some(DetachedCause::CleanDeregister);
    if conversation_mismatch
        || participant_mismatch
        || epoch_mismatch
        || order_mismatch
        || cause_mismatch
    {
        return Err(DetachReplayError::StatePair);
    }
    Ok(finalization)
}

fn validate_member_authority<EF>(
    member: &LiveMember<EF>,
    binding: ActiveBinding,
) -> Result<(), DetachReplayError> {
    if member.participant_id() != binding.participant_id
        || member.conversation_id() != binding.conversation_id
        || member.generation() != binding.binding_epoch.capability_generation
    {
        return Err(DetachReplayError::MembershipAuthority);
    }
    Ok(())
}

fn validate_previous_cell<EF, V>(
    member: &LiveMember<EF>,
    cell: &DetachCell<V>,
) -> Result<(), DetachCommitError> {
    match cell {
        DetachCell::Empty(_) => Ok(()),
        DetachCell::Pending(_) => Err(DetachCommitError::PendingCell),
        DetachCell::Committed(_) => Err(DetachCommitError::CommittedCell),
        DetachCell::Terminalized(cell) => {
            let participant_mismatch = cell.participant_id() != member.participant_id();
            let generation_is_not_older = cell.request_generation() >= member.generation();
            if participant_mismatch || generation_is_not_older {
                Err(DetachCommitError::TerminalizedCellAuthority)
            } else {
                Ok(())
            }
        }
    }
}

fn validate_member_finalization<EF>(
    member: &LiveMember<EF>,
    finalization: PendingFinalization,
) -> Result<(), DetachReplayError> {
    if member.participant_id() != finalization.participant_id()
        || member.conversation_id() != finalization.conversation_id()
        || member.generation() != finalization.binding_epoch().capability_generation
    {
        return Err(DetachReplayError::MembershipAuthority);
    }
    Ok(())
}

impl<V: Copy + Eq> CommittedDetach<V> {
    /// Verifies exact replay against the stored token, request fields, and verifier.
    ///
    /// # Errors
    ///
    /// Returns [`DetachReplayError`] at the first mismatch.
    pub fn verify_exact(
        &self,
        request: &DetachRequest,
        request_verifier: V,
    ) -> Result<VerifiedCommittedDetach<'_, V>, DetachReplayError> {
        verify_stored_request(
            self.token,
            self.participant_id,
            self.request_generation,
            &self.request_verifier,
            request,
            &request_verifier,
        )?;
        Ok(VerifiedCommittedDetach { state: self })
    }

    pub(crate) const fn terminalize_after_attach(self) -> TerminalizedDetach<V> {
        TerminalizedDetach {
            token: self.token,
            participant_id: self.participant_id,
            request_generation: self.request_generation,
            request_verifier: self.request_verifier,
            committed_binding_epoch: self.committed_binding_epoch,
        }
    }

    const fn outcome(self, conversation_id: ConversationId) -> DetachCommitted {
        DetachCommitted::new(
            conversation_id,
            self.participant_id,
            self.token,
            self.committed_binding_epoch,
            self.detached_delivery_seq,
        )
    }
}

/// Exact verified view of a committed detach cell.
#[derive(Clone, Copy, Debug)]
pub struct VerifiedCommittedDetach<'a, V> {
    state: &'a CommittedDetach<V>,
}

impl<V: Copy + Eq> VerifiedCommittedDetach<'_, V> {
    /// Replays the stable committed detach result.
    #[must_use]
    pub const fn outcome(self, conversation_id: ConversationId) -> DetachCommitted {
        self.state.outcome(conversation_id)
    }
}

impl<V> TerminalizedDetach<V> {
    pub(crate) const fn token(&self) -> DetachAttemptToken {
        self.token
    }

    pub(crate) const fn participant_id(&self) -> ParticipantId {
        self.participant_id
    }

    pub(crate) const fn request_generation(&self) -> Generation {
        self.request_generation
    }

    pub(crate) const fn committed_binding_epoch(&self) -> BindingEpoch {
        self.committed_binding_epoch
    }

    /// Verifies an old exact request against the retained terminalized cell.
    ///
    /// # Errors
    ///
    /// Returns [`DetachReplayError`] at the first mismatch. Exact token without
    /// the stored verifier is insufficient.
    pub fn verify_exact(
        &self,
        request: &DetachRequest,
        request_verifier: V,
    ) -> Result<VerifiedTerminalizedDetach<'_, V>, DetachReplayError>
    where
        V: Copy + Eq,
    {
        verify_stored_request(
            self.token,
            self.participant_id,
            self.request_generation,
            &self.request_verifier,
            request,
            &request_verifier,
        )?;
        Ok(VerifiedTerminalizedDetach { state: self })
    }
}

/// Exact verified view whose receiver is the sole state-derived constructor for
/// [`TerminalizedDetachCell`].
#[derive(Clone, Copy, Debug)]
pub struct VerifiedTerminalizedDetach<'a, V> {
    state: &'a TerminalizedDetach<V>,
}

impl<V> VerifiedTerminalizedDetach<'_, V> {
    /// Constructs the terminalized old-cell authority response.
    #[must_use]
    pub const fn outcome(
        self,
        conversation_id: ConversationId,
        current_generation: Generation,
        binding_state: BindingStateView,
    ) -> TerminalizedDetachCell {
        TerminalizedDetachCell::from_terminalized_state(
            self.state,
            conversation_id,
            current_generation,
            binding_state,
        )
    }
}

fn verify_stored_request<V: Eq>(
    token: DetachAttemptToken,
    participant_id: ParticipantId,
    request_generation: Generation,
    stored_verifier: &V,
    request: &DetachRequest,
    request_verifier: &V,
) -> Result<(), DetachReplayError> {
    if request.detach_attempt_token != token {
        return Err(DetachReplayError::Token);
    }
    if request.participant_id != participant_id {
        return Err(DetachReplayError::Participant);
    }
    if request.capability_generation != request_generation {
        return Err(DetachReplayError::Generation);
    }
    if request_verifier != stored_verifier {
        return Err(DetachReplayError::RequestVerifier);
    }
    Ok(())
}
