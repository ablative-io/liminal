use crate::wire::{
    BindingEpoch, BindingStateView, CloseCause, ConversationId, DeliverySeq, DetachAttemptToken,
    DetachCommitted, DetachEnvelope, DetachInProgress, DetachRequest, Generation,
    ObserverBackpressure, ObserverBackpressureState, ObserverEpoch, ParticipantId,
    TerminalizedDetachCell,
};

use super::binding::{
    ActiveBinding, AdmissionOrder, BindingState, BindingTerminalKind, PendingFinalization,
};

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
}

/// Detach request proven to match one active binding.
#[derive(Clone, Debug)]
pub struct VerifiedDetachRequest<V> {
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
            request,
            request_verifier,
        })
    }
}

/// Commits an immediate detach atomically with binding release.
#[must_use]
pub const fn commit_detach<V: Copy + Eq>(
    binding: ActiveBinding,
    verified: VerifiedDetachRequest<V>,
    detached_delivery_seq: DeliverySeq,
) -> (BindingState, CommittedDetach<V>, DetachCommitted) {
    let VerifiedDetachRequest {
        request,
        request_verifier,
    } = verified;
    let cell = CommittedDetach {
        token: request.detach_attempt_token,
        participant_id: request.participant_id,
        request_generation: request.capability_generation,
        request_verifier,
        committed_binding_epoch: binding.binding_epoch,
        detached_delivery_seq,
    };
    let outcome = cell.outcome(request.conversation_id);
    (BindingState::Detached, cell, outcome)
}

/// Starts an observer-blocked detach with terminal authority already ended.
#[must_use]
pub const fn start_blocked_detach<V: Copy + Eq>(
    binding: ActiveBinding,
    verified: VerifiedDetachRequest<V>,
    admission_order: AdmissionOrder,
    refused_epoch: ObserverEpoch,
    observer_progress: DeliverySeq,
) -> (PendingFinalization, PendingDetach<V>, ObserverBackpressure) {
    let VerifiedDetachRequest {
        request,
        request_verifier,
    } = verified;
    let finalization = PendingFinalization {
        participant_id: binding.participant_id,
        conversation_id: binding.conversation_id,
        binding_epoch: binding.binding_epoch,
        original_cause: CloseCause::CleanDeregister,
        event_kind: BindingTerminalKind::Detached,
        admission_order,
    };
    let cell = PendingDetach {
        token: request.detach_attempt_token,
        participant_id: request.participant_id,
        request_generation: request.capability_generation,
        request_verifier,
        committed_binding_epoch: binding.binding_epoch,
        admission_order,
        refused_epoch,
    };
    let outcome = cell.backpressure(binding.conversation_id, observer_progress);
    (finalization, cell, outcome)
}

/// Completes one paired pending finalization and detach replay cell.
///
/// # Errors
///
/// Returns [`DetachReplayError::StatePair`] when the two durable states do not
/// describe the same participant, binding epoch, and admission order.
pub fn complete_pending_detach<V: Copy + Eq>(
    finalization: PendingFinalization,
    cell: PendingDetach<V>,
    detached_delivery_seq: DeliverySeq,
) -> Result<(BindingState, CommittedDetach<V>, DetachCommitted), DetachReplayError> {
    let participant_mismatch = finalization.participant_id != cell.participant_id;
    let epoch_mismatch = finalization.binding_epoch != cell.committed_binding_epoch;
    let order_mismatch = finalization.admission_order != cell.admission_order;
    let kind_mismatch = finalization.event_kind != BindingTerminalKind::Detached;
    if participant_mismatch || epoch_mismatch || order_mismatch || kind_mismatch {
        return Err(DetachReplayError::StatePair);
    }
    let committed = cell.commit(detached_delivery_seq);
    let outcome = committed.outcome(finalization.conversation_id);
    Ok((BindingState::Detached, committed, outcome))
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
                participant_id: self.participant_id,
                capability_generation: self.request_generation,
                detach_attempt_token: self.token,
            },
            committed_binding_epoch: self.committed_binding_epoch,
            state: ObserverBackpressureState {
                backpressure_epoch: self.refused_epoch,
                observer_progress,
            },
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
}

/// Exact verified view of a pending detach cell.
#[derive(Clone, Copy, Debug)]
pub struct VerifiedPendingDetach<'a, V> {
    state: &'a PendingDetach<V>,
}

impl<V: Copy + Eq> VerifiedPendingDetach<'_, V> {
    /// Replays the current observer refusal without allocating a sequence.
    #[must_use]
    pub const fn outcome(
        self,
        conversation_id: ConversationId,
        observer_progress: DeliverySeq,
    ) -> ObserverBackpressure {
        self.state.backpressure(conversation_id, observer_progress)
    }
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

    /// Successful attach consumes committed replay authority while retaining the
    /// exact old token, verifier, generation, and binding epoch.
    #[must_use]
    pub const fn terminalize(self) -> TerminalizedDetach<V> {
        TerminalizedDetach {
            token: self.token,
            participant_id: self.participant_id,
            request_generation: self.request_generation,
            request_verifier: self.request_verifier,
            committed_binding_epoch: self.committed_binding_epoch,
        }
    }

    const fn outcome(self, conversation_id: ConversationId) -> DetachCommitted {
        DetachCommitted {
            conversation_id,
            participant_id: self.participant_id,
            capability_generation: self.request_generation,
            detach_attempt_token: self.token,
            committed_binding_epoch: self.committed_binding_epoch,
            detached_delivery_seq: self.detached_delivery_seq,
        }
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
