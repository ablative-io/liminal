use alloc::boxed::Box;

use crate::wire::{
    AttachBound, AttachSecret, BindingEpoch, CredentialAttachRequest, DeliverySeq, Generation,
};

use super::{
    ActiveBinding, AttachedLifecycleRecord, AttachedRecordPosition, BindingOrigin, BindingState,
    ClosureState, CommittedBindingTerminal, CommittedBindingTerminalPosition,
    CommittedDetachedTerminal, CommittedDiedTerminal, DetachCell, Event, FencedAttachCommit,
    LiveMember, MembershipInvariantError, ObserverProgressProjection, OrdinaryBindingAuthority,
    OrdinaryBindingFate, OrdinaryDetachedAttachAdmission, PendingFinalization,
    RecoveredBindingFate, detach::validate_pending_pair, lookup::AttachSecretProof,
};

/// Result allocation owned by one successful credential-attach transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AttachCommitParameters {
    /// Newly committed binding epoch.
    pub binding: ActiveBinding,
    /// Newly minted result attach secret.
    pub attach_secret: AttachSecret,
    /// Assigned `Attached` lifecycle record position.
    pub attached_position: AttachedRecordPosition,
    /// Live receipt deadline.
    pub receipt_expires_at: u128,
    /// Provenance deadline.
    pub provenance_expires_at: u128,
}

/// Failure while proving credential-attach authority before commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachVerificationError {
    /// Request names another conversation.
    Conversation,
    /// Request names another participant.
    Participant,
    /// Request generation is not the current live generation.
    Generation,
    /// Presented attach secret failed the constant-time verifier.
    Secret,
    /// Current binding state cannot execute the selected attach mode.
    BindingState,
    /// Ordinary/superseding attach presented a marker, or fenced proof differed.
    MarkerProof,
    /// Fenced proof names another participant or binding epoch.
    RecoveryAuthority,
    /// Pending finalization lacks a real terminal sequence or appears in another mode.
    PendingTerminalSequence,
    /// Result binding does not name this member.
    ResultBinding,
    /// Current generation is exhausted or result generation is not its successor.
    ResultGeneration,
    /// Supersession terminal and Attached record do not share one transaction major.
    LifecycleOrder,
    /// Retained committed-terminal history cannot prove the detached recovery epoch.
    TerminalHistory,
}

/// Failure while atomically applying a previously verified attach.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachCommitError {
    /// A pending detach is not paired with fenced pending-finalization commit.
    PendingDetach,
    /// Detach cell belongs to another participant or generation.
    DetachCellAuthority,
    /// Binding finalization and pending detach cell do not describe one commit.
    BindingCellState,
    /// Committed detach cell and retained terminal history disagree.
    TerminalHistory,
    /// Rotated membership rejected the committed terminal.
    MembershipInvariant(MembershipInvariantError),
    /// Canonical attach receipt rejected the verified transaction.
    ReceiptInvariant,
}

/// Binding-terminal effect selected by one successful attach.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachTransition {
    /// Already-detached member bound without creating another terminal.
    Detached,
    /// Old active epoch committed `Detached(Superseded)` in the handoff.
    Superseded {
        /// Exact old terminal including its real delivery sequence.
        terminal: CommittedDetachedTerminal,
    },
    /// Marker-fenced recovery accepted the delivered marker atomically.
    FencedRecovery {
        /// Exact dead epoch that durably received the marker.
        prior_binding_epoch: BindingEpoch,
        /// Terminal committed from pending state in this transaction, if any.
        composed_terminal: Option<CommittedBindingTerminal>,
        /// Closure successor restricted to clear, projection, or compaction.
        next_closure_state: ClosureState,
    },
}

/// Complete atomic result of a credential attach.
#[derive(Debug, PartialEq, Eq)]
pub struct AttachCommit<F, V> {
    /// Rotated membership with preserved or replaced terminal history.
    pub member: LiveMember<F>,
    /// Exact new authoritative binding.
    pub binding_state: BindingState,
    /// Fix 1 detach cell after terminalization/preservation.
    pub detach_cell: DetachCell<V>,
    /// Exact committed phase-2 `Attached` lifecycle record.
    pub attached: AttachedLifecycleRecord,
    /// Exact wire success payload.
    pub outcome: AttachBound,
    /// Typed old-binding or fenced-recovery terminal effect.
    pub transition: AttachTransition,
    binding_origin: BindingOrigin,
    ordinary_binding_authority: Option<OrdinaryBindingAuthority>,
    recovered_binding_authority: Option<FencedAttachCommit>,
}

/// Operational attach state separated from occurrence authority exactly once.
#[derive(Debug, PartialEq, Eq)]
pub struct InstalledAttachState<F, V> {
    /// Rotated membership state.
    pub member: LiveMember<F>,
    /// Newly authoritative binding state.
    pub binding_state: BindingState,
    /// Detach-cell state after attach.
    pub detach_cell: DetachCell<V>,
    /// Committed Attached lifecycle record.
    pub attached: AttachedLifecycleRecord,
    /// Wire success payload.
    pub outcome: AttachBound,
    /// Typed terminal/attach transition audit.
    pub transition: AttachTransition,
    binding_origin: BindingOrigin,
}

/// One move-only binding-fate authority emitted by an attach commit.
#[derive(Debug, PartialEq, Eq)]
pub struct SealedBindingFateToken {
    ordinary: Option<OrdinaryBindingAuthority>,
    recovered: Option<FencedAttachCommit>,
}

impl SealedBindingFateToken {
    /// Reports whether this token carries recovered occurrence authority.
    #[must_use]
    pub const fn is_recovered(&self) -> bool {
        self.recovered.is_some()
    }

    /// Consumes recovered authority into one fate.
    ///
    /// # Errors
    ///
    /// Returns the same move-only token on refusal, boxed to keep the successful
    /// return path compact.
    pub fn recovered_binding_fate(
        mut self,
        event: Event,
    ) -> Result<RecoveredBindingFate, Box<Self>> {
        let Some(proof) = self.recovered.take() else {
            return Err(Box::new(self));
        };
        match proof.recovered_binding_fate(event) {
            Ok(fate) => Ok(fate),
            Err(proof) => {
                self.recovered = Some(*proof);
                Err(Box::new(self))
            }
        }
    }
}

impl<F, V> AttachCommit<F, V> {
    /// Consumes this commit into operational state and exactly one fate token.
    #[must_use]
    pub fn into_slot_and_fate(self) -> (InstalledAttachState<F, V>, SealedBindingFateToken) {
        let Self {
            member,
            binding_state,
            detach_cell,
            attached,
            outcome,
            transition,
            binding_origin,
            ordinary_binding_authority,
            recovered_binding_authority,
        } = self;
        (
            InstalledAttachState {
                member,
                binding_state,
                detach_cell,
                attached,
                outcome,
                transition,
                binding_origin,
            },
            SealedBindingFateToken {
                ordinary: ordinary_binding_authority,
                recovered: recovered_binding_authority,
            },
        )
    }

    /// Projects the binding-ending terminal committed by this attach, if any.
    #[must_use]
    pub fn observer_progress_projection(&self) -> Option<ObserverProgressProjection> {
        let terminal = match self.transition {
            AttachTransition::Detached
            | AttachTransition::FencedRecovery {
                composed_terminal: None,
                ..
            } => return None,
            AttachTransition::Superseded { terminal } => terminal.into(),
            AttachTransition::FencedRecovery {
                composed_terminal: Some(terminal),
                ..
            } => terminal,
        };
        Some(ObserverProgressProjection::new(
            terminal.conversation_id(),
            terminal.delivery_seq(),
        ))
    }

    /// Consumes one exact normal-ack event into this ordinary attach's cursor authority.
    ///
    /// Fenced recovery has no ordinary authority and therefore returns the
    /// unchanged commit. The raw authority never crosses the public boundary.
    ///
    /// # Errors
    ///
    /// Returns the unchanged commit for fenced recovery or when the event names
    /// another participant, epoch, previous cursor, or event class.
    pub fn ordinary_cursor_progressed(mut self, event: Event) -> Result<Self, Box<Self>> {
        let Some(authority) = self.ordinary_binding_authority.take() else {
            return Err(Box::new(self));
        };
        match authority.cursor_progressed(event) {
            Ok(authority) => {
                self.ordinary_binding_authority = Some(authority);
                Ok(self)
            }
            Err(authority) => {
                self.ordinary_binding_authority = Some(authority);
                Err(Box::new(self))
            }
        }
    }

    /// Consumes this ordinary attach and its exact durable death into cursor-release fate.
    ///
    /// A fenced attach cannot enter this path because it carries no ordinary
    /// binding authority; recovered epochs still require the distinct
    /// [`FencedAttachCommit`] provenance transition.
    ///
    /// # Errors
    ///
    /// Returns the unchanged commit for fenced recovery or unless the terminal
    /// names this exact ordinary participant, conversation, and binding epoch.
    pub fn ordinary_binding_fate(
        mut self,
        terminal: CommittedDiedTerminal,
        resulting_floor: DeliverySeq,
    ) -> Result<OrdinaryBindingFate, Box<Self>> {
        let Some(authority) = self.ordinary_binding_authority.take() else {
            return Err(Box::new(self));
        };
        match authority.binding_fate(terminal, resulting_floor) {
            Ok(fate) => Ok(fate),
            Err(authority) => {
                self.ordinary_binding_authority = Some(authority);
                Err(Box::new(self))
            }
        }
    }

    /// Returns no-marker fate authority only for a committed ordinary attach.
    ///
    /// Fenced recovery returns `None`; its recovered epoch must instead use the
    /// [`FencedAttachCommit`] proof that authorized that commit.
    #[must_use]
    #[allow(
        dead_code,
        reason = "the crate-owned binding-fate operation consumes this sealed attach authority"
    )]
    pub(crate) const fn ordinary_binding_authority(&self) -> Option<OrdinaryBindingAuthority> {
        self.ordinary_binding_authority
    }

    /// Returns the opaque binding origin emitted from the verified attach mode.
    #[must_use]
    #[allow(
        dead_code,
        reason = "the crate-owned event replay boundary persists this producer-emitted origin"
    )]
    pub(crate) const fn binding_origin(&self) -> BindingOrigin {
        self.binding_origin
    }
}

#[derive(Debug)]
enum AttachMode {
    Detached(OrdinaryDetachedAttachAdmission),
    Superseded(CommittedDetachedTerminal),
    Fenced {
        proof: FencedAttachCommit,
        pending: Option<(PendingFinalization, DeliverySeq)>,
    },
}

/// Successful attach proof; fields are private so only mode verification mints it.
#[derive(Debug)]
pub struct VerifiedAttachCommit<F> {
    member: LiveMember<F>,
    request: CredentialAttachRequest,
    parameters: AttachCommitParameters,
    mode: AttachMode,
}

/// Fenced verification refusal that preserves both consumed authorities.
#[derive(Debug)]
pub struct FencedAttachVerificationRefusal<F> {
    member: LiveMember<F>,
    proof: FencedAttachCommit,
    error: AttachVerificationError,
}

impl<F> FencedAttachVerificationRefusal<F> {
    /// Returns the typed verification error.
    #[must_use]
    pub const fn error(&self) -> AttachVerificationError {
        self.error
    }

    /// Returns the unchanged member and proof for same-lock serial retry.
    #[must_use]
    pub fn into_parts(self) -> (LiveMember<F>, FencedAttachCommit) {
        (self.member, self.proof)
    }
}

impl<F> LiveMember<F> {
    /// Verifies an ordinary attach from an already detached member.
    ///
    /// # Errors
    ///
    /// Returns [`AttachVerificationError`] for authority, marker, state, or allocation mismatch.
    pub fn verify_detached_attach(
        self,
        binding_state: BindingState,
        closure_admission: OrdinaryDetachedAttachAdmission,
        request: CredentialAttachRequest,
        secret_proof: AttachSecretProof,
        parameters: AttachCommitParameters,
    ) -> Result<VerifiedAttachCommit<F>, AttachVerificationError> {
        self.verify_attach_common(&request, secret_proof, &parameters)?;
        if binding_state != BindingState::Detached {
            return Err(AttachVerificationError::BindingState);
        }
        if request.accept_marker_delivery_seq.is_some() {
            return Err(AttachVerificationError::MarkerProof);
        }
        Ok(VerifiedAttachCommit {
            member: self,
            request,
            parameters,
            mode: AttachMode::Detached(closure_admission),
        })
    }

    /// Verifies an attach that atomically supersedes this member's active epoch.
    ///
    /// # Errors
    ///
    /// Returns [`AttachVerificationError`] when authority, marker absence,
    /// handoff ordering, or result allocation is inconsistent.
    pub fn verify_superseding_attach(
        self,
        active_binding: ActiveBinding,
        request: CredentialAttachRequest,
        secret_proof: AttachSecretProof,
        terminal_position: CommittedBindingTerminalPosition,
        parameters: AttachCommitParameters,
    ) -> Result<VerifiedAttachCommit<F>, AttachVerificationError> {
        self.verify_attach_common(&request, secret_proof, &parameters)?;
        if active_binding.conversation_id != self.conversation_id()
            || active_binding.participant_id != self.participant_id()
            || active_binding.binding_epoch.capability_generation != self.generation()
        {
            return Err(AttachVerificationError::BindingState);
        }
        if request.accept_marker_delivery_seq.is_some() {
            return Err(AttachVerificationError::MarkerProof);
        }
        if terminal_position.transaction_order() != parameters.attached_position.transaction_order()
        {
            return Err(AttachVerificationError::LifecycleOrder);
        }
        let terminal = active_binding.superseded(terminal_position);
        Ok(VerifiedAttachCommit {
            member: self,
            request,
            parameters,
            mode: AttachMode::Superseded(terminal),
        })
    }

    /// Verifies marker-fenced recovery proven by the closure-edge transition.
    ///
    /// # Errors
    ///
    /// Returns [`AttachVerificationError`] when credential authority,
    /// marker/epoch proof, pending-terminal shape, or result allocation differs.
    pub fn verify_fenced_attach(
        self,
        binding_state: BindingState,
        request: CredentialAttachRequest,
        secret_proof: AttachSecretProof,
        proof: FencedAttachCommit,
        pending_terminal_delivery_seq: Option<DeliverySeq>,
        parameters: AttachCommitParameters,
    ) -> Result<VerifiedAttachCommit<F>, Box<FencedAttachVerificationRefusal<F>>> {
        let pending = match self.validate_fenced_attach(
            binding_state,
            &request,
            secret_proof,
            &proof,
            pending_terminal_delivery_seq,
            &parameters,
        ) {
            Ok(pending) => pending,
            Err(error) => {
                return Err(Box::new(FencedAttachVerificationRefusal {
                    member: self,
                    proof,
                    error,
                }));
            }
        };
        Ok(VerifiedAttachCommit {
            member: self,
            request,
            parameters,
            mode: AttachMode::Fenced { proof, pending },
        })
    }

    fn validate_fenced_attach(
        &self,
        binding_state: BindingState,
        request: &CredentialAttachRequest,
        secret_proof: AttachSecretProof,
        proof: &FencedAttachCommit,
        pending_terminal_delivery_seq: Option<DeliverySeq>,
        parameters: &AttachCommitParameters,
    ) -> Result<Option<(PendingFinalization, DeliverySeq)>, AttachVerificationError> {
        self.verify_attach_common(request, secret_proof, parameters)?;
        if request.accept_marker_delivery_seq != Some(proof.marker_delivery_seq()) {
            return Err(AttachVerificationError::MarkerProof);
        }
        if proof.participant_id() != self.participant_id()
            || proof.prior_binding_epoch().capability_generation != self.generation()
            || proof.new_binding_epoch() != parameters.binding.binding_epoch
        {
            return Err(AttachVerificationError::RecoveryAuthority);
        }
        match (binding_state, pending_terminal_delivery_seq) {
            (BindingState::Detached, None) => {
                if self
                    .latest_terminal()
                    .is_none_or(|terminal| terminal.binding_epoch() != proof.prior_binding_epoch())
                {
                    return Err(AttachVerificationError::TerminalHistory);
                }
                Ok(None)
            }
            (BindingState::PendingFinalization(finalization), Some(sequence)) => {
                let same_conversation = finalization.conversation_id() == self.conversation_id();
                let same_participant = finalization.participant_id() == self.participant_id();
                let same_prior_epoch = finalization.binding_epoch() == proof.prior_binding_epoch();
                if !(same_conversation && same_participant && same_prior_epoch) {
                    return Err(AttachVerificationError::BindingState);
                }
                Ok(Some((finalization, sequence)))
            }
            (BindingState::PendingFinalization(_), None) | (BindingState::Detached, Some(_)) => {
                Err(AttachVerificationError::PendingTerminalSequence)
            }
            (BindingState::Bound(_), _) => Err(AttachVerificationError::BindingState),
        }
    }

    fn verify_attach_common(
        &self,
        request: &CredentialAttachRequest,
        secret_proof: AttachSecretProof,
        parameters: &AttachCommitParameters,
    ) -> Result<(), AttachVerificationError> {
        if request.conversation_id != self.conversation_id() {
            return Err(AttachVerificationError::Conversation);
        }
        if request.participant_id != self.participant_id() {
            return Err(AttachVerificationError::Participant);
        }
        if request.capability_generation != self.generation() {
            return Err(AttachVerificationError::Generation);
        }
        if secret_proof == AttachSecretProof::Mismatch {
            return Err(AttachVerificationError::Secret);
        }
        if parameters.binding.conversation_id != self.conversation_id()
            || parameters.binding.participant_id != self.participant_id()
        {
            return Err(AttachVerificationError::ResultBinding);
        }
        let Some(next_raw) = self.generation().get().checked_add(1) else {
            return Err(AttachVerificationError::ResultGeneration);
        };
        let Some(next_generation) = Generation::new(next_raw) else {
            return Err(AttachVerificationError::ResultGeneration);
        };
        if parameters.binding.binding_epoch.capability_generation != next_generation {
            return Err(AttachVerificationError::ResultGeneration);
        }
        Ok(())
    }
}

/// Atomically commits a verified attach and applies Fix 1's detach-cell rule.
///
/// # Errors
///
/// Returns [`AttachCommitError`] for cell/history mismatch, invalid rotation,
/// or a canonical receipt invariant rejected after verification.
pub fn commit_attach<F, V>(
    verified: VerifiedAttachCommit<F>,
    detach_cell: DetachCell<V>,
) -> Result<AttachCommit<F, V>, AttachCommitError>
where
    V: Copy + Eq,
{
    let VerifiedAttachCommit {
        member: previous_member,
        request,
        parameters,
        mode,
    } = verified;
    let next_cell = transition_detach_cell(&mode, &previous_member, detach_cell)?;
    let attached =
        AttachedLifecycleRecord::from_binding(parameters.binding, parameters.attached_position);
    let (persisted_cursor, terminal, transition, binding_origin, recovered_binding_authority) =
        match mode {
            AttachMode::Detached(_) => (
                previous_member.cursor(),
                None,
                AttachTransition::Detached,
                BindingOrigin::unfenced(attached),
                None,
            ),
            AttachMode::Superseded(committed) => (
                previous_member.cursor(),
                Some(CommittedBindingTerminal::from(committed)),
                AttachTransition::Superseded {
                    terminal: committed,
                },
                BindingOrigin::unfenced(attached),
                None,
            ),
            AttachMode::Fenced { proof, pending } => {
                let composed_terminal =
                    pending.map(|(finalization, sequence)| finalization.commit(sequence));
                let marker_delivery_seq = proof.marker_delivery_seq();
                let prior_binding_epoch = proof.prior_binding_epoch();
                let next_closure_state = proof.next_state();
                (
                    marker_delivery_seq,
                    composed_terminal,
                    AttachTransition::FencedRecovery {
                        prior_binding_epoch,
                        composed_terminal,
                        next_closure_state,
                    },
                    BindingOrigin::recovered(attached, marker_delivery_seq, prior_binding_epoch),
                    Some(proof),
                )
            }
        };
    let result_generation = parameters.binding.binding_epoch.capability_generation;
    let member = previous_member
        .rotate(
            result_generation,
            parameters.attach_secret,
            persisted_cursor,
            terminal,
        )
        .map_err(AttachCommitError::MembershipInvariant)?;
    let outcome = match transition {
        AttachTransition::FencedRecovery { .. } => AttachBound::fenced(
            request.conversation_id,
            request.attach_attempt_token,
            request.participant_id,
            request.capability_generation,
            parameters.attach_secret,
            parameters.binding.binding_epoch,
            persisted_cursor,
            parameters.receipt_expires_at,
            parameters.provenance_expires_at,
        ),
        AttachTransition::Detached | AttachTransition::Superseded { .. } => AttachBound::ordinary(
            request.conversation_id,
            request.attach_attempt_token,
            request.participant_id,
            request.capability_generation,
            parameters.attach_secret,
            parameters.binding.binding_epoch,
            persisted_cursor,
            parameters.receipt_expires_at,
            parameters.provenance_expires_at,
        ),
    }
    .ok_or(AttachCommitError::ReceiptInvariant)?;
    let ordinary_binding_authority = match transition {
        AttachTransition::Detached | AttachTransition::Superseded { .. } => Some(
            OrdinaryBindingAuthority::new(parameters.binding, persisted_cursor),
        ),
        AttachTransition::FencedRecovery { .. } => None,
    };
    Ok(AttachCommit {
        member,
        binding_state: BindingState::Bound(parameters.binding),
        detach_cell: next_cell,
        attached,
        outcome,
        transition,
        binding_origin,
        ordinary_binding_authority,
        recovered_binding_authority,
    })
}

fn transition_detach_cell<F, V>(
    mode: &AttachMode,
    member: &LiveMember<F>,
    detach_cell: DetachCell<V>,
) -> Result<DetachCell<V>, AttachCommitError>
where
    V: Copy + Eq,
{
    match detach_cell {
        DetachCell::Empty(cell) => Ok(DetachCell::Empty(cell)),
        DetachCell::Pending(cell) => {
            let AttachMode::Fenced {
                pending: Some((binding_state, _)),
                ..
            } = mode
            else {
                return Err(AttachCommitError::PendingDetach);
            };
            validate_pending_pair(
                BindingState::PendingFinalization(*binding_state),
                &cell,
                Some(member.conversation_id()),
            )
            .map_err(|_| AttachCommitError::BindingCellState)?;
            Ok(DetachCell::Terminalized(cell.terminalize_after_attach()))
        }
        DetachCell::Committed(cell) => {
            if matches!(mode, AttachMode::Superseded(_))
                || cell.participant_id() != member.participant_id()
                || cell.request_generation() != member.generation()
            {
                return Err(AttachCommitError::DetachCellAuthority);
            }
            let Some(terminal) = member.latest_terminal() else {
                return Err(AttachCommitError::TerminalHistory);
            };
            if terminal.detached_cause() != Some(crate::wire::DetachedCause::CleanDeregister)
                || terminal.binding_epoch() != cell.committed_binding_epoch()
                || terminal.delivery_seq() != cell.detached_delivery_seq()
            {
                return Err(AttachCommitError::TerminalHistory);
            }
            Ok(DetachCell::Terminalized(cell.terminalize_after_attach()))
        }
        DetachCell::Terminalized(cell) => {
            if cell.participant_id() != member.participant_id() {
                return Err(AttachCommitError::DetachCellAuthority);
            }
            Ok(DetachCell::Terminalized(cell))
        }
    }
}
