use crate::wire::{
    AttachBound, AttachSecret, BindingEpoch, CredentialAttachRequest, DeliverySeq, Generation,
};

use super::{
    ActiveBinding, AttachedLifecycleRecord, AttachedRecordPosition, BindingState, ClosureState,
    CommittedBindingTerminal, CommittedBindingTerminalPosition, CommittedDetachedTerminal,
    DetachCell, FencedAttachCommit, LiveMember, MembershipInvariantError, PendingFinalization,
    detach::validate_pending_pair, lookup::AttachSecretProof,
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
#[derive(Clone, Debug, PartialEq, Eq)]
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
}

#[derive(Clone, Copy, Debug)]
enum AttachMode<'a> {
    Detached,
    Superseded(CommittedDetachedTerminal),
    Fenced {
        proof: &'a FencedAttachCommit,
        pending: Option<(PendingFinalization, DeliverySeq)>,
    },
}

/// Successful attach proof; fields are private so only mode verification mints it.
#[derive(Clone, Debug)]
pub struct VerifiedAttachCommit<'a, F> {
    member: LiveMember<F>,
    request: CredentialAttachRequest,
    parameters: AttachCommitParameters,
    mode: AttachMode<'a>,
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
        request: CredentialAttachRequest,
        secret_proof: AttachSecretProof,
        parameters: AttachCommitParameters,
    ) -> Result<VerifiedAttachCommit<'static, F>, AttachVerificationError> {
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
            mode: AttachMode::Detached,
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
    ) -> Result<VerifiedAttachCommit<'static, F>, AttachVerificationError> {
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
        proof: &FencedAttachCommit,
        pending_terminal_delivery_seq: Option<DeliverySeq>,
        parameters: AttachCommitParameters,
    ) -> Result<VerifiedAttachCommit<'_, F>, AttachVerificationError> {
        self.verify_attach_common(&request, secret_proof, &parameters)?;
        if request.accept_marker_delivery_seq != Some(proof.marker_delivery_seq()) {
            return Err(AttachVerificationError::MarkerProof);
        }
        if proof.participant_id() != self.participant_id()
            || proof.prior_binding_epoch().capability_generation != self.generation()
            || proof.new_binding_epoch() != parameters.binding.binding_epoch
        {
            return Err(AttachVerificationError::RecoveryAuthority);
        }
        let pending = match (binding_state, pending_terminal_delivery_seq) {
            (BindingState::Detached, None) => {
                if self
                    .latest_terminal()
                    .is_none_or(|terminal| terminal.binding_epoch() != proof.prior_binding_epoch())
                {
                    return Err(AttachVerificationError::TerminalHistory);
                }
                None
            }
            (BindingState::PendingFinalization(finalization), Some(sequence)) => {
                let same_conversation = finalization.conversation_id() == self.conversation_id();
                let same_participant = finalization.participant_id() == self.participant_id();
                let same_prior_epoch = finalization.binding_epoch() == proof.prior_binding_epoch();
                if !(same_conversation && same_participant && same_prior_epoch) {
                    return Err(AttachVerificationError::BindingState);
                }
                Some((finalization, sequence))
            }
            (BindingState::PendingFinalization(_), None) | (BindingState::Detached, Some(_)) => {
                return Err(AttachVerificationError::PendingTerminalSequence);
            }
            (BindingState::Bound(_), _) => {
                return Err(AttachVerificationError::BindingState);
            }
        };
        Ok(VerifiedAttachCommit {
            member: self,
            request,
            parameters,
            mode: AttachMode::Fenced { proof, pending },
        })
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
    verified: VerifiedAttachCommit<'_, F>,
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
    let (persisted_cursor, terminal, transition) = match mode {
        AttachMode::Detached => (previous_member.cursor(), None, AttachTransition::Detached),
        AttachMode::Superseded(committed) => (
            previous_member.cursor(),
            Some(CommittedBindingTerminal::from(committed)),
            AttachTransition::Superseded {
                terminal: committed,
            },
        ),
        AttachMode::Fenced { proof, pending } => {
            let composed_terminal =
                pending.map(|(finalization, sequence)| finalization.commit(sequence));
            (
                proof.marker_delivery_seq(),
                composed_terminal,
                AttachTransition::FencedRecovery {
                    prior_binding_epoch: proof.prior_binding_epoch(),
                    composed_terminal,
                    next_closure_state: proof.next_state(),
                },
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
    Ok(AttachCommit {
        member,
        binding_state: BindingState::Bound(parameters.binding),
        detach_cell: next_cell,
        attached,
        outcome,
        transition,
    })
}

fn transition_detach_cell<F, V>(
    mode: &AttachMode<'_>,
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
