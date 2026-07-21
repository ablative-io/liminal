//! Shared live/cold verification of closed credential-attach modes.

use std::sync::Arc;

use liminal::durability::DurableStore;
use liminal::durability::bridge::block_on;
use liminal_protocol::lifecycle::{
    ActiveBinding, AttachCommitParameters, AttachSecretProof, AttachedRecordPosition, BindingState,
    ClosureState, CommittedBindingTerminalPosition, LiveFrontierOwner, MintFencedAttachResult,
    VerifiedAttachCommit,
};
use liminal_protocol::wire::{AttachSecret, CredentialAttachRequest, Generation};

use super::facts::Digest;
use super::fenced_attach_codec::FencedAttachProofContext;
use super::log::{OperationLogError, StoredAttachAllocation, StoredAttachModeV3};
use super::marker_source::validate_marker_source;
use super::state::StateError;

/// Verifies one attach transition in its allocation-derived mode.
///
/// Ordinary and superseding modes retain the frontier owner unchanged. Fenced
/// mode resolves and validates one exact durable marker source, consumes its
/// owner-held marker authority into one proof, and passes that proof by value
/// into protocol verification.
pub(super) fn mint_associated_fenced_attach(
    owner: LiveFrontierOwner,
    marker_source_sequence: u64,
    inputs: super::fenced_attach_codec::FencedAttachMintInputs,
) -> MintFencedAttachResult {
    owner.mint_fenced_attach(
        marker_source_sequence,
        inputs.recovery,
        inputs.predecessor_debt,
        inputs.event,
        inputs.successor,
    )
}

pub(super) struct AttachVerification<'a> {
    pub(super) request: &'a CredentialAttachRequest,
    pub(super) mode: &'a StoredAttachModeV3,
    pub(super) parameters: AttachCommitParameters,
    pub(super) store: Arc<dyn DurableStore>,
    pub(super) source_sequence: u64,
}

pub(super) fn stored_attach_parameters(
    request: &CredentialAttachRequest,
    allocation: &StoredAttachAllocation,
) -> Result<(Generation, AttachCommitParameters), StateError> {
    let binding_epoch = allocation.binding_epoch.to_epoch()?;
    Ok((
        binding_epoch.capability_generation,
        AttachCommitParameters {
            binding: ActiveBinding {
                participant_id: request.participant_id,
                conversation_id: request.conversation_id,
                binding_epoch,
            },
            attach_secret: AttachSecret::new(allocation.attach_secret),
            attached_position: AttachedRecordPosition::new(
                allocation.attached_order,
                allocation.attached_seq,
            ),
            receipt_expires_at: allocation.receipt_expires_at.get(),
            provenance_expires_at: allocation.provenance_expires_at.get(),
        },
    ))
}

pub(super) fn verify_attach_mode(
    member: liminal_protocol::lifecycle::LiveMember<Digest>,
    binding: BindingState,
    frontier_owner: LiveFrontierOwner,
    verification: AttachVerification<'_>,
) -> Result<(VerifiedAttachCommit<Digest>, LiveFrontierOwner), StateError> {
    let AttachVerification {
        request,
        mode,
        parameters,
        store,
        source_sequence,
    } = verification;
    match (binding, mode) {
        (BindingState::Detached, StoredAttachModeV3::Ordinary) => {
            let closure_admission = ClosureState::Clear
                .ordinary_detached_attach_admission()
                .map_err(|error| {
                    StateError::invariant(format!(
                        "clear closure refused detached attach admission: {error:?}"
                    ))
                })?;
            member
                .verify_detached_attach(
                    BindingState::Detached,
                    closure_admission,
                    request.clone(),
                    AttachSecretProof::Verified,
                    parameters,
                )
                .map(|verified| (verified, frontier_owner))
                .map_err(|error| {
                    StateError::invariant(format!("protocol attach verification failed: {error:?}"))
                })
        }
        (
            BindingState::Bound(active),
            StoredAttachModeV3::Superseding {
                prior_binding_epoch,
                terminal_transaction_order,
                terminal_delivery_seq,
            },
        ) if active.binding_epoch == prior_binding_epoch.to_epoch()?
            && *terminal_transaction_order == parameters.attached_position.transaction_order() =>
        {
            member
                .verify_superseding_attach(
                    active,
                    request.clone(),
                    AttachSecretProof::Verified,
                    CommittedBindingTerminalPosition::new(
                        *terminal_transaction_order,
                        *terminal_delivery_seq,
                    ),
                    parameters,
                )
                .map(|verified| (verified, frontier_owner))
                .map_err(|error| {
                    StateError::invariant(format!("protocol attach verification failed: {error:?}"))
                })
        }
        (
            BindingState::Detached | BindingState::PendingFinalization(_),
            StoredAttachModeV3::Fenced { .. },
        ) => verify_fenced_mode(
            member,
            binding,
            frontier_owner,
            AttachVerification {
                request,
                mode,
                parameters,
                store,
                source_sequence,
            },
        ),
        (_, StoredAttachModeV3::Fenced { .. }) => Err(OperationLogError::FencedAttachProof {
            sequence: source_sequence,
            reason: super::log::FencedAttachProofRefusal::ComposedReplayStateMismatch,
        }
        .into()),
        (_, _) => Err(StateError::invariant(
            "attach allocation mode does not match the slot's binding authority",
        )),
    }
}

fn verify_fenced_mode(
    member: liminal_protocol::lifecycle::LiveMember<Digest>,
    binding: BindingState,
    frontier_owner: LiveFrontierOwner,
    verification: AttachVerification<'_>,
) -> Result<(VerifiedAttachCommit<Digest>, LiveFrontierOwner), StateError> {
    let AttachVerification {
        request,
        mode,
        parameters,
        store,
        source_sequence,
    } = verification;
    let StoredAttachModeV3::Fenced {
        prior_binding_epoch,
        marker_delivery_seq,
        marker_source_sequence,
        proof,
        composed_terminal,
    } = mode
    else {
        return Err(StateError::invariant(
            "fenced attach verifier received a non-fenced mode",
        ));
    };
    let context = FencedAttachProofContext {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        request_marker_delivery_seq: request.accept_marker_delivery_seq,
        prior_binding_epoch: *prior_binding_epoch,
        marker_delivery_seq: *marker_delivery_seq,
        new_binding_epoch: parameters.binding.binding_epoch.into(),
    };
    let mint_inputs = proof
        .decode(context)
        .and_then(|decoded| decoded.into_mint_inputs(context.new_binding_epoch))
        .map_err(|reason| OperationLogError::FencedAttachProof {
            sequence: source_sequence,
            reason,
        })?;
    let retained = frontier_owner
        .retain_fenced_marker_source(mint_inputs.recovery)
        .map_err(|_| {
            StateError::invariant(
                "fenced attach frontier does not retain the decoded marker recovery",
            )
        })?;
    let associated = block_on(validate_marker_source(
        store,
        retained,
        *marker_source_sequence,
    ))
    .map_err(|error| {
        StateError::invariant(format!("marker source validation task failed: {error}"))
    })?
    .map_err(|refused| {
        StateError::invariant(format!(
            "fenced attach marker source was refused: {:?}",
            refused.reason()
        ))
    })?;
    let (frontier_owner, recovery, associated_sequence) = associated.into_parts();
    let mint_inputs = super::fenced_attach_codec::FencedAttachMintInputs {
        recovery,
        ..mint_inputs
    };
    let minted =
        match mint_associated_fenced_attach(frontier_owner, associated_sequence, mint_inputs) {
            MintFencedAttachResult::Minted(minted) => minted,
            MintFencedAttachResult::MintRefused(refused) => {
                return Err(StateError::invariant(format!(
                    "fenced attach proof mint was refused: {:?}",
                    refused.reason()
                )));
            }
        };
    let (frontier_owner, proof) = minted.into_parts();
    member
        .verify_fenced_attach(
            binding,
            request.clone(),
            AttachSecretProof::Verified,
            proof,
            composed_terminal
                .as_ref()
                .map(|terminal| terminal.delivery_seq),
            parameters,
        )
        .map(|verified| (verified, frontier_owner))
        .map_err(|refused| {
            StateError::invariant(format!(
                "protocol fenced attach verification failed: {:?}",
                refused.error()
            ))
        })
}
