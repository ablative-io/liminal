use std::error::Error;
use std::sync::Arc;

use liminal::durability::bridge::block_on;
use liminal_protocol::algebra::WideResourceVector;
use liminal_protocol::lifecycle::{
    ActiveBinding, AttachCommitParameters, AttachSecretProof, AttachedRecordPosition, BindingState,
    BindingTerminalDisposition, ClosureDebt, CommittedBindingTerminalPosition, DebtCompletion,
    DetachCell, EnrollmentFingerprint, Event, FencedAttachCommit, FencedAttachMintRefusalReason,
    LiveMember, LiveMemberRestore, MintFencedAttachResult, commit_attach,
};
use liminal_protocol::wire::{
    AttachAttemptToken, AttachSecret, BindingEpoch, ConnectionIncarnation, CredentialAttachRequest,
    Generation,
};

use super::fenced_attach_codec::{
    FencedAttachMintInputs, StoredDebtCompletion, StoredDetachedCredentialRecovery,
    StoredMarkerCursorProgress, StoredMarkerDelivery, StoredProofBinding, StoredRecoveryTerminal,
    StoredWideResourceVector,
};
use super::log::{
    OperationLog, STREAM_PREFIX, StoredAttachModeV3, StoredBindingEpoch, StoredFencedAttachProof,
    StoredOperation, StoredU128,
};
use super::log_v3::StoredComposedTerminalCause;
use super::marker_source::validate_marker_source;
use super::ops_attach_verify::mint_associated_fenced_attach;
use super::tests_w1b_marker_source::{CONVERSATION, MARKER, PARTICIPANT, epoch, source_fixture};

const fn stored_wide(entries: u128, bytes: u128) -> StoredWideResourceVector {
    StoredWideResourceVector {
        entries: StoredU128(entries.to_be_bytes()),
        bytes: StoredU128(bytes.to_be_bytes()),
    }
}

fn fenced_mode() -> Result<StoredAttachModeV3, Box<dyn Error>> {
    let prior: StoredBindingEpoch = epoch()?.into();
    let marker_predecessor = MARKER
        .checked_sub(1)
        .ok_or("marker predecessor underflow")?;
    let recovery = StoredDetachedCredentialRecovery {
        conversation_id: CONVERSATION,
        participant_id: PARTICIPANT,
        marker_delivery_seq: MARKER,
        prior_binding_epoch: prior,
        resulting_floor: marker_predecessor,
        terminal: StoredRecoveryTerminal::Committed {
            binding: StoredProofBinding {
                conversation_id: CONVERSATION,
                participant_id: PARTICIPANT,
                binding_epoch: prior,
            },
            cause: StoredComposedTerminalCause::CleanDeregister,
            transaction_order: 1,
            delivery_seq: marker_predecessor,
        },
        progress: StoredMarkerCursorProgress {
            conversation_id: CONVERSATION,
            participant_id: PARTICIPANT,
            binding_epoch: prior,
            through_seq: MARKER,
            marker_delivery_seq: MARKER,
            delivery: StoredMarkerDelivery {
                participant_id: PARTICIPANT,
                binding_epoch: prior,
                marker_delivery_seq: MARKER,
            },
        },
    };
    let proof = StoredFencedAttachProof::encode(
        &recovery,
        stored_wide(2, 2),
        MARKER.checked_add(1).ok_or("fenced floor overflow")?,
        StoredDebtCompletion::ObserverProjection {
            debt: stored_wide(1, 1),
            through_seq: MARKER
                .checked_add(2)
                .ok_or("projection boundary overflow")?,
        },
    )?;
    Ok(StoredAttachModeV3::Fenced {
        prior_binding_epoch: prior,
        marker_delivery_seq: MARKER,
        marker_source_sequence: 0,
        proof,
        composed_terminal: None,
    })
}

fn chain_member() -> Result<LiveMember<Vec<u8>>, Box<dyn Error>> {
    let prior = epoch()?;
    let active = ActiveBinding {
        conversation_id: CONVERSATION,
        participant_id: PARTICIPANT,
        binding_epoch: prior,
    };
    let terminal_seq = MARKER.checked_sub(1).ok_or("terminal sequence underflow")?;
    let transition = active.clean_deregister(BindingTerminalDisposition::Committed(
        CommittedBindingTerminalPosition::new(1, terminal_seq),
    ));
    let liminal_protocol::lifecycle::DetachedBindingTransition::Committed(terminal) = transition
    else {
        return Err("committed disposition did not produce committed terminal".into());
    };
    Ok(LiveMember::restore(LiveMemberRestore {
        participant_id: PARTICIPANT,
        conversation_id: CONVERSATION,
        generation: prior.capability_generation,
        attach_secret: AttachSecret::new([7; 32]),
        cursor: MARKER,
        enrollment_fingerprint: EnrollmentFingerprint::new(vec![8]),
        latest_terminal: Some(terminal.into()),
    })
    .map_err(|error| format!("member restore failed: {error:?}"))?)
}

fn production_chain(cold: bool) -> Result<[u64; 5], Box<dyn Error>> {
    let fixture = source_fixture()?;
    let log = OperationLog::new(Arc::clone(&fixture.store), CONVERSATION);
    block_on(log.append(&StoredOperation::MarkerDrained { row: fixture.row }, 0))??;
    let mode = if cold {
        let bytes = serde_json::to_vec(&fenced_mode()?)?;
        serde_json::from_slice(&bytes)?
    } else {
        fenced_mode()?
    };
    let StoredAttachModeV3::Fenced {
        prior_binding_epoch,
        marker_delivery_seq,
        marker_source_sequence,
        proof,
        ..
    } = mode
    else {
        return Err("fixture mode was not fenced".into());
    };
    let new_generation = Generation::new(
        prior_binding_epoch
            .capability_generation
            .checked_add(1)
            .ok_or("new generation overflow")?,
    )
    .ok_or("new generation was zero")?;
    let new_epoch = BindingEpoch::new(ConnectionIncarnation::new(1, 2), new_generation);
    let context = super::fenced_attach_codec::FencedAttachProofContext {
        conversation_id: CONVERSATION,
        participant_id: PARTICIPANT,
        request_marker_delivery_seq: Some(marker_delivery_seq),
        prior_binding_epoch,
        marker_delivery_seq,
        new_binding_epoch: new_epoch.into(),
    };
    let inputs = proof
        .decode(context)?
        .into_mint_inputs(context.new_binding_epoch)?;
    let validated = block_on(validate_marker_source(
        Arc::clone(&fixture.store),
        fixture.retained,
        marker_source_sequence,
    ))?
    .map_err(|refused| format!("marker source association refused: {:?}", refused.reason()))?;
    let (owner, recovery, source_sequence) = validated.into_parts();
    let inputs = FencedAttachMintInputs { recovery, ..inputs };
    let MintFencedAttachResult::Minted(minted) =
        mint_associated_fenced_attach(owner, source_sequence, inputs)
    else {
        return Err("production proof mint refused exact inputs".into());
    };
    let (owner, proof) = minted.into_parts();
    drop(owner);
    let mut observed = finish_production_chain(proof, new_epoch)?;
    observed[0] = observed[0].checked_add(1).ok_or("mint count overflow")?;
    Ok(observed)
}

fn finish_production_chain(
    proof: FencedAttachCommit,
    new_epoch: BindingEpoch,
) -> Result<[u64; 5], Box<dyn Error>> {
    let mut observed = [0_u64; 5];
    let request = CredentialAttachRequest {
        conversation_id: CONVERSATION,
        participant_id: PARTICIPANT,
        capability_generation: epoch()?.capability_generation,
        attach_secret: AttachSecret::new([7; 32]),
        attach_attempt_token: AttachAttemptToken::new([9; 16]),
        accept_marker_delivery_seq: Some(MARKER),
    };
    let verified = chain_member()?
        .verify_fenced_attach(
            BindingState::Detached,
            request,
            AttachSecretProof::Verified,
            proof,
            None,
            AttachCommitParameters {
                binding: ActiveBinding {
                    conversation_id: CONVERSATION,
                    participant_id: PARTICIPANT,
                    binding_epoch: new_epoch,
                },
                attach_secret: AttachSecret::new([10; 32]),
                attached_position: AttachedRecordPosition::new(
                    2,
                    MARKER.checked_add(3).ok_or("attached sequence overflow")?,
                ),
                receipt_expires_at: 1,
                provenance_expires_at: 2,
            },
        )
        .map_err(|refused| format!("production verification refused: {:?}", refused.error()))?;
    observed[1] = observed[1]
        .checked_add(1)
        .ok_or("verification count overflow")?;
    let committed = commit_attach(verified, DetachCell::<[u8; 32]>::default())
        .map_err(|error| format!("attach commit failed: {error:?}"))?;
    observed[2] = observed[2].checked_add(1).ok_or("commit count overflow")?;
    let (_, token) = committed.into_slot_and_fate();
    observed[3] = observed[3].checked_add(1).ok_or("split count overflow")?;
    let fate_floor = MARKER.checked_add(4).ok_or("fate floor overflow")?;
    let fate = token
        .recovered_binding_fate(Event::binding_fate_observed(
            PARTICIPANT,
            new_epoch,
            fate_floor,
        ))
        .map_err(|_| "recovered authority refused exact fate event")?;
    assert_eq!(fate.participant_id(), PARTICIPANT);
    observed[4] = observed[4]
        .checked_add(1)
        .ok_or("fate consumption count overflow")?;
    Ok(observed)
}

fn mint_refused_observation() -> Result<(), Box<dyn Error>> {
    let fixture = source_fixture()?;
    let log = OperationLog::new(Arc::clone(&fixture.store), CONVERSATION);
    block_on(log.append(&StoredOperation::MarkerDrained { row: fixture.row }, 0))??;
    let stream_key = format!("{STREAM_PREFIX}{CONVERSATION}");
    let durable_before = block_on(fixture.store.read_from(&stream_key, 0, 16))??;
    let validated = block_on(validate_marker_source(
        Arc::clone(&fixture.store),
        fixture.retained,
        0,
    ))?
    .map_err(|refused| format!("marker association refused: {:?}", refused.reason()))?;
    let (owner, recovery, source_sequence) = validated.into_parts();
    let prior = recovery.prior_binding_epoch();
    let new_generation = Generation::new(
        prior
            .capability_generation
            .get()
            .checked_add(1)
            .ok_or("retry generation overflow")?,
    )
    .ok_or("retry generation was zero")?;
    let new_epoch = BindingEpoch::new(ConnectionIncarnation::new(1, 2), new_generation);
    let debt = ClosureDebt::new(WideResourceVector::new(2, 2)).ok_or("retry debt was zero")?;
    let successor = DebtCompletion::observer_projection(
        ClosureDebt::new(WideResourceVector::new(1, 1)).ok_or("successor debt was zero")?,
        liminal_protocol::lifecycle::ObserverProjection::new(
            MARKER.checked_add(2).ok_or("retry projection overflow")?,
        ),
    );
    let resulting_floor = MARKER.checked_add(1).ok_or("retry floor overflow")?;
    let wrong_event = Event::fenced_recovery_committed(
        PARTICIPANT,
        MARKER.checked_add(1).ok_or("wrong marker overflow")?,
        prior,
        new_epoch,
        resulting_floor,
    );
    let MintFencedAttachResult::MintRefused(refused) = mint_associated_fenced_attach(
        owner,
        source_sequence,
        FencedAttachMintInputs {
            recovery,
            predecessor_debt: debt,
            event: wrong_event,
            successor,
        },
    ) else {
        return Err("MintRefused injection unexpectedly minted a proof".into());
    };
    assert_eq!(refused.reason(), FencedAttachMintRefusalReason::ProofInputs);
    let (owner, source_sequence, recovery, debt, returned_event, successor) = refused.into_parts();
    assert_eq!(returned_event, wrong_event);
    let durable_after_refusal = block_on(fixture.store.read_from(&stream_key, 0, 16))??;
    assert_eq!(durable_after_refusal, durable_before);

    let mut successful_retries = 0_u64;
    let MintFencedAttachResult::Minted(minted) = mint_associated_fenced_attach(
        owner,
        source_sequence,
        FencedAttachMintInputs {
            recovery,
            predecessor_debt: debt,
            event: Event::fenced_recovery_committed(
                PARTICIPANT,
                MARKER,
                prior,
                new_epoch,
                resulting_floor,
            ),
            successor,
        },
    ) else {
        return Err("reinstalled marker authority refused serial retry".into());
    };
    successful_retries = successful_retries
        .checked_add(1)
        .ok_or("successful retry count overflow")?;
    let (spent_owner, proof) = minted.into_parts();
    drop((spent_owner, proof));
    assert_eq!(successful_retries, 1);
    let durable_after_retry = block_on(fixture.store.read_from(&stream_key, 0, 16))??;
    assert_eq!(durable_after_retry, durable_before);
    Ok(())
}

#[test]
fn attach_commit_splits_operational_state_and_one_noncloneable_fate_token()
-> Result<(), Box<dyn Error>> {
    assert_eq!(production_chain(false)?, [1, 1, 1, 1, 1]);
    assert_eq!(production_chain(true)?, [1, 1, 1, 1, 1]);
    mint_refused_observation()?;
    Ok(())
}
