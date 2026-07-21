use std::error::Error;
use std::sync::Arc;

use liminal::durability::bridge::block_on;
use liminal_protocol::algebra::WideResourceVector;
use liminal_protocol::lifecycle::{
    ActiveBinding, AttachCommitParameters, AttachedRecordPosition, BindingState,
    BindingTerminalDisposition, ClosureDebt, CommittedBindingTerminalPosition, DebtCompletion,
    DetachCell, EnrollmentFingerprint, Event, FencedAttachMintRefusalReason, LiveMember,
    LiveMemberRestore, MintFencedAttachResult, commit_attach,
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
    DecodedStoredOperation, OperationLog, STREAM_PREFIX, StoredAttachAllocation,
    StoredAttachModeV3, StoredBindingEpoch, StoredFencedAttachProof, StoredOperation, StoredU128,
};
use super::log_v3::StoredComposedTerminalCause;
use super::marker_source::validate_marker_source;
use super::ops_attach_verify::{
    AttachVerification, mint_associated_fenced_attach, verify_attach_mode,
};
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

fn chain_member() -> Result<LiveMember<[u8; 32]>, Box<dyn Error>> {
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
        enrollment_fingerprint: EnrollmentFingerprint::new([8; 32]),
        latest_terminal: Some(terminal.into()),
    })
    .map_err(|error| format!("member restore failed: {error:?}"))?)
}

fn attach_request() -> Result<CredentialAttachRequest, Box<dyn Error>> {
    Ok(CredentialAttachRequest {
        conversation_id: CONVERSATION,
        participant_id: PARTICIPANT,
        capability_generation: epoch()?.capability_generation,
        attach_secret: AttachSecret::new([7; 32]),
        attach_attempt_token: AttachAttemptToken::new([9; 16]),
        accept_marker_delivery_seq: Some(MARKER),
    })
}

fn next_epoch() -> Result<BindingEpoch, Box<dyn Error>> {
    let prior = epoch()?;
    let new_generation = Generation::new(
        prior
            .capability_generation
            .get()
            .checked_add(1)
            .ok_or("new generation overflow")?,
    )
    .ok_or("new generation was zero")?;
    Ok(BindingEpoch::new(
        ConnectionIncarnation::new(1, 2),
        new_generation,
    ))
}

fn attach_allocation(new_epoch: BindingEpoch) -> Result<StoredAttachAllocation, Box<dyn Error>> {
    Ok(StoredAttachAllocation {
        binding_epoch: new_epoch.into(),
        attach_secret: [10; 32],
        attached_order: 2,
        attached_seq: MARKER.checked_add(3).ok_or("attached sequence overflow")?,
        receipt_expires_at: StoredU128(1_u128.to_be_bytes()),
        provenance_expires_at: StoredU128(2_u128.to_be_bytes()),
        admitted_now_ms: 0,
    })
}

fn production_chain(cold: bool) -> Result<[u64; 5], Box<dyn Error>> {
    let fixture = source_fixture()?;
    let log = OperationLog::new(Arc::clone(&fixture.store), CONVERSATION);
    block_on(log.append(&StoredOperation::MarkerDrained { row: fixture.row }, 0))??;
    let request = attach_request()?;
    let new_epoch = next_epoch()?;
    let allocation = attach_allocation(new_epoch)?;
    let mode = fenced_mode()?;
    let (request, allocation, mode) = if cold {
        block_on(log.append(
            &StoredOperation::Attached {
                request: (&request).into(),
                secret_verified: true,
                allocation,
                mode: Box::new(mode),
                event: Vec::new(),
            },
            1,
        ))??;
        let decoded = block_on(log.read_at(1))??.ok_or("durable Attached row missing")?;
        let DecodedStoredOperation::V3(StoredOperation::Attached {
            request,
            allocation,
            mode,
            ..
        }) = decoded.operation
        else {
            return Err("cold fixture did not decode a v3 Attached row".into());
        };
        (request.to_request()?, allocation, *mode)
    } else {
        (request, allocation, mode)
    };
    let (frontier_owner, _) = fixture.retained.into_parts();
    let parameters = AttachCommitParameters {
        binding: ActiveBinding {
            conversation_id: CONVERSATION,
            participant_id: PARTICIPANT,
            binding_epoch: new_epoch,
        },
        attach_secret: AttachSecret::new(allocation.attach_secret),
        attached_position: AttachedRecordPosition::new(
            allocation.attached_order,
            allocation.attached_seq,
        ),
        receipt_expires_at: allocation.receipt_expires_at.get(),
        provenance_expires_at: allocation.provenance_expires_at.get(),
    };
    let mut observed = [0_u64; 5];
    let (verified, spent_owner) = verify_attach_mode(
        chain_member()?,
        BindingState::Detached,
        frontier_owner,
        AttachVerification {
            request: &request,
            mode: &mode,
            parameters,
            store: Arc::clone(&fixture.store),
            source_sequence: 1,
        },
    )
    .map_err(|error| format!("production fenced verifier refused: {error}"))?;
    observed[0] = observed[0].checked_add(1).ok_or("mint count overflow")?;
    observed[1] = observed[1]
        .checked_add(1)
        .ok_or("verification count overflow")?;
    let committed = commit_attach(verified, DetachCell::<[u8; 32]>::default())
        .map_err(|error| format!("attach commit failed: {error:?}"))?;
    observed[2] = observed[2].checked_add(1).ok_or("commit count overflow")?;
    drop(spent_owner);
    let split = committed.into_slot_and_fate();
    assert!(split.1.is_recovered());
    observed[3] = observed[3].checked_add(1).ok_or("split count overflow")?;
    super::tests_w1b_fate_completion::run_recovered_completion()?;
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
