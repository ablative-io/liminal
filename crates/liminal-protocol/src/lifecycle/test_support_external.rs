//! Feature-gated full-authority fixtures for downstream acceptance tests.
//!
//! This module is absent from production builds. Its recovered-attach fixture
//! restores the complete DCR sequence/order blocks, consumes the one validated
//! marker record, and runs the same verify/commit/frontier-apply chain as a live
//! fenced attach.

use alloc::{
    format,
    string::{String, ToString},
    vec,
    vec::Vec,
};

use crate::{
    algebra::WideResourceVector,
    outcome::CandidatePhase,
    wire::{
        AttachAttemptToken, AttachSecret, BindingEpoch, CloseCause, ConnectionIncarnation,
        CredentialAttachRequest, Generation,
    },
};

mod application;

use super::claim_frontier::MarkerRecordRequest;
use super::edge::{marker_delivery_for_test, validated_marker_record_for_recovery_test};
use super::{
    ActiveBinding, AdmissionOrder, AttachCommit, AttachCommitParameters, AttachFrontierCharges,
    AttachSecretProof, AttachedRecordPosition, BindingState, BindingTerminalOwner, ClaimFrontiers,
    ClaimFrontiersRestore, ClosureAccounting, ClosureDebt, ClosureState,
    CommittedBindingTerminalRestore, CursorFateSuccessor, DebtCompletion, DetachCell,
    DetachedCredentialRecovery, EnrollmentFingerprint, Event, FrontierBinding, FrontierParticipant,
    HistoricalMarkerDeliveryFactRestore, LiveFrontierOwner, LiveMember, LiveMemberRestore,
    MovableOrderClaim, MovableSequenceClaim, ObserverProjection, OrderClaimFrontierRestore,
    OrderClaims, OrderDirectOwner, OrderHigh, OrderLedger, RecoveryOrderBlockRestore,
    RecoverySequenceBlockRestore, RecoverySequenceReserve, ReplacementTerminalProductRangeRestore,
    RetainedCausalRecord, RetainedCausalRecordKind, RetainedRecordCharge, SealedBindingFateToken,
    SequenceClaimFrontierRestore, SequenceClaims, SequenceDirectOwner, SequenceLedger,
    SequenceProductRangesRestore, StoredEdge, apply_attach_frontier, commit_attach,
};
use application::apply_dcr_attach;

/// Post-fenced-attach authority produced by a complete executable DCR frontier.
pub struct ExecutableRecoveredAttach {
    /// Frontier after consuming both DCR claim blocks through `apply_attach_frontier`.
    pub owner: LiveFrontierOwner,
    /// Member installed by the fenced attach.
    pub member: LiveMember<[u8; 32]>,
    /// Bound state installed by the fenced attach.
    pub binding: BindingState,
    /// Detach cell installed by the fenced attach.
    pub detach_cell: DetachCell<[u8; 32]>,
    /// Sole recovered fate token split from that exact attach commit.
    pub fate_token: SealedBindingFateToken,
    /// Authoritative pre-recovery binding epoch.
    pub prior_binding_epoch: BindingEpoch,
    /// Newly bound recovery epoch.
    pub recovered_binding_epoch: BindingEpoch,
    /// Exact delivered recovery marker.
    pub marker_delivery_seq: u64,
    /// Exact replacement-terminal sequence claim left by the fenced attach.
    pub next_terminal_sequence: u64,
    /// Exact replacement-terminal order claim left by the fenced attach.
    pub next_terminal_order: u64,
}

#[derive(Clone, Copy)]
struct DcrContext {
    conversation_id: u64,
    participant_id: u64,
    marker_delivery_seq: u64,
    prior_binding_epoch: BindingEpoch,
    recovered_binding_epoch: BindingEpoch,
    debt: ClosureDebt,
}

#[derive(Clone, Copy)]
struct DcrClaims {
    marker_order: AdmissionOrder,
    terminal_delivery_seq: u64,
    recovery_attach_seq: u64,
    replacement_terminal_seq: u64,
    replacement_product_seq: u64,
    membership_exit_seq: u64,
    recovery_operation_order: u64,
    replacement_terminal_order: u64,
    membership_exit_order: u64,
}

struct DcrFrontiers {
    restore: ClaimFrontiersRestore,
    frontiers: ClaimFrontiers,
}

fn generation(value: u64) -> Result<Generation, String> {
    Generation::new(value).ok_or_else(|| "DCR fixture generation must be nonzero".into())
}

fn epoch(server: u64, ordinal: u64, generation_value: u64) -> Result<BindingEpoch, String> {
    Ok(BindingEpoch::new(
        ConnectionIncarnation::new(server, ordinal),
        generation(generation_value)?,
    ))
}

fn context() -> Result<DcrContext, String> {
    let debt = ClosureDebt::new(WideResourceVector::new(1, 1))
        .ok_or_else(|| "DCR fixture debt must be nonzero".to_string())?;
    Ok(DcrContext {
        conversation_id: 1,
        participant_id: 0,
        marker_delivery_seq: 1,
        prior_binding_epoch: epoch(1, 1, 1)?,
        recovered_binding_epoch: epoch(2, 2, 2)?,
        debt,
    })
}

fn claims(context: DcrContext) -> Result<DcrClaims, String> {
    let marker_order =
        AdmissionOrder::new(0, CandidatePhase::CompactionMarker, context.participant_id);
    let terminal_delivery_seq = context
        .marker_delivery_seq
        .checked_add(1)
        .ok_or_else(|| "DCR prior terminal sequence overflow".to_string())?;
    let recovery_attach_seq = terminal_delivery_seq
        .checked_add(1)
        .ok_or_else(|| "DCR recovery attach sequence overflow".to_string())?;
    let replacement_terminal_seq = recovery_attach_seq
        .checked_add(1)
        .ok_or_else(|| "DCR replacement terminal sequence overflow".to_string())?;
    let replacement_product_seq = replacement_terminal_seq
        .checked_add(1)
        .ok_or_else(|| "DCR replacement product sequence overflow".to_string())?;
    let membership_exit_seq = replacement_product_seq
        .checked_add(1)
        .ok_or_else(|| "DCR membership exit sequence overflow".to_string())?;
    let recovery_operation_order = marker_order
        .transaction_order()
        .checked_add(2)
        .ok_or_else(|| "DCR recovery order overflow".to_string())?;
    let replacement_terminal_order = recovery_operation_order
        .checked_add(1)
        .ok_or_else(|| "DCR replacement terminal order overflow".to_string())?;
    let membership_exit_order = replacement_terminal_order
        .checked_add(1)
        .ok_or_else(|| "DCR membership exit order overflow".to_string())?;
    Ok(DcrClaims {
        marker_order,
        terminal_delivery_seq,
        recovery_attach_seq,
        replacement_terminal_seq,
        replacement_product_seq,
        membership_exit_seq,
        recovery_operation_order,
        replacement_terminal_order,
        membership_exit_order,
    })
}

fn credential_recovery(context: DcrContext) -> Result<DetachedCredentialRecovery, String> {
    let delivery = marker_delivery_for_test(
        context.participant_id,
        context.prior_binding_epoch,
        context.marker_delivery_seq,
    )
    .map_err(|error| format!("DCR marker delivery restore failed: {error:?}"))?;
    let delivered = delivery
        .delivered(
            context.debt,
            Event::marker_delivered(
                context.participant_id,
                context.prior_binding_epoch,
                context.marker_delivery_seq,
            ),
        )
        .map_err(|error| format!("DCR marker delivery failed: {error:?}"))?;
    let ClosureState::Owed {
        edge: StoredEdge::ParticipantCursorProgress(progress),
        ..
    } = delivered
    else {
        return Err("DCR marker delivery did not produce cursor progress".into());
    };
    let successor = progress
        .binding_fate(
            context.debt,
            Event::binding_fate_observed(
                context.participant_id,
                context.prior_binding_epoch,
                context.marker_delivery_seq,
            ),
        )
        .map_err(|error| format!("DCR marker fate failed: {error:?}"))?;
    let CursorFateSuccessor::DetachedCredentialRecovery(recovery) = successor else {
        return Err("DCR marker fate did not produce credential recovery".into());
    };
    Ok(recovery)
}

fn retained_records(context: DcrContext, claims: DcrClaims) -> Vec<RetainedCausalRecord> {
    let terminal_owner = BindingTerminalOwner {
        participant_index: context.participant_id,
        binding_epoch: context.prior_binding_epoch,
    };
    vec![
        RetainedCausalRecord {
            delivery_seq: context.marker_delivery_seq,
            admission_order: claims.marker_order,
            kind: RetainedCausalRecordKind::CompactionMarker {
                participant_index: context.participant_id,
                provenance: super::MarkerProvenance::NonProductM,
            },
        },
        RetainedCausalRecord {
            delivery_seq: claims.terminal_delivery_seq,
            admission_order: AdmissionOrder::new(
                1,
                CandidatePhase::BindingTerminal,
                context.participant_id,
            ),
            kind: RetainedCausalRecordKind::BindingTerminal(terminal_owner),
        },
    ]
}

fn restore_shape(context: DcrContext, claims: DcrClaims) -> Result<ClaimFrontiersRestore, String> {
    Ok(ClaimFrontiersRestore {
        conversation_id: context.conversation_id,
        active_identities: vec![FrontierParticipant::new(
            context.participant_id,
            0,
            FrontierBinding::Detached(context.prior_binding_epoch),
        )],
        identity_slot_limit: context
            .participant_id
            .checked_add(1)
            .ok_or_else(|| "DCR identity limit overflow".to_string())?,
        retained_floor: u128::from(context.marker_delivery_seq),
        retained_record_limit: 2,
        retained_records: retained_records(context, claims),
        active_marker_anchors: vec![context.marker_delivery_seq],
        historical_marker_deliveries: vec![HistoricalMarkerDeliveryFactRestore {
            conversation_id: context.conversation_id,
            participant_index: context.participant_id,
            marker_delivery_seq: context.marker_delivery_seq,
            delivered_binding_epoch: context.prior_binding_epoch,
        }],
        historical_causal_facts: vec![],
        sequence: sequence_restore(context, claims),
        order: order_restore(context, claims),
        recovery_marker_delivery_seq: Some(context.marker_delivery_seq),
    })
}

fn sequence_restore(context: DcrContext, claims: DcrClaims) -> SequenceClaimFrontierRestore {
    SequenceClaimFrontierRestore {
        movable_claims: vec![MovableSequenceClaim {
            delivery_seq: claims.membership_exit_seq,
            owner: SequenceDirectOwner::MembershipExit {
                participant_index: context.participant_id,
            },
        }],
        immutable_candidates: vec![],
        products: SequenceProductRangesRestore {
            live_times_terminal: vec![],
            live_times_replacement_terminal: Some(ReplacementTerminalProductRangeRestore {
                start: claims.replacement_product_seq,
                length: 1,
            }),
            other_live_times_exit: vec![],
        },
        recovery: Some(RecoverySequenceBlockRestore {
            terminal: None,
            recovery_attach_seq: claims.recovery_attach_seq,
            replacement_terminal_seq: claims.replacement_terminal_seq,
        }),
    }
}

fn order_restore(context: DcrContext, claims: DcrClaims) -> OrderClaimFrontierRestore {
    OrderClaimFrontierRestore {
        movable_claims: vec![MovableOrderClaim {
            transaction_order: claims.membership_exit_order,
            owner: OrderDirectOwner::MembershipExit {
                participant_index: context.participant_id,
            },
        }],
        immutable_candidates: vec![],
        recovery: Some(RecoveryOrderBlockRestore {
            active_binding: None,
            recovery_operation_order: claims.recovery_operation_order,
            replacement_terminal_order: claims.replacement_terminal_order,
        }),
    }
}

fn validated_frontiers(
    context: DcrContext,
    claims: DcrClaims,
    recovery: DetachedCredentialRecovery,
) -> Result<DcrFrontiers, String> {
    let restore = restore_shape(context, claims)?;
    let sequence = SequenceLedger::try_new(
        claims.terminal_delivery_seq,
        SequenceClaims::new(1, 0, 0, RecoverySequenceReserve::DetachedCredentialRecovery),
    )
    .map_err(|error| format!("DCR sequence ledger failed: {error:?}"))?;
    let order = OrderLedger::try_new(
        OrderHigh::Allocated(1),
        OrderClaims::new(0, 1, true, true)
            .map_err(|error| format!("DCR order claims failed: {error:?}"))?,
    )
    .map_err(|error| format!("DCR order ledger failed: {error:?}"))?;
    let request = MarkerRecordRequest::delivered(
        context.participant_id,
        context.marker_delivery_seq,
        FrontierBinding::Detached(context.prior_binding_epoch),
    );
    let mut prevalidated = ClaimFrontiers::prevalidate(restore.clone(), sequence, order)
        .map_err(|error| format!("DCR frontier prevalidation failed: {error:?}"))?;
    prevalidated
        .take_marker_record(request)
        .ok_or_else(|| "DCR frontier did not own its delivered marker record".to_string())?;
    let frontiers = prevalidated
        .finish(Some(StoredEdge::DetachedCredentialRecovery(recovery)))
        .map_err(|error| format!("DCR frontier finish failed: {error:?}"))?;
    Ok(DcrFrontiers { restore, frontiers })
}

fn fenced_attach_commit(
    context: DcrContext,
    claims: DcrClaims,
    recovery: DetachedCredentialRecovery,
) -> Result<AttachCommit<[u8; 32], [u8; 32]>, String> {
    let proof = recovery
        .fenced_attach(
            validated_marker_record_for_recovery_test(recovery),
            context.debt,
            Event::fenced_recovery_committed(
                context.participant_id,
                context.marker_delivery_seq,
                context.prior_binding_epoch,
                context.recovered_binding_epoch,
                claims.replacement_terminal_seq,
            ),
            DebtCompletion::observer_projection(
                context.debt,
                ObserverProjection::new(claims.replacement_terminal_seq),
            ),
        )
        .map_err(|error| format!("DCR fenced proof mint failed: {error:?}"))?;
    let previous_terminal = previous_terminal(context, claims)?;
    let attach_secret = AttachSecret::new([0x41; 32]);
    let member = LiveMember::restore(LiveMemberRestore {
        participant_id: context.participant_id,
        conversation_id: context.conversation_id,
        generation: context.prior_binding_epoch.capability_generation,
        attach_secret,
        cursor: 0,
        enrollment_fingerprint: EnrollmentFingerprint::new([0x41; 32]),
        latest_terminal: Some(previous_terminal),
    })
    .map_err(|error| format!("DCR member restore failed: {error:?}"))?;
    let verified = member
        .verify_fenced_attach(
            BindingState::Detached,
            attach_request(context, attach_secret),
            AttachSecretProof::Verified,
            proof,
            None,
            attach_parameters(context, claims),
        )
        .map_err(|error| format!("DCR fenced attach verification failed: {error:?}"))?;
    commit_attach(verified, DetachCell::<[u8; 32]>::default())
        .map_err(|error| format!("DCR fenced attach commit failed: {error:?}"))
}

fn previous_terminal(
    context: DcrContext,
    claims: DcrClaims,
) -> Result<super::CommittedBindingTerminal, String> {
    CommittedBindingTerminalRestore {
        binding: ActiveBinding {
            participant_id: context.participant_id,
            conversation_id: context.conversation_id,
            binding_epoch: context.prior_binding_epoch,
        },
        cause: CloseCause::ConnectionLost,
        transaction_order: 1,
        delivery_seq: claims.terminal_delivery_seq,
    }
    .restore()
    .map_err(|error| format!("DCR prior terminal restore failed: {error:?}"))
}

const fn attach_request(
    context: DcrContext,
    attach_secret: AttachSecret,
) -> CredentialAttachRequest {
    CredentialAttachRequest {
        conversation_id: context.conversation_id,
        participant_id: context.participant_id,
        capability_generation: context.prior_binding_epoch.capability_generation,
        attach_secret,
        attach_attempt_token: AttachAttemptToken::new([0x42; 16]),
        accept_marker_delivery_seq: Some(context.marker_delivery_seq),
    }
}

const fn attach_parameters(context: DcrContext, claims: DcrClaims) -> AttachCommitParameters {
    AttachCommitParameters {
        binding: ActiveBinding {
            participant_id: context.participant_id,
            conversation_id: context.conversation_id,
            binding_epoch: context.recovered_binding_epoch,
        },
        attach_secret: AttachSecret::new([0x43; 32]),
        attached_position: AttachedRecordPosition::new(
            claims.recovery_operation_order,
            claims.recovery_attach_seq,
        ),
        receipt_expires_at: 100,
        provenance_expires_at: 200,
    }
}

/// Builds and executes one exact DCR fenced attach with real sequence/order
/// recovery blocks and the one validated marker-record authority.
///
/// # Errors
///
/// Returns a diagnostic when any typed restore, proof, commit, or frontier
/// transition rejects the fixture or when checked claim arithmetic exhausts.
pub fn executable_recovered_attach() -> Result<ExecutableRecoveredAttach, String> {
    let context = context()?;
    let claims = claims(context)?;
    let recovery = credential_recovery(context)?;
    let fixture = validated_frontiers(context, claims, recovery)?;
    let committed = fenced_attach_commit(context, claims, recovery)?;
    let applied = apply_dcr_attach(context, claims, recovery, fixture, committed)?;
    let owner = applied.owner;
    if owner.frontiers().sequence().recovery().is_some()
        || owner.frontiers().order().recovery().is_some()
    {
        return Err("DCR attach did not consume both recovery claim blocks".into());
    }
    let (installed, fate_token) = applied.committed.into_slot_and_fate();
    Ok(ExecutableRecoveredAttach {
        owner,
        member: installed.member,
        binding: installed.binding_state,
        detach_cell: installed.detach_cell,
        fate_token,
        prior_binding_epoch: context.prior_binding_epoch,
        recovered_binding_epoch: context.recovered_binding_epoch,
        marker_delivery_seq: context.marker_delivery_seq,
        next_terminal_sequence: applied.next_terminal_sequence,
        next_terminal_order: applied.next_terminal_order,
    })
}
