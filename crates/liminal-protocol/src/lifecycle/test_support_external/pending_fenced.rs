use super::{
    ActiveBinding, AdmissionOrder, AttachSecret, BindingState, BindingTerminalOwner,
    CandidatePhase, ClaimFrontiers, CloseCause, ClosureState, CursorFateSuccessor, DcrClaims,
    DcrContext, DcrFrontiers, DetachCell, DetachedCredentialRecovery, EnrollmentFingerprint, Event,
    ExecutablePendingFencedAttach, FrontierBinding, FrontierParticipant,
    ImmutableOrderCandidateMajorRestore, ImmutableSequenceCandidate, LiveMember, LiveMemberRestore,
    MarkerRecordRequest, OrderClaims, OrderHigh, OrderLedger, PendingFinalizationRestore,
    RecoverySequenceReserve, ReplacementTerminalProductRangeRestore, SequenceClaims,
    SequenceLedger, StoredEdge, String, TerminalProductRangeRestore, ToString, claims, context,
    epoch, format, frontier_owner, marker_delivery_for_test, restore_shape, vec,
};

fn pending_context() -> Result<DcrContext, String> {
    let mut pending = context()?;
    pending.marker_delivery_seq = 3;
    pending.prior_binding_epoch = pending.recovered_binding_epoch;
    pending.recovered_binding_epoch = epoch(3, 3, 3)?;
    Ok(pending)
}

fn pending_claims(context: DcrContext) -> Result<DcrClaims, String> {
    let mut pending = claims(context)?;
    pending.marker_order =
        AdmissionOrder::new(2, CandidatePhase::CompactionMarker, context.participant_id);
    pending.recovery_operation_order = 4;
    pending.replacement_terminal_order = 5;
    pending.membership_exit_order = 6;
    Ok(pending)
}

fn pending_recovery(context: DcrContext) -> Result<DetachedCredentialRecovery, String> {
    let delivery = marker_delivery_for_test(
        context.participant_id,
        context.prior_binding_epoch,
        context.marker_delivery_seq,
    )
    .map_err(|error| format!("pending DCR marker delivery restore failed: {error:?}"))?;
    let delivered = delivery
        .delivered(
            context.debt,
            Event::marker_delivered(
                context.participant_id,
                context.prior_binding_epoch,
                context.marker_delivery_seq,
            ),
        )
        .map_err(|error| format!("pending DCR marker delivery failed: {error:?}"))?;
    let ClosureState::Owed {
        edge: StoredEdge::ParticipantCursorProgress(progress),
        ..
    } = delivered
    else {
        return Err("pending DCR marker did not produce cursor progress".into());
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
        .map_err(|error| format!("pending DCR binding fate failed: {error:?}"))?;
    let CursorFateSuccessor::DetachedCredentialRecovery(recovery) = successor else {
        return Err("pending DCR binding fate did not produce recovery".into());
    };
    Ok(recovery)
}

fn pending_frontiers(
    context: DcrContext,
    claims: DcrClaims,
    recovery: DetachedCredentialRecovery,
) -> Result<DcrFrontiers, String> {
    let mut restore = restore_shape(context, claims)?;
    let owner = BindingTerminalOwner {
        participant_index: context.participant_id,
        binding_epoch: context.prior_binding_epoch,
    };
    restore.active_identities = vec![FrontierParticipant::new(
        context.participant_id,
        context.marker_delivery_seq,
        FrontierBinding::Detached(context.prior_binding_epoch),
    )];
    let exit_seq = claims
        .replacement_terminal_seq
        .checked_add(1)
        .ok_or_else(|| "pending DCR exit sequence overflow".to_string())?;
    let terminal_product_seq = exit_seq
        .checked_add(1)
        .ok_or_else(|| "pending DCR terminal product overflow".to_string())?;
    let replacement_product_seq = terminal_product_seq
        .checked_add(1)
        .ok_or_else(|| "pending DCR replacement product overflow".to_string())?;
    let exit = restore
        .sequence
        .movable_claims
        .first_mut()
        .ok_or_else(|| "pending DCR membership exit is absent".to_string())?;
    exit.delivery_seq = exit_seq;
    let terminal_order = claims
        .recovery_operation_order
        .checked_sub(1)
        .ok_or_else(|| "pending DCR terminal order underflow".to_string())?;
    let admission_order = AdmissionOrder::new(
        terminal_order,
        CandidatePhase::BindingTerminal,
        context.participant_id,
    );
    restore.sequence.immutable_candidates = vec![ImmutableSequenceCandidate::BindingTerminal {
        delivery_seq: claims.terminal_delivery_seq,
        admission_order,
        owner,
    }];
    restore.order.immutable_candidates = vec![ImmutableOrderCandidateMajorRestore {
        transaction_order: terminal_order,
        candidate_keys: vec![admission_order],
    }];
    restore.sequence.products.live_times_terminal = vec![TerminalProductRangeRestore {
        start: terminal_product_seq,
        length: 1,
        terminal: owner,
    }];
    restore.sequence.products.live_times_replacement_terminal =
        Some(ReplacementTerminalProductRangeRestore {
            start: replacement_product_seq,
            length: 1,
        });
    restore.retained_records.truncate(1);
    restore.retained_record_limit = 3;
    let sequence = SequenceLedger::try_new(
        claims
            .terminal_delivery_seq
            .checked_sub(1)
            .ok_or_else(|| "pending DCR sequence high underflow".to_string())?,
        SequenceClaims::new(1, 1, 0, RecoverySequenceReserve::DetachedCredentialRecovery),
    )
    .map_err(|error| format!("pending DCR sequence ledger failed: {error:?}"))?;
    let order = OrderLedger::try_new(
        OrderHigh::Allocated(terminal_order),
        OrderClaims::new(0, 1, true, true)
            .map_err(|error| format!("pending DCR order claims failed: {error:?}"))?,
    )
    .map_err(|error| format!("pending DCR order ledger failed: {error:?}"))?;
    let mut prevalidated = ClaimFrontiers::prevalidate(restore.clone(), sequence, order)
        .map_err(|error| format!("pending DCR frontier prevalidation failed: {error:?}"))?;
    let request = MarkerRecordRequest::delivered(
        context.participant_id,
        context.marker_delivery_seq,
        FrontierBinding::Detached(context.prior_binding_epoch),
    );
    prevalidated
        .take_marker_record(request)
        .ok_or_else(|| "pending DCR frontier did not own its marker".to_string())?;
    let frontiers = prevalidated
        .finish(Some(StoredEdge::DetachedCredentialRecovery(recovery)))
        .map_err(|error| format!("pending DCR frontier finish failed: {error:?}"))?;
    Ok(DcrFrontiers { restore, frontiers })
}

/// Builds the exact pre-commit authority for a fenced Attached that finalizes
/// one Pending Died terminal through real DCR sequence/order blocks.
///
/// # Errors
///
/// Returns a diagnostic when any terminal, recovery, member, frontier, or
/// closure-accounting restore rejects the canonical fixture.
pub fn executable_pending_fenced_attach() -> Result<ExecutablePendingFencedAttach, String> {
    let context = pending_context()?;
    let claims = pending_claims(context)?;
    executable_pending_fenced_attach_with_claims(context, claims)
}

/// Builds the Pending-Died fenced fixture after the downstream ordinary setup.
///
/// The real-selector setup's peer Attached advances the acknowledged marker
/// boundary by one, while its debt-establishing record consumes one additional
/// order before Pending Died is selected.
///
/// # Errors
///
/// Returns a diagnostic when marker/order arithmetic exhausts or the resulting
/// terminal, recovery, member, frontier, or closure-accounting restore fails.
pub fn executable_pending_fenced_attach_after_ordinary_setup()
-> Result<ExecutablePendingFencedAttach, String> {
    let mut context = pending_context()?;
    context.marker_delivery_seq = context
        .marker_delivery_seq
        .checked_add(1)
        .ok_or_else(|| "pending DCR marker sequence overflow".to_string())?;
    let mut claims = pending_claims(context)?;
    claims.recovery_operation_order = claims
        .recovery_operation_order
        .checked_add(1)
        .ok_or_else(|| "pending DCR recovery order overflow".to_string())?;
    claims.replacement_terminal_order = claims
        .replacement_terminal_order
        .checked_add(1)
        .ok_or_else(|| "pending DCR replacement order overflow".to_string())?;
    claims.membership_exit_order = claims
        .membership_exit_order
        .checked_add(1)
        .ok_or_else(|| "pending DCR membership-exit order overflow".to_string())?;
    executable_pending_fenced_attach_with_claims(context, claims)
}

fn executable_pending_fenced_attach_with_claims(
    context: DcrContext,
    claims: DcrClaims,
) -> Result<ExecutablePendingFencedAttach, String> {
    let recovery = pending_recovery(context)?;
    let fixture = pending_frontiers(context, claims, recovery)?;
    let owner = frontier_owner(context, recovery, fixture)?;
    let pending = PendingFinalizationRestore {
        binding: ActiveBinding {
            participant_id: context.participant_id,
            conversation_id: context.conversation_id,
            binding_epoch: context.prior_binding_epoch,
        },
        cause: CloseCause::ConnectionLost,
        transaction_order: claims
            .recovery_operation_order
            .checked_sub(1)
            .ok_or_else(|| "pending DCR terminal order underflow".to_string())?,
    }
    .restore()
    .map_err(|error| format!("pending DCR finalization restore failed: {error:?}"))?;
    let attach_secret = AttachSecret::new([0x41; 32]);
    let member = LiveMember::restore(LiveMemberRestore {
        participant_id: context.participant_id,
        conversation_id: context.conversation_id,
        generation: context.prior_binding_epoch.capability_generation,
        attach_secret,
        cursor: 0,
        enrollment_fingerprint: EnrollmentFingerprint::new([0x41; 32]),
        latest_terminal: None,
    })
    .map_err(|error| format!("pending DCR member restore failed: {error:?}"))?;
    Ok(ExecutablePendingFencedAttach {
        owner,
        recovery,
        member,
        binding: BindingState::PendingFinalization(pending),
        detach_cell: DetachCell::default(),
        attach_secret,
        prior_binding_epoch: context.prior_binding_epoch,
        recovered_binding_epoch: context.recovered_binding_epoch,
        marker_delivery_seq: context.marker_delivery_seq,
        terminal_order: claims
            .recovery_operation_order
            .checked_sub(1)
            .ok_or_else(|| "pending DCR terminal order underflow".to_string())?,
        terminal_delivery_seq: claims.terminal_delivery_seq,
        attached_order: claims.recovery_operation_order,
        attached_seq: claims.recovery_attach_seq,
        fenced_resulting_floor: claims.replacement_terminal_seq,
        predecessor_debt: context.debt.value(),
    })
}
