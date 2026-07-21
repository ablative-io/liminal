use alloc::{
    format,
    string::{String, ToString},
    vec,
};

use crate::{
    algebra::{ResourceVector, WideResourceVector},
    wire::{BindingEpoch, ConnectionIncarnation, DeliverySeq, Generation},
};

use super::{
    BindingFateTerminal, BindingTerminalAdmission, BindingTerminalCauseClass, LiveFrontierOwner,
    MeasuredBindingFate,
};
use crate::lifecycle::{
    ActiveBinding, BindingState, BindingTerminalDisposition, BindingTerminalOwner, ClaimFrontiers,
    ClaimFrontiersRestore, ClosureAccounting, ClosureDebt, ClosureState, CommittedDiedTerminal,
    DebtCompletion, DiedBindingTransition, Event, FrontierBinding, FrontierParticipant,
    MovableOrderClaim, MovableSequenceClaim, ObserverProjection, OrderClaimFrontierRestore,
    OrderClaims, OrderDirectOwner, OrderHigh, OrderLedger, RecoverySequenceReserve,
    SealedBindingFateToken, SequenceClaimFrontierRestore, SequenceClaims, SequenceDirectOwner,
    SequenceLedger, SequenceProductRangesRestore, TerminalProductRangeRestore,
};

fn generation(value: u64) -> Result<Generation, String> {
    Generation::new(value).ok_or_else(|| "test generation must be nonzero".to_string())
}

fn epoch(server: u64, ordinal: u64, generation_value: u64) -> Result<BindingEpoch, String> {
    Ok(BindingEpoch::new(
        ConnectionIncarnation::new(server, ordinal),
        generation(generation_value)?,
    ))
}

fn debt(entries: u128, bytes: u128) -> Result<ClosureDebt, String> {
    ClosureDebt::new(WideResourceVector::new(entries, bytes))
        .ok_or_else(|| "test closure debt must be nonzero".to_string())
}

pub(super) fn frontier_owner_with_limit(
    conversation_id: u64,
    participant_id: u64,
    binding_epoch: BindingEpoch,
    cursor: DeliverySeq,
    high_watermark: DeliverySeq,
    retained_record_limit: u64,
) -> Result<LiveFrontierOwner, String> {
    let terminal = BindingTerminalOwner {
        participant_index: participant_id,
        binding_epoch,
    };
    let sequence = SequenceLedger::try_new(
        high_watermark,
        SequenceClaims::new(1, 1, 0, RecoverySequenceReserve::None),
    )
    .map_err(|error| format!("fate sequence ledger refused: {error:?}"))?;
    let order = OrderLedger::try_new(
        OrderHigh::Empty,
        OrderClaims::new(1, 1, false, false)
            .map_err(|error| format!("fate order claims refused: {error:?}"))?,
    )
    .map_err(|error| format!("fate order ledger refused: {error:?}"))?;
    let terminal_sequence = high_watermark
        .checked_add(1)
        .ok_or_else(|| "terminal claim overflow".to_string())?;
    let exit_sequence = terminal_sequence
        .checked_add(1)
        .ok_or_else(|| "exit claim overflow".to_string())?;
    let product_sequence = exit_sequence
        .checked_add(1)
        .ok_or_else(|| "product claim overflow".to_string())?;
    let frontiers = ClaimFrontiers::restore(
        ClaimFrontiersRestore {
            conversation_id,
            active_identities: vec![FrontierParticipant::new(
                participant_id,
                cursor,
                FrontierBinding::Bound(binding_epoch),
            )],
            identity_slot_limit: participant_id
                .checked_add(1)
                .ok_or_else(|| "identity limit overflow".to_string())?,
            retained_floor: u128::from(high_watermark) + 1,
            retained_record_limit: 0,
            retained_records: vec![],
            active_marker_anchors: vec![],
            historical_marker_deliveries: vec![],
            historical_causal_facts: vec![],
            sequence: SequenceClaimFrontierRestore {
                movable_claims: vec![
                    MovableSequenceClaim {
                        delivery_seq: terminal_sequence,
                        owner: SequenceDirectOwner::BindingTerminal(terminal),
                    },
                    MovableSequenceClaim {
                        delivery_seq: exit_sequence,
                        owner: SequenceDirectOwner::MembershipExit {
                            participant_index: participant_id,
                        },
                    },
                ],
                immutable_candidates: vec![],
                products: SequenceProductRangesRestore {
                    live_times_terminal: vec![TerminalProductRangeRestore {
                        start: product_sequence,
                        length: 1,
                        terminal,
                    }],
                    ..SequenceProductRangesRestore::default()
                },
                recovery: None,
            },
            order: OrderClaimFrontierRestore {
                movable_claims: vec![
                    MovableOrderClaim {
                        transaction_order: 0,
                        owner: OrderDirectOwner::ActiveBindingTerminal(terminal),
                    },
                    MovableOrderClaim {
                        transaction_order: 1,
                        owner: OrderDirectOwner::MembershipExit {
                            participant_index: participant_id,
                        },
                    },
                ],
                immutable_candidates: vec![],
                recovery: None,
            },
            recovery_marker_delivery_seq: None,
        },
        sequence,
        order,
    )
    .map_err(|error| format!("fate frontier refused: {error:?}"))?;
    Ok(LiveFrontierOwner::from_test_parts(
        frontiers,
        clear_accounting()?,
        vec![],
        retained_record_limit,
    ))
}

fn clear_accounting() -> Result<ClosureAccounting, String> {
    ClosureAccounting::try_new(
        ClosureState::Clear,
        0,
        0,
        0,
        0,
        ResourceVector::default(),
        WideResourceVector::default(),
        ResourceVector::new(16, 1024),
        0,
        2,
    )
    .map_err(|error| format!("fate accounting refused: {error:?}"))
}

fn committed_died_owner(
    active: ActiveBinding,
    cursor: DeliverySeq,
    high_watermark: DeliverySeq,
) -> Result<(LiveFrontierOwner, CommittedDiedTerminal), String> {
    let candidate_sequence = high_watermark
        .checked_add(1)
        .ok_or_else(|| "committed terminal sequence overflow".to_string())?;
    let owner = frontier_owner_with_limit(
        active.conversation_id,
        active.participant_id,
        active.binding_epoch,
        cursor,
        high_watermark,
        1,
    )?;
    let prepared = owner
        .prepare_binding_terminal(
            active,
            BindingTerminalCauseClass::Died,
            0,
            candidate_sequence,
            high_watermark,
        )
        .map_err(|refused| format!("committed terminal prepare refused: {:?}", refused.error()))?;
    let key = prepared.candidate_key();
    let BindingTerminalAdmission::Commit(committed) =
        prepared.admit(key.bind_v3_charge(ResourceVector::new(1, 73)))
    else {
        return Err("Died selector did not commit the ordinary terminal".to_string());
    };
    let (owner, position) = committed.into_parts();
    let DiedBindingTransition::Committed(terminal) =
        active.connection_lost(BindingTerminalDisposition::Committed(position))
    else {
        return Err("committed selector position did not produce committed Died".to_string());
    };
    Ok((owner, terminal))
}

fn pending_died_owner(
    active: ActiveBinding,
    cursor: DeliverySeq,
    high_watermark: DeliverySeq,
) -> Result<LiveFrontierOwner, String> {
    let candidate_sequence = high_watermark
        .checked_add(1)
        .ok_or_else(|| "pending terminal sequence overflow".to_string())?;
    let owner = frontier_owner_with_limit(
        active.conversation_id,
        active.participant_id,
        active.binding_epoch,
        cursor,
        high_watermark,
        0,
    )?;
    let prepared = owner
        .prepare_binding_terminal(
            active,
            BindingTerminalCauseClass::Died,
            0,
            candidate_sequence,
            high_watermark,
        )
        .map_err(|refused| format!("pending terminal prepare refused: {:?}", refused.error()))?;
    let key = prepared.candidate_key();
    let BindingTerminalAdmission::Pending(pending) =
        prepared.admit(key.bind_v3_charge(ResourceVector::new(1, 73)))
    else {
        return Err("Died selector did not pend the recovered terminal".to_string());
    };
    let (owner, _) = pending.into_parts();
    Ok(owner)
}

pub(super) fn ordinary_token()
-> Result<(SealedBindingFateToken, ActiveBinding, DeliverySeq), String> {
    let commit = super::super::operation_event_tests::superseding_attach_commit();
    let binding_state = commit.binding_state;
    let cursor = commit.member.cursor();
    let (_, token) = commit.into_slot_and_fate();
    let BindingState::Bound(binding) = binding_state else {
        return Err("ordinary attach did not install a bound binding".to_string());
    };
    Ok((token, binding, cursor))
}

fn recovered_token() -> Result<(SealedBindingFateToken, u64, BindingEpoch, DeliverySeq), String> {
    let participant_id = 4;
    let marker_delivery_seq = 14;
    let recovery_debt = debt(2, 20)?;
    let attached_debt = debt(2, 18)?;
    let prior_epoch = epoch(1, 2, 2)?;
    let recovered_epoch = epoch(1, 3, 3)?;
    let delivery = super::super::edge::marker_delivery_for_test(
        participant_id,
        prior_epoch,
        marker_delivery_seq,
    )
    .map_err(|_| "validated marker fixture refused".to_string())?;
    let delivered = delivery
        .delivered(
            recovery_debt,
            Event::marker_delivered(participant_id, prior_epoch, marker_delivery_seq),
        )
        .map_err(|_| "marker delivery refused".to_string())?;
    let ClosureState::Owed {
        edge: super::super::StoredEdge::ParticipantCursorProgress(progress),
        ..
    } = delivered
    else {
        return Err("marker delivery selected a non-progress edge".to_string());
    };
    let recovery = match progress
        .binding_fate(
            recovery_debt,
            Event::binding_fate_observed(participant_id, prior_epoch, marker_delivery_seq),
        )
        .map_err(|_| "marker fate refused".to_string())?
    {
        super::super::CursorFateSuccessor::DetachedCredentialRecovery(recovery) => recovery,
        super::super::CursorFateSuccessor::DetachedCursorRelease(_) => {
            return Err("marker fate selected cursor release".to_string());
        }
    };
    let proof = recovery
        .fenced_attach(
            super::super::edge::validated_marker_record_for_recovery_test(recovery),
            recovery_debt,
            Event::fenced_recovery_committed(
                participant_id,
                marker_delivery_seq,
                prior_epoch,
                recovered_epoch,
                marker_delivery_seq
                    .checked_add(1)
                    .ok_or_else(|| "recovery floor overflow".to_string())?,
            ),
            DebtCompletion::observer_projection(
                attached_debt,
                ObserverProjection::new(
                    marker_delivery_seq
                        .checked_add(1)
                        .ok_or_else(|| "projection boundary overflow".to_string())?,
                ),
            ),
        )
        .map_err(|_| "fenced recovery proof refused".to_string())?;
    Ok((
        SealedBindingFateToken::from_recovered_for_test(proof),
        participant_id,
        recovered_epoch,
        marker_delivery_seq,
    ))
}

#[test]
fn ordinary_binding_fate_projects_measured_resulting_floor() -> Result<(), String> {
    let (token, binding, cursor) = ordinary_token()?;
    let high_watermark = cursor
        .checked_add(1)
        .ok_or_else(|| "ordinary high watermark overflow".to_string())?;
    let (owner, died) = committed_died_owner(binding, cursor, high_watermark)?;
    let candidate_high_watermark = owner.frontiers().sequence().ledger().high_watermark();
    let observed_floor = candidate_high_watermark
        .checked_add(1)
        .ok_or_else(|| "ordinary observed floor overflow".to_string())?;
    let prepared = owner
        .prepare_binding_fate(
            token,
            BindingFateTerminal::Ordinary(died),
            candidate_high_watermark,
        )
        .map_err(|refused| format!("ordinary measurement refused: {:?}", refused.error()))?;
    let MeasuredBindingFate::Ordinary(fate) = prepared.fate() else {
        return Err("ordinary token produced recovered fate".to_string());
    };
    assert_eq!(fate.resulting_floor(), observed_floor);
    assert_eq!(
        fate.observer_progress_projection().new_observer_progress(),
        observed_floor
    );
    assert_eq!(
        prepared.event(),
        Event::binding_fate_observed(
            binding.participant_id,
            binding.binding_epoch,
            observed_floor,
        )
    );
    let (owner, _, _) = prepared.into_parts();
    assert_eq!(
        owner.frontiers().retained_floor(),
        u128::from(observed_floor)
    );
    assert!(owner.frontiers().retained_records().is_empty());
    assert!(owner.retained_charges().is_empty());
    let participant = owner.frontiers().active_identities().participants()[0];
    assert_eq!(participant.cursor(), candidate_high_watermark);
    assert_eq!(
        participant.binding(),
        FrontierBinding::Detached(binding.binding_epoch)
    );
    Ok(())
}

#[test]
fn recovered_binding_fate_projects_measured_resulting_floor() -> Result<(), String> {
    let (token, participant_id, binding_epoch, cursor) = recovered_token()?;
    let high_watermark = cursor
        .checked_add(1)
        .ok_or_else(|| "recovered high watermark overflow".to_string())?;
    let active = ActiveBinding {
        participant_id,
        conversation_id: 1,
        binding_epoch,
    };
    let owner = pending_died_owner(active, cursor, high_watermark)?;
    let candidate_high_watermark = owner.frontiers().sequence().ledger().high_watermark();
    let observed_floor = candidate_high_watermark
        .checked_add(1)
        .ok_or_else(|| "recovered observed floor overflow".to_string())?;
    let prepared = owner
        .prepare_binding_fate(
            token,
            BindingFateTerminal::Recovered,
            candidate_high_watermark,
        )
        .map_err(|refused| format!("recovered measurement refused: {:?}", refused.error()))?;
    let MeasuredBindingFate::Recovered(fate) = prepared.fate() else {
        return Err("recovered token produced ordinary fate".to_string());
    };
    assert_eq!(fate.resulting_floor(), observed_floor);
    assert_eq!(
        fate.observer_progress_projection().new_observer_progress(),
        observed_floor
    );
    assert_eq!(
        prepared.event(),
        Event::binding_fate_observed(participant_id, binding_epoch, observed_floor,)
    );
    let (owner, _, _) = prepared.into_parts();
    assert_eq!(
        owner.frontiers().retained_floor(),
        u128::from(observed_floor)
    );
    let participant = owner.frontiers().active_identities().participants()[0];
    assert_eq!(participant.cursor(), candidate_high_watermark);
    assert_eq!(
        participant.binding(),
        FrontierBinding::Detached(binding_epoch)
    );
    Ok(())
}
