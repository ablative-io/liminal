#![allow(clippy::expect_used)]

use alloc::{vec, vec::Vec};

use crate::{
    algebra::{ResourceVector, WideResourceVector},
    outcome::CandidatePhase,
    wire::{BindingEpoch, ConnectionIncarnation, Generation},
};

use super::super::{
    AdmissionOrder, BindingTerminalOwner, ClaimFrontiers, ClaimFrontiersRestore, ClosureAccounting,
    ClosureDebt, ClosureState, FrontierBinding, FrontierParticipant,
    ImmutableOrderCandidateMajorRestore, ImmutableSequenceCandidate, MarkerCandidateAuthority,
    MarkerProvenance, MarkerSequenceOwner, MovableOrderClaim, MovableSequenceClaim,
    ObserverProjection, OrderClaimFrontierRestore, OrderClaims, OrderDirectOwner, OrderHigh,
    OrderLedger, PhysicalCompaction, RecoverySequenceReserve, RetainedCausalRecord,
    RetainedCausalRecordKind, SequenceClaimFrontierRestore, SequenceClaims, SequenceDirectOwner,
    SequenceLedger, SequenceProductRangesRestore, StoredEdge, TerminalProductRangeRestore,
    storage::{MarkerDeliveryRestore, StorageRestoreError},
};
use super::{
    MarkerDrainCommit, MarkerDrainError, RetainedRecordCharge,
    drain_next_marker as drain_next_marker_owned,
};

fn drain_next_marker(
    frontiers: ClaimFrontiers,
    closure: ClosureState,
) -> Result<MarkerDrainCommit, MarkerDrainError> {
    let retained_charges = frontiers
        .retained_records()
        .iter()
        .map(|record| {
            RetainedRecordCharge::new(
                record.delivery_seq,
                record.admission_order,
                ResourceVector::new(1, 1),
            )
        })
        .collect();
    let marker_charge = frontiers
        .sequence()
        .immutable_candidates()
        .first()
        .map_or_else(
            || RetainedRecordCharge::new(0, marker_key(), ResourceVector::new(1, 1)),
            |candidate| {
                RetainedRecordCharge::new(
                    candidate.delivery_seq(),
                    candidate.admission_order(),
                    ResourceVector::new(1, 1),
                )
            },
        );
    let accounting = ClosureAccounting::try_new(
        closure,
        1,
        1,
        0,
        0,
        ResourceVector::default(),
        WideResourceVector::new(1, 1),
        ResourceVector::new(100, 100),
        0,
        2,
    )
    .expect("marker drain accounting fixture is valid");
    drain_next_marker_owned(frontiers, accounting, retained_charges, marker_charge)
}

const PARTICIPANT_ID: u64 = 0;
const CONVERSATION_ID: u64 = 91;

fn epoch() -> BindingEpoch {
    BindingEpoch::new(ConnectionIncarnation::new(91, 1), Generation::ONE)
}

fn marker_key() -> AdmissionOrder {
    AdmissionOrder::new(0, CandidatePhase::CompactionMarker, PARTICIPANT_ID)
}

fn marker_candidates(
    with_marker: bool,
    target_binding: FrontierBinding,
) -> (
    Vec<ImmutableSequenceCandidate>,
    Vec<ImmutableOrderCandidateMajorRestore>,
) {
    if with_marker {
        (
            vec![ImmutableSequenceCandidate::Marker(
                MarkerCandidateAuthority {
                    delivery_seq: 2,
                    admission_order: marker_key(),
                    target_binding,
                    provenance: MarkerProvenance::NonProductM,
                    current_owner: MarkerSequenceOwner::Marker,
                },
            )],
            vec![ImmutableOrderCandidateMajorRestore {
                transaction_order: 0,
                candidate_keys: vec![marker_key()],
            }],
        )
    } else {
        (vec![], vec![])
    }
}

fn bound_claims(
    terminal: BindingTerminalOwner,
    marker_offset: u64,
) -> (
    Vec<MovableSequenceClaim>,
    SequenceProductRangesRestore,
    Vec<MovableOrderClaim>,
) {
    (
        vec![
            MovableSequenceClaim {
                delivery_seq: 2 + marker_offset,
                owner: SequenceDirectOwner::MembershipExit {
                    participant_index: PARTICIPANT_ID,
                },
            },
            MovableSequenceClaim {
                delivery_seq: 3 + marker_offset,
                owner: SequenceDirectOwner::BindingTerminal(terminal),
            },
        ],
        SequenceProductRangesRestore {
            live_times_terminal: vec![TerminalProductRangeRestore {
                start: 4 + marker_offset,
                length: 1,
                terminal,
            }],
            live_times_replacement_terminal: None,
            other_live_times_exit: vec![],
        },
        vec![
            MovableOrderClaim {
                transaction_order: 1,
                owner: OrderDirectOwner::ActiveBindingTerminal(terminal),
            },
            MovableOrderClaim {
                transaction_order: 2,
                owner: OrderDirectOwner::MembershipExit {
                    participant_index: PARTICIPANT_ID,
                },
            },
        ],
    )
}

fn detached_claims(
    marker_offset: u64,
) -> (
    Vec<MovableSequenceClaim>,
    SequenceProductRangesRestore,
    Vec<MovableOrderClaim>,
) {
    (
        vec![MovableSequenceClaim {
            delivery_seq: 2 + marker_offset,
            owner: SequenceDirectOwner::MembershipExit {
                participant_index: PARTICIPANT_ID,
            },
        }],
        SequenceProductRangesRestore::default(),
        vec![MovableOrderClaim {
            transaction_order: 1,
            owner: OrderDirectOwner::MembershipExit {
                participant_index: PARTICIPANT_ID,
            },
        }],
    )
}

fn frontiers(target_binding: FrontierBinding, with_marker: bool) -> ClaimFrontiers {
    let terminal = BindingTerminalOwner {
        participant_index: PARTICIPANT_ID,
        binding_epoch: epoch(),
    };
    let marker_count = u64::from(with_marker);
    let (terminal_count, active_terminal_count, claim_parts) = match target_binding {
        FrontierBinding::Bound(_) => (1, 1, bound_claims(terminal, marker_count)),
        FrontierBinding::Detached(_) => (0, 0, detached_claims(marker_count)),
    };
    let (sequence_movable, products, order_movable) = claim_parts;
    let sequence_ledger = SequenceLedger::try_new(
        1,
        SequenceClaims::new(
            1,
            terminal_count,
            marker_count,
            RecoverySequenceReserve::None,
        ),
    )
    .expect("test sequence reserve fits after H=1");
    let order_ledger = OrderLedger::try_new(
        OrderHigh::Allocated(0),
        OrderClaims::new(active_terminal_count, 1, false, false).expect("no recovery half-pair"),
    )
    .expect("test order reserve fits after major zero");
    let (immutable_candidates, order_candidates) = marker_candidates(with_marker, target_binding);

    ClaimFrontiers::restore(
        ClaimFrontiersRestore {
            conversation_id: CONVERSATION_ID,
            active_identities: vec![FrontierParticipant::new(PARTICIPANT_ID, 0, target_binding)],
            identity_slot_limit: 1,
            retained_floor: 1,
            retained_record_limit: 1,
            retained_records: vec![RetainedCausalRecord {
                delivery_seq: 1,
                admission_order: AdmissionOrder::new(
                    0,
                    CandidatePhase::OrdinaryRecord,
                    PARTICIPANT_ID,
                ),
                kind: RetainedCausalRecordKind::OrdinaryRecord {
                    participant_index: PARTICIPANT_ID,
                },
            }],
            active_marker_anchors: vec![],
            historical_marker_deliveries: vec![],
            historical_causal_facts: vec![],
            sequence: SequenceClaimFrontierRestore {
                movable_claims: sequence_movable,
                immutable_candidates,
                products,
                recovery: None,
            },
            order: OrderClaimFrontierRestore {
                movable_claims: order_movable,
                immutable_candidates: order_candidates,
                recovery: None,
            },
            recovery_marker_delivery_seq: None,
        },
        sequence_ledger,
        order_ledger,
    )
    .expect("complete marker-drain fixture restores")
}

fn debt() -> ClosureDebt {
    ClosureDebt::new(WideResourceVector::new(1, 1)).expect("test debt is nonzero")
}

#[test]
fn drain_consumes_next_marker_and_returns_one_atomic_authority_commit() -> Result<(), &'static str>
{
    let commit = drain_next_marker(
        frontiers(FrontierBinding::Bound(epoch()), true),
        ClosureState::Clear,
    )
    .expect("next bound marker must drain");

    let StoredEdge::MarkerDelivery(delivery) = commit.marker_successor() else {
        return Err("bound candidate did not select marker delivery");
    };
    assert_eq!(delivery.participant_id(), PARTICIPANT_ID);
    assert_eq!(delivery.binding_epoch(), epoch());
    assert_eq!(delivery.marker_delivery_seq(), 2);
    assert_eq!(commit.closure(), ClosureState::Clear);
    assert_eq!(commit.frontiers().conversation_id(), CONVERSATION_ID);
    assert_eq!(commit.frontiers().retained_marker_records().len(), 1);
    assert_eq!(
        commit.frontiers().retained_marker_records()[0].delivery_seq,
        2
    );
    assert_eq!(
        commit.frontiers().retained_marker_records()[0].admission_order,
        marker_key()
    );
    assert_eq!(commit.frontiers().sequence().ledger().high_watermark(), 2);
    assert_eq!(commit.frontiers().sequence().ledger().claims().markers(), 0);
    assert_eq!(
        commit.frontiers().order().ledger().high(),
        OrderHigh::Allocated(0)
    );
    assert!(
        commit
            .frontiers()
            .sequence()
            .immutable_candidates()
            .is_empty()
    );
    assert!(commit.frontiers().order().immutable_candidates().is_empty());
    assert_eq!(commit.frontiers().retained_marker_records().len(), 1);
    Ok(())
}

#[test]
fn missing_candidate_is_an_invariant_fault_and_replay_is_deterministic() {
    assert_eq!(
        drain_next_marker(
            frontiers(FrontierBinding::Bound(epoch()), false),
            ClosureState::Clear,
        ),
        Err(MarkerDrainError::NoCandidate)
    );
    assert_eq!(
        drain_next_marker(
            frontiers(FrontierBinding::Bound(epoch()), true),
            ClosureState::Clear,
        ),
        drain_next_marker(
            frontiers(FrontierBinding::Bound(epoch()), true),
            ClosureState::Clear,
        )
    );
}

#[test]
fn detached_candidate_appends_and_selects_exact_dmr() -> Result<(), &'static str> {
    let commit = drain_next_marker(
        frontiers(FrontierBinding::Detached(epoch()), true),
        ClosureState::Clear,
    )
    .expect("detached marker candidate still appends");

    let StoredEdge::DetachedMarkerRelease(release) = commit.marker_successor() else {
        return Err("detached candidate did not select DMR");
    };
    assert_eq!(release.participant_id(), PARTICIPANT_ID);
    assert_eq!(release.last_dead_binding_epoch(), epoch());
    assert_eq!(release.marker_delivery_seq(), 2);
    assert_eq!(commit.frontiers().sequence().ledger().high_watermark(), 2);
    assert_eq!(commit.frontiers().retained_marker_records().len(), 1);
    Ok(())
}

#[test]
fn drained_detached_marker_cannot_be_reused_as_delivered_recovery_proof() {
    let commit = drain_next_marker(
        frontiers(FrontierBinding::Detached(epoch()), true),
        ClosureState::Clear,
    )
    .expect("detached marker candidate still appends");
    assert!(matches!(
        commit.marker_successor(),
        StoredEdge::DetachedMarkerRelease(_)
    ));

    let undelivered_record = commit.into_record_for_test();
    assert_eq!(
        MarkerDeliveryRestore {
            participant_id: PARTICIPANT_ID,
            binding_epoch: epoch(),
            marker_delivery_seq: 2,
        }
        .restore_detached_delivered_for_test(CONVERSATION_ID, undelivered_record),
        Err(StorageRestoreError::StoredEdgeProvenance),
        "a DMR append lacks the delivery occurrence required by DCR/fenced recovery"
    );
}

#[test]
fn op_and_pc_remain_strict_while_bound_marker_appends() {
    let closure_debt = debt();
    let op_commit = drain_next_marker(
        frontiers(FrontierBinding::Bound(epoch()), true),
        ClosureState::Owed {
            debt: closure_debt,
            edge: StoredEdge::ObserverProjection(ObserverProjection::new(1)),
        },
    )
    .expect("marker append advances exact current OP");
    assert_eq!(
        op_commit.closure(),
        ClosureState::Owed {
            debt: closure_debt,
            edge: StoredEdge::ObserverProjection(ObserverProjection::new(2)),
        }
    );

    let compaction = PhysicalCompaction::new(1, 1).expect("test PC range is valid");
    let pc_commit = drain_next_marker(
        frontiers(FrontierBinding::Bound(epoch()), true),
        ClosureState::Owed {
            debt: closure_debt,
            edge: StoredEdge::PhysicalCompaction(compaction),
        },
    )
    .expect("marker append preserves exact current PC");
    assert_eq!(
        pc_commit.closure(),
        ClosureState::Owed {
            debt: closure_debt,
            edge: StoredEdge::PhysicalCompaction(compaction),
        }
    );
}

#[test]
fn already_materialized_marker_edge_cannot_coexist_with_pending_candidate() {
    let selected = drain_next_marker(
        frontiers(FrontierBinding::Bound(epoch()), true),
        ClosureState::Clear,
    )
    .expect("fixture obtains the sealed marker successor")
    .marker_successor();

    assert_eq!(
        drain_next_marker(
            frontiers(FrontierBinding::Bound(epoch()), true),
            ClosureState::Owed {
                debt: debt(),
                edge: selected,
            },
        ),
        Err(MarkerDrainError::CurrentEdgeMismatch),
        "candidate-not-consumed and already-materialized successor is corrupt"
    );
}
