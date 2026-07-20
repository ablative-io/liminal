use std::error::Error;
use std::sync::Arc;

use liminal::durability::bridge::block_on;
use liminal::durability::{DurableStore, open_ephemeral};
use liminal_protocol::algebra::{ResourceVector, WideResourceVector};
use liminal_protocol::lifecycle::{
    AdmissionOrder, BindingTerminalOwner, ClaimFrontiers, ClaimFrontiersRestore, ClosureAccounting,
    ClosureDebt, ClosureState, CursorFateSuccessor, DebtCompletion, Event, FrontierBinding,
    FrontierParticipant, ImmutableOrderCandidateMajorRestore, ImmutableSequenceCandidate,
    LiveFrontierOwner, MarkerCandidateAuthority, MarkerProvenance, MarkerSequenceOwner,
    MintFencedAttachResult, MovableOrderClaim, MovableSequenceClaim, OrderClaimFrontierRestore,
    OrderClaims, OrderDirectOwner, OrderHigh, OrderLedger, RecoverySequenceReserve,
    RetainedCausalRecord, RetainedCausalRecordKind, RetainedFencedMarkerSource,
    RetainedRecordCharge, SequenceClaimFrontierRestore, SequenceClaims, SequenceDirectOwner,
    SequenceLedger, SequenceProductRangesRestore, StoredEdge, TerminalProductRangeRestore,
    drain_next_marker,
};
use liminal_protocol::outcome::CandidatePhase;
use liminal_protocol::wire::{BindingEpoch, ConnectionIncarnation, Generation};

use super::log::{
    OperationLog, StoredMarkerDrain, StoredOperation, StoredResourceVector, StoredRetainedCharge,
};
use super::marker_source::{
    MarkerSourceRefusalReason, canonical_marker_bytes, validate_marker_source,
};

pub(super) const CONVERSATION: u64 = 1;
pub(super) const PARTICIPANT: u64 = 0;
pub(super) const MARKER: u64 = 5;

struct MarkerClaims {
    sequence: SequenceClaims,
    movable_sequence: Vec<MovableSequenceClaim>,
    products: SequenceProductRangesRestore,
    order: OrderClaims,
    movable_order: Vec<MovableOrderClaim>,
}

pub(super) struct SourceFixture {
    pub(super) store: Arc<dyn DurableStore>,
    pub(super) retained: RetainedFencedMarkerSource,
    pub(super) row: StoredMarkerDrain,
}

pub(super) fn epoch() -> Result<BindingEpoch, Box<dyn Error>> {
    let generation = Generation::new(2).ok_or("fixture generation must be nonzero")?;
    Ok(BindingEpoch::new(
        ConnectionIncarnation::new(1, 1),
        generation,
    ))
}

fn marker_claims(
    target: FrontierBinding,
    exit_seq: u64,
    terminal: BindingTerminalOwner,
) -> Result<MarkerClaims, Box<dyn Error>> {
    match target {
        FrontierBinding::Bound(_) => {
            let terminal_seq = exit_seq
                .checked_add(1)
                .ok_or("terminal sequence overflow")?;
            let product_seq = terminal_seq
                .checked_add(1)
                .ok_or("product sequence overflow")?;
            Ok(MarkerClaims {
                sequence: SequenceClaims::new(1, 1, 1, RecoverySequenceReserve::None),
                movable_sequence: vec![
                    MovableSequenceClaim {
                        delivery_seq: exit_seq,
                        owner: SequenceDirectOwner::MembershipExit {
                            participant_index: PARTICIPANT,
                        },
                    },
                    MovableSequenceClaim {
                        delivery_seq: terminal_seq,
                        owner: SequenceDirectOwner::BindingTerminal(terminal),
                    },
                ],
                products: SequenceProductRangesRestore {
                    live_times_terminal: vec![TerminalProductRangeRestore {
                        start: product_seq,
                        length: 1,
                        terminal,
                    }],
                    live_times_replacement_terminal: None,
                    other_live_times_exit: vec![],
                },
                order: OrderClaims::new(1, 1, false, false)
                    .map_err(|error| format!("bound order claims: {error:?}"))?,
                movable_order: vec![
                    MovableOrderClaim {
                        transaction_order: 1,
                        owner: OrderDirectOwner::ActiveBindingTerminal(terminal),
                    },
                    MovableOrderClaim {
                        transaction_order: 2,
                        owner: OrderDirectOwner::MembershipExit {
                            participant_index: PARTICIPANT,
                        },
                    },
                ],
            })
        }
        FrontierBinding::Detached(_) => Ok(MarkerClaims {
            sequence: SequenceClaims::new(1, 0, 1, RecoverySequenceReserve::None),
            movable_sequence: vec![MovableSequenceClaim {
                delivery_seq: exit_seq,
                owner: SequenceDirectOwner::MembershipExit {
                    participant_index: PARTICIPANT,
                },
            }],
            products: SequenceProductRangesRestore::default(),
            order: OrderClaims::new(0, 1, false, false)
                .map_err(|error| format!("detached order claims: {error:?}"))?,
            movable_order: vec![MovableOrderClaim {
                transaction_order: 1,
                owner: OrderDirectOwner::MembershipExit {
                    participant_index: PARTICIPANT,
                },
            }],
        }),
    }
}

fn marker_frontier(
    target: FrontierBinding,
    cursor: u64,
) -> Result<(ClaimFrontiersRestore, SequenceLedger, OrderLedger), Box<dyn Error>> {
    let high = MARKER.checked_sub(1).ok_or("marker must be positive")?;
    let exit_seq = MARKER.checked_add(1).ok_or("exit sequence overflow")?;
    let terminal = BindingTerminalOwner {
        participant_index: PARTICIPANT,
        binding_epoch: match target {
            FrontierBinding::Bound(value) | FrontierBinding::Detached(value) => value,
        },
    };
    let claims = marker_claims(target, exit_seq, terminal)?;
    let sequence = SequenceLedger::try_new(high, claims.sequence)
        .map_err(|error| format!("sequence ledger: {error:?}"))?;
    let order = OrderLedger::try_new(OrderHigh::Allocated(0), claims.order)
        .map_err(|error| format!("order ledger: {error:?}"))?;
    let ordinary_order = AdmissionOrder::new(0, CandidatePhase::OrdinaryRecord, PARTICIPANT);
    let marker_order = AdmissionOrder::new(0, CandidatePhase::CompactionMarker, PARTICIPANT);
    Ok((
        ClaimFrontiersRestore {
            conversation_id: CONVERSATION,
            active_identities: vec![FrontierParticipant::new(PARTICIPANT, cursor, target)],
            identity_slot_limit: 1,
            retained_floor: u128::from(high),
            retained_record_limit: 1,
            retained_records: vec![RetainedCausalRecord {
                delivery_seq: high,
                admission_order: ordinary_order,
                kind: RetainedCausalRecordKind::OrdinaryRecord {
                    participant_index: PARTICIPANT,
                },
            }],
            active_marker_anchors: vec![],
            historical_marker_deliveries: vec![],
            historical_causal_facts: vec![],
            sequence: SequenceClaimFrontierRestore {
                movable_claims: claims.movable_sequence,
                immutable_candidates: vec![ImmutableSequenceCandidate::Marker(
                    MarkerCandidateAuthority {
                        delivery_seq: MARKER,
                        admission_order: marker_order,
                        target_binding: target,
                        provenance: MarkerProvenance::NonProductM,
                        abandoned_after: cursor,
                        abandoned_through: high,
                        physical_floor_at_decision: high,
                        current_owner: MarkerSequenceOwner::Marker,
                    },
                )],
                products: claims.products,
                recovery: None,
            },
            order: OrderClaimFrontierRestore {
                movable_claims: claims.movable_order,
                immutable_candidates: vec![ImmutableOrderCandidateMajorRestore {
                    transaction_order: marker_order.transaction_order(),
                    candidate_keys: vec![marker_order],
                }],
                recovery: None,
            },
            recovery_marker_delivery_seq: None,
        },
        sequence,
        order,
    ))
}

fn drained_frontier(
    target: FrontierBinding,
) -> Result<(LiveFrontierOwner, StoredEdge), Box<dyn Error>> {
    let prior_floor = MARKER
        .checked_sub(1)
        .ok_or("prior marker floor underflow")?;
    let (restore, sequence, order) = marker_frontier(target, prior_floor)?;
    let frontiers = ClaimFrontiers::restore(restore, sequence, order)
        .map_err(|error| format!("frontier restore: {error:?}"))?;
    let retained = frontiers
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
    let candidate = frontiers
        .sequence()
        .immutable_candidates()
        .first()
        .copied()
        .ok_or("marker candidate absent")?;
    let marker_charge = RetainedRecordCharge::new(
        candidate.delivery_seq(),
        candidate.admission_order(),
        ResourceVector::new(1, 1),
    );
    let accounting = ClosureAccounting::try_new(
        ClosureState::Clear,
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
    .map_err(|error| format!("closure accounting: {error:?}"))?;
    let commit = drain_next_marker(frontiers, accounting, retained, marker_charge)
        .map_err(|error| format!("marker drain: {error:?}"))?;
    let (owner, successor, _) = LiveFrontierOwner::from_marker_drain(commit, 2);
    Ok((owner, successor))
}

fn recovery() -> Result<liminal_protocol::lifecycle::DetachedCredentialRecovery, Box<dyn Error>> {
    let prior = epoch()?;
    let (_, successor) = drained_frontier(FrontierBinding::Bound(prior))?;
    let StoredEdge::MarkerDelivery(delivery) = successor else {
        return Err("bound marker did not produce delivery".into());
    };
    let debt = ClosureDebt::new(WideResourceVector::new(2, 2)).ok_or("debt must be nonzero")?;
    let delivered = delivery
        .delivered(debt, Event::marker_delivered(PARTICIPANT, prior, MARKER))
        .map_err(|error| format!("marker delivery: {error:?}"))?;
    let ClosureState::Owed {
        edge: StoredEdge::ParticipantCursorProgress(progress),
        ..
    } = delivered
    else {
        return Err("marker delivery did not produce progress".into());
    };
    let prior_floor = MARKER
        .checked_sub(1)
        .ok_or("prior marker floor underflow")?;
    let successor = progress
        .binding_fate(
            debt,
            Event::binding_fate_observed(PARTICIPANT, prior, prior_floor),
        )
        .map_err(|error| format!("binding fate: {error:?}"))?;
    let CursorFateSuccessor::DetachedCredentialRecovery(recovery) = successor else {
        return Err("binding fate did not produce recovery".into());
    };
    Ok(recovery)
}

pub(super) fn source_fixture() -> Result<SourceFixture, Box<dyn Error>> {
    let recovery = recovery()?;
    let (owner, _) = drained_frontier(FrontierBinding::Detached(epoch()?))?;
    let retained = owner
        .retain_fenced_marker_source(recovery)
        .map_err(|_| "frontier refused exact recovery")?;
    let expectation = retained.expectation();
    let marker = canonical_marker_bytes(expectation);
    let marker_bytes = u64::try_from(marker.len())?;
    let order = expectation.admission_order();
    let row = StoredMarkerDrain {
        marker,
        retained_charge: StoredRetainedCharge {
            delivery_seq: expectation.marker_delivery_seq(),
            transaction_order: order.transaction_order(),
            candidate_phase: order.candidate_phase() as u8,
            participant_id: expectation.participant_id(),
            charge: StoredResourceVector {
                entries: 1,
                bytes: marker_bytes,
            },
        },
        resulting_retained_charges: vec![],
        successor: vec![],
    };
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    Ok(SourceFixture {
        store,
        retained,
        row,
    })
}

pub(super) fn assert_exact_source_association() -> Result<(), Box<dyn Error>> {
    let fixture = source_fixture()?;
    let log = OperationLog::new(Arc::clone(&fixture.store), CONVERSATION);
    block_on(log.append(&StoredOperation::MarkerDrained { row: fixture.row }, 0))??;
    let validated = block_on(validate_marker_source(fixture.store, fixture.retained, 0))?
        .map_err(|refused| format!("exact source refused: {:?}", refused.reason()))?;
    let (owner, returned_recovery, source_sequence) = validated.into_parts();
    assert_eq!(returned_recovery, recovery()?);
    assert_eq!(source_sequence, 0);

    let prior = epoch()?;
    let new_generation = Generation::new(3).ok_or("new generation must be nonzero")?;
    let new_epoch = BindingEpoch::new(ConnectionIncarnation::new(1, 2), new_generation);
    let debt = ClosureDebt::new(WideResourceVector::new(2, 2)).ok_or("debt must be nonzero")?;
    let minted = owner.mint_fenced_attach(
        source_sequence,
        returned_recovery,
        debt,
        Event::fenced_recovery_committed(
            PARTICIPANT,
            MARKER,
            prior,
            new_epoch,
            MARKER.checked_add(1).ok_or("fenced floor overflow")?,
        ),
        DebtCompletion::clear(),
    );
    assert!(matches!(minted, MintFencedAttachResult::Minted(_)));
    Ok(())
}

fn assert_refusal(
    mutate: impl FnOnce(&mut StoredMarkerDrain),
    expected: MarkerSourceRefusalReason,
) -> Result<(), Box<dyn Error>> {
    let mut fixture = source_fixture()?;
    mutate(&mut fixture.row);
    let log = OperationLog::new(Arc::clone(&fixture.store), CONVERSATION);
    block_on(log.append(&StoredOperation::MarkerDrained { row: fixture.row }, 0))??;
    let Err(refused) = block_on(validate_marker_source(fixture.store, fixture.retained, 0))? else {
        return Err("mismatched durable source was accepted".into());
    };
    assert_eq!(refused.reason(), expected);
    Ok(())
}

#[test]
fn marker_source_validation_refuses_typed_before_authority_construction()
-> Result<(), Box<dyn Error>> {
    assert_refusal(
        |row| row.marker.push(0),
        MarkerSourceRefusalReason::MarkerBody,
    )?;
    assert_refusal(
        |row| row.retained_charge.delivery_seq = 99,
        MarkerSourceRefusalReason::DeliverySequence,
    )?;
    assert_refusal(
        |row| row.retained_charge.transaction_order = 99,
        MarkerSourceRefusalReason::TransactionOrder,
    )?;
    assert_refusal(
        |row| row.retained_charge.candidate_phase = 99,
        MarkerSourceRefusalReason::CandidatePhase,
    )?;
    assert_refusal(
        |row| row.retained_charge.participant_id = 99,
        MarkerSourceRefusalReason::Participant,
    )?;
    assert_refusal(
        |row| row.retained_charge.charge.bytes = 99,
        MarkerSourceRefusalReason::RetainedCharge,
    )?;

    let missing = source_fixture()?;
    let Err(refused) = block_on(validate_marker_source(missing.store, missing.retained, 0))? else {
        return Err("missing marker source was accepted".into());
    };
    assert_eq!(refused.reason(), MarkerSourceRefusalReason::Missing);

    let wrong = source_fixture()?;
    let log = OperationLog::new(Arc::clone(&wrong.store), CONVERSATION);
    block_on(log.append(&StoredOperation::Genesis { event: vec![] }, 0))??;
    let Err(refused) = block_on(validate_marker_source(wrong.store, wrong.retained, 0))? else {
        return Err("wrong marker source kind was accepted".into());
    };
    assert_eq!(refused.reason(), MarkerSourceRefusalReason::WrongOperation);
    Ok(())
}
