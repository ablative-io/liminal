use crate::lifecycle::TerminalProductSource;

use super::*;

fn prior_epoch() -> BindingEpoch {
    BindingEpoch::new(ConnectionIncarnation::new(90, 9), Generation::ONE)
}

fn retained_cause(provenance: MarkerProvenance) -> RetainedCausalRecord {
    let (phase, participant, kind) = match provenance {
        MarkerProvenance::NonProductM => (
            CandidatePhase::OrdinaryRecord,
            PARTICIPANT_ID,
            RetainedCausalRecordKind::OrdinaryRecord {
                participant_index: PARTICIPANT_ID,
            },
        ),
        MarkerProvenance::TerminalProduct { terminal, .. } => {
            let owner = match terminal {
                TerminalProductSource::Binding(owner) => owner,
                TerminalProductSource::RecoveryReplacement {
                    participant_index,
                    binding_epoch,
                } => BindingTerminalOwner {
                    participant_index,
                    binding_epoch,
                },
            };
            (
                CandidatePhase::BindingTerminal,
                owner.participant_index,
                RetainedCausalRecordKind::BindingTerminal(owner),
            )
        }
        MarkerProvenance::ExitProduct {
            exit_participant, ..
        } => (
            CandidatePhase::MembershipExit,
            exit_participant,
            RetainedCausalRecordKind::MembershipExit {
                participant_index: exit_participant,
            },
        ),
    };
    RetainedCausalRecord {
        delivery_seq: 1,
        admission_order: AdmissionOrder::new(0, phase, participant),
        kind,
    }
}

fn frontiers_for_shape(
    target_binding: FrontierBinding,
    provenance: MarkerProvenance,
) -> ClaimFrontiers {
    let terminal = BindingTerminalOwner {
        participant_index: PARTICIPANT_ID,
        binding_epoch: epoch(),
    };
    let (terminal_count, active_terminal_count, claim_parts) = match target_binding {
        FrontierBinding::Bound(_) => (1, 1, bound_claims(terminal, 1)),
        FrontierBinding::Detached(_) => (0, 0, detached_claims(1)),
    };
    let (sequence_movable, products, order_movable) = claim_parts;
    let sequence_ledger = SequenceLedger::try_new(
        1,
        SequenceClaims::new(1, terminal_count, 1, RecoverySequenceReserve::None),
    )
    .expect("marker shape sequence reserve fits after H=1");
    let order_ledger = OrderLedger::try_new(
        OrderHigh::Allocated(0),
        OrderClaims::new(active_terminal_count, 1, false, false)
            .expect("marker shape has no recovery half-pair"),
    )
    .expect("marker shape order reserve fits after major zero");
    let candidate = ImmutableSequenceCandidate::Marker(MarkerCandidateAuthority {
        delivery_seq: 2,
        admission_order: marker_key(),
        target_binding,
        provenance,
        abandoned_after: 0,
        abandoned_through: 1,
        physical_floor_at_decision: 1,
        current_owner: MarkerSequenceOwner::Marker,
    });

    ClaimFrontiers::restore(
        ClaimFrontiersRestore {
            conversation_id: CONVERSATION_ID,
            active_identities: vec![FrontierParticipant::new(PARTICIPANT_ID, 0, target_binding)],
            identity_slot_limit: 2,
            retained_floor: 1,
            retained_record_limit: 1,
            retained_records: vec![retained_cause(provenance)],
            active_marker_anchors: vec![],
            historical_marker_deliveries: vec![],
            historical_causal_facts: vec![],
            sequence: SequenceClaimFrontierRestore {
                movable_claims: sequence_movable,
                immutable_candidates: vec![candidate],
                products,
                recovery: None,
            },
            order: OrderClaimFrontierRestore {
                movable_claims: order_movable,
                immutable_candidates: vec![ImmutableOrderCandidateMajorRestore {
                    transaction_order: 0,
                    candidate_keys: vec![marker_key()],
                }],
                recovery: None,
            },
            recovery_marker_delivery_seq: None,
        },
        sequence_ledger,
        order_ledger,
    )
    .expect("complete marker provenance/target fixture restores")
}

#[test]
fn marker_commit_projects_typed_history_compacted_without_debug_parse() {
    let old_terminal = BindingTerminalOwner {
        participant_index: PARTICIPANT_ID,
        binding_epoch: prior_epoch(),
    };
    let provenances = [
        MarkerProvenance::NonProductM,
        MarkerProvenance::terminal_product(
            TerminalProductSource::Binding(old_terminal),
            PARTICIPANT_ID,
        ),
        MarkerProvenance::terminal_product(
            TerminalProductSource::recovery_replacement(PARTICIPANT_ID, prior_epoch()),
            PARTICIPANT_ID,
        ),
        MarkerProvenance::exit_product(1, PARTICIPANT_ID),
    ];
    let targets = [
        FrontierBinding::Bound(epoch()),
        FrontierBinding::Detached(epoch()),
    ];

    for provenance in provenances {
        for target in targets {
            let frontiers = frontiers_for_shape(target, provenance);
            let ImmutableSequenceCandidate::Marker(selected) =
                frontiers.sequence().immutable_candidates()[0]
            else {
                unreachable!("fixture installs a marker candidate")
            };
            let commit = drain_next_marker(frontiers, ClosureState::Clear)
                .expect("every validated provenance/target marker drains");
            let retained = commit
                .frontiers()
                .retained_marker_records()
                .iter()
                .find(|record| record.delivery_seq == selected.delivery_seq)
                .copied()
                .expect("drain retains the selected marker record");
            assert_eq!(retained.admission_order, selected.admission_order);
            assert_eq!(
                retained.kind,
                RetainedCausalRecordKind::CompactionMarker {
                    participant_index: PARTICIPANT_ID,
                    provenance,
                }
            );

            let (_, _, _, successor, projection) = commit.into_parts();
            assert_eq!(
                matches!(target, FrontierBinding::Bound(_)),
                matches!(successor, StoredEdge::MarkerDelivery(_))
            );
            assert_eq!(
                projection.into_delivery(),
                ParticipantDelivery {
                    conversation_id: CONVERSATION_ID,
                    delivery_seq: retained.delivery_seq,
                    record: ParticipantRecord::HistoryCompacted {
                        affected_participant_id: PARTICIPANT_ID,
                        abandoned_after: selected.abandoned_after,
                        abandoned_through: selected.abandoned_through,
                        physical_floor_at_decision: selected.physical_floor_at_decision,
                    },
                }
            );
        }
    }
}
