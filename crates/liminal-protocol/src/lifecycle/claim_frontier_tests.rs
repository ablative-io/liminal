#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use alloc::{vec, vec::Vec};

use crate::{
    algebra::WideResourceVector,
    outcome::{CandidatePhase, ClaimCounter, ParticipantStateCorruptReason},
    wire::{BindingEpoch, CloseCause, ConnectionIncarnation, Generation},
};

use super::{
    ActiveBinding, AdmissionOrder, ClosureDebt, ClosureState, CursorFateSuccessor, Event,
    OrderClaims, OrderHigh, OrderLedger, PhysicalCompaction, RecoverySequenceReserve,
    SequenceClaims, SequenceLedger, StoredEdge,
    claim_frontier::{
        BindingTerminalOwner, ClaimFrontierCounter, ClaimFrontierInvalidReason, ClaimFrontiers,
        ClaimFrontiersRestore, ExitProductRangeRestore, FrontierBinding, FrontierParticipant,
        HistoricalCausalFactRestore, HistoricalMarkerDeliveryFactRestore,
        ImmutableOrderCandidateMajorRestore, ImmutableSequenceCandidate, MarkerCandidateAuthority,
        MarkerProvenance, MarkerRecordRequest, MarkerSequenceOwner, MovableOrderClaim,
        MovableSequenceClaim, OrderClaimFrontierRestore, OrderDirectOwner,
        RecoveryOrderActiveBindingRestore, RecoveryOrderBlockRestore, RecoverySequenceBlockRestore,
        RecoverySequenceTerminalRestore, ReplacementTerminalProductRangeRestore,
        RetainedCausalRecord, RetainedCausalRecordKind, SequenceClaimFrontierRestore,
        SequenceDirectOwner, SequenceProductClass, SequenceProductRangesRestore,
        TerminalProductRangeRestore, TerminalProductSource, validate_numeric_union_for_test,
    },
    edge::marker_delivery_for_test,
    storage::CommittedBindingTerminalRestore,
};

const CONVERSATION_ID: u64 = 41;
const PARTICIPANT_ZERO: u64 = 0;

fn epoch(generation: u64, ordinal: u64) -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(7, ordinal),
        Generation::new(generation).expect("test generation is nonzero"),
    )
}

fn sequence_ledger(high: u64, claims: SequenceClaims) -> SequenceLedger {
    SequenceLedger::try_new(high, claims).expect("test sequence ledger fits")
}

fn order_ledger(high: OrderHigh, claims: OrderClaims) -> OrderLedger {
    OrderLedger::try_new(high, claims).expect("test order ledger fits")
}

fn order_claims(active: u64, exits: u64, recovery: bool) -> OrderClaims {
    OrderClaims::new(active, exits, recovery, recovery)
        .expect("test recovery order claims are paired")
}

fn empty_restore(
    retained_floor: u128,
    retained_records: Vec<RetainedCausalRecord>,
) -> ClaimFrontiersRestore {
    ClaimFrontiersRestore {
        conversation_id: CONVERSATION_ID,
        active_identities: vec![],
        identity_slot_limit: 1,
        retained_floor,
        retained_record_limit: u64::try_from(retained_records.len())
            .expect("test retained corpus fits u64"),
        retained_records,
        active_marker_anchors: vec![],
        historical_marker_deliveries: vec![],
        historical_causal_facts: vec![],
        sequence: SequenceClaimFrontierRestore::default(),
        order: OrderClaimFrontierRestore::default(),
        recovery_marker_delivery_seq: None,
    }
}

fn two_released_marker_records() -> Vec<RetainedCausalRecord> {
    vec![
        RetainedCausalRecord {
            delivery_seq: 0,
            admission_order: AdmissionOrder::new(0, CandidatePhase::CompactionMarker, 0),
            kind: RetainedCausalRecordKind::CompactionMarker {
                participant_index: 0,
                provenance: MarkerProvenance::NonProductM,
            },
        },
        RetainedCausalRecord {
            delivery_seq: 1,
            admission_order: AdmissionOrder::new(1, CandidatePhase::OrdinaryRecord, 0),
            kind: RetainedCausalRecordKind::OrdinaryRecord {
                participant_index: 0,
            },
        },
        RetainedCausalRecord {
            delivery_seq: 2,
            admission_order: AdmissionOrder::new(2, CandidatePhase::CompactionMarker, 0),
            kind: RetainedCausalRecordKind::CompactionMarker {
                participant_index: 0,
                provenance: MarkerProvenance::NonProductM,
            },
        },
    ]
}

fn single_bound_marker_fixture(
    marker_major: u64,
    marker_above_high: bool,
) -> (ClaimFrontiersRestore, SequenceLedger, OrderLedger) {
    let binding_epoch = epoch(1, 1);
    let terminal = BindingTerminalOwner {
        participant_index: PARTICIPANT_ZERO,
        binding_epoch,
    };
    let marker_order = AdmissionOrder::new(
        marker_major,
        CandidatePhase::CompactionMarker,
        PARTICIPANT_ZERO,
    );
    let first_direct_major = if marker_above_high {
        marker_major + 1
    } else {
        1
    };
    let restore = ClaimFrontiersRestore {
        conversation_id: CONVERSATION_ID,
        active_identities: vec![FrontierParticipant::new(
            PARTICIPANT_ZERO,
            0,
            FrontierBinding::Bound(binding_epoch),
        )],
        identity_slot_limit: 1,
        retained_floor: 1,
        retained_record_limit: 0,
        retained_records: vec![],
        active_marker_anchors: vec![],
        historical_marker_deliveries: vec![],
        historical_causal_facts: vec![],
        sequence: SequenceClaimFrontierRestore {
            movable_claims: vec![
                MovableSequenceClaim {
                    delivery_seq: 2,
                    owner: SequenceDirectOwner::MembershipExit {
                        participant_index: PARTICIPANT_ZERO,
                    },
                },
                MovableSequenceClaim {
                    delivery_seq: 3,
                    owner: SequenceDirectOwner::BindingTerminal(terminal),
                },
            ],
            immutable_candidates: vec![ImmutableSequenceCandidate::Marker(
                MarkerCandidateAuthority {
                    delivery_seq: 1,
                    admission_order: marker_order,
                    target_binding: FrontierBinding::Bound(binding_epoch),
                    provenance: MarkerProvenance::NonProductM,
                    current_owner: MarkerSequenceOwner::Marker,
                },
            )],
            products: SequenceProductRangesRestore {
                live_times_terminal: vec![TerminalProductRangeRestore {
                    start: 4,
                    length: 1,
                    terminal,
                }],
                live_times_replacement_terminal: None,
                other_live_times_exit: vec![],
            },
            recovery: None,
        },
        order: OrderClaimFrontierRestore {
            movable_claims: vec![
                MovableOrderClaim {
                    transaction_order: first_direct_major,
                    owner: OrderDirectOwner::ActiveBindingTerminal(terminal),
                },
                MovableOrderClaim {
                    transaction_order: first_direct_major + 1,
                    owner: OrderDirectOwner::MembershipExit {
                        participant_index: PARTICIPANT_ZERO,
                    },
                },
            ],
            immutable_candidates: vec![ImmutableOrderCandidateMajorRestore {
                transaction_order: marker_major,
                candidate_keys: vec![marker_order],
            }],
            recovery: None,
        },
        recovery_marker_delivery_seq: None,
    };
    (
        restore,
        sequence_ledger(
            0,
            SequenceClaims::new(1, 1, 1, RecoverySequenceReserve::None),
        ),
        order_ledger(OrderHigh::Allocated(0), order_claims(1, 1, false)),
    )
}

fn two_participant_product_fixture() -> (ClaimFrontiersRestore, SequenceLedger, OrderLedger) {
    let first = BindingTerminalOwner {
        participant_index: 0,
        binding_epoch: epoch(1, 1),
    };
    let second = BindingTerminalOwner {
        participant_index: 1,
        binding_epoch: epoch(1, 2),
    };
    let restore = ClaimFrontiersRestore {
        conversation_id: CONVERSATION_ID,
        active_identities: vec![
            FrontierParticipant::new(0, 15, FrontierBinding::Bound(first.binding_epoch)),
            FrontierParticipant::new(1, 15, FrontierBinding::Bound(second.binding_epoch)),
        ],
        identity_slot_limit: 2,
        retained_floor: 16,
        retained_record_limit: 0,
        retained_records: vec![],
        active_marker_anchors: vec![],
        historical_marker_deliveries: vec![],
        historical_causal_facts: vec![],
        sequence: SequenceClaimFrontierRestore {
            movable_claims: vec![
                MovableSequenceClaim {
                    delivery_seq: 16,
                    owner: SequenceDirectOwner::MembershipExit {
                        participant_index: 0,
                    },
                },
                MovableSequenceClaim {
                    delivery_seq: 17,
                    owner: SequenceDirectOwner::MembershipExit {
                        participant_index: 1,
                    },
                },
                MovableSequenceClaim {
                    delivery_seq: 18,
                    owner: SequenceDirectOwner::BindingTerminal(first),
                },
                MovableSequenceClaim {
                    delivery_seq: 19,
                    owner: SequenceDirectOwner::BindingTerminal(second),
                },
            ],
            immutable_candidates: vec![],
            products: SequenceProductRangesRestore {
                live_times_terminal: vec![
                    TerminalProductRangeRestore {
                        start: 20,
                        length: 2,
                        terminal: first,
                    },
                    TerminalProductRangeRestore {
                        start: 22,
                        length: 2,
                        terminal: second,
                    },
                ],
                live_times_replacement_terminal: None,
                other_live_times_exit: vec![
                    ExitProductRangeRestore {
                        start: 24,
                        length: 1,
                        exit_participant: 0,
                    },
                    ExitProductRangeRestore {
                        start: 25,
                        length: 1,
                        exit_participant: 1,
                    },
                ],
            },
            recovery: None,
        },
        order: OrderClaimFrontierRestore {
            movable_claims: vec![
                MovableOrderClaim {
                    transaction_order: 1,
                    owner: OrderDirectOwner::ActiveBindingTerminal(first),
                },
                MovableOrderClaim {
                    transaction_order: 2,
                    owner: OrderDirectOwner::ActiveBindingTerminal(second),
                },
                MovableOrderClaim {
                    transaction_order: 3,
                    owner: OrderDirectOwner::MembershipExit {
                        participant_index: 0,
                    },
                },
                MovableOrderClaim {
                    transaction_order: 4,
                    owner: OrderDirectOwner::MembershipExit {
                        participant_index: 1,
                    },
                },
            ],
            immutable_candidates: vec![],
            recovery: None,
        },
        recovery_marker_delivery_seq: None,
    };
    (
        restore,
        sequence_ledger(
            15,
            SequenceClaims::new(2, 2, 0, RecoverySequenceReserve::None),
        ),
        order_ledger(OrderHigh::Allocated(0), order_claims(2, 2, false)),
    )
}

fn recovery_candidate_fixture(
    target_binding: FrontierBinding,
) -> (ClaimFrontiersRestore, SequenceLedger, OrderLedger) {
    let binding_epoch = match target_binding {
        FrontierBinding::Bound(epoch) | FrontierBinding::Detached(epoch) => epoch,
    };
    let terminal = BindingTerminalOwner {
        participant_index: PARTICIPANT_ZERO,
        binding_epoch,
    };
    let marker_order = AdmissionOrder::new(0, CandidatePhase::CompactionMarker, PARTICIPANT_ZERO);
    let restore = ClaimFrontiersRestore {
        conversation_id: CONVERSATION_ID,
        active_identities: vec![FrontierParticipant::new(
            PARTICIPANT_ZERO,
            0,
            target_binding,
        )],
        identity_slot_limit: 1,
        retained_floor: 1,
        retained_record_limit: 0,
        retained_records: vec![],
        active_marker_anchors: vec![],
        historical_marker_deliveries: vec![],
        historical_causal_facts: vec![],
        sequence: SequenceClaimFrontierRestore {
            movable_claims: vec![MovableSequenceClaim {
                delivery_seq: 5,
                owner: SequenceDirectOwner::MembershipExit {
                    participant_index: PARTICIPANT_ZERO,
                },
            }],
            immutable_candidates: vec![ImmutableSequenceCandidate::Marker(
                MarkerCandidateAuthority {
                    delivery_seq: 1,
                    admission_order: marker_order,
                    target_binding,
                    provenance: MarkerProvenance::NonProductM,
                    current_owner: MarkerSequenceOwner::Marker,
                },
            )],
            products: SequenceProductRangesRestore {
                live_times_terminal: vec![TerminalProductRangeRestore {
                    start: 6,
                    length: 1,
                    terminal,
                }],
                live_times_replacement_terminal: Some(ReplacementTerminalProductRangeRestore {
                    start: 7,
                    length: 1,
                }),
                other_live_times_exit: vec![],
            },
            recovery: Some(RecoverySequenceBlockRestore {
                terminal: Some(RecoverySequenceTerminalRestore {
                    delivery_seq: 2,
                    owner: terminal,
                }),
                recovery_attach_seq: 3,
                replacement_terminal_seq: 4,
            }),
        },
        order: OrderClaimFrontierRestore {
            movable_claims: vec![MovableOrderClaim {
                transaction_order: 4,
                owner: OrderDirectOwner::MembershipExit {
                    participant_index: PARTICIPANT_ZERO,
                },
            }],
            immutable_candidates: vec![ImmutableOrderCandidateMajorRestore {
                transaction_order: 0,
                candidate_keys: vec![marker_order],
            }],
            recovery: Some(RecoveryOrderBlockRestore {
                active_binding: Some(RecoveryOrderActiveBindingRestore {
                    transaction_order: 1,
                    owner: terminal,
                }),
                recovery_operation_order: 2,
                replacement_terminal_order: 3,
            }),
        },
        recovery_marker_delivery_seq: Some(1),
    };
    (
        restore,
        sequence_ledger(
            0,
            SequenceClaims::new(1, 1, 1, RecoverySequenceReserve::DetachedCredentialRecovery),
        ),
        order_ledger(OrderHigh::Allocated(0), order_claims(1, 1, true)),
    )
}

fn assert_frontier_error(
    result: Result<ClaimFrontiers, ParticipantStateCorruptReason>,
    counter: ClaimCounter,
    first_bad_position: u128,
) {
    assert_eq!(
        result.expect_err("fixture must fail frontier validation"),
        ParticipantStateCorruptReason::ClaimFrontierInvalid {
            counter,
            first_bad_position,
        }
    );
}

#[test]
fn endpoint_sweep_reports_first_expanded_collision_inside_compact_ranges() {
    let error = validate_numeric_union_for_test(16, 7, &[(16, 5), (18, 1), (21, 1)])
        .expect_err("point inside a compact range duplicates sequence 18");
    assert_eq!(error.counter, ClaimFrontierCounter::DeliverySequence);
    assert_eq!(error.first_bad_position, 3);
    assert_eq!(error.reason, ClaimFrontierInvalidReason::NumericPosition);

    let same_start = validate_numeric_union_for_test(16, 4, &[(16, 2), (16, 2)])
        .expect_err("same-start ranges collide at their second expanded value");
    assert_eq!(same_start.first_bad_position, 1);
}

#[test]
fn candidate_cells_accept_exactly_two_i_and_reject_the_third() {
    let binding_epoch = epoch(1, 1);
    let terminal = BindingTerminalOwner {
        participant_index: 0,
        binding_epoch,
    };
    let terminal_order = AdmissionOrder::new(0, CandidatePhase::BindingTerminal, 0);
    let marker_order = AdmissionOrder::new(0, CandidatePhase::CompactionMarker, 0);
    let mut restore = ClaimFrontiersRestore {
        conversation_id: CONVERSATION_ID,
        active_identities: vec![FrontierParticipant::new(
            0,
            0,
            FrontierBinding::Detached(binding_epoch),
        )],
        identity_slot_limit: 1,
        retained_floor: 1,
        retained_record_limit: 0,
        retained_records: vec![],
        active_marker_anchors: vec![],
        historical_marker_deliveries: vec![],
        historical_causal_facts: vec![],
        sequence: SequenceClaimFrontierRestore {
            movable_claims: vec![MovableSequenceClaim {
                delivery_seq: 3,
                owner: SequenceDirectOwner::MembershipExit {
                    participant_index: 0,
                },
            }],
            immutable_candidates: vec![
                ImmutableSequenceCandidate::BindingTerminal {
                    delivery_seq: 1,
                    admission_order: terminal_order,
                    owner: terminal,
                },
                ImmutableSequenceCandidate::Marker(MarkerCandidateAuthority {
                    delivery_seq: 2,
                    admission_order: marker_order,
                    target_binding: FrontierBinding::Detached(binding_epoch),
                    provenance: MarkerProvenance::NonProductM,
                    current_owner: MarkerSequenceOwner::Marker,
                }),
            ],
            products: SequenceProductRangesRestore {
                live_times_terminal: vec![TerminalProductRangeRestore {
                    start: 4,
                    length: 1,
                    terminal,
                }],
                live_times_replacement_terminal: None,
                other_live_times_exit: vec![],
            },
            recovery: None,
        },
        order: OrderClaimFrontierRestore {
            movable_claims: vec![MovableOrderClaim {
                transaction_order: 1,
                owner: OrderDirectOwner::MembershipExit {
                    participant_index: 0,
                },
            }],
            immutable_candidates: vec![ImmutableOrderCandidateMajorRestore {
                transaction_order: 0,
                candidate_keys: vec![terminal_order, marker_order],
            }],
            recovery: None,
        },
        recovery_marker_delivery_seq: None,
    };
    let sequence = sequence_ledger(
        0,
        SequenceClaims::new(1, 1, 1, RecoverySequenceReserve::None),
    );
    let order = order_ledger(OrderHigh::Allocated(0), order_claims(0, 1, false));
    ClaimFrontiers::restore(restore.clone(), sequence, order)
        .expect("two candidate cells equal the signed 2I cap");

    restore
        .sequence
        .immutable_candidates
        .push(ImmutableSequenceCandidate::Marker(
            MarkerCandidateAuthority {
                delivery_seq: 5,
                admission_order: AdmissionOrder::new(1, CandidatePhase::CompactionMarker, 0),
                target_binding: FrontierBinding::Detached(binding_epoch),
                provenance: MarkerProvenance::NonProductM,
                current_owner: MarkerSequenceOwner::Marker,
            },
        ));
    assert!(
        ClaimFrontiers::restore(restore, sequence, order).is_err(),
        "a third candidate exceeds 2I before its other malformed fields matter"
    );
}

#[test]
fn complete_retained_corpus_accepts_end_and_stores_only_marker_anchors() {
    let end = u128::from(u64::MAX) + 1;
    let at_end = empty_restore(end, vec![]);
    let restored = ClaimFrontiers::restore(
        at_end,
        sequence_ledger(u64::MAX, SequenceClaims::default()),
        order_ledger(OrderHigh::Empty, order_claims(0, 0, false)),
    )
    .expect("F=MAX+1 represents the empty suffix after H=MAX");
    assert_eq!(restored.retained_floor(), end);

    let records = vec![
        RetainedCausalRecord {
            delivery_seq: 0,
            admission_order: AdmissionOrder::new(0, CandidatePhase::OrdinaryRecord, 0),
            kind: RetainedCausalRecordKind::OrdinaryRecord {
                participant_index: 0,
            },
        },
        RetainedCausalRecord {
            delivery_seq: 1,
            admission_order: AdmissionOrder::new(1, CandidatePhase::OrdinaryRecord, 0),
            kind: RetainedCausalRecordKind::OrdinaryRecord {
                participant_index: 0,
            },
        },
        RetainedCausalRecord {
            delivery_seq: 2,
            admission_order: AdmissionOrder::new(2, CandidatePhase::CompactionMarker, 0),
            kind: RetainedCausalRecordKind::CompactionMarker {
                participant_index: 0,
                provenance: MarkerProvenance::NonProductM,
            },
        },
        RetainedCausalRecord {
            delivery_seq: 3,
            admission_order: AdmissionOrder::new(3, CandidatePhase::OrdinaryRecord, 0),
            kind: RetainedCausalRecordKind::OrdinaryRecord {
                participant_index: 0,
            },
        },
    ];
    let released_history = empty_restore(0, records);
    let restored = ClaimFrontiers::restore(
        released_history.clone(),
        sequence_ledger(3, SequenceClaims::default()),
        order_ledger(OrderHigh::Allocated(3), order_claims(0, 0, false)),
    )
    .expect("complete F..=H direct corpus restores");
    assert!(restored.retained_marker_records().is_empty());

    let mut active_anchor = released_history;
    active_anchor.active_marker_anchors = vec![2];
    let restored = ClaimFrontiers::restore(
        active_anchor,
        sequence_ledger(3, SequenceClaims::default()),
        order_ledger(OrderHigh::Allocated(3), order_claims(0, 0, false)),
    )
    .expect("the explicitly selected retained marker remains a current anchor");
    assert_eq!(restored.retained_marker_records().len(), 1);

    let missing = empty_restore(0, vec![]);
    assert_frontier_error(
        ClaimFrontiers::restore(
            missing,
            sequence_ledger(3, SequenceClaims::default()),
            order_ledger(OrderHigh::Allocated(3), order_claims(0, 0, false)),
        ),
        ClaimCounter::DeliverySeq,
        0,
    );
}

#[test]
fn released_marker_history_allows_multiple_old_records_for_one_participant() {
    let released_history = empty_restore(0, two_released_marker_records());
    let restored = ClaimFrontiers::restore(
        released_history.clone(),
        sequence_ledger(2, SequenceClaims::default()),
        order_ledger(OrderHigh::Allocated(2), order_claims(0, 0, false)),
    )
    .expect("two released retained markers are historical records, not duplicate anchors");
    assert!(restored.retained_marker_records().is_empty());

    let mut latest_active = released_history;
    latest_active.active_marker_anchors = vec![2];
    let restored = ClaimFrontiers::restore(
        latest_active,
        sequence_ledger(2, SequenceClaims::default()),
        order_ledger(OrderHigh::Allocated(2), order_claims(0, 0, false)),
    )
    .expect("one current anchor can coexist with an older released marker for its participant");
    assert_eq!(
        restored.retained_marker_records(),
        &[RetainedCausalRecord {
            delivery_seq: 2,
            admission_order: AdmissionOrder::new(2, CandidatePhase::CompactionMarker, 0),
            kind: RetainedCausalRecordKind::CompactionMarker {
                participant_index: 0,
                provenance: MarkerProvenance::NonProductM,
            },
        }]
    );
}

#[test]
fn duplicate_scan_selects_exact_lowest_tuple_across_record_and_candidate() {
    let (mut restore, sequence, order) = single_bound_marker_fixture(0, false);
    let duplicate = AdmissionOrder::new(0, CandidatePhase::CompactionMarker, 0);
    restore.retained_floor = 0;
    restore.retained_record_limit = 1;
    restore.retained_records = vec![RetainedCausalRecord {
        delivery_seq: 0,
        admission_order: duplicate,
        kind: RetainedCausalRecordKind::CompactionMarker {
            participant_index: 0,
            provenance: MarkerProvenance::NonProductM,
        },
    }];
    assert_eq!(
        ClaimFrontiers::restore(restore, sequence, order),
        Err(ParticipantStateCorruptReason::DuplicateCandidateKey {
            transaction_order: 0,
            candidate_phase: CandidatePhase::CompactionMarker,
            participant_index: 0,
        })
    );
}

#[test]
fn numeric_frontiers_precede_duplicate_candidate_diagnosis() {
    let (mut sequence_fault, sequence, order) = single_bound_marker_fixture(0, false);
    let duplicate = AdmissionOrder::new(0, CandidatePhase::CompactionMarker, 0);
    sequence_fault.retained_floor = 0;
    sequence_fault.retained_record_limit = 1;
    sequence_fault.retained_records = vec![RetainedCausalRecord {
        delivery_seq: 0,
        admission_order: duplicate,
        kind: RetainedCausalRecordKind::CompactionMarker {
            participant_index: 0,
            provenance: MarkerProvenance::NonProductM,
        },
    }];
    let ImmutableSequenceCandidate::Marker(marker) =
        &mut sequence_fault.sequence.immutable_candidates[0]
    else {
        panic!("fixture candidate is a marker")
    };
    marker.delivery_seq = 2;
    assert_frontier_error(
        ClaimFrontiers::restore(sequence_fault, sequence, order),
        ClaimCounter::DeliverySeq,
        0,
    );

    let (mut order_fault, sequence, order) = single_bound_marker_fixture(0, false);
    order_fault.retained_floor = 0;
    order_fault.retained_record_limit = 1;
    order_fault.retained_records = vec![RetainedCausalRecord {
        delivery_seq: 0,
        admission_order: duplicate,
        kind: RetainedCausalRecordKind::CompactionMarker {
            participant_index: 0,
            provenance: MarkerProvenance::NonProductM,
        },
    }];
    order_fault.order.movable_claims[0].transaction_order = 2;
    assert_frontier_error(
        ClaimFrontiers::restore(order_fault, sequence, order),
        ClaimCounter::TransactionOrder,
        0,
    );
}

#[test]
fn candidate_and_retained_sequence_order_must_follow_admission_order() {
    let binding_epoch = epoch(1, 1);
    let terminal = BindingTerminalOwner {
        participant_index: 0,
        binding_epoch,
    };
    let (mut restore, sequence, _order) = single_bound_marker_fixture(0, false);
    restore.active_identities[0] =
        FrontierParticipant::new(0, 0, FrontierBinding::Detached(binding_epoch));
    restore.sequence.immutable_candidates = vec![
        ImmutableSequenceCandidate::Marker(MarkerCandidateAuthority {
            delivery_seq: 1,
            admission_order: AdmissionOrder::new(0, CandidatePhase::CompactionMarker, 0),
            target_binding: FrontierBinding::Detached(binding_epoch),
            provenance: MarkerProvenance::NonProductM,
            current_owner: MarkerSequenceOwner::Marker,
        }),
        ImmutableSequenceCandidate::BindingTerminal {
            delivery_seq: 2,
            admission_order: AdmissionOrder::new(0, CandidatePhase::BindingTerminal, 0),
            owner: terminal,
        },
    ];
    restore.sequence.movable_claims = vec![MovableSequenceClaim {
        delivery_seq: 3,
        owner: SequenceDirectOwner::MembershipExit {
            participant_index: 0,
        },
    }];
    restore.order.movable_claims = vec![MovableOrderClaim {
        transaction_order: 1,
        owner: OrderDirectOwner::MembershipExit {
            participant_index: 0,
        },
    }];
    restore.order.immutable_candidates[0].candidate_keys = vec![
        AdmissionOrder::new(0, CandidatePhase::BindingTerminal, 0),
        AdmissionOrder::new(0, CandidatePhase::CompactionMarker, 0),
    ];
    assert!(
        ClaimFrontiers::restore(
            restore,
            sequence,
            order_ledger(OrderHigh::Allocated(0), order_claims(0, 1, false)),
        )
        .is_err(),
        "sequence assignment cannot reverse phase-0 and phase-4 lane order"
    );

    let records = vec![
        RetainedCausalRecord {
            delivery_seq: 0,
            admission_order: AdmissionOrder::new(1, CandidatePhase::OrdinaryRecord, 0),
            kind: RetainedCausalRecordKind::OrdinaryRecord {
                participant_index: 0,
            },
        },
        RetainedCausalRecord {
            delivery_seq: 1,
            admission_order: AdmissionOrder::new(0, CandidatePhase::OrdinaryRecord, 0),
            kind: RetainedCausalRecordKind::OrdinaryRecord {
                participant_index: 0,
            },
        },
    ];
    assert!(
        ClaimFrontiers::restore(
            empty_restore(0, records),
            sequence_ledger(1, SequenceClaims::default()),
            order_ledger(OrderHigh::Allocated(1), order_claims(0, 0, false)),
        )
        .is_err(),
        "complete retained sequence must preserve canonical tuple order"
    );
}

#[test]
fn compact_product_accessors_enforce_the_validated_rank_extent() {
    let (restore, sequence, order) = two_participant_product_fixture();
    let frontiers = ClaimFrontiers::restore(restore, sequence, order)
        .expect("two-participant compact products restore");
    let row = frontiers.sequence().products().live_times_terminal()[0];
    assert_eq!(row.length(), 2);
    assert_eq!(row.value_at_rank(0), Some(20));
    assert_eq!(row.value_at_rank(1), Some(21));
    assert_eq!(row.value_at_rank(2), None);

    let exit = frontiers.sequence().products().other_live_times_exit()[0];
    assert_eq!(exit.length(), 1);
    assert_eq!(
        exit.value_for_affected_rank(frontiers.active_identities(), 1),
        Some(24)
    );
    assert_eq!(
        exit.value_for_affected_rank(frontiers.active_identities(), 0),
        None
    );
}

#[test]
fn immutable_marker_requires_m_owner_and_typed_compacted_cause() {
    let (mut wrong_owner, sequence, order) = single_bound_marker_fixture(0, false);
    let ImmutableSequenceCandidate::Marker(marker) =
        &mut wrong_owner.sequence.immutable_candidates[0]
    else {
        panic!("fixture candidate is a marker")
    };
    marker.current_owner =
        MarkerSequenceOwner::ConditionalProduct(SequenceProductClass::LiveTimesTerminal);
    assert!(ClaimFrontiers::restore(wrong_owner, sequence, order).is_err());

    let old_epoch = epoch(1, 1);
    let current_epoch = epoch(2, 2);
    let old_terminal = BindingTerminalOwner {
        participant_index: 0,
        binding_epoch: old_epoch,
    };
    let (mut causal, sequence, order) = single_bound_marker_fixture(0, false);
    causal.active_identities[0] =
        FrontierParticipant::new(0, 0, FrontierBinding::Bound(current_epoch));
    let ImmutableSequenceCandidate::Marker(marker) = &mut causal.sequence.immutable_candidates[0]
    else {
        panic!("fixture candidate is a marker")
    };
    marker.target_binding = FrontierBinding::Bound(current_epoch);
    marker.provenance = MarkerProvenance::TerminalProduct {
        terminal: TerminalProductSource::Binding(old_terminal),
        affected_participant: 0,
    };
    let SequenceDirectOwner::BindingTerminal(current_terminal) =
        &mut causal.sequence.movable_claims[1].owner
    else {
        panic!("fixture has current terminal claim")
    };
    current_terminal.binding_epoch = current_epoch;
    causal.sequence.products.live_times_terminal[0]
        .terminal
        .binding_epoch = current_epoch;
    let OrderDirectOwner::ActiveBindingTerminal(current_order_terminal) =
        &mut causal.order.movable_claims[0].owner
    else {
        panic!("fixture has current order terminal claim")
    };
    current_order_terminal.binding_epoch = current_epoch;

    assert!(
        ClaimFrontiers::restore(causal.clone(), sequence, order).is_err(),
        "raw product provenance without retained typed cause is corrupt"
    );
    let committed = CommittedBindingTerminalRestore {
        binding: ActiveBinding {
            participant_id: 0,
            conversation_id: CONVERSATION_ID,
            binding_epoch: old_epoch,
        },
        cause: CloseCause::ConnectionLost,
        transaction_order: 0,
        delivery_seq: 0,
    }
    .restore()
    .expect("typed compacted terminal source restores");
    causal.historical_causal_facts = vec![HistoricalCausalFactRestore::BindingTerminal {
        conversation_id: committed.conversation_id(),
        participant_index: committed.participant_id(),
        binding_epoch: committed.binding_epoch(),
        admission_order: committed.admission_order(),
    }];
    assert!(
        ClaimFrontiers::restore(causal, sequence, order).is_err(),
        "copying exact typed terminal fields into a naked row cannot launder authority"
    );
}

#[test]
fn case_56_shape_derives_prefate_recovery_from_candidate_while_pc_is_current() {
    let binding_epoch = epoch(1, 1);
    let (restore, sequence, order) =
        recovery_candidate_fixture(FrontierBinding::Bound(binding_epoch));
    let prevalidated = ClaimFrontiers::prevalidate(restore, sequence, order)
        .expect("candidate recovery frontier prevalidates");
    let pc = PhysicalCompaction::new(0, 0).expect("single-record compaction range");
    let restored = prevalidated
        .finish(Some(StoredEdge::PhysicalCompaction(pc)))
        .expect("PC does not hide the pre-endowed candidate recovery quartet");
    let recovery = restored
        .sequence()
        .recovery()
        .expect("recovery sequence block remains exact");
    assert_eq!(recovery.participant_index(), 0);
    assert_eq!(recovery.marker_delivery_seq(), 1);
    assert_eq!(recovery.recovered_binding_epoch(), binding_epoch);
}

#[test]
fn recovery_selector_rejects_missing_and_detached_prefate_authority() {
    let binding_epoch = epoch(1, 1);
    let (mut missing, sequence, order) =
        recovery_candidate_fixture(FrontierBinding::Bound(binding_epoch));
    missing.recovery_marker_delivery_seq = None;
    assert!(ClaimFrontiers::restore(missing, sequence, order).is_err());

    let (detached, sequence, order) =
        recovery_candidate_fixture(FrontierBinding::Detached(binding_epoch));
    assert!(ClaimFrontiers::restore(detached, sequence, order).is_err());
}

#[test]
fn exact_detached_dcr_is_the_only_postfate_recovery_authority() {
    let binding_epoch = epoch(1, 1);
    let debt = ClosureDebt::new(WideResourceVector::new(1, 1)).expect("test debt is nonzero");
    let delivery =
        marker_delivery_for_test(0, binding_epoch, 1).expect("validated marker fixture restores");
    let progress = match delivery
        .delivered(debt, Event::marker_delivered(0, binding_epoch, 1))
        .expect("exact marker delivery commits")
    {
        ClosureState::Owed {
            edge: StoredEdge::ParticipantCursorProgress(progress),
            ..
        } => progress,
        state => panic!("unexpected marker delivery state: {state:?}"),
    };
    let dcr = match progress
        .binding_fate(debt, Event::binding_fate_observed(0, binding_epoch, 1))
        .expect("exact marker fate commits")
    {
        CursorFateSuccessor::DetachedCredentialRecovery(dcr) => dcr,
        successor @ CursorFateSuccessor::DetachedCursorRelease(_) => {
            panic!("unexpected marker fate successor: {successor:?}")
        }
    };

    let marker_order = AdmissionOrder::new(0, CandidatePhase::CompactionMarker, 0);
    let terminal_owner = BindingTerminalOwner {
        participant_index: 0,
        binding_epoch,
    };
    let restore = ClaimFrontiersRestore {
        conversation_id: CONVERSATION_ID,
        active_identities: vec![FrontierParticipant::new(
            0,
            0,
            FrontierBinding::Detached(binding_epoch),
        )],
        identity_slot_limit: 1,
        retained_floor: 1,
        retained_record_limit: 2,
        retained_records: vec![
            RetainedCausalRecord {
                delivery_seq: 1,
                admission_order: marker_order,
                kind: RetainedCausalRecordKind::CompactionMarker {
                    participant_index: 0,
                    provenance: MarkerProvenance::NonProductM,
                },
            },
            RetainedCausalRecord {
                delivery_seq: 2,
                admission_order: AdmissionOrder::new(1, CandidatePhase::BindingTerminal, 0),
                kind: RetainedCausalRecordKind::BindingTerminal(terminal_owner),
            },
        ],
        active_marker_anchors: vec![1],
        historical_marker_deliveries: vec![HistoricalMarkerDeliveryFactRestore {
            conversation_id: CONVERSATION_ID,
            participant_index: 0,
            marker_delivery_seq: 1,
            delivered_binding_epoch: binding_epoch,
        }],
        historical_causal_facts: vec![],
        sequence: SequenceClaimFrontierRestore {
            movable_claims: vec![MovableSequenceClaim {
                delivery_seq: 6,
                owner: SequenceDirectOwner::MembershipExit {
                    participant_index: 0,
                },
            }],
            immutable_candidates: vec![],
            products: SequenceProductRangesRestore {
                live_times_terminal: vec![],
                live_times_replacement_terminal: Some(ReplacementTerminalProductRangeRestore {
                    start: 5,
                    length: 1,
                }),
                other_live_times_exit: vec![],
            },
            recovery: Some(RecoverySequenceBlockRestore {
                terminal: None,
                recovery_attach_seq: 3,
                replacement_terminal_seq: 4,
            }),
        },
        order: OrderClaimFrontierRestore {
            movable_claims: vec![MovableOrderClaim {
                transaction_order: 4,
                owner: OrderDirectOwner::MembershipExit {
                    participant_index: 0,
                },
            }],
            immutable_candidates: vec![],
            recovery: Some(RecoveryOrderBlockRestore {
                active_binding: None,
                recovery_operation_order: 2,
                replacement_terminal_order: 3,
            }),
        },
        recovery_marker_delivery_seq: Some(1),
    };
    let sequence = sequence_ledger(
        2,
        SequenceClaims::new(1, 0, 0, RecoverySequenceReserve::DetachedCredentialRecovery),
    );
    let order = order_ledger(OrderHigh::Allocated(1), order_claims(0, 1, true));
    let request = MarkerRecordRequest::delivered(0, 1, FrontierBinding::Detached(binding_epoch));
    let mut prevalidated = ClaimFrontiers::prevalidate(restore.clone(), sequence, order)
        .expect("post-fate frontier prevalidates before edge restore");
    let _record = prevalidated
        .take_marker_record(request)
        .expect("exact retained DCR marker is selected once");
    prevalidated
        .finish(Some(StoredEdge::DetachedCredentialRecovery(dcr)))
        .expect("exact detached DCR proves the post-fate recovery pair");

    let wrong_epoch = epoch(2, 2);
    let mut wrong = restore;
    wrong.active_identities[0] =
        FrontierParticipant::new(0, 0, FrontierBinding::Detached(wrong_epoch));
    let mut prevalidated = ClaimFrontiers::prevalidate(wrong, sequence, order)
        .expect("wrong context remains numerically prevalidatable");
    assert!(prevalidated.take_marker_record(request).is_none());
    assert!(
        prevalidated
            .finish(Some(StoredEdge::DetachedCredentialRecovery(dcr)))
            .is_err(),
        "DCR epoch must match the exact Detached context"
    );
}

#[test]
fn allocated_high_excludes_old_candidate_major_but_future_candidate_owns_union() {
    let (same_major, sequence, order) = single_bound_marker_fixture(0, false);
    let restored = ClaimFrontiers::restore(same_major, sequence, order)
        .expect("same-major marker is ordered work, not a second numeric major");
    assert_eq!(restored.order().ledger().high(), OrderHigh::Allocated(0));
    assert_eq!(restored.order().movable_claims()[0].transaction_order, 1);

    let (future, sequence, order) = single_bound_marker_fixture(1, true);
    ClaimFrontiers::restore(future.clone(), sequence, order)
        .expect("candidate major above high participates in the exact numeric union");

    let mut collision = future;
    collision.order.movable_claims[0].transaction_order = 1;
    assert_frontier_error(
        ClaimFrontiers::restore(collision, sequence, order),
        ClaimCounter::TransactionOrder,
        1,
    );
}

#[test]
fn order_candidates_reject_duplicate_major_groups_and_below_high_keys() {
    let binding_epoch = epoch(1, 1);
    let terminal = BindingTerminalOwner {
        participant_index: 0,
        binding_epoch,
    };
    let terminal_order = AdmissionOrder::new(0, CandidatePhase::BindingTerminal, 0);
    let marker_order = AdmissionOrder::new(0, CandidatePhase::CompactionMarker, 0);
    let (mut duplicate_groups, sequence, _order) = single_bound_marker_fixture(0, false);
    duplicate_groups.active_identities[0] =
        FrontierParticipant::new(0, 0, FrontierBinding::Detached(binding_epoch));
    duplicate_groups.sequence.immutable_candidates = vec![
        ImmutableSequenceCandidate::BindingTerminal {
            delivery_seq: 1,
            admission_order: terminal_order,
            owner: terminal,
        },
        ImmutableSequenceCandidate::Marker(MarkerCandidateAuthority {
            delivery_seq: 2,
            admission_order: marker_order,
            target_binding: FrontierBinding::Detached(binding_epoch),
            provenance: MarkerProvenance::NonProductM,
            current_owner: MarkerSequenceOwner::Marker,
        }),
    ];
    duplicate_groups.sequence.movable_claims = vec![MovableSequenceClaim {
        delivery_seq: 3,
        owner: SequenceDirectOwner::MembershipExit {
            participant_index: 0,
        },
    }];
    duplicate_groups.order.movable_claims = vec![MovableOrderClaim {
        transaction_order: 1,
        owner: OrderDirectOwner::MembershipExit {
            participant_index: 0,
        },
    }];
    duplicate_groups.order.immutable_candidates = vec![
        ImmutableOrderCandidateMajorRestore {
            transaction_order: 0,
            candidate_keys: vec![terminal_order],
        },
        ImmutableOrderCandidateMajorRestore {
            transaction_order: 0,
            candidate_keys: vec![marker_order],
        },
    ];
    assert_frontier_error(
        ClaimFrontiers::restore(
            duplicate_groups,
            sequence,
            order_ledger(OrderHigh::Allocated(0), order_claims(0, 1, false)),
        ),
        ClaimCounter::TransactionOrder,
        0,
    );

    let (mut below_high, sequence, _order) = single_bound_marker_fixture(0, false);
    below_high.order.movable_claims[0].transaction_order = 2;
    below_high.order.movable_claims[1].transaction_order = 3;
    assert_frontier_error(
        ClaimFrontiers::restore(
            below_high,
            sequence,
            order_ledger(OrderHigh::Allocated(1), order_claims(1, 1, false)),
        ),
        ClaimCounter::TransactionOrder,
        0,
    );

    let (same_high, sequence, _order) = single_bound_marker_fixture(1, true);
    ClaimFrontiers::restore(
        same_high,
        sequence,
        order_ledger(OrderHigh::Allocated(1), order_claims(1, 1, false)),
    )
    .expect("candidate at allocated high remains legal");

    let (above_high, sequence, _order) = single_bound_marker_fixture(2, true);
    ClaimFrontiers::restore(
        above_high,
        sequence,
        order_ledger(OrderHigh::Allocated(1), order_claims(1, 1, false)),
    )
    .expect("candidate above high owns the first numeric frontier major");
}

#[test]
fn marker_drain_core_consumes_m_and_tuple_without_advancing_allocated_high() {
    let (restore, sequence, order) = single_bound_marker_fixture(0, false);
    let frontiers = ClaimFrontiers::restore(restore, sequence, order)
        .expect("same-major marker frontier restores");
    let drained = frontiers
        .drain_next_marker_core()
        .expect("exact next bound marker drains");
    let (frontiers, candidate, record) = drained.into_parts();

    assert_eq!(candidate.conversation_id(), CONVERSATION_ID);
    assert_eq!(candidate.delivery_seq(), 1);
    assert_eq!(record.conversation_id(), CONVERSATION_ID);
    assert_eq!(record.delivery_seq(), 1);
    assert_eq!(frontiers.sequence().ledger().high_watermark(), 1);
    assert_eq!(frontiers.sequence().ledger().claims().markers(), 0);
    assert!(frontiers.sequence().immutable_candidates().is_empty());
    assert!(frontiers.order().immutable_candidates().is_empty());
    assert_eq!(frontiers.order().ledger().high(), OrderHigh::Allocated(0));
    assert_eq!(frontiers.retained_marker_records().len(), 1);
}
