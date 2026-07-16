#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_arguments,
    clippy::too_many_lines
)]

use alloc::{boxed::Box, vec, vec::Vec};

use crate::{
    algebra::{ResourceDimension, ResourceVector, WideResourceVector},
    outcome::CandidatePhase,
    wire::{
        AttachSecret, BindingEpoch, ConnectionIncarnation, ConversationSequenceExhausted,
        Generation, ObserverBackpressureState, RecordAdmission, RecordAdmissionEnvelope,
        RecordAdmissionResponse, RecordCommitted, RecordTooLarge, SequenceAllocatingEnvelope,
        SequenceBudget, ServerValue,
    },
};

use super::super::{
    ActiveBinding, AdmissionOrder, BindingState, BindingTerminalOwner, CapacityCounter,
    ClaimFrontiers, ClaimFrontiersRestore, ClosureAccounting, ClosureState,
    ConnectionConversationTracking, EnrollmentFingerprint, FrontierBinding, FrontierParticipant,
    LiveMember, LiveMemberRestore, MovableOrderClaim, MovableSequenceClaim,
    OrderClaimFrontierRestore, OrderClaims, OrderDirectOwner, OrderHigh, OrderLedger,
    PresentedIdentity, RecoverySequenceReserve, RetainedCausalRecord, RetainedCausalRecordKind,
    SequenceClaimFrontierRestore, SequenceClaims, SequenceDirectOwner, SequenceLedger,
    SequenceProductRangesRestore, TerminalProductRangeRestore,
    operations::{OrdinaryProjectionLimits, RetainedRecordCharge},
};
use super::{
    ordinary_record_projection_tests::{
        UNIT, ordinary_capacity_walk_fixture, ordinary_capacity_walk_frontiers,
    },
    record_admission::{
        RecordAdmissionCommit, RecordAdmissionDecision, RecordAdmissionPersistenceParts,
        RecordAdmissionPrestate, apply_record_admission,
    },
};

type TestFingerprint = [u8; 32];
type TestMember = LiveMember<TestFingerprint>;
type TestPrestate<'a> =
    RecordAdmissionPrestate<'a, TestFingerprint, TestFingerprint, TestFingerprint>;
type TestDecision<'a> =
    RecordAdmissionDecision<'a, TestFingerprint, TestFingerprint, TestFingerprint>;

fn test_member(conversation_id: u64, generation: Generation, cursor: u64) -> TestMember {
    LiveMember::restore(LiveMemberRestore {
        participant_id: 0,
        conversation_id,
        generation,
        attach_secret: AttachSecret::new([0xA5; 32]),
        cursor,
        enrollment_fingerprint: EnrollmentFingerprint::new([0xE1; 32]),
        latest_terminal: None,
    })
    .expect("test member has consistent identity history")
}

fn request(envelope: &RecordAdmissionEnvelope, payload: &[u8]) -> RecordAdmission {
    RecordAdmission {
        conversation_id: envelope.conversation_id,
        participant_id: envelope.participant_id,
        capability_generation: envelope.capability_generation,
        payload: payload.to_vec(),
    }
}

fn prestate<'a>(
    request: RecordAdmission,
    member: &'a TestMember,
    binding: &'a BindingState,
    receiving_binding_epoch: BindingEpoch,
    frontiers: ClaimFrontiers,
    retained_charges: Vec<RetainedRecordCharge>,
    closure_accounting: ClosureAccounting,
    max_ordinary_record_charge: ResourceVector,
    observer_progress: u64,
    limits: OrdinaryProjectionLimits,
) -> TestPrestate<'a> {
    RecordAdmissionPrestate::new(
        request,
        PresentedIdentity::<TestFingerprint, TestFingerprint, TestFingerprint>::Live(member),
        binding,
        receiving_binding_epoch,
        ConnectionConversationTracking::AlreadyTracked,
        CapacityCounter::try_new(4, 1).expect("test connection capacity is valid"),
        closure_accounting,
        max_ordinary_record_charge,
        frontiers,
        retained_charges,
        observer_progress,
        limits,
    )
}

fn committed(decision: TestDecision<'_>) -> Box<RecordAdmissionCommit> {
    let RecordAdmissionDecision::Commit(commit) = decision else {
        panic!("ordinary record should commit")
    };
    commit
}

fn ordinary_record(sequence: u64, major: u64) -> RetainedCausalRecord {
    RetainedCausalRecord {
        delivery_seq: sequence,
        admission_order: AdmissionOrder::new(major, CandidatePhase::OrdinaryRecord, 0),
        kind: RetainedCausalRecordKind::OrdinaryRecord {
            participant_index: 0,
        },
    }
}

fn keyed_charges(
    records: &[RetainedCausalRecord],
    charge: ResourceVector,
) -> Vec<RetainedRecordCharge> {
    records
        .iter()
        .map(|record| {
            RetainedRecordCharge::new(record.delivery_seq, record.admission_order, charge)
        })
        .collect()
}

fn clear_accounting(baseline: WideResourceVector, cap: ResourceVector) -> ClosureAccounting {
    ClosureAccounting::try_new(
        ClosureState::Clear,
        0,
        0,
        0,
        0,
        ResourceVector::default(),
        baseline,
        cap,
        0,
        2,
    )
    .expect("test clear accounting is valid")
}

fn one_participant_frontiers(
    conversation_id: u64,
    binding_epoch: BindingEpoch,
    high_watermark: u64,
    order_high: u64,
    cursor: u64,
    floor: u128,
    retained_records: Vec<RetainedCausalRecord>,
) -> ClaimFrontiers {
    let terminal = BindingTerminalOwner {
        participant_index: 0,
        binding_epoch,
    };
    let sequence = SequenceLedger::try_new(
        high_watermark,
        SequenceClaims::new(1, 1, 0, RecoverySequenceReserve::None),
    )
    .expect("three sequence claims fit");
    let order = OrderLedger::try_new(
        OrderHigh::Allocated(order_high),
        OrderClaims::new(1, 1, false, false).expect("two order claims fit"),
    )
    .expect("order suffix fits");
    ClaimFrontiers::restore(
        ClaimFrontiersRestore {
            conversation_id,
            active_identities: vec![FrontierParticipant::new(
                0,
                cursor,
                FrontierBinding::Bound(binding_epoch),
            )],
            identity_slot_limit: 1,
            retained_floor: floor,
            retained_record_limit: 8,
            retained_records,
            active_marker_anchors: Vec::new(),
            historical_marker_deliveries: Vec::new(),
            historical_causal_facts: Vec::new(),
            sequence: SequenceClaimFrontierRestore {
                movable_claims: vec![
                    MovableSequenceClaim {
                        delivery_seq: high_watermark + 1,
                        owner: SequenceDirectOwner::MembershipExit {
                            participant_index: 0,
                        },
                    },
                    MovableSequenceClaim {
                        delivery_seq: high_watermark + 2,
                        owner: SequenceDirectOwner::BindingTerminal(terminal),
                    },
                ],
                immutable_candidates: Vec::new(),
                products: SequenceProductRangesRestore {
                    live_times_terminal: vec![TerminalProductRangeRestore {
                        start: high_watermark + 3,
                        length: 1,
                        terminal,
                    }],
                    live_times_replacement_terminal: None,
                    other_live_times_exit: Vec::new(),
                },
                recovery: None,
            },
            order: OrderClaimFrontierRestore {
                movable_claims: vec![
                    MovableOrderClaim {
                        transaction_order: order_high + 1,
                        owner: OrderDirectOwner::ActiveBindingTerminal(terminal),
                    },
                    MovableOrderClaim {
                        transaction_order: order_high + 2,
                        owner: OrderDirectOwner::MembershipExit {
                            participant_index: 0,
                        },
                    },
                ],
                immutable_candidates: Vec::new(),
                recovery: None,
            },
            recovery_marker_delivery_seq: None,
        },
        sequence,
        order,
    )
    .expect("one-participant exact frontiers restore")
}

fn assert_case_31_unchanged(
    subject: &TestPrestate<'_>,
    expected_frontiers: &ClaimFrontiers,
    high: u64,
    order_high: u64,
    binding_epoch: BindingEpoch,
) {
    let frontiers = subject.frontiers();
    assert_eq!(frontiers, expected_frontiers);
    assert_eq!(frontiers.sequence().ledger().high_watermark(), high);
    assert_eq!(frontiers.retained_floor(), u128::from(high - 1));
    assert_eq!(subject.observer_progress(), high);
    assert_eq!(subject.receiving_binding_epoch(), binding_epoch);
    assert_eq!(
        subject.binding(),
        &BindingState::Bound(ActiveBinding {
            participant_id: 0,
            conversation_id: 31_002,
            binding_epoch,
        })
    );
    assert_eq!(subject.request().payload.len(), 75);
    assert_eq!(
        frontiers
            .retained_records()
            .iter()
            .map(|record| (
                record.delivery_seq,
                record.admission_order.transaction_order()
            ))
            .collect::<Vec<_>>(),
        vec![(high - 1, order_high - 1), (high, order_high)]
    );
    assert_eq!(subject.retained_charges().len(), 2);
    assert_eq!(
        frontiers.active_identities().participants()[0].cursor(),
        high - 2
    );
    assert_eq!(
        frontiers.active_identities().participants()[0].binding(),
        FrontierBinding::Bound(binding_epoch)
    );
    assert_eq!(
        frontiers
            .sequence()
            .movable_claims()
            .iter()
            .map(|claim| claim.delivery_seq)
            .collect::<Vec<_>>(),
        vec![high + 1, high + 2]
    );
    assert_eq!(
        frontiers.sequence().products().live_times_terminal()[0].start(),
        high + 3
    );
    assert!(frontiers.sequence().immutable_candidates().is_empty());
    assert_eq!(
        frontiers.order().ledger().high(),
        OrderHigh::Allocated(order_high)
    );
    assert_eq!(
        frontiers
            .order()
            .movable_claims()
            .iter()
            .map(|claim| claim.transaction_order)
            .collect::<Vec<_>>(),
        vec![order_high + 1, order_high + 2]
    );
    assert!(frontiers.order().immutable_candidates().is_empty());
    assert!(frontiers.retained_marker_records().is_empty());
    assert_eq!(subject.closure_accounting().state(), ClosureState::Clear);
    assert_eq!(subject.closure_accounting().marker_capacity_credits(), 0);
    assert_eq!(subject.closure_accounting().marker_anchors(), 0);
}

#[test]
fn capacity_walk_total_operation_commits_full_payload_and_replays_identically() {
    let fixture = ordinary_capacity_walk_fixture();
    let member = test_member(
        fixture.request.conversation_id,
        fixture.request.capability_generation,
        fixture.active_identities[0].cursor(),
    );
    let active = ActiveBinding {
        participant_id: 0,
        conversation_id: fixture.request.conversation_id,
        binding_epoch: fixture.receiving_binding_epoch,
    };
    let binding = BindingState::Bound(active);
    let payload = vec![0; 3 * usize::try_from(UNIT).expect("unit fits usize")];
    let make_prestate = || {
        prestate(
            request(&fixture.request, &payload),
            &member,
            &binding,
            fixture.receiving_binding_epoch,
            ordinary_capacity_walk_frontiers(&fixture),
            fixture.retained_charges.clone(),
            fixture.closure_accounting,
            fixture.encoded_record_charge,
            fixture.observer_progress,
            fixture.limits,
        )
    };

    let first = committed(apply_record_admission(
        make_prestate(),
        fixture.encoded_record_charge,
    ));
    assert_eq!(
        first.outcome(),
        &RecordCommitted::new(fixture.request.clone(), 95)
    );
    assert_eq!(first.record().request().payload, payload);
    assert_eq!(first.record().delivery_seq(), 95);
    assert_eq!(first.record().admission_order().transaction_order(), 86);
    assert_eq!(
        first.record().encoded_record_charge(),
        ResourceVector::new(1, 40)
    );
    assert_eq!(first.projection().floor().resulting_floor, 91);
    assert_eq!(
        first.projection().new_marker_candidates()[0].delivery_seq,
        96
    );
    assert_eq!(first.observer_floor().observer_progress(), 94);
    assert_eq!(first.observer_floor().cap_floor(), 91);
    assert_eq!(first.connection_capacity().resulting().occupied(), 1);

    let replay = committed(apply_record_admission(
        make_prestate(),
        fixture.encoded_record_charge,
    ));
    assert_eq!(first, replay);

    let persisted = (*replay).into_persistence_parts();
    assert_eq!(persisted.outcome.delivery_seq(), 95);
    assert_eq!(persisted.record.request().payload, payload);
    assert_eq!(persisted.connection_capacity.resulting().occupied(), 1);
    assert_eq!(persisted.order.major(), 86);
    assert_eq!(persisted.sequence.resulting().high_watermark(), 95);
    assert_eq!(persisted.observer_floor.cap_floor(), 91);
    assert_eq!(persisted.closure.accounting(), fixture.closure_accounting);
    assert_eq!(persisted.frontiers.retained_floor(), 91);
    assert_eq!(persisted.retained_charges.len(), 5);
    assert_eq!(persisted.accounting.baseline(), persisted.baseline);
    assert_eq!(
        persisted.required_capacity,
        persisted.closure.required_capacity()
    );
    assert_eq!(persisted.caller_record.delivery_seq, 95);
    assert_eq!(persisted.caller_charge.delivery_seq(), 95);
    assert_eq!(persisted.marker_candidates[0].delivery_seq, 96);
}

#[test]
fn into_persistence_parts_moves_the_complete_atomic_set_exactly_once() {
    let fixture = ordinary_capacity_walk_fixture();
    let member = test_member(
        fixture.request.conversation_id,
        fixture.request.capability_generation,
        fixture.active_identities[0].cursor(),
    );
    let active = ActiveBinding {
        participant_id: 0,
        conversation_id: fixture.request.conversation_id,
        binding_epoch: fixture.receiving_binding_epoch,
    };
    let binding = BindingState::Bound(active);
    let payload = vec![0; 3 * usize::try_from(UNIT).expect("unit fits usize")];
    let commit = committed(apply_record_admission(
        prestate(
            request(&fixture.request, &payload),
            &member,
            &binding,
            fixture.receiving_binding_epoch,
            ordinary_capacity_walk_frontiers(&fixture),
            fixture.retained_charges.clone(),
            fixture.closure_accounting,
            fixture.encoded_record_charge,
            fixture.observer_progress,
            fixture.limits,
        ),
        fixture.encoded_record_charge,
    ));

    // Pre-move witnesses captured through the borrowing accessors only.
    let expected_outcome = commit.outcome().clone();
    let expected_record = commit.record().clone();
    let expected_connection = commit.connection_capacity();
    let expected_order = commit.order();
    let expected_sequence = commit.sequence();
    let expected_observer = commit.observer_floor();
    let expected_closure = *commit.closure();
    let expected_floor = commit.projection().floor();
    let expected_retained_charge = commit.projection().retained_charge();
    let expected_baseline = commit.projection().baseline();
    let expected_accounting = commit.projection().accounting();
    let expected_required = commit.projection().required_capacity();
    let expected_caller_record = commit.projection().caller_record();
    let expected_caller_charge = commit.projection().caller_charge();
    let expected_row_charges = commit.projection().retained_charges().to_vec();
    let expected_markers = commit.projection().new_marker_candidates().to_vec();
    let expected_high = commit
        .projection()
        .frontiers()
        .sequence()
        .ledger()
        .high_watermark();
    let expected_retained_floor = commit.projection().frontiers().retained_floor();
    let expected_rows: Vec<_> = commit
        .projection()
        .frontiers()
        .retained_records()
        .iter()
        .map(|record| (record.delivery_seq, record.admission_order))
        .collect();
    let expected_cursor = commit
        .projection()
        .frontiers()
        .active_identities()
        .participants()[0]
        .cursor();

    // Consuming the commit moves the parts out. `RecordAdmissionCommit` is
    // gone after this statement and `ClaimFrontiers` is neither `Clone` nor
    // `Copy`, so no frontier, accounting, row, or marker authority stays
    // reachable through a second owner; the borrow checker rejects any later
    // use of `commit`. The exhaustive destructuring below (no `..` rest
    // pattern) proves the complete atomic set is transferred.
    let RecordAdmissionPersistenceParts {
        outcome,
        record,
        connection_capacity,
        order,
        sequence,
        observer_floor,
        closure,
        frontiers,
        floor,
        retained_charge,
        baseline,
        accounting,
        required_capacity,
        caller_record,
        caller_charge,
        retained_charges,
        marker_candidates,
    } = commit.into_persistence_parts();

    assert_eq!(outcome, expected_outcome);
    assert_eq!(record, expected_record);
    assert_eq!(connection_capacity, expected_connection);
    assert_eq!(order, expected_order);
    assert_eq!(sequence, expected_sequence);
    assert_eq!(observer_floor, expected_observer);
    assert_eq!(closure, expected_closure);
    assert_eq!(floor, expected_floor);
    assert_eq!(retained_charge, expected_retained_charge);
    assert_eq!(baseline, expected_baseline);
    assert_eq!(accounting, expected_accounting);
    assert_eq!(required_capacity, expected_required);
    assert_eq!(caller_record, expected_caller_record);
    assert_eq!(caller_charge, expected_caller_charge);
    assert_eq!(retained_charges, expected_row_charges);
    assert_eq!(marker_candidates, expected_markers);

    // The moved frontier authority is the exact projected poststate.
    assert_eq!(
        frontiers.sequence().ledger().high_watermark(),
        expected_high
    );
    assert_eq!(frontiers.retained_floor(), expected_retained_floor);
    assert_eq!(
        frontiers
            .retained_records()
            .iter()
            .map(|record| (record.delivery_seq, record.admission_order))
            .collect::<Vec<_>>(),
        expected_rows
    );
    assert_eq!(
        frontiers.active_identities().participants()[0].cursor(),
        expected_cursor
    );

    // Cross-field authority coupling: the moved caller row is the committed
    // record's exact key, and the moved outcome matches the moved record.
    assert_eq!(caller_record.delivery_seq, record.delivery_seq());
    assert_eq!(caller_record.admission_order, record.admission_order());
    assert_eq!(outcome.delivery_seq(), record.delivery_seq());
    assert_eq!(caller_charge.delivery_seq(), record.delivery_seq());
}

#[test]
fn case_31_sequence_exhaustion_returns_exact_budget_and_unchanged_replay() {
    let conversation_id = 31_002;
    let generation = Generation::new((u64::MAX - 5) / 2).expect("frozen generation is nonzero");
    let binding_epoch = BindingEpoch::new(ConnectionIncarnation::new(31, 2), generation);
    let high = u64::MAX - 4;
    let order_high = (u64::MAX - 3) / 2;
    let retained = vec![
        ordinary_record(high - 1, order_high - 1),
        ordinary_record(high, order_high),
    ];
    let retained_charges = keyed_charges(&retained, ResourceVector::new(1, 100));
    let frontiers = || {
        one_participant_frontiers(
            conversation_id,
            binding_epoch,
            high,
            order_high,
            high - 2,
            u128::from(high - 1),
            retained.clone(),
        )
    };
    let member = test_member(conversation_id, generation, high - 2);
    let binding = BindingState::Bound(ActiveBinding {
        participant_id: 0,
        conversation_id,
        binding_epoch,
    });
    let envelope = RecordAdmissionEnvelope {
        conversation_id,
        participant_id: 0,
        capability_generation: generation,
    };
    let accounting = clear_accounting(WideResourceVector::new(3, 300), ResourceVector::new(7, 700));
    let limits = OrdinaryProjectionLimits::new(
        ResourceVector::new(1, 100),
        ResourceVector::new(2, 200),
        ResourceVector::new(2, 200),
    );
    let encoded = ResourceVector::new(1, 100);
    let expected_frontiers = frontiers();
    let decision = apply_record_admission(
        prestate(
            request(&envelope, &[0; 75]),
            &member,
            &binding,
            binding_epoch,
            frontiers(),
            retained_charges,
            accounting,
            encoded,
            high,
            limits,
        ),
        encoded,
    );
    let RecordAdmissionDecision::Respond(refusal) = decision else {
        panic!("case 31 must refuse sequence exhaustion")
    };
    let expected =
        ServerValue::ConversationSequenceExhausted(Box::new(ConversationSequenceExhausted {
            request: SequenceAllocatingEnvelope::RecordAdmission(envelope),
            sequence_budget: SequenceBudget {
                high_watermark: u64::MAX - 3,
                remaining: 3,
                e: 1,
                t: 1,
                m: 1,
                rs: 0,
                rt: 0,
                l_times_t: 1,
                l_times_rt: 0,
                l_other_times_e: 0,
            },
        }));
    assert_eq!(refusal.response().server_value(), &expected);
    assert_case_31_unchanged(
        refusal.unchanged().prestate(),
        &expected_frontiers,
        high,
        order_high,
        binding_epoch,
    );
    assert_eq!(refusal.unchanged().encoded_record_charge(), encoded);

    let (_, unchanged) = refusal.into_parts();
    let (replay_prestate, replay_charge) = unchanged.into_parts();
    let RecordAdmissionDecision::Respond(replayed) =
        apply_record_admission(replay_prestate, replay_charge)
    else {
        panic!("unchanged case 31 state must replay the same refusal")
    };
    assert_eq!(replayed.response().server_value(), &expected);
    assert_case_31_unchanged(
        replayed.unchanged().prestate(),
        &expected_frontiers,
        high,
        order_high,
        binding_epoch,
    );
}

#[test]
fn case_32_size_dimensions_preserve_state_and_lookup_precedes_size() {
    let conversation_id = 32_004;
    let generation = Generation::new(7).expect("seven is nonzero");
    let binding_epoch = BindingEpoch::new(ConnectionIncarnation::new(32, 4), generation);
    let retained = vec![ordinary_record(1, 1)];
    let retained_charges = keyed_charges(&retained, ResourceVector::new(1, 100));
    let frontiers =
        || one_participant_frontiers(conversation_id, binding_epoch, 1, 1, 0, 1, retained.clone());
    let member = test_member(conversation_id, generation, 0);
    let bound = BindingState::Bound(ActiveBinding {
        participant_id: 0,
        conversation_id,
        binding_epoch,
    });
    let envelope = RecordAdmissionEnvelope {
        conversation_id,
        participant_id: 0,
        capability_generation: generation,
    };
    let accounting = clear_accounting(
        WideResourceVector::new(2, 200),
        ResourceVector::new(10, 1_000),
    );
    let limits = OrdinaryProjectionLimits::new(
        ResourceVector::new(1, 100),
        ResourceVector::new(2, 200),
        ResourceVector::new(2, 200),
    );
    let make_prestate = |payload_len: usize, maximum: ResourceVector| {
        prestate(
            request(&envelope, &vec![0; payload_len]),
            &member,
            &bound,
            binding_epoch,
            frontiers(),
            retained_charges.clone(),
            accounting,
            maximum,
            1,
            limits,
        )
    };

    let below = committed(apply_record_admission(
        make_prestate(9, ResourceVector::new(2, 110)),
        ResourceVector::new(1, 109),
    ));
    assert_eq!(below.record().request().payload.len(), 9);
    assert_eq!(
        below.record().encoded_record_charge(),
        ResourceVector::new(1, 109)
    );

    let equal = committed(apply_record_admission(
        make_prestate(10, ResourceVector::new(1, 110)),
        ResourceVector::new(1, 110),
    ));
    assert_eq!(equal.record().request().payload.len(), 10);
    assert_eq!(
        equal.record().encoded_record_charge(),
        ResourceVector::new(1, 110)
    );

    let encoded = ResourceVector::new(1, 111);
    let bytes_max = ResourceVector::new(1, 110);
    let expected_frontiers = frontiers();
    let RecordAdmissionDecision::Respond(bytes) =
        apply_record_admission(make_prestate(11, bytes_max), encoded)
    else {
        panic!("case 32 byte maximum must refuse")
    };
    let expected_bytes = ServerValue::RecordTooLarge(RecordTooLarge {
        request: envelope.clone(),
        dimension: ResourceDimension::Bytes,
        encoded_record_charge: encoded,
        max_ordinary_record_charge: bytes_max,
    });
    assert_eq!(bytes.response().server_value(), &expected_bytes);
    assert_eq!(
        bytes.unchanged().prestate().frontiers(),
        &expected_frontiers
    );
    assert_eq!(
        bytes
            .unchanged()
            .prestate()
            .connection_capacity()
            .occupied(),
        1
    );
    let (_, unchanged) = bytes.into_parts();
    let (replay_prestate, replay_charge) = unchanged.into_parts();
    let RecordAdmissionDecision::Respond(replayed) =
        apply_record_admission(replay_prestate, replay_charge)
    else {
        panic!("size refusal must replay from returned prestate")
    };
    assert_eq!(replayed.response().server_value(), &expected_bytes);

    let entries_max = ResourceVector::new(0, 110);
    let RecordAdmissionDecision::Respond(entries) =
        apply_record_admission(make_prestate(10, entries_max), ResourceVector::new(1, 110))
    else {
        panic!("case 32 zero entry maximum must refuse")
    };
    assert_eq!(
        entries.response().server_value(),
        &ServerValue::RecordTooLarge(RecordTooLarge {
            request: envelope.clone(),
            dimension: ResourceDimension::Entries,
            encoded_record_charge: ResourceVector::new(1, 110),
            max_ordinary_record_charge: entries_max,
        })
    );

    let RecordAdmissionDecision::Respond(simultaneous) =
        apply_record_admission(make_prestate(11, entries_max), encoded)
    else {
        panic!("simultaneous case 32 failure must select entries")
    };
    assert_eq!(
        simultaneous.response().server_value(),
        &ServerValue::RecordTooLarge(RecordTooLarge {
            request: envelope.clone(),
            dimension: ResourceDimension::Entries,
            encoded_record_charge: encoded,
            max_ordinary_record_charge: entries_max,
        })
    );

    let detached = BindingState::Detached;
    let RecordAdmissionDecision::Respond(no_binding) = apply_record_admission(
        prestate(
            request(&envelope, &[0; 11]),
            &member,
            &detached,
            binding_epoch,
            frontiers(),
            retained_charges,
            accounting,
            entries_max,
            1,
            limits,
        ),
        encoded,
    ) else {
        panic!("binding lookup must refuse before static size")
    };
    assert!(matches!(
        no_binding.response().server_value(),
        ServerValue::NoBinding(_)
    ));
    assert_eq!(
        no_binding.unchanged().prestate().frontiers(),
        &expected_frontiers
    );
}

#[test]
fn untracked_semantic_conversation_refusal_carries_exact_envelope_and_limit() {
    let conversation_id = 33_002;
    let generation = Generation::new(7).expect("seven is nonzero");
    let binding_epoch = BindingEpoch::new(ConnectionIncarnation::new(33, 2), generation);
    let retained = vec![ordinary_record(1, 1)];
    let retained_charges = keyed_charges(&retained, ResourceVector::new(1, 100));
    let frontiers =
        || one_participant_frontiers(conversation_id, binding_epoch, 1, 1, 0, 1, retained.clone());
    let member = test_member(conversation_id, generation, 0);
    let binding = BindingState::Bound(ActiveBinding {
        participant_id: 0,
        conversation_id,
        binding_epoch,
    });
    let envelope = RecordAdmissionEnvelope {
        conversation_id,
        participant_id: 0,
        capability_generation: generation,
    };
    let accounting = clear_accounting(
        WideResourceVector::new(2, 200),
        ResourceVector::new(10, 1_000),
    );
    let limits = OrdinaryProjectionLimits::new(
        ResourceVector::new(1, 100),
        ResourceVector::new(2, 200),
        ResourceVector::new(2, 200),
    );
    let encoded = ResourceVector::new(1, 100);
    let expected_frontiers = frontiers();
    let subject = RecordAdmissionPrestate::new(
        request(&envelope, &[0; 9]),
        PresentedIdentity::<TestFingerprint, TestFingerprint, TestFingerprint>::Live(&member),
        &binding,
        binding_epoch,
        ConnectionConversationTracking::Untracked,
        CapacityCounter::try_new(2, 2).expect("full test connection capacity is valid"),
        accounting,
        ResourceVector::new(2, 110),
        frontiers(),
        retained_charges,
        1,
        limits,
    );
    let RecordAdmissionDecision::Respond(refusal) = apply_record_admission(subject, encoded) else {
        panic!("first untracked semantic conversation at the full limit must refuse")
    };

    // Register row 5641: the refusal carries the triggering request's exact
    // common envelope plus the signed connection-conversation limit, asserted
    // as the complete wire value through the production flow.
    let expected = RecordAdmissionResponse::connection_conversation_capacity_exceeded(envelope, 2);
    assert_eq!(refusal.response(), &expected);
    assert_eq!(
        refusal.unchanged().prestate().frontiers(),
        &expected_frontiers
    );
    assert_eq!(
        refusal
            .unchanged()
            .prestate()
            .connection_capacity()
            .occupied(),
        2
    );
    assert_eq!(refusal.unchanged().encoded_record_charge(), encoded);

    let (_, unchanged) = refusal.into_parts();
    let (replay_prestate, replay_charge) = unchanged.into_parts();
    let RecordAdmissionDecision::Respond(replayed) =
        apply_record_admission(replay_prestate, replay_charge)
    else {
        panic!("capacity refusal must replay from the returned prestate")
    };
    assert_eq!(replayed.response(), &expected);
    assert_eq!(
        replayed.unchanged().prestate().frontiers(),
        &expected_frontiers
    );
}

#[test]
fn projected_marker_drains_first_and_replays_with_complete_unchanged_input() {
    let fixture = ordinary_capacity_walk_fixture();
    let member = test_member(
        fixture.request.conversation_id,
        fixture.request.capability_generation,
        fixture.active_identities[0].cursor(),
    );
    let binding = BindingState::Bound(ActiveBinding {
        participant_id: 0,
        conversation_id: fixture.request.conversation_id,
        binding_epoch: fixture.receiving_binding_epoch,
    });
    let first = committed(apply_record_admission(
        prestate(
            request(&fixture.request, &[1]),
            &member,
            &binding,
            fixture.receiving_binding_epoch,
            ordinary_capacity_walk_frontiers(&fixture),
            fixture.retained_charges.clone(),
            fixture.closure_accounting,
            fixture.encoded_record_charge,
            fixture.observer_progress,
            fixture.limits,
        ),
        fixture.encoded_record_charge,
    ));
    let persisted = (*first).into_persistence_parts();
    let next_request = request(&fixture.request, &[2]);
    let RecordAdmissionDecision::DrainFirst(drain) = apply_record_admission(
        prestate(
            next_request.clone(),
            &member,
            &binding,
            fixture.receiving_binding_epoch,
            persisted.frontiers,
            persisted.retained_charges,
            persisted.accounting,
            fixture.encoded_record_charge,
            fixture.observer_progress,
            fixture.limits,
        ),
        fixture.encoded_record_charge,
    ) else {
        panic!("owned marker must drain before the second ordinary record")
    };
    assert_eq!(drain.candidate().delivery_seq(), 96);
    assert_eq!(drain.unchanged().prestate().request(), &next_request);
    assert_eq!(
        drain.unchanged().prestate().frontiers().retained_floor(),
        91
    );
    assert_eq!(drain.unchanged().prestate().retained_charges().len(), 5);
    assert_eq!(
        drain.unchanged().encoded_record_charge(),
        ResourceVector::new(1, 40)
    );

    let candidate = drain.candidate();
    let (_, unchanged) = drain.into_parts();
    let (prestate, charge) = unchanged.into_parts();
    let RecordAdmissionDecision::DrainFirst(replayed) = apply_record_admission(prestate, charge)
    else {
        panic!("drain-first must replay until the candidate commits")
    };
    assert_eq!(replayed.candidate(), candidate);
    assert_eq!(replayed.unchanged().prestate().request(), &next_request);
    assert_eq!(
        replayed.unchanged().prestate().frontiers().retained_floor(),
        91
    );
}

#[test]
fn observer_fixed_point_refusal_maps_through_shared_selector_without_mutation() {
    let mut fixture = ordinary_capacity_walk_fixture();
    fixture.observer_progress = 89;
    let member = test_member(
        fixture.request.conversation_id,
        fixture.request.capability_generation,
        fixture.active_identities[0].cursor(),
    );
    let binding = BindingState::Bound(ActiveBinding {
        participant_id: 0,
        conversation_id: fixture.request.conversation_id,
        binding_epoch: fixture.receiving_binding_epoch,
    });
    let expected_frontiers = ordinary_capacity_walk_frontiers(&fixture);
    let RecordAdmissionDecision::Respond(refusal) = apply_record_admission(
        prestate(
            request(&fixture.request, &[0x0B]),
            &member,
            &binding,
            fixture.receiving_binding_epoch,
            ordinary_capacity_walk_frontiers(&fixture),
            fixture.retained_charges.clone(),
            fixture.closure_accounting,
            fixture.encoded_record_charge,
            fixture.observer_progress,
            fixture.limits,
        ),
        fixture.encoded_record_charge,
    ) else {
        panic!("capacity-walk floor must be blocked below observer 90")
    };
    let ServerValue::ObserverBackpressure(value) = refusal.response().server_value() else {
        panic!("fixed-point observer failure must map to observer response")
    };
    let crate::wire::ObserverBackpressure::RecordAdmission { state, .. } = value else {
        panic!("response must retain the record-admission selector")
    };
    assert_eq!(*state, ObserverBackpressureState::initial(89));
    assert_eq!(
        refusal.unchanged().prestate().frontiers(),
        &expected_frontiers
    );
    assert_eq!(
        refusal.unchanged().prestate().frontiers().retained_floor(),
        89
    );
}
