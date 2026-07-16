#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use alloc::{vec, vec::Vec};

use crate::{
    algebra::{ResourceVector, WideResourceVector},
    outcome::CandidatePhase,
    wire::{BindingEpoch, ConnectionIncarnation, Generation, RecordAdmissionEnvelope},
};

use super::super::{
    AdmissionOrder, BindingTerminalOwner, ClaimFrontiers, ClaimFrontiersRestore, ClosureAccounting,
    ClosureState, FrontierBinding, FrontierParticipant, ImmutableSequenceCandidate,
    MarkerCandidateAuthority, MarkerProvenance, MarkerSequenceOwner, MovableOrderClaim,
    MovableSequenceClaim, OrderClaimFrontierRestore, OrderClaims, OrderDirectOwner, OrderHigh,
    OrderLedger, RecoverySequenceReserve, RetainedCausalRecord, RetainedCausalRecordKind,
    SequenceClaimFrontierRestore, SequenceClaims, SequenceDirectOwner, SequenceLedger,
    SequenceProductRangesRestore, TerminalProductRangeRestore,
};
use super::ordinary_record_projection::{
    OrdinaryProjectionError, OrdinaryProjectionFacts, OrdinaryProjectionKernelDecision,
    OrdinaryProjectionLimits, OrdinaryRecordProjectionDecision, OrdinaryRecordProjectionInput,
    RetainedRecordCharge, project_ordinary_fixed_point,
};

pub(super) const CONVERSATION: u64 = 54;
pub(super) const UNIT: u64 = 10;

#[derive(Clone)]
pub(super) struct Fixture {
    pub(super) request: RecordAdmissionEnvelope,
    pub(super) receiving_binding_epoch: BindingEpoch,
    pub(super) encoded_record_charge: ResourceVector,
    pub(super) retained_records: Vec<RetainedCausalRecord>,
    pub(super) retained_charges: Vec<RetainedRecordCharge>,
    pub(super) active_marker_credit_records: Vec<RetainedCausalRecord>,
    pub(super) unaccepted_marker_anchors: Vec<u64>,
    pub(super) active_identities: Vec<FrontierParticipant>,
    pub(super) identity_slot_limit: u64,
    pub(super) current_floor: u128,
    pub(super) observer_progress: u64,
    pub(super) order_ledger: OrderLedger,
    pub(super) sequence_ledger: SequenceLedger,
    pub(super) immutable_candidates: Vec<ImmutableSequenceCandidate>,
    pub(super) closure_accounting: ClosureAccounting,
    pub(super) limits: OrdinaryProjectionLimits,
}

impl Fixture {
    fn project(&self) -> Result<OrdinaryProjectionKernelDecision, OrdinaryProjectionError> {
        project_ordinary_fixed_point(&OrdinaryProjectionFacts {
            request: self.request.clone(),
            receiving_binding_epoch: self.receiving_binding_epoch,
            encoded_record_charge: self.encoded_record_charge,
            retained_records: &self.retained_records,
            retained_charges: &self.retained_charges,
            active_marker_credit_records: &self.active_marker_credit_records,
            unaccepted_marker_anchors: &self.unaccepted_marker_anchors,
            active_identities: &self.active_identities,
            identity_slot_limit: self.identity_slot_limit,
            current_floor: self.current_floor,
            observer_progress: self.observer_progress,
            order_ledger: self.order_ledger,
            sequence_ledger: self.sequence_ledger,
            immutable_candidates: &self.immutable_candidates,
            closure_accounting: self.closure_accounting,
            remaining_recovery_claim: ResourceVector::default(),
            limits: self.limits,
        })
    }
}

pub(super) fn epoch(ordinal: u64) -> BindingEpoch {
    BindingEpoch::new(ConnectionIncarnation::new(1, ordinal), Generation::ONE)
}

fn request() -> RecordAdmissionEnvelope {
    RecordAdmissionEnvelope {
        conversation_id: CONVERSATION,
        participant_id: 0,
        capability_generation: Generation::ONE,
    }
}

fn order_ledger(high: u64, active: u64, exits: u64) -> OrderLedger {
    let claims = OrderClaims::new(active, exits, false, false).expect("coupled clear claims");
    OrderLedger::try_new(OrderHigh::Allocated(high), claims).expect("order suffix fits")
}

fn sequence_ledger(high: u64, live: u64, terminals: u64, markers: u64) -> SequenceLedger {
    SequenceLedger::try_new(
        high,
        SequenceClaims::new(live, terminals, markers, RecoverySequenceReserve::None),
    )
    .expect("sequence suffix fits")
}

fn ordinary_record(sequence: u64, major: u64, participant: u64) -> RetainedCausalRecord {
    RetainedCausalRecord {
        delivery_seq: sequence,
        admission_order: AdmissionOrder::new(major, CandidatePhase::OrdinaryRecord, participant),
        kind: RetainedCausalRecordKind::OrdinaryRecord {
            participant_index: participant,
        },
    }
}

fn marker_record(sequence: u64, major: u64, participant: u64) -> RetainedCausalRecord {
    RetainedCausalRecord {
        delivery_seq: sequence,
        admission_order: AdmissionOrder::new(major, CandidatePhase::CompactionMarker, participant),
        kind: RetainedCausalRecordKind::CompactionMarker {
            participant_index: participant,
            provenance: MarkerProvenance::NonProductM,
        },
    }
}

fn keyed_charges(
    records: &[RetainedCausalRecord],
    byte_charges: &[u64],
) -> Vec<RetainedRecordCharge> {
    records
        .iter()
        .zip(byte_charges)
        .map(|(record, bytes)| {
            RetainedRecordCharge::new(
                record.delivery_seq,
                record.admission_order,
                ResourceVector::new(1, *bytes),
            )
        })
        .collect()
}

fn accounting(
    credits: u64,
    anchors: u64,
    baseline: WideResourceVector,
    cap: ResourceVector,
) -> ClosureAccounting {
    ClosureAccounting::try_new(
        ClosureState::Clear,
        credits,
        anchors,
        0,
        0,
        ResourceVector::default(),
        baseline,
        cap,
        0,
        2,
    )
    .expect("valid clear accounting")
}

pub(super) fn ordinary_capacity_walk_fixture() -> Fixture {
    let h = 100;
    let records: Vec<_> = (h - 11..=h - 6)
        .enumerate()
        .map(|(index, sequence)| {
            ordinary_record(
                sequence,
                80 + u64::try_from(index).expect("fixture rank fits u64"),
                0,
            )
        })
        .collect();
    let charges = keyed_charges(
        &records,
        &[UNIT, 11 * UNIT, 4 * UNIT, 4 * UNIT, 4 * UNIT, 4 * UNIT],
    );
    let cap = ResourceVector::new(12, 48 * UNIT);
    Fixture {
        request: request(),
        receiving_binding_epoch: epoch(7),
        encoded_record_charge: ResourceVector::new(1, 4 * UNIT),
        retained_records: records,
        retained_charges: charges,
        active_marker_credit_records: Vec::new(),
        unaccepted_marker_anchors: Vec::new(),
        active_identities: vec![FrontierParticipant::new(
            0,
            h - 12,
            FrontierBinding::Bound(epoch(7)),
        )],
        identity_slot_limit: 1,
        current_floor: u128::from(h - 11),
        observer_progress: h - 6,
        order_ledger: order_ledger(h - 15, 1, 1),
        sequence_ledger: sequence_ledger(h - 6, 1, 1, 0),
        immutable_candidates: Vec::new(),
        closure_accounting: accounting(
            0,
            0,
            WideResourceVector::new(7, u128::from(32 * UNIT)),
            cap,
        ),
        limits: OrdinaryProjectionLimits::new(
            ResourceVector::new(1, 4 * UNIT),
            ResourceVector::new(2, 8 * UNIT),
            ResourceVector::new(2, 8 * UNIT),
        ),
    }
}

pub(super) fn ordinary_capacity_walk_frontiers(fixture: &Fixture) -> ClaimFrontiers {
    let terminal = BindingTerminalOwner {
        participant_index: 0,
        binding_epoch: fixture.receiving_binding_epoch,
    };
    ClaimFrontiers::restore(
        ClaimFrontiersRestore {
            conversation_id: CONVERSATION,
            active_identities: fixture.active_identities.clone(),
            identity_slot_limit: fixture.identity_slot_limit,
            retained_floor: fixture.current_floor,
            retained_record_limit: 32,
            retained_records: fixture.retained_records.clone(),
            active_marker_anchors: fixture
                .active_marker_credit_records
                .iter()
                .map(|record| record.delivery_seq)
                .collect(),
            historical_marker_deliveries: Vec::new(),
            historical_causal_facts: Vec::new(),
            sequence: SequenceClaimFrontierRestore {
                movable_claims: vec![
                    MovableSequenceClaim {
                        delivery_seq: 95,
                        owner: SequenceDirectOwner::MembershipExit {
                            participant_index: 0,
                        },
                    },
                    MovableSequenceClaim {
                        delivery_seq: 96,
                        owner: SequenceDirectOwner::BindingTerminal(terminal),
                    },
                ],
                immutable_candidates: fixture.immutable_candidates.clone(),
                products: SequenceProductRangesRestore {
                    live_times_terminal: vec![TerminalProductRangeRestore {
                        start: 97,
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
                        transaction_order: 86,
                        owner: OrderDirectOwner::ActiveBindingTerminal(terminal),
                    },
                    MovableOrderClaim {
                        transaction_order: 87,
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
        fixture.sequence_ledger,
        fixture.order_ledger,
    )
    .expect("ordinary capacity-walk frontiers restore")
}

fn credited_marker_frontiers(
    records: Vec<RetainedCausalRecord>,
    binding_epoch: BindingEpoch,
) -> ClaimFrontiers {
    let terminal = BindingTerminalOwner {
        participant_index: 0,
        binding_epoch,
    };
    ClaimFrontiers::restore(
        ClaimFrontiersRestore {
            conversation_id: CONVERSATION,
            active_identities: vec![FrontierParticipant::new(
                0,
                1,
                FrontierBinding::Bound(binding_epoch),
            )],
            identity_slot_limit: 1,
            retained_floor: 1,
            retained_record_limit: 8,
            retained_records: records,
            active_marker_anchors: vec![1],
            historical_marker_deliveries: Vec::new(),
            historical_causal_facts: Vec::new(),
            sequence: SequenceClaimFrontierRestore {
                movable_claims: vec![
                    MovableSequenceClaim {
                        delivery_seq: 3,
                        owner: SequenceDirectOwner::MembershipExit {
                            participant_index: 0,
                        },
                    },
                    MovableSequenceClaim {
                        delivery_seq: 4,
                        owner: SequenceDirectOwner::BindingTerminal(terminal),
                    },
                ],
                immutable_candidates: Vec::new(),
                products: SequenceProductRangesRestore {
                    live_times_terminal: vec![TerminalProductRangeRestore {
                        start: 5,
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
                        transaction_order: 2,
                        owner: OrderDirectOwner::ActiveBindingTerminal(terminal),
                    },
                    MovableOrderClaim {
                        transaction_order: 3,
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
        sequence_ledger(2, 1, 1, 0),
        order_ledger(1, 1, 1),
    )
    .expect("credited-marker exact frontiers restore")
}

#[test]
fn consuming_capacity_walk_wrapper_relays_exact_owners_and_seals_marker_prefix() {
    let fixture = ordinary_capacity_walk_fixture();
    let frontiers = ordinary_capacity_walk_frontiers(&fixture);
    let decision = frontiers
        .project_ordinary_record(OrdinaryRecordProjectionInput::new(
            fixture.request.clone(),
            fixture.receiving_binding_epoch,
            fixture.encoded_record_charge,
            fixture.retained_charges.clone(),
            fixture.observer_progress,
            fixture.closure_accounting,
            fixture.limits,
        ))
        .expect("ordinary capacity-walk projection is legal");
    let OrdinaryRecordProjectionDecision::Projected(projected) = decision else {
        panic!("ordinary capacity walk has no pre-owned prefix")
    };

    assert_eq!(projected.floor().resulting_floor, 91);
    assert_eq!(projected.sequence().resulting().high_watermark(), 95);
    assert_eq!(projected.order().major(), 86);
    assert_eq!(
        projected
            .retained_records()
            .iter()
            .map(|record| record.delivery_seq)
            .collect::<Vec<_>>(),
        vec![91, 92, 93, 94, 95]
    );
    assert_eq!(projected.retained_charges().len(), 5);
    assert_eq!(projected.new_marker_candidates()[0].delivery_seq, 96);
    assert_eq!(
        projected
            .frontiers()
            .sequence()
            .movable_claims()
            .iter()
            .map(|claim| claim.delivery_seq)
            .collect::<Vec<_>>(),
        vec![97, 98]
    );
    assert_eq!(
        projected
            .frontiers()
            .sequence()
            .products()
            .live_times_terminal()[0]
            .start(),
        99
    );
    assert_eq!(
        projected
            .frontiers()
            .order()
            .movable_claims()
            .iter()
            .map(|claim| claim.transaction_order)
            .collect::<Vec<_>>(),
        vec![87, 88]
    );
    let marker_major = &projected.frontiers().order().immutable_candidates()[0];
    assert_eq!(marker_major.transaction_order(), 86);
    assert_eq!(marker_major.candidate_keys().len(), 1);
    assert!(projected.frontiers().cross_counter_valid_for_test());

    let next_charges = projected.retained_charges().to_vec();
    let next_accounting = projected.accounting();
    let next_frontiers = projected.into_frontiers();
    let next = next_frontiers
        .project_ordinary_record(OrdinaryRecordProjectionInput::new(
            request(),
            fixture.receiving_binding_epoch,
            fixture.encoded_record_charge,
            next_charges,
            fixture.observer_progress,
            next_accounting,
            fixture.limits,
        ))
        .expect("pre-owned marker is durable progress");
    let OrdinaryRecordProjectionDecision::DrainFirst(drain) = next else {
        panic!("the marker prefix must drain before another ordinary record")
    };
    assert_eq!(drain.candidate().delivery_seq(), 96);
    assert_eq!(drain.frontiers().sequence().ledger().high_watermark(), 95);
    assert_eq!(
        drain.frontiers().order().ledger().high(),
        OrderHigh::Allocated(86)
    );
    assert_eq!(drain.frontiers().retained_floor(), 91);
    assert_eq!(
        drain
            .frontiers()
            .retained_records()
            .iter()
            .map(|record| record.delivery_seq)
            .collect::<Vec<_>>(),
        vec![91, 92, 93, 94, 95]
    );
    assert!(drain.frontiers().cross_counter_valid_for_test());
}

#[test]
fn consuming_wrapper_rejects_a_disconnected_keyed_charge() {
    let fixture = ordinary_capacity_walk_fixture();
    let mut charges = fixture.retained_charges.clone();
    charges[2] = RetainedRecordCharge::new(
        92,
        fixture.retained_records[2].admission_order,
        ResourceVector::new(1, 4 * UNIT),
    );
    let expected_frontiers = ordinary_capacity_walk_frontiers(&fixture);
    let input = OrdinaryRecordProjectionInput::new(
        fixture.request.clone(),
        fixture.receiving_binding_epoch,
        fixture.encoded_record_charge,
        charges,
        fixture.observer_progress,
        fixture.closure_accounting,
        fixture.limits,
    );
    let expected_input = input.clone();
    let Err(failure) = ordinary_capacity_walk_frontiers(&fixture).project_ordinary_record(input)
    else {
        panic!("disconnected charge must fail");
    };
    assert_eq!(
        failure.error(),
        &OrdinaryProjectionError::RetainedChargeKey { index: 2 }
    );
    assert_eq!(failure.frontiers(), &expected_frontiers);
    assert_eq!(failure.projection_input(), &expected_input);
}

#[test]
fn consuming_wrapper_releases_compacted_credit_then_reovertakes_same_participant() {
    let marker = marker_record(1, 0, 0);
    let ordinary = ordinary_record(2, 1, 0);
    let records = vec![marker, ordinary];
    let charges = keyed_charges(&records, &[UNIT, UNIT]);
    let binding_epoch = epoch(7);
    let projected = credited_marker_frontiers(records, binding_epoch)
        .project_ordinary_record(OrdinaryRecordProjectionInput::new(
            request(),
            binding_epoch,
            ResourceVector::new(1, UNIT),
            charges,
            2,
            accounting(
                1,
                0,
                WideResourceVector::new(2, u128::from(2 * UNIT)),
                ResourceVector::new(6, 6 * UNIT),
            ),
            OrdinaryProjectionLimits::new(
                ResourceVector::new(1, UNIT),
                ResourceVector::new(2, 2 * UNIT),
                ResourceVector::new(2, 2 * UNIT),
            ),
        ))
        .expect("credit release and re-overtake are one legal fixed point");
    let OrdinaryRecordProjectionDecision::Projected(projected) = projected else {
        panic!("credited marker fixture has no earlier prefix")
    };

    assert_eq!(projected.floor().resulting_floor, 3);
    assert_eq!(
        projected
            .retained_records()
            .iter()
            .map(|record| record.delivery_seq)
            .collect::<Vec<_>>(),
        vec![3]
    );
    assert!(projected.frontiers().retained_marker_records().is_empty());
    assert_eq!(projected.new_marker_candidates().len(), 1);
    assert_eq!(projected.new_marker_candidates()[0].delivery_seq, 4);
    assert_eq!(
        projected.new_marker_candidates()[0]
            .admission_order
            .participant_index(),
        0
    );
    assert_eq!(projected.accounting().marker_capacity_credits(), 1);
    assert_eq!(projected.accounting().marker_anchors(), 1);
    assert_eq!(
        projected
            .frontiers()
            .sequence()
            .movable_claims()
            .iter()
            .map(|claim| claim.delivery_seq)
            .collect::<Vec<_>>(),
        vec![5, 6]
    );
    assert_eq!(
        projected
            .frontiers()
            .sequence()
            .products()
            .live_times_terminal()[0]
            .start(),
        7
    );
    assert!(projected.frontiers().cross_counter_valid_for_test());
}

#[test]
fn ordinary_capacity_walk_derives_floor_marker_and_both_ledgers() {
    let OrdinaryProjectionKernelDecision::Projected(projected) = ordinary_capacity_walk_fixture()
        .project()
        .expect("ordinary capacity-walk fixed point is legal")
    else {
        panic!("ordinary capacity walk has no earlier mandatory prefix")
    };

    assert_eq!(projected.floor().preferred_floor, 89);
    assert_eq!(projected.floor().resulting_floor, 91);
    assert_eq!(
        projected.retained_charge(),
        WideResourceVector::new(6, u128::from(24 * UNIT))
    );
    assert_eq!(
        projected.baseline(),
        WideResourceVector::new(6, u128::from(24 * UNIT))
    );
    assert_eq!(
        projected.required_capacity().maximum(),
        WideResourceVector::new(10, u128::from(40 * UNIT))
    );
    assert_eq!(projected.order().major(), 86);
    assert_eq!(projected.sequence().resulting().high_watermark(), 95);
    assert_eq!(projected.sequence().resulting().claims().markers(), 1);
    assert_eq!(projected.caller_record().delivery_seq, 95);
    assert_eq!(
        projected.caller_charge().encoded_charge(),
        ResourceVector::new(1, 40)
    );
    assert_eq!(
        projected
            .retained_records()
            .iter()
            .map(|record| record.delivery_seq)
            .collect::<Vec<_>>(),
        vec![91, 92, 93, 94, 95]
    );
    assert_eq!(projected.retained_charges().len(), 5);

    let marker = projected.marker_candidates()[0];
    assert_eq!(marker.delivery_seq, 96);
    assert_eq!(marker.admission_order.transaction_order(), 86);
    assert_eq!(
        marker.admission_order.candidate_phase(),
        CandidatePhase::CompactionMarker
    );
    assert_eq!(marker.admission_order.participant_index(), 0);
    assert_eq!(marker.target_binding, FrontierBinding::Bound(epoch(7)));
    assert_eq!(marker.provenance, MarkerProvenance::NonProductM);
    assert_eq!(marker.current_owner, MarkerSequenceOwner::Marker);
    assert_eq!(
        projected.resulting_accounting().marker_capacity_credits(),
        1
    );
    assert_eq!(projected.resulting_accounting().marker_anchors(), 1);
    assert_eq!(
        projected.resulting_accounting().state(),
        ClosureState::Clear
    );
}

#[test]
fn two_overtaken_participants_receive_ascending_exact_marker_candidates() {
    let records = vec![ordinary_record(1, 0, 0), ordinary_record(2, 1, 0)];
    let cap = ResourceVector::new(8, 8 * UNIT);
    let fixture = Fixture {
        request: request(),
        receiving_binding_epoch: epoch(7),
        encoded_record_charge: ResourceVector::new(1, UNIT),
        retained_charges: keyed_charges(&records, &[UNIT, UNIT]),
        retained_records: records,
        active_marker_credit_records: Vec::new(),
        unaccepted_marker_anchors: Vec::new(),
        active_identities: vec![
            FrontierParticipant::new(0, 0, FrontierBinding::Bound(epoch(7))),
            FrontierParticipant::new(1, 0, FrontierBinding::Detached(epoch(9))),
        ],
        identity_slot_limit: 2,
        current_floor: 1,
        observer_progress: 2,
        order_ledger: order_ledger(1, 1, 2),
        sequence_ledger: sequence_ledger(2, 2, 1, 0),
        immutable_candidates: Vec::new(),
        closure_accounting: accounting(0, 0, WideResourceVector::new(4, u128::from(4 * UNIT)), cap),
        limits: OrdinaryProjectionLimits::new(
            ResourceVector::new(1, UNIT),
            ResourceVector::new(2, 2 * UNIT),
            ResourceVector::new(2, 2 * UNIT),
        ),
    };
    let OrdinaryProjectionKernelDecision::Projected(projected) = fixture
        .project()
        .expect("one removal closes the two-participant fixed point")
    else {
        panic!("no mandatory prefix exists")
    };

    assert_eq!(projected.floor().resulting_floor, 2);
    assert_eq!(
        projected
            .marker_candidates()
            .iter()
            .map(|marker| (
                marker.admission_order.participant_index(),
                marker.delivery_seq,
                marker.target_binding,
            ))
            .collect::<Vec<_>>(),
        vec![
            (0, 4, FrontierBinding::Bound(epoch(7))),
            (1, 5, FrontierBinding::Detached(epoch(9))),
        ]
    );
    assert_eq!(projected.sequence().resulting().claims().markers(), 2);
}

#[test]
fn uncredited_historical_marker_contributes_its_exact_charge_not_marker_max() {
    let marker = marker_record(1, 0, 0);
    let cap = ResourceVector::new(10, 20 * UNIT);
    let fixture = Fixture {
        request: request(),
        receiving_binding_epoch: epoch(7),
        encoded_record_charge: ResourceVector::new(1, UNIT),
        retained_records: vec![marker],
        retained_charges: keyed_charges(&[marker], &[3 * UNIT]),
        active_marker_credit_records: Vec::new(),
        unaccepted_marker_anchors: Vec::new(),
        active_identities: vec![FrontierParticipant::new(
            0,
            1,
            FrontierBinding::Bound(epoch(7)),
        )],
        identity_slot_limit: 1,
        current_floor: 1,
        observer_progress: 0,
        order_ledger: order_ledger(0, 1, 1),
        sequence_ledger: sequence_ledger(1, 1, 1, 0),
        immutable_candidates: Vec::new(),
        closure_accounting: accounting(0, 0, WideResourceVector::new(2, u128::from(7 * UNIT)), cap),
        limits: OrdinaryProjectionLimits::new(
            ResourceVector::new(1, 4 * UNIT),
            ResourceVector::new(2, 2 * UNIT),
            ResourceVector::new(2, 2 * UNIT),
        ),
    };
    let OrdinaryProjectionKernelDecision::Projected(projected) = fixture
        .project()
        .expect("historical marker charge remains physical occupancy")
    else {
        panic!("no mandatory prefix exists")
    };
    assert_eq!(
        projected.baseline(),
        WideResourceVector::new(3, u128::from(8 * UNIT))
    );
    assert!(projected.marker_candidates().is_empty());
}

#[test]
fn current_credited_marker_uses_marker_max_and_keeps_accepted_credit() {
    let marker = marker_record(1, 0, 0);
    let cap = ResourceVector::new(10, 20 * UNIT);
    let fixture = Fixture {
        request: request(),
        receiving_binding_epoch: epoch(7),
        encoded_record_charge: ResourceVector::new(1, UNIT),
        retained_records: vec![marker],
        retained_charges: keyed_charges(&[marker], &[3 * UNIT]),
        active_marker_credit_records: vec![marker],
        unaccepted_marker_anchors: Vec::new(),
        active_identities: vec![FrontierParticipant::new(
            0,
            1,
            FrontierBinding::Bound(epoch(7)),
        )],
        identity_slot_limit: 1,
        current_floor: 1,
        observer_progress: 0,
        order_ledger: order_ledger(0, 1, 1),
        sequence_ledger: sequence_ledger(1, 1, 1, 0),
        immutable_candidates: Vec::new(),
        closure_accounting: accounting(1, 0, WideResourceVector::new(1, u128::from(4 * UNIT)), cap),
        limits: OrdinaryProjectionLimits::new(
            ResourceVector::new(1, 4 * UNIT),
            ResourceVector::new(2, 2 * UNIT),
            ResourceVector::new(2, 2 * UNIT),
        ),
    };
    let OrdinaryProjectionKernelDecision::Projected(projected) = fixture
        .project()
        .expect("credited marker remains charged at marker_max")
    else {
        panic!("no mandatory prefix exists")
    };
    assert_eq!(
        projected.baseline(),
        WideResourceVector::new(2, u128::from(5 * UNIT))
    );
    assert_eq!(
        projected.resulting_accounting().marker_capacity_credits(),
        1
    );
    assert_eq!(projected.resulting_accounting().marker_anchors(), 0);
}

#[test]
fn compacted_accepted_marker_releases_credit_without_planning_a_replacement() {
    let marker = marker_record(1, 0, 0);
    let cap = ResourceVector::new(10, 10 * UNIT);
    let fixture = Fixture {
        request: request(),
        receiving_binding_epoch: epoch(7),
        encoded_record_charge: ResourceVector::new(1, UNIT),
        retained_records: vec![marker],
        retained_charges: keyed_charges(&[marker], &[UNIT]),
        active_marker_credit_records: vec![marker],
        unaccepted_marker_anchors: Vec::new(),
        active_identities: vec![FrontierParticipant::new(
            0,
            1,
            FrontierBinding::Bound(epoch(7)),
        )],
        identity_slot_limit: 1,
        current_floor: 1,
        observer_progress: 1,
        order_ledger: order_ledger(0, 1, 1),
        sequence_ledger: sequence_ledger(1, 1, 1, 0),
        immutable_candidates: Vec::new(),
        closure_accounting: accounting(1, 0, WideResourceVector::new(1, u128::from(UNIT)), cap),
        limits: OrdinaryProjectionLimits::new(
            ResourceVector::new(1, UNIT),
            ResourceVector::new(2, 2 * UNIT),
            ResourceVector::new(2, 2 * UNIT),
        ),
    };
    let OrdinaryProjectionKernelDecision::Projected(projected) = fixture
        .project()
        .expect("preferred floor compacts the accepted marker")
    else {
        panic!("no mandatory prefix exists")
    };

    assert_eq!(projected.floor().resulting_floor, 2);
    assert!(projected.marker_candidates().is_empty());
    assert_eq!(
        projected.resulting_accounting().marker_capacity_credits(),
        0
    );
    assert_eq!(projected.resulting_accounting().marker_anchors(), 0);
    assert_eq!(
        projected
            .retained_records()
            .iter()
            .map(|record| record.delivery_seq)
            .collect::<Vec<_>>(),
        vec![2]
    );
}

#[test]
fn compacted_credit_owner_can_receive_one_new_marker_in_the_same_fixed_point() {
    let marker = marker_record(1, 0, 0);
    let ordinary = ordinary_record(2, 1, 0);
    let records = vec![marker, ordinary];
    let cap = ResourceVector::new(6, 6 * UNIT);
    let fixture = Fixture {
        request: request(),
        receiving_binding_epoch: epoch(7),
        encoded_record_charge: ResourceVector::new(1, UNIT),
        retained_charges: keyed_charges(&records, &[UNIT, UNIT]),
        retained_records: records,
        active_marker_credit_records: vec![marker],
        unaccepted_marker_anchors: Vec::new(),
        active_identities: vec![FrontierParticipant::new(
            0,
            1,
            FrontierBinding::Bound(epoch(7)),
        )],
        identity_slot_limit: 1,
        current_floor: 1,
        observer_progress: 2,
        order_ledger: order_ledger(1, 1, 1),
        sequence_ledger: sequence_ledger(2, 1, 1, 0),
        immutable_candidates: Vec::new(),
        closure_accounting: accounting(1, 0, WideResourceVector::new(2, u128::from(2 * UNIT)), cap),
        limits: OrdinaryProjectionLimits::new(
            ResourceVector::new(1, UNIT),
            ResourceVector::new(2, 2 * UNIT),
            ResourceVector::new(2, 2 * UNIT),
        ),
    };
    let OrdinaryProjectionKernelDecision::Projected(projected) = fixture
        .project()
        .expect("removal releases then deterministically replaces the credit")
    else {
        panic!("no mandatory prefix exists")
    };

    assert_eq!(projected.floor().resulting_floor, 3);
    assert_eq!(projected.marker_candidates().len(), 1);
    let replacement = projected.marker_candidates()[0];
    assert_eq!(replacement.admission_order.participant_index(), 0);
    assert_eq!(replacement.delivery_seq, 4);
    assert_eq!(replacement.provenance, MarkerProvenance::NonProductM);
    assert_eq!(
        projected.resulting_accounting().marker_capacity_credits(),
        1
    );
    assert_eq!(projected.resulting_accounting().marker_anchors(), 1);
    assert_eq!(projected.sequence().resulting().claims().markers(), 1);
}

#[test]
fn hard_observer_blocks_the_exact_capacity_walk_floor() {
    let mut fixture = ordinary_capacity_walk_fixture();
    fixture.observer_progress = 89;
    assert_eq!(
        fixture.project(),
        Err(OrdinaryProjectionError::ObserverBackpressure {
            cap_floor: 91,
            observer_progress: 89,
        })
    );
}

#[test]
fn unaccepted_marker_prevents_capacity_search_crossing_its_sequence() {
    let records = vec![
        ordinary_record(1, 0, 0),
        marker_record(2, 1, 0),
        marker_record(3, 2, 0),
    ];
    let cap = ResourceVector::new(20, 12 * UNIT);
    let fixture = Fixture {
        request: request(),
        receiving_binding_epoch: epoch(7),
        encoded_record_charge: ResourceVector::new(1, 6 * UNIT),
        retained_charges: keyed_charges(&records, &[3 * UNIT, 3 * UNIT, 3 * UNIT]),
        retained_records: records.clone(),
        active_marker_credit_records: vec![records[1]],
        unaccepted_marker_anchors: vec![2],
        active_identities: vec![FrontierParticipant::new(
            0,
            0,
            FrontierBinding::Bound(epoch(7)),
        )],
        identity_slot_limit: 1,
        current_floor: 1,
        observer_progress: 3,
        order_ledger: order_ledger(2, 1, 1),
        sequence_ledger: sequence_ledger(3, 1, 1, 0),
        immutable_candidates: Vec::new(),
        closure_accounting: accounting(
            1,
            1,
            WideResourceVector::new(3, u128::from(10 * UNIT)),
            cap,
        ),
        limits: OrdinaryProjectionLimits::new(
            ResourceVector::new(1, 4 * UNIT),
            ResourceVector::new(2, UNIT),
            ResourceVector::new(2, UNIT),
        ),
    };
    assert_eq!(
        fixture.project(),
        Err(OrdinaryProjectionError::MarkerAnchorCapacity {
            marker_delivery_seq: 2,
            required: WideResourceVector::new(7, u128::from(15 * UNIT)),
            limit: cap,
        })
    );
}

#[test]
fn mandatory_candidate_wins_before_optional_projection_or_counter_planning() {
    let mut fixture = ordinary_capacity_walk_fixture();
    let candidate = MarkerCandidateAuthority {
        delivery_seq: 95,
        admission_order: AdmissionOrder::new(80, CandidatePhase::CompactionMarker, 0),
        target_binding: FrontierBinding::Bound(epoch(7)),
        provenance: MarkerProvenance::NonProductM,
        current_owner: MarkerSequenceOwner::Marker,
    };
    fixture.immutable_candidates = vec![ImmutableSequenceCandidate::Marker(candidate)];
    fixture.order_ledger = order_ledger(u64::MAX - 2, 1, 1);
    let OrdinaryProjectionKernelDecision::DrainFirst(authority) = fixture
        .project()
        .expect("mandatory prefix is durable progress")
    else {
        panic!("the candidate must win before exhausted caller order")
    };
    assert_eq!(
        authority.candidate(),
        ImmutableSequenceCandidate::Marker(candidate)
    );
}

#[test]
fn keyed_charge_mismatch_is_rejected_before_any_derived_total_is_used() {
    let mut fixture = ordinary_capacity_walk_fixture();
    fixture.retained_charges[2] = RetainedRecordCharge::new(
        92,
        fixture.retained_records[2].admission_order,
        ResourceVector::new(1, 4 * UNIT),
    );
    assert_eq!(
        fixture.project(),
        Err(OrdinaryProjectionError::RetainedChargeKey { index: 2 })
    );
}
