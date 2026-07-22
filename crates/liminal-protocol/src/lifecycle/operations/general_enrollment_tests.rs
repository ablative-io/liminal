#![allow(clippy::expect_used, clippy::panic)]

use alloc::{vec, vec::Vec};
use core::cell::Cell;

use crate::{
    algebra::{ResourceVector, WideResourceVector},
    outcome::CandidatePhase,
    wire::{
        AttachSecret, BindingEpoch, ConnectionIncarnation, EnrollmentRequest, EnrollmentToken,
        Generation, ServerValue,
    },
};

use super::super::{
    AdmissionOrder, BindingSlotOccupancy, BindingState, BindingTerminalOwner, CapacityCounter,
    ClaimFrontiers, ClaimFrontiersRestore, ClosureAccounting, ClosureState,
    ConnectionConversationTracking, EnrollmentCapacityCounters, EnrollmentFingerprint,
    EnrollmentTokenPhase, FreshParticipantCapacityCounter, FrontierBinding, FrontierParticipant,
    ImmutableOrderCandidateMajorRestore, ImmutableSequenceCandidate, LiveMember, LiveMemberRestore,
    MarkerCandidateAuthority, MarkerProvenance, MarkerSequenceOwner, MovableOrderClaim,
    MovableSequenceClaim, OrderClaimFrontierRestore, OrderClaims, OrderDirectOwner, OrderHigh,
    OrderLedger, OrdinaryProjectionLimits, RecoverySequenceReserve, ResolvedIdentity,
    RetainedCausalRecord, RetainedCausalRecordKind, RetainedRecordCharge,
    SequenceClaimFrontierRestore, SequenceClaims, SequenceDirectOwner, SequenceLedger,
    SequenceProductRangesRestore, TerminalProductRangeRestore,
};
use super::general_enrollment::{
    GeneralEnrollmentDecision, GeneralEnrollmentPrestate, GeneralEnrollmentProjectionInput,
    prepare_general_enrollment,
};

const CONVERSATION_ID: u64 = 54;

fn epoch(connection_ordinal: u64) -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(7, connection_ordinal),
        Generation::ONE,
    )
}

fn counter(limit: u64, occupied: u64) -> CapacityCounter {
    CapacityCounter::try_new(limit, occupied).expect("test counter is bounded")
}

fn fresh_counter(limit: u64) -> FreshParticipantCapacityCounter {
    FreshParticipantCapacityCounter::try_new(limit, 0).expect("new participant owns no rows")
}

fn enrollment_capacity(identity_server: CapacityCounter) -> EnrollmentCapacityCounters {
    EnrollmentCapacityCounters::new(
        identity_server,
        counter(4, 1),
        counter(4, 0),
        fresh_counter(4),
        counter(4, 0),
        counter(4, 0),
        fresh_counter(4),
    )
}

fn frontiers(with_candidate: bool) -> ClaimFrontiers {
    let binding_epoch = epoch(1);
    let terminal = BindingTerminalOwner {
        participant_index: 0,
        binding_epoch,
    };
    let attached_order = AdmissionOrder::new(0, CandidatePhase::AttachLifecycle, 0);
    let marker_order = AdmissionOrder::new(0, CandidatePhase::CompactionMarker, 0);
    let marker_count = u64::from(with_candidate);
    let sequence_ledger = SequenceLedger::try_new(
        1,
        SequenceClaims::new(1, 1, marker_count, RecoverySequenceReserve::None),
    )
    .expect("test sequence reserve fits");
    let order_ledger = OrderLedger::try_new(
        OrderHigh::Allocated(0),
        OrderClaims::new(1, 1, false, false).expect("no recovery pair"),
    )
    .expect("test order reserve fits");
    let (immutable_sequence, immutable_order, terminal_sequence, exit_sequence, product_sequence) =
        if with_candidate {
            (
                vec![ImmutableSequenceCandidate::Marker(
                    MarkerCandidateAuthority {
                        delivery_seq: 2,
                        admission_order: marker_order,
                        target_binding: FrontierBinding::Bound(binding_epoch),
                        provenance: MarkerProvenance::NonProductM,
                        current_owner: MarkerSequenceOwner::Marker,
                    },
                )],
                vec![ImmutableOrderCandidateMajorRestore {
                    transaction_order: 0,
                    candidate_keys: vec![marker_order],
                }],
                3,
                4,
                5,
            )
        } else {
            (vec![], vec![], 2, 3, 4)
        };
    ClaimFrontiers::restore(
        ClaimFrontiersRestore {
            conversation_id: CONVERSATION_ID,
            active_identities: vec![FrontierParticipant::new(
                0,
                0,
                FrontierBinding::Bound(binding_epoch),
            )],
            identity_slot_limit: 2,
            retained_floor: 1,
            retained_record_limit: 1,
            retained_records: vec![RetainedCausalRecord {
                delivery_seq: 1,
                admission_order: attached_order,
                kind: RetainedCausalRecordKind::AttachLifecycle {
                    participant_index: 0,
                    binding_epoch,
                },
            }],
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
                            participant_index: 0,
                        },
                    },
                ],
                immutable_candidates: immutable_sequence,
                products: SequenceProductRangesRestore {
                    live_times_terminal: vec![TerminalProductRangeRestore {
                        start: product_sequence,
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
                        transaction_order: 1,
                        owner: OrderDirectOwner::ActiveBindingTerminal(terminal),
                    },
                    MovableOrderClaim {
                        transaction_order: 2,
                        owner: OrderDirectOwner::MembershipExit {
                            participant_index: 0,
                        },
                    },
                ],
                immutable_candidates: immutable_order,
                recovery: None,
            },
            recovery_marker_delivery_seq: None,
        },
        sequence_ledger,
        order_ledger,
    )
    .expect("general-enrollment shell fixture restores")
}

fn projection_input() -> GeneralEnrollmentProjectionInput {
    GeneralEnrollmentProjectionInput::new(
        ResourceVector::new(1, 4),
        vec![RetainedRecordCharge::new(
            1,
            AdmissionOrder::new(0, CandidatePhase::AttachLifecycle, 0),
            ResourceVector::new(1, 4),
        )],
        1,
        ClosureAccounting::try_new(
            ClosureState::Clear,
            0,
            0,
            0,
            0,
            ResourceVector::default(),
            WideResourceVector::new(3, 12),
            ResourceVector::new(16, 64),
            0,
            2,
        )
        .expect("clear shell accounting is valid"),
        OrdinaryProjectionLimits::new(
            ResourceVector::new(1, 4),
            ResourceVector::new(2, 8),
            ResourceVector::new(2, 8),
        ),
    )
}

type TestPrestate<'a> = GeneralEnrollmentPrestate<'a, Vec<u8>, u64, Vec<u8>>;

fn prestate<'a>(
    binding: &'a BindingState,
    with_candidate: bool,
    tracking: ConnectionConversationTracking,
    connection_capacity: CapacityCounter,
    identity_server: CapacityCounter,
) -> TestPrestate<'a> {
    prestate_with(
        binding,
        EnrollmentTokenPhase::Unmapped,
        with_candidate,
        tracking,
        connection_capacity,
        BindingSlotOccupancy::Empty,
        enrollment_capacity(identity_server),
    )
}

#[allow(clippy::too_many_arguments)]
fn prestate_with<'a>(
    binding: &'a BindingState,
    token_phase: EnrollmentTokenPhase<'a, Vec<u8>, u64, Vec<u8>>,
    with_candidate: bool,
    tracking: ConnectionConversationTracking,
    connection_capacity: CapacityCounter,
    binding_occupancy: BindingSlotOccupancy,
    enrollment_capacity: EnrollmentCapacityCounters,
) -> TestPrestate<'a> {
    GeneralEnrollmentPrestate::new(
        EnrollmentRequest {
            conversation_id: CONVERSATION_ID,
            enrollment_token: EnrollmentToken::new([0x54; 16]),
        },
        token_phase,
        binding,
        tracking,
        connection_capacity,
        binding_occupancy,
        enrollment_capacity,
        frontiers(with_candidate),
        projection_input(),
    )
}

fn member() -> LiveMember<Vec<u8>> {
    LiveMember::restore(LiveMemberRestore {
        participant_id: 0,
        conversation_id: CONVERSATION_ID,
        generation: Generation::ONE,
        attach_secret: AttachSecret::new([0xA5; 32]),
        cursor: 0,
        enrollment_fingerprint: EnrollmentFingerprint::new(vec![5, 4]),
        latest_terminal: None,
    })
    .expect("lookup member is internally consistent")
}

#[test]
fn token_lookup_precedes_capacities_binding_and_candidate() {
    let binding = BindingState::Detached;
    let member = member();
    let decision = prepare_general_enrollment(prestate_with(
        &binding,
        EnrollmentTokenPhase::LifetimeMapping {
            identity: ResolvedIdentity::Live(&member),
        },
        true,
        ConnectionConversationTracking::Untracked,
        counter(1, 1),
        BindingSlotOccupancy::Occupied { participant_id: 99 },
        enrollment_capacity(counter(1, 1)),
    ));
    let GeneralEnrollmentDecision::Respond(refusal) = decision else {
        panic!("token replay must precede every fresh-enrollment selector");
    };
    assert!(matches!(
        refusal.response(),
        ServerValue::EnrollmentKnown(_)
    ));
    assert_eq!(
        refusal
            .prestate()
            .frontiers()
            .sequence()
            .immutable_candidates()
            .len(),
        1
    );
    assert_eq!(refusal.prestate().connection_capacity().occupied(), 1);
}

#[test]
fn fixed_capacity_and_binding_refusals_precede_candidate_in_order() {
    let binding = BindingState::Detached;
    let semantic = prepare_general_enrollment(prestate_with(
        &binding,
        EnrollmentTokenPhase::Unmapped,
        true,
        ConnectionConversationTracking::Untracked,
        counter(1, 1),
        BindingSlotOccupancy::Occupied { participant_id: 99 },
        enrollment_capacity(counter(1, 1)),
    ));
    let GeneralEnrollmentDecision::Respond(refusal) = semantic else {
        panic!("semantic connection capacity must precede binding and enrollment capacity");
    };
    assert!(matches!(
        refusal.response(),
        ServerValue::ConnectionConversationCapacityExceeded(_)
    ));

    let binding_slot = prepare_general_enrollment(prestate_with(
        &binding,
        EnrollmentTokenPhase::Unmapped,
        true,
        ConnectionConversationTracking::AlreadyTracked,
        counter(1, 1),
        BindingSlotOccupancy::Occupied { participant_id: 99 },
        enrollment_capacity(counter(1, 1)),
    ));
    let GeneralEnrollmentDecision::Respond(refusal) = binding_slot else {
        panic!("binding occupancy must precede enrollment capacity");
    };
    assert!(matches!(
        refusal.response(),
        ServerValue::ConnectionConversationBindingOccupied(_)
    ));

    let enrollment = prepare_general_enrollment(prestate_with(
        &binding,
        EnrollmentTokenPhase::Unmapped,
        true,
        ConnectionConversationTracking::AlreadyTracked,
        counter(1, 1),
        BindingSlotOccupancy::Empty,
        enrollment_capacity(counter(1, 1)),
    ));
    let GeneralEnrollmentDecision::Respond(refusal) = enrollment else {
        panic!("enrollment capacity must precede the immutable candidate");
    };
    assert!(matches!(
        refusal.response(),
        ServerValue::IdentityCapacityExceeded(_)
    ));
    assert_eq!(
        refusal
            .prestate()
            .frontiers()
            .sequence()
            .immutable_candidates()
            .len(),
        1
    );
}

#[test]
fn immutable_candidate_drains_before_allocator_or_mint_can_be_supplied() {
    let binding = BindingState::Detached;
    let allocations = Cell::new(0);
    let mints = Cell::new(0);
    let _allocator = || allocations.set(allocations.get() + 1);
    let _mint = || mints.set(mints.get() + 1);
    let decision = prepare_general_enrollment(prestate(
        &binding,
        true,
        ConnectionConversationTracking::AlreadyTracked,
        counter(4, 1),
        counter(4, 1),
    ));
    let GeneralEnrollmentDecision::DrainFirst(blocked) = decision else {
        panic!("the exact marker candidate must drain");
    };
    assert_eq!(blocked.candidate().delivery_seq(), 2);
    assert_eq!(
        blocked.candidate().admission_order(),
        AdmissionOrder::new(0, CandidatePhase::CompactionMarker, 0)
    );
    assert_eq!(allocations.get(), 0);
    assert_eq!(mints.get(), 0);
}

#[test]
fn candidate_free_preprojection_cannot_allocate_or_mint() {
    let binding = BindingState::Detached;
    let allocations = Cell::new(0);
    let mints = Cell::new(0);
    let _allocator = || allocations.set(allocations.get() + 1);
    let _mint = || mints.set(mints.get() + 1);
    let decision = prepare_general_enrollment(prestate(
        &binding,
        false,
        ConnectionConversationTracking::AlreadyTracked,
        counter(4, 1),
        counter(4, 1),
    ));
    let GeneralEnrollmentDecision::Project(project) = decision else {
        panic!("candidate-free shell must reach the consuming frontier hook");
    };
    assert_eq!(allocations.get(), 0);
    assert_eq!(mints.get(), 0);
    assert_eq!(
        project
            .prestate()
            .frontiers()
            .sequence()
            .ledger()
            .high_watermark(),
        1
    );
    assert_eq!(
        project
            .enrollment_capacity()
            .resulting()
            .identity_server()
            .occupied(),
        2
    );
}

#[test]
fn preprojection_crash_replay_is_deterministic() {
    let binding = BindingState::Detached;
    let run = || {
        let decision = prepare_general_enrollment(prestate(
            &binding,
            false,
            ConnectionConversationTracking::AlreadyTracked,
            counter(4, 1),
            counter(4, 1),
        ));
        let GeneralEnrollmentDecision::Project(project) = decision else {
            panic!("replay fixture must reach projection");
        };
        (
            project.prestate().frontiers().sequence().ledger(),
            project.prestate().projection().attached_charge(),
            project.connection_capacity().resulting(),
            project.enrollment_capacity().resulting(),
        )
    };
    assert_eq!(run(), run());
}
