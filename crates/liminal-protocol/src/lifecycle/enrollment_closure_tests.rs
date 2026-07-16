#![allow(clippy::expect_used)]

use crate::algebra::{ResourceVector, WideResourceVector};
use crate::wire::{
    BindingEpoch, ClosureCheckedEnvelope, ConnectionIncarnation, EnrollmentEnvelope,
    EnrollmentToken, Generation, OrderAllocatingEnvelope, SequenceAllocatingEnvelope,
};

use super::admission::{
    OrderClaims, OrderHigh, OrderLedger, RecoverySequenceReserve, SequenceClaims, SequenceLedger,
    admit_sequence, allocate_order,
};
use super::{
    ClosureAccounting, ClosureState, InitialEnrollmentClosureError, InitialEnrollmentClosureInput,
    ObserverProjection, RecoveryQuartetStatus, RemainingClosureDecision, StoredEdge,
    project_initial_enrollment_closure,
};

const MARKER: ResourceVector = ResourceVector::new(1, 4);
const Q: ResourceVector = ResourceVector::new(2, 8);
const STARTUP_CAP: ResourceVector = ResourceVector::new(5, 20);

fn binding_epoch(generation: Generation) -> BindingEpoch {
    BindingEpoch::new(ConnectionIncarnation::new(7, 9), generation)
}

fn accounting(cap: ResourceVector, churn_limit: u64) -> ClosureAccounting {
    ClosureAccounting::try_new(
        ClosureState::Clear,
        0,
        0,
        0,
        0,
        ResourceVector::default(),
        WideResourceVector::new(1, 4),
        cap,
        0,
        churn_limit,
    )
    .expect("initial accounting is valid")
}

fn input(cap: ResourceVector) -> InitialEnrollmentClosureInput {
    InitialEnrollmentClosureInput::new(
        accounting(cap, 2),
        1,
        Q,
        Q,
        MARKER,
        ResourceVector::new(1, 4),
        0,
        binding_epoch(Generation::ONE),
        OrderLedger::try_new(OrderHigh::Empty, OrderClaims::default())
            .expect("empty order ledger is valid"),
        SequenceLedger::try_new(0, SequenceClaims::default())
            .expect("empty sequence ledger is valid"),
        1,
        0,
    )
}

fn enrollment_envelope() -> EnrollmentEnvelope {
    EnrollmentEnvelope {
        conversation_id: 25,
        enrollment_token: EnrollmentToken::new([0x25; 16]),
    }
}

#[test]
fn case_25_startup_equality_begins_debt_without_a_recovery_quartet() {
    let projection = project_initial_enrollment_closure(input(STARTUP_CAP))
        .expect("the frozen startup-equality enrollment is legal");

    assert_eq!(
        projection.resulting_retained_charge(),
        ResourceVector::new(1, 4)
    );
    assert_eq!(projection.resulting_floor(), 1);
    assert_eq!(
        projection.resulting_baseline(),
        WideResourceVector::new(2, 8)
    );
    assert_eq!(projection.debt(), WideResourceVector::new(1, 4));
    assert_eq!(projection.remaining_recovery_claim(), Q);
    assert_eq!(projection.recovery_quartet(), RecoveryQuartetStatus::None);
    assert!(projection.new_marker_candidates().is_empty());
    assert_eq!(
        projection.required_capacity().maximum(),
        WideResourceVector::new(5, 20)
    );
    assert!(matches!(
        projection.resulting_closure_state(),
        ClosureState::Owed {
            edge: StoredEdge::ObserverProjection(edge),
            ..
        } if edge == ObserverProjection::new(1)
    ));
    let resulting_accounting = projection.resulting_closure_accounting();
    assert_eq!(resulting_accounting.edge_k_remaining(), Q);
    assert_eq!(resulting_accounting.edge_sequence_claims(), 0);
    assert_eq!(resulting_accounting.edge_order_position_claims(), 0);

    let order = allocate_order(
        OrderAllocatingEnvelope::Enrollment(enrollment_envelope()),
        OrderLedger::try_new(OrderHigh::Empty, OrderClaims::default())
            .expect("empty order ledger is valid"),
        projection.plan_order().expect("A/X additions fit"),
    )
    .expect("caller major zero and A/X claims fit")
    .resulting();
    assert_eq!(order.high(), OrderHigh::Allocated(0));
    assert_eq!(order.claims().active_binding_terminals(), 1);
    assert_eq!(order.claims().membership_exits(), 1);
    assert!(!order.claims().recovery_operation());
    assert!(!order.claims().recovery_replacement_terminal());

    let sequence = admit_sequence(
        SequenceAllocatingEnvelope::Enrollment(enrollment_envelope()),
        projection.plan_sequence().expect("enrollment claims fit"),
    )
    .expect("the canonical three-value suffix reserve fits")
    .resulting();
    assert_eq!(sequence.high_watermark(), 1);
    assert_eq!(sequence.claims().live_members(), 1);
    assert_eq!(sequence.claims().binding_terminals(), 1);
    assert_eq!(sequence.claims().markers(), 0);
    assert_eq!(sequence.claims().recovery(), RecoverySequenceReserve::None);
    assert_eq!(sequence.required_reserve(), 3);

    assert!(matches!(
        projection
            .remaining_closure_decision(&ClosureCheckedEnvelope::Enrollment(enrollment_envelope())),
        RemainingClosureDecision::Eligible(_)
    ));
}

#[test]
fn ample_headroom_keeps_initial_enrollment_clear_and_marker_free() {
    let projection = project_initial_enrollment_closure(input(ResourceVector::new(6, 24)))
        .expect("full post-enrollment envelope fits");

    assert_eq!(projection.resulting_closure_state(), ClosureState::Clear);
    assert_eq!(projection.debt(), WideResourceVector::default());
    assert_eq!(
        projection.remaining_recovery_claim(),
        ResourceVector::default()
    );
    assert_eq!(projection.recovery_quartet(), RecoveryQuartetStatus::None);
    assert!(projection.new_marker_candidates().is_empty());
    assert_eq!(
        projection.required_capacity().maximum(),
        WideResourceVector::new(6, 24)
    );
}

#[test]
fn initial_allocator_binding_and_attached_shape_are_checked() {
    let bad_index = InitialEnrollmentClosureInput::new(
        ClosureAccounting::try_new(
            ClosureState::Clear,
            0,
            0,
            0,
            0,
            ResourceVector::default(),
            WideResourceVector::new(2, 8),
            ResourceVector::new(8, 32),
            0,
            2,
        )
        .expect("two-slot initial accounting is valid"),
        2,
        Q,
        Q,
        MARKER,
        ResourceVector::new(1, 4),
        1,
        binding_epoch(Generation::ONE),
        OrderLedger::try_new(OrderHigh::Empty, OrderClaims::default())
            .expect("empty order ledger is valid"),
        SequenceLedger::try_new(0, SequenceClaims::default())
            .expect("empty sequence ledger is valid"),
        1,
        0,
    );
    assert_eq!(
        project_initial_enrollment_closure(bad_index),
        Err(
            InitialEnrollmentClosureError::InitialParticipantIndexNotZero {
                participant_index: 1,
            }
        )
    );

    let bad_generation = input(STARTUP_CAP)
        .with_binding_epoch(binding_epoch(Generation::new(2).expect("two is nonzero")));
    assert!(matches!(
        project_initial_enrollment_closure(bad_generation),
        Err(InitialEnrollmentClosureError::BindingGeneration { .. })
    ));

    for actual in [0, 2] {
        let bad_charge = input(STARTUP_CAP).with_attached_charge(ResourceVector::new(actual, 4));
        assert_eq!(
            project_initial_enrollment_closure(bad_charge),
            Err(InitialEnrollmentClosureError::AttachedEntryCharge { actual })
        );
    }
}
