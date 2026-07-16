#![allow(clippy::expect_used, clippy::panic)]

use crate::wire::{
    AttachAttemptToken, AttachEnvelope, EnrollmentEnvelope, EnrollmentToken, Generation,
    RecordAdmissionEnvelope, SequenceAllocatingEnvelope, SequenceBudget,
};

use super::sequence::{
    RecoverySequenceReserve, ResultingSequenceState, SequenceAdmissionError, SequenceClaims,
    SequenceLedger, SequenceLedgerInvariantError, admit_sequence,
};

fn generation(value: u64) -> Generation {
    Generation::new(value).expect("test generation is nonzero")
}

fn enrollment(conversation_id: u64) -> SequenceAllocatingEnvelope {
    SequenceAllocatingEnvelope::Enrollment(EnrollmentEnvelope {
        conversation_id,
        enrollment_token: EnrollmentToken::new([0xE1; 16]),
    })
}

fn attach(conversation_id: u64) -> SequenceAllocatingEnvelope {
    SequenceAllocatingEnvelope::CredentialAttach(AttachEnvelope {
        conversation_id,
        participant_id: 0,
        capability_generation: generation(1),
        attach_attempt_token: AttachAttemptToken::new([0xA1; 16]),
        accept_marker_delivery_seq: None,
    })
}

fn record(conversation_id: u64) -> SequenceAllocatingEnvelope {
    SequenceAllocatingEnvelope::RecordAdmission(RecordAdmissionEnvelope {
        conversation_id,
        participant_id: 0,
        capability_generation: generation(1),
    })
}

fn restored(high_watermark: u64, claims: SequenceClaims) -> SequenceLedger {
    SequenceLedger::try_new(high_watermark, claims).expect("fixture ledger is valid")
}

fn admitted(
    request: SequenceAllocatingEnvelope,
    resulting: ResultingSequenceState,
) -> SequenceLedger {
    admit_sequence(request, resulting)
        .expect("fixture reserve fits")
        .resulting()
}

fn exhausted_budget(
    request: SequenceAllocatingEnvelope,
    resulting: ResultingSequenceState,
) -> SequenceBudget {
    match admit_sequence(request, resulting).expect_err("fixture exhausts the sequence suffix") {
        SequenceAdmissionError::Exhausted(exhausted) => exhausted.sequence_budget,
        error => panic!("expected wire exhaustion, got {error:?}"),
    }
}

#[test]
fn optional_planners_apply_exact_primitive_claim_deltas() {
    let current = restored(
        100,
        SequenceClaims::new(2, 2, 3, RecoverySequenceReserve::None),
    );

    let enrollment_result = admitted(
        enrollment(70_001),
        current
            .plan_enrollment(2)
            .expect("enrollment additions fit"),
    );
    assert_ledger(
        enrollment_result,
        101,
        3,
        3,
        5,
        RecoverySequenceReserve::None,
    );

    let detached_result = admitted(
        attach(70_002),
        current
            .plan_detached_attach(2)
            .expect("detached attach additions fit"),
    );
    assert_ledger(detached_result, 101, 2, 3, 5, RecoverySequenceReserve::None);

    let supersession_result = admitted(
        attach(70_003),
        current
            .plan_supersession(2)
            .expect("supersession additions fit"),
    );
    assert_ledger(
        supersession_result,
        102,
        2,
        2,
        5,
        RecoverySequenceReserve::None,
    );

    let ordinary_result = admitted(
        record(70_004),
        current
            .plan_ordinary_record(2)
            .expect("ordinary additions fit"),
    );
    assert_ledger(ordinary_result, 101, 2, 2, 5, RecoverySequenceReserve::None);
}

#[test]
fn optional_planners_preserve_an_existing_recovery_pair() {
    let current = restored(
        10,
        SequenceClaims::new(1, 1, 0, RecoverySequenceReserve::DetachedCredentialRecovery),
    );
    let result = admitted(
        record(71),
        current
            .plan_ordinary_record(1)
            .expect("ordinary marker addition fits"),
    );
    assert_ledger(
        result,
        11,
        1,
        1,
        1,
        RecoverySequenceReserve::DetachedCredentialRecovery,
    );
}

#[test]
fn checked_planners_report_counter_overflow_before_admission() {
    let high_max = restored(u64::MAX, SequenceClaims::default());
    assert_eq!(
        high_max.plan_ordinary_record(0),
        Err(SequenceAdmissionError::HighWatermarkOverflow {
            high_watermark: u64::MAX,
            required_values: 1,
        })
    );

    let one_value_left = restored(u64::MAX - 1, SequenceClaims::default());
    assert_eq!(
        one_value_left.plan_supersession(0),
        Err(SequenceAdmissionError::HighWatermarkOverflow {
            high_watermark: u64::MAX - 1,
            required_values: 2,
        })
    );

    let terminal_max = restored(
        0,
        SequenceClaims::new(0, u64::MAX, 0, RecoverySequenceReserve::None),
    );
    assert_eq!(
        terminal_max.plan_detached_attach(0),
        Err(SequenceAdmissionError::BindingTerminalClaimOverflow {
            binding_terminals: u64::MAX,
        })
    );

    let marker_max = restored(
        0,
        SequenceClaims::new(0, 0, u64::MAX, RecoverySequenceReserve::None),
    );
    assert_eq!(
        marker_max.plan_ordinary_record(1),
        Err(SequenceAdmissionError::MarkerClaimOverflow {
            markers: u64::MAX,
            new_markers: 1,
        })
    );
}

#[test]
fn fenced_recovery_consumes_rs_and_transfers_rt_into_t() {
    let current = restored(
        10,
        SequenceClaims::new(1, 0, 2, RecoverySequenceReserve::DetachedCredentialRecovery),
    );
    let resulting = current
        .apply_fenced_recovery()
        .expect("coupled sequence reserve transfers exactly");

    assert_ledger(resulting, 11, 1, 1, 2, RecoverySequenceReserve::None);
    assert_eq!(current.required_reserve(), 6);
    assert_eq!(resulting.required_reserve(), 5);
    assert_eq!(resulting.budget().rs, 0);
    assert_eq!(resulting.budget().rt, 0);
    assert_eq!(resulting.budget().l_times_rt, 0);
    assert_eq!(resulting.budget().l_times_t, 1);
}

#[test]
fn fenced_recovery_requires_the_coupled_sequence_pair() {
    let current = restored(
        10,
        SequenceClaims::new(1, 0, 0, RecoverySequenceReserve::None),
    );
    assert_eq!(
        current.apply_fenced_recovery(),
        Err(SequenceAdmissionError::RecoverySequenceReserveMissing)
    );
}

#[test]
fn closure_projection_endows_one_sequence_pair_and_rejects_a_second() {
    let clear = restored(0, SequenceClaims::default());
    let endowed = clear
        .plan_enrollment_with_recovery_quartet(0, true)
        .expect("clear state may receive its sole sequence pair");
    let resulting = admitted(enrollment(72), endowed);
    assert_ledger(
        resulting,
        1,
        1,
        1,
        0,
        RecoverySequenceReserve::DetachedCredentialRecovery,
    );

    assert_eq!(
        resulting.plan_enrollment_with_recovery_quartet(0, true),
        Err(SequenceAdmissionError::RecoverySequenceReserveAlreadyPresent)
    );
    let ordinary = admitted(
        record(73),
        resulting
            .plan_ordinary_record(0)
            .expect("ordinary planning preserves and never endows recovery"),
    );
    assert_ledger(
        ordinary,
        2,
        1,
        1,
        0,
        RecoverySequenceReserve::DetachedCredentialRecovery,
    );
}

#[test]
fn case_21_supersession_projects_the_exact_exhausted_budget() {
    let current = restored(
        u64::MAX - 4,
        SequenceClaims::new(1, 1, 0, RecoverySequenceReserve::None),
    );
    let resulting = current
        .plan_supersession(0)
        .expect("two resulting record values are representable");
    let budget = exhausted_budget(attach(21), resulting);

    assert_eq!(
        budget,
        SequenceBudget {
            high_watermark: u64::MAX - 2,
            remaining: 2,
            e: 1,
            t: 1,
            m: 0,
            rs: 0,
            rt: 0,
            l_times_t: 1,
            l_times_rt: 0,
            l_other_times_e: 0,
        }
    );
}

#[test]
fn case_26_exact_seventeen_value_suffix_is_admissible() {
    let high_watermark = u64::MAX - 17;
    let claims = SequenceClaims::new(3, 2, 0, RecoverySequenceReserve::None);
    let ledger = restored(high_watermark, claims);

    assert_eq!(ledger.required_reserve(), 17);
    assert_eq!(
        ledger.budget(),
        SequenceBudget {
            high_watermark,
            remaining: 17,
            e: 3,
            t: 2,
            m: 0,
            rs: 0,
            rt: 0,
            l_times_t: 6,
            l_times_rt: 0,
            l_other_times_e: 6,
        }
    );
}

#[test]
fn case_31_enrollment_and_record_fixed_points_both_exhaust() {
    let enrollment_current = restored(u64::MAX - 2, SequenceClaims::default());
    let enrollment_budget = exhausted_budget(
        enrollment(31_001),
        enrollment_current
            .plan_enrollment(1)
            .expect("enrollment counters fit before reserve admission"),
    );

    let ordinary_current = restored(
        u64::MAX - 4,
        SequenceClaims::new(1, 1, 0, RecoverySequenceReserve::None),
    );
    let ordinary_budget = exhausted_budget(
        record(31_002),
        ordinary_current
            .plan_ordinary_record(1)
            .expect("ordinary counters fit before reserve admission"),
    );

    assert_eq!(enrollment_budget.remaining, 1);
    assert_eq!(ordinary_budget.remaining, 3);
    for budget in [enrollment_budget, ordinary_budget] {
        assert_eq!(budget.e, 1);
        assert_eq!(budget.t, 1);
        assert_eq!(budget.m, 1);
        assert_eq!(budget.l_times_t, 1);
    }
}

#[test]
fn case_47_derives_all_four_canonical_budgets() {
    let arm_a = restored(
        u64::MAX - 2,
        SequenceClaims::new(1, 0, 0, RecoverySequenceReserve::None),
    );
    let h = u64::MAX - 6;
    let arm_b = restored(
        h,
        SequenceClaims::new(1, 1, 0, RecoverySequenceReserve::DetachedCredentialRecovery),
    );
    let arm_c = restored(
        h + 1,
        SequenceClaims::new(1, 0, 1, RecoverySequenceReserve::None),
    );
    let arm_d = restored(
        u64::MAX - 3,
        SequenceClaims::new(1, 1, 0, RecoverySequenceReserve::None),
    );

    assert_eq!(
        arm_a.budget(),
        SequenceBudget {
            high_watermark: u64::MAX - 2,
            remaining: 2,
            e: 1,
            t: 0,
            m: 0,
            rs: 0,
            rt: 0,
            l_times_t: 0,
            l_times_rt: 0,
            l_other_times_e: 0,
        }
    );
    assert_eq!(
        arm_b.budget(),
        SequenceBudget {
            high_watermark: h,
            remaining: 6,
            e: 1,
            t: 1,
            m: 0,
            rs: 1,
            rt: 1,
            l_times_t: 1,
            l_times_rt: 1,
            l_other_times_e: 0,
        }
    );
    assert_eq!(arm_b.required_reserve(), 6);
    assert_eq!(
        arm_c.budget(),
        SequenceBudget {
            high_watermark: h + 1,
            remaining: 5,
            e: 1,
            t: 0,
            m: 1,
            rs: 0,
            rt: 0,
            l_times_t: 0,
            l_times_rt: 0,
            l_other_times_e: 0,
        }
    );
    assert_eq!(
        arm_d.budget(),
        SequenceBudget {
            high_watermark: u64::MAX - 3,
            remaining: 3,
            e: 1,
            t: 1,
            m: 0,
            rs: 0,
            rt: 0,
            l_times_t: 1,
            l_times_rt: 0,
            l_other_times_e: 0,
        }
    );
}

#[test]
fn case_54_shutdown_reserve_is_derived_as_six() {
    let ledger = restored(
        4,
        SequenceClaims::new(2, 0, 2, RecoverySequenceReserve::None),
    );

    assert_eq!(ledger.required_reserve(), 6);
    assert_eq!(
        ledger.budget(),
        SequenceBudget {
            high_watermark: 4,
            remaining: u64::MAX - 4,
            e: 2,
            t: 0,
            m: 2,
            rs: 0,
            rt: 0,
            l_times_t: 0,
            l_times_rt: 0,
            l_other_times_e: 2,
        }
    );
}

#[test]
fn maximum_boundary_allows_only_a_zero_resulting_reserve() {
    let clear_current = restored(u64::MAX - 1, SequenceClaims::default());
    let clear = admitted(
        record(60),
        clear_current
            .plan_ordinary_record(0)
            .expect("final value is representable"),
    );
    assert_eq!(clear.high_watermark(), u64::MAX);
    assert_eq!(clear.required_reserve(), 0);

    let one_exit = SequenceClaims::new(1, 0, 0, RecoverySequenceReserve::None);
    let claimed_current = restored(u64::MAX - 1, one_exit);
    let budget = exhausted_budget(
        record(61),
        claimed_current
            .plan_ordinary_record(0)
            .expect("final value is representable"),
    );
    assert_eq!(budget.remaining, 0);
    assert_eq!(budget.e, 1);

    let equality_current = restored(u64::MAX - 2, one_exit);
    let equality = admitted(
        record(62),
        equality_current
            .plan_ordinary_record(0)
            .expect("penultimate value is representable"),
    );
    assert_eq!(equality.required_reserve(), 1);
    assert_eq!(equality.budget().remaining, 1);
}

#[test]
fn restore_rejects_an_unowned_suffix_and_reports_derived_budget() {
    let error = SequenceLedger::try_new(
        u64::MAX,
        SequenceClaims::new(1, 0, 0, RecoverySequenceReserve::None),
    )
    .expect_err("one exit claim cannot fit after MAX");

    let SequenceLedgerInvariantError::ClaimsExceedRemaining {
        budget,
        required_reserve,
    } = error;
    assert_eq!(budget.remaining, 0);
    assert_eq!(budget.e, 1);
    assert_eq!(required_reserve, Some(1));
}

#[test]
fn checked_wide_derived_sum_never_wraps() {
    let claims = SequenceClaims::new(
        u64::MAX,
        u64::MAX,
        u64::MAX,
        RecoverySequenceReserve::DetachedCredentialRecovery,
    );
    assert_eq!(claims.checked_required_reserve(), None);
    let budget = claims.budget(0);
    assert_eq!(budget.e, u64::MAX);
    assert_eq!(budget.rs, 1);
    assert_eq!(budget.rt, 1);
    assert_eq!(
        budget.l_times_t,
        u128::from(u64::MAX) * u128::from(u64::MAX)
    );
}

fn assert_ledger(
    ledger: SequenceLedger,
    high_watermark: u64,
    live_members: u64,
    binding_terminals: u64,
    markers: u64,
    recovery: RecoverySequenceReserve,
) {
    let claims = ledger.claims();
    assert_eq!(ledger.high_watermark(), high_watermark);
    assert_eq!(claims.live_members(), live_members);
    assert_eq!(claims.binding_terminals(), binding_terminals);
    assert_eq!(claims.markers(), markers);
    assert_eq!(claims.recovery(), recovery);
}
