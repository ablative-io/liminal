#![allow(clippy::expect_used)]

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

fn admitted(
    request: SequenceAllocatingEnvelope,
    high_watermark: u64,
    claims: SequenceClaims,
) -> SequenceLedger {
    admit_sequence(
        request,
        ResultingSequenceState::from_parts(high_watermark, claims),
    )
    .expect("fixture reserve fits")
    .resulting()
}

fn exhausted_budget(
    request: SequenceAllocatingEnvelope,
    high_watermark: u64,
    claims: SequenceClaims,
) -> SequenceBudget {
    let error = admit_sequence(
        request,
        ResultingSequenceState::from_parts(high_watermark, claims),
    )
    .expect_err("fixture exhausts the sequence suffix");
    let SequenceAdmissionError::Exhausted(exhausted) = error;
    exhausted.sequence_budget
}

#[test]
fn case_21_supersession_projects_the_exact_exhausted_budget() {
    let claims = SequenceClaims::new(1, 1, 0, RecoverySequenceReserve::None);
    let budget = exhausted_budget(attach(21), u64::MAX - 2, claims);

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
    assert_eq!(claims.checked_required_reserve(), Some(3));
}

#[test]
fn case_26_exact_seventeen_value_suffix_is_admissible() {
    let high_watermark = u64::MAX - 17;
    let claims = SequenceClaims::new(3, 2, 0, RecoverySequenceReserve::None);
    let ledger = SequenceLedger::try_new(high_watermark, claims)
        .expect("all seventeen remaining values are owned exactly once");

    assert_eq!(claims.live_members(), 3);
    assert_eq!(claims.binding_terminals(), 2);
    assert_eq!(claims.markers(), 0);
    assert_eq!(claims.recovery(), RecoverySequenceReserve::None);
    assert_eq!(ledger.high_watermark(), high_watermark);
    assert_eq!(ledger.claims(), claims);
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
    let claims = SequenceClaims::new(1, 1, 1, RecoverySequenceReserve::None);
    let enrollment_budget = exhausted_budget(enrollment(31_001), u64::MAX - 1, claims);
    let record_budget = exhausted_budget(record(31_002), u64::MAX - 3, claims);

    assert_eq!(enrollment_budget.remaining, 1);
    assert_eq!(record_budget.remaining, 3);
    assert_eq!(enrollment_budget.e, 1);
    assert_eq!(enrollment_budget.t, 1);
    assert_eq!(enrollment_budget.m, 1);
    assert_eq!(enrollment_budget.l_times_t, 1);
    assert_eq!(record_budget.e, 1);
    assert_eq!(record_budget.t, 1);
    assert_eq!(record_budget.m, 1);
    assert_eq!(record_budget.l_times_t, 1);
    assert_eq!(claims.checked_required_reserve(), Some(4));
}

#[test]
fn case_47_derives_all_four_canonical_budgets() {
    let arm_a = admitted(
        record(47_001),
        u64::MAX - 2,
        SequenceClaims::new(1, 0, 0, RecoverySequenceReserve::None),
    );
    let h = u64::MAX - 6;
    let arm_b = admitted(
        attach(47_002),
        h,
        SequenceClaims::new(1, 1, 0, RecoverySequenceReserve::DetachedCredentialRecovery),
    );
    let arm_c = admitted(
        attach(47_003),
        h + 1,
        SequenceClaims::new(1, 0, 1, RecoverySequenceReserve::None),
    );
    let arm_d = admitted(
        record(47_004),
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
    let ledger = admitted(
        enrollment(54),
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
fn maximum_boundary_allows_only_a_zero_reserve() {
    let empty = admitted(record(60), u64::MAX, SequenceClaims::default());
    assert_eq!(empty.required_reserve(), 0);
    assert_eq!(empty.budget().remaining, 0);

    let one_exit = SequenceClaims::new(1, 0, 0, RecoverySequenceReserve::None);
    let budget = exhausted_budget(record(61), u64::MAX, one_exit);
    assert_eq!(budget.remaining, 0);
    assert_eq!(budget.e, 1);

    let equality = admitted(record(62), u64::MAX - 1, one_exit);
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
fn checked_wide_sum_overflow_is_an_exhaustion_not_a_wrap() {
    let claims = SequenceClaims::new(
        u64::MAX,
        u64::MAX,
        u64::MAX,
        RecoverySequenceReserve::DetachedCredentialRecovery,
    );
    assert_eq!(claims.checked_required_reserve(), None);

    let budget = exhausted_budget(record(63), 0, claims);
    assert_eq!(budget.e, u64::MAX);
    assert_eq!(budget.t, u64::MAX);
    assert_eq!(budget.m, u64::MAX);
    assert_eq!(budget.rs, 1);
    assert_eq!(budget.rt, 1);
    assert_eq!(
        budget.l_times_t,
        u128::from(u64::MAX) * u128::from(u64::MAX)
    );
}
