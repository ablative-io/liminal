#![allow(clippy::expect_used)]

use super::{
    ResourceDimension, ResourceVector, WideResourceVector, mandatory_capacity, no_edge_legal,
    recovery_transfer, retained_baseline, zero_debt_admission, zero_debt_capacity_failure,
};

const U: u64 = 7;
const MARKER_MAX: ResourceVector = ResourceVector::new(1, 4 * U);
const Q: ResourceVector = ResourceVector::new(2, 8 * U);
const K: ResourceVector = ResourceVector::new(2, 8 * U);

#[test]
fn baseline_and_zero_debt_admission_match_case_21() {
    let baseline = retained_baseline(ResourceVector::new(0, 0), 1, 0, MARKER_MAX)
        .expect("the fixture satisfies C <= I");

    assert_eq!(baseline, WideResourceVector::new(1, 4 * u128::from(U)));
    assert!(zero_debt_admission(
        baseline,
        Q,
        K,
        ResourceVector::new(16, 64 * U),
    ));
}

#[test]
fn startup_equality_enrollment_borrows_its_actual_charge() {
    let result = mandatory_capacity(
        ResourceVector::new(2, 8 * U),
        Q,
        K,
        ResourceVector::new(5, 20 * U),
    );

    assert_eq!(result.debt, WideResourceVector::new(1, 4 * u128::from(U)));
    assert!(result.absolute_fit);
    assert!(result.debt_within_mandatory_bound);
    assert!(result.is_legal());
}

#[test]
fn marker_credit_keeps_the_baseline_constant() {
    let before = retained_baseline(ResourceVector::new(2, 8 * U), 1, 0, MARKER_MAX)
        .expect("the fixture satisfies C <= I");
    let after = retained_baseline(ResourceVector::new(3, 12 * U), 1, 1, MARKER_MAX)
        .expect("the fixture satisfies C <= I");

    assert_eq!(before, WideResourceVector::new(3, 12 * u128::from(U)));
    assert_eq!(after, before);
}

#[test]
fn exact_q_supersession_reaches_the_mandatory_bound() {
    let result = mandatory_capacity(
        ResourceVector::new(10, 40 * U),
        Q,
        K,
        ResourceVector::new(12, 48 * U),
    );

    assert_eq!(result.debt, Q.widen());
    assert!(result.is_legal());
}

#[test]
fn required_capacity_uses_entry_before_byte_precedence() {
    let baseline = ResourceVector::new(5, 20 * U).widen();

    assert_eq!(
        zero_debt_capacity_failure(baseline, Q, K, ResourceVector::new(7, 36 * U)),
        Some(ResourceDimension::Entries)
    );
    assert_eq!(
        zero_debt_capacity_failure(baseline, Q, K, ResourceVector::new(9, 28 * U)),
        Some(ResourceDimension::Bytes)
    );
    assert_eq!(
        zero_debt_capacity_failure(baseline, Q, K, ResourceVector::new(7, 28 * U)),
        Some(ResourceDimension::Entries)
    );
}

#[test]
fn byte_budget_walk_preserves_the_printed_totals() {
    let baseline = retained_baseline(ResourceVector::new(6, 28 * U), 1, 0, MARKER_MAX)
        .expect("the fixture satisfies C <= I");
    assert_eq!(baseline, WideResourceVector::new(7, 32 * u128::from(U)));
    assert!(zero_debt_admission(
        baseline,
        Q,
        K,
        ResourceVector::new(11, 48 * U),
    ));

    let after_second_removal = ResourceVector::new(6, 24 * U).widen();
    assert!(zero_debt_admission(
        after_second_removal,
        Q,
        K,
        ResourceVector::new(10, 40 * U),
    ));
}

#[test]
fn recovery_charge_moves_from_k_to_baseline_once() {
    let transfer = recovery_transfer(
        ResourceVector::new(4, 16 * U),
        K,
        ResourceVector::new(1, 4 * U),
    )
    .expect("the charge is backed by K");

    assert_eq!(
        transfer.baseline,
        WideResourceVector::new(5, 20 * u128::from(U))
    );
    assert_eq!(
        transfer.remaining_recovery_claim,
        ResourceVector::new(1, 4 * U)
    );
    assert!(no_edge_legal(
        WideResourceVector::new(0, 0),
        ResourceVector::new(5, 20 * U).widen(),
        Q,
        K,
        ResourceVector::new(9, 36 * U),
    ));
}

#[test]
fn baseline_rejects_more_credits_than_identities() {
    assert!(retained_baseline(ResourceVector::new(0, 0), 1, 2, MARKER_MAX).is_err());
}
