use liminal_protocol::algebra::{
    ResourceDimension, ResourceVector, WideResourceVector, floor_transition, mandatory_capacity,
    no_edge_legal, recovery_transfer, retained_baseline, zero_debt_admission,
    zero_debt_capacity_failure,
};

type TestResult = Result<(), String>;

const ZERO_DEBT: WideResourceVector = WideResourceVector::new(0, 0);

fn baseline(
    retained_charge: ResourceVector,
    identity_slots: u64,
    marker_credits: u64,
    marker_max: ResourceVector,
) -> Result<WideResourceVector, String> {
    retained_baseline(retained_charge, identity_slots, marker_credits, marker_max)
        .map_err(|error| error.to_string())
}

/// Frozen `PARTICIPANT-CONTRACT.md` lines 3567-3600: both one-below
/// configuration arms, equality startup, enrollment debt, and no-edge return.
#[test]
fn startup_equality_checks_both_one_below_components() -> TestResult {
    const UNIT: u64 = 7;
    const MARKER: ResourceVector = ResourceVector::new(1, 4 * UNIT);
    const Q: ResourceVector = ResourceVector::new(2, 8 * UNIT);
    const K: ResourceVector = ResourceVector::new(2, 8 * UNIT);
    const CAP: ResourceVector = ResourceVector::new(5, 20 * UNIT);

    let empty = baseline(ResourceVector::new(0, 0), 1, 0, MARKER)?;
    assert_eq!(empty, WideResourceVector::new(1, 4 * u128::from(UNIT)));
    assert_eq!(
        zero_debt_capacity_failure(empty, Q, K, ResourceVector::new(4, CAP.bytes)),
        Some(ResourceDimension::Entries)
    );
    assert_eq!(
        zero_debt_capacity_failure(empty, Q, K, ResourceVector::new(CAP.entries, CAP.bytes - 1)),
        Some(ResourceDimension::Bytes)
    );
    assert!(zero_debt_admission(empty, Q, K, CAP));

    let after_enrollment = baseline(ResourceVector::new(1, 4 * UNIT), 1, 0, MARKER)?;
    let enrollment = mandatory_capacity(after_enrollment, Q, K, CAP);
    assert_eq!(
        after_enrollment,
        WideResourceVector::new(2, 8 * u128::from(UNIT))
    );
    assert_eq!(
        enrollment.debt,
        WideResourceVector::new(1, 4 * u128::from(UNIT))
    );
    assert!(enrollment.absolute_fit);
    assert!(enrollment.debt_within_mandatory_bound);
    assert!(enrollment.is_legal());

    assert!(no_edge_legal(ZERO_DEBT, empty, Q, K, CAP));
    Ok(())
}

/// Frozen `PARTICIPANT-CONTRACT.md` lines 3844-3887: the complete uniform-Bm
/// three-member capacity walk and the simultaneous marker charge/credit swap.
#[test]
fn three_member_admission_walk_is_marker_credit_neutral() -> TestResult {
    const BM: u64 = 100;
    const MARKER: ResourceVector = ResourceVector::new(1, BM);
    const Q: ResourceVector = ResourceVector::new(2, 2 * BM);
    const K: ResourceVector = ResourceVector::new(2, 2 * BM);
    const CAP: ResourceVector = ResourceVector::new(9, 9 * BM);

    let empty = baseline(ResourceVector::new(0, 0), 3, 0, MARKER)?;
    assert_eq!(empty, WideResourceVector::new(3, 3 * u128::from(BM)));
    assert_eq!(
        zero_debt_capacity_failure(empty, Q, K, ResourceVector::new(6, 9 * BM)),
        Some(ResourceDimension::Entries)
    );

    let after_rows_31_and_32 = baseline(ResourceVector::new(2, 2 * BM), 3, 0, MARKER)?;
    assert_eq!(
        after_rows_31_and_32,
        WideResourceVector::new(5, 5 * u128::from(BM))
    );
    assert!(zero_debt_admission(after_rows_31_and_32, Q, K, CAP));

    let tentative_third_row = baseline(ResourceVector::new(3, 3 * BM), 3, 0, MARKER)?;
    assert_eq!(
        tentative_third_row,
        WideResourceVector::new(6, 6 * u128::from(BM))
    );
    assert_eq!(
        zero_debt_capacity_failure(tentative_third_row, Q, K, CAP),
        Some(ResourceDimension::Entries)
    );

    let after_removing_row_31 = baseline(ResourceVector::new(2, 2 * BM), 3, 0, MARKER)?;
    let after_planning_three_markers = baseline(ResourceVector::new(5, 5 * BM), 3, 3, MARKER)?;
    assert_eq!(after_removing_row_31, after_rows_31_and_32);
    assert_eq!(after_planning_three_markers, after_removing_row_31);

    let after_third_admission = floor_transition(31, Some(30), 33, 32, 32);
    assert_eq!(after_third_admission.preferred_floor, 31);
    assert_eq!(after_third_admission.resulting_floor, 32);

    let after_marker_compaction = floor_transition(32, Some(36), 36, 36, 37);
    assert_eq!(after_marker_compaction.preferred_floor, 37);
    assert_eq!(after_marker_compaction.resulting_floor, 37);
    assert_eq!(
        baseline(ResourceVector::new(0, 0), 3, 0, MARKER)?,
        WideResourceVector::new(3, 3 * u128::from(BM))
    );
    Ok(())
}

/// Frozen `PARTICIPANT-CONTRACT.md` lines 4190-4211: the required-capacity
/// vector's three component-cap copies, including their exact post-U debts.
#[test]
fn required_capacity_component_copies_have_exact_debts() -> TestResult {
    const BM: u64 = 100;
    const MARKER: ResourceVector = ResourceVector::new(1, BM);
    const Q: ResourceVector = ResourceVector::new(2, 2 * BM);
    const K: ResourceVector = ResourceVector::new(2, 2 * BM);

    let baseline = baseline(ResourceVector::new(4, 4 * BM), 1, 0, MARKER)?;
    assert_eq!(baseline, WideResourceVector::new(5, 5 * u128::from(BM)));
    assert!(zero_debt_admission(
        baseline,
        Q,
        K,
        ResourceVector::new(9, 9 * BM)
    ));

    let entry_cap = ResourceVector::new(7, 9 * BM);
    let entry_result = mandatory_capacity(baseline, Q, K, entry_cap);
    assert_eq!(entry_result.debt, WideResourceVector::new(2, 0));
    assert!(entry_result.is_legal());
    assert_eq!(
        zero_debt_capacity_failure(baseline, Q, K, entry_cap),
        Some(ResourceDimension::Entries)
    );

    let byte_cap = ResourceVector::new(9, 7 * BM);
    let byte_result = mandatory_capacity(baseline, Q, K, byte_cap);
    assert_eq!(
        byte_result.debt,
        WideResourceVector::new(0, 2 * u128::from(BM))
    );
    assert!(byte_result.is_legal());
    assert_eq!(
        zero_debt_capacity_failure(baseline, Q, K, byte_cap),
        Some(ResourceDimension::Bytes)
    );

    let both_cap = ResourceVector::new(7, 7 * BM);
    let both_result = mandatory_capacity(baseline, Q, K, both_cap);
    assert_eq!(
        both_result.debt,
        WideResourceVector::new(2, 2 * u128::from(BM))
    );
    assert!(both_result.is_legal());
    assert_eq!(
        zero_debt_capacity_failure(baseline, Q, K, both_cap),
        Some(ResourceDimension::Entries)
    );
    Ok(())
}

/// Frozen `PARTICIPANT-CONTRACT.md` lines 4766-4773, 4827-4842, 4896-4901,
/// and 4950-4964: the multi-binding debt walk and full-K release boundary.
#[test]
fn multi_binding_debt_walk_respects_q_and_full_k() -> TestResult {
    const BM: u64 = 100;
    const MARKER: ResourceVector = ResourceVector::new(1, BM);
    const Q: ResourceVector = ResourceVector::new(2, 2 * BM);
    const K: ResourceVector = ResourceVector::new(2, 2 * BM);
    const NO_K: ResourceVector = ResourceVector::new(0, 0);
    const ONE_K: ResourceVector = ResourceVector::new(1, BM);
    const CAP: ResourceVector = ResourceVector::new(7, 7 * BM);

    let startup = baseline(ResourceVector::new(0, 0), 2, 0, MARKER)?;
    let after_p0 = baseline(ResourceVector::new(1, BM), 2, 0, MARKER)?;
    let after_p1 = baseline(ResourceVector::new(2, 2 * BM), 2, 0, MARKER)?;
    assert_eq!(startup, WideResourceVector::new(2, 2 * u128::from(BM)));
    assert_eq!(after_p0, WideResourceVector::new(3, 3 * u128::from(BM)));
    assert_eq!(after_p1, WideResourceVector::new(4, 4 * u128::from(BM)));
    assert!(zero_debt_admission(startup, Q, K, CAP));
    assert!(zero_debt_admission(after_p0, Q, K, CAP));
    assert_eq!(mandatory_capacity(after_p0, Q, NO_K, CAP).debt, ZERO_DEBT);

    let second_enrollment = mandatory_capacity(after_p1, Q, K, CAP);
    assert_eq!(
        second_enrollment.debt,
        WideResourceVector::new(1, u128::from(BM))
    );
    assert!(second_enrollment.is_legal());

    let first_terminal = baseline(ResourceVector::new(3, 3 * BM), 2, 0, MARKER)?;
    let first_terminal_capacity = mandatory_capacity(first_terminal, Q, K, CAP);
    assert_eq!(
        first_terminal,
        WideResourceVector::new(5, 5 * u128::from(BM))
    );
    assert_eq!(
        first_terminal_capacity.debt,
        WideResourceVector::new(2, 2 * u128::from(BM))
    );
    assert!(first_terminal_capacity.is_legal());

    let second_terminal_tentative = baseline(ResourceVector::new(4, 4 * BM), 2, 0, MARKER)?;
    let over_q = mandatory_capacity(second_terminal_tentative, Q, K, CAP);
    assert_eq!(
        second_terminal_tentative,
        WideResourceVector::new(6, 6 * u128::from(BM))
    );
    assert_eq!(over_q.debt, WideResourceVector::new(3, 3 * u128::from(BM)));
    assert!(!over_q.absolute_fit);
    assert!(!over_q.debt_within_mandatory_bound);
    assert!(!over_q.is_legal());

    let after_removing_attached_1 = baseline(ResourceVector::new(3, 3 * BM), 2, 0, MARKER)?;
    assert_eq!(after_removing_attached_1, first_terminal);
    assert!(mandatory_capacity(after_removing_attached_1, Q, K, CAP).is_legal());

    let terminal_plus_left = mandatory_capacity(second_terminal_tentative, Q, ONE_K, CAP);
    assert_eq!(
        terminal_plus_left.debt,
        WideResourceVector::new(2, 2 * u128::from(BM))
    );
    assert!(terminal_plus_left.absolute_fit);
    assert!(terminal_plus_left.debt_within_mandatory_bound);
    assert!(terminal_plus_left.is_legal());

    let post_batch = baseline(ResourceVector::new(5, 5 * BM), 2, 2, MARKER)?;
    assert_eq!(post_batch, first_terminal);
    assert_eq!(
        mandatory_capacity(post_batch, Q, K, CAP).debt,
        WideResourceVector::new(2, 2 * u128::from(BM))
    );

    let final_no_edge = baseline(ResourceVector::new(1, BM), 2, 0, MARKER)?;
    assert_eq!(final_no_edge, after_p0);
    assert!(no_edge_legal(ZERO_DEBT, final_no_edge, Q, K, CAP));
    Ok(())
}

/// Frozen `PARTICIPANT-CONTRACT.md` lines 5117-5137: every printed byte-budget
/// candidate, including the first-removal failure and exact floor computation.
#[test]
fn byte_budget_walk_tests_every_removal_candidate() -> TestResult {
    const U: u64 = 7;
    const MARKER: ResourceVector = ResourceVector::new(1, 4 * U);
    const Q: ResourceVector = ResourceVector::new(2, 8 * U);
    const K: ResourceVector = ResourceVector::new(2, 8 * U);
    const CAP: ResourceVector = ResourceVector::new(12, 48 * U);

    let before_overtake = baseline(ResourceVector::new(6, 28 * U), 1, 0, MARKER)?;
    assert_eq!(
        before_overtake,
        WideResourceVector::new(7, 32 * u128::from(U))
    );
    assert!(zero_debt_admission(before_overtake, Q, K, CAP));

    let tentative = baseline(ResourceVector::new(7, 32 * U), 1, 0, MARKER)?;
    assert_eq!(tentative, WideResourceVector::new(8, 36 * u128::from(U)));
    assert_eq!(
        zero_debt_capacity_failure(tentative, Q, K, CAP),
        Some(ResourceDimension::Bytes)
    );

    let after_first_removal = baseline(ResourceVector::new(6, 31 * U), 1, 0, MARKER)?;
    assert_eq!(
        after_first_removal,
        WideResourceVector::new(7, 35 * u128::from(U))
    );
    assert_eq!(
        zero_debt_capacity_failure(after_first_removal, Q, K, CAP),
        Some(ResourceDimension::Bytes)
    );

    let after_second_removal = baseline(ResourceVector::new(5, 20 * U), 1, 0, MARKER)?;
    assert_eq!(
        after_second_removal,
        WideResourceVector::new(6, 24 * u128::from(U))
    );
    assert!(zero_debt_admission(after_second_removal, Q, K, CAP));

    let floor = floor_transition(89, Some(88), 95, 94, 91);
    assert_eq!(floor.member_cursor, 88);
    assert_eq!(floor.preferred_floor, 89);
    assert_eq!(floor.resulting_floor, 91);
    Ok(())
}

/// Frozen `PARTICIPANT-CONTRACT.md` lines 5144-5193: exact-Q debt followed by
/// recovery, detached-Leave, and live-Leave full-K release checks.
#[test]
fn byte_budget_recovery_and_leave_releases_restore_full_k() -> TestResult {
    const U: u64 = 7;
    const MARKER: ResourceVector = ResourceVector::new(1, 4 * U);
    const Q: ResourceVector = ResourceVector::new(2, 8 * U);
    const K: ResourceVector = ResourceVector::new(2, 8 * U);
    const RECOVERY_CHARGE: ResourceVector = ResourceVector::new(1, 4 * U);
    const CAP: ResourceVector = ResourceVector::new(12, 48 * U);

    let exact_q_baseline = baseline(ResourceVector::new(10, 40 * U), 1, 1, MARKER)?;
    let exact_q = mandatory_capacity(exact_q_baseline, Q, K, CAP);
    assert_eq!(
        exact_q_baseline,
        WideResourceVector::new(10, 40 * u128::from(U))
    );
    assert_eq!(exact_q.debt, WideResourceVector::new(2, 8 * u128::from(U)));
    assert!(exact_q.is_legal());

    let recovered = recovery_transfer(
        WideResourceVector::new(6, 24 * u128::from(U)),
        K,
        RECOVERY_CHARGE,
    )
    .map_err(|error| error.to_string())?;
    assert_eq!(
        recovered.baseline,
        WideResourceVector::new(7, 28 * u128::from(U))
    );
    assert_eq!(
        recovered.remaining_recovery_claim,
        ResourceVector::new(1, 4 * U)
    );
    assert_eq!(
        mandatory_capacity(
            recovered.baseline,
            Q,
            recovered.remaining_recovery_claim,
            CAP
        )
        .debt,
        ZERO_DEBT
    );
    assert!(no_edge_legal(ZERO_DEBT, recovered.baseline, Q, K, CAP));

    let detached_leave = recovery_transfer(
        WideResourceVector::new(2, 8 * u128::from(U)),
        K,
        RECOVERY_CHARGE,
    )
    .map_err(|error| error.to_string())?;
    assert_eq!(
        detached_leave.baseline,
        WideResourceVector::new(3, 12 * u128::from(U))
    );
    assert_eq!(
        detached_leave.remaining_recovery_claim,
        ResourceVector::new(1, 4 * U)
    );
    assert!(no_edge_legal(ZERO_DEBT, detached_leave.baseline, Q, K, CAP));

    let detached_floor = floor_transition(92, None, 102, 100, 101);
    assert_eq!(detached_floor.preferred_floor, 101);
    assert_eq!(detached_floor.resulting_floor, 101);

    let live_leave = baseline(ResourceVector::new(1, 4 * U), 1, 0, MARKER)?;
    assert_eq!(live_leave, WideResourceVector::new(2, 8 * u128::from(U)));
    assert!(no_edge_legal(ZERO_DEBT, live_leave, Q, K, CAP));

    let live_floor = floor_transition(91, None, 101, 100, 101);
    assert_eq!(live_floor.preferred_floor, 101);
    assert_eq!(live_floor.resulting_floor, 101);
    Ok(())
}
