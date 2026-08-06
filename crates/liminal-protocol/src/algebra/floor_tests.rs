use super::{AdmissibleFloor, admissible_installed_floor, floor_transition, marker_clamped_floor};

/// THE TRAP, asserted rather than described: the two are opposites.
///
/// `cap_floor` is `floor_transition`'s fifth argument and it RAISES;
/// `marker_clamped_floor` LOWERS. Routing the lowest marker through `cap_floor`
/// yields a floor still sitting above the marker in exactly the poisoning case,
/// which is the shape a "bound the floor by the marker" instruction produces
/// naturally. This unit fails if the two ever converge.
#[test]
fn the_marker_clamp_lowers_where_cap_floor_raises() {
    let marker = 7_u64;
    let computed = 12_u128;

    // Through `cap_floor`: still above the marker. This is the wrong answer.
    let raised = floor_transition(1, Some(11), 11, 11, u128::from(marker)).resulting_floor;
    assert_eq!(raised, computed);
    assert!(raised > u128::from(marker));

    // Through the clamp: at the marker, never past it.
    assert_eq!(marker_clamped_floor(computed, Some(marker)), 7);
}

#[test]
fn an_empty_marker_set_passes_the_computed_floor_through_unchanged() {
    assert_eq!(marker_clamped_floor(0, None), 0);
    assert_eq!(marker_clamped_floor(41, None), 41);
    assert_eq!(
        marker_clamped_floor(u128::from(u64::MAX) + 1, None),
        u128::from(u64::MAX) + 1
    );
}

#[test]
fn the_clamp_target_is_the_marker_itself_and_not_one_below_it() {
    assert_eq!(marker_clamped_floor(9, Some(9)), 9);
    assert_eq!(marker_clamped_floor(10, Some(9)), 9);
    // Below the marker there is nothing to clamp: the floor already stops short.
    assert_eq!(marker_clamped_floor(8, Some(9)), 8);
}

/// F4: when the lowest marker equals the retained floor, the floor does not
/// advance. A marker pins the floor; that is the whole point of the rule.
#[test]
fn a_marker_at_the_retained_floor_pins_it_where_it_stands() {
    let installed = admissible_installed_floor(20, 9, Some(9), 30);
    assert_eq!(installed, AdmissibleFloor::Install(9));
}

#[test]
fn an_admissible_floor_is_bounded_by_the_current_high_watermark() {
    // No marker: the upper end is `high_watermark + 1` and nothing else.
    assert_eq!(
        admissible_installed_floor(99, 3, None, 10),
        AdmissibleFloor::Install(11)
    );
    // The marker binds first when it is lower than the retained end.
    assert_eq!(
        admissible_installed_floor(99, 3, Some(6), 10),
        AdmissibleFloor::Install(6)
    );
    // An uncontested floor inside the interval installs unchanged.
    assert_eq!(
        admissible_installed_floor(5, 3, Some(6), 10),
        AdmissibleFloor::Install(5)
    );
}

/// M1a: a measurement the frontier has already moved past is a no-op, NOT a
/// refusal and NOT an install that would drive the floor backwards.
#[test]
fn a_measurement_the_frontier_moved_past_is_subsumed() {
    assert_eq!(
        admissible_installed_floor(4, 9, None, 30),
        AdmissibleFloor::Subsumed
    );
    // Subsumed by the marker bound rather than by the measured value: the
    // marker sits below the retained floor, which means the frontier already
    // broke its own retention invariant. Installing nothing is the safe answer.
    assert_eq!(
        admissible_installed_floor(20, 9, Some(4), 30),
        AdmissibleFloor::Subsumed
    );
    // Exactly at the retained floor installs; only strictly below is subsumed.
    assert_eq!(
        admissible_installed_floor(9, 9, None, 30),
        AdmissibleFloor::Install(9)
    );
}

/// The re-mint never RAISES a measured floor to reach the interval. A raise
/// would eat retained rows this fate never measured.
#[test]
fn the_remint_never_raises_the_measured_floor() {
    // The interval's upper end is 101 and its lower end is 3, so a raise to
    // reach either end is available and must not be taken.
    assert_eq!(
        admissible_installed_floor(5, 3, None, 100),
        AdmissibleFloor::Install(5)
    );
    assert_eq!(
        admissible_installed_floor(3, 3, None, 100),
        AdmissibleFloor::Install(3)
    );
}

#[test]
fn multiple_claims_match_the_document_floor_walk() {
    let floor = floor_transition(1, Some(10), 100, 100, 25);

    assert_eq!(floor.member_cursor, 10);
    assert_eq!(floor.preferred_floor, 11);
    assert_eq!(floor.resulting_floor, 25);
}

#[test]
fn final_leave_substitutes_candidate_watermark_for_empty_membership() {
    let after_leave = floor_transition(1, None, 101, 100, 101);
    assert_eq!(after_leave.member_cursor, 101);
    assert_eq!(after_leave.preferred_floor, 101);
    assert_eq!(after_leave.resulting_floor, 101);

    let after_projection = floor_transition(101, None, 101, 101, 102);
    assert_eq!(after_projection.preferred_floor, 102);
    assert_eq!(after_projection.resulting_floor, 102);
}

#[test]
fn a_late_cursor_zero_member_never_lowers_the_floor() {
    let floor = floor_transition(25, Some(0), 101, 100, 25);

    assert_eq!(floor.preferred_floor, 1);
    assert_eq!(floor.resulting_floor, 25);
}

#[test]
fn one_past_maximum_is_representable() {
    let floor = floor_transition(
        u128::from(u64::MAX),
        None,
        u64::MAX,
        u64::MAX,
        u128::from(u64::MAX) + 1,
    );

    assert_eq!(floor.preferred_floor, u128::from(u64::MAX) + 1);
    assert_eq!(floor.resulting_floor, u128::from(u64::MAX) + 1);
}
