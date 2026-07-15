use super::floor_transition;

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
