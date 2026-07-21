//! Structural W1b close-out census oracles.

#[test]
fn w1b_tear_rider_removes_tautological_four_counter_tuple() {
    let source = include_str!("tests_w1a.rs");
    let removed = [
        ["arm_", "removals"].concat(),
        ["wa", "kes"].concat(),
        ["owner_", "publications"].concat(),
        ["classifi", "cations"].concat(),
    ];
    for name in &removed {
        assert!(
            !source.contains(name),
            "ruled tautological counter remains: {name}"
        );
    }
    let tuple = format!(
        "({}, {}, {}, {})",
        removed[0], removed[1], removed[2], removed[3]
    );
    assert!(
        !source.contains(&tuple),
        "ruled constant-only tuple remains"
    );
}
