//! Structural W1b close-out census oracles.

use super::log::StoredOperation;

#[derive(Debug, PartialEq, Eq)]
enum FinalizerCarrier {
    Enclosing,
    NotFinalizer,
}

fn finalizer_carrier(operation: &StoredOperation) -> FinalizerCarrier {
    match operation {
        StoredOperation::Attached { .. } | StoredOperation::Left { .. } => {
            FinalizerCarrier::Enclosing
        }
        StoredOperation::Genesis { .. }
        | StoredOperation::Enrolled { .. }
        | StoredOperation::Died { .. }
        | StoredOperation::Detached { .. }
        | StoredOperation::Ordinary { .. }
        | StoredOperation::Recovered { .. }
        | StoredOperation::ZeroDebtAck { .. }
        | StoredOperation::NonzeroDebtAck { .. }
        | StoredOperation::MarkerDrained { .. }
        | StoredOperation::RecordAdmission { .. } => FinalizerCarrier::NotFinalizer,
    }
}

#[test]
fn standalone_pending_finalizer_has_no_production_entry_point() {
    // This match is intentionally exhaustive: adding any standalone durable
    // finalizer variant makes the census fail to compile until it is classified.
    let genesis = StoredOperation::Genesis { event: Vec::new() };
    assert_eq!(finalizer_carrier(&genesis), FinalizerCarrier::NotFinalizer);
}

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
