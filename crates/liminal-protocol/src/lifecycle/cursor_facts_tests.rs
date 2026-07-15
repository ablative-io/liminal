//! Regression for `docs/design/LP-EXTRACTION-GOAL.md` Fix 2.
//!
//! Case 54's two participants require four independent facts for the same two
//! retained boundaries. A record-scoped fixed occurrence group cannot represent
//! this history; the participant/boundary key can.

#![allow(clippy::expect_used)]

use super::{CursorProgressFact, CursorProgressFacts, CursorProgressKey};

#[test]
fn two_participants_ack_the_same_suffix_during_debt() {
    let mut facts = CursorProgressFacts::new();
    let keys = [
        CursorProgressKey {
            participant_index: 0,
            boundary: 1,
        },
        CursorProgressKey {
            participant_index: 1,
            boundary: 1,
        },
        CursorProgressKey {
            participant_index: 0,
            boundary: 2,
        },
        CursorProgressKey {
            participant_index: 1,
            boundary: 2,
        },
    ];

    for key in keys {
        assert!(facts.record(key));
        assert!(!facts.encode().expect("four facts fit u32").is_empty());
    }
    assert_eq!(facts.len(), 4);

    let p0 = facts.consume_through(0, 2);
    assert_eq!(p0.len(), 2);
    assert_eq!(
        facts.get(CursorProgressKey {
            participant_index: 1,
            boundary: 1,
        }),
        Some(CursorProgressFact::Pending)
    );
    assert_eq!(
        facts.get(CursorProgressKey {
            participant_index: 1,
            boundary: 2,
        }),
        Some(CursorProgressFact::Pending)
    );

    let p1_first = facts.consume_through(1, 1);
    assert_eq!(p1_first.len(), 1);
    let p1_second = facts.consume_through(1, 2);
    assert_eq!(p1_second.len(), 1);
    assert_eq!(facts.encode().expect("four facts fit u32").len(), 72);
}
