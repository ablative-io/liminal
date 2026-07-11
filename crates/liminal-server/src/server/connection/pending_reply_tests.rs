//! R1(vi) (§1.2(3b)) pending-reply state-machine tests. Every tombstone/FIFO/cap
//! ruling in the design text is pinned here at the table level — the state machine
//! is where the subtle correctness lives (never-match-younger, self-wedge,
//! scope-not-time reclamation), so it is exercised directly and deterministically.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::items_after_statements
)]

use std::time::{Duration, Instant};

use liminal::protocol::Frame;

use super::{DEFAULT_REPLY_TIMEOUT, PendingReplyTable, test_reply_envelope};

/// §5 defaults: sub-cap 8 per conversation, 32 per connection.
fn table() -> PendingReplyTable {
    PendingReplyTable::new(8, 32, DEFAULT_REPLY_TIMEOUT)
}

fn now() -> Instant {
    Instant::now()
}

/// A time far past any admitted entry's deadline.
fn later(base: Instant) -> Instant {
    base + DEFAULT_REPLY_TIMEOUT + Duration::from_secs(1)
}

#[test]
fn multiple_pipelined_replies_on_one_conversation_match_fifo() {
    let mut t = table();
    let base = now();
    // Three reply-requested frames pipelined on conversation 1, streams 10/11/12.
    t.admit(1, 10, base).expect("admit 1");
    t.admit(1, 11, base).expect("admit 2");
    t.admit(1, 12, base).expect("admit 3");
    assert_eq!(t.pending_for(1), 3);

    // Replies arrive; each matches the OLDEST pending entry (FIFO) — stream 10
    // first, then 11, then 12.
    for stream in [10_u32, 11, 12] {
        let frame = t
            .match_reply(1, test_reply_envelope(b"r"))
            .expect("a pending entry matches");
        assert!(
            matches!(frame, Frame::ConversationMessage { stream_id, conversation_id: 1, .. } if stream_id == stream),
            "FIFO: reply matches the oldest pending entry (stream {stream})"
        );
    }
    assert_eq!(t.len(), 0, "all entries consumed");
}

#[test]
fn replies_are_correlated_per_conversation() {
    let mut t = table();
    let base = now();
    t.admit(1, 10, base).expect("admit c1");
    t.admit(2, 20, base).expect("admit c2");

    // A reply for conversation 2 matches conversation 2's entry, not c1's.
    let frame = t
        .match_reply(2, test_reply_envelope(b"r"))
        .expect("c2 match");
    assert!(matches!(
        frame,
        Frame::ConversationMessage {
            stream_id: 20,
            conversation_id: 2,
            ..
        }
    ));
    assert_eq!(t.pending_for(1), 1, "conversation 1's entry is untouched");
}

#[test]
fn timeout_then_late_reply_then_new_request() {
    let mut t = table();
    let base = now();
    t.admit(1, 10, base).expect("admit");

    // Deadline passes: the entry tombstones and a timeout frame is produced.
    let expired = t.expire_due(later(base));
    assert_eq!(expired.len(), 1, "one timeout frame");
    assert!(matches!(
        expired[0],
        Frame::ConversationError {
            stream_id: 10,
            conversation_id: 1,
            ..
        }
    ));
    assert_eq!(t.tombstones_for(1), 1);
    assert_eq!(t.pending_for(1), 0);

    // The LATE reply arrives: it consumes the tombstone (discarded), never
    // delivered late.
    assert!(
        t.match_reply(1, test_reply_envelope(b"late")).is_none(),
        "a late reply is discarded, not delivered"
    );
    assert_eq!(
        t.len(),
        0,
        "the tombstone is freed by the late-reply consume"
    );

    // A NEW request on the same conversation is admitted cleanly now.
    t.admit(1, 11, base).expect("new admit after recovery");
    let frame = t
        .match_reply(1, test_reply_envelope(b"fresh"))
        .expect("match");
    assert!(matches!(
        frame,
        Frame::ConversationMessage { stream_id: 11, .. }
    ));
}

#[test]
fn capacity_recovers_via_late_reply_consume() {
    let mut t = table();
    let base = now();
    t.admit(1, 10, base).expect("admit");
    t.expire_due(later(base)); // -> tombstone
    assert_eq!(t.tombstones_for(1), 1);
    // Consume the tombstone via its late reply: capacity is freed.
    assert!(t.match_reply(1, test_reply_envelope(b"late")).is_none());
    assert_eq!(t.len(), 0, "late-reply consume frees the slot");
}

#[test]
fn capacity_recovers_via_conversation_close() {
    let mut t = table();
    let base = now();
    t.admit(1, 10, base).expect("admit");
    t.expire_due(later(base)); // -> tombstone
    t.admit(1, 11, base).expect("second admit under sub-cap");
    assert_eq!(t.len(), 2);
    // Closing the conversation sweeps BOTH the tombstone and the pending entry.
    t.remove_conversation(1);
    assert_eq!(
        t.len(),
        0,
        "close sweep clears every entry for the conversation"
    );
}

#[test]
fn wedged_conversation_refuses_while_siblings_proceed() {
    // Sub-cap of 2 so the wedge is quick to reach.
    let mut t = PendingReplyTable::new(2, 32, DEFAULT_REPLY_TIMEOUT);
    let base = now();
    // Fill conversation 1's sub-cap, then time both out into tombstones.
    t.admit(1, 10, base).expect("c1 admit 1");
    t.admit(1, 11, base).expect("c1 admit 2");
    t.expire_due(later(base));
    assert_eq!(
        t.tombstones_for(1),
        2,
        "conversation 1 is full of tombstones"
    );

    // A new reply-requested admission on the WEDGED conversation is refused with
    // the typed sub-cap error (tombstones count against the sub-cap).
    let refusal = t
        .admit(1, 12, base)
        .expect_err("wedged conversation refuses");
    assert!(
        matches!(
            refusal,
            crate::ServerError::ConnectionCapReached {
                cap: "max_pending_replies_per_conversation",
                ..
            }
        ),
        "the self-wedge is the typed per-conversation cap refusal"
    );

    // A SIBLING conversation proceeds entirely unaffected.
    t.admit(2, 20, base).expect("sibling conversation proceeds");
    let frame = t
        .match_reply(2, test_reply_envelope(b"r"))
        .expect("sibling match");
    assert!(matches!(
        frame,
        Frame::ConversationMessage {
            conversation_id: 2,
            ..
        }
    ));
}

/// The slow-actor sequence end to end: a tombstone-only conversation, a new
/// admission under the sub-cap, then the VERY LATE reply — asserted consumed by
/// the oldest (the tombstone), NEVER matched to the younger admitted entry.
#[test]
fn slow_actor_late_reply_never_matches_a_younger_entry() {
    let mut t = table();
    let base = now();
    // Conversation 1: admit, time out -> tombstone (the "slow, not dead" actor).
    t.admit(1, 10, base).expect("admit old");
    t.expire_due(later(base));
    assert_eq!(t.tombstones_for(1), 1);

    // A NEW request is admitted under the sub-cap (younger entry, stream 11).
    t.admit(1, 11, base).expect("admit young");
    assert_eq!(t.pending_for(1), 1);

    // The very-late reply for the OLD request finally arrives. FIFO consumes the
    // OLDEST entry — the tombstone — discarding the reply. It must NOT be
    // delivered on stream 11 (the younger request), which is still pending.
    assert!(
        t.match_reply(1, test_reply_envelope(b"very-late"))
            .is_none(),
        "the very-late reply is discarded via the tombstone, not delivered"
    );
    assert_eq!(
        t.pending_for(1),
        1,
        "the younger entry is still pending — never mis-matched to the old reply"
    );
    assert_eq!(t.tombstones_for(1), 0, "the tombstone was consumed");

    // The younger request's OWN reply then matches it correctly.
    let frame = t
        .match_reply(1, test_reply_envelope(b"young"))
        .expect("young match");
    assert!(matches!(
        frame,
        Frame::ConversationMessage { stream_id: 11, .. }
    ));
}

#[test]
fn connection_cap_counts_pending_only_not_tombstones() {
    // Per-connection cap of 2 pending; a large sub-cap so it is not the trip.
    let mut t = PendingReplyTable::new(64, 2, DEFAULT_REPLY_TIMEOUT);
    let base = now();
    t.admit(1, 10, base).expect("admit 1");
    t.admit(2, 20, base).expect("admit 2"); // now 2 pending across the connection
    // A third PENDING admission is refused by the connection table.
    let refusal = t.admit(3, 30, base).expect_err("connection table full");
    assert!(matches!(
        refusal,
        crate::ServerError::ConnectionCapReached {
            cap: "max_pending_conversation_replies_per_connection",
            ..
        }
    ));
    // Time out both pending -> tombstones. Tombstones do NOT count against the
    // connection table, so a fresh conversation can be admitted again.
    t.expire_due(later(base));
    t.admit(3, 30, base)
        .expect("tombstones free the connection table for new pending entries");
}

#[test]
fn expire_is_idempotent_across_slices() {
    let mut t = table();
    let base = now();
    t.admit(1, 10, base).expect("admit");
    let first = t.expire_due(later(base));
    assert_eq!(
        first.len(),
        1,
        "the entry tombstones once, emitting one frame"
    );
    // A subsequent slice's deadline check must NOT re-emit a timeout for the same
    // (already-tombstoned) entry.
    let second = t.expire_due(later(base));
    assert!(
        second.is_empty(),
        "an already-tombstoned entry never re-times-out"
    );
}

#[test]
fn conversations_awaiting_reply_lists_only_pending_conversations() {
    let mut t = table();
    let base = now();
    t.admit(1, 10, base).expect("admit c1");
    t.admit(2, 20, base).expect("admit c2");
    t.expire_due(later(base)); // both -> tombstones
    assert!(
        t.conversations_awaiting_reply().is_empty(),
        "a conversation with only tombstones is not awaiting a (matchable) reply"
    );
    t.admit(3, 30, base).expect("admit c3 pending");
    assert_eq!(t.conversations_awaiting_reply(), vec![3]);
}

#[test]
fn cancel_all_drops_every_entry() {
    let mut t = table();
    let base = now();
    t.admit(1, 10, base).expect("admit");
    t.admit(2, 20, base).expect("admit");
    t.expire_due(later(base));
    t.cancel_all();
    assert_eq!(t.len(), 0, "finalization cancels every entry");
}
