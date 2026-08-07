//! Load-end orphaned-anchor reconcile: the conversation-6 residue, executed.
//!
//! An anchor is DERIVED only while its marker record survives retention AND its
//! owning participant is still active with a cursor short of the marker. The
//! STORED side lives in closure accounting and is retired only by an
//! acknowledgement (or, since `67d780e`, an ordinary ack crossing the marker).
//! Participant erasure or record retirement therefore zeroes the derived side
//! with NO log row left that could ever retire the stored side — and because
//! cold replay rebuilds state by re-running rows, every boot rebuilds the same
//! split and every admission faults on the `MarkerAnchorAccounting`
//! cross-check, forever. That is the 2026-08-07 conversation-6 wedge: 36
//! continuing warns across a boot that healed its crossing-ack sibling.
//!
//! `reconcile_orphaned_marker_anchors` retires exactly the orphaned excess at
//! load end. These units pin its three obligations: heal the orphan, stay
//! idempotent, and NEVER touch the derived-ahead direction (that split is the
//! admission projection's to fault on, not ours to paper over).

#![allow(clippy::expect_used, clippy::panic)]

use alloc::{vec, vec::Vec};

use crate::algebra::{ResourceVector, WideResourceVector};
use crate::outcome::CandidatePhase;
use crate::wire::{BindingEpoch, ConnectionIncarnation, Generation};

use super::{
    super::{
        AdmissionOrder, ClosureAccounting, ClosureState, OrderClaims, OrderHigh, OrderLedger,
        RecoverySequenceReserve, SequenceClaims, SequenceLedger,
        claim_frontier::{
            BindingTerminalOwner, ClaimFrontiers, ClaimFrontiersRestore, FrontierBinding,
            FrontierParticipant, MarkerProvenance, MovableOrderClaim, MovableSequenceClaim,
            OrderClaimFrontierRestore, OrderDirectOwner, RetainedCausalRecord,
            RetainedCausalRecordKind, SequenceClaimFrontierRestore, SequenceDirectOwner,
            SequenceProductRangesRestore, TerminalProductRangeRestore,
        },
    },
    live_frontier::LiveFrontierOwner,
};

const CONVERSATION_ID: u64 = 6;
const P0: u64 = 0;
const MARKER_SEQ: u64 = 12;

fn epoch() -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(6, 0),
        Generation::new(1).expect("test generation is nonzero"),
    )
}

/// One active participant, one retained compaction marker at `MARKER_SEQ`, the
/// participant's cursor and the stored anchor count parameterized — so a
/// single validated shape can hold the marker as unaccepted (cursor short of
/// the marker), accepted (cursor at it), or orphaned (accepted by cursor,
/// never retired from accounting).
fn owner_with(cursor: u64, stored_anchors: u64) -> LiveFrontierOwner {
    let binding_epoch = epoch();
    let terminal_owner = BindingTerminalOwner {
        participant_index: P0,
        binding_epoch,
    };
    let exit_seq = MARKER_SEQ + 1;
    let terminal_seq = exit_seq + 1;
    let product_seq = terminal_seq + 1;
    let sequence = SequenceLedger::try_new(
        MARKER_SEQ,
        SequenceClaims::new(1, 1, 0, RecoverySequenceReserve::None),
    )
    .expect("reconcile fixture sequence ledger is valid");
    let order = OrderLedger::try_new(
        OrderHigh::Allocated(0),
        OrderClaims::new(1, 1, false, false).expect("reconcile fixture order claims are valid"),
    )
    .expect("reconcile fixture order ledger is valid");
    let frontiers = ClaimFrontiers::restore(
        ClaimFrontiersRestore {
            conversation_id: CONVERSATION_ID,
            active_identities: vec![FrontierParticipant::new(
                P0,
                cursor,
                FrontierBinding::Bound(binding_epoch),
            )],
            identity_slot_limit: 1,
            retained_floor: u128::from(MARKER_SEQ),
            retained_record_limit: 1,
            retained_records: vec![RetainedCausalRecord {
                delivery_seq: MARKER_SEQ,
                admission_order: AdmissionOrder::new(0, CandidatePhase::CompactionMarker, P0),
                kind: RetainedCausalRecordKind::CompactionMarker {
                    participant_index: P0,
                    provenance: MarkerProvenance::NonProductM,
                },
            }],
            active_marker_anchors: vec![MARKER_SEQ],
            historical_marker_deliveries: vec![],
            historical_causal_facts: vec![],
            sequence: SequenceClaimFrontierRestore {
                movable_claims: vec![
                    MovableSequenceClaim {
                        delivery_seq: exit_seq,
                        owner: SequenceDirectOwner::MembershipExit {
                            participant_index: P0,
                        },
                    },
                    MovableSequenceClaim {
                        delivery_seq: terminal_seq,
                        owner: SequenceDirectOwner::BindingTerminal(terminal_owner),
                    },
                ],
                immutable_candidates: vec![],
                products: SequenceProductRangesRestore {
                    live_times_terminal: vec![TerminalProductRangeRestore {
                        start: product_seq,
                        length: 1,
                        terminal: terminal_owner,
                    }],
                    ..SequenceProductRangesRestore::default()
                },
                recovery: None,
            },
            order: OrderClaimFrontierRestore {
                movable_claims: vec![
                    MovableOrderClaim {
                        transaction_order: 1,
                        owner: OrderDirectOwner::ActiveBindingTerminal(terminal_owner),
                    },
                    MovableOrderClaim {
                        transaction_order: 2,
                        owner: OrderDirectOwner::MembershipExit {
                            participant_index: P0,
                        },
                    },
                ],
                immutable_candidates: vec![],
                recovery: None,
            },
            recovery_marker_delivery_seq: None,
        },
        sequence,
        order,
    )
    .expect("reconcile fixture restores from the canonical cold shape");
    let accounting = ClosureAccounting::try_new(
        ClosureState::Clear,
        stored_anchors,
        stored_anchors,
        0,
        0,
        ResourceVector::default(),
        WideResourceVector::default(),
        ResourceVector::new(16, 1024),
        0,
        2,
    )
    .expect("reconcile fixture accounting is valid");
    LiveFrontierOwner::from_test_parts(frontiers, accounting, vec![], 1)
}

#[test]
fn reconcile_retires_the_orphaned_anchor_and_is_idempotent() {
    // Cursor AT the marker: the derived census counts nothing, yet the stored
    // accounting still carries the anchor — the wedge state as it survives in
    // durable rows, where no replayed ack can ever retire it.
    let mut owner = owner_with(MARKER_SEQ, 1);
    assert_eq!(owner.frontiers().unaccepted_marker_anchor_count(), 0);
    assert_eq!(owner.closure_accounting().marker_anchors(), 1);

    assert_eq!(owner.reconcile_orphaned_marker_anchors(), 1);
    assert_eq!(owner.closure_accounting().marker_anchors(), 0);

    // In-step now; a second reconcile must retire nothing.
    assert_eq!(owner.reconcile_orphaned_marker_anchors(), 0);
    assert_eq!(owner.closure_accounting().marker_anchors(), 0);
}

#[test]
fn reconcile_leaves_an_in_step_unaccepted_marker_alone() {
    // Cursor short of the marker: derived == stored == 1 — a live, healthy
    // delivered marker awaiting acceptance. Nothing may move.
    let mut owner = owner_with(MARKER_SEQ - 1, 1);
    assert_eq!(owner.frontiers().unaccepted_marker_anchor_count(), 1);

    assert_eq!(owner.reconcile_orphaned_marker_anchors(), 0);
    assert_eq!(owner.closure_accounting().marker_anchors(), 1);
}

#[test]
fn reconcile_never_touches_the_derived_ahead_direction() {
    // Derived 1, stored 0: the OPPOSITE split. Reconciling must do nothing —
    // that divergence is the admission projection's to fault on loudly, and
    // manufacturing anchors here would paper over a corruption.
    let mut owner = owner_with(MARKER_SEQ - 1, 0);
    assert_eq!(owner.frontiers().unaccepted_marker_anchor_count(), 1);
    assert_eq!(owner.closure_accounting().marker_anchors(), 0);

    assert_eq!(owner.reconcile_orphaned_marker_anchors(), 0);
    assert_eq!(owner.closure_accounting().marker_anchors(), 0);
}
