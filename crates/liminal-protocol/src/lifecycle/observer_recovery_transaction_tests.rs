//! A4 observer-recovery atomic-transaction coverage.
//!
//! The prose invariants of `observer_recovery.rs` — the arm plan and response
//! are produced together and persist atomically; the progress reads and arm
//! installation share one serialization boundary — are exercised here as
//! types and tests: unit tests for every owned-aggregate transition and
//! property tests proving that arbitrary interleavings of recovery with
//! acknowledgement/fate progress never double-install or half-install arms,
//! and that crash replay from any intermediate durable state converges.

#![allow(clippy::expect_used, clippy::panic)]

use alloc::collections::BTreeMap;
use alloc::vec;
use alloc::vec::Vec;

use proptest::prelude::*;

use crate::wire::{
    ConversationId, DeliverySeq, InvalidObserverEpoch, ObserverEpoch, ObserverProgressStatus,
    ObserverRecoveryHandshake, ObserverRecoveryResponse, ObserverRefusal,
};

use super::{
    ObserverProgressAdvanceDecision, ObserverProgressAdvanceError,
    ObserverProgressAdvanceTransaction, ObserverProgressTrackError, ObserverRecoveryAggregate,
    ObserverRecoveryAggregateRestoreError, ObserverRecoveryTransaction,
    ObserverRecoveryTransactionDecision,
};

const CONVERSATIONS: [ConversationId; 4] = [101, 102, 103, 104];
const INITIAL_PROGRESS: DeliverySeq = 10;
const LIMIT: u64 = 16;

fn request(entries: &[(ConversationId, ObserverEpoch)]) -> ObserverRecoveryHandshake {
    ObserverRecoveryHandshake {
        observer_refusals: entries
            .iter()
            .map(|(conversation_id, refused_epoch)| ObserverRefusal {
                conversation_id: *conversation_id,
                refused_epoch: *refused_epoch,
            })
            .collect(),
    }
}

fn tracked_aggregate() -> ObserverRecoveryAggregate {
    let mut aggregate = ObserverRecoveryAggregate::new();
    for conversation_id in CONVERSATIONS {
        aggregate
            .track(conversation_id, INITIAL_PROGRESS)
            .expect("fresh conversations track once");
    }
    aggregate
}

fn commit_transaction(
    decision: ObserverRecoveryTransactionDecision,
) -> ObserverRecoveryTransaction {
    match decision {
        ObserverRecoveryTransactionDecision::Commit(transaction) => transaction,
        ObserverRecoveryTransactionDecision::Respond { response, .. } => {
            panic!("a valid batch must commit, refused with {response:?}")
        }
    }
}

fn advance_transaction(
    decision: ObserverProgressAdvanceDecision,
) -> ObserverProgressAdvanceTransaction {
    match decision {
        ObserverProgressAdvanceDecision::Commit(transaction) => transaction,
        ObserverProgressAdvanceDecision::Refuse { error, .. } => {
            panic!("a valid advance must commit, refused with {error:?}")
        }
    }
}

fn refused_advance(
    decision: ObserverProgressAdvanceDecision,
) -> (ObserverRecoveryAggregate, ObserverProgressAdvanceError) {
    match decision {
        ObserverProgressAdvanceDecision::Refuse { aggregate, error } => (aggregate, error),
        ObserverProgressAdvanceDecision::Commit(transaction) => {
            panic!("an invalid advance must refuse, committed {transaction:?}")
        }
    }
}

#[test]
fn track_registers_once_and_refuses_duplicates() {
    let mut aggregate = ObserverRecoveryAggregate::new();
    assert_eq!(aggregate.observer_progress(101), None);
    aggregate.track(101, 5).expect("fresh conversation tracks");
    assert_eq!(aggregate.observer_progress(101), Some(5));
    assert_eq!(
        aggregate.track(101, 6),
        Err(ObserverProgressTrackError::AlreadyTracked {
            conversation_id: 101
        })
    );
    assert_eq!(
        aggregate.observer_progress(101),
        Some(5),
        "a refused duplicate track must not change the authoritative row"
    );
}

#[test]
fn advance_requires_a_known_conversation_and_strict_progress() {
    let aggregate = tracked_aggregate();
    let (aggregate, error) = refused_advance(aggregate.decide_progress_advance(999, 11));
    assert_eq!(
        error,
        ObserverProgressAdvanceError::ConversationUnknown {
            conversation_id: 999
        }
    );
    let (aggregate, error) =
        refused_advance(aggregate.decide_progress_advance(101, INITIAL_PROGRESS));
    assert_eq!(
        error,
        ObserverProgressAdvanceError::NotAdvancing {
            conversation_id: 101,
            current_observer_progress: INITIAL_PROGRESS,
            presented_progress: INITIAL_PROGRESS,
        }
    );
    let (aggregate, error) =
        refused_advance(aggregate.decide_progress_advance(101, INITIAL_PROGRESS - 1));
    assert_eq!(
        error,
        ObserverProgressAdvanceError::NotAdvancing {
            conversation_id: 101,
            current_observer_progress: INITIAL_PROGRESS,
            presented_progress: INITIAL_PROGRESS - 1,
        }
    );
    assert_eq!(
        aggregate.observer_progress(101),
        Some(INITIAL_PROGRESS),
        "a refused advance must return the aggregate unchanged"
    );

    let transaction =
        advance_transaction(aggregate.decide_progress_advance(101, INITIAL_PROGRESS + 1));
    assert_eq!(transaction.conversation_id(), 101);
    assert_eq!(transaction.presented_progress(), INITIAL_PROGRESS + 1);
    assert_eq!(
        transaction.fired_arm(),
        None,
        "advancing an unarmed conversation plans no fire"
    );
    let (aggregate, fired) = transaction.commit();
    assert_eq!(
        fired, None,
        "advancing an unarmed conversation fires nothing"
    );
    assert_eq!(aggregate.observer_progress(101), Some(INITIAL_PROGRESS + 1));
}

#[test]
fn aborted_advance_leaves_progress_and_arm_untouched() {
    let (aggregate, _) = commit_transaction(tracked_aggregate().decide_recovery(
        &request(&[(101, INITIAL_PROGRESS)]),
        LIMIT,
        LIMIT,
        &CONVERSATIONS,
    ))
    .commit();
    let before_progress = aggregate.progress_rows();
    let before_armed = aggregate.armed_rows();

    // Crash between the decision and the durable append: abort applies
    // neither the progress write nor the arm fire.
    let transaction =
        advance_transaction(aggregate.decide_progress_advance(101, INITIAL_PROGRESS + 2));
    let planned = transaction
        .fired_arm()
        .expect("the pending advance plans the installed arm's fire");
    assert_eq!(planned.conversation_id(), 101);
    assert_eq!(planned.refused_epoch(), INITIAL_PROGRESS);
    let aggregate = transaction.abort();
    assert_eq!(
        aggregate.progress_rows(),
        before_progress,
        "an aborted advance must not leave live progress ahead of durable state"
    );
    assert_eq!(
        aggregate.armed_rows(),
        before_armed,
        "an aborted advance must not surrender the installed arm"
    );

    // The untouched aggregate still answers a replayed advance identically.
    let transaction =
        advance_transaction(aggregate.decide_progress_advance(101, INITIAL_PROGRESS + 2));
    let (aggregate, fired) = transaction.commit();
    let arm = fired.expect("the replayed advance fires the still-installed arm");
    assert_eq!(arm.conversation_id(), 101);
    assert_eq!(arm.refused_epoch(), INITIAL_PROGRESS);
    assert_eq!(aggregate.observer_progress(101), Some(INITIAL_PROGRESS + 2));
    assert_eq!(aggregate.armed_epoch(101), None);
}

#[test]
fn refused_recovery_returns_the_aggregate_unchanged() {
    let aggregate = tracked_aggregate();
    let before_progress = aggregate.progress_rows();
    let before_armed = aggregate.armed_rows();
    let decision = aggregate.decide_recovery(
        &request(&[(101, INITIAL_PROGRESS + 1)]),
        LIMIT,
        LIMIT,
        &CONVERSATIONS,
    );
    let ObserverRecoveryTransactionDecision::Respond {
        aggregate,
        response,
    } = decision
    else {
        panic!("an ahead epoch must refuse the whole batch");
    };
    assert_eq!(
        response,
        ObserverRecoveryResponse::invalid_observer_epoch(InvalidObserverEpoch::EpochAhead {
            conversation_id: 101,
            presented_epoch: INITIAL_PROGRESS + 1,
            current_observer_progress: INITIAL_PROGRESS,
        })
    );
    assert_eq!(aggregate.progress_rows(), before_progress);
    assert_eq!(aggregate.armed_rows(), before_armed);
}

#[test]
fn abort_installs_no_arm_and_commit_installs_the_whole_plan() {
    let aggregate = tracked_aggregate();
    let batch = [(101, INITIAL_PROGRESS), (102, INITIAL_PROGRESS)];

    // Crash between the durable append and installation: abort installs none.
    let transaction = commit_transaction(aggregate.decide_recovery(
        &request(&batch),
        LIMIT,
        LIMIT,
        &CONVERSATIONS,
    ));
    assert_eq!(transaction.arms().len(), 2);
    let aggregate = transaction.abort();
    assert!(
        aggregate.armed_rows().is_empty(),
        "an aborted transaction must not leave a partially-armed request"
    );

    // Confirmed durable append: commit installs every planned arm at once.
    let transaction = commit_transaction(aggregate.decide_recovery(
        &request(&batch),
        LIMIT,
        LIMIT,
        &CONVERSATIONS,
    ));
    let planned: Vec<_> = transaction
        .arms()
        .iter()
        .map(|arm| (arm.conversation_id(), arm.refused_epoch()))
        .collect();
    let (aggregate, outcome) = transaction.commit();
    assert_eq!(aggregate.armed_rows(), planned);
    assert_eq!(
        outcome.statuses,
        vec![
            ObserverProgressStatus {
                conversation_id: 101,
                refused_epoch: INITIAL_PROGRESS,
                current_observer_progress: INITIAL_PROGRESS,
                armed: true,
                progressed: false,
            },
            ObserverProgressStatus {
                conversation_id: 102,
                refused_epoch: INITIAL_PROGRESS,
                current_observer_progress: INITIAL_PROGRESS,
                armed: true,
                progressed: false,
            },
        ],
    );
}

#[test]
fn replaying_a_committed_recovery_is_idempotent() {
    let batch = [(101, INITIAL_PROGRESS)];
    let (aggregate, _) = commit_transaction(tracked_aggregate().decide_recovery(
        &request(&batch),
        LIMIT,
        LIMIT,
        &CONVERSATIONS,
    ))
    .commit();
    let before_progress = aggregate.progress_rows();
    let before_armed = aggregate.armed_rows();
    let (aggregate, _) = commit_transaction(aggregate.decide_recovery(
        &request(&batch),
        LIMIT,
        LIMIT,
        &CONVERSATIONS,
    ))
    .commit();
    assert_eq!(aggregate.progress_rows(), before_progress);
    assert_eq!(
        aggregate.armed_rows(),
        before_armed,
        "replay against the post-state must not double-install the arm"
    );
}

#[test]
fn advancing_past_an_installed_arm_fires_it_exactly_once() {
    let (aggregate, _) = commit_transaction(tracked_aggregate().decide_recovery(
        &request(&[(101, INITIAL_PROGRESS)]),
        LIMIT,
        LIMIT,
        &CONVERSATIONS,
    ))
    .commit();
    assert_eq!(aggregate.armed_epoch(101), Some(INITIAL_PROGRESS));

    let (aggregate, fired) =
        advance_transaction(aggregate.decide_progress_advance(101, INITIAL_PROGRESS + 2)).commit();
    let arm = fired.expect("advancing past the armed epoch must fire the arm");
    assert_eq!(arm.conversation_id(), 101);
    assert_eq!(arm.refused_epoch(), INITIAL_PROGRESS);
    assert_eq!(aggregate.armed_epoch(101), None);

    let (_, fired_again) =
        advance_transaction(aggregate.decide_progress_advance(101, INITIAL_PROGRESS + 3)).commit();
    assert_eq!(fired_again, None, "an arm fires exactly once");
}

#[test]
fn restore_round_trips_and_rejects_every_corruption_class() {
    let (aggregate, _) = commit_transaction(tracked_aggregate().decide_recovery(
        &request(&[(102, INITIAL_PROGRESS)]),
        LIMIT,
        LIMIT,
        &CONVERSATIONS,
    ))
    .commit();
    let restored =
        ObserverRecoveryAggregate::restore(&aggregate.progress_rows(), &aggregate.armed_rows())
            .expect("a durable snapshot of a live aggregate restores");
    assert_eq!(restored, aggregate);

    assert_eq!(
        ObserverRecoveryAggregate::restore(&[(101, 5), (101, 6)], &[]),
        Err(ObserverRecoveryAggregateRestoreError::DuplicateProgress {
            conversation_id: 101
        })
    );
    assert_eq!(
        ObserverRecoveryAggregate::restore(&[(101, 5)], &[(101, 5), (101, 5)]),
        Err(ObserverRecoveryAggregateRestoreError::DuplicateArm {
            conversation_id: 101
        })
    );
    assert_eq!(
        ObserverRecoveryAggregate::restore(&[(101, 5)], &[(102, 5)]),
        Err(ObserverRecoveryAggregateRestoreError::ArmWithoutProgress {
            conversation_id: 102
        })
    );
    assert_eq!(
        ObserverRecoveryAggregate::restore(&[(101, 5)], &[(101, 4)]),
        Err(ObserverRecoveryAggregateRestoreError::ArmEpochMismatch {
            conversation_id: 101,
            armed_epoch: 4,
            current_observer_progress: 5,
        })
    );
}

/// One generated interleaving step: an acknowledgement/fate progress advance
/// or an observer-recovery batch, each of which either commits or crashes
/// (aborts) between the durable-append decision and its application.
#[derive(Clone, Debug)]
enum ModelOp {
    Advance {
        slot: usize,
        delta: u64,
        crash: bool,
    },
    Recover {
        entries: Vec<(usize, u8)>,
        crash: bool,
    },
}

fn op_strategy() -> impl Strategy<Value = ModelOp> {
    prop_oneof![
        (0_usize..CONVERSATIONS.len(), 1_u64..4, any::<bool>())
            .prop_map(|(slot, delta, crash)| ModelOp::Advance { slot, delta, crash }),
        (
            proptest::collection::vec((0_usize..CONVERSATIONS.len(), 0_u8..3), 0..4),
            any::<bool>(),
        )
            .prop_map(|(entries, crash)| ModelOp::Recover { entries, crash }),
    ]
}

fn assert_equal_epoch_invariant(aggregate: &ObserverRecoveryAggregate) {
    for (conversation_id, armed_epoch) in aggregate.armed_rows() {
        assert_eq!(
            aggregate.observer_progress(conversation_id),
            Some(armed_epoch),
            "every installed arm is equal-epoch with its conversation's progress",
        );
    }
}

proptest! {
    /// Arbitrary interleavings of recovery with acknowledgement/fate progress
    /// never double-install or half-install arms, and every intermediate
    /// durable state restores to exactly the live aggregate (crash replay
    /// from any durable point converges).
    #[test]
    fn interleavings_never_split_arm_installation_and_durable_replay_converges(
        ops in proptest::collection::vec(op_strategy(), 0..24)
    ) {
        let mut live = tracked_aggregate();
        // The durable mirror: what a server binding has atomically persisted.
        let mut durable_progress: BTreeMap<ConversationId, DeliverySeq> =
            CONVERSATIONS.iter().map(|id| (*id, INITIAL_PROGRESS)).collect();
        let mut durable_arms: BTreeMap<ConversationId, ObserverEpoch> = BTreeMap::new();

        for op in ops {
            match op {
                ModelOp::Advance { slot, delta, crash } => {
                    let conversation_id = CONVERSATIONS[slot];
                    let current = live
                        .observer_progress(conversation_id)
                        .expect("model conversations stay tracked");
                    let presented = current + delta;
                    let had_arm = live.armed_epoch(conversation_id);
                    let before_progress = live.progress_rows();
                    let before_armed = live.armed_rows();

                    let transaction = advance_transaction(
                        live.decide_progress_advance(conversation_id, presented),
                    );
                    // The plan fires an arm exactly when one was installed,
                    // and the planned arm names the exact installed epoch.
                    match (had_arm, transaction.fired_arm()) {
                        (Some(epoch), Some(arm)) => {
                            prop_assert_eq!(arm.conversation_id(), conversation_id);
                            prop_assert_eq!(arm.refused_epoch(), epoch);
                        }
                        (None, None) => {}
                        (had, planned) => {
                            panic!("arm/fire disagreement: installed {had:?}, planned {planned:?}");
                        }
                    }
                    live = if crash {
                        // Crash between the decision and the durable append:
                        // neither progress nor arm changes, live or durable.
                        let aggregate = transaction.abort();
                        prop_assert_eq!(aggregate.progress_rows(), before_progress);
                        prop_assert_eq!(aggregate.armed_rows(), before_armed);
                        aggregate
                    } else {
                        let planned = transaction.fired_arm();
                        let (aggregate, fired) = transaction.commit();
                        prop_assert_eq!(&fired, &planned, "commit surrenders the exact plan");
                        // One durable transaction: progress advance + arm
                        // removal + wake.
                        durable_progress.insert(conversation_id, presented);
                        if fired.is_some() {
                            durable_arms.remove(&conversation_id);
                        }
                        aggregate
                    };
                }
                ModelOp::Recover { entries, crash } => {
                    let batch: Vec<(ConversationId, ObserverEpoch)> = entries
                        .iter()
                        .map(|(slot, select)| {
                            let conversation_id = CONVERSATIONS[*slot];
                            let current = live
                                .observer_progress(conversation_id)
                                .expect("model conversations stay tracked");
                            let epoch = match select {
                                0 => current,
                                1 => current - 1,
                                _ => current + 1,
                            };
                            (conversation_id, epoch)
                        })
                        .collect();
                    let mut seen = Vec::new();
                    let mut duplicate = false;
                    for (conversation_id, _) in &batch {
                        if seen.contains(conversation_id) {
                            duplicate = true;
                        }
                        seen.push(*conversation_id);
                    }
                    let ahead = entries.iter().any(|(_, select)| *select >= 2);
                    let before_progress = live.progress_rows();
                    let before_armed = live.armed_rows();

                    let decision =
                        live.decide_recovery(&request(&batch), LIMIT, LIMIT, &CONVERSATIONS);
                    live = match decision {
                        ObserverRecoveryTransactionDecision::Respond { aggregate, .. } => {
                            prop_assert!(
                                duplicate || ahead,
                                "an all-valid batch must not be refused"
                            );
                            prop_assert_eq!(aggregate.progress_rows(), before_progress.clone());
                            prop_assert_eq!(aggregate.armed_rows(), before_armed.clone());
                            aggregate
                        }
                        ObserverRecoveryTransactionDecision::Commit(transaction) => {
                            prop_assert!(
                                !(duplicate || ahead),
                                "a refused class must not reach the arm plan"
                            );
                            let planned: Vec<_> = transaction
                                .arms()
                                .iter()
                                .map(|arm| (arm.conversation_id(), arm.refused_epoch()))
                                .collect();
                            // Exactly the equal-epoch entries are planned.
                            let expected: Vec<_> = batch
                                .iter()
                                .copied()
                                .filter(|(conversation_id, epoch)| {
                                    live_progress_of(&before_progress, *conversation_id)
                                        == Some(*epoch)
                                })
                                .collect();
                            prop_assert_eq!(&planned, &expected);
                            if crash {
                                // Crash between durable append and install:
                                // nothing is installed, nothing is durable.
                                let aggregate = transaction.abort();
                                prop_assert_eq!(
                                    aggregate.progress_rows(),
                                    before_progress.clone()
                                );
                                prop_assert_eq!(aggregate.armed_rows(), before_armed.clone());
                                aggregate
                            } else {
                                let (aggregate, _outcome) = transaction.commit();
                                // Whole-plan installation, never a subset.
                                for (conversation_id, epoch) in &planned {
                                    prop_assert_eq!(
                                        aggregate.armed_epoch(*conversation_id),
                                        Some(*epoch)
                                    );
                                }
                                // One durable transaction persists the plan.
                                for (conversation_id, epoch) in planned {
                                    durable_arms.insert(conversation_id, epoch);
                                }
                                aggregate
                            }
                        }
                    };
                }
            }

            // No arm is ever installed for a conversation whose progress
            // moved past it, and at most one arm exists per conversation
            // (the row list is keyed by conversation).
            assert_equal_epoch_invariant(&live);

            // Crash replay from this durable point converges: restoring the
            // durable mirror reproduces the live aggregate exactly.
            let progress_rows: Vec<_> = durable_progress
                .iter()
                .map(|(conversation_id, progress)| (*conversation_id, *progress))
                .collect();
            let armed_rows: Vec<_> = durable_arms
                .iter()
                .map(|(conversation_id, epoch)| (*conversation_id, *epoch))
                .collect();
            let restored = ObserverRecoveryAggregate::restore(&progress_rows, &armed_rows)
                .expect("every intermediate durable state validates");
            prop_assert_eq!(&restored, &live);
        }
    }
}

fn live_progress_of(
    rows: &[(ConversationId, DeliverySeq)],
    conversation_id: ConversationId,
) -> Option<DeliverySeq> {
    rows.iter()
        .find(|(row_conversation, _)| *row_conversation == conversation_id)
        .map(|(_, progress)| *progress)
}
