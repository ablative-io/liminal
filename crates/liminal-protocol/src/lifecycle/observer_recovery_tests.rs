#![allow(clippy::panic)]

use alloc::{vec, vec::Vec};
use core::cell::Cell;

use crate::wire::{
    ConnectionConversationCapacityExceeded, InvalidObserverEpoch, InvalidObserverEpochList,
    ObserverProgressStatus, ObserverRecoveryAccepted, ObserverRecoveryHandshake, ObserverRefusal,
    ServerValue,
};

use super::{ObserverRecoveryDecision, apply_observer_recovery};

fn request(entries: &[(u64, u64)]) -> ObserverRecoveryHandshake {
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

#[test]
fn over_limit_beats_duplicate_without_progress_lookup() {
    let progress_lookups = Cell::new(0);
    let too_many = apply_observer_recovery(&request(&[(7, 1), (7, 2)]), 1, 0, &[], |_| {
        progress_lookups.set(progress_lookups.get() + 1);
        None
    });
    assert_eq!(
        too_many,
        ObserverRecoveryDecision::Respond(ServerValue::InvalidObserverEpochList(
            InvalidObserverEpochList::TooManyEntries {
                presented_entries: 2,
                max_entries: 1,
            }
        ))
    );
    assert_eq!(progress_lookups.get(), 0);
}

#[test]
fn first_request_order_duplicate_precedes_capacity_and_progress_lookup() {
    let progress_lookups = Cell::new(0);
    let duplicate = apply_observer_recovery(
        &request(&[(7, 1), (8, 1), (7, 2), (8, 2)]),
        4,
        0,
        &[],
        |_| {
            progress_lookups.set(progress_lookups.get() + 1);
            None
        },
    );
    assert_eq!(
        duplicate,
        ObserverRecoveryDecision::Respond(ServerValue::InvalidObserverEpochList(
            InvalidObserverEpochList::DuplicateConversation {
                conversation_id: 7,
                first_index: 0,
                duplicate_index: 2,
            }
        ))
    );
    assert_eq!(progress_lookups.get(), 0);
}

#[test]
fn request_order_capacity_precedes_unknown_and_ahead() {
    let progress_lookups = Cell::new(0);
    let capacity = apply_observer_recovery(
        &request(&[(11, 5), (12, 6)]),
        2,
        3,
        &[90, 91],
        |conversation_id| {
            progress_lookups.set(progress_lookups.get() + 1);
            match conversation_id {
                11 | 12 => Some(5),
                _ => None,
            }
        },
    );
    assert_eq!(
        capacity,
        ObserverRecoveryDecision::Respond(ServerValue::ConnectionConversationCapacityExceeded(
            ConnectionConversationCapacityExceeded::ObserverRecovery {
                conversation_id: 12,
                limit: 3,
            }
        ))
    );
    assert_eq!(progress_lookups.get(), 0);

    let reversed = apply_observer_recovery(&request(&[(12, 6), (11, 5)]), 2, 3, &[90, 91], |_| {
        panic!("capacity preflight must run before epoch lookup")
    });
    assert_eq!(
        reversed,
        ObserverRecoveryDecision::Respond(ServerValue::ConnectionConversationCapacityExceeded(
            ConnectionConversationCapacityExceeded::ObserverRecovery {
                conversation_id: 11,
                limit: 3,
            }
        ))
    );
}

#[test]
fn tracked_conversation_adds_zero_occupancy() {
    let decision = apply_observer_recovery(&request(&[(11, 5)]), 1, 1, &[11], |id| {
        (id == 11).then_some(5)
    });
    let ObserverRecoveryDecision::Commit(commit) = decision else {
        panic!("an already tracked conversation must fit at the limit");
    };
    assert_eq!(commit.arms().len(), 1);
    assert_eq!(commit.arms()[0].conversation_id(), 11);
}

#[test]
fn unknown_and_ahead_are_selected_by_request_index() {
    let ahead_first = apply_observer_recovery(
        &request(&[(31, 6), (32, 5)]),
        2,
        2,
        &[],
        |conversation_id| (conversation_id == 31).then_some(5),
    );
    assert_eq!(
        ahead_first,
        ObserverRecoveryDecision::Respond(ServerValue::InvalidObserverEpoch(
            InvalidObserverEpoch::EpochAhead {
                conversation_id: 31,
                presented_epoch: 6,
                current_observer_progress: 5,
            },
        )),
    );

    let unknown_first = apply_observer_recovery(
        &request(&[(32, 5), (31, 6)]),
        2,
        2,
        &[],
        |conversation_id| (conversation_id == 31).then_some(5),
    );
    assert_eq!(
        unknown_first,
        ObserverRecoveryDecision::Respond(ServerValue::InvalidObserverEpoch(
            InvalidObserverEpoch::ConversationUnknown {
                conversation_id: 32,
                presented_epoch: 5,
            },
        )),
    );
}

#[test]
fn accepted_batch_preserves_order_and_arms_only_equal_epochs() {
    let decision = apply_observer_recovery(
        &request(&[(11, 4), (12, 5)]),
        2,
        2,
        &[],
        |conversation_id| matches!(conversation_id, 11 | 12).then_some(5),
    );
    let ObserverRecoveryDecision::Commit(commit) = decision else {
        panic!("valid batch must commit");
    };
    assert_eq!(commit.arms().len(), 1);
    assert_eq!(commit.arms()[0].conversation_id(), 12);
    assert_eq!(commit.arms()[0].refused_epoch(), 5);
    assert_eq!(
        commit.outcome(),
        &ObserverRecoveryAccepted {
            statuses: vec![
                ObserverProgressStatus {
                    conversation_id: 11,
                    refused_epoch: 4,
                    current_observer_progress: 5,
                    armed: false,
                    progressed: true,
                },
                ObserverProgressStatus {
                    conversation_id: 12,
                    refused_epoch: 5,
                    current_observer_progress: 5,
                    armed: true,
                    progressed: false,
                },
            ],
        },
    );
}

#[test]
fn later_epoch_failure_produces_no_partial_arm_plan() {
    let lookups = Cell::new(0);
    let decision = apply_observer_recovery(
        &request(&[(21, 4), (22, 7)]),
        2,
        2,
        &[],
        |conversation_id| {
            lookups.set(lookups.get() + 1);
            match conversation_id {
                21 => Some(4),
                22 => Some(6),
                _ => None,
            }
        },
    );
    assert_eq!(
        decision,
        ObserverRecoveryDecision::Respond(ServerValue::InvalidObserverEpoch(
            InvalidObserverEpoch::EpochAhead {
                conversation_id: 22,
                presented_epoch: 7,
                current_observer_progress: 6,
            }
        ))
    );
    assert_eq!(lookups.get(), 2);
}

#[test]
fn empty_batch_is_an_empty_commit() {
    let decision = apply_observer_recovery(&request(&[]), 3, 3, &[], |_| None);
    let ObserverRecoveryDecision::Commit(commit) = decision else {
        panic!("empty recovery is accepted");
    };
    assert!(commit.arms().is_empty());
    assert_eq!(
        commit.outcome(),
        &ObserverRecoveryAccepted {
            statuses: Vec::new(),
        },
    );
}
