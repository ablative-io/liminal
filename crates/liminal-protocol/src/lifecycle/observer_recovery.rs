use alloc::collections::{BTreeMap, BTreeSet};
use alloc::vec::Vec;

use crate::wire::{
    ConnectionConversationCapacityExceeded, ConversationId, DeliverySeq, InvalidObserverEpoch,
    InvalidObserverEpochList, ObserverEpoch, ObserverProgressStatus, ObserverRecoveryAccepted,
    ObserverRecoveryHandshake, ServerValue,
};

/// One equal observer epoch that must be armed by an accepted recovery batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObserverRecoveryArm {
    conversation_id: ConversationId,
    refused_epoch: ObserverEpoch,
}

impl ObserverRecoveryArm {
    /// Returns the conversation whose progress event must wake the parked rows.
    #[must_use]
    pub const fn conversation_id(self) -> ConversationId {
        self.conversation_id
    }

    /// Returns the exact refusal epoch being armed.
    #[must_use]
    pub const fn refused_epoch(self) -> ObserverEpoch {
        self.refused_epoch
    }
}

/// Whole-batch observer-recovery commit selected after exhaustive validation.
///
/// The arm list and response are produced together. A consumer must persist all
/// arms atomically before sending [`Self::outcome`], so no validation failure or
/// crash can expose a partially armed request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObserverRecoveryCommit {
    arms: Vec<ObserverRecoveryArm>,
    outcome: ObserverRecoveryAccepted,
}

impl ObserverRecoveryCommit {
    /// Borrows the complete equal-epoch arm plan in request order.
    #[must_use]
    pub fn arms(&self) -> &[ObserverRecoveryArm] {
        &self.arms
    }

    /// Borrows the exact request-ordered success response.
    #[must_use]
    pub const fn outcome(&self) -> &ObserverRecoveryAccepted {
        &self.outcome
    }

    /// Consumes the commit into the atomic arm plan and success response.
    #[must_use]
    pub fn into_parts(self) -> (Vec<ObserverRecoveryArm>, ObserverRecoveryAccepted) {
        (self.arms, self.outcome)
    }
}

/// Total observer-recovery decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObserverRecoveryDecision {
    /// Validation or connection-capacity preflight refused the whole batch.
    Respond(ServerValue),
    /// Every entry validated and the complete arm plan may commit atomically.
    Commit(ObserverRecoveryCommit),
}

fn wire_count(value: usize) -> u64 {
    u64::try_from(value).map_or(u64::MAX, core::convert::identity)
}

/// Applies the observer-recovery list, capacity, and epoch precedence.
///
/// Validation order is the frozen R-D1 order: list length, duplicate
/// conversation, request-index connection capacity, then request-index unknown
/// or ahead epoch. Only after every entry passes is an arm plan produced.
/// `observer_progress` must return the current hard observer progress for a
/// known conversation and `None` for an unknown conversation. The progress
/// reads and installation of every returned arm must share one serialization
/// boundary: releasing it between evaluation and installation would violate
/// the equal-epoch subscribe-then-snapshot rule.
#[must_use]
pub fn apply_observer_recovery<F>(
    request: &ObserverRecoveryHandshake,
    max_entries: u64,
    connection_conversation_limit: u64,
    tracked_conversations: &[ConversationId],
    mut observer_progress: F,
) -> ObserverRecoveryDecision
where
    F: FnMut(ConversationId) -> Option<DeliverySeq>,
{
    let presented_entries = wire_count(request.observer_refusals.len());
    if presented_entries > max_entries {
        return ObserverRecoveryDecision::Respond(ServerValue::InvalidObserverEpochList(
            InvalidObserverEpochList::TooManyEntries {
                presented_entries,
                max_entries,
            },
        ));
    }

    let mut first_indices = BTreeMap::new();
    for (index, refusal) in request.observer_refusals.iter().enumerate() {
        let request_index = wire_count(index);
        if let Some(first_index) = first_indices.insert(refusal.conversation_id, request_index) {
            return ObserverRecoveryDecision::Respond(ServerValue::InvalidObserverEpochList(
                InvalidObserverEpochList::DuplicateConversation {
                    conversation_id: refusal.conversation_id,
                    first_index,
                    duplicate_index: request_index,
                },
            ));
        }
    }

    let mut tracked: BTreeSet<_> = tracked_conversations.iter().copied().collect();
    for refusal in &request.observer_refusals {
        if tracked.contains(&refusal.conversation_id) {
            continue;
        }
        let occupied = wire_count(tracked.len());
        if occupied >= connection_conversation_limit {
            return ObserverRecoveryDecision::Respond(
                ServerValue::ConnectionConversationCapacityExceeded(
                    ConnectionConversationCapacityExceeded::ObserverRecovery {
                        conversation_id: refusal.conversation_id,
                        limit: connection_conversation_limit,
                    },
                ),
            );
        }
        tracked.insert(refusal.conversation_id);
    }

    let mut validated = Vec::with_capacity(request.observer_refusals.len());
    for refusal in &request.observer_refusals {
        let Some(current_observer_progress) = observer_progress(refusal.conversation_id) else {
            return ObserverRecoveryDecision::Respond(ServerValue::InvalidObserverEpoch(
                InvalidObserverEpoch::ConversationUnknown {
                    conversation_id: refusal.conversation_id,
                    presented_epoch: refusal.refused_epoch,
                },
            ));
        };
        if refusal.refused_epoch > current_observer_progress {
            return ObserverRecoveryDecision::Respond(ServerValue::InvalidObserverEpoch(
                InvalidObserverEpoch::EpochAhead {
                    conversation_id: refusal.conversation_id,
                    presented_epoch: refusal.refused_epoch,
                    current_observer_progress,
                },
            ));
        }
        validated.push((refusal, current_observer_progress));
    }

    let mut arms = Vec::new();
    let mut statuses = Vec::with_capacity(validated.len());
    for (refusal, current_observer_progress) in validated {
        let armed = refusal.refused_epoch == current_observer_progress;
        if armed {
            arms.push(ObserverRecoveryArm {
                conversation_id: refusal.conversation_id,
                refused_epoch: refusal.refused_epoch,
            });
        }
        statuses.push(ObserverProgressStatus {
            conversation_id: refusal.conversation_id,
            refused_epoch: refusal.refused_epoch,
            current_observer_progress,
            armed,
            progressed: !armed,
        });
    }

    ObserverRecoveryDecision::Commit(ObserverRecoveryCommit {
        arms,
        outcome: ObserverRecoveryAccepted { statuses },
    })
}
