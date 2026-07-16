use alloc::collections::{BTreeMap, BTreeSet};
use alloc::vec::Vec;

use crate::wire::{
    ConversationId, DeliverySeq, InvalidObserverEpoch, InvalidObserverEpochList, ObserverEpoch,
    ObserverProgressStatus, ObserverRecoveryAccepted, ObserverRecoveryHandshake,
    ObserverRecoveryResponse,
};

/// Restore-time validation failure for the owned observer-recovery aggregate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObserverRecoveryAggregateRestoreError {
    /// One conversation appears twice in the durable progress rows.
    DuplicateProgress {
        /// Duplicated conversation.
        conversation_id: ConversationId,
    },
    /// One conversation appears twice in the durable arm rows.
    DuplicateArm {
        /// Duplicated conversation.
        conversation_id: ConversationId,
    },
    /// A durable arm names a conversation with no durable progress row.
    ArmWithoutProgress {
        /// Conversation named by the orphaned arm.
        conversation_id: ConversationId,
    },
    /// A durable arm's epoch differs from its conversation's progress.
    ///
    /// An installed arm is always equal-epoch: it is installed at the exact
    /// read progress and fired (removed) by the same mutation that advances
    /// progress past it, so any durable disagreement is corruption.
    ArmEpochMismatch {
        /// Conversation whose arm disagrees.
        conversation_id: ConversationId,
        /// Epoch stored with the arm.
        armed_epoch: ObserverEpoch,
        /// Durable hard observer progress.
        current_observer_progress: DeliverySeq,
    },
}

/// Failure while registering a newly tracked conversation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObserverProgressTrackError {
    /// The conversation already has an authoritative progress row.
    AlreadyTracked {
        /// Conversation presented twice.
        conversation_id: ConversationId,
    },
}

/// Failure while advancing hard observer progress.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObserverProgressAdvanceError {
    /// The conversation has no authoritative progress row.
    ConversationUnknown {
        /// Unknown conversation.
        conversation_id: ConversationId,
    },
    /// The presented progress does not strictly advance the current value.
    NotAdvancing {
        /// Conversation whose progress was presented.
        conversation_id: ConversationId,
        /// Current hard observer progress.
        current_observer_progress: DeliverySeq,
        /// Non-advancing presented progress.
        presented_progress: DeliverySeq,
    },
}

/// Exclusively owned observer-recovery aggregate: per-conversation hard
/// observer progress plus every installed equal-epoch arm, as ONE owned unit.
///
/// This is the A4 transactional surface
/// (`docs/design/LP-GAP-CLOSURE-GOAL.md`): the equal-epoch progress read and
/// the arm installation share one serialization boundary *by construction*,
/// because [`Self::decide_recovery`] consumes the aggregate and only
/// [`ObserverRecoveryTransaction::commit`] or
/// [`ObserverRecoveryTransaction::abort`] returns it. No acknowledgement or
/// binding fate can advance progress between the read and the installation,
/// and a crash while the transaction is pending installs nothing.
///
/// The aggregate is deliberately not `Clone`: at most one owner may read
/// progress for arm selection.
///
/// ```compile_fail
/// use liminal_protocol::lifecycle::ObserverRecoveryAggregate;
///
/// fn require_clone<T: Clone>() {}
/// require_clone::<ObserverRecoveryAggregate>();
/// ```
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ObserverRecoveryAggregate {
    progress: BTreeMap<ConversationId, DeliverySeq>,
    armed: BTreeMap<ConversationId, ObserverEpoch>,
}

impl ObserverRecoveryAggregate {
    /// Creates an empty aggregate tracking no conversations.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            progress: BTreeMap::new(),
            armed: BTreeMap::new(),
        }
    }

    /// Rebuilds the aggregate from durable progress and arm rows.
    ///
    /// # Errors
    ///
    /// Returns [`ObserverRecoveryAggregateRestoreError`] for duplicate rows,
    /// an arm without its progress row, or an arm whose epoch is not the
    /// exact current progress of its conversation.
    pub fn restore(
        progress_rows: &[(ConversationId, DeliverySeq)],
        armed_rows: &[(ConversationId, ObserverEpoch)],
    ) -> Result<Self, ObserverRecoveryAggregateRestoreError> {
        let mut progress = BTreeMap::new();
        for (conversation_id, observer_progress) in progress_rows {
            if progress
                .insert(*conversation_id, *observer_progress)
                .is_some()
            {
                return Err(ObserverRecoveryAggregateRestoreError::DuplicateProgress {
                    conversation_id: *conversation_id,
                });
            }
        }
        let mut armed = BTreeMap::new();
        for (conversation_id, armed_epoch) in armed_rows {
            let Some(current_observer_progress) = progress.get(conversation_id).copied() else {
                return Err(ObserverRecoveryAggregateRestoreError::ArmWithoutProgress {
                    conversation_id: *conversation_id,
                });
            };
            if current_observer_progress != *armed_epoch {
                return Err(ObserverRecoveryAggregateRestoreError::ArmEpochMismatch {
                    conversation_id: *conversation_id,
                    armed_epoch: *armed_epoch,
                    current_observer_progress,
                });
            }
            if armed.insert(*conversation_id, *armed_epoch).is_some() {
                return Err(ObserverRecoveryAggregateRestoreError::DuplicateArm {
                    conversation_id: *conversation_id,
                });
            }
        }
        Ok(Self { progress, armed })
    }

    /// Returns the current hard observer progress for one conversation.
    #[must_use]
    pub fn observer_progress(&self, conversation_id: ConversationId) -> Option<DeliverySeq> {
        self.progress.get(&conversation_id).copied()
    }

    /// Returns the installed equal-epoch arm for one conversation, if any.
    #[must_use]
    pub fn armed_epoch(&self, conversation_id: ConversationId) -> Option<ObserverEpoch> {
        self.armed.get(&conversation_id).copied()
    }

    /// Returns every durable progress row in conversation order.
    #[must_use]
    pub fn progress_rows(&self) -> Vec<(ConversationId, DeliverySeq)> {
        self.progress
            .iter()
            .map(|(conversation_id, observer_progress)| (*conversation_id, *observer_progress))
            .collect()
    }

    /// Returns every installed arm row in conversation order.
    #[must_use]
    pub fn armed_rows(&self) -> Vec<(ConversationId, ObserverEpoch)> {
        self.armed
            .iter()
            .map(|(conversation_id, armed_epoch)| (*conversation_id, *armed_epoch))
            .collect()
    }

    /// Registers a newly tracked conversation at its current progress.
    ///
    /// # Errors
    ///
    /// Returns [`ObserverProgressTrackError::AlreadyTracked`] without any
    /// change when the conversation already has an authoritative row.
    pub fn track(
        &mut self,
        conversation_id: ConversationId,
        observer_progress: DeliverySeq,
    ) -> Result<(), ObserverProgressTrackError> {
        if self.progress.contains_key(&conversation_id) {
            return Err(ObserverProgressTrackError::AlreadyTracked { conversation_id });
        }
        self.progress.insert(conversation_id, observer_progress);
        Ok(())
    }

    /// Advances one conversation's hard observer progress and fires its arm.
    ///
    /// This is the acknowledgement/binding-fate feed. When the conversation
    /// carries an installed equal-epoch arm, strictly advancing past it fires
    /// the arm: the arm is removed and returned so the caller wakes the
    /// parked rows in the same durable transaction (LAW-1: wake on the event,
    /// never poll).
    ///
    /// # Errors
    ///
    /// Returns [`ObserverProgressAdvanceError`] without any change for an
    /// unknown conversation or a presented value that does not strictly
    /// advance the current progress.
    pub fn advance_progress(
        &mut self,
        conversation_id: ConversationId,
        presented_progress: DeliverySeq,
    ) -> Result<Option<ObserverRecoveryArm>, ObserverProgressAdvanceError> {
        let Some(current) = self.progress.get_mut(&conversation_id) else {
            return Err(ObserverProgressAdvanceError::ConversationUnknown { conversation_id });
        };
        if presented_progress <= *current {
            return Err(ObserverProgressAdvanceError::NotAdvancing {
                conversation_id,
                current_observer_progress: *current,
                presented_progress,
            });
        }
        *current = presented_progress;
        let fired = self
            .armed
            .remove(&conversation_id)
            .map(|refused_epoch| ObserverRecoveryArm {
                conversation_id,
                refused_epoch,
            });
        Ok(fired)
    }

    /// Consumes the aggregate into one observer-recovery transaction.
    ///
    /// [`apply_observer_recovery`]'s selection is unchanged; this wrapper
    /// binds its progress reads to the owned aggregate so the read and the
    /// arm installation are one owned unit. A refused batch returns the
    /// aggregate unchanged alongside the exact refusal response.
    #[must_use]
    pub fn decide_recovery(
        self,
        request: &ObserverRecoveryHandshake,
        max_entries: u64,
        connection_conversation_limit: u64,
        tracked_conversations: &[ConversationId],
    ) -> ObserverRecoveryTransactionDecision {
        let decision = apply_observer_recovery(
            request,
            max_entries,
            connection_conversation_limit,
            tracked_conversations,
            |conversation_id| self.progress.get(&conversation_id).copied(),
        );
        match decision {
            ObserverRecoveryDecision::Respond(response) => {
                ObserverRecoveryTransactionDecision::Respond {
                    aggregate: self,
                    response,
                }
            }
            ObserverRecoveryDecision::Commit(commit) => {
                ObserverRecoveryTransactionDecision::Commit(ObserverRecoveryTransaction {
                    aggregate: self,
                    commit,
                })
            }
        }
    }
}

/// Total transactional observer-recovery decision against the owned aggregate.
#[derive(Debug, PartialEq, Eq)]
pub enum ObserverRecoveryTransactionDecision {
    /// The whole batch was refused; the aggregate is unchanged.
    Respond {
        /// Unchanged aggregate returned to its owner.
        aggregate: ObserverRecoveryAggregate,
        /// Exact refusal response.
        response: ObserverRecoveryResponse,
    },
    /// Every entry validated; the arm plan may commit atomically.
    Commit(ObserverRecoveryTransaction),
}

/// Ownership barrier between arm selection and atomic arm installation.
///
/// The aggregate is unreachable while the transaction is pending, so no
/// progress can advance between the equal-epoch read and the installation.
/// Consuming [`Self::commit`] installs every arm of the plan at once — a
/// subset is unrepresentable — and consuming [`Self::abort`] installs none,
/// so a crash at any point leaves either the complete arm plan or no arm at
/// all, never a partially-armed request.
#[derive(Debug, PartialEq, Eq)]
pub struct ObserverRecoveryTransaction {
    aggregate: ObserverRecoveryAggregate,
    commit: ObserverRecoveryCommit,
}

impl ObserverRecoveryTransaction {
    /// Borrows the complete equal-epoch arm plan for the durable append.
    #[must_use]
    pub fn arms(&self) -> &[ObserverRecoveryArm] {
        self.commit.arms()
    }

    /// Borrows the exact request-ordered success response.
    #[must_use]
    pub const fn outcome(&self) -> &ObserverRecoveryAccepted {
        self.commit.outcome()
    }

    /// Installs the whole arm plan after a confirmed durable append.
    ///
    /// Installation is idempotent: replaying a durable recovery against the
    /// post-state reinstalls the identical equal-epoch arms, so at most one
    /// arm per conversation ever exists and crash replay converges.
    #[must_use]
    pub fn commit(mut self) -> (ObserverRecoveryAggregate, ObserverRecoveryAccepted) {
        let (arms, outcome) = self.commit.into_parts();
        for arm in arms {
            self.aggregate
                .armed
                .insert(arm.conversation_id(), arm.refused_epoch());
        }
        (self.aggregate, outcome)
    }

    /// Cancels a failed durable append; not one arm is installed.
    #[must_use]
    pub fn abort(self) -> ObserverRecoveryAggregate {
        self.aggregate
    }
}

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
    Respond(ObserverRecoveryResponse),
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
        return ObserverRecoveryDecision::Respond(
            ObserverRecoveryResponse::invalid_observer_epoch_list(
                InvalidObserverEpochList::TooManyEntries {
                    presented_entries,
                    max_entries,
                },
            ),
        );
    }

    let mut first_indices = BTreeMap::new();
    for (index, refusal) in request.observer_refusals.iter().enumerate() {
        let request_index = wire_count(index);
        if let Some(first_index) = first_indices.insert(refusal.conversation_id, request_index) {
            return ObserverRecoveryDecision::Respond(
                ObserverRecoveryResponse::invalid_observer_epoch_list(
                    InvalidObserverEpochList::DuplicateConversation {
                        conversation_id: refusal.conversation_id,
                        first_index,
                        duplicate_index: request_index,
                    },
                ),
            );
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
                ObserverRecoveryResponse::connection_capacity_exceeded(
                    refusal.conversation_id,
                    connection_conversation_limit,
                ),
            );
        }
        tracked.insert(refusal.conversation_id);
    }

    let mut validated = Vec::with_capacity(request.observer_refusals.len());
    for refusal in &request.observer_refusals {
        let Some(current_observer_progress) = observer_progress(refusal.conversation_id) else {
            return ObserverRecoveryDecision::Respond(
                ObserverRecoveryResponse::invalid_observer_epoch(
                    InvalidObserverEpoch::ConversationUnknown {
                        conversation_id: refusal.conversation_id,
                        presented_epoch: refusal.refused_epoch,
                    },
                ),
            );
        };
        if refusal.refused_epoch > current_observer_progress {
            return ObserverRecoveryDecision::Respond(
                ObserverRecoveryResponse::invalid_observer_epoch(
                    InvalidObserverEpoch::EpochAhead {
                        conversation_id: refusal.conversation_id,
                        presented_epoch: refusal.refused_epoch,
                        current_observer_progress,
                    },
                ),
            );
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
