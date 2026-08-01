//! Boot-recovery candidate-lane drain (F8B `R-BOOT-DRAIN`, `R-BOOT-VERDICT`).
//!
//! `docs/design/F8B-INTENT-DEADLOCK.md` §6.2. Boot recovery empties each
//! restored conversation's immutable-candidate lane BEFORE any retained
//! connection-fate `Open` replays, through the same `persist_drain_first`
//! machinery a live publish uses. Without it a store whose lane rests on a
//! crash-restored pending binding terminal deadlocks: the replayed `Open`
//! must admit a terminal of its own, the occupied lane refuses it
//! (`Precedence`), and nothing in the boot path can clear the occupant.
//!
//! The drain is BOOT-ONLY by construction. It is called from
//! `restore_all_conversations`, never from `replay_and_repair`, which has four
//! live non-boot callers (`handler.rs:350`, `:373`, `:401`;
//! `handler_observer.rs:170`) whose live behaviour must not change.
//!
//! Every attempt ends in a NAMED verdict, never a bare `?` — §6.2
//! R-BOOT-VERDICT — which is why the drain returns [`BootDrainVerdict`]
//! rather than a `Result`. `Drained` and `AlreadyEmpty` proceed to replay;
//! the two refusals refuse the boot loudly, naming the conversation, the
//! candidate shape and this design document, so an operator can tell them
//! from the deadlock they replace.

use liminal_protocol::lifecycle::ImmutableSequenceCandidate;
use liminal_protocol::wire::ConversationId;

use crate::server::participant::{
    BootDrainRefusal, ParticipantSemanticError, dispatch_impact::DispatchImpactAccumulator,
};

use super::handler::{LogAppender, ProductionParticipantHandler};
use super::log::OperationLog;
use super::state::ConversationAuthority;

/// Named outcome of one restored conversation's boot drain attempt (§6.2
/// R-BOOT-VERDICT). The two refusals carry the exact lane head that refused
/// them, so the boot's own error can name the shape it could not repair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum BootDrainVerdict {
    /// The lane held candidates and every one of them drained. N markers need
    /// N drains, so `drains` counts heads removed, not conversations.
    Drained {
        /// Heads removed from this conversation's lane by this boot.
        drains: usize,
    },
    /// The lane was already empty when boot reached it.
    AlreadyEmpty,
    /// §5.3(ii): the lane head is a pending binding terminal under an armed
    /// fenced-attach recovery block. `validate_first_terminal_candidate`
    /// refuses the drain outright while `recovery.is_some()`, and the only
    /// consumer of a recovery block is a live fenced attach that boot cannot
    /// perform — so this store is NOT repairable by the boot drain. The
    /// verdict is the honest answer, not a repair.
    RefusedRecoveryArmed {
        /// Conversation whose lane refused.
        conversation_id: ConversationId,
        /// Exact lane head that refused.
        candidate: ImmutableSequenceCandidate,
    },
    /// Any other drain refusal.
    RefusedShape {
        /// Conversation whose lane refused.
        conversation_id: ConversationId,
        /// Exact lane head that refused.
        candidate: ImmutableSequenceCandidate,
        /// The drain's own refusal, rendered for the operator.
        reason: String,
    },
}

impl BootDrainVerdict {
    /// Whether this boot appended drain rows, so the caller must rebuild the
    /// restored owner against its own enlarged durable log.
    pub(super) const fn drained_any(&self) -> bool {
        matches!(*self, Self::Drained { .. })
    }

    /// Emits the verdict and converts the two refusals into the boot's own
    /// refusal.
    ///
    /// The refusal travels TYPED — [`BootDrainRefusal`] — so a consumer
    /// discriminates recovery-armed from every other shape without reading a
    /// formatted message, exactly as the foundation leg's
    /// `BindingTerminalAdmissionRefused` carrier does for lane occupancy.
    pub(super) fn observe(self) -> Result<(), ParticipantSemanticError> {
        match self {
            Self::Drained { drains } => {
                tracing::info!(
                    drains,
                    "F8B boot drain emptied a restored immutable-candidate lane"
                );
                Ok(())
            }
            Self::AlreadyEmpty => Ok(()),
            Self::RefusedRecoveryArmed {
                conversation_id,
                candidate,
            } => Err(refused(
                conversation_id,
                BootDrainRefusal::RecoveryArmed,
                candidate,
                "the lane rests under an armed fenced-attach recovery block, which only a live \
                 fenced attach can consume"
                    .to_owned(),
            )),
            Self::RefusedShape {
                conversation_id,
                candidate,
                reason,
            } => Err(refused(
                conversation_id,
                BootDrainRefusal::Shape,
                candidate,
                reason,
            )),
        }
    }
}

/// Builds the loud boot refusal and emits it before it travels, so the reason
/// reaches the operator's log even where an outer startup mapping reduces the
/// error to its own phase text.
fn refused(
    conversation_id: ConversationId,
    refusal: BootDrainRefusal,
    candidate: ImmutableSequenceCandidate,
    reason: String,
) -> ParticipantSemanticError {
    let error = ParticipantSemanticError::BootDrainRefused {
        conversation_id,
        refusal,
        candidate: format!("{candidate:?}"),
        reason,
    };
    tracing::error!(%error, "F8B boot drain refused the boot");
    error
}

impl ProductionParticipantHandler {
    /// Drains one restored conversation's immutable-candidate lane to empty
    /// (§6.2 R-BOOT-DRAIN) and returns the attempt's named verdict.
    ///
    /// The head is drained repeatedly until the lane reports empty — N markers
    /// need N drains, because the marker drain removes exactly the head. A
    /// head that survived its own successful drain would spin boot forever, so
    /// a lane that fails to shrink is refused as a shape defect instead of
    /// looped on: nothing in the protocol permits it, and boot must terminate.
    ///
    /// The impact accumulator is fresh and DISCARDED — boot has no connection
    /// to receive an impact; the listener is not bound until after
    /// `SupervisorInner::new` returns.
    pub(super) fn drain_restored_candidate_lane(
        &self,
        conversation_id: ConversationId,
        replayed: &mut ConversationAuthority,
        log: &OperationLog,
    ) -> BootDrainVerdict {
        let appender = LogAppender {
            log,
            registry: &self.registry,
            conversation_id,
        };
        let mut impact = DispatchImpactAccumulator::new();
        let mut drains: usize = 0;
        while let Some(head) = lane_head(replayed) {
            if head.recovery_armed
                && matches!(
                    head.candidate,
                    ImmutableSequenceCandidate::BindingTerminal { .. }
                )
            {
                // §5.3(ii), measured: the terminal drain refuses while any
                // recovery block is armed. A marker head is unaffected — the
                // marker drain does not read `recovery` — so the guard is
                // asked only of a binding-terminal head.
                return BootDrainVerdict::RefusedRecoveryArmed {
                    conversation_id,
                    candidate: head.candidate,
                };
            }
            let owner = match replayed.take_frontier() {
                Ok(owner) => owner,
                Err(error) => {
                    return BootDrainVerdict::RefusedShape {
                        conversation_id,
                        candidate: head.candidate,
                        reason: error.to_string(),
                    };
                }
            };
            if let Err(error) =
                replayed.persist_drain_first(head.candidate, owner, &appender, &mut impact)
            {
                return BootDrainVerdict::RefusedShape {
                    conversation_id,
                    candidate: head.candidate,
                    reason: error.to_string(),
                };
            }
            if lane_head(replayed).is_some_and(|next| next.remaining >= head.remaining) {
                return BootDrainVerdict::RefusedShape {
                    conversation_id,
                    candidate: head.candidate,
                    reason: "the drained head did not leave the immutable-candidate lane"
                        .to_owned(),
                };
            }
            drains = match drains.checked_add(1) {
                Some(drains) => drains,
                None => {
                    return BootDrainVerdict::RefusedShape {
                        conversation_id,
                        candidate: head.candidate,
                        reason: "the boot drain count overflowed".to_owned(),
                    };
                }
            };
        }
        if drains == 0 {
            BootDrainVerdict::AlreadyEmpty
        } else {
            BootDrainVerdict::Drained { drains }
        }
    }
}

/// The lane's head plus the two facts the drain decision needs from it.
struct LaneHead {
    candidate: ImmutableSequenceCandidate,
    recovery_armed: bool,
    remaining: usize,
}

/// Reads the restored owner's lane head. A conversation with no coupled
/// frontier owner has no lane, and boot leaves it exactly as replay left it.
fn lane_head(replayed: &ConversationAuthority) -> Option<LaneHead> {
    let frontiers = replayed.frontier()?.frontiers();
    let candidates = frontiers.sequence().immutable_candidates();
    Some(LaneHead {
        candidate: *candidates.first()?,
        recovery_armed: frontiers.sequence().recovery().is_some()
            || frontiers.order().recovery().is_some(),
        remaining: candidates.len(),
    })
}
