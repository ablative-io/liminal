//! Observer-recovery arms of the production handler (split from
//! [`super::handler`] under the 500-code-line lens).
//!
//! The A4 atomic transaction, idempotent Track registration, and the
//! crash-window repair pre-pass all operate on the handler's server-wide
//! observer aggregate and its durable row log.

use std::collections::BTreeMap;
use std::sync::Arc;

use liminal::durability::bridge::block_on;
use liminal_protocol::lifecycle::{
    ObserverProgressAdvanceDecision, ObserverProgressProjection, ObserverProgressTrackDecision,
    ObserverRecoveryTransactionDecision,
};
use liminal_protocol::wire::{
    ConversationId, ObserverRecoveryHandshake, ObserverRecoveryResponse, ServerValue,
};

use crate::server::participant::{
    ObserverPublication, ObserverPublicationTarget, ParticipantConnectionContext,
    ParticipantConnectionConversations, ParticipantSemanticError,
};

use super::handler::{
    ObserverArmTarget, ObserverOwner, ProductionParticipantHandler, bridge_error, log_error,
    state_error,
};
use super::log::OperationLog;
use super::observer::{ObserverLog, ObserverRow};
use super::state::StateError;

impl ProductionParticipantHandler {
    /// Applies one observer-recovery batch through the A4 atomic transaction.
    ///
    /// The aggregate is owned by the transaction between the progress read
    /// and the arm installation, and the whole arm plan is durably appended
    /// before installation — a crash leaves the complete plan or none.
    pub(super) fn apply_observer_recovery(
        &self,
        context: ParticipantConnectionContext,
        conversations: &mut ParticipantConnectionConversations,
        request: &ObserverRecoveryHandshake,
        target: Option<ObserverPublicationTarget>,
    ) -> Result<ServerValue, ParticipantSemanticError> {
        // Crash-window repair pre-pass: every named conversation's observer
        // registration is made derivable from its own durable conversation
        // log before the batch classifies, so an enrollment whose Track row
        // was lost between the two appends is re-registered idempotently
        // instead of being refused forever.
        for refusal in &request.observer_refusals {
            self.ensure_tracking_from_log(refusal.conversation_id)?;
        }
        // Connection-conversation occupancy is the SAME connection-local
        // dispatch map the semantic stage-6 gate consumes, so the batch
        // preflight and every semantic operation count against one signed
        // bound.
        let tracked = conversations.tracked_conversations();
        let observer_log = ObserverLog::new(Arc::clone(&self.store));
        let mut owner = self
            .observer
            .lock()
            .map_err(|_| ParticipantSemanticError::Internal {
                message: "observer recovery aggregate lock is poisoned".to_owned(),
            })?;
        if owner.is_none() {
            let restored = block_on(observer_log.restore())
                .map_err(|error| bridge_error(&error))?
                .map_err(|error| log_error(&error))?;
            *owner = Some(ObserverOwner {
                aggregate: restored.aggregate,
                head: restored.next_sequence,
                arm_targets: BTreeMap::new(),
            });
        }
        let ObserverOwner {
            aggregate,
            head,
            mut arm_targets,
        } = owner
            .take()
            .ok_or_else(|| ParticipantSemanticError::Internal {
                message: "observer recovery aggregate is absent".to_owned(),
            })?;
        let result = match aggregate.decide_recovery(
            request,
            self.config.observer_recovery_max_entries,
            self.config.max_semantic_conversations_per_connection,
            &tracked,
        ) {
            ObserverRecoveryTransactionDecision::Respond {
                aggregate,
                response,
            } => {
                *owner = Some(ObserverOwner {
                    aggregate,
                    head,
                    arm_targets,
                });
                Ok(response.into_server_value())
            }
            ObserverRecoveryTransactionDecision::Commit(transaction) => {
                let arms = transaction
                    .arms()
                    .iter()
                    .map(|arm| (arm.conversation_id(), arm.refused_epoch()))
                    .collect::<Vec<_>>();
                match block_on(observer_log.append(&ObserverRow::Arms { arms: arms.clone() }, head))
                    .map_err(|error| bridge_error(&error))?
                {
                    Ok(()) => {
                        let (aggregate, outcome) = transaction.commit();
                        let next_head = head.checked_add(1).ok_or_else(|| {
                            ParticipantSemanticError::Internal {
                                message: "observer durable row sequence exhausted".to_owned(),
                            }
                        })?;
                        // Every armed refusal-only recipient occupies one
                        // connection-conversation slot (the batch preflight
                        // already admitted them against the signed bound).
                        for (conversation_id, refused_epoch) in &arms {
                            conversations.track(*conversation_id);
                            if let Some(target) = target.as_ref() {
                                arm_targets.insert(
                                    *conversation_id,
                                    ObserverArmTarget {
                                        refused_epoch: *refused_epoch,
                                        connection_incarnation: context.connection_incarnation(),
                                        target: target.clone(),
                                    },
                                );
                            } else {
                                arm_targets.remove(conversation_id);
                            }
                        }
                        *owner = Some(ObserverOwner {
                            aggregate,
                            head: next_head,
                            arm_targets,
                        });
                        Ok(ObserverRecoveryResponse::accepted(outcome).into_server_value())
                    }
                    Err(error) => {
                        // Nothing installed; the aggregate is durably restored
                        // on the next observer operation.
                        Err(state_error(&StateError::Log(error)))
                    }
                }
            }
        };
        drop(owner);
        result
    }

    /// Registers a conversation's observer progress row on first touch.
    ///
    /// Idempotent: an already-tracked conversation returns unchanged.
    pub(super) fn ensure_observer_tracked(
        &self,
        conversation_id: ConversationId,
    ) -> Result<(), ParticipantSemanticError> {
        let observer_log = ObserverLog::new(Arc::clone(&self.store));
        let mut owner = self
            .observer
            .lock()
            .map_err(|_| ParticipantSemanticError::Internal {
                message: "observer recovery aggregate lock is poisoned".to_owned(),
            })?;
        if owner.is_none() {
            let restored = block_on(observer_log.restore())
                .map_err(|error| bridge_error(&error))?
                .map_err(|error| log_error(&error))?;
            *owner = Some(ObserverOwner {
                aggregate: restored.aggregate,
                head: restored.next_sequence,
                arm_targets: BTreeMap::new(),
            });
        }
        let ObserverOwner {
            aggregate,
            head,
            arm_targets,
        } = owner
            .take()
            .ok_or_else(|| ParticipantSemanticError::Internal {
                message: "observer recovery aggregate is absent".to_owned(),
            })?;
        let result = match aggregate.decide_track(conversation_id, 0) {
            ObserverProgressTrackDecision::Refuse { aggregate, .. } => {
                // Already tracked — the registration is durable.
                *owner = Some(ObserverOwner {
                    aggregate,
                    head,
                    arm_targets,
                });
                Ok(())
            }
            ObserverProgressTrackDecision::Commit(transaction) => {
                match block_on(observer_log.append(
                    &ObserverRow::Track {
                        conversation_id,
                        observer_progress: 0,
                    },
                    head,
                ))
                .map_err(|error| bridge_error(&error))?
                {
                    Ok(()) => {
                        *owner = Some(ObserverOwner {
                            aggregate: transaction.commit(),
                            head: head.saturating_add(1),
                            arm_targets,
                        });
                        Ok(())
                    }
                    Err(error) => Err(state_error(&StateError::Log(error))),
                }
            }
        };
        drop(owner);
        result
    }

    /// Reconciles protocol-produced source projections into durable observer
    /// advances while the observer aggregate remains exclusively serialized.
    ///
    /// The projections arrive in participant replay order after their source
    /// append/flush barriers. An equal-or-greater durable value proves an
    /// earlier live append already closed the crash window; a lower value is
    /// advanced through the aggregate's consuming transaction and only
    /// committed after the `Advance` row flushes.
    pub(super) fn reconcile_observer_progress(
        &self,
        conversation_id: ConversationId,
        projections: Vec<ObserverProgressProjection>,
    ) -> Result<(), ParticipantSemanticError> {
        if projections.is_empty() {
            return Ok(());
        }
        let observer_log = ObserverLog::new(Arc::clone(&self.store));
        let mut owner = self
            .observer
            .lock()
            .map_err(|_| ParticipantSemanticError::Internal {
                message: "observer recovery aggregate lock is poisoned".to_owned(),
            })?;
        if owner.is_none() {
            let restored = block_on(observer_log.restore())
                .map_err(|error| bridge_error(&error))?
                .map_err(|error| log_error(&error))?;
            *owner = Some(ObserverOwner {
                aggregate: restored.aggregate,
                head: restored.next_sequence,
                arm_targets: BTreeMap::new(),
            });
        }
        for projection in projections {
            if projection.conversation_id() != conversation_id {
                return Err(ParticipantSemanticError::Internal {
                    message: format!(
                        "observer projection conversation {} disagrees with source {conversation_id}",
                        projection.conversation_id()
                    ),
                });
            }
            let ObserverOwner {
                aggregate,
                head,
                mut arm_targets,
            } = owner
                .take()
                .ok_or_else(|| ParticipantSemanticError::Internal {
                    message: "observer recovery aggregate is absent".to_owned(),
                })?;
            let presented = projection.new_observer_progress();
            let current = aggregate
                .observer_progress(conversation_id)
                .ok_or_else(|| ParticipantSemanticError::Internal {
                    message: format!(
                        "observer projection names untracked conversation {conversation_id}"
                    ),
                })?;
            if current >= presented {
                *owner = Some(ObserverOwner {
                    aggregate,
                    head,
                    arm_targets,
                });
                continue;
            }
            let ObserverProgressAdvanceDecision::Commit(transaction) =
                aggregate.decide_progress_advance(conversation_id, presented)
            else {
                return Err(ParticipantSemanticError::Internal {
                    message: format!(
                        "observer aggregate refused advancing source for conversation {conversation_id}"
                    ),
                });
            };
            let append = block_on(observer_log.append(
                &ObserverRow::Advance {
                    conversation_id,
                    observer_progress: presented,
                },
                head,
            ))
            .map_err(|error| bridge_error(&error))?;
            match append {
                Ok(()) => {
                    let next_head =
                        head.checked_add(1)
                            .ok_or_else(|| ParticipantSemanticError::Internal {
                                message: "observer durable row sequence exhausted".to_owned(),
                            })?;
                    let (aggregate, fired) = transaction.commit();
                    let publication_result = if let Some(fired) = fired {
                        match arm_targets.remove(&fired.conversation_id()) {
                            Some(association)
                                if association.refused_epoch == fired.refused_epoch() =>
                            {
                                association
                                    .target
                                    .publish(ObserverPublication {
                                        conversation_id: fired.conversation_id(),
                                        refused_epoch: fired.refused_epoch(),
                                        observer_progress: presented,
                                    })
                                    .map(|_| ())
                                    .map_err(|error| ParticipantSemanticError::Internal {
                                        message: format!(
                                            "observer progressed publication failed: {error}"
                                        ),
                                    })
                            }
                            Some(association) => Err(ParticipantSemanticError::Internal {
                                message: format!(
                                    "observer arm target epoch {} on incarnation {:?} disagrees with fired epoch {} for conversation {conversation_id}",
                                    association.refused_epoch,
                                    association.connection_incarnation,
                                    fired.refused_epoch()
                                ),
                            }),
                            None => Ok(()),
                        }
                    } else {
                        Ok(())
                    };
                    *owner = Some(ObserverOwner {
                        aggregate,
                        head: next_head,
                        arm_targets,
                    });
                    publication_result?;
                }
                Err(error) => {
                    *owner = Some(ObserverOwner {
                        aggregate: transaction.abort(),
                        head,
                        arm_targets,
                    });
                    return Err(state_error(&StateError::Log(error)));
                }
            }
        }
        drop(owner);
        Ok(())
    }

    /// Ensures a conversation's observer registration is derivable from its
    /// own durable log (idempotent crash-window repair).
    ///
    /// A never-committed conversation id leaves no residue: its probe cell is
    /// evicted. An already-live enrolled owner re-registers idempotently,
    /// covering an earlier failed Track append inside this process lifetime.
    pub(super) fn ensure_tracking_from_log(
        &self,
        conversation_id: ConversationId,
    ) -> Result<(), ParticipantSemanticError> {
        let cell = self.cell(conversation_id)?;
        let mut owner = cell
            .lock()
            .map_err(|_| ParticipantSemanticError::Internal {
                message: format!(
                    "participant conversation {conversation_id} owner lock is poisoned"
                ),
            })?;
        if owner.is_none() {
            let log = OperationLog::new(Arc::clone(&self.store), conversation_id);
            let replayed = self.replay_and_repair(conversation_id, &log)?;
            if replayed.next_log_sequence == 0 {
                drop(owner);
                return self.evict_uncommitted(conversation_id, &cell);
            }
            *owner = Some(replayed);
            drop(owner);
            return Ok(());
        }
        let enrolled = owner
            .as_ref()
            .is_some_and(|authority| !authority.tokens.is_empty());
        drop(owner);
        if enrolled {
            self.ensure_observer_tracked(conversation_id)?;
        }
        Ok(())
    }
}
