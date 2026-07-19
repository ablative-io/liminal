//! Observer-recovery arms of the production handler (split from
//! [`super::handler`] under the 500-code-line lens).
//!
//! The A4 atomic transaction, idempotent Track registration, and the
//! crash-window repair pre-pass all operate on the handler's server-wide
//! observer aggregate and its durable row log.

use std::collections::BTreeMap;
use std::sync::Arc;

use liminal::durability::bridge::block_on;
use liminal_protocol::lifecycle::{ObserverRecoveryArm, ObserverRecoveryTransactionDecision};
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
        target: Option<&ObserverPublicationTarget>,
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
                                        target: (*target).clone(),
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

    /// Replays one named conversation before observer-recovery classification.
    ///
    /// A never-committed conversation id leaves no residue: its probe cell is
    /// evicted. Even when a live owner exists, the complete durable pass is the
    /// only authority allowed to validate and repair observer progress.
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
        let log = OperationLog::new(Arc::clone(&self.store), conversation_id);
        let replayed = self.replay_and_repair(conversation_id, &log)?;
        if replayed.next_log_sequence == 0 {
            drop(owner);
            return self.evict_uncommitted(conversation_id, &cell);
        }
        *owner = Some(replayed);
        drop(owner);
        Ok(())
    }
}

pub(super) fn publish_fired_observer(
    arm_targets: &mut BTreeMap<ConversationId, ObserverArmTarget>,
    fired: Option<ObserverRecoveryArm>,
    observer_progress: u64,
    conversation_id: ConversationId,
) -> Result<(), ParticipantSemanticError> {
    fired.map_or(Ok(()), |fired| {
        match arm_targets.remove(&fired.conversation_id()) {
            Some(association) if association.refused_epoch == fired.refused_epoch() => association
                .target
                .publish(ObserverPublication {
                    conversation_id: fired.conversation_id(),
                    refused_epoch: fired.refused_epoch(),
                    observer_progress,
                })
                .map(|_| ())
                .map_err(|error| ParticipantSemanticError::Internal {
                    message: format!("observer progressed publication failed: {error}"),
                }),
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
    })
}
