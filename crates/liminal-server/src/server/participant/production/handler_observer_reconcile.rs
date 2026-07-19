//! Fused observer-progress preflight, plan, and serialized execution.

use std::collections::BTreeMap;
use std::sync::Arc;

use liminal::durability::bridge::block_on;
use liminal_protocol::lifecycle::{
    ObserverProgressAdvanceDecision, ObserverProgressTrackDecision, ObserverRecoveryAggregate,
    ObserverRecoveryArm,
};
use liminal_protocol::wire::{ConversationId, DeliverySeq};

use crate::server::participant::ParticipantSemanticError;

use super::handler::{
    ObserverOwner, ProductionParticipantHandler, bridge_error, log_error, state_error,
};
use super::handler_observer::publish_fired_observer;
use super::observer::{ObserverLog, ObserverRow};
use super::observer_progress::{ObserverProgressConformanceError, ObserverProgressSourceWitness};
use super::observer_progress_plan::{
    ObserverProgressPreflight, ObserverProgressReconcilePlan, plan_observer_progress_reconcile,
};
use super::state::StateError;

impl ProductionParticipantHandler {
    /// Validates durable observer state and executes one complete repair plan.
    ///
    /// Source replay has already validated every witness. Restoring and
    /// inspecting the observer owner is read-only; a conformance refusal leaves
    /// the owner uninstalled when it was absent and appends no row. On success,
    /// Track and every strict running-maximum Advance execute under this one
    /// lock, in that order.
    pub(super) fn reconcile_observer_progress(
        &self,
        conversation_id: ConversationId,
        witnesses: &[ObserverProgressSourceWitness],
        authoritative_maximum: DeliverySeq,
    ) -> Result<(), ParticipantSemanticError> {
        let observer_log = ObserverLog::new(Arc::clone(&self.store));
        let mut owner = self
            .observer
            .lock()
            .map_err(|_| ParticipantSemanticError::Internal {
                message: "observer recovery aggregate lock is poisoned".to_owned(),
            })?;
        let inspection = inspect_observer_progress(
            &observer_log,
            owner.as_ref(),
            conversation_id,
            witnesses,
            authoritative_maximum,
        )?;
        let ObserverProgressInspection {
            restored,
            plan,
            planned_final_head,
        } = inspection;
        let active =
            owner
                .take()
                .or(restored)
                .ok_or_else(|| ParticipantSemanticError::Internal {
                    message: "validated observer recovery aggregate is absent".to_owned(),
                })?;
        let ObserverOwner {
            mut aggregate,
            mut head,
            mut arm_targets,
        } = active;

        if plan.track_baseline() {
            match append_track(&observer_log, aggregate, head, conversation_id) {
                Ok((next_aggregate, next_head)) => {
                    aggregate = next_aggregate;
                    head = next_head;
                }
                Err(failure) => {
                    *owner = Some(ObserverOwner {
                        aggregate: failure.aggregate,
                        head: failure.head,
                        arm_targets,
                    });
                    drop(owner);
                    return Err(failure.error);
                }
            }
        }

        for &progress in plan.advances() {
            match append_advance(&observer_log, aggregate, head, conversation_id, progress) {
                Ok((next_aggregate, next_head, fired)) => {
                    aggregate = next_aggregate;
                    head = next_head;
                    if let Err(error) =
                        publish_fired_observer(&mut arm_targets, fired, progress, conversation_id)
                    {
                        *owner = Some(ObserverOwner {
                            aggregate,
                            head,
                            arm_targets,
                        });
                        drop(owner);
                        return Err(error);
                    }
                }
                Err(failure) => {
                    *owner = Some(ObserverOwner {
                        aggregate: failure.aggregate,
                        head: failure.head,
                        arm_targets,
                    });
                    drop(owner);
                    return Err(failure.error);
                }
            }
        }

        let final_progress = aggregate.observer_progress(conversation_id);
        let final_matches = final_progress == Some(plan.validated_maximum())
            && authoritative_maximum == plan.validated_maximum()
            && head == planned_final_head;
        *owner = Some(ObserverOwner {
            aggregate,
            head,
            arm_targets,
        });
        drop(owner);
        if final_matches {
            Ok(())
        } else {
            Err(conformance_error(
                ObserverProgressConformanceError::FinalProgressMismatch,
            ))
        }
    }
}

struct ObserverProgressInspection {
    restored: Option<ObserverOwner>,
    plan: ObserverProgressReconcilePlan,
    planned_final_head: u64,
}

fn inspect_observer_progress(
    observer_log: &ObserverLog,
    owner: Option<&ObserverOwner>,
    conversation_id: ConversationId,
    witnesses: &[ObserverProgressSourceWitness],
    authoritative_maximum: DeliverySeq,
) -> Result<ObserverProgressInspection, ParticipantSemanticError> {
    // Restore into a local candidate. It is not installed unless the full
    // source and durable-prefix plan succeeds.
    let restored = if owner.is_none() {
        let restored = block_on(observer_log.restore())
            .map_err(|error| bridge_error(&error))?
            .map_err(|error| log_error(&error))?;
        Some(ObserverOwner {
            aggregate: restored.aggregate,
            head: restored.next_sequence,
            arm_targets: BTreeMap::new(),
        })
    } else {
        None
    };
    let inspected =
        owner
            .or(restored.as_ref())
            .ok_or_else(|| ParticipantSemanticError::Internal {
                message: "observer recovery aggregate is absent".to_owned(),
            })?;
    let preflight = inspected
        .aggregate
        .observer_progress(conversation_id)
        .map_or(ObserverProgressPreflight::Untracked, |progress| {
            ObserverProgressPreflight::Tracked(progress)
        });
    let plan = plan_observer_progress_reconcile(witnesses, authoritative_maximum, preflight)
        .map_err(conformance_error)?;

    // Validate the complete row-head movement before the first append.
    let advance_rows = u64::try_from(plan.advances().len()).map_err(|_| {
        state_error(&StateError::AllocationExhausted {
            domain: "observer repair row count",
        })
    })?;
    let planned_rows = advance_rows
        .checked_add(u64::from(plan.track_baseline()))
        .ok_or_else(|| {
            state_error(&StateError::AllocationExhausted {
                domain: "observer repair row count",
            })
        })?;
    let planned_final_head = inspected.head.checked_add(planned_rows).ok_or_else(|| {
        state_error(&StateError::AllocationExhausted {
            domain: "observer durable row sequence",
        })
    })?;
    Ok(ObserverProgressInspection {
        restored,
        plan,
        planned_final_head,
    })
}

struct ObserverExecutionFailure {
    aggregate: ObserverRecoveryAggregate,
    head: u64,
    error: ParticipantSemanticError,
}

fn append_track(
    observer_log: &ObserverLog,
    aggregate: ObserverRecoveryAggregate,
    head: u64,
    conversation_id: ConversationId,
) -> Result<(ObserverRecoveryAggregate, u64), ObserverExecutionFailure> {
    let Some(next_head) = head.checked_add(1) else {
        return Err(ObserverExecutionFailure {
            aggregate,
            head,
            error: state_error(&StateError::AllocationExhausted {
                domain: "observer durable row sequence",
            }),
        });
    };
    let transaction = match aggregate.decide_track(conversation_id, 0) {
        ObserverProgressTrackDecision::Commit(transaction) => transaction,
        ObserverProgressTrackDecision::Refuse { aggregate, .. } => {
            return Err(ObserverExecutionFailure {
                aggregate,
                head,
                error: conformance_error(ObserverProgressConformanceError::FinalProgressMismatch),
            });
        }
    };
    match block_on(observer_log.append(
        &ObserverRow::Track {
            conversation_id,
            observer_progress: 0,
        },
        head,
    )) {
        Ok(Ok(())) => Ok((transaction.commit(), next_head)),
        Ok(Err(error)) => Err(ObserverExecutionFailure {
            aggregate: transaction.abort(),
            head,
            error: state_error(&StateError::Log(error)),
        }),
        Err(error) => Err(ObserverExecutionFailure {
            aggregate: transaction.abort(),
            head,
            error: bridge_error(&error),
        }),
    }
}

fn append_advance(
    observer_log: &ObserverLog,
    aggregate: ObserverRecoveryAggregate,
    head: u64,
    conversation_id: ConversationId,
    progress: DeliverySeq,
) -> Result<(ObserverRecoveryAggregate, u64, Option<ObserverRecoveryArm>), ObserverExecutionFailure>
{
    let Some(next_head) = head.checked_add(1) else {
        return Err(ObserverExecutionFailure {
            aggregate,
            head,
            error: state_error(&StateError::AllocationExhausted {
                domain: "observer durable row sequence",
            }),
        });
    };
    let transaction = match aggregate.decide_progress_advance(conversation_id, progress) {
        ObserverProgressAdvanceDecision::Commit(transaction) => transaction,
        ObserverProgressAdvanceDecision::Refuse { aggregate, .. } => {
            return Err(ObserverExecutionFailure {
                aggregate,
                head,
                error: conformance_error(ObserverProgressConformanceError::FinalProgressMismatch),
            });
        }
    };
    match block_on(observer_log.append(
        &ObserverRow::Advance {
            conversation_id,
            observer_progress: progress,
        },
        head,
    )) {
        Ok(Ok(())) => {
            let (aggregate, fired) = transaction.commit();
            Ok((aggregate, next_head, fired))
        }
        Ok(Err(error)) => Err(ObserverExecutionFailure {
            aggregate: transaction.abort(),
            head,
            error: state_error(&StateError::Log(error)),
        }),
        Err(error) => Err(ObserverExecutionFailure {
            aggregate: transaction.abort(),
            head,
            error: bridge_error(&error),
        }),
    }
}

fn conformance_error(error: ObserverProgressConformanceError) -> ParticipantSemanticError {
    state_error(&StateError::ObserverProgressConformance(error))
}
