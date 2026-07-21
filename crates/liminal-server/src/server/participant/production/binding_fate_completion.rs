//! Measured Ordinary/Recovered completion after one durable Died source.

use liminal_protocol::lifecycle::{
    AggregateOperationDecision, BindingFateTerminal, CommittedDiedTerminal, LiveFrontierOwner,
    MeasuredBindingFate, OrdinaryBindingFate, RecoveredBindingFate, SealedBindingFateIntent,
    decide_ordinary_binding_fate_operation, decide_recovered_binding_fate_operation,
};
use liminal_protocol::wire::{DiedCause, ParticipantId};

use super::barrier::{CommitMode, commit_through_barrier};
use super::log::{
    OperationLogError, StoredBindingEpoch, StoredCommittedTerminalAudit, StoredDiedCause,
    StoredOperation, StoredOrdinaryFate, StoredOrdinaryTerminalSource, StoredRecoveredFate,
    StoredRecoveredPresentation, StoredSpecificFateIntent,
};
use super::state::{ConversationAuthority, DurableAppend, PendingBindingFate, StateError};

#[derive(Clone, Copy)]
struct OrdinaryCompletion {
    died_source_sequence: u64,
    attached_source_sequence: u64,
    terminal: CommittedDiedTerminal,
}

#[derive(Clone, Copy)]
struct RecoveredCompletion {
    died_source_sequence: u64,
    attached_source_sequence: u64,
    prior_binding_epoch: StoredBindingEpoch,
    marker_delivery_seq: u64,
    presentation: StoredRecoveredPresentation,
}

impl ConversationAuthority {
    /// Consumes the sole sealed fate token through protocol measurement and
    /// appends its exact specific row after the owning Died source is durable.
    pub(super) fn complete_binding_fate_intent(
        &mut self,
        participant_id: ParticipantId,
        died_source_sequence: u64,
        intent: StoredSpecificFateIntent,
        terminal: Option<CommittedDiedTerminal>,
        appender: &dyn DurableAppend,
    ) -> Result<(), StateError> {
        if matches!(intent, StoredSpecificFateIntent::Ordinary { .. }) && terminal.is_none() {
            return Ok(());
        }
        let pending = self.take_pending_binding_fate(participant_id, intent)?;
        let attached_source_sequence = pending.attached_source_sequence;
        let terminal_input = match intent {
            StoredSpecificFateIntent::Ordinary { .. } => {
                let Some(terminal) = terminal else {
                    return Err(StateError::invariant(
                        "ordinary binding fate completion has no committed Died terminal",
                    ));
                };
                BindingFateTerminal::Ordinary(terminal)
            }
            StoredSpecificFateIntent::Recovered { .. } => BindingFateTerminal::Recovered,
        };
        let owner = self.take_frontier()?;
        let prepared =
            match owner.prepare_binding_fate(pending.token, terminal_input, self.observer_progress)
            {
                Ok(prepared) => prepared,
                Err(refused) => {
                    let error = refused.error();
                    let (owner, token, _) = refused.into_parts();
                    self.install_frontier(owner);
                    self.restore_pending_binding_fate(
                        participant_id,
                        PendingBindingFate {
                            attached_source_sequence,
                            token,
                        },
                    )?;
                    return Err(StateError::invariant(format!(
                        "binding-fate measurement refused: {error:?}"
                    )));
                }
            };
        let (owner, fate, _) = prepared.into_parts();
        match (intent, fate) {
            (
                StoredSpecificFateIntent::Ordinary {
                    attached_source_sequence,
                },
                MeasuredBindingFate::Ordinary(fate),
            ) => {
                let Some(terminal) = terminal else {
                    self.install_frontier(owner);
                    return Err(StateError::invariant(
                        "measured ordinary fate lost its committed Died terminal",
                    ));
                };
                self.append_ordinary_binding_fate(
                    owner,
                    fate,
                    OrdinaryCompletion {
                        died_source_sequence,
                        attached_source_sequence,
                        terminal,
                    },
                    appender,
                )?;
            }
            (
                StoredSpecificFateIntent::Recovered {
                    attached_source_sequence,
                    prior_binding_epoch,
                    marker_delivery_seq,
                },
                MeasuredBindingFate::Recovered(fate),
            ) => {
                let presentation = if terminal.is_some() {
                    StoredRecoveredPresentation::DiedCommittedOwns
                } else {
                    StoredRecoveredPresentation::RecoveredOwnsAndReservesFinalizer
                };
                self.append_recovered_binding_fate(
                    owner,
                    fate,
                    RecoveredCompletion {
                        died_source_sequence,
                        attached_source_sequence,
                        prior_binding_epoch,
                        marker_delivery_seq,
                        presentation,
                    },
                    appender,
                )?;
            }
            _ => {
                self.install_frontier(owner);
                return Err(StateError::invariant(
                    "measured binding fate class disagrees with durable Died intent",
                ));
            }
        }
        Ok(())
    }

    fn append_ordinary_binding_fate(
        &mut self,
        owner: LiveFrontierOwner,
        fate: OrdinaryBindingFate,
        completion: OrdinaryCompletion,
        appender: &dyn DurableAppend,
    ) -> Result<(), StateError> {
        let row = StoredOrdinaryFate {
            participant_id: fate.participant_id(),
            last_dead_binding_epoch: fate.last_dead_binding_epoch().into(),
            ordinary_attached_source_sequence: completion.attached_source_sequence,
            terminal_source: StoredOrdinaryTerminalSource::DiedCommitted {
                died_source_sequence: completion.died_source_sequence,
            },
            committed_terminal_audit: StoredCommittedTerminalAudit {
                cause: stored_died_cause(completion.terminal.cause()),
                transaction_order: completion.terminal.admission_order().transaction_order(),
                terminal_seq: completion.terminal.delivery_seq(),
            },
            resulting_floor: fate.resulting_floor(),
        };
        let shell = self.take_shell()?;
        let barrier = match decide_ordinary_binding_fate_operation(shell, fate) {
            AggregateOperationDecision::Commit(barrier) => barrier,
            AggregateOperationDecision::Refused(refused) => {
                let reason = refused.reason();
                let (shell, _) = refused.into_parts();
                self.shell = Some(shell);
                self.install_frontier(owner);
                return Err(StateError::ShellRefused { reason });
            }
        };
        let make_operation = |event| StoredOperation::Ordinary {
            row: row.clone(),
            event,
        };
        self.route_fate_occurrence(&make_operation(Vec::new()), self.next_log_sequence)?;
        let (shell, _) = commit_through_barrier(
            barrier,
            CommitMode::Live(appender),
            self.next_log_sequence,
            &make_operation,
        )?;
        self.shell = Some(shell);
        self.install_frontier(owner);
        self.advance_log_head()?;
        Ok(())
    }

    fn append_recovered_binding_fate(
        &mut self,
        owner: LiveFrontierOwner,
        fate: RecoveredBindingFate,
        completion: RecoveredCompletion,
        appender: &dyn DurableAppend,
    ) -> Result<(), StateError> {
        let row = StoredRecoveredFate {
            participant_id: fate.participant_id(),
            last_dead_binding_epoch: fate.last_dead_binding_epoch().into(),
            died_source_sequence: completion.died_source_sequence,
            fenced_attached_source_sequence: completion.attached_source_sequence,
            prior_binding_epoch: completion.prior_binding_epoch,
            marker_delivery_seq: completion.marker_delivery_seq,
            resulting_floor: fate.resulting_floor(),
            presentation: completion.presentation,
        };
        let shell = self.take_shell()?;
        let barrier = match decide_recovered_binding_fate_operation(shell, fate) {
            AggregateOperationDecision::Commit(barrier) => barrier,
            AggregateOperationDecision::Refused(refused) => {
                let reason = refused.reason();
                let (shell, _) = refused.into_parts();
                self.shell = Some(shell);
                self.install_frontier(owner);
                return Err(StateError::ShellRefused { reason });
            }
        };
        let make_operation = |event| StoredOperation::Recovered {
            row: row.clone(),
            event,
        };
        self.route_fate_occurrence(&make_operation(Vec::new()), self.next_log_sequence)?;
        let (shell, _) = commit_through_barrier(
            barrier,
            CommitMode::Live(appender),
            self.next_log_sequence,
            &make_operation,
        )?;
        self.shell = Some(shell);
        self.install_frontier(owner);
        self.advance_log_head()?;
        Ok(())
    }

    pub(super) fn repair_pending_specific_fates(
        &mut self,
        appender: &dyn DurableAppend,
    ) -> Result<(), StateError> {
        let pending = self
            .pending_specific_fates
            .iter()
            .map(|(participant_id, pending)| (*participant_id, *pending))
            .collect::<Vec<_>>();
        for (participant_id, pending) in pending {
            let ready = pending.terminal.is_some()
                || matches!(pending.intent, StoredSpecificFateIntent::Recovered { .. });
            if !ready {
                continue;
            }
            self.complete_binding_fate_intent(
                participant_id,
                pending.died_source_sequence,
                pending.intent,
                pending.terminal,
                appender,
            )?;
            self.pending_specific_fates.remove(&participant_id);
        }
        Ok(())
    }

    pub(super) fn replay_specific_fate(
        &mut self,
        operation: &StoredOperation,
        sequence: u64,
    ) -> Result<(), StateError> {
        let participant_id = match &operation {
            StoredOperation::Ordinary { row, .. } => row.participant_id,
            StoredOperation::Recovered { row, .. } => row.participant_id,
            _ => {
                return Err(StateError::invariant(
                    "specific-fate replay received a non-specific operation",
                ));
            }
        };
        let pending = self
            .pending_specific_fates
            .get(&participant_id)
            .copied()
            .ok_or_else(|| {
                StateError::invariant(
                    "specific-fate row has no earlier unconsumed durable Died intent",
                )
            })?;
        if self.next_log_sequence != sequence {
            return Err(StateError::invariant(
                "specific-fate replay sequence disagrees with the checked log head",
            ));
        }
        let appender = ReplayFateAppender {
            expected: operation,
            sequence,
        };
        self.complete_binding_fate_intent(
            participant_id,
            pending.died_source_sequence,
            pending.intent,
            pending.terminal,
            &appender,
        )?;
        self.pending_specific_fates.remove(&participant_id);
        Ok(())
    }

    fn take_pending_binding_fate(
        &mut self,
        participant_id: ParticipantId,
        intent: StoredSpecificFateIntent,
    ) -> Result<PendingBindingFate, StateError> {
        let Some(slot) = self.slots.get_mut(&participant_id) else {
            return Err(StateError::invariant(
                "binding-fate completion participant slot is absent",
            ));
        };
        let Some(pending) = slot.binding_fate.take() else {
            return Err(StateError::invariant(
                "durable Died intent has no sealed binding-fate authority",
            ));
        };
        if pending.attached_source_sequence != intent.attached_source_sequence()
            || !intent_matches_token(intent, &pending)
        {
            slot.binding_fate = Some(pending);
            return Err(StateError::invariant(
                "durable Died intent disagrees with sealed binding-fate authority",
            ));
        }
        Ok(pending)
    }

    fn restore_pending_binding_fate(
        &mut self,
        participant_id: ParticipantId,
        pending: PendingBindingFate,
    ) -> Result<(), StateError> {
        let Some(slot) = self.slots.get_mut(&participant_id) else {
            return Err(StateError::invariant(
                "binding-fate refusal participant slot is absent",
            ));
        };
        if slot.binding_fate.replace(pending).is_some() {
            return Err(StateError::invariant(
                "binding-fate refusal would overwrite sealed authority",
            ));
        }
        Ok(())
    }
}

trait StoredSpecificFateIntentExt {
    fn attached_source_sequence(self) -> u64;
}

impl StoredSpecificFateIntentExt for StoredSpecificFateIntent {
    fn attached_source_sequence(self) -> u64 {
        match self {
            Self::Ordinary {
                attached_source_sequence,
            }
            | Self::Recovered {
                attached_source_sequence,
                ..
            } => attached_source_sequence,
        }
    }
}

fn intent_matches_token(intent: StoredSpecificFateIntent, pending: &PendingBindingFate) -> bool {
    match (intent, pending.token.intent()) {
        (StoredSpecificFateIntent::Ordinary { .. }, Some(SealedBindingFateIntent::Ordinary)) => {
            true
        }
        (
            StoredSpecificFateIntent::Recovered {
                prior_binding_epoch,
                marker_delivery_seq,
                ..
            },
            Some(SealedBindingFateIntent::Recovered {
                prior_binding_epoch: token_epoch,
                marker_delivery_seq: token_marker,
            }),
        ) => prior_binding_epoch == token_epoch.into() && marker_delivery_seq == token_marker,
        _ => false,
    }
}

const fn stored_died_cause(cause: DiedCause) -> StoredDiedCause {
    match cause {
        DiedCause::ConnectionLost => StoredDiedCause::ConnectionLost,
        DiedCause::ProcessKilled => StoredDiedCause::ProcessKilled,
        DiedCause::ProtocolError => StoredDiedCause::ProtocolError,
        DiedCause::UncleanServerRestart {
            prior_server_incarnation,
        } => StoredDiedCause::UncleanServerRestart {
            prior_server_incarnation,
        },
    }
}

struct ReplayFateAppender<'a> {
    expected: &'a StoredOperation,
    sequence: u64,
}

impl DurableAppend for ReplayFateAppender<'_> {
    fn append(
        &self,
        operation: &StoredOperation,
        expected_sequence: u64,
    ) -> Result<(), OperationLogError> {
        if operation != self.expected || expected_sequence != self.sequence {
            return Err(OperationLogError::FateReplayDrift {
                sequence: self.sequence,
            });
        }
        Ok(())
    }
}
