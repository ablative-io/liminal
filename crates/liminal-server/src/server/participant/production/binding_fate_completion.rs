//! Measured Ordinary/Recovered completion after one durable Died source.

use liminal_protocol::lifecycle::{
    AggregateOperationDecision, BindingFateTerminal, CommittedDiedTerminal, LiveFrontierOwner,
    MeasuredBindingFate, OrdinaryBindingFate, RecoveredBindingFate, SealedBindingFateIntent,
    decide_ordinary_binding_fate_operation, decide_recovered_binding_fate_operation,
};
use liminal_protocol::wire::{DeliverySeq, DiedCause, ParticipantId};

use super::barrier::{CommitMode, commit_through_barrier};
use super::log::{
    OperationLogError, StoredBindingEpoch, StoredCommittedTerminalAudit, StoredDiedCause,
    StoredOperation, StoredOrdinaryFate, StoredOrdinaryTerminalSource, StoredRecoveredFate,
    StoredRecoveredPresentation, StoredSpecificFateIntent,
};
use super::observer_progress::ObserverProgressSourceMetadata;
use super::state::{
    ConversationAuthority, DurableAppend, PendingBindingFate, PendingSpecificFate,
    PendingSpecificFateTerminal, StateError,
};

#[derive(Clone, Copy)]
struct OrdinaryCompletion {
    attached_source_sequence: u64,
    terminal: CommittedDiedTerminal,
    terminal_source: StoredOrdinaryTerminalSource,
}

#[derive(Clone, Copy)]
struct RecoveredCompletion {
    died_source_sequence: u64,
    attached_source_sequence: u64,
    prior_binding_epoch: StoredBindingEpoch,
    marker_delivery_seq: u64,
    presentation: StoredRecoveredPresentation,
}

/// One measured binding fate held as a VALUE between its measurement and the
/// durable append that completes it.
///
/// §3.2's split exists so this can be carried across the Died source append:
/// the measurement that produces it is pure, so it may run before any durable
/// row exists, and the append that consumes it runs only once that row is.
pub(super) struct MeasuredSpecificFate {
    owner: LiveFrontierOwner,
    fate: MeasuredBindingFate,
    died_source_sequence: u64,
    intent: StoredSpecificFateIntent,
    terminal: Option<PendingSpecificFateTerminal>,
}

/// Validates one durable Died intent against its sealed token and resolves the
/// exact terminal input.
///
/// Pure: acquires no frontier, appends nothing, and mutates no pending state,
/// so a refusal here disturbs neither half of §3.2's split. Both halves call it
/// before any owner is acquired, which is what keeps a validation refusal from
/// ever leaving an owner taken.
fn validated_completion_terminal(
    intent: StoredSpecificFateIntent,
    terminal: Option<PendingSpecificFateTerminal>,
    pending: &PendingBindingFate,
) -> Result<BindingFateTerminal, StateError> {
    if pending.attached_source_sequence != intent.attached_source_sequence()
        || !intent_matches_token(intent, pending)
    {
        return Err(StateError::invariant(
            "durable Died intent disagrees with sealed binding-fate authority",
        ));
    }
    completion_terminal(intent, terminal)
}

/// Measures one sealed fate token on a frontier owner held as a VALUE, before
/// the enclosing Died source row is durable.
///
/// This is §3.2's measure half. It installs nothing and appends nothing in
/// either direction; every disposition belongs to the caller, which is what
/// lets a refusal leave NO DURABLE RESIDUE.
///
/// `hard_observer_progress` is an EXPLICIT INPUT, not ambient state, per
/// amendment 4. BOOT IS CANON: boot's reconstruction measures at the completion
/// row's sequence, by which point replay has already folded the Died row's OWN
/// projection into observer progress, so the live measurement must be handed
/// that same joined value or the two disagree and `ReplayFateAppender` refuses.
/// The caller computes the join under the exact conditional mirror of
/// `replay_died_source` (`connection_fate_replay.rs:173-185`); receiving it as a
/// parameter is what keeps it a computed input rather than an ambient mutation,
/// so the mirror-defect family stays untouched.
///
/// A free function rather than a method BECAUSE that input is explicit: with
/// progress passed in, nothing on the authority is read here, and
/// `clippy::unused_self` is denied workspace-wide.
pub(super) fn measure_specific_fate_on_owner(
    owner: LiveFrontierOwner,
    hard_observer_progress: DeliverySeq,
    died_source_sequence: u64,
    intent: StoredSpecificFateIntent,
    terminal: Option<PendingSpecificFateTerminal>,
    pending: PendingBindingFate,
) -> Result<MeasuredSpecificFate, StateError> {
    let terminal_input = validated_completion_terminal(intent, terminal, &pending)?;
    match owner.prepare_binding_fate(pending.token, terminal_input, hard_observer_progress) {
        Ok(prepared) => {
            let (owner, fate, _) = prepared.into_parts();
            Ok(MeasuredSpecificFate {
                owner,
                fate,
                died_source_sequence,
                intent,
                terminal,
            })
        }
        // §3.2 STEP 4, AS RULED: refused => Err with NOTHING appended and
        // NOTHING installed. The owner is deliberately NOT restored and the
        // intent is deliberately NOT re-inserted. That leaves the authority
        // frontier-less with its transition begun, and every subsequent
        // acquisition reports it loudly at take_frontier's two guards
        // (production/state.rs:483-496) — which is the design's measured
        // ground, not an oversight. Do not "repair" this arm.
        Err(refused) => Err(StateError::invariant(format!(
            "binding-fate measurement refused: {:?}",
            refused.error()
        ))),
    }
}

impl ConversationAuthority {
    /// Completes one open durable Died intent from its owner-held move-only token.
    pub(super) fn complete_pending_specific_fate(
        &mut self,
        participant_id: ParticipantId,
        appender: &dyn DurableAppend,
    ) -> Result<(), StateError> {
        let pending = self
            .pending_specific_fates
            .remove(&participant_id)
            .ok_or_else(|| StateError::invariant("binding-fate completion intent is absent"))?;
        self.complete_binding_fate_intent(participant_id, pending, appender)
    }

    /// Consumes the sole sealed fate token through protocol measurement and
    /// appends its exact specific row after the owning Died source is durable.
    fn complete_binding_fate_intent(
        &mut self,
        participant_id: ParticipantId,
        pending_specific: PendingSpecificFate,
        appender: &dyn DurableAppend,
    ) -> Result<(), StateError> {
        let PendingSpecificFate {
            died_source_sequence,
            intent,
            terminal,
            binding_fate: pending,
        } = pending_specific;
        if matches!(intent, StoredSpecificFateIntent::Ordinary { .. }) && terminal.is_none() {
            self.pending_specific_fates.insert(
                participant_id,
                PendingSpecificFate {
                    died_source_sequence,
                    intent,
                    terminal,
                    binding_fate: pending,
                },
            );
            return Ok(());
        }
        let attached_source_sequence = pending.attached_source_sequence;
        let terminal_input = validated_completion_terminal(intent, terminal, &pending)?;
        let owner = self.take_frontier()?;
        let prepared =
            match owner.prepare_binding_fate(pending.token, terminal_input, self.observer_progress)
            {
                Ok(prepared) => prepared,
                Err(refused) => {
                    let error = refused.error();
                    let (owner, token, _) = refused.into_parts();
                    self.install_frontier(owner)?;
                    self.pending_specific_fates.insert(
                        participant_id,
                        PendingSpecificFate {
                            died_source_sequence,
                            intent,
                            terminal,
                            binding_fate: PendingBindingFate {
                                attached_source_sequence,
                                token,
                            },
                        },
                    );
                    return Err(StateError::invariant(format!(
                        "binding-fate measurement refused: {error:?}"
                    )));
                }
            };
        let (owner, fate, _) = prepared.into_parts();
        self.append_measured_binding_fate(
            owner,
            fate,
            died_source_sequence,
            intent,
            terminal,
            appender,
        )
    }

    /// Appends one measured fate's exact specific row and only then installs
    /// the transitioned owner.
    ///
    /// This is §3.2's append half, and it is called only once the enclosing
    /// Died source row is durable. The install trails the append throughout,
    /// per the shape's own invariant.
    pub(super) fn append_measured_specific_fate(
        &mut self,
        measured: MeasuredSpecificFate,
        appender: &dyn DurableAppend,
    ) -> Result<(), StateError> {
        let MeasuredSpecificFate {
            owner,
            fate,
            died_source_sequence,
            intent,
            terminal,
        } = measured;
        self.append_measured_binding_fate(
            owner,
            fate,
            died_source_sequence,
            intent,
            terminal,
            appender,
        )
    }

    fn append_measured_binding_fate(
        &mut self,
        owner: LiveFrontierOwner,
        fate: MeasuredBindingFate,
        died_source_sequence: u64,
        intent: StoredSpecificFateIntent,
        terminal: Option<PendingSpecificFateTerminal>,
        appender: &dyn DurableAppend,
    ) -> Result<(), StateError> {
        match (intent, fate) {
            (
                StoredSpecificFateIntent::Ordinary {
                    attached_source_sequence,
                },
                MeasuredBindingFate::Ordinary(fate),
            ) => {
                let Some(finalized) = terminal else {
                    self.install_frontier(owner)?;
                    return Err(StateError::invariant(
                        "measured ordinary fate lost its finalized Died terminal",
                    ));
                };
                self.append_ordinary_binding_fate(
                    owner,
                    fate,
                    OrdinaryCompletion {
                        attached_source_sequence,
                        terminal: finalized.terminal,
                        terminal_source: finalized.source,
                    },
                    appender,
                )
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
                )
            }
            _ => {
                self.install_frontier(owner)?;
                Err(StateError::invariant(
                    "measured binding fate class disagrees with durable Died intent",
                ))
            }
        }
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
            terminal_source: completion.terminal_source,
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
                self.install_frontier(owner)?;
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
        self.install_frontier(owner)?;
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
        let observer_projection = fate.observer_progress_projection();
        let shell = self.take_shell()?;
        let barrier = match decide_recovered_binding_fate_operation(shell, fate) {
            AggregateOperationDecision::Commit(barrier) => barrier,
            AggregateOperationDecision::Refused(refused) => {
                let reason = refused.reason();
                let (shell, _) = refused.into_parts();
                self.shell = Some(shell);
                self.install_frontier(owner)?;
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
        self.install_frontier(owner)?;
        if completion.presentation == StoredRecoveredPresentation::RecoveredOwnsAndReservesFinalizer
        {
            let metadata = ObserverProgressSourceMetadata::recovered_binding_fate(
                self.next_log_sequence,
                self.conversation_id,
                row.participant_id,
                row.last_dead_binding_epoch.to_epoch()?,
                row.resulting_floor,
            );
            self.record_observer_progress_projection(observer_projection, metadata)?;
        }
        self.advance_log_head()?;
        Ok(())
    }

    pub(super) fn repair_pending_specific_fates(
        &mut self,
        appender: &dyn DurableAppend,
    ) -> Result<(), StateError> {
        let prepared = self
            .prepared_ordinary_finalizers
            .keys()
            .copied()
            .collect::<Vec<_>>();
        for participant_id in prepared {
            self.complete_prepared_ordinary_finalizer(participant_id, appender)?;
        }
        let ready = self
            .pending_specific_fates
            .iter()
            .filter_map(|(participant_id, pending)| {
                (pending.terminal.is_some()
                    || matches!(pending.intent, StoredSpecificFateIntent::Recovered { .. }))
                .then_some(*participant_id)
            })
            .collect::<Vec<_>>();
        for participant_id in ready {
            self.complete_pending_specific_fate(participant_id, appender)?;
        }
        Ok(())
    }

    /// Appends one already-measured Ordinary row after its enclosing source is durable.
    pub(super) fn complete_prepared_ordinary_finalizer(
        &mut self,
        participant_id: ParticipantId,
        appender: &dyn DurableAppend,
    ) -> Result<(), StateError> {
        let prepared = self
            .prepared_ordinary_finalizers
            .remove(&participant_id)
            .ok_or_else(|| StateError::invariant("prepared Ordinary finalizer is absent"))?;
        let owner = self
            .take_frontier()?
            .complete_pending_died_ordinary_finalizer(prepared.finalizer)
            .map_err(|error| {
                StateError::invariant(format!("Ordinary finalizer floor failed: {error:?}"))
            })?;
        self.append_ordinary_binding_fate(
            owner,
            prepared.fate,
            OrdinaryCompletion {
                attached_source_sequence: prepared.attached_source_sequence,
                terminal: prepared.terminal,
                terminal_source: prepared.terminal_source,
            },
            appender,
        )
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
        if matches!(operation, StoredOperation::Ordinary { .. })
            && self
                .prepared_ordinary_finalizers
                .contains_key(&participant_id)
        {
            let appender = ReplayFateAppender {
                expected: operation,
                sequence,
            };
            return self.complete_prepared_ordinary_finalizer(participant_id, &appender);
        }
        if !self.pending_specific_fates.contains_key(&participant_id) {
            return Err(StateError::invariant(
                "specific-fate row has no earlier unconsumed durable Died intent",
            ));
        }
        if self.next_log_sequence != sequence {
            return Err(StateError::invariant(
                "specific-fate replay sequence disagrees with the checked log head",
            ));
        }
        let appender = ReplayFateAppender {
            expected: operation,
            sequence,
        };
        self.complete_pending_specific_fate(participant_id, &appender)
    }
}

pub(super) trait StoredSpecificFateIntentExt {
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

pub(super) fn intent_matches_token(
    intent: StoredSpecificFateIntent,
    pending: &PendingBindingFate,
) -> bool {
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

pub(super) const fn stored_died_cause(cause: DiedCause) -> StoredDiedCause {
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

fn completion_terminal(
    intent: StoredSpecificFateIntent,
    terminal: Option<PendingSpecificFateTerminal>,
) -> Result<BindingFateTerminal, StateError> {
    match intent {
        StoredSpecificFateIntent::Ordinary { .. } => terminal
            .map(|finalized| BindingFateTerminal::Ordinary(finalized.terminal))
            .ok_or_else(|| {
                StateError::invariant("ordinary binding fate completion has no finalized terminal")
            }),
        StoredSpecificFateIntent::Recovered { .. } if terminal.is_some() => {
            Ok(BindingFateTerminal::Recovered)
        }
        StoredSpecificFateIntent::Recovered { .. } => {
            Ok(BindingFateTerminal::RecoveredAndReserveFinalizer)
        }
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
