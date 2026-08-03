//! Typed per-conversation connection-fate transaction boundary.
//!
//! Leg 4b owns target discovery and orchestration. The durable Died/Detached
//! source-row transaction body is deliberately isolated in [`PreparedConnectionFate::complete`]
//! so callers cannot supply participant ids or binding epochs.

use liminal_protocol::lifecycle::{
    ActiveBinding, BindingState, BindingTerminalAdmission, BindingTerminalCauseClass,
    BindingTerminalDisposition, CommittedDiedTerminal, LiveFrontierOwner,
    ObserverProgressProjection, SealedBindingFateIntent,
};
use liminal_protocol::wire::{BindingEpoch, ParticipantId};

use crate::server::participant::dispatch_impact::DispatchImpactAccumulator;
use crate::server::participant::{ConnectionFateClass, ConnectionFateWorkItem};

use super::binding_fate_completion::{MeasuredSpecificFate, measure_specific_fate_on_owner};
use super::connection_fate_allocation::checked_fate_allocations;
use super::connection_fate_rows::source_operation;
use super::frontier;
use super::log::{
    StoredOperation, StoredOrdinaryTerminalSource, StoredSpecificFateIntent,
    StoredTerminalDisposition,
};
use super::observer_progress::ObserverProgressSourceMetadata;
use super::outbox_projection::{
    ReplayedProjectionFacts, capture_projection_prestate, project_committed_source,
};
use super::state::{
    ConversationAuthority, DurableAppend, PendingBindingFate, PendingSpecificFate,
    PendingSpecificFateTerminal, StateError,
};

/// Exact source authority copied from one durable server-wide Open.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ConnectionFateSource {
    Open {
        open_sequence: u64,
        connection_incarnation: liminal_protocol::wire::ConnectionIncarnation,
        class: ConnectionFateClass,
    },
    UncleanServerRestart {
        current_server_incarnation: u64,
    },
}

/// One slot selected from conversation authority, never from transport input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ConnectionFateTarget {
    pub(super) participant_id: ParticipantId,
    pub(super) binding_epoch: BindingEpoch,
}

/// Prepared transaction for one listed conversation.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct PreparedConnectionFate {
    source: ConnectionFateSource,
    targets: Vec<ConnectionFateTarget>,
}

struct SpecificFateOpen {
    participant_id: ParticipantId,
    source_sequence: u64,
    intent: StoredSpecificFateIntent,
    committed_terminal: Option<CommittedDiedTerminal>,
    binding_fate: PendingBindingFate,
}

impl ConversationAuthority {
    /// Snapshots every Bound slot matching the Open's exact connection.
    pub(super) fn prepare_connection_fate_transaction(
        &self,
        work_item: &ConnectionFateWorkItem,
    ) -> PreparedConnectionFate {
        let targets = self
            .slots
            .iter()
            .filter_map(|(participant_id, slot)| {
                let BindingState::Bound(active) = slot.binding else {
                    return None;
                };
                (active.binding_epoch.connection_incarnation == work_item.connection_incarnation)
                    .then_some(ConnectionFateTarget {
                        participant_id: *participant_id,
                        binding_epoch: active.binding_epoch,
                    })
            })
            .collect();
        PreparedConnectionFate {
            source: ConnectionFateSource::Open {
                open_sequence: work_item.open_sequence,
                connection_incarnation: work_item.connection_incarnation,
                class: work_item.class,
            },
            targets,
        }
    }

    /// Snapshots every Bound slot owned by a strictly prior server incarnation.
    pub(super) fn prepare_unclean_server_restart_transaction(
        &self,
        current_server_incarnation: u64,
    ) -> Result<PreparedConnectionFate, StateError> {
        let mut targets = Vec::new();
        for (participant_id, slot) in &self.slots {
            let BindingState::Bound(active) = slot.binding else {
                continue;
            };
            let bound_server = active
                .binding_epoch
                .connection_incarnation
                .server_incarnation;
            if bound_server >= current_server_incarnation {
                return Err(StateError::invariant(
                    "startup found a Bound epoch not owned by a prior server incarnation",
                ));
            }
            targets.push(ConnectionFateTarget {
                participant_id: *participant_id,
                binding_epoch: active.binding_epoch,
            });
        }
        Ok(PreparedConnectionFate {
            source: ConnectionFateSource::UncleanServerRestart {
                current_server_incarnation,
            },
            targets,
        })
    }
}

impl PreparedConnectionFate {
    /// Consumes the exact prepared target set under the same conversation lock.
    ///
    /// Every target is revalidated before the first mutation. Each target then
    /// consumes the sealed protocol terminal selector, appends and flushes its
    /// exact source row, and only afterwards installs the selected frontier,
    /// allocators, and binding state.
    pub(super) fn complete(
        self,
        authority: &mut ConversationAuthority,
        appender: &dyn DurableAppend,
    ) -> Result<(), StateError> {
        self.complete_inner(authority, appender, None)
    }

    /// Completes live fate while staging every installed source/finalizer effect.
    pub(super) fn complete_with_impact(
        self,
        authority: &mut ConversationAuthority,
        appender: &dyn DurableAppend,
        impact: &mut DispatchImpactAccumulator,
    ) -> Result<(), StateError> {
        self.complete_inner(authority, appender, Some(impact))
    }

    fn complete_inner(
        self,
        authority: &mut ConversationAuthority,
        appender: &dyn DurableAppend,
        mut impact: Option<&mut DispatchImpactAccumulator>,
    ) -> Result<(), StateError> {
        for target in &self.targets {
            let Some(slot) = authority.slots.get(&target.participant_id) else {
                return Err(StateError::invariant(
                    "prepared connection-fate target disappeared under its conversation lock",
                ));
            };
            let BindingState::Bound(active) = slot.binding else {
                return Err(StateError::invariant(
                    "prepared connection-fate target stopped being Bound under its conversation lock",
                ));
            };
            let source_matches = match self.source {
                ConnectionFateSource::Open {
                    connection_incarnation,
                    ..
                } => active.binding_epoch.connection_incarnation == connection_incarnation,
                ConnectionFateSource::UncleanServerRestart {
                    current_server_incarnation,
                } => {
                    active
                        .binding_epoch
                        .connection_incarnation
                        .server_incarnation
                        < current_server_incarnation
                }
            };
            if active.binding_epoch != target.binding_epoch || !source_matches {
                return Err(StateError::invariant(
                    "prepared connection-fate target changed epoch under its conversation lock",
                ));
            }
        }
        for target in self.targets {
            complete_target(
                self.source,
                target,
                authority,
                appender,
                impact.as_deref_mut(),
            )?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn targets(&self) -> &[ConnectionFateTarget] {
        &self.targets
    }
}

/// The admitted owner's exact §3.2 disposition between admission and install.
///
/// The two arms are mutually exclusive by construction, which is the point:
/// an owner either carries a measured fate — and is installed by the completion
/// append, after that append — or carries none and is installed directly once
/// the source row is durable. No path installs it twice, and no path installs
/// it before a durable row.
enum AdmittedOwnerFate {
    Measured(MeasuredSpecificFate),
    Plain(LiveFrontierOwner),
}

fn complete_target(
    source: ConnectionFateSource,
    target: ConnectionFateTarget,
    authority: &mut ConversationAuthority,
    appender: &dyn DurableAppend,
    mut impact: Option<&mut DispatchImpactAccumulator>,
) -> Result<(), StateError> {
    let active = match authority.slots.get(&target.participant_id) {
        Some(slot) => match slot.binding {
            BindingState::Bound(active) => active,
            _ => {
                return Err(StateError::invariant(
                    "validated connection-fate target stopped being Bound",
                ));
            }
        },
        None => {
            return Err(StateError::invariant(
                "validated connection-fate target disappeared",
            ));
        }
    };
    let specific_fate_intent = match source_row_class(source) {
        BindingTerminalCauseClass::Died => authority
            .slots
            .get(&target.participant_id)
            .and_then(|slot| slot.binding_fate.as_ref())
            .map(stored_specific_fate_intent)
            .transpose()?,
        BindingTerminalCauseClass::Detached => None,
    };
    let allocations = checked_fate_allocations(authority)?;
    let source_sequence = allocations.source_sequence;
    let AdmittedTerminal {
        owner: admitted_owner,
        disposition,
        stored_disposition,
        committed,
    } = admit_terminal(authority, active, source_row_class(source))?;

    let completed = source_operation(
        source,
        active,
        disposition,
        stored_disposition,
        specific_fate_intent,
    );

    // §3.2 STEP 3: MEASURE BEFORE THE SOURCE APPEND. The measurement is pure,
    // so it runs here on the admitted owner held as a VALUE — ahead of the
    // durable Died row and ahead of any install. A refusal returns Err from
    // inside this match with nothing appended and nothing installed (step 4),
    // which is what makes the drop re-processable instead of poisonous.
    let owner_fate = match specific_fate_intent {
        Some(intent)
            if completed.committed_died_terminal.is_some()
                || matches!(intent, StoredSpecificFateIntent::Recovered { .. }) =>
        {
            if authority
                .pending_specific_fates
                .contains_key(&target.participant_id)
            {
                return Err(StateError::invariant(
                    "durable Died opened a second participant-specific fate intent",
                ));
            }
            let binding_fate = authority
                .slots
                .get_mut(&target.participant_id)
                .and_then(|slot| slot.binding_fate.take())
                .ok_or_else(|| {
                    StateError::invariant(
                        "durable Died intent lost its sealed binding-fate authority",
                    )
                })?;
            let terminal =
                completed
                    .committed_died_terminal
                    .map(|terminal| PendingSpecificFateTerminal {
                        terminal,
                        source: StoredOrdinaryTerminalSource::DiedCommitted {
                            died_source_sequence: source_sequence,
                        },
                    });
            // AMENDMENT 4 — BOOT IS CANON, so the measurement is handed an
            // EXPLICIT progress input rather than reading ambient state.
            //
            // Boot's reconstruction measures at the completion row's sequence,
            // by which point replay has already folded THIS row's own
            // projection into observer progress — replay_died_source at
            // connection_fate_replay.rs:173-185, the fold at :184. So the live
            // measurement must see the same joined value or the two disagree
            // and ReplayFateAppender refuses. The conditional below is the
            // EXACT mirror of that site: a Committed disposition AND a Some
            // projection, both required.
            //
            // COMPUTED INPUT ONLY. Ambient observer_progress is not mutated
            // here; the join is passed as an argument and the ambient advance
            // still happens later, at its own site, after the append. That is
            // what keeps the mirror-defect family untouched.
            let measured_observer_progress =
                match (&completed.observer_projection, stored_disposition) {
                    (Some(projection), StoredTerminalDisposition::Committed { .. }) => authority
                        .observer_progress
                        .max(projection.new_observer_progress()),
                    _ => authority.observer_progress,
                };
            AdmittedOwnerFate::Measured(measure_specific_fate_on_owner(
                admitted_owner,
                measured_observer_progress,
                source_sequence,
                intent,
                terminal,
                binding_fate,
            )?)
        }
        _ => AdmittedOwnerFate::Plain(admitted_owner),
    };

    let projection_facts = capture_projection_prestate(authority, &completed.operation);
    authority.route_fate_occurrence(&completed.operation, source_sequence)?;
    appender.append(&completed.operation, authority.next_log_sequence)?;
    let measured = match owner_fate {
        AdmittedOwnerFate::Measured(measured) => Some(measured),
        AdmittedOwnerFate::Plain(owner) => {
            authority.install_frontier(owner)?;
            None
        }
    };
    authority.next_order = allocations.next_order;
    if committed {
        authority.next_seq = allocations.next_sequence;
    }
    authority.next_log_sequence = allocations.next_log_sequence;
    // §3.2 STEP 5: the Died row is now durable, so the completion row may be
    // appended; `append_measured_specific_fate` installs the transitioned owner
    // only after its own append lands. The install must precede the observer-
    // progress work below, which refuses loudly while a transition is still
    // begun (production/state.rs:403-407).
    let measured_fate_taken = if let Some(measured) = measured {
        authority.append_measured_specific_fate(measured, appender)?;
        true
    } else {
        false
    };
    let Some(slot) = authority.slots.get_mut(&target.participant_id) else {
        return Err(StateError::invariant(
            "connection-fate target disappeared after durable source append",
        ));
    };
    slot.binding = completed.binding_state;
    let binding_fate = if measured_fate_taken {
        // Taken before the measurement, above.
        None
    } else if specific_fate_intent.is_some() {
        Some(slot.binding_fate.take().ok_or_else(|| {
            StateError::invariant("durable Died intent lost its sealed binding-fate authority")
        })?)
    } else {
        None
    };
    if completed.clear_fate_token && binding_fate.is_none() {
        slot.binding_fate = None;
    }
    if let Some(projection) = completed.observer_projection {
        record_source_projection(authority, &completed.operation, source_sequence, projection)?;
    }
    if let Some(impact) = impact.as_deref_mut() {
        record_terminal_impact(
            authority,
            source_sequence,
            &completed.operation,
            projection_facts,
            target.participant_id,
            impact,
        )?;
    }
    if measured_fate_taken {
        // The measure half already appended this fate's completion row above,
        // so open_specific_fate's insert-then-complete round trip has nothing
        // left to discharge. Its episode effect still belongs here.
        if let Some(impact) = impact {
            authority.record_episode_changed(impact);
        }
    } else if let Some(intent) = specific_fate_intent {
        let binding_fate = binding_fate.ok_or_else(|| {
            StateError::invariant("durable Died intent has no binding-fate authority")
        })?;
        open_specific_fate(
            authority,
            SpecificFateOpen {
                participant_id: target.participant_id,
                source_sequence,
                intent,
                committed_terminal: completed.committed_died_terminal,
                binding_fate,
            },
            appender,
            impact,
        )?;
    }
    Ok(())
}

pub(super) fn record_terminal_impact(
    authority: &ConversationAuthority,
    source_sequence: u64,
    operation: &StoredOperation,
    projection_facts: ReplayedProjectionFacts,
    participant_id: ParticipantId,
    impact: &mut DispatchImpactAccumulator,
) -> Result<(), StateError> {
    if let Some(projection) =
        project_committed_source(authority, source_sequence, operation, projection_facts)?
    {
        authority.record_published_projection(&projection, impact)?;
    }
    authority.record_binding_changed(participant_id, impact);
    authority.record_episode_changed(impact);
    Ok(())
}

fn open_specific_fate(
    authority: &mut ConversationAuthority,
    prepared: SpecificFateOpen,
    appender: &dyn DurableAppend,
    impact: Option<&mut DispatchImpactAccumulator>,
) -> Result<(), StateError> {
    let SpecificFateOpen {
        participant_id,
        source_sequence,
        intent,
        committed_terminal,
        binding_fate,
    } = prepared;
    let terminal = committed_terminal.map(|terminal| PendingSpecificFateTerminal {
        terminal,
        source: StoredOrdinaryTerminalSource::DiedCommitted {
            died_source_sequence: source_sequence,
        },
    });
    if authority
        .pending_specific_fates
        .insert(
            participant_id,
            PendingSpecificFate {
                died_source_sequence: source_sequence,
                intent,
                terminal,
                binding_fate,
            },
        )
        .is_some()
    {
        return Err(StateError::invariant(
            "durable Died opened a second participant-specific fate intent",
        ));
    }
    // NO-RECONNECT MARK (Sol landing review, carried item 3). THIS BRANCH IS
    // UNREACHABLE FROM LIVE COMPLETION EVERYWHERE, and that is a property of the
    // caller rather than of this line.
    //
    // `open_specific_fate` has ONE live call site, `:413`, and it sits in the
    // ELSE of `measured_fate_taken`. The Measured arm at `:278-341` already
    // claims every case where `committed_died_terminal.is_some()` or the intent
    // is `Recovered`, and this function is handed those SAME two values
    // (`committed_terminal` is `completed.committed_died_terminal`, passed at
    // `:419`). So arriving here at all means that guard measured FALSE, and the
    // condition below is the same test over the same inputs: after the §3.2
    // split it cannot be true.
    //
    // The combined insert-then-complete form is NOT dead and must not be
    // deleted. It stays BOOT CANON through its DIRECT callers in
    // binding_fate_completion.rs (`:419-430` and `:497-501`), which invoke
    // `complete_pending_specific_fate` themselves — they reach the callee
    // without ever passing through this branch.
    //
    // Reconnecting this arm to live completion would append the completion row
    // from inside the post-source path and so violate measure-before-source
    // ordering (§3.2). DELETION IS BARRED, and so is re-wiring: the branch is
    // load-bearing for boot and inert for live, which is the shape intended.
    let completes_without_terminal = matches!(intent, StoredSpecificFateIntent::Recovered { .. });
    if committed_terminal.is_some() || completes_without_terminal {
        authority.complete_pending_specific_fate(participant_id, appender)?;
        if let Some(impact) = impact {
            authority.record_episode_changed(impact);
        }
    }
    Ok(())
}

fn record_source_projection(
    authority: &mut ConversationAuthority,
    operation: &StoredOperation,
    source_sequence: u64,
    projection: ObserverProgressProjection,
) -> Result<(), StateError> {
    let metadata = match operation {
        StoredOperation::Died { row } => ObserverProgressSourceMetadata::died(
            source_sequence,
            authority.conversation_id,
            row.participant_id,
            projection.new_observer_progress(),
        ),
        StoredOperation::Detached { row } => ObserverProgressSourceMetadata::detached(
            source_sequence,
            authority.conversation_id,
            row.participant_id,
            projection.new_observer_progress(),
        ),
        _ => {
            return Err(StateError::invariant(
                "connection-fate source produced a non-terminal observer projection",
            ));
        }
    };
    authority.record_observer_progress_projection(projection, metadata)
}

pub(super) struct AdmittedTerminal {
    pub(super) owner: LiveFrontierOwner,
    pub(super) disposition: BindingTerminalDisposition,
    pub(super) stored_disposition: StoredTerminalDisposition,
    pub(super) committed: bool,
}

pub(super) fn admit_terminal(
    authority: &mut ConversationAuthority,
    active: ActiveBinding,
    cause_class: BindingTerminalCauseClass,
) -> Result<AdmittedTerminal, StateError> {
    let owner = authority.take_frontier()?;
    let prepared = match owner.prepare_binding_terminal(
        active,
        cause_class,
        authority.next_order,
        authority.next_seq,
        authority.observer_progress,
    ) {
        Ok(prepared) => prepared,
        Err(refused) => {
            let error = refused.error();
            authority.install_frontier(refused.into_owner())?;
            return Err(StateError::invariant(format!(
                "binding-terminal prepare refused: {error:?}"
            )));
        }
    };
    let key = prepared.candidate_key();
    let charge = match frontier::terminal_charge(
        key.conversation_id(),
        key.participant_id(),
        key.binding_epoch(),
        key.admission_order().transaction_order(),
        key.delivery_seq(),
    ) {
        Ok(charge) => charge,
        Err(error) => {
            authority.install_frontier(prepared.into_owner())?;
            return Err(error);
        }
    };
    match prepared.admit(key.bind_v3_charge(charge)) {
        BindingTerminalAdmission::Commit(committed) => {
            let (owner, position) = committed.into_parts();
            Ok(AdmittedTerminal {
                owner,
                disposition: BindingTerminalDisposition::Committed(position),
                stored_disposition: StoredTerminalDisposition::Committed {
                    terminal_seq: position.delivery_seq(),
                },
                committed: true,
            })
        }
        BindingTerminalAdmission::Pending(pending) => {
            let (owner, position) = pending.into_parts();
            Ok(AdmittedTerminal {
                owner,
                disposition: BindingTerminalDisposition::Pending(position),
                stored_disposition: StoredTerminalDisposition::Pending,
                committed: false,
            })
        }
        BindingTerminalAdmission::Refused(refused) => {
            let error = refused.error();
            authority.install_frontier(refused.into_owner())?;
            Err(StateError::BindingTerminalAdmissionRefused { error })
        }
    }
}

pub(super) fn stored_specific_fate_intent(
    pending: &PendingBindingFate,
) -> Result<StoredSpecificFateIntent, StateError> {
    match pending.token.intent() {
        Some(SealedBindingFateIntent::Ordinary) => Ok(StoredSpecificFateIntent::Ordinary {
            attached_source_sequence: pending.attached_source_sequence,
        }),
        Some(SealedBindingFateIntent::Recovered {
            prior_binding_epoch,
            marker_delivery_seq,
        }) => Ok(StoredSpecificFateIntent::Recovered {
            attached_source_sequence: pending.attached_source_sequence,
            prior_binding_epoch: prior_binding_epoch.into(),
            marker_delivery_seq,
        }),
        None => Err(StateError::invariant(
            "sealed binding-fate token has no unique durable intent",
        )),
    }
}

const fn source_row_class(source: ConnectionFateSource) -> BindingTerminalCauseClass {
    match source {
        ConnectionFateSource::Open {
            class: ConnectionFateClass::CleanDisconnect | ConnectionFateClass::ServerShutdown,
            ..
        } => BindingTerminalCauseClass::Detached,
        ConnectionFateSource::Open {
            class: ConnectionFateClass::ConnectionLost | ConnectionFateClass::ProtocolError,
            ..
        }
        | ConnectionFateSource::UncleanServerRestart { .. } => BindingTerminalCauseClass::Died,
    }
}
