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
    let admitted = admit_terminal(authority, active, source_row_class(source))?;

    let completed = source_operation(
        source,
        active,
        admitted.disposition,
        admitted.stored_disposition,
        specific_fate_intent,
    );
    let projection_facts = capture_projection_prestate(authority, &completed.operation);
    authority.route_fate_occurrence(&completed.operation, source_sequence)?;
    appender.append(&completed.operation, authority.next_log_sequence)?;
    authority.install_frontier(admitted.owner)?;
    authority.next_order = allocations.next_order;
    if admitted.committed {
        authority.next_seq = allocations.next_sequence;
    }
    authority.next_log_sequence = allocations.next_log_sequence;
    let Some(slot) = authority.slots.get_mut(&target.participant_id) else {
        return Err(StateError::invariant(
            "connection-fate target disappeared after durable source append",
        ));
    };
    slot.binding = completed.binding_state;
    let binding_fate = if specific_fate_intent.is_some() {
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
    if let Some(intent) = specific_fate_intent {
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

fn record_terminal_impact(
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
            Err(StateError::invariant(format!(
                "binding-terminal admission refused: {error:?}"
            )))
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
