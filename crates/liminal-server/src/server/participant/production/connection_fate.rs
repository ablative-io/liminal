//! Typed per-conversation connection-fate transaction boundary.
//!
//! Leg 4b owns target discovery and orchestration. The durable Died/Detached
//! source-row transaction body is deliberately isolated in [`PreparedConnectionFate::complete`]
//! for leg 4c; callers cannot supply participant ids or binding epochs.

use liminal_protocol::lifecycle::BindingState;
use liminal_protocol::wire::{BindingEpoch, ParticipantId};

use crate::server::participant::{ConnectionFateClass, ConnectionFateWorkItem};

use super::state::{ConversationAuthority, DurableAppend, StateError};

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
    /// This is the typed leg-4c transaction seam. Today it verifies that every
    /// recorded target still names the authority-selected Bound epoch and then
    /// completes without fabricating a source row. Leg 4c replaces this body with
    /// the disposition/source append-and-flush transaction while retaining the
    /// non-caller-constructible target set and source authority.
    pub(super) fn complete(
        self,
        authority: &mut ConversationAuthority,
        appender: &dyn DurableAppend,
    ) -> Result<(), StateError> {
        let _: &dyn DurableAppend = appender;
        for target in self.targets {
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
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn targets(&self) -> &[ConnectionFateTarget] {
        &self.targets
    }
}
