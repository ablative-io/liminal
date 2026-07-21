//! Cold application of durable connection-fate source rows.

use liminal_protocol::lifecycle::{
    BindingState, BindingTerminalCauseClass, BindingTerminalDisposition, DiedBindingTransition,
};

use super::connection_fate::{admit_terminal, stored_specific_fate_intent};
use super::log::{
    StoredDetached, StoredDetachedCause, StoredDetachedSource, StoredDied, StoredDiedCause,
};
use super::state::{ConversationAuthority, PendingSpecificFate, StateError};

impl ConversationAuthority {
    pub(super) fn replay_connection_detached(
        &mut self,
        row: &StoredDetached,
        sequence: u64,
    ) -> Result<(), StateError> {
        if !matches!(row.source, StoredDetachedSource::ConnectionClose { .. }) {
            return Err(StateError::invariant(
                "connection Detached replay received a non-connection source",
            ));
        }
        let active = self.replay_fate_active(row.participant_id, row.binding_epoch.to_epoch()?)?;
        let admitted = admit_terminal(self, active, BindingTerminalCauseClass::Detached)?;
        validate_terminal_row(row.terminal_order, row.disposition, &admitted, sequence)?;
        let binding = match row.cause {
            StoredDetachedCause::CleanDeregister => active
                .clean_disconnect(admitted.disposition)
                .binding_state(),
            StoredDetachedCause::ServerShutdown => {
                active.server_shutdown(admitted.disposition).binding_state()
            }
        };
        self.install_replayed_terminal(row.participant_id, binding, admitted, sequence, true)
    }

    pub(super) fn replay_died_source(
        &mut self,
        row: &StoredDied,
        sequence: u64,
    ) -> Result<(), StateError> {
        let binding_epoch = row.binding_epoch.to_epoch()?;
        let active = self.replay_fate_active(row.participant_id, binding_epoch)?;
        let expected_intent = self
            .slots
            .get(&row.participant_id)
            .and_then(|slot| slot.binding_fate.as_ref())
            .map(stored_specific_fate_intent)
            .transpose()?;
        if expected_intent != row.specific_fate_intent {
            return Err(StateError::invariant(
                "durable Died specific intent disagrees with the sealed slot token",
            ));
        }
        let admitted = admit_terminal(self, active, BindingTerminalCauseClass::Died)?;
        validate_terminal_row(row.terminal_order, row.disposition, &admitted, sequence)?;
        let transition = died_transition(active, row.cause, admitted.disposition);
        let terminal = match transition {
            DiedBindingTransition::Committed(terminal) => Some(terminal),
            DiedBindingTransition::Pending(_) => None,
        };
        let binding = transition.binding_state();
        self.install_replayed_terminal(row.participant_id, binding, admitted, sequence, false)?;
        if let Some(intent) = row.specific_fate_intent {
            if self
                .pending_specific_fates
                .insert(
                    row.participant_id,
                    PendingSpecificFate {
                        died_source_sequence: sequence,
                        intent,
                        terminal,
                    },
                )
                .is_some()
            {
                return Err(StateError::invariant(
                    "durable Died opened a second participant-specific fate intent",
                ));
            }
        }
        Ok(())
    }

    fn replay_fate_active(
        &self,
        participant_id: u64,
        binding_epoch: liminal_protocol::wire::BindingEpoch,
    ) -> Result<liminal_protocol::lifecycle::ActiveBinding, StateError> {
        let Some(slot) = self.slots.get(&participant_id) else {
            return Err(StateError::invariant(
                "durable connection-fate row names an absent participant",
            ));
        };
        let BindingState::Bound(active) = slot.binding else {
            return Err(StateError::invariant(
                "durable connection-fate row does not follow a Bound participant",
            ));
        };
        if active.binding_epoch != binding_epoch {
            return Err(StateError::invariant(
                "durable connection-fate row names the wrong binding epoch",
            ));
        }
        Ok(active)
    }

    fn install_replayed_terminal(
        &mut self,
        participant_id: u64,
        binding: BindingState,
        admitted: super::connection_fate::AdmittedTerminal,
        sequence: u64,
        clear_fate_token: bool,
    ) -> Result<(), StateError> {
        self.install_frontier(admitted.owner);
        self.next_order =
            self.next_order
                .checked_add(1)
                .ok_or(StateError::AllocationExhausted {
                    domain: "transaction order",
                })?;
        if admitted.committed {
            self.next_seq =
                self.next_seq
                    .checked_add(1)
                    .ok_or(StateError::AllocationExhausted {
                        domain: "delivery sequence",
                    })?;
        }
        let Some(slot) = self.slots.get_mut(&participant_id) else {
            return Err(StateError::invariant(
                "connection-fate participant disappeared during replay",
            ));
        };
        slot.binding = binding;
        if clear_fate_token {
            slot.binding_fate = None;
        }
        if self.next_log_sequence != sequence {
            return Err(StateError::invariant(
                "connection-fate replay log head disagrees with durable sequence",
            ));
        }
        self.advance_log_head()
    }
}

fn validate_terminal_row(
    terminal_order: u64,
    disposition: super::log::StoredTerminalDisposition,
    admitted: &super::connection_fate::AdmittedTerminal,
    sequence: u64,
) -> Result<(), StateError> {
    let actual_order = match admitted.disposition {
        BindingTerminalDisposition::Committed(position) => position.transaction_order(),
        BindingTerminalDisposition::Pending(position) => position.transaction_order(),
    };
    if terminal_order != actual_order || disposition != admitted.stored_disposition {
        return Err(StateError::invariant(format!(
            "durable terminal disposition diverged during replay at sequence {sequence}"
        )));
    }
    Ok(())
}

const fn died_transition(
    active: liminal_protocol::lifecycle::ActiveBinding,
    cause: StoredDiedCause,
    disposition: BindingTerminalDisposition,
) -> DiedBindingTransition {
    match cause {
        StoredDiedCause::ConnectionLost => active.connection_lost(disposition),
        StoredDiedCause::ProcessKilled => active.process_killed(disposition),
        StoredDiedCause::ProtocolError => active.protocol_error(disposition),
        StoredDiedCause::UncleanServerRestart { .. } => active.unclean_server_restart(disposition),
    }
}
