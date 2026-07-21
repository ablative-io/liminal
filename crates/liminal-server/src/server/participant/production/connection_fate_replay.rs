//! Cold application of durable connection-fate source rows.

use liminal_protocol::lifecycle::{
    BindingState, BindingTerminalCauseClass, BindingTerminalDisposition, DetachCell,
    DiedBindingTransition, start_blocked_detach,
};

use super::connection_fate::{admit_terminal, stored_specific_fate_intent};
use super::log::{
    StoredDetached, StoredDetachedCause, StoredDetachedSource, StoredDied, StoredDiedCause,
    StoredOrdinaryTerminalSource,
};
use super::observer_progress::ObserverProgressSourceMetadata;
use super::state::{
    ConversationAuthority, PendingSpecificFate, PendingSpecificFateTerminal, StateError,
};

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
        let transition = match row.cause {
            StoredDetachedCause::CleanDeregister => active.clean_disconnect(admitted.disposition),
            StoredDetachedCause::ServerShutdown => active.server_shutdown(admitted.disposition),
        };
        let projection = transition.observer_progress_projection();
        let binding = transition.binding_state();
        self.install_replayed_terminal(row.participant_id, binding, admitted, sequence, true)?;
        if let (
            Some(projection),
            super::log::StoredTerminalDisposition::Committed { terminal_seq },
        ) = (projection, row.disposition)
        {
            let metadata = ObserverProgressSourceMetadata::detached(
                sequence,
                self.conversation_id,
                row.participant_id,
                terminal_seq,
            );
            self.record_observer_progress_projection(projection, metadata)?;
        }
        Ok(())
    }

    pub(super) fn replay_explicit_pending_detached(
        &mut self,
        row: &StoredDetached,
        sequence: u64,
    ) -> Result<(), StateError> {
        let StoredDetachedSource::ExplicitRequestPending {
            request,
            secret_verified: true,
            verifier,
            receiving_epoch,
            observer_baseline,
        } = row.source.clone()
        else {
            return Err(StateError::invariant(
                "pending explicit Detached replay received contradictory source evidence",
            ));
        };
        if row.cause != StoredDetachedCause::CleanDeregister
            || row.disposition != super::log::StoredTerminalDisposition::Pending
            || row.participant_id != request.participant_id
            || row.binding_epoch != receiving_epoch
            || observer_baseline != self.observer_progress
        {
            return Err(StateError::invariant(
                "pending explicit Detached replay row disagrees with its request or observer baseline",
            ));
        }
        let active = self.replay_fate_active(row.participant_id, row.binding_epoch.to_epoch()?)?;
        let admitted = admit_terminal(self, active, BindingTerminalCauseClass::Detached)?;
        validate_terminal_row(row.terminal_order, row.disposition, &admitted, sequence)?;
        let BindingTerminalDisposition::Pending(position) = admitted.disposition else {
            return Err(StateError::invariant(
                "pending explicit Detached row replay selected a committed terminal",
            ));
        };
        let request = request.to_request()?;
        let verified = active
            .verify_detach_request(request.clone(), verifier)
            .map_err(|error| {
                StateError::invariant(format!(
                    "pending explicit Detached request verification failed: {error:?}"
                ))
            })?;
        let (participant_id, mut slot) =
            self.slots
                .remove_entry(&row.participant_id)
                .ok_or_else(|| {
                    StateError::invariant(
                        "pending explicit Detached participant disappeared during replay",
                    )
                })?;
        let transition = start_blocked_detach(
            slot.member,
            verified,
            slot.cell,
            position,
            observer_baseline,
        )
        .map_err(|error| {
            StateError::invariant(format!(
                "pending explicit Detached transition failed: {error:?}"
            ))
        })?;
        let (member, binding, cell, _outcome) = transition.into_parts();
        self.install_frontier(admitted.owner)?;
        self.next_order =
            self.next_order
                .checked_add(1)
                .ok_or(StateError::AllocationExhausted {
                    domain: "transaction order",
                })?;
        if self.next_log_sequence != sequence {
            return Err(StateError::invariant(
                "pending explicit Detached replay log head disagrees with durable sequence",
            ));
        }
        self.advance_log_head()?;
        slot.member = member;
        slot.binding = binding;
        slot.cell = DetachCell::Pending(cell);
        slot.exact_detach_token = Some(request.detach_attempt_token);
        self.slots.insert(participant_id, slot);
        Ok(())
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
        let projection = transition.observer_progress_projection();
        let terminal = match transition {
            DiedBindingTransition::Committed(terminal) => Some(PendingSpecificFateTerminal {
                terminal,
                source: StoredOrdinaryTerminalSource::DiedCommitted {
                    died_source_sequence: sequence,
                },
            }),
            DiedBindingTransition::Pending(_) => None,
        };
        let binding = transition.binding_state();
        self.install_replayed_terminal(row.participant_id, binding, admitted, sequence, false)?;
        if let (
            Some(projection),
            super::log::StoredTerminalDisposition::Committed { terminal_seq },
        ) = (projection, row.disposition)
        {
            let metadata = ObserverProgressSourceMetadata::died(
                sequence,
                self.conversation_id,
                row.participant_id,
                terminal_seq,
            );
            self.record_observer_progress_projection(projection, metadata)?;
        }
        if let Some(intent) = row.specific_fate_intent {
            let binding_fate = self
                .slots
                .get_mut(&row.participant_id)
                .and_then(|slot| slot.binding_fate.take())
                .ok_or_else(|| {
                    StateError::invariant(
                        "durable Died replay lost its sealed binding-fate authority",
                    )
                })?;
            if self
                .pending_specific_fates
                .insert(
                    row.participant_id,
                    PendingSpecificFate {
                        died_source_sequence: sequence,
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
        self.install_frontier(admitted.owner)?;
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
