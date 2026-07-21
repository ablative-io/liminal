//! Validate-first source and presentation checks for composed fenced terminals.

use std::collections::BTreeMap;

use super::log::{
    DecodedStoredOperation, FencedAttachProofRefusal, OperationLog, OperationLogError,
    StoredAttachModeV3, StoredComposedTerminal, StoredComposedTerminalCause,
    StoredComposedTerminalKind, StoredDetachedCause, StoredDiedCause, StoredFinalizerPresentation,
    StoredOperationV3, StoredRecoveredFate, StoredRecoveredPresentation, StoredTerminalDisposition,
};
use super::state::StateError;

/// Config-bounded replay state for open recovered finalizer reservations.
pub(super) struct ComposedTerminalValidation {
    recovered_reservations: BTreeMap<u64, u64>,
    maximum_recovered_reservations: usize,
}

impl ComposedTerminalValidation {
    pub(super) const fn new(maximum_recovered_reservations: usize) -> Self {
        Self {
            recovered_reservations: BTreeMap::new(),
            maximum_recovered_reservations,
        }
    }

    pub(super) fn active_reservation_count(&self) -> usize {
        self.recovered_reservations.len()
    }

    /// Validates one decoded operation without mutating conversation authority.
    pub(super) async fn validate(
        &mut self,
        log: &OperationLog,
        sequence: u64,
        operation: &DecodedStoredOperation,
    ) -> Result<(), StateError> {
        let DecodedStoredOperation::V3(operation) = operation else {
            return Ok(());
        };
        if let StoredOperationV3::Recovered { row, .. } = operation {
            return self.validate_recovered_reservation(sequence, row);
        }
        let StoredOperationV3::Attached { request, mode, .. } = operation else {
            return Ok(());
        };
        let StoredAttachModeV3::Fenced {
            prior_binding_epoch,
            composed_terminal: Some(terminal),
            ..
        } = mode.as_ref()
        else {
            return Ok(());
        };
        self.validate_pending_source(
            log,
            sequence,
            request.participant_id,
            *prior_binding_epoch,
            terminal,
        )
        .await?;
        match terminal.presentation {
            StoredFinalizerPresentation::PresentEnclosing => {
                if self
                    .recovered_reservations
                    .contains_key(&terminal.pending_source_sequence)
                {
                    return Err(composed_error(
                        sequence,
                        FencedAttachProofRefusal::ComposedRecoveredReservationMismatch,
                    ));
                }
            }
            StoredFinalizerPresentation::ConsumeRecoveredReservation {
                recovered_source_sequence,
            } => {
                let reserved = self
                    .recovered_reservations
                    .remove(&terminal.pending_source_sequence);
                if reserved != Some(recovered_source_sequence) {
                    return Err(composed_error(
                        sequence,
                        FencedAttachProofRefusal::ComposedRecoveredReservationMismatch,
                    ));
                }
                let recovered = log
                    .read_at(recovered_source_sequence)
                    .await?
                    .ok_or_else(|| {
                        composed_error(
                            sequence,
                            FencedAttachProofRefusal::ComposedRecoveredReservationMismatch,
                        )
                    })?;
                let DecodedStoredOperation::V3(StoredOperationV3::Recovered { row, .. }) =
                    recovered.operation
                else {
                    return Err(composed_error(
                        sequence,
                        FencedAttachProofRefusal::ComposedRecoveredReservationMismatch,
                    ));
                };
                if row.presentation
                    != StoredRecoveredPresentation::RecoveredOwnsAndReservesFinalizer
                    || row.died_source_sequence != terminal.pending_source_sequence
                    || row.participant_id != request.participant_id
                {
                    return Err(composed_error(
                        sequence,
                        FencedAttachProofRefusal::ComposedRecoveredReservationMismatch,
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_recovered_reservation(
        &mut self,
        sequence: u64,
        row: &StoredRecoveredFate,
    ) -> Result<(), StateError> {
        if row.presentation != StoredRecoveredPresentation::RecoveredOwnsAndReservesFinalizer {
            return Ok(());
        }
        if row.died_source_sequence >= sequence
            || self
                .recovered_reservations
                .contains_key(&row.died_source_sequence)
        {
            return Err(composed_error(
                sequence,
                FencedAttachProofRefusal::ComposedRecoveredReservationMismatch,
            ));
        }
        if self.recovered_reservations.len() >= self.maximum_recovered_reservations {
            return Err(composed_error(
                sequence,
                FencedAttachProofRefusal::ComposedRecoveredReservationCapacity,
            ));
        }
        self.recovered_reservations
            .insert(row.died_source_sequence, sequence);
        Ok(())
    }

    async fn validate_pending_source(
        &self,
        log: &OperationLog,
        sequence: u64,
        participant_id: u64,
        prior_binding_epoch: super::log::StoredBindingEpoch,
        terminal: &StoredComposedTerminal,
    ) -> Result<(), StateError> {
        let source = log
            .read_at(terminal.pending_source_sequence)
            .await?
            .ok_or_else(|| {
                composed_error(
                    sequence,
                    FencedAttachProofRefusal::ComposedPendingSourceMismatch,
                )
            })?;
        let matches = match source.operation {
            DecodedStoredOperation::V3(StoredOperationV3::Died { row }) => {
                terminal.kind == StoredComposedTerminalKind::Died
                    && row.participant_id == participant_id
                    && row.binding_epoch == prior_binding_epoch
                    && row.terminal_order == terminal.transaction_order
                    && row.disposition == StoredTerminalDisposition::Pending
                    && died_cause_matches(row.cause, terminal.cause)
            }
            DecodedStoredOperation::V3(StoredOperationV3::Detached { row }) => {
                terminal.kind == StoredComposedTerminalKind::Detached
                    && row.participant_id == participant_id
                    && row.binding_epoch == prior_binding_epoch
                    && row.terminal_order == terminal.transaction_order
                    && row.disposition == StoredTerminalDisposition::Pending
                    && detached_cause_matches(row.cause, terminal.cause)
            }
            DecodedStoredOperation::V2(_) | DecodedStoredOperation::V3(_) => false,
        };
        if matches {
            Ok(())
        } else {
            Err(composed_error(
                sequence,
                FencedAttachProofRefusal::ComposedPendingSourceMismatch,
            ))
        }
    }
}

const fn died_cause_matches(cause: StoredDiedCause, audit: StoredComposedTerminalCause) -> bool {
    match (cause, audit) {
        (StoredDiedCause::ConnectionLost, StoredComposedTerminalCause::ConnectionLost)
        | (StoredDiedCause::ProcessKilled, StoredComposedTerminalCause::ProcessKilled)
        | (StoredDiedCause::ProtocolError, StoredComposedTerminalCause::ProtocolError) => true,
        (
            StoredDiedCause::UncleanServerRestart {
                prior_server_incarnation: left,
            },
            StoredComposedTerminalCause::UncleanServerRestart {
                prior_server_incarnation: right,
            },
        ) => left == right,
        _ => false,
    }
}

const fn detached_cause_matches(
    cause: StoredDetachedCause,
    audit: StoredComposedTerminalCause,
) -> bool {
    matches!(
        (cause, audit),
        (
            StoredDetachedCause::CleanDeregister,
            StoredComposedTerminalCause::CleanDeregister
        ) | (
            StoredDetachedCause::ServerShutdown,
            StoredComposedTerminalCause::ServerShutdown
        )
    )
}

const fn composed_error(sequence: u64, reason: FencedAttachProofRefusal) -> StateError {
    StateError::Log(OperationLogError::FencedAttachProof { sequence, reason })
}
