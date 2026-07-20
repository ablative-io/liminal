//! Participant operation schema preflight, migration, and cold replay.

use liminal_protocol::lifecycle::{BindingState, RecipientAckObligations};
use liminal_protocol::wire::ParticipantDelivery;

use crate::config::types::ParticipantConfig;

use super::log::{
    DecodedOperation, DecodedStoredOperation, OperationLog, OperationLogError,
    OperationSchemaPhase, StoredDetached, StoredDetachedCause, StoredDetachedSource,
    StoredOperation, StoredOperationV2, StoredTerminalDisposition, V2AttachedPrestate,
    migrate_v2_attached,
};
use super::ops_session::DetachReplayInputs;
use super::outbox::ConversationOutboxLimits;
use super::outbox_log::OutboxLog;
use super::outbox_projection::{capture_projection_prestate, project_committed_source};
#[cfg(test)]
use super::outbox_replay::AggregateExtensionMerge;
use super::outbox_replay::{ExtensionMerge, RestoreError};
use super::state::{ConversationAuthority, StateError};

impl ConversationAuthority {
    /// Cold-replays one conversation's complete durable log.
    pub(super) async fn replay(
        conversation_id: u64,
        log: &OperationLog,
        outbox_log: &OutboxLog,
        config: &ParticipantConfig,
        outbox_limits: ConversationOutboxLimits,
    ) -> Result<Self, RestoreError> {
        validate_operation_schema(log).await?;
        let mut authority = Self::empty(conversation_id);
        let mut merge = ExtensionMerge::new(outbox_log, conversation_id, outbox_limits)?;
        merge.apply_boundary(&mut authority, 0, None).await?;
        let mut sequence = 0_u64;
        let mut phase = OperationSchemaPhase::V2Prefix;
        loop {
            let page = log
                .read_page(sequence, phase)
                .await
                .map_err(StateError::from)?;
            phase = page.next_phase;
            if page.rows.is_empty() {
                break;
            }
            let page_len = page.rows.len();
            for decoded in page.rows {
                let stored_sequence = decoded.sequence;
                if stored_sequence != sequence {
                    return Err(RestoreError::Semantic(StateError::Log(
                        super::log::OperationLogError::Sequence {
                            expected: sequence,
                            actual: stored_sequence,
                        },
                    )));
                }
                let operation = authority.decode_operation(decoded)?;
                let operation_for_projection = operation.clone();
                let ack_obligations = match &operation {
                    StoredOperation::ZeroDebtAck { request, .. } => {
                        let acknowledged_through = authority
                            .slots
                            .get(&request.participant_id)
                            .map_or(0, |slot| slot.member.cursor());
                        Some(merge.recipient_ack_obligations(
                            request.participant_id,
                            acknowledged_through,
                        )?)
                    }
                    _ => None,
                };
                authority.begin_observer_progress_source()?;
                let mut facts = capture_projection_prestate(&authority, &operation_for_projection);
                facts.marker_delivery = authority.replay_operation(
                    operation,
                    stored_sequence,
                    config,
                    ack_obligations,
                    log.store(),
                )?;
                let expected = project_committed_source(
                    &authority,
                    stored_sequence,
                    &operation_for_projection,
                    facts,
                )?;
                authority.end_observer_progress_source()?;
                sequence = sequence
                    .checked_add(1)
                    .ok_or(StateError::AllocationExhausted {
                        domain: "log sequence",
                    })?;
                merge
                    .apply_boundary(&mut authority, sequence, expected.as_ref())
                    .await?;
            }
            if page_len < super::log::READ_BATCH_SIZE {
                break;
            }
        }
        if authority.tokens.is_empty() {
            if authority.frontier.is_some() {
                return Err(RestoreError::Semantic(StateError::invariant(
                    "durably empty conversation rebuilt an executable frontier",
                )));
            }
        } else if authority.frontier.is_none() {
            return Err(RestoreError::Semantic(StateError::invariant(
                "enrolled conversation replay completed without executable frontier ownership",
            )));
        }
        merge.finish(&mut authority, sequence)?;
        Ok(authority)
    }

    /// Frozen pre-W3 complete-vector replay used only by equivalence oracles.
    #[cfg(test)]
    pub(super) async fn replay_aggregate_reference(
        conversation_id: u64,
        log: &OperationLog,
        outbox_log: &OutboxLog,
        extension_rows: Vec<(u64, super::outbox_log::OutboxRow)>,
        config: &ParticipantConfig,
        outbox_limits: ConversationOutboxLimits,
    ) -> Result<Self, StateError> {
        let mut authority = Self::empty(conversation_id);
        let mut merge = AggregateExtensionMerge::new(
            outbox_log,
            extension_rows,
            conversation_id,
            outbox_limits,
        )?;
        validate_operation_schema(log).await?;
        merge.apply_boundary(&mut authority, 0, None).await?;
        let mut sequence = 0_u64;
        let mut phase = OperationSchemaPhase::V2Prefix;
        loop {
            let page = log.read_page(sequence, phase).await?;
            phase = page.next_phase;
            if page.rows.is_empty() {
                break;
            }
            let page_len = page.rows.len();
            for decoded in page.rows {
                let stored_sequence = decoded.sequence;
                if stored_sequence != sequence {
                    return Err(StateError::Log(super::log::OperationLogError::Sequence {
                        expected: sequence,
                        actual: stored_sequence,
                    }));
                }
                let operation = authority.decode_operation(decoded)?;
                let operation_for_projection = operation.clone();
                let ack_obligations = match &operation {
                    StoredOperation::ZeroDebtAck { request, .. } => {
                        let acknowledged_through = authority
                            .slots
                            .get(&request.participant_id)
                            .map_or(0, |slot| slot.member.cursor());
                        Some(merge.recipient_ack_obligations(
                            request.participant_id,
                            acknowledged_through,
                        )?)
                    }
                    _ => None,
                };
                authority.begin_observer_progress_source()?;
                let mut facts = capture_projection_prestate(&authority, &operation_for_projection);
                facts.marker_delivery = authority.replay_operation(
                    operation,
                    stored_sequence,
                    config,
                    ack_obligations,
                    log.store(),
                )?;
                let expected = project_committed_source(
                    &authority,
                    stored_sequence,
                    &operation_for_projection,
                    facts,
                )?;
                authority.end_observer_progress_source()?;
                sequence = sequence
                    .checked_add(1)
                    .ok_or(StateError::AllocationExhausted {
                        domain: "log sequence",
                    })?;
                merge
                    .apply_boundary(&mut authority, sequence, expected.as_ref())
                    .await?;
            }
            if page_len < super::log::READ_BATCH_SIZE {
                break;
            }
        }
        if authority.tokens.is_empty() {
            if authority.frontier.is_some() {
                return Err(StateError::invariant(
                    "durably empty conversation rebuilt an executable frontier",
                ));
            }
        } else if authority.frontier.is_none() {
            return Err(StateError::invariant(
                "enrolled conversation replay completed without executable frontier ownership",
            ));
        }
        merge.finish(&mut authority, sequence)?;
        Ok(authority)
    }

    /// Converts a frozen v2 row into the v3 in-memory model using exact replay
    /// prestate where the old Attached option grammar omitted its prior epoch.
    fn decode_operation(&self, decoded: DecodedOperation) -> Result<StoredOperation, StateError> {
        match (decoded.schema_version, decoded.operation) {
            (super::log::SCHEMA_VERSION, DecodedStoredOperation::V3(operation)) => Ok(operation),
            (super::log::SCHEMA_VERSION_V2, DecodedStoredOperation::V2(operation)) => {
                self.migrate_v2_operation(operation, decoded.sequence)
            }
            (actual, _) => Err(OperationLogError::SchemaVersion(actual).into()),
        }
    }

    fn migrate_v2_operation(
        &self,
        operation: StoredOperationV2,
        sequence: u64,
    ) -> Result<StoredOperation, StateError> {
        Ok(match operation {
            StoredOperationV2::Genesis { event } => StoredOperation::Genesis { event },
            StoredOperationV2::Enrolled {
                request,
                allocation,
                event,
            } => StoredOperation::Enrolled {
                request,
                allocation,
                event,
            },
            StoredOperationV2::Attached {
                request,
                secret_verified,
                allocation,
                event,
            } => {
                let prestate = match self
                    .slots
                    .get(&request.participant_id)
                    .map(|slot| slot.binding)
                {
                    Some(BindingState::Detached) => V2AttachedPrestate::Detached,
                    Some(BindingState::Bound(active)) => V2AttachedPrestate::Bound {
                        binding_epoch: active.binding_epoch.into(),
                    },
                    Some(BindingState::PendingFinalization(_)) | None => V2AttachedPrestate::Other,
                };
                migrate_v2_attached(
                    request,
                    secret_verified,
                    allocation,
                    event,
                    prestate,
                    sequence,
                )?
            }
            StoredOperationV2::Detached {
                request,
                verifier,
                receiving_epoch,
                terminal_order,
                terminal_seq,
                event,
            } => StoredOperation::Detached {
                row: StoredDetached {
                    participant_id: request.participant_id,
                    binding_epoch: receiving_epoch,
                    cause: StoredDetachedCause::CleanDeregister,
                    terminal_order,
                    disposition: StoredTerminalDisposition::Committed { terminal_seq },
                    source: StoredDetachedSource::ExplicitRequestCommitted {
                        request,
                        secret_verified: true,
                        verifier,
                        receiving_epoch,
                        event,
                    },
                },
            },
            StoredOperationV2::ZeroDebtAck {
                request,
                receiving_epoch,
                contiguously_available_through,
            } => StoredOperation::ZeroDebtAck {
                request,
                receiving_epoch,
                contiguously_available_through,
            },
            StoredOperationV2::MarkerDrained { row } => StoredOperation::MarkerDrained { row },
            StoredOperationV2::RecordAdmission { row } => StoredOperation::RecordAdmission { row },
            StoredOperationV2::Left { row } => StoredOperation::Left { row },
        })
    }

    /// Replays one durable entry through the exact live transition cores.
    fn replay_operation(
        &mut self,
        operation: StoredOperation,
        sequence: u64,
        config: &ParticipantConfig,
        ack_obligations: Option<(RecipientAckObligations, u64)>,
        store: std::sync::Arc<dyn liminal::durability::DurableStore>,
    ) -> Result<Option<ParticipantDelivery>, StateError> {
        match operation {
            StoredOperation::Genesis { event } => self.replay_genesis(&event).map(|()| None),
            StoredOperation::Enrolled {
                request,
                allocation,
                event,
            } => self
                .replay_enrolled(request, &allocation, &event, sequence, config)
                .map(|()| None),
            StoredOperation::Attached {
                request,
                secret_verified,
                allocation,
                mode,
                event,
            } => {
                if !secret_verified {
                    return Err(StateError::invariant(
                        "durable attach entry recorded an unverified secret",
                    ));
                }
                self.replay_attached(request, &allocation, &mode, &event, sequence, store)
                    .map(|()| None)
            }
            StoredOperation::Detached { row } => match (row.disposition, row.source) {
                (
                    StoredTerminalDisposition::Committed { terminal_seq },
                    StoredDetachedSource::ExplicitRequestCommitted {
                        request,
                        secret_verified: true,
                        verifier,
                        receiving_epoch,
                        event,
                    },
                ) if row.cause == StoredDetachedCause::CleanDeregister
                    && row.participant_id == request.participant_id
                    && row.binding_epoch == receiving_epoch =>
                {
                    self.replay_detached(
                        DetachReplayInputs {
                            request,
                            verifier,
                            receiving_epoch,
                            terminal_order: row.terminal_order,
                            terminal_seq,
                        },
                        &event,
                        sequence,
                    )
                    .map(|()| None)
                }
                _ => Err(OperationLogError::V3FateReplayUnavailable { sequence }.into()),
            },
            StoredOperation::Died { .. }
            | StoredOperation::Ordinary { .. }
            | StoredOperation::Recovered { .. } => {
                Err(OperationLogError::V3FateReplayUnavailable { sequence }.into())
            }
            StoredOperation::ZeroDebtAck {
                request,
                receiving_epoch,
                contiguously_available_through,
            } => {
                let (obligations, reconciled_available_through) =
                    ack_obligations.ok_or_else(|| {
                        StateError::invariant(
                            "zero-debt ack replay is missing recipient obligations",
                        )
                    })?;
                self.replay_zero_debt_ack(
                    request,
                    receiving_epoch,
                    contiguously_available_through,
                    reconciled_available_through,
                    &obligations,
                )
                .map(|()| None)
            }
            StoredOperation::RecordAdmission { row } => {
                self.replay_record_admission(&row, config).map(|()| None)
            }
            StoredOperation::MarkerDrained { row } => self.replay_marker_drain(&row).map(Some),
            StoredOperation::Left { row } => self.replay_leave(&row).map(|()| None),
        }
    }
}

/// Completes a bounded-page schema and contiguity pass before replay mutates
/// authority or reconciles any extension row. The apply pass rereads pages.
pub(super) async fn validate_operation_schema(log: &OperationLog) -> Result<(), StateError> {
    let mut sequence = 0_u64;
    let mut phase = OperationSchemaPhase::V2Prefix;
    loop {
        let page = log.read_page(sequence, phase).await?;
        phase = page.next_phase;
        if page.rows.is_empty() {
            return Ok(());
        }
        let page_len = page.rows.len();
        for decoded in page.rows {
            if decoded.sequence != sequence {
                return Err(OperationLogError::Sequence {
                    expected: sequence,
                    actual: decoded.sequence,
                }
                .into());
            }
            sequence = sequence
                .checked_add(1)
                .ok_or(StateError::AllocationExhausted {
                    domain: "log sequence",
                })?;
        }
        if page_len < super::log::READ_BATCH_SIZE {
            return Ok(());
        }
    }
}
