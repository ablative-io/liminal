//! Exhaustive v2 committed-source to Unit 2 extension-row projection.

use liminal_protocol::lifecycle::BindingState;
use liminal_protocol::wire::{
    BindingEpoch, DetachedCause, DiedCause, ParticipantDelivery, ParticipantId, ParticipantRecord,
};

use super::log::{
    StoredAttachAllocation, StoredAttachModeV3, StoredBindingEpoch, StoredComposedTerminal,
    StoredComposedTerminalCause, StoredComposedTerminalKind, StoredDetachedCause,
    StoredDetachedSource, StoredDied, StoredDiedCause, StoredFinalizerPresentation,
    StoredOperation, StoredRecordAdmission, StoredTerminalDisposition,
};
use super::outbox::ConversationOutboxError;
use super::outbox_log::{OutboxRow, ProducedBatch, ProducedSourceKind, ProjectedRecord};
use super::state::{ConversationAuthority, StateError};

/// Replay-only facts surrendered by the authoritative protocol transition.
pub(super) struct ReplayedProjectionFacts {
    pub(super) superseded_binding_epoch: Option<BindingEpoch>,
    pub(super) marker_delivery: Option<ParticipantDelivery>,
}

/// Captures the prior binding required by a superseding attach projection.
pub(super) fn capture_projection_prestate(
    authority: &ConversationAuthority,
    operation: &StoredOperation,
) -> ReplayedProjectionFacts {
    let superseded_binding_epoch = match operation {
        StoredOperation::Attached { request, mode, .. }
            if matches!(mode.as_ref(), StoredAttachModeV3::Superseding { .. }) =>
        {
            authority
                .slots
                .get(&request.participant_id)
                .and_then(|slot| match slot.binding {
                    BindingState::Bound(binding) => Some(binding.binding_epoch),
                    BindingState::Detached | BindingState::PendingFinalization(_) => None,
                })
        }
        _ => None,
    };
    ReplayedProjectionFacts {
        superseded_binding_epoch,
        marker_delivery: None,
    }
}

/// Projects exactly one committed v2 row after its protocol poststate is installed.
pub(super) fn project_committed_source(
    authority: &ConversationAuthority,
    source_log_sequence: u64,
    operation: &StoredOperation,
    facts: ReplayedProjectionFacts,
) -> Result<Option<OutboxRow>, StateError> {
    let projected = match operation {
        StoredOperation::Genesis { .. }
        | StoredOperation::Ordinary { .. }
        | StoredOperation::Recovered { .. } => None,
        StoredOperation::Enrolled { allocation, .. } => Some(produced(
            authority,
            source_log_sequence,
            ProducedSourceKind::Enrolled,
            Some(allocation.participant_id),
            vec![(
                allocation.attached_seq,
                ParticipantRecord::Attached {
                    affected_participant_id: allocation.participant_id,
                    binding_epoch: allocation.origin_epoch.to_epoch()?,
                },
            )],
        )?),
        StoredOperation::Attached {
            request,
            allocation,
            mode,
            ..
        } => Some(project_attached(
            authority,
            source_log_sequence,
            request.participant_id,
            allocation,
            mode,
            facts.superseded_binding_epoch,
        )?),
        StoredOperation::Detached { row } => match (&row.disposition, &row.source) {
            (
                StoredTerminalDisposition::Committed { terminal_seq },
                StoredDetachedSource::ExplicitRequestCommitted { .. }
                | StoredDetachedSource::ConnectionClose { .. },
            ) => Some(produced(
                authority,
                source_log_sequence,
                ProducedSourceKind::Detached,
                Some(row.participant_id),
                vec![(
                    *terminal_seq,
                    ParticipantRecord::Detached {
                        affected_participant_id: row.participant_id,
                        binding_epoch: row.binding_epoch.to_epoch()?,
                        cause: match row.cause {
                            StoredDetachedCause::CleanDeregister => DetachedCause::CleanDeregister,
                            StoredDetachedCause::ServerShutdown => DetachedCause::ServerShutdown,
                        },
                    },
                )],
            )?),
            _ => None,
        },
        StoredOperation::ZeroDebtAck { request, .. }
        | StoredOperation::NonzeroDebtAck { request, .. } => Some(OutboxRow::AckAdvanced {
            source_log_sequence,
            participant_id: request.participant_id,
            through_seq: request.through_seq,
        }),
        StoredOperation::MarkerDrained { .. } => {
            let delivery = facts.marker_delivery.ok_or_else(|| {
                StateError::invariant("marker drain replay lost its typed delivery projection")
            })?;
            Some(produced(
                authority,
                source_log_sequence,
                ProducedSourceKind::MarkerDrained,
                None,
                vec![(delivery.delivery_seq, delivery.record)],
            )?)
        }
        StoredOperation::RecordAdmission { row } => Some(project_record_admission(
            authority,
            source_log_sequence,
            row,
        )?),
        StoredOperation::Left { row } => project_left(authority, source_log_sequence, row)?,
        StoredOperation::Died { row } => project_died(authority, source_log_sequence, row)?,
    };
    Ok(projected)
}

fn project_left(
    authority: &ConversationAuthority,
    source_log_sequence: u64,
    row: &super::log::StoredLeaveV3,
) -> Result<Option<OutboxRow>, StateError> {
    if matches!(
        row.finalizer_presentation,
        StoredFinalizerPresentation::ConsumeRecoveredReservation { .. }
    ) {
        return Ok(None);
    }
    produced(
        authority,
        source_log_sequence,
        ProducedSourceKind::Left,
        Some(row.request.participant_id),
        vec![(
            row.left_delivery_seq,
            ParticipantRecord::Left {
                affected_participant_id: row.request.participant_id,
                ended_binding_epoch: row
                    .ended_binding_epoch
                    .map(StoredBindingEpoch::to_epoch)
                    .transpose()?,
            },
        )],
    )
    .map(Some)
}

fn project_died(
    authority: &ConversationAuthority,
    source_log_sequence: u64,
    row: &StoredDied,
) -> Result<Option<OutboxRow>, StateError> {
    let StoredTerminalDisposition::Committed { terminal_seq } = row.disposition else {
        return Ok(None);
    };
    let cause = match row.cause {
        StoredDiedCause::ConnectionLost => DiedCause::ConnectionLost,
        StoredDiedCause::ProcessKilled => DiedCause::ProcessKilled,
        StoredDiedCause::ProtocolError => DiedCause::ProtocolError,
        StoredDiedCause::UncleanServerRestart {
            prior_server_incarnation,
        } => DiedCause::UncleanServerRestart {
            prior_server_incarnation,
        },
    };
    produced(
        authority,
        source_log_sequence,
        ProducedSourceKind::Died,
        Some(row.participant_id),
        vec![(
            terminal_seq,
            ParticipantRecord::Died {
                affected_participant_id: row.participant_id,
                binding_epoch: row.binding_epoch.to_epoch()?,
                cause,
            },
        )],
    )
    .map(Some)
}

fn project_attached(
    authority: &ConversationAuthority,
    source_log_sequence: u64,
    participant_id: ParticipantId,
    allocation: &StoredAttachAllocation,
    mode: &StoredAttachModeV3,
    superseded_binding_epoch: Option<BindingEpoch>,
) -> Result<OutboxRow, StateError> {
    let records =
        project_attached_records(participant_id, allocation, mode, superseded_binding_epoch)?;
    produced(
        authority,
        source_log_sequence,
        ProducedSourceKind::Attached,
        Some(participant_id),
        records,
    )
}

pub(super) fn project_attached_records(
    participant_id: ParticipantId,
    allocation: &StoredAttachAllocation,
    mode: &StoredAttachModeV3,
    superseded_binding_epoch: Option<BindingEpoch>,
) -> Result<Vec<(u64, ParticipantRecord)>, StateError> {
    let attached = (
        allocation.attached_seq,
        ParticipantRecord::Attached {
            affected_participant_id: participant_id,
            binding_epoch: allocation.binding_epoch.to_epoch()?,
        },
    );
    let records = match mode {
        StoredAttachModeV3::Ordinary => vec![attached],
        StoredAttachModeV3::Superseding {
            terminal_delivery_seq,
            ..
        } => {
            let prior_epoch = superseded_binding_epoch.ok_or_else(|| {
                StateError::invariant("superseding attach projection lost its prior binding epoch")
            })?;
            vec![
                (
                    *terminal_delivery_seq,
                    ParticipantRecord::Detached {
                        affected_participant_id: participant_id,
                        binding_epoch: prior_epoch,
                        cause: DetachedCause::Superseded,
                    },
                ),
                attached,
            ]
        }
        StoredAttachModeV3::Fenced {
            prior_binding_epoch,
            composed_terminal,
            ..
        } => match composed_terminal {
            Some(terminal)
                if terminal.presentation == StoredFinalizerPresentation::PresentEnclosing =>
            {
                vec![
                    project_composed_terminal(participant_id, *prior_binding_epoch, terminal)?,
                    attached,
                ]
            }
            None | Some(_) => vec![attached],
        },
    };
    Ok(records)
}

fn project_composed_terminal(
    participant_id: ParticipantId,
    prior_binding_epoch: StoredBindingEpoch,
    terminal: &StoredComposedTerminal,
) -> Result<(u64, ParticipantRecord), StateError> {
    let binding_epoch = prior_binding_epoch.to_epoch()?;
    let body = match (terminal.kind, terminal.cause) {
        (StoredComposedTerminalKind::Detached, StoredComposedTerminalCause::CleanDeregister) => {
            ParticipantRecord::Detached {
                affected_participant_id: participant_id,
                binding_epoch,
                cause: DetachedCause::CleanDeregister,
            }
        }
        (StoredComposedTerminalKind::Detached, StoredComposedTerminalCause::ServerShutdown) => {
            ParticipantRecord::Detached {
                affected_participant_id: participant_id,
                binding_epoch,
                cause: DetachedCause::ServerShutdown,
            }
        }
        (StoredComposedTerminalKind::Died, StoredComposedTerminalCause::ConnectionLost) => {
            ParticipantRecord::Died {
                affected_participant_id: participant_id,
                binding_epoch,
                cause: DiedCause::ConnectionLost,
            }
        }
        (StoredComposedTerminalKind::Died, StoredComposedTerminalCause::ProcessKilled) => {
            ParticipantRecord::Died {
                affected_participant_id: participant_id,
                binding_epoch,
                cause: DiedCause::ProcessKilled,
            }
        }
        (StoredComposedTerminalKind::Died, StoredComposedTerminalCause::ProtocolError) => {
            ParticipantRecord::Died {
                affected_participant_id: participant_id,
                binding_epoch,
                cause: DiedCause::ProtocolError,
            }
        }
        (
            StoredComposedTerminalKind::Died,
            StoredComposedTerminalCause::UncleanServerRestart {
                prior_server_incarnation,
            },
        ) => ParticipantRecord::Died {
            affected_participant_id: participant_id,
            binding_epoch,
            cause: DiedCause::UncleanServerRestart {
                prior_server_incarnation,
            },
        },
        _ => {
            return Err(StateError::invariant(
                "composed terminal kind and cause disagree",
            ));
        }
    };
    Ok((terminal.delivery_seq, body))
}

fn project_record_admission(
    authority: &ConversationAuthority,
    source_log_sequence: u64,
    row: &StoredRecordAdmission,
) -> Result<OutboxRow, StateError> {
    produced(
        authority,
        source_log_sequence,
        ProducedSourceKind::RecordAdmission,
        Some(row.request.participant_id),
        vec![(
            row.delivery_seq,
            ParticipantRecord::OrdinaryRecord {
                sender_participant_id: row.request.participant_id,
                payload: row.request.payload.clone(),
            },
        )],
    )
}

fn produced(
    authority: &ConversationAuthority,
    source_log_sequence: u64,
    source_kind: ProducedSourceKind,
    sender: Option<ParticipantId>,
    records: Vec<(u64, ParticipantRecord)>,
) -> Result<OutboxRow, StateError> {
    let recipients: Vec<_> = authority
        .slots
        .iter()
        .filter_map(|(participant_id, slot)| {
            matches!(slot.binding, BindingState::Bound(_)).then_some(*participant_id)
        })
        .filter(|participant_id| Some(*participant_id) != sender)
        .collect();
    let mut ordered_records = Vec::with_capacity(records.len());
    for (delivery_seq, body) in records {
        ordered_records.push(
            ProjectedRecord::try_new(
                authority.conversation_id,
                delivery_seq,
                body,
                recipients.clone(),
                sender,
            )
            .map_err(ConversationOutboxError::from)?,
        );
    }
    Ok(OutboxRow::Produced(ProducedBatch::new(
        source_log_sequence,
        source_kind,
        ordered_records,
    )))
}
