//! Exhaustive v2 committed-source to Unit 2 extension-row projection.

use liminal_protocol::lifecycle::BindingState;
use liminal_protocol::wire::{
    BindingEpoch, DetachedCause, ParticipantDelivery, ParticipantId, ParticipantRecord,
};

use super::log::{
    StoredAttachAllocation, StoredBindingEpoch, StoredOperation, StoredRecordAdmission,
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
        StoredOperation::Attached {
            request,
            allocation,
            ..
        } if allocation.superseded_terminal_seq.is_some() => authority
            .slots
            .get(&request.participant_id)
            .and_then(|slot| match slot.binding {
                BindingState::Bound(binding) => Some(binding.binding_epoch),
                BindingState::Detached | BindingState::PendingFinalization(_) => None,
            }),
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
        StoredOperation::Genesis { .. } => None,
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
            ..
        } => Some(project_attached(
            authority,
            source_log_sequence,
            request.participant_id,
            allocation,
            facts.superseded_binding_epoch,
        )?),
        StoredOperation::Detached {
            request,
            receiving_epoch,
            terminal_seq,
            ..
        } => Some(produced(
            authority,
            source_log_sequence,
            ProducedSourceKind::Detached,
            Some(request.participant_id),
            vec![(
                *terminal_seq,
                ParticipantRecord::Detached {
                    affected_participant_id: request.participant_id,
                    binding_epoch: receiving_epoch.to_epoch()?,
                    cause: DetachedCause::CleanDeregister,
                },
            )],
        )?),
        StoredOperation::ZeroDebtAck { request, .. } => Some(OutboxRow::AckAdvanced {
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
        StoredOperation::Left { row } => Some(produced(
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
        )?),
    };
    Ok(projected)
}

fn project_attached(
    authority: &ConversationAuthority,
    source_log_sequence: u64,
    participant_id: ParticipantId,
    allocation: &StoredAttachAllocation,
    superseded_binding_epoch: Option<BindingEpoch>,
) -> Result<OutboxRow, StateError> {
    let attached = (
        allocation.attached_seq,
        ParticipantRecord::Attached {
            affected_participant_id: participant_id,
            binding_epoch: allocation.binding_epoch.to_epoch()?,
        },
    );
    let records = if let Some(terminal_seq) = allocation.superseded_terminal_seq {
        let prior_epoch = superseded_binding_epoch.ok_or_else(|| {
            StateError::invariant("superseding attach projection lost its prior binding epoch")
        })?;
        vec![
            (
                terminal_seq,
                ParticipantRecord::Detached {
                    affected_participant_id: participant_id,
                    binding_epoch: prior_epoch,
                    cause: DetachedCause::Superseded,
                },
            ),
            attached,
        ]
    } else {
        vec![attached]
    };
    produced(
        authority,
        source_log_sequence,
        ProducedSourceKind::Attached,
        Some(participant_id),
        records,
    )
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
