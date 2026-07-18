//! Semantic validation for restored Unit 2 source batches.

use std::collections::BTreeSet;

use liminal_protocol::wire::{DetachedCause, ParticipantId, ParticipantRecord};

use super::{ConversationOutboxError, ProducedBatch, ProducedSourceKind, ProjectedRecord};

pub(super) fn validate_batch_shape(batch: &ProducedBatch) -> Result<(), ConversationOutboxError> {
    let records = batch.ordered_records();
    let count_valid = if batch.source_kind() == ProducedSourceKind::Attached {
        matches!(records.len(), 1 | 2)
    } else {
        records.len() == 1
    };
    if !count_valid {
        return Err(ConversationOutboxError::RecordCount {
            source_sequence: batch.source_log_sequence(),
            count: records.len(),
        });
    }
    if body_and_sender_match(batch.source_kind(), records) {
        Ok(())
    } else {
        Err(ConversationOutboxError::SourceBody {
            source_sequence: batch.source_log_sequence(),
            source_kind: batch.source_kind(),
        })
    }
}

fn body_and_sender_match(kind: ProducedSourceKind, records: &[ProjectedRecord]) -> bool {
    match (kind, records) {
        (ProducedSourceKind::Enrolled, [record]) | (ProducedSourceKind::Attached, [record]) => {
            match record.body() {
                ParticipantRecord::Attached {
                    affected_participant_id,
                    ..
                } => record.sender() == Some(*affected_participant_id),
                _ => false,
            }
        }
        (ProducedSourceKind::Attached, [terminal, attached]) => {
            match (terminal.body(), attached.body()) {
                (
                    ParticipantRecord::Detached {
                        affected_participant_id: terminal_id,
                        cause: DetachedCause::Superseded,
                        ..
                    },
                    ParticipantRecord::Attached {
                        affected_participant_id: attached_id,
                        ..
                    },
                ) => {
                    terminal_id == attached_id
                        && terminal.sender() == Some(*terminal_id)
                        && attached.sender() == Some(*attached_id)
                }
                _ => false,
            }
        }
        (ProducedSourceKind::Detached, [record]) => match record.body() {
            ParticipantRecord::Detached {
                affected_participant_id,
                cause: DetachedCause::CleanDeregister,
                ..
            } => record.sender() == Some(*affected_participant_id),
            _ => false,
        },
        (ProducedSourceKind::MarkerDrained, [record]) => {
            matches!(record.body(), ParticipantRecord::HistoryCompacted { .. })
                && record.sender().is_none()
        }
        (ProducedSourceKind::RecordAdmission, [record]) => match record.body() {
            ParticipantRecord::OrdinaryRecord {
                sender_participant_id,
                ..
            } => record.sender() == Some(*sender_participant_id),
            _ => false,
        },
        (ProducedSourceKind::Left, [record]) => match record.body() {
            ParticipantRecord::Left {
                affected_participant_id,
                ..
            } => record.sender() == Some(*affected_participant_id),
            _ => false,
        },
        _ => false,
    }
}

pub(super) fn validate_record_snapshot(
    record: &ProjectedRecord,
    retired: &BTreeSet<ParticipantId>,
) -> Result<(), ConversationOutboxError> {
    if record
        .recipients()
        .windows(2)
        .any(|pair| pair[0] >= pair[1])
    {
        return Err(ConversationOutboxError::RecipientOrder {
            delivery_seq: record.delivery_seq(),
        });
    }
    if let Some(sender) = record.sender() {
        if record.recipients().binary_search(&sender).is_ok() {
            return Err(ConversationOutboxError::SenderIncluded {
                delivery_seq: record.delivery_seq(),
                sender,
            });
        }
    }
    if let Some(participant_id) = record
        .recipients()
        .iter()
        .find(|participant| retired.contains(participant))
    {
        return Err(ConversationOutboxError::RetiredRecipient {
            delivery_seq: record.delivery_seq(),
            participant_id: *participant_id,
        });
    }
    Ok(())
}
