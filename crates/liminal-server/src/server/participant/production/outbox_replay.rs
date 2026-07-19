//! Ordered merge of literal v2 base rows and schema-v1 Unit 2 extension rows.

use liminal_protocol::lifecycle::RecipientAckObligations;
#[cfg(test)]
use std::collections::VecDeque;

use super::outbox::{ConversationOutbox, ConversationOutboxError, ConversationOutboxLimits};
use super::outbox_log::{OutboxLog, OutboxLogError, OutboxRestoreCursor, OutboxRow};
use super::state::{ConversationAuthority, StateError};

/// Restore failure with cursor faults kept separate from semantic replay.
#[derive(Debug, thiserror::Error)]
pub(super) enum RestoreError {
    /// Extension cursor durability, stream, schema, or codec failure.
    #[error(transparent)]
    Extension(#[from] OutboxLogError),
    /// Base/extension merge, authority replay, repair, or finish failure.
    #[error(transparent)]
    Semantic(#[from] StateError),
}

/// Move-only merge/reconciliation transaction for one cold first touch.
pub(super) struct ExtensionMerge<'a> {
    log: &'a OutboxLog,
    cursor: OutboxRestoreCursor<'a>,
    outbox: ConversationOutbox,
    repair_head: Option<u64>,
}

impl<'a> ExtensionMerge<'a> {
    pub(super) fn new(
        log: &'a OutboxLog,
        conversation_id: u64,
        limits: ConversationOutboxLimits,
    ) -> Result<Self, StateError> {
        Ok(Self {
            log,
            cursor: log.restore_cursor(),
            outbox: ConversationOutbox::restore(conversation_id, Vec::new(), limits)?,
            repair_head: None,
        })
    }

    pub(super) fn recipient_ack_obligations(
        &self,
        participant_id: u64,
        acknowledged_through: u64,
    ) -> Result<(RecipientAckObligations, u64), StateError> {
        Ok(self
            .outbox
            .recipient_ack_obligations(participant_id, acknowledged_through)?)
    }

    /// Applies every physical extension row tied at `base_log_head`, then
    /// repairs the current base source only when the durable extension is an
    /// exact prefix with no later row already present.
    pub(super) async fn apply_boundary(
        &mut self,
        authority: &mut ConversationAuthority,
        base_log_head: u64,
        expected_projection: Option<&OutboxRow>,
    ) -> Result<(), RestoreError> {
        let mut projection_seen = false;
        loop {
            let Some((physical_sequence, row)) = self.cursor.front().await? else {
                break;
            };
            let boundary = row.base_log_head().ok_or_else(|| {
                RestoreError::Semantic(StateError::invariant(
                    "Unit 2 extension base boundary overflowed",
                ))
            })?;
            if boundary < base_log_head {
                return Err(RestoreError::Semantic(StateError::invariant(format!(
                    "Unit 2 extension boundary {boundary} is nonmonotone below {base_log_head}"
                ))));
            }
            if boundary > base_log_head {
                break;
            }
            let physical_sequence = *physical_sequence;
            let (_, row) = self.cursor.pop_front().ok_or_else(|| {
                RestoreError::Semantic(StateError::invariant(
                    "Unit 2 extension merge lost its physical head",
                ))
            })?;
            match &row {
                OutboxRow::Produced(_) | OutboxRow::AckAdvanced { .. } => {
                    if expected_projection != Some(&row) {
                        return Err(RestoreError::Semantic(StateError::invariant(format!(
                            "Unit 2 extension projection at physical sequence {physical_sequence} conflicts with v2 source"
                        ))));
                    }
                    projection_seen = true;
                }
                OutboxRow::MarkerAckCommitted(marker) => {
                    if marker.extension_sequence != physical_sequence {
                        return Err(RestoreError::Semantic(StateError::invariant(format!(
                            "MarkerAck extension sequence {} differs from physical {physical_sequence}",
                            marker.extension_sequence
                        ))));
                    }
                    authority
                        .replay_marker_ack_extension(marker)
                        .map_err(RestoreError::Semantic)?;
                }
            }
            self.outbox
                .apply_row(physical_sequence, row)
                .map_err(StateError::from)
                .map_err(RestoreError::Semantic)?;
        }

        if let Some(expected) = expected_projection {
            if projection_seen {
                return Ok(());
            }
            if self.cursor.confirmed_head().is_none() {
                return Err(RestoreError::Semantic(StateError::invariant(
                    "Unit 2 extension is missing a projection before a later physical row",
                )));
            }
            let next_extension_sequence =
                *self
                    .repair_head
                    .get_or_insert(self.cursor.confirmed_head().ok_or_else(|| {
                        RestoreError::Semantic(StateError::invariant(
                            "Unit 2 extension repair attempted before confirmed EOF",
                        ))
                    })?);
            self.log
                .append(expected, next_extension_sequence)
                .await
                .map_err(ConversationOutboxError::from)
                .map_err(StateError::from)
                .map_err(RestoreError::Semantic)?;
            self.outbox
                .apply_row(next_extension_sequence, expected.clone())
                .map_err(StateError::from)
                .map_err(RestoreError::Semantic)?;
            self.repair_head = Some(next_extension_sequence.checked_add(1).ok_or_else(|| {
                RestoreError::Semantic(StateError::invariant("Unit 2 extension head overflowed"))
            })?);
        }
        Ok(())
    }

    /// Refuses impossible future boundaries and installs the fully validated
    /// move-only owner only after the complete merge has succeeded.
    pub(super) fn finish(
        mut self,
        authority: &mut ConversationAuthority,
        final_base_log_head: u64,
    ) -> Result<(), StateError> {
        if let Some((physical_sequence, row)) = self.cursor.pop_front() {
            let boundary = row.base_log_head().ok_or_else(|| {
                StateError::invariant("Unit 2 extension base boundary overflowed")
            })?;
            return Err(StateError::invariant(format!(
                "Unit 2 extension physical sequence {physical_sequence} has impossible future boundary {boundary} above base head {final_base_log_head}"
            )));
        }
        if self.cursor.confirmed_head().is_none() {
            return Err(StateError::invariant(
                "Unit 2 extension merge finished before confirmed EOF",
            ));
        }
        authority.outbox = Some(self.outbox);
        Ok(())
    }
}

/// Frozen pre-W3 aggregate merge used only for byte/error equivalence oracles.
///
/// Its pending-history ownership and repair placement are deliberately
/// independent of [`ExtensionMerge`]; production cannot select this path.
#[cfg(test)]
pub(super) struct AggregateExtensionMerge<'a> {
    log: &'a OutboxLog,
    pending: VecDeque<(u64, OutboxRow)>,
    outbox: ConversationOutbox,
    next_extension_sequence: u64,
}

#[cfg(test)]
impl<'a> AggregateExtensionMerge<'a> {
    pub(super) fn new(
        log: &'a OutboxLog,
        rows: Vec<(u64, OutboxRow)>,
        conversation_id: u64,
        limits: ConversationOutboxLimits,
    ) -> Result<Self, StateError> {
        let next_extension_sequence = u64::try_from(rows.len())
            .map_err(|_| StateError::invariant("Unit 2 extension stream length exceeds u64"))?;
        Ok(Self {
            log,
            pending: rows.into(),
            outbox: ConversationOutbox::restore(conversation_id, Vec::new(), limits)?,
            next_extension_sequence,
        })
    }

    pub(super) fn recipient_ack_obligations(
        &self,
        participant_id: u64,
        acknowledged_through: u64,
    ) -> Result<(RecipientAckObligations, u64), StateError> {
        Ok(self
            .outbox
            .recipient_ack_obligations(participant_id, acknowledged_through)?)
    }

    pub(super) async fn apply_boundary(
        &mut self,
        authority: &mut ConversationAuthority,
        base_log_head: u64,
        expected_projection: Option<&OutboxRow>,
    ) -> Result<(), StateError> {
        let mut projection_seen = false;
        loop {
            let Some((physical_sequence, row)) = self.pending.front() else {
                break;
            };
            let boundary = row.base_log_head().ok_or_else(|| {
                StateError::invariant("Unit 2 extension base boundary overflowed")
            })?;
            if boundary < base_log_head {
                return Err(StateError::invariant(format!(
                    "Unit 2 extension boundary {boundary} is nonmonotone below {base_log_head}"
                )));
            }
            if boundary > base_log_head {
                break;
            }
            let physical_sequence = *physical_sequence;
            let (_, row) = self.pending.pop_front().ok_or_else(|| {
                StateError::invariant("Unit 2 extension merge lost its physical head")
            })?;
            match &row {
                OutboxRow::Produced(_) | OutboxRow::AckAdvanced { .. } => {
                    if expected_projection != Some(&row) {
                        return Err(StateError::invariant(format!(
                            "Unit 2 extension projection at physical sequence {physical_sequence} conflicts with v2 source"
                        )));
                    }
                    projection_seen = true;
                }
                OutboxRow::MarkerAckCommitted(marker) => {
                    if marker.extension_sequence != physical_sequence {
                        return Err(StateError::invariant(format!(
                            "MarkerAck extension sequence {} differs from physical {physical_sequence}",
                            marker.extension_sequence
                        )));
                    }
                    authority.replay_marker_ack_extension(marker)?;
                }
            }
            self.outbox.apply_row(physical_sequence, row)?;
        }

        if let Some(expected) = expected_projection {
            if projection_seen {
                return Ok(());
            }
            if !self.pending.is_empty() {
                return Err(StateError::invariant(
                    "Unit 2 extension is missing a projection before a later physical row",
                ));
            }
            self.log
                .append(expected, self.next_extension_sequence)
                .await
                .map_err(ConversationOutboxError::from)?;
            self.outbox
                .apply_row(self.next_extension_sequence, expected.clone())?;
            self.next_extension_sequence = self
                .next_extension_sequence
                .checked_add(1)
                .ok_or_else(|| StateError::invariant("Unit 2 extension head overflowed"))?;
        }
        Ok(())
    }

    pub(super) fn finish(
        self,
        authority: &mut ConversationAuthority,
        final_base_log_head: u64,
    ) -> Result<(), StateError> {
        if let Some((physical_sequence, row)) = self.pending.front() {
            let boundary = row.base_log_head().ok_or_else(|| {
                StateError::invariant("Unit 2 extension base boundary overflowed")
            })?;
            return Err(StateError::invariant(format!(
                "Unit 2 extension physical sequence {physical_sequence} has impossible future boundary {boundary} above base head {final_base_log_head}"
            )));
        }
        authority.outbox = Some(self.outbox);
        Ok(())
    }
}
