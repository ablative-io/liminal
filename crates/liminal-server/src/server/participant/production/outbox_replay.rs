//! Ordered merge of literal v2 base rows and schema-v1 Unit 2 extension rows.

use std::collections::VecDeque;

use super::outbox::{ConversationOutbox, ConversationOutboxError};
use super::outbox_log::{OutboxLog, OutboxRow};
use super::state::{ConversationAuthority, StateError};

/// Move-only merge/reconciliation transaction for one cold first touch.
pub(super) struct ExtensionMerge<'a> {
    log: &'a OutboxLog,
    pending: VecDeque<(u64, OutboxRow)>,
    folded: Vec<(u64, OutboxRow)>,
    next_extension_sequence: u64,
}

impl<'a> ExtensionMerge<'a> {
    pub(super) fn new(log: &'a OutboxLog, rows: Vec<(u64, OutboxRow)>) -> Result<Self, StateError> {
        let next_extension_sequence = u64::try_from(rows.len())
            .map_err(|_| StateError::invariant("Unit 2 extension stream length exceeds u64"))?;
        Ok(Self {
            log,
            pending: rows.into(),
            folded: Vec::new(),
            next_extension_sequence,
        })
    }

    /// Applies every physical extension row tied at `base_log_head`, then
    /// repairs the current base source only when the durable extension is an
    /// exact prefix with no later row already present.
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
            self.folded.push((physical_sequence, row));
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
            self.folded
                .push((self.next_extension_sequence, expected.clone()));
            self.next_extension_sequence = self
                .next_extension_sequence
                .checked_add(1)
                .ok_or_else(|| StateError::invariant("Unit 2 extension head overflowed"))?;
        }
        Ok(())
    }

    /// Refuses impossible future boundaries and installs the fully validated
    /// move-only owner only after the complete merge has succeeded.
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
        authority.outbox = Some(ConversationOutbox::restore(
            authority.conversation_id,
            self.folded,
        )?);
        Ok(())
    }
}
