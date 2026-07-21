//! Read-only delivery, acknowledgement-testimony, and measurement views.

use std::collections::BTreeSet;

use liminal_protocol::lifecycle::RecipientAckObligations;
use liminal_protocol::wire::{ParticipantDelivery, ParticipantId, ParticipantRecord};

#[cfg(test)]
use super::RetainedAuthorityMeasurements;
use super::{ConversationOutbox, ConversationOutboxError};

impl ConversationOutbox {
    /// Sorted participants that still own at least one durable obligation.
    pub(in crate::server::participant::production) fn live_recipients(
        &self,
    ) -> impl Iterator<Item = ParticipantId> + '_ {
        self.next_live_obligations.keys().copied()
    }

    /// Selects the least live durable obligation strictly after `offered_through`
    /// for one participant. Acked obligations have already been removed from each
    /// live record, so this simultaneously enforces the durable ack frontier.
    pub(in crate::server::participant::production) fn delivery_after(
        &self,
        participant_id: ParticipantId,
        offered_through: u64,
    ) -> Option<ParticipantDelivery> {
        self.records
            .range((
                std::ops::Bound::Excluded(offered_through),
                std::ops::Bound::Unbounded,
            ))
            .find_map(|(_, record)| {
                record
                    .recipients
                    .contains(&participant_id)
                    .then(|| record.delivery.clone())
            })
    }

    /// Seals one participant's current durable-obligation index for protocol
    /// ack selection and returns the corresponding reconciled scalar audit.
    pub(in crate::server::participant::production) fn recipient_ack_obligations(
        &self,
        participant_id: ParticipantId,
        acknowledged_through: u64,
    ) -> Result<(RecipientAckObligations, u64), ConversationOutboxError> {
        let outbox_ack_through = self.durable_ack_through(participant_id);
        if outbox_ack_through > acknowledged_through {
            return Err(ConversationOutboxError::AckFrontierAhead {
                participant_id,
                outbox_ack_through,
                acknowledged_through,
            });
        }
        let delivery_sequences: Vec<_> = self
            .all_obligations
            .get(&participant_id)
            .into_iter()
            .flat_map(BTreeSet::iter)
            .copied()
            .filter(|delivery_seq| *delivery_seq > acknowledged_through)
            .collect();
        let contiguously_available_through = delivery_sequences
            .last()
            .copied()
            .unwrap_or(acknowledged_through);
        let obligations = RecipientAckObligations::try_new(
            participant_id,
            acknowledged_through,
            delivery_sequences,
        )
        .map_err(|error| ConversationOutboxError::AckObligations {
            participant_id,
            error,
        })?;
        Ok((obligations, contiguously_available_through))
    }

    /// Reconciles durable outbox, validated marker, protocol, and current-offer
    /// cursors for one read-only delivery selection.
    pub(in crate::server::participant::production) fn dispatch_after(
        &self,
        participant_id: ParticipantId,
        protocol_cursor: u64,
        offered_cursor: Option<u64>,
    ) -> Result<u64, ConversationOutboxError> {
        let outbox_ack_through = self.durable_ack_through(participant_id);
        if outbox_ack_through > protocol_cursor {
            return Err(ConversationOutboxError::AckFrontierAhead {
                participant_id,
                outbox_ack_through,
                acknowledged_through: protocol_cursor,
            });
        }
        let marker_cursor = self
            .marker_ack_frontiers
            .get(&participant_id)
            .copied()
            .unwrap_or(outbox_ack_through);
        let marker_reconciled_cursor = outbox_ack_through.max(marker_cursor);
        if marker_reconciled_cursor != protocol_cursor {
            return Err(ConversationOutboxError::ProtocolCursorProvenance {
                participant_id,
                outbox_ack_through,
                marker_reconciled_cursor,
                protocol_cursor,
            });
        }
        Ok(marker_reconciled_cursor.max(offered_cursor.unwrap_or(0)))
    }

    /// Durable cumulative ack cursor used when a new binding discards an old
    /// connection's volatile offered progress.
    pub(in crate::server::participant::production) fn durable_ack_through(
        &self,
        participant_id: ParticipantId,
    ) -> u64 {
        self.ack_frontiers
            .get(&participant_id)
            .copied()
            .unwrap_or(0)
    }

    /// Whether this exact participant still owns the named marker obligation.
    pub(in crate::server::participant::production) fn is_marker_obligation(
        &self,
        participant_id: ParticipantId,
        delivery_seq: u64,
    ) -> bool {
        self.records.get(&delivery_seq).is_some_and(|record| {
            record.recipients.contains(&participant_id)
                && matches!(
                    record.delivery.record,
                    ParticipantRecord::HistoryCompacted { .. }
                )
        })
    }

    pub(in crate::server::participant::production) const fn next_extension_sequence(&self) -> u64 {
        self.next_extension_sequence
    }

    #[cfg(test)]
    pub(in crate::server::participant::production) const fn charged_bytes(&self) -> u64 {
        self.charged_bytes
    }

    #[cfg(test)]
    pub(in crate::server::participant::production) fn ack_through(
        &self,
        participant_id: ParticipantId,
    ) -> u64 {
        self.ack_frontiers
            .get(&participant_id)
            .copied()
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub(in crate::server::participant::production) fn next_live(
        &self,
        participant_id: ParticipantId,
    ) -> Option<u64> {
        self.next_live_obligations.get(&participant_id).copied()
    }

    #[cfg(test)]
    pub(in crate::server::participant::production) fn live_record_count(&self) -> usize {
        self.records.len()
    }

    #[cfg(test)]
    pub(in crate::server::participant::production) const fn live_recipient_obligation_count(
        &self,
    ) -> u64 {
        self.live_recipient_obligations
    }

    #[cfg(test)]
    pub(in crate::server::participant::production) fn source_batch_count(&self) -> usize {
        self.source_batches.len()
    }

    #[cfg(test)]
    pub(in crate::server::participant::production) fn retained_authority_measurements(
        &self,
    ) -> Result<RetainedAuthorityMeasurements, ConversationOutboxError> {
        let source_batch_owned_bytes =
            self.source_batches
                .iter()
                .try_fold(0_usize, |bytes, (_source_sequence, canonical)| {
                    bytes
                        .checked_add(std::mem::size_of::<u64>())
                        .and_then(|bytes| bytes.checked_add(canonical.len()))
                });
        let ack_source_owned_bytes = self.ack_sources.len().checked_mul(
            std::mem::size_of::<u64>()
                .checked_add(std::mem::size_of::<ParticipantId>())
                .and_then(|bytes| bytes.checked_add(std::mem::size_of::<u64>()))
                .ok_or(ConversationOutboxError::ChargeOverflow)?,
        );
        let obligation_sequence_count = self
            .all_obligations
            .values()
            .try_fold(0_usize, |count, sequences| {
                count.checked_add(sequences.len())
            });
        let all_obligations_owned_bytes = self
            .all_obligations
            .len()
            .checked_mul(std::mem::size_of::<ParticipantId>())
            .and_then(|participant_bytes| {
                obligation_sequence_count.and_then(|count| {
                    count
                        .checked_mul(std::mem::size_of::<u64>())
                        .and_then(|sequence_bytes| participant_bytes.checked_add(sequence_bytes))
                })
            });
        Ok(RetainedAuthorityMeasurements {
            source_batch_count: self.source_batches.len(),
            source_batch_owned_bytes: source_batch_owned_bytes
                .ok_or(ConversationOutboxError::ChargeOverflow)?,
            ack_source_count: self.ack_sources.len(),
            ack_source_owned_bytes: ack_source_owned_bytes
                .ok_or(ConversationOutboxError::ChargeOverflow)?,
            obligation_participant_count: self.all_obligations.len(),
            obligation_sequence_count: obligation_sequence_count
                .ok_or(ConversationOutboxError::ChargeOverflow)?,
            all_obligations_owned_bytes: all_obligations_owned_bytes
                .ok_or(ConversationOutboxError::ChargeOverflow)?,
        })
    }
}
