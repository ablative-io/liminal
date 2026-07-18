//! Move-only in-memory owner rebuilt from the Unit 2 extension stream.

mod validation;

use std::collections::{BTreeMap, BTreeSet};

use liminal_protocol::lifecycle::RecipientAckObligations;
use liminal_protocol::wire::{ParticipantDelivery, ParticipantId, ParticipantRecord};

use super::outbox_log::{
    OutboxLogError, OutboxRow, ProducedBatch, ProducedSourceKind, ProjectedRecord, encode_row,
};
use validation::{validate_batch_shape, validate_record_snapshot};

/// Typed corruption found while folding canonical Unit 2 rows.
#[derive(Debug, thiserror::Error)]
pub(super) enum ConversationOutboxError {
    /// Canonical row encoding or charge validation failed.
    #[error(transparent)]
    Log(#[from] OutboxLogError),
    /// Physical extension sequence did not match the optimistic head.
    #[error("Unit 2 owner expected extension sequence {expected}, found {actual}")]
    ExtensionSequence { expected: u64, actual: u64 },
    /// A source batch did not contain the ruled one-or-two record count.
    #[error("Unit 2 source {source_sequence} has invalid record count {count}")]
    RecordCount { source_sequence: u64, count: usize },
    /// A source kind and typed record body disagree with the exhaustive mapping.
    #[error("Unit 2 source {source_sequence} has a body inconsistent with {source_kind:?}")]
    SourceBody {
        source_sequence: u64,
        source_kind: ProducedSourceKind,
    },
    /// Two physical rows project the same source with different canonical bytes.
    #[error("Unit 2 source {source_sequence} has a conflicting duplicate batch")]
    ConflictingSource { source_sequence: u64 },
    /// A projected delivery sequence is not strictly increasing.
    #[error("Unit 2 delivery sequence {actual} does not advance past {previous}")]
    DeliverySequence { previous: u64, actual: u64 },
    /// Two distinct sources claim the same delivery sequence.
    #[error("Unit 2 delivery sequence {delivery_seq} belongs to more than one source")]
    ConflictingDelivery { delivery_seq: u64 },
    /// A recipient snapshot is not sorted and duplicate free.
    #[error("Unit 2 delivery {delivery_seq} recipient snapshot is not strictly sorted")]
    RecipientOrder { delivery_seq: u64 },
    /// A sender appears in its own recipient snapshot.
    #[error("Unit 2 delivery {delivery_seq} includes excluded sender {sender}")]
    SenderIncluded {
        delivery_seq: u64,
        sender: ParticipantId,
    },
    /// A retired identity appears in a later recipient snapshot.
    #[error("Unit 2 delivery {delivery_seq} names retired recipient {participant_id}")]
    RetiredRecipient {
        delivery_seq: u64,
        participant_id: ParticipantId,
    },
    /// Persisted canonical push charge differs from a fresh canonical measurement.
    #[error("Unit 2 delivery {delivery_seq} charge drifted: stored {stored}, measured {measured}")]
    ChargeDrift {
        delivery_seq: u64,
        stored: u64,
        measured: u64,
    },
    /// A cumulative ack source was duplicated with different facts.
    #[error("Unit 2 ack source {source_sequence} has conflicting facts")]
    ConflictingAckSource { source_sequence: u64 },
    /// A cumulative ack regressed or repeated its participant frontier.
    #[error(
        "Unit 2 ack for participant {participant_id} regressed from {current} to {through_seq}"
    )]
    AckRegression {
        participant_id: ParticipantId,
        current: u64,
        through_seq: u64,
    },
    /// An ack ended on no committed obligation for that recipient.
    #[error("Unit 2 ack for participant {participant_id} ends on non-obligation {through_seq}")]
    AckGap {
        participant_id: ParticipantId,
        through_seq: u64,
    },
    /// Checked live charge arithmetic overflowed.
    #[error("Unit 2 live outbox charge overflowed")]
    ChargeOverflow,
    /// Protocol obligation testimony rejected owner state.
    #[error("Unit 2 recipient obligation testimony drifted: {message}")]
    ObligationTestimony { message: String },
}

#[derive(Debug)]
struct LiveRecord {
    delivery: ParticipantDelivery,
    recipients: BTreeSet<ParticipantId>,
    encoded_push_bytes: u64,
}

/// Sole move-only owner of one conversation's durable delivery obligations.
#[derive(Debug)]
pub(super) struct ConversationOutbox {
    conversation_id: u64,
    records: BTreeMap<u64, LiveRecord>,
    source_batches: BTreeMap<u64, Vec<u8>>,
    ack_sources: BTreeMap<u64, (ParticipantId, u64)>,
    all_obligations: BTreeMap<ParticipantId, BTreeSet<u64>>,
    ack_frontiers: BTreeMap<ParticipantId, u64>,
    next_live_obligations: BTreeMap<ParticipantId, u64>,
    retired: BTreeSet<ParticipantId>,
    highest_delivery_seq: u64,
    next_extension_sequence: u64,
    charged_bytes: u64,
}

impl ConversationOutbox {
    /// Restores one owner from a physically ordered extension stream.
    pub(super) fn restore(
        conversation_id: u64,
        rows: Vec<(u64, OutboxRow)>,
    ) -> Result<Self, ConversationOutboxError> {
        let mut owner = Self {
            conversation_id,
            records: BTreeMap::new(),
            source_batches: BTreeMap::new(),
            ack_sources: BTreeMap::new(),
            all_obligations: BTreeMap::new(),
            ack_frontiers: BTreeMap::new(),
            next_live_obligations: BTreeMap::new(),
            retired: BTreeSet::new(),
            highest_delivery_seq: 0,
            next_extension_sequence: 0,
            charged_bytes: 0,
        };
        for (physical_sequence, row) in rows {
            owner.apply_row(physical_sequence, row)?;
        }
        Ok(owner)
    }

    fn apply_row(
        &mut self,
        physical_sequence: u64,
        row: OutboxRow,
    ) -> Result<(), ConversationOutboxError> {
        if physical_sequence != self.next_extension_sequence {
            return Err(ConversationOutboxError::ExtensionSequence {
                expected: self.next_extension_sequence,
                actual: physical_sequence,
            });
        }
        if let OutboxRow::MarkerAckCommitted(marker) = &row {
            if marker.extension_sequence != physical_sequence {
                return Err(ConversationOutboxError::ExtensionSequence {
                    expected: physical_sequence,
                    actual: marker.extension_sequence,
                });
            }
        }
        match row {
            OutboxRow::Produced(batch) => self.apply_produced(batch)?,
            OutboxRow::AckAdvanced {
                source_log_sequence,
                participant_id,
                through_seq,
            } => self.apply_ack(source_log_sequence, participant_id, through_seq)?,
            OutboxRow::MarkerAckCommitted(_) => {}
        }
        self.next_extension_sequence = self.next_extension_sequence.checked_add(1).ok_or(
            ConversationOutboxError::ExtensionSequence {
                expected: u64::MAX,
                actual: physical_sequence,
            },
        )?;
        Ok(())
    }

    fn apply_produced(&mut self, batch: ProducedBatch) -> Result<(), ConversationOutboxError> {
        validate_batch_shape(&batch)?;
        let source_sequence = batch.source_log_sequence();
        let canonical = encode_row(&OutboxRow::Produced(batch.clone()))?;
        if let Some(existing) = self.source_batches.get(&source_sequence) {
            if existing == &canonical {
                return Ok(());
            }
            return Err(ConversationOutboxError::ConflictingSource { source_sequence });
        }

        let mut prepared = Vec::with_capacity(batch.ordered_records().len());
        let mut previous = self.highest_delivery_seq;
        for record in batch.ordered_records() {
            validate_record_snapshot(record, &self.retired)?;
            if record.delivery_seq() <= previous {
                return Err(ConversationOutboxError::DeliverySequence {
                    previous,
                    actual: record.delivery_seq(),
                });
            }
            if self.records.contains_key(&record.delivery_seq()) {
                return Err(ConversationOutboxError::ConflictingDelivery {
                    delivery_seq: record.delivery_seq(),
                });
            }
            let measured = ProjectedRecord::try_new(
                self.conversation_id,
                record.delivery_seq(),
                record.body().clone(),
                record.recipients().to_vec(),
                record.sender(),
            )?
            .encoded_push_bytes();
            if measured != record.encoded_push_bytes() {
                return Err(ConversationOutboxError::ChargeDrift {
                    delivery_seq: record.delivery_seq(),
                    stored: record.encoded_push_bytes(),
                    measured,
                });
            }
            previous = record.delivery_seq();
            prepared.push(record.clone());
        }

        self.source_batches.insert(source_sequence, canonical);
        for record in prepared {
            self.install_record(record)?;
        }
        if batch.source_kind() == ProducedSourceKind::Left {
            let ParticipantRecord::Left {
                affected_participant_id,
                ..
            } = batch.ordered_records()[0].body()
            else {
                return Err(ConversationOutboxError::SourceBody {
                    source_sequence,
                    source_kind: batch.source_kind(),
                });
            };
            self.discharge_retired(*affected_participant_id)?;
        }
        Ok(())
    }

    fn install_record(&mut self, record: ProjectedRecord) -> Result<(), ConversationOutboxError> {
        let sequence = record.delivery_seq();
        self.highest_delivery_seq = sequence;
        for participant in record.recipients() {
            self.all_obligations
                .entry(*participant)
                .or_default()
                .insert(sequence);
            self.ack_frontiers.entry(*participant).or_insert(0);
        }
        let recipients: BTreeSet<_> = record.recipients().iter().copied().collect();
        if !recipients.is_empty() {
            let encoded_push_bytes = record.encoded_push_bytes();
            self.charged_bytes = self
                .charged_bytes
                .checked_add(encoded_push_bytes)
                .ok_or(ConversationOutboxError::ChargeOverflow)?;
            self.records.insert(
                sequence,
                LiveRecord {
                    delivery: record.into_delivery(self.conversation_id),
                    recipients,
                    encoded_push_bytes,
                },
            );
        }
        self.recompute_next_live();
        Ok(())
    }

    fn apply_ack(
        &mut self,
        source_sequence: u64,
        participant_id: ParticipantId,
        through_seq: u64,
    ) -> Result<(), ConversationOutboxError> {
        if let Some(existing) = self.ack_sources.get(&source_sequence) {
            return if *existing == (participant_id, through_seq) {
                Ok(())
            } else {
                Err(ConversationOutboxError::ConflictingAckSource { source_sequence })
            };
        }
        let current = self
            .ack_frontiers
            .get(&participant_id)
            .copied()
            .unwrap_or(0);
        if through_seq <= current {
            return Err(ConversationOutboxError::AckRegression {
                participant_id,
                current,
                through_seq,
            });
        }
        let endpoint_exists = self
            .all_obligations
            .get(&participant_id)
            .is_some_and(|sequences| sequences.contains(&through_seq));
        if !endpoint_exists {
            return Err(ConversationOutboxError::AckGap {
                participant_id,
                through_seq,
            });
        }
        self.ack_sources
            .insert(source_sequence, (participant_id, through_seq));
        self.ack_frontiers.insert(participant_id, through_seq);
        self.discharge_through(participant_id, through_seq)?;
        Ok(())
    }

    fn discharge_through(
        &mut self,
        participant_id: ParticipantId,
        through_seq: u64,
    ) -> Result<(), ConversationOutboxError> {
        for record in self.records.values_mut() {
            if record.delivery.delivery_seq <= through_seq {
                record.recipients.remove(&participant_id);
            }
        }
        self.reclaim_empty_records()?;
        self.recompute_next_live();
        Ok(())
    }

    fn discharge_retired(
        &mut self,
        participant_id: ParticipantId,
    ) -> Result<(), ConversationOutboxError> {
        self.retired.insert(participant_id);
        for record in self.records.values_mut() {
            record.recipients.remove(&participant_id);
        }
        self.reclaim_empty_records()?;
        self.recompute_next_live();
        Ok(())
    }

    fn reclaim_empty_records(&mut self) -> Result<(), ConversationOutboxError> {
        let reclaimed: Vec<_> = self
            .records
            .iter()
            .filter_map(|(sequence, record)| record.recipients.is_empty().then_some(*sequence))
            .collect();
        for sequence in reclaimed {
            if let Some(record) = self.records.remove(&sequence) {
                self.charged_bytes = self
                    .charged_bytes
                    .checked_sub(record.encoded_push_bytes)
                    .ok_or(ConversationOutboxError::ChargeOverflow)?;
            }
        }
        Ok(())
    }

    fn recompute_next_live(&mut self) {
        self.next_live_obligations.clear();
        for (sequence, record) in &self.records {
            for participant in &record.recipients {
                self.next_live_obligations
                    .entry(*participant)
                    .or_insert(*sequence);
            }
        }
    }

    /// Supplies sealed durable-obligation testimony to the protocol selector.
    pub(super) fn recipient_ack_obligations(
        &self,
        participant_id: ParticipantId,
    ) -> Result<RecipientAckObligations, ConversationOutboxError> {
        let acknowledged_through = self
            .ack_frontiers
            .get(&participant_id)
            .copied()
            .unwrap_or(0);
        let live = self
            .all_obligations
            .get(&participant_id)
            .into_iter()
            .flat_map(|sequences| sequences.iter().copied())
            .filter(|sequence| *sequence > acknowledged_through)
            .filter(|sequence| {
                !self.retired.contains(&participant_id) && self.records.contains_key(sequence)
            })
            .collect();
        RecipientAckObligations::try_new(participant_id, acknowledged_through, live).map_err(
            |error| ConversationOutboxError::ObligationTestimony {
                message: format!("{error:?}"),
            },
        )
    }

    #[cfg(test)]
    pub(super) const fn next_extension_sequence(&self) -> u64 {
        self.next_extension_sequence
    }

    #[cfg(test)]
    pub(super) const fn charged_bytes(&self) -> u64 {
        self.charged_bytes
    }

    #[cfg(test)]
    pub(super) fn ack_through(&self, participant_id: ParticipantId) -> u64 {
        self.ack_frontiers
            .get(&participant_id)
            .copied()
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub(super) fn next_live(&self, participant_id: ParticipantId) -> Option<u64> {
        self.next_live_obligations.get(&participant_id).copied()
    }

    #[cfg(test)]
    pub(super) fn live_record_count(&self) -> usize {
        self.records.len()
    }

    #[cfg(test)]
    pub(super) fn source_batch_count(&self) -> usize {
        self.source_batches.len()
    }
}
