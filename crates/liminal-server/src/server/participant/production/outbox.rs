//! Move-only in-memory owner rebuilt from the Unit 2 extension stream.

mod limits;
mod validation;

use std::collections::{BTreeMap, BTreeSet};

use liminal_protocol::lifecycle::{RecipientAckObligations, RecipientAckObligationsError};
use liminal_protocol::wire::{ParticipantDelivery, ParticipantId, ParticipantRecord};

use super::outbox_log::{
    OutboxLogError, OutboxRow, ProducedBatch, ProducedSourceKind, ProjectedRecord, encode_row,
};
pub(super) use limits::ConversationOutboxLimits;
use limits::ensure_live_obligation_capacity;
use validation::{retiring_participant, validate_batch_shape, validate_record_snapshot};

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
    /// The protocol rejected testimony projected from the validated obligation index.
    #[error("Unit 2 obligations for participant {participant_id} are malformed: {error:?}")]
    AckObligations {
        participant_id: ParticipantId,
        error: RecipientAckObligationsError,
    },
    /// The outbox's normal-ack frontier is ahead of the protocol-owned cursor.
    #[error(
        "Unit 2 ack frontier {outbox_ack_through} for participant {participant_id} is ahead of protocol cursor {acknowledged_through}"
    )]
    AckFrontierAhead {
        participant_id: ParticipantId,
        outbox_ack_through: u64,
        acknowledged_through: u64,
    },
    /// Checked live charge arithmetic overflowed.
    #[error("Unit 2 live outbox charge overflowed")]
    ChargeOverflow,
    /// A signed outbox bound could not be formed from its signed inputs.
    #[error("signed outbox bound {name} overflowed")]
    BoundOverflow {
        /// Signed §9 placeholder whose checked derivation failed.
        name: &'static str,
    },
    /// A new live recipient obligation would exceed the signed product bound.
    #[error(
        "Unit 2 live recipient obligations would reach {attempted}, exceeding signed bound {limit}"
    )]
    LiveRecipientObligationsExceeded {
        /// Derived signed maximum.
        limit: u64,
        /// Prospective live count rejected before owner mutation.
        attempted: u64,
    },
}

#[derive(Debug)]
struct LiveRecord {
    delivery: ParticipantDelivery,
    recipients: BTreeSet<ParticipantId>,
    encoded_push_bytes: u64,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct RetainedAuthorityMeasurements {
    pub(super) source_batch_count: usize,
    pub(super) source_batch_owned_bytes: usize,
    pub(super) ack_source_count: usize,
    pub(super) ack_source_owned_bytes: usize,
    pub(super) obligation_participant_count: usize,
    pub(super) obligation_sequence_count: usize,
    pub(super) all_obligations_owned_bytes: usize,
}

/// Sole move-only owner of one conversation's durable delivery obligations.
#[derive(Debug)]
pub(super) struct ConversationOutbox {
    limits: ConversationOutboxLimits,
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
    live_recipient_obligations: u64,
    charged_bytes: u64,
}

impl ConversationOutbox {
    /// Restores one owner from a physically ordered extension stream.
    pub(super) fn restore(
        conversation_id: u64,
        rows: Vec<(u64, OutboxRow)>,
        limits: ConversationOutboxLimits,
    ) -> Result<Self, ConversationOutboxError> {
        let mut owner = Self {
            limits,
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
            live_recipient_obligations: 0,
            charged_bytes: 0,
        };
        for (physical_sequence, row) in rows {
            owner.apply_row(physical_sequence, row)?;
        }
        Ok(owner)
    }

    pub(super) fn apply_row(
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
            OutboxRow::Produced(batch) => self.apply_produced(&batch)?,
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

    fn apply_produced(&mut self, batch: &ProducedBatch) -> Result<(), ConversationOutboxError> {
        validate_batch_shape(batch)?;
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

        let retiring = retiring_participant(batch)?;
        ensure_live_obligation_capacity(self, &prepared, retiring)?;
        self.source_batches.insert(source_sequence, canonical);
        if let Some(participant_id) = retiring {
            self.discharge_retired(participant_id)?;
        }
        for record in prepared {
            self.install_record(record)?;
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
        let added = u64::try_from(record.recipients().len())
            .map_err(|_| ConversationOutboxError::ChargeOverflow)?;
        self.live_recipient_obligations = self
            .live_recipient_obligations
            .checked_add(added)
            .ok_or(ConversationOutboxError::ChargeOverflow)?;
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
        let mut discharged = 0_u64;
        for record in self.records.values_mut() {
            if record.delivery.delivery_seq <= through_seq
                && record.recipients.remove(&participant_id)
            {
                discharged = discharged
                    .checked_add(1)
                    .ok_or(ConversationOutboxError::ChargeOverflow)?;
            }
        }
        self.subtract_live_obligations(discharged)?;
        self.reclaim_empty_records()?;
        self.recompute_next_live();
        Ok(())
    }

    fn discharge_retired(
        &mut self,
        participant_id: ParticipantId,
    ) -> Result<(), ConversationOutboxError> {
        self.retired.insert(participant_id);
        let mut discharged = 0_u64;
        for record in self.records.values_mut() {
            if record.recipients.remove(&participant_id) {
                discharged = discharged
                    .checked_add(1)
                    .ok_or(ConversationOutboxError::ChargeOverflow)?;
            }
        }
        self.subtract_live_obligations(discharged)?;
        self.reclaim_empty_records()?;
        self.recompute_next_live();
        Ok(())
    }

    fn subtract_live_obligations(
        &mut self,
        discharged: u64,
    ) -> Result<(), ConversationOutboxError> {
        self.live_recipient_obligations = self
            .live_recipient_obligations
            .checked_sub(discharged)
            .ok_or(ConversationOutboxError::ChargeOverflow)?;
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

    /// Sorted participants that still own at least one durable obligation.
    pub(super) fn live_recipients(&self) -> impl Iterator<Item = ParticipantId> + '_ {
        self.next_live_obligations.keys().copied()
    }

    /// Selects the least live durable obligation strictly after `offered_through`
    /// for one participant. Acked obligations have already been removed from each
    /// live record, so this simultaneously enforces the durable ack frontier.
    pub(super) fn delivery_after(
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
    pub(super) fn recipient_ack_obligations(
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

    /// Durable cumulative ack cursor used when a new binding discards an old
    /// connection's volatile offered progress.
    pub(super) fn durable_ack_through(&self, participant_id: ParticipantId) -> u64 {
        self.ack_frontiers
            .get(&participant_id)
            .copied()
            .unwrap_or(0)
    }

    /// Whether this exact participant still owns the named marker obligation.
    pub(super) fn is_marker_obligation(
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
    pub(super) const fn live_recipient_obligation_count(&self) -> u64 {
        self.live_recipient_obligations
    }

    #[cfg(test)]
    pub(super) fn source_batch_count(&self) -> usize {
        self.source_batches.len()
    }

    #[cfg(test)]
    pub(super) fn retained_authority_measurements(
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
