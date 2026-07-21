//! Provenance-bearing observer-progress replay witnesses and repair planning.
//!
//! [`ObserverProgressWitnessState`] enriches the authority's pre-existing
//! replay vector; it does not retain a second copy of source history. The
//! separately owned authoritative maximum remains available to replayed
//! protocol transitions while this module validates provenance and plans the
//! exact running-maximum repair suffix.

#[cfg(test)]
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};

use liminal_protocol::lifecycle::ObserverProgressProjection;
use liminal_protocol::wire::{BindingEpoch, ConversationId, DeliverySeq, ParticipantId};

/// Closed refusal sum for observer-progress source and durable-prefix drift.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub(super) enum ObserverProgressConformanceError {
    /// A projection or occurrence names another conversation.
    #[error("observer progress source conversation mismatch")]
    ConversationMismatch,
    /// A checked merged replay position is not strictly increasing.
    #[error("observer progress source order is invalid")]
    SourceOrder,
    /// Source kind, occurrence, producer, or raw durable coordinates disagree.
    #[error("observer progress source identity mismatch")]
    SourceIdentityMismatch,
    /// One source occurrence was presented more than once.
    #[error("duplicate observer progress occurrence producer")]
    DuplicateOccurrenceProducer,
    /// One source-specific lineage moved backwards.
    #[error("observer progress source lineage regressed")]
    SourceLineageRegression,
    /// Durable observer progress is above the complete validated source maximum.
    #[error("durable observer progress is ahead of validated source maximum")]
    AheadOfValidatedSourceMaximum,
    /// Durable nonzero progress has no exact running-maximum-establishing source.
    #[error("durable observer advance has no running-maximum witness")]
    AdvanceWithoutRunningMaximumWitness,
    /// Planned, durable, and authoritative final progress do not agree exactly.
    #[error("final observer progress does not equal the validated source maximum")]
    FinalProgressMismatch,
}

/// Durable operation kind that surrendered one base-log projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum BaseSourceKind {
    Attached,
    Detached,
    ParticipantAck,
    Left,
    Recovered,
}

/// Durable extension kind that surrendered one projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ExtensionSourceKind {
    MarkerAck,
}

/// Raw durable identity retained beside a projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ObserverProgressSourceIdentity {
    Base {
        sequence: u64,
        kind: BaseSourceKind,
    },
    Extension {
        base_log_head: u64,
        extension_sequence: u64,
        kind: ExtensionSourceKind,
    },
}

/// Exact typed source occurrence represented by a projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ObserverProgressOccurrence {
    AttachedTerminal {
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        terminal_delivery_seq: DeliverySeq,
    },
    DetachedTerminal {
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        terminal_delivery_seq: DeliverySeq,
    },
    ParticipantAck {
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        through_seq: DeliverySeq,
        source_sequence: u64,
    },
    MarkerAck {
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        marker_delivery_seq: DeliverySeq,
        resulting_cursor: DeliverySeq,
        base_log_head: u64,
        extension_sequence: u64,
    },
    Leave {
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        left_delivery_seq: DeliverySeq,
    },
    RecoveredFate {
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
        resulting_floor: DeliverySeq,
    },
}

impl ObserverProgressOccurrence {
    const fn conversation_id(self) -> ConversationId {
        match self {
            Self::AttachedTerminal {
                conversation_id, ..
            }
            | Self::DetachedTerminal {
                conversation_id, ..
            }
            | Self::ParticipantAck {
                conversation_id, ..
            }
            | Self::MarkerAck {
                conversation_id, ..
            }
            | Self::Leave {
                conversation_id, ..
            }
            | Self::RecoveredFate {
                conversation_id, ..
            } => conversation_id,
        }
    }

    const fn participant_id(self) -> ParticipantId {
        match self {
            Self::AttachedTerminal { participant_id, .. }
            | Self::DetachedTerminal { participant_id, .. }
            | Self::ParticipantAck { participant_id, .. }
            | Self::MarkerAck { participant_id, .. }
            | Self::Leave { participant_id, .. }
            | Self::RecoveredFate { participant_id, .. } => participant_id,
        }
    }

    const fn progress(self) -> DeliverySeq {
        match self {
            Self::AttachedTerminal {
                terminal_delivery_seq,
                ..
            }
            | Self::DetachedTerminal {
                terminal_delivery_seq,
                ..
            } => terminal_delivery_seq,
            Self::ParticipantAck { through_seq, .. } => through_seq,
            Self::MarkerAck {
                resulting_cursor, ..
            } => resulting_cursor,
            Self::Leave {
                left_delivery_seq, ..
            } => left_delivery_seq,
            Self::RecoveredFate {
                resulting_floor, ..
            } => resulting_floor,
        }
    }
}

/// Source-specific monotonicity domain.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ObserverProgressLineage {
    ParticipantCursor(ParticipantId),
    ParticipantTerminal(ParticipantId),
    BindingFateFloor(ParticipantId),
}

/// Canonical producer that surrendered a sealed projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ObserverProgressProducer {
    Attach,
    Detach,
    ParticipantAck,
    MarkerAck,
    LiveLeaveCommit,
    RecoveredBindingFate,
    #[cfg(test)]
    InjectedDuplicate,
}

/// Typed metadata constructed while the durable source row and commit are owned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ObserverProgressSourceMetadata {
    source: ObserverProgressSourceIdentity,
    occurrence: ObserverProgressOccurrence,
    lineage: ObserverProgressLineage,
    producer: ObserverProgressProducer,
}

impl ObserverProgressSourceMetadata {
    pub(super) const fn attached(
        source_sequence: u64,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        terminal_delivery_seq: DeliverySeq,
    ) -> Self {
        Self {
            source: ObserverProgressSourceIdentity::Base {
                sequence: source_sequence,
                kind: BaseSourceKind::Attached,
            },
            occurrence: ObserverProgressOccurrence::AttachedTerminal {
                conversation_id,
                participant_id,
                terminal_delivery_seq,
            },
            lineage: ObserverProgressLineage::ParticipantTerminal(participant_id),
            producer: ObserverProgressProducer::Attach,
        }
    }

    pub(super) const fn detached(
        source_sequence: u64,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        terminal_delivery_seq: DeliverySeq,
    ) -> Self {
        Self {
            source: ObserverProgressSourceIdentity::Base {
                sequence: source_sequence,
                kind: BaseSourceKind::Detached,
            },
            occurrence: ObserverProgressOccurrence::DetachedTerminal {
                conversation_id,
                participant_id,
                terminal_delivery_seq,
            },
            lineage: ObserverProgressLineage::ParticipantTerminal(participant_id),
            producer: ObserverProgressProducer::Detach,
        }
    }

    pub(super) const fn participant_ack(
        source_sequence: u64,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        through_seq: DeliverySeq,
    ) -> Self {
        Self {
            source: ObserverProgressSourceIdentity::Base {
                sequence: source_sequence,
                kind: BaseSourceKind::ParticipantAck,
            },
            occurrence: ObserverProgressOccurrence::ParticipantAck {
                conversation_id,
                participant_id,
                through_seq,
                source_sequence,
            },
            lineage: ObserverProgressLineage::ParticipantCursor(participant_id),
            producer: ObserverProgressProducer::ParticipantAck,
        }
    }

    pub(super) const fn marker_ack(
        base_log_head: u64,
        extension_sequence: u64,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        marker_delivery_seq: DeliverySeq,
        resulting_cursor: DeliverySeq,
    ) -> Self {
        Self {
            source: ObserverProgressSourceIdentity::Extension {
                base_log_head,
                extension_sequence,
                kind: ExtensionSourceKind::MarkerAck,
            },
            occurrence: ObserverProgressOccurrence::MarkerAck {
                conversation_id,
                participant_id,
                marker_delivery_seq,
                resulting_cursor,
                base_log_head,
                extension_sequence,
            },
            lineage: ObserverProgressLineage::ParticipantCursor(participant_id),
            producer: ObserverProgressProducer::MarkerAck,
        }
    }

    pub(super) const fn leave(
        source_sequence: u64,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        left_delivery_seq: DeliverySeq,
    ) -> Self {
        Self {
            source: ObserverProgressSourceIdentity::Base {
                sequence: source_sequence,
                kind: BaseSourceKind::Left,
            },
            occurrence: ObserverProgressOccurrence::Leave {
                conversation_id,
                participant_id,
                left_delivery_seq,
            },
            lineage: ObserverProgressLineage::ParticipantTerminal(participant_id),
            producer: ObserverProgressProducer::LiveLeaveCommit,
        }
    }

    pub(super) const fn recovered_binding_fate(
        source_sequence: u64,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
        resulting_floor: DeliverySeq,
    ) -> Self {
        Self {
            source: ObserverProgressSourceIdentity::Base {
                sequence: source_sequence,
                kind: BaseSourceKind::Recovered,
            },
            occurrence: ObserverProgressOccurrence::RecoveredFate {
                conversation_id,
                participant_id,
                binding_epoch,
                resulting_floor,
            },
            lineage: ObserverProgressLineage::BindingFateFloor(participant_id),
            producer: ObserverProgressProducer::RecoveredBindingFate,
        }
    }

    #[cfg(test)]
    const fn injected_duplicate(mut self) -> Self {
        self.producer = ObserverProgressProducer::InjectedDuplicate;
        self
    }

    #[cfg(test)]
    pub(super) const fn producer(self) -> ObserverProgressProducer {
        self.producer
    }
}

/// One move-only element of the authority's pre-existing replay vector.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ObserverProgressSourceWitness {
    projection: ObserverProgressProjection,
    merged_ordinal: u64,
    metadata: ObserverProgressSourceMetadata,
}

impl ObserverProgressSourceWitness {
    pub(super) const fn progress(&self) -> DeliverySeq {
        self.projection.new_observer_progress()
    }

    pub(super) const fn merged_ordinal(&self) -> u64 {
        self.merged_ordinal
    }
}

/// Validation bookkeeping surrounding the enriched, pre-existing replay vector.
#[derive(Debug, Default)]
pub(super) struct ObserverProgressWitnessState {
    witnesses: Vec<ObserverProgressSourceWitness>,
    next_merged_ordinal: u64,
    active_merged_ordinal: Option<u64>,
    occurrences: BTreeSet<ObserverProgressOccurrence>,
    lineage_progress: BTreeMap<ObserverProgressLineage, DeliverySeq>,
}

impl ObserverProgressWitnessState {
    pub(super) const fn new() -> Self {
        Self {
            witnesses: Vec::new(),
            next_merged_ordinal: 0,
            active_merged_ordinal: None,
            occurrences: BTreeSet::new(),
            lineage_progress: BTreeMap::new(),
        }
    }

    pub(super) fn record(
        &mut self,
        conversation_id: ConversationId,
        projection: ObserverProgressProjection,
        metadata: ObserverProgressSourceMetadata,
    ) -> Result<(), ObserverProgressConformanceError> {
        let owns_visit = self.active_merged_ordinal.is_none();
        if owns_visit {
            self.begin_source()?;
        }
        let ordinal = self
            .active_merged_ordinal
            .ok_or(ObserverProgressConformanceError::SourceOrder)?;
        let result = self.record_at(conversation_id, projection, metadata, ordinal);
        if owns_visit {
            self.active_merged_ordinal = None;
        }
        result
    }

    /// Begins one base or extension source visit in actual merged replay order.
    pub(super) fn begin_source(&mut self) -> Result<(), ObserverProgressConformanceError> {
        if self.active_merged_ordinal.is_some() {
            return Err(ObserverProgressConformanceError::SourceOrder);
        }
        let ordinal = self.next_merged_ordinal;
        self.next_merged_ordinal = ordinal
            .checked_add(1)
            .ok_or(ObserverProgressConformanceError::SourceOrder)?;
        self.active_merged_ordinal = Some(ordinal);
        Ok(())
    }

    /// Completes the current source visit after its typed transition succeeds.
    pub(super) const fn end_source(&mut self) -> Result<(), ObserverProgressConformanceError> {
        if self.active_merged_ordinal.take().is_none() {
            return Err(ObserverProgressConformanceError::SourceOrder);
        }
        Ok(())
    }

    fn record_at(
        &mut self,
        conversation_id: ConversationId,
        projection: ObserverProgressProjection,
        metadata: ObserverProgressSourceMetadata,
        merged_ordinal: u64,
    ) -> Result<(), ObserverProgressConformanceError> {
        if projection.conversation_id() != conversation_id
            || metadata.occurrence.conversation_id() != conversation_id
        {
            return Err(ObserverProgressConformanceError::ConversationMismatch);
        }
        if self
            .witnesses
            .last()
            .is_some_and(|previous| previous.merged_ordinal >= merged_ordinal)
        {
            return Err(ObserverProgressConformanceError::SourceOrder);
        }
        if self.occurrences.contains(&metadata.occurrence) {
            return Err(ObserverProgressConformanceError::DuplicateOccurrenceProducer);
        }
        validate_metadata(projection.new_observer_progress(), metadata)?;
        if self
            .lineage_progress
            .get(&metadata.lineage)
            .is_some_and(|previous| projection.new_observer_progress() < *previous)
        {
            return Err(ObserverProgressConformanceError::SourceLineageRegression);
        }
        self.occurrences.insert(metadata.occurrence);
        self.lineage_progress
            .insert(metadata.lineage, projection.new_observer_progress());
        self.witnesses.push(ObserverProgressSourceWitness {
            projection,
            merged_ordinal,
            metadata,
        });
        Ok(())
    }

    pub(super) fn take(&mut self) -> Vec<ObserverProgressSourceWitness> {
        std::mem::take(&mut self.witnesses)
    }

    #[cfg(test)]
    pub(super) fn inject_duplicate_producer(
        &self,
        metadata: ObserverProgressSourceMetadata,
    ) -> Result<(), ObserverProgressConformanceError> {
        let injected = metadata.injected_duplicate();
        if self.occurrences.contains(&injected.occurrence) {
            return Err(ObserverProgressConformanceError::DuplicateOccurrenceProducer);
        }
        validate_metadata(injected.occurrence.progress(), injected)
    }
}

#[cfg(test)]
std::thread_local! {
    static DUPLICATE_LEAVE_INJECTION: Cell<bool> = const { Cell::new(false) };
}

#[cfg(test)]
pub(super) fn with_duplicate_leave_injection<T>(operation: impl FnOnce() -> T) -> T {
    DUPLICATE_LEAVE_INJECTION.with(|armed| {
        let previous = armed.replace(true);
        let result = operation();
        armed.set(previous);
        result
    })
}

#[cfg(test)]
pub(super) fn duplicate_leave_injection_armed() -> bool {
    DUPLICATE_LEAVE_INJECTION.with(Cell::get)
}

const fn validate_metadata(
    progress: DeliverySeq,
    metadata: ObserverProgressSourceMetadata,
) -> Result<(), ObserverProgressConformanceError> {
    let participant_id = metadata.occurrence.participant_id();
    let valid = progress == metadata.occurrence.progress()
        && match (
            metadata.source,
            metadata.occurrence,
            metadata.lineage,
            metadata.producer,
        ) {
            (
                ObserverProgressSourceIdentity::Base {
                    kind: BaseSourceKind::Attached,
                    ..
                },
                ObserverProgressOccurrence::AttachedTerminal { .. },
                ObserverProgressLineage::ParticipantTerminal(lineage_participant),
                ObserverProgressProducer::Attach,
            )
            | (
                ObserverProgressSourceIdentity::Base {
                    kind: BaseSourceKind::Detached,
                    ..
                },
                ObserverProgressOccurrence::DetachedTerminal { .. },
                ObserverProgressLineage::ParticipantTerminal(lineage_participant),
                ObserverProgressProducer::Detach,
            )
            | (
                ObserverProgressSourceIdentity::Base {
                    kind: BaseSourceKind::Left,
                    ..
                },
                ObserverProgressOccurrence::Leave { .. },
                ObserverProgressLineage::ParticipantTerminal(lineage_participant),
                ObserverProgressProducer::LiveLeaveCommit,
            )
            | (
                ObserverProgressSourceIdentity::Base {
                    kind: BaseSourceKind::Recovered,
                    ..
                },
                ObserverProgressOccurrence::RecoveredFate { .. },
                ObserverProgressLineage::BindingFateFloor(lineage_participant),
                ObserverProgressProducer::RecoveredBindingFate,
            ) => lineage_participant == participant_id,
            (
                ObserverProgressSourceIdentity::Base {
                    sequence,
                    kind: BaseSourceKind::ParticipantAck,
                },
                ObserverProgressOccurrence::ParticipantAck {
                    source_sequence, ..
                },
                ObserverProgressLineage::ParticipantCursor(lineage_participant),
                ObserverProgressProducer::ParticipantAck,
            ) => lineage_participant == participant_id && sequence == source_sequence,
            (
                ObserverProgressSourceIdentity::Extension {
                    base_log_head,
                    extension_sequence,
                    kind: ExtensionSourceKind::MarkerAck,
                },
                ObserverProgressOccurrence::MarkerAck {
                    base_log_head: occurrence_base_log_head,
                    extension_sequence: occurrence_extension_sequence,
                    ..
                },
                ObserverProgressLineage::ParticipantCursor(lineage_participant),
                ObserverProgressProducer::MarkerAck,
            ) => {
                lineage_participant == participant_id
                    && base_log_head == occurrence_base_log_head
                    && extension_sequence == occurrence_extension_sequence
            }
            _ => false,
        };
    if valid {
        Ok(())
    } else {
        Err(ObserverProgressConformanceError::SourceIdentityMismatch)
    }
}
