//! Durable four-class binding-fate occurrence routing.
//!
//! Every Died, Detached, Ordinary, and Recovered row enters this router before
//! its protocol transition can surrender an observer-progress projection.  The
//! in-memory table is reconstructed exclusively from durable row tags during
//! cold replay; it is active-occurrence state, not a history-linear index.

use std::collections::BTreeMap;

use liminal_protocol::wire::{BindingEpoch, ParticipantId};

use super::log::{
    StoredDetached, StoredDied, StoredOperation, StoredOrdinaryTerminalSource,
    StoredRecoveredPresentation, StoredSpecificFateIntent, StoredTerminalDisposition,
};
use super::state::{ConversationAuthority, StateError};

/// Exact identity shared by the four binding-fate classes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct FateOccurrenceKey {
    pub(super) conversation_id: u64,
    pub(super) participant_id: ParticipantId,
    pub(super) binding_epoch: BindingEpoch,
}

/// Closed binding-fate class used in typed conflict diagnostics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FateOccurrenceClass {
    Died,
    Detached,
    Ordinary,
    Recovered,
}

/// Durable owner of the occurrence's sole observer presentation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FatePresentationOwner {
    Died,
    Detached,
    Recovered,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExpectedSpecificFate {
    Ordinary,
    Recovered,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PrimaryFate {
    Died {
        source_sequence: u64,
        committed: bool,
        expected_specific: Option<ExpectedSpecificFate>,
    },
    Detached,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct FateOccurrenceState {
    primary: PrimaryFate,
    specific: Option<FateOccurrenceClass>,
    presentation_owner: Option<FatePresentationOwner>,
    recovered_reservation: Option<u64>,
    reservation_consumed: bool,
}

/// One typed refusal emitted before observer mutation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub(super) enum FateOccurrenceConflict {
    #[error("binding-fate occurrence already has a primary {existing:?} row; refused {incoming:?}")]
    PrimaryClass {
        existing: FateOccurrenceClass,
        incoming: FateOccurrenceClass,
    },
    #[error("binding-fate specific row has no earlier Died occurrence")]
    MissingDied,
    #[error("binding-fate specific row does not consume its exact lower Died source")]
    DiedSource,
    #[error("binding-fate specific row class disagrees with the Died intent")]
    SpecificClass,
    #[error("binding-fate occurrence already consumed a specific row")]
    DuplicateSpecific,
    #[error("binding-fate occurrence epoch disagrees with its Died source")]
    BindingEpoch,
    #[error("binding-fate presentation tag disagrees with durable occurrence ownership")]
    PresentationOwner,
}

/// Active occurrence table rebuilt from the validated durable stream.
#[derive(Debug, Default)]
pub(super) struct FateOccurrenceRouter {
    occurrences: BTreeMap<FateOccurrenceKey, FateOccurrenceState>,
}

impl FateOccurrenceRouter {
    pub(super) const fn new() -> Self {
        Self {
            occurrences: BTreeMap::new(),
        }
    }

    pub(super) fn route(
        &mut self,
        conversation_id: u64,
        operation: &StoredOperation,
        source_sequence: u64,
    ) -> Result<(), FateOccurrenceConflict> {
        match operation {
            StoredOperation::Died { row } => self.route_died(conversation_id, row, source_sequence),
            StoredOperation::Detached { row } => self.route_detached(conversation_id, row),
            StoredOperation::Ordinary { row, .. } => {
                let died_source_sequence = match row.terminal_source {
                    StoredOrdinaryTerminalSource::DiedCommitted {
                        died_source_sequence,
                    }
                    | StoredOrdinaryTerminalSource::PendingDiedFinalized {
                        died_source_sequence,
                        ..
                    } => died_source_sequence,
                };
                self.route_specific(
                    FateOccurrenceKey {
                        conversation_id,
                        participant_id: row.participant_id,
                        binding_epoch: row
                            .last_dead_binding_epoch
                            .to_epoch()
                            .map_err(|_| FateOccurrenceConflict::BindingEpoch)?,
                    },
                    died_source_sequence,
                    source_sequence,
                    FateOccurrenceClass::Ordinary,
                    None,
                )
            }
            StoredOperation::Recovered { row, .. } => self.route_specific(
                FateOccurrenceKey {
                    conversation_id,
                    participant_id: row.participant_id,
                    binding_epoch: row
                        .last_dead_binding_epoch
                        .to_epoch()
                        .map_err(|_| FateOccurrenceConflict::BindingEpoch)?,
                },
                row.died_source_sequence,
                source_sequence,
                FateOccurrenceClass::Recovered,
                Some(row.presentation),
            ),
            _ => Ok(()),
        }
    }

    fn route_died(
        &mut self,
        conversation_id: u64,
        row: &StoredDied,
        source_sequence: u64,
    ) -> Result<(), FateOccurrenceConflict> {
        let key = FateOccurrenceKey {
            conversation_id,
            participant_id: row.participant_id,
            binding_epoch: row
                .binding_epoch
                .to_epoch()
                .map_err(|_| FateOccurrenceConflict::BindingEpoch)?,
        };
        let expected_specific = row.specific_fate_intent.map(|intent| match intent {
            StoredSpecificFateIntent::Ordinary { .. } => ExpectedSpecificFate::Ordinary,
            StoredSpecificFateIntent::Recovered { .. } => ExpectedSpecificFate::Recovered,
        });
        let committed = matches!(row.disposition, StoredTerminalDisposition::Committed { .. });
        self.insert_primary(
            key,
            FateOccurrenceState {
                primary: PrimaryFate::Died {
                    source_sequence,
                    committed,
                    expected_specific,
                },
                specific: None,
                presentation_owner: committed.then_some(FatePresentationOwner::Died),
                recovered_reservation: None,
                reservation_consumed: false,
            },
            FateOccurrenceClass::Died,
        )
    }

    fn route_detached(
        &mut self,
        conversation_id: u64,
        row: &StoredDetached,
    ) -> Result<(), FateOccurrenceConflict> {
        let key = FateOccurrenceKey {
            conversation_id,
            participant_id: row.participant_id,
            binding_epoch: row
                .binding_epoch
                .to_epoch()
                .map_err(|_| FateOccurrenceConflict::BindingEpoch)?,
        };
        let committed = matches!(row.disposition, StoredTerminalDisposition::Committed { .. });
        self.insert_primary(
            key,
            FateOccurrenceState {
                primary: PrimaryFate::Detached,
                specific: None,
                presentation_owner: committed.then_some(FatePresentationOwner::Detached),
                recovered_reservation: None,
                reservation_consumed: false,
            },
            FateOccurrenceClass::Detached,
        )
    }

    fn insert_primary(
        &mut self,
        key: FateOccurrenceKey,
        state: FateOccurrenceState,
        incoming: FateOccurrenceClass,
    ) -> Result<(), FateOccurrenceConflict> {
        if let Some(existing) = self.occurrences.insert(key, state) {
            self.occurrences.insert(key, existing);
            return Err(FateOccurrenceConflict::PrimaryClass {
                existing: primary_class(existing.primary),
                incoming,
            });
        }
        Ok(())
    }

    fn route_specific(
        &mut self,
        key: FateOccurrenceKey,
        died_source_sequence: u64,
        source_sequence: u64,
        class: FateOccurrenceClass,
        recovered_presentation: Option<StoredRecoveredPresentation>,
    ) -> Result<(), FateOccurrenceConflict> {
        let state = self
            .occurrences
            .get_mut(&key)
            .ok_or(FateOccurrenceConflict::MissingDied)?;
        let PrimaryFate::Died {
            source_sequence: expected_source,
            committed,
            expected_specific,
        } = state.primary
        else {
            return Err(FateOccurrenceConflict::PrimaryClass {
                existing: FateOccurrenceClass::Detached,
                incoming: class,
            });
        };
        if died_source_sequence != expected_source || died_source_sequence >= source_sequence {
            return Err(FateOccurrenceConflict::DiedSource);
        }
        let expected_class = match expected_specific {
            Some(ExpectedSpecificFate::Ordinary) => FateOccurrenceClass::Ordinary,
            Some(ExpectedSpecificFate::Recovered) => FateOccurrenceClass::Recovered,
            None => return Err(FateOccurrenceConflict::SpecificClass),
        };
        if expected_class != class {
            return Err(FateOccurrenceConflict::SpecificClass);
        }
        if state.specific.is_some() {
            return Err(FateOccurrenceConflict::DuplicateSpecific);
        }
        match (class, committed, recovered_presentation) {
            (
                FateOccurrenceClass::Recovered,
                true,
                Some(StoredRecoveredPresentation::DiedCommittedOwns),
            )
            | (FateOccurrenceClass::Ordinary, _, None) => {}
            (
                FateOccurrenceClass::Recovered,
                false,
                Some(StoredRecoveredPresentation::RecoveredOwnsAndReservesFinalizer),
            ) => {
                if state.presentation_owner.is_some() {
                    return Err(FateOccurrenceConflict::PresentationOwner);
                }
                state.presentation_owner = Some(FatePresentationOwner::Recovered);
                state.recovered_reservation = Some(source_sequence);
            }
            _ => return Err(FateOccurrenceConflict::PresentationOwner),
        }
        state.specific = Some(class);
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn state(&self, key: FateOccurrenceKey) -> Option<FateOccurrenceState> {
        self.occurrences.get(&key).copied()
    }
}

const fn primary_class(primary: PrimaryFate) -> FateOccurrenceClass {
    match primary {
        PrimaryFate::Died { .. } => FateOccurrenceClass::Died,
        PrimaryFate::Detached => FateOccurrenceClass::Detached,
    }
}

impl FateOccurrenceState {
    #[cfg(test)]
    pub(super) const fn presentation_owner(self) -> Option<FatePresentationOwner> {
        self.presentation_owner
    }

    #[cfg(test)]
    pub(super) const fn reservation(self) -> Option<(u64, bool)> {
        match self.recovered_reservation {
            Some(source) => Some((source, self.reservation_consumed)),
            None => None,
        }
    }
}

impl ConversationAuthority {
    /// Routes one durable fate candidate before any observer-progress mutation.
    pub(super) fn route_fate_occurrence(
        &mut self,
        operation: &StoredOperation,
        source_sequence: u64,
    ) -> Result<(), StateError> {
        self.fate_occurrences
            .route(self.conversation_id, operation, source_sequence)
            .map_err(StateError::from)
    }
}
