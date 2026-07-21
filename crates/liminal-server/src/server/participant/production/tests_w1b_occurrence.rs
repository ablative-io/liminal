use std::error::Error;

use liminal_protocol::wire::{BindingEpoch, ConnectionIncarnation, Generation, ParticipantId};

use super::fate_occurrence::{
    FateOccurrenceClass, FateOccurrenceConflict, FateOccurrenceKey, FateOccurrenceRouter,
    FatePresentationOwner,
};
use crate::server::participant::production::log::{
    StoredBindingEpoch, StoredCommittedTerminalAudit, StoredDetached, StoredDetachedCause,
    StoredDetachedSource, StoredDied, StoredDiedCause, StoredOperation, StoredOrdinaryFate,
    StoredOrdinaryTerminalSource, StoredRecoveredFate, StoredRecoveredPresentation,
    StoredSpecificFateIntent, StoredTerminalDisposition,
};

fn epoch(connection: u64, generation: Generation) -> BindingEpoch {
    BindingEpoch::new(ConnectionIncarnation::new(7, connection), generation)
}

fn died(
    participant_id: ParticipantId,
    binding_epoch: BindingEpoch,
    disposition: StoredTerminalDisposition,
    intent: Option<StoredSpecificFateIntent>,
) -> StoredOperation {
    StoredOperation::Died {
        row: StoredDied {
            participant_id,
            binding_epoch: StoredBindingEpoch::from(binding_epoch),
            cause: StoredDiedCause::ConnectionLost,
            terminal_order: 11,
            disposition,
            connection_intent_sequence: Some(3),
            specific_fate_intent: intent,
        },
    }
}

fn detached(participant_id: ParticipantId, binding_epoch: BindingEpoch) -> StoredOperation {
    StoredOperation::Detached {
        row: StoredDetached {
            participant_id,
            binding_epoch: StoredBindingEpoch::from(binding_epoch),
            cause: StoredDetachedCause::CleanDeregister,
            terminal_order: 13,
            disposition: StoredTerminalDisposition::Committed { terminal_seq: 17 },
            source: StoredDetachedSource::ConnectionClose {
                connection_intent_sequence: 5,
            },
        },
    }
}

fn ordinary(
    participant_id: ParticipantId,
    binding_epoch: BindingEpoch,
    died_source_sequence: u64,
) -> StoredOperation {
    StoredOperation::Ordinary {
        row: StoredOrdinaryFate {
            participant_id,
            last_dead_binding_epoch: StoredBindingEpoch::from(binding_epoch),
            ordinary_attached_source_sequence: 1,
            terminal_source: StoredOrdinaryTerminalSource::DiedCommitted {
                died_source_sequence,
            },
            committed_terminal_audit: StoredCommittedTerminalAudit {
                cause: StoredDiedCause::ConnectionLost,
                transaction_order: 11,
                terminal_seq: 19,
            },
            resulting_floor: 23,
        },
        event: vec![29],
    }
}

fn recovered(
    participant_id: ParticipantId,
    binding_epoch: BindingEpoch,
    died_source_sequence: u64,
    presentation: StoredRecoveredPresentation,
) -> StoredOperation {
    StoredOperation::Recovered {
        row: StoredRecoveredFate {
            participant_id,
            last_dead_binding_epoch: StoredBindingEpoch::from(binding_epoch),
            died_source_sequence,
            fenced_attached_source_sequence: 2,
            prior_binding_epoch: StoredBindingEpoch::from(epoch(1, Generation::ONE)),
            marker_delivery_seq: 31,
            resulting_floor: 37,
            presentation,
        },
        event: vec![41],
    }
}

#[test]
fn fate_occurrence_key_presents_each_new_arm_at_most_once() -> Result<(), Box<dyn Error>> {
    let conversation_id = 43;
    let died_participant = 1;
    let detached_participant = 2;
    let ordinary_participant = 3;
    let recovered_participant = 4;
    let died_epoch = epoch(2, Generation::ONE);
    let detached_epoch = epoch(3, Generation::ONE);
    let ordinary_epoch = epoch(4, Generation::ONE);
    let recovered_epoch = epoch(5, Generation::ONE);
    let mut router = FateOccurrenceRouter::new();

    router.route(
        conversation_id,
        &died(
            died_participant,
            died_epoch,
            StoredTerminalDisposition::Committed { terminal_seq: 5 },
            None,
        ),
        0,
    )?;
    router.route(
        conversation_id,
        &detached(detached_participant, detached_epoch),
        1,
    )?;
    router.route(
        conversation_id,
        &died(
            ordinary_participant,
            ordinary_epoch,
            StoredTerminalDisposition::Committed { terminal_seq: 7 },
            Some(StoredSpecificFateIntent::Ordinary {
                attached_source_sequence: 2,
            }),
        ),
        2,
    )?;
    router.route(
        conversation_id,
        &ordinary(ordinary_participant, ordinary_epoch, 2),
        3,
    )?;
    router.route(
        conversation_id,
        &died(
            recovered_participant,
            recovered_epoch,
            StoredTerminalDisposition::Pending,
            Some(StoredSpecificFateIntent::Recovered {
                attached_source_sequence: 4,
                prior_binding_epoch: StoredBindingEpoch::from(epoch(1, Generation::ONE)),
                marker_delivery_seq: 31,
            }),
        ),
        4,
    )?;
    router.route(
        conversation_id,
        &recovered(
            recovered_participant,
            recovered_epoch,
            4,
            StoredRecoveredPresentation::RecoveredOwnsAndReservesFinalizer,
        ),
        5,
    )?;

    let duplicate = router.route(conversation_id, &detached(died_participant, died_epoch), 6);
    assert!(matches!(
        duplicate,
        Err(FateOccurrenceConflict::PrimaryClass {
            existing: FateOccurrenceClass::Died,
            incoming: FateOccurrenceClass::Detached
        })
    ));
    assert_eq!(
        router
            .state(FateOccurrenceKey {
                conversation_id,
                participant_id: recovered_participant,
                binding_epoch: recovered_epoch,
            })
            .ok_or("Recovered occurrence was not retained")?
            .presentation_owner(),
        Some(FatePresentationOwner::Recovered)
    );
    Ok(())
}

#[test]
fn died_then_recovered_same_epoch_presents_died_once() -> Result<(), Box<dyn Error>> {
    let conversation_id = 47;
    let participant_id = 5;
    let binding_epoch = epoch(6, Generation::ONE);
    let mut router = FateOccurrenceRouter::new();
    router.route(
        conversation_id,
        &died(
            participant_id,
            binding_epoch,
            StoredTerminalDisposition::Committed { terminal_seq: 11 },
            Some(StoredSpecificFateIntent::Recovered {
                attached_source_sequence: 1,
                prior_binding_epoch: StoredBindingEpoch::from(epoch(5, Generation::ONE)),
                marker_delivery_seq: 13,
            }),
        ),
        2,
    )?;
    router.route(
        conversation_id,
        &recovered(
            participant_id,
            binding_epoch,
            2,
            StoredRecoveredPresentation::DiedCommittedOwns,
        ),
        3,
    )?;
    let state = router
        .state(FateOccurrenceKey {
            conversation_id,
            participant_id,
            binding_epoch,
        })
        .ok_or("Died occurrence was not retained")?;
    assert_eq!(
        state.presentation_owner(),
        Some(FatePresentationOwner::Died)
    );
    assert_eq!(state.reservation(), None);
    Ok(())
}

#[test]
fn recovered_then_died_same_epoch_refuses_before_observer_mutation() {
    let conversation_id = 53;
    let participant_id = 6;
    let binding_epoch = epoch(7, Generation::ONE);
    let mut router = FateOccurrenceRouter::new();
    let reversed = router.route(
        conversation_id,
        &recovered(
            participant_id,
            binding_epoch,
            1,
            StoredRecoveredPresentation::RecoveredOwnsAndReservesFinalizer,
        ),
        0,
    );
    assert_eq!(reversed, Err(FateOccurrenceConflict::MissingDied));
    assert!(
        router
            .state(FateOccurrenceKey {
                conversation_id,
                participant_id,
                binding_epoch,
            })
            .is_none()
    );
}

#[test]
fn recovered_after_pending_died_presents_measured_floor_once() -> Result<(), Box<dyn Error>> {
    let conversation_id = 59;
    let participant_id = 7;
    let binding_epoch = epoch(8, Generation::ONE);
    let mut router = FateOccurrenceRouter::new();
    router.route(
        conversation_id,
        &died(
            participant_id,
            binding_epoch,
            StoredTerminalDisposition::Pending,
            Some(StoredSpecificFateIntent::Recovered {
                attached_source_sequence: 1,
                prior_binding_epoch: StoredBindingEpoch::from(epoch(7, Generation::ONE)),
                marker_delivery_seq: 17,
            }),
        ),
        1,
    )?;
    router.route(
        conversation_id,
        &recovered(
            participant_id,
            binding_epoch,
            1,
            StoredRecoveredPresentation::RecoveredOwnsAndReservesFinalizer,
        ),
        2,
    )?;
    let state = router
        .state(FateOccurrenceKey {
            conversation_id,
            participant_id,
            binding_epoch,
        })
        .ok_or("pending-Died occurrence was not retained")?;
    assert_eq!(
        state.presentation_owner(),
        Some(FatePresentationOwner::Recovered)
    );
    assert_eq!(state.reservation(), Some((2, false)));
    Ok(())
}
