use std::error::Error;
use std::sync::Arc;

use liminal::durability::{bridge::block_on, open_ephemeral};

use super::log::{
    FencedAttachProofRefusal, OperationLog, OperationLogError, READ_BATCH_SIZE, StoredBindingEpoch,
    StoredOperation, StoredRecoveredFate, StoredRecoveredPresentation,
};
use super::ops_session_replay::{ValidationMemoryHighWater, validate_operation_schema_measured};
use super::state::StateError;
use super::tests::test_participant_config;

const CONVERSATION: u64 = 0x0F1B_1121;

const fn epoch(seed: u64) -> StoredBindingEpoch {
    StoredBindingEpoch {
        server_incarnation: seed,
        connection_ordinal: seed,
        capability_generation: seed,
    }
}

#[test]
fn validate_pass_memory_is_page_and_active_state_bounded() -> Result<(), Box<dyn Error>> {
    let store = Arc::new(open_ephemeral(1)?);
    let log = OperationLog::new(store, CONVERSATION);
    let padding_rows = READ_BATCH_SIZE
        .checked_add(1)
        .ok_or("memory-proof padding row count overflow")?;
    for index in 0..padding_rows {
        block_on(log.append(
            &StoredOperation::Genesis {
                event: u64::try_from(index)?.to_be_bytes().to_vec(),
            },
            u64::try_from(index)?,
        ))??;
    }

    let active_bound = test_participant_config().identity_slots;
    let reservation_rows = usize::try_from(active_bound)?
        .checked_add(1)
        .ok_or("memory-proof reservation count overflow")?;
    for index in 0..reservation_rows {
        let sequence = padding_rows
            .checked_add(index)
            .ok_or("memory-proof operation sequence overflow")?;
        let seed = u64::try_from(index)?
            .checked_add(1)
            .ok_or("memory-proof row seed overflow")?;
        block_on(log.append(
            &StoredOperation::Recovered {
                row: StoredRecoveredFate {
                    participant_id: seed,
                    last_dead_binding_epoch: epoch(seed),
                    died_source_sequence: u64::try_from(index)?,
                    fenced_attached_source_sequence: seed,
                    prior_binding_epoch: epoch(seed),
                    marker_delivery_seq: seed,
                    resulting_floor: seed,
                    presentation: StoredRecoveredPresentation::RecoveredOwnsAndReservesFinalizer,
                },
                event: seed.to_be_bytes().to_vec(),
            },
            u64::try_from(sequence)?,
        ))??;
    }

    let mut high_water = ValidationMemoryHighWater::default();
    let refusal = block_on(validate_operation_schema_measured(
        &log,
        active_bound,
        &mut high_water,
    ))?;
    assert!(matches!(
        refusal,
        Err(StateError::Log(OperationLogError::FencedAttachProof {
            reason: FencedAttachProofRefusal::ComposedRecoveredReservationCapacity,
            ..
        }))
    ));
    assert_eq!(high_water.maximum_page_rows, READ_BATCH_SIZE);
    assert_eq!(
        high_water.maximum_active_reservations,
        usize::try_from(active_bound)?
    );
    assert!(padding_rows > READ_BATCH_SIZE);
    Ok(())
}
