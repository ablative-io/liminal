use std::error::Error;
use std::sync::Arc;

use haematite::{Database, DatabaseConfig, EventStore};
use liminal::durability::{DurableStore, HaematiteStore};
use liminal_protocol::algebra::WideResourceVector;
use liminal_protocol::lifecycle::{
    BoundParticipantCursor, ClosureDebt, CumulativeAckOutcome, CursorProgressFact,
    CursorProgressKey,
};
use liminal_protocol::wire::{
    AckRegressionReason, BindingEpoch, ConnectionIncarnation, Generation, ParticipantAck,
    ParticipantAckEnvelope,
};

use super::cursor_repository::{CursorAckCommand, CursorEpisodeRepository, CursorEpisodeStart};

const CONVERSATION_ID: u64 = 700;
const FIRST_PARTICIPANT: u64 = 11;
const SECOND_PARTICIPANT: u64 = 22;
const STREAM_KEY: &str = "participant/cursor-episode/700";

fn create_store(data_dir: &std::path::Path) -> Result<Arc<dyn DurableStore>, Box<dyn Error>> {
    let database = Database::create(DatabaseConfig {
        data_dir: data_dir.to_path_buf(),
        shard_count: 2,
        distributed: None,
    })?;
    Ok(Arc::new(HaematiteStore::new(Arc::new(EventStore::new(
        database,
    )))))
}

fn reopen_store(data_dir: &std::path::Path) -> Result<Arc<dyn DurableStore>, Box<dyn Error>> {
    let database = Database::open(data_dir)?;
    Ok(Arc::new(HaematiteStore::new(Arc::new(EventStore::new(
        database,
    )))))
}

#[test]
#[allow(clippy::too_many_lines)]
fn two_participants_ack_same_retained_suffix_and_survive_cold_reopen() -> Result<(), Box<dyn Error>>
{
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let temp_dir = tempfile::tempdir()?;
    let store = create_store(temp_dir.path())?;
    let first_epoch = binding_epoch(1);
    let second_epoch = binding_epoch(2);
    let debt =
        ClosureDebt::new(WideResourceVector::new(2, 64)).ok_or("test debt must be nonzero")?;
    let start = CursorEpisodeStart {
        conversation_id: CONVERSATION_ID,
        debt,
        observer_progress: 0,
        candidate_high_watermark: 2,
        current_floor: 1,
        cap_floor: 1,
        participants: vec![
            BoundParticipantCursor::new(FIRST_PARTICIPANT, first_epoch, 0),
            BoundParticipantCursor::new(SECOND_PARTICIPANT, second_epoch, 0),
        ],
    };
    let mut repository = runtime.block_on(CursorEpisodeRepository::create(
        STREAM_KEY,
        Arc::clone(&store),
        start,
    ))?;

    let steps = [
        (FIRST_PARTICIPANT, first_epoch, 1),
        (SECOND_PARTICIPANT, second_epoch, 1),
        (FIRST_PARTICIPANT, first_epoch, 2),
        (SECOND_PARTICIPANT, second_epoch, 2),
    ];
    for (step_index, (participant_index, epoch, boundary)) in steps.into_iter().enumerate() {
        let outcome = runtime.block_on(repository.acknowledge(ack_command(
            participant_index,
            epoch,
            boundary,
        )))?;
        assert!(matches!(outcome, CumulativeAckOutcome::Committed(_)));

        let episode = repository.episode();
        assert_eq!(episode.observer_progress(), 0);
        assert_eq!(episode.candidate_high_watermark(), 2);
        assert_eq!(episode.floor_computation().resulting_floor, 1);
        assert_eq!(episode.cap_floor(), 1);
        assert_eq!(episode.retained_suffix_start(), Some(1));
        assert!(episode.retains(1), "record 1 must remain retained");
        assert!(episode.retains(2), "record 2 must remain retained");

        let key = CursorProgressKey {
            participant_index,
            boundary,
        };
        assert_eq!(episode.facts().get(key), Some(CursorProgressFact::Consumed));
        assert_eq!(episode.facts().len(), step_index + 1);
        assert!(
            !episode
                .facts()
                .encode()
                .map_err(|error| format!("fact serialization failed: {error:?}"))?
                .is_empty(),
            "all participant-scoped facts must remain serializable"
        );
        assert!(
            !episode
                .encode()
                .map_err(|error| format!("episode serialization failed: {error:?}"))?
                .is_empty(),
            "the transition-derived episode must serialize after every ack"
        );
    }

    let before_regression = repository
        .episode()
        .encode()
        .map_err(|error| format!("pre-regression episode serialization failed: {error:?}"))?;
    let regression =
        runtime.block_on(repository.acknowledge(ack_command(FIRST_PARTICIPANT, first_epoch, 1)))?;
    let CumulativeAckOutcome::Regression(regression) = regression else {
        return Err("ack below the durable cursor was not refused as a regression".into());
    };
    assert_eq!(
        regression.request(),
        &ParticipantAckEnvelope {
            conversation_id: CONVERSATION_ID,
            participant_id: FIRST_PARTICIPANT,
            capability_generation: Generation::ONE,
            through_seq: 1,
        }
    );
    assert_eq!(regression.current_cursor(), 2);
    assert_eq!(regression.reason(), AckRegressionReason::BelowCursor);
    assert_eq!(repository.next_expected_sequence(), 5);
    let before_restart = repository
        .episode()
        .encode()
        .map_err(|error| format!("pre-restart episode serialization failed: {error:?}"))?;
    assert_eq!(before_restart, before_regression);
    runtime.block_on(repository.flush())?;
    let written_entries = runtime.block_on(store.read_from(STREAM_KEY, 0, 8))?;
    assert_eq!(written_entries.len(), 5);
    assert_eq!(
        written_entries
            .iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>(),
        vec![0, 1, 2, 3, 4]
    );
    drop(repository);
    drop(store);

    let reopened_store = reopen_store(temp_dir.path())?;
    let reopened = runtime
        .block_on(CursorEpisodeRepository::recover(
            STREAM_KEY,
            Arc::clone(&reopened_store),
        ))?
        .ok_or("cursor episode must exist after cold reopen")?;
    assert_eq!(reopened.stream_key(), STREAM_KEY);
    assert_eq!(reopened.next_expected_sequence(), 5);
    assert_eq!(reopened.episode().observer_progress(), 0);
    assert_eq!(reopened.episode().candidate_high_watermark(), 2);
    assert_eq!(reopened.episode().floor_computation().resulting_floor, 1);
    assert_eq!(reopened.episode().retained_suffix_start(), Some(1));
    assert!(reopened.episode().retains(1));
    assert!(reopened.episode().retains(2));
    assert_eq!(
        reopened
            .episode()
            .participant(FIRST_PARTICIPANT)
            .ok_or("first participant must survive replay")?
            .cursor(),
        2
    );
    assert_eq!(
        reopened
            .episode()
            .participant(SECOND_PARTICIPANT)
            .ok_or("second participant must survive replay")?
            .cursor(),
        2
    );

    let expected_keys = [
        CursorProgressKey {
            participant_index: FIRST_PARTICIPANT,
            boundary: 1,
        },
        CursorProgressKey {
            participant_index: FIRST_PARTICIPANT,
            boundary: 2,
        },
        CursorProgressKey {
            participant_index: SECOND_PARTICIPANT,
            boundary: 1,
        },
        CursorProgressKey {
            participant_index: SECOND_PARTICIPANT,
            boundary: 2,
        },
    ];
    assert_eq!(reopened.episode().facts().len(), expected_keys.len());
    for key in expected_keys {
        assert_eq!(
            reopened.episode().facts().get(key),
            Some(CursorProgressFact::Consumed)
        );
    }
    let after_restart = reopened
        .episode()
        .encode()
        .map_err(|error| format!("post-restart episode serialization failed: {error:?}"))?;
    assert_eq!(after_restart, before_restart);
    let reopened_entries = runtime.block_on(reopened_store.read_from(STREAM_KEY, 0, 8))?;
    assert_eq!(reopened_entries, written_entries);

    Ok(())
}

fn binding_epoch(connection_ordinal: u64) -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(90, connection_ordinal),
        Generation::ONE,
    )
}

fn ack_command(
    participant_index: u64,
    receiving_binding_epoch: BindingEpoch,
    through_seq: u64,
) -> CursorAckCommand {
    CursorAckCommand {
        participant_index,
        receiving_binding_epoch,
        request: ParticipantAck {
            conversation_id: CONVERSATION_ID,
            participant_id: participant_index,
            capability_generation: Generation::ONE,
            through_seq,
        },
        contiguously_available_through: 2,
    }
}
