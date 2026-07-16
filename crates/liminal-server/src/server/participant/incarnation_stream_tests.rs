use std::sync::Arc;

use liminal::durability::{DurableStore, open_ephemeral};
use liminal_protocol::{
    lifecycle::{
        ConnectionIncarnationAllocationDecision, ConnectionIncarnationAllocator,
        ConnectionIncarnationAllocatorRestore, DurableIncarnationReferences,
        allocate_connection_incarnation,
    },
    outcome::ConnectionIncarnationExhausted,
    wire::ConnectionIncarnation,
};

use super::incarnation_stream::{
    IncarnationAllocation, IncarnationStartup, IncarnationStream, IncarnationStreamError,
    StartedIncarnationStream, encode_allocate_event_fixture, encode_startup_event_fixture,
};

fn store() -> Result<Arc<dyn DurableStore>, Box<dyn std::error::Error>> {
    Ok(Arc::new(open_ephemeral(1)?))
}

fn started(
    store: Arc<dyn DurableStore>,
) -> Result<StartedIncarnationStream, Box<dyn std::error::Error>> {
    started_with_bound(store, 4)
}

fn started_with_bound(
    store: Arc<dyn DurableStore>,
    maximum_references: usize,
) -> Result<StartedIncarnationStream, Box<dyn std::error::Error>> {
    let startup = liminal::durability::bridge::block_on(
        IncarnationStream::new(store, maximum_references).startup(),
    )??;
    let IncarnationStartup::Started(started) = startup else {
        return Err("fresh incarnation stream unexpectedly exhausted".into());
    };
    Ok(started)
}

fn append_event(
    store: &Arc<dyn DurableStore>,
    sequence: u64,
    payload: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let assigned = liminal::durability::bridge::block_on(store.append(
        IncarnationStream::stream_key(),
        payload,
        sequence,
    ))??;
    assert_eq!(assigned, sequence);
    liminal::durability::bridge::block_on(store.flush())??;
    Ok(())
}

#[test]
fn event_codec_pins_exact_startup_and_allocate_bytes() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        IncarnationStream::stream_key(),
        "liminal/participant/incarnation/v2",
        "the LPIE transition-input codec must not reuse the scalar LPIC v1 namespace"
    );
    assert_eq!(
        encode_startup_event_fixture()?,
        [b'L', b'P', b'I', b'E', 1, 1, 0, 0, 0, 0]
    );

    let references = [
        ConnectionIncarnation::new(7, 11),
        ConnectionIncarnation::new(7, 11),
    ];
    let mut expected = vec![b'L', b'P', b'I', b'E', 1, 2, 0, 0, 0, 48];
    expected.extend_from_slice(&4_u64.to_be_bytes());
    expected.extend_from_slice(&2_u64.to_be_bytes());
    for incarnation in references {
        expected.extend_from_slice(&incarnation.server_incarnation.to_be_bytes());
        expected.extend_from_slice(&incarnation.connection_ordinal.to_be_bytes());
    }
    assert_eq!(encode_allocate_event_fixture(4, &references)?, expected);
    Ok(())
}

fn seeded_started(
    store: Arc<dyn DurableStore>,
    maximum_references: usize,
    seed: ConnectionIncarnationAllocatorRestore,
) -> Result<StartedIncarnationStream, Box<dyn std::error::Error>> {
    let stream = IncarnationStream::seeded_for_test(store, maximum_references, seed)?;
    Ok(liminal::durability::bridge::block_on(
        stream.resume_started_for_test(),
    )??)
}

#[test]
fn startup_allocations_cold_replay_preserves_header_and_next_pair()
-> Result<(), Box<dyn std::error::Error>> {
    let store = store()?;
    let mut live = started(Arc::clone(&store))?;
    let references = [
        ConnectionIncarnation::new(1, 0),
        ConnectionIncarnation::new(1, 0),
        ConnectionIncarnation::new(1, 1),
        ConnectionIncarnation::new(7, 2),
    ];
    assert_eq!(
        liminal::durability::bridge::block_on(live.allocate(&references))??,
        IncarnationAllocation::Allocated {
            connection_incarnation: ConnectionIncarnation::new(1, 2),
            skipped_collisions: 2,
        }
    );
    let live_header = live.header();
    drop(live);

    let mut replayed = liminal::durability::bridge::block_on(
        IncarnationStream::new(Arc::clone(&store), 4).resume_started_for_test(),
    )??;
    assert_eq!(replayed.header(), live_header);

    let expected_allocator = ConnectionIncarnationAllocator::try_restore(live_header)
        .map_err(|error| format!("live header failed restore: {error:?}"))?;
    let empty = DurableIncarnationReferences::try_new(&[], 4)
        .map_err(|error| format!("empty references failed: {error:?}"))?;
    let ConnectionIncarnationAllocationDecision::Allocated(expected) =
        allocate_connection_incarnation(expected_allocator, empty)
    else {
        return Err("protocol unexpectedly exhausted the next replayed pair".into());
    };
    assert_eq!(
        liminal::durability::bridge::block_on(replayed.allocate(&[]))??,
        IncarnationAllocation::Allocated {
            connection_incarnation: expected.connection_incarnation(),
            skipped_collisions: expected.skipped_collisions(),
        }
    );
    Ok(())
}

#[test]
fn cold_start_replays_each_historical_allocation_under_its_stored_bound()
-> Result<(), Box<dyn std::error::Error>> {
    let store = store()?;
    let mut first_server = started_with_bound(Arc::clone(&store), 4)?;
    let historical_references = [
        ConnectionIncarnation::new(1, 0),
        ConnectionIncarnation::new(1, 1),
        ConnectionIncarnation::new(1, 2),
        ConnectionIncarnation::new(1, 3),
    ];
    assert_eq!(
        liminal::durability::bridge::block_on(first_server.allocate(&historical_references))??,
        IncarnationAllocation::Allocated {
            connection_incarnation: ConnectionIncarnation::new(1, 4),
            skipped_collisions: 4,
        }
    );
    drop(first_server);

    let restarted = liminal::durability::bridge::block_on(
        IncarnationStream::new(Arc::clone(&store), 2).startup(),
    )??;
    let IncarnationStartup::Started(mut restarted) = restarted else {
        return Err("lower-bound restart unexpectedly exhausted".into());
    };
    assert_eq!(restarted.header().server_incarnation, 2);
    assert_eq!(restarted.header().last_examined_connection_ordinal, None);

    let too_many_live = [
        ConnectionIncarnation::new(2, 0),
        ConnectionIncarnation::new(2, 1),
        ConnectionIncarnation::new(2, 2),
    ];
    assert!(matches!(
        liminal::durability::bridge::block_on(restarted.allocate(&too_many_live))?,
        Err(IncarnationStreamError::DurableReferences(_))
    ));
    let entries = liminal::durability::bridge::block_on(store.read_from(
        IncarnationStream::stream_key(),
        0,
        8,
    ))??;
    assert_eq!(entries.len(), 3, "startup, allocation, restarted startup");
    Ok(())
}

#[test]
fn second_startup_replays_history_and_resets_ordinal_namespace()
-> Result<(), Box<dyn std::error::Error>> {
    let store = store()?;
    let mut first_server = started(Arc::clone(&store))?;
    assert_eq!(first_server.header().server_incarnation, 1);
    assert!(matches!(
        liminal::durability::bridge::block_on(first_server.allocate(&[]))??,
        IncarnationAllocation::Allocated {
            connection_incarnation: ConnectionIncarnation {
                server_incarnation: 1,
                connection_ordinal: 0,
            },
            ..
        }
    ));
    drop(first_server);

    let mut second_server = started(Arc::clone(&store))?;
    assert_eq!(second_server.header().server_incarnation, 2);
    assert_eq!(
        second_server.header().last_examined_connection_ordinal,
        None
    );
    assert_eq!(
        liminal::durability::bridge::block_on(second_server.allocate(&[]))??,
        IncarnationAllocation::Allocated {
            connection_incarnation: ConnectionIncarnation::new(2, 0),
            skipped_collisions: 0,
        }
    );

    let entries = liminal::durability::bridge::block_on(store.read_from(
        IncarnationStream::stream_key(),
        0,
        8,
    ))??;
    assert_eq!(entries.len(), 4, "startup, allocation, startup, allocation");
    Ok(())
}

#[test]
fn cold_replay_crosses_bounded_pages_without_polling() -> Result<(), Box<dyn std::error::Error>> {
    let store = store()?;
    let mut first_server = started(Arc::clone(&store))?;
    for ordinal in 0..260_u64 {
        let allocation = liminal::durability::bridge::block_on(first_server.allocate(&[]))??;
        assert_eq!(
            allocation,
            IncarnationAllocation::Allocated {
                connection_incarnation: ConnectionIncarnation::new(1, ordinal),
                skipped_collisions: 0,
            }
        );
    }
    drop(first_server);

    let replayed = liminal::durability::bridge::block_on(
        IncarnationStream::new(store, 4).resume_started_for_test(),
    )??;
    assert_eq!(replayed.header().server_incarnation, 1);
    assert_eq!(
        replayed.header().last_examined_connection_ordinal,
        Some(259)
    );
    Ok(())
}

#[test]
fn configured_reference_bound_refuses_before_any_append() -> Result<(), Box<dyn std::error::Error>>
{
    let store = store()?;
    let mut stream = started_with_bound(Arc::clone(&store), 3)?;
    let references = [
        ConnectionIncarnation::new(1, 0),
        ConnectionIncarnation::new(1, 1),
        ConnectionIncarnation::new(1, 2),
        ConnectionIncarnation::new(1, 3),
    ];
    let result = liminal::durability::bridge::block_on(stream.allocate(&references))?;
    assert!(matches!(
        result,
        Err(IncarnationStreamError::DurableReferences(_))
    ));
    assert_eq!(stream.header().last_examined_connection_ordinal, None);
    let entries = liminal::durability::bridge::block_on(store.read_from(
        IncarnationStream::stream_key(),
        0,
        8,
    ))??;
    assert_eq!(entries.len(), 1, "only startup may have appended");
    Ok(())
}

#[test]
fn stored_declared_reference_bound_is_checked_before_reference_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let store = store()?;
    append_event(&store, 0, encode_startup_event_fixture()?)?;
    append_event(
        &store,
        1,
        encode_allocate_event_fixture(
            1,
            &[
                ConnectionIncarnation::new(1, 0),
                ConnectionIncarnation::new(1, 1),
            ],
        )?,
    )?;

    let result = liminal::durability::bridge::block_on(
        IncarnationStream::new(store, 1).resume_started_for_test(),
    )?;
    assert!(matches!(
        result,
        Err(IncarnationStreamError::StoredReferenceCountExceedsBound {
            actual: 2,
            maximum: 1,
        })
    ));
    Ok(())
}

#[test]
fn allocate_before_first_startup_is_rejected_as_corrupt_history()
-> Result<(), Box<dyn std::error::Error>> {
    let store = store()?;
    append_event(&store, 0, encode_allocate_event_fixture(1, &[])?)?;

    let result = liminal::durability::bridge::block_on(IncarnationStream::new(store, 1).startup())?;
    assert!(matches!(
        result,
        Err(IncarnationStreamError::AllocateBeforeStartup { stored_sequence: 0 })
    ));
    Ok(())
}

#[test]
fn old_scalar_header_and_regression_are_not_representable() -> Result<(), Box<dyn std::error::Error>>
{
    let store = store()?;
    let mut old_scalar = Vec::new();
    old_scalar.extend_from_slice(b"LPIC");
    old_scalar.push(1);
    old_scalar.extend_from_slice(&9_u64.to_be_bytes());
    old_scalar.push(1);
    old_scalar.extend_from_slice(&0_u64.to_be_bytes());
    old_scalar.push(0);
    append_event(&store, 0, old_scalar)?;

    let result = liminal::durability::bridge::block_on(IncarnationStream::new(store, 1).startup())?;
    assert!(matches!(result, Err(IncarnationStreamError::EventMagic)));
    Ok(())
}

#[test]
fn malformed_event_codec_is_rejected_before_protocol_replay()
-> Result<(), Box<dyn std::error::Error>> {
    let truncated_store = store()?;
    append_event(&truncated_store, 0, vec![0; 9])?;
    assert!(matches!(
        liminal::durability::bridge::block_on(
            IncarnationStream::new(truncated_store, 1).startup()
        )?,
        Err(IncarnationStreamError::EventTruncated { .. })
    ));

    let version_store = store()?;
    let mut bad_version = encode_startup_event_fixture()?;
    bad_version[4] = 2;
    append_event(&version_store, 0, bad_version)?;
    assert!(matches!(
        liminal::durability::bridge::block_on(IncarnationStream::new(version_store, 1).startup())?,
        Err(IncarnationStreamError::EventSchemaVersion(2))
    ));

    let tag_store = store()?;
    let mut bad_tag = encode_startup_event_fixture()?;
    bad_tag[5] = 0xFF;
    append_event(&tag_store, 0, bad_tag)?;
    assert!(matches!(
        liminal::durability::bridge::block_on(IncarnationStream::new(tag_store, 1).startup())?,
        Err(IncarnationStreamError::EventKind(0xFF))
    ));

    let body_store = store()?;
    let mut bad_body_length = encode_startup_event_fixture()?;
    bad_body_length[9] = 1;
    append_event(&body_store, 0, bad_body_length)?;
    assert!(matches!(
        liminal::durability::bridge::block_on(IncarnationStream::new(body_store, 1).startup())?,
        Err(IncarnationStreamError::EventBodyLength {
            declared: 1,
            actual: 0,
        })
    ));
    Ok(())
}

#[test]
fn allocate_count_must_select_exact_fixed_width_suffix() -> Result<(), Box<dyn std::error::Error>> {
    let store = store()?;
    append_event(&store, 0, encode_startup_event_fixture()?)?;
    let mut allocate = encode_allocate_event_fixture(2, &[ConnectionIncarnation::new(1, 0)])?;
    allocate[18..26].copy_from_slice(&2_u64.to_be_bytes());
    append_event(&store, 1, allocate)?;

    let result = liminal::durability::bridge::block_on(
        IncarnationStream::new(store, 2).resume_started_for_test(),
    )?;
    assert!(matches!(
        result,
        Err(IncarnationStreamError::AllocateBodyLength {
            count: 2,
            expected: 48,
            actual: 32,
        })
    ));
    Ok(())
}

#[test]
fn max_ordinal_event_replays_then_exhaustion_emits_no_extra_event()
-> Result<(), Box<dyn std::error::Error>> {
    let store = store()?;
    let seed = ConnectionIncarnationAllocatorRestore {
        server_incarnation: 7,
        last_examined_connection_ordinal: Some(u64::MAX - 1),
        connection_ordinal_exhausted: false,
    };
    let mut stream = seeded_started(Arc::clone(&store), 1, seed)?;
    assert_eq!(
        liminal::durability::bridge::block_on(stream.allocate(&[]))??,
        IncarnationAllocation::Allocated {
            connection_incarnation: ConnectionIncarnation::new(7, u64::MAX),
            skipped_collisions: 0,
        }
    );
    assert!(stream.header().connection_ordinal_exhausted);
    drop(stream);

    let mut replayed = seeded_started(Arc::clone(&store), 1, seed)?;
    assert_eq!(
        liminal::durability::bridge::block_on(replayed.allocate(&[]))??,
        IncarnationAllocation::Exhausted(ConnectionIncarnationExhausted::ConnectionOrdinal {
            attempted_server_incarnation: 7,
        })
    );
    let entries = liminal::durability::bridge::block_on(store.read_from(
        IncarnationStream::stream_key(),
        0,
        8,
    ))??;
    assert_eq!(entries.len(), 1, "exhaustion replay must not append");
    Ok(())
}

#[test]
fn referenced_max_collision_event_replays_terminal_exhaustion()
-> Result<(), Box<dyn std::error::Error>> {
    let store = store()?;
    let seed = ConnectionIncarnationAllocatorRestore {
        server_incarnation: 9,
        last_examined_connection_ordinal: Some(u64::MAX - 1),
        connection_ordinal_exhausted: false,
    };
    let mut stream = seeded_started(Arc::clone(&store), 1, seed)?;
    let references = [ConnectionIncarnation::new(9, u64::MAX)];
    assert_eq!(
        liminal::durability::bridge::block_on(stream.allocate(&references))??,
        IncarnationAllocation::Exhausted(ConnectionIncarnationExhausted::ConnectionOrdinal {
            attempted_server_incarnation: 9,
        })
    );
    drop(stream);

    let mut replayed = seeded_started(Arc::clone(&store), 1, seed)?;
    assert!(replayed.header().connection_ordinal_exhausted);
    assert_eq!(
        liminal::durability::bridge::block_on(replayed.allocate(&[]))??,
        IncarnationAllocation::Exhausted(ConnectionIncarnationExhausted::ConnectionOrdinal {
            attempted_server_incarnation: 9,
        })
    );
    let entries = liminal::durability::bridge::block_on(store.read_from(
        IncarnationStream::stream_key(),
        0,
        8,
    ))??;
    assert_eq!(entries.len(), 1, "only first exhaustion transition appends");
    Ok(())
}

#[test]
fn stored_allocate_after_terminal_exhaustion_is_corrupt() -> Result<(), Box<dyn std::error::Error>>
{
    let store = store()?;
    append_event(&store, 0, encode_allocate_event_fixture(1, &[])?)?;
    let seed = ConnectionIncarnationAllocatorRestore {
        server_incarnation: 9,
        last_examined_connection_ordinal: Some(u64::MAX),
        connection_ordinal_exhausted: true,
    };
    let result = liminal::durability::bridge::block_on(
        IncarnationStream::seeded_for_test(store, 1, seed)?.resume_started_for_test(),
    )?;
    assert!(matches!(
        result,
        Err(IncarnationStreamError::AllocateAfterOrdinalExhaustion { stored_sequence: 0 })
    ));
    Ok(())
}

#[test]
fn stored_startup_after_server_exhaustion_is_corrupt() -> Result<(), Box<dyn std::error::Error>> {
    let store = store()?;
    append_event(&store, 0, encode_startup_event_fixture()?)?;
    let seed = ConnectionIncarnationAllocatorRestore {
        server_incarnation: u64::MAX,
        last_examined_connection_ordinal: None,
        connection_ordinal_exhausted: false,
    };
    let result = liminal::durability::bridge::block_on(
        IncarnationStream::seeded_for_test(store, 1, seed)?.resume_started_for_test(),
    )?;
    assert!(matches!(
        result,
        Err(IncarnationStreamError::StartupAfterServerExhaustion { stored_sequence: 0 })
    ));
    Ok(())
}

#[test]
fn max_server_incarnation_refuses_live_startup_without_append()
-> Result<(), Box<dyn std::error::Error>> {
    let store = store()?;
    let seeded = IncarnationStream::seeded_for_test(
        Arc::clone(&store),
        1,
        ConnectionIncarnationAllocatorRestore {
            server_incarnation: u64::MAX,
            last_examined_connection_ordinal: None,
            connection_ordinal_exhausted: false,
        },
    )?;
    let decision = liminal::durability::bridge::block_on(seeded.startup())??;
    let IncarnationStartup::Exhausted(outcome) = decision else {
        return Err("MAX server incarnation unexpectedly started".into());
    };
    assert_eq!(outcome, ConnectionIncarnationExhausted::ServerIncarnation);
    let entries = liminal::durability::bridge::block_on(store.read_from(
        IncarnationStream::stream_key(),
        0,
        8,
    ))??;
    assert!(entries.is_empty());
    Ok(())
}
