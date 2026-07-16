use std::sync::Arc;

use liminal::durability::{DurableStore, open_ephemeral};
use liminal_protocol::{
    lifecycle::ConnectionIncarnationAllocatorRestore, outcome::ConnectionIncarnationExhausted,
    wire::ConnectionIncarnation,
};

use super::incarnation_stream::{
    IncarnationAllocation, IncarnationStartup, IncarnationStream, IncarnationStreamError,
    encode_header_fixture,
};

fn store() -> Result<Arc<dyn DurableStore>, Box<dyn std::error::Error>> {
    Ok(Arc::new(open_ephemeral(1)?))
}

fn started(
    store: Arc<dyn DurableStore>,
) -> Result<super::incarnation_stream::StartedIncarnationStream, Box<dyn std::error::Error>> {
    let startup =
        liminal::durability::bridge::block_on(IncarnationStream::new(store, 3).startup())??;
    let IncarnationStartup::Started(started) = startup else {
        return Err("fresh incarnation stream unexpectedly exhausted".into());
    };
    Ok(started)
}

fn append_fixture_header(
    store: &Arc<dyn DurableStore>,
    header: ConnectionIncarnationAllocatorRestore,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload = encode_header_fixture(header);
    let assigned = liminal::durability::bridge::block_on(store.append(
        IncarnationStream::stream_key(),
        payload,
        0,
    ))??;
    assert_eq!(assigned, 0);
    liminal::durability::bridge::block_on(store.flush())??;
    Ok(())
}

#[test]
fn cold_replay_advances_server_namespace_and_starts_ordinals_at_zero()
-> Result<(), Box<dyn std::error::Error>> {
    let store = store()?;
    let mut first_server = started(Arc::clone(&store))?;
    assert_eq!(first_server.header().server_incarnation, 1);

    let references = [
        ConnectionIncarnation::new(1, 0),
        ConnectionIncarnation::new(1, 1),
        ConnectionIncarnation::new(7, 2),
    ];
    let allocation = liminal::durability::bridge::block_on(first_server.allocate(&references))??;
    assert_eq!(
        allocation,
        IncarnationAllocation::Allocated {
            connection_incarnation: ConnectionIncarnation::new(1, 2),
            skipped_collisions: 2,
        }
    );
    drop(first_server);

    let mut second_server = started(Arc::clone(&store))?;
    assert_eq!(second_server.header().server_incarnation, 2);
    assert_eq!(
        second_server.header().last_examined_connection_ordinal,
        None
    );
    assert_eq!(
        liminal::durability::bridge::block_on(second_server.allocate(&references))??,
        IncarnationAllocation::Allocated {
            connection_incarnation: ConnectionIncarnation::new(2, 0),
            skipped_collisions: 0,
        }
    );
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

    let second_server = started(store)?;
    assert_eq!(second_server.header().server_incarnation, 2);
    assert_eq!(
        second_server.header().last_examined_connection_ordinal,
        None
    );
    Ok(())
}

#[test]
fn configured_reference_bound_refuses_before_any_append() -> Result<(), Box<dyn std::error::Error>>
{
    let store = store()?;
    let mut stream = started(Arc::clone(&store))?;
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
    assert_eq!(
        stream.header().last_examined_connection_ordinal,
        None,
        "a rejected reference collection cannot advance the allocator"
    );
    let entries = liminal::durability::bridge::block_on(store.read_from(
        IncarnationStream::stream_key(),
        0,
        8,
    ))??;
    assert_eq!(entries.len(), 1, "only startup may have appended");
    Ok(())
}

#[test]
fn max_ordinal_is_persisted_once_then_cold_replay_is_stably_exhausted()
-> Result<(), Box<dyn std::error::Error>> {
    let store = store()?;
    append_fixture_header(
        &store,
        ConnectionIncarnationAllocatorRestore {
            server_incarnation: 7,
            last_examined_connection_ordinal: Some(u64::MAX - 1),
            connection_ordinal_exhausted: false,
        },
    )?;
    let mut stream = liminal::durability::bridge::block_on(
        IncarnationStream::new(Arc::clone(&store), 1).resume_started_for_test(),
    )??;

    assert_eq!(
        liminal::durability::bridge::block_on(stream.allocate(&[]))??,
        IncarnationAllocation::Allocated {
            connection_incarnation: ConnectionIncarnation::new(7, u64::MAX),
            skipped_collisions: 0,
        }
    );
    assert_eq!(
        stream.header(),
        ConnectionIncarnationAllocatorRestore {
            server_incarnation: 7,
            last_examined_connection_ordinal: Some(u64::MAX),
            connection_ordinal_exhausted: true,
        }
    );
    drop(stream);

    let mut replayed = liminal::durability::bridge::block_on(
        IncarnationStream::new(Arc::clone(&store), 1).resume_started_for_test(),
    )??;
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
    assert_eq!(entries.len(), 2, "exhaustion replay must not append");
    Ok(())
}

#[test]
fn referenced_max_collision_atomically_marks_exhaustion_before_refusal()
-> Result<(), Box<dyn std::error::Error>> {
    let store = store()?;
    append_fixture_header(
        &store,
        ConnectionIncarnationAllocatorRestore {
            server_incarnation: 9,
            last_examined_connection_ordinal: Some(u64::MAX - 1),
            connection_ordinal_exhausted: false,
        },
    )?;
    let mut stream = liminal::durability::bridge::block_on(
        IncarnationStream::new(Arc::clone(&store), 1).resume_started_for_test(),
    )??;
    let references = [ConnectionIncarnation::new(9, u64::MAX)];
    assert_eq!(
        liminal::durability::bridge::block_on(stream.allocate(&references))??,
        IncarnationAllocation::Exhausted(ConnectionIncarnationExhausted::ConnectionOrdinal {
            attempted_server_incarnation: 9,
        })
    );
    assert!(stream.header().connection_ordinal_exhausted);
    drop(stream);

    let mut replayed = liminal::durability::bridge::block_on(
        IncarnationStream::new(Arc::clone(&store), 1).resume_started_for_test(),
    )??;
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
    assert_eq!(
        entries.len(),
        2,
        "only the first exhaustion transition appends"
    );
    Ok(())
}

#[test]
fn max_server_incarnation_refuses_startup_without_an_append()
-> Result<(), Box<dyn std::error::Error>> {
    let store = store()?;
    append_fixture_header(
        &store,
        ConnectionIncarnationAllocatorRestore {
            server_incarnation: u64::MAX,
            last_examined_connection_ordinal: None,
            connection_ordinal_exhausted: false,
        },
    )?;

    let decision = liminal::durability::bridge::block_on(
        IncarnationStream::new(Arc::clone(&store), 1).startup(),
    )??;
    let IncarnationStartup::Exhausted(outcome) = decision else {
        return Err("MAX server incarnation unexpectedly started".into());
    };
    assert_eq!(outcome, ConnectionIncarnationExhausted::ServerIncarnation);
    let entries = liminal::durability::bridge::block_on(store.read_from(
        IncarnationStream::stream_key(),
        0,
        8,
    ))??;
    assert_eq!(
        entries.len(),
        1,
        "server exhaustion must preserve the header"
    );
    Ok(())
}
