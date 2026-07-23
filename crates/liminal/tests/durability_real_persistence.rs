//! Falsifiable persistence test against the REAL on-disk haematite engine.
//!
//! These tests write through the durable surface backed by a real
//! [`haematite::EventStore`] over an on-disk [`haematite::Database`], drop every
//! in-memory handle, then *reopen the same data directory from scratch* and read
//! the data back. An in-memory store could never satisfy this: dropping its
//! handles discards the data. Surviving a `Database::open` from disk proves the
//! bytes were persisted by the real engine.

use std::sync::Arc;

use haematite::{Database, DatabaseConfig, EventStore};
use liminal::durability::bridge::block_on;
use liminal::durability::{
    DurableChannel, DurableStore, HaematiteStore, MessageEnvelope, StoredEntry,
};

const SHARD_COUNT: usize = 4;

fn envelope(payload: &[u8], publisher: &str) -> MessageEnvelope {
    MessageEnvelope {
        payload: payload.to_vec(),
        causal_context: None,
        timestamp: 1_700_000_000_000,
        publisher_id: publisher.to_owned(),
        idempotency_key: None,
    }
}

fn open_store(data_dir: &std::path::Path) -> Result<HaematiteStore, Box<dyn std::error::Error>> {
    let config_file = data_dir.join("config.json");
    let database = if config_file.exists() {
        Database::open(data_dir)?
    } else {
        Database::create(DatabaseConfig {
            data_dir: data_dir.to_path_buf(),
            shard_count: SHARD_COUNT,
            distributed: None,
            executor_threads: None,
        })?
    };
    Ok(HaematiteStore::new(Arc::new(EventStore::new(database))))
}

#[test]
fn durable_channel_messages_survive_a_real_on_disk_reopen() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("db");

    // Stream key the single-partition channel writes to (see
    // `DurableChannel::stream_key_for`).
    let stream_key = "orders:0";
    let first = b"{\"order\":1}".to_vec();
    let second = b"{\"order\":2}".to_vec();
    let third = b"{\"order\":3}".to_vec();

    // --- Process lifetime 1: write, flush, then drop everything. ---
    {
        let store = open_store(&data_dir)?;
        let store_arc: Arc<dyn DurableStore> = Arc::new(store);
        let mut channel = DurableChannel::new("orders", 1, Arc::clone(&store_arc))?;

        block_on(channel.publish(&envelope(&first, "p1")))??;
        block_on(channel.publish(&envelope(&second, "p1")))??;
        block_on(channel.publish(&envelope(&third, "p1")))??;

        // Persist to durable storage, then drop the channel, the store, and the
        // database so the shard actors shut down and nothing in memory remains.
        block_on(channel.flush_store())??;
        drop(channel);
        drop(store_arc);
    }

    // --- Process lifetime 2: reopen the SAME directory from scratch. ---
    let reopened = open_store(&data_dir)?;
    let entries: Vec<StoredEntry> = block_on(reopened.read_from(stream_key, 0, 1024))??;

    let payloads: Vec<Vec<u8>> = entries.iter().map(|entry| entry.payload.clone()).collect();
    let recovered: Vec<Vec<u8>> = payloads
        .iter()
        .map(|bytes| {
            MessageEnvelope::deserialize(bytes)
                .map(|message| message.payload)
                .map_err(Into::into)
        })
        .collect::<Result<Vec<Vec<u8>>, Box<dyn std::error::Error>>>()?;

    assert_eq!(recovered, vec![first, second, third]);
    assert_eq!(entries[0].sequence, 0);
    assert_eq!(entries[1].sequence, 1);
    assert_eq!(entries[2].sequence, 2);

    Ok(())
}

#[test]
fn cursor_checkpoint_round_trip_against_the_real_engine() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("db");
    let store = open_store(&data_dir)?;
    let key = "consumer-a:orders:0";

    // Fresh cursor: the key is absent, so the "absent == 0" contract means the
    // first checkpoint is a `cas(0, 5)`. This must atomically create the key.
    block_on(store.cas(key, 0, 5))??;
    assert_eq!(block_on(store.read_value(key))??, Some(5));

    // Advance from the stored value.
    block_on(store.cas(key, 5, 9))??;
    assert_eq!(block_on(store.read_value(key))??, Some(9));

    // Regression / stale-checkpoint rejection: moving from a stale expected value
    // must fail and leave the stored value untouched.
    match block_on(store.cas(key, 5, 11))? {
        Err(liminal::durability::DurabilityError::CursorRegression {
            stored: 9,
            attempted: 5,
        }) => {}
        other => return Err(format!("expected cursor regression, got {other:?}").into()),
    }
    assert_eq!(block_on(store.read_value(key))??, Some(9));

    // A second "absent == 0" create against an already-present key must also be
    // rejected (the key exists, so expect-absent fails).
    match block_on(store.cas(key, 0, 1))? {
        Err(liminal::durability::DurabilityError::CursorRegression {
            stored: 9,
            attempted: 0,
        }) => {}
        other => return Err(format!("expected regression on re-create, got {other:?}").into()),
    }

    // The checkpointed cursor also survives a real on-disk reopen.
    block_on(store.flush())??;
    drop(store);
    let reopened = open_store(&data_dir)?;
    assert_eq!(block_on(reopened.read_value(key))??, Some(9));

    Ok(())
}
