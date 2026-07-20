use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use haematite::{Database, DatabaseConfig, EventStore};
use liminal::durability::{
    CheckpointPolicy, DurabilityConfig, DurabilityError, DurabilityMode, DurableStore,
    HaematiteStore, StoredEntry,
};
use tempfile::TempDir;

/// Builds an on-disk haematite-backed store in a fresh tempdir.
///
/// Returns the store together with the `TempDir` guard, which must outlive the
/// store: dropping it removes the on-disk database directory.
fn disk_store() -> Result<(HaematiteStore, TempDir), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let database = Database::create(DatabaseConfig {
        data_dir: dir.path().join("db"),
        shard_count: 4,
        distributed: None,
    })?;
    Ok((
        HaematiteStore::new(Arc::new(EventStore::new(database))),
        dir,
    ))
}

fn durability_error_question_mark_compile_check() -> Result<(), Box<dyn std::error::Error>> {
    Err(DurabilityError::ConfigError("test".into()))?;
    Ok(())
}

#[test]
fn durability_error_variants_carry_required_context_and_error_trait() {
    fn assert_error<E: std::error::Error>() {}
    assert_error::<DurabilityError>();

    let store_error = DurabilityError::from(haematite::ApiError::Storage(
        haematite::DatabaseError::IoError(std::io::Error::other("disk unavailable")),
    ));
    assert!(matches!(store_error, DurabilityError::StoreError(_)));

    let sequence = DurabilityError::from(haematite::SequenceConflict {
        expected: 7,
        actual: 9,
    });
    assert!(matches!(
        sequence,
        DurabilityError::SequenceConflict {
            expected: 7,
            actual: 9
        }
    ));

    let cursor = DurabilityError::CursorRegression {
        stored: 5,
        attempted: 3,
    };
    assert!(matches!(
        cursor,
        DurabilityError::CursorRegression {
            stored: 5,
            attempted: 3
        }
    ));

    assert!(durability_error_question_mark_compile_check().is_err());
}

#[test]
fn durability_config_validates_fields_and_exposes_values() -> Result<(), Box<dyn std::error::Error>>
{
    assert!(matches!(
        DurabilityConfig::new(
            DurabilityMode::Durable,
            0,
            Duration::from_secs(60),
            CheckpointPolicy::PerMessage,
        ),
        Err(DurabilityError::ConfigError(_))
    ));

    assert!(matches!(
        DurabilityConfig::new(
            DurabilityMode::DurableDedup,
            1,
            Duration::ZERO,
            CheckpointPolicy::PerMessage,
        ),
        Err(DurabilityError::ConfigError(_))
    ));

    assert!(matches!(
        DurabilityConfig::new(
            DurabilityMode::Durable,
            1,
            Duration::ZERO,
            CheckpointPolicy::PerBatch(0),
        ),
        Err(DurabilityError::ConfigError(_))
    ));

    let config = DurabilityConfig::new(
        DurabilityMode::DurableConversation,
        4,
        Duration::from_secs(30),
        CheckpointPolicy::ExplicitFlush,
    )?;

    assert_eq!(config.mode(), DurabilityMode::DurableConversation);
    assert_eq!(config.partition_count(), 4);
    assert_eq!(config.dedup_ttl(), Duration::from_secs(30));
    assert_eq!(config.checkpoint_policy(), CheckpointPolicy::ExplicitFlush);

    let modes = [
        DurabilityMode::Ephemeral,
        DurabilityMode::Durable,
        DurabilityMode::DurableDedup,
        DurabilityMode::DurableConversation,
    ];
    assert_eq!(modes.len(), 4);

    let policies = [
        CheckpointPolicy::PerMessage,
        CheckpointPolicy::PerBatch(1),
        CheckpointPolicy::ExplicitFlush,
    ];
    assert_eq!(policies.len(), 3);

    Ok(())
}

#[test]
fn durable_store_trait_is_object_safe_and_has_expected_entry_shape()
-> Result<(), Box<dyn std::error::Error>> {
    fn assert_debug<T: std::fmt::Debug>() {}
    fn assert_object_safe(_: &dyn DurableStore) {}

    assert_debug::<StoredEntry>();

    let (store, _dir) = disk_store()?;
    assert_object_safe(&store);

    let entry = StoredEntry {
        payload: vec![1, 2, 3],
        sequence: 11,
        timestamp: 12,
    };
    assert_eq!(entry.payload, vec![1, 2, 3]);
    assert_eq!(entry.sequence, 11);
    assert_eq!(entry.timestamp, 12);

    Ok(())
}

#[test]
fn haematite_store_delegates_append_read_scan_and_maps_sequence_conflict()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, _dir) = disk_store()?;

    let sequence = block_on_ready(store.append("stream-a", b"first".to_vec(), 0))?;
    assert_eq!(sequence, 0);

    match block_on_durability(store.append("stream-a", b"stale".to_vec(), 0)) {
        Err(DurabilityError::SequenceConflict {
            expected: 0,
            actual: 1,
        }) => {}
        result => {
            return Err(format!("expected sequence conflict, got {result:?}").into());
        }
    }

    let read = block_on_durability(store.read_from("stream-a", 0, 10))?;
    assert_eq!(read.len(), 1);
    assert_eq!(read[0].payload, b"first".to_vec());
    assert_eq!(read[0].sequence, 0);

    let scanned = block_on_durability(store.scan("stream-"))?;
    assert_eq!(scanned, read);

    Ok(())
}

#[test]
fn haematite_store_delegates_cas_and_maps_mismatch_to_cursor_regression()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, _dir) = disk_store()?;

    block_on_ready(store.cas("consumer-a", 0, 10))?;
    match block_on_durability(store.cas("consumer-a", 5, 11)) {
        Err(DurabilityError::CursorRegression {
            stored: 10,
            attempted: 5,
        }) => {}
        result => {
            return Err(format!("expected cursor regression, got {result:?}").into());
        }
    }

    Ok(())
}

#[test]
fn cas_to_offset_zero_does_not_brick_a_cursor_that_later_advances()
-> Result<(), Box<dyn std::error::Error>> {
    // Regression: a cursor may legitimately checkpoint at offset 0 (`cas(0, 0)`).
    // That MUST NOT persist a physical zero, or the next forward checkpoint
    // `cas(0, n)` — which maps to expect-absent — would wrongly fail against the
    // now-present key and stall the cursor permanently. (The mock FakeStore hid
    // this because it encodes a different "absent == stored-0" CAS contract.)
    let (store, _dir) = disk_store()?;

    // Checkpoint at offset 0 succeeds and writes nothing.
    block_on_ready(store.cas("consumer-z", 0, 0))?;
    if block_on_ready(store.read_value("consumer-z"))?.is_some() {
        return Err("cas(0, 0) must not persist a physical zero".into());
    }

    // The next forward checkpoint must succeed — this FAILED before the fix.
    block_on_ready(store.cas("consumer-z", 0, 5))?;
    if block_on_ready(store.read_value("consumer-z"))? != Some(5) {
        return Err("forward checkpoint after cas(0, 0) did not advance the cursor".into());
    }

    // A stale `cas(0, 0)` against the now-advanced key is still caught honestly.
    match block_on_durability(store.cas("consumer-z", 0, 0)) {
        Err(DurabilityError::CursorRegression {
            stored: 5,
            attempted: 0,
        }) => {}
        result => {
            return Err(format!("expected regression for stale cas(0, 0), got {result:?}").into());
        }
    }

    Ok(())
}

fn block_on_ready<T, E>(
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, Box<dyn std::error::Error>>
where
    E: std::error::Error + 'static,
{
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = pin!(future);

    match Future::poll(Pin::as_mut(&mut future), &mut context) {
        Poll::Ready(Ok(output)) => Ok(output),
        Poll::Ready(Err(error)) => Err(Box::new(error)),
        Poll::Pending => Err(Box::new(PendingFuture)),
    }
}

fn block_on_durability<T>(
    future: impl Future<Output = Result<T, DurabilityError>>,
) -> Result<T, DurabilityError> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = pin!(future);

    match Future::poll(Pin::as_mut(&mut future), &mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => Err(DurabilityError::StoreError(haematite::ApiError::Storage(
            haematite::DatabaseError::IoError(std::io::Error::other(
                "future unexpectedly returned Poll::Pending",
            )),
        ))),
    }
}

#[derive(Debug)]
struct PendingFuture;

impl std::fmt::Display for PendingFuture {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("future unexpectedly returned Poll::Pending")
    }
}

impl std::error::Error for PendingFuture {}
