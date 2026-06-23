use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use super::*;
use crate::durability::{DurabilityMode, StoredEntry};

#[test]
fn cursor_constructors_expose_partition_state_and_debug_clone() {
    let fresh = ConsumerCursor::new("consumer-a", "orders:0");
    let persisted = ConsumerCursor::from_persisted("consumer-a", "orders:0", 42);
    let cloned = persisted.clone();

    assert_eq!(fresh.consumer_id, "consumer-a");
    assert_eq!(fresh.partition_key, "orders:0");
    assert_eq!(fresh.current_offset, 0);
    assert_eq!(persisted.current_offset(), 42);
    assert_eq!(cloned, persisted);
    assert!(format!("{persisted:?}").contains("current_offset: 42"));
}

#[test]
fn checkpoint_uses_current_offset_key_and_updates_on_success() -> Result<(), Box<dyn Error>> {
    let store = FakeStore::default();
    let mut cursor = ConsumerCursor::new("consumer-a", "orders:0");

    block_on(cursor.checkpoint(&store, 5))?;

    assert_eq!(cursor.current_offset(), 5);
    assert_eq!(store.value("consumer-a:orders:0")?, Some(5));
    assert_eq!(
        store.cas_calls()?,
        vec![CasCall::new("consumer-a:orders:0", 0, 5)]
    );
    Ok(())
}

#[test]
fn checkpoint_returns_regression_for_stale_or_backward_offsets() -> Result<(), Box<dyn Error>> {
    let store = FakeStore::default();
    store.set_value("consumer-a:orders:0", 7)?;
    let mut cursor = ConsumerCursor::from_persisted("consumer-a", "orders:0", 5);

    let stale = block_on(cursor.checkpoint(&store, 6));
    assert!(matches!(
        stale,
        Err(DurabilityError::CursorRegression {
            stored: 7,
            attempted: 5,
        })
    ));
    assert_eq!(cursor.current_offset(), 5);
    assert_eq!(store.value("consumer-a:orders:0")?, Some(7));
    assert_eq!(store.cas_calls()?.len(), 1);

    let backward = block_on(cursor.checkpoint(&store, 4));
    assert!(matches!(
        backward,
        Err(DurabilityError::CursorRegression {
            stored: 5,
            attempted: 4,
        })
    ));
    assert_eq!(store.cas_calls()?.len(), 1);
    Ok(())
}

#[test]
fn resume_reads_persisted_offset_without_writing() -> Result<(), Box<dyn Error>> {
    let store = FakeStore::default();
    store.set_value("consumer-a:orders:0", 42)?;

    let mut cursor = block_on(ConsumerCursor::resume("consumer-a", "orders:0", &store))?;

    assert_eq!(cursor.current_offset(), 42);
    assert_eq!(store.read_value_calls()?, vec!["consumer-a:orders:0"]);
    assert_eq!(store.cas_calls()?, Vec::new());

    block_on(cursor.checkpoint(&store, 45))?;

    assert_eq!(cursor.current_offset(), 45);
    assert_eq!(store.value("consumer-a:orders:0")?, Some(45));
    assert_eq!(
        store.cas_calls()?,
        vec![CasCall::new("consumer-a:orders:0", 42, 45)]
    );
    Ok(())
}

#[test]
fn resume_without_persisted_offset_starts_at_zero() -> Result<(), Box<dyn Error>> {
    let store = FakeStore::default();

    let cursor = block_on(ConsumerCursor::resume("consumer-a", "empty:0", &store))?;

    assert_eq!(cursor.current_offset(), 0);
    assert_eq!(store.read_value_calls()?, vec!["consumer-a:empty:0"]);
    assert_eq!(store.cas_calls()?, Vec::new());
    Ok(())
}

#[test]
fn per_message_driver_checkpoints_each_message_and_resets() -> Result<(), Box<dyn Error>> {
    let store = FakeStore::default();
    let mut cursor = ConsumerCursor::new("consumer-a", "orders:0");
    let mut driver = CheckpointDriver::new(CheckpointPolicy::PerMessage);

    block_on(driver.record_processed(&mut cursor, &store, 1))?;
    block_on(driver.record_processed(&mut cursor, &store, 2))?;

    assert_eq!(cursor.current_offset(), 2);
    assert_eq!(driver.messages_since_last_checkpoint(), 0);
    assert_eq!(driver.pending_offset(), None);
    assert_eq!(
        store.cas_calls()?,
        vec![
            CasCall::new("consumer-a:orders:0", 0, 1),
            CasCall::new("consumer-a:orders:0", 1, 2),
        ]
    );
    Ok(())
}

#[test]
fn per_batch_driver_waits_for_batch_size_then_resets() -> Result<(), Box<dyn Error>> {
    let store = FakeStore::default();
    let mut cursor = ConsumerCursor::new("consumer-a", "orders:0");
    let mut driver = CheckpointDriver::new(CheckpointPolicy::PerBatch(10));

    for next_offset in 1..10 {
        block_on(driver.record_processed(&mut cursor, &store, next_offset))?;
    }

    assert_eq!(cursor.current_offset(), 0);
    assert_eq!(driver.messages_since_last_checkpoint(), 9);
    assert_eq!(store.cas_calls()?, Vec::new());

    block_on(driver.record_processed(&mut cursor, &store, 10))?;

    assert_eq!(cursor.current_offset(), 10);
    assert_eq!(driver.messages_since_last_checkpoint(), 0);
    assert_eq!(
        store.cas_calls()?,
        vec![CasCall::new("consumer-a:orders:0", 0, 10)]
    );
    Ok(())
}

#[test]
fn explicit_flush_driver_checkpoints_only_on_flush() -> Result<(), Box<dyn Error>> {
    let store = FakeStore::default();
    let mut cursor = ConsumerCursor::new("consumer-a", "orders:0");
    let mut driver = CheckpointDriver::new(CheckpointPolicy::ExplicitFlush);

    block_on(driver.record_processed(&mut cursor, &store, 1))?;
    block_on(driver.record_processed(&mut cursor, &store, 2))?;
    block_on(driver.record_processed(&mut cursor, &store, 3))?;

    assert_eq!(cursor.current_offset(), 0);
    assert_eq!(driver.messages_since_last_checkpoint(), 3);
    assert_eq!(driver.pending_offset(), Some(3));
    assert_eq!(store.cas_calls()?, Vec::new());

    block_on(driver.flush(&mut cursor, &store))?;
    block_on(driver.flush(&mut cursor, &store))?;

    assert_eq!(cursor.current_offset(), 3);
    assert_eq!(driver.messages_since_last_checkpoint(), 0);
    assert_eq!(
        store.cas_calls()?,
        vec![CasCall::new("consumer-a:orders:0", 0, 3)]
    );
    Ok(())
}

#[test]
fn driver_can_be_built_from_durability_config() -> Result<(), Box<dyn Error>> {
    let config = DurabilityConfig::new(
        DurabilityMode::Durable,
        1,
        std::time::Duration::from_secs(60),
        CheckpointPolicy::PerBatch(2),
    )?;
    let driver = CheckpointDriver::new(config);

    assert_eq!(driver.policy(), CheckpointPolicy::PerBatch(2));
    assert_eq!(driver.messages_since_last_checkpoint(), 0);
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CasCall {
    key: String,
    old_value: u64,
    new_value: u64,
}

impl CasCall {
    fn new(key: &str, old_value: u64, new_value: u64) -> Self {
        Self {
            key: key.to_owned(),
            old_value,
            new_value,
        }
    }
}

#[derive(Debug, Default)]
struct FakeStore {
    values: Mutex<HashMap<String, u64>>,
    cas_calls: Mutex<Vec<CasCall>>,
    read_value_calls: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl DurableStore for FakeStore {
    async fn flush(&self) -> Result<(), DurabilityError> {
        Ok(())
    }

    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        Err(store_error(&format!(
            "cursor tests do not append {stream_key} at {expected_seq} with {} bytes",
            payload.len()
        )))
    }

    async fn read_from(
        &self,
        stream_key: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, DurabilityError> {
        Err(store_error(&format!(
            "cursor tests do not read {stream_key} from {offset} with limit {limit}"
        )))
    }

    async fn cas(&self, key: &str, old_value: u64, new_value: u64) -> Result<(), DurabilityError> {
        {
            let mut calls = self.cas_calls.lock().map_err(|_| lock_error())?;
            calls.push(CasCall::new(key, old_value, new_value));
        }

        {
            let mut values = self.values.lock().map_err(|_| lock_error())?;
            let actual = values.get(key).copied().unwrap_or(0);
            if old_value != actual {
                return Err(DurabilityError::CursorRegression {
                    stored: actual,
                    attempted: old_value,
                });
            }
            values.insert(key.to_owned(), new_value);
        }
        Ok(())
    }

    async fn read_value(&self, key: &str) -> Result<Option<u64>, DurabilityError> {
        self.read_value_calls
            .lock()
            .map_err(|_| lock_error())?
            .push(key.to_owned());
        self.value(key)
    }

    async fn scan(&self, prefix: &str) -> Result<Vec<StoredEntry>, DurabilityError> {
        if prefix.len() == usize::MAX {
            return Err(store_error("unreachable cursor scan request"));
        }
        Ok(Vec::new())
    }
}

impl FakeStore {
    fn set_value(&self, key: &str, value: u64) -> Result<(), DurabilityError> {
        self.values
            .lock()
            .map_err(|_| lock_error())?
            .insert(key.to_owned(), value);
        Ok(())
    }

    fn value(&self, key: &str) -> Result<Option<u64>, DurabilityError> {
        self.values
            .lock()
            .map(|values| values.get(key).copied())
            .map_err(|_| lock_error())
    }

    fn cas_calls(&self) -> Result<Vec<CasCall>, DurabilityError> {
        self.cas_calls
            .lock()
            .map(|calls| calls.clone())
            .map_err(|_| lock_error())
    }

    fn read_value_calls(&self) -> Result<Vec<String>, DurabilityError> {
        self.read_value_calls
            .lock()
            .map(|calls| calls.clone())
            .map_err(|_| lock_error())
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(NoopWaker));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match Future::poll(Pin::as_mut(&mut future), &mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

struct NoopWaker;

impl Wake for NoopWaker {
    fn wake(self: Arc<Self>) {}
}

fn lock_error() -> DurabilityError {
    store_error("fake store lock poisoned")
}

fn store_error(message: &str) -> DurabilityError {
    DurabilityError::StoreError(haematite::EventStoreError::StoreIo(std::io::Error::other(
        message.to_owned(),
    )))
}
