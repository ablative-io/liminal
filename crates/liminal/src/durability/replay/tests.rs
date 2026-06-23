use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use super::*;
use crate::durability::StoredEntry;

#[test]
fn replay_returns_messages_in_sequence_order_from_offset() -> Result<(), Box<dyn Error>> {
    let store = FakeStore::default();
    store.seed_messages("orders:0", 5, Some(vec![3, 1, 2]))?;

    let replayed = block_on(replay_from(&store, "orders:0", 1))?;

    assert_eq!(
        payloads(&replayed),
        vec!["message-1", "message-2", "message-3", "message-4"]
    );
    assert_eq!(
        store.read_calls()?,
        vec![
            ReadCall::new("orders:0", 1, READ_BATCH_SIZE),
            ReadCall::new("orders:0", 4, READ_BATCH_SIZE),
            ReadCall::new("orders:0", 5, READ_BATCH_SIZE),
        ]
    );
    Ok(())
}

#[test]
fn replay_paginates_until_empty_even_when_pages_are_partial() -> Result<(), Box<dyn Error>> {
    let store = FakeStore::default();
    store.seed_messages("orders:0", 6, Some(vec![2, 2, 1, 1]))?;

    let replayed = block_on(replay_from(&store, "orders:0", 0))?;

    assert_eq!(
        payloads(&replayed),
        vec![
            "message-0",
            "message-1",
            "message-2",
            "message-3",
            "message-4",
            "message-5",
        ]
    );
    assert_eq!(
        store.read_calls()?,
        vec![
            ReadCall::new("orders:0", 0, READ_BATCH_SIZE),
            ReadCall::new("orders:0", 2, READ_BATCH_SIZE),
            ReadCall::new("orders:0", 4, READ_BATCH_SIZE),
            ReadCall::new("orders:0", 5, READ_BATCH_SIZE),
            ReadCall::new("orders:0", 6, READ_BATCH_SIZE),
        ]
    );
    Ok(())
}

#[test]
fn replay_empty_range_returns_empty_without_error() -> Result<(), Box<dyn Error>> {
    let store = FakeStore::default();
    store.seed_messages("orders:0", 3, None)?;

    let replayed = block_on(replay_from(&store, "orders:0", 3))?;

    assert_eq!(replayed, Vec::new());
    assert_eq!(
        store.read_calls()?,
        vec![ReadCall::new("orders:0", 3, READ_BATCH_SIZE)]
    );
    Ok(())
}

#[test]
fn replay_empty_partition_from_zero_returns_empty_without_error() -> Result<(), Box<dyn Error>> {
    let store = FakeStore::default();

    let replayed = block_on(replay_from(&store, "empty:0", 0))?;

    assert_eq!(replayed, Vec::new());
    assert_eq!(
        store.read_calls()?,
        vec![ReadCall::new("empty:0", 0, READ_BATCH_SIZE)]
    );
    Ok(())
}

#[test]
fn deserialize_orders_strictly_by_sequence_when_input_is_shuffled() -> Result<(), Box<dyn Error>> {
    // Stored entries whose Vec order (3, 1, 2) does NOT match their sequence order.
    // The deserialize step MUST reorder them strictly ascending by `sequence`.
    let entries = vec![
        stored_entry("message-3", 3)?,
        stored_entry("message-1", 1)?,
        stored_entry("message-2", 2)?,
    ];

    let replayed = deserialize_in_sequence_order(entries)?;

    assert_eq!(
        payloads(&replayed),
        vec!["message-1", "message-2", "message-3"]
    );
    Ok(())
}

fn stored_entry(payload: &str, sequence: u64) -> Result<StoredEntry, DurabilityError> {
    Ok(StoredEntry {
        payload: message(payload).serialize()?,
        sequence,
        timestamp: 0,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ReadCall {
    stream_key: String,
    offset: u64,
    limit: usize,
}

impl ReadCall {
    fn new(stream_key: &str, offset: u64, limit: usize) -> Self {
        Self {
            stream_key: stream_key.to_owned(),
            offset,
            limit,
        }
    }
}

#[derive(Debug, Default)]
struct FakeStore {
    streams: Mutex<HashMap<String, Vec<StoredEntry>>>,
    page_sizes: Mutex<HashMap<String, Vec<usize>>>,
    read_calls: Mutex<Vec<ReadCall>>,
}

#[async_trait::async_trait]
impl DurableStore for FakeStore {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        Err(store_error(&format!(
            "replay tests do not append {stream_key} at {expected_seq} with {} bytes",
            payload.len()
        )))
    }

    async fn read_from(
        &self,
        stream_key: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, DurabilityError> {
        self.read_calls
            .lock()
            .map_err(|_| lock_error())?
            .push(ReadCall::new(stream_key, offset, limit));

        let start = usize::try_from(offset).map_err(|error| {
            DurabilityError::ConfigError(format!("test offset cannot fit usize: {error}"))
        })?;
        let requested = self.next_page_size(stream_key, limit)?;
        let streams = self.streams.lock().map_err(|_| lock_error())?;
        Ok(streams.get(stream_key).map_or_else(Vec::new, |stream| {
            stream.iter().skip(start).take(requested).cloned().collect()
        }))
    }

    async fn cas(&self, key: &str, old_value: u64, new_value: u64) -> Result<(), DurabilityError> {
        if key.is_empty() && old_value == new_value {
            return Err(store_error("unreachable replay cas request"));
        }
        Ok(())
    }

    async fn read_value(&self, key: &str) -> Result<Option<u64>, DurabilityError> {
        if key.len() == usize::MAX {
            return Err(store_error("unreachable replay read value request"));
        }
        Ok(None)
    }

    async fn scan(&self, prefix: &str) -> Result<Vec<StoredEntry>, DurabilityError> {
        if prefix.len() == usize::MAX {
            return Err(store_error("unreachable replay scan request"));
        }
        Ok(Vec::new())
    }
}

impl FakeStore {
    fn seed_messages(
        &self,
        stream_key: &str,
        count: usize,
        page_sizes: Option<Vec<usize>>,
    ) -> Result<(), DurabilityError> {
        let mut entries = Vec::with_capacity(count);
        for index in 0..count {
            entries.push(StoredEntry {
                payload: message(&format!("message-{index}")).serialize()?,
                sequence: u64::try_from(index).map_err(|error| {
                    DurabilityError::ConfigError(format!("test index cannot fit u64: {error}"))
                })?,
                timestamp: 0,
            });
        }
        self.streams
            .lock()
            .map_err(|_| lock_error())?
            .insert(stream_key.to_owned(), entries);
        if let Some(sizes) = page_sizes {
            self.page_sizes
                .lock()
                .map_err(|_| lock_error())?
                .insert(stream_key.to_owned(), sizes);
        }
        Ok(())
    }

    fn next_page_size(&self, stream_key: &str, limit: usize) -> Result<usize, DurabilityError> {
        let mut page_sizes = self.page_sizes.lock().map_err(|_| lock_error())?;
        Ok(page_sizes
            .get_mut(stream_key)
            .and_then(next_configured_size)
            .unwrap_or(limit))
    }

    fn read_calls(&self) -> Result<Vec<ReadCall>, DurabilityError> {
        self.read_calls
            .lock()
            .map(|calls| calls.clone())
            .map_err(|_| lock_error())
    }
}

fn next_configured_size(sizes: &mut Vec<usize>) -> Option<usize> {
    if sizes.is_empty() {
        None
    } else {
        Some(sizes.remove(0))
    }
}

fn message(payload: &str) -> MessageEnvelope {
    MessageEnvelope {
        payload: payload.as_bytes().to_vec(),
        causal_context: None,
        timestamp: 1,
        publisher_id: "publisher".to_owned(),
        idempotency_key: None,
    }
}

fn payloads(envelopes: &[MessageEnvelope]) -> Vec<String> {
    envelopes
        .iter()
        .map(|envelope| String::from_utf8_lossy(&envelope.payload).into_owned())
        .collect()
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
