use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use super::*;
use crate::durability::{ConversationEvent, RedeliveryDecision, StoredEntry, cursor_key_for};

#[test]
fn channel_recovery_reconstructs_partition_sequences() -> Result<(), Box<dyn Error>> {
    let store = Arc::new(FakeStore::default());
    store.seed_sequence("orders:0", 42)?;
    store.seed_sequence("orders:2", 5)?;

    let first = block_on(recover_durable_channel("orders", 3, store.clone()))?;
    let second_sequences = block_on(recover_partition_sequences("orders", 3, store.as_ref()))?;

    assert_eq!(first.next_sequences(), &[42, 0, 5]);
    assert_eq!(second_sequences, vec![42, 0, 5]);
    assert_eq!(store.append_count()?, 0);
    assert_eq!(store.cas_count()?, 0);
    assert!(
        store
            .read_from_calls()?
            .contains(&ReadCall::new("orders:0", 0, READ_BATCH_SIZE))
    );
    assert!(
        store
            .read_from_calls()?
            .contains(&ReadCall::new("orders:1", 0, READ_BATCH_SIZE))
    );
    assert!(
        store
            .read_from_calls()?
            .contains(&ReadCall::new("orders:2", 0, READ_BATCH_SIZE))
    );

    let mut recovered = first;
    let published = block_on(recovered.publish(&message("next")))?;

    assert_eq!(published, 42);
    assert_eq!(recovered.next_expected_sequence(0), Some(43));
    assert_eq!(store.last_append()?, Some(("orders:0".to_owned(), 42)));
    Ok(())
}

#[test]
fn conversation_recovery_replays_partial_and_finished_state() -> Result<(), Box<dyn Error>> {
    let store = Arc::new(FakeStore::default());
    store.seed_events(
        "conversation-partial",
        &[
            ConversationEvent::MessageReceived {
                message_id: "msg".to_owned(),
                received_at: 1,
            },
            ConversationEvent::ProcessingStarted {
                message_id: "msg".to_owned(),
            },
            ConversationEvent::StepCompleted {
                message_id: "msg".to_owned(),
                step_index: 0,
                output: vec![7],
            },
        ],
    )?;
    store.seed_events(
        "conversation-finished",
        &[
            ConversationEvent::MessageReceived {
                message_id: "done".to_owned(),
                received_at: 1,
            },
            ConversationEvent::ProcessingStarted {
                message_id: "done".to_owned(),
            },
            ConversationEvent::ProcessingFinished {
                message_id: "done".to_owned(),
            },
        ],
    )?;

    let partial_once = block_on(recover_conversation("conversation-partial", store.clone()))?;
    let partial_twice = block_on(recover_conversation("conversation-partial", store.clone()))?;
    let finished = block_on(recover_conversation("conversation-finished", store.clone()))?;

    assert_eq!(partial_once.expected_seq(), 3);
    assert_eq!(partial_once.state(), partial_twice.state());
    assert_eq!(partial_once.state().last_completed_step("msg"), Some(0));

    let mut resumable = partial_once;
    let decision = block_on(resumable.receive_message("msg", 2))?;
    assert_eq!(decision, RedeliveryDecision::ResumeFrom(1));

    assert_eq!(finished.expected_seq(), 3);
    assert!(finished.state().is_fully_processed("done"));
    let mut finished_for_redelivery = finished;
    let finished_decision = block_on(finished_for_redelivery.receive_message("done", 2))?;
    assert_eq!(finished_decision, RedeliveryDecision::Skip);
    assert_eq!(store.append_count()?, 0);
    assert_eq!(store.cas_count()?, 0);
    Ok(())
}

#[test]
fn cursor_recovery_resumes_offset_and_replays_missed_messages() -> Result<(), Box<dyn Error>> {
    let store = Arc::new(FakeStore::default());
    store.seed_messages("orders:0", 45)?;
    store.set_value(&cursor_key_for("consumer-a", "orders:0"), 42)?;

    let recovered = block_on(recover_cursor_with_replay(
        "consumer-a",
        "orders:0",
        store.as_ref(),
    ))?;
    let recovered_again = block_on(recover_cursor_with_replay(
        "consumer-a",
        "orders:0",
        store.as_ref(),
    ))?;

    assert_eq!(recovered.cursor.current_offset(), 42);
    assert_eq!(recovered_again.cursor.current_offset(), 42);
    assert_eq!(
        recovered.replayed_messages,
        recovered_again.replayed_messages
    );
    assert_eq!(recovered.replayed_messages.len(), 3);
    assert_eq!(
        recovered.replayed_messages[0].payload,
        b"message-42".to_vec()
    );
    assert_eq!(store.append_count()?, 0);
    assert_eq!(store.cas_count()?, 0);

    let mut cursor = recovered.cursor;
    let stale_checkpoint = block_on(cursor.checkpoint(store.as_ref(), 41));
    assert!(matches!(
        stale_checkpoint,
        Err(DurabilityError::CursorRegression {
            stored: 42,
            attempted: 41,
        })
    ));
    assert_eq!(cursor.current_offset(), 42);
    assert_eq!(
        store.value(&cursor_key_for("consumer-a", "orders:0"))?,
        Some(42)
    );
    assert_eq!(store.cas_count()?, 0);

    block_on(cursor.checkpoint(store.as_ref(), 45))?;

    assert_eq!(cursor.current_offset(), 45);
    assert_eq!(
        store.value(&cursor_key_for("consumer-a", "orders:0"))?,
        Some(45)
    );
    assert_eq!(store.cas_count()?, 1);
    Ok(())
}

#[test]
fn cursor_recovery_without_persisted_offset_starts_at_zero() -> Result<(), Box<dyn Error>> {
    let store = Arc::new(FakeStore::default());

    let recovered = block_on(recover_cursor_with_replay(
        "consumer-a",
        "empty:0",
        store.as_ref(),
    ))?;
    let recovered_again = block_on(recover_cursor("consumer-a", "empty:0", store.as_ref()))?;

    assert_eq!(recovered.cursor.current_offset(), 0);
    assert_eq!(recovered.replayed_messages, Vec::new());
    assert_eq!(recovered_again.current_offset(), 0);
    assert_eq!(store.append_count()?, 0);
    assert_eq!(store.cas_count()?, 0);
    Ok(())
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
    values: Mutex<HashMap<String, u64>>,
    append_count: Mutex<usize>,
    cas_count: Mutex<usize>,
    appends: Mutex<Vec<(String, u64)>>,
    read_from_calls: Mutex<Vec<ReadCall>>,
}

#[async_trait::async_trait]
impl DurableStore for FakeStore {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        let actual = self.append_stream_entry(stream_key, payload, expected_seq)?;
        *self.append_count.lock().map_err(|_| lock_error())? += 1;
        self.appends
            .lock()
            .map_err(|_| lock_error())?
            .push((stream_key.to_owned(), expected_seq));
        Ok(actual)
    }

    async fn read_from(
        &self,
        stream_key: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, DurabilityError> {
        self.read_from_calls
            .lock()
            .map_err(|_| lock_error())?
            .push(ReadCall::new(stream_key, offset, limit));
        let start = usize::try_from(offset).map_err(|error| {
            DurabilityError::ConfigError(format!("test offset cannot fit usize: {error}"))
        })?;
        let streams = self.streams.lock().map_err(|_| lock_error())?;
        Ok(streams.get(stream_key).map_or_else(Vec::new, |stream| {
            stream.iter().skip(start).take(limit).cloned().collect()
        }))
    }

    async fn cas(&self, key: &str, old_value: u64, new_value: u64) -> Result<(), DurabilityError> {
        let mut values = self.values.lock().map_err(|_| lock_error())?;
        let actual = values.get(key).copied().unwrap_or(0);
        if old_value != actual {
            return Err(DurabilityError::CursorRegression {
                stored: actual,
                attempted: old_value,
            });
        }
        values.insert(key.to_owned(), new_value);
        drop(values);
        *self.cas_count.lock().map_err(|_| lock_error())? += 1;
        Ok(())
    }

    async fn read_value(&self, key: &str) -> Result<Option<u64>, DurabilityError> {
        self.value(key)
    }

    async fn scan(&self, prefix: &str) -> Result<Vec<StoredEntry>, DurabilityError> {
        let streams = self.streams.lock().map_err(|_| lock_error())?;
        Ok(streams
            .iter()
            .filter(|(stream_key, _)| stream_key.starts_with(prefix))
            .flat_map(|(_, entries)| entries.clone())
            .collect())
    }

    async fn flush(&self) -> Result<(), DurabilityError> {
        Ok(())
    }
}

impl FakeStore {
    fn append_stream_entry(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        let mut streams = self.streams.lock().map_err(|_| lock_error())?;
        let stream = streams.entry(stream_key.to_owned()).or_default();
        let actual = len_to_u64(stream.len())?;
        if expected_seq != actual {
            return Err(DurabilityError::SequenceConflict {
                expected: expected_seq,
                actual,
            });
        }
        stream.push(StoredEntry {
            payload,
            sequence: actual,
            timestamp: 0,
        });
        drop(streams);
        Ok(actual)
    }

    fn seed_sequence(&self, stream_key: &str, count: usize) -> Result<(), DurabilityError> {
        let mut entries = Vec::with_capacity(count);
        for index in 0..count {
            entries.push(StoredEntry {
                payload: Vec::new(),
                sequence: len_to_u64(index)?,
                timestamp: 0,
            });
        }
        self.seed_entries(stream_key, entries)
    }

    fn seed_events(
        &self,
        stream_key: &str,
        events: &[ConversationEvent],
    ) -> Result<(), DurabilityError> {
        let mut entries = Vec::with_capacity(events.len());
        for (index, event) in events.iter().enumerate() {
            entries.push(StoredEntry {
                payload: event.serialize()?,
                sequence: len_to_u64(index)?,
                timestamp: 0,
            });
        }
        self.seed_entries(stream_key, entries)
    }

    fn seed_messages(&self, stream_key: &str, count: usize) -> Result<(), DurabilityError> {
        let mut entries = Vec::with_capacity(count);
        for index in 0..count {
            entries.push(StoredEntry {
                payload: message(&format!("message-{index}")).serialize()?,
                sequence: len_to_u64(index)?,
                timestamp: 0,
            });
        }
        self.seed_entries(stream_key, entries)
    }

    fn seed_entries(
        &self,
        stream_key: &str,
        entries: Vec<StoredEntry>,
    ) -> Result<(), DurabilityError> {
        self.streams
            .lock()
            .map_err(|_| lock_error())?
            .insert(stream_key.to_owned(), entries);
        Ok(())
    }

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

    fn append_count(&self) -> Result<usize, DurabilityError> {
        self.append_count
            .lock()
            .map(|guard| *guard)
            .map_err(|_| lock_error())
    }

    fn cas_count(&self) -> Result<usize, DurabilityError> {
        self.cas_count
            .lock()
            .map(|guard| *guard)
            .map_err(|_| lock_error())
    }

    fn last_append(&self) -> Result<Option<(String, u64)>, DurabilityError> {
        self.appends
            .lock()
            .map(|guard| guard.last().cloned())
            .map_err(|_| lock_error())
    }

    fn read_from_calls(&self) -> Result<Vec<ReadCall>, DurabilityError> {
        self.read_from_calls
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| lock_error())
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

fn len_to_u64(len: usize) -> Result<u64, DurabilityError> {
    u64::try_from(len).map_err(|error| {
        DurabilityError::ConfigError(format!("test length cannot fit u64: {error}"))
    })
}

fn lock_error() -> DurabilityError {
    DurabilityError::StoreError(haematite::EventStoreError::StoreIo(std::io::Error::other(
        "fake store lock poisoned",
    )))
}
