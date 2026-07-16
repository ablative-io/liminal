use std::collections::HashMap;
use std::sync::{
    Arc, Barrier, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc,
};

use liminal::durability::{DurabilityError, DurableStore, StoredEntry, open_ephemeral};
use liminal_protocol::lifecycle::{
    ConversationDecision, ConversationGenesis, ParticipantConversation,
};

use super::{
    aggregate::{ConversationAggregateOpen, ParticipantConversationAggregate},
    aggregate_registry::{ConversationRegistryError, ParticipantConversationRegistry},
    conversation_stream::ConversationStreamError,
};

const CONVERSATION_ID: u64 = 81;

fn ephemeral_store() -> Result<Arc<dyn DurableStore>, Box<dyn std::error::Error>> {
    Ok(Arc::new(open_ephemeral(1)?))
}

fn canonical_genesis(conversation_id: u64) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let conversation =
        ParticipantConversation::from_genesis(ConversationGenesis::new(conversation_id));
    let ConversationDecision::Commit(commit) = conversation.decide_genesis_validation() else {
        return Err(std::io::Error::other("fresh protocol genesis was refused").into());
    };
    Ok(commit.event().encode_canonical())
}

#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn DurableStore>,
    appends: AtomicUsize,
}

impl CountingStore {
    fn new(inner: Arc<dyn DurableStore>) -> Self {
        Self {
            inner,
            appends: AtomicUsize::new(0),
        }
    }

    fn append_count(&self) -> usize {
        self.appends.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl DurableStore for CountingStore {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        self.appends.fetch_add(1, Ordering::SeqCst);
        self.inner.append(stream_key, payload, expected_seq).await
    }

    async fn read_from(
        &self,
        stream_key: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, DurabilityError> {
        self.inner.read_from(stream_key, offset, limit).await
    }

    async fn cas(&self, key: &str, old_value: u64, new_value: u64) -> Result<(), DurabilityError> {
        self.inner.cas(key, old_value, new_value).await
    }

    async fn read_value(&self, key: &str) -> Result<Option<u64>, DurabilityError> {
        self.inner.read_value(key).await
    }

    async fn scan(&self, prefix: &str) -> Result<Vec<StoredEntry>, DurabilityError> {
        self.inner.scan(prefix).await
    }

    async fn flush(&self) -> Result<(), DurabilityError> {
        self.inner.flush().await
    }
}

#[derive(Debug)]
struct AmbiguousFirstAppend {
    inner: Arc<dyn DurableStore>,
    first: AtomicBool,
    write_competing_event: bool,
    appends: AtomicUsize,
}

impl AmbiguousFirstAppend {
    fn conflicting(inner: Arc<dyn DurableStore>) -> Self {
        Self {
            inner,
            first: AtomicBool::new(true),
            write_competing_event: true,
            appends: AtomicUsize::new(0),
        }
    }

    fn rejecting(inner: Arc<dyn DurableStore>) -> Self {
        Self {
            inner,
            first: AtomicBool::new(true),
            write_competing_event: false,
            appends: AtomicUsize::new(0),
        }
    }

    fn append_count(&self) -> usize {
        self.appends.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl DurableStore for AmbiguousFirstAppend {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        self.appends.fetch_add(1, Ordering::SeqCst);
        if self.first.swap(false, Ordering::SeqCst) {
            if self.write_competing_event {
                let _assigned = self
                    .inner
                    .append(stream_key, payload.clone(), expected_seq)
                    .await?;
            } else {
                return Err(DurabilityError::SequenceConflict {
                    expected: expected_seq,
                    actual: expected_seq.saturating_add(1),
                });
            }
        }
        self.inner.append(stream_key, payload, expected_seq).await
    }

    async fn read_from(
        &self,
        stream_key: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, DurabilityError> {
        self.inner.read_from(stream_key, offset, limit).await
    }

    async fn cas(&self, key: &str, old_value: u64, new_value: u64) -> Result<(), DurabilityError> {
        self.inner.cas(key, old_value, new_value).await
    }

    async fn read_value(&self, key: &str) -> Result<Option<u64>, DurabilityError> {
        self.inner.read_value(key).await
    }

    async fn scan(&self, prefix: &str) -> Result<Vec<StoredEntry>, DurabilityError> {
        self.inner.scan(prefix).await
    }

    async fn flush(&self) -> Result<(), DurabilityError> {
        self.inner.flush().await
    }
}

#[derive(Debug, Default)]
struct CrashableStoreState {
    visible_streams: HashMap<String, Vec<StoredEntry>>,
    durable_streams: HashMap<String, Vec<StoredEntry>>,
    visible_values: HashMap<String, u64>,
    durable_values: HashMap<String, u64>,
}

#[derive(Debug)]
struct CrashableFlushStore {
    state: Mutex<CrashableStoreState>,
    reads_include_unflushed: bool,
    flush_failures_remaining: AtomicUsize,
    flush_attempts: AtomicUsize,
    appends: AtomicUsize,
}

impl CrashableFlushStore {
    fn new(flush_failures: usize) -> Self {
        Self::with_read_visibility(flush_failures, true)
    }

    fn hidden_until_flush(flush_failures: usize) -> Self {
        Self::with_read_visibility(flush_failures, false)
    }

    fn with_read_visibility(flush_failures: usize, reads_include_unflushed: bool) -> Self {
        Self {
            state: Mutex::new(CrashableStoreState::default()),
            reads_include_unflushed,
            flush_failures_remaining: AtomicUsize::new(flush_failures),
            flush_attempts: AtomicUsize::new(0),
            appends: AtomicUsize::new(0),
        }
    }

    fn append_count(&self) -> usize {
        self.appends.load(Ordering::SeqCst)
    }

    fn flush_attempt_count(&self) -> usize {
        self.flush_attempts.load(Ordering::SeqCst)
    }

    fn durable_entry_count(&self, stream_key: &str) -> Result<usize, DurabilityError> {
        let state = self.state.lock().map_err(|_| {
            DurabilityError::ConfigError("crashable store lock poisoned".to_owned())
        })?;
        Ok(state.durable_streams.get(stream_key).map_or(0, Vec::len))
    }

    fn crash(&self) -> Result<(), DurabilityError> {
        let mut state = self.state.lock().map_err(|_| {
            DurabilityError::ConfigError("crashable store lock poisoned".to_owned())
        })?;
        state.visible_streams = state.durable_streams.clone();
        state.visible_values = state.durable_values.clone();
        drop(state);
        Ok(())
    }
}

#[async_trait::async_trait]
impl DurableStore for CrashableFlushStore {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        let mut state = self.state.lock().map_err(|_| {
            DurabilityError::ConfigError("crashable store lock poisoned".to_owned())
        })?;
        let entries = state
            .visible_streams
            .entry(stream_key.to_owned())
            .or_default();
        let actual = u64::try_from(entries.len()).map_err(|_| {
            DurabilityError::ConfigError("crashable stream length exceeds u64".to_owned())
        })?;
        if actual != expected_seq {
            return Err(DurabilityError::SequenceConflict {
                expected: expected_seq,
                actual,
            });
        }
        entries.push(StoredEntry {
            payload,
            sequence: expected_seq,
            timestamp: 0,
        });
        drop(state);
        self.appends.fetch_add(1, Ordering::SeqCst);
        Ok(expected_seq)
    }

    async fn read_from(
        &self,
        stream_key: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, DurabilityError> {
        let state = self.state.lock().map_err(|_| {
            DurabilityError::ConfigError("crashable store lock poisoned".to_owned())
        })?;
        let streams = if self.reads_include_unflushed {
            &state.visible_streams
        } else {
            &state.durable_streams
        };
        Ok(streams
            .get(stream_key)
            .into_iter()
            .flatten()
            .filter(|entry| entry.sequence >= offset)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn cas(&self, key: &str, old_value: u64, new_value: u64) -> Result<(), DurabilityError> {
        let mut state = self.state.lock().map_err(|_| {
            DurabilityError::ConfigError("crashable store lock poisoned".to_owned())
        })?;
        let actual = state.visible_values.get(key).copied().unwrap_or(0);
        if actual != old_value {
            return Err(DurabilityError::CursorRegression {
                stored: actual,
                attempted: old_value,
            });
        }
        if new_value == 0 {
            state.visible_values.remove(key);
        } else {
            state.visible_values.insert(key.to_owned(), new_value);
        }
        drop(state);
        Ok(())
    }

    async fn read_value(&self, key: &str) -> Result<Option<u64>, DurabilityError> {
        let state = self.state.lock().map_err(|_| {
            DurabilityError::ConfigError("crashable store lock poisoned".to_owned())
        })?;
        let values = if self.reads_include_unflushed {
            &state.visible_values
        } else {
            &state.durable_values
        };
        Ok(values.get(key).copied())
    }

    async fn scan(&self, prefix: &str) -> Result<Vec<StoredEntry>, DurabilityError> {
        let state = self.state.lock().map_err(|_| {
            DurabilityError::ConfigError("crashable store lock poisoned".to_owned())
        })?;
        let streams = if self.reads_include_unflushed {
            &state.visible_streams
        } else {
            &state.durable_streams
        };
        Ok(streams
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .flat_map(|(_, entries)| entries.iter().cloned())
            .collect())
    }

    async fn flush(&self) -> Result<(), DurabilityError> {
        self.flush_attempts.fetch_add(1, Ordering::SeqCst);
        let should_fail = self
            .flush_failures_remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok();
        if should_fail {
            return Err(DurabilityError::ConfigError(
                "injected read-visible flush failure".to_owned(),
            ));
        }
        let mut state = self.state.lock().map_err(|_| {
            DurabilityError::ConfigError("crashable store lock poisoned".to_owned())
        })?;
        state.durable_streams = state.visible_streams.clone();
        state.durable_values = state.visible_values.clone();
        drop(state);
        Ok(())
    }
}

#[test]
fn concurrent_cold_open_creates_one_cell_and_one_genesis_append()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = ephemeral_store()?;
    let counted = Arc::new(CountingStore::new(inner));
    let store: Arc<dyn DurableStore> = counted.clone();
    let registry = Arc::new(ParticipantConversationRegistry::new(store));
    let start = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for _ in 0..2 {
        let registry = Arc::clone(&registry);
        let start = Arc::clone(&start);
        workers.push(std::thread::spawn(move || {
            start.wait();
            registry.with_conversation(CONVERSATION_ID, |aggregate| {
                (aggregate.conversation_id(), aggregate.stream_head())
            })
        }));
    }
    start.wait();
    for worker in workers {
        let result = worker
            .join()
            .map_err(|_| std::io::Error::other("cold-open worker panicked"))??;
        assert_eq!(result, (CONVERSATION_ID, 1));
    }
    assert_eq!(registry.live_cell_count()?, 1);
    assert_eq!(counted.append_count(), 1);
    Ok(())
}

#[test]
fn existing_stream_cold_opens_without_appending() -> Result<(), Box<dyn std::error::Error>> {
    let inner = ephemeral_store()?;
    let assigned = liminal::durability::bridge::block_on(inner.append(
        "liminal/participant/conversation/v1/81",
        canonical_genesis(CONVERSATION_ID)?,
        0,
    ))??;
    assert_eq!(assigned, 0);
    let counted = Arc::new(CountingStore::new(inner));
    let store: Arc<dyn DurableStore> = counted.clone();
    let registry = ParticipantConversationRegistry::new(store);

    let state = registry.with_conversation(CONVERSATION_ID, |aggregate| {
        (aggregate.stream_head(), aggregate.genesis_validated())
    })?;
    assert_eq!(state, (1, true));
    assert_eq!(counted.append_count(), 0);
    Ok(())
}

#[test]
fn same_conversation_operations_are_exclusive_but_other_conversations_progress()
-> Result<(), Box<dyn std::error::Error>> {
    let registry = Arc::new(ParticipantConversationRegistry::new(ephemeral_store()?));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let first_registry = Arc::clone(&registry);
    let first = std::thread::spawn(move || {
        first_registry.with_conversation(CONVERSATION_ID, |_aggregate| {
            let _sent = entered_tx.send(());
            release_rx.recv()
        })
    });
    entered_rx.recv()?;
    assert!(registry.owner_lock_is_held(CONVERSATION_ID)?);

    let other =
        registry.with_conversation(CONVERSATION_ID + 1, |aggregate| aggregate.stream_head())?;
    assert_eq!(other, 1);

    let (second_started_tx, second_started_rx) = mpsc::channel();
    let (second_entered_tx, second_entered_rx) = mpsc::channel();
    let second_registry = Arc::clone(&registry);
    let second = std::thread::spawn(move || {
        let _sent = second_started_tx.send(());
        second_registry.with_conversation(CONVERSATION_ID, |aggregate| {
            let _sent = second_entered_tx.send(());
            aggregate.stream_head()
        })
    });
    second_started_rx.recv()?;
    assert!(registry.owner_lock_is_held(CONVERSATION_ID)?);
    assert!(second_entered_rx.try_recv().is_err());

    release_tx.send(())?;
    first
        .join()
        .map_err(|_| std::io::Error::other("first operation panicked"))???;
    second_entered_rx.recv()?;
    assert_eq!(
        second
            .join()
            .map_err(|_| std::io::Error::other("second operation panicked"))??,
        1
    );
    Ok(())
}

#[test]
fn append_conflict_quarantines_then_cold_replays_without_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = ephemeral_store()?;
    let ambiguous = Arc::new(AmbiguousFirstAppend::conflicting(inner));
    let store: Arc<dyn DurableStore> = ambiguous.clone();
    let registry = ParticipantConversationRegistry::new(store);
    let called = AtomicBool::new(false);

    let first = registry.with_conversation(CONVERSATION_ID, |_aggregate| {
        called.store(true, Ordering::SeqCst);
    });
    assert!(matches!(
        first,
        Err(ConversationRegistryError::AppendFailed {
            conversation_id: CONVERSATION_ID,
            source: ConversationStreamError::Durability(_),
        })
    ));
    assert!(!called.load(Ordering::SeqCst));
    assert_eq!(registry.live_cell_count()?, 1);
    assert_eq!(ambiguous.append_count(), 1);

    let state = registry.with_conversation(CONVERSATION_ID, |aggregate| {
        (aggregate.stream_head(), aggregate.genesis_validated())
    })?;
    assert_eq!(state, (1, true));
    assert_eq!(ambiguous.append_count(), 1);
    Ok(())
}

#[test]
fn flush_ambiguous_owner_remains_quarantined_until_a_successful_barrier()
-> Result<(), Box<dyn std::error::Error>> {
    let crashable = Arc::new(CrashableFlushStore::new(2));
    let store: Arc<dyn DurableStore> = crashable.clone();
    let registry = ParticipantConversationRegistry::new(store);
    let callback_called = AtomicBool::new(false);
    let stream_key = "liminal/participant/conversation/v1/81";

    let first = registry.with_conversation(CONVERSATION_ID, |_| {
        callback_called.store(true, Ordering::SeqCst);
    });
    assert!(matches!(
        first,
        Err(ConversationRegistryError::AppendFailed {
            conversation_id: CONVERSATION_ID,
            source: ConversationStreamError::Durability(DurabilityError::ConfigError(message)),
        }) if message == "injected read-visible flush failure"
    ));
    assert!(!callback_called.load(Ordering::SeqCst));
    assert_eq!(crashable.append_count(), 1);
    assert_eq!(crashable.flush_attempt_count(), 1);
    assert_eq!(crashable.durable_entry_count(stream_key)?, 0);
    assert!(matches!(
        registry.release_if_idle(CONVERSATION_ID),
        Err(ConversationRegistryError::AmbiguityFlushRequired {
            conversation_id: CONVERSATION_ID,
        })
    ));

    let second = registry.with_conversation(CONVERSATION_ID, |_| {
        callback_called.store(true, Ordering::SeqCst);
    });
    assert!(matches!(
        second,
        Err(ConversationRegistryError::AmbiguityFlushFailed {
            conversation_id: CONVERSATION_ID,
            source: DurabilityError::ConfigError(message),
        }) if message == "injected read-visible flush failure"
    ));
    assert!(!callback_called.load(Ordering::SeqCst));
    assert_eq!(crashable.append_count(), 1, "the append is never retried");
    assert_eq!(crashable.flush_attempt_count(), 2);
    assert_eq!(crashable.durable_entry_count(stream_key)?, 0);

    let state = registry.with_conversation(CONVERSATION_ID, |aggregate| {
        callback_called.store(true, Ordering::SeqCst);
        (aggregate.stream_head(), aggregate.genesis_validated())
    })?;
    assert_eq!(state, (1, true));
    assert!(callback_called.load(Ordering::SeqCst));
    assert_eq!(crashable.append_count(), 1);
    assert_eq!(crashable.flush_attempt_count(), 3);
    assert_eq!(crashable.durable_entry_count(stream_key)?, 1);
    Ok(())
}

#[test]
fn recovery_barrier_cold_replays_an_append_hidden_until_flush()
-> Result<(), Box<dyn std::error::Error>> {
    let crashable = Arc::new(CrashableFlushStore::hidden_until_flush(1));
    let store: Arc<dyn DurableStore> = crashable.clone();
    let registry = ParticipantConversationRegistry::new(store);
    let callback_called = AtomicBool::new(false);

    assert!(matches!(
        registry.with_conversation(CONVERSATION_ID, |_| {
            callback_called.store(true, Ordering::SeqCst);
        }),
        Err(ConversationRegistryError::AppendFailed { .. })
    ));
    assert!(!callback_called.load(Ordering::SeqCst));
    assert_eq!(crashable.append_count(), 1);

    let state = registry.with_conversation(CONVERSATION_ID, |aggregate| {
        callback_called.store(true, Ordering::SeqCst);
        (aggregate.stream_head(), aggregate.genesis_validated())
    })?;
    assert_eq!(state, (1, true));
    assert!(callback_called.load(Ordering::SeqCst));
    assert_eq!(
        crashable.append_count(),
        1,
        "post-barrier cold replay must observe the first append instead of duplicating it"
    );
    Ok(())
}

#[test]
fn process_crash_discards_unflushed_visible_state_before_cold_reopen()
-> Result<(), Box<dyn std::error::Error>> {
    let crashable = Arc::new(CrashableFlushStore::new(1));
    let store: Arc<dyn DurableStore> = crashable.clone();
    let callback_called = AtomicBool::new(false);
    let first_registry = ParticipantConversationRegistry::new(Arc::clone(&store));

    assert!(matches!(
        first_registry.with_conversation(CONVERSATION_ID, |_| {
            callback_called.store(true, Ordering::SeqCst);
        }),
        Err(ConversationRegistryError::AppendFailed { .. })
    ));
    assert!(!callback_called.load(Ordering::SeqCst));
    assert_eq!(crashable.append_count(), 1);
    assert_eq!(
        crashable.durable_entry_count("liminal/participant/conversation/v1/81")?,
        0
    );

    drop(first_registry);
    crashable.crash()?;
    let recovered_registry = ParticipantConversationRegistry::new(Arc::clone(&store));
    let recovered = recovered_registry.with_conversation(CONVERSATION_ID, |aggregate| {
        (aggregate.stream_head(), aggregate.genesis_validated())
    })?;
    assert_eq!(recovered, (1, true));
    assert_eq!(crashable.append_count(), 2);
    assert_eq!(
        crashable.durable_entry_count("liminal/participant/conversation/v1/81")?,
        1
    );

    drop(recovered_registry);
    crashable.crash()?;
    let second_recovery = ParticipantConversationRegistry::new(store);
    assert_eq!(
        second_recovery.with_conversation(CONVERSATION_ID, |aggregate| {
            (aggregate.stream_head(), aggregate.genesis_validated())
        })?,
        (1, true)
    );
    assert_eq!(
        crashable.append_count(),
        2,
        "durable recovery must replay genesis without appending a third copy"
    );
    Ok(())
}

#[test]
fn rejected_append_is_quarantined_and_resumed_only_by_later_explicit_call()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = ephemeral_store()?;
    let ambiguous = Arc::new(AmbiguousFirstAppend::rejecting(inner));
    let store: Arc<dyn DurableStore> = ambiguous.clone();
    let registry = ParticipantConversationRegistry::new(store);

    assert!(matches!(
        registry.with_conversation(CONVERSATION_ID, |_| ()),
        Err(ConversationRegistryError::AppendFailed { .. })
    ));
    assert_eq!(
        ambiguous.append_count(),
        1,
        "the failed call must not retry"
    );

    let state = registry.with_conversation(CONVERSATION_ID, |aggregate| {
        (aggregate.stream_head(), aggregate.genesis_validated())
    })?;
    assert_eq!(state, (1, true));
    assert_eq!(ambiguous.append_count(), 2);
    Ok(())
}

#[test]
fn malformed_cold_history_returns_error_without_invoking_handler()
-> Result<(), Box<dyn std::error::Error>> {
    let store = ephemeral_store()?;
    let assigned = liminal::durability::bridge::block_on(store.append(
        "liminal/participant/conversation/v1/81",
        vec![0xFF; 30],
        0,
    ))??;
    assert_eq!(assigned, 0);
    let registry = ParticipantConversationRegistry::new(store);
    let called = AtomicBool::new(false);

    let result = registry.with_conversation(CONVERSATION_ID, |_| {
        called.store(true, Ordering::SeqCst);
    });
    assert!(matches!(
        result,
        Err(ConversationRegistryError::Aggregate(_))
    ));
    assert!(!called.load(Ordering::SeqCst));
    Ok(())
}

#[test]
fn consume_replace_returns_the_same_noncloneable_owner_to_the_registry()
-> Result<(), Box<dyn std::error::Error>> {
    let registry = ParticipantConversationRegistry::new(ephemeral_store()?);
    let observed = registry.consume_replace(CONVERSATION_ID, |aggregate| {
        let observed = (aggregate.conversation_id(), aggregate.stream_head());
        (aggregate, observed)
    })?;
    assert_eq!(observed, (CONVERSATION_ID, 1));
    assert_eq!(
        registry.with_conversation(CONVERSATION_ID, |aggregate| aggregate.stream_head())?,
        1
    );
    Ok(())
}

#[test]
fn mismatched_replacement_is_dropped_and_original_cell_cold_reopens()
-> Result<(), Box<dyn std::error::Error>> {
    let store = ephemeral_store()?;
    let opened = liminal::durability::bridge::block_on(ParticipantConversationAggregate::open(
        Arc::clone(&store),
        CONVERSATION_ID + 1,
    ))??;
    let ConversationAggregateOpen::Ready(replacement) = opened else {
        return Err(std::io::Error::other("replacement aggregate append failed").into());
    };
    let registry = ParticipantConversationRegistry::new(store);

    let mismatch = registry.consume_replace(CONVERSATION_ID, |_original| (replacement, ()));
    assert!(matches!(
        mismatch,
        Err(ConversationRegistryError::ReplacementConversationMismatch {
            expected: CONVERSATION_ID,
            actual,
        }) if actual == CONVERSATION_ID + 1
    ));

    let reopened = registry.with_conversation(CONVERSATION_ID, |aggregate| {
        (aggregate.conversation_id(), aggregate.stream_head())
    })?;
    assert_eq!(reopened, (CONVERSATION_ID, 1));
    Ok(())
}

#[test]
fn idle_release_drops_cell_and_later_cold_reopens_without_new_genesis()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = ephemeral_store()?;
    let counted = Arc::new(CountingStore::new(inner));
    let store: Arc<dyn DurableStore> = counted.clone();
    let registry = ParticipantConversationRegistry::new(store);

    let opened = registry.with_conversation(CONVERSATION_ID, |aggregate| {
        (aggregate.stream_head(), aggregate.genesis_validated())
    })?;
    assert_eq!(opened, (1, true));
    assert_eq!(registry.live_cell_count()?, 1);
    assert_eq!(counted.append_count(), 1);

    assert!(registry.release_if_idle(CONVERSATION_ID)?);
    assert_eq!(registry.live_cell_count()?, 0);
    assert!(!registry.release_if_idle(CONVERSATION_ID)?);

    let reopened = registry.with_conversation(CONVERSATION_ID, |aggregate| {
        (aggregate.stream_head(), aggregate.genesis_validated())
    })?;
    assert_eq!(reopened, (1, true));
    assert_eq!(registry.live_cell_count()?, 1);
    assert_eq!(
        counted.append_count(),
        1,
        "cold reopen must replay the durable genesis rather than append it again"
    );
    Ok(())
}

#[test]
fn release_refuses_held_operation_then_succeeds_after_operation_exits()
-> Result<(), Box<dyn std::error::Error>> {
    let registry = Arc::new(ParticipantConversationRegistry::new(ephemeral_store()?));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let operation_registry = Arc::clone(&registry);
    let operation = std::thread::spawn(move || {
        operation_registry.with_conversation(CONVERSATION_ID, |_aggregate| {
            let _sent = entered_tx.send(());
            release_rx.recv()
        })
    });
    entered_rx.recv()?;

    assert!(matches!(
        registry.release_if_idle(CONVERSATION_ID),
        Err(ConversationRegistryError::ConversationInUse {
            conversation_id: CONVERSATION_ID,
        })
    ));
    assert_eq!(registry.live_cell_count()?, 1);

    release_tx.send(())?;
    operation
        .join()
        .map_err(|_| std::io::Error::other("held operation panicked"))???;
    assert!(registry.release_if_idle(CONVERSATION_ID)?);
    assert_eq!(registry.live_cell_count()?, 0);
    Ok(())
}
