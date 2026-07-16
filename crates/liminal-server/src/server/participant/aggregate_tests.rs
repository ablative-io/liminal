use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use liminal::durability::{DurabilityError, DurableStore, StoredEntry, open_ephemeral};
use liminal_protocol::lifecycle::{
    ConversationDecision, ConversationGenesis, ParticipantConversation,
};

use super::aggregate::{
    ConversationAggregateError, ConversationAggregateOpen, ParticipantConversationAggregate,
};
use super::conversation_stream::ConversationStreamError;

const CONVERSATION_ID: u64 = 71;

fn store() -> Result<Arc<dyn DurableStore>, Box<dyn std::error::Error>> {
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

fn ready(
    opened: ConversationAggregateOpen,
) -> Result<ParticipantConversationAggregate, Box<dyn std::error::Error>> {
    match opened {
        ConversationAggregateOpen::Ready(aggregate) => Ok(aggregate),
        ConversationAggregateOpen::AppendFailed(_) => {
            Err(std::io::Error::other("aggregate append unexpectedly failed").into())
        }
    }
}

fn open_ready(
    store: Arc<dyn DurableStore>,
) -> Result<ParticipantConversationAggregate, Box<dyn std::error::Error>> {
    let opened = liminal::durability::bridge::block_on(ParticipantConversationAggregate::open(
        store,
        CONVERSATION_ID,
    ))??;
    ready(opened)
}

#[test]
fn cold_reopen_replays_exact_event_and_never_appends_second_genesis()
-> Result<(), Box<dyn std::error::Error>> {
    let store = store()?;
    let first = open_ready(Arc::clone(&store))?;
    assert_eq!(first.conversation_id(), CONVERSATION_ID);
    assert_eq!(first.stream_head(), 1);
    assert_eq!(first.next_event_ordinal(), 1);
    assert!(first.genesis_validated());
    drop(first);

    let entries = liminal::durability::bridge::block_on(store.read_from(
        "liminal/participant/conversation/v1/71",
        0,
        8,
    ))??;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].payload, canonical_genesis(CONVERSATION_ID)?);

    let reopened = open_ready(Arc::clone(&store))?;
    assert_eq!(reopened.stream_head(), 1);
    assert_eq!(reopened.next_event_ordinal(), 1);
    assert!(reopened.genesis_validated());
    let entries = liminal::durability::bridge::block_on(store.read_from(
        "liminal/participant/conversation/v1/71",
        0,
        8,
    ))??;
    assert_eq!(
        entries.len(),
        1,
        "cold reopen must not append genesis again"
    );
    Ok(())
}

#[test]
fn malformed_canonical_event_fails_cold_load_before_state_use()
-> Result<(), Box<dyn std::error::Error>> {
    let store = store()?;
    let assigned = liminal::durability::bridge::block_on(store.append(
        "liminal/participant/conversation/v1/71",
        vec![b'X'; 30],
        0,
    ))??;
    assert_eq!(assigned, 0);

    let result = liminal::durability::bridge::block_on(ParticipantConversationAggregate::open(
        store,
        CONVERSATION_ID,
    ))?;
    assert!(matches!(
        result,
        Err(ConversationAggregateError::EventDecode {
            stored_sequence: 0,
            ..
        })
    ));
    Ok(())
}

#[test]
fn stored_sequence_must_equal_canonical_event_ordinal() -> Result<(), Box<dyn std::error::Error>> {
    let store = store()?;
    let mut event = canonical_genesis(CONVERSATION_ID)?;
    event[16..24].copy_from_slice(&1_u64.to_be_bytes());
    let assigned = liminal::durability::bridge::block_on(store.append(
        "liminal/participant/conversation/v1/71",
        event,
        0,
    ))??;
    assert_eq!(assigned, 0);

    let result = liminal::durability::bridge::block_on(ParticipantConversationAggregate::open(
        store,
        CONVERSATION_ID,
    ))?;
    assert!(matches!(
        result,
        Err(ConversationAggregateError::StoredEventOrdinal {
            stored_sequence: 0,
            event_ordinal: 1,
        })
    ));
    Ok(())
}

#[derive(Debug)]
struct ConflictOnFirstAppend {
    inner: Arc<dyn DurableStore>,
    inject_conflict: AtomicBool,
    write_competing_event: bool,
}

impl ConflictOnFirstAppend {
    fn new(inner: Arc<dyn DurableStore>) -> Self {
        Self {
            inner,
            inject_conflict: AtomicBool::new(true),
            write_competing_event: true,
        }
    }

    fn rejecting(inner: Arc<dyn DurableStore>) -> Self {
        Self {
            inner,
            inject_conflict: AtomicBool::new(true),
            write_competing_event: false,
        }
    }
}

#[async_trait::async_trait]
impl DurableStore for ConflictOnFirstAppend {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        if self.inject_conflict.swap(false, Ordering::SeqCst) {
            if self.write_competing_event {
                let competing = payload.clone();
                let _assigned = self
                    .inner
                    .append(stream_key, competing, expected_seq)
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

#[derive(Debug)]
struct FailFirstFlush {
    inner: Arc<dyn DurableStore>,
    fail_flush: AtomicBool,
}

impl FailFirstFlush {
    fn new(inner: Arc<dyn DurableStore>) -> Self {
        Self {
            inner,
            fail_flush: AtomicBool::new(true),
        }
    }
}

#[async_trait::async_trait]
impl DurableStore for FailFirstFlush {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
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
        if self.fail_flush.swap(false, Ordering::SeqCst) {
            return Err(DurabilityError::ConfigError(
                "injected aggregate flush failure".into(),
            ));
        }
        self.inner.flush().await
    }
}

#[test]
fn append_conflict_aborts_speculation_and_cold_reloads_competing_commit()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = store()?;
    let conflicting: Arc<dyn DurableStore> =
        Arc::new(ConflictOnFirstAppend::new(Arc::clone(&inner)));

    let opened = liminal::durability::bridge::block_on(ParticipantConversationAggregate::open(
        conflicting,
        CONVERSATION_ID,
    ))??;
    let ConversationAggregateOpen::AppendFailed(failure) = opened else {
        return Err(std::io::Error::other("conflict unexpectedly committed locally").into());
    };
    assert!(matches!(
        failure.error(),
        ConversationStreamError::Durability(_)
    ));
    assert_eq!(failure.reloaded().stream_head(), 1);
    assert_eq!(failure.reloaded().next_event_ordinal(), 1);
    assert!(failure.reloaded().genesis_validated());
    let recovered = failure.into_reloaded();
    assert_eq!(recovered.conversation_id(), CONVERSATION_ID);

    let entries = liminal::durability::bridge::block_on(inner.read_from(
        "liminal/participant/conversation/v1/71",
        0,
        8,
    ))??;
    assert_eq!(entries.len(), 1, "the aggregate must not retry its append");
    assert_eq!(entries[0].payload, canonical_genesis(CONVERSATION_ID)?);
    Ok(())
}

#[test]
fn rejected_append_cannot_publish_speculative_protocol_state()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = store()?;
    let rejecting: Arc<dyn DurableStore> =
        Arc::new(ConflictOnFirstAppend::rejecting(Arc::clone(&inner)));

    let opened = liminal::durability::bridge::block_on(ParticipantConversationAggregate::open(
        rejecting,
        CONVERSATION_ID,
    ))??;
    let ConversationAggregateOpen::AppendFailed(failure) = opened else {
        return Err(std::io::Error::other("rejected append unexpectedly committed").into());
    };
    assert!(matches!(
        failure.error(),
        ConversationStreamError::Durability(DurabilityError::SequenceConflict { .. })
    ));
    assert_eq!(failure.reloaded().stream_head(), 0);
    assert_eq!(failure.reloaded().next_event_ordinal(), 0);
    assert!(!failure.reloaded().genesis_validated());

    let entries = liminal::durability::bridge::block_on(inner.read_from(
        "liminal/participant/conversation/v1/71",
        0,
        8,
    ))??;
    assert!(entries.is_empty());
    Ok(())
}

#[test]
fn flush_failure_aborts_speculation_and_reloads_appended_durable_reality()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = store()?;
    let failing: Arc<dyn DurableStore> = Arc::new(FailFirstFlush::new(Arc::clone(&inner)));

    let opened = liminal::durability::bridge::block_on(ParticipantConversationAggregate::open(
        failing,
        CONVERSATION_ID,
    ))??;
    let ConversationAggregateOpen::AppendFailed(failure) = opened else {
        return Err(std::io::Error::other("failed flush published speculative state").into());
    };
    assert!(matches!(
        failure.error(),
        ConversationStreamError::Durability(DurabilityError::ConfigError(message))
            if message == "injected aggregate flush failure"
    ));
    assert_eq!(failure.reloaded().stream_head(), 1);
    assert_eq!(failure.reloaded().next_event_ordinal(), 1);
    assert!(failure.reloaded().genesis_validated());

    let entries = liminal::durability::bridge::block_on(inner.read_from(
        "liminal/participant/conversation/v1/71",
        0,
        8,
    ))??;
    assert_eq!(entries.len(), 1, "flush ambiguity must not retry append");
    assert_eq!(entries[0].payload, canonical_genesis(CONVERSATION_ID)?);
    Ok(())
}
