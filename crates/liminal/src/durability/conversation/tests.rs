use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use super::*;
use crate::durability::StoredEntry;

#[test]
fn event_serialization_round_trips_all_variants() -> Result<(), Box<dyn Error>> {
    let events = vec![
        ConversationEvent::MessageReceived {
            message_id: "msg-1".to_owned(),
            received_at: 42,
        },
        ConversationEvent::ProcessingStarted {
            message_id: "msg-1".to_owned(),
        },
        ConversationEvent::StepCompleted {
            message_id: "msg-1".to_owned(),
            step_index: 7,
            output: vec![1, 2, 3],
        },
        ConversationEvent::ProcessingFinished {
            message_id: "msg-1".to_owned(),
        },
        ConversationEvent::ErrorOccurred {
            message_id: "msg-2".to_owned(),
            error: "boom".to_owned(),
        },
    ];

    for event in events {
        let decoded = ConversationEvent::deserialize(&event.serialize()?)?;
        assert_eq!(decoded, event);
        assert_eq!(decoded.message_id(), event.message_id());
    }
    Ok(())
}

#[test]
fn state_default_is_empty_and_helpers_track_finished_and_steps() {
    let mut state = ConversationState::default();

    assert!(state.received_messages.is_empty());
    assert!(state.in_progress.is_empty());
    assert!(state.completed_steps.is_empty());
    assert!(state.finished_messages.is_empty());
    assert!(state.errored_messages.is_empty());
    assert!(!state.is_fully_processed("msg"));
    assert_eq!(state.last_completed_step("msg"), None);

    state.apply(&ConversationEvent::MessageReceived {
        message_id: "msg".to_owned(),
        received_at: 1,
    });
    state.apply(&ConversationEvent::ProcessingStarted {
        message_id: "msg".to_owned(),
    });
    state.apply(&ConversationEvent::StepCompleted {
        message_id: "msg".to_owned(),
        step_index: 0,
        output: vec![10],
    });
    state.apply(&ConversationEvent::StepCompleted {
        message_id: "msg".to_owned(),
        step_index: 2,
        output: vec![12],
    });
    state.apply(&ConversationEvent::ProcessingFinished {
        message_id: "msg".to_owned(),
    });

    assert!(state.received_messages.contains("msg"));
    assert!(!state.in_progress.contains("msg"));
    assert!(state.is_fully_processed("msg"));
    assert_eq!(state.last_completed_step("msg"), Some(2));
    assert_eq!(
        state.completed_steps.get(&("msg".to_owned(), 0)),
        Some(&vec![10])
    );
}

#[test]
fn recovery_replays_event_log_and_sets_expected_sequence() -> Result<(), Box<dyn Error>> {
    let store = Arc::new(FakeStore::default());
    let mut conversation = DurableConversation::new("conversation-a", store.clone());

    block_on(conversation.record_message_received("msg", 1))?;
    block_on(conversation.record_processing_started("msg"))?;
    block_on(conversation.record_step_completed("msg", 0, vec![1]))?;
    block_on(conversation.record_step_completed("msg", 1, vec![2]))?;
    block_on(conversation.record_processing_finished("msg"))?;

    let recovered = block_on(DurableConversation::recover(
        "conversation-a",
        store.clone(),
    ))?;
    let state = recovered.state();

    assert_eq!(recovered.expected_seq(), 5);
    assert!(state.is_fully_processed("msg"));
    assert_eq!(state.last_completed_step("msg"), Some(1));
    assert_eq!(
        state.completed_steps.get(&("msg".to_owned(), 0)),
        Some(&vec![1])
    );
    assert_eq!(
        state.completed_steps.get(&("msg".to_owned(), 1)),
        Some(&vec![2])
    );
    assert_eq!(store.reads()?, vec![0]);
    Ok(())
}

#[test]
fn replaying_same_events_twice_produces_identical_state() {
    let events = vec![
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
            output: vec![1],
        },
        ConversationEvent::ProcessingFinished {
            message_id: "msg".to_owned(),
        },
    ];

    let once = ConversationState::replay(&events);
    let mut twice = ConversationState::default();
    for event in events.iter().chain(events.iter()) {
        twice.apply(event);
    }

    assert_eq!(ConversationState::replay(&events), once);
    assert_eq!(twice, once);
}

#[test]
fn append_advances_sequence_and_conflict_is_not_retried() -> Result<(), Box<dyn Error>> {
    let store = Arc::new(FakeStore::default());
    let mut first = DurableConversation::new("conversation-a", store.clone());
    let mut second = DurableConversation::new("conversation-a", store.clone());

    let assigned = block_on(first.record_message_received("msg-1", 1))?;
    let conflict = block_on(second.record_message_received("msg-2", 2));

    assert_eq!(assigned, 0);
    assert_eq!(first.expected_seq(), 1);
    assert!(matches!(
        conflict,
        Err(DurabilityError::SequenceConflict {
            expected: 0,
            actual: 1
        })
    ));
    assert_eq!(second.expected_seq(), 0);
    assert_eq!(store.append_count()?, 1);
    assert_eq!(store.last_append()?, Some(("conversation-a".to_owned(), 0)));
    Ok(())
}

#[test]
fn fully_processed_redelivery_is_no_op() -> Result<(), Box<dyn Error>> {
    let store = Arc::new(FakeStore::default());
    let mut conversation = DurableConversation::new("conversation-a", store.clone());

    block_on(conversation.record_message_received("msg", 1))?;
    block_on(conversation.record_processing_started("msg"))?;
    block_on(conversation.record_processing_finished("msg"))?;
    let append_count = store.append_count()?;

    let decision = block_on(conversation.receive_message("msg", 2))?;

    assert_eq!(decision, RedeliveryDecision::Skip);
    assert_eq!(store.append_count()?, append_count);
    Ok(())
}

#[test]
fn partial_redelivery_resumes_after_last_completed_step() -> Result<(), Box<dyn Error>> {
    let store = Arc::new(FakeStore::default());
    let mut conversation = DurableConversation::new("conversation-a", store.clone());

    block_on(conversation.record_message_received("msg", 1))?;
    block_on(conversation.record_processing_started("msg"))?;
    block_on(conversation.record_step_completed("msg", 0, vec![1]))?;
    let append_count = store.append_count()?;

    let decision = block_on(conversation.receive_message("msg", 2))?;

    assert_eq!(decision, RedeliveryDecision::ResumeFrom(1));
    assert_eq!(store.append_count()?, append_count);
    Ok(())
}

#[test]
fn never_seen_delivery_appends_received_event_and_starts() -> Result<(), Box<dyn Error>> {
    let store = Arc::new(FakeStore::default());
    let mut conversation = DurableConversation::new("conversation-a", store.clone());

    let decision = block_on(conversation.receive_message("msg", 1))?;

    assert_eq!(decision, RedeliveryDecision::Start);
    assert_eq!(store.append_count()?, 1);
    assert!(conversation.state().received_messages.contains("msg"));
    Ok(())
}

#[derive(Debug, Default)]
struct FakeStore {
    streams: Mutex<HashMap<String, Vec<StoredEntry>>>,
    append_count: Mutex<usize>,
    appends: Mutex<Vec<(String, u64)>>,
    reads: Mutex<Vec<u64>>,
}

#[async_trait::async_trait]
impl DurableStore for FakeStore {
    async fn append(
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
        self.reads.lock().map_err(|_| lock_error())?.push(offset);
        let start = usize::try_from(offset).map_err(|error| {
            DurabilityError::ConfigError(format!("test offset cannot fit usize: {error}"))
        })?;
        let streams = self.streams.lock().map_err(|_| lock_error())?;
        Ok(streams.get(stream_key).map_or_else(Vec::new, |stream| {
            stream.iter().skip(start).take(limit).cloned().collect()
        }))
    }

    async fn cas(&self, _: &str, _: u64, _: u64) -> Result<(), DurabilityError> {
        Ok(())
    }

    async fn scan(&self, _: &str) -> Result<Vec<StoredEntry>, DurabilityError> {
        Ok(Vec::new())
    }
}

impl FakeStore {
    fn append_count(&self) -> Result<usize, DurabilityError> {
        self.append_count
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

    fn reads(&self) -> Result<Vec<u64>, DurabilityError> {
        self.reads
            .lock()
            .map(|guard| guard.clone())
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
    DurabilityError::StoreError(haematite::EventStoreError::StoreIo(std::io::Error::other(
        "fake store lock poisoned",
    )))
}
