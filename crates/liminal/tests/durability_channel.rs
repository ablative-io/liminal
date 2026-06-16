use std::collections::VecDeque;
use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use liminal::durability::{
    CausalContext, DurabilityError, DurableChannel, DurableStore, EphemeralChannel,
    MessageEnvelope, StoredEntry,
};

#[test]
fn message_envelope_round_trips_with_deterministic_bytes() -> Result<(), Box<dyn std::error::Error>>
{
    fn assert_debug_clone<T: Clone + std::fmt::Debug>() {}
    assert_debug_clone::<MessageEnvelope>();
    assert_debug_clone::<CausalContext>();

    let envelope = sample_envelope(vec![1, 2, 3]);
    let bytes = envelope.serialize()?;
    let decoded = MessageEnvelope::deserialize(&bytes)?;
    let bytes_again = decoded.serialize()?;

    assert_eq!(decoded, envelope);
    assert_eq!(bytes_again, bytes);
    assert_eq!(decoded.payload, vec![1, 2, 3]);
    assert_eq!(decoded.timestamp, 1_717_171_717_000);
    assert_eq!(decoded.publisher_id, "publisher-a");
    assert_eq!(decoded.idempotency_key, Some("idem-a".to_owned()));
    assert_eq!(
        decoded.causal_context,
        Some(CausalContext {
            parent_id: Some("parent-1".to_owned()),
            vector_clock_entry: Some(42),
        })
    );

    Ok(())
}

#[test]
fn partition_routing_uses_key_function_and_modulo() -> Result<(), Box<dyn std::error::Error>> {
    let store = RecordingStore::new();
    let envelope = sample_envelope(vec![9]);

    let channel = DurableChannel::with_partition_key("routed", 4, store.clone(), |message| {
        u64::from(!message.publisher_id.is_empty()) * 7
    })?;
    assert_eq!(channel.partition_for(&envelope), 3);

    let single = DurableChannel::with_partition_key("single", 1, store.clone(), |message| {
        u64::from(!message.publisher_id.is_empty()) * u64::MAX
    })?;
    assert_eq!(single.partition_for(&envelope), 0);

    let no_key = DurableChannel::new("default", 8, store.clone())?;
    assert_eq!(no_key.partition_for(&envelope), 0);

    let keys = [0, 1, 7, u64::MAX - 1, u64::MAX];
    for key in keys {
        let channel =
            DurableChannel::with_partition_key("bounded", 5, store.clone(), move |_| key)?;
        assert!(channel.partition_for(&envelope) < channel.partition_count());
    }

    Ok(())
}

#[test]
fn durable_channel_tracks_per_partition_next_sequences() -> Result<(), Box<dyn std::error::Error>> {
    let store = RecordingStore::new();
    let mut channel = DurableChannel::new("sequences", 2, store.clone())?;
    assert_eq!(channel.next_sequences(), &[0, 0]);

    block_on_durability(channel.publish(&sample_envelope(vec![0])))?;
    block_on_durability(channel.publish(&sample_envelope(vec![0])))?;
    block_on_durability(channel.publish(&sample_envelope(vec![0])))?;

    assert_eq!(channel.next_expected_sequence(0), Some(3));
    assert_eq!(channel.next_expected_sequence(1), Some(0));

    let calls = store.append_calls()?;
    let expected = [0, 1, 2];
    for (call, expected_seq) in calls.iter().zip(expected) {
        assert_eq!(call.stream_key, "sequences:0");
        assert_eq!(call.expected_seq, expected_seq);
    }

    let store = RecordingStore::new();
    let mut partitioned =
        DurableChannel::with_partition_key("independent", 2, store.clone(), |message| {
            message.payload.first().copied().map_or(0, u64::from)
        })?;

    block_on_durability(partitioned.publish(&sample_envelope(vec![0])))?;
    block_on_durability(partitioned.publish(&sample_envelope(vec![0])))?;
    block_on_durability(partitioned.publish(&sample_envelope(vec![1])))?;

    assert_eq!(partitioned.next_sequences(), &[2, 1]);
    let calls = store.append_calls()?;
    assert_eq!(calls[0].expected_seq, 0);
    assert_eq!(calls[1].expected_seq, 1);
    assert_eq!(calls[2].expected_seq, 0);
    assert_eq!(calls[2].stream_key, "independent:1");

    Ok(())
}

#[test]
fn durable_publish_appends_serialized_envelope_before_returning_ok()
-> Result<(), Box<dyn std::error::Error>> {
    let store = RecordingStore::new();
    let mut channel = DurableChannel::new("orders", 1, store.clone())?;
    let envelope = sample_envelope(vec![4, 5, 6]);
    let expected_payload = envelope.serialize()?;

    let assigned = block_on_durability(channel.publish(&envelope))?;

    assert_eq!(assigned, 0);
    assert_eq!(channel.next_expected_sequence(0), Some(1));
    let calls = store.append_calls()?;
    assert_eq!(calls.len(), 1);
    let Some(call) = calls.first() else {
        return Err("append call missing".into());
    };
    assert_eq!(call.stream_key, "orders:0");
    assert_eq!(call.expected_seq, 0);
    assert_eq!(call.payload, expected_payload);

    Ok(())
}

#[test]
fn durable_publish_propagates_sequence_conflict_without_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let store = RecordingStore::with_outcomes(vec![AppendOutcome::Conflict {
        expected: 0,
        actual: 2,
    }]);
    let mut channel = DurableChannel::new("conflict", 1, store.clone())?;

    match block_on_durability(channel.publish(&sample_envelope(vec![1]))) {
        Err(DurabilityError::SequenceConflict {
            expected: 0,
            actual: 2,
        }) => {}
        result => return Err(format!("expected sequence conflict, got {result:?}").into()),
    }

    assert_eq!(store.append_calls()?.len(), 1);
    assert_eq!(channel.next_expected_sequence(0), Some(0));

    Ok(())
}

#[test]
fn ephemeral_channel_has_no_store_and_performs_no_store_operations()
-> Result<(), Box<dyn std::error::Error>> {
    fn assert_debug<T: std::fmt::Debug>() {}
    assert_debug::<EphemeralChannel>();

    let store = RecordingStore::new();
    let channel = EphemeralChannel::new("ephemeral", 1)?;
    let envelope = sample_envelope(vec![8]);

    assert_eq!(channel.channel_id(), "ephemeral");
    assert_eq!(channel.partition_count(), 1);
    assert_eq!(channel.partition_for(&envelope), 0);
    assert_eq!(store.append_calls()?.len(), 0);
    assert_eq!(store.read_calls(), 0);
    assert_eq!(store.cas_calls(), 0);
    assert_eq!(store.scan_calls(), 0);

    let durable = DurableChannel::new("requires-store", 1, store)?;
    assert_eq!(durable.channel_id(), "requires-store");

    Ok(())
}

fn sample_envelope(payload: Vec<u8>) -> MessageEnvelope {
    MessageEnvelope {
        payload,
        causal_context: Some(CausalContext {
            parent_id: Some("parent-1".to_owned()),
            vector_clock_entry: Some(42),
        }),
        timestamp: 1_717_171_717_000,
        publisher_id: "publisher-a".to_owned(),
        idempotency_key: Some("idem-a".to_owned()),
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
        Poll::Pending => Err(store_error("future unexpectedly returned Poll::Pending")),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AppendCall {
    stream_key: String,
    payload: Vec<u8>,
    expected_seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ReadCall {
    stream_key: String,
    offset: u64,
    limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CasCall {
    key: String,
    old_value: u64,
    new_value: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ScanCall {
    prefix: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AppendOutcome {
    Conflict { expected: u64, actual: u64 },
}

#[derive(Debug, Default)]
struct RecordingStore {
    append_calls: Mutex<Vec<AppendCall>>,
    append_outcomes: Mutex<VecDeque<AppendOutcome>>,
    read_requests: Mutex<Vec<ReadCall>>,
    cas_requests: Mutex<Vec<CasCall>>,
    scan_requests: Mutex<Vec<ScanCall>>,
    read_calls: AtomicUsize,
    cas_calls: AtomicUsize,
    scan_calls: AtomicUsize,
}

impl RecordingStore {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn with_outcomes(outcomes: Vec<AppendOutcome>) -> Arc<Self> {
        Arc::new(Self {
            append_calls: Mutex::default(),
            append_outcomes: Mutex::new(VecDeque::from(outcomes)),
            read_requests: Mutex::default(),
            cas_requests: Mutex::default(),
            scan_requests: Mutex::default(),
            read_calls: AtomicUsize::default(),
            cas_calls: AtomicUsize::default(),
            scan_calls: AtomicUsize::default(),
        })
    }

    fn append_calls(&self) -> Result<Vec<AppendCall>, DurabilityError> {
        self.append_calls
            .lock()
            .map(|calls| calls.clone())
            .map_err(|_| store_error("append call lock poisoned"))
    }

    fn read_calls(&self) -> usize {
        self.read_calls.load(Ordering::Relaxed)
    }

    fn cas_calls(&self) -> usize {
        self.cas_calls.load(Ordering::Relaxed)
    }

    fn scan_calls(&self) -> usize {
        self.scan_calls.load(Ordering::Relaxed)
    }
}

#[async_trait::async_trait]
impl DurableStore for RecordingStore {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        self.append_calls
            .lock()
            .map_err(|_| store_error("append call lock poisoned"))?
            .push(AppendCall {
                stream_key: stream_key.to_owned(),
                payload,
                expected_seq,
            });

        let outcome = self
            .append_outcomes
            .lock()
            .map_err(|_| store_error("append outcome lock poisoned"))?
            .pop_front();

        match outcome {
            Some(AppendOutcome::Conflict { expected, actual }) => {
                Err(DurabilityError::SequenceConflict { expected, actual })
            }
            None => Ok(expected_seq),
        }
    }

    async fn read_from(
        &self,
        stream_key: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, DurabilityError> {
        self.read_requests
            .lock()
            .map_err(|_| store_error("read request lock poisoned"))?
            .push(ReadCall {
                stream_key: stream_key.to_owned(),
                offset,
                limit,
            });
        self.read_calls.fetch_add(1, Ordering::Relaxed);
        Ok(Vec::new())
    }

    async fn cas(&self, key: &str, old_value: u64, new_value: u64) -> Result<(), DurabilityError> {
        self.cas_requests
            .lock()
            .map_err(|_| store_error("cas request lock poisoned"))?
            .push(CasCall {
                key: key.to_owned(),
                old_value,
                new_value,
            });
        self.cas_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn scan(&self, prefix: &str) -> Result<Vec<StoredEntry>, DurabilityError> {
        self.scan_requests
            .lock()
            .map_err(|_| store_error("scan request lock poisoned"))?
            .push(ScanCall {
                prefix: prefix.to_owned(),
            });
        self.scan_calls.fetch_add(1, Ordering::Relaxed);
        Ok(Vec::new())
    }
}

fn store_error(message: &str) -> DurabilityError {
    DurabilityError::StoreError(haematite::EventStoreError::from(std::io::Error::other(
        message.to_owned(),
    )))
}
