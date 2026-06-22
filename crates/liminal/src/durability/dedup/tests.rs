use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use super::*;

#[test]
fn entry_serialization_round_trips_required_fields() -> Result<(), Box<dyn Error>> {
    let entry = DedupEntry::new("key-a", Some(vec![7, 8, 9]), 42);

    let decoded = DedupEntry::deserialize(&entry.serialize()?)?;

    assert_eq!(decoded.idempotency_key(), "key-a");
    assert_eq!(decoded.receipt(), Some(&[7, 8, 9][..]));
    assert_eq!(decoded.timestamp_millis(), 42);
    Ok(())
}

#[test]
fn key_hash_and_stream_key_are_deterministic() {
    let store = Arc::new(FakeStore::default());
    let cache = DedupCache::new(store, "dedup");

    assert_eq!(key_hash("same-key"), key_hash("same-key"));
    assert_eq!(
        cache.stream_key_for("same-key"),
        cache.stream_key_for("same-key")
    );
    assert!(cache.stream_key_for("same-key").starts_with("dedup:"));
}

#[test]
fn duplicate_completed_returns_receipt_without_append() -> Result<(), Box<dyn Error>> {
    let store = Arc::new(FakeStore::default());
    let cache = DedupCache::new(store.clone(), "dedup");

    assert_eq!(
        block_on(cache.claim_or_get("key", 1_000))?,
        DedupDecision::Claimed
    );
    block_on(cache.complete_receipt("key", ProcessingReceipt::new(vec![1, 2, 3])))?;
    let append_count = store.append_count()?;

    let decision = block_on(cache.claim_or_get("key", 2_000))?;

    assert_eq!(
        decision,
        DedupDecision::Completed(ProcessingReceipt::new(vec![1, 2, 3]))
    );
    assert_eq!(store.append_count()?, append_count);
    Ok(())
}

#[test]
fn duplicate_inflight_returns_status_without_timestamp_refresh() -> Result<(), Box<dyn Error>> {
    let store = Arc::new(FakeStore::default());
    let cache = DedupCache::new(store.clone(), "dedup");

    assert_eq!(
        block_on(cache.claim_or_get("key", 1_000))?,
        DedupDecision::Claimed
    );
    let append_count = store.append_count()?;

    assert_eq!(
        block_on(cache.claim_or_get("key", 2_000))?,
        DedupDecision::InFlight
    );

    let stream = store.stream(&cache.stream_key_for("key"))?;
    let first = stream.first().ok_or("missing first entry")?;
    let entry = DedupEntry::deserialize(&first.payload)?;
    assert_eq!(store.append_count()?, append_count);
    assert_eq!(entry.timestamp_millis(), 1_000);
    Ok(())
}

#[test]
fn complete_receipt_appends_completed_entry_with_receipt_timestamp() -> Result<(), Box<dyn Error>> {
    let store = Arc::new(FakeStore::default());
    let cache = DedupCache::new(store.clone(), "dedup");

    assert_eq!(
        block_on(cache.claim_or_get("key", 1_000))?,
        DedupDecision::Claimed
    );
    block_on(cache.complete_receipt_at("key", ProcessingReceipt::new(vec![4, 5, 6]), 10_000))?;

    let stream = store.stream(&cache.stream_key_for("key"))?;
    assert_eq!(stream.len(), 2);
    let completed = stream.get(1).ok_or("missing completed entry")?;
    let entry = DedupEntry::deserialize(&completed.payload)?;
    assert_eq!(entry.receipt(), Some(&[4, 5, 6][..]));
    assert_eq!(entry.timestamp_millis(), 10_000);
    Ok(())
}

#[test]
fn completing_completed_key_is_idempotent_only_for_matching_receipt() -> Result<(), Box<dyn Error>>
{
    let store = Arc::new(FakeStore::default());
    let cache = DedupCache::new(store.clone(), "dedup");

    assert_eq!(
        block_on(cache.claim_or_get("key", 1_000))?,
        DedupDecision::Claimed
    );
    block_on(cache.complete_receipt_at("key", ProcessingReceipt::new(vec![1]), 2_000))?;
    let append_count = store.append_count()?;

    block_on(cache.complete_receipt_at("key", ProcessingReceipt::new(vec![1]), 3_000))?;
    let mismatched =
        block_on(cache.complete_receipt_at("key", ProcessingReceipt::new(vec![2]), 4_000));

    assert_eq!(store.append_count()?, append_count);
    assert!(matches!(
        mismatched,
        Err(DurabilityError::DedupCollision { .. })
    ));
    assert_eq!(store.append_count()?, append_count);
    assert_eq!(
        block_on(cache.claim_or_get("key", 5_000))?,
        DedupDecision::Completed(ProcessingReceipt::new(vec![1]))
    );
    Ok(())
}

#[test]
fn completing_missing_key_returns_collision() {
    let store = Arc::new(FakeStore::default());
    let cache = DedupCache::new(store, "dedup");

    let result = block_on(cache.complete_receipt("missing", ProcessingReceipt::new(vec![1])));

    assert!(matches!(
        result,
        Err(DurabilityError::DedupCollision { .. })
    ));
}

#[test]
fn sweep_uses_scan_and_respects_ttl_grace() -> Result<(), Box<dyn Error>> {
    let store = Arc::new(FakeStore::default());
    let cache = DedupCache::new(store.clone(), "dedup");
    let sweeper = DedupSweeper::new(
        cache.clone(),
        Duration::from_millis(60_000),
        Duration::from_millis(5_000),
    );
    assert_eq!(sweeper.sweep_interval(), Duration::from_millis(5_000));
    assert_eq!(
        block_on(cache.claim_or_get("key", 1_000))?,
        DedupDecision::Claimed
    );

    let first_report = block_on(sweeper.sweep_once(60_999))?;
    assert_eq!(first_report.expired_entries(), 0);
    assert_eq!(
        block_on(cache.lookup("key"))?,
        Some(DedupDecision::InFlight)
    );

    let second_report = block_on(sweeper.sweep_once(62_001))?;
    assert_eq!(second_report.expired_entries(), 1);
    assert_eq!(block_on(cache.lookup("key"))?, None);
    assert_eq!(
        store.scans()?,
        vec![String::from("dedup:"), String::from("dedup:")]
    );
    Ok(())
}

#[test]
fn sweep_keeps_receipt_younger_than_ttl_even_when_claim_is_old() -> Result<(), Box<dyn Error>> {
    let store = Arc::new(FakeStore::default());
    let cache = DedupCache::new(store, "dedup");
    let sweeper = DedupSweeper::new(
        cache.clone(),
        Duration::from_millis(60_000),
        Duration::from_millis(5_000),
    );

    assert_eq!(
        block_on(cache.claim_or_get("key", 1_000))?,
        DedupDecision::Claimed
    );
    block_on(cache.complete_receipt_at("key", ProcessingReceipt::new(vec![9]), 62_000))?;

    let young_report = block_on(sweeper.sweep_once(122_999))?;
    assert_eq!(young_report.expired_entries(), 0);
    assert_eq!(
        block_on(cache.lookup("key"))?,
        Some(DedupDecision::Completed(ProcessingReceipt::new(vec![9])))
    );

    let expired_report = block_on(sweeper.sweep_once(123_001))?;
    assert_eq!(expired_report.expired_entries(), 1);
    assert_eq!(block_on(cache.lookup("key"))?, None);
    Ok(())
}

#[derive(Debug, Default)]
struct FakeStore {
    streams: Mutex<HashMap<String, Vec<StoredEntry>>>,
    append_count: Mutex<usize>,
    scans: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl DurableStore for FakeStore {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        let actual = {
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
            actual
        };
        *self.append_count.lock().map_err(|_| lock_error())? += 1;
        Ok(actual)
    }

    async fn read_from(
        &self,
        stream_key: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, DurabilityError> {
        let start = usize::try_from(offset).map_err(|error| {
            DurabilityError::ConfigError(format!("test offset cannot fit usize: {error}"))
        })?;
        let entries = {
            let streams = self.streams.lock().map_err(|_| lock_error())?;
            let entries = streams.get(stream_key).map_or_else(Vec::new, |stream| {
                stream.iter().skip(start).take(limit).cloned().collect()
            });
            drop(streams);
            entries
        };
        Ok(entries)
    }

    async fn cas(&self, _: &str, _: u64, _: u64) -> Result<(), DurabilityError> {
        Ok(())
    }

    async fn read_value(&self, _: &str) -> Result<Option<u64>, DurabilityError> {
        Ok(None)
    }

    async fn scan(&self, prefix: &str) -> Result<Vec<StoredEntry>, DurabilityError> {
        self.scans
            .lock()
            .map_err(|_| lock_error())?
            .push(prefix.to_owned());
        let streams = self.streams.lock().map_err(|_| lock_error())?;
        Ok(streams
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .flat_map(|(_, entries)| entries.clone())
            .collect())
    }
}

impl FakeStore {
    fn append_count(&self) -> Result<usize, DurabilityError> {
        self.append_count
            .lock()
            .map(|guard| *guard)
            .map_err(|_| lock_error())
    }

    fn stream(&self, stream_key: &str) -> Result<Vec<StoredEntry>, DurabilityError> {
        self.streams
            .lock()
            .map_err(|_| lock_error())
            .map(|streams| streams.get(stream_key).cloned().unwrap_or_default())
    }

    fn scans(&self) -> Result<Vec<String>, DurabilityError> {
        self.scans
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
