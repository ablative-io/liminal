use std::collections::BTreeMap;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use super::{CasMismatch, Event, EventStoreError, SequenceConflict};

/// Haematite event-store handle exposing append/read/read-from/scan/cas operations.
#[derive(Debug, Default)]
pub struct EventStore {
    streams: RwLock<BTreeMap<String, Vec<Event>>>,
    values: RwLock<BTreeMap<String, u64>>,
}

impl EventStore {
    /// Creates an empty event-store handle.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends one payload to a stream using optimistic concurrency.
    ///
    /// # Errors
    ///
    /// Returns [`EventStoreError::SequenceConflict`] when `expected_seq` differs
    /// from the stream's next sequence number, or a store error if the lock is poisoned.
    pub async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, EventStoreError> {
        let mut streams = self.streams.write().map_err(lock_error)?;
        let stream = streams.entry(stream_key.to_owned()).or_default();
        let actual = u64::try_from(stream.len()).map_err(length_error)?;

        if expected_seq != actual {
            return Err(SequenceConflict {
                expected: expected_seq,
                actual,
            }
            .into());
        }

        let event = Event::new(payload, actual, now_millis()?);
        stream.push(event);
        Ok(actual)
    }

    /// Reads events from `offset`, returning at most `limit` entries.
    ///
    /// # Errors
    ///
    /// Returns a store error if the lock is poisoned.
    pub async fn read_from(
        &self,
        stream_key: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<Event>, EventStoreError> {
        let streams = self.streams.read().map_err(lock_error)?;
        let Some(stream) = streams.get(stream_key) else {
            return Ok(Vec::new());
        };
        let start = usize::try_from(offset).map_err(offset_error)?;

        Ok(stream.iter().skip(start).take(limit).cloned().collect())
    }

    /// Atomically replaces the stored value for `key` if it equals `old_value`.
    ///
    /// # Errors
    ///
    /// Returns [`EventStoreError::CasMismatch`] when the stored value differs,
    /// or a store error if the lock is poisoned.
    pub async fn cas(
        &self,
        key: &str,
        old_value: u64,
        new_value: u64,
    ) -> Result<(), EventStoreError> {
        let mut values = self.values.write().map_err(lock_error)?;
        let actual = values.get(key).copied().unwrap_or(0);

        if actual != old_value {
            return Err(CasMismatch {
                expected: old_value,
                actual,
            }
            .into());
        }

        values.insert(key.to_owned(), new_value);
        Ok(())
    }

    /// Reads a numeric value previously updated through compare-and-swap.
    ///
    /// # Errors
    ///
    /// Returns a store error if the lock is poisoned.
    pub async fn read_value(&self, key: &str) -> Result<Option<u64>, EventStoreError> {
        let values = self.values.read().map_err(lock_error)?;
        Ok(values.get(key).copied())
    }

    /// Scans event streams whose stream keys begin with `prefix`.
    ///
    /// # Errors
    ///
    /// Returns a store error if the lock is poisoned.
    pub async fn scan(&self, prefix: &str) -> Result<Vec<Event>, EventStoreError> {
        let streams = self.streams.read().map_err(lock_error)?;

        Ok(streams
            .iter()
            .filter(|(stream_key, _)| stream_key.starts_with(prefix))
            .flat_map(|(_, events)| events.iter().cloned())
            .collect())
    }

    /// Flushes buffered event-store state to durable storage.
    ///
    /// # Errors
    ///
    /// Returns a store error if the backend cannot prove pending writes are durable.
    ///
    /// The current haematite backend is in-memory and every write is committed
    /// under the store lock before its operation returns, so there is no extra
    /// buffer to drain. File-backed backends must perform their fsync-equivalent
    /// work here before returning.
    pub async fn flush(&self) -> Result<(), EventStoreError> {
        Ok(())
    }
}

fn lock_error<T>(_: std::sync::PoisonError<T>) -> EventStoreError {
    std::io::Error::other("haematite event-store lock poisoned").into()
}

fn length_error(_: std::num::TryFromIntError) -> EventStoreError {
    std::io::Error::other("event stream length exceeds u64::MAX").into()
}

fn offset_error(_: std::num::TryFromIntError) -> EventStoreError {
    std::io::Error::other("event stream offset exceeds usize::MAX").into()
}

fn now_millis() -> Result<u64, EventStoreError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    u64::try_from(duration.as_millis())
        .map_err(|_| std::io::Error::other("timestamp milliseconds exceed u64::MAX").into())
}
