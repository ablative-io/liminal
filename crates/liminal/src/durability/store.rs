use std::sync::Arc;

use haematite::{ApiError, Event, EventStore};

use super::DurabilityError;

/// Entry read from a durable haematite stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredEntry {
    /// Opaque stored payload bytes.
    pub payload: Vec<u8>,
    /// Sequence number assigned by the stream.
    pub sequence: u64,
    /// Store timestamp associated with the entry.
    pub timestamp: u64,
}

/// Direct durability surface matching haematite's append/read/cas/scan API.
#[async_trait::async_trait]
pub trait DurableStore: std::fmt::Debug + Send + Sync {
    /// Appends `payload` to `stream_key` if `expected_seq` matches the stream head.
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError>;

    /// Reads entries from `stream_key` beginning at `offset`, up to `limit` entries.
    async fn read_from(
        &self,
        stream_key: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, DurabilityError>;

    /// Atomically replaces a stored numeric value if it equals `old_value`.
    ///
    /// An `old_value` of `0` matches a key that is currently *absent* as well as
    /// one explicitly stored as `0`: a fresh cursor is created on its first
    /// checkpoint without a prior write. See [`HaematiteStore::cas`] for how this
    /// "absent == 0" contract is preserved atomically over the real engine.
    async fn cas(&self, key: &str, old_value: u64, new_value: u64) -> Result<(), DurabilityError>;

    /// Reads a numeric value previously updated through compare-and-swap.
    async fn read_value(&self, key: &str) -> Result<Option<u64>, DurabilityError>;

    /// Scans entries by store prefix.
    async fn scan(&self, prefix: &str) -> Result<Vec<StoredEntry>, DurabilityError>;

    /// Flushes buffered writes so completed durable operations are persisted.
    ///
    /// # Errors
    /// Returns [`DurabilityError`] when the underlying store cannot complete the flush.
    async fn flush(&self) -> Result<(), DurabilityError>;
}

/// `DurableStore` implementation that delegates directly to haematite's `EventStore`.
///
/// The real [`EventStore`] is synchronous (every call blocks on the owning
/// shard actor's reply), so each `async` method below completes on its first
/// poll. The synchronous bridge in [`super::bridge`] relies on exactly that.
#[derive(Clone, Debug)]
pub struct HaematiteStore {
    event_store: Arc<EventStore>,
}

impl HaematiteStore {
    /// Wraps a haematite `EventStore` handle.
    #[must_use]
    pub const fn new(event_store: Arc<EventStore>) -> Self {
        Self { event_store }
    }
}

#[async_trait::async_trait]
impl DurableStore for HaematiteStore {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        // Contract bridge: liminal's `DurableStore::append` returns the *assigned
        // event sequence* (0-based position of the just-appended event), which is
        // exactly `expected_seq` for a single append. The real `EventStore::append`
        // instead returns the stream's new next-sequence (`expected_seq + 1`), so
        // subtract one to recover the assigned seq. A `0` next-seq is impossible
        // after a successful single append, so the `checked_sub` cannot saturate
        // silently; if it ever did the engine returned a contract-violating value.
        let next_seq = self
            .event_store
            .append(stream_key.as_bytes(), &payload, expected_seq)
            .map_err(DurabilityError::from)?;
        next_seq.checked_sub(1).ok_or_else(|| {
            DurabilityError::StoreError(ApiError::CorruptEvent(format!(
                "append returned next-seq 0 for stream {stream_key}"
            )))
        })
    }

    async fn read_from(
        &self,
        stream_key: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, DurabilityError> {
        // The real `read_from` returns every event with seq >= offset and applies
        // no limit; truncate to `limit` entries to honour the trait contract.
        let mut events = self
            .event_store
            .read_from(stream_key.as_bytes(), offset)
            .map_err(DurabilityError::from)?;
        events.truncate(limit);
        Ok(events.into_iter().map(StoredEntry::from).collect())
    }

    async fn cas(&self, key: &str, old_value: u64, new_value: u64) -> Result<(), DurabilityError> {
        // Preserve liminal's "absent == 0" cursor contract faithfully over an
        // engine that distinguishes `None` (absent) from `Some(0)` (a stored
        // zero). The invariant that makes the mapping below correct: we NEVER
        // persist a physical zero, so a logical value of 0 and physical absence
        // always coincide.
        //
        // A `cas` whose target `new_value` is 0 must therefore write nothing — it
        // only asserts the precondition. This is reachable as `cas(0, 0)` (a
        // cursor checkpoint at offset 0; offsets are monotonic so they never CAS
        // down to 0 from a higher value). Were we instead to let it store a
        // physical zero, the *next* `cas(0, n)` — mapped to expect-absent `None`
        // — would wrongly fail against the now-present key and permanently stall
        // the cursor. Asserting via a read is race-free here precisely because no
        // value is written, so there is no lost-update window.
        if new_value == 0 {
            return self
                .event_store
                .read_value(key.as_bytes())
                .map_err(DurabilityError::from)?
                .map_or(Ok(()), |stored| {
                    Err(DurabilityError::CursorRegression {
                        stored,
                        attempted: old_value,
                    })
                });
        }
        // With a physical zero never stored, `old_value == 0` is exactly the
        // expect-absent expectation. Any other `old_value` maps to `Some(_)`.
        // This is a single CAS routed to the owning shard actor, where read,
        // compare, and write run with no interleaving point (haematite's
        // `ShardActor::cas`) — the engine's atomicity is preserved end to end.
        let expected = if old_value == 0 {
            None
        } else {
            Some(old_value)
        };
        self.event_store
            .cas(key.as_bytes(), expected, new_value)
            .map_err(DurabilityError::from)
    }

    async fn read_value(&self, key: &str) -> Result<Option<u64>, DurabilityError> {
        self.event_store
            .read_value(key.as_bytes())
            .map_err(DurabilityError::from)
    }

    async fn scan(&self, prefix: &str) -> Result<Vec<StoredEntry>, DurabilityError> {
        // The real `scan` predicate yields stream *metadata* (key + next_seq),
        // not events. Liminal's contract is to return the events of every stream
        // whose key matches `prefix`, so collect the matching stream keys, then
        // read each stream's full event list and flatten the results.
        let prefix_bytes = prefix.as_bytes().to_vec();
        let matches = self
            .event_store
            .scan(|meta| meta.stream_key.starts_with(&prefix_bytes))
            .map_err(DurabilityError::from)?;
        let mut entries = Vec::new();
        for stream in matches {
            let events = self
                .event_store
                .read(&stream.stream_key)
                .map_err(DurabilityError::from)?;
            entries.extend(events.into_iter().map(StoredEntry::from));
        }
        Ok(entries)
    }

    async fn flush(&self) -> Result<(), DurabilityError> {
        self.event_store.flush().map_err(DurabilityError::from)
    }
}

impl From<Event> for StoredEntry {
    fn from(event: Event) -> Self {
        Self {
            payload: event.payload,
            sequence: event.seq,
            timestamp: event.timestamp,
        }
    }
}

/// Maps a real-engine [`ApiError`] onto liminal's [`DurabilityError`].
///
/// The optimistic-concurrency variants route to their dedicated `DurabilityError`
/// cases (`SequenceConflict`, `CursorRegression`); everything else is a
/// store-level failure carried verbatim.
impl From<ApiError> for DurabilityError {
    fn from(error: ApiError) -> Self {
        match error {
            ApiError::SequenceConflict(conflict) => conflict.into(),
            ApiError::CasMismatch(mismatch) => mismatch.into(),
            other @ (ApiError::CorruptEvent(_)
            | ApiError::Storage(_)
            | ApiError::HistoryCompacted(_)) => Self::StoreError(other),
        }
    }
}
