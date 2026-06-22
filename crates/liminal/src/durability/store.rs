use std::sync::Arc;

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
    async fn cas(&self, key: &str, old_value: u64, new_value: u64) -> Result<(), DurabilityError>;

    /// Reads a numeric value previously updated through compare-and-swap.
    async fn read_value(&self, key: &str) -> Result<Option<u64>, DurabilityError>;

    /// Scans entries by store prefix.
    async fn scan(&self, prefix: &str) -> Result<Vec<StoredEntry>, DurabilityError>;
}

/// `DurableStore` implementation that delegates directly to haematite's `EventStore`.
#[derive(Clone, Debug)]
pub struct HaematiteStore {
    event_store: Arc<haematite::EventStore>,
}

impl HaematiteStore {
    /// Wraps a haematite `EventStore` handle.
    #[must_use]
    pub const fn new(event_store: Arc<haematite::EventStore>) -> Self {
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
        self.event_store
            .append(stream_key, payload, expected_seq)
            .await
            .map_err(map_store_error)
    }

    async fn read_from(
        &self,
        stream_key: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, DurabilityError> {
        let entries = self
            .event_store
            .read_from(stream_key, offset, limit)
            .await
            .map_err(map_store_error)?;

        Ok(entries.into_iter().map(StoredEntry::from).collect())
    }

    async fn cas(&self, key: &str, old_value: u64, new_value: u64) -> Result<(), DurabilityError> {
        self.event_store
            .cas(key, old_value, new_value)
            .await
            .map_err(map_store_error)
    }

    async fn read_value(&self, key: &str) -> Result<Option<u64>, DurabilityError> {
        self.event_store
            .read_value(key)
            .await
            .map_err(map_store_error)
    }

    async fn scan(&self, prefix: &str) -> Result<Vec<StoredEntry>, DurabilityError> {
        let entries = self
            .event_store
            .scan(prefix)
            .await
            .map_err(map_store_error)?;

        Ok(entries.into_iter().map(StoredEntry::from).collect())
    }
}

impl From<haematite::Event> for StoredEntry {
    fn from(entry: haematite::Event) -> Self {
        Self {
            payload: entry.payload,
            sequence: entry.sequence,
            timestamp: entry.timestamp,
        }
    }
}

fn map_store_error(error: haematite::EventStoreError) -> DurabilityError {
    match error {
        haematite::EventStoreError::SequenceConflict(conflict) => conflict.into(),
        haematite::EventStoreError::CasMismatch(mismatch) => mismatch.into(),
        store_error @ haematite::EventStoreError::StoreIo(_) => {
            DurabilityError::StoreError(store_error)
        }
    }
}
