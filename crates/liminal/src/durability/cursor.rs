#![allow(clippy::module_name_repetitions)]

use super::{DurabilityError, DurableStore};

/// Partition-specific durable read position for one consumer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerCursor {
    consumer_id: String,
    partition_key: String,
    current_offset: u64,
}

impl ConsumerCursor {
    /// Creates a fresh cursor at offset zero.
    #[must_use]
    pub fn new(consumer_id: impl Into<String>, partition_key: impl Into<String>) -> Self {
        Self::from_persisted(consumer_id, partition_key, 0)
    }

    /// Creates a cursor from an offset read from durable storage.
    #[must_use]
    pub fn from_persisted(
        consumer_id: impl Into<String>,
        partition_key: impl Into<String>,
        current_offset: u64,
    ) -> Self {
        Self {
            consumer_id: consumer_id.into(),
            partition_key: partition_key.into(),
            current_offset,
        }
    }

    /// Resumes a cursor by reading the CAS-backed offset value.
    ///
    /// # Errors
    ///
    /// Propagates store read errors from [`DurableStore::read_value`].
    pub async fn resume(
        consumer_id: impl Into<String>,
        partition_key: impl Into<String>,
        store: &dyn DurableStore,
    ) -> Result<Self, DurabilityError> {
        let consumer_id = consumer_id.into();
        let partition_key = partition_key.into();
        let cursor_key = cursor_key_for(&consumer_id, &partition_key);
        let current_offset = store.read_value(&cursor_key).await?.unwrap_or(0);
        Ok(Self::from_persisted(
            consumer_id,
            partition_key,
            current_offset,
        ))
    }

    /// Returns the stable consumer identifier.
    #[must_use]
    pub fn consumer_id(&self) -> &str {
        &self.consumer_id
    }

    /// Returns the durable channel partition key, formatted as `channel_id:partition_index`.
    #[must_use]
    pub fn partition_key(&self) -> &str {
        &self.partition_key
    }

    /// Returns the current persisted read offset.
    #[must_use]
    pub const fn current_offset(&self) -> u64 {
        self.current_offset
    }

    /// Returns the haematite CAS key used for this cursor.
    #[must_use]
    pub fn cursor_key(&self) -> String {
        cursor_key_for(&self.consumer_id, &self.partition_key)
    }

    /// Persists a new cursor position with compare-and-swap.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError::CursorRegression`] when `new_offset` is lower than this
    /// cursor's current offset, and propagates store CAS errors including stale checkpoints.
    pub async fn checkpoint(
        &mut self,
        store: &dyn DurableStore,
        new_offset: u64,
    ) -> Result<(), DurabilityError> {
        if new_offset < self.current_offset {
            return Err(DurabilityError::CursorRegression {
                stored: self.current_offset,
                attempted: new_offset,
            });
        }

        store
            .cas(&self.cursor_key(), self.current_offset, new_offset)
            .await?;
        self.current_offset = new_offset;
        Ok(())
    }
}

/// Formats the haematite key used for cursor compare-and-swap state.
#[must_use]
pub fn cursor_key_for(consumer_id: &str, partition_key: &str) -> String {
    format!("{consumer_id}:{partition_key}")
}
