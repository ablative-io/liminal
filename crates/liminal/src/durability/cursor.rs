use super::{CheckpointPolicy, DurabilityConfig, DurabilityError, DurableStore};

/// Partition-specific durable read position for one consumer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerCursor {
    /// Stable consumer identifier.
    pub consumer_id: String,
    /// Durable channel partition key, formatted as `channel_id:partition_index`.
    pub partition_key: String,
    /// Current persisted read offset for this consumer and partition.
    pub current_offset: u64,
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

/// Drives cursor checkpoints according to the channel's configured policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointDriver {
    policy: CheckpointPolicy,
    messages_since_last_checkpoint: usize,
    pending_offset: Option<u64>,
}

impl CheckpointDriver {
    /// Creates a checkpoint driver from a checkpoint policy or full durability config.
    #[must_use]
    pub fn new(policy: impl Into<CheckpointPolicy>) -> Self {
        Self::from_policy(policy.into())
    }

    /// Creates a checkpoint driver from an explicit checkpoint policy.
    #[must_use]
    pub const fn from_policy(policy: CheckpointPolicy) -> Self {
        Self {
            policy,
            messages_since_last_checkpoint: 0,
            pending_offset: None,
        }
    }

    /// Creates a checkpoint driver from a durability configuration.
    #[must_use]
    pub const fn from_config(config: DurabilityConfig) -> Self {
        Self::from_policy(config.checkpoint_policy())
    }

    /// Returns the active checkpoint policy.
    #[must_use]
    pub const fn policy(&self) -> CheckpointPolicy {
        self.policy
    }

    /// Returns the number of processed messages since the last successful checkpoint.
    #[must_use]
    pub const fn messages_since_last_checkpoint(&self) -> usize {
        self.messages_since_last_checkpoint
    }

    /// Returns the latest processed offset waiting to be checkpointed.
    #[must_use]
    pub const fn pending_offset(&self) -> Option<u64> {
        self.pending_offset
    }

    /// Records one processed message and checkpoints if the configured policy requires it.
    ///
    /// `next_offset` is the partition offset from which the consumer should resume after the
    /// processed message. The driver delegates all persistence to [`ConsumerCursor::checkpoint`].
    ///
    /// # Errors
    ///
    /// Returns cursor checkpoint errors from the store, or a configuration error for an invalid
    /// raw batch policy.
    pub async fn record_processed(
        &mut self,
        cursor: &mut ConsumerCursor,
        store: &dyn DurableStore,
        next_offset: u64,
    ) -> Result<(), DurabilityError> {
        self.validate_policy()?;
        self.messages_since_last_checkpoint = self
            .messages_since_last_checkpoint
            .checked_add(1)
            .ok_or_else(|| {
                DurabilityError::ConfigError("messages since last checkpoint overflow".to_owned())
            })?;
        self.pending_offset = Some(next_offset);

        match self.policy {
            CheckpointPolicy::PerMessage => self.checkpoint_pending(cursor, store).await,
            CheckpointPolicy::PerBatch(batch_size)
                if self.messages_since_last_checkpoint >= batch_size =>
            {
                self.checkpoint_pending(cursor, store).await
            }
            CheckpointPolicy::PerBatch(_) | CheckpointPolicy::ExplicitFlush => Ok(()),
        }
    }

    /// Flushes any processed offset that is waiting to be checkpointed.
    ///
    /// # Errors
    ///
    /// Returns cursor checkpoint errors from the store, or a configuration error for an invalid
    /// raw batch policy.
    pub async fn flush(
        &mut self,
        cursor: &mut ConsumerCursor,
        store: &dyn DurableStore,
    ) -> Result<(), DurabilityError> {
        self.validate_policy()?;
        self.checkpoint_pending(cursor, store).await
    }

    async fn checkpoint_pending(
        &mut self,
        cursor: &mut ConsumerCursor,
        store: &dyn DurableStore,
    ) -> Result<(), DurabilityError> {
        let Some(next_offset) = self.pending_offset else {
            return Ok(());
        };
        cursor.checkpoint(store, next_offset).await?;
        self.messages_since_last_checkpoint = 0;
        self.pending_offset = None;
        Ok(())
    }

    fn validate_policy(&self) -> Result<(), DurabilityError> {
        if self.policy == CheckpointPolicy::PerBatch(0) {
            return Err(DurabilityError::ConfigError(
                "checkpoint batch size must be at least 1".to_owned(),
            ));
        }
        Ok(())
    }
}

impl From<DurabilityConfig> for CheckpointPolicy {
    fn from(config: DurabilityConfig) -> Self {
        config.checkpoint_policy()
    }
}

/// Formats the haematite key used for cursor compare-and-swap state.
#[must_use]
pub fn cursor_key_for(consumer_id: &str, partition_key: &str) -> String {
    format!("{consumer_id}:{partition_key}")
}

#[cfg(test)]
mod tests;
