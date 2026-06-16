use std::fmt;
use std::sync::Arc;

use super::MessageEnvelope;
use crate::durability::{DurabilityConfig, DurabilityError, DurabilityMode, DurableStore};

/// Caller-provided function used to route envelopes to partitions.
#[derive(Clone)]
pub struct PartitionKey {
    function: Arc<dyn Fn(&MessageEnvelope) -> u64 + Send + Sync + 'static>,
}

impl PartitionKey {
    /// Wraps a partition key closure.
    #[must_use]
    pub fn new<F>(function: F) -> Self
    where
        F: Fn(&MessageEnvelope) -> u64 + Send + Sync + 'static,
    {
        Self {
            function: Arc::new(function),
        }
    }

    fn apply(&self, envelope: &MessageEnvelope) -> u64 {
        (self.function)(envelope)
    }
}

impl fmt::Debug for PartitionKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PartitionKey(<closure>)")
    }
}

/// Haematite-backed durable channel state.
#[derive(Clone)]
pub struct DurableChannel {
    channel_id: String,
    partition_count: usize,
    partition_key: Option<PartitionKey>,
    next_sequences: Vec<u64>,
    store: Arc<dyn DurableStore>,
}

impl DurableChannel {
    /// Creates a new durable channel with all partition sequences initialized to zero.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError::ConfigError`] when `partition_count` is zero.
    pub fn new(
        channel_id: impl Into<String>,
        partition_count: usize,
        store: Arc<dyn DurableStore>,
    ) -> Result<Self, DurabilityError> {
        Self::from_parts(channel_id.into(), partition_count, store, None, None)
    }

    /// Creates a durable channel with a caller-provided partition key function.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError::ConfigError`] when `partition_count` is zero.
    pub fn with_partition_key<F>(
        channel_id: impl Into<String>,
        partition_count: usize,
        store: Arc<dyn DurableStore>,
        partition_key: F,
    ) -> Result<Self, DurabilityError>
    where
        F: Fn(&MessageEnvelope) -> u64 + Send + Sync + 'static,
    {
        Self::from_parts(
            channel_id.into(),
            partition_count,
            store,
            Some(PartitionKey::new(partition_key)),
            None,
        )
    }

    /// Creates a durable channel from explicit recovered next sequence counters.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError::ConfigError`] when `partition_count` is zero
    /// or the recovered sequence vector length differs from `partition_count`.
    pub fn from_recovered_sequences(
        channel_id: impl Into<String>,
        partition_count: usize,
        store: Arc<dyn DurableStore>,
        next_sequences: Vec<u64>,
    ) -> Result<Self, DurabilityError> {
        Self::from_parts(
            channel_id.into(),
            partition_count,
            store,
            None,
            Some(next_sequences),
        )
    }

    /// Creates a durable channel from validated durability configuration.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError::ConfigError`] when `config` is ephemeral.
    pub fn from_config(
        channel_id: impl Into<String>,
        config: DurabilityConfig,
        store: Arc<dyn DurableStore>,
    ) -> Result<Self, DurabilityError> {
        if config.mode() == DurabilityMode::Ephemeral {
            return Err(DurabilityError::ConfigError(
                "durable channel requires a durable durability mode".to_owned(),
            ));
        }
        Self::new(channel_id, config.partition_count(), store)
    }

    /// Returns the durable channel identifier.
    #[must_use]
    pub fn channel_id(&self) -> &str {
        &self.channel_id
    }

    /// Returns the number of partitions owned by this channel.
    #[must_use]
    pub const fn partition_count(&self) -> usize {
        self.partition_count
    }

    /// Returns the next expected sequence for a partition, if it exists.
    #[must_use]
    pub fn next_expected_sequence(&self, partition_index: usize) -> Option<u64> {
        self.next_sequences.get(partition_index).copied()
    }

    /// Returns all next expected sequence counters.
    #[must_use]
    pub fn next_sequences(&self) -> &[u64] {
        &self.next_sequences
    }

    /// Computes the partition index for an envelope without touching storage.
    #[must_use]
    pub fn partition_for(&self, envelope: &MessageEnvelope) -> usize {
        route_partition(self.partition_count, self.partition_key.as_ref(), envelope)
    }

    /// Formats the haematite stream key for a channel partition.
    #[must_use]
    pub fn stream_key_for(&self, partition_index: usize) -> String {
        format!("{}:{partition_index}", self.channel_id)
    }

    /// Persists an envelope before acknowledging the publish.
    ///
    /// # Errors
    ///
    /// Returns envelope serialization errors and propagates any
    /// [`DurabilityError`] returned by [`DurableStore::append`], including
    /// [`DurabilityError::SequenceConflict`].
    pub async fn publish(&mut self, envelope: &MessageEnvelope) -> Result<u64, DurabilityError> {
        let payload = envelope.serialize()?;
        let partition_index = self.partition_for(envelope);
        let expected_seq = self.sequence_for_append(partition_index)?;
        let stream_key = self.stream_key_for(partition_index);
        let assigned_seq = self
            .store
            .append(&stream_key, payload, expected_seq)
            .await?;
        let next_seq = assigned_seq.checked_add(1).ok_or_else(|| {
            DurabilityError::ConfigError("sequence number overflow after append".to_owned())
        })?;
        self.set_next_sequence(partition_index, next_seq)?;
        Ok(assigned_seq)
    }

    fn from_parts(
        channel_id: String,
        partition_count: usize,
        store: Arc<dyn DurableStore>,
        partition_key: Option<PartitionKey>,
        next_sequences: Option<Vec<u64>>,
    ) -> Result<Self, DurabilityError> {
        validate_partition_count(partition_count)?;
        let next_sequences = next_sequences.unwrap_or_else(|| vec![0; partition_count]);
        if next_sequences.len() != partition_count {
            return Err(DurabilityError::ConfigError(
                "recovered sequence count must match partition_count".to_owned(),
            ));
        }
        Ok(Self {
            channel_id,
            partition_count,
            partition_key,
            next_sequences,
            store,
        })
    }

    fn sequence_for_append(&self, partition_index: usize) -> Result<u64, DurabilityError> {
        self.next_sequences
            .get(partition_index)
            .copied()
            .ok_or_else(|| {
                DurabilityError::ConfigError("partition sequence state missing".to_owned())
            })
    }

    fn set_next_sequence(
        &mut self,
        partition_index: usize,
        next_sequence: u64,
    ) -> Result<(), DurabilityError> {
        let Some(sequence) = self.next_sequences.get_mut(partition_index) else {
            return Err(DurabilityError::ConfigError(
                "partition sequence state missing".to_owned(),
            ));
        };
        *sequence = next_sequence;
        Ok(())
    }
}

impl fmt::Debug for DurableChannel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DurableChannel")
            .field("channel_id", &self.channel_id)
            .field("partition_count", &self.partition_count)
            .field("partition_key_configured", &self.partition_key.is_some())
            .field("next_sequences", &self.next_sequences)
            .field("store", &self.store)
            .finish()
    }
}

/// Ephemeral channel state with no durable store reference.
#[derive(Clone)]
pub struct EphemeralChannel {
    channel_id: String,
    partition_count: usize,
    partition_key: Option<PartitionKey>,
}

impl EphemeralChannel {
    /// Creates an ephemeral channel without requiring any durable store.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError::ConfigError`] when `partition_count` is zero.
    pub fn new(
        channel_id: impl Into<String>,
        partition_count: usize,
    ) -> Result<Self, DurabilityError> {
        Self::from_parts(channel_id.into(), partition_count, None)
    }

    /// Creates an ephemeral channel with a caller-provided partition key function.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError::ConfigError`] when `partition_count` is zero.
    pub fn with_partition_key<F>(
        channel_id: impl Into<String>,
        partition_count: usize,
        partition_key: F,
    ) -> Result<Self, DurabilityError>
    where
        F: Fn(&MessageEnvelope) -> u64 + Send + Sync + 'static,
    {
        Self::from_parts(
            channel_id.into(),
            partition_count,
            Some(PartitionKey::new(partition_key)),
        )
    }

    /// Creates an ephemeral channel from validated durability configuration.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError::ConfigError`] when `config` is not ephemeral.
    pub fn from_config(
        channel_id: impl Into<String>,
        config: DurabilityConfig,
    ) -> Result<Self, DurabilityError> {
        if config.mode() != DurabilityMode::Ephemeral {
            return Err(DurabilityError::ConfigError(
                "ephemeral channel requires Ephemeral durability mode".to_owned(),
            ));
        }
        Self::new(channel_id, config.partition_count())
    }

    /// Returns the channel identifier.
    #[must_use]
    pub fn channel_id(&self) -> &str {
        &self.channel_id
    }

    /// Returns the number of configured partitions.
    #[must_use]
    pub const fn partition_count(&self) -> usize {
        self.partition_count
    }

    /// Computes the partition index for an envelope without touching storage.
    #[must_use]
    pub fn partition_for(&self, envelope: &MessageEnvelope) -> usize {
        route_partition(self.partition_count, self.partition_key.as_ref(), envelope)
    }

    fn from_parts(
        channel_id: String,
        partition_count: usize,
        partition_key: Option<PartitionKey>,
    ) -> Result<Self, DurabilityError> {
        validate_partition_count(partition_count)?;
        Ok(Self {
            channel_id,
            partition_count,
            partition_key,
        })
    }
}

impl fmt::Debug for EphemeralChannel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EphemeralChannel")
            .field("channel_id", &self.channel_id)
            .field("partition_count", &self.partition_count)
            .field("partition_key_configured", &self.partition_key.is_some())
            .finish()
    }
}

fn route_partition(
    partition_count: usize,
    partition_key: Option<&PartitionKey>,
    envelope: &MessageEnvelope,
) -> usize {
    if partition_count == 1 {
        return 0;
    }

    let Some(partition_key) = partition_key else {
        return 0;
    };

    let Ok(partition_count_u64) = u64::try_from(partition_count) else {
        return 0;
    };
    let routed = partition_key.apply(envelope) % partition_count_u64;
    usize::try_from(routed).unwrap_or_else(|_| partition_count.saturating_sub(1))
}

fn validate_partition_count(partition_count: usize) -> Result<(), DurabilityError> {
    if partition_count == 0 {
        return Err(DurabilityError::ConfigError(
            "partition_count must be at least 1".to_owned(),
        ));
    }
    Ok(())
}
