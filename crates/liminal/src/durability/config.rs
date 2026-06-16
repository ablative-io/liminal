use std::time::Duration;

use super::DurabilityError;

/// Per-channel durability strategy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DurabilityMode {
    /// No persistence or durability bookkeeping.
    Ephemeral,
    /// Persist messages for replay and crash recovery.
    Durable,
    /// Persist messages and apply the lightweight dedup contract.
    DurableDedup,
    /// Persist resumable processing state for durable conversations.
    DurableConversation,
}

/// Policy controlling when consumer progress is checkpointed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckpointPolicy {
    /// Checkpoint after every processed message.
    PerMessage,
    /// Checkpoint after processing batches of the configured size.
    PerBatch(usize),
    /// Checkpoint only when the caller explicitly flushes progress.
    ExplicitFlush,
}

/// Explicit durability configuration supplied per channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DurabilityConfig {
    /// Channel durability strategy.
    mode: DurabilityMode,
    /// Number of independent durable partitions.
    partition_count: usize,
    /// Time-to-live for dedup receipts.
    dedup_ttl: Duration,
    /// Consumer checkpoint policy.
    checkpoint_policy: CheckpointPolicy,
}

impl DurabilityConfig {
    /// Creates validated durability configuration from caller-supplied fields.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError::ConfigError`] when `partition_count` is zero,
    /// when `mode` is [`DurabilityMode::DurableDedup`] and `dedup_ttl` is zero,
    /// or when `checkpoint_policy` is [`CheckpointPolicy::PerBatch`] with a zero batch size.
    pub fn new(
        mode: DurabilityMode,
        partition_count: usize,
        dedup_ttl: Duration,
        checkpoint_policy: CheckpointPolicy,
    ) -> Result<Self, DurabilityError> {
        if partition_count == 0 {
            return Err(DurabilityError::ConfigError(
                "partition_count must be at least 1".to_owned(),
            ));
        }

        if mode == DurabilityMode::DurableDedup && dedup_ttl == Duration::ZERO {
            return Err(DurabilityError::ConfigError(
                "dedup_ttl must be greater than zero for DurableDedup mode".to_owned(),
            ));
        }

        if checkpoint_policy == CheckpointPolicy::PerBatch(0) {
            return Err(DurabilityError::ConfigError(
                "checkpoint batch size must be at least 1".to_owned(),
            ));
        }

        Ok(Self {
            mode,
            partition_count,
            dedup_ttl,
            checkpoint_policy,
        })
    }

    /// Returns the configured channel durability strategy.
    #[must_use]
    pub const fn mode(&self) -> DurabilityMode {
        self.mode
    }

    /// Returns the number of independent durable partitions.
    #[must_use]
    pub const fn partition_count(&self) -> usize {
        self.partition_count
    }

    /// Returns the time-to-live for dedup receipts.
    #[must_use]
    pub const fn dedup_ttl(&self) -> Duration {
        self.dedup_ttl
    }

    /// Returns the consumer checkpoint policy.
    #[must_use]
    pub const fn checkpoint_policy(&self) -> CheckpointPolicy {
        self.checkpoint_policy
    }
}
