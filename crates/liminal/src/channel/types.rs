use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

use crate::causal::CausalContext;
use crate::channel::SubscriptionHandle;
use crate::channel::actor::ChannelActor;
use crate::channel::schema::{Schema, SchemaId, SchemaValidationError};
use crate::durability::bridge::block_on;
use crate::durability::{DurableChannel, DurableStore, MessageEnvelope};
use crate::envelope::PublisherId;
use crate::error::LiminalError;

/// Single-partition count used to back a flat runtime channel with durable storage.
///
/// The runtime channel model is flat (no operator-visible partitioning), so a
/// durable runtime channel maps onto exactly one durable partition. Partitioned
/// durable topologies are a durability-subsystem concern, not a runtime-channel
/// one.
const RUNTIME_DURABLE_PARTITIONS: usize = 1;

/// Defines whether a channel is memory-only or durable across restarts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChannelMode {
    /// In-memory channel mode with no persistence overhead.
    Ephemeral,
    /// Durable channel mode reserved for future haematite-backed storage.
    Durable,
}

/// Compatibility alias for the channel-owned schema definition.
pub type SchemaRef = Schema;

/// Required configuration for creating a typed channel.
#[derive(Clone, Debug)]
pub struct ChannelConfig {
    /// Explicit channel name.
    pub name: String,
    /// Explicit schema for validating published payloads.
    pub schema: Schema,
    /// Explicit durability mode for the channel.
    pub mode: ChannelMode,
}

impl ChannelConfig {
    /// Creates channel configuration from its required fields.
    #[must_use]
    pub const fn new(name: String, schema: Schema, mode: ChannelMode) -> Self {
        Self { name, schema, mode }
    }
}

/// Cloneable handle for interacting with a channel process.
#[derive(Clone, Debug)]
pub struct ChannelHandle {
    config: ChannelConfig,
    actor: Arc<Mutex<ChannelActor>>,
    durable: Option<Arc<Mutex<DurableChannel>>>,
}

impl ChannelHandle {
    /// Creates an ephemeral handle backed only by in-memory channel actor state.
    ///
    /// Ephemeral channels carry no durable store; [`Self::publish`] fans out in
    /// memory and [`Self::flush`] is a no-op.
    #[must_use]
    pub fn new(config: ChannelConfig) -> Self {
        let actor = ChannelActor::new(config.schema.clone());
        Self {
            config,
            actor: Arc::new(Mutex::new(actor)),
            durable: None,
        }
    }

    /// Creates a durable handle that persists every accepted publish to `store`
    /// before fanning it out to subscribers.
    ///
    /// The durable channel is keyed by `config.name` and backed by a single
    /// partition (see [`RUNTIME_DURABLE_PARTITIONS`]). Each publish appends the
    /// message to the store with a monotonic sequence; [`Self::flush`] drives
    /// the store's own flush so all accepted writes are persisted before it
    /// returns.
    ///
    /// # Errors
    ///
    /// Returns [`LiminalError::PublishFailed`] when the durable channel cannot be
    /// initialized over `store`.
    pub fn new_durable(
        config: ChannelConfig,
        store: Arc<dyn DurableStore>,
    ) -> Result<Self, LiminalError> {
        let durable = DurableChannel::new(config.name.clone(), RUNTIME_DURABLE_PARTITIONS, store)
            .map_err(|error| LiminalError::PublishFailed {
            message: format!(
                "failed to initialize durable channel '{}': {error}",
                config.name
            ),
        })?;
        let actor = ChannelActor::new(config.schema.clone());
        Ok(Self {
            config,
            actor: Arc::new(Mutex::new(actor)),
            durable: Some(Arc::new(Mutex::new(durable))),
        })
    }

    /// Returns the channel configuration used to create this handle.
    #[must_use]
    pub const fn config(&self) -> &ChannelConfig {
        &self.config
    }

    /// Publishes a payload to the channel with the default publisher identity.
    ///
    /// # Errors
    ///
    /// Returns a [`LiminalError`] when the channel cannot accept the payload or the schema rejects it.
    pub fn publish<Payload>(&self, payload: Payload) -> Result<(), LiminalError>
    where
        Payload: AsRef<[u8]>,
    {
        self.publish_with_context(payload, PublisherId::default(), None)
    }

    /// Publishes a payload with an explicit publisher identity.
    ///
    /// # Errors
    ///
    /// Returns a [`LiminalError`] when the channel cannot accept the payload or the schema rejects it.
    pub fn publish_from<Payload>(
        &self,
        publisher_id: impl Into<PublisherId>,
        payload: Payload,
    ) -> Result<(), LiminalError>
    where
        Payload: AsRef<[u8]>,
    {
        self.publish_with_context(payload, publisher_id.into(), None)
    }

    /// Publishes a payload with explicit publisher and causal metadata.
    ///
    /// # Errors
    ///
    /// Returns a [`LiminalError`] when the channel cannot accept the payload or the schema rejects it.
    pub fn publish_with_context<Payload>(
        &self,
        payload: Payload,
        publisher_id: PublisherId,
        causal_context: Option<CausalContext>,
    ) -> Result<(), LiminalError>
    where
        Payload: AsRef<[u8]>,
    {
        // Durable channels persist the message to the store BEFORE acknowledging
        // the publish (and before fanning out): a published message that was
        // not durably recorded would be lost on shutdown, which CN7 forbids.
        if let Some(durable) = self.durable.as_ref() {
            self.persist_durable(durable, payload.as_ref(), &publisher_id)?;
        }
        let mut actor = self.lock_actor()?;
        actor.publish(payload.as_ref(), publisher_id, causal_context)
    }

    fn persist_durable(
        &self,
        durable: &Arc<Mutex<DurableChannel>>,
        payload: &[u8],
        publisher_id: &PublisherId,
    ) -> Result<(), LiminalError> {
        let envelope = MessageEnvelope {
            payload: payload.to_vec(),
            causal_context: None,
            timestamp: now_millis(),
            publisher_id: publisher_id.as_str().to_owned(),
            idempotency_key: None,
        };
        let publish_result = {
            let mut channel = durable
                .lock()
                .map_err(|error| LiminalError::PublishFailed {
                    message: format!("durable channel state unavailable: {error}"),
                })?;
            // The guard is released at the end of this block, before the error is
            // mapped and propagated, keeping the critical section minimal.
            block_on(channel.publish(&envelope))
        };
        publish_result
            .map_err(|error| LiminalError::PublishFailed {
                message: format!(
                    "durable publish bridge for channel '{}' failed: {error}",
                    self.config.name
                ),
            })?
            .map_err(|error| LiminalError::PublishFailed {
                message: format!(
                    "durable publish to channel '{}' failed: {error}",
                    self.config.name
                ),
            })?;
        Ok(())
    }

    /// Returns the schema version currently owned by the channel actor.
    ///
    /// # Errors
    ///
    /// Returns a [`LiminalError`] when the channel actor cannot be read.
    pub fn current_schema_id(&self) -> Result<SchemaId, LiminalError> {
        let actor = self.lock_actor()?;
        Ok(actor.schema_id())
    }

    /// Evolves the channel schema by adding a defaulted field without disconnecting subscribers.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaValidationError`] when the schema cannot be evolved.
    pub fn evolve_schema_add_field(
        &self,
        name: impl Into<String>,
        field_schema: Value,
        default: Value,
    ) -> Result<SchemaId, SchemaValidationError> {
        let mut actor = self.lock_actor_for_schema()?;
        actor.evolve_add_field(name, field_schema, default)
    }

    /// Subscribes to the channel.
    ///
    /// # Errors
    ///
    /// Returns a [`LiminalError`] when a subscription cannot be created.
    pub fn subscribe(&self) -> Result<SubscriptionHandle, LiminalError> {
        let mut actor = self.lock_actor()?;
        actor.subscribe()
    }

    /// Flushes buffered durable channel state to the backing store before shutdown.
    ///
    /// # Errors
    ///
    /// Returns a [`LiminalError`] when the channel actor cannot be inspected or
    /// when the durable store flush fails.
    ///
    /// For a durable channel this drives the backing [`DurableStore::flush`],
    /// guaranteeing every accepted publish (each already appended synchronously
    /// during [`Self::publish`]) is persisted before this call returns. For an
    /// ephemeral channel there is no store, so this only confirms the actor is
    /// reachable and returns.
    pub fn flush(&self) -> Result<(), LiminalError> {
        drop(self.lock_actor()?);
        let Some(durable) = self.durable.as_ref() else {
            return Ok(());
        };
        let flush_result = {
            let channel = durable
                .lock()
                .map_err(|error| LiminalError::PublishFailed {
                    message: format!("durable channel state unavailable: {error}"),
                })?;
            // The guard is released at the end of this block, before the error is
            // mapped and propagated, keeping the critical section minimal.
            block_on(channel.flush_store())
        };
        flush_result
            .map_err(|error| LiminalError::PublishFailed {
                message: format!(
                    "durable flush bridge for channel '{}' failed: {error}",
                    self.config.name
                ),
            })?
            .map_err(|error| LiminalError::PublishFailed {
                message: format!(
                    "durable flush for channel '{}' failed: {error}",
                    self.config.name
                ),
            })?;
        Ok(())
    }

    /// Closes the channel gracefully.
    ///
    /// # Errors
    ///
    /// Returns a [`LiminalError`] when the channel cannot be shut down.
    pub fn close(&self) -> Result<(), LiminalError> {
        self.lock_actor()?.close();
        Ok(())
    }

    fn lock_actor(&self) -> Result<std::sync::MutexGuard<'_, ChannelActor>, LiminalError> {
        self.actor
            .lock()
            .map_err(|error| LiminalError::PublishFailed {
                message: format!("channel actor unavailable: {error}"),
            })
    }

    fn lock_actor_for_schema(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, ChannelActor>, SchemaValidationError> {
        self.actor
            .lock()
            .map_err(|error| SchemaValidationError::InvalidSchema {
                message: format!("channel actor unavailable: {error}"),
            })
    }
}

/// Returns the current epoch milliseconds, saturating to zero before the epoch.
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
