use std::sync::{Arc, Mutex};

use serde_json::Value;

use crate::causal::CausalContext;
use crate::channel::SubscriptionHandle;
use crate::channel::actor::ChannelActor;
use crate::channel::schema::{Schema, SchemaId, SchemaValidationError};
use crate::envelope::PublisherId;
use crate::error::LiminalError;

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
}

impl ChannelHandle {
    /// Creates a handle backed by in-memory channel actor state.
    #[must_use]
    pub fn new(config: ChannelConfig) -> Self {
        let actor = ChannelActor::new(config.schema.clone());
        Self {
            config,
            actor: Arc::new(Mutex::new(actor)),
        }
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
        let mut actor = self.lock_actor()?;
        actor.publish(payload.as_ref(), publisher_id, causal_context)
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
