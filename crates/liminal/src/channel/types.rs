use crate::channel::SubscriptionHandle;
use crate::error::LiminalError;

/// Defines whether a channel is memory-only or durable across restarts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChannelMode {
    /// In-memory channel mode with no persistence overhead.
    Ephemeral,
    /// Durable channel mode reserved for future haematite-backed storage.
    Durable,
}

/// Opaque schema reference placeholder filled in by LIM-003.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SchemaRef;

/// Required configuration for creating a typed channel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelConfig {
    /// Explicit channel name.
    pub name: String,
    /// Explicit schema reference for validating published payloads.
    pub schema: SchemaRef,
    /// Explicit durability mode for the channel.
    pub mode: ChannelMode,
}

impl ChannelConfig {
    /// Creates channel configuration from its required fields.
    #[must_use]
    pub const fn new(name: String, schema: SchemaRef, mode: ChannelMode) -> Self {
        Self { name, schema, mode }
    }
}

/// Cloneable handle for interacting with a channel process.
#[derive(Clone, Debug)]
pub struct ChannelHandle;

impl ChannelHandle {
    /// Publishes a payload to the channel.
    ///
    /// # Errors
    ///
    /// Returns a [`LiminalError`] when the channel cannot accept the payload.
    /// Schema validation and actor delivery are implemented by later briefs.
    pub fn publish<Payload>(&self, payload: Payload) -> Result<(), LiminalError> {
        drop(payload);

        Ok(())
    }

    /// Subscribes to the channel.
    ///
    /// # Errors
    ///
    /// Returns a [`LiminalError`] when a subscription cannot be created.
    /// Subscriber registration is implemented by later briefs.
    pub const fn subscribe(&self) -> Result<SubscriptionHandle, LiminalError> {
        Ok(SubscriptionHandle::new())
    }

    /// Closes the channel gracefully.
    ///
    /// # Errors
    ///
    /// Returns a [`LiminalError`] when the channel cannot be shut down.
    /// Actor shutdown signaling is implemented by later briefs.
    pub const fn close(&self) -> Result<(), LiminalError> {
        Ok(())
    }
}
