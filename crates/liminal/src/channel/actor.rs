use serde_json::Value;

use crate::causal::CausalContext;
use crate::channel::SubscriptionHandle;
use crate::channel::schema::{Schema, SchemaId, SchemaValidationError};
use crate::envelope::{Envelope, PublisherId};
use crate::error::LiminalError;

/// Minimal channel actor state that owns schema validation and subscriber fan-out.
#[derive(Debug)]
pub(crate) struct ChannelActor {
    schema: Schema,
    subscribers: Vec<crate::channel::subscription::SubscriberRegistration>,
    closed: bool,
}

impl ChannelActor {
    /// Creates channel actor state for a schema-owned channel.
    #[must_use]
    pub(crate) const fn new(schema: Schema) -> Self {
        Self {
            schema,
            subscribers: Vec::new(),
            closed: false,
        }
    }

    /// Returns the schema version currently owned by the actor.
    #[must_use]
    pub(crate) const fn schema_id(&self) -> SchemaId {
        self.schema.id()
    }

    /// Registers a subscriber without altering schema or existing subscribers.
    pub(crate) fn subscribe(&mut self) -> Result<SubscriptionHandle, LiminalError> {
        if self.closed {
            return Err(LiminalError::ChannelClosed {
                message: "channel is closed".to_owned(),
            });
        }

        let subscription = SubscriptionHandle::new();
        self.subscribers.push(subscription.registration());
        Ok(subscription)
    }

    /// Validates a payload, wraps it in an envelope, and fans it out to subscribers.
    pub(crate) fn publish(
        &mut self,
        payload: &[u8],
        publisher_id: PublisherId,
        causal_context: Option<CausalContext>,
    ) -> Result<(), LiminalError> {
        if self.closed {
            return Err(LiminalError::ChannelClosed {
                message: "channel is closed".to_owned(),
            });
        }

        self.schema
            .validate(payload)
            .map_err(|error| schema_mismatch(&error))?;
        let normalized_payload = self
            .schema
            .validate_and_apply_defaults(payload)
            .map_err(|error| schema_mismatch(&error))?;
        let envelope = Envelope::new(
            normalized_payload,
            causal_context,
            self.schema.id(),
            publisher_id,
        );
        self.deliver(&envelope)
    }

    /// Evolves the actor-owned schema without touching subscriber registrations.
    pub(crate) fn evolve_add_field(
        &mut self,
        name: impl Into<String>,
        field_schema: Value,
        default: Value,
    ) -> Result<SchemaId, SchemaValidationError> {
        let evolved = self.schema.evolve_add_field(name, field_schema, default)?;
        let schema_id = evolved.id();
        self.schema = evolved;
        Ok(schema_id)
    }

    /// Closes the actor and releases subscriber registrations.
    pub(crate) fn close(&mut self) {
        self.closed = true;
        self.subscribers.clear();
    }

    fn deliver(&mut self, envelope: &Envelope) -> Result<(), LiminalError> {
        let mut active_subscribers = Vec::with_capacity(self.subscribers.len());
        for subscriber in &self.subscribers {
            if let Some(inbox) = subscriber.upgrade() {
                {
                    let mut messages =
                        inbox.lock().map_err(|error| LiminalError::DeliveryFailed {
                            message: format!("subscriber inbox unavailable: {error}"),
                        })?;
                    messages.push_back(envelope.clone());
                }
                active_subscribers.push(std::sync::Arc::downgrade(&inbox));
            }
        }
        self.subscribers = active_subscribers;
        Ok(())
    }
}

fn schema_mismatch(error: &SchemaValidationError) -> LiminalError {
    LiminalError::SchemaMismatch {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::io;

    use serde_json::{Value, json};

    use super::ChannelActor;
    use crate::channel::{
        ChannelConfig, ChannelHandle, ChannelMode, Schema, SchemaValidationError,
    };
    use crate::envelope::Envelope;
    use crate::error::LiminalError;

    #[test]
    fn publish_valid_message_delivers_envelope() -> Result<(), Box<dyn Error>> {
        let handle = order_channel()?;
        let subscription = handle.subscribe()?;

        handle.publish_from("publisher-1", br#"{"order_id":"A1","quantity":3}"#)?;
        let envelope = next_envelope(&subscription)?;

        assert_eq!(
            envelope.payload,
            br#"{"order_id":"A1","quantity":3}"#.to_vec()
        );
        assert_eq!(envelope.publisher_id.as_str(), "publisher-1");
        assert!(envelope.causal_context.is_none());
        Ok(())
    }

    #[test]
    fn publish_invalid_message_returns_schema_mismatch_without_delivery()
    -> Result<(), Box<dyn Error>> {
        let handle = order_channel()?;
        let subscription = handle.subscribe()?;

        let result = handle.publish(br#"{"order_id":"A1","quantity":0}"#);

        assert!(matches!(result, Err(LiminalError::SchemaMismatch { .. })));
        assert!(subscription.try_next()?.is_none());
        Ok(())
    }

    #[test]
    fn evolved_schema_keeps_existing_subscriber_and_applies_default() -> Result<(), Box<dyn Error>>
    {
        let handle = order_channel()?;
        let subscription = handle.subscribe()?;
        let schema_id = handle.evolve_schema_add_field(
            "priority",
            json!({"type":"string"}),
            json!("normal"),
        )?;

        handle.publish(br#"{"order_id":"A1","quantity":3}"#)?;
        let envelope = next_envelope(&subscription)?;
        let payload: Value = serde_json::from_slice(&envelope.payload)?;

        assert_eq!(envelope.schema_id, schema_id);
        assert_eq!(payload.get("priority"), Some(&json!("normal")));
        Ok(())
    }

    #[test]
    fn actor_evolution_does_not_disconnect_existing_subscribers() -> Result<(), Box<dyn Error>> {
        let schema = order_schema()?;
        let mut actor = ChannelActor::new(schema);
        let old_schema_id = actor.schema_id();
        let subscription = actor.subscribe()?;
        let new_schema_id =
            actor.evolve_add_field("priority", json!({"type":"string"}), json!("normal"))?;

        actor.publish(
            br#"{"order_id":"A1","quantity":3,"priority":"urgent"}"#,
            "publisher-1".into(),
            None,
        )?;
        let envelope = next_envelope(&subscription)?;
        let payload: Value = serde_json::from_slice(&envelope.payload)?;

        assert_ne!(new_schema_id, old_schema_id);
        assert_eq!(envelope.schema_id, new_schema_id);
        assert_eq!(payload.get("priority"), Some(&json!("urgent")));
        Ok(())
    }

    fn order_channel() -> Result<ChannelHandle, SchemaValidationError> {
        let config =
            ChannelConfig::new("orders".to_owned(), order_schema()?, ChannelMode::Ephemeral);
        Ok(ChannelHandle::new(config))
    }

    fn order_schema() -> Result<Schema, SchemaValidationError> {
        Schema::new(json!({
            "type": "object",
            "properties": {
                "order_id": {"type": "string"},
                "quantity": {"type": "integer", "minimum": 1}
            },
            "required": ["order_id", "quantity"],
            "additionalProperties": false
        }))
    }

    fn next_envelope(
        subscription: &crate::channel::SubscriptionHandle,
    ) -> Result<Envelope, Box<dyn Error>> {
        subscription.try_next()?.map_or_else(
            || -> Result<Envelope, Box<dyn Error>> {
                Err(Box::new(io::Error::other("expected delivered envelope")))
            },
            Ok,
        )
    }
}
