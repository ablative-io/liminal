use chrono::{DateTime, Utc};

use crate::causal::CausalContext;
use crate::channel::SchemaId;

/// Identity of the publisher that submitted a message.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PublisherId(String);

impl PublisherId {
    /// Creates a publisher identifier from a string-like value.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the publisher identifier as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for PublisherId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for PublisherId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl Default for PublisherId {
    fn default() -> Self {
        Self::new("anonymous")
    }
}

/// Message envelope used as the delivery unit inside the core bus.
#[derive(Clone, Debug)]
pub struct Envelope {
    /// Validated payload bytes, normalized by the schema when defaults are applied.
    pub payload: Vec<u8>,
    /// Optional parent reference for causal-chain metadata.
    pub causal_context: Option<CausalContext>,
    /// Schema version that validated this payload.
    pub schema_id: SchemaId,
    /// Publisher identity attached at publish time.
    pub publisher_id: PublisherId,
    /// UTC timestamp captured when the message was published.
    pub timestamp: DateTime<Utc>,
}

impl Envelope {
    /// Creates an envelope with the current UTC publish timestamp.
    #[must_use]
    pub fn new(
        payload: Vec<u8>,
        causal_context: Option<CausalContext>,
        schema_id: SchemaId,
        publisher_id: PublisherId,
    ) -> Self {
        Self::with_timestamp(payload, causal_context, schema_id, publisher_id, Utc::now())
    }

    /// Creates an envelope with an explicit timestamp.
    #[must_use]
    pub const fn with_timestamp(
        payload: Vec<u8>,
        causal_context: Option<CausalContext>,
        schema_id: SchemaId,
        publisher_id: PublisherId,
        timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            payload,
            causal_context,
            schema_id,
            publisher_id,
            timestamp,
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::{Envelope, PublisherId};
    use crate::causal::{CausalContext, MessageId};
    use crate::channel::SchemaId;

    #[test]
    fn envelope_carries_required_fields() {
        let schema_id = SchemaId::new();
        let publisher_id = PublisherId::from("publisher-1");
        let parent = MessageId::new();
        let causal_context = Some(CausalContext::child_of(parent));
        let timestamp = fixed_timestamp();

        let envelope = Envelope::with_timestamp(
            b"{}".to_vec(),
            causal_context.clone(),
            schema_id,
            publisher_id.clone(),
            timestamp,
        );

        assert_eq!(envelope.payload, b"{}".to_vec());
        assert_eq!(envelope.causal_context, causal_context);
        assert_eq!(envelope.schema_id, schema_id);
        assert_eq!(envelope.publisher_id, publisher_id);
        assert_eq!(envelope.timestamp, timestamp);
    }

    fn fixed_timestamp() -> chrono::DateTime<Utc> {
        Utc::now()
    }
}
