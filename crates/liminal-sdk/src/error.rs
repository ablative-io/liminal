use alloc::string::String;

/// Error taxonomy returned by the Rust SDK API surface.
///
/// The SDK keeps application code independent from transport, protocol framing,
/// and core implementation error types. Concrete embedded and remote adapters
/// map their internal failures into these variants.
#[derive(Debug, thiserror::Error)]
pub enum SdkError {
    /// Establishing, keeping, or recovering a client connection failed.
    #[error("connection error: {description}")]
    Connection {
        /// Human-readable context from the failing connection operation.
        description: String,
    },

    /// Encoding, decoding, or interpreting SDK-internal protocol state failed.
    #[error("protocol error: {description}")]
    Protocol {
        /// Human-readable protocol failure context.
        description: String,
    },

    /// Serialising an outbound value or deserialising an inbound value failed.
    #[error("serialization error: {description}")]
    Serialization {
        /// Human-readable serialization failure context.
        description: String,
    },

    /// A typed message did not satisfy the schema declared for its channel.
    #[error("type validation failed: {description}")]
    TypeValidation {
        /// Human-readable type-validation context.
        description: String,
    },

    /// A publish operation encountered application-visible backpressure.
    #[error("backpressure: {reason}")]
    Backpressure {
        /// Human-readable pressure reason.
        reason: String,
    },

    /// A conversation operation failed.
    #[error("conversation {conversation_id}: {description}")]
    Conversation {
        /// Application-visible conversation identifier.
        conversation_id: String,
        /// Human-readable conversation failure context.
        description: String,
    },

    /// Persisted subscription or recovery state could not be read or written.
    #[error("store error: {description}")]
    Store {
        /// Human-readable store failure context.
        description: String,
    },
}
