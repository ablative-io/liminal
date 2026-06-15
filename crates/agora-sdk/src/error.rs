extern crate alloc;

use alloc::{boxed::Box, string::String};
use core::error::Error;

type ErrorSource = Box<dyn Error + Send + Sync + 'static>;

/// Error taxonomy for SDK transport, protocol, validation, pressure, conversation, and store failures.
#[derive(Debug, thiserror::Error)]
pub enum SdkError {
    /// Connection setup, use, or recovery failed.
    #[error("connection failure during {operation}: {source}")]
    Connection {
        /// The SDK operation that encountered the connection failure.
        operation: &'static str,
        /// The underlying connection error.
        #[source]
        source: ErrorSource,
    },

    /// Protocol encoding, decoding, or semantic validation failed.
    #[error("protocol failure during {operation}: {source}")]
    Protocol {
        /// The protocol operation that failed.
        operation: &'static str,
        /// The underlying protocol error.
        #[source]
        source: ErrorSource,
    },

    /// Message serialization or deserialization failed.
    #[error("serialization failure for {format}: {source}")]
    Serialization {
        /// The serialization format or codec being used.
        format: &'static str,
        /// The underlying serialization error.
        #[source]
        source: ErrorSource,
    },

    /// A typed message failed SDK-level validation.
    #[error("type validation failure for {type_name}: {description}")]
    TypeValidation {
        /// The message or schema type that failed validation.
        type_name: String,
        /// Human-readable validation details.
        description: String,
    },

    /// A backpressure signal reached the SDK caller.
    #[error("backpressure signal: {reason}")]
    Backpressure {
        /// Human-readable pressure reason supplied by the bus or transport.
        reason: String,
    },

    /// Conversation handling failed for a specific conversation.
    #[error("conversation {conversation_id} failed: {description}")]
    Conversation {
        /// Identifier of the conversation that failed.
        conversation_id: String,
        /// Human-readable conversation failure details.
        description: String,
    },

    /// Durable store access failed.
    #[error("store failure during {operation}: {source}")]
    Store {
        /// The store operation that failed.
        operation: &'static str,
        /// The underlying storage error.
        #[source]
        source: ErrorSource,
    },
}
