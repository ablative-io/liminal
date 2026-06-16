mod codec;
mod storage;

pub use storage::{DurableChannel, EphemeralChannel, PartitionKey};

/// Storage causal metadata persisted with a durable message envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CausalContext {
    /// Optional parent message identifier.
    pub parent_id: Option<String>,
    /// Optional vector clock entry for causal ordering.
    pub vector_clock_entry: Option<u64>,
}

/// Deterministic storage envelope for durable channel messages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageEnvelope {
    /// Opaque message payload bytes.
    pub payload: Vec<u8>,
    /// Optional causal metadata.
    pub causal_context: Option<CausalContext>,
    /// Publisher-provided epoch-millisecond timestamp.
    pub timestamp: u64,
    /// Stable publisher identity.
    pub publisher_id: String,
    /// Optional producer idempotency key.
    pub idempotency_key: Option<String>,
}
