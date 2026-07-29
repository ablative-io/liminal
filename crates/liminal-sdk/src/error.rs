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

    /// A remote operation ran against a configuration whose transport was never
    /// connected, so nothing it did could have reached any wire.
    ///
    /// `RemoteConfig::new` installs the unconnected in-process transport. Call
    /// `RemoteConfig::connect_tcp`, `RemoteConfig::connect_tcp_with_auth`,
    /// `RemoteConfig::connect_websocket`, or
    /// `RemoteConfig::connect_websocket_with_auth` before publishing,
    /// subscribing, conversing, or resuming. The SDK refuses here rather than
    /// encoding a frame it discards and reporting the success that would imply.
    #[error(
        "transport not connected: {operation} needs a connected transport; call \
         RemoteConfig::connect_tcp or RemoteConfig::connect_websocket first"
    )]
    NotConnected {
        /// The SDK operation that refused.
        operation: String,
    },

    /// An SDK surface exists but has no machinery behind it, so it refused
    /// rather than reporting a success it cannot deliver.
    ///
    /// This is the SDK being loud about an absent feature. `seam` records where
    /// the gap is written down (a ledger row, or that nothing records it) and
    /// `alternative` names the surface that does work today.
    #[error("{surface} is not wired ({seam}); {alternative}")]
    Unwired {
        /// The SDK surface that refused.
        surface: String,
        /// Where the gap is recorded, or that nothing records it.
        seam: String,
        /// The working surface a caller should reach for instead.
        alternative: String,
    },
}
