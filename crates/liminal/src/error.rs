/// Error taxonomy for liminal channel, conversation, schema, and delivery failures.
#[derive(Debug, thiserror::Error)]
pub enum LiminalError {
    /// The requested channel does not exist.
    #[error("{message}")]
    ChannelNotFound { message: String },

    /// The requested channel is closed.
    #[error("{message}")]
    ChannelClosed { message: String },

    /// A payload does not match the channel schema.
    #[error("{message}")]
    SchemaMismatch { message: String },

    /// Publishing a payload failed.
    #[error("{message}")]
    PublishFailed { message: String },

    /// Creating or maintaining a subscription failed.
    #[error("{message}")]
    SubscriptionFailed { message: String },

    /// Conversation execution failed.
    #[error("{message}")]
    ConversationFailed { message: String },

    /// Conversation execution timed out.
    #[error("{message}")]
    ConversationTimeout { message: String },

    /// A linked participant process crashed.
    #[error("{message}")]
    ParticipantCrashed { message: String },

    /// Delivering a payload failed.
    #[error("{message}")]
    DeliveryFailed { message: String },
}
