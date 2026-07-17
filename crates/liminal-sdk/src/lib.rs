#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

pub mod channel;
pub mod connection;
pub mod conversation;
pub mod embedded;
pub mod error;
pub mod pressure;
pub mod remote;
pub mod types;

pub use channel::ChannelHandle;
pub use connection::{
    ConnectionEvent, ConnectionEvents, ConnectionLifecycle, ConnectionPool, ConnectionPoolConfig,
    ConnectionState, DisconnectReason, PoolConnectionId, ReconnectConfig, ReconnectJitter,
    ResumeRequest, SubscriptionAssignment, SubscriptionId, SubscriptionRecovery,
};
pub use conversation::{ConversationEvent, ConversationHandle, ConversationId};
pub use embedded::{EmbeddedChannelHandle, EmbeddedConfig, EmbeddedConversationHandle};
pub use error::SdkError;
pub use pressure::{DeliveryAck, PressureResponse};
#[cfg(feature = "std")]
pub use remote::{
    DeliveredMessage, OBSERVABILITY_CHANNEL, PushClient, PushWriter, PushedFrame,
    SubscriptionStream, WebSocketDeliveredMessage, WebSocketRemoteTransport,
    WebSocketSubscriptionStream,
};
pub use remote::{
    RemoteChannelHandle, RemoteConfig, RemoteConversationHandle, SdkChannelHandle, SdkConfig,
    SdkConversationHandle, ServerAddress, build_channel_handle, build_conversation_handle,
};
pub use types::{SchemaMetadata, SchemaValidate};
