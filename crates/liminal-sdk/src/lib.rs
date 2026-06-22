#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

pub mod channel;
pub mod connection;
pub mod conversation;
pub mod error;
pub mod pressure;
pub mod types;

pub use channel::ChannelHandle;
pub use connection::{
    ConnectionEvent, ConnectionEvents, ConnectionLifecycle, ConnectionState, DisconnectReason,
    ReconnectConfig, ReconnectJitter, ResumeRequest, SubscriptionId, SubscriptionRecovery,
};
pub use conversation::{ConversationEvent, ConversationHandle, ConversationId};
pub use error::SdkError;
pub use pressure::PressureResponse;
pub use types::{SchemaMetadata, SchemaValidate};
