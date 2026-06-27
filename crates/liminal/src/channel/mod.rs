//! Channels: typed, schema-validated, in-memory message buses backed by real
//! supervised beamr actor processes (LIM-002).

pub mod actor;
pub mod observer;
pub mod registry;
pub mod schema;
pub mod subscription;
pub mod supervisor;
pub mod types;
pub mod wire;

pub use observer::ClusterObserver;
pub use registry::{ChannelRegistry, ChannelSummary};
pub use schema::{Schema, SchemaId, SchemaValidationError};
pub use subscription::SubscriptionHandle;
pub use supervisor::{ChannelRestartPolicy, ChannelSupervisor, shared_supervisor};
pub use types::{ChannelConfig, ChannelDelivery, ChannelHandle, ChannelMode, SchemaRef};
pub use wire::{WireError, decode_envelope, encode_envelope};

#[cfg(test)]
mod tests;
