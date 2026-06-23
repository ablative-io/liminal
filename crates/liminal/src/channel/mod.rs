//! Channels: typed, schema-validated, in-memory message buses backed by real
//! supervised beamr actor processes (LIM-002).

pub mod actor;
pub mod registry;
pub mod schema;
pub mod subscription;
pub mod supervisor;
pub mod types;

pub use registry::{ChannelRegistry, ChannelSummary};
pub use schema::{Schema, SchemaId, SchemaValidationError};
pub use subscription::SubscriptionHandle;
pub use supervisor::{ChannelRestartPolicy, ChannelSupervisor, shared_supervisor};
pub use types::{ChannelConfig, ChannelHandle, ChannelMode, SchemaRef};

#[cfg(test)]
mod tests;
