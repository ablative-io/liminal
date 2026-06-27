#[path = "connection/conversation.rs"]
mod conversation;
#[path = "connection/process.rs"]
mod process;
#[path = "connection/services.rs"]
pub mod services;
#[path = "connection/services_cluster.rs"]
mod services_cluster;
#[cfg(test)]
#[path = "connection/services_r5_tests.rs"]
mod services_r5_tests;
#[path = "connection/supervisor.rs"]
mod supervisor;

pub use conversation::{ConnectionConversation, ConversationResource};
pub use services::{
    ChannelCluster, ConnectionServices, ConnectionSubscription, LiminalConnectionServices,
    PublishOutcome,
};
pub use supervisor::{ConnectionHandle, ConnectionSupervisor};
