#[path = "connection/process.rs"]
mod process;
#[path = "connection/services.rs"]
mod services;
#[cfg(test)]
#[path = "connection/services_r5_tests.rs"]
mod services_r5_tests;
#[path = "connection/supervisor.rs"]
mod supervisor;

pub use services::{
    ConnectionConversation, ConnectionServices, ConnectionSubscription, LiminalConnectionServices,
};
pub use supervisor::{ConnectionHandle, ConnectionSupervisor};
