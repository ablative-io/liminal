pub mod config;
pub mod error;
pub mod store;

pub use config::{CheckpointPolicy, DurabilityConfig, DurabilityMode};
pub use error::DurabilityError;
pub use store::{DurableStore, HaematiteStore, StoredEntry};
