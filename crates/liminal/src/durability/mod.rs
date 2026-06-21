pub mod channel;
pub mod config;
pub mod dedup;
pub mod error;
pub mod receipt;
pub mod store;

pub use channel::{CausalContext, DurableChannel, EphemeralChannel, MessageEnvelope, PartitionKey};
pub use config::{CheckpointPolicy, DurabilityConfig, DurabilityMode};
pub use dedup::{DedupCache, DedupDecision, DedupEntry, DedupSweepReport, DedupSweeper};
pub use error::DurabilityError;
pub use receipt::ProcessingReceipt;
pub use store::{DurableStore, HaematiteStore, StoredEntry};
