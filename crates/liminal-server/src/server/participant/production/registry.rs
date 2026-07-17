//! Durable registry of every production participant conversation.
//!
//! One server-wide append-only stream holds one row per created
//! conversation, appended and flushed IMMEDIATELY BEFORE the conversation's
//! own genesis append (the one conversation-creating write). Startup reads
//! this stream to enumerate every durable conversation for the server-scope
//! capacity restore — a per-key read materialises its shard on demand, so
//! the enumeration is exact on a freshly reopened database (unlike a store
//! scan, which visits only shards this process has already touched).
//!
//! Ordering makes the registry complete by construction: a conversation
//! stream can exist only if its registry row was durable first. The reverse
//! window (registry row durable, genesis append failed) leaves a row whose
//! conversation replays empty — startup skips it exactly like a refused
//! probe, and a later retry of the creating enrollment re-registers, so
//! duplicate rows are legal and deduplicated on read.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use liminal::durability::DurableStore;
use liminal::durability::bridge::block_on;
use serde::{Deserialize, Serialize};

use super::log::OperationLogError;

/// Stream key of the server-wide conversation registry.
const REGISTRY_STREAM_KEY: &str = "liminal:participant-conversation-registry";
/// Durable page size used during restore reads.
const READ_BATCH_SIZE: usize = 64;
/// Stored-row schema version.
const SCHEMA_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct RegistryRow {
    schema_version: u8,
    conversation_id: u64,
}

/// Durable conversation-registry handle with its serialized append head.
///
/// Appends are rare (once per created conversation) and serialized under one
/// lock so concurrent creations cannot race the optimistic sequence.
#[derive(Debug)]
pub(super) struct ConversationRegistry {
    store: Arc<dyn DurableStore>,
    head: Mutex<u64>,
}

impl ConversationRegistry {
    /// Creates the handle; the head is set by [`Self::restore`].
    pub(super) fn new(store: Arc<dyn DurableStore>) -> Self {
        Self {
            store,
            head: Mutex::new(0),
        }
    }

    /// Locks the append head, recovering from poison: the critical sections
    /// below contain no panicking operation (panics are denied crate-wide),
    /// so a poisoned guard can only come from a foreign unwind and never
    /// observes a torn head.
    fn head(&self) -> MutexGuard<'_, u64> {
        self.head.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Reads every registered conversation id (deduplicated, in first-seen
    /// order is irrelevant — callers replay each once) and installs the
    /// durable append head.
    ///
    /// # Errors
    ///
    /// Returns [`OperationLogError`] for an unreadable stream, undecodable
    /// row, or unsupported schema version — a corrupt registry fails startup
    /// loudly.
    pub(super) fn restore(&self) -> Result<Vec<u64>, OperationLogError> {
        let mut sequence = 0_u64;
        let mut seen = std::collections::BTreeSet::new();
        loop {
            let entries = block_on(self.store.read_from(
                REGISTRY_STREAM_KEY,
                sequence,
                READ_BATCH_SIZE,
            ))??;
            if entries.is_empty() {
                break;
            }
            let count = entries.len();
            for entry in entries {
                if entry.sequence != sequence {
                    return Err(OperationLogError::Sequence {
                        expected: sequence,
                        actual: entry.sequence,
                    });
                }
                let row: RegistryRow = serde_json::from_slice(&entry.payload)?;
                if row.schema_version != SCHEMA_VERSION {
                    return Err(OperationLogError::SchemaVersion(row.schema_version));
                }
                seen.insert(row.conversation_id);
                sequence = sequence.checked_add(1).ok_or(OperationLogError::Sequence {
                    expected: u64::MAX,
                    actual: entry.sequence,
                })?;
            }
            if count < READ_BATCH_SIZE {
                break;
            }
        }
        *self.head() = sequence;
        Ok(seen.into_iter().collect())
    }

    /// Durably registers one conversation id, flushed before returning — the
    /// caller's genesis append may only follow a durable registry row.
    ///
    /// # Errors
    ///
    /// Returns [`OperationLogError`] when the append or flush fails; the
    /// caller aborts the conversation-creating commit and publishes nothing.
    pub(super) fn register(&self, conversation_id: u64) -> Result<(), OperationLogError> {
        let payload = serde_json::to_vec(&RegistryRow {
            schema_version: SCHEMA_VERSION,
            conversation_id,
        })?;
        let mut head = self.head();
        let assigned = block_on(self.store.append(REGISTRY_STREAM_KEY, payload, *head))??;
        if assigned != *head {
            return Err(OperationLogError::AssignedSequence {
                expected: *head,
                actual: assigned,
            });
        }
        block_on(self.store.flush())??;
        *head = head.checked_add(1).ok_or(OperationLogError::Sequence {
            expected: u64::MAX,
            actual: assigned,
        })?;
        drop(head);
        Ok(())
    }
}
