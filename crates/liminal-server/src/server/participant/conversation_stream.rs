//! Durable append-only stream boundary for one participant conversation.
//!
//! This module owns storage mechanics only. Lifecycle selection and the bytes
//! representing one atomic transition remain the shared protocol crate's
//! responsibility. Reads are explicit bounded replay pages; an empty page is
//! end-of-stream, never a timer or polling signal.

use std::sync::Arc;

use liminal::durability::{DurabilityError, DurableStore, StoredEntry};

const STREAM_PREFIX: &str = "liminal/participant/conversation/v1/";
const REPLAY_PAGE_SIZE: usize = 256;

/// One validated bounded replay page from a participant-conversation stream.
#[derive(Debug)]
pub(super) struct ConversationEventPage {
    entries: Vec<StoredEntry>,
    next_sequence: u64,
}

impl ConversationEventPage {
    /// Returns whether replay reached the current durable stream head.
    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Consumes the page into its validated, contiguous entries.
    pub(super) fn into_entries(self) -> Vec<StoredEntry> {
        self.entries
    }

    /// Returns the exact sequence expected by the next page or append.
    pub(super) const fn next_sequence(&self) -> u64 {
        self.next_sequence
    }
}

/// Storage failure at the participant conversation's single transaction seam.
#[derive(Debug, thiserror::Error)]
pub(super) enum ConversationStreamError {
    /// The durable engine rejected a read or append.
    #[error(transparent)]
    Durability(#[from] DurabilityError),
    /// A replay page did not continue at the requested stream sequence.
    #[error("participant event sequence mismatch: expected {expected}, got {actual}")]
    EventSequence {
        /// Requested next stream sequence.
        expected: u64,
        /// Sequence returned by durable storage.
        actual: u64,
    },
    /// A successful append returned a sequence other than its optimistic head.
    #[error("participant append sequence mismatch: expected {expected}, got {actual}")]
    AssignedSequence {
        /// Optimistic stream head supplied to append.
        expected: u64,
        /// Sequence reported by durable storage.
        actual: u64,
    },
    /// The stream cannot represent another event sequence.
    #[error("participant event stream sequence exhausted at u64::MAX")]
    SequenceExhausted,
}

/// Stateless handle for one conversation's durable participant event stream.
#[derive(Debug)]
pub(super) struct ConversationEventStream {
    store: Arc<dyn DurableStore>,
    stream_key: String,
}

impl ConversationEventStream {
    /// Binds a conversation id to its namespaced durable stream.
    pub(super) fn new(store: Arc<dyn DurableStore>, conversation_id: u64) -> Self {
        Self {
            store,
            stream_key: format!("{STREAM_PREFIX}{conversation_id}"),
        }
    }

    /// Returns the stable namespaced stream key.
    #[cfg(test)]
    pub(super) fn stream_key(&self) -> &str {
        &self.stream_key
    }

    /// Reads and validates one bounded contiguous replay page.
    ///
    /// An empty page means the caller has reached the current stream head. A
    /// caller reconstructing an aggregate advances only through nonempty pages
    /// and stops on that first empty page; no delayed retry is part of this API.
    ///
    /// # Errors
    ///
    /// Returns [`ConversationStreamError`] if durable storage fails, returns a
    /// gap/reordering, or a nonempty page would advance beyond `u64::MAX`.
    pub(super) async fn read_page(
        &self,
        expected_sequence: u64,
    ) -> Result<ConversationEventPage, ConversationStreamError> {
        let entries = self
            .store
            .read_from(&self.stream_key, expected_sequence, REPLAY_PAGE_SIZE)
            .await?;
        let mut next_sequence = expected_sequence;
        for entry in &entries {
            if entry.sequence != next_sequence {
                return Err(ConversationStreamError::EventSequence {
                    expected: next_sequence,
                    actual: entry.sequence,
                });
            }
            next_sequence = next_sequence
                .checked_add(1)
                .ok_or(ConversationStreamError::SequenceExhausted)?;
        }
        Ok(ConversationEventPage {
            entries,
            next_sequence,
        })
    }

    /// Appends and flushes exactly one aggregate transition at the optimistic
    /// stream head.
    ///
    /// The event payload must represent the complete protocol-produced atomic
    /// transition. A conflict is returned directly; the aggregate owner may
    /// immediately reload on a new inbound event, but this boundary never sleeps
    /// or retries on a timer.
    ///
    /// # Errors
    ///
    /// Returns [`ConversationStreamError`] for stream exhaustion, append or
    /// flush failure, or an engine result inconsistent with the optimistic
    /// append. A flush failure is ambiguous: the aggregate owner must discard
    /// speculative state and cold-reload durable reality once without retrying
    /// this append.
    pub(super) async fn append(
        &self,
        expected_sequence: u64,
        payload: Vec<u8>,
    ) -> Result<u64, ConversationStreamError> {
        let next_sequence = expected_sequence
            .checked_add(1)
            .ok_or(ConversationStreamError::SequenceExhausted)?;
        let assigned = self
            .store
            .append(&self.stream_key, payload, expected_sequence)
            .await?;
        if assigned != expected_sequence {
            return Err(ConversationStreamError::AssignedSequence {
                expected: expected_sequence,
                actual: assigned,
            });
        }
        self.store.flush().await?;
        Ok(next_sequence)
    }
}
