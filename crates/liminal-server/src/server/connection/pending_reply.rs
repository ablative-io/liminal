//! R1(vi) (§1.2(3b)) pending-reply state machine.
//!
//! Removing the in-slice blocking reply drain (the LIM-005 request-reply path)
//! removes the serialization it accidentally provided, so pending replies need
//! real bookkeeping: ONE bounded table per connection, keyed by a monotonic
//! internal operation id, each entry carrying the conversation id, stream id,
//! deadline, and terminal state. Matching is FIFO within a conversation.
//!
//! # Tombstone lifecycle (pair ruling, 2026-07-11)
//!
//! On timeout an entry becomes a TOMBSTONE, and the timeout error frame is written
//! in the woken slice. A tombstone is reclaimed by exactly TWO events — its late
//! reply arriving (consume: the reply is discarded, the tombstone freed) or its
//! conversation closing (close sweep). There is NO time-based expiry: any
//! time-based reclamation eventually races an actor that is slow-not-dead — exactly
//! what produced the timeout — and would let a very-late reply FIFO-match a younger
//! entry admitted after reclamation, delivering the wrong reply as the right one
//! (semantic corruption, which no signature attaches to at any probability).
//!
//! Bounding is by scope, not time: tombstones count against
//! `max_pending_replies_per_conversation` (§5, the sub-cap) ONLY; pending entries
//! count against both the sub-cap AND `max_pending_conversation_replies_per_connection`
//! (§5, the connection table). A conversation accumulating tombstones therefore
//! wedges ITSELF — new reply-requested admissions on it draw the typed cap refusal
//! until it closes — while sibling conversations and the connection table are
//! untouched. The self-wedge is the honest semantic: a conversation holding a
//! tombstone whose reply may still arrive IS ambiguous, and the refusal confines
//! that ambiguity to the party that created it.
//!
//! # Deadline mechanics
//!
//! Under the current busy loop, [`PendingReplyTable::expire_due`] is called each
//! slice. The DEADLINE-CHECK SEAM is named so the park-flip commit can add the
//! seventh wake source — a timer-driven `READY` wake at each entry's deadline
//! (contract R1(vi) as amended, Waffles' finding: nothing else guarantees a wake,
//! so without it a parked connection whose reply expires under zero other traffic
//! never wakes and the client waits forever for a timeout the connection believes
//! it handled). beamr 0.13 exposes a timer wheel (`Scheduler::timers`) but wiring a
//! per-entry timer to a `READY` enqueue is the park-flip's job; this table builds
//! the seam and leaves the wake source to it (flagged, not faked).

use std::time::{Duration, Instant};

use liminal::protocol::{CONVERSATION_REPLY_REQUESTED_FLAG, Frame, MessageEnvelope};

use crate::ServerError;

/// Server-error reason code carried on the reply timeout / cap-refusal frames,
/// matching the code the rest of the connection paths use.
const SERVER_ERROR_CODE: u16 = 0xFFFF;

/// Default bound on how long the server holds a reply-requested operation before
/// it times out. Replaces the old in-slice 5 s BLOCKING drain: the wait is no
/// longer in the slice path — the entry sits in the table and the deadline is
/// checked each slice — but the 5 s bound to a client's correlated reply is
/// unchanged, so client-visible timeout behaviour is preserved.
pub(super) const DEFAULT_REPLY_TIMEOUT: Duration = Duration::from_secs(5);

/// Terminal disposition of a table entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryState {
    /// Awaiting the participant's reply; counts against BOTH caps.
    Pending,
    /// Timed out; the late reply may still arrive. Counts against the
    /// per-conversation sub-cap ONLY. Reclaimed only by late-reply consume or
    /// conversation-close sweep.
    Tombstone,
}

/// One in-flight (or timed-out) reply-requested operation.
#[derive(Debug)]
struct PendingReplyEntry {
    /// Monotonic internal operation id — the FIFO ordering key within a
    /// conversation. Younger entries have strictly larger ids.
    op_id: u64,
    conversation_id: u64,
    /// Client-chosen application stream the correlated reply / timeout rides.
    stream_id: u32,
    /// When a still-`Pending` entry becomes a tombstone.
    deadline: Instant,
    state: EntryState,
}

/// Per-connection pending-reply table (§1.2(3b)).
#[derive(Debug)]
pub(super) struct PendingReplyTable {
    /// All entries (pending and tombstone), ordered by ascending `op_id`, so the
    /// FRONT-most entry for a conversation is always its oldest — the only one a
    /// reply may consume (never a younger one).
    entries: Vec<PendingReplyEntry>,
    next_op_id: u64,
    /// §5 `max_pending_replies_per_conversation` sub-cap (default 8): counts
    /// pending + tombstone entries for one conversation.
    max_per_conversation: usize,
    /// §5 `max_pending_conversation_replies_per_connection` (default 32): counts
    /// PENDING entries across the whole connection.
    max_per_connection: usize,
    reply_timeout: Duration,
}

impl Default for PendingReplyTable {
    /// A table carrying the §5 default caps and the default reply timeout. Used by
    /// [`ConnectionProcessState::default`]; the connection replaces it with a table
    /// built from the runtime's configured limits at construction. The non-config
    /// runtimes use the same §5 defaults, so the values match.
    fn default() -> Self {
        Self::new(
            crate::config::types::LimitsConfig::DEFAULT_MAX_PENDING_REPLIES_PER_CONVERSATION,
            crate::config::types::LimitsConfig::DEFAULT_MAX_PENDING_CONVERSATION_REPLIES_PER_CONNECTION,
            DEFAULT_REPLY_TIMEOUT,
        )
    }
}

impl PendingReplyTable {
    /// Builds a table with the §5 caps and the reply timeout.
    pub(super) const fn new(
        max_per_conversation: usize,
        max_per_connection: usize,
        reply_timeout: Duration,
    ) -> Self {
        Self {
            entries: Vec::new(),
            next_op_id: 1,
            max_per_conversation,
            max_per_connection,
            reply_timeout,
        }
    }

    /// Admits a reply-requested operation on `conversation_id`/`stream_id`,
    /// deadlined at `now + reply_timeout`. Enforces both §5 caps.
    ///
    /// # Errors
    /// Returns [`ServerError::ConnectionCapReached`] when the per-conversation
    /// sub-cap (pending + tombstone) or the per-connection pending cap is full. A
    /// conversation whose sub-cap is full of tombstones self-wedges here while
    /// siblings proceed.
    pub(super) fn admit(
        &mut self,
        conversation_id: u64,
        stream_id: u32,
        now: Instant,
    ) -> Result<(), ServerError> {
        // Sub-cap: pending + tombstones for THIS conversation.
        let per_conversation = self
            .entries
            .iter()
            .filter(|entry| entry.conversation_id == conversation_id)
            .count();
        if per_conversation >= self.max_per_conversation {
            return Err(ServerError::ConnectionCapReached {
                operation: "conversation reply".to_owned(),
                cap: "max_pending_replies_per_conversation",
                limit: self.max_per_conversation,
            });
        }
        // Connection table: PENDING entries only (tombstones do not count here).
        let per_connection_pending = self
            .entries
            .iter()
            .filter(|entry| entry.state == EntryState::Pending)
            .count();
        if per_connection_pending >= self.max_per_connection {
            return Err(ServerError::ConnectionCapReached {
                operation: "conversation reply".to_owned(),
                cap: "max_pending_conversation_replies_per_connection",
                limit: self.max_per_connection,
            });
        }
        let op_id = self.next_op_id;
        self.next_op_id += 1;
        self.entries.push(PendingReplyEntry {
            op_id,
            conversation_id,
            stream_id,
            deadline: now + self.reply_timeout,
            state: EntryState::Pending,
        });
        Ok(())
    }

    /// Correlates a reply that became available for `conversation_id` against the
    /// OLDEST entry for that conversation (FIFO).
    ///
    /// - Oldest is `Pending`: it is removed and the correlated reply frame
    ///   returned to write on the connection's slice.
    /// - Oldest is a `Tombstone`: this is the timed-out request's late reply — the
    ///   tombstone is consumed (freed) and the reply DISCARDED (`None`). A late
    ///   reply can therefore never FIFO-match a younger entry.
    /// - No entry: the reply is discarded (`None`) — nothing correlates it.
    pub(super) fn match_reply(
        &mut self,
        conversation_id: u64,
        reply: MessageEnvelope,
    ) -> Option<Frame> {
        let index = self.oldest_index_for(conversation_id)?;
        let entry = self.entries.remove(index);
        match entry.state {
            EntryState::Pending => Some(Frame::ConversationMessage {
                flags: CONVERSATION_REPLY_REQUESTED_FLAG,
                stream_id: entry.stream_id,
                conversation_id: entry.conversation_id,
                envelope: reply,
            }),
            // The late reply for a timed-out request: consume the tombstone, drop
            // the reply. Never delivered late, never mis-correlated to a younger
            // request.
            EntryState::Tombstone => None,
        }
    }

    /// DEADLINE-CHECK SEAM (R1(vi) expiry half). Converts every `Pending` entry
    /// whose deadline has passed into a `Tombstone` and returns the timeout error
    /// frame to write for each, in FIFO order.
    ///
    /// Called each slice under the busy loop. PARK-FLIP: the park-flip adds a
    /// timer-driven `READY` wake at each entry's deadline (contract R1(vi) as
    /// amended) so a parked connection with zero other traffic still wakes to
    /// write the timeout — this seam is where that wake feeds in; the tombstone
    /// transition and frame generation stay exactly here.
    pub(super) fn expire_due(&mut self, now: Instant) -> Vec<Frame> {
        let mut frames = Vec::new();
        for entry in &mut self.entries {
            if entry.state == EntryState::Pending && entry.deadline <= now {
                entry.state = EntryState::Tombstone;
                frames.push(reply_timeout_frame(entry.stream_id, entry.conversation_id));
            }
        }
        frames
    }

    /// Conversations that currently hold at least one `Pending` entry — the set
    /// the connection slice polls for available replies. Bounded by the caps, so
    /// this is a small scan, not an unbounded one.
    pub(super) fn conversations_awaiting_reply(&self) -> Vec<u64> {
        let mut ids: Vec<u64> = self
            .entries
            .iter()
            .filter(|entry| entry.state == EntryState::Pending)
            .map(|entry| entry.conversation_id)
            .collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// Conversation-close sweep: removes ALL entries (pending and tombstone) for a
    /// closing conversation — one of the two tombstone-reclamation triggers. A
    /// pending entry swept here had its reply never arrive; the client already saw
    /// (or will not need) the reply because the conversation is gone.
    pub(super) fn remove_conversation(&mut self, conversation_id: u64) {
        self.entries
            .retain(|entry| entry.conversation_id != conversation_id);
    }

    /// Connection-finalization cancel (§1.2(5)): drops every entry. Called before
    /// conversation actors are torn down, so no entry outlives its connection.
    pub(super) fn cancel_all(&mut self) {
        self.entries.clear();
    }

    /// Total entries (pending + tombstone) — test/observability.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Pending entries for `conversation_id` (test/observability).
    #[cfg(test)]
    pub(super) fn pending_for(&self, conversation_id: u64) -> usize {
        self.entries
            .iter()
            .filter(|entry| {
                entry.conversation_id == conversation_id && entry.state == EntryState::Pending
            })
            .count()
    }

    /// Tombstones for `conversation_id` (test/observability).
    #[cfg(test)]
    pub(super) fn tombstones_for(&self, conversation_id: u64) -> usize {
        self.entries
            .iter()
            .filter(|entry| {
                entry.conversation_id == conversation_id && entry.state == EntryState::Tombstone
            })
            .count()
    }

    /// Index of the oldest entry (smallest `op_id`) for `conversation_id`. Keyed on
    /// the monotonic `op_id` rather than Vec position, so FIFO order is robust to
    /// any future reordering of the backing store — a late reply always consumes
    /// the genuinely oldest entry, never a younger one.
    fn oldest_index_for(&self, conversation_id: u64) -> Option<usize> {
        self.entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| entry.conversation_id == conversation_id)
            .min_by_key(|(_, entry)| entry.op_id)
            .map(|(index, _)| index)
    }
}

/// Builds the reply-timeout error frame for a conversation stream — the same
/// `ConversationError` vocabulary the old in-slice drain produced on timeout, so
/// the client sees an unchanged error shape.
fn reply_timeout_frame(stream_id: u32, conversation_id: u64) -> Frame {
    Frame::ConversationError {
        flags: 0,
        stream_id,
        conversation_id,
        reason_code: SERVER_ERROR_CODE,
        message: Some(
            "conversation reply timed out: no participant reply arrived within the deadline"
                .to_owned(),
        ),
    }
}

/// Renders a cap-refusal into a `ConversationError` frame for the reply-requested
/// frame that was refused, so the client learns its request was not admitted.
pub(super) fn cap_refusal_frame(
    stream_id: u32,
    conversation_id: u64,
    error: &ServerError,
) -> Frame {
    Frame::ConversationError {
        flags: 0,
        stream_id,
        conversation_id,
        reason_code: SERVER_ERROR_CODE,
        message: Some(error.to_string()),
    }
}

/// Builds a protocol reply envelope carrying `payload` under the wildcard schema,
/// mirroring how the old drain framed a participant reply (schema/causal metadata
/// are not bridged in v1).
#[cfg(test)]
pub(super) fn test_reply_envelope(payload: &[u8]) -> MessageEnvelope {
    use liminal::protocol::{CausalContext, SchemaId as ProtocolSchemaId};
    MessageEnvelope::new(
        ProtocolSchemaId::new([0; ProtocolSchemaId::WIRE_LEN]),
        CausalContext::independent(),
        payload.to_vec(),
    )
}

#[cfg(test)]
#[path = "pending_reply_tests.rs"]
mod tests;
