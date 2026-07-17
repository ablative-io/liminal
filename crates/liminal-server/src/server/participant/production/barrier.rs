//! Shared aggregate-barrier plumbing for the production operation arms.
//!
//! Every committing arm (enrollment, attach, detach) resolves its pending A3
//! aggregate barrier through [`commit_through_barrier`]: the live mode
//! appends the operation's stored inputs plus canonical event bytes at the
//! optimistic head before the shell advances; the replay mode re-mints the
//! canonical bytes and cross-checks them against the stored entry, so byte
//! drift between live and replayed decisions fails loudly.

use liminal_protocol::lifecycle::{
    CapacityCounter, ConnectionConversationCapacityCommit, ConnectionConversationTracking,
    ReceiptDeadlines, SemanticConnectionCapacityDecision, select_semantic_connection_capacity,
};
use liminal_protocol::wire::{ConnectionIncarnation, ServerValue};

use super::log::StoredOperation;
use super::state::{DurableAppend, StateError};

/// Connection-scoped and configured facts supplied to each operation.
#[derive(Clone, Copy, Debug)]
pub(super) struct OperationFacts {
    /// Durable incarnation of the receiving connection.
    pub(super) receiving_incarnation: ConnectionIncarnation,
    /// Admitted wall-clock read for deadline derivation and receipt phases.
    pub(super) now_ms: u64,
    /// Configured per-conversation identity limit `I` (the contract's
    /// half-open `0..=I` bound on permanent participant ordinals).
    pub(super) identity_slots: u64,
    /// Configured secret-bearing receipt TTL.
    pub(super) attach_receipt_ttl_ms: u64,
    /// Configured non-secret provenance TTL.
    pub(super) receipt_provenance_ttl_ms: u64,
    /// Signed R-D1 stage-8 identity/receipt capacity limits.
    pub(super) receipt_limits: ReceiptCapacityLimits,
    /// Whether the receiving connection already tracks this conversation.
    pub(super) connection_tracking: ConnectionConversationTracking,
    /// Signed connection-conversation limit with current occupancy.
    pub(super) connection_capacity: CapacityCounter,
}

/// Signed stage-8 capacity limits, straight from validated configuration.
#[derive(Clone, Copy, Debug)]
pub(super) struct ReceiptCapacityLimits {
    /// Server-wide identity-slot limit.
    pub(super) identity_server: u64,
    /// Server-wide live-receipt cap.
    pub(super) live_receipts_server: u64,
    /// Per-participant live-receipt cap.
    pub(super) live_receipts_per_participant: u64,
    /// Server-wide provenance-fingerprint cap.
    pub(super) provenance_server: u64,
    /// Per-conversation provenance-fingerprint cap.
    pub(super) provenance_per_conversation: u64,
    /// Per-participant provenance-fingerprint cap.
    pub(super) provenance_per_participant: u64,
}

impl OperationFacts {
    /// Derives the receipt/provenance deadline pair from the admitted clock.
    pub(super) fn deadlines(&self) -> Result<ReceiptDeadlines, StateError> {
        ReceiptDeadlines::try_from_ttls(
            self.now_ms,
            self.attach_receipt_ttl_ms,
            self.receipt_provenance_ttl_ms,
        )
        .map_err(|error| {
            StateError::invariant(format!("validated TTL configuration rejected: {error:?}"))
        })
    }

    /// Runs the crate's stage-6 semantic connection-conversation capacity
    /// selector for this operation's connection facts.
    pub(super) const fn semantic_connection_capacity(&self) -> SemanticConnectionCapacityDecision {
        select_semantic_connection_capacity(self.connection_tracking, self.connection_capacity)
    }
}

/// One operation arm's response paired with its connection-tracking effect.
///
/// `newly_tracked` is `true` exactly when the operation COMMITTED and its
/// stage-6 capacity commit reserved a new connection-conversation slot; every
/// refusal and replay carries `false`, mirroring the crate's rule that a
/// refused operation leaves the connection counter unchanged.
#[derive(Debug)]
pub(super) struct ArmOutcome {
    /// Protocol-owned response value.
    pub(super) value: ServerValue,
    /// Whether the caller must install this conversation's connection slot.
    pub(super) newly_tracked: bool,
}

impl ArmOutcome {
    /// A refusal or replay: the connection's dispatch map is unchanged.
    pub(super) const fn respond(value: ServerValue) -> Self {
        Self {
            value,
            newly_tracked: false,
        }
    }

    /// A committed operation carrying its stage-6 capacity commit.
    pub(super) const fn committed(
        value: ServerValue,
        capacity: ConnectionConversationCapacityCommit,
    ) -> Self {
        Self {
            value,
            newly_tracked: capacity.newly_tracked(),
        }
    }
}

/// One barrier resolution mode: live durable append or replay byte-check.
#[derive(Clone, Copy)]
pub(super) enum CommitMode<'a> {
    /// Append the operation at the optimistic head, then commit.
    Live(&'a dyn DurableAppend),
    /// Cross-check re-minted canonical bytes against the stored entry.
    Replay {
        /// Canonical event bytes read from the durable entry.
        stored_event: &'a [u8],
        /// Durable log sequence of the entry (for drift diagnostics).
        sequence: u64,
    },
}

/// Resolves one pending aggregate barrier through the selected mode.
pub(super) fn commit_through_barrier<T>(
    barrier: liminal_protocol::lifecycle::AggregateOperationCommit<T>,
    mode: CommitMode<'_>,
    next_log_sequence: u64,
    make_operation: &dyn Fn(Vec<u8>) -> StoredOperation,
) -> Result<(liminal_protocol::lifecycle::ParticipantConversation, T), StateError> {
    let event = barrier.event().encode_canonical();
    match mode {
        CommitMode::Live(appender) => {
            let operation = make_operation(event);
            appender.append(&operation, next_log_sequence)?;
        }
        CommitMode::Replay {
            stored_event,
            sequence,
        } => {
            if event != stored_event {
                return Err(StateError::ReplayedEventDrift { sequence });
            }
        }
    }
    Ok(barrier.commit())
}
