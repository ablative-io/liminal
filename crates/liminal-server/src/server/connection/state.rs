//! Per-connection process state and the inbound-frame action types shared by the
//! connection handler ([`super::process`]), the frame-application logic
//! ([`super::apply`]), and the delivery pump ([`super::delivery`]).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use liminal::channel::ConnectionInboxBudget;
use liminal::protocol::Frame;
use liminal_protocol::wire::ConnectionIncarnation;

use super::conversation::ConnectionConversation;
use super::participant_delivery::{HeldObserverHead, HeldParticipantHead};
use super::services::ConnectionSubscription;
use crate::server::participant::{
    ConnectionFateClass, ParticipantConnectionConversations, ParticipantOfferedProgress,
    ParticipantPublicationError, ParticipantPublicationInbox, ParticipantSession,
};

/// The one signed pool of exact encoded participant and observer heads held for
/// current outbound pressure.
///
/// The maps remain separate because their payload types and replacement rules
/// differ, but every vacant insertion passes through the same checked capacity
/// gate. This prevents the two push classes from each consuming the full signed
/// semantic-conversation allowance.
#[derive(Debug, Default)]
pub(super) struct HeldPushHeads {
    participant: BTreeMap<u64, HeldParticipantHead>,
    observer: BTreeMap<u64, HeldObserverHead>,
    capacity_refused: bool,
}

impl HeldPushHeads {
    pub(super) fn participant_keys(&self) -> impl Iterator<Item = &u64> {
        self.participant.keys()
    }

    pub(super) fn observer_keys(&self) -> impl Iterator<Item = &u64> {
        self.observer.keys()
    }

    pub(super) fn contains_participant(&self, conversation_id: u64) -> bool {
        self.participant.contains_key(&conversation_id)
    }

    pub(super) fn contains_observer(&self, conversation_id: u64) -> bool {
        self.observer.contains_key(&conversation_id)
    }

    pub(super) fn remove_participant(
        &mut self,
        conversation_id: u64,
    ) -> Option<HeldParticipantHead> {
        self.participant.remove(&conversation_id)
    }

    pub(super) fn remove_observer(&mut self, conversation_id: u64) -> Option<HeldObserverHead> {
        self.observer.remove(&conversation_id)
    }

    pub(super) fn try_insert_participant(
        &mut self,
        conversation_id: u64,
        head: HeldParticipantHead,
        limit: u64,
    ) -> Result<(), ParticipantPublicationError> {
        if !self.participant.contains_key(&conversation_id) {
            self.ensure_vacant_capacity(limit)?;
        }
        self.participant.insert(conversation_id, head);
        Ok(())
    }

    pub(super) fn try_insert_observer(
        &mut self,
        conversation_id: u64,
        head: HeldObserverHead,
        limit: u64,
    ) -> Result<(), ParticipantPublicationError> {
        if !self.observer.contains_key(&conversation_id) {
            self.ensure_vacant_capacity(limit)?;
        }
        self.observer.insert(conversation_id, head);
        Ok(())
    }

    fn ensure_vacant_capacity(&self, limit: u64) -> Result<(), ParticipantPublicationError> {
        let occupied = self
            .participant
            .len()
            .checked_add(self.observer.len())
            .and_then(|occupied| u64::try_from(occupied).ok())
            .ok_or(ParticipantPublicationError::InboxCapacity { limit })?;
        if occupied >= limit {
            return Err(ParticipantPublicationError::InboxCapacity { limit });
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn participant_len(&self) -> usize {
        self.participant.len()
    }

    #[cfg(test)]
    pub(super) fn observer_len(&self) -> usize {
        self.observer.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.participant.is_empty() && self.observer.is_empty()
    }

    pub(super) const fn capacity_refused(&self) -> bool {
        self.capacity_refused
    }

    pub(super) const fn mark_capacity_refused(&mut self) {
        self.capacity_refused = true;
    }

    pub(super) const fn clear_capacity_refused(&mut self) {
        self.capacity_refused = false;
    }
}

/// State a connection process carries across scheduler slices: the resources it
/// owns (subscriptions, conversations) plus the per-subscription delivery
/// sequence counters the pump advances.
#[derive(Debug, Default)]
pub(super) struct ConnectionProcessState {
    /// Whether a shutdown `Disconnect` was already enqueued for this connection,
    /// so a repeated shutdown signal never double-sends it.
    pub(super) shutdown_notification_attempted: bool,
    /// Whether this connection has cleared the auth gate. Set once a `Connect`
    /// frame passes the configured token check (or immediately, when no token is
    /// configured); consulted by [`super::apply::apply_frame`] to reject any
    /// application frame that arrives before a successful handshake. Without this
    /// flag a client that simply skips `Connect` and sends
    /// `Publish`/`Subscribe`/`WorkerRegister` would never reach `connect_response`
    /// — the only place the token is read — and would bypass the gate entirely.
    pub(super) authenticated: bool,
    /// Shared participant capability state stored by a successful connection
    /// handshake and consumed by the `liminal-protocol` inbound gate.
    pub(super) participant_session: ParticipantSession,
    /// Connection-local semantic-conversation dispatch map consumed by the
    /// participant handler's stage-6 connection-conversation capacity gate.
    /// Bounded by the signed `max_semantic_conversations_per_connection` and
    /// dropped with the connection.
    pub(super) participant_conversations: ParticipantConnectionConversations,
    /// Durable incarnation allocated and flushed before this process was spawned.
    /// `None` means the supervisor had no complete participant service installed.
    pub(super) connection_incarnation: Option<ConnectionIncarnation>,
    /// Strongly owned bounded/coalescing participant-ready inbox. The server-wide
    /// registry holds only a weak handle to this exact value.
    pub(super) participant_publication: Option<ParticipantPublicationInbox>,
    /// Whether this process installed its incarnation/inbox pair in the shared
    /// registry. Registration happens on the first slice after the host record
    /// exists and deregistration is idempotent on every cleanup path.
    pub(super) participant_publication_registered: bool,
    /// Volatile per-conversation offered progress, scoped to the exact binding
    /// epoch and advanced only after successful outbound enqueue.
    pub(super) participant_offered: BTreeMap<u64, ParticipantOfferedProgress>,
    /// Exact encoded participant and observer heads sharing one checked signed
    /// connection-conversation allowance.
    pub(super) held_pushes: HeldPushHeads,
    /// Library subscriptions owned by this connection, keyed by subscription id.
    pub(super) subscriptions: HashMap<u64, ConnectionSubscription>,
    /// Supervised conversations owned by this connection, keyed by conversation id.
    pub(super) conversations: HashMap<u64, ConnectionConversation>,
    /// Per-subscription monotonic delivery sequence, keyed by subscription id.
    ///
    /// The first `Deliver` for a subscription carries `1`; each subsequent
    /// delivery increments. Carried from day one so the future ack/resume (A1 v2
    /// credit) protocol has a stable anchor.
    pub(super) delivery_seqs: HashMap<u64, u64>,
    /// A `Deliver` frame the pump built but could not enqueue because the outbound
    /// buffer lacked headroom, keyed by subscription id. Its `delivery_seq` is
    /// already assigned, so flushing it first on the next slice preserves
    /// per-subscription order; holding it back (rather than enqueuing past the cap)
    /// is what lets a pipelined burst ride out across slices without tearing down a
    /// healthy fast-reading connection.
    pub(super) held_deliveries: HashMap<u64, Frame>,
    /// The connection's ONE shared subscription-inbox byte budget (§5), spent
    /// across ALL its subscription inboxes. Created lazily on the first subscribe
    /// (so a connection that never subscribes allocates nothing) and installed
    /// into every subscription's inbox, so the signed 4 MiB product is
    /// connection-scoped and exact.
    pub(super) inbox_budget: Option<Arc<ConnectionInboxBudget>>,
    /// R1(vi) (§1.2(3b)) per-connection pending-reply table. Reply-requested
    /// conversation frames admit an entry here instead of blocking the slice on a
    /// 5 s drain; the slice checks deadlines and drains correlated replies. The
    /// connection replaces the default-capped table with one built from the
    /// runtime's configured §5 limits at construction.
    pub(super) pending_replies: super::pending_reply::PendingReplyTable,
}

/// Whether a decoded inbound frame still leaves the connection open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProcessStatus {
    Continue,
    Close,
    CloseWithFate(ConnectionFateClass),
}

/// The connection's response to one applied inbound frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum FrameAction {
    /// Enqueue this frame back to the client.
    Respond(Frame),
    /// Consume the frame with no wire response.
    NoResponse,
    /// Enqueue `response` to the client, then tear the connection down. Used by the
    /// auth gate: a rejected handshake must both inform the client (a `ConnectError`
    /// carrying the auth reason code) and close, unlike a bare [`Self::Close`] that
    /// stays silent or a [`Self::Respond`] that leaves the connection open.
    RespondThenClose(Frame),
    /// Close the connection.
    Close,
    /// Close after durably folding the exact participant fate class.
    CloseWithFate(ConnectionFateClass),
}
