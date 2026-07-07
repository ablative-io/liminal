//! Per-connection process state and the inbound-frame action types shared by the
//! connection handler ([`super::process`]), the frame-application logic
//! ([`super::apply`]), and the delivery pump ([`super::delivery`]).

use std::collections::HashMap;

use liminal::protocol::Frame;

use super::conversation::ConnectionConversation;
use super::services::ConnectionSubscription;

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
}

/// Whether a decoded inbound frame still leaves the connection open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProcessStatus {
    Continue,
    Close,
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
}
