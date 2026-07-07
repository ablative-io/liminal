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
    /// Close the connection.
    Close,
}
