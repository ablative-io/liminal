//! Typed commands a [`super::ChannelActorCore`] services on its mailbox.
//!
//! Each command carries owned values plus a `SyncSender` reply channel: the
//! handle pushes a command onto the shared queue and wakes the process with a
//! plain atom; the reply travels back over the per-command channel. The payload
//! never crosses a beamr term boundary — identical to haematite's `ShardCommand`
//! and the conversation actor's `QueuedCommand`.

use std::sync::mpsc::SyncSender;

use serde_json::Value;

use crate::causal::CausalContext;
use crate::channel::schema::{SchemaId, SchemaValidationError};
use crate::channel::subscription::{SubscriberRegistration, SubscriptionPredicate};
use crate::envelope::PublisherId;
use crate::error::LiminalError;

/// A queued channel command: a monotonic id (so a failed wake can be rolled
/// back off the queue) plus the typed request.
pub struct ChannelCommand {
    pub id: u64,
    pub kind: ChannelCommandKind,
}

/// One subscriber's name + count, returned by `ListSubscribers`.
pub type SubscriberSummary = Vec<u64>;

/// The typed channel requests. Each carries a reply sender; binary payloads
/// travel as owned `Vec<u8>`, never as beamr terms.
pub enum ChannelCommandKind {
    /// Re-establish the actor's links to every surviving subscriber pid. Run
    /// once from inside the freshly-spawned process context on boot/restart, so
    /// a restarted actor regains EXIT detection for subscribers that outlived
    /// the crash (R2/R4). Must run on the process side because linking to an
    /// already-existing pid requires the process's `link_facility`.
    Boot {
        reply: SyncSender<Result<(), LiminalError>>,
    },
    /// Validate, normalise, wrap, and fan a payload out to matching subscribers.
    Publish {
        payload: Vec<u8>,
        publisher_id: PublisherId,
        causal_context: Option<CausalContext>,
        reply: SyncSender<Result<(), LiminalError>>,
    },
    /// Register a subscriber (already-spawned process) and link to its pid.
    Subscribe {
        registration: SubscriberRegistration,
        reply: SyncSender<Result<(), LiminalError>>,
    },
    /// Unlink and remove a subscriber by pid (caller-driven unsubscribe).
    Unsubscribe {
        pid: u64,
        reply: SyncSender<Result<(), LiminalError>>,
    },
    /// Evolve the actor-owned schema by adding a defaulted field.
    Evolve {
        name: String,
        field_schema: Value,
        default: Value,
        reply: SyncSender<Result<SchemaId, SchemaValidationError>>,
    },
    /// Read the schema version currently owned by the actor.
    SchemaId {
        reply: SyncSender<Result<SchemaId, LiminalError>>,
    },
    /// Return the pids of all currently active subscribers.
    ListSubscribers {
        reply: SyncSender<Result<SubscriberSummary, LiminalError>>,
    },
    /// Close the channel: release subscribers and stop the process.
    Close {
        reply: SyncSender<Result<(), LiminalError>>,
    },
}

/// Build a [`SubscriptionPredicate`] error-free helper used by callers wiring
/// predicate subscriptions; kept here so the queue module owns the alias.
#[must_use]
pub fn predicate_from<F>(predicate: F) -> SubscriptionPredicate
where
    F: Fn(&crate::envelope::Envelope) -> bool + Send + Sync + 'static,
{
    std::sync::Arc::new(predicate)
}
