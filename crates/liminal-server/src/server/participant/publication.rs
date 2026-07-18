//! Server-wide participant publication registry and connection-owned ready inboxes.
//!
//! Durable recipient snapshots name participant ids. Production resolves each
//! participant's current binding to a durable connection incarnation and uses
//! this registry only to wake that exact live connection. The registry stores a
//! weak inbox and a weak-scheduler READY waker, so it cannot keep a connection
//! process or scheduler alive.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, Weak};

use liminal_protocol::wire::{
    BindingEpoch, ConnectionIncarnation, ConversationId, ParticipantDelivery, ParticipantId,
};

use crate::server::connection::ReadyWaker;

/// Registry/inbox invariant failure. These are internal configuration or
/// ownership faults, never transport pressure policy.

/// Connection-local volatile offer cursor for one conversation and exact
/// binding. A different binding epoch discards this progress and restarts from
/// the durable recipient acknowledgement frontier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParticipantOfferedProgress {
    pub(crate) binding_epoch: BindingEpoch,
    pub(crate) through_seq: u64,
}

/// One exact durable obligation selected for the connection's current binding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParticipantPublication {
    pub(crate) participant_id: ParticipantId,
    pub(crate) binding_epoch: BindingEpoch,
    pub(crate) delivery: ParticipantDelivery,
}

impl ParticipantPublication {
    #[must_use]
    pub(crate) const fn conversation_id(&self) -> ConversationId {
        self.delivery.conversation_id
    }

    #[must_use]
    pub(crate) const fn delivery_seq(&self) -> u64 {
        self.delivery.delivery_seq
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ParticipantPublicationError {
    /// A live incarnation was registered more than once.
    #[error("participant publication incarnation {incarnation:?} is already registered")]
    DuplicateRegistration {
        /// Durable incarnation that already owns a live inbox.
        incarnation: ConnectionIncarnation,
    },
    /// The connection-owned inbox mutex was poisoned.
    #[error("participant publication inbox is poisoned")]
    InboxPoisoned,
    /// A new ready conversation would exceed the signed connection bound.
    #[error("participant publication inbox exceeds its signed conversation bound {limit}")]
    InboxCapacity {
        /// Signed maximum semantic conversations for the connection.
        limit: u64,
    },
}

#[derive(Debug)]
struct ReadyConversations {
    limit: u64,
    conversations: BTreeSet<ConversationId>,
}

/// Inbox strongly owned by exactly one connection process.
///
/// The value is intentionally not `Clone`: the registry receives only a weak
/// projection and cannot become another owner. Readiness stores conversation
/// ids, never payload copies, and coalesces duplicate ids.
#[derive(Debug)]
pub(crate) struct ParticipantPublicationInbox {
    inner: Arc<Mutex<ReadyConversations>>,
}

impl ParticipantPublicationInbox {
    /// Creates the connection-owned bounded ready set from the signed semantic
    /// conversation limit.
    #[must_use]
    pub(crate) fn new(limit: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ReadyConversations {
                limit,
                conversations: BTreeSet::new(),
            })),
        }
    }

    fn weak(&self) -> Weak<Mutex<ReadyConversations>> {
        Arc::downgrade(&self.inner)
    }

    /// Atomically removes the sorted, duplicate-free ready-conversation set for
    /// one scheduler slice.
    pub(crate) fn take_ready(&self) -> Result<Vec<ConversationId>, ParticipantPublicationError> {
        let mut inbox = self
            .inner
            .lock()
            .map_err(|_| ParticipantPublicationError::InboxPoisoned)?;
        Ok(std::mem::take(&mut inbox.conversations)
            .into_iter()
            .collect())
    }

    /// Requeues deferred conversations after a budget-limited or held-back
    /// slice. Existing ids remain coalesced.
    pub(crate) fn requeue(
        &self,
        conversations: impl IntoIterator<Item = ConversationId>,
    ) -> Result<(), ParticipantPublicationError> {
        let mut inbox = self
            .inner
            .lock()
            .map_err(|_| ParticipantPublicationError::InboxPoisoned)?;
        for conversation_id in conversations {
            if inbox.conversations.contains(&conversation_id) {
                continue;
            }
            let occupied = u64::try_from(inbox.conversations.len()).unwrap_or(u64::MAX);
            if occupied >= inbox.limit {
                return Err(ParticipantPublicationError::InboxCapacity { limit: inbox.limit });
            }
            inbox.conversations.insert(conversation_id);
        }
        Ok(())
    }

    /// Non-consuming final-probe fact used after socket readiness is armed.
    pub(crate) fn has_pending(&self) -> Result<bool, ParticipantPublicationError> {
        self.inner
            .lock()
            .map(|inbox| !inbox.conversations.is_empty())
            .map_err(|_| ParticipantPublicationError::InboxPoisoned)
    }
}

#[derive(Debug)]
struct ParticipantPublicationHandle {
    inbox: Weak<Mutex<ReadyConversations>>,
    waker: ReadyWaker,
}

/// Server-wide incarnation-to-connection publication registry.
#[derive(Debug, Default)]
pub(crate) struct ParticipantPublicationRegistry {
    registrations: Mutex<BTreeMap<ConnectionIncarnation, ParticipantPublicationHandle>>,
}

impl ParticipantPublicationRegistry {
    /// Registers one connection-owned inbox. A stale weak entry may be replaced;
    /// a second live owner for one durable incarnation is a typed invariant fault.
    pub(crate) fn register(
        &self,
        incarnation: ConnectionIncarnation,
        inbox: &ParticipantPublicationInbox,
        waker: ReadyWaker,
    ) -> Result<(), ParticipantPublicationError> {
        let mut registrations = self
            .registrations
            .lock()
            .map_err(|_| ParticipantPublicationError::InboxPoisoned)?;
        if registrations
            .get(&incarnation)
            .is_some_and(|existing| existing.inbox.strong_count() > 0)
        {
            return Err(ParticipantPublicationError::DuplicateRegistration { incarnation });
        }
        registrations.insert(
            incarnation,
            ParticipantPublicationHandle {
                inbox: inbox.weak(),
                waker,
            },
        );
        Ok(())
    }

    /// Removes the registration at explicit process teardown. The weak handle
    /// already makes stale delivery harmless; eager removal keeps lookup exact.
    pub(crate) fn deregister(&self, incarnation: ConnectionIncarnation) {
        if let Ok(mut registrations) = self.registrations.lock() {
            registrations.remove(&incarnation);
        }
    }

    /// Coalesces one conversation into the exact live incarnation's inbox and
    /// fires READY only on the empty-to-nonempty edge.
    ///
    /// Returns `false` when the incarnation or connection process is gone.
    pub(crate) fn notify(
        &self,
        incarnation: ConnectionIncarnation,
        conversation_id: ConversationId,
    ) -> Result<bool, ParticipantPublicationError> {
        let (weak_inbox, waker) = {
            let registrations = self
                .registrations
                .lock()
                .map_err(|_| ParticipantPublicationError::InboxPoisoned)?;
            let Some(handle) = registrations.get(&incarnation) else {
                return Ok(false);
            };
            (Weak::clone(&handle.inbox), handle.waker.clone())
        };
        let Some(inbox) = weak_inbox.upgrade() else {
            self.deregister(incarnation);
            return Ok(false);
        };
        let should_wake = {
            let mut inbox = inbox
                .lock()
                .map_err(|_| ParticipantPublicationError::InboxPoisoned)?;
            if inbox.conversations.contains(&conversation_id) {
                return Ok(true);
            }
            let occupied = u64::try_from(inbox.conversations.len()).unwrap_or(u64::MAX);
            if occupied >= inbox.limit {
                return Err(ParticipantPublicationError::InboxCapacity { limit: inbox.limit });
            }
            let was_empty = inbox.conversations.is_empty();
            inbox.conversations.insert(conversation_id);
            was_empty
        };
        if should_wake {
            waker.fire();
        }
        Ok(true)
    }
}
