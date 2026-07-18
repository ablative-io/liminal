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

#[derive(Clone, Copy, Debug, thiserror::Error)]
pub enum ParticipantPublicationError {
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
pub struct ParticipantPublicationInbox {
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
        drop(inbox);
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
pub struct ParticipantPublicationRegistry {
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
        drop(registrations);
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
            let weak_inbox = Weak::clone(&handle.inbox);
            let waker = handle.waker.clone();
            drop(registrations);
            (weak_inbox, waker)
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use liminal_protocol::wire::ConnectionIncarnation;

    use super::{ParticipantPublicationInbox, ParticipantPublicationRegistry};
    use crate::server::connection::ReadyWaker;

    #[test]
    fn parked_connection_wakes_on_outbox_and_no_polling_occurs()
    -> Result<(), Box<dyn std::error::Error>> {
        let incarnation = ConnectionIncarnation::new(12, 34);
        let wake_count = Arc::new(AtomicU64::new(0));
        let registry = ParticipantPublicationRegistry::default();
        let inbox = ParticipantPublicationInbox::new(3);
        registry.register(
            incarnation,
            &inbox,
            ReadyWaker::for_test(Arc::clone(&wake_count)),
        )?;

        assert!(!inbox.has_pending()?);
        assert!(registry.notify(incarnation, 7)?);
        assert_eq!(wake_count.load(Ordering::SeqCst), 1);
        assert!(inbox.has_pending()?);

        // Duplicate and additional ready conversations coalesce behind the one
        // empty-to-nonempty wake; no repeated probe or timer drives progress.
        assert!(registry.notify(incarnation, 7)?);
        assert!(registry.notify(incarnation, 8)?);
        assert_eq!(wake_count.load(Ordering::SeqCst), 1);
        assert_eq!(inbox.take_ready()?, vec![7, 8]);
        assert!(!inbox.has_pending()?);

        // This notification models the execute-to-wait race: it lands after a
        // drain but before the process final probe. The non-consuming probe sees
        // it and the edge fires exactly one new READY.
        assert!(registry.notify(incarnation, 9)?);
        assert!(inbox.has_pending()?);
        assert_eq!(wake_count.load(Ordering::SeqCst), 2);
        assert_eq!(inbox.take_ready()?, vec![9]);
        assert!(!inbox.has_pending()?);

        let idle_count = wake_count.load(Ordering::SeqCst);
        assert_eq!(wake_count.load(Ordering::SeqCst), idle_count);
        registry.deregister(incarnation);
        assert!(!registry.notify(incarnation, 10)?);
        assert_eq!(wake_count.load(Ordering::SeqCst), idle_count);
        Ok(())
    }
}
