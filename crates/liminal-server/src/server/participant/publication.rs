//! Server-wide participant publication registry and connection-owned ready inboxes.
//!
//! Durable recipient snapshots name participant ids. Production resolves each
//! participant's current binding to a durable connection incarnation and uses
//! this registry only to wake that exact live connection. The registry stores a
//! weak inbox and a weak-scheduler READY waker, so it cannot keep a connection
//! process or scheduler alive.

use std::collections::{BTreeMap, BTreeSet};
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use liminal_protocol::wire::{
    BindingEpoch, ConnectionIncarnation, ConversationId, ParticipantDelivery, ParticipantId,
    ServerPush,
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

/// Exact refusal-arm wake transferred by the observer owner after its durable
/// `Advance` flush. It is volatile connection work, not a participant outbox
/// record, and deliberately carries no participant recipient or delivery
/// sequence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObserverPublication {
    pub(crate) conversation_id: ConversationId,
    pub(crate) refused_epoch: u64,
    pub(crate) observer_progress: u64,
}

impl ObserverPublication {
    #[must_use]
    pub(crate) const fn into_server_push(self) -> ServerPush {
        ServerPush::ObserverProgressed {
            conversation_id: self.conversation_id,
            refused_epoch: self.refused_epoch,
            observer_progress: self.observer_progress,
        }
    }
}

/// Weak exact-live-connection target captured when an observer arm is installed.
///
/// Cloning this value clones only weak/non-owning publication capability; it
/// cannot keep the connection process or inbox alive.
#[derive(Clone, Debug)]
pub struct ObserverPublicationTarget {
    inbox: Weak<Mutex<ReadyPublications>>,
    waker: ReadyWaker,
}

impl ObserverPublicationTarget {
    /// Transfers one fired payload to the exact live inbox. The queue keeps the
    /// latest payload per conversation: later progress supersedes an undrained
    /// older wake because the recovery consumer only needs the newest durable
    /// progress. A dead weak target drops only this wake.
    pub(crate) fn publish(
        &self,
        publication: ObserverPublication,
    ) -> Result<bool, ParticipantPublicationError> {
        let Some(inbox) = self.inbox.upgrade() else {
            return Ok(false);
        };
        let should_wake = {
            let mut inbox = inbox
                .lock()
                .map_err(|_| ParticipantPublicationError::InboxPoisoned)?;
            let replacing = inbox
                .observer_progressed
                .contains_key(&publication.conversation_id);
            if !replacing {
                let occupied = u64::try_from(inbox.observer_progressed.len()).unwrap_or(u64::MAX);
                if occupied >= inbox.limit {
                    return Err(ParticipantPublicationError::InboxCapacity { limit: inbox.limit });
                }
            }
            let was_empty = inbox.is_empty();
            inbox
                .observer_progressed
                .insert(publication.conversation_id, publication);
            was_empty
        };
        if should_wake {
            self.waker.fire();
        }
        Ok(true)
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
struct ReadyPublications {
    limit: u64,
    conversations: BTreeSet<ConversationId>,
    observer_progressed: BTreeMap<ConversationId, ObserverPublication>,
}

impl ReadyPublications {
    fn is_empty(&self) -> bool {
        self.conversations.is_empty() && self.observer_progressed.is_empty()
    }
}

/// All participant and observer work atomically removed for one shared push
/// slice. The pump merges these collections by conversation before applying the
/// single signed budget.
#[derive(Debug)]
pub struct ReadyPublicationBatch {
    pub(crate) conversations: Vec<ConversationId>,
    pub(crate) observer_progressed: Vec<ObserverPublication>,
}

/// Inbox strongly owned by exactly one connection process.
///
/// The value is intentionally not `Clone`: the registry receives only a weak
/// projection and cannot become another owner. Participant readiness coalesces
/// conversation ids; observer readiness retains only the latest fired payload
/// per conversation.
#[derive(Debug)]
pub struct ParticipantPublicationInbox {
    inner: Arc<Mutex<ReadyPublications>>,
}

impl ParticipantPublicationInbox {
    /// Creates the connection-owned bounded ready set from the signed semantic
    /// conversation limit.
    #[must_use]
    pub(crate) fn new(limit: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ReadyPublications {
                limit,
                conversations: BTreeSet::new(),
                observer_progressed: BTreeMap::new(),
            })),
        }
    }

    fn weak(&self) -> Weak<Mutex<ReadyPublications>> {
        Arc::downgrade(&self.inner)
    }

    /// Atomically removes all sorted, coalesced work for one shared push slice.
    pub(crate) fn take_ready(&self) -> Result<ReadyPublicationBatch, ParticipantPublicationError> {
        let mut inbox = self
            .inner
            .lock()
            .map_err(|_| ParticipantPublicationError::InboxPoisoned)?;
        Ok(ReadyPublicationBatch {
            conversations: std::mem::take(&mut inbox.conversations)
                .into_iter()
                .collect(),
            observer_progressed: std::mem::take(&mut inbox.observer_progressed)
                .into_values()
                .collect(),
        })
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

    /// Requeues budget-deferred observer payloads only for vacant conversations.
    /// Any incumbent arrived after the pump's take and supersedes the deferred
    /// payload; skipping it consumes no signed conversation capacity.
    pub(crate) fn requeue_observers(
        &self,
        publications: impl IntoIterator<Item = ObserverPublication>,
    ) -> Result<(), ParticipantPublicationError> {
        let mut inbox = self
            .inner
            .lock()
            .map_err(|_| ParticipantPublicationError::InboxPoisoned)?;
        for publication in publications {
            if inbox
                .observer_progressed
                .contains_key(&publication.conversation_id)
            {
                continue;
            }
            let occupied = u64::try_from(inbox.observer_progressed.len()).unwrap_or(u64::MAX);
            if occupied >= inbox.limit {
                return Err(ParticipantPublicationError::InboxCapacity { limit: inbox.limit });
            }
            inbox
                .observer_progressed
                .insert(publication.conversation_id, publication);
        }
        drop(inbox);
        Ok(())
    }

    /// Non-consuming final-probe fact used after socket readiness is armed.
    pub(crate) fn has_pending(&self) -> Result<bool, ParticipantPublicationError> {
        self.inner
            .lock()
            .map(|inbox| !inbox.is_empty())
            .map_err(|_| ParticipantPublicationError::InboxPoisoned)
    }
}

#[derive(Debug)]
struct ParticipantPublicationHandle {
    inbox: Weak<Mutex<ReadyPublications>>,
    waker: ReadyWaker,
}

/// Server-wide incarnation-to-connection publication registry.
#[derive(Debug, Default)]
pub struct ParticipantPublicationRegistry {
    registrations: Mutex<BTreeMap<ConnectionIncarnation, ParticipantPublicationHandle>>,
    #[cfg(test)]
    ready_fires: AtomicU64,
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

    /// Captures the weak exact-live-connection target for an accepted observer
    /// recovery arm. Missing or already-dead registrations yield no target;
    /// callers must never substitute or broadcast.
    pub(crate) fn observer_target(
        &self,
        incarnation: ConnectionIncarnation,
    ) -> Result<Option<ObserverPublicationTarget>, ParticipantPublicationError> {
        let registrations = self
            .registrations
            .lock()
            .map_err(|_| ParticipantPublicationError::InboxPoisoned)?;
        let target = registrations.get(&incarnation).and_then(|handle| {
            (handle.inbox.strong_count() > 0).then(|| ObserverPublicationTarget {
                inbox: Weak::clone(&handle.inbox),
                waker: handle.waker.clone(),
            })
        });
        drop(registrations);
        Ok(target)
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
            let was_empty = inbox.is_empty();
            inbox.conversations.insert(conversation_id);
            was_empty
        };
        if should_wake {
            #[cfg(test)]
            self.ready_fires.fetch_add(1, Ordering::SeqCst);
            waker.fire();
        }
        Ok(true)
    }

    #[cfg(test)]
    pub(crate) fn ready_fire_count(&self) -> u64 {
        self.ready_fires.load(Ordering::SeqCst)
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
        let ready = inbox.take_ready()?;
        assert_eq!(ready.conversations, vec![7, 8]);
        assert!(ready.observer_progressed.is_empty());
        assert!(!inbox.has_pending()?);

        // This notification models the execute-to-wait race: it lands after a
        // drain but before the process final probe. The non-consuming probe sees
        // it and the edge fires exactly one new READY.
        assert!(registry.notify(incarnation, 9)?);
        assert!(inbox.has_pending()?);
        assert_eq!(wake_count.load(Ordering::SeqCst), 2);
        let ready = inbox.take_ready()?;
        assert_eq!(ready.conversations, vec![9]);
        assert!(ready.observer_progressed.is_empty());
        assert!(!inbox.has_pending()?);

        let idle_count = wake_count.load(Ordering::SeqCst);
        assert_eq!(wake_count.load(Ordering::SeqCst), idle_count);
        registry.deregister(incarnation);
        assert!(!registry.notify(incarnation, 10)?);
        assert_eq!(wake_count.load(Ordering::SeqCst), idle_count);
        Ok(())
    }
}
