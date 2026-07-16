//! Process-local ownership and serialization for participant conversations.
//!
//! The registry keeps exactly one non-cloneable aggregate owner per live
//! conversation. A cell can instead hold a flush-ambiguity quarantine marker;
//! no pre-barrier replay is executable. A short map lock selects the conversation
//! cell; a distinct per-conversation lock then covers cold replay and each handler operation.
//! Different conversations therefore remain independent, while operations for
//! one conversation cannot observe or publish overlapping protocol state.
//! Cells leave the registry only through explicit event-driven
//! [`ParticipantConversationRegistry::release_if_idle`] calls. No timer,
//! background sweep, or polling loop performs retirement.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

use liminal::durability::{DurabilityError, DurableStore, bridge::BridgeError};

use super::{
    aggregate::{
        ConversationAggregateError, ConversationAggregateOpen, ParticipantConversationAggregate,
    },
    conversation_stream::ConversationStreamError,
};

/// Failure to select, cold-open, or exclusively borrow one conversation owner.
#[derive(Debug, thiserror::Error)]
pub(super) enum ConversationRegistryError {
    /// The process-wide conversation map was poisoned by a panicking caller.
    #[error("participant conversation registry lock is poisoned")]
    RegistryLockPoisoned,
    /// One conversation's exclusive owner lock was poisoned by a panicking caller.
    #[error("participant conversation {conversation_id} owner lock is poisoned")]
    ConversationLockPoisoned {
        /// Conversation whose owner can no longer be borrowed safely.
        conversation_id: u64,
    },
    /// An operation still owns or is waiting on the selected conversation cell.
    #[error("participant conversation {conversation_id} is in use")]
    ConversationInUse {
        /// Conversation whose cell still has an outstanding operation reference.
        conversation_id: u64,
    },
    /// The bounded synchronous durability bridge rejected a suspending backend.
    #[error(transparent)]
    Bridge(#[from] BridgeError),
    /// Cold replay or protocol genesis initialization failed.
    #[error(transparent)]
    Aggregate(#[from] ConversationAggregateError),
    /// Genesis append was ambiguous; executable access is quarantined.
    #[error("participant conversation {conversation_id} genesis append failed: {source}")]
    AppendFailed {
        /// Conversation whose owner remains non-executable until a flush barrier.
        conversation_id: u64,
        /// Original non-retried append failure.
        #[source]
        source: ConversationStreamError,
    },
    /// A prior ambiguous append still lacks a successful durability barrier.
    #[error("participant conversation {conversation_id} ambiguity flush failed: {source}")]
    AmbiguityFlushFailed {
        /// Conversation whose callbacks remain quarantined.
        conversation_id: u64,
        /// Store failure from the explicit recovery barrier.
        #[source]
        source: DurabilityError,
    },
    /// An idle owner cannot be released while its append result remains ambiguous.
    #[error("participant conversation {conversation_id} requires an ambiguity flush barrier")]
    AmbiguityFlushRequired {
        /// Conversation whose quarantined owner must remain process-local.
        conversation_id: u64,
    },
    /// Internal owner slot was unexpectedly empty after successful cold-open.
    #[error("participant conversation {conversation_id} owner is unavailable")]
    OwnerUnavailable {
        /// Conversation whose slot lost its owner.
        conversation_id: u64,
    },
    /// A consuming callback returned an aggregate for another conversation.
    #[error(
        "participant conversation replacement mismatch: expected {expected}, returned {actual}"
    )]
    ReplacementConversationMismatch {
        /// Conversation whose cell granted the consuming borrow.
        expected: u64,
        /// Conversation owned by the returned aggregate.
        actual: u64,
    },
}

#[derive(Debug)]
struct ConversationCell {
    conversation_id: u64,
    aggregate: Mutex<Option<ConversationOwner>>,
}

#[derive(Debug)]
enum ConversationOwner {
    Ready(ParticipantConversationAggregate),
    FlushAmbiguous,
}

impl ConversationCell {
    const fn new(conversation_id: u64) -> Self {
        Self {
            conversation_id,
            aggregate: Mutex::new(None),
        }
    }

    fn lock(&self) -> Result<MutexGuard<'_, Option<ConversationOwner>>, ConversationRegistryError> {
        self.aggregate
            .lock()
            .map_err(|_| ConversationRegistryError::ConversationLockPoisoned {
                conversation_id: self.conversation_id,
            })
    }
}

/// Sole process-local registry for participant conversation aggregates.
///
/// The type is intentionally not cloneable. Runtime owners share it through one
/// `Arc`, while each map entry retains the only executable
/// `ParticipantConversationAggregate` value for that conversation or its
/// non-executable flush-ambiguity quarantine marker.
#[derive(Debug)]
pub(super) struct ParticipantConversationRegistry {
    store: Arc<dyn DurableStore>,
    conversations: Mutex<HashMap<u64, Arc<ConversationCell>>>,
}

impl ParticipantConversationRegistry {
    /// Creates an empty live-owner registry over the server's shared store.
    #[must_use]
    pub(super) fn new(store: Arc<dyn DurableStore>) -> Self {
        Self {
            store,
            conversations: Mutex::new(HashMap::new()),
        }
    }

    /// Exclusively borrows one cold-opened aggregate for a synchronous handler.
    ///
    /// Cold replay occurs at most once concurrently for a conversation. The
    /// handler receives the sole mutable aggregate and cannot clone or replace
    /// protocol state. Its return value is passed through unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`ConversationRegistryError`] for poisoned ownership, cold-open
    /// failure, or an ambiguous genesis append. An ambiguous owner is quarantined:
    /// the next explicit operation must first complete a successful store flush,
    /// then cold-replay durable reality before its callback can run.
    pub(super) fn with_conversation<R>(
        &self,
        conversation_id: u64,
        operation: impl FnOnce(&mut ParticipantConversationAggregate) -> R,
    ) -> Result<R, ConversationRegistryError> {
        let cell = self.cell(conversation_id)?;
        let mut owner = cell.lock()?;
        self.ensure_open(conversation_id, &mut owner)?;
        let aggregate = owner
            .as_mut()
            .and_then(|owner| match owner {
                ConversationOwner::Ready(aggregate) => Some(aggregate),
                ConversationOwner::FlushAmbiguous => None,
            })
            .ok_or(ConversationRegistryError::OwnerUnavailable { conversation_id })?;
        let result = operation(aggregate);
        drop(owner);
        Ok(result)
    }

    /// Consumes and replaces one aggregate under its exclusive conversation lock.
    ///
    /// The callback must return the replacement aggregate together with its
    /// result. This shape supports protocol transitions whose APIs consume state
    /// without cloning or temporarily publishing an absent ready owner. A prior
    /// append failure is resolved by the registry's quarantine barrier and fresh
    /// cold replay before this callback is entered.
    ///
    /// # Errors
    ///
    /// Returns [`ConversationRegistryError`] under the same conditions as
    /// [`Self::with_conversation`]. The callback is not invoked on those errors.
    pub(super) fn consume_replace<R>(
        &self,
        conversation_id: u64,
        operation: impl FnOnce(
            ParticipantConversationAggregate,
        ) -> (ParticipantConversationAggregate, R),
    ) -> Result<R, ConversationRegistryError> {
        let cell = self.cell(conversation_id)?;
        let mut owner = cell.lock()?;
        self.ensure_open(conversation_id, &mut owner)?;
        let aggregate = owner
            .take()
            .and_then(|owner| match owner {
                ConversationOwner::Ready(aggregate) => Some(aggregate),
                ConversationOwner::FlushAmbiguous => None,
            })
            .ok_or(ConversationRegistryError::OwnerUnavailable { conversation_id })?;
        let (replacement, result) = operation(aggregate);
        let actual = replacement.conversation_id();
        if actual != conversation_id {
            drop(owner);
            return Err(ConversationRegistryError::ReplacementConversationMismatch {
                expected: conversation_id,
                actual,
            });
        }
        *owner = Some(ConversationOwner::Ready(replacement));
        drop(owner);
        Ok(result)
    }

    /// Explicitly removes and drops one idle live-owner cell.
    ///
    /// The caller invokes this from an event-driven lifecycle boundary after the
    /// conversation no longer needs a process-local owner. The registry map lock
    /// remains held while the cell's strong-reference count is checked, the entry
    /// is removed, and its final map-owned [`Arc`] is dropped. A concurrent lookup
    /// therefore cannot install a replacement until the old owner is gone. No
    /// timer, polling loop, or background sweep participates in release.
    ///
    /// Returns `Ok(false)` when the conversation is absent and `Ok(true)` after an
    /// idle cell is removed.
    ///
    /// # Errors
    ///
    /// Returns [`ConversationRegistryError::ConversationInUse`] while any
    /// operation holds or waits with a clone of the cell,
    /// [`ConversationRegistryError::AmbiguityFlushRequired`] while the cell guards
    /// an ambiguous append, or [`ConversationRegistryError::RegistryLockPoisoned`]
    /// if registry ownership can no longer be inspected safely.
    pub(super) fn release_if_idle(
        &self,
        conversation_id: u64,
    ) -> Result<bool, ConversationRegistryError> {
        let mut conversations = self
            .conversations
            .lock()
            .map_err(|_| ConversationRegistryError::RegistryLockPoisoned)?;
        let Some(cell) = conversations.get(&conversation_id) else {
            drop(conversations);
            return Ok(false);
        };
        if Arc::strong_count(cell) != 1 {
            drop(conversations);
            return Err(ConversationRegistryError::ConversationInUse { conversation_id });
        }
        let owner = cell.lock()?;
        if matches!(owner.as_ref(), Some(ConversationOwner::FlushAmbiguous)) {
            drop(owner);
            drop(conversations);
            return Err(ConversationRegistryError::AmbiguityFlushRequired { conversation_id });
        }
        drop(owner);
        let Some(removed) = conversations.remove(&conversation_id) else {
            drop(conversations);
            return Ok(false);
        };
        drop(removed);
        drop(conversations);
        Ok(true)
    }

    fn cell(
        &self,
        conversation_id: u64,
    ) -> Result<Arc<ConversationCell>, ConversationRegistryError> {
        let mut conversations = self
            .conversations
            .lock()
            .map_err(|_| ConversationRegistryError::RegistryLockPoisoned)?;
        Ok(Arc::clone(
            conversations
                .entry(conversation_id)
                .or_insert_with(|| Arc::new(ConversationCell::new(conversation_id))),
        ))
    }

    fn ensure_open(
        &self,
        conversation_id: u64,
        owner: &mut Option<ConversationOwner>,
    ) -> Result<(), ConversationRegistryError> {
        self.clear_flush_quarantine(conversation_id, owner)?;
        if matches!(
            owner.as_ref(),
            Some(ConversationOwner::Ready(aggregate)) if aggregate.genesis_validated()
        ) {
            return Ok(());
        }
        let opened = match owner.take() {
            Some(ConversationOwner::Ready(reloaded)) => {
                match liminal::durability::bridge::block_on(reloaded.resume_open())? {
                    Ok(opened) => opened,
                    Err(error) => {
                        if matches!(
                            error,
                            ConversationAggregateError::ReloadAfterAppendFailure { .. }
                        ) {
                            *owner = Some(ConversationOwner::FlushAmbiguous);
                        }
                        return Err(ConversationRegistryError::Aggregate(error));
                    }
                }
            }
            Some(ConversationOwner::FlushAmbiguous) => {
                return Err(ConversationRegistryError::OwnerUnavailable { conversation_id });
            }
            None => liminal::durability::bridge::block_on(ParticipantConversationAggregate::open(
                Arc::clone(&self.store),
                conversation_id,
            ))?
            .map_err(|error| {
                if matches!(
                    error,
                    ConversationAggregateError::ReloadAfterAppendFailure { .. }
                ) {
                    *owner = Some(ConversationOwner::FlushAmbiguous);
                }
                ConversationRegistryError::Aggregate(error)
            })?,
        };
        match opened {
            ConversationAggregateOpen::Ready(aggregate) => {
                *owner = Some(ConversationOwner::Ready(aggregate));
                Ok(())
            }
            ConversationAggregateOpen::AppendFailed(failure) => {
                let source = failure.into_error_discarding_reload();
                *owner = Some(ConversationOwner::FlushAmbiguous);
                Err(ConversationRegistryError::AppendFailed {
                    conversation_id,
                    source,
                })
            }
        }
    }

    fn clear_flush_quarantine(
        &self,
        conversation_id: u64,
        owner: &mut Option<ConversationOwner>,
    ) -> Result<(), ConversationRegistryError> {
        if !matches!(owner.as_ref(), Some(ConversationOwner::FlushAmbiguous)) {
            return Ok(());
        }
        let flush = liminal::durability::bridge::block_on(self.store.flush())?;
        if let Err(source) = flush {
            return Err(ConversationRegistryError::AmbiguityFlushFailed {
                conversation_id,
                source,
            });
        }
        // The pre-barrier reload is deliberately discarded. An append can be
        // read-visible before flush, or hidden until the recovery flush; neither
        // observation proves which bytes became durable. Cold replay after the
        // successful barrier is the sole state admitted to a callback.
        *owner = None;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn live_cell_count(&self) -> Result<usize, ConversationRegistryError> {
        self.conversations
            .lock()
            .map(|conversations| conversations.len())
            .map_err(|_| ConversationRegistryError::RegistryLockPoisoned)
    }

    #[cfg(test)]
    pub(super) fn owner_lock_is_held(
        &self,
        conversation_id: u64,
    ) -> Result<bool, ConversationRegistryError> {
        let cell = self.cell(conversation_id)?;
        match cell.aggregate.try_lock() {
            Ok(_owner) => Ok(false),
            Err(std::sync::TryLockError::WouldBlock) => Ok(true),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                Err(ConversationRegistryError::ConversationLockPoisoned { conversation_id })
            }
        }
    }
}
