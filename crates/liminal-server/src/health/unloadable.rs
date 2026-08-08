//! The operator read surface for conversations this node refused to load.
//!
//! Containment (`server/participant/production/handler.rs`) answers a
//! conversation it cannot load by refusing that ONE conversation, naming it,
//! and serving every other one. Naming is only half a surface while the record
//! it writes has no reader: an operator deciding whether a node may be
//! restarted has to ask the running node which conversations it is refusing,
//! and the node has to be able to answer.
//!
//! This module is that answer's shape and its plumbing, and it is PULL-ONLY.
//! The participant handler writes into [`UnloadableConversationRecord`] on the
//! two paths that already record a refusal; the health endpoint reads a
//! snapshot only when an operator scrapes it. Nothing here starts a thread,
//! arms a timer, or samples anything, so the endpoint's zero-idle-wake
//! property (W4 leg 2, LAW-1) is untouched: a node nobody scrapes does no work
//! at all for this surface.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use liminal_protocol::wire::ConversationId;

/// One conversation the node refused to load, as an operator reads it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct UnloadableConversation {
    /// Conversation whose durable state could not be loaded.
    pub conversation_id: ConversationId,
    /// Stable refusal class, so a consumer discriminates on a field instead of
    /// on a substring of `reason` — the rendered text is a diagnostic and is
    /// allowed to move.
    pub class: &'static str,
    /// The load failure's own text, exactly as the refusal carries it.
    pub reason: String,
}

/// The body served by `GET /unloadable-conversations`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct UnloadableConversationsStatus {
    /// Whether a participant record is attached to this server at all.
    ///
    /// A node with no participant configured and a node whose participant has
    /// refused nothing both report `count: 0`; this field is the only thing
    /// separating them. Without it a zero reads as a clean node when it may
    /// instead mean the surface is looking at nothing.
    pub participant_installed: bool,
    /// How many conversations are refused right now.
    pub count: usize,
    /// The refused conversations, in conversation-id order.
    pub conversations: Vec<UnloadableConversation>,
}

/// The write side: the record a participant handler maintains.
///
/// Cloning shares the one record (an `Arc`), which is exactly what lets the
/// handler keep writing while the health endpoint reads.
#[derive(Clone, Debug, Default)]
pub struct UnloadableConversationRecord {
    entries: Arc<Mutex<BTreeMap<ConversationId, UnloadableConversation>>>,
}

impl UnloadableConversationRecord {
    /// Retains one refusal, replacing any earlier refusal of the same
    /// conversation.
    ///
    /// Answers `false` when the record's lock is poisoned and the refusal was
    /// therefore NOT retained. The caller must report that: a refusal that was
    /// reported but not retained is a weaker state than a retained one, and an
    /// operator reading this surface must not be told a shorter story than the
    /// log tells.
    #[must_use]
    pub fn record(&self, entry: UnloadableConversation) -> bool {
        self.entries.lock().is_ok_and(|mut entries| {
            entries.insert(entry.conversation_id, entry);
            true
        })
    }

    /// Retires a conversation's refusal once it has actually loaded, so the
    /// surface never keeps reporting a conversation that recovered.
    ///
    /// Answers `false` when the lock is poisoned and the retirement therefore
    /// did not happen.
    #[must_use]
    pub fn retire(&self, conversation_id: ConversationId) -> bool {
        self.entries.lock().is_ok_and(|mut entries| {
            entries.remove(&conversation_id);
            true
        })
    }

    /// Every refusal currently retained, in conversation-id order.
    ///
    /// A poisoned lock is read THROUGH ([`PoisonError::into_inner`]) rather
    /// than answered with an empty set: the retained refusals are still the
    /// node's true answer, and reporting "nothing is refused" because a
    /// mutex was poisoned would be the surface's own false green. The write
    /// side deliberately does not do this — a refusal it could not retain is
    /// reported as not retained.
    #[must_use]
    pub fn snapshot(&self) -> Vec<UnloadableConversation> {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }
}

/// The read side the health endpoint serves from.
///
/// The health server binds BEFORE the participant handler exists — liveness
/// has to be answerable during startup — so the endpoint holds this slot from
/// the moment it starts and the participant's record is published into it once
/// built. Until that happens the surface says so through
/// [`UnloadableConversationsStatus::participant_installed`] rather than
/// reporting a zero that means nothing.
#[derive(Clone, Debug, Default)]
pub struct SharedUnloadableConversations {
    record: Arc<Mutex<Option<UnloadableConversationRecord>>>,
}

impl SharedUnloadableConversations {
    /// Publishes a participant's record into the surface, replacing any record
    /// installed before it.
    pub fn install(&self, record: UnloadableConversationRecord) {
        *self.record.lock().unwrap_or_else(PoisonError::into_inner) = Some(record);
    }

    /// The surface's current answer.
    #[must_use]
    pub fn status(&self) -> UnloadableConversationsStatus {
        let record = self
            .record
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        record.map_or_else(
            || UnloadableConversationsStatus {
                participant_installed: false,
                count: 0,
                conversations: Vec::new(),
            },
            |record| {
                let conversations = record.snapshot();
                UnloadableConversationsStatus {
                    participant_installed: true,
                    count: conversations.len(),
                    conversations,
                }
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SharedUnloadableConversations, UnloadableConversation, UnloadableConversationRecord,
    };

    fn refusal(conversation_id: u64) -> UnloadableConversation {
        UnloadableConversation {
            conversation_id,
            class: "internal",
            reason: "expected value at line 1 column 1".to_owned(),
        }
    }

    /// An uninstalled surface must not answer like a clean node: the zero it
    /// reports is qualified by `participant_installed`.
    #[test]
    fn an_uninstalled_surface_reports_that_it_is_looking_at_nothing() {
        let status = SharedUnloadableConversations::default().status();

        assert!(!status.participant_installed);
        assert_eq!(status.count, 0);
        assert!(status.conversations.is_empty());
    }

    /// An installed record with nothing refused is the OTHER zero, and the two
    /// are distinguishable — which is the whole reason the flag exists.
    #[test]
    fn an_installed_but_empty_record_is_a_different_zero() {
        let surface = SharedUnloadableConversations::default();
        surface.install(UnloadableConversationRecord::default());

        let status = surface.status();

        assert!(status.participant_installed);
        assert_eq!(status.count, 0);
    }

    /// The surface reads the LIVE record: a refusal recorded after installation
    /// is reported, and a retirement removes it again.
    #[test]
    fn the_surface_follows_the_record_it_was_given() {
        let record = UnloadableConversationRecord::default();
        let surface = SharedUnloadableConversations::default();
        surface.install(record.clone());

        assert!(record.record(refusal(7_201)));
        assert!(record.record(refusal(7_100)));

        let status = surface.status();
        assert!(status.participant_installed);
        assert_eq!(status.count, 2);
        // Conversation-id order, not insertion order.
        assert_eq!(
            status
                .conversations
                .iter()
                .map(|entry| entry.conversation_id)
                .collect::<Vec<_>>(),
            vec![7_100, 7_201]
        );
        assert_eq!(status.conversations[1].class, "internal");

        assert!(record.retire(7_100));
        let status = surface.status();
        assert_eq!(status.count, 1);
        assert_eq!(status.conversations[0].conversation_id, 7_201);
    }
}
