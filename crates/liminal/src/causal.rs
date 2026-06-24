pub mod orderer;

pub use orderer::{CausalOrderer, OrderedEnvelope};

use uuid::Uuid;

/// Unique identifier for a message in a causal chain.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MessageId(Uuid);

impl MessageId {
    /// Generates a new message identifier.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Wraps an existing UUID as a message identifier.
    #[must_use]
    pub const fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Returns the underlying UUID.
    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

/// Causal metadata used to relate a message to an optional parent message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CausalContext {
    /// Parent message that must causally precede this message, if any.
    pub parent: Option<MessageId>,
    /// Full causal parent chain ordered as parent, grandparent, and so on.
    pub parent_chain: Vec<MessageId>,
}

impl CausalContext {
    /// Creates an independent context with no parent message.
    #[must_use]
    pub const fn root() -> Self {
        Self {
            parent: None,
            parent_chain: Vec::new(),
        }
    }

    /// Creates a context for a message that directly follows `parent`, with a
    /// depth-1 chain (`parent_chain == [parent]`).
    ///
    /// WARNING: this records ONLY the immediate parent, so
    /// [`happened_before`](Self::happened_before) will not see `parent`'s own
    /// ancestors. Use this only when `parent` is a chain root (no ancestors of
    /// its own). When the parent itself has a causal context, build the child
    /// with [`child_of_context`](Self::child_of_context) so transitive
    /// happens-before is preserved — otherwise causal ordering is silently
    /// truncated to one hop.
    #[must_use]
    pub fn child_of(parent: MessageId) -> Self {
        Self::from_parent_chain(vec![parent])
    }

    /// Creates a child context by prepending `parent` to the parent's own causal chain.
    #[must_use]
    pub fn child_of_context(parent: MessageId, parent_context: &Self) -> Self {
        let mut parent_chain = Vec::with_capacity(parent_context.parent_chain.len() + 1);
        parent_chain.push(parent);
        parent_chain.extend_from_slice(&parent_context.parent_chain);
        Self::from_parent_chain(parent_chain)
    }

    /// Creates a context from an explicit parent chain ordered parent to oldest ancestor.
    #[must_use]
    pub fn from_parent_chain(parent_chain: Vec<MessageId>) -> Self {
        let parent = parent_chain.first().copied();
        Self {
            parent,
            parent_chain,
        }
    }

    /// Returns the full parent chain ordered parent to oldest ancestor.
    #[must_use]
    pub fn parent_chain(&self) -> &[MessageId] {
        &self.parent_chain
    }

    /// Returns whether `ancestor_id` causally happened before this context's message.
    #[must_use]
    pub fn happened_before(&self, ancestor_id: MessageId) -> bool {
        self.parent_chain.contains(&ancestor_id)
    }
}

impl Default for CausalContext {
    fn default() -> Self {
        Self::root()
    }
}

#[cfg(test)]
mod tests {
    use super::{CausalContext, MessageId};

    #[test]
    fn root_context_has_no_parent() {
        assert_eq!(CausalContext::root().parent, None);
        assert!(CausalContext::root().parent_chain().is_empty());
    }

    #[test]
    fn child_context_carries_parent_reference() {
        let parent = MessageId::new();
        let context = CausalContext::child_of(parent);

        assert_eq!(context.parent, Some(parent));
        assert_eq!(context.parent_chain(), &[parent]);
    }

    #[test]
    fn generated_message_ids_are_unique() {
        let first = MessageId::new();
        let second = MessageId::new();

        assert_ne!(first, second);
    }

    #[test]
    fn happened_before_follows_full_parent_chain() {
        let a_id = MessageId::new();
        let a_context = CausalContext::root();

        let b_id = MessageId::new();
        let b_context = CausalContext::child_of(a_id);

        let c_id = MessageId::new();
        let c_context = CausalContext::child_of_context(b_id, &b_context);

        let d_context = CausalContext::root();

        assert!(c_context.happened_before(a_id));
        assert!(c_context.happened_before(b_id));
        assert_eq!(c_context.parent_chain(), &[b_id, a_id]);
        assert!(!a_context.happened_before(c_id));
        assert!(!d_context.happened_before(a_id));
    }
}
