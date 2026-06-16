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
}

impl CausalContext {
    /// Creates an independent context with no parent message.
    #[must_use]
    pub const fn root() -> Self {
        Self { parent: None }
    }

    /// Creates a context for a message that follows `parent` in the same causal chain.
    #[must_use]
    pub const fn child_of(parent: MessageId) -> Self {
        Self {
            parent: Some(parent),
        }
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
    }

    #[test]
    fn child_context_carries_parent_reference() {
        let parent = MessageId::new();
        let context = CausalContext::child_of(parent);

        assert_eq!(context.parent, Some(parent));
    }

    #[test]
    fn generated_message_ids_are_unique() {
        let first = MessageId::new();
        let second = MessageId::new();

        assert_ne!(first, second);
    }
}
