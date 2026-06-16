use crate::tracing::{ConversationSpan, FinishedSpan, TraceContext};

#[derive(Debug)]
pub struct Conversation {
    span: ConversationSpan,
}

impl Conversation {
    #[must_use]
    pub fn start(conversation_id: impl Into<String>) -> Self {
        Self {
            span: ConversationSpan::root(conversation_id),
        }
    }

    #[must_use]
    pub fn spawn_child(&self, conversation_id: impl Into<String>) -> Self {
        Self {
            span: self.span.child(conversation_id),
        }
    }

    #[must_use]
    pub const fn message<Payload>(&self, payload: Payload) -> ConversationMessage<Payload> {
        ConversationMessage::new(payload, self.span.message_context())
    }

    #[must_use]
    pub fn name(&self) -> &str {
        self.span.name()
    }

    #[must_use]
    pub const fn trace_context(&self) -> TraceContext {
        self.span.context()
    }

    #[must_use]
    pub const fn parent_trace_context(&self) -> Option<TraceContext> {
        self.span.parent()
    }

    #[must_use]
    pub fn finish(self) -> FinishedSpan {
        self.span.finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConversationMessage<Payload> {
    payload: Payload,
    trace_context: TraceContext,
}

impl<Payload> ConversationMessage<Payload> {
    const fn new(payload: Payload, trace_context: TraceContext) -> Self {
        Self {
            payload,
            trace_context,
        }
    }

    #[must_use]
    pub const fn trace_context(&self) -> TraceContext {
        self.trace_context
    }

    #[must_use]
    pub const fn payload(&self) -> &Payload {
        &self.payload
    }

    #[must_use]
    pub fn into_payload(self) -> Payload {
        self.payload
    }

    #[must_use]
    pub fn map<NextPayload>(
        self,
        map_payload: impl FnOnce(Payload) -> NextPayload,
    ) -> ConversationMessage<NextPayload> {
        ConversationMessage {
            payload: map_payload(self.payload),
            trace_context: self.trace_context,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Conversation;

    #[test]
    fn starting_conversation_creates_named_span_with_fresh_trace_context() {
        let first = Conversation::start("conversation-1");
        let second = Conversation::start("conversation-2");

        assert_eq!(first.name(), "conversation-1");
        assert_eq!(first.parent_trace_context(), None);
        assert_ne!(first.trace_context().trace_id(), 0);
        assert_ne!(first.trace_context().span_id(), 0);
        assert_ne!(
            first.trace_context().trace_id(),
            second.trace_context().trace_id()
        );
    }

    #[test]
    fn messages_inherit_conversation_trace_context_automatically() {
        let conversation = Conversation::start("conversation");
        let message = conversation.message("payload");

        assert_eq!(message.payload(), &"payload");
        assert_eq!(message.trace_context(), conversation.trace_context());
    }

    #[test]
    fn child_conversation_references_parent_trace_context() {
        let parent = Conversation::start("parent");
        let child = parent.spawn_child("child");

        assert_eq!(child.name(), "child");
        assert_eq!(child.parent_trace_context(), Some(parent.trace_context()));
        assert_eq!(
            child.trace_context().trace_id(),
            parent.trace_context().trace_id()
        );
        assert_ne!(
            child.trace_context().span_id(),
            parent.trace_context().span_id()
        );
    }

    #[test]
    fn message_mapping_preserves_trace_context() {
        let conversation = Conversation::start("conversation");
        let context = conversation.trace_context();
        let mapped = conversation.message(1_u8).map(u16::from);

        assert_eq!(mapped.payload(), &1_u16);
        assert_eq!(mapped.trace_context(), context);
    }
}
