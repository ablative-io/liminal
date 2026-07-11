//! Capability-scoped worker front door services (D2).
//!
//! [`WorkerFrontDoorServices`] is the [`ConnectionServices`] adapter for
//! worker-only ("worker front door") deployments — aion-style hosts that use the
//! bus for worker registration, correlated server push/reply, and
//! notifier-consumed reserved publishes, and nothing else. It constructs no
//! channel supervisor, conversation supervisor, haematite database/store, dedup
//! cache, or temporary directory: it is a stateless adapter, so the only runtime
//! the worker-front-door construction path spins up is the connection supervisor's
//! own scheduler (owned by [`super::supervisor`], shared by both profiles).
//!
//! Worker registration, correlated push/reply, and the reserved observability tap
//! do NOT flow through this adapter at all — they are served by the connection
//! supervisor and the [`ConnectionNotifier`](super::notifier::ConnectionNotifier)
//! independently of which `ConnectionServices` is installed, so they behave exactly
//! as in full mode. This adapter carries only the channel/conversation operations,
//! every one of which it rejects with a typed error frame.

use liminal::protocol::{MessageEnvelope, SchemaId as ProtocolSchemaId};

use super::conversation::ConnectionConversation;
use super::services::{ConnectionServices, ConnectionSubscription, PublishOutcome};
use crate::ServerError;
use crate::config::types::ServiceProfile;

/// [`ConnectionServices`] for the worker front door: registration, correlated
/// push/reply, and notifier-consumed reserved publishes only.
///
/// The type holds no state and constructs nothing, so [`Self::new`] is an
/// infallible `const fn` — a structural signal that this path starts no scheduler
/// and opens no store (both of which are fallible in full mode). Ordinary
/// publish/subscribe/conversation operations return a typed [`ServerError`], which
/// [`super::apply`] renders as the matching typed error frame
/// (`PublishError`/`SubscribeError`/`ConversationError`); the connection stays
/// healthy across the rejection.
#[derive(Debug, Default, Clone, Copy)]
pub struct WorkerFrontDoorServices;

impl WorkerFrontDoorServices {
    /// Creates the worker-front-door adapter. Constructs nothing.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Builds the typed rejection returned for an unsupported channel/conversation
    /// operation. Rendered by [`super::apply`] as the operation's typed error frame.
    fn unsupported(operation: &str) -> ServerError {
        ServerError::UnsupportedOperation {
            operation: operation.to_owned(),
            profile: ServiceProfile::WORKER_FRONT_DOOR,
        }
    }
}

impl ConnectionServices for WorkerFrontDoorServices {
    fn publish(
        &self,
        channel: &str,
        _envelope: &MessageEnvelope,
        _idempotency_key: Option<&str>,
    ) -> Result<PublishOutcome, ServerError> {
        // Reserved observability publishes never reach here: `apply_frame` offers
        // every publish to the notifier's observability tap FIRST and only falls
        // through to `services.publish` when the tap did not consume it. So an
        // ordinary (untapped) publish is the only thing that lands here, and the
        // front door serves no channels — reject it explicitly.
        Err(Self::unsupported(&format!(
            "publish to channel '{channel}'"
        )))
    }

    fn subscribe(
        &self,
        channel: &str,
        _accepted_schemas: &[ProtocolSchemaId],
    ) -> Result<ConnectionSubscription, ServerError> {
        Err(Self::unsupported(&format!(
            "subscribe to channel '{channel}'"
        )))
    }

    fn unsubscribe(&self, subscription: ConnectionSubscription) -> Result<(), ServerError> {
        // Unreachable in practice: `subscribe` never yields a `ConnectionSubscription`,
        // so no owned subscription can exist to pass here, and `apply_frame` rejects
        // an `Unsubscribe` frame before this method via `supports_channel_operations`.
        // Release the resource and succeed rather than invent a second rejection
        // surface for a value that cannot be constructed in this profile.
        subscription.unsubscribe()
    }

    fn open_conversation(
        &self,
        _conversation_id: u64,
        subject: &str,
    ) -> Result<ConnectionConversation, ServerError> {
        Err(Self::unsupported(&format!(
            "opening conversation '{subject}'"
        )))
    }

    fn conversation_message(
        &self,
        _conversation: &ConnectionConversation,
        _envelope: &MessageEnvelope,
    ) -> Result<(), ServerError> {
        // Unreachable: no `ConnectionConversation` can exist because
        // `open_conversation` always rejects. `apply_frame` rejects a
        // `ConversationMessage` frame before this method via
        // `supports_channel_operations`.
        Err(Self::unsupported("conversation message"))
    }

    fn close_conversation(&self, conversation: ConnectionConversation) -> Result<(), ServerError> {
        // Unreachable for the same reason as `unsubscribe`; release and succeed.
        conversation.close()
    }

    fn flush_durable_state(&self) -> Result<(), ServerError> {
        // No durable channels exist, so a graceful-shutdown flush is a total no-op —
        // never an error, so shutting a worker-front-door server down stays clean.
        Ok(())
    }

    fn supports_channel_operations(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::WorkerFrontDoorServices;
    use crate::server::connection::services::ConnectionServices;

    /// Structural adjunct to the §9 D2 gate: the front-door adapter constructs
    /// nothing, so its constructor is infallible and starts no scheduler. A
    /// `Scheduler::new` (channel/conversation/haematite) is fallible and returns
    /// `Result`; this returning a bare `Self` is the in-language signal that no
    /// beamr scheduler is created by the adapter itself.
    ///
    /// The thread half of the §9 gate is the record-by-construction SEAM CENSUS,
    /// not this test: the profile-aware construction path reaches every
    /// scheduler-owning subsystem only through `services::SubsystemFactory`, whose
    /// recording test implementation cannot skip a record without also failing to
    /// construct (asserted with a full-profile positive control in
    /// `services::durable_store_tests` and `supervisor::tests`). A true OS-level
    /// thread census still belongs to the beamr composition lane's upcoming
    /// scheduler-inventory API; the census assertions upgrade to it when it lands.
    /// It is deliberately NOT faked with a platform-specific OS thread count.
    #[test]
    fn construction_is_infallible_and_starts_no_scheduler() {
        let services = WorkerFrontDoorServices::new();
        // The type carries no channel/conversation/store/dedup fields — that absence
        // is compile-time enforced by the struct definition. This adapter reports it
        // serves no channel/conversation operations.
        assert!(
            !services.supports_channel_operations(),
            "the worker front door must report that it serves no channel operations"
        );
    }
}
