//! Pins the teardown seam: a connection releasing its conversations goes
//! through [`ConversationResource::finalize`] — the bounded, non-blocking
//! release — never through the RPC `close`. The probe resource's cleanup is
//! observable ONLY through `finalize`, so any future teardown path (or
//! resource impl) that stops calling it fails here instead of leaking
//! silently.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use liminal::protocol::MessageEnvelope;

use super::ConnectionProcess;
use crate::ServerError;
use crate::server::connection::conversation::{ConnectionConversation, ConversationResource};
use crate::server::connection::services::{ConnectionServices, ConnectionSubscription};
use crate::server::connection::supervisor::ConnectionRuntime;

/// Conversation resource whose release is observable only through `finalize`.
#[derive(Debug)]
struct FinalizeProbe {
    finalized: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
}

impl ConversationResource for FinalizeProbe {
    fn message(&self, _envelope: &MessageEnvelope) -> Result<(), ServerError> {
        Ok(())
    }

    fn participant_pids(&self) -> Vec<u64> {
        Vec::new()
    }

    fn has_detected_crash(&self) -> bool {
        false
    }

    fn await_crash(&self, _timeout: Duration) -> Option<std::time::Instant> {
        None
    }

    fn receive_reply(&self, _timeout: Duration) -> Result<MessageEnvelope, ServerError> {
        Err(ServerError::ListenerAccept {
            message: "probe conversation produces no reply".to_owned(),
        })
    }

    fn close(self: Box<Self>) -> Result<(), ServerError> {
        self.closed.store(true, Ordering::Release);
        Ok(())
    }

    fn finalize(self: Box<Self>) {
        self.finalized.store(true, Ordering::Release);
    }
}

#[derive(Debug)]
struct NoopServices;

impl ConnectionServices for NoopServices {
    fn publish(
        &self,
        _channel: &str,
        _envelope: &MessageEnvelope,
        _idempotency_key: Option<&str>,
    ) -> Result<crate::server::connection::services::PublishOutcome, ServerError> {
        Ok(crate::server::connection::services::PublishOutcome {
            message_id: 1,
            delivered: false,
        })
    }

    fn subscribe(
        &self,
        _channel: &str,
        _accepted_schemas: &[liminal::protocol::SchemaId],
        _install: Option<liminal::channel::InboxInstall>,
    ) -> Result<ConnectionSubscription, ServerError> {
        Err(ServerError::ListenerAccept {
            message: "noop services do not subscribe".to_owned(),
        })
    }

    fn unsubscribe(&self, subscription: ConnectionSubscription) -> Result<(), ServerError> {
        subscription.unsubscribe()
    }

    fn open_conversation(
        &self,
        _conversation_id: u64,
        _subject: &str,
    ) -> Result<ConnectionConversation, ServerError> {
        Err(ServerError::ListenerAccept {
            message: "noop services do not open conversations".to_owned(),
        })
    }

    fn conversation_message(
        &self,
        _conversation: &ConnectionConversation,
        _envelope: &MessageEnvelope,
    ) -> Result<(), ServerError> {
        Ok(())
    }

    fn close_conversation(&self, conversation: ConnectionConversation) -> Result<(), ServerError> {
        conversation.close()
    }

    fn flush_durable_state(&self) -> Result<(), ServerError> {
        Ok(())
    }
}

#[test]
fn teardown_releases_conversations_through_finalize_not_close() {
    let runtime = Arc::new(ConnectionRuntime::for_tests(Arc::new(NoopServices)));
    let finalized = Arc::new(AtomicBool::new(false));
    let closed = Arc::new(AtomicBool::new(false));

    // A stream-less process: no slice ever runs, so only the `Drop` backstop
    // (the last-resort teardown route every other route funnels past) can
    // release the conversation.
    let holder = Arc::new(Mutex::new(None));
    let mut process = ConnectionProcess::from_holder(runtime, None, &holder);
    process.state.conversations.insert(
        1,
        ConnectionConversation::new(Box::new(FinalizeProbe {
            finalized: Arc::clone(&finalized),
            closed: Arc::clone(&closed),
        })),
    );

    drop(process);

    assert!(
        finalized.load(Ordering::Acquire),
        "teardown must release the conversation through finalize"
    );
    assert!(
        !closed.load(Ordering::Acquire),
        "teardown must never use the blocking RPC close"
    );
}
