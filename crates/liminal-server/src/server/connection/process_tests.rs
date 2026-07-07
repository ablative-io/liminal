use std::sync::{Arc, Mutex};

use liminal::protocol::{
    CausalContext, MessageEnvelope, SchemaId, WorkerRegisterOutcome, WorkerRegistration,
};

use super::*;
use crate::ServerError;
use crate::server::connection::conversation::ConversationResource;
use crate::server::connection::notifier::ConnectionNotifier;
use crate::server::connection::services::{
    ConnectionSubscription, PublishOutcome, SubscriptionResource,
};

/// Fixed connection pid used by the scheduler-free `apply_frame` unit tests.
const TEST_PID: u64 = 1;

#[derive(Debug, Default)]
struct RecordingServices {
    publishes: Mutex<Vec<(String, Vec<u8>)>>,
    subscriptions: Mutex<Vec<(String, usize)>>,
    conversations: Mutex<Vec<(u64, String)>>,
}

impl ConnectionServices for RecordingServices {
    fn publish(
        &self,
        channel: &str,
        envelope: &MessageEnvelope,
        _idempotency_key: Option<&str>,
    ) -> Result<PublishOutcome, ServerError> {
        self.publishes
            .lock()
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("test publish recorder unavailable: {error}"),
            })?
            .push((channel.to_owned(), envelope.payload.clone()));
        Ok(PublishOutcome {
            message_id: 42,
            delivered: true,
        })
    }

    fn subscribe(
        &self,
        channel: &str,
        accepted_schemas: &[ProtocolSchemaId],
    ) -> Result<ConnectionSubscription, ServerError> {
        self.subscriptions
            .lock()
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("test subscription recorder unavailable: {error}"),
            })?
            .push((channel.to_owned(), accepted_schemas.len()));
        Ok(ConnectionSubscription::new(
            7,
            schema_id(),
            Box::new(TestSubscription),
        ))
    }

    fn unsubscribe(&self, subscription: ConnectionSubscription) -> Result<(), ServerError> {
        subscription.unsubscribe()
    }

    fn open_conversation(
        &self,
        conversation_id: u64,
        subject: &str,
    ) -> Result<ConnectionConversation, ServerError> {
        self.conversations
            .lock()
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("test conversation recorder unavailable: {error}"),
            })?
            .push((conversation_id, subject.to_owned()));
        Ok(ConnectionConversation::new(Box::new(TestConversation)))
    }

    fn conversation_message(
        &self,
        conversation: &ConnectionConversation,
        envelope: &MessageEnvelope,
    ) -> Result<(), ServerError> {
        conversation.message(envelope)
    }

    fn close_conversation(&self, conversation: ConnectionConversation) -> Result<(), ServerError> {
        conversation.close()
    }

    fn flush_durable_state(&self) -> Result<(), ServerError> {
        Ok(())
    }
}

#[derive(Debug)]
struct TestSubscription;

impl SubscriptionResource for TestSubscription {
    fn unsubscribe(self: Box<Self>) -> Result<(), ServerError> {
        Ok(())
    }

    fn try_next(&mut self) -> Option<liminal::envelope::Envelope> {
        None
    }
}

#[derive(Debug)]
struct TestConversation;

impl ConversationResource for TestConversation {
    fn message(&self, envelope: &MessageEnvelope) -> Result<(), ServerError> {
        if envelope.payload.is_empty() {
            return Err(ServerError::ListenerAccept {
                message: "empty test payload".to_owned(),
            });
        }
        Ok(())
    }

    fn participant_pids(&self) -> Vec<u64> {
        Vec::new()
    }

    fn has_detected_crash(&self) -> bool {
        false
    }

    fn await_crash(&self, _timeout: std::time::Duration) -> Option<std::time::Instant> {
        None
    }

    fn receive_reply(&self, _timeout: std::time::Duration) -> Result<MessageEnvelope, ServerError> {
        Err(ServerError::ListenerAccept {
            message: "test conversation produces no reply".to_owned(),
        })
    }

    fn close(self: Box<Self>) -> Result<(), ServerError> {
        Ok(())
    }
}

/// Wraps recording services in a runtime so `apply_frame` can be exercised without
/// a live scheduler, returning both the runtime and a handle to the shared services
/// for post-call assertions.
fn runtime_with(services: RecordingServices) -> (ConnectionRuntime, Arc<RecordingServices>) {
    let services = Arc::new(services);
    let runtime = ConnectionRuntime::for_tests(Arc::clone(&services) as Arc<_>);
    (runtime, services)
}

#[test]
fn publish_frame_delegates_to_liminal_services() -> Result<(), ServerError> {
    let (runtime, services) = runtime_with(RecordingServices::default());
    let envelope = envelope(b"hello".to_vec());
    let frame = Frame::Publish {
        flags: 0,
        stream_id: 3,
        channel: "orders".to_owned(),
        envelope,
        idempotency_key: None,
    };
    let mut state = ConnectionProcessState::default();

    let action = apply_frame(TEST_PID, &runtime, &mut state, frame);

    assert!(matches!(
        action,
        FrameAction::Respond(Frame::PublishAck {
            stream_id: 3,
            message_id: 42,
            ..
        })
    ));
    let first_call = {
        let calls = services
            .publishes
            .lock()
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("test publish recorder unavailable: {error}"),
            })?;
        assert_eq!(calls.len(), 1);
        calls[0].clone()
    };
    assert_eq!(first_call.0, "orders");
    assert_eq!(first_call.1, b"hello".to_vec());
    Ok(())
}

#[test]
fn subscribe_and_unsubscribe_delegate_to_services() -> Result<(), ServerError> {
    let (runtime, services) = runtime_with(RecordingServices::default());
    let mut state = ConnectionProcessState::default();
    let subscribe = Frame::Subscribe {
        flags: 0,
        stream_id: 1,
        channel: "orders".to_owned(),
        accepted_schemas: Vec::new(),
        max_in_flight: 16,
    };

    let action = apply_frame(TEST_PID, &runtime, &mut state, subscribe);

    assert!(matches!(
        action,
        FrameAction::Respond(Frame::SubscribeAck {
            subscription_id: 7,
            ..
        })
    ));
    assert!(state.subscriptions.contains_key(&7));
    let unsubscribe = Frame::Unsubscribe {
        flags: 0,
        stream_id: 1,
        subscription_id: 7,
    };
    let action = apply_frame(TEST_PID, &runtime, &mut state, unsubscribe);
    assert!(matches!(action, FrameAction::NoResponse));
    assert!(!state.subscriptions.contains_key(&7));
    let first_subscription = {
        let calls = services
            .subscriptions
            .lock()
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("test subscription recorder unavailable: {error}"),
            })?;
        assert_eq!(calls.len(), 1);
        calls[0].clone()
    };
    assert_eq!(first_subscription, ("orders".to_owned(), 0));
    Ok(())
}

#[derive(Debug)]
struct RecordingNotifier {
    registered: Mutex<Vec<(u64, WorkerRegistration)>>,
    unregistered: Mutex<Vec<u64>>,
    reject_with: Option<String>,
    /// The channel this notifier taps out-of-band (consuming publishes to it), if
    /// any. A publish to this exact channel is recorded and reported consumed.
    tap_channel: Option<String>,
    tapped: Mutex<Vec<(u64, String, Vec<u8>)>>,
}

impl RecordingNotifier {
    fn accepting() -> Self {
        Self {
            registered: Mutex::new(Vec::new()),
            unregistered: Mutex::new(Vec::new()),
            reject_with: None,
            tap_channel: None,
            tapped: Mutex::new(Vec::new()),
        }
    }

    fn rejecting(reason: &str) -> Self {
        Self {
            registered: Mutex::new(Vec::new()),
            unregistered: Mutex::new(Vec::new()),
            reject_with: Some(reason.to_owned()),
            tap_channel: None,
            tapped: Mutex::new(Vec::new()),
        }
    }

    fn tapping(channel: &str) -> Self {
        Self {
            registered: Mutex::new(Vec::new()),
            unregistered: Mutex::new(Vec::new()),
            reject_with: None,
            tap_channel: Some(channel.to_owned()),
            tapped: Mutex::new(Vec::new()),
        }
    }
}

impl ConnectionNotifier for RecordingNotifier {
    fn on_worker_registered(
        &self,
        pid: u64,
        registration: &WorkerRegistration,
    ) -> Result<(), ServerError> {
        self.registered
            .lock()
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("test notifier registration recorder unavailable: {error}"),
            })?
            .push((pid, registration.clone()));
        self.reject_with.as_ref().map_or(Ok(()), |reason| {
            Err(ServerError::ListenerAccept {
                message: reason.clone(),
            })
        })
    }

    fn on_worker_unregistered(&self, pid: u64) {
        if let Ok(mut unregistered) = self.unregistered.lock() {
            unregistered.push(pid);
        }
    }

    fn on_channel_publish(&self, pid: u64, channel: &str, payload: &[u8]) -> bool {
        if self.tap_channel.as_deref() != Some(channel) {
            return false;
        }
        if let Ok(mut tapped) = self.tapped.lock() {
            tapped.push((pid, channel.to_owned(), payload.to_vec()));
        }
        true
    }
}

fn sample_registration() -> WorkerRegistration {
    WorkerRegistration {
        namespaces: vec!["default".to_owned(), "billing".to_owned()],
        task_queue: "payments".to_owned(),
        node: Some("node-a".to_owned()),
        activity_types: vec!["charge".to_owned()],
        identity: "worker-1".to_owned(),
    }
}

#[test]
fn worker_register_invokes_notifier_and_accepts() -> Result<(), ServerError> {
    let notifier = Arc::new(RecordingNotifier::accepting());
    let runtime = ConnectionRuntime::for_tests_with_notifier(
        Arc::new(RecordingServices::default()),
        Arc::clone(&notifier) as Arc<_>,
    );
    let mut state = ConnectionProcessState::default();
    let registration = sample_registration();
    let frame = Frame::WorkerRegister {
        flags: 0,
        registration: registration.clone(),
    };

    let action = apply_frame(TEST_PID, &runtime, &mut state, frame);

    assert!(matches!(
        action,
        FrameAction::Respond(Frame::WorkerRegisterAck {
            outcome: WorkerRegisterOutcome::Accepted,
            ..
        })
    ));
    let calls = {
        let guard = notifier
            .registered
            .lock()
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("test notifier recorder unavailable: {error}"),
            })?;
        guard.clone()
    };
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, TEST_PID);
    assert_eq!(calls[0].1, registration);
    Ok(())
}

#[test]
fn worker_register_rejection_surfaces_reason() -> Result<(), ServerError> {
    let notifier = Arc::new(RecordingNotifier::rejecting("task queue not served"));
    let runtime = ConnectionRuntime::for_tests_with_notifier(
        Arc::new(RecordingServices::default()),
        Arc::clone(&notifier) as Arc<_>,
    );
    let mut state = ConnectionProcessState::default();
    let frame = Frame::WorkerRegister {
        flags: 0,
        registration: sample_registration(),
    };

    let action = apply_frame(TEST_PID, &runtime, &mut state, frame);

    let FrameAction::Respond(Frame::WorkerRegisterAck {
        outcome: WorkerRegisterOutcome::Rejected { reason },
        ..
    }) = action
    else {
        return Err(ServerError::ListenerAccept {
            message: format!("expected a rejected ack, got {action:?}"),
        });
    };
    assert!(
        reason.contains("task queue not served"),
        "rejection reason should carry the notifier error text, got: {reason}"
    );
    Ok(())
}

#[test]
fn worker_register_without_notifier_is_accepted() {
    // Standalone liminal: no notifier configured. The frame must still be
    // acknowledged Accepted and must not panic.
    let runtime = ConnectionRuntime::for_tests(Arc::new(RecordingServices::default()));
    let mut state = ConnectionProcessState::default();
    let frame = Frame::WorkerRegister {
        flags: 0,
        registration: sample_registration(),
    };

    let action = apply_frame(TEST_PID, &runtime, &mut state, frame);

    assert!(matches!(
        action,
        FrameAction::Respond(Frame::WorkerRegisterAck {
            outcome: WorkerRegisterOutcome::Accepted,
            ..
        })
    ));
}

/// A publish to a notifier-tapped channel is consumed out-of-band: the connection
/// process routes it to the notifier and answers with NO wire response, and it NEVER
/// reaches the normal channel fan-out (`services.publish`). This is the
/// observability-drain demux the worker->server transcript bus rides.
#[test]
fn publish_to_tapped_channel_bypasses_fan_out() -> Result<(), ServerError> {
    let notifier = Arc::new(RecordingNotifier::tapping("aion.observability.v1"));
    let services = Arc::new(RecordingServices::default());
    let runtime = ConnectionRuntime::for_tests_with_notifier(
        Arc::clone(&services) as Arc<_>,
        Arc::clone(&notifier) as Arc<_>,
    );
    let mut state = ConnectionProcessState::default();
    let frame = Frame::Publish {
        flags: 0,
        stream_id: 3,
        channel: "aion.observability.v1".to_owned(),
        envelope: envelope(b"event-bytes".to_vec()),
        idempotency_key: None,
    };

    let action = apply_frame(TEST_PID, &runtime, &mut state, frame);

    // A tapped publish gets no wire response (one-way notification).
    assert!(matches!(action, FrameAction::NoResponse));
    // The notifier recorded it verbatim...
    let tapped = {
        let guard = notifier
            .tapped
            .lock()
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("test notifier tap recorder unavailable: {error}"),
            })?;
        guard.clone()
    };
    assert_eq!(
        tapped,
        vec![(
            TEST_PID,
            "aion.observability.v1".to_owned(),
            b"event-bytes".to_vec()
        )]
    );
    // ...and it NEVER reached the normal channel fan-out.
    let published_count = {
        let published = services
            .publishes
            .lock()
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("test publish recorder unavailable: {error}"),
            })?;
        published.len()
    };
    assert_eq!(
        published_count, 0,
        "a tapped publish must not reach services.publish"
    );
    Ok(())
}

/// A publish to any OTHER channel is untouched by the tap: it flows to the normal
/// channel fan-out exactly as before (the default-no-op path for a non-tapped
/// channel, so liminal's general pub/sub is unchanged).
#[test]
fn publish_to_untapped_channel_still_fans_out() -> Result<(), ServerError> {
    let notifier = Arc::new(RecordingNotifier::tapping("aion.observability.v1"));
    let services = Arc::new(RecordingServices::default());
    let runtime = ConnectionRuntime::for_tests_with_notifier(
        Arc::clone(&services) as Arc<_>,
        Arc::clone(&notifier) as Arc<_>,
    );
    let mut state = ConnectionProcessState::default();
    let frame = Frame::Publish {
        flags: 0,
        stream_id: 5,
        channel: "orders".to_owned(),
        envelope: envelope(b"order".to_vec()),
        idempotency_key: None,
    };

    let action = apply_frame(TEST_PID, &runtime, &mut state, frame);

    assert!(matches!(
        action,
        FrameAction::Respond(Frame::PublishAck { stream_id: 5, .. })
    ));
    let published_count = {
        let published = services
            .publishes
            .lock()
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("test publish recorder unavailable: {error}"),
            })?;
        published.len()
    };
    assert_eq!(
        published_count, 1,
        "an untapped publish must reach the normal fan-out"
    );
    Ok(())
}

fn envelope(payload: Vec<u8>) -> MessageEnvelope {
    MessageEnvelope::new(schema_id(), CausalContext::independent(), payload)
}

fn schema_id() -> SchemaId {
    SchemaId::new([1; SchemaId::WIRE_LEN])
}
