use std::sync::{Arc, Mutex};

use liminal::protocol::{
    CausalContext, MessageEnvelope, MessageId, SchemaId, WorkerRegisterOutcome, WorkerRegistration,
};

use super::*;
use crate::ServerError;
use crate::server::connection::conversation::{ConnectionConversation, ConversationResource};
use crate::server::connection::notifier::ConnectionNotifier;
use crate::server::connection::services::{
    ConnectionSubscription, PublishOutcome, SubscriptionResource,
};
use crate::server::connection::worker_front_door::WorkerFrontDoorServices;

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
        _install: Option<liminal::channel::InboxInstall>,
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

    fn finalize(self: Box<Self>) {
        drop(self);
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

// ---------------------------------------------------------------------------
// §9 D2 front-door construction gate: explicit rejection of unsupported frames.
//
// Each unsupported channel/conversation frame applied against the worker front
// door must produce the matching typed error frame, and the connection must stay
// healthy (a rejection is a `Respond(...Error)`, never a `Close`). The reserved
// observability tap, worker registration, and correlated push-reply frames must
// keep working in the SAME session, proving the front door serves its capability
// set unchanged while refusing everything else. The fs/thread half of this gate
// lives in `services::durable_store_tests` and `worker_front_door::tests`.
// ---------------------------------------------------------------------------

/// Builds a runtime backed by the worker front door adapter and a notifier that
/// accepts worker registration and taps `tap_channel` out-of-band — the exact
/// capability surface an aion-style worker host uses.
fn worker_front_door_runtime(tap_channel: &str) -> (ConnectionRuntime, Arc<RecordingNotifier>) {
    let notifier = Arc::new(RecordingNotifier::tapping(tap_channel));
    let runtime = ConnectionRuntime::for_tests_with_notifier(
        Arc::new(WorkerFrontDoorServices::new()),
        Arc::clone(&notifier) as Arc<_>,
    );
    (runtime, notifier)
}

#[test]
fn worker_front_door_rejects_ordinary_publish_with_publish_error() -> Result<(), ServerError> {
    let (runtime, _notifier) = worker_front_door_runtime("aion.observability.v1");
    let mut state = ConnectionProcessState::default();
    let frame = Frame::Publish {
        flags: 0,
        stream_id: 4,
        channel: "orders".to_owned(),
        envelope: envelope(b"order".to_vec()),
        idempotency_key: None,
    };

    let action = apply_frame(TEST_PID, &runtime, &mut state, frame);

    let FrameAction::Respond(Frame::PublishError {
        stream_id: 4,
        message: Some(message),
        ..
    }) = action
    else {
        return Err(ServerError::ListenerAccept {
            message: format!(
                "an ordinary publish must be rejected with a PublishError, got {action:?}"
            ),
        });
    };
    // Pins the `ServerError::UnsupportedOperation` rendering: the client sees the
    // honest refusal text, not the repurposed listener-accept prefix.
    assert!(
        message.contains("is not supported by the worker-front-door services profile"),
        "rejection must carry the unsupported-operation text, got: {message}"
    );
    assert!(
        !message.contains("listener accept failed"),
        "rejection must not carry the listener-accept prefix, got: {message}"
    );
    Ok(())
}

/// MAJOR-3: backpressure signals (`Accept`/`Defer`/`Reject`) act on subscription
/// delivery state the front door does not serve, so each is rejected with a typed
/// `SubscribeError` on the incoming stream — never silently accepted.
#[test]
fn worker_front_door_rejects_accept_pressure_frame_with_subscribe_error() {
    let (runtime, _notifier) = worker_front_door_runtime("aion.observability.v1");
    let mut state = ConnectionProcessState::default();
    let frame = Frame::Accept {
        flags: 0,
        stream_id: 6,
        referenced_message_id: MessageId::new("m-1"),
    };

    let action = apply_frame(TEST_PID, &runtime, &mut state, frame);

    assert!(
        matches!(
            action,
            FrameAction::Respond(Frame::SubscribeError { stream_id: 6, .. })
        ),
        "an Accept pressure frame must be rejected with a SubscribeError, got {action:?}"
    );
}

#[test]
fn worker_front_door_rejects_defer_pressure_frame_with_subscribe_error() {
    let (runtime, _notifier) = worker_front_door_runtime("aion.observability.v1");
    let mut state = ConnectionProcessState::default();
    let frame = Frame::Defer {
        flags: 0,
        stream_id: 6,
        referenced_message_id: MessageId::new("m-2"),
        reason: Some("buffering".to_owned()),
    };

    let action = apply_frame(TEST_PID, &runtime, &mut state, frame);

    assert!(
        matches!(
            action,
            FrameAction::Respond(Frame::SubscribeError { stream_id: 6, .. })
        ),
        "a Defer pressure frame must be rejected with a SubscribeError, got {action:?}"
    );
}

#[test]
fn worker_front_door_rejects_reject_pressure_frame_with_subscribe_error() {
    let (runtime, _notifier) = worker_front_door_runtime("aion.observability.v1");
    let mut state = ConnectionProcessState::default();
    let frame = Frame::Reject {
        flags: 0,
        stream_id: 6,
        referenced_message_id: MessageId::new("m-3"),
        reason: Some("shedding".to_owned()),
    };

    let action = apply_frame(TEST_PID, &runtime, &mut state, frame);

    assert!(
        matches!(
            action,
            FrameAction::Respond(Frame::SubscribeError { stream_id: 6, .. })
        ),
        "a Reject pressure frame must be rejected with a SubscribeError, got {action:?}"
    );
}

/// MAJOR-3 full-mode regression: pressure frames stay `NoResponse` in full mode,
/// exactly as before the front-door split.
#[test]
fn pressure_frames_remain_no_response_in_full_mode() {
    let (runtime, _services) = runtime_with(RecordingServices::default());
    let mut state = ConnectionProcessState::default();
    let frames = [
        Frame::Accept {
            flags: 0,
            stream_id: 6,
            referenced_message_id: MessageId::new("m-1"),
        },
        Frame::Defer {
            flags: 0,
            stream_id: 6,
            referenced_message_id: MessageId::new("m-2"),
            reason: None,
        },
        Frame::Reject {
            flags: 0,
            stream_id: 6,
            referenced_message_id: MessageId::new("m-3"),
            reason: None,
        },
    ];

    for frame in frames {
        let action = apply_frame(TEST_PID, &runtime, &mut state, frame);
        assert!(
            matches!(action, FrameAction::NoResponse),
            "full mode must keep consuming pressure frames silently, got {action:?}"
        );
    }
}

/// MAJOR-3 auth gating: pressure frames are application frames, so with a token
/// configured an UNAUTHED session can no longer send them freely (the gate closes
/// the connection), while an authed session keeps today's silent consumption.
#[test]
fn pressure_frames_are_auth_gated_when_token_configured() {
    let runtime = ConnectionRuntime::for_tests_with_auth_token(
        Arc::new(RecordingServices::default()),
        b"s3cr3t".to_vec(),
    );
    let mut state = ConnectionProcessState::default();
    let accept_frame = || Frame::Accept {
        flags: 0,
        stream_id: 6,
        referenced_message_id: MessageId::new("m-1"),
    };

    // Pre-auth: the gate tears the connection down.
    let action = apply_frame(TEST_PID, &runtime, &mut state, accept_frame());
    assert!(
        matches!(action, FrameAction::Close),
        "an unauthenticated pressure frame must close the connection, got {action:?}"
    );

    // Post-auth: full-mode semantics are unchanged (silent consumption).
    let mut state = ConnectionProcessState::default();
    let connect = apply_frame(TEST_PID, &runtime, &mut state, connect_frame(b"s3cr3t"));
    assert!(matches!(
        connect,
        FrameAction::Respond(Frame::ConnectAck { .. })
    ));
    let action = apply_frame(TEST_PID, &runtime, &mut state, accept_frame());
    assert!(
        matches!(action, FrameAction::NoResponse),
        "an authenticated pressure frame keeps full-mode NoResponse, got {action:?}"
    );
}

#[test]
fn worker_front_door_rejects_subscribe_with_subscribe_error() {
    let (runtime, _notifier) = worker_front_door_runtime("aion.observability.v1");
    let mut state = ConnectionProcessState::default();
    let frame = Frame::Subscribe {
        flags: 0,
        stream_id: 2,
        channel: "orders".to_owned(),
        accepted_schemas: Vec::new(),
        max_in_flight: 16,
    };

    let action = apply_frame(TEST_PID, &runtime, &mut state, frame);

    assert!(
        matches!(
            action,
            FrameAction::Respond(Frame::SubscribeError { stream_id: 2, .. })
        ),
        "a subscribe must be rejected with a SubscribeError, got {action:?}"
    );
    // No subscription was recorded on the connection: a rejected subscribe leaves
    // no local state behind.
    assert!(state.subscriptions.is_empty());
}

#[test]
fn worker_front_door_rejects_unsubscribe_with_subscribe_error() {
    // No dedicated `UnsubscribeError` frame exists, so the front door rejects an
    // unsubscribe of a never-existent subscription via `SubscribeError` (the closest
    // honest fit) rather than swallowing it as full mode's idempotent no-op would.
    let (runtime, _notifier) = worker_front_door_runtime("aion.observability.v1");
    let mut state = ConnectionProcessState::default();
    let frame = Frame::Unsubscribe {
        flags: 0,
        stream_id: 2,
        subscription_id: 99,
    };

    let action = apply_frame(TEST_PID, &runtime, &mut state, frame);

    assert!(
        matches!(
            action,
            FrameAction::Respond(Frame::SubscribeError { stream_id: 2, .. })
        ),
        "an unsubscribe must be rejected with a SubscribeError, got {action:?}"
    );
}

#[test]
fn worker_front_door_rejects_conversation_open_message_and_close() {
    let (runtime, _notifier) = worker_front_door_runtime("aion.observability.v1");
    let mut state = ConnectionProcessState::default();

    let open = Frame::ConversationOpen {
        flags: 0,
        stream_id: 1,
        conversation_id: 7,
        subject: "charge".to_owned(),
    };
    let open_action = apply_frame(TEST_PID, &runtime, &mut state, open);
    assert!(
        matches!(
            open_action,
            FrameAction::Respond(Frame::ConversationError {
                conversation_id: 7,
                ..
            })
        ),
        "conversation open must be rejected with a ConversationError, got {open_action:?}"
    );
    assert!(
        state.conversations.is_empty(),
        "a rejected open leaves no conversation on the connection"
    );

    let message = Frame::ConversationMessage {
        flags: 0,
        stream_id: 1,
        conversation_id: 7,
        envelope: envelope(b"do-work".to_vec()),
    };
    let message_action = apply_frame(TEST_PID, &runtime, &mut state, message);
    assert!(
        matches!(
            message_action,
            FrameAction::Respond(Frame::ConversationError {
                conversation_id: 7,
                ..
            })
        ),
        "conversation message must be rejected with a ConversationError, got {message_action:?}"
    );

    let close = Frame::ConversationClose {
        flags: 0,
        stream_id: 1,
        conversation_id: 7,
        reason_code: None,
        message: None,
    };
    let close_action = apply_frame(TEST_PID, &runtime, &mut state, close);
    assert!(
        matches!(
            close_action,
            FrameAction::Respond(Frame::ConversationError {
                conversation_id: 7,
                ..
            })
        ),
        "conversation close must be rejected with a ConversationError, got {close_action:?}"
    );
}

/// The load-bearing in-session assertion: the front door's supported capabilities —
/// worker registration, the notifier-consumed reserved publish, and correlated
/// push-reply — keep working across a rejected ordinary publish, and none of the
/// rejections tears the connection down.
#[test]
fn worker_front_door_serves_its_capabilities_across_rejections() -> Result<(), ServerError> {
    let (runtime, notifier) = worker_front_door_runtime("aion.observability.v1");
    let mut state = ConnectionProcessState::default();

    // 1. An ordinary publish is rejected (typed error), but stays open.
    let rejected = apply_frame(
        TEST_PID,
        &runtime,
        &mut state,
        Frame::Publish {
            flags: 0,
            stream_id: 9,
            channel: "orders".to_owned(),
            envelope: envelope(b"order".to_vec()),
            idempotency_key: None,
        },
    );
    assert!(
        matches!(rejected, FrameAction::Respond(Frame::PublishError { .. })),
        "ordinary publish rejected, connection stays open (not a Close): {rejected:?}"
    );

    // 2. Worker registration still works — served by the notifier, not the services.
    let register = apply_frame(
        TEST_PID,
        &runtime,
        &mut state,
        Frame::WorkerRegister {
            flags: 0,
            registration: sample_registration(),
        },
    );
    assert!(
        matches!(
            register,
            FrameAction::Respond(Frame::WorkerRegisterAck {
                outcome: WorkerRegisterOutcome::Accepted,
                ..
            })
        ),
        "worker registration must still be accepted, got {register:?}"
    );

    // 3. A reserved-channel publish is consumed by the tap out-of-band (no wire
    //    response), exactly as in full mode — it never reaches services.publish.
    let reserved = apply_frame(
        TEST_PID,
        &runtime,
        &mut state,
        Frame::Publish {
            flags: 0,
            stream_id: 3,
            channel: "aion.observability.v1".to_owned(),
            envelope: envelope(b"event".to_vec()),
            idempotency_key: None,
        },
    );
    assert!(
        matches!(reserved, FrameAction::NoResponse),
        "a reserved-channel publish is consumed by the tap (no wire response), got {reserved:?}"
    );
    let tapped = notifier
        .tapped
        .lock()
        .map_err(|error| ServerError::ListenerAccept {
            message: format!("test notifier tap recorder unavailable: {error}"),
        })?
        .clone();
    assert_eq!(
        tapped,
        vec![(
            TEST_PID,
            "aion.observability.v1".to_owned(),
            b"event".to_vec()
        )],
        "the reserved publish reached the notifier tap verbatim"
    );

    // 4. A correlated push-reply frame is handled (resolves its slot, no wire
    //    response); an unknown correlation id is a harmless no-op, never a reject.
    let push_reply = apply_frame(
        TEST_PID,
        &runtime,
        &mut state,
        Frame::PushReply {
            flags: 0,
            stream_id: 1,
            correlation_id: 1,
            payload: b"reply".to_vec(),
        },
    );
    assert!(
        matches!(push_reply, FrameAction::NoResponse),
        "a push reply is handled with no wire response, got {push_reply:?}"
    );

    Ok(())
}

/// Builds a `Connect` frame carrying `auth_token`, mirroring the client handshake.
fn connect_frame(auth_token: &[u8]) -> Frame {
    Frame::Connect {
        flags: 0,
        min_version: ProtocolVersion::new(1, 0),
        max_version: ProtocolVersion::new(1, 0),
        auth_token: auth_token.to_vec(),
    }
}

/// With no `[auth]` token configured the handshake is open: any token (including an
/// empty one) is accepted and answered with `ConnectAck`, byte-identical to the
/// pre-auth behaviour.
#[test]
fn connect_without_configured_token_accepts_any_token() {
    let (runtime, _services) = runtime_with(RecordingServices::default());
    let mut state = ConnectionProcessState::default();

    let action = apply_frame(TEST_PID, &runtime, &mut state, connect_frame(&[]));
    assert!(matches!(
        action,
        FrameAction::Respond(Frame::ConnectAck { .. })
    ));

    // A non-empty token is likewise ignored when no gate is configured.
    let action = apply_frame(TEST_PID, &runtime, &mut state, connect_frame(b"anything"));
    assert!(matches!(
        action,
        FrameAction::Respond(Frame::ConnectAck { .. })
    ));
}

/// With a token configured, a matching handshake token is accepted with a
/// `ConnectAck` (and the connection stays open for subsequent frames).
#[test]
fn connect_with_matching_token_is_accepted() {
    let runtime = ConnectionRuntime::for_tests_with_auth_token(
        Arc::new(RecordingServices::default()),
        b"s3cr3t".to_vec(),
    );
    let mut state = ConnectionProcessState::default();

    let action = apply_frame(TEST_PID, &runtime, &mut state, connect_frame(b"s3cr3t"));

    assert!(matches!(
        action,
        FrameAction::Respond(Frame::ConnectAck { .. })
    ));
}

/// A wrong token is rejected with a `ConnectError` carrying the authentication
/// reason code, and the connection is torn down (`RespondThenClose`).
#[test]
fn connect_with_wrong_token_is_rejected_and_closed() {
    let runtime = ConnectionRuntime::for_tests_with_auth_token(
        Arc::new(RecordingServices::default()),
        b"s3cr3t".to_vec(),
    );
    let mut state = ConnectionProcessState::default();

    let action = apply_frame(TEST_PID, &runtime, &mut state, connect_frame(b"wrong"));

    assert!(matches!(
        action,
        FrameAction::RespondThenClose(Frame::ConnectError {
            reason_code,
            ..
        }) if reason_code == liminal::protocol::ProtocolError::AUTHENTICATION_FAILURE_CODE
    ));
}

/// An absent token (empty handshake) against a configured gate is rejected and
/// closed just like a wrong token: absence is not a bypass.
#[test]
fn connect_with_absent_token_against_gate_is_rejected_and_closed() {
    let runtime = ConnectionRuntime::for_tests_with_auth_token(
        Arc::new(RecordingServices::default()),
        b"s3cr3t".to_vec(),
    );
    let mut state = ConnectionProcessState::default();

    let action = apply_frame(TEST_PID, &runtime, &mut state, connect_frame(&[]));

    assert!(matches!(
        action,
        FrameAction::RespondThenClose(Frame::ConnectError {
            reason_code,
            ..
        }) if reason_code == liminal::protocol::ProtocolError::AUTHENTICATION_FAILURE_CODE
    ));
}

/// A token that shares a prefix with the configured secret but differs in length is
/// rejected: the length-difference fold means a prefix match is not accepted.
#[test]
fn connect_with_prefix_but_wrong_length_is_rejected() {
    let runtime = ConnectionRuntime::for_tests_with_auth_token(
        Arc::new(RecordingServices::default()),
        b"s3cr3t".to_vec(),
    );
    let mut state = ConnectionProcessState::default();

    let action = apply_frame(TEST_PID, &runtime, &mut state, connect_frame(b"s3cr3"));

    assert!(matches!(
        action,
        FrameAction::RespondThenClose(Frame::ConnectError { .. })
    ));
}

/// The auth gate is enforced per frame, not only inside the `Connect` arm: a client
/// that skips `Connect` entirely and sends a `Publish` against a configured gate is
/// torn down (`Close`) and the publish never reaches `services.publish`. This is the
/// bypass the flag closes — without it the `Publish` arm dispatched straight to the
/// fan-out because the token is only ever read in `connect_response`.
#[test]
fn publish_before_connect_against_gate_is_closed() -> Result<(), ServerError> {
    let services = Arc::new(RecordingServices::default());
    let runtime = ConnectionRuntime::for_tests_with_auth_token(
        Arc::clone(&services) as Arc<_>,
        b"s3cr3t".to_vec(),
    );
    let mut state = ConnectionProcessState::default();
    let frame = Frame::Publish {
        flags: 0,
        stream_id: 3,
        channel: "orders".to_owned(),
        envelope: envelope(b"hello".to_vec()),
        idempotency_key: None,
    };

    let action = apply_frame(TEST_PID, &runtime, &mut state, frame);

    assert!(matches!(action, FrameAction::Close));
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
        "a pre-auth publish must never reach services.publish"
    );
    Ok(())
}

/// `Subscribe` and `WorkerRegister` are gated the same way: neither is honoured
/// before a successful handshake against a configured gate.
#[test]
fn subscribe_and_worker_register_before_connect_are_closed() {
    let runtime = ConnectionRuntime::for_tests_with_auth_token(
        Arc::new(RecordingServices::default()),
        b"s3cr3t".to_vec(),
    );
    let mut state = ConnectionProcessState::default();

    let subscribe = Frame::Subscribe {
        flags: 0,
        stream_id: 1,
        channel: "orders".to_owned(),
        accepted_schemas: Vec::new(),
        max_in_flight: 16,
    };
    assert!(matches!(
        apply_frame(TEST_PID, &runtime, &mut state, subscribe),
        FrameAction::Close
    ));
    assert!(state.subscriptions.is_empty());

    let register = Frame::WorkerRegister {
        flags: 0,
        registration: sample_registration(),
    };
    assert!(matches!(
        apply_frame(TEST_PID, &runtime, &mut state, register),
        FrameAction::Close
    ));
}

/// After a matching `Connect` clears the gate, subsequent application frames on the
/// same connection are honoured — authentication persists for the connection's life.
#[test]
fn publish_after_successful_connect_is_honoured() -> Result<(), ServerError> {
    let services = Arc::new(RecordingServices::default());
    let runtime = ConnectionRuntime::for_tests_with_auth_token(
        Arc::clone(&services) as Arc<_>,
        b"s3cr3t".to_vec(),
    );
    let mut state = ConnectionProcessState::default();

    let connect = apply_frame(TEST_PID, &runtime, &mut state, connect_frame(b"s3cr3t"));
    assert!(matches!(
        connect,
        FrameAction::Respond(Frame::ConnectAck { .. })
    ));
    assert!(state.authenticated);

    let publish = Frame::Publish {
        flags: 0,
        stream_id: 3,
        channel: "orders".to_owned(),
        envelope: envelope(b"hello".to_vec()),
        idempotency_key: None,
    };
    let action = apply_frame(TEST_PID, &runtime, &mut state, publish);
    assert!(matches!(
        action,
        FrameAction::Respond(Frame::PublishAck { stream_id: 3, .. })
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
    assert_eq!(published_count, 1);
    Ok(())
}

/// A rejected handshake (wrong token) does not authenticate the connection: a
/// follow-up `Publish` on the same state is still closed. Belt-and-braces, since the
/// rejected handshake also returns `RespondThenClose`.
#[test]
fn publish_after_rejected_connect_is_still_closed() {
    let runtime = ConnectionRuntime::for_tests_with_auth_token(
        Arc::new(RecordingServices::default()),
        b"s3cr3t".to_vec(),
    );
    let mut state = ConnectionProcessState::default();

    let connect = apply_frame(TEST_PID, &runtime, &mut state, connect_frame(b"wrong"));
    assert!(matches!(
        connect,
        FrameAction::RespondThenClose(Frame::ConnectError { .. })
    ));
    assert!(!state.authenticated);

    let publish = Frame::Publish {
        flags: 0,
        stream_id: 3,
        channel: "orders".to_owned(),
        envelope: envelope(b"hello".to_vec()),
        idempotency_key: None,
    };
    assert!(matches!(
        apply_frame(TEST_PID, &runtime, &mut state, publish),
        FrameAction::Close
    ));
}

fn envelope(payload: Vec<u8>) -> MessageEnvelope {
    MessageEnvelope::new(schema_id(), CausalContext::independent(), payload)
}

fn schema_id() -> SchemaId {
    SchemaId::new([1; SchemaId::WIRE_LEN])
}

/// §5 cap-refusal (behavioural half). A [`LimitsConfig`] with a single named cap
/// squeezed to one, everything else at its signed default.
fn limits_with(
    mutate: impl FnOnce(&mut crate::config::types::LimitsConfig),
) -> crate::config::types::LimitsConfig {
    let mut limits = crate::config::types::LimitsConfig::default();
    mutate(&mut limits);
    limits
}

#[test]
fn subscription_cap_refuses_past_the_limit_with_a_typed_error() -> Result<(), ServerError> {
    // One subscription allowed; the second is refused BEFORE the services adapter
    // is touched, on the subscription error channel carrying the typed cap message.
    let runtime = ConnectionRuntime::for_tests_with_limits(
        Arc::new(RecordingServices::default()),
        limits_with(|l| l.max_subscriptions_per_connection = 1),
    );
    let mut state = ConnectionProcessState::default();
    let frame = || Frame::Subscribe {
        flags: 0,
        stream_id: 5,
        channel: "orders".to_owned(),
        accepted_schemas: Vec::new(),
        max_in_flight: 16,
    };
    let first = apply_frame(TEST_PID, &runtime, &mut state, frame());
    assert!(
        matches!(first, FrameAction::Respond(Frame::SubscribeAck { .. })),
        "the first subscription is admitted under the cap"
    );
    let second = apply_frame(TEST_PID, &runtime, &mut state, frame());
    let FrameAction::Respond(Frame::SubscribeError {
        message: Some(m), ..
    }) = second
    else {
        return Err(ServerError::ListenerAccept {
            message: format!("expected a typed SubscribeError past the cap, got {second:?}"),
        });
    };
    assert!(
        m.contains("max_subscriptions_per_connection"),
        "the refusal names the cap: {m}"
    );
    Ok(())
}

#[test]
fn conversation_cap_refuses_past_the_limit_with_a_typed_error() -> Result<(), ServerError> {
    let runtime = ConnectionRuntime::for_tests_with_limits(
        Arc::new(RecordingServices::default()),
        limits_with(|l| l.max_conversations_per_connection = 1),
    );
    let mut state = ConnectionProcessState::default();
    let open = |id: u64| Frame::ConversationOpen {
        flags: 0,
        stream_id: 1,
        conversation_id: id,
        subject: "s".to_owned(),
    };
    let first = apply_frame(TEST_PID, &runtime, &mut state, open(100));
    assert!(
        matches!(first, FrameAction::NoResponse),
        "the first conversation opens under the cap"
    );
    let second = apply_frame(TEST_PID, &runtime, &mut state, open(200));
    let FrameAction::Respond(Frame::ConversationError {
        message: Some(m), ..
    }) = second
    else {
        return Err(ServerError::ListenerAccept {
            message: format!("expected a typed ConversationError past the cap, got {second:?}"),
        });
    };
    assert!(
        m.contains("max_conversations_per_connection"),
        "the refusal names the cap: {m}"
    );
    Ok(())
}

/// Services whose `conversation_message` counts forwards and can be set to fail,
/// so the admit-before-forward discipline (review round 1 item 1) is observable:
/// a cap-refused request must NEVER reach the participant, and a forward failure
/// must roll back its exact reservation.
#[derive(Debug, Default)]
struct ForwardCountingServices {
    forwards: std::sync::atomic::AtomicUsize,
    fail_forwards: std::sync::atomic::AtomicBool,
    fail_opens: std::sync::atomic::AtomicBool,
}

impl ForwardCountingServices {
    fn forwards(&self) -> usize {
        self.forwards.load(std::sync::atomic::Ordering::Acquire)
    }

    fn fail_next_forwards(&self, fail: bool) {
        self.fail_forwards
            .store(fail, std::sync::atomic::Ordering::Release);
    }

    fn fail_next_opens(&self, fail: bool) {
        self.fail_opens
            .store(fail, std::sync::atomic::Ordering::Release);
    }
}

impl ConnectionServices for ForwardCountingServices {
    fn publish(
        &self,
        _channel: &str,
        _envelope: &MessageEnvelope,
        _idempotency_key: Option<&str>,
    ) -> Result<PublishOutcome, ServerError> {
        Ok(PublishOutcome {
            message_id: 1,
            delivered: false,
        })
    }

    fn subscribe(
        &self,
        _channel: &str,
        _accepted_schemas: &[ProtocolSchemaId],
        _install: Option<liminal::channel::InboxInstall>,
    ) -> Result<ConnectionSubscription, ServerError> {
        Err(ServerError::ListenerAccept {
            message: "not under test".to_owned(),
        })
    }

    fn unsubscribe(&self, _subscription: ConnectionSubscription) -> Result<(), ServerError> {
        Ok(())
    }

    fn open_conversation(
        &self,
        _conversation_id: u64,
        _subject: &str,
    ) -> Result<ConnectionConversation, ServerError> {
        if self.fail_opens.load(std::sync::atomic::Ordering::Acquire) {
            return Err(ServerError::ListenerAccept {
                message: "open failed under test".to_owned(),
            });
        }
        Ok(ConnectionConversation::new(Box::new(TestConversation)))
    }

    fn conversation_message(
        &self,
        _conversation: &ConnectionConversation,
        _envelope: &MessageEnvelope,
    ) -> Result<(), ServerError> {
        if self
            .fail_forwards
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(ServerError::ListenerAccept {
                message: "forward failed under test".to_owned(),
            });
        }
        self.forwards
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Ok(())
    }

    fn close_conversation(&self, conversation: ConnectionConversation) -> Result<(), ServerError> {
        conversation.close()
    }

    fn flush_durable_state(&self) -> Result<(), ServerError> {
        Ok(())
    }
}

fn reply_requested_message(conversation_id: u64, stream_id: u32) -> Frame {
    Frame::ConversationMessage {
        flags: liminal::protocol::CONVERSATION_REPLY_REQUESTED_FLAG,
        stream_id,
        conversation_id,
        envelope: envelope(b"request".to_vec()),
    }
}

fn open_frame(conversation_id: u64) -> Frame {
    Frame::ConversationOpen {
        flags: 0,
        stream_id: 1,
        conversation_id,
        subject: "s".to_owned(),
    }
}

/// Review round 1 item 1 (BLOCKER): admission comes BEFORE the forward. A
/// reply-requested request refused by the per-conversation sub-cap (here: the
/// tombstone self-wedge shape, cap 1) never reaches the participant, so no
/// orphan reply can exist; after capacity frees, a new request's reply matches
/// the NEW operation, never anything stale.
#[test]
fn cap_refused_request_never_reaches_the_participant() -> Result<(), ServerError> {
    let services = Arc::new(ForwardCountingServices::default());
    let runtime = ConnectionRuntime::for_tests_with_limits(
        Arc::clone(&services) as Arc<_>,
        limits_with(|l| l.max_pending_replies_per_conversation = 1),
    );
    let mut state = ConnectionProcessState {
        pending_replies: crate::server::connection::pending_reply::PendingReplyTable::new(
            1,
            32,
            crate::server::connection::pending_reply::DEFAULT_REPLY_TIMEOUT,
        ),
        ..ConnectionProcessState::default()
    };
    assert!(matches!(
        apply_frame(TEST_PID, &runtime, &mut state, open_frame(1)),
        FrameAction::NoResponse
    ));

    // First request: admitted then forwarded.
    let first = apply_frame(
        TEST_PID,
        &runtime,
        &mut state,
        reply_requested_message(1, 10),
    );
    assert!(matches!(first, FrameAction::NoResponse));
    assert_eq!(services.forwards(), 1);

    // Second request: refused at the sub-cap. It must NOT have been forwarded —
    // a refused-but-forwarded request would produce an orphan reply able to
    // FIFO-match a younger admitted operation.
    let second = apply_frame(
        TEST_PID,
        &runtime,
        &mut state,
        reply_requested_message(1, 11),
    );
    let FrameAction::Respond(Frame::ConversationError {
        message: Some(m), ..
    }) = second
    else {
        return Err(ServerError::ListenerAccept {
            message: format!("expected a typed cap refusal, got {second:?}"),
        });
    };
    assert!(m.contains("max_pending_replies_per_conversation"));
    assert_eq!(
        services.forwards(),
        1,
        "the refused request never reached the participant"
    );

    // Free capacity: the first operation's reply arrives and matches stream 10.
    let matched = state
        .pending_replies
        .match_reply(
            1,
            crate::server::connection::pending_reply::test_reply_envelope(b"r1"),
        )
        .ok_or_else(|| ServerError::ListenerAccept {
            message: "the pending operation must match its reply".to_owned(),
        })?;
    assert!(matches!(
        matched,
        Frame::ConversationMessage { stream_id: 10, .. }
    ));

    // A NEW request is admitted and forwarded; its reply matches the NEW
    // operation's stream — no stale/orphan reply exists to mis-correlate,
    // because the refused request never ran.
    let third = apply_frame(
        TEST_PID,
        &runtime,
        &mut state,
        reply_requested_message(1, 12),
    );
    assert!(matches!(third, FrameAction::NoResponse));
    assert_eq!(services.forwards(), 2);
    let matched = state
        .pending_replies
        .match_reply(
            1,
            crate::server::connection::pending_reply::test_reply_envelope(b"r3"),
        )
        .ok_or_else(|| ServerError::ListenerAccept {
            message: "the new operation must match its reply".to_owned(),
        })?;
    assert!(matches!(
        matched,
        Frame::ConversationMessage { stream_id: 12, .. }
    ));
    Ok(())
}

/// Review round 1 item 1 (connection-cap leg): a request refused by the
/// per-connection pending table is not forwarded either — cap-before-mutation
/// holds for BOTH caps.
#[test]
fn connection_cap_refused_request_never_reaches_the_participant() -> Result<(), ServerError> {
    let services = Arc::new(ForwardCountingServices::default());
    let runtime = ConnectionRuntime::for_tests_with_limits(
        Arc::clone(&services) as Arc<_>,
        limits_with(|l| l.max_pending_conversation_replies_per_connection = 1),
    );
    let mut state = ConnectionProcessState {
        pending_replies: crate::server::connection::pending_reply::PendingReplyTable::new(
            8,
            1,
            crate::server::connection::pending_reply::DEFAULT_REPLY_TIMEOUT,
        ),
        ..ConnectionProcessState::default()
    };
    assert!(matches!(
        apply_frame(TEST_PID, &runtime, &mut state, open_frame(1)),
        FrameAction::NoResponse
    ));
    assert!(matches!(
        apply_frame(TEST_PID, &runtime, &mut state, open_frame(2)),
        FrameAction::NoResponse
    ));

    let first = apply_frame(
        TEST_PID,
        &runtime,
        &mut state,
        reply_requested_message(1, 10),
    );
    assert!(matches!(first, FrameAction::NoResponse));
    assert_eq!(services.forwards(), 1);

    // The connection table is full: conversation 2's request is refused and
    // never forwarded.
    let second = apply_frame(
        TEST_PID,
        &runtime,
        &mut state,
        reply_requested_message(2, 20),
    );
    let FrameAction::Respond(Frame::ConversationError {
        message: Some(m), ..
    }) = second
    else {
        return Err(ServerError::ListenerAccept {
            message: format!("expected a typed cap refusal, got {second:?}"),
        });
    };
    assert!(m.contains("max_pending_conversation_replies_per_connection"));
    assert_eq!(services.forwards(), 1);
    Ok(())
}

/// Review round 1 item 1 (rollback leg): a forward that fails AFTER admission
/// rolls back its exact reservation — the entry must not linger to time out into
/// a spurious tombstone for a message the participant never received.
#[test]
fn failed_forward_rolls_back_its_reservation() {
    let services = Arc::new(ForwardCountingServices::default());
    let runtime = ConnectionRuntime::for_tests(Arc::clone(&services) as Arc<_>);
    let mut state = ConnectionProcessState::default();
    assert!(matches!(
        apply_frame(TEST_PID, &runtime, &mut state, open_frame(1)),
        FrameAction::NoResponse
    ));

    services.fail_next_forwards(true);
    let action = apply_frame(
        TEST_PID,
        &runtime,
        &mut state,
        reply_requested_message(1, 10),
    );
    assert!(
        matches!(
            action,
            FrameAction::Respond(Frame::ConversationError { .. })
        ),
        "the forward failure surfaces as a conversation error"
    );
    assert_eq!(
        state.pending_replies.len(),
        0,
        "the reservation was rolled back — no entry lingers to tombstone"
    );
}

/// Review round 1 item 2: an explicit `ConversationClose` synchronously sweeps the
/// pending-reply table — the close sweep is one of the only two sanctioned
/// tombstone-reclamation triggers. A tombstone-only conversation reclaims its
/// sub-cap slots at close, and a pipelined Close/Open/Message sequence carries
/// no stale reply state (a reply after the reopen matches the NEW operation).
#[test]
fn conversation_close_sweeps_pending_and_tombstone_entries() -> Result<(), ServerError> {
    let services = Arc::new(ForwardCountingServices::default());
    let runtime = ConnectionRuntime::for_tests(Arc::clone(&services) as Arc<_>);
    let mut state = ConnectionProcessState {
        pending_replies: crate::server::connection::pending_reply::PendingReplyTable::new(
            2,
            32,
            std::time::Duration::from_millis(1),
        ),
        ..ConnectionProcessState::default()
    };
    assert!(matches!(
        apply_frame(TEST_PID, &runtime, &mut state, open_frame(1)),
        FrameAction::NoResponse
    ));

    // Two requests, both timed out into tombstones: the conversation is wedged.
    assert!(matches!(
        apply_frame(
            TEST_PID,
            &runtime,
            &mut state,
            reply_requested_message(1, 10)
        ),
        FrameAction::NoResponse
    ));
    assert!(matches!(
        apply_frame(
            TEST_PID,
            &runtime,
            &mut state,
            reply_requested_message(1, 11)
        ),
        FrameAction::NoResponse
    ));
    let expired = state
        .pending_replies
        .expire_due(std::time::Instant::now() + std::time::Duration::from_secs(1));
    assert_eq!(expired.len(), 2, "both entries tombstone");
    assert_eq!(state.pending_replies.len(), 2);

    // Pipelined Close/Open/Message: the close sweep reclaims the tombstones
    // synchronously, the reopen admits, and the new request's reply matches the
    // NEW operation — no stale state survives the sequence.
    let close = Frame::ConversationClose {
        flags: 0,
        stream_id: 1,
        conversation_id: 1,
        reason_code: None,
        message: None,
    };
    assert!(matches!(
        apply_frame(TEST_PID, &runtime, &mut state, close),
        FrameAction::NoResponse
    ));
    assert_eq!(
        state.pending_replies.len(),
        0,
        "the close sweep reclaims every pending and tombstone entry"
    );

    assert!(matches!(
        apply_frame(TEST_PID, &runtime, &mut state, open_frame(1)),
        FrameAction::NoResponse
    ));
    assert!(matches!(
        apply_frame(
            TEST_PID,
            &runtime,
            &mut state,
            reply_requested_message(1, 12)
        ),
        FrameAction::NoResponse
    ));
    let matched = state
        .pending_replies
        .match_reply(
            1,
            crate::server::connection::pending_reply::test_reply_envelope(b"fresh"),
        )
        .ok_or_else(|| ServerError::ListenerAccept {
            message: "the reopened conversation's request must match its reply".to_owned(),
        })?;
    assert!(matches!(
        matched,
        Frame::ConversationMessage { stream_id: 12, .. }
    ));
    Ok(())
}

/// Review round 1 item 3 (ruling: fail-closed): a duplicate open of a LIVE
/// conversation id is refused with a typed error naming the remedy; the original
/// conversation instance stays live and usable, and the services adapter is
/// never asked to construct a second instance.
#[test]
fn duplicate_open_of_a_live_conversation_is_refused() -> Result<(), ServerError> {
    let (runtime, services) = runtime_with(RecordingServices::default());
    let mut state = ConnectionProcessState::default();

    assert!(matches!(
        apply_frame(TEST_PID, &runtime, &mut state, open_frame(7)),
        FrameAction::NoResponse
    ));

    let duplicate = apply_frame(TEST_PID, &runtime, &mut state, open_frame(7));
    let FrameAction::Respond(Frame::ConversationError {
        conversation_id: 7,
        message: Some(m),
        ..
    }) = duplicate
    else {
        return Err(ServerError::ListenerAccept {
            message: format!("expected a typed duplicate-open refusal, got {duplicate:?}"),
        });
    };
    assert!(
        m.contains("already open"),
        "the refusal names the condition: {m}"
    );
    assert!(
        m.contains("close it before") || m.contains("fresh id"),
        "the refusal names the remedy: {m}"
    );

    // The original instance is untouched and still live.
    assert!(state.conversations.contains_key(&7));
    let opens = services
        .conversations
        .lock()
        .map_err(|error| ServerError::ListenerAccept {
            message: format!("test recorder unavailable: {error}"),
        })?
        .len();
    assert_eq!(
        opens, 1,
        "the services adapter never constructed a second instance"
    );
    Ok(())
}

/// A `ConversationOpen` on a caller-chosen (non-default) stream.
fn open_frame_on(stream_id: u32, conversation_id: u64) -> Frame {
    Frame::ConversationOpen {
        flags: 0,
        stream_id,
        conversation_id,
        subject: "s".to_owned(),
    }
}

/// Extracts the `(stream_id, message)` from an expected `ConversationError`
/// response, or reports what was actually returned.
fn conversation_error_parts(action: FrameAction) -> Result<(u32, String), ServerError> {
    let FrameAction::Respond(Frame::ConversationError {
        stream_id,
        message: Some(message),
        ..
    }) = action
    else {
        return Err(ServerError::ListenerAccept {
            message: format!("expected a ConversationError, got {action:?}"),
        });
    };
    Ok((stream_id, message))
}

/// Sol round 2: every `ConversationOpen` refusal rides the REQUEST'S stream id,
/// never a hard-coded stream. A client that opened on stream 17 must see its
/// typed refusal on stream 17 — all three refusal paths (duplicate-open, the
/// `max_conversations` cap, and an adapter open error) are pinned here.
#[test]
fn open_refusals_preserve_the_request_stream_id() -> Result<(), ServerError> {
    const OPEN_STREAM: u32 = 17;
    let services = Arc::new(ForwardCountingServices::default());
    let runtime = ConnectionRuntime::for_tests_with_limits(
        Arc::clone(&services) as Arc<_>,
        limits_with(|l| l.max_conversations_per_connection = 1),
    );
    let mut state = ConnectionProcessState::default();

    // A successful open on stream 17 (fills the cap of 1).
    assert!(matches!(
        apply_frame(
            TEST_PID,
            &runtime,
            &mut state,
            open_frame_on(OPEN_STREAM, 1)
        ),
        FrameAction::NoResponse
    ));

    // Refusal path 1 — duplicate open of the live id: refusal on stream 17.
    let action = apply_frame(
        TEST_PID,
        &runtime,
        &mut state,
        open_frame_on(OPEN_STREAM, 1),
    );
    let (stream, message) = conversation_error_parts(action)?;
    assert_eq!(
        stream, OPEN_STREAM,
        "the duplicate-open refusal rides the request's stream"
    );
    assert!(message.contains("already open"));

    // Refusal path 2 — the max_conversations cap: refusal on stream 17.
    let action = apply_frame(
        TEST_PID,
        &runtime,
        &mut state,
        open_frame_on(OPEN_STREAM, 2),
    );
    let (stream, message) = conversation_error_parts(action)?;
    assert_eq!(
        stream, OPEN_STREAM,
        "the cap refusal rides the request's stream"
    );
    assert!(message.contains("max_conversations_per_connection"));

    // Refusal path 3 — the services adapter's open error: refusal on stream 17.
    // Free the cap first so the adapter is actually reached.
    let close = Frame::ConversationClose {
        flags: 0,
        stream_id: OPEN_STREAM,
        conversation_id: 1,
        reason_code: None,
        message: None,
    };
    assert!(matches!(
        apply_frame(TEST_PID, &runtime, &mut state, close),
        FrameAction::NoResponse
    ));
    services.fail_next_opens(true);
    let action = apply_frame(
        TEST_PID,
        &runtime,
        &mut state,
        open_frame_on(OPEN_STREAM, 3),
    );
    let (stream, message) = conversation_error_parts(action)?;
    assert_eq!(
        stream, OPEN_STREAM,
        "the adapter open error rides the request's stream"
    );
    assert!(message.contains("open failed under test"));
    Ok(())
}
