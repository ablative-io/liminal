//! Inbound-frame application: maps a decoded client frame to a [`FrameAction`]
//! (a response to enqueue, silence, or a close) by delegating to the liminal
//! library through [`ConnectionServices`]. Split out of [`super::process`] so the
//! connection handler there stays focused on socket IO and the slice pump.

use std::time::Duration;

use liminal::protocol::{
    CONVERSATION_REPLY_REQUESTED_FLAG, Frame, MessageEnvelope, PUBLISH_DELIVERED_FLAG,
    ProtocolError, ProtocolVersion, SchemaId as ProtocolSchemaId, WorkerRegisterOutcome,
    WorkerRegistration, negotiate_version,
};

use super::conversation::ConnectionConversation;
use super::services::ConnectionServices;
use super::state::{ConnectionProcessState, FrameAction};
use super::supervisor::ConnectionRuntime;

const SERVER_ERROR_CODE: u16 = 0xFFFF;
const SUPPORTED_PROTOCOL: ProtocolVersion = ProtocolVersion::new(1, 0);

pub(super) fn apply_frame(
    pid: u64,
    runtime: &ConnectionRuntime,
    state: &mut ConnectionProcessState,
    frame: Frame,
) -> FrameAction {
    // Auth gate. When a token is configured, a connection must clear the `Connect`
    // handshake before any application frame is honoured. The token check itself
    // lives in `connect_response`, reached only via a `Connect` frame, so gating
    // solely there would let a client that skips `Connect` and sends
    // `Publish`/`Subscribe`/`WorkerRegister` straight through to the fan-out — this
    // per-frame gate closes that bypass, and the observability-tap arm below sits
    // behind it too. Frames without an application side effect are still admitted
    // pre-auth: `Connect` (which authenticates), `Disconnect` (a clean bow-out),
    // `Ping` (liveness), and every server-to-client/unknown frame the server ignores
    // unconditionally. Any other frame from an unauthenticated peer tears the
    // connection down with a silent `Close`. With no token configured this gate is
    // inert and the pre-auth behaviour is byte-identical.
    if runtime.auth_token().is_some() && !state.authenticated && !permitted_before_auth(&frame) {
        return FrameAction::Close;
    }
    let services = runtime.services();
    match frame {
        Frame::Connect {
            min_version,
            max_version,
            auth_token,
            ..
        } => connect_response(runtime, state, min_version, max_version, &auth_token),
        Frame::Disconnect { .. } => FrameAction::Close,
        Frame::Ping { .. } => FrameAction::Respond(Frame::Pong { flags: 0 }),
        Frame::Publish {
            stream_id,
            channel,
            envelope,
            idempotency_key,
            ..
        } => {
            // Offer the publish to the application's observability-drain tap first.
            // When it consumes the frame (the reserved observability channel), the
            // event was persisted/fanned-out out-of-band, so it must NOT also flow
            // through the normal channel machinery (which would reject an undeclared
            // channel), and the one-way publish gets no wire response.
            if runtime.notifier_channel_publish(pid, &channel, &envelope.payload) {
                FrameAction::NoResponse
            } else {
                publish_response(
                    services,
                    stream_id,
                    &channel,
                    &envelope,
                    idempotency_key.as_deref(),
                )
            }
        }
        Frame::Subscribe {
            stream_id,
            channel,
            accepted_schemas,
            ..
        } => subscribe_response(services, state, stream_id, &channel, &accepted_schemas),
        Frame::Unsubscribe {
            subscription_id, ..
        } => unsubscribe_response(services, state, subscription_id),
        Frame::ConversationOpen {
            conversation_id,
            subject,
            ..
        } => conversation_open(services, state, conversation_id, &subject),
        Frame::ConversationMessage {
            flags,
            stream_id,
            conversation_id,
            envelope,
        } => conversation_message(
            services,
            state,
            flags,
            stream_id,
            conversation_id,
            &envelope,
        ),
        Frame::ConversationClose {
            conversation_id, ..
        } => conversation_close(services, state, conversation_id),
        Frame::PushReply {
            correlation_id,
            payload,
            ..
        } => {
            // The client answered a server-initiated push: resolve the matching
            // one-shot reply slot so the server-side `PushReplyAwaiter` wakes with
            // the correlated payload. The server stays silent on the wire — the
            // reply terminates the push round trip.
            runtime.resolve_push(correlation_id, payload);
            FrameAction::NoResponse
        }
        Frame::WorkerRegister { registration, .. } => {
            worker_register_response(pid, runtime, registration)
        }
        // `Push`/`Deliver`/`WorkerRegisterAck` are server-to-client only; a client
        // must never originate one. Ignore these (and any stray/unknown inbound
        // frame) rather than treating them as fatal so a confused or malicious
        // client cannot tear the connection down with a stray frame.
        Frame::Push { .. }
        | Frame::Deliver { .. }
        | Frame::WorkerRegisterAck { .. }
        | Frame::Unknown { .. }
        | Frame::ConnectAck { .. }
        | Frame::ConnectError { .. }
        | Frame::SubscribeAck { .. }
        | Frame::SubscribeError { .. }
        | Frame::PublishAck { .. }
        | Frame::PublishError { .. }
        | Frame::ConversationError { .. }
        | Frame::Accept { .. }
        | Frame::Defer { .. }
        | Frame::Reject { .. }
        | Frame::Pong { .. } => FrameAction::NoResponse,
    }
}

/// Associates a worker registration with this connection and invokes the
/// configured connection notifier.
///
/// The notifier is consulted FIRST: only after the application accepts (or when
/// no notifier is configured) is the registration stored on the connection
/// record, so the close-path `on_worker_unregistered` fires for exactly the
/// connections the application accepted — a rejected worker leaves no record and
/// triggers no later deregistration. The ack is synchronous: a notifier error
/// yields a `Rejected` ack carrying the reason so the worker never believes it is
/// registered after the application declined it. With no notifier configured the
/// registration is accepted unconditionally, keeping liminal usable standalone.
fn worker_register_response(
    pid: u64,
    runtime: &ConnectionRuntime,
    registration: WorkerRegistration,
) -> FrameAction {
    if let Some(notifier) = runtime.notifier() {
        if let Err(error) = notifier.on_worker_registered(pid, &registration) {
            return worker_register_rejected(error.to_string());
        }
    }
    // Store only after acceptance. A poisoned-registry error here means the
    // accepted registration cannot be tracked for deregistration, so reject the
    // worker (and undo the application-side registration) rather than leave a
    // silent, never-deregistered association.
    if let Err(error) = runtime.set_registration(pid, registration) {
        if let Some(notifier) = runtime.notifier() {
            notifier.on_worker_unregistered(pid);
        }
        return worker_register_rejected(error.to_string());
    }
    FrameAction::Respond(Frame::WorkerRegisterAck {
        flags: 0,
        outcome: WorkerRegisterOutcome::Accepted,
    })
}

const fn worker_register_rejected(reason: String) -> FrameAction {
    FrameAction::Respond(Frame::WorkerRegisterAck {
        flags: 0,
        outcome: WorkerRegisterOutcome::Rejected { reason },
    })
}

/// Whether `frame` may be processed before the connection clears the auth gate.
///
/// Default-deny: only the handshake (`Connect`), a clean disconnect, a liveness
/// `Ping`, and the frames the server ignores unconditionally (server-to-client and
/// unknown kinds, which return `NoResponse` regardless) are admitted. Every
/// application frame — publish, subscribe, unsubscribe, conversation, worker-register,
/// push-reply — is held back until a successful `Connect`, so a newly added frame is
/// gated by default until it is explicitly listed here.
const fn permitted_before_auth(frame: &Frame) -> bool {
    matches!(
        frame,
        Frame::Connect { .. }
            | Frame::Disconnect { .. }
            | Frame::Ping { .. }
            | Frame::Push { .. }
            | Frame::Deliver { .. }
            | Frame::WorkerRegisterAck { .. }
            | Frame::Unknown { .. }
            | Frame::ConnectAck { .. }
            | Frame::ConnectError { .. }
            | Frame::SubscribeAck { .. }
            | Frame::SubscribeError { .. }
            | Frame::PublishAck { .. }
            | Frame::PublishError { .. }
            | Frame::ConversationError { .. }
            | Frame::Accept { .. }
            | Frame::Defer { .. }
            | Frame::Reject { .. }
            | Frame::Pong { .. }
    )
}

fn connect_response(
    runtime: &ConnectionRuntime,
    state: &mut ConnectionProcessState,
    min_version: ProtocolVersion,
    max_version: ProtocolVersion,
    auth_token: &[u8],
) -> FrameAction {
    // The auth check runs before version negotiation: an unauthenticated peer learns
    // nothing about the negotiated protocol. When no token is configured the server
    // is open and this check is skipped entirely, keeping the handshake byte-identical
    // to the pre-auth behaviour.
    if let Some(expected) = runtime.auth_token() {
        if !constant_time_eq(expected, auth_token) {
            let error = ProtocolError::AuthenticationFailure {
                message: Some("connection authentication token rejected".to_owned()),
            };
            return FrameAction::RespondThenClose(Frame::ConnectError {
                flags: 0,
                reason_code: error.reason_code(),
                message: error.message().map(str::to_owned),
            });
        }
    }
    // The token matched (or none is configured): mark the connection authenticated
    // so the `apply_frame` gate admits subsequent application frames on it.
    state.authenticated = true;
    match negotiate_version(min_version, max_version, &[SUPPORTED_PROTOCOL]) {
        Ok(selected_version) => FrameAction::Respond(Frame::ConnectAck {
            flags: 0,
            selected_version,
            capabilities: 0,
        }),
        Err(error) => FrameAction::Respond(Frame::ConnectError {
            flags: 0,
            reason_code: error.reason_code(),
            message: error.message().map(str::to_owned),
        }),
    }
}

/// Constant-time byte-slice equality for the connection auth token.
///
/// A short-circuiting `==` returns as soon as it hits the first differing byte, so
/// its running time leaks how many leading bytes a guess got right — the classic
/// timing side channel that lets an attacker recover a secret one byte at a time.
/// This folds an XOR of every overlapping byte pair into a single accumulator and
/// never returns early, so the loop's work depends only on the input lengths, not
/// on where (or whether) the first mismatch occurs. A length difference is folded
/// in up front so unequal-length inputs still traverse the whole overlap and always
/// report unequal. Implemented locally rather than pulling a crate: the only
/// constant-time-compare dependency in the tree (`constant_time_eq`, transitively
/// via blake3) is not a direct workspace dependency, and this five-line fold is the
/// spec-sanctioned shape.
fn constant_time_eq(expected: &[u8], candidate: &[u8]) -> bool {
    let mut difference = u8::from(expected.len() != candidate.len());
    for (left, right) in expected.iter().zip(candidate.iter()) {
        difference |= left ^ right;
    }
    difference == 0
}

fn publish_response(
    services: &dyn ConnectionServices,
    stream_id: u32,
    channel: &str,
    envelope: &MessageEnvelope,
    idempotency_key: Option<&str>,
) -> FrameAction {
    match services.publish(channel, envelope, idempotency_key) {
        Ok(outcome) => FrameAction::Respond(Frame::PublishAck {
            // Set the genuine-delivery flag only when the publish was accepted by
            // at least one subscriber. The ack is always sent on success (the
            // backpressure contract is unchanged); the flag bit is the additive
            // delivery-ack signal the caller can observe.
            flags: if outcome.delivered {
                PUBLISH_DELIVERED_FLAG
            } else {
                0
            },
            stream_id,
            message_id: outcome.message_id,
        }),
        Err(error) => FrameAction::Respond(Frame::PublishError {
            flags: 0,
            stream_id,
            reason_code: SERVER_ERROR_CODE,
            message: Some(error.to_string()),
        }),
    }
}

fn subscribe_response(
    services: &dyn ConnectionServices,
    state: &mut ConnectionProcessState,
    stream_id: u32,
    channel: &str,
    accepted_schemas: &[ProtocolSchemaId],
) -> FrameAction {
    match services.subscribe(channel, accepted_schemas) {
        Ok(mut subscription) => {
            // Record the client-chosen delivery stream so the pump can address
            // every `Deliver` to the stream the client is reading this subscription
            // on.
            subscription.set_stream_id(stream_id);
            let subscription_id = subscription.id();
            let selected_schema = subscription.selected_schema();
            state.subscriptions.insert(subscription_id, subscription);
            FrameAction::Respond(Frame::SubscribeAck {
                flags: 0,
                stream_id,
                subscription_id,
                selected_schema,
            })
        }
        Err(error) => FrameAction::Respond(Frame::SubscribeError {
            flags: 0,
            stream_id,
            reason_code: SERVER_ERROR_CODE,
            message: Some(error.to_string()),
        }),
    }
}

fn unsubscribe_response(
    services: &dyn ConnectionServices,
    state: &mut ConnectionProcessState,
    subscription_id: u64,
) -> FrameAction {
    if let Some(subscription) = state.subscriptions.remove(&subscription_id) {
        // Drop the delivery-sequence counter and any held-back frame with the
        // subscription so a re-subscribe that reuses the id restarts clean at 1 and
        // never flushes a stale delivery.
        state.delivery_seqs.remove(&subscription_id);
        state.held_deliveries.remove(&subscription_id);
        if let Err(error) = services.unsubscribe(subscription) {
            tracing::warn!(subscription_id, %error, "liminal unsubscribe failed");
        }
    }
    FrameAction::NoResponse
}

fn conversation_open(
    services: &dyn ConnectionServices,
    state: &mut ConnectionProcessState,
    conversation_id: u64,
    subject: &str,
) -> FrameAction {
    match services.open_conversation(conversation_id, subject) {
        Ok(conversation) => {
            state.conversations.insert(conversation_id, conversation);
            FrameAction::NoResponse
        }
        Err(error) => FrameAction::Respond(Frame::ConversationError {
            flags: 0,
            stream_id: 1,
            conversation_id,
            reason_code: SERVER_ERROR_CODE,
            message: Some(error.to_string()),
        }),
    }
}

/// Bounds how long the server waits for the participant's reply on the
/// request-reply path before reporting a `ConversationError` back to the caller.
/// The participant processes the forwarded message on a beamr scheduler slice, so
/// a reply is normally available promptly; this only guards against a stuck or
/// non-replying participant so the connection thread never blocks indefinitely.
const CONVERSATION_REPLY_TIMEOUT: Duration = Duration::from_secs(5);

fn conversation_message(
    services: &dyn ConnectionServices,
    state: &ConnectionProcessState,
    flags: u8,
    stream_id: u32,
    conversation_id: u64,
    envelope: &MessageEnvelope,
) -> FrameAction {
    let Some(conversation) = state.conversations.get(&conversation_id) else {
        return conversation_error(
            stream_id,
            conversation_id,
            "conversation is not open on this connection",
        );
    };
    if let Err(error) = services.conversation_message(conversation, envelope) {
        return conversation_error(stream_id, conversation_id, &error.to_string());
    }
    // Pre-existing fire-and-forget semantics: without the reply-requested flag the
    // server stays silent on success, exactly as before. The reply leg is purely
    // additive and only runs when the client explicitly asked for a correlated
    // reply on this frame.
    if flags & CONVERSATION_REPLY_REQUESTED_FLAG == 0 {
        return FrameAction::NoResponse;
    }
    conversation_reply(conversation, stream_id, conversation_id)
}

/// Drains the participant's correlated reply and frames it back to the caller.
///
/// The reply carries the same `conversation_id` (the correlation key) and the
/// reply-requested flag so the client read path can recognise it as the answer to
/// its request rather than an unrelated server-initiated frame.
fn conversation_reply(
    conversation: &ConnectionConversation,
    stream_id: u32,
    conversation_id: u64,
) -> FrameAction {
    match conversation.receive_reply(CONVERSATION_REPLY_TIMEOUT) {
        Ok(reply) => FrameAction::Respond(Frame::ConversationMessage {
            flags: CONVERSATION_REPLY_REQUESTED_FLAG,
            stream_id,
            conversation_id,
            envelope: reply,
        }),
        Err(error) => conversation_error(stream_id, conversation_id, &error.to_string()),
    }
}

fn conversation_error(stream_id: u32, conversation_id: u64, message: &str) -> FrameAction {
    FrameAction::Respond(Frame::ConversationError {
        flags: 0,
        stream_id,
        conversation_id,
        reason_code: SERVER_ERROR_CODE,
        message: Some(message.to_owned()),
    })
}

fn conversation_close(
    services: &dyn ConnectionServices,
    state: &mut ConnectionProcessState,
    conversation_id: u64,
) -> FrameAction {
    if let Some(conversation) = state.conversations.remove(&conversation_id) {
        if let Err(error) = services.close_conversation(conversation) {
            tracing::warn!(conversation_id, %error, "liminal conversation close failed");
        }
    }
    FrameAction::NoResponse
}

#[cfg(test)]
#[path = "process_tests.rs"]
mod tests;
