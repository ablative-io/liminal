//! Inbound-frame application: maps a decoded client frame to a [`FrameAction`]
//! (a response to enqueue, silence, or a close) by delegating to the liminal
//! library through [`ConnectionServices`]. Split out of [`super::process`] so the
//! connection handler there stays focused on socket IO and the slice pump.

use std::time::Instant;

use liminal::protocol::{
    CONVERSATION_REPLY_REQUESTED_FLAG, Frame, MessageEnvelope, PUBLISH_DELIVERED_FLAG,
    ProtocolError, ProtocolVersion, SchemaId as ProtocolSchemaId, WorkerRegisterOutcome,
    WorkerRegistration, negotiate_version,
};
use liminal_protocol::wire::PARTICIPANT_FRAME_TYPE;

use super::services::ConnectionServices;
use super::state::{ConnectionProcessState, FrameAction};
use super::supervisor::ConnectionRuntime;
use crate::ServerError;
use crate::config::types::ServiceProfile;
use crate::server::participant::{
    PARTICIPANT_CAPABILITY_BIT, ParticipantConnectionContext, ParticipantDispatch,
    ParticipantIngress, constant_time_eq, dispatch_generic_frame, encode_server_value,
    gate_generic_frame,
};

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
        } => connect_once(runtime, state, min_version, max_version, &auth_token),
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
        } => subscribe_response(pid, runtime, state, stream_id, &channel, &accepted_schemas),
        Frame::Unsubscribe {
            stream_id,
            subscription_id,
            ..
        } => unsubscribe_response(services, state, stream_id, subscription_id),
        Frame::ConversationOpen {
            stream_id,
            conversation_id,
            subject,
            ..
        } => conversation_open(pid, runtime, state, stream_id, conversation_id, &subject),
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
            stream_id,
            conversation_id,
            ..
        } => conversation_close(services, state, stream_id, conversation_id),
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
        frame @ Frame::Unknown {
            type_id: PARTICIPANT_FRAME_TYPE,
            ..
        } => participant_frame_response(runtime, state, &frame),
        // Client backpressure signals ride the subscription delivery machinery, so
        // they are application frames: gated by auth (they are NOT on the pre-auth
        // allowlist) and refused by a services profile that serves no
        // subscriptions.
        Frame::Accept { stream_id, .. }
        | Frame::Defer { stream_id, .. }
        | Frame::Reject { stream_id, .. } => pressure_response(services, stream_id),
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
        | Frame::Pong { .. } => FrameAction::NoResponse,
    }
}

/// Applies the core protocol's single-use connection handshake.
fn connect_once(
    runtime: &ConnectionRuntime,
    state: &mut ConnectionProcessState,
    min_version: ProtocolVersion,
    max_version: ProtocolVersion,
    auth_token: &[u8],
) -> FrameAction {
    // The core lifecycle excludes `Connect` from its Active state; accepting a
    // second one would silently renegotiate capability/session state in place.
    if state.authenticated {
        FrameAction::Close
    } else {
        connect_response(runtime, state, min_version, max_version, auth_token)
    }
}

/// Applies the shared participant transport gate before semantic dispatch.
///
/// Semantic request handling is installed by the participant lifecycle service;
/// this boundary already guarantees that malformed, unauthenticated, and
/// capability-missing frames receive the crate's exact typed rejection.
fn participant_frame_response(
    runtime: &ConnectionRuntime,
    state: &mut ConnectionProcessState,
    frame: &Frame,
) -> FrameAction {
    match (runtime.participant_service(), state.connection_incarnation) {
        (Some(service), Some(connection_incarnation)) => {
            match dispatch_generic_frame(
                frame,
                state.authenticated,
                state.participant_session,
                ParticipantConnectionContext::new(connection_incarnation),
                &mut state.participant_conversations,
                service.handler(),
            ) {
                ParticipantDispatch::NotParticipant => FrameAction::NoResponse,
                ParticipantDispatch::Respond(response) => FrameAction::Respond(response),
                ParticipantDispatch::RespondThenClose(response) => {
                    FrameAction::RespondThenClose(response)
                }
                ParticipantDispatch::Fatal(error) => {
                    tracing::warn!(%error, "participant request failed closed");
                    FrameAction::Close
                }
            }
        }
        (None, None) => participant_frame_without_service(state, frame),
        // A complete service and a durable connection incarnation are one
        // activation invariant. Any mismatch is internal configuration drift;
        // close without fabricating a lifecycle outcome.
        (Some(_), None) | (None, Some(_)) => FrameAction::Close,
    }
}

/// Applies only the crate-owned pre-semantic gate while participant capability
/// is disabled. A decoded request cannot occur because the session is missing.
fn participant_frame_without_service(state: &ConnectionProcessState, frame: &Frame) -> FrameAction {
    match gate_generic_frame(frame, state.authenticated, state.participant_session) {
        ParticipantIngress::NotParticipant => FrameAction::NoResponse,
        // A decoded request is unreachable while the capability is deliberately
        // not advertised. Close if configuration drift ever makes it reachable;
        // silently consuming an authorized lifecycle operation would make a
        // retry indistinguishable from a lost commit. A locally unrepresentable
        // generic frame is likewise terminal.
        ParticipantIngress::Request(_) | ParticipantIngress::InvalidGenericFrame => {
            FrameAction::Close
        }
        ParticipantIngress::Rejected(rejection) => encode_server_value(
            liminal_protocol::wire::ServerValue::ParticipantTransportRejected(rejection),
        )
        .map_or(FrameAction::Close, FrameAction::RespondThenClose),
    }
}

/// Client backpressure signals (`Accept`/`Defer`/`Reject`). Full mode consumes
/// them with no wire response, exactly as before. The worker front door serves no
/// subscriptions — no delivery can exist for a client to signal pressure on — so
/// the frame is rejected on the subscription error channel (`SubscribeError`, the
/// same closest-honest-fit vocabulary as the unsubscribe rejection).
fn pressure_response(services: &dyn ConnectionServices, stream_id: u32) -> FrameAction {
    if services.supports_channel_operations() {
        FrameAction::NoResponse
    } else {
        FrameAction::Respond(Frame::SubscribeError {
            flags: 0,
            stream_id,
            reason_code: SERVER_ERROR_CODE,
            message: Some(unsupported_by_front_door("backpressure signaling")),
        })
    }
}

/// Renders the worker-front-door refusal text for `operation` from the typed
/// [`ServerError::UnsupportedOperation`] variant, so every front-door rejection —
/// whether raised in the services adapter or short-circuited here — speaks the one
/// error vocabulary.
fn unsupported_by_front_door(operation: &str) -> String {
    ServerError::UnsupportedOperation {
        operation: operation.to_owned(),
        profile: ServiceProfile::WORKER_FRONT_DOOR,
    }
    .to_string()
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
/// push-reply, and the `Accept`/`Defer`/`Reject` backpressure signals (which act on
/// subscription delivery state) — is held back until a successful `Connect`, so a
/// newly added frame is gated by default until it is explicitly listed here.
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
    let selected_version = match negotiate_version(min_version, max_version, &[SUPPORTED_PROTOCOL])
    {
        Ok(selected_version) => selected_version,
        Err(error) => {
            return FrameAction::RespondThenClose(Frame::ConnectError {
                flags: 0,
                reason_code: error.reason_code(),
                message: error.message().map(str::to_owned),
            });
        }
    };
    let (participant_session, capabilities) =
        match (runtime.participant_service(), state.connection_incarnation) {
            (Some(service), Some(_)) => {
                let mut participant_session =
                    crate::server::participant::ParticipantSession::default();
                participant_session.negotiate_v1(service.frame_limit());
                (participant_session, PARTICIPANT_CAPABILITY_BIT)
            }
            // A generic durable channel store alone does not activate the participant
            // protocol. Both service and incarnation are absent.
            (None, None) => (crate::server::participant::ParticipantSession::default(), 0),
            // Fail closed on impossible partial activation without inventing a
            // participant response.
            (Some(_), None) | (None, Some(_)) => return FrameAction::Close,
        };

    // Publish handshake state only after authentication, version negotiation,
    // and participant posture have all succeeded. Every rejection above leaves
    // the pre-handshake state untouched.
    state.authenticated = true;
    state.participant_session = participant_session;
    FrameAction::Respond(Frame::ConnectAck {
        flags: 0,
        selected_version,
        capabilities,
    })
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
    pid: u64,
    runtime: &ConnectionRuntime,
    state: &mut ConnectionProcessState,
    stream_id: u32,
    channel: &str,
    accepted_schemas: &[ProtocolSchemaId],
) -> FrameAction {
    let limits = runtime.limits();
    let max_subscriptions = limits.max_subscriptions_per_connection;
    // §5 `max_subscriptions_per_connection`: refuse a subscription past the cap
    // with the subscription error channel (the same vocabulary an unsupported or
    // failed subscribe uses), before touching the services adapter so an over-cap
    // subscribe never spawns a subscriber process.
    if state.subscriptions.len() >= max_subscriptions {
        return FrameAction::Respond(Frame::SubscribeError {
            flags: 0,
            stream_id,
            reason_code: SERVER_ERROR_CODE,
            message: Some(
                ServerError::ConnectionCapReached {
                    operation: "subscribe".to_owned(),
                    cap: "max_subscriptions_per_connection",
                    limit: max_subscriptions,
                }
                .to_string(),
            ),
        });
    }
    // R3 + §5: build the install spec CARRIED INTO subscribe, so the shared
    // connection byte budget, per-inbox fairness cap, and wake notifier are
    // installed on the inbox at construction — strictly BEFORE the registration
    // is published to the channel actor. There is no pre-install window: no
    // envelope can be admitted uncharged, past the depth cap, or without a wake.
    // The budget is created lazily on the first subscribe and shared across
    // every later subscription, so the 4 MiB is one connection-scoped pool. The
    // notifier captures the CONNECTION scheduler's `READY` enqueue handle here
    // (§1.2(2), Vesper advisory 3); under the busy loop the wake is redundant
    // (the every-slice pump still drains the inbox), so a missing waker —
    // scheduler-free unit tests — is harmless. PARK-FLIP: this is the wake that
    // replaces the delivery pump's every-slice assumption.
    let budget = state
        .inbox_budget
        .get_or_insert_with(|| {
            liminal::channel::ConnectionInboxBudget::new(limits.max_connection_inbox_bytes)
        })
        .clone();
    let notifier: Option<liminal::channel::InboxNotifier> = runtime.ready_waker(pid).map(|waker| {
        std::sync::Arc::new(move || {
            waker.fire();
        }) as liminal::channel::InboxNotifier
    });
    let install = liminal::channel::InboxInstall {
        budget,
        depth_cap: limits.max_subscription_inbox_depth,
        notifier,
    };
    let services = runtime.services();
    match services.subscribe(channel, accepted_schemas, Some(install)) {
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
    stream_id: u32,
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
        return FrameAction::NoResponse;
    }
    // No such subscription on this connection. Full mode keeps the idempotent no-op
    // (unsubscribing an unknown id is harmless). A capability-scoped adapter that
    // serves no channels never issued a subscription, so this frame targets an
    // operation the profile does not support: reject it explicitly rather than
    // silently accept it. There is no dedicated `UnsubscribeError` frame in the
    // vocabulary, so the closest honest fit — `SubscribeError`, the subscription
    // error channel — carries the rejection.
    if services.supports_channel_operations() {
        FrameAction::NoResponse
    } else {
        FrameAction::Respond(Frame::SubscribeError {
            flags: 0,
            stream_id,
            reason_code: SERVER_ERROR_CODE,
            message: Some(unsupported_by_front_door("unsubscribe")),
        })
    }
}

// Every `ConversationError` this function emits rides the REQUEST'S stream id
// (Sol round 2): a client that opened on stream 17 must see its typed refusal on
// stream 17 — a refusal on a hard-coded stream would leave the client's actual
// stream silent, turning the fail-closed refusals into a silent-timeout shape.
fn conversation_open(
    pid: u64,
    runtime: &ConnectionRuntime,
    state: &mut ConnectionProcessState,
    stream_id: u32,
    conversation_id: u64,
    subject: &str,
) -> FrameAction {
    // Duplicate open of a LIVE conversation id is refused fail-closed (review
    // ruling, round 1 item 3): an insert-replace would drop the old resource
    // without close/finalize (violating the D4 every-teardown-path-finalizes
    // discipline) and leave its pending/tombstone reply entries to FIFO-correlate
    // against the NEW instance's replies — cross-instance mis-correlation, the
    // corruption class the tombstone ruling exists to kill. Nothing sanctions
    // re-open-as-replace: the SDK explicitly guards against re-sending
    // `ConversationOpen` for an open id and calls a duplicate a defect
    // (`liminal-sdk/src/remote/tcp/connection.rs:48`). The remedy is named in the
    // error: close the conversation first, or use a fresh id.
    if state.conversations.contains_key(&conversation_id) {
        return conversation_error(
            stream_id,
            conversation_id,
            "conversation id is already open on this connection; close it before \
             reopening or use a fresh id",
        );
    }
    let max_conversations = runtime.limits().max_conversations_per_connection;
    // §5 `max_conversations_per_connection`: refuse opening past the cap before
    // constructing any supervised conversation actor. Duplicate ids were refused
    // above, so every admission here is a distinct live conversation.
    if state.conversations.len() >= max_conversations {
        return conversation_error(
            stream_id,
            conversation_id,
            &ServerError::ConnectionCapReached {
                operation: "conversation open".to_owned(),
                cap: "max_conversations_per_connection",
                limit: max_conversations,
            }
            .to_string(),
        );
    }
    match runtime
        .services()
        .open_conversation(conversation_id, subject)
    {
        Ok(conversation) => {
            // R1(vi)(a): install the reply-availability notifier PERMANENTLY at
            // conversation open (normative — not per-message). It fires the
            // connection's `READY` marker on the reply queue's empty→non-empty edge
            // and on terminal actor error, capturing the CONNECTION scheduler's
            // enqueue handle here at install. Under the busy loop the wake is
            // redundant (the slice polls pending conversations every slice); it is
            // the park-flip wake source. The notifier is cleared by the
            // conversation core at close/finalize, so a marker never fires after
            // teardown.
            if let Some(waker) = runtime.ready_waker(pid) {
                conversation.register_reply_notifier(std::sync::Arc::new(move || {
                    waker.fire();
                }));
            }
            state.conversations.insert(conversation_id, conversation);
            FrameAction::NoResponse
        }
        Err(error) => FrameAction::Respond(Frame::ConversationError {
            flags: 0,
            stream_id,
            conversation_id,
            reason_code: SERVER_ERROR_CODE,
            message: Some(error.to_string()),
        }),
    }
}

fn conversation_message(
    services: &dyn ConnectionServices,
    state: &mut ConnectionProcessState,
    flags: u8,
    stream_id: u32,
    conversation_id: u64,
    envelope: &MessageEnvelope,
) -> FrameAction {
    // A capability-scoped adapter serves no conversations, so a conversation could
    // never have been opened: reject with a clear "not supported" message rather
    // than the misleading "not open" one. Full mode keeps the existing path.
    if !services.supports_channel_operations() {
        return conversation_error(
            stream_id,
            conversation_id,
            &unsupported_by_front_door("conversation messaging"),
        );
    }
    let Some(conversation) = state.conversations.get(&conversation_id) else {
        return conversation_error(
            stream_id,
            conversation_id,
            "conversation is not open on this connection",
        );
    };
    // Pre-existing fire-and-forget semantics: without the reply-requested flag the
    // message is forwarded and the server stays silent on success, exactly as
    // before. The reply leg is purely additive and only runs when the client
    // explicitly asked for a correlated reply on this frame.
    if flags & CONVERSATION_REPLY_REQUESTED_FLAG == 0 {
        if let Err(error) = services.conversation_message(conversation, envelope) {
            return conversation_error(stream_id, conversation_id, &error.to_string());
        }
        return FrameAction::NoResponse;
    }
    // R1(vi) (§1.2(3b)) — THE LIVE BEHAVIOR CHANGE. The old path BLOCKED the
    // connection slice for up to 5 s draining the participant's reply
    // (`receive_reply`); now the reply-requested operation rides the bounded
    // per-connection pending-reply table and the slice returns immediately.
    //
    // ADMISSION COMES FIRST (cap-before-mutation, review round 1 item 1): the
    // reservation is made BEFORE the message is forwarded, so a cap-refused
    // request — including the tombstone self-wedge — NEVER reaches the
    // participant. A refused-but-forwarded request would produce an orphan
    // reply with no table entry, which could later FIFO-match a younger
    // admitted operation on the same conversation: wrong-reply delivery, the
    // corruption class the tombstone ruling kills. If the forward itself fails
    // after admission, the exact reservation is rolled back (no reply can ever
    // arrive for a message the participant never received, so the entry must
    // not linger to time out into a spurious tombstone).
    let op_id = match state
        .pending_replies
        .admit(conversation_id, stream_id, Instant::now())
    {
        Ok(op_id) => op_id,
        Err(error) => {
            return FrameAction::Respond(super::pending_reply::cap_refusal_frame(
                stream_id,
                conversation_id,
                &error,
            ));
        }
    };
    if let Err(error) = services.conversation_message(conversation, envelope) {
        state.pending_replies.cancel(op_id);
        return conversation_error(stream_id, conversation_id, &error.to_string());
    }
    FrameAction::NoResponse
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
    stream_id: u32,
    conversation_id: u64,
) -> FrameAction {
    if let Some(conversation) = state.conversations.remove(&conversation_id) {
        // §1.2(3b) close sweep — one of only TWO sanctioned tombstone-reclamation
        // triggers (review round 1 item 2): synchronously remove EVERY pending and
        // tombstone entry for this conversation, IN the close transaction, before
        // the actor close/finalize below and before any same-id reopen can be
        // applied from the same buffer. Without this, a tombstone-only
        // conversation's entries linger to connection teardown (the lazy
        // service_pending_replies sweep only visits conversations with PENDING
        // entries) and could FIFO-correlate against a reopened id's replies.
        state.pending_replies.remove_conversation(conversation_id);
        if let Err(error) = services.close_conversation(conversation) {
            tracing::warn!(conversation_id, %error, "liminal conversation close failed");
        }
        return FrameAction::NoResponse;
    }
    // No such conversation on this connection. Full mode keeps the idempotent
    // no-op; the worker front door serves no conversations, so a close targets an
    // unsupported operation — reject it explicitly rather than swallow it.
    if services.supports_channel_operations() {
        FrameAction::NoResponse
    } else {
        conversation_error(
            stream_id,
            conversation_id,
            &unsupported_by_front_door("conversation close"),
        )
    }
}

#[cfg(test)]
#[path = "process_tests.rs"]
mod tests;
