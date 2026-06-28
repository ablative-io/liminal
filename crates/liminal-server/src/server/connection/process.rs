use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use beamr::atom::Atom;
use beamr::native::native_process::{NativeContext, NativeHandler, NativeOutcome};
use beamr::process::ExitReason;
use beamr::term::Term;

use liminal::protocol::{
    CONVERSATION_REPLY_REQUESTED_FLAG, Frame, MessageEnvelope, PUBLISH_DELIVERED_FLAG,
    ProtocolError, ProtocolVersion, SchemaId as ProtocolSchemaId, decode, encode, encoded_len,
    negotiate_version,
};

use super::conversation::ConnectionConversation;
use super::services::{ConnectionServices, ConnectionSubscription, server_error_from_protocol};
use super::supervisor::{ConnectionControl, ConnectionRuntime};
use crate::ServerError;

const READ_BUFFER_BYTES: usize = 8192;
const SERVER_ERROR_CODE: u16 = 0xFFFF;
const SUPPORTED_PROTOCOL: ProtocolVersion = ProtocolVersion::new(1, 0);
/// Application stream id used for server-initiated push frames. Push is an
/// application-stream frame (non-zero stream id), like publish and conversation.
const PUSH_STREAM_ID: u32 = 1;

#[derive(Debug)]
pub(super) struct ConnectionProcess {
    runtime: Arc<ConnectionRuntime>,
    peer_addr: Option<SocketAddr>,
    stream: Option<TcpStream>,
    buffer: Vec<u8>,
    state: ConnectionProcessState,
}

impl ConnectionProcess {
    pub(super) fn from_holder(
        runtime: Arc<ConnectionRuntime>,
        peer_addr: Option<SocketAddr>,
        holder: &Arc<Mutex<Option<TcpStream>>>,
    ) -> Self {
        // The `NativeHandlerFactory` is `Fn + Send + Sync`, so the accepted
        // `TcpStream` cannot be moved into the closure (a `Fn` captures by shared
        // reference and may be invoked more than once for restart). The shared
        // `Arc<Mutex<Option<TcpStream>>>` is the interior-mutability proxy that
        // lets the FIRST handler build take the stream out exactly once; the
        // Mutex is required by the `Sync` bound, not incidental.
        //
        // If the lock is poisoned the take silently yields `None`, and the
        // process would later stop with a bare crash and no root cause. Log the
        // poisoning clearly (with the peer address) so a missing-stream handoff
        // is diagnosable instead of a mystery crash.
        let stream = match holder.lock() {
            Ok(mut held) => held.take(),
            Err(poisoned) => {
                tracing::error!(
                    peer_addr = ?peer_addr,
                    error = %poisoned,
                    "connection stream handoff failed: stream holder mutex was poisoned; \
                     the connection process will start without a stream and stop immediately"
                );
                None
            }
        };
        Self {
            runtime,
            peer_addr,
            stream,
            buffer: Vec::new(),
            state: ConnectionProcessState::default(),
        }
    }

    fn handle_stream(&mut self, pid: u64) -> NativeOutcome {
        let Some(stream) = self.stream.as_mut() else {
            self.runtime
                .mark_crashed(pid, ExitReason::Error, self.peer_addr);
            return NativeOutcome::Stop(ExitReason::Error);
        };
        match read_available(stream, &mut self.buffer) {
            Ok(ReadStatus::Closed) => {
                self.runtime.finish(pid);
                return NativeOutcome::Stop(ExitReason::Normal);
            }
            Ok(ReadStatus::WouldBlock) => {
                // No bytes ready on this non-blocking socket right now. Do NOT
                // sleep: that would block a beamr scheduler worker thread (the
                // supervisor runs `CONNECTION_SCHEDULER_THREADS`) on every idle
                // poll and starve every other connection process sharing it.
                // `NativeOutcome::Continue` maps to `SliceOutcome::Requeue`,
                // which re-queues this pid behind every other runnable process
                // (cooperative round-robin) and reschedules us to poll again —
                // yielding the thread without parking. `Wait` is wrong here:
                // it parks until a *message* arrives, but socket readiness does
                // not enqueue a message, so the connection would hang forever.
                return NativeOutcome::Continue;
            }
            Ok(ReadStatus::Read) => {}
            Err(error) => {
                tracing::warn!(connection_pid = pid, %error, "connection read failed");
                self.runtime
                    .mark_crashed(pid, ExitReason::Error, self.peer_addr);
                return NativeOutcome::Stop(ExitReason::Error);
            }
        }
        match process_buffer(stream, &self.runtime, &mut self.state, &mut self.buffer) {
            Ok(ProcessStatus::Continue) => NativeOutcome::Continue,
            Ok(ProcessStatus::Close) => {
                self.runtime.finish(pid);
                NativeOutcome::Stop(ExitReason::Normal)
            }
            Err(error) => {
                tracing::warn!(connection_pid = pid, %error, "connection process failed");
                self.runtime
                    .mark_crashed(pid, ExitReason::Error, self.peer_addr);
                NativeOutcome::Stop(ExitReason::Error)
            }
        }
    }

    fn handle_control(&mut self, pid: u64, control: ConnectionControl) -> Option<NativeOutcome> {
        match control {
            ConnectionControl::NotifyShutdown => {
                self.notify_shutdown(pid, true);
                None
            }
            ConnectionControl::ForceClose => {
                self.notify_shutdown(pid, false);
                self.stream.take();
                self.runtime.finish(pid);
                Some(NativeOutcome::Stop(ExitReason::Normal))
            }
            ConnectionControl::Push {
                correlation_id,
                payload,
            } => {
                self.write_push(pid, correlation_id, payload);
                None
            }
        }
    }

    /// Writes a server-initiated [`Frame::Push`] out on this connection's stream.
    ///
    /// This is the only place the server originates a frame to the client. A write
    /// failure (or an encode failure, or a missing stream) is logged and the slot
    /// is cancelled so the awaiter does not block forever on a reply that can never
    /// arrive; the connection itself is left to its normal read-side lifecycle.
    fn write_push(&mut self, pid: u64, correlation_id: u64, payload: Vec<u8>) {
        let Some(stream) = self.stream.as_mut() else {
            tracing::warn!(
                connection_pid = pid,
                correlation_id,
                "server push skipped because connection stream is unavailable"
            );
            self.runtime.cancel_push(correlation_id);
            return;
        };
        let frame = match Frame::new_push(PUSH_STREAM_ID, correlation_id, payload) {
            Ok(frame) => frame,
            Err(error) => {
                tracing::warn!(
                    connection_pid = pid,
                    correlation_id,
                    %error,
                    "server push frame could not be constructed"
                );
                self.runtime.cancel_push(correlation_id);
                return;
            }
        };
        if let Err(error) = write_frame(stream, &frame) {
            tracing::warn!(
                connection_pid = pid,
                correlation_id,
                %error,
                "server push write failed; the push reply slot is cancelled"
            );
            self.runtime.cancel_push(correlation_id);
        }
    }

    fn notify_shutdown(&mut self, pid: u64, subscribers_only: bool) {
        if self.state.shutdown_notification_attempted {
            return;
        }
        if subscribers_only && self.state.subscriptions.is_empty() {
            return;
        }

        self.state.shutdown_notification_attempted = true;
        let Some(stream) = self.stream.as_mut() else {
            tracing::warn!(
                connection_pid = pid,
                peer_addr = ?self.peer_addr,
                "shutdown notification skipped because connection stream is unavailable"
            );
            return;
        };

        match write_frame(stream, &Frame::Disconnect { flags: 0 }) {
            Ok(()) => {
                tracing::debug!(
                    connection_pid = pid,
                    peer_addr = ?self.peer_addr,
                    subscriber_count = self.state.subscriptions.len(),
                    "sent shutdown notification to connection"
                );
            }
            Err(error) => {
                tracing::warn!(
                    connection_pid = pid,
                    peer_addr = ?self.peer_addr,
                    %error,
                    "shutdown notification failed; connection will not be retried"
                );
            }
        }
    }

    fn handle_message(&mut self, pid: u64, message: Term) -> Option<NativeOutcome> {
        if message == Term::atom(Atom::ERROR) {
            self.runtime
                .mark_crashed(pid, ExitReason::Error, self.peer_addr);
            return Some(NativeOutcome::Stop(ExitReason::Error));
        }
        if message.as_atom() == Some(self.runtime.control_atom()) {
            while let Some(control) = self.runtime.pop_control(pid) {
                if let Some(outcome) = self.handle_control(pid, control) {
                    return Some(outcome);
                }
            }
        }
        None
    }
}

impl NativeHandler for ConnectionProcess {
    fn handle(&mut self, ctx: &mut NativeContext<'_>) -> NativeOutcome {
        let pid = ctx.self_pid();
        // Registration is owned solely by the spawn thread (`SupervisorInner::
        // spawn_connection` calls `runtime.register` before returning the
        // handle), so the handler never writes the registry — it only reads its
        // record via `mark_crashed`/`finish`. This removes the previous
        // double-write (spawn-thread `insert` racing a handler `or_insert`) and
        // its lost-update/duplicate-record hazard.
        while let Some(message) = ctx.recv() {
            if let Some(outcome) = self.handle_message(pid, message) {
                return outcome;
            }
        }
        self.handle_stream(pid)
    }
}

#[derive(Debug, Default)]
pub(super) struct ConnectionProcessState {
    shutdown_notification_attempted: bool,
    subscriptions: HashMap<u64, ConnectionSubscription>,
    conversations: HashMap<u64, ConnectionConversation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProcessStatus {
    Continue,
    Close,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum FrameAction {
    Respond(Frame),
    NoResponse,
    Close,
}

pub(super) fn apply_frame(
    runtime: &ConnectionRuntime,
    state: &mut ConnectionProcessState,
    frame: Frame,
) -> FrameAction {
    let services = runtime.services();
    match frame {
        Frame::Connect {
            min_version,
            max_version,
            ..
        } => connect_response(min_version, max_version),
        Frame::Disconnect { .. } => FrameAction::Close,
        Frame::Ping { .. } => FrameAction::Respond(Frame::Pong { flags: 0 }),
        Frame::Publish {
            stream_id,
            channel,
            envelope,
            idempotency_key,
            ..
        } => publish_response(
            services,
            stream_id,
            &channel,
            &envelope,
            idempotency_key.as_deref(),
        ),
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
        // `Push` is server-to-client only; a client must never originate one. Ignore
        // it rather than treating it as a fatal protocol error so a confused or
        // malicious client cannot tear the connection down with a stray push.
        Frame::Push { .. }
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

fn process_buffer(
    stream: &mut TcpStream,
    runtime: &ConnectionRuntime,
    state: &mut ConnectionProcessState,
    buffer: &mut Vec<u8>,
) -> Result<ProcessStatus, ServerError> {
    loop {
        let (frame, consumed) = match decode(buffer) {
            Ok(decoded) => decoded,
            Err(
                ProtocolError::IncompleteHeader { .. } | ProtocolError::TruncatedPayload { .. },
            ) => {
                return Ok(ProcessStatus::Continue);
            }
            Err(error) => return Err(server_error_from_protocol(&error)),
        };
        buffer.drain(..consumed);
        match apply_frame(runtime, state, frame) {
            FrameAction::Respond(response) => write_frame(stream, &response)?,
            FrameAction::NoResponse => {}
            FrameAction::Close => return Ok(ProcessStatus::Close),
        }
    }
}

fn connect_response(min_version: ProtocolVersion, max_version: ProtocolVersion) -> FrameAction {
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
        Ok(subscription) => {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadStatus {
    Read,
    WouldBlock,
    Closed,
}

fn read_available(stream: &mut TcpStream, buffer: &mut Vec<u8>) -> Result<ReadStatus, ServerError> {
    let mut chunk = [0_u8; READ_BUFFER_BYTES];
    match stream.read(&mut chunk) {
        Ok(0) => Ok(ReadStatus::Closed),
        Ok(bytes_read) => {
            buffer.extend_from_slice(&chunk[..bytes_read]);
            Ok(ReadStatus::Read)
        }
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(ReadStatus::WouldBlock),
        Err(error) if error.kind() == std::io::ErrorKind::Interrupted => Ok(ReadStatus::WouldBlock),
        Err(error) => Err(ServerError::ListenerAccept {
            message: format!("failed to read connection stream: {error}"),
        }),
    }
}

fn write_frame(stream: &mut TcpStream, frame: &Frame) -> Result<(), ServerError> {
    let frame_len = encoded_len(frame).map_err(|error| server_error_from_protocol(&error))?;
    let mut bytes = vec![0_u8; frame_len];
    let written = encode(frame, &mut bytes).map_err(|error| server_error_from_protocol(&error))?;
    let Some(encoded) = bytes.get(..written) else {
        return Err(ServerError::ListenerAccept {
            message: "protocol encoder returned an invalid byte count".to_owned(),
        });
    };
    stream
        .write_all(encoded)
        .map_err(|error| ServerError::ListenerAccept {
            message: format!("failed to write connection response: {error}"),
        })
}

#[cfg(test)]
#[path = "process_tests.rs"]
mod tests;
