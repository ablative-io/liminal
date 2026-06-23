use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Mutex};

use beamr::atom::Atom;
use beamr::native::native_process::{NativeContext, NativeHandler, NativeOutcome};
use beamr::process::ExitReason;
use beamr::term::Term;
use liminal::protocol::{
    Frame, MessageEnvelope, ProtocolError, ProtocolVersion, SchemaId as ProtocolSchemaId, decode,
    encode, encoded_len, negotiate_version,
};

use super::services::{
    ConnectionConversation, ConnectionServices, ConnectionSubscription, server_error_from_protocol,
};
use super::supervisor::ConnectionRuntime;
use crate::ServerError;

const READ_BUFFER_BYTES: usize = 8192;
const SERVER_ERROR_CODE: u16 = 0xFFFF;
const SUPPORTED_PROTOCOL: ProtocolVersion = ProtocolVersion::new(1, 0);

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
            if message == Term::atom(Atom::ERROR) {
                self.runtime
                    .mark_crashed(pid, ExitReason::Error, self.peer_addr);
                return NativeOutcome::Stop(ExitReason::Error);
            }
        }
        self.handle_stream(pid)
    }
}

#[derive(Debug, Default)]
pub(super) struct ConnectionProcessState {
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
    services: &dyn ConnectionServices,
    state: &mut ConnectionProcessState,
    frame: Frame,
) -> FrameAction {
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
            ..
        } => publish_response(services, stream_id, &channel, &envelope),
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
            stream_id,
            conversation_id,
            envelope,
            ..
        } => conversation_message(services, state, stream_id, conversation_id, &envelope),
        Frame::ConversationClose {
            conversation_id, ..
        } => conversation_close(services, state, conversation_id),
        Frame::Unknown { .. }
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
        match apply_frame(runtime.services(), state, frame) {
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
) -> FrameAction {
    match services.publish(channel, envelope) {
        Ok(message_id) => FrameAction::Respond(Frame::PublishAck {
            flags: 0,
            stream_id,
            message_id,
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

fn conversation_message(
    services: &dyn ConnectionServices,
    state: &ConnectionProcessState,
    stream_id: u32,
    conversation_id: u64,
    envelope: &MessageEnvelope,
) -> FrameAction {
    let Some(conversation) = state.conversations.get(&conversation_id) else {
        return FrameAction::Respond(Frame::ConversationError {
            flags: 0,
            stream_id,
            conversation_id,
            reason_code: SERVER_ERROR_CODE,
            message: Some("conversation is not open on this connection".to_owned()),
        });
    };
    match services.conversation_message(conversation, envelope) {
        Ok(()) => FrameAction::NoResponse,
        Err(error) => FrameAction::Respond(Frame::ConversationError {
            flags: 0,
            stream_id,
            conversation_id,
            reason_code: SERVER_ERROR_CODE,
            message: Some(error.to_string()),
        }),
    }
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
