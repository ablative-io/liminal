//! Per-connection process for the WebSocket sibling transport (R1.2/R1.3).
//!
//! One supervised beamr native process owns one upgraded
//! `tungstenite::WebSocket<TcpStream>`. Each scheduler slice services inbound
//! messages, the pending-reply table, the shared delivery pump, the Q-A
//! keepalive schedule, and the outbound drain — the SAME slice discipline,
//! semantic seam ([`apply_frame`]), shared [`ConnectionRuntime`], and
//! close-cause/cleanup paths as the TCP [`super::super::process`] reference
//! implementation, with the byte-stream read/write halves replaced by the
//! one-binary-message-one-canonical-frame transport contract.
//!
//! RFC 6455 Ping/Pong and Close are transport control here: they are never
//! converted to liminal `Frame::Ping`/`Frame::Pong`/`Frame::Disconnect`, and
//! liminal control frames remain ordinary canonical binary-message payloads.

use std::net::{SocketAddr, TcpStream};
use std::os::fd::AsRawFd;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use beamr::atom::Atom;
use beamr::native::native_process::{NativeContext, NativeHandler, NativeOutcome};
use beamr::process::ExitReason;
use beamr::scheduler::{Interest, ReadinessToken};
use beamr::term::Term;
use beamr::timer::TimerRef;

use tungstenite::Message;
use tungstenite::protocol::WebSocket;
use tungstenite::protocol::frame::CloseFrame;
use tungstenite::protocol::frame::coding::CloseCode;

use liminal::protocol::Frame;
use liminal_protocol::wire::ConnectionIncarnation;

use super::super::apply::apply_frame;
use super::super::delivery::{DELIVERY_SLICE_BUDGET, service_subscriptions};
use super::super::outbound::{DrainOutcome, OutboundError};
use super::super::participant_delivery::{
    UNIT2_PUSH_SLICE_BUDGET, has_held_participant_head, service_participant_publications,
};
use super::super::services::server_error_from_protocol;
use super::super::state::{ConnectionProcessState, FrameAction, ProcessStatus};
use super::super::supervisor::{ConnectionControl, ConnectionRuntime};
use super::outbound::WebSocketOutbound;
use super::{AcceptorSettings, WsInboundViolation, decode_ws_binary};
use crate::ServerError;
use crate::server::participant::ConnectionFateClass;

#[cfg(test)]
#[path = "process_tests.rs"]
mod tests;

/// Per-slice byte budget for the outbound drain, matching the TCP process's
/// read-buffer-sized drain budget so one connection cannot monopolize a
/// scheduler thread writing.
const DRAIN_SLICE_BYTES: usize = 8192;

/// Per-slice cap on complete inbound messages applied by one connection,
/// mirroring the delivery pump's slice budget rationale: bounds the work one
/// fast sender forces into a single slice so connections sharing the scheduler
/// stay fair. An exhausted budget re-queues the process (never parks it), so no
/// buffered message can be stranded behind a park.
const INBOUND_SLICE_MESSAGE_BUDGET: usize = 32;

/// Application stream id used for server-initiated push frames — the same
/// value the TCP process uses, so a push looks identical on both transports.
const PUSH_STREAM_ID: u32 = 1;

/// Q-A keepalive schedule: one transport-level Ping per interval per
/// connection. Pings are transport liveness ONLY — they never mint application
/// events, never re-arm application state, and never serve as a source of
/// truth; failure detection remains the socket's typed terminal events. Wakes
/// are timer-driven (`send_after` of the connection's `READY` atom), never
/// polled.
#[derive(Debug)]
struct KeepaliveSchedule {
    interval: Duration,
    next_due: Instant,
    /// The armed one-shot wake timer and the deadline it was armed for.
    armed: Option<(TimerRef, Instant)>,
}

impl KeepaliveSchedule {
    /// Builds the schedule with its first due instant. `None` when the
    /// interval overflows the monotonic clock — an extreme configured value
    /// must surface as a typed refusal, never an `Instant` addition panic
    /// (the S5 precedent).
    fn new(interval: Duration) -> Option<Self> {
        Instant::now().checked_add(interval).map(|next_due| Self {
            interval,
            next_due,
            armed: None,
        })
    }
}

/// Whether a slice's socket/inbound servicing leaves the connection running or
/// has already resolved its lifecycle (the TCP process's `SliceStep` shape).
enum SliceStep {
    Continue,
    Stop(ExitReason),
}

#[derive(Debug)]
pub(in super::super) struct WebSocketConnectionProcess {
    runtime: Arc<ConnectionRuntime>,
    peer_addr: Option<SocketAddr>,
    socket: Option<WebSocket<TcpStream>>,
    state: ConnectionProcessState,
    outbound: WebSocketOutbound,
    readiness_token: Option<ReadinessToken>,
    /// The configured Q-A keepalive interval; `None` means pings are disabled.
    keepalive_interval: Option<Duration>,
    /// The live keepalive schedule, initialised on the first slice so that an
    /// interval overflowing the monotonic clock is a typed connection failure
    /// rather than a constructor panic.
    keepalive: Option<KeepaliveSchedule>,
    /// Set when the inbound slice budget was exhausted with messages possibly
    /// still buffered inside tungstenite; consulted by the final probe so the
    /// process re-queues instead of parking past buffered work.
    inbound_budget_exhausted: bool,
}

impl WebSocketConnectionProcess {
    /// Builds the process from the spawn holder, taking the upgraded socket out
    /// exactly once — the same interior-mutability handoff the TCP process
    /// uses, with the same loud poisoned-holder diagnostics.
    pub(in super::super) fn from_holder(
        runtime: Arc<ConnectionRuntime>,
        peer_addr: Option<SocketAddr>,
        holder: &Arc<Mutex<Option<WebSocket<TcpStream>>>>,
        connection_incarnation: Option<ConnectionIncarnation>,
        settings: &Arc<AcceptorSettings>,
    ) -> Self {
        let socket = match holder.lock() {
            Ok(mut held) => held.take(),
            Err(poisoned) => {
                tracing::error!(
                    peer_addr = ?peer_addr,
                    error = %poisoned,
                    "websocket connection handoff failed: socket holder mutex was poisoned; \
                     the connection process will start without a socket and stop immediately"
                );
                None
            }
        };
        let limits = runtime.limits();
        let pending_replies = super::super::pending_reply::PendingReplyTable::new(
            limits.max_pending_replies_per_conversation,
            limits.max_pending_conversation_replies_per_connection,
            super::super::pending_reply::DEFAULT_REPLY_TIMEOUT,
        );
        let participant_publication = connection_incarnation.and_then(|_| {
            runtime
                .participant_service()
                .map(crate::server::participant::InstalledParticipantService::new_publication_inbox)
        });
        let state = ConnectionProcessState {
            connection_incarnation,
            participant_publication,
            pending_replies,
            ..ConnectionProcessState::default()
        };
        Self {
            runtime,
            peer_addr,
            socket,
            state,
            outbound: WebSocketOutbound::new(),
            readiness_token: None,
            keepalive_interval: settings.ping_interval,
            keepalive: None,
            inbound_budget_exhausted: false,
        }
    }

    /// Runs one connection scheduler slice with the TCP process's exact
    /// ordering: inbound socket/control work, pending replies, the shared
    /// delivery pump, the keepalive schedule, the outbound drain, timers,
    /// readiness arming, and the final probe.
    fn handle_slice(&mut self, pid: u64, ctx: &mut NativeContext<'_>) -> NativeOutcome {
        #[cfg(test)]
        self.runtime.record_slice(pid);
        if !self.runtime.is_registered(pid) {
            return NativeOutcome::Continue;
        }
        if let Err(error) = self.ensure_participant_publication_registered(pid) {
            return self.fail_slice(pid, &error);
        }
        match self.service_socket(pid) {
            SliceStep::Stop(reason) => return NativeOutcome::Stop(reason),
            SliceStep::Continue => {}
        }
        if let Err(error) = self.service_pending_replies() {
            tracing::warn!(
                connection_pid = pid,
                %error,
                "outbound overflow while writing conversation replies; tearing down"
            );
            return self.stop_crashed(pid);
        }
        if let Some(service) = self.runtime.participant_service() {
            let result = service_participant_publications(
                &mut self.state,
                service,
                &mut self.outbound,
                UNIT2_PUSH_SLICE_BUDGET,
            );
            if let Err(error) = result
                && !error.is_capacity_refusal()
            {
                tracing::error!(
                    connection_pid = pid,
                    %error,
                    "participant publication failed; tearing down the connection"
                );
                return self.stop_crashed(pid);
            }
        }
        match service_subscriptions(&mut self.state, &mut self.outbound, DELIVERY_SLICE_BUDGET) {
            Ok(shed) => self.shed_subscriptions(shed),
            Err(error) => {
                tracing::warn!(
                    connection_pid = pid,
                    %error,
                    "outbound overflow while delivering; tearing down the connection"
                );
                return self.stop_crashed(pid);
            }
        }
        if let Err(error) = self.service_keepalive(pid) {
            tracing::warn!(
                connection_pid = pid,
                %error,
                "keepalive ping write failed; tearing down the connection"
            );
            return self.stop_crashed(pid);
        }
        let drain = match self.drain_outbound(Some(DRAIN_SLICE_BYTES)) {
            Ok(drain) => drain,
            Err(error) => {
                tracing::warn!(
                    connection_pid = pid,
                    %error,
                    "outbound drain failed; tearing down the connection"
                );
                return self.stop_crashed(pid);
            }
        };
        if let Err(error) = self.sync_deadline_timers(pid, ctx) {
            return self.fail_slice(pid, &error);
        }
        if drain == DrainOutcome::Progress {
            return NativeOutcome::Continue;
        }
        if drain == DrainOutcome::Drained && has_held_participant_head(&self.state) {
            return NativeOutcome::Continue;
        }
        let interest = if drain == DrainOutcome::WouldBlockWithResidue {
            Interest::both()
        } else {
            Interest::READABLE
        };
        if let Err(error) = self.arm_readiness(pid, ctx, interest) {
            return self.fail_slice(pid, &error);
        }
        match self.final_probe(pid, ctx) {
            Ok(true) => NativeOutcome::Continue,
            Ok(false) => {
                #[cfg(test)]
                self.runtime.record_park(pid);
                NativeOutcome::Wait
            }
            Err(error) => self.fail_slice(pid, &error),
        }
    }

    /// Terminal crash path: release resources, record the crash, stop.
    fn stop_crashed(&mut self, pid: u64) -> NativeOutcome {
        self.close_transport_abnormal();
        self.release_conversations();
        self.runtime
            .mark_crashed(pid, ExitReason::Error, self.peer_addr);
        NativeOutcome::Stop(ExitReason::Error)
    }

    fn fail_slice(&mut self, pid: u64, error: &ServerError) -> NativeOutcome {
        tracing::error!(connection_pid = pid, %error, "connection readiness contract failed");
        self.stop_crashed(pid)
    }

    /// Services the inbound half of a slice: applies complete binary messages
    /// up to the slice budget, treating transport control per RFC 6455 and
    /// every contract violation as a typed terminal failure.
    fn service_socket(&mut self, pid: u64) -> SliceStep {
        self.inbound_budget_exhausted = false;
        if self.socket.is_none() {
            self.release_conversations();
            self.runtime
                .mark_crashed(pid, ExitReason::Error, self.peer_addr);
            return SliceStep::Stop(ExitReason::Error);
        }
        let mut processed: usize = 0;
        loop {
            if processed >= INBOUND_SLICE_MESSAGE_BUDGET {
                // Messages may remain buffered inside tungstenite: re-queue,
                // never park (the final probe consults this flag).
                self.inbound_budget_exhausted = true;
                return SliceStep::Continue;
            }
            let Some(socket) = self.socket.as_mut() else {
                return SliceStep::Stop(ExitReason::Error);
            };
            let message = match socket.read() {
                Ok(message) => message,
                Err(tungstenite::Error::Io(error))
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        || error.kind() == std::io::ErrorKind::Interrupted =>
                {
                    return SliceStep::Continue;
                }
                Err(tungstenite::Error::ConnectionClosed) => {
                    // The close handshake completed: the WS-clean equivalent of
                    // a TCP FIN. Mirror the TCP EOF path exactly.
                    let _ = self.drain_outbound(None);
                    if let Err(error) =
                        self.complete_connection_fate(ConnectionFateClass::ConnectionLost)
                    {
                        return self.fail_fate(pid, &error);
                    }
                    self.release_conversations();
                    self.runtime.finish(pid);
                    return SliceStep::Stop(ExitReason::Normal);
                }
                Err(error) => {
                    // Abrupt peer loss, a WS protocol violation, an oversize
                    // declared length beyond the pinned F2 bound, or an I/O
                    // failure: the same terminal supervision discipline as
                    // malformed TCP input. Close codes/details enrich the log
                    // only; they never select a different lifecycle.
                    tracing::warn!(
                        connection_pid = pid,
                        error = %error,
                        "websocket connection read failed"
                    );
                    if let Err(fate_error) =
                        self.complete_connection_fate(ConnectionFateClass::ConnectionLost)
                    {
                        return self.fail_fate(pid, &fate_error);
                    }
                    self.close_transport_abnormal();
                    self.release_conversations();
                    self.runtime
                        .mark_crashed(pid, ExitReason::Error, self.peer_addr);
                    return SliceStep::Stop(ExitReason::Error);
                }
            };
            processed = processed.saturating_add(1);
            if let Some(step) = self.handle_inbound_message(pid, message) {
                return step;
            }
        }
    }

    fn handle_inbound_message(&mut self, pid: u64, message: Message) -> Option<SliceStep> {
        match message {
            Message::Binary(bytes) => match self.apply_binary(pid, &bytes) {
                Ok(ProcessStatus::Continue) => None,
                Ok(ProcessStatus::Close) => Some(self.finish_normal_close(pid)),
                Ok(ProcessStatus::CloseWithFate(class)) => Some(self.finish_fate_close(pid, class)),
                Err(error) => {
                    tracing::warn!(connection_pid = pid, %error, "connection process failed");
                    self.close_transport_abnormal();
                    self.release_conversations();
                    self.runtime
                        .mark_crashed(pid, ExitReason::Error, self.peer_addr);
                    Some(SliceStep::Stop(ExitReason::Error))
                }
            },
            Message::Text(_) => {
                let violation = WsInboundViolation::TextMessage;
                tracing::warn!(connection_pid = pid, %violation, "websocket contract violation");
                let bound = match self.bound_protocol_refusal() {
                    Ok(bound) => bound,
                    Err(error) => return Some(self.fail_fate(pid, &error)),
                };
                if bound {
                    return Some(self.finish_fate_close(pid, ConnectionFateClass::ProtocolError));
                }
                self.close_transport_abnormal();
                self.release_conversations();
                self.runtime
                    .mark_crashed(pid, ExitReason::Error, self.peer_addr);
                Some(SliceStep::Stop(ExitReason::Error))
            }
            Message::Ping(_) => {
                // tungstenite queued the pong reply internally (RFC 6455
                // transport control); it rides out with the next flush.
                self.outbound.note_transport_write_pending();
                None
            }
            Message::Pong(_) => None,
            Message::Close(close_frame) => {
                tracing::debug!(
                    connection_pid = pid,
                    close_frame = ?close_frame,
                    "websocket peer initiated close"
                );
                self.outbound.note_transport_write_pending();
                Some(self.finish_fate_close(pid, ConnectionFateClass::ConnectionLost))
            }
            Message::Frame(_) => {
                tracing::warn!(
                    connection_pid = pid,
                    "unexpected raw websocket frame surfaced by the transport library"
                );
                let bound = match self.bound_protocol_refusal() {
                    Ok(bound) => bound,
                    Err(error) => return Some(self.fail_fate(pid, &error)),
                };
                if bound {
                    return Some(self.finish_fate_close(pid, ConnectionFateClass::ProtocolError));
                }
                self.close_transport_abnormal();
                self.release_conversations();
                self.runtime
                    .mark_crashed(pid, ExitReason::Error, self.peer_addr);
                Some(SliceStep::Stop(ExitReason::Error))
            }
        }
    }

    /// Applies one complete binary message: the shared participant frame-limit
    /// preflight, the one-message-one-canonical-frame check, then the SAME
    /// `apply_frame` seam and `FrameAction` handling as the TCP process.
    fn apply_binary(&mut self, pid: u64, bytes: &[u8]) -> Result<ProcessStatus, ServerError> {
        if let Some(rejection) = crate::server::participant::preflight_generic_bytes(
            bytes,
            self.state.authenticated,
            self.state.participant_session,
        ) {
            let response = crate::server::participant::encode_server_value(
                liminal_protocol::wire::ServerValue::ParticipantTransportRejected(rejection),
            )
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("failed to encode participant frame-limit rejection: {error:?}"),
            })?;
            self.outbound.enqueue_frame(&response).map_err(|error| {
                ServerError::ListenerAccept {
                    message: format!(
                        "failed to enqueue participant frame-limit rejection: {error}"
                    ),
                }
            })?;
            return Ok(ProcessStatus::Close);
        }
        let frame = match decode_ws_binary(bytes) {
            Ok(frame) => frame,
            Err(violation)
                if self.runtime.connection_has_bound_participant(
                    self.state.connection_incarnation,
                    &self.state.participant_conversations.tracked_conversations(),
                )? =>
            {
                tracing::warn!(connection_pid = pid, %violation, "bound websocket protocol refusal");
                return Ok(ProcessStatus::CloseWithFate(
                    ConnectionFateClass::ProtocolError,
                ));
            }
            Err(WsInboundViolation::MalformedFrame(
                error @ (liminal::protocol::ProtocolError::IncompleteHeader { .. }
                | liminal::protocol::ProtocolError::TruncatedPayload { .. }),
            )) => {
                return Err(ServerError::ListenerAccept {
                    message: format!(
                        "websocket contract violation: binary message is not one complete canonical liminal frame: {error}"
                    ),
                });
            }
            Err(WsInboundViolation::MalformedFrame(error)) => {
                return Err(server_error_from_protocol(&error));
            }
            Err(other) => {
                return Err(ServerError::ListenerAccept {
                    message: format!("websocket contract violation: {other}"),
                });
            }
        };
        match apply_frame(pid, &self.runtime, &mut self.state, frame) {
            FrameAction::Respond(response) => {
                self.outbound.enqueue_frame(&response).map_err(|error| {
                    ServerError::ListenerAccept {
                        message: format!("failed to enqueue connection response: {error}"),
                    }
                })?;
                Ok(ProcessStatus::Continue)
            }
            FrameAction::NoResponse => Ok(ProcessStatus::Continue),
            FrameAction::RespondThenClose(response) => {
                if let Err(error) = self.outbound.enqueue_frame(&response) {
                    tracing::warn!(
                        connection_pid = pid,
                        %error,
                        "auth-rejection frame could not be enqueued; closing anyway"
                    );
                }
                Ok(ProcessStatus::Close)
            }
            FrameAction::Close => Ok(ProcessStatus::Close),
            FrameAction::CloseWithFate(class) => Ok(ProcessStatus::CloseWithFate(class)),
        }
    }

    fn bound_protocol_refusal(&self) -> Result<bool, ServerError> {
        let conversations = self.state.participant_conversations.tracked_conversations();
        self.runtime
            .connection_has_bound_participant(self.state.connection_incarnation, &conversations)
    }

    fn complete_connection_fate(&self, class: ConnectionFateClass) -> Result<(), ServerError> {
        let conversations = self.state.participant_conversations.tracked_conversations();
        self.runtime.complete_connection_fate(
            self.state.connection_incarnation,
            class,
            &conversations,
        )
    }

    fn fail_fate(&mut self, pid: u64, error: &ServerError) -> SliceStep {
        tracing::error!(connection_pid = pid, %error, "websocket connection fate fold failed");
        self.close_transport_abnormal();
        self.release_conversations();
        self.runtime
            .mark_crashed(pid, ExitReason::Error, self.peer_addr);
        SliceStep::Stop(ExitReason::Error)
    }

    fn finish_fate_close(&mut self, pid: u64, class: ConnectionFateClass) -> SliceStep {
        let _ = self.drain_outbound(None);
        if let Err(error) = self.complete_connection_fate(class) {
            return self.fail_fate(pid, &error);
        }
        self.finish_normal_close(pid)
    }

    /// Finishes a client-initiated or terminal-rejection close: one unbudgeted
    /// best-effort drain (so terminal responses are not truncated by the slice
    /// budget), a best-effort transport close frame, then the shared cleanup.
    fn finish_normal_close(&mut self, pid: u64) -> SliceStep {
        let _ = self.drain_outbound(None);
        if let Some(socket) = self.socket.as_mut() {
            if let Err(error) = socket.close(Some(CloseFrame {
                code: CloseCode::Normal,
                reason: "".into(),
            })) {
                tracing::debug!(connection_pid = pid, %error, "websocket close frame not sent");
            }
            if let Err(error) = socket.flush() {
                tracing::debug!(connection_pid = pid, %error, "websocket close flush incomplete");
            }
        }
        self.release_conversations();
        self.runtime.finish(pid);
        SliceStep::Stop(ExitReason::Normal)
    }

    /// Best-effort abnormal-close notice (diagnostics for the peer only; the
    /// lifecycle decision was already taken). Never blocks, never retries.
    fn close_transport_abnormal(&mut self) {
        if let Some(socket) = self.socket.as_mut() {
            let _ = socket.close(Some(CloseFrame {
                code: CloseCode::Protocol,
                reason: "liminal websocket transport contract violation".into(),
            }));
            let _ = socket.flush();
        }
    }

    /// The TCP process's pending-reply servicing, byte-for-byte, against the
    /// message-framed outbound queue.
    fn service_pending_replies(&mut self) -> Result<(), OutboundError> {
        let now = Instant::now();
        for frame in self.state.pending_replies.expire_due(now) {
            self.outbound.enqueue_frame(&frame)?;
        }
        for conversation_id in self.state.pending_replies.conversations_awaiting_reply() {
            let Some(conversation) = self.state.conversations.get(&conversation_id) else {
                self.state
                    .pending_replies
                    .remove_conversation(conversation_id);
                continue;
            };
            while let Some(reply) = conversation.try_receive_reply() {
                if let Some(frame) = self
                    .state
                    .pending_replies
                    .match_reply(conversation_id, reply)
                {
                    self.outbound.enqueue_frame(&frame)?;
                }
            }
        }
        Ok(())
    }

    /// The TCP process's shed handling, byte-for-byte.
    fn shed_subscriptions(&mut self, shed: Vec<u64>) {
        for subscription_id in shed {
            self.state.delivery_seqs.remove(&subscription_id);
            self.state.held_deliveries.remove(&subscription_id);
            if let Some(subscription) = self.state.subscriptions.remove(&subscription_id) {
                if let Err(error) = self.runtime.services().unsubscribe(subscription) {
                    tracing::warn!(
                        subscription_id,
                        %error,
                        "releasing a shed (inbox-overflowed) subscription failed"
                    );
                }
            }
        }
    }

    /// Q-A: writes one transport-level Ping when the interval is due. Bounded
    /// to one ping per interval per connection; a transport-wedged buffer skips
    /// the ping (the next interval retries) rather than growing anything.
    fn service_keepalive(&mut self, pid: u64) -> Result<(), OutboundError> {
        let Some(interval) = self.keepalive_interval else {
            return Ok(());
        };
        if self.keepalive.is_none() {
            let Some(schedule) = KeepaliveSchedule::new(interval) else {
                return Err(OutboundError::Write(std::io::Error::other(
                    "websocket.ping_interval_ms overflows the monotonic clock",
                )));
            };
            self.keepalive = Some(schedule);
        }
        let Some(keepalive) = self.keepalive.as_mut() else {
            return Ok(());
        };
        let now = Instant::now();
        if now < keepalive.next_due {
            return Ok(());
        }
        let Some(next_due) = now.checked_add(keepalive.interval) else {
            return Err(OutboundError::Write(std::io::Error::other(
                "websocket.ping_interval_ms overflows the monotonic clock",
            )));
        };
        keepalive.next_due = next_due;
        let Some(socket) = self.socket.as_mut() else {
            return Ok(());
        };
        match socket.write(Message::Ping(tungstenite::Bytes::new())) {
            Ok(()) => {
                self.outbound.note_transport_write_pending();
                Ok(())
            }
            Err(tungstenite::Error::Io(error))
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.kind() == std::io::ErrorKind::Interrupted =>
            {
                self.outbound.note_transport_write_pending();
                Ok(())
            }
            Err(tungstenite::Error::WriteBufferFull(_)) => {
                tracing::debug!(
                    connection_pid = pid,
                    "keepalive ping skipped: transport write buffer is full"
                );
                Ok(())
            }
            Err(tungstenite::Error::Io(error)) => Err(OutboundError::Write(error)),
            Err(other) => Err(OutboundError::Write(std::io::Error::other(
                other.to_string(),
            ))),
        }
    }

    /// The TCP process's conversation/pending-reply release, byte-for-byte.
    fn release_conversations(&mut self) {
        self.release_participant_publication();
        self.state.pending_replies.cancel_all();
        for (_conversation_id, conversation) in std::mem::take(&mut self.state.conversations) {
            conversation.finalize();
        }
    }

    fn ensure_participant_publication_registered(&mut self, pid: u64) -> Result<(), ServerError> {
        if self.state.participant_publication_registered {
            return Ok(());
        }
        let (Some(incarnation), Some(inbox), Some(service), Some(waker)) = (
            self.state.connection_incarnation,
            self.state.participant_publication.as_ref(),
            self.runtime.participant_service(),
            self.runtime.ready_waker(pid),
        ) else {
            return Ok(());
        };
        service
            .publication_registry()
            .register(incarnation, inbox, waker)
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("participant publication registry failed: {error}"),
            })?;
        self.state.participant_publication_registered = true;
        Ok(())
    }

    fn release_participant_publication(&mut self) {
        if self.state.participant_publication_registered {
            if let (Some(incarnation), Some(service)) = (
                self.state.connection_incarnation,
                self.runtime.participant_service(),
            ) {
                service.publication_registry().deregister(incarnation);
            }
            self.state.participant_publication_registered = false;
        }
    }

    fn drain_outbound(&mut self, budget: Option<usize>) -> Result<DrainOutcome, OutboundError> {
        let Some(socket) = self.socket.as_mut() else {
            return Ok(DrainOutcome::Drained);
        };
        self.outbound.drain(socket, budget)
    }

    fn arm_readiness(
        &mut self,
        pid: u64,
        ctx: &NativeContext<'_>,
        interest: Interest,
    ) -> Result<(), ServerError> {
        let facility = ctx
            .readiness_facility()
            .ok_or_else(|| ServerError::ListenerAccept {
                message: "connection scheduler started without its required readiness service"
                    .to_owned(),
            })?;
        if let Some(token) = self.readiness_token {
            return facility
                .rearm(&token, interest)
                .map_err(|error| ServerError::ListenerAccept {
                    message: format!("failed to rearm connection readiness: {error}"),
                });
        }
        let socket = self
            .socket
            .as_ref()
            .ok_or_else(|| ServerError::ListenerAccept {
                message: "cannot register readiness for a missing connection socket".to_owned(),
            })?;
        let fd = socket.get_ref().as_raw_fd();
        let token = facility
            .register(fd, interest, pid, self.runtime.ready_atom())
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("failed to register connection readiness: {error}"),
            })?;
        if let Err(error) = self.runtime.set_readiness_token_once(pid, token, fd) {
            self.runtime.deregister_unpublished_readiness(token);
            return Err(error);
        }
        self.readiness_token = Some(token);
        Ok(())
    }

    /// The TCP process's deadline-timer sync plus the keepalive timer: retired
    /// pending-reply timers are cancelled, due deadlines are armed, and one
    /// timer wake is kept armed for the next keepalive due instant so a parked
    /// connection still pings on schedule.
    fn sync_deadline_timers(
        &mut self,
        pid: u64,
        ctx: &mut NativeContext<'_>,
    ) -> Result<(), ServerError> {
        for timer in self.state.pending_replies.take_retired_timers() {
            ctx.cancel_timer(timer);
        }
        for (op_id, delay) in self.state.pending_replies.timers_to_arm(Instant::now()) {
            let timer = ctx
                .send_after(delay, pid, Term::atom(self.runtime.ready_atom()))
                .ok_or_else(|| ServerError::ListenerAccept {
                    message: "connection scheduler has no timer facility for reply deadlines"
                        .to_owned(),
                })?;
            if !self.state.pending_replies.install_timer(op_id, timer) {
                ctx.cancel_timer(timer);
            }
        }
        if let Some(keepalive) = self.keepalive.as_mut() {
            let now = Instant::now();
            if let Some((timer, armed_for)) = keepalive.armed.take() {
                if armed_for == keepalive.next_due && armed_for > now {
                    // Still armed for the current deadline; keep it.
                    keepalive.armed = Some((timer, armed_for));
                } else {
                    // Fired or stale (the schedule advanced): retire it.
                    ctx.cancel_timer(timer);
                }
            }
            if keepalive.armed.is_none() {
                let delay = keepalive.next_due.saturating_duration_since(now);
                let timer = ctx
                    .send_after(delay, pid, Term::atom(self.runtime.ready_atom()))
                    .ok_or_else(|| ServerError::ListenerAccept {
                        message: "connection scheduler has no timer facility for keepalive pings"
                            .to_owned(),
                    })?;
                keepalive.armed = Some((timer, keepalive.next_due));
            }
        }
        Ok(())
    }

    /// Post-arm barrier probe: socket bytes, budget-stranded inbound, pumped
    /// sources, queued controls, and native mail — the TCP probe plus the
    /// inbound-budget flag that covers tungstenite's internal reassembly buffer.
    fn final_probe(&self, pid: u64, ctx: &NativeContext<'_>) -> Result<bool, ServerError> {
        let socket_ready = if let Some(socket) = self.socket.as_ref() {
            let mut byte = [0_u8; 1];
            match socket.get_ref().peek(&mut byte) {
                Ok(_) => true,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => false,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => true,
                Err(error) => {
                    return Err(ServerError::ListenerAccept {
                        message: format!("connection readiness probe failed: {error}"),
                    });
                }
            }
        } else {
            true
        };
        let subscription_ready = !self.state.held_deliveries.is_empty()
            || self
                .state
                .subscriptions
                .values()
                .any(|subscription| subscription.is_overflowed() || subscription.has_pending());
        let participant_ready = if self.state.held_pushes.capacity_refused()
            && has_held_participant_head(&self.state)
        {
            false
        } else {
            match self.state.participant_publication.as_ref() {
                Some(inbox) => {
                    inbox
                        .has_pending()
                        .map_err(|error| ServerError::ListenerAccept {
                            message: format!("participant publication final probe failed: {error}"),
                        })?
                }
                None => false,
            }
        };
        let reply_ready = self.state.pending_replies.has_due(Instant::now())
            || self
                .state
                .pending_replies
                .conversations_awaiting_reply()
                .into_iter()
                .filter_map(|id| self.state.conversations.get(&id))
                .any(super::super::conversation::ConnectionConversation::has_pending_reply);
        Ok(socket_ready
            || self.inbound_budget_exhausted
            || subscription_ready
            || participant_ready
            || reply_ready
            || self.runtime.has_control(pid)
            || self.runtime.ready_pending(pid)
            || ctx.has_messages())
    }

    /// The TCP process's control handling against the WebSocket transport.
    fn handle_control(&mut self, pid: u64, control: ConnectionControl) -> Option<NativeOutcome> {
        match control {
            ConnectionControl::NotifyShutdown => {
                self.notify_shutdown(pid, true);
                None
            }
            ConnectionControl::ForceClose => {
                self.notify_shutdown(pid, false);
                let _ = self.drain_outbound(None);
                if let Err(error) =
                    self.complete_connection_fate(ConnectionFateClass::ServerShutdown)
                {
                    return Some(match self.fail_fate(pid, &error) {
                        SliceStep::Continue => NativeOutcome::Continue,
                        SliceStep::Stop(reason) => NativeOutcome::Stop(reason),
                    });
                }
                if let Some(socket) = self.socket.as_mut() {
                    let _ = socket.close(Some(CloseFrame {
                        code: CloseCode::Away,
                        reason: "server shutdown".into(),
                    }));
                    let _ = socket.flush();
                }
                self.release_conversations();
                self.runtime.finish(pid);
                self.socket.take();
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

    /// The TCP process's push writer against the message-framed queue: a
    /// missing socket, an encode failure, or an outbound overflow cancels the
    /// push slot so the awaiter resolves promptly instead of hanging.
    fn write_push(&mut self, pid: u64, correlation_id: u64, payload: Vec<u8>) {
        if self.socket.is_none() {
            tracing::warn!(
                connection_pid = pid,
                correlation_id,
                "server push skipped because connection socket is unavailable"
            );
            self.runtime.cancel_push(correlation_id);
            return;
        }
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
        if let Err(error) = self.outbound.enqueue_frame(&frame) {
            tracing::warn!(
                connection_pid = pid,
                correlation_id,
                %error,
                "server push could not be enqueued; the push reply slot is cancelled"
            );
            self.runtime.cancel_push(correlation_id);
        }
    }

    /// The TCP process's shutdown notification, byte-for-byte: the liminal
    /// `Disconnect` frame rides as an ordinary canonical binary message.
    fn notify_shutdown(&mut self, pid: u64, subscribers_only: bool) {
        if self.state.shutdown_notification_attempted {
            return;
        }
        if subscribers_only && self.state.subscriptions.is_empty() {
            return;
        }
        self.state.shutdown_notification_attempted = true;
        if self.socket.is_none() {
            tracing::warn!(
                connection_pid = pid,
                peer_addr = ?self.peer_addr,
                "shutdown notification skipped because connection socket is unavailable"
            );
            return;
        }
        match self.outbound.enqueue_frame(&Frame::Disconnect { flags: 0 }) {
            Ok(()) => {
                tracing::debug!(
                    connection_pid = pid,
                    peer_addr = ?self.peer_addr,
                    subscriber_count = self.state.subscriptions.len(),
                    "enqueued shutdown notification to connection"
                );
            }
            Err(error) => {
                tracing::warn!(
                    connection_pid = pid,
                    peer_addr = ?self.peer_addr,
                    %error,
                    "shutdown notification could not be enqueued; connection will not be retried"
                );
            }
        }
    }

    /// The TCP process's mailbox handling: crash requests, control drains, and
    /// the bare `READY` wake vocabulary, unchanged.
    fn handle_message(&mut self, pid: u64, message: Term) -> Option<NativeOutcome> {
        if message == Term::atom(Atom::ERROR) {
            self.close_transport_abnormal();
            self.release_conversations();
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

impl NativeHandler for WebSocketConnectionProcess {
    fn handle(&mut self, ctx: &mut NativeContext<'_>) -> NativeOutcome {
        let pid = ctx.self_pid();
        while let Some(message) = ctx.recv() {
            if let Some(outcome) = self.handle_message(pid, message) {
                return outcome;
            }
        }
        self.runtime.acknowledge_ready(pid);
        self.handle_slice(pid, ctx)
    }
}

impl Drop for WebSocketConnectionProcess {
    fn drop(&mut self) {
        // Backstop for termination paths that never run another handler slice
        // (external termination, scheduler shutdown) — identical to the TCP
        // process's Drop discipline, plus the keepalive timer.
        self.release_conversations();
        let mut timers = self.state.pending_replies.take_retired_timers();
        if let Some(keepalive) = self.keepalive.as_mut() {
            if let Some((timer, _)) = keepalive.armed.take() {
                timers.push(timer);
            }
        }
        self.runtime.cancel_deadline_timers(timers);
    }
}
