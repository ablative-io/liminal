use std::io::Read;
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Mutex};

use beamr::atom::Atom;
use beamr::native::native_process::{NativeContext, NativeHandler, NativeOutcome};
use beamr::process::ExitReason;
use beamr::term::Term;

use liminal::protocol::{Frame, ProtocolError, decode};

use super::apply::apply_frame;
use super::delivery::{DELIVERY_SLICE_BUDGET, service_subscriptions};
use super::outbound::OutboundWriter;
use super::services::server_error_from_protocol;
use super::state::{ConnectionProcessState, FrameAction, ProcessStatus};
use super::supervisor::{ConnectionControl, ConnectionRuntime};
use crate::ServerError;

const READ_BUFFER_BYTES: usize = 8192;
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
    /// Per-connection outbound byte buffer. EVERY server-originated frame (acks,
    /// errors, `Push`, `Disconnect`, `Pong`, `Deliver`, ...) is enqueued here and
    /// drained cooperatively on the slice loop with partial-write tracking, so a
    /// frame larger than the socket send buffer streams out across slices instead
    /// of failing a `write_all` on the non-blocking socket (ledger G4).
    outbound: OutboundWriter,
}

/// Whether a connection slice's socket/inbound servicing leaves the connection
/// running or has already resolved its lifecycle.
enum SliceStep {
    Continue,
    Stop(ExitReason),
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
            outbound: OutboundWriter::new(),
        }
    }

    /// Runs one connection scheduler slice: service inbound socket/control work,
    /// then service subscriptions into the outbound buffer, then drain the
    /// outbound buffer to the socket.
    ///
    /// The ordering is load-bearing (subscriptions are pumped AFTER socket/control
    /// work), and the whole slice preserves the no-sleep `Continue` discipline: a
    /// slice never parks a scheduler thread, it re-queues the process to poll again.
    fn handle_slice(&mut self, pid: u64) -> NativeOutcome {
        match self.service_socket(pid) {
            SliceStep::Stop(reason) => return NativeOutcome::Stop(reason),
            SliceStep::Continue => {}
        }
        // Pump subscriptions into the outbound buffer. An overflow (or an encode
        // fault) is fatal: a dropped or truncated delivery would desync the stream,
        // so the connection is torn down rather than allowed to continue desynced.
        if let Err(error) =
            service_subscriptions(&mut self.state, &mut self.outbound, DELIVERY_SLICE_BUDGET)
        {
            tracing::warn!(
                connection_pid = pid,
                %error,
                "outbound overflow while delivering; tearing down the connection"
            );
            self.runtime
                .mark_crashed(pid, ExitReason::Error, self.peer_addr);
            return NativeOutcome::Stop(ExitReason::Error);
        }
        // Drain queued outbound bytes with partial-write tracking. A hard write
        // error (or overflow surfaced here) tears the connection down.
        if let Err(error) = self.drain_outbound() {
            tracing::warn!(
                connection_pid = pid,
                %error,
                "outbound drain failed; tearing down the connection"
            );
            self.runtime
                .mark_crashed(pid, ExitReason::Error, self.peer_addr);
            return NativeOutcome::Stop(ExitReason::Error);
        }
        NativeOutcome::Continue
    }

    /// Services the inbound half of a slice: reads available bytes and applies any
    /// complete frames, enqueuing responses into the outbound buffer.
    fn service_socket(&mut self, pid: u64) -> SliceStep {
        let Some(stream) = self.stream.as_mut() else {
            self.runtime
                .mark_crashed(pid, ExitReason::Error, self.peer_addr);
            return SliceStep::Stop(ExitReason::Error);
        };
        match read_available(stream, &mut self.buffer) {
            Ok(ReadStatus::Closed) => {
                // Best-effort flush of anything still queued (e.g. WouldBlock residue
                // from earlier slices to a half-closed peer) before we let the stream
                // drop, mirroring the ForceClose drain. The buffered-writer refactor
                // removed this on the EOF path; without it, queued responses are lost.
                let _ = self.drain_outbound();
                self.runtime.finish(pid);
                return SliceStep::Stop(ExitReason::Normal);
            }
            Ok(ReadStatus::WouldBlock) => {
                // No bytes ready on this non-blocking socket right now. Do NOT
                // sleep: that would block a beamr scheduler worker thread (the
                // supervisor runs `CONNECTION_SCHEDULER_THREADS`) on every idle
                // poll and starve every other connection process sharing it. The
                // caller returns `NativeOutcome::Continue` (mapping to
                // `SliceOutcome::Requeue`), which re-queues this pid behind every
                // other runnable process (cooperative round-robin) and reschedules
                // us to poll — and, crucially, to drain any queued outbound bytes —
                // without parking. `Wait` is wrong here: it parks until a *message*
                // arrives, but socket readiness does not enqueue a message, so the
                // connection would hang forever.
                return SliceStep::Continue;
            }
            Ok(ReadStatus::Read) => {}
            Err(error) => {
                tracing::warn!(connection_pid = pid, %error, "connection read failed");
                self.runtime
                    .mark_crashed(pid, ExitReason::Error, self.peer_addr);
                return SliceStep::Stop(ExitReason::Error);
            }
        }
        match process_buffer(
            pid,
            &self.runtime,
            &mut self.state,
            &mut self.buffer,
            &mut self.outbound,
        ) {
            Ok(ProcessStatus::Continue) => SliceStep::Continue,
            Ok(ProcessStatus::Close) => {
                // A client-initiated close (e.g. a pipelined [Ping, Disconnect] or
                // [Publish, Disconnect] in one segment) may have enqueued a response
                // — the Pong/PublishAck — into the outbound buffer just before the
                // Disconnect returned Close. Drain it best-effort before finishing so
                // that queued reply is not dropped, mirroring the ForceClose drain.
                let _ = self.drain_outbound();
                self.runtime.finish(pid);
                SliceStep::Stop(ExitReason::Normal)
            }
            Err(error) => {
                tracing::warn!(connection_pid = pid, %error, "connection process failed");
                self.runtime
                    .mark_crashed(pid, ExitReason::Error, self.peer_addr);
                SliceStep::Stop(ExitReason::Error)
            }
        }
    }

    /// Drains queued outbound bytes to the socket, if the stream is still present.
    fn drain_outbound(&mut self) -> Result<(), super::outbound::OutboundError> {
        let Some(stream) = self.stream.as_mut() else {
            return Ok(());
        };
        self.outbound.drain(stream)
    }

    fn handle_control(&mut self, pid: u64, control: ConnectionControl) -> Option<NativeOutcome> {
        match control {
            ConnectionControl::NotifyShutdown => {
                self.notify_shutdown(pid, true);
                None
            }
            ConnectionControl::ForceClose => {
                self.notify_shutdown(pid, false);
                // Flush the enqueued `Disconnect` (and any residue) before the
                // stream is dropped; best-effort, since we are stopping regardless.
                let _ = self.drain_outbound();
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

    /// Enqueues a server-initiated [`Frame::Push`] into the outbound buffer.
    ///
    /// The frame is flushed by the slice's outbound drain (this control message is
    /// handled at the start of the slice, before `handle_slice` runs). A missing
    /// stream, an encode failure, or an outbound overflow cancels the push slot so
    /// the awaiter does not block forever on a reply that can never arrive; the
    /// connection itself is left to its normal lifecycle.
    fn write_push(&mut self, pid: u64, correlation_id: u64, payload: Vec<u8>) {
        if self.stream.is_none() {
            tracing::warn!(
                connection_pid = pid,
                correlation_id,
                "server push skipped because connection stream is unavailable"
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

    fn notify_shutdown(&mut self, pid: u64, subscribers_only: bool) {
        if self.state.shutdown_notification_attempted {
            return;
        }
        if subscribers_only && self.state.subscriptions.is_empty() {
            return;
        }

        self.state.shutdown_notification_attempted = true;
        if self.stream.is_none() {
            tracing::warn!(
                connection_pid = pid,
                peer_addr = ?self.peer_addr,
                "shutdown notification skipped because connection stream is unavailable"
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
        self.handle_slice(pid)
    }
}

fn process_buffer(
    pid: u64,
    runtime: &ConnectionRuntime,
    state: &mut ConnectionProcessState,
    buffer: &mut Vec<u8>,
    outbound: &mut OutboundWriter,
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
        match apply_frame(pid, runtime, state, frame) {
            FrameAction::Respond(response) => {
                outbound
                    .enqueue_frame(&response)
                    .map_err(|error| ServerError::ListenerAccept {
                        message: format!("failed to enqueue connection response: {error}"),
                    })?;
            }
            FrameAction::NoResponse => {}
            FrameAction::RespondThenClose(response) => {
                // Best-effort: enqueue the rejection frame (a `ConnectError` from the
                // auth gate) so the slice's Close-path drain can flush it, then close
                // regardless. Unlike `Respond`, a failed enqueue here is logged and
                // swallowed, never propagated: a rejected connection must be torn down
                // even when its rejection notice cannot be queued. The single
                // best-effort drain happens on the `ProcessStatus::Close` path in
                // `service_socket`, so no write is forced (or awaited) here.
                if let Err(error) = outbound.enqueue_frame(&response) {
                    tracing::warn!(
                        connection_pid = pid,
                        %error,
                        "auth-rejection frame could not be enqueued; closing anyway"
                    );
                }
                return Ok(ProcessStatus::Close);
            }
            FrameAction::Close => return Ok(ProcessStatus::Close),
        }
    }
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
            buffer.extend_from_slice(chunk.get(..bytes_read).unwrap_or(&[]));
            Ok(ReadStatus::Read)
        }
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(ReadStatus::WouldBlock),
        Err(error) if error.kind() == std::io::ErrorKind::Interrupted => Ok(ReadStatus::WouldBlock),
        Err(error) => Err(ServerError::ListenerAccept {
            message: format!("failed to read connection stream: {error}"),
        }),
    }
}
