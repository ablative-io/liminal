use std::io::Read;
use std::net::{SocketAddr, TcpStream};
use std::os::fd::AsRawFd;
use std::sync::{Arc, Mutex};

use beamr::atom::Atom;
use beamr::native::native_process::{NativeContext, NativeHandler, NativeOutcome};
use beamr::process::ExitReason;
use beamr::scheduler::{Interest, ReadinessToken};
use beamr::term::Term;

use liminal::protocol::{Frame, ProtocolError, decode};
use liminal_protocol::wire::ConnectionIncarnation;

use super::apply::apply_frame;
use super::delivery::{DELIVERY_SLICE_BUDGET, service_subscriptions};
use super::outbound::{DrainOutcome, OutboundWriter};
use super::participant_delivery::{
    UNIT2_PUSH_SLICE_BUDGET, has_held_participant_head, service_participant_publications,
};
use super::services::server_error_from_protocol;
use super::state::{ConnectionProcessState, FrameAction, ProcessStatus};
use super::supervisor::{ConnectionControl, ConnectionRuntime};
use crate::ServerError;
use crate::server::participant::ConnectionFateClass;

#[cfg(test)]
#[path = "process_teardown_tests.rs"]
mod teardown_tests;

#[cfg(test)]
#[path = "process_terminal_tests.rs"]
mod terminal_tests;

#[cfg(test)]
#[path = "process_wake_tests.rs"]
mod wake_tests;

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
    /// One registration for this connection's lifetime. The same token is copied
    /// into the host record so external death can deregister it.
    readiness_token: Option<ReadinessToken>,
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
        connection_incarnation: Option<ConnectionIncarnation>,
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
        // Build the pending-reply table from the runtime's configured §5 caps
        // (R1(vi), §1.2(3b)). The default table carries the signed defaults; this
        // uses the connection's actual limits.
        let limits = runtime.limits();
        let pending_replies = super::pending_reply::PendingReplyTable::new(
            limits.max_pending_replies_per_conversation,
            limits.max_pending_conversation_replies_per_connection,
            super::pending_reply::DEFAULT_REPLY_TIMEOUT,
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
        #[cfg(test)]
        let outbound = runtime
            .take_next_outbound_capacity()
            .map_or_else(OutboundWriter::new, OutboundWriter::with_capacity);
        #[cfg(not(test))]
        let outbound = OutboundWriter::new();
        Self {
            runtime,
            peer_addr,
            stream,
            buffer: Vec::new(),
            state,
            outbound,
            readiness_token: None,
        }
    }

    /// Runs one connection scheduler slice: service inbound socket/control work,
    /// then service subscriptions into the outbound buffer, then drain the
    /// outbound buffer to the socket.
    ///
    /// The ordering is load-bearing (subscriptions are pumped AFTER socket/control
    /// work), and the whole slice preserves the no-sleep `Continue` discipline: a
    /// slice never parks a scheduler thread, it re-queues the process to poll again.
    fn handle_slice(&mut self, pid: u64, ctx: &mut NativeContext<'_>) -> NativeOutcome {
        // R7 (§1.2(6)): count this serviced slice. The park-flip's permanent
        // rule-1 quiescence assertion — a parked connection's counter must not
        // advance without an event — reads this; under the busy loop the counter
        // advances every slice, and the instrument proves it counts.
        #[cfg(test)]
        self.runtime.record_slice(pid);
        // `spawn_native` may schedule the first slice before the spawn thread has
        // inserted the host record. Do not mint an unpublishable token; yield once
        // and register after the record exists.
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
        // R1(vi) (§1.2(3b)): service the pending-reply table each slice — expire
        // due deadlines (writing timeout frames) and drain/correlate any replies
        // the participants produced. A fatal outbound condition here tears the
        // connection down, matching the delivery path.
        if let Err(error) = self.service_pending_replies() {
            tracing::warn!(
                connection_pid = pid,
                %error,
                "outbound overflow while writing conversation replies; tearing down"
            );
            self.release_conversations();
            self.runtime
                .mark_crashed(pid, ExitReason::Error, self.peer_addr);
            return NativeOutcome::Stop(ExitReason::Error);
        }
        if let Some(outcome) = self.service_participant_pushes(pid) {
            return outcome;
        }
        // Pump subscriptions into the outbound buffer. An overflow (or an encode
        // fault) is fatal: a dropped or truncated delivery would desync the stream,
        // so the connection is torn down rather than allowed to continue desynced.
        match service_subscriptions(&mut self.state, &mut self.outbound, DELIVERY_SLICE_BUDGET) {
            Ok(shed) => self.shed_subscriptions(shed),
            Err(error) => {
                tracing::warn!(
                    connection_pid = pid,
                    %error,
                    "outbound overflow while delivering; tearing down the connection"
                );
                self.release_conversations();
                self.runtime
                    .mark_crashed(pid, ExitReason::Error, self.peer_addr);
                return NativeOutcome::Stop(ExitReason::Error);
            }
        }
        // Drain queued outbound bytes with partial-write tracking. A hard write
        // error (or overflow surfaced here) tears the connection down.
        let drain = match self.drain_outbound() {
            Ok(drain) => drain,
            Err(error) => {
                tracing::warn!(
                    connection_pid = pid,
                    %error,
                    "outbound drain failed; tearing down the connection"
                );
                if let Err(fate_error) =
                    self.complete_connection_fate(ConnectionFateClass::ConnectionLost)
                {
                    tracing::error!(connection_pid = pid, %fate_error, "connection-loss fate fold failed");
                }
                self.release_conversations();
                self.runtime
                    .mark_crashed(pid, ExitReason::Error, self.peer_addr);
                return NativeOutcome::Stop(ExitReason::Error);
            }
        };
        if let Err(error) = self.sync_deadline_timers(pid, ctx) {
            return self.fail_slice(pid, &error);
        }
        // A successful budget-limited write proves immediately actionable work
        // remains. Do not arm a permanently-writable socket in this state.
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
        #[cfg(test)]
        let barrier_staged = self.runtime.run_pre_wait_barrier();
        match self.final_probe(pid, ctx) {
            Ok(true) => {
                #[cfg(test)]
                if barrier_staged {
                    self.runtime.record_pre_wait_probe_hit();
                }
                NativeOutcome::Continue
            }
            Ok(false) => {
                #[cfg(test)]
                self.runtime.record_park(pid);
                NativeOutcome::Wait
            }
            Err(error) => self.fail_slice(pid, &error),
        }
    }

    fn service_participant_pushes(&mut self, pid: u64) -> Option<NativeOutcome> {
        let service = self.runtime.participant_service()?;
        #[cfg(test)]
        let held_before = self.state.held_pushes.participant_len();
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
            self.release_conversations();
            self.runtime
                .mark_crashed(pid, ExitReason::Error, self.peer_addr);
            return Some(NativeOutcome::Stop(ExitReason::Error));
        }
        #[cfg(test)]
        if self.state.held_pushes.participant_len() > held_before
            && self.runtime.pause_participant_holdback(pid)
        {
            return Some(NativeOutcome::Wait);
        }
        None
    }

    fn fail_slice(&mut self, pid: u64, error: &ServerError) -> NativeOutcome {
        tracing::error!(connection_pid = pid, %error, "connection readiness contract failed");
        self.release_conversations();
        self.runtime
            .mark_crashed(pid, ExitReason::Error, self.peer_addr);
        NativeOutcome::Stop(ExitReason::Error)
    }

    /// Services the inbound half of a slice: reads available bytes and applies any
    /// complete frames, enqueuing responses into the outbound buffer.
    fn service_socket(&mut self, pid: u64) -> SliceStep {
        if self.stream.is_none() {
            self.release_conversations();
            self.runtime
                .mark_crashed(pid, ExitReason::Error, self.peer_addr);
            return SliceStep::Stop(ExitReason::Error);
        }
        let Some(stream) = self.stream.as_mut() else {
            // Unreachable: the `is_none` guard above already handled a missing
            // stream. Kept as a total match so `stream` binds without an unwrap.
            return SliceStep::Stop(ExitReason::Error);
        };
        match read_available(stream, &mut self.buffer) {
            Ok(ReadStatus::Closed) => {
                // Best-effort flush of anything still queued (e.g. WouldBlock residue
                // from earlier slices to a half-closed peer) before we let the stream
                // drop, mirroring the ForceClose drain. The buffered-writer refactor
                // removed this on the EOF path; without it, queued responses are lost.
                let _ = self.drain_outbound();
                if let Err(error) =
                    self.complete_connection_fate(ConnectionFateClass::ConnectionLost)
                {
                    return self.fail_fate(pid, &error);
                }
                self.release_conversations();
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
                if let Err(fate_error) =
                    self.complete_connection_fate(ConnectionFateClass::ConnectionLost)
                {
                    return self.fail_fate(pid, &fate_error);
                }
                self.release_conversations();
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
            Ok(ProcessStatus::Close) => self.finish_normal_close(pid),
            Ok(ProcessStatus::CloseWithFate(class)) => self.finish_fate_close(pid, class),
            Err(error) => {
                tracing::warn!(connection_pid = pid, %error, "connection process failed");
                self.release_conversations();
                self.runtime
                    .mark_crashed(pid, ExitReason::Error, self.peer_addr);
                SliceStep::Stop(ExitReason::Error)
            }
        }
    }

    /// Releases every conversation this connection opened, finalizing each so its
    /// supervised actor and participant are terminated and their runtime
    /// registrations dropped — an abrupt connection teardown never leaks them.
    /// Finalization is bounded, non-blocking, and does not require the
    /// conversation scheduler to run a slice (or even be live): teardown runs on
    /// a connection scheduler worker and inside `Drop`, where waiting on another
    /// scheduler would wedge the worker or hang reap/shutdown. The interactive
    /// close round trip stays exclusively on the client-requested
    /// `ConversationClose` frame path. Every in-handler termination path (EOF,
    /// client `Close`, `ForceClose`, and each crash route) calls this before the
    /// runtime teardown; the `Drop` backstop covers the external-termination/reap
    /// and scheduler-shutdown paths that never run another slice. The
    /// conversation map is drained, so a second call finds nothing — the explicit
    /// paths and the backstop cannot double-finalize.
    fn release_conversations(&mut self) {
        self.release_participant_publication();
        // R1(vi)/§1.2(5): cancel every pending-reply entry BEFORE the conversation
        // actors are torn down, so no entry (and no timeout write) outlives its
        // connection. Finalizing each conversation below clears its reply notifier
        // in the conversation core, so no marker fires after teardown.
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
            .map_err(participant_publication_error)?;
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

    /// Finishes a client-initiated or terminal-rejection close.
    fn complete_connection_fate(&self, class: ConnectionFateClass) -> Result<(), ServerError> {
        let conversations = self.state.participant_conversations.tracked_conversations();
        self.runtime.complete_connection_fate(
            self.state.connection_incarnation,
            class,
            &conversations,
        )
    }

    fn fail_fate(&mut self, pid: u64, error: &ServerError) -> SliceStep {
        tracing::error!(connection_pid = pid, %error, "connection fate fold failed");
        self.release_conversations();
        self.runtime
            .mark_crashed(pid, ExitReason::Error, self.peer_addr);
        SliceStep::Stop(ExitReason::Error)
    }

    fn finish_fate_close(&mut self, pid: u64, class: ConnectionFateClass) -> SliceStep {
        let _ = self.drain_outbound_for_close();
        if let Err(error) = self.complete_connection_fate(class) {
            return self.fail_fate(pid, &error);
        }
        self.release_conversations();
        self.runtime.finish(pid);
        SliceStep::Stop(ExitReason::Normal)
    }

    fn finish_normal_close(&mut self, pid: u64) -> SliceStep {
        // Multiple responses may already precede the terminal frame. Make one
        // unbudgeted, nonblocking drain so the ordinary 8 KiB slice budget cannot
        // deterministically truncate it. `WouldBlock` remains best-effort: never
        // sleep, poll, or retry.
        let _ = self.drain_outbound_for_close();
        self.release_conversations();
        self.runtime.finish(pid);
        SliceStep::Stop(ExitReason::Normal)
    }

    /// R1(vi) (§1.2(3b)): services the pending-reply table for this slice.
    ///
    /// 1. DEADLINE-CHECK SEAM: expire every pending entry whose deadline passed,
    ///    tombstoning it and enqueuing its timeout error frame. Under the busy loop
    ///    this runs every slice; PARK-FLIP adds a timer-driven `READY` wake at each
    ///    entry's deadline (contract R1(vi) as amended) so a parked connection with
    ///    zero other traffic still wakes to write the timeout — that wake feeds this
    ///    same seam.
    /// 2. Drain and correlate: for each conversation still awaiting a reply, pull
    ///    buffered participant replies non-blocking and match them FIFO, enqueuing
    ///    each correlated reply frame. A conversation gone from the map (closed) is
    ///    swept from the table so its entries do not linger.
    ///
    /// # Errors
    /// Returns [`OutboundError`](super::outbound::OutboundError) when a reply or
    /// timeout frame cannot be enqueued (a fatal outbound condition).
    fn service_pending_replies(&mut self) -> Result<(), super::outbound::OutboundError> {
        let now = std::time::Instant::now();
        for frame in self.state.pending_replies.expire_due(now) {
            self.outbound.enqueue_frame(&frame)?;
        }
        for conversation_id in self.state.pending_replies.conversations_awaiting_reply() {
            let Some(conversation) = self.state.conversations.get(&conversation_id) else {
                // The conversation closed while entries were pending: sweep them
                // (the close-sweep tombstone-reclamation trigger) rather than poll a
                // conversation that no longer exists.
                self.state
                    .pending_replies
                    .remove_conversation(conversation_id);
                continue;
            };
            // Drain every buffered reply for this conversation this slice, matching
            // each FIFO. `try_receive_reply` is non-blocking, so an empty queue ends
            // the loop immediately (no slice is ever blocked).
            while let Some(reply) = conversation.try_receive_reply() {
                if let Some(frame) = self
                    .state
                    .pending_replies
                    .match_reply(conversation_id, reply)
                {
                    self.outbound.enqueue_frame(&frame)?;
                } else {
                    // The reply consumed a tombstone (or found nothing to
                    // correlate): discarded, never delivered late. Keep draining in
                    // case more replies are buffered.
                }
            }
        }
        Ok(())
    }

    /// Removes and releases subscriptions the delivery pump shed on inbox overflow
    /// (§5). The pump has already enqueued each subscription's typed `SubscribeError`
    /// frame; here the subscription is dropped from connection state (its
    /// delivery-sequence counter and any held frame with it, so a re-subscribe that
    /// reuses the id starts clean) and released through the services adapter, the
    /// same teardown path an explicit `Unsubscribe` uses. A slow consumer thus sheds
    /// its own subscription without growing server memory or tearing down the
    /// connection's other streams.
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

    /// Drains queued outbound bytes to the socket, if the stream is still present.
    ///
    /// Returns the [`DrainOutcome`](super::outbound::DrainOutcome) tri-state. Under
    /// the busy loop every caller ignores the distinction and re-services on the
    /// next slice; the value is reported so the seam is honest.
    ///
    /// PARK-FLIP SEAM (R2, §1.2(1)): when this returns
    /// `Ok(DrainOutcome::WouldBlockWithResidue)` the park-flip commit arms writable
    /// readiness interest for this connection's socket (and only then), so the
    /// connection re-wakes when the kernel send buffer drains instead of parking
    /// with unflushed residue. `Drained`/`Progress` arm nothing. The current live
    /// slice uses the fixed read-buffer budget; terminal close uses
    /// [`Self::drain_outbound_for_close`] instead.
    fn drain_outbound(
        &mut self,
    ) -> Result<super::outbound::DrainOutcome, super::outbound::OutboundError> {
        let Some(stream) = self.stream.as_mut() else {
            return Ok(super::outbound::DrainOutcome::Drained);
        };
        self.outbound.drain(stream, Some(READ_BUFFER_BYTES))
    }

    /// Makes one unbudgeted, nonblocking attempt to flush terminal output.
    ///
    /// The outbound queue is bounded, so removing the normal slice budget cannot
    /// create unbounded work. A full socket can still return
    /// [`DrainOutcome::WouldBlockWithResidue`]; terminal delivery is best-effort
    /// at that transport boundary and this method never waits or retries.
    fn drain_outbound_for_close(
        &mut self,
    ) -> Result<super::outbound::DrainOutcome, super::outbound::OutboundError> {
        let Some(stream) = self.stream.as_mut() else {
            return Ok(super::outbound::DrainOutcome::Drained);
        };
        self.outbound.drain(stream, None)
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
        let stream = self
            .stream
            .as_ref()
            .ok_or_else(|| ServerError::ListenerAccept {
                message: "cannot register readiness for a missing connection stream".to_owned(),
            })?;
        let token = facility
            .register(stream.as_raw_fd(), interest, pid, self.runtime.ready_atom())
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("failed to register connection readiness: {error}"),
            })?;
        if let Err(error) = self
            .runtime
            .set_readiness_token_once(pid, token, stream.as_raw_fd())
        {
            self.runtime.deregister_unpublished_readiness(token);
            return Err(error);
        }
        self.readiness_token = Some(token);
        Ok(())
    }

    fn sync_deadline_timers(
        &mut self,
        pid: u64,
        ctx: &mut NativeContext<'_>,
    ) -> Result<(), ServerError> {
        for timer in self.state.pending_replies.take_retired_timers() {
            ctx.cancel_timer(timer);
        }
        for (op_id, delay) in self
            .state
            .pending_replies
            .timers_to_arm(std::time::Instant::now())
        {
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
        Ok(())
    }

    /// Post-arm C1/C4 barrier. Each query is nonblocking and non-consuming,
    /// including the native mailbox so READY enqueued by this slice cannot be
    /// mistaken for quiescence after its inbox work was already consumed.
    fn final_probe(&self, pid: u64, ctx: &NativeContext<'_>) -> Result<bool, ServerError> {
        let socket_ready = if let Some(stream) = self.stream.as_ref() {
            let mut byte = [0_u8; 1];
            match stream.peek(&mut byte) {
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
                Some(inbox) => inbox.has_pending().map_err(participant_publication_error)?,
                None => false,
            }
        };
        let reply_ready = self
            .state
            .pending_replies
            .has_due(std::time::Instant::now())
            || self
                .state
                .pending_replies
                .conversations_awaiting_reply()
                .into_iter()
                .filter_map(|id| self.state.conversations.get(&id))
                .any(super::conversation::ConnectionConversation::has_pending_reply);
        Ok(socket_ready
            || subscription_ready
            || participant_ready
            || reply_ready
            || self.runtime.has_control(pid)
            || self.runtime.ready_pending(pid)
            || ctx.has_messages())
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
                if let Err(error) =
                    self.complete_connection_fate(ConnectionFateClass::ServerShutdown)
                {
                    return Some(match self.fail_fate(pid, &error) {
                        SliceStep::Continue => NativeOutcome::Continue,
                        SliceStep::Stop(reason) => NativeOutcome::Stop(reason),
                    });
                }
                self.release_conversations();
                // Host removal ACKs readiness deregistration while both the
                // process stream and record fd guard are still live.
                self.runtime.finish(pid);
                self.stream.take();
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
        // R6 (§1.2(4)): a `READY` marker (from any wake source — subscription
        // inbox R3, reply availability R1(vi), reply-deadline expiry) is a bare
        // wake with no payload. It carries no lifecycle decision: the sole action
        // is that ONE full slice runs, which the caller (`handle`) does exactly
        // once after draining the whole mailbox — so N coalesced markers, or a
        // duplicate marker, collapse to one slice and never double-apply work.
        // Recognised explicitly (rather than falling through) so the discipline is
        // legible and the park-flip inherits it unchanged. Returning `None` lets
        // the drain continue and the single post-drain slice service every source.
        if message.as_atom() == Some(self.runtime.ready_atom()) {
            return None;
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
        self.runtime.acknowledge_ready(pid);
        self.handle_slice(pid, ctx)
    }
}

impl Drop for ConnectionProcess {
    fn drop(&mut self) {
        // Backstop for termination paths that never run another handler slice:
        // external termination (killed via the scheduler) and scheduler shutdown
        // drop the handler directly. This drop does NOT reclaim the host record;
        // W4 leg 1 delivers that reclamation TOLD, the instant beamr publishes
        // the process's exit event, through the supervisor's exit-event reactor
        // into the ordinary `remove()` funnel (replacing the retired per-accept
        // `reap_crashed` scan for this class). On the explicit in-handler teardown
        // paths the conversation map is already drained, so this is a no-op there.
        self.release_conversations();
        let timers = self.state.pending_replies.take_retired_timers();
        self.runtime.cancel_deadline_timers(timers);
        // External scheduler termination can remove the process-table entry while
        // this native handler is still executing. Tests that need the descriptor
        // allocator boundary must observe the stream's actual drop, not table
        // absence. Production keeps the ordinary field-drop path byte-for-byte.
        #[cfg(test)]
        if let Some(stream) = self.stream.take() {
            let fd = stream.as_raw_fd();
            drop(stream);
            self.runtime.record_process_stream_drop(fd);
        }
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
        if let Some(rejection) = crate::server::participant::preflight_generic_bytes(
            buffer,
            state.authenticated,
            state.participant_session,
        ) {
            let response = crate::server::participant::encode_server_value(
                liminal_protocol::wire::ServerValue::ParticipantTransportRejected(rejection),
            )
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("failed to encode participant frame-limit rejection: {error:?}"),
            })?;
            outbound
                .enqueue_frame(&response)
                .map_err(|error| ServerError::ListenerAccept {
                    message: format!(
                        "failed to enqueue participant frame-limit rejection: {error}"
                    ),
                })?;
            buffer.clear();
            return Ok(ProcessStatus::Close);
        }
        let (frame, consumed) = match decode(buffer) {
            Ok(decoded) => decoded,
            Err(
                ProtocolError::IncompleteHeader { .. } | ProtocolError::TruncatedPayload { .. },
            ) => {
                return Ok(ProcessStatus::Continue);
            }
            Err(error) => {
                let conversations = state.participant_conversations.tracked_conversations();
                if runtime.connection_has_bound_participant(
                    state.connection_incarnation,
                    &conversations,
                )? {
                    tracing::warn!(connection_pid = pid, %error, "bound connection protocol refusal");
                    return Ok(ProcessStatus::CloseWithFate(
                        ConnectionFateClass::ProtocolError,
                    ));
                }
                return Err(server_error_from_protocol(&error));
            }
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
            FrameAction::CloseWithFate(class) => {
                return Ok(ProcessStatus::CloseWithFate(class));
            }
        }
    }
}

fn participant_publication_error(
    error: crate::server::participant::ParticipantPublicationError,
) -> ServerError {
    ServerError::ListenerAccept {
        message: format!("participant publication registry failed: {error}"),
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
