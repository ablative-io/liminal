//! The conversation actor's shared core: state machine, command queue, inbox,
//! pending receives, participant forwarding, and crash handling.
//!
//! Split out of `actor.rs` to keep each file under the 500-line boundary. The
//! core is shared (`Arc<ActorCore>`) between the `ConversationActor`/handle and
//! the beamr actor process's NIF command loop.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Instant;

use beamr::atom::Atom;
use beamr::native::ProcessContext;
use beamr::process::ExitReason;
use beamr::term::Term;

use super::beam;
use super::exit::ExitNotifierRegistry;
use super::queue::{QueuedCommand, QueuedCommandKind};
use super::sync::{self, lock, send_reply, wait_for};
use super::{ParticipantChannel, SupervisorInner};
use crate::conversation::types::{
    ConversationConfig, ConversationPhase, ConversationState, CrashPolicy, ParticipantHealth,
    ParticipantPid,
};
use crate::envelope::Envelope;
use crate::error::LiminalError;

/// Bounds the wait for the actor's boot slice. Boot is a construction-time
/// round trip (the boot slice links participants and the exit watcher); an
/// actor that dies between the command enqueue and its first slice would
/// otherwise never reply, and the spawner would hold the lifecycle gate
/// forever. On expiry the queued command is purged and the whole spawn attempt
/// is rolled back by the caller.
const BOOT_REPLY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

fn closed_error() -> LiminalError {
    LiminalError::ConversationFailed {
        message: "conversation is closed".to_owned(),
    }
}

pub struct ActorCore {
    supervisor: Arc<SupervisorInner>,
    pub(super) config: ConversationConfig,
    state: Mutex<ConversationState>,
    inbox: Mutex<VecDeque<Envelope>>,
    pending_receives: Mutex<VecDeque<mpsc::SyncSender<Result<Envelope, LiminalError>>>>,
    commands: Mutex<VecDeque<QueuedCommand>>,
    current_pid: Mutex<Option<ParticipantPid>>,
    restart_lock: Mutex<()>,
    next_command_id: AtomicU64,
    exit_notifiers: ExitNotifierRegistry,
    /// Real participant processes this conversation forwards requests to. Empty
    /// for inert/test participants (which are only linked for crash detection),
    /// so the conversation falls back to its own inbox in that case.
    participant_channels: Vec<ParticipantChannel>,
    /// Monotonic: set by any successful close and by `finalize`, never cleared.
    /// Once set, no handle operation may spawn an actor process again — without
    /// this, a retained handle's `ensure_running` would respawn the actor after
    /// close and re-open the leak the close just repaired.
    finalized: AtomicBool,
    /// Participant pids whose termination THIS conversation's close/finalize
    /// initiated, recorded before `terminate_process` is called. Only a Normal
    /// EXIT matching an entry here is suppressed as the close's own doing; an
    /// abnormal EXIT racing the close carries a real crash and is recorded.
    close_terminations: Mutex<HashSet<u64>>,
    /// Pid of the exit watcher spawned alongside the current actor process,
    /// linked to it during boot so actor death is observed even when no handle
    /// operation ever runs again.
    watcher_pid: Mutex<Option<ParticipantPid>>,
    /// R1(vi)(a) reply-availability notifier, installed PERMANENTLY at conversation
    /// open (not per-message). Fired on the reply queue's (inbox's) empty→non-empty
    /// transition and on terminal actor error, so a parked connection wakes to
    /// drain a reply that landed on another scheduler's slice. `None` until the
    /// connection installs it; removed at close/finalize so a marker never fires
    /// after teardown. It captures the CONNECTION scheduler's enqueue handle
    /// (§1.2(3a), Vesper advisory 3) — the connection installs a closure that fires
    /// its own `READY` marker, never this (conversation) scheduler's.
    reply_notifier: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl std::fmt::Debug for ActorCore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ActorCore")
            .field("config", &self.config)
            .field("current_pid", &self.current_pid.lock().ok())
            .finish_non_exhaustive()
    }
}

impl ActorCore {
    pub(super) fn new(
        supervisor: Arc<SupervisorInner>,
        config: ConversationConfig,
        participant_channels: Vec<ParticipantChannel>,
    ) -> Self {
        let state = ConversationState::from_config(&config, Instant::now());
        Self {
            supervisor,
            config,
            state: Mutex::new(state),
            inbox: Mutex::new(VecDeque::new()),
            pending_receives: Mutex::new(VecDeque::new()),
            commands: Mutex::new(VecDeque::new()),
            current_pid: Mutex::new(None),
            restart_lock: Mutex::new(()),
            next_command_id: AtomicU64::new(1),
            exit_notifiers: ExitNotifierRegistry::default(),
            participant_channels,
            finalized: AtomicBool::new(false),
            close_terminations: Mutex::new(HashSet::new()),
            watcher_pid: Mutex::new(None),
            reply_notifier: Mutex::new(None),
        }
    }

    /// Installs the R1(vi)(a) reply-availability notifier, fired on the reply
    /// queue's empty→non-empty transition and on terminal actor error. Installed
    /// permanently at conversation open; cleared at close/finalize.
    pub(crate) fn register_reply_notifier(&self, notifier: Arc<dyn Fn() + Send + Sync>) {
        if let Ok(mut slot) = self.reply_notifier.lock() {
            *slot = Some(notifier);
        }
    }

    /// Non-blocking host-side drain of one buffered participant reply, if any.
    ///
    /// Replaces the removed in-slice BLOCKING `receive_timeout` on the
    /// request-reply path (§1.2(3b)): the connection polls this on its own slice
    /// (woken by the reply-availability notifier at park-flip) and correlates the
    /// reply through its pending-reply table, so the connection thread never blocks
    /// waiting on the conversation scheduler. Pops directly from the buffered
    /// inbox, symmetric with how [`Self::deliver_participant_reply`] pushes to it
    /// host-side; a finalized conversation drains nothing.
    pub(crate) fn try_take_reply(&self) -> Option<Envelope> {
        if self.is_finalized() {
            return None;
        }
        self.inbox.lock().ok()?.pop_front()
    }

    /// Fires the reply-availability notifier once, if installed. Called on the
    /// reply-queue empty→non-empty edge and on terminal actor error.
    fn fire_reply_notifier(&self) {
        let notifier = self
            .reply_notifier
            .lock()
            .ok()
            .and_then(|slot| slot.clone());
        if let Some(notifier) = notifier {
            notifier();
        }
    }

    /// Clears the reply notifier at close/finalize so no marker fires after
    /// teardown (§1.2(3a): markers arriving after close are discarded).
    fn clear_reply_notifier(&self) {
        if let Ok(mut slot) = self.reply_notifier.lock() {
            *slot = None;
        }
    }

    /// Whether this conversation has been finalized (closed or torn down). Once
    /// true, no operation may spawn an actor process for it again.
    pub(super) fn is_finalized(&self) -> bool {
        self.finalized.load(Ordering::Acquire)
    }

    pub(super) fn ensure_running(self: &Arc<Self>) -> Result<ParticipantPid, LiminalError> {
        let restart_guard = lock(&self.restart_lock, "actor restart")?;
        // A finalized conversation is terminal regardless of phase (a Failed
        // conversation stays Failed for diagnostics but must never respawn its
        // actor after a successful close).
        if self.is_finalized() || self.is_closed()? {
            return Err(LiminalError::ConversationFailed {
                message: "conversation is closed".to_owned(),
            });
        }
        let current = *lock(&self.current_pid, "actor pid")?;
        if let Some(pid) = current {
            if self
                .supervisor
                .scheduler
                .process_table()
                .get(pid.get())
                .is_some()
            {
                return Ok(pid);
            }
            // The current actor process is gone; drop its stale registry entry
            // before spawning a replacement so restarts never accumulate dead
            // actor keys (the owning core survives the restart, so the
            // strong-count prune in `ActorRuntime::register` cannot catch it).
            self.supervisor.runtime.deregister_owned(pid, self);
        }
        let pid = self.supervisor.spawn_actor_for(self);
        drop(restart_guard);
        pid
    }

    pub(super) fn set_current_pid(&self, pid: ParticipantPid) -> Result<(), LiminalError> {
        *lock(&self.current_pid, "actor pid")? = Some(pid);
        Ok(())
    }

    pub(super) fn boot(self: &Arc<Self>, pid: ParticipantPid) -> Result<(), LiminalError> {
        self.boot_with_timeout(pid, BOOT_REPLY_TIMEOUT)
    }

    /// Boot with an explicit reply bound. On any failure after the command was
    /// admitted, the still-queued command is purged so it cannot linger for a
    /// later actor incarnation (a purge after the command was already popped is
    /// a harmless no-op; its late reply lands in a dropped receiver).
    pub(super) fn boot_with_timeout(
        self: &Arc<Self>,
        pid: ParticipantPid,
        timeout: std::time::Duration,
    ) -> Result<(), LiminalError> {
        let (reply, response) = mpsc::sync_channel(1);
        let command_id = self.enqueue_for_pid(pid, QueuedCommandKind::Boot { reply })?;
        let result = sync::wait_for_timeout(&response, "conversation actor boot", timeout);
        if result.is_err() {
            self.remove_command(command_id)?;
        }
        result
    }

    pub(super) fn submit_send(self: &Arc<Self>, message: Envelope) -> Result<(), LiminalError> {
        let pid = self.ensure_running()?;
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue_for_pid(pid, QueuedCommandKind::Send { message, reply })?;
        wait_for(&response, "conversation send")
    }

    pub(super) fn submit_receive(self: &Arc<Self>) -> Result<Envelope, LiminalError> {
        let pid = self.ensure_running()?;
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue_for_pid(pid, QueuedCommandKind::Receive { reply })?;
        wait_for(&response, "conversation receive")
    }

    pub(super) fn submit_receive_timeout(
        self: &Arc<Self>,
        timeout: std::time::Duration,
    ) -> Result<Envelope, LiminalError> {
        let pid = self.ensure_running()?;
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue_for_pid(pid, QueuedCommandKind::Receive { reply })?;
        sync::wait_for_timeout(&response, "conversation receive", timeout)
    }

    pub(super) fn submit_close(self: &Arc<Self>) -> Result<(), LiminalError> {
        let pid = self.ensure_running()?;
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue_for_pid(pid, QueuedCommandKind::Close { reply })?;
        wait_for(&response, "conversation close")
    }

    pub(super) fn submit_query_state(self: &Arc<Self>) -> Result<ConversationState, LiminalError> {
        // A finalized conversation still answers state queries from its host-side
        // snapshot (a Failed phase remains observable as the diagnostic outcome);
        // only operations that would need a live actor are refused.
        if self.is_finalized() || self.is_closed()? {
            return self.snapshot();
        }
        let pid = self.ensure_running()?;
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue_for_pid(pid, QueuedCommandKind::QueryState { reply })?;
        wait_for(&response, "conversation state query")
    }

    /// Admits a command, returning its id (used by boot to purge on timeout).
    ///
    /// Admission and terminal publication share the command-queue lock: the
    /// finalized flag is set inside a command-lock critical section that also
    /// drains the queue (see [`Self::mark_finalized_and_drain_commands`]), so a
    /// command is either admitted before finalization — and drained with the
    /// typed error — or rejected here. There is no interleaving in which a
    /// caller blocks on a command finalization can no longer see.
    pub(super) fn enqueue_for_pid(
        &self,
        pid: ParticipantPid,
        kind: QueuedCommandKind,
    ) -> Result<u64, LiminalError> {
        let id = self.next_command_id.fetch_add(1, Ordering::Relaxed);
        {
            let mut commands = lock(&self.commands, "actor command queue")?;
            if self.is_finalized() {
                return Err(closed_error());
            }
            commands.push_back(QueuedCommand { id, kind });
        }
        if self
            .supervisor
            .scheduler
            .enqueue_atom_message(pid.get(), self.supervisor.runtime.command_atom())
        {
            Ok(id)
        } else {
            self.remove_command(id)?;
            Err(LiminalError::DeliveryFailed {
                message: format!("conversation actor pid {} is not live", pid.get()),
            })
        }
    }

    fn remove_command(&self, id: u64) -> Result<(), LiminalError> {
        lock(&self.commands, "actor command queue")?.retain(|command| command.id != id);
        Ok(())
    }

    pub(super) fn process_next_command(
        &self,
        context: &mut ProcessContext<'_>,
    ) -> Result<Term, Term> {
        let Some(command) = lock(&self.commands, "actor command queue")
            .map_err(|_| Term::atom(Atom::BADARG))?
            .pop_front()
        else {
            return Ok(Term::atom(Atom::OK));
        };
        match command.kind {
            QueuedCommandKind::Boot { reply } => {
                send_reply(&reply, beam::link_participants(self, context));
            }
            QueuedCommandKind::Send { message, reply } => {
                send_reply(&reply, self.apply_send(message));
            }
            QueuedCommandKind::Receive { reply } => {
                self.apply_receive(reply);
            }
            QueuedCommandKind::Close { reply } => {
                let result = self.apply_close();
                let should_shutdown = result.is_ok();
                send_reply(&reply, result);
                if should_shutdown {
                    context.request_shutdown();
                }
            }
            QueuedCommandKind::QueryState { reply } => send_reply(&reply, self.snapshot()),
        }
        Ok(Term::atom(Atom::OK))
    }

    fn apply_send(&self, message: Envelope) -> Result<(), LiminalError> {
        {
            let mut state = lock(&self.state, "conversation state")?;
            state.activate()?;
            state.record_sent(message.clone());
        }
        // A real participant gets the request FORWARDED to its process, which
        // processes it and delivers any reply back through
        // `deliver_participant_reply`. The reply is the participant's genuine
        // output — there is NO self-echo here, so an inert participant (linked
        // but running no handler) produces no reply at all.
        if let Some(channel) = self.participant_channels.first() {
            return channel.forward(
                message,
                &self.supervisor.scheduler,
                self.supervisor.participant_wakeup_atom,
            );
        }
        // A conversation configured with participant pids but NO real
        // participant channel (an inert/linked-only participant) has no one to
        // produce a reply: the message is recorded as sent and accepted, but the
        // conversation does NOT loop it back to itself. A later `receive`
        // therefore genuinely blocks/times out — exactly what distinguishes a
        // real processing participant from an inert stand-in.
        if !self.config.participants.is_empty() {
            return Ok(());
        }
        // No participants at all: the conversation is a bare message buffer (the
        // LIM-004 lifecycle case). A `receive` drains what `send` enqueued.
        let reply = { lock(&self.pending_receives, "pending receives")?.pop_front() };
        if let Some(reply) = reply {
            lock(&self.state, "conversation state")?.record_received(message.clone());
            send_reply(&reply, Ok(message));
        } else {
            lock(&self.inbox, "conversation inbox")?.push_back(message);
        }
        Ok(())
    }

    /// Delivers a reply produced by a real participant process back into the
    /// conversation: it satisfies a pending `receive` immediately, or is buffered
    /// in the inbox for the next `receive`. This is the reply leg of the
    /// request-reply path — the participant's processing result flowing back to
    /// the caller through the conversation.
    ///
    /// # Errors
    /// Returns [`LiminalError`] when a conversation lock is poisoned.
    pub fn deliver_participant_reply(&self, reply: Envelope) -> Result<(), LiminalError> {
        let waiter = { lock(&self.pending_receives, "pending receives")?.pop_front() };
        let mut state = lock(&self.state, "conversation state")?;
        state.record_received(reply.clone());
        drop(state);
        if let Some(waiter) = waiter {
            send_reply(&waiter, Ok(reply));
        } else {
            // R1(vi)(a): fire the reply-availability notifier on the reply queue's
            // empty→non-empty edge (only when this reply is the first buffered one
            // — coalescing is R6-harmless), so a parked connection wakes to drain
            // it. A reply that satisfied a waiter directly needs no wake.
            let fire = {
                let mut inbox = lock(&self.inbox, "conversation inbox")?;
                let was_empty = inbox.is_empty();
                inbox.push_back(reply);
                was_empty
            };
            if fire {
                self.fire_reply_notifier();
            }
        }
        Ok(())
    }

    fn apply_receive(&self, reply: mpsc::SyncSender<Result<Envelope, LiminalError>>) {
        let envelope = match lock(&self.inbox, "conversation inbox") {
            Ok(mut inbox) => inbox.pop_front(),
            Err(error) => {
                send_reply(&reply, Err(error));
                return;
            }
        };
        {
            let mut state = match lock(&self.state, "conversation state") {
                Ok(state) => state,
                Err(error) => {
                    send_reply(&reply, Err(error));
                    return;
                }
            };
            // A receive issued against an already-failed conversation (a
            // participant crashed) reports the crash honestly rather than the
            // confusing "invalid transition" error from `activate`. Any buffered
            // pre-crash reply is still deliverable, so only fail when empty.
            if state.current_phase == ConversationPhase::Failed && envelope.is_none() {
                send_reply(
                    &reply,
                    Err(LiminalError::ParticipantCrashed {
                        message: "conversation participant crashed".to_owned(),
                    }),
                );
                return;
            }
            if let Err(error) = state.activate() {
                send_reply(&reply, Err(error));
                return;
            }
            if let Some(envelope) = &envelope {
                state.record_received(envelope.clone());
            }
        }
        if let Some(envelope) = envelope {
            send_reply(&reply, Ok(envelope));
        } else {
            match lock(&self.pending_receives, "pending receives") {
                // Recheck finalization under the pending-queue lock before
                // publishing the waiter: this receive was popped from the command
                // queue before finalization drained it, so a plain push here
                // could land AFTER finalization's own pending drain and block its
                // caller forever. The mutex orders the check against the drain —
                // whichever runs second sees the other's work.
                Ok(mut pending) => {
                    if self.is_finalized() {
                        send_reply(&reply, Err(closed_error()));
                    } else {
                        pending.push_back(reply);
                    }
                }
                Err(error) => send_reply(&reply, Err(error)),
            }
        }
    }

    fn apply_close(&self) -> Result<(), LiminalError> {
        {
            let mut state = lock(&self.state, "conversation state")?;
            // A conversation that already failed is terminal and does not transition
            // to Closed (Failed stays Failed: the phase is the diagnostic record,
            // finalization is the `finalized` flag); one already Closed was
            // finalized by a concurrent teardown while this Close was in flight.
            // Both still fall through to release participants and registrations —
            // every step below is idempotent.
            if !matches!(
                state.current_phase,
                ConversationPhase::Failed | ConversationPhase::Closed
            ) {
                if state.current_phase == ConversationPhase::Created {
                    state.activate()?;
                }
                state.begin_completing()?;
                state.close()?;
            }
        }
        // The interactive close publishes the terminal marker and drains queued
        // commands exactly as `finalize` does, in the same one atomic step.
        self.mark_finalized_and_drain_commands();
        self.drain_pending_receives();
        // Close terminates every real participant process and drops both the
        // participant and this actor's runtime registration: a closed conversation
        // must leave nothing parked or registered behind it (the actor process
        // itself is stopped by the caller via `request_shutdown`).
        self.release_participants();
        let current_pid = *lock(&self.current_pid, "actor pid")?;
        if let Some(pid) = current_pid {
            self.supervisor.runtime.deregister_owned(pid, self);
        }
        // The watcher's job (observing this actor's exit) is done; terminate it
        // directly rather than relying on its self-stop slice, so close leaves no
        // process behind even when the scheduler never runs the watcher again.
        // Idempotent when the watcher already self-stopped.
        if let Some(watcher) = self.watcher_pid() {
            self.supervisor
                .scheduler
                .terminate_process(watcher.get(), ExitReason::Normal);
        }
        Ok(())
    }

    /// Finalizes the conversation without requiring its actor process to run:
    /// bounded, non-blocking on the conversation scheduler, and idempotent.
    /// Marks the core finalized, moves a non-Failed phase to Closed (Failed is
    /// kept as the diagnostic outcome), fails pending receives and queued
    /// commands with the typed closed error, terminates every participant, the
    /// actor process, and its exit watcher directly (scheduler tombstone
    /// writes, not requests into the actor's command loop), and drops both
    /// runtime registrations. This is the teardown-path entry point — a
    /// connection releasing its conversations must never wait on the
    /// conversation scheduler being live or responsive.
    pub(crate) fn finalize(&self) {
        // Same lifecycle gate as `ensure_running`: finalization serializes with
        // any in-progress spawn, so it can never observe a half-published actor
        // (and a spawn that completes after finalization is rolled back by the
        // spawner's own post-boot recheck). The hold is bounded — the gate is
        // held across spawn's bounded arm/boot waits, never an unbounded one. A
        // poisoned gate (a panicking spawner, denied workspace-wide) degrades to
        // ungated finalization rather than leaking the conversation.
        let _restart_guard = self.restart_lock.lock().ok();
        self.finalize_with(true);
    }

    /// Finalization body, parameterized on who stops the actor process.
    /// `terminate_actor` is false only when the caller IS the actor's own
    /// executing slice (the watcher-death path): the cleanup runs here and the
    /// slice then stops itself through beamr's ordinary shutdown request,
    /// instead of writing an external tombstone under a process that is
    /// currently checked out and executing. Every step tolerates the other
    /// disposition having already run.
    fn finalize_with(&self, terminate_actor: bool) {
        if self.mark_finalized_and_drain_commands() {
            return;
        }
        if let Ok(mut state) = self.state.lock() {
            if state.current_phase != ConversationPhase::Failed {
                state.current_phase = ConversationPhase::Closed;
            }
        }
        self.drain_pending_receives();
        self.release_participants();
        let current_pid = self.current_pid.lock().ok().and_then(|pid| *pid);
        if let Some(pid) = current_pid {
            if terminate_actor {
                self.supervisor
                    .scheduler
                    .terminate_process(pid.get(), ExitReason::Normal);
            }
            self.supervisor.runtime.deregister_owned(pid, self);
        }
        // Directly terminate the watcher too: on a stopped scheduler its
        // self-stop slice never runs, and a retained (parked) watcher body per
        // conversation is exactly the class of residue this repair removes.
        if let Some(watcher) = self.watcher_pid() {
            self.supervisor
                .scheduler
                .terminate_process(watcher.get(), ExitReason::Normal);
        }
    }

    /// Handles an EXIT whose source is a watcher pid, returning whether the
    /// actor's slice should stop itself (the conversation was failed and
    /// finalized here).
    ///
    /// An unexpected watcher death is a supervision-integrity failure, and the
    /// response is to FAIL and finalize the conversation — never to respawn a
    /// replacement watcher (certifying ruling). Nothing legitimate terminates
    /// the watcher externally: it is internal supervision infrastructure, so
    /// its death means the supervision structure was violated (a bug, an
    /// administrative kill, or tampering), and the campaign's bias is
    /// refuse-loudly over limp-cleverly. A mid-life replacement would also
    /// re-open the whole construction surface the spawn transaction exists to
    /// close — arm/link/probe ordering, rollback, and the quiet-conversation
    /// gap where the actor may never run another slice to install the
    /// replacement's link — buying that risk back for a conversation whose
    /// integrity is already broken.
    ///
    /// Two suppressions keep this precise: a pid that is not the CURRENT
    /// recorded watcher (a retired incarnation's late EXIT — the record is
    /// replaced under the lifecycle gate on every spawn, and pids are
    /// monotonic) is ignored; and once the core is finalized, the watcher's
    /// death is the close/finalize path's own doing — the finalized marker is
    /// the suppression token.
    pub(super) fn handle_watcher_exit(
        &self,
        watcher: ParticipantPid,
        reason: Option<ExitReason>,
    ) -> bool {
        if self.watcher_pid() != Some(watcher) {
            return false;
        }
        if self.is_finalized() {
            return false;
        }
        if let Ok(mut state) = self.state.lock() {
            state.context.push(
                crate::conversation::types::ConversationContextEntry::SupervisionFailed {
                    watcher,
                    reason,
                },
            );
            state.fail();
        }
        tracing::warn!(
            watcher = watcher.get(),
            ?reason,
            "conversation exit watcher died while the conversation was live; \
             supervision integrity is broken — failing and finalizing the conversation"
        );
        // The caller is the actor's own slice: clean up everything else here and
        // let the slice stop itself (finalize's actor-terminate stays idempotent
        // against that self-stop for every other caller).
        self.finalize_with(false);
        true
    }

    /// Publishes the terminal marker and drains the command queue in ONE
    /// command-lock critical section, returning whether the core was already
    /// finalized. Pairing the flag-set with the drain under the same lock that
    /// admission holds makes the terminal transition atomic with command
    /// admission: everything admitted before it is drained with the typed
    /// error, everything after is rejected at [`Self::enqueue_for_pid`]. Lock
    /// poisoning is tolerated (marker still set): finalization must complete.
    fn mark_finalized_and_drain_commands(&self) -> bool {
        let Ok(mut commands) = self.commands.lock() else {
            return self.finalized.swap(true, Ordering::AcqRel);
        };
        let already = self.finalized.swap(true, Ordering::AcqRel);
        if !already {
            for command in commands.drain(..) {
                fail_command(command.kind);
            }
            // R1(vi)(a): the notifier is removed at close/finalize so a reply
            // landing after teardown never fires a marker into a dead connection.
            self.clear_reply_notifier();
        }
        already
    }

    /// Fails every pending receive with the typed closed error. Lock poisoning is
    /// tolerated (skip, not error): finalization must always run to completion.
    fn drain_pending_receives(&self) {
        let message = "conversation closed before receive completed".to_owned();
        if let Ok(mut pending) = self.pending_receives.lock() {
            for reply in pending.drain(..) {
                send_reply(
                    &reply,
                    Err(LiminalError::ConversationFailed {
                        message: message.clone(),
                    }),
                );
            }
        }
    }

    /// Terminates every real participant process this conversation forwards to and
    /// drops its runtime registration. Idempotent per participant: terminating an
    /// already-dead pid and deregistering an absent one are both no-ops, so a
    /// Failed conversation (participant already gone) closes cleanly too. Each pid
    /// is recorded as a close-initiated termination BEFORE `terminate_process`, so
    /// the trapped-EXIT recording path can suppress exactly this Normal exit —
    /// and only this one; an abnormal exit racing the close is a real crash.
    fn release_participants(&self) {
        for channel in &self.participant_channels {
            let participant = channel.pid();
            if let Ok(mut terminations) = self.close_terminations.lock() {
                terminations.insert(participant.get());
            }
            self.supervisor
                .scheduler
                .terminate_process(participant.get(), ExitReason::Normal);
            self.supervisor.participant_runtime.deregister(participant);
        }
    }

    /// Records the watcher spawned for the current actor process, so boot can
    /// link the actor to it.
    pub(super) fn set_watcher_pid(&self, pid: ParticipantPid) -> Result<(), LiminalError> {
        *lock(&self.watcher_pid, "actor watcher pid")? = Some(pid);
        Ok(())
    }

    /// Pid of the watcher for the current actor process, if one was spawned.
    pub(super) fn watcher_pid(&self) -> Option<ParticipantPid> {
        self.watcher_pid.lock().ok().and_then(|pid| *pid)
    }

    /// Exit-driven registry cleanup, run by the actor's exit watcher when the
    /// actor process for `actor` dies. Drops the dead actor's registration and
    /// terminates + deregisters this conversation's participants. Deliberately
    /// does NOT mark the core finalized: an abnormal actor exit on a live core is
    /// the restartable-crash case, and `ensure_running` re-registers on restart.
    /// (Participants of a crashed actor are already dead — the abnormal EXIT
    /// cascades through the boot-time links to the non-trapping participant
    /// processes — so terminating them here is the idempotent backstop, not a
    /// behaviour change; restarts have always rediscovered dead participants via
    /// boot pruning.)
    pub(super) fn finalize_after_actor_exit(&self, actor: ParticipantPid) {
        self.supervisor.runtime.deregister_owned(actor, self);
        for channel in &self.participant_channels {
            let participant = channel.pid();
            self.supervisor
                .scheduler
                .terminate_process(participant.get(), ExitReason::Normal);
            self.supervisor.participant_runtime.deregister(participant);
        }
    }

    fn snapshot(&self) -> Result<ConversationState, LiminalError> {
        Ok(lock(&self.state, "conversation state")?.clone())
    }

    /// Registers `notifier` for `participant`'s EXIT, replaying an already-
    /// recorded crash immediately to close the crash-before-register race.
    /// Serialized against crash recording so exactly one notification is
    /// delivered, never zero.
    ///
    /// # Errors
    /// Returns [`LiminalError`] when a state or registry lock is poisoned.
    pub(super) fn register_exit_notifier(
        &self,
        participant: ParticipantPid,
        notifier: mpsc::SyncSender<Instant>,
    ) -> Result<(), LiminalError> {
        // Hold the state lock across the dead-check and register-or-fire (state →
        // registry ordering, matching `handle_participant_exit`) so a crash
        // cannot signal an empty registry between the check and the push.
        let state = lock(&self.state, "conversation state")?;
        self.exit_notifiers
            .register(participant, notifier, &state.participants)
    }

    pub(super) fn handle_participant_exit(
        &self,
        participant: ParticipantPid,
        reason: Option<ExitReason>,
    ) -> Result<(), LiminalError> {
        // Capture the link-fire instant first: the real detection moment,
        // propagated to blocked dispatchers as the start of reroute latency.
        self.record_participant_exit(participant, Instant::now(), reason)
    }

    /// Returns whether `participant`'s process is still present in the
    /// scheduler's process table. Used by boot to prune (and record) a
    /// participant that died while no actor was linked to it, instead of
    /// failing the restart on an impossible link.
    pub(super) fn participant_process_is_live(&self, participant: ParticipantPid) -> bool {
        self.supervisor
            .scheduler
            .process_table()
            .get(participant.get())
            .is_some()
    }

    /// Records `participant`'s death observed at `observed_at` (with the EXIT
    /// reason, when the signal carried one) and applies the configured crash
    /// policy. This is the single host-side recording path, shared by the live
    /// trapped-EXIT handler and boot's dead-pid pruning.
    ///
    /// Recording is exactly-once: a participant already marked Dead (its crash
    /// was recorded before the actor process itself died — host-side state
    /// survives actor restarts) is skipped without a duplicate context entry,
    /// notifier signal, or `exited_at` restamp.
    pub(super) fn record_participant_exit(
        &self,
        participant: ParticipantPid,
        observed_at: Instant,
        reason: Option<ExitReason>,
    ) -> Result<(), LiminalError> {
        // Drop any participant runtime registration so a dead pid stops draining
        // a queue (and so its pid can be safely reused by the scheduler later).
        // Idempotent, so a replayed recording attempt is harmless here.
        self.supervisor.participant_runtime.deregister(participant);
        // A close-initiated termination is suppressed by its token, and ONLY
        // with the Normal reason that termination used: an abnormal EXIT racing
        // the close means the participant genuinely crashed before the close's
        // own terminate landed, and must be recorded with its real reason. The
        // token is consumed either way — a process exits exactly once.
        let close_initiated = self
            .close_terminations
            .lock()
            .is_ok_and(|mut terminations| terminations.remove(&participant.get()));
        if close_initiated && reason == Some(ExitReason::Normal) {
            return Ok(());
        }
        // Record the crash and signal notifiers under the SAME state lock that
        // `register_exit_notifier` holds, so a registrant can never observe the
        // participant alive yet have the signal fire into an empty registry.
        // Either it registers first (then this signal wakes it) or this records
        // Dead first (then it replays the recorded instant): exactly one wakeup.
        let failed = {
            let mut state = lock(&self.state, "conversation state")?;
            let already_recorded = state.participants.iter().any(|status| {
                status.participant == participant && status.health == ParticipantHealth::Dead
            });
            if already_recorded {
                return Ok(());
            }
            state.record_participant_crash(participant, self.config.on_crash, observed_at, reason);
            self.exit_notifiers.signal(participant, observed_at)?;
            // Precedence (certifying review ruling): once the conversation is
            // finalized/Closed its terminal phase stands — an abnormal exit
            // racing the close is recorded above (status, reason, context entry,
            // notifier) but does not flip the phase or fail pending receives.
            let terminal =
                self.is_finalized() || matches!(state.current_phase, ConversationPhase::Closed);
            if terminal {
                tracing::warn!(
                    participant = participant.get(),
                    ?reason,
                    "participant exited abnormally while the conversation was closing; \
                     recorded without reopening the closed conversation"
                );
                false
            } else if self.config.on_crash == CrashPolicy::Fail {
                state.fail();
                true
            } else {
                false
            }
        };
        if failed {
            let message = format!("conversation participant {} crashed", participant.get());
            for reply in lock(&self.pending_receives, "pending receives")?.drain(..) {
                send_reply(
                    &reply,
                    Err(LiminalError::ParticipantCrashed {
                        message: message.clone(),
                    }),
                );
            }
            // R1(vi)(a): a terminal actor error is a reply-availability event in its
            // own right — the connection must wake to fail/time-out the pending
            // reply entries this conversation can no longer satisfy.
            self.fire_reply_notifier();
        }
        Ok(())
    }

    fn is_closed(&self) -> Result<bool, LiminalError> {
        Ok(matches!(
            lock(&self.state, "conversation state")?.current_phase,
            ConversationPhase::Closed
        ))
    }

    /// Number of commands currently queued (lifecycle-gate observability).
    #[cfg(test)]
    pub(super) fn queued_command_count(&self) -> usize {
        self.commands.lock().map_or(0, |commands| commands.len())
    }

    /// Number of receive waiters currently parked (lifecycle-gate observability).
    #[cfg(test)]
    pub(super) fn pending_receive_count(&self) -> usize {
        self.pending_receives
            .lock()
            .map_or(0, |pending| pending.len())
    }
}

/// Fails one drained command with the typed closed error, waking its caller.
fn fail_command(kind: QueuedCommandKind) {
    match kind {
        QueuedCommandKind::Boot { reply } | QueuedCommandKind::Send { reply, .. } => {
            send_reply(&reply, Err(closed_error()));
        }
        QueuedCommandKind::Receive { reply } => send_reply(&reply, Err(closed_error())),
        QueuedCommandKind::Close { reply } => send_reply(&reply, Err(closed_error())),
        QueuedCommandKind::QueryState { reply } => send_reply(&reply, Err(closed_error())),
    }
}
