//! The conversation actor's shared core: state machine, command queue, inbox,
//! pending receives, participant forwarding, and crash handling.
//!
//! Split out of `actor.rs` to keep each file under the 500-line boundary. The
//! core is shared (`Arc<ActorCore>`) between the `ConversationActor`/handle and
//! the beamr actor process's NIF command loop.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Instant;

use beamr::atom::Atom;
use beamr::native::ProcessContext;
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
        }
    }

    pub(super) fn ensure_running(self: &Arc<Self>) -> Result<ParticipantPid, LiminalError> {
        let restart_guard = lock(&self.restart_lock, "actor restart")?;
        if self.is_closed()? {
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
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue_for_pid(pid, QueuedCommandKind::Boot { reply })?;
        wait_for(&response, "conversation actor boot")
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
        if self.is_closed()? {
            return self.snapshot();
        }
        let pid = self.ensure_running()?;
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue_for_pid(pid, QueuedCommandKind::QueryState { reply })?;
        wait_for(&response, "conversation state query")
    }

    fn enqueue_for_pid(
        &self,
        pid: ParticipantPid,
        kind: QueuedCommandKind,
    ) -> Result<(), LiminalError> {
        let id = self.next_command_id.fetch_add(1, Ordering::Relaxed);
        lock(&self.commands, "actor command queue")?.push_back(QueuedCommand { id, kind });
        if self
            .supervisor
            .scheduler
            .enqueue_atom_message(pid.get(), self.supervisor.runtime.command_atom())
        {
            Ok(())
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
            lock(&self.inbox, "conversation inbox")?.push_back(reply);
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
                Ok(mut pending) => pending.push_back(reply),
                Err(error) => send_reply(&reply, Err(error)),
            }
        }
    }

    fn apply_close(&self) -> Result<(), LiminalError> {
        {
            let mut state = lock(&self.state, "conversation state")?;
            if state.current_phase == ConversationPhase::Created {
                state.activate()?;
            }
            state.begin_completing()?;
            state.close()?;
        }
        let message = "conversation closed before receive completed".to_owned();
        for reply in lock(&self.pending_receives, "pending receives")?.drain(..) {
            send_reply(
                &reply,
                Err(LiminalError::ConversationFailed {
                    message: message.clone(),
                }),
            );
        }
        Ok(())
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
    ) -> Result<(), LiminalError> {
        // Capture the link-fire instant first: the real detection moment,
        // propagated to blocked dispatchers as the start of reroute latency.
        self.record_participant_exit(participant, Instant::now())
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

    /// Records `participant`'s death observed at `observed_at` and applies the
    /// configured crash policy. This is the single host-side recording path,
    /// shared by the live trapped-EXIT handler and boot's dead-pid pruning.
    ///
    /// Recording is exactly-once: a participant already marked Dead (its crash
    /// was recorded before the actor process itself died — host-side state
    /// survives actor restarts) is skipped without a duplicate context entry,
    /// notifier signal, or `exited_at` restamp.
    pub(super) fn record_participant_exit(
        &self,
        participant: ParticipantPid,
        observed_at: Instant,
    ) -> Result<(), LiminalError> {
        // Drop any participant runtime registration so a dead pid stops draining
        // a queue (and so its pid can be safely reused by the scheduler later).
        // Idempotent, so a replayed recording attempt is harmless here.
        self.supervisor.participant_runtime.deregister(participant);
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
            state.record_participant_crash(participant, self.config.on_crash, observed_at);
            self.exit_notifiers.signal(participant, observed_at)?;
            if self.config.on_crash == CrashPolicy::Fail {
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
        }
        Ok(())
    }

    fn is_closed(&self) -> Result<bool, LiminalError> {
        Ok(matches!(
            lock(&self.state, "conversation state")?.current_phase,
            ConversationPhase::Closed
        ))
    }
}
