use std::any::Any;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Instant;

use beamr::atom::{Atom, AtomTable};
use beamr::module::ModuleRegistry;
use beamr::native::ProcessContext;
use beamr::scheduler::{Scheduler, SchedulerConfig};
use beamr::term::Term;

mod backend;
mod beam;
mod exit;
mod queue;
mod sync;

use crate::conversation::types::{
    ConversationConfig, ConversationHandle, ConversationPhase, ConversationState, CrashPolicy,
    ParticipantPid,
};
use crate::envelope::Envelope;
use crate::error::LiminalError;
use backend::ActorBackend;
use beam::{ActorRuntime, actor_module};
use exit::ExitNotifierRegistry;
use queue::{QueuedCommand, QueuedCommandKind};
use sync::{lock, send_reply, wait_for};

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConversationCommand {
    Send(Envelope),
    Receive,
    Close,
    QueryState,
}

#[derive(Clone, Debug)]
pub struct ConversationSupervisor {
    inner: Arc<SupervisorInner>,
}

impl ConversationSupervisor {
    /// # Errors
    /// Returns [`LiminalError`] when the beamr scheduler cannot start.
    pub fn new() -> Result<Self, LiminalError> {
        SupervisorInner::new().map(|inner| Self {
            inner: Arc::new(inner),
        })
    }

    /// Spawns one supervised conversation actor.
    ///
    /// # Errors
    /// Returns [`LiminalError`] when spawn, boot, or participant linking fails.
    pub fn spawn(&self, config: ConversationConfig) -> Result<ConversationActor, LiminalError> {
        let core = Arc::new(ActorCore::new(Arc::clone(&self.inner), config));
        self.inner.spawn_actor_for(&core)?;
        let handle = ConversationHandle::new(Arc::new(ActorBackend {
            core: Arc::clone(&core),
        }));
        Ok(ConversationActor { core, handle })
    }

    /// Returns the scheduler used by this supervisor.
    #[must_use]
    pub fn scheduler(&self) -> Arc<Scheduler> {
        Arc::clone(&self.inner.scheduler)
    }

    /// Stops the underlying scheduler.
    pub fn shutdown(&self) {
        self.inner.scheduler.shutdown();
    }
}

#[derive(Clone, Debug)]
pub struct ConversationActor {
    core: Arc<ActorCore>,
    handle: ConversationHandle,
}

impl ConversationActor {
    /// Returns a cloneable command handle.
    #[must_use]
    pub fn handle(&self) -> ConversationHandle {
        self.handle.clone()
    }

    /// Returns the current actor PID, restarting after crash when needed.
    ///
    /// # Errors
    /// Returns [`LiminalError`] when the actor is closed or cannot restart.
    pub fn pid(&self) -> Result<ParticipantPid, LiminalError> {
        self.core.ensure_running()
    }

    /// Queries actor state.
    ///
    /// # Errors
    /// Returns [`LiminalError`] when the actor cannot service the query.
    pub fn state(&self) -> Result<ConversationState, LiminalError> {
        self.handle.query_state()
    }

    /// Registers a one-shot notifier fired the instant `participant`'s trapped
    /// EXIT is processed (carrying the observed [`Instant`] — a structural link
    /// wakeup, not a poll). If `participant` is already dead at registration
    /// (it crashed before this call), the recorded EXIT instant is replayed
    /// immediately, so a crash-before-register is never lost. See
    /// [`ActorCore::register_exit_notifier`].
    ///
    /// # Errors
    /// Returns [`LiminalError`] when a state or registry lock is poisoned.
    pub fn notify_on_participant_exit(
        &self,
        participant: ParticipantPid,
        notifier: mpsc::SyncSender<Instant>,
    ) -> Result<(), LiminalError> {
        self.core.register_exit_notifier(participant, notifier)
    }
}

struct SupervisorInner {
    scheduler: Arc<Scheduler>,
    runtime: Arc<ActorRuntime>,
    module_name: Atom,
    entry_function: Atom,
}

impl std::fmt::Debug for SupervisorInner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SupervisorInner")
            .field("runtime", &self.runtime)
            .field("module_name", &self.module_name)
            .field("entry_function", &self.entry_function)
            .finish_non_exhaustive()
    }
}

impl SupervisorInner {
    fn new() -> Result<Self, LiminalError> {
        let atoms = AtomTable::with_common_atoms();
        let module_name = atoms.intern("liminal_conversation_actor");
        let entry_function = atoms.intern("main");
        let command_function = atoms.intern("process_command");
        let command_atom = atoms.intern("liminal_conversation_command");
        let runtime = Arc::new(ActorRuntime::new(command_atom));
        let registry = Arc::new(ModuleRegistry::new());
        registry.insert(actor_module(module_name, entry_function, command_function));
        let private_data: Arc<dyn Any + Send + Sync> = runtime.clone();
        let scheduler = Scheduler::new(
            SchedulerConfig {
                thread_count: Some(1),
                nif_private_data: Some(private_data),
                ..SchedulerConfig::default()
            },
            registry,
        )
        .map_err(|message| LiminalError::ConversationFailed { message })?;
        Ok(Self {
            scheduler: Arc::new(scheduler),
            runtime,
            module_name,
            entry_function,
        })
    }

    fn spawn_actor_for(&self, core: &Arc<ActorCore>) -> Result<ParticipantPid, LiminalError> {
        let pid = self
            .scheduler
            .spawn_trap_exit(self.module_name, self.entry_function, Vec::new())
            .map_err(|error| LiminalError::ConversationFailed {
                message: format!("failed to spawn conversation actor: {error}"),
            })?;
        let participant = ParticipantPid::new(pid);
        self.runtime.register(participant, Arc::downgrade(core))?;
        core.set_current_pid(participant)?;
        core.boot(participant)?;
        Ok(participant)
    }
}

struct ActorCore {
    supervisor: Arc<SupervisorInner>,
    config: ConversationConfig,
    state: Mutex<ConversationState>,
    inbox: Mutex<VecDeque<Envelope>>,
    pending_receives: Mutex<VecDeque<mpsc::SyncSender<Result<Envelope, LiminalError>>>>,
    commands: Mutex<VecDeque<QueuedCommand>>,
    current_pid: Mutex<Option<ParticipantPid>>,
    restart_lock: Mutex<()>,
    next_command_id: AtomicU64,
    exit_notifiers: ExitNotifierRegistry,
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
    fn new(supervisor: Arc<SupervisorInner>, config: ConversationConfig) -> Self {
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
        }
    }

    fn ensure_running(self: &Arc<Self>) -> Result<ParticipantPid, LiminalError> {
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

    fn set_current_pid(&self, pid: ParticipantPid) -> Result<(), LiminalError> {
        *lock(&self.current_pid, "actor pid")? = Some(pid);
        Ok(())
    }

    fn boot(self: &Arc<Self>, pid: ParticipantPid) -> Result<(), LiminalError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue_for_pid(pid, QueuedCommandKind::Boot { reply })?;
        wait_for(&response, "conversation actor boot")
    }

    fn submit_send(self: &Arc<Self>, message: Envelope) -> Result<(), LiminalError> {
        let pid = self.ensure_running()?;
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue_for_pid(pid, QueuedCommandKind::Send { message, reply })?;
        wait_for(&response, "conversation send")
    }

    fn submit_receive(self: &Arc<Self>) -> Result<Envelope, LiminalError> {
        let pid = self.ensure_running()?;
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue_for_pid(pid, QueuedCommandKind::Receive { reply })?;
        wait_for(&response, "conversation receive")
    }

    fn submit_close(self: &Arc<Self>) -> Result<(), LiminalError> {
        let pid = self.ensure_running()?;
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue_for_pid(pid, QueuedCommandKind::Close { reply })?;
        wait_for(&response, "conversation close")
    }

    fn submit_query_state(self: &Arc<Self>) -> Result<ConversationState, LiminalError> {
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

    fn process_next_command(&self, context: &mut ProcessContext<'_>) -> Result<Term, Term> {
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
        let reply = { lock(&self.pending_receives, "pending receives")?.pop_front() };
        if let Some(reply) = reply {
            lock(&self.state, "conversation state")?.record_received(message.clone());
            send_reply(&reply, Ok(message));
        } else {
            lock(&self.inbox, "conversation inbox")?.push_back(message);
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
    fn register_exit_notifier(
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

    fn handle_participant_exit(&self, participant: ParticipantPid) -> Result<(), LiminalError> {
        // Capture the link-fire instant first: the real detection moment,
        // propagated to blocked dispatchers as the start of reroute latency.
        let observed_at = Instant::now();
        // Record the crash and signal notifiers under the SAME state lock that
        // `register_exit_notifier` holds, so a registrant can never observe the
        // participant alive yet have the signal fire into an empty registry.
        // Either it registers first (then this signal wakes it) or this records
        // Dead first (then it replays the recorded instant): exactly one wakeup.
        let failed = {
            let mut state = lock(&self.state, "conversation state")?;
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
