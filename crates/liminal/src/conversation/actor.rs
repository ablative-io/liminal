use std::any::Any;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Instant;

use beamr::atom::{Atom, AtomTable};
use beamr::module::ModuleRegistry;
use beamr::scheduler::{Scheduler, SchedulerConfig};

mod backend;
mod beam;
mod core;
mod exit;
mod queue;
mod sync;
mod watcher;

use crate::conversation::participant::{
    ParticipantBehaviour, ParticipantChannel, ParticipantProcess, ParticipantRuntime,
};
use crate::conversation::types::{
    ConversationConfig, ConversationHandle, ConversationState, CrashPolicy, ParticipantPid,
};
use crate::envelope::Envelope;
use crate::error::LiminalError;
use backend::ActorBackend;
use beam::{ActorRuntime, actor_module};
pub(crate) use core::ActorCore;

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

    /// Spawns one supervised conversation actor over the given participant pids.
    ///
    /// The participants are linked for crash detection but are NOT forwarded
    /// requests (they are inert from the conversation's perspective). Use
    /// [`ConversationSupervisor::spawn_with_participant`] to attach a real
    /// participant that processes forwarded messages.
    ///
    /// # Errors
    /// Returns [`LiminalError`] when spawn, boot, or participant linking fails.
    pub fn spawn(&self, config: ConversationConfig) -> Result<ConversationActor, LiminalError> {
        let core = Arc::new(ActorCore::new(Arc::clone(&self.inner), config, Vec::new()));
        self.inner.spawn_actor_for(&core)?;
        let handle = ConversationHandle::new(Arc::new(ActorBackend {
            core: Arc::clone(&core),
        }));
        Ok(ConversationActor { core, handle })
    }

    /// Spawns a real participant native process running `behaviour`, then a
    /// supervised conversation actor linked to it. Requests sent through the
    /// returned actor's handle are FORWARDED to the participant process, which
    /// genuinely processes them and delivers any reply back into the
    /// conversation — the request-reply path from LIM-005.
    ///
    /// Returns the actor and the spawned participant's pid (for crash injection
    /// and linkage assertions).
    ///
    /// # Errors
    /// Returns [`LiminalError`] when participant spawn, actor spawn, boot, or
    /// linking fails.
    pub fn spawn_with_participant(
        &self,
        behaviour: Arc<dyn ParticipantBehaviour>,
        timeout: Option<std::time::Duration>,
        mode: crate::channel::ChannelMode,
        on_crash: CrashPolicy,
    ) -> Result<(ConversationActor, ParticipantPid), LiminalError> {
        let channel = self.inner.spawn_participant()?;
        let participant = channel.pid();
        let config = ConversationConfig::new(vec![participant], timeout, mode, on_crash);
        let core = Arc::new(ActorCore::new(
            Arc::clone(&self.inner),
            config,
            vec![channel.clone()],
        ));
        // Register the participant with its inbox, behaviour, and a weak handle to
        // the core so produced replies route back into this conversation.
        self.inner.participant_runtime.register(
            participant,
            channel.inbox_arc(),
            behaviour,
            Arc::downgrade(&core),
        )?;
        // The participant belongs to this construction attempt: a failed actor
        // spawn rolls it back too (terminate + deregister), so no path out of a
        // failed open leaves a parked participant behind.
        if let Err(error) = self.inner.spawn_actor_for(&core) {
            self.inner
                .scheduler
                .terminate_process(participant.get(), beamr::process::ExitReason::Normal);
            self.inner.participant_runtime.deregister(participant);
            return Err(error);
        }
        let handle = ConversationHandle::new(Arc::new(ActorBackend {
            core: Arc::clone(&core),
        }));
        Ok((ConversationActor { core, handle }, participant))
    }

    /// Returns the scheduler used by this supervisor.
    #[must_use]
    pub fn scheduler(&self) -> Arc<Scheduler> {
        Arc::clone(&self.inner.scheduler)
    }

    /// Number of actor registrations currently held by this supervisor's
    /// runtime. Lifecycle observability for the leak/churn gates: every closed
    /// or torn-down conversation must have removed its entry, so this count is
    /// pinned bounded across open/close cycles.
    #[must_use]
    pub fn registered_actor_count(&self) -> usize {
        self.inner.runtime.registration_count()
    }

    /// Number of participant registrations currently held by this supervisor's
    /// runtime. Same lifecycle-gate role as
    /// [`Self::registered_actor_count`].
    #[must_use]
    pub fn registered_participant_count(&self) -> usize {
        self.inner.participant_runtime.registration_count()
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

    /// Receives the next reply from the conversation, bounded by `timeout`.
    ///
    /// Returns [`LiminalError::ConversationTimeout`] if no reply arrives in time,
    /// or [`LiminalError::ParticipantCrashed`] if a linked participant crashes
    /// while waiting (the crash drains the pending receive immediately).
    ///
    /// # Errors
    /// Returns [`LiminalError`] on timeout, participant crash, or actor failure.
    pub fn receive_timeout(&self, timeout: std::time::Duration) -> Result<Envelope, LiminalError> {
        self.core.submit_receive_timeout(timeout)
    }

    /// Finalizes the conversation without requiring its actor process to run:
    /// bounded, non-blocking, and idempotent. Terminates the actor and every
    /// participant directly (scheduler tombstone writes, not requests into the
    /// actor's command loop), fails pending receives and queued commands with
    /// the typed closed error, and removes both runtime registrations. This is
    /// the teardown-path counterpart to [`ConversationHandle::close`]: a caller
    /// releasing a conversation during ITS OWN teardown must never block on the
    /// conversation scheduler being live or responsive.
    ///
    /// [`ConversationHandle::close`]: crate::conversation::ConversationHandle::close
    pub fn finalize(&self) {
        self.core.finalize();
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

/// Test-only rendezvous installed at the arm→boot seam of one spawn attempt:
/// the spawner sends the fresh actor pid and blocks until the test signals it
/// to proceed, letting construction-ordering tests inject events (e.g. an
/// actor kill) at an exact point instead of sleeping.
#[cfg(test)]
type BootBarrier = (mpsc::Sender<ParticipantPid>, mpsc::Receiver<()>);

struct SupervisorInner {
    scheduler: Arc<Scheduler>,
    runtime: Arc<ActorRuntime>,
    participant_runtime: Arc<ParticipantRuntime>,
    participant_wakeup_atom: Atom,
    module_name: Atom,
    entry_function: Atom,
    #[cfg(test)]
    boot_barrier: Mutex<Option<BootBarrier>>,
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
        let participant_wakeup_atom = atoms.intern("liminal_conversation_participant_wakeup");
        let runtime = Arc::new(ActorRuntime::new(command_atom));
        let participant_runtime = Arc::new(ParticipantRuntime::default());
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
            participant_runtime,
            participant_wakeup_atom,
            module_name,
            entry_function,
            #[cfg(test)]
            boot_barrier: Mutex::new(None),
        })
    }

    /// Installs the one-shot arm→boot rendezvous consumed by the next spawn
    /// attempt; see [`BootBarrier`].
    #[cfg(test)]
    fn install_boot_barrier(&self, barrier: BootBarrier) {
        if let Ok(mut slot) = self.boot_barrier.lock() {
            *slot = Some(barrier);
        }
    }

    /// Spawns a real participant native process running `behaviour`, registers it
    /// with the participant runtime, and returns the channel the conversation
    /// actor forwards requests through plus the participant pid. The process is a
    /// first-class beamr [`NativeHandler`]; the conversation actor links to it
    /// during boot for structural crash detection.
    fn spawn_participant(&self) -> Result<ParticipantChannel, LiminalError> {
        let runtime = Arc::clone(&self.participant_runtime);
        let wakeup_atom = self.participant_wakeup_atom;
        let factory = Box::new(move || {
            Box::new(ParticipantProcess::new(Arc::clone(&runtime), wakeup_atom))
                as Box<dyn beamr::native::native_process::NativeHandler>
        });
        let pid = self.scheduler.spawn_native(factory).map_err(|error| {
            LiminalError::ConversationFailed {
                message: format!("failed to spawn conversation participant: {error}"),
            }
        })?;
        let participant = ParticipantPid::new(pid);
        let inbox = Arc::new(Mutex::new(VecDeque::new()));
        Ok(ParticipantChannel::new(participant, inbox))
    }

    /// Spawns and boots one actor incarnation as a rollback-safe transaction:
    /// on ANY failure after the actor process exists, every process this
    /// attempt created is terminated, the registration is removed, and the
    /// queued boot command is purged (inside the bounded `boot`), so a failed
    /// spawn leaves nothing behind. Finalization is rechecked immediately
    /// before publishing the actor and again after boot: a spawn that loses to
    /// a concurrent close/finalize rolls itself back rather than returning a
    /// fresh actor nobody will ever clean up.
    fn spawn_actor_for(
        self: &Arc<Self>,
        core: &Arc<ActorCore>,
    ) -> Result<ParticipantPid, LiminalError> {
        if core.is_finalized() {
            return Err(LiminalError::ConversationFailed {
                message: "conversation is closed".to_owned(),
            });
        }
        let pid = self
            .scheduler
            .spawn_trap_exit(self.module_name, self.entry_function, Vec::new())
            .map_err(|error| LiminalError::ConversationFailed {
                message: format!("failed to spawn conversation actor: {error}"),
            })?;
        let actor = ParticipantPid::new(pid);
        if let Err(error) = self.runtime.register(actor, Arc::downgrade(core)) {
            self.rollback_actor_attempt(core, actor, None);
            return Err(error);
        }
        let watcher = match self.spawn_watcher(core, actor) {
            Ok(watcher) => watcher,
            // `spawn_watcher` already terminated its own watcher on failure.
            Err(error) => {
                self.rollback_actor_attempt(core, actor, None);
                return Err(error);
            }
        };
        if let Err(error) = core
            .set_watcher_pid(watcher)
            .and_then(|()| core.set_current_pid(actor))
        {
            self.rollback_actor_attempt(core, actor, Some(watcher));
            return Err(error);
        }
        #[cfg(test)]
        self.boot_barrier_rendezvous(actor);
        if let Err(error) = core.boot(actor) {
            self.rollback_actor_attempt(core, actor, Some(watcher));
            return Err(error);
        }
        // A close/finalize that could not take the lifecycle gate (the actor's
        // own Close slice) may have finalized the core while boot was in
        // flight; a success return here would publish an actor nobody cleans.
        if core.is_finalized() {
            self.rollback_actor_attempt(core, actor, Some(watcher));
            return Err(LiminalError::ConversationFailed {
                message: "conversation is closed".to_owned(),
            });
        }
        Ok(actor)
    }

    /// Aborts one spawn attempt: terminates every process the attempt created
    /// and removes the actor registration. Idempotent — each step tolerates
    /// already-dead pids and already-removed entries, so it composes with the
    /// watcher's own cleanup and with a concurrent finalize.
    fn rollback_actor_attempt(
        &self,
        core: &ActorCore,
        actor: ParticipantPid,
        watcher: Option<ParticipantPid>,
    ) {
        if let Some(watcher) = watcher {
            self.scheduler
                .terminate_process(watcher.get(), beamr::process::ExitReason::Normal);
        }
        self.scheduler
            .terminate_process(actor.get(), beamr::process::ExitReason::Normal);
        self.runtime.deregister_owned(actor, core);
    }

    /// Blocks the spawning thread at the arm→boot seam when a test installed a
    /// rendezvous, handing it the actor pid and waiting for its proceed signal.
    /// This is the held-gap injection point for the construction-ordering pins
    /// (actor killed after watcher arm, before the boot enqueue); it does not
    /// exist outside tests.
    #[cfg(test)]
    fn boot_barrier_rendezvous(&self, actor: ParticipantPid) {
        let barrier = self
            .boot_barrier
            .lock()
            .ok()
            .and_then(|mut barrier| barrier.take());
        if let Some((notify, proceed)) = barrier {
            let _ = notify.send(actor);
            let _ = proceed.recv();
        }
    }

    /// Spawns the exit watcher for a freshly spawned actor process and waits
    /// (bounded) for its first slice to arm trap-exit. The wait is what makes
    /// the later boot-slice link race-free: a link created before the trap is
    /// armed would let an abnormal actor exit cascade-kill the watcher
    /// unobserved. Construction is already a blocking path (boot itself is a
    /// command round trip); teardown paths never wait on the watcher.
    fn spawn_watcher(
        self: &Arc<Self>,
        core: &Arc<ActorCore>,
        actor: ParticipantPid,
    ) -> Result<ParticipantPid, LiminalError> {
        let (armed_tx, armed_rx) = mpsc::sync_channel::<()>(1);
        let watcher_core = Arc::downgrade(core);
        let watcher_supervisor = Arc::downgrade(self);
        let factory = Box::new(move || {
            Box::new(watcher::ActorExitWatcher::new(
                watcher_core.clone(),
                watcher_supervisor.clone(),
                actor,
                armed_tx.clone(),
            )) as Box<dyn beamr::native::native_process::NativeHandler>
        });
        let watcher_pid = self.scheduler.spawn_native(factory).map_err(|error| {
            LiminalError::ConversationFailed {
                message: format!("failed to spawn conversation exit watcher: {error}"),
            }
        })?;
        // One scheduler slice away; the generous bound only guards a wedged
        // scheduler at construction time.
        if armed_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .is_err()
        {
            self.scheduler
                .terminate_process(watcher_pid, beamr::process::ExitReason::Normal);
            return Err(LiminalError::ConversationFailed {
                message: "conversation exit watcher failed to arm".to_owned(),
            });
        }
        Ok(ParticipantPid::new(watcher_pid))
    }
}
