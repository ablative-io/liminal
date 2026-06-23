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
        self.inner.spawn_actor_for(&core)?;
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
    participant_runtime: Arc<ParticipantRuntime>,
    participant_wakeup_atom: Atom,
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
        })
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
