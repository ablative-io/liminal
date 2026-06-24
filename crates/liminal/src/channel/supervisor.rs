//! LIM-002 R4: channel supervision.
//!
//! [`ChannelSupervisor`] owns the beamr [`Scheduler`] every channel actor runs
//! on, the bytecode module they share, and the per-scheduler [`ActorRuntime`]
//! that maps actor pids to their cores. It spawns each channel actor as a
//! `trap_exit` process and re-spawns one on demand if its pid is no longer live,
//! so a crashed channel is restarted WITHOUT affecting any other channel (each
//! channel is an independent process with its own pid and subscriber list). The
//! restart strategy is configurable through [`ChannelRestartPolicy`].
//!
//! A process-global default supervisor (`shared_supervisor`) backs the
//! infallible [`crate::channel::ChannelHandle::new`] constructor so existing
//! call-sites keep working; tests and the registry can construct dedicated
//! supervisors for isolation.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};

use beamr::atom::{Atom, AtomTable};
use beamr::distribution::{DistributionConfig, Resolver};
use beamr::module::ModuleRegistry;
use beamr::scheduler::{Scheduler, SchedulerConfig};

use crate::channel::actor::{ActorRuntime, ChannelActorCore, actor_module, private_data};
use crate::channel::observer::ClusterObserver;
use crate::channel::schema::Schema;
use crate::error::LiminalError;

/// How a supervised channel actor is restarted after its process dies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChannelRestartPolicy {
    /// Maximum number of restarts permitted before the actor is left dead.
    pub max_restarts: u32,
    /// Whether a dead actor is restarted at all (one-for-one when `true`).
    pub restart: bool,
}

impl ChannelRestartPolicy {
    /// One-for-one restart with a bounded restart budget.
    #[must_use]
    pub const fn one_for_one(max_restarts: u32) -> Self {
        Self {
            max_restarts,
            restart: true,
        }
    }

    /// No automatic restart (the actor stays dead once it exits).
    #[must_use]
    pub const fn never() -> Self {
        Self {
            max_restarts: 0,
            restart: false,
        }
    }
}

impl Default for ChannelRestartPolicy {
    fn default() -> Self {
        Self::one_for_one(8)
    }
}

/// Number of scheduler threads channel actors share. One thread keeps every
/// actor's mailbox processing serialized per-process while remaining cheap.
const CHANNEL_SCHEDULER_THREADS: usize = 1;

/// Supervises channel actor processes on a shared beamr scheduler.
#[derive(Clone)]
pub struct ChannelSupervisor {
    inner: Arc<SupervisorInner>,
}

struct SupervisorInner {
    scheduler: Arc<Scheduler>,
    runtime: Arc<ActorRuntime>,
    policy: ChannelRestartPolicy,
    module_name: Atom,
    entry_function: Atom,
    /// Optional cluster observer installed once, after construction, by the
    /// standalone server when clustering is configured (SRV-005). The library
    /// itself never installs one — clustering is an out-of-library concern.
    observer: OnceLock<Arc<dyn ClusterObserver>>,
}

impl std::fmt::Debug for ChannelSupervisor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ChannelSupervisor")
            .field("policy", &self.inner.policy)
            .finish_non_exhaustive()
    }
}

impl ChannelSupervisor {
    /// Builds a supervisor with its own scheduler and the default restart policy.
    ///
    /// # Errors
    /// Returns [`LiminalError::ConversationFailed`] when the scheduler cannot start.
    pub fn new() -> Result<Self, LiminalError> {
        Self::with_policy(ChannelRestartPolicy::default())
    }

    /// Builds a supervisor with an explicit restart policy on a non-clustered
    /// scheduler.
    ///
    /// # Errors
    /// Returns [`LiminalError::ConversationFailed`] when the scheduler cannot start.
    pub fn with_policy(policy: ChannelRestartPolicy) -> Result<Self, LiminalError> {
        Self::build(policy, None, None, None)
    }

    /// Builds a supervisor whose scheduler is distribution-enabled (SRV-005), so
    /// every channel actor and every subscriber process this supervisor spawns
    /// shares ONE clustered scheduler. This is the scheduler the cluster attaches
    /// its process-group transport to: a subscriber pid joined to a channel's pg
    /// group MUST live on the same scheduler that owns the distribution
    /// connections, or cross-node delivery cannot reach it.
    ///
    /// `node_name`/`creation` form this node's distribution identity; `cookie`
    /// and `resolver` are handed verbatim to the scheduler's
    /// [`DistributionConfig`] (the resolver MUST be the same instance the cluster
    /// uses to dial seeds, so handshake-established names resolve consistently).
    ///
    /// # Errors
    /// Returns [`LiminalError::ConversationFailed`] when the scheduler cannot start.
    pub fn with_distribution(
        node_name: String,
        creation: u32,
        cookie: String,
        resolver: Resolver,
        policy: ChannelRestartPolicy,
    ) -> Result<Self, LiminalError> {
        let distribution = DistributionConfig { resolver, cookie };
        Self::build(policy, Some(node_name), Some(creation), Some(distribution))
    }

    fn build(
        policy: ChannelRestartPolicy,
        node_name: Option<String>,
        creation: Option<u32>,
        distribution: Option<DistributionConfig>,
    ) -> Result<Self, LiminalError> {
        let atoms = AtomTable::with_common_atoms();
        let module_name = atoms.intern("liminal_channel_actor");
        let entry_function = atoms.intern("main");
        let command_function = atoms.intern("process_command");
        let command_atom = atoms.intern("liminal_channel_command");
        let runtime = Arc::new(ActorRuntime::new(command_atom));
        let registry = Arc::new(ModuleRegistry::new());
        registry.insert(actor_module(module_name, entry_function, command_function));
        let scheduler = Scheduler::new(
            SchedulerConfig {
                thread_count: Some(CHANNEL_SCHEDULER_THREADS),
                nif_private_data: Some(private_data(Arc::clone(&runtime))),
                node_name,
                creation,
                distribution,
                ..SchedulerConfig::default()
            },
            registry,
        )
        .map_err(|message| LiminalError::ConversationFailed { message })?;
        Ok(Self {
            inner: Arc::new(SupervisorInner {
                scheduler: Arc::new(scheduler),
                runtime,
                policy,
                module_name,
                entry_function,
                observer: OnceLock::new(),
            }),
        })
    }

    /// The scheduler channel actors and their subscribers run on.
    #[must_use]
    pub fn scheduler(&self) -> Arc<Scheduler> {
        Arc::clone(&self.inner.scheduler)
    }

    /// Installs the cluster observer (SRV-005). Idempotent: the first install
    /// wins and later attempts are ignored, so the observer can be wired exactly
    /// once after the supervisor (and its scheduler) exist.
    pub fn install_observer(&self, observer: Arc<dyn ClusterObserver>) {
        let _ = self.inner.observer.set(observer);
    }

    /// The installed cluster observer, if any.
    #[must_use]
    pub(crate) fn observer(&self) -> Option<&Arc<dyn ClusterObserver>> {
        self.inner.observer.get()
    }

    /// The configured restart policy.
    #[must_use]
    pub fn policy(&self) -> &ChannelRestartPolicy {
        &self.inner.policy
    }

    /// Spawns a fresh channel actor for `schema` and returns its shared core.
    ///
    /// # Errors
    /// Returns [`LiminalError`] when the actor process cannot be spawned.
    pub(crate) fn spawn_channel(
        &self,
        schema: Schema,
    ) -> Result<Arc<ChannelActorCore>, LiminalError> {
        let core = Arc::new(ChannelActorCore::new(
            self.scheduler(),
            self.inner.runtime.command_atom(),
            schema,
        ));
        self.spawn_actor_for(&core)?;
        Ok(core)
    }

    /// Ensures `core` has a live actor process, spawning (or restarting) one if
    /// its current pid is dead. Honours the restart budget: once exhausted, a
    /// dead actor is not restarted and a [`LiminalError::DeliveryFailed`] is
    /// returned. This is the one-for-one restart that leaves other channels
    /// untouched (each `core` is supervised independently).
    ///
    /// # Errors
    /// Returns [`LiminalError`] when restart is disabled/exhausted or the spawn
    /// fails.
    pub(crate) fn ensure_running(
        &self,
        core: &Arc<ChannelActorCore>,
        restarts: &AtomicU32,
    ) -> Result<u64, LiminalError> {
        // Fast path: a live pid needs no lock. The slow (respawn) path below is
        // serialised so two concurrent callers that both see a dead pid cannot
        // both spawn a replacement (the restart TOCTOU).
        if let Some(pid) = self.live_pid(core)? {
            return Ok(pid);
        }
        // Hold the per-channel restart lock across the dead-check and respawn so
        // exactly one thread restarts; any racing caller re-reads the now-live
        // pid below. The lock lives on `core` so each channel is supervised
        // independently (mirrors `conversation/actor/core.rs`'s `restart_lock`).
        let guard = core
            .restart_lock()
            .lock()
            .map_err(|error| LiminalError::DeliveryFailed {
                message: format!("channel actor restart lock poisoned: {error}"),
            })?;
        // Double-checked liveness AFTER acquiring the lock: the thread that won
        // the race has already respawned, so we must not spawn a second actor.
        if let Some(pid) = self.live_pid(core)? {
            return Ok(pid);
        }
        if !self.inner.policy.restart {
            return Err(LiminalError::DeliveryFailed {
                message: "channel actor died and restart is disabled".to_owned(),
            });
        }
        let used = restarts.fetch_add(1, Ordering::Relaxed);
        if used >= self.inner.policy.max_restarts {
            return Err(LiminalError::DeliveryFailed {
                message: format!(
                    "channel actor restart budget ({}) exhausted",
                    self.inner.policy.max_restarts
                ),
            });
        }
        let pid = self.spawn_actor_for(core)?;
        drop(guard);
        Ok(pid)
    }

    /// The current pid if it is still live in the scheduler's process table,
    /// otherwise `None` (the actor needs spawning/restarting).
    fn live_pid(&self, core: &Arc<ChannelActorCore>) -> Result<Option<u64>, LiminalError> {
        if let Some(pid) = core.current_pid()? {
            if self.inner.scheduler.process_table().get(pid).is_some() {
                return Ok(Some(pid));
            }
        }
        Ok(None)
    }

    fn spawn_actor_for(&self, core: &Arc<ChannelActorCore>) -> Result<u64, LiminalError> {
        let pid = self
            .inner
            .scheduler
            .spawn_trap_exit(
                self.inner.module_name,
                self.inner.entry_function,
                Vec::new(),
            )
            .map_err(|error| LiminalError::ConversationFailed {
                message: format!("failed to spawn channel actor: {error:?}"),
            })?;
        self.inner.runtime.register(pid, Arc::downgrade(core))?;
        core.set_current_pid(pid)?;
        // Re-link the new process to every surviving subscriber so subscriber
        // death (EXIT) detection works after a restart — exactly as the
        // conversation actor's `spawn_actor_for` calls `core.boot(...)`. On the
        // very first spawn the subscriber list is empty, so this is a no-op.
        core.boot()?;
        Ok(pid)
    }

    /// Stops the underlying scheduler.
    pub fn shutdown(&self) {
        self.inner.scheduler.shutdown();
    }
}

/// The process-global default channel supervisor, lazily started on first use.
static SHARED: OnceLock<ChannelSupervisor> = OnceLock::new();

/// Returns the shared default supervisor, starting it on first use.
///
/// # Errors
/// Returns [`LiminalError`] when the shared scheduler cannot start.
pub fn shared_supervisor() -> Result<ChannelSupervisor, LiminalError> {
    if let Some(existing) = SHARED.get() {
        return Ok(existing.clone());
    }
    let supervisor = ChannelSupervisor::new()?;
    Ok(SHARED.get_or_init(|| supervisor).clone())
}
