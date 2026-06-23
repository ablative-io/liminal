//! LIM-002: the channel actor as a REAL supervised beamr process.
//!
//! [`ChannelActorCore`] is the shared state (`Arc<ChannelActorCore>`) owned
//! jointly by the host-side handle and the beamr process's NIF command loop. The
//! process is a bytecode `trap_exit` process whose single NIF (`beam.rs`) either
//! drains one queued command or handles a trapped `{EXIT, pid, reason}` signal
//! from a dead subscriber. This is the same machinery the conversation actor
//! uses (`conversation/actor/{beam,core}.rs`) — chosen over a `NativeHandler`
//! because R2 requires LINKING to *already-existing* subscriber processes, which
//! only `ProcessContext::link_facility().link(actor, subscriber)` can do; the
//! `NativeContext` given to a `NativeHandler` can only link a freshly-spawned
//! child.
//!
//! Owns its subscriber list in-memory (R1). Subscriber death is detected via the
//! real link/EXIT path, not weak-pointer polling (R2). Predicates are owned by
//! the actor and evaluated at delivery (R3). Nothing on the delivery path
//! touches persistence or serialisation, and while Ephemeral no haematite type
//! is referenced (R5).

pub(crate) mod beam;
pub(crate) mod queue;
mod wait;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, MutexGuard};

use beamr::atom::Atom;
use beamr::native::ProcessContext;
use beamr::scheduler::Scheduler;
use serde_json::Value;

use crate::causal::CausalContext;
use crate::channel::schema::{Schema, SchemaId, SchemaValidationError};
use crate::channel::subscription::SubscriberRegistration;
use crate::envelope::{Envelope, PublisherId};
use crate::error::LiminalError;

pub(crate) use beam::{ActorRuntime, actor_module, private_data};
pub(crate) use queue::predicate_from;
use queue::{ChannelCommand, ChannelCommandKind, SubscriberSummary};

/// Shared channel-actor state: schema, subscriber fan-out list, closed flag,
/// the command queue, and the wiring needed to wake the process.
pub(crate) struct ChannelActorCore {
    scheduler: Arc<Scheduler>,
    command_atom: Atom,
    schema: Mutex<Schema>,
    subscribers: Mutex<Vec<SubscriberRegistration>>,
    closed: Mutex<bool>,
    commands: Mutex<VecDeque<ChannelCommand>>,
    current_pid: Mutex<Option<u64>>,
    /// Serialises the supervisor's dead-check + respawn so two concurrent callers
    /// can never both observe the dead pid and both spawn a replacement actor
    /// (the restart TOCTOU). Held across the double-checked liveness test and the
    /// spawn in [`crate::channel::supervisor::ChannelSupervisor::ensure_running`].
    restart_lock: Mutex<()>,
    next_command_id: AtomicU64,
}

impl std::fmt::Debug for ChannelActorCore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ChannelActorCore")
            .field("current_pid", &self.current_pid.lock().ok().map(|pid| *pid))
            .finish_non_exhaustive()
    }
}

impl ChannelActorCore {
    pub(crate) const fn new(scheduler: Arc<Scheduler>, command_atom: Atom, schema: Schema) -> Self {
        Self {
            scheduler,
            command_atom,
            schema: Mutex::new(schema),
            subscribers: Mutex::new(Vec::new()),
            closed: Mutex::new(false),
            commands: Mutex::new(VecDeque::new()),
            current_pid: Mutex::new(None),
            restart_lock: Mutex::new(()),
            next_command_id: AtomicU64::new(1),
        }
    }

    pub(crate) const fn scheduler(&self) -> &Arc<Scheduler> {
        &self.scheduler
    }

    pub(crate) const fn restart_lock(&self) -> &Mutex<()> {
        &self.restart_lock
    }

    pub(crate) fn set_current_pid(&self, pid: u64) -> Result<(), LiminalError> {
        *lock(&self.current_pid)? = Some(pid);
        Ok(())
    }

    pub(crate) fn current_pid(&self) -> Result<Option<u64>, LiminalError> {
        Ok(*lock(&self.current_pid)?)
    }

    // ---- host-side request surface (blocks on a per-command reply) ----------

    /// Drives the freshly-spawned actor process to re-establish its links to
    /// every surviving subscriber pid. Called by the supervisor immediately
    /// after a spawn/restart (with `current_pid` already set to the new pid), so
    /// EXIT detection for subscribers that outlived a crash is restored before
    /// the channel is used again (R2/R4).
    pub(crate) fn boot(&self) -> Result<(), LiminalError> {
        let (reply, response) = mpsc::sync_channel(1);
        let pid = self.enqueue(ChannelCommandKind::Boot { reply })?;
        self.wait_live(&response, pid)?
    }

    pub(crate) fn publish(
        &self,
        payload: Vec<u8>,
        publisher_id: PublisherId,
        causal_context: Option<CausalContext>,
    ) -> Result<(), LiminalError> {
        let (reply, response) = mpsc::sync_channel(1);
        let pid = self.enqueue(ChannelCommandKind::Publish {
            payload,
            publisher_id,
            causal_context,
            reply,
        })?;
        self.wait_live(&response, pid)?
    }

    pub(crate) fn subscribe(
        &self,
        registration: SubscriberRegistration,
    ) -> Result<(), LiminalError> {
        let (reply, response) = mpsc::sync_channel(1);
        let pid = self.enqueue(ChannelCommandKind::Subscribe {
            registration,
            reply,
        })?;
        self.wait_live(&response, pid)?
    }

    pub(crate) fn unsubscribe(&self, pid: u64) -> Result<(), LiminalError> {
        let (reply, response) = mpsc::sync_channel(1);
        let target = self.enqueue(ChannelCommandKind::Unsubscribe { pid, reply })?;
        self.wait_live(&response, target)?
    }

    pub(crate) fn evolve(
        &self,
        name: String,
        field_schema: Value,
        default: Value,
    ) -> Result<SchemaId, SchemaValidationError> {
        let (reply, response) = mpsc::sync_channel(1);
        let pid = self
            .enqueue(ChannelCommandKind::Evolve {
                name,
                field_schema,
                default,
                reply,
            })
            .map_err(|error| SchemaValidationError::InvalidSchema {
                message: error.to_string(),
            })?;
        self.wait_schema_live(&response, pid)?
    }

    pub(crate) fn schema_id(&self) -> Result<SchemaId, LiminalError> {
        let (reply, response) = mpsc::sync_channel(1);
        let pid = self.enqueue(ChannelCommandKind::SchemaId { reply })?;
        self.wait_live(&response, pid)?
    }

    pub(crate) fn list_subscribers(&self) -> Result<SubscriberSummary, LiminalError> {
        let (reply, response) = mpsc::sync_channel(1);
        let pid = self.enqueue(ChannelCommandKind::ListSubscribers { reply })?;
        self.wait_live(&response, pid)?
    }

    pub(crate) fn close(&self) -> Result<(), LiminalError> {
        let (reply, response) = mpsc::sync_channel(1);
        let pid = self.enqueue(ChannelCommandKind::Close { reply })?;
        self.wait_live(&response, pid)?
    }

    fn wait_live<T>(
        &self,
        response: &mpsc::Receiver<Result<T, LiminalError>>,
        pid: u64,
    ) -> Result<Result<T, LiminalError>, LiminalError> {
        wait::wait_live(&self.scheduler, response, pid)
    }

    fn wait_schema_live(
        &self,
        response: &mpsc::Receiver<Result<SchemaId, SchemaValidationError>>,
        pid: u64,
    ) -> Result<Result<SchemaId, SchemaValidationError>, SchemaValidationError> {
        wait::wait_schema_live(&self.scheduler, response, pid)
    }

    fn enqueue(&self, kind: ChannelCommandKind) -> Result<u64, LiminalError> {
        let pid = lock(&self.current_pid)?.ok_or_else(|| LiminalError::DeliveryFailed {
            message: "channel actor has no live pid".to_owned(),
        })?;
        let id = self.next_command_id.fetch_add(1, Ordering::Relaxed);
        lock(&self.commands)?.push_back(ChannelCommand { id, kind });
        if self.scheduler.enqueue_atom_message(pid, self.command_atom) {
            Ok(pid)
        } else {
            self.remove_command(id)?;
            Err(LiminalError::DeliveryFailed {
                message: format!("channel actor pid {pid} is not live"),
            })
        }
    }

    fn remove_command(&self, id: u64) -> Result<(), LiminalError> {
        lock(&self.commands)?.retain(|command| command.id != id);
        Ok(())
    }

    // ---- process-side command loop (runs inside the beamr process) ----------

    /// Pop and service exactly one queued command. Returns `true` when the
    /// command requested the process stop (a successful `Close`).
    pub(crate) fn process_next_command(&self, context: &ProcessContext<'_>) -> bool {
        let Some(command) = self.pop_command() else {
            return false;
        };
        match command.kind {
            ChannelCommandKind::Boot { reply } => {
                let _ = reply.send(self.apply_boot(context));
                false
            }
            ChannelCommandKind::Publish {
                payload,
                publisher_id,
                causal_context,
                reply,
            } => {
                let _ = reply.send(self.apply_publish(&payload, publisher_id, causal_context));
                false
            }
            ChannelCommandKind::Subscribe {
                registration,
                reply,
            } => {
                let _ = reply.send(self.apply_subscribe(registration, context));
                false
            }
            ChannelCommandKind::Unsubscribe { pid, reply } => {
                let _ = reply.send(self.apply_unsubscribe(pid, context));
                false
            }
            ChannelCommandKind::Evolve {
                name,
                field_schema,
                default,
                reply,
            } => {
                let _ = reply.send(self.apply_evolve(name, field_schema, default));
                false
            }
            ChannelCommandKind::SchemaId { reply } => {
                let _ = reply.send(self.apply_schema_id());
                false
            }
            ChannelCommandKind::ListSubscribers { reply } => {
                let _ = reply.send(self.apply_list_subscribers());
                false
            }
            ChannelCommandKind::Close { reply } => {
                let result = self.apply_close();
                let stop = result.is_ok();
                let _ = reply.send(result);
                stop
            }
        }
    }

    fn pop_command(&self) -> Option<ChannelCommand> {
        self.commands.lock().ok()?.pop_front()
    }

    /// Re-link the actor process to every surviving subscriber. Runs inside the
    /// freshly-spawned process context (the only place a link to an existing pid
    /// can be created). On the initial spawn `subscribers` is empty so this is a
    /// no-op; after a restart it re-establishes the links a crash tore down, so
    /// subscriber-death EXIT detection works again (R2/R4) — exactly mirroring
    /// the conversation actor's `link_participants` boot step.
    fn apply_boot(&self, context: &ProcessContext<'_>) -> Result<(), LiminalError> {
        // Take the registrations out under a short lock, then re-link outside it.
        let registrations = std::mem::take(&mut *lock(&self.subscribers)?);
        // Re-link only to subscribers whose process is still alive; a subscriber
        // that did not outlive the crash is pruned rather than failing boot (its
        // pid no longer exists). Subscribers trap exits (see `subscription.rs`)
        // so in practice they survive, but pruning keeps boot robust either way.
        let mut live = Vec::with_capacity(registrations.len());
        for subscriber in registrations {
            if self
                .scheduler
                .process_table()
                .get(subscriber.pid())
                .is_none()
            {
                continue;
            }
            beam::link_subscriber(context, subscriber.pid())?;
            live.push(subscriber);
        }
        *lock(&self.subscribers)? = live;
        Ok(())
    }

    fn apply_publish(
        &self,
        payload: &[u8],
        publisher_id: PublisherId,
        causal_context: Option<CausalContext>,
    ) -> Result<(), LiminalError> {
        if *lock(&self.closed)? {
            return Err(LiminalError::ChannelClosed {
                message: "channel is closed".to_owned(),
            });
        }
        let envelope = {
            let schema = lock(&self.schema)?;
            schema
                .validate(payload)
                .map_err(|error| schema_mismatch(&error))?;
            let normalized = schema
                .validate_and_apply_defaults(payload)
                .map_err(|error| schema_mismatch(&error))?;
            Envelope::new(normalized, causal_context, schema.id(), publisher_id)
        };
        let subscribers = lock(&self.subscribers)?;
        for subscriber in subscribers.iter() {
            subscriber.deliver(&envelope)?;
        }
        drop(subscribers);
        Ok(())
    }

    fn apply_subscribe(
        &self,
        registration: SubscriberRegistration,
        context: &ProcessContext<'_>,
    ) -> Result<(), LiminalError> {
        if *lock(&self.closed)? {
            return Err(LiminalError::ChannelClosed {
                message: "channel is closed".to_owned(),
            });
        }
        beam::link_subscriber(context, registration.pid())?;
        lock(&self.subscribers)?.push(registration);
        Ok(())
    }

    fn apply_unsubscribe(
        &self,
        pid: u64,
        context: &ProcessContext<'_>,
    ) -> Result<(), LiminalError> {
        beam::unlink_subscriber(context, pid)?;
        lock(&self.subscribers)?.retain(|subscriber| subscriber.pid() != pid);
        Ok(())
    }

    fn apply_evolve(
        &self,
        name: String,
        field_schema: Value,
        default: Value,
    ) -> Result<SchemaId, SchemaValidationError> {
        let mut schema =
            self.schema
                .lock()
                .map_err(|error| SchemaValidationError::InvalidSchema {
                    message: format!("channel schema lock poisoned: {error}"),
                })?;
        let evolved = schema.evolve_add_field(name, field_schema, default)?;
        let schema_id = evolved.id();
        *schema = evolved;
        drop(schema);
        Ok(schema_id)
    }

    fn apply_schema_id(&self) -> Result<SchemaId, LiminalError> {
        Ok(lock(&self.schema)?.id())
    }

    fn apply_list_subscribers(&self) -> Result<SubscriberSummary, LiminalError> {
        Ok(lock(&self.subscribers)?
            .iter()
            .map(SubscriberRegistration::pid)
            .collect())
    }

    /// Closes the channel and stops the actor process.
    ///
    /// Subscriber-process lifetime is deliberately owned by the host-side
    /// [`crate::channel::SubscriptionHandle`] (its `Drop` terminates the beamr
    /// subscriber process), NOT by the channel actor. This is the correct model:
    /// a subscriber may outlive any single actor incarnation (it survives an
    /// actor crash + restart, which is precisely what MAJOR-1's re-link path
    /// depends on — the surviving subscriber pids are the ones `apply_boot`
    /// re-links to). If `close` terminated the subscriber processes here it would
    /// (a) race the handle's own `Drop`-driven terminate and (b) destroy
    /// processes the actor does not own. So `close` only releases the actor's
    /// *references* to its subscribers (dropping the inbox `Arc`s and predicates);
    /// each subscriber process is reclaimed when its owning handle drops. The
    /// trapped-EXIT pruning path means the actor never leaks a stale link either:
    /// a subscriber that dies first is removed from the fan-out before close.
    fn apply_close(&self) -> Result<(), LiminalError> {
        *lock(&self.closed)? = true;
        lock(&self.subscribers)?.clear();
        Ok(())
    }

    /// Handle a trapped EXIT from a dead subscriber: drop it from the fan-out
    /// list. This is the structural unsubscribe-on-death path (R2).
    pub(crate) fn handle_subscriber_exit(&self, pid: u64) -> Result<(), LiminalError> {
        lock(&self.subscribers)?.retain(|subscriber| subscriber.pid() != pid);
        Ok(())
    }
}

fn schema_mismatch(error: &SchemaValidationError) -> LiminalError {
    LiminalError::SchemaMismatch {
        message: error.to_string(),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>, LiminalError> {
    mutex.lock().map_err(|error| LiminalError::DeliveryFailed {
        message: format!("channel actor lock poisoned: {error}"),
    })
}
