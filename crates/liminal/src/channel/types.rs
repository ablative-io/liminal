//! Public channel surface: [`ChannelConfig`], [`ChannelMode`], and the
//! cloneable [`ChannelHandle`] that drives a REAL supervised beamr channel
//! actor (LIM-002).
//!
//! The handle is a thin, synchronous-looking facade over the process-backed
//! actor: every operation enqueues a typed command onto the actor's mailbox and
//! blocks on a per-command reply (the haematite `ShardHandle` pattern). It owns
//! no subscriber state and performs no fan-out itself — the actor process does.
//!
//! `ChannelHandle::new` stays infallible (existing call-sites depend on it): the
//! actor is spawned lazily on first use and the spawn result is memoised, so a
//! scheduler failure surfaces as a `LiminalError` from the first operation
//! rather than as a panic.

use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

use crate::causal::CausalContext;
use crate::channel::actor::{ChannelActorCore, predicate_from};
use crate::channel::observer::ClusterObserver;
use crate::channel::schema::{Schema, SchemaId, SchemaValidationError};
use crate::channel::subscription::{SubscriptionHandle, SubscriptionPredicate};
use crate::channel::supervisor::{ChannelSupervisor, shared_supervisor};
use crate::durability::bridge::block_on;
use crate::durability::{DurableChannel, DurableStore, MessageEnvelope};
use crate::envelope::{Envelope, PublisherId};
use crate::error::LiminalError;

/// Single-partition count used to back a flat runtime channel with durable storage.
const RUNTIME_DURABLE_PARTITIONS: usize = 1;

/// Defines whether a channel is memory-only or durable across restarts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChannelMode {
    /// In-memory channel mode with no persistence overhead.
    Ephemeral,
    /// Durable channel mode reserved for future haematite-backed storage.
    Durable,
}

/// Compatibility alias for the channel-owned schema definition.
pub type SchemaRef = Schema;

/// Required configuration for creating a typed channel.
#[derive(Clone, Debug)]
pub struct ChannelConfig {
    /// Explicit channel name.
    pub name: String,
    /// Explicit schema for validating published payloads.
    pub schema: Schema,
    /// Explicit durability mode for the channel.
    pub mode: ChannelMode,
}

impl ChannelConfig {
    /// Creates channel configuration from its required fields.
    #[must_use]
    pub const fn new(name: String, schema: Schema, mode: ChannelMode) -> Self {
        Self { name, schema, mode }
    }
}

/// A lazily-spawned, supervised channel actor shared by every clone of a handle.
///
/// `supervisor` is stored as a `Result` so [`ChannelHandle::new`] can stay
/// infallible: a scheduler-start failure is captured here and surfaced as a
/// `LiminalError` the first time the actor is actually used.
struct ChannelActorState {
    supervisor: Result<ChannelSupervisor, String>,
    core: OnceLock<Result<Arc<ChannelActorCore>, String>>,
    restarts: AtomicU32,
}

impl ChannelActorState {
    const fn new(supervisor: Result<ChannelSupervisor, String>) -> Self {
        Self {
            supervisor,
            core: OnceLock::new(),
            restarts: AtomicU32::new(0),
        }
    }

    fn supervisor(&self) -> Result<&ChannelSupervisor, LiminalError> {
        self.supervisor
            .as_ref()
            .map_err(|message| LiminalError::PublishFailed {
                message: format!("channel supervisor unavailable: {message}"),
            })
    }

    /// The installed cluster observer, if this channel runs on a clustered
    /// supervisor (SRV-005). Returns `None` for non-clustered channels.
    fn observer(&self) -> Option<Arc<dyn ClusterObserver>> {
        self.supervisor
            .as_ref()
            .ok()
            .and_then(|supervisor| supervisor.observer().cloned())
    }

    /// Returns the live actor core, spawning it (and any restart) on demand.
    fn core(&self, schema: &Schema) -> Result<Arc<ChannelActorCore>, LiminalError> {
        let supervisor = self.supervisor()?;
        let stored = self.core.get_or_init(|| {
            supervisor
                .spawn_channel(schema.clone())
                .map_err(|error| error.to_string())
        });
        let core = stored
            .as_ref()
            .map_err(|message| LiminalError::PublishFailed {
                message: format!("channel actor unavailable: {message}"),
            })?;
        // Restart on a dead pid (R4) before returning the core for use.
        supervisor.ensure_running(core, &self.restarts)?;
        Ok(Arc::clone(core))
    }
}

impl std::fmt::Debug for ChannelActorState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ChannelActorState")
            .field("supervisor", &self.supervisor)
            .finish_non_exhaustive()
    }
}

/// Cloneable handle for interacting with a channel actor process.
#[derive(Clone, Debug)]
pub struct ChannelHandle {
    config: ChannelConfig,
    actor: Arc<ChannelActorState>,
    durable: Option<Arc<Mutex<DurableChannel>>>,
}

impl ChannelHandle {
    /// Creates an ephemeral handle backed by a real supervised channel actor on
    /// the shared default supervisor.
    ///
    /// The actor process is spawned lazily on first use; a scheduler failure is
    /// surfaced as a [`LiminalError`] from the first operation, not a panic.
    #[must_use]
    pub fn new(config: ChannelConfig) -> Self {
        let supervisor = shared_supervisor().map_err(|error| error.to_string());
        Self {
            config,
            actor: Arc::new(ChannelActorState::new(supervisor)),
            durable: None,
        }
    }

    /// Creates an ephemeral handle bound to an explicit `supervisor` (isolation
    /// for the registry and tests).
    #[must_use]
    pub fn with_supervisor(config: ChannelConfig, supervisor: ChannelSupervisor) -> Self {
        Self {
            config,
            actor: Arc::new(ChannelActorState::new(Ok(supervisor))),
            durable: None,
        }
    }

    /// Creates a durable handle that persists every accepted publish to `store`
    /// before fanning it out to subscribers.
    ///
    /// # Errors
    ///
    /// Returns [`LiminalError::PublishFailed`] when the durable channel cannot be
    /// initialized over `store`.
    pub fn new_durable(
        config: ChannelConfig,
        store: Arc<dyn DurableStore>,
    ) -> Result<Self, LiminalError> {
        let durable = DurableChannel::new(config.name.clone(), RUNTIME_DURABLE_PARTITIONS, store)
            .map_err(|error| LiminalError::PublishFailed {
            message: format!(
                "failed to initialize durable channel '{}': {error}",
                config.name
            ),
        })?;
        let supervisor = shared_supervisor()?;
        Ok(Self {
            config,
            actor: Arc::new(ChannelActorState::new(Ok(supervisor))),
            durable: Some(Arc::new(Mutex::new(durable))),
        })
    }

    /// Creates a durable handle bound to an explicit `supervisor`.
    ///
    /// Used by the standalone server so every channel — durable or ephemeral —
    /// shares ONE (optionally clustered) supervisor and thus one scheduler, which
    /// is the precondition for cross-node delivery (SRV-005): a subscriber pid
    /// joined to a channel's distributed process group must live on the same
    /// scheduler that owns the distribution links.
    ///
    /// # Errors
    ///
    /// Returns [`LiminalError::PublishFailed`] when the durable channel cannot be
    /// initialized over `store`.
    pub fn new_durable_with_supervisor(
        config: ChannelConfig,
        store: Arc<dyn DurableStore>,
        supervisor: ChannelSupervisor,
    ) -> Result<Self, LiminalError> {
        let durable = DurableChannel::new(config.name.clone(), RUNTIME_DURABLE_PARTITIONS, store)
            .map_err(|error| LiminalError::PublishFailed {
            message: format!(
                "failed to initialize durable channel '{}': {error}",
                config.name
            ),
        })?;
        Ok(Self {
            config,
            actor: Arc::new(ChannelActorState::new(Ok(supervisor))),
            durable: Some(Arc::new(Mutex::new(durable))),
        })
    }

    /// Returns the channel configuration used to create this handle.
    #[must_use]
    pub const fn config(&self) -> &ChannelConfig {
        &self.config
    }

    /// Publishes a payload to the channel with the default publisher identity.
    ///
    /// # Errors
    ///
    /// Returns a [`LiminalError`] when the channel cannot accept the payload or the schema rejects it.
    pub fn publish<Payload>(&self, payload: Payload) -> Result<(), LiminalError>
    where
        Payload: AsRef<[u8]>,
    {
        self.publish_with_context(payload, PublisherId::default(), None)
    }

    /// Publishes a payload with an explicit publisher identity.
    ///
    /// # Errors
    ///
    /// Returns a [`LiminalError`] when the channel cannot accept the payload or the schema rejects it.
    pub fn publish_from<Payload>(
        &self,
        publisher_id: impl Into<PublisherId>,
        payload: Payload,
    ) -> Result<(), LiminalError>
    where
        Payload: AsRef<[u8]>,
    {
        self.publish_with_context(payload, publisher_id.into(), None)
    }

    /// Publishes a payload with explicit publisher and causal metadata.
    ///
    /// # Errors
    ///
    /// Returns a [`LiminalError`] when the channel cannot accept the payload or the schema rejects it.
    pub fn publish_with_context<Payload>(
        &self,
        payload: Payload,
        publisher_id: PublisherId,
        causal_context: Option<CausalContext>,
    ) -> Result<(), LiminalError>
    where
        Payload: AsRef<[u8]>,
    {
        // Durable channels persist the message to the store BEFORE acknowledging
        // the publish (and before fanning out): a published message that was not
        // durably recorded would be lost on shutdown, which CN7 forbids.
        if let Some(durable) = self.durable.as_ref() {
            self.persist_durable(durable, payload.as_ref(), &publisher_id)?;
        }
        let core = self.core()?;
        let envelope = core.publish(payload.as_ref().to_vec(), publisher_id, causal_context)?;
        // SRV-005: hand the normalised envelope to the cluster observer so it can
        // fan the message out to remote subscribers. Local fan-out already
        // happened inside `core.publish`; this is purely the cross-node leg and
        // is a no-op when no observer is installed (non-clustered channels).
        if let Some(observer) = self.actor.observer() {
            observer.on_publish(&self.config.name, &envelope);
        }
        Ok(())
    }

    fn persist_durable(
        &self,
        durable: &Arc<Mutex<DurableChannel>>,
        payload: &[u8],
        publisher_id: &PublisherId,
    ) -> Result<(), LiminalError> {
        let envelope = MessageEnvelope {
            payload: payload.to_vec(),
            causal_context: None,
            timestamp: now_millis(),
            publisher_id: publisher_id.as_str().to_owned(),
            idempotency_key: None,
        };
        let publish_result = {
            let mut channel = durable
                .lock()
                .map_err(|error| LiminalError::PublishFailed {
                    message: format!("durable channel state unavailable: {error}"),
                })?;
            block_on(channel.publish(&envelope))
        };
        publish_result
            .map_err(|error| LiminalError::PublishFailed {
                message: format!(
                    "durable publish bridge for channel '{}' failed: {error}",
                    self.config.name
                ),
            })?
            .map_err(|error| LiminalError::PublishFailed {
                message: format!(
                    "durable publish to channel '{}' failed: {error}",
                    self.config.name
                ),
            })?;
        Ok(())
    }

    /// Returns the schema version currently owned by the channel actor.
    ///
    /// # Errors
    ///
    /// Returns a [`LiminalError`] when the channel actor cannot be read.
    pub fn current_schema_id(&self) -> Result<SchemaId, LiminalError> {
        self.core()?.schema_id()
    }

    /// Evolves the channel schema by adding a defaulted field without disconnecting subscribers.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaValidationError`] when the schema cannot be evolved.
    pub fn evolve_schema_add_field(
        &self,
        name: impl Into<String>,
        field_schema: Value,
        default: Value,
    ) -> Result<SchemaId, SchemaValidationError> {
        let core = self
            .core()
            .map_err(|error| SchemaValidationError::InvalidSchema {
                message: error.to_string(),
            })?;
        core.evolve(name.into(), field_schema, default)
    }

    /// Subscribes to the channel, receiving every published message.
    ///
    /// # Errors
    ///
    /// Returns a [`LiminalError`] when a subscription cannot be created.
    pub fn subscribe(&self) -> Result<SubscriptionHandle, LiminalError> {
        self.subscribe_inner(None)
    }

    /// Subscribes with a delivery predicate: only messages for which `predicate`
    /// returns `true` are delivered to this subscriber. The predicate is owned
    /// and evaluated by the actor process (R3).
    ///
    /// # Clustering
    ///
    /// The predicate filters **local-node publishes only**. Under clustering
    /// (SRV-005), messages published on a remote node are delivered to this
    /// subscriber *ungated* — the predicate is a non-serializable closure and is
    /// not propagated across the wire, so remote nodes cannot evaluate it. If you
    /// need filtering to hold for cross-node traffic, filter again on receipt
    /// rather than relying on this predicate alone.
    ///
    /// # Errors
    ///
    /// Returns a [`LiminalError`] when a subscription cannot be created.
    pub fn subscribe_filtered<F>(&self, predicate: F) -> Result<SubscriptionHandle, LiminalError>
    where
        F: Fn(&Envelope) -> bool + Send + Sync + 'static,
    {
        self.subscribe_inner(Some(predicate_from(predicate)))
    }

    fn subscribe_inner(
        &self,
        predicate: Option<SubscriptionPredicate>,
    ) -> Result<SubscriptionHandle, LiminalError> {
        let core = self.core()?;
        let (handle, registration) = SubscriptionHandle::spawn(core.scheduler(), predicate)?;
        let pid = registration.pid();
        core.subscribe(registration)?;
        // SRV-005: tell the cluster a local subscriber joined this channel so it
        // can advertise the subscription to peers via its process group.
        if let Some(observer) = self.actor.observer() {
            observer.on_subscribe(&self.config.name, pid);
        }
        Ok(handle)
    }

    /// Unsubscribes the subscriber owning `subscription` by its process pid.
    ///
    /// # Errors
    ///
    /// Returns a [`LiminalError`] when the unsubscribe command fails.
    pub fn unsubscribe(&self, subscription: &SubscriptionHandle) -> Result<(), LiminalError> {
        let pid = subscription.pid();
        self.core()?.unsubscribe(pid)?;
        // SRV-005: tell the cluster the local subscriber left so it can withdraw
        // the subscription from its process group.
        if let Some(observer) = self.actor.observer() {
            observer.on_unsubscribe(&self.config.name, pid);
        }
        Ok(())
    }

    /// Flushes buffered durable channel state to the backing store before shutdown.
    ///
    /// # Errors
    ///
    /// Returns a [`LiminalError`] when the channel actor cannot be inspected or
    /// when the durable store flush fails.
    pub fn flush(&self) -> Result<(), LiminalError> {
        // Confirm the actor is reachable (and restart it if needed) before flush.
        drop(self.core()?);
        let Some(durable) = self.durable.as_ref() else {
            return Ok(());
        };
        let flush_result = {
            let channel = durable
                .lock()
                .map_err(|error| LiminalError::PublishFailed {
                    message: format!("durable channel state unavailable: {error}"),
                })?;
            block_on(channel.flush_store())
        };
        flush_result
            .map_err(|error| LiminalError::PublishFailed {
                message: format!(
                    "durable flush bridge for channel '{}' failed: {error}",
                    self.config.name
                ),
            })?
            .map_err(|error| LiminalError::PublishFailed {
                message: format!(
                    "durable flush for channel '{}' failed: {error}",
                    self.config.name
                ),
            })?;
        Ok(())
    }

    /// Returns the number of currently-active subscribers on the channel actor.
    ///
    /// # Errors
    ///
    /// Returns a [`LiminalError`] when the actor cannot service the query.
    pub fn subscriber_count(&self) -> Result<usize, LiminalError> {
        Ok(self.core()?.list_subscribers()?.len())
    }

    /// Closes the channel gracefully, stopping the actor process.
    ///
    /// # Errors
    ///
    /// Returns a [`LiminalError`] when the channel cannot be shut down.
    pub fn close(&self) -> Result<(), LiminalError> {
        self.core()?.close()
    }

    fn core(&self) -> Result<Arc<ChannelActorCore>, LiminalError> {
        self.actor.core(&self.config.schema)
    }

    /// The channel actor's current beamr pid, ensuring it is running first.
    /// Test-only: lets restart tests crash the exact actor process.
    #[cfg(test)]
    pub(crate) fn actor_pid(&self) -> Result<u64, LiminalError> {
        let core = self.core()?;
        core.current_pid()?
            .ok_or_else(|| LiminalError::DeliveryFailed {
                message: "channel actor has no live pid".to_owned(),
            })
    }

    /// The scheduler the channel actor and its subscribers run on (test-only).
    #[cfg(test)]
    pub(crate) fn scheduler(&self) -> Result<Arc<beamr::scheduler::Scheduler>, LiminalError> {
        Ok(Arc::clone(self.core()?.scheduler()))
    }
}

/// Returns the current epoch milliseconds, saturating to zero before the epoch.
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
