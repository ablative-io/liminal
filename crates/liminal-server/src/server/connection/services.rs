use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Instant;

use haematite::{Database, DatabaseConfig, EventStore};
use liminal::channel::{ChannelConfig, ChannelHandle, ChannelMode, Schema};
use liminal::conversation::{
    ConversationSupervisor, CrashPolicy, EchoBehaviour, ParticipantBehaviour,
};
use liminal::durability::bridge::block_on;
use liminal::durability::{
    DedupCache, DedupDecision, DurabilityError, DurableStore, EphemeralHaematiteStore,
    HaematiteStore, ProcessingReceipt, open_ephemeral,
};
use liminal::protocol::{MessageEnvelope, ProtocolError, SchemaId as ProtocolSchemaId};

use super::conversation::{ConnectionConversation, LiminalConversationResource};
use super::services_cluster::build_channel_cluster;
use super::services_schema::resolve_channel_schema;
use super::worker_front_door::WorkerFrontDoorServices;
use crate::ServerError;
use crate::config::types::{ClusterConfig, ServerConfig, ServiceProfile};
use crate::server::participant::{InstalledParticipantService, ProductionParticipantHandler};

pub use super::services_cluster::ChannelCluster;

/// Registry of custom conversation responders, keyed by conversation subject.
///
/// A registered [`ParticipantBehaviour`] becomes the participant for any
/// conversation opened on its subject; subjects with no entry fall back to the
/// built-in [`EchoBehaviour`].
type ResponderRegistry = HashMap<String, Arc<dyn ParticipantBehaviour>>;

/// Marker for resources retained by a connection process until unsubscribe.
pub trait SubscriptionResource: std::fmt::Debug + Send {
    /// Releases the library subscription resource.
    ///
    /// # Errors
    /// Returns [`ServerError`] when the liminal library reports an unsubscribe failure.
    fn unsubscribe(self: Box<Self>) -> Result<(), ServerError>;

    /// Attempts to pull the next delivered envelope from the wrapped library
    /// subscription without blocking.
    ///
    /// Returns `None` when the subscriber inbox is empty (or momentarily
    /// unavailable): the connection process is the delivery pump, so a transient
    /// empty read is simply "nothing to deliver this slice", never an error.
    fn try_next(&mut self) -> Option<liminal::envelope::Envelope>;

    /// Non-consuming availability query for the post-arm race barrier.
    fn has_pending(&self) -> bool;

    /// Whether an overflow has marked this subscription for shedding (§5). The
    /// delivery pump sheds an overflowed subscription with a typed error frame.
    /// Defaulted to `false`: a resource with no bounded inbox never overflows.
    fn is_overflowed(&self) -> bool {
        false
    }
}

/// Library subscription resource owned by a single connection process.
#[derive(Debug)]
pub struct ConnectionSubscription {
    id: u64,
    /// Client-chosen application stream the server delivers this subscription's
    /// messages on (echoed on `SubscribeAck`, carried on every `Deliver`). Set by
    /// the connection process from the `Subscribe` frame before the subscription is
    /// stored; `0` only while momentarily unset during construction.
    stream_id: u32,
    selected_schema: ProtocolSchemaId,
    resource: Box<dyn SubscriptionResource>,
}

impl ConnectionSubscription {
    /// Creates an owned subscription resource for one connection process.
    #[must_use]
    pub fn new(
        id: u64,
        selected_schema: ProtocolSchemaId,
        resource: Box<dyn SubscriptionResource>,
    ) -> Self {
        Self {
            id,
            stream_id: 0,
            selected_schema,
            resource,
        }
    }

    /// Returns the protocol subscription id.
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }

    /// Records the client-chosen delivery stream id for this subscription.
    pub(super) const fn set_stream_id(&mut self, stream_id: u32) {
        self.stream_id = stream_id;
    }

    /// Returns the client-chosen application stream id deliveries ride on.
    #[must_use]
    pub(super) const fn stream_id(&self) -> u32 {
        self.stream_id
    }

    /// Returns the schema selected for this subscription stream.
    #[must_use]
    pub const fn selected_schema(&self) -> ProtocolSchemaId {
        self.selected_schema
    }

    /// Attempts to pull the next delivered envelope without blocking.
    pub(super) fn try_next(&mut self) -> Option<liminal::envelope::Envelope> {
        self.resource.try_next()
    }

    pub(super) fn has_pending(&self) -> bool {
        self.resource.has_pending()
    }

    /// Whether this subscription has been shed by an inbox overflow (§5).
    pub(super) fn is_overflowed(&self) -> bool {
        self.resource.is_overflowed()
    }

    pub(super) fn unsubscribe(self) -> Result<(), ServerError> {
        self.resource.unsubscribe()
    }
}

/// Outcome of a server publish.
///
/// Carries the assigned message id plus a genuine delivery ack (`delivered` = the
/// message was accepted by at least one live subscriber on this publish, after any
/// dedup-on-delivery suppression).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublishOutcome {
    /// Monotonic message id assigned to the accepted publish.
    pub message_id: u64,
    /// Whether the message was genuinely delivered to a subscriber. `false` means
    /// the publish was accepted but reached no subscriber (empty channel) or was
    /// a duplicate suppressed by dedup-on-delivery.
    pub delivered: bool,
}

/// Operations that adapt wire frames to liminal library calls.
pub trait ConnectionServices: std::fmt::Debug + Send + Sync {
    /// Returns the complete participant service installed on this adapter.
    ///
    /// `None` keeps participant capability disabled even when the adapter owns a
    /// durable store for unrelated channel traffic. The returned token is
    /// server-sealed and atomically carries declared semantics plus durability,
    /// making a handler-without-store activation impossible by construction.
    fn participant_service(&self) -> Option<InstalledParticipantService> {
        None
    }

    /// Delegates a publish request to the liminal library.
    ///
    /// `idempotency_key`, when `Some`, drives dedup-on-delivery: a re-publish with
    /// the same key is delivered to subscribers at most once. The returned
    /// [`PublishOutcome`] carries the genuine delivery ack.
    ///
    /// # Errors
    /// Returns [`ServerError`] when the liminal publish operation fails.
    fn publish(
        &self,
        channel: &str,
        envelope: &MessageEnvelope,
        idempotency_key: Option<&str>,
    ) -> Result<PublishOutcome, ServerError>;

    /// Delegates a subscribe request to the liminal library.
    ///
    /// `install`, when `Some`, carries the connection's §5 shared inbox byte
    /// budget, per-inbox fairness cap, and R3 wake notifier. The implementation
    /// MUST install it on the subscription's inbox BEFORE the registration is
    /// published to the channel actor (i.e. before any envelope can be
    /// delivered), so no envelope is ever admitted uncharged, past the depth
    /// cap, or without a wake. Implementations with no real inbox (test
    /// stand-ins, capability-scoped profiles that refuse subscribe) may ignore
    /// it.
    ///
    /// # Errors
    /// Returns [`ServerError`] when the liminal subscribe operation fails.
    fn subscribe(
        &self,
        channel: &str,
        accepted_schemas: &[ProtocolSchemaId],
        install: Option<liminal::channel::InboxInstall>,
    ) -> Result<ConnectionSubscription, ServerError>;

    /// Delegates unsubscribe to the liminal library.
    ///
    /// # Errors
    /// Returns [`ServerError`] when the liminal unsubscribe operation fails.
    fn unsubscribe(&self, subscription: ConnectionSubscription) -> Result<(), ServerError>;

    /// Delegates conversation open to the liminal library.
    ///
    /// # Errors
    /// Returns [`ServerError`] when the liminal conversation open operation fails.
    fn open_conversation(
        &self,
        conversation_id: u64,
        subject: &str,
    ) -> Result<ConnectionConversation, ServerError>;

    /// Delegates a conversation message to the liminal library.
    ///
    /// # Errors
    /// Returns [`ServerError`] when the liminal conversation message operation fails.
    fn conversation_message(
        &self,
        conversation: &ConnectionConversation,
        envelope: &MessageEnvelope,
    ) -> Result<(), ServerError>;

    /// Delegates conversation close to the liminal library.
    ///
    /// # Errors
    /// Returns [`ServerError`] when the liminal conversation close operation fails.
    fn close_conversation(&self, conversation: ConnectionConversation) -> Result<(), ServerError>;

    /// Flushes durable channel state through the liminal library boundary.
    ///
    /// # Errors
    /// Returns [`ServerError`] when the liminal channel flush operation fails.
    fn flush_durable_state(&self) -> Result<(), ServerError>;

    /// Whether this adapter backs ordinary channel and conversation operations.
    ///
    /// The default is `true` — the full-service adapter serves publish, subscribe,
    /// and conversation frames, so full mode is byte-for-byte unchanged. The
    /// capability-scoped worker front door overrides this to `false`, letting
    /// [`super::apply`] reject the channel/conversation frames it short-circuits on
    /// empty connection state (`Unsubscribe`, `ConversationMessage`,
    /// `ConversationClose`) with a typed error frame instead of silently swallowing
    /// an operation for a resource that could never have been created in this
    /// profile. Frames that always reach a service method (`Publish`, `Subscribe`,
    /// `ConversationOpen`) are rejected by the front door's own method bodies and do
    /// not consult this flag.
    fn supports_channel_operations(&self) -> bool {
        true
    }
}

/// Default adapter from server wire frames to liminal channel/conversation APIs.
#[derive(Debug)]
pub struct LiminalConnectionServices {
    channels: HashMap<String, ConfiguredChannel>,
    cluster: ChannelCluster,
    durable_store: Arc<dyn DurableStore>,
    /// Complete participant service, installed only when semantic lifecycle
    /// handling and its durable aggregate store are both ready.
    participant_service: Option<InstalledParticipantService>,
    /// In-memory (haematite-backed) dedup cache for dedup-on-delivery. Keyed by
    /// the per-message idempotency key carried on the publish frame; a duplicate
    /// key is suppressed before fan-out so a subscriber receives it at most once.
    /// Not persisted across restarts (13-L1 scope; durable dedup is deferred).
    dedup: DedupCache,
    conversation_supervisor: Arc<ConversationSupervisor>,
    /// Registered custom conversation responders, keyed by conversation subject.
    ///
    /// When a conversation is opened (`open_conversation`), the subject is looked
    /// up here: a registered [`ParticipantBehaviour`] becomes the conversation's
    /// participant; with no registration the conversation falls back to the
    /// built-in [`EchoBehaviour`], preserving the original echo semantics exactly.
    /// This is the seam aion #13 plugs a remote worker responder into. Interior
    /// mutability is required because the services are shared behind `&self`.
    responders: Mutex<ResponderRegistry>,
    next_message_id: AtomicU64,
    next_subscription_id: AtomicU64,
}

impl LiminalConnectionServices {
    /// Builds library-backed services from validated server configuration.
    ///
    /// Durable-mode channels are backed by a shared haematite event store so
    /// their publishes are persisted and survive the graceful-shutdown flush;
    /// ephemeral channels carry no store.
    ///
    /// Full-only: a config selecting the worker-front-door profile is rejected at
    /// entry (before any store is built), so this constructor can never build full
    /// services for a profile that forbids them. Profile-aware callers go through
    /// [`build_connection_services`] instead.
    ///
    /// # Errors
    /// Returns [`ServerError`] when the config selects a non-full profile or a
    /// configured channel cannot be initialized.
    pub fn from_config(config: &ServerConfig) -> Result<Self, ServerError> {
        require_full_profile(config)?;
        let store = ProductionSubsystems.durable_store(config.persistence_path.as_deref())?;
        Self::from_config_with_store_via(config, store, &ProductionSubsystems)
    }

    /// Builds services over a caller-provided durable store.
    ///
    /// Used by tests that need to inspect persisted state through the same store
    /// handle the durable channels write to.
    ///
    /// Full-only: rejects a worker-front-door profile at entry, exactly like
    /// [`Self::from_config`].
    ///
    /// # Errors
    /// Returns [`ServerError`] when the config selects a non-full profile or a
    /// configured channel cannot be initialized.
    pub fn from_config_with_store(
        config: &ServerConfig,
        durable_store: Arc<dyn DurableStore>,
    ) -> Result<Self, ServerError> {
        require_full_profile(config)?;
        Self::from_config_with_store_via(config, durable_store, &ProductionSubsystems)
    }

    /// [`Self::from_config_with_store`] with the subsystem factory injected.
    ///
    /// The channel supervisor and conversation supervisor are constructed ONLY
    /// through `subsystems` — there is no direct constructor call in this body —
    /// so a factory that records as a side effect of constructing cannot have its
    /// recording omitted (§9 D2 seam census, record-by-construction). No profile
    /// check here: the caller (the public wrapper or the profile dispatch in
    /// [`build_connection_services`]) has already established the full profile.
    fn from_config_with_store_via(
        config: &ServerConfig,
        durable_store: Arc<dyn DurableStore>,
        subsystems: &dyn SubsystemFactory,
    ) -> Result<Self, ServerError> {
        // Build ONE shared channel supervisor for the whole server. When a
        // [cluster] section is present it is distribution-enabled, so every
        // channel actor and subscriber shares the clustered scheduler the cluster
        // attaches its process-group transport to (SRV-005, Constraint B).
        let cluster = subsystems.channel_cluster(config.cluster.as_ref())?;
        let mut channels = HashMap::new();
        for channel in &config.channels {
            // Resolve the channel's real JSON Schema (loaded from `schema_ref`
            // during config validation) or the permissive empty schema when the
            // channel declared none. The protocol schema id advertised at
            // subscribe time is derived from the SAME schema bytes so an SDK
            // deriving ids from schema bytes converges on it.
            let resolved = resolve_channel_schema(channel);
            let schema =
                Schema::new(resolved.document).map_err(|error| ServerError::ConfigValidation {
                    message: format!("failed to initialize channel '{}': {error}", channel.name),
                })?;
            let channel_config = if channel.durable {
                ChannelConfig::new(channel.name.clone(), schema, ChannelMode::Durable)
            } else {
                ChannelConfig::new(channel.name.clone(), schema, ChannelMode::Ephemeral)
            };
            let handle = if channel.durable {
                ChannelHandle::new_durable_with_supervisor(
                    channel_config,
                    Arc::clone(&durable_store),
                    cluster.supervisor().clone(),
                )
                .map_err(|error| ServerError::ConfigValidation {
                    message: format!(
                        "failed to initialize durable channel '{}': {error}",
                        channel.name
                    ),
                })?
            } else {
                ChannelHandle::with_supervisor(channel_config, cluster.supervisor().clone())
            };
            channels.insert(
                channel.name.clone(),
                ConfiguredChannel {
                    handle,
                    protocol_schema: resolved.protocol_id,
                },
            );
        }
        let conversation_supervisor = subsystems.conversation_supervisor()?;
        let dedup = DedupCache::new(Arc::clone(&durable_store), DELIVERY_DEDUP_NAMESPACE);
        // Production participant activation (LP gap closure, Part B): the
        // deployment's [participant] section installs the ONE production
        // semantic handler, sealed together with the same durable store the
        // conversation logs live in, under the configured wire-frame limit.
        // No section, no service — the capability bit stays off and the
        // connection path is byte-identical to the pre-activation build.
        let participant_service = config
            .participant
            .as_ref()
            .map(|participant| {
                let handler =
                    ProductionParticipantHandler::new(Arc::clone(&durable_store), *participant)
                        .map_err(|error| ServerError::ParticipantStartupRestore {
                            message: error.to_string(),
                        })?;
                InstalledParticipantService::new(
                    Arc::new(handler),
                    Arc::clone(&durable_store),
                    participant.wire_frame_limit,
                )
                .map_err(|error| ServerError::ConfigValidation {
                    message: format!(
                        "participant.wire_frame_limit: {} is below the protocol's minimum \
                         complete participant frame ({error:?})",
                        participant.wire_frame_limit
                    ),
                })
            })
            .transpose()?;
        Ok(Self {
            channels,
            cluster,
            durable_store,
            participant_service,
            dedup,
            conversation_supervisor,
            responders: Mutex::new(HashMap::new()),
            next_message_id: AtomicU64::new(1),
            next_subscription_id: AtomicU64::new(1),
        })
    }

    /// Builds services with no configured channels.
    ///
    /// # Errors
    /// Returns [`ServerError`] when the conversation supervisor scheduler cannot start.
    pub fn empty() -> Result<Self, ServerError> {
        let conversation_supervisor = ProductionSubsystems.conversation_supervisor()?;
        let durable_store = build_durable_store(None)?;
        let dedup = DedupCache::new(Arc::clone(&durable_store), DELIVERY_DEDUP_NAMESPACE);
        Ok(Self {
            channels: HashMap::new(),
            cluster: build_channel_cluster(None)?,
            durable_store,
            participant_service: None,
            dedup,
            conversation_supervisor,
            responders: Mutex::new(HashMap::new()),
            next_message_id: AtomicU64::new(1),
            next_subscription_id: AtomicU64::new(1),
        })
    }

    /// The shared channel supervisor + cluster resolver backing this service.
    ///
    /// The server runtime uses this to attach the cluster to the channel
    /// supervisor's clustered scheduler (SRV-005).
    #[must_use]
    pub const fn channel_cluster(&self) -> &ChannelCluster {
        &self.cluster
    }

    /// Returns the shared durable store backing this service's durable channels.
    #[must_use]
    pub fn durable_store(&self) -> Arc<dyn DurableStore> {
        Arc::clone(&self.durable_store)
    }

    /// Installs a complete participant bundle in full-service supervisor tests.
    ///
    /// Production full services intentionally stay disabled until a concrete
    /// lifecycle handler exists. This consuming test builder exercises the real
    /// supervisor activation path without allowing an already-shared adapter to
    /// change capability posture.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_participant_service(
        mut self,
        participant_service: InstalledParticipantService,
    ) -> Self {
        self.participant_service = Some(participant_service);
        self
    }

    /// Returns the conversation supervisor backing supervised conversations.
    ///
    /// Tests use this to reach the underlying beamr scheduler so they can spawn
    /// or terminate participant processes and exercise crash detection.
    #[must_use]
    pub fn conversation_supervisor(&self) -> Arc<ConversationSupervisor> {
        Arc::clone(&self.conversation_supervisor)
    }

    /// Registers a custom conversation responder for a routing `subject`.
    ///
    /// When a conversation is later opened with this exact `subject`, its
    /// participant runs `behaviour` instead of the built-in [`EchoBehaviour`].
    /// The responder is spawned and supervised identically to the echo
    /// participant — a real linked beamr process with the same crash-detection
    /// semantics — so this exposes the responder seam without changing how
    /// participants run. Registering a subject that already has a responder
    /// replaces it; the previous behaviour is returned.
    ///
    /// This is the liminal-side seam aion #13 plugs a remote worker into: it
    /// registers a responder that forwards each request to the worker and routes
    /// the worker's reply back through the conversation. Subjects with no
    /// registration keep echoing, so existing callers are unaffected.
    ///
    /// # Errors
    /// Returns [`ServerError`] when the responder registry lock is poisoned.
    pub fn register_responder(
        &self,
        subject: impl Into<String>,
        behaviour: Arc<dyn ParticipantBehaviour>,
    ) -> Result<Option<Arc<dyn ParticipantBehaviour>>, ServerError> {
        let mut responders = self.lock_responders()?;
        Ok(responders.insert(subject.into(), behaviour))
    }

    /// Removes the custom responder registered for `subject`, if any.
    ///
    /// After removal the subject reverts to the built-in [`EchoBehaviour`] on the
    /// next [`Self::open_conversation`]. Returns the removed behaviour when one
    /// was registered.
    ///
    /// # Errors
    /// Returns [`ServerError`] when the responder registry lock is poisoned.
    pub fn unregister_responder(
        &self,
        subject: &str,
    ) -> Result<Option<Arc<dyn ParticipantBehaviour>>, ServerError> {
        let mut responders = self.lock_responders()?;
        Ok(responders.remove(subject))
    }

    /// Resolves the responder behaviour for `subject`: the registered custom
    /// responder when present, otherwise the built-in [`EchoBehaviour`].
    ///
    /// This is the single routing decision behind the seam — registered-or-echo —
    /// so the fallback is identical to the original hard-wired echo path.
    fn responder_for(&self, subject: &str) -> Result<Arc<dyn ParticipantBehaviour>, ServerError> {
        let responders = self.lock_responders()?;
        Ok(responders.get(subject).map_or_else(
            || Arc::new(EchoBehaviour) as Arc<dyn ParticipantBehaviour>,
            Arc::clone,
        ))
    }

    /// Locks the responder registry, mapping a poisoned lock to a [`ServerError`]
    /// rather than panicking (the workspace denies `unwrap`/`expect`/`panic`).
    fn lock_responders(&self) -> Result<std::sync::MutexGuard<'_, ResponderRegistry>, ServerError> {
        self.responders
            .lock()
            .map_err(|_poisoned| ServerError::ListenerAccept {
                message: "responder registry lock poisoned".to_owned(),
            })
    }

    /// Subscribes to a configured channel and returns the raw library
    /// subscription handle so a test can drain the subscriber inbox directly and
    /// observe exactly which messages reached a subscriber.
    #[cfg(test)]
    pub(crate) fn subscribe_handle_for_test(
        &self,
        channel: &str,
    ) -> Result<liminal::channel::SubscriptionHandle, ServerError> {
        let configured = self
            .channels
            .get(channel)
            .ok_or_else(|| ServerError::ListenerAccept {
                message: format!("channel '{channel}' is not configured"),
            })?;
        configured
            .handle
            .subscribe()
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("liminal subscribe failed for channel '{channel}': {error}"),
            })
    }

    /// Claims the delivery right for an idempotency key.
    ///
    /// Returns `Ok(true)` when this is the first publish for the key (the caller
    /// may deliver), and `Ok(false)` when the key was already claimed/completed (a
    /// duplicate the caller must suppress). The dedup cache is driven synchronously
    /// over the in-memory haematite store via the durable bridge.
    fn claim_delivery(&self, key: &str) -> Result<bool, ServerError> {
        let decision = block_on(self.dedup.claim_or_get(key, dedup_timestamp_millis()))
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("dedup bridge failed for key '{key}': {error}"),
            })?
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("dedup claim failed for key '{key}': {error}"),
            })?;
        Ok(matches!(decision, DedupDecision::Claimed))
    }

    /// Releases a dangling in-flight dedup claim after a failed delivery.
    ///
    /// Best-effort: a release failure cannot mask the original publish error, so
    /// this returns nothing and logs at `error` level instead of surfacing. It is
    /// never silent — the leak (a permanently suppressed key) must be observable.
    /// `release_claim` itself never clobbers a stored receipt, so calling it on the
    /// failure path is safe even if a concurrent completion raced ahead.
    fn release_claim(&self, key: &str) {
        match block_on(self.dedup.release_claim(key)) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::error!(
                    idempotency_key = key,
                    %error,
                    "failed to release dedup claim after publish failure; key may stay suppressed"
                );
            }
            Err(error) => {
                tracing::error!(
                    idempotency_key = key,
                    %error,
                    "dedup release bridge failed after publish failure; key may stay suppressed"
                );
            }
        }
    }
}

/// Returns the current epoch-millis timestamp used as the dedup entry anchor.
///
/// A clock error before the Unix epoch yields `0`: the timestamp is only a TTL
/// anchor for the in-memory cache and a zero anchor never breaks the at-most-once
/// claim semantics, so this avoids surfacing a clock fault on the publish path.
fn dedup_timestamp_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

/// Default shard count for an on-disk durable store.
///
/// Haematite routes keys across this many single-threaded shard actors; a small
/// power of two gives parallelism across cursors/streams without spawning an
/// actor per core. The value is fixed (haematite has no silent default) and not
/// yet surfaced in server config.
const DEFAULT_SHARD_COUNT: usize = 8;

/// Namespace prefix for the dedup-on-delivery cache streams. Keeps delivery dedup
/// keys from colliding with any other haematite streams in the shared store.
const DELIVERY_DEDUP_NAMESPACE: &str = "liminal:delivery-dedup";

/// Beamr-scheduler-owning subsystems the full-service construction path builds
/// beyond the connection supervisor's own scheduler. The §9 D2 seam census counts
/// these: the worker-front-door profile must construct NONE of them. Test-gated:
/// this is the recording vocabulary of the gate's instrument, not production state.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum SchedulerSubsystem {
    /// The shared channel supervisor (its own beamr scheduler).
    ChannelSupervisor,
    /// The conversation supervisor (its own beamr scheduler).
    ConversationSupervisor,
    /// The haematite store's database (its shard-actor scheduler).
    HaematiteStore,
}

/// Constructor seam for every scheduler-owning subsystem (`SchedulerSubsystem`)
/// the profile-aware construction path can create.
///
/// These methods are the ONLY route through which [`build_connection_services`]
/// and the [`LiminalConnectionServices`] config constructors reach
/// `build_channel_cluster`, `ConversationSupervisor::new`, and the durable-store
/// constructors — no direct constructor call exists in those bodies. The §9 D2
/// gate therefore injects a factory that records as a side effect of
/// constructing (the D3 store-seam ownership move applied to schedulers): a
/// recording cannot be omitted without also failing to construct the subsystem,
/// which closes the "hand-placed census call beside the constructor" gap where a
/// future subsystem could be built without its courtesy call.
pub(super) trait SubsystemFactory {
    /// Constructs the shared channel supervisor + cluster resolver (a beamr
    /// scheduler).
    ///
    /// # Errors
    /// Returns [`ServerError`] when the channel supervisor scheduler cannot start.
    fn channel_cluster(
        &self,
        cluster_config: Option<&ClusterConfig>,
    ) -> Result<ChannelCluster, ServerError>;

    /// Constructs the conversation supervisor (a beamr scheduler).
    ///
    /// # Errors
    /// Returns [`ServerError`] when the conversation supervisor scheduler cannot
    /// start.
    fn conversation_supervisor(&self) -> Result<Arc<ConversationSupervisor>, ServerError>;

    /// Constructs the durable store (haematite's shard-actor scheduler):
    /// persistent under `persistence_path`, self-owning ephemeral otherwise.
    ///
    /// # Errors
    /// Returns [`ServerError`] when the store cannot be opened.
    fn durable_store(
        &self,
        persistence_path: Option<&Path>,
    ) -> Result<Arc<dyn DurableStore>, ServerError>;
}

/// The production factory: the real constructors, recording nothing.
pub(super) struct ProductionSubsystems;

impl SubsystemFactory for ProductionSubsystems {
    fn channel_cluster(
        &self,
        cluster_config: Option<&ClusterConfig>,
    ) -> Result<ChannelCluster, ServerError> {
        build_channel_cluster(cluster_config)
    }

    fn conversation_supervisor(&self) -> Result<Arc<ConversationSupervisor>, ServerError> {
        Ok(Arc::new(ConversationSupervisor::new().map_err(
            |error| ServerError::ConfigValidation {
                message: format!("failed to start conversation supervisor: {error}"),
            },
        )?))
    }

    fn durable_store(
        &self,
        persistence_path: Option<&Path>,
    ) -> Result<Arc<dyn DurableStore>, ServerError> {
        build_durable_store(persistence_path)
    }
}

/// Test-only recording implementation of [`SubsystemFactory`] for the §9 D2
/// construction gate. Lives here (not in a test module's private scope) so the
/// supervisor-level gate test reuses the same instrument.
#[cfg(test)]
#[allow(clippy::expect_used)]
pub(super) mod subsystem_census {
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use liminal::conversation::ConversationSupervisor;
    use liminal::durability::{DurableStore, open_ephemeral_rooted};

    use super::{
        ChannelCluster, DEFAULT_SHARD_COUNT, ProductionSubsystems, SchedulerSubsystem,
        SubsystemFactory, build_durable_store_with,
    };
    use crate::ServerError;
    use crate::config::types::ClusterConfig;

    /// Records each [`SchedulerSubsystem`] AS A SIDE EFFECT of constructing it,
    /// then hands back the production-constructed subsystem (with ephemeral
    /// stores rooted in an isolated directory, so the fs half of the gate is a
    /// real negative assertion). Because [`SubsystemFactory`] is the only route
    /// the profile-aware construction path has to these constructors, a
    /// recording cannot be omitted without also failing to construct — the
    /// record-by-construction guarantee.
    pub struct RecordingSubsystems {
        census: Mutex<Vec<SchedulerSubsystem>>,
        ephemeral_root: PathBuf,
    }

    impl RecordingSubsystems {
        /// A recording factory whose ephemeral stores live under `ephemeral_root`.
        pub fn rooted(ephemeral_root: &Path) -> Self {
            Self {
                census: Mutex::new(Vec::new()),
                ephemeral_root: ephemeral_root.to_path_buf(),
            }
        }

        /// The recorded construction census, sorted for order-independent
        /// comparison.
        pub fn recorded(&self) -> Vec<SchedulerSubsystem> {
            let mut recorded = self
                .census
                .lock()
                .expect("subsystem census lock is never poisoned in tests")
                .clone();
            recorded.sort();
            recorded
        }

        fn record(&self, subsystem: SchedulerSubsystem) {
            self.census
                .lock()
                .expect("subsystem census lock is never poisoned in tests")
                .push(subsystem);
        }
    }

    impl SubsystemFactory for RecordingSubsystems {
        fn channel_cluster(
            &self,
            cluster_config: Option<&ClusterConfig>,
        ) -> Result<ChannelCluster, ServerError> {
            let cluster = ProductionSubsystems.channel_cluster(cluster_config)?;
            self.record(SchedulerSubsystem::ChannelSupervisor);
            Ok(cluster)
        }

        fn conversation_supervisor(&self) -> Result<Arc<ConversationSupervisor>, ServerError> {
            let supervisor = ProductionSubsystems.conversation_supervisor()?;
            self.record(SchedulerSubsystem::ConversationSupervisor);
            Ok(supervisor)
        }

        fn durable_store(
            &self,
            persistence_path: Option<&Path>,
        ) -> Result<Arc<dyn DurableStore>, ServerError> {
            let store = build_durable_store_with(persistence_path, || {
                open_ephemeral_rooted(&self.ephemeral_root, DEFAULT_SHARD_COUNT)
            })?;
            self.record(SchedulerSubsystem::HaematiteStore);
            Ok(store)
        }
    }
}

/// Full-only constructor guard: rejects a config whose profile is not `Full`.
///
/// [`LiminalConnectionServices`]' config-based constructors call this at entry so
/// the full service stack can never be built for a worker-front-door config —
/// profile enforcement holds on every public construction path, not only the
/// file-loading pipeline.
fn require_full_profile(config: &ServerConfig) -> Result<(), ServerError> {
    match config.services.profile()? {
        ServiceProfile::Full => Ok(()),
        ServiceProfile::WorkerFrontDoor => Err(ServerError::ConfigValidation {
            message: format!(
                "services.profile: \"{}\" cannot construct the full LiminalConnectionServices; \
                 build profile-selected services via build_connection_services",
                ServiceProfile::WORKER_FRONT_DOOR
            ),
        }),
    }
}

/// Builds the connection-services adapter selected by `config`'s service profile.
///
/// `Full` builds [`LiminalConnectionServices`] (channels, conversations, durable
/// store, dedup cache) exactly as today. `WorkerFrontDoor` builds
/// [`WorkerFrontDoorServices`], which constructs none of that machinery. This is the
/// single profile-dispatch authority: [`super::supervisor::ConnectionSupervisor`]'s
/// config constructor and the standalone runtime's worker arm both route through it
/// (the runtime's full arm stays on the explicit
/// [`LiminalConnectionServices::from_config`] path because it also needs the shared
/// channel cluster, which the trait object does not expose — that path is guarded
/// full-only at entry).
///
/// # Errors
/// Returns [`ServerError`] when the selected adapter cannot be constructed, the
/// configured profile value is not recognised, or the worker-front-door profile is
/// combined with full-only config fields.
pub fn build_connection_services(
    config: &ServerConfig,
) -> Result<Arc<dyn ConnectionServices>, ServerError> {
    build_connection_services_via(config, &ProductionSubsystems)
}

/// [`build_connection_services`] with the subsystem factory injected — the §9 D2
/// gate seam, both halves at once.
///
/// Every scheduler-owning subsystem the `Full` branch creates is constructed
/// through `subsystems` and nowhere else, so a recording factory observes exactly
/// what was built (thread half, record-by-construction); the gate's factory also
/// roots its ephemeral stores in an isolated directory, so "the root stays empty
/// on the worker branch" is a real negative assertion (fs half, the D3 pattern).
/// The `WorkerFrontDoor` branch never touches the factory — a regression that gave
/// the front door any subsystem would both record in the census and land a store
/// directory in the injected root.
///
/// The worker branch re-runs the cross-field checks here, not only in file-loading
/// validation, so a directly-constructed config cannot smuggle full-only machinery
/// past the profile.
pub(super) fn build_connection_services_via(
    config: &ServerConfig,
    subsystems: &dyn SubsystemFactory,
) -> Result<Arc<dyn ConnectionServices>, ServerError> {
    match config.services.profile()? {
        ServiceProfile::Full => {
            let store = subsystems.durable_store(config.persistence_path.as_deref())?;
            Ok(Arc::new(
                LiminalConnectionServices::from_config_with_store_via(config, store, subsystems)?,
            ))
        }
        ServiceProfile::WorkerFrontDoor => {
            let errors = crate::config::validation::worker_front_door_field_errors(config);
            if !errors.is_empty() {
                return Err(ServerError::ConfigValidation {
                    message: errors.join("; "),
                });
            }
            Ok(Arc::new(WorkerFrontDoorServices::new()))
        }
    }
}

/// Builds the haematite-backed durable store.
///
/// When `persistence_path` is `Some`, the database lives there and survives
/// process restarts: an existing database directory is reopened, a fresh one is
/// created. When it is `None` (no durable path configured, or the channel-free
/// `empty()` services used by tests), a self-owning ephemeral store is opened
/// instead: its temporary directory is created and removed by the store itself
/// (D3), so it leaves no residue once the last store handle drops. The two paths
/// return distinct concrete stores on purpose — only the ephemeral one carries a
/// directory guard; the persistent path is untouched.
fn build_durable_store(
    persistence_path: Option<&Path>,
) -> Result<Arc<dyn DurableStore>, ServerError> {
    build_durable_store_with(persistence_path, || open_ephemeral(DEFAULT_SHARD_COUNT))
}

/// [`build_durable_store`] with the ephemeral factory injected.
///
/// The split exists for the D3 construction gates: the SAME branch logic runs
/// in production and tests, and only the factory closure differs — tests root
/// the ephemeral store in an isolated directory (via liminal's test-gated
/// rooted factory) so "no ephemeral directory was created" is a real assertion
/// rather than a scan of the shared system temp dir.
fn build_durable_store_with(
    persistence_path: Option<&Path>,
    make_ephemeral: impl FnOnce() -> Result<EphemeralHaematiteStore, DurabilityError>,
) -> Result<Arc<dyn DurableStore>, ServerError> {
    let Some(path) = persistence_path else {
        let store = make_ephemeral().map_err(|error| ServerError::ConfigValidation {
            message: format!("failed to open ephemeral durable store: {error}"),
        })?;
        return Ok(Arc::new(store));
    };
    let data_dir = path.join("durability");
    let database = open_or_create_database(&data_dir)?;
    let event_store = EventStore::new(database);
    Ok(Arc::new(HaematiteStore::new(Arc::new(event_store))))
}

/// Opens an existing haematite database at `data_dir`, or creates one.
fn open_or_create_database(data_dir: &Path) -> Result<Database, ServerError> {
    let config_file = data_dir.join("config.json");
    let result = if config_file.exists() {
        Database::open(data_dir)
    } else {
        Database::create(DatabaseConfig {
            data_dir: data_dir.to_path_buf(),
            shard_count: DEFAULT_SHARD_COUNT,
            distributed: None,
            executor_threads: None,
        })
    };
    result.map_err(|error| ServerError::ConfigValidation {
        message: format!(
            "failed to open durable store at {}: {error}",
            data_dir.display()
        ),
    })
}

impl ConnectionServices for LiminalConnectionServices {
    fn participant_service(&self) -> Option<InstalledParticipantService> {
        self.participant_service.clone()
    }

    fn publish(
        &self,
        channel: &str,
        envelope: &MessageEnvelope,
        idempotency_key: Option<&str>,
    ) -> Result<PublishOutcome, ServerError> {
        let handle = self
            .channels
            .get(channel)
            .map(|configured| configured.handle.clone())
            .ok_or_else(|| ServerError::ListenerAccept {
                message: format!("channel '{channel}' is not configured"),
            })?;

        // Dedup-on-delivery: a publish carrying an idempotency key is delivered to
        // subscribers AT MOST ONCE across re-publishes of the same key. Only a
        // fresh `Claimed` decision proceeds to fan-out; a `Completed`/`InFlight`
        // decision is a duplicate and is suppressed (no second delivery), which is
        // the at-most-once guarantee the aion outbox relies on.
        if let Some(key) = idempotency_key {
            if !self.claim_delivery(key)? {
                // A dedup-suppressed re-publish is still an accepted publish (it is
                // assigned a message id), but it reaches no subscriber, so it counts
                // toward publishes and not deliveries.
                crate::metrics::publish_accepted();
                return Ok(PublishOutcome {
                    message_id: self.next_message_id.fetch_add(1, Ordering::Relaxed),
                    delivered: false,
                });
            }
        }

        let delivery = handle.publish_with_delivery(
            &envelope.payload,
            liminal::envelope::PublisherId::default(),
            None,
        );
        let delivery = match delivery {
            Ok(delivery) => delivery,
            Err(error) => {
                // The claim above appended an `InFlight` entry but the delivery
                // failed before `complete_receipt` could run. Release the claim so
                // the key is re-claimable; otherwise every re-publish would see
                // `InFlight` and be suppressed forever. Best-effort: surface the
                // ORIGINAL publish error regardless, but never swallow a release
                // failure silently (it leaves the leak intact).
                if let Some(key) = idempotency_key {
                    self.release_claim(key);
                }
                return Err(ServerError::ListenerAccept {
                    message: format!("liminal publish failed for channel '{channel}': {error}"),
                });
            }
        };

        // Record the dedup completion AFTER a successful claimed delivery so the
        // claim is not left dangling `InFlight` (which would wrongly defer every
        // future duplicate). The receipt body is empty: the dedup contract here
        // only needs presence, not a stored result.
        if let Some(key) = idempotency_key {
            block_on(
                self.dedup
                    .complete_receipt(key, ProcessingReceipt::new(Vec::new())),
            )
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("dedup receipt bridge failed for key '{key}': {error}"),
            })?
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("dedup receipt write failed for key '{key}': {error}"),
            })?;
        }

        // Record the accepted publish and its genuine subscriber deliveries. The
        // delivered count (0 for an empty channel) is the same signal the delivery
        // ack is derived from.
        crate::metrics::publish_accepted();
        let delivered_count = u64::try_from(delivery.delivered_count()).unwrap_or(u64::MAX);
        crate::metrics::deliveries_recorded(delivered_count);

        Ok(PublishOutcome {
            message_id: self.next_message_id.fetch_add(1, Ordering::Relaxed),
            delivered: delivery.is_delivered(),
        })
    }

    fn subscribe(
        &self,
        channel: &str,
        accepted_schemas: &[ProtocolSchemaId],
        install: Option<liminal::channel::InboxInstall>,
    ) -> Result<ConnectionSubscription, ServerError> {
        let configured = self
            .channels
            .get(channel)
            .ok_or_else(|| ServerError::ListenerAccept {
                message: format!("channel '{channel}' is not configured"),
            })?;
        let selected_schema = if accepted_schemas.is_empty() {
            configured.protocol_schema
        } else {
            liminal::protocol::negotiate_schema(configured.protocol_schema, accepted_schemas)
                .map_err(|error| server_error_from_protocol(&error))?
        };
        // `subscribe_with_install` installs the §5 budget/fairness cap and the R3
        // wake notifier on the inbox at construction — strictly before the
        // registration is published to the channel actor — so there is no window
        // in which a publish can land uncharged or without a wake.
        let subscription = install
            .map_or_else(
                || configured.handle.subscribe(),
                |install| configured.handle.subscribe_with_install(install),
            )
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("liminal subscribe failed for channel '{channel}': {error}"),
            })?;
        let id = self.next_subscription_id.fetch_add(1, Ordering::Relaxed);
        Ok(ConnectionSubscription::new(
            id,
            selected_schema,
            Box::new(LiminalSubscriptionResource { subscription }),
        ))
    }

    fn unsubscribe(&self, subscription: ConnectionSubscription) -> Result<(), ServerError> {
        subscription.unsubscribe()
    }

    fn open_conversation(
        &self,
        conversation_id: u64,
        subject: &str,
    ) -> Result<ConnectionConversation, ServerError> {
        // Spawn a REAL participant process (a beamr `NativeHandler` running the
        // resolved responder behaviour) on the conversation supervisor's
        // scheduler, and a supervised conversation actor linked to it. The actor
        // FORWARDS each conversation message to the participant, which genuinely
        // processes it and delivers a reply back. The actor traps the
        // participant's EXIT (a beamr process link), so killing it fires a
        // structural, microsecond-scale crash signal.
        //
        // The responder is chosen by `subject`: a custom responder registered via
        // `register_responder` for this subject, or the built-in `EchoBehaviour`
        // when none is registered. Either way it runs as the SAME supervised,
        // linked participant process — the seam changes WHO responds, not HOW the
        // participant is spawned or supervised.
        let behaviour = self.responder_for(subject)?;
        let (actor, participant) = self
            .conversation_supervisor
            .spawn_with_participant(behaviour, None, ChannelMode::Ephemeral, CrashPolicy::Fail)
            .map_err(|error| ServerError::ListenerAccept {
                message: format!(
                    "failed to spawn supervised conversation {conversation_id} ('{subject}'): {error}"
                ),
            })?;

        // Drive boot to completion so the beamr link to the participant exists
        // before any message is forwarded (link-before-forward), mirroring the
        // ROUTING-004 dispatch pattern.
        actor.pid().map_err(|error| ServerError::ListenerAccept {
            message: format!(
                "failed to boot supervised conversation {conversation_id} ('{subject}'): {error}"
            ),
        })?;

        // Register the structural EXIT notifier BEFORE returning, so a crash that
        // fires the instant a message reaches the participant is never missed.
        // The notifier is woken by the actor's trapped-EXIT handler (event
        // driven), and a crash that already landed is replayed immediately.
        let (exit_tx, exit_rx) = mpsc::sync_channel::<Instant>(1);
        actor
            .notify_on_participant_exit(participant, exit_tx)
            .map_err(|error| ServerError::ListenerAccept {
                message: format!(
                    "failed to arm crash detection for conversation {conversation_id}: {error}"
                ),
            })?;

        Ok(ConnectionConversation::new(Box::new(
            LiminalConversationResource::new(actor, participant, exit_rx),
        )))
    }

    fn conversation_message(
        &self,
        conversation: &ConnectionConversation,
        envelope: &MessageEnvelope,
    ) -> Result<(), ServerError> {
        conversation.message(envelope)
    }

    fn close_conversation(&self, conversation: ConnectionConversation) -> Result<(), ServerError> {
        conversation.close()
    }

    fn flush_durable_state(&self) -> Result<(), ServerError> {
        for (channel_name, configured) in &self.channels {
            if configured.handle.config().mode == ChannelMode::Durable {
                configured
                    .handle
                    .flush()
                    .map_err(|error| ServerError::ShutdownFlush {
                        message: format!(
                            "failed to flush durable channel '{channel_name}': {error}"
                        ),
                    })?;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ConfiguredChannel {
    handle: ChannelHandle,
    protocol_schema: ProtocolSchemaId,
}

#[derive(Debug)]
struct LiminalSubscriptionResource {
    subscription: liminal::channel::SubscriptionHandle,
}

impl SubscriptionResource for LiminalSubscriptionResource {
    fn unsubscribe(self: Box<Self>) -> Result<(), ServerError> {
        drop(self.subscription);
        Ok(())
    }

    fn is_overflowed(&self) -> bool {
        self.subscription.is_overflowed()
    }

    fn has_pending(&self) -> bool {
        self.subscription.has_pending()
    }

    fn try_next(&mut self) -> Option<liminal::envelope::Envelope> {
        match self.subscription.try_next() {
            Ok(envelope) => envelope,
            Err(error) => {
                // A poisoned inbox lock is PERMANENT, not transient: once poisoned it
                // stays poisoned, so every future `try_next` also returns `Err` and this
                // subscription goes silent for the rest of its life — no further
                // deliveries, not "held for the next slice". Poisoning requires a panic
                // while the lock is held, which the workspace lints forbid
                // (no unwrap/expect/panic), so this is an accepted low-probability
                // failure rather than a recoverable one. We keep the connection alive (a
                // single permanently-silent subscription is less harmful than tearing
                // down every other subscription and stream the connection multiplexes)
                // but log loudly so the silence is diagnosable. The log cannot storm:
                // it can only fire after the one panic that poisoned the lock.
                tracing::error!(
                    %error,
                    "subscription inbox lock is poisoned; this subscription is now \
                     permanently silent and will deliver no further messages"
                );
                None
            }
        }
    }
}

pub(super) fn server_error_from_protocol(error: &ProtocolError) -> ServerError {
    ServerError::ListenerAccept {
        message: format!("protocol operation failed: {error}"),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod durable_store_tests {
    use liminal::durability::open_ephemeral_rooted;

    use super::subsystem_census::RecordingSubsystems;
    use super::{
        ConnectionServices, DEFAULT_SHARD_COUNT, LiminalConnectionServices, SchedulerSubsystem,
        build_connection_services, build_connection_services_via, build_durable_store_with,
    };
    use crate::ServerError;
    use crate::config::types::{LimitsConfig, ServerConfig, ServicesConfig};

    /// Counts directory entries under `root`, for the empty/one-dir assertions
    /// on an injected ephemeral root.
    fn entry_count(root: &std::path::Path) -> usize {
        std::fs::read_dir(root)
            .expect("ephemeral root is readable")
            .count()
    }

    /// A minimal channel-free config with the given service `profile`. No channels,
    /// routing, persistence, or cluster — the shape both profiles accept (the full
    /// profile simply builds an empty channel set; the worker-front-door profile
    /// requires exactly this shape).
    fn config_with_profile(profile: &str) -> ServerConfig {
        ServerConfig {
            listen_address: "127.0.0.1:0".parse().expect("valid socket addr"),
            health_listen_address: "127.0.0.1:1".parse().expect("valid socket addr"),
            drain_timeout_ms: 30_000,
            channels: Vec::new(),
            routing_rules: Vec::new(),
            persistence_path: None,
            cluster: None,
            auth: None,
            services: ServicesConfig {
                profile: profile.to_owned(),
            },
            limits: LimitsConfig::default(),
            participant: None,
            websocket: None,
        }
    }

    /// §9 D2 front-door construction gate (fs half): building the worker-front-door
    /// services creates NO haematite store and NO temp dir, while the full profile
    /// over an equally-rooted factory DOES create exactly one store directory.
    ///
    /// The injected root is the only place an ephemeral store directory can appear
    /// (the recording factory roots its stores there), so "the root stays empty on
    /// the front-door branch" is a real negative assertion: a regression that gave
    /// the front door a store would land its directory here and fail this test. The
    /// full-profile arm is the positive control proving the seam genuinely
    /// constructs a store when the profile asks for one.
    #[test]
    fn worker_front_door_builds_no_store_and_no_temp_dir() {
        let front_door_root = tempfile::tempdir().expect("test can create an ephemeral root");
        let full_root = tempfile::tempdir().expect("test can create an ephemeral root");

        let front_door_subsystems = RecordingSubsystems::rooted(front_door_root.path());
        let front_door: std::sync::Arc<dyn ConnectionServices> = build_connection_services_via(
            &config_with_profile("worker-front-door"),
            &front_door_subsystems,
        )
        .expect("worker-front-door services build");
        assert!(
            !front_door.supports_channel_operations(),
            "the worker front door serves no channel operations"
        );
        assert_eq!(
            entry_count(front_door_root.path()),
            0,
            "the worker front door creates no ephemeral store directory (no haematite, no temp dir)"
        );

        let full_subsystems = RecordingSubsystems::rooted(full_root.path());
        let full = build_connection_services_via(&config_with_profile("full"), &full_subsystems)
            .expect("full services build");
        assert!(
            full.supports_channel_operations(),
            "full mode serves channel operations"
        );
        assert_eq!(
            entry_count(full_root.path()),
            1,
            "full mode with no persistence path builds exactly one ephemeral store directory"
        );

        drop(front_door);
        drop(full);
    }

    /// §9 D2 front-door construction gate (thread half — record-by-construction
    /// census): the worker profile constructs NO channel-supervisor,
    /// conversation-supervisor, or haematite scheduler, while the SAME instrument
    /// over the full profile records all three — the positive control proving the
    /// census detects the extra schedulers, so an empty census on the worker branch
    /// is a real observation, not a decoration.
    ///
    /// The instrument's boundary: recording happens INSIDE the [`SubsystemFactory`]
    /// methods that are the profile-aware path's only route to these constructors,
    /// so a recording cannot be silently omitted — a future subsystem added to this
    /// path either goes through the factory (and is recorded by construction) or
    /// bypasses it, which is a code-review-visible structural violation of the
    /// factory seam, not a silently-missing side call. The connection supervisor's
    /// own scheduler is the shared baseline of both profiles and is asserted at the
    /// supervisor level (`supervisor::tests`); an OS-level thread census upgrades
    /// this when the beamr composition lane's scheduler-inventory API (currently on
    /// their branch, not yet consumable from liminal) lands.
    #[test]
    fn worker_profile_census_is_empty_and_full_profile_records_all_schedulers() {
        let worker_root = tempfile::tempdir().expect("test can create an ephemeral root");
        let full_root = tempfile::tempdir().expect("test can create an ephemeral root");

        let worker_subsystems = RecordingSubsystems::rooted(worker_root.path());
        let front_door = build_connection_services_via(
            &config_with_profile("worker-front-door"),
            &worker_subsystems,
        )
        .expect("worker-front-door services build");
        assert_eq!(
            worker_subsystems.recorded(),
            Vec::<SchedulerSubsystem>::new(),
            "the worker front door constructs no scheduler-owning subsystem"
        );

        let full_subsystems = RecordingSubsystems::rooted(full_root.path());
        let full = build_connection_services_via(&config_with_profile("full"), &full_subsystems)
            .expect("full services build");
        assert_eq!(
            full_subsystems.recorded(),
            vec![
                SchedulerSubsystem::ChannelSupervisor,
                SchedulerSubsystem::ConversationSupervisor,
                SchedulerSubsystem::HaematiteStore,
            ],
            "the full profile constructs every scheduler-owning subsystem, once each — \
             the positive control proving the census instrument detects them"
        );

        drop(front_door);
        drop(full);
    }

    /// MAJOR-1 regression: the full-only constructors reject a worker-front-door
    /// config with a typed `ConfigValidation` error AT ENTRY — no full service can
    /// be created through any public config-based constructor under that profile.
    #[test]
    fn full_only_constructors_reject_worker_profile() {
        let config = config_with_profile("worker-front-door");

        let from_config = LiminalConnectionServices::from_config(&config);
        assert!(
            matches!(from_config, Err(ServerError::ConfigValidation { .. })),
            "from_config must reject a worker-front-door profile with ConfigValidation, got {from_config:?}"
        );

        let root = tempfile::tempdir().expect("test can create an ephemeral root");
        let store = open_ephemeral_rooted(root.path(), DEFAULT_SHARD_COUNT)
            .expect("test store for the rejection check builds");
        let from_config_with_store =
            LiminalConnectionServices::from_config_with_store(&config, std::sync::Arc::new(store));
        assert!(
            matches!(
                from_config_with_store,
                Err(ServerError::ConfigValidation { .. })
            ),
            "from_config_with_store must reject a worker-front-door profile with ConfigValidation"
        );
    }

    /// MAJOR-1 regression: the profile-aware factory itself re-runs the
    /// worker-front-door cross-field checks, so a directly-constructed config (one
    /// that never passed file-loading validation) combining the worker profile with
    /// full-only machinery is refused with the same typed `ConfigValidation` errors.
    #[test]
    fn build_connection_services_rejects_worker_profile_with_full_only_fields() {
        let mut config = config_with_profile("worker-front-door");
        config.channels = vec![crate::config::types::ChannelDef {
            name: "orders".to_owned(),
            schema_ref: None,
            durable: false,
            loaded_schema: None,
        }];
        config.persistence_path = Some(std::path::PathBuf::from("/tmp"));

        let result = build_connection_services(&config);
        let Err(ServerError::ConfigValidation { message }) = result else {
            panic!("expected ConfigValidation for worker profile with full-only fields");
        };
        assert!(message.contains("builds no channels"), "got: {message}");
        assert!(
            message.contains("builds no durable store"),
            "got: {message}"
        );
    }

    /// §9 D3 construction gate (persistent half): requesting a *persistent* store
    /// creates its database under the configured path and NO ephemeral directory.
    ///
    /// Exercises `build_durable_store_with` — the same branch logic production
    /// runs — with only the ephemeral factory swapped to root in an isolated
    /// directory. That root is where any ephemeral directory would have to
    /// appear, so "the root stays empty" is a real negative assertion — a
    /// regression that constructs an ephemeral store on the persistent branch
    /// lands its directory here and fails this test.
    #[test]
    fn persistent_store_uses_configured_path_and_creates_no_temp_dir() {
        let home = tempfile::tempdir().expect("test can create a temp dir");
        let ephemeral_root = tempfile::tempdir().expect("test can create an ephemeral root");

        let store = build_durable_store_with(Some(home.path()), || {
            open_ephemeral_rooted(ephemeral_root.path(), DEFAULT_SHARD_COUNT)
        })
        .expect("persistent store builds");

        assert!(
            home.path().join("durability").join("config.json").exists(),
            "the persistent database is created under the configured path"
        );
        assert_eq!(
            entry_count(ephemeral_root.path()),
            0,
            "the persistent branch creates no ephemeral guard directory"
        );

        drop(store);
    }

    /// Pins the wiring seam: the ephemeral (`None`) branch of the shared build
    /// logic goes through the guarded constructor — exactly one directory
    /// appears under the injected root while the store lives, and zero residue
    /// remains after the last handle drops.
    #[test]
    fn ephemeral_store_directory_is_owned_through_the_build_seam() {
        let ephemeral_root = tempfile::tempdir().expect("test can create an ephemeral root");

        let store = build_durable_store_with(None, || {
            open_ephemeral_rooted(ephemeral_root.path(), DEFAULT_SHARD_COUNT)
        })
        .expect("ephemeral store builds");

        assert_eq!(
            entry_count(ephemeral_root.path()),
            1,
            "the ephemeral branch creates exactly one guard directory"
        );

        drop(store);

        assert_eq!(
            entry_count(ephemeral_root.path()),
            0,
            "dropping the last store handle removes the guard directory — zero residue"
        );
    }
}
