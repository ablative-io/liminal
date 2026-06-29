use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
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
    DedupCache, DedupDecision, DurableStore, HaematiteStore, ProcessingReceipt,
};
use liminal::protocol::{MessageEnvelope, ProtocolError, SchemaId as ProtocolSchemaId};

use super::conversation::{ConnectionConversation, LiminalConversationResource};
use super::services_cluster::build_channel_cluster;
use crate::ServerError;
use crate::config::types::ServerConfig;

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
}

/// Library subscription resource owned by a single connection process.
#[derive(Debug)]
pub struct ConnectionSubscription {
    id: u64,
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
            selected_schema,
            resource,
        }
    }

    /// Returns the protocol subscription id.
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }

    /// Returns the schema selected for this subscription stream.
    #[must_use]
    pub const fn selected_schema(&self) -> ProtocolSchemaId {
        self.selected_schema
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
    /// # Errors
    /// Returns [`ServerError`] when the liminal subscribe operation fails.
    fn subscribe(
        &self,
        channel: &str,
        accepted_schemas: &[ProtocolSchemaId],
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
}

/// Default adapter from server wire frames to liminal channel/conversation APIs.
#[derive(Debug)]
pub struct LiminalConnectionServices {
    channels: HashMap<String, ConfiguredChannel>,
    cluster: ChannelCluster,
    durable_store: Arc<dyn DurableStore>,
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
    /// # Errors
    /// Returns [`ServerError`] when a configured channel cannot be initialized.
    pub fn from_config(config: &ServerConfig) -> Result<Self, ServerError> {
        let store = build_durable_store(config.persistence_path.as_deref())?;
        Self::from_config_with_store(config, store)
    }

    /// Builds services over a caller-provided durable store.
    ///
    /// Used by tests that need to inspect persisted state through the same store
    /// handle the durable channels write to.
    ///
    /// # Errors
    /// Returns [`ServerError`] when a configured channel cannot be initialized.
    pub fn from_config_with_store(
        config: &ServerConfig,
        durable_store: Arc<dyn DurableStore>,
    ) -> Result<Self, ServerError> {
        // Build ONE shared channel supervisor for the whole server. When a
        // [cluster] section is present it is distribution-enabled, so every
        // channel actor and subscriber shares the clustered scheduler the cluster
        // attaches its process-group transport to (SRV-005, Constraint B).
        let cluster = build_channel_cluster(config.cluster.as_ref())?;
        let mut channels = HashMap::new();
        for channel in &config.channels {
            let schema = Schema::new(serde_json::json!({})).map_err(|error| {
                ServerError::ConfigValidation {
                    message: format!("failed to initialize channel '{}': {error}", channel.name),
                }
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
                    protocol_schema: schema_ref_id(&channel.schema_ref),
                },
            );
        }
        let conversation_supervisor = Arc::new(ConversationSupervisor::new().map_err(|error| {
            ServerError::ConfigValidation {
                message: format!("failed to start conversation supervisor: {error}"),
            }
        })?);
        let dedup = DedupCache::new(Arc::clone(&durable_store), DELIVERY_DEDUP_NAMESPACE);
        Ok(Self {
            channels,
            cluster,
            durable_store,
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
        let conversation_supervisor = Arc::new(ConversationSupervisor::new().map_err(|error| {
            ServerError::ConfigValidation {
                message: format!("failed to start conversation supervisor: {error}"),
            }
        })?);
        let durable_store = build_durable_store(None)?;
        let dedup = DedupCache::new(Arc::clone(&durable_store), DELIVERY_DEDUP_NAMESPACE);
        Ok(Self {
            channels: HashMap::new(),
            cluster: build_channel_cluster(None)?,
            durable_store,
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

/// Builds the on-disk haematite-backed durable store.
///
/// When `persistence_path` is `Some`, the database lives there and survives
/// process restarts: an existing database directory is reopened, a fresh one is
/// created. When it is `None` (no durable path configured, or the channel-free
/// `empty()` services used by tests), an ephemeral per-instance directory under
/// the system temp dir is created instead — it still persists to disk for the
/// lifetime of the process, but is not a stable restart location.
fn build_durable_store(
    persistence_path: Option<&Path>,
) -> Result<Arc<dyn DurableStore>, ServerError> {
    let data_dir = persistence_path.map_or_else(ephemeral_data_dir, |path| path.join("durability"));
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
            sweep_interval: None,
            distributed: None,
        })
    };
    result.map_err(|error| ServerError::ConfigValidation {
        message: format!(
            "failed to open durable store at {}: {error}",
            data_dir.display()
        ),
    })
}

/// Produces a unique on-disk directory under the system temp dir.
///
/// The pid plus a monotonic counter keep concurrent servers and parallel tests
/// from sharing a database directory.
fn ephemeral_data_dir() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "liminal-durability-{}-{unique}",
        std::process::id()
    ))
}

impl ConnectionServices for LiminalConnectionServices {
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

        Ok(PublishOutcome {
            message_id: self.next_message_id.fetch_add(1, Ordering::Relaxed),
            delivered: delivery.is_delivered(),
        })
    }

    fn subscribe(
        &self,
        channel: &str,
        accepted_schemas: &[ProtocolSchemaId],
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
        let subscription =
            configured
                .handle
                .subscribe()
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
}

pub(super) fn server_error_from_protocol(error: &ProtocolError) -> ServerError {
    ServerError::ListenerAccept {
        message: format!("protocol operation failed: {error}"),
    }
}

fn schema_ref_id(schema_ref: &str) -> ProtocolSchemaId {
    let mut bytes = [0_u8; ProtocolSchemaId::WIRE_LEN];
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    schema_ref.hash(&mut hash);
    let seed = hash.finish().to_be_bytes();
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = seed[index % seed.len()];
    }
    ProtocolSchemaId::new(bytes)
}
