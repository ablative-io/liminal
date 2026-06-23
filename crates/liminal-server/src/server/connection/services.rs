use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use haematite::EventStore;
use liminal::channel::{ChannelConfig, ChannelHandle, ChannelMode, Schema};
use liminal::conversation::Conversation;
use liminal::durability::{DurableStore, HaematiteStore};
use liminal::protocol::{MessageEnvelope, ProtocolError, SchemaId as ProtocolSchemaId};

use crate::ServerError;
use crate::config::types::ServerConfig;

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

/// Marker for library conversation state owned by a single connection process.
pub trait ConversationResource: std::fmt::Debug + Send {
    /// Delegates one conversation message to the library resource.
    ///
    /// # Errors
    /// Returns [`ServerError`] when the liminal library rejects the conversation message.
    fn message(&self, envelope: &MessageEnvelope) -> Result<(), ServerError>;

    /// Releases or finishes the library conversation resource.
    ///
    /// # Errors
    /// Returns [`ServerError`] when the liminal library reports a close failure.
    fn close(self: Box<Self>) -> Result<(), ServerError>;
}

/// Library conversation resource owned by a single connection process.
#[derive(Debug)]
pub struct ConnectionConversation {
    resource: Box<dyn ConversationResource>,
}

impl ConnectionConversation {
    /// Creates an owned conversation resource for one connection process.
    #[must_use]
    pub fn new(resource: Box<dyn ConversationResource>) -> Self {
        Self { resource }
    }

    pub(super) fn message(&self, envelope: &MessageEnvelope) -> Result<(), ServerError> {
        self.resource.message(envelope)
    }

    pub(super) fn close(self) -> Result<(), ServerError> {
        self.resource.close()
    }
}

/// Operations that adapt wire frames to liminal library calls.
pub trait ConnectionServices: std::fmt::Debug + Send + Sync {
    /// Delegates a publish request to the liminal library.
    ///
    /// # Errors
    /// Returns [`ServerError`] when the liminal publish operation fails.
    fn publish(&self, channel: &str, envelope: &MessageEnvelope) -> Result<u64, ServerError>;

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
    durable_store: Arc<dyn DurableStore>,
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
        Self::from_config_with_store(config, default_store())
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
                ChannelHandle::new_durable(channel_config, Arc::clone(&durable_store)).map_err(
                    |error| ServerError::ConfigValidation {
                        message: format!(
                            "failed to initialize durable channel '{}': {error}",
                            channel.name
                        ),
                    },
                )?
            } else {
                ChannelHandle::new(channel_config)
            };
            channels.insert(
                channel.name.clone(),
                ConfiguredChannel {
                    handle,
                    protocol_schema: schema_ref_id(&channel.schema_ref),
                },
            );
        }
        Ok(Self {
            channels,
            durable_store,
            next_message_id: AtomicU64::new(1),
            next_subscription_id: AtomicU64::new(1),
        })
    }

    /// Builds services with no configured channels.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            channels: HashMap::new(),
            durable_store: default_store(),
            next_message_id: AtomicU64::new(1),
            next_subscription_id: AtomicU64::new(1),
        }
    }

    /// Returns the shared durable store backing this service's durable channels.
    #[must_use]
    pub fn durable_store(&self) -> Arc<dyn DurableStore> {
        Arc::clone(&self.durable_store)
    }
}

/// Constructs the default in-memory haematite-backed durable store.
fn default_store() -> Arc<dyn DurableStore> {
    Arc::new(HaematiteStore::new(Arc::new(EventStore::new())))
}

impl ConnectionServices for LiminalConnectionServices {
    fn publish(&self, channel: &str, envelope: &MessageEnvelope) -> Result<u64, ServerError> {
        let handle = self
            .channels
            .get(channel)
            .map(|configured| configured.handle.clone())
            .ok_or_else(|| ServerError::ListenerAccept {
                message: format!("channel '{channel}' is not configured"),
            })?;
        handle
            .publish(&envelope.payload)
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("liminal publish failed for channel '{channel}': {error}"),
            })?;
        Ok(self.next_message_id.fetch_add(1, Ordering::Relaxed))
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
        let name = format!("{conversation_id}:{subject}");
        Ok(ConnectionConversation::new(Box::new(
            LiminalConversationResource {
                conversation: Conversation::start(name),
            },
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

#[derive(Debug)]
struct LiminalConversationResource {
    conversation: Conversation,
}

impl ConversationResource for LiminalConversationResource {
    fn message(&self, envelope: &MessageEnvelope) -> Result<(), ServerError> {
        let conversation_message = self.conversation.message(envelope.payload.clone());
        drop(conversation_message);
        Ok(())
    }

    fn close(self: Box<Self>) -> Result<(), ServerError> {
        let Self { conversation } = *self;
        let span = conversation.finish();
        drop(span);
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
