use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use spin::Mutex;

use crate::connection::{
    ConnectionEvent, ConnectionLifecycle, ConnectionPool, ConnectionState, ReconnectJitter,
    ResumeRequest, SubscriptionId,
};
use crate::embedded::{
    EmbeddedChannelHandle, EmbeddedConversationHandle, EmptyLifecycleStream, ReadyResult,
    SdkSubscription,
};
use crate::{
    ChannelHandle, ConversationHandle, ConversationId, PressureResponse, SchemaValidate, SdkError,
};

use super::config::SdkConfig;
use super::protocol::{
    RemoteTransport, WireConversationRequest, WirePublishRequest, WireResumeRequest,
    WireSubscribeRequest, serialize_payload,
};
use super::{RemoteConfig, ServerAddress, connection_error};

#[derive(Debug)]
struct RemoteChannelState {
    lifecycle: ConnectionLifecycle,
    pool: ConnectionPool,
    next_subscription: u64,
}

/// Channel handle that communicates through SDK-internal wire protocol transport.
#[derive(Clone, Debug)]
pub struct RemoteChannelHandle {
    server_address: ServerAddress,
    channel_name: String,
    state: Arc<Mutex<RemoteChannelState>>,
    transport: Arc<dyn RemoteTransport>,
}

impl RemoteChannelHandle {
    /// Creates a remote channel handle from validated configuration.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] if the connection pool cannot be created.
    pub fn new(config: &RemoteConfig) -> Result<Self, SdkError> {
        Ok(Self {
            server_address: config.server_address.clone(),
            channel_name: config.channel_name.clone(),
            state: Arc::new(Mutex::new(RemoteChannelState {
                lifecycle: ConnectionLifecycle::new(config.reconnect_config),
                pool: ConnectionPool::new(config.pool_config, config.reconnect_config)?,
                next_subscription: 0,
            })),
            transport: Arc::clone(&config.transport),
        })
    }

    /// Returns current lifecycle state from the SDK-003 state machine.
    #[must_use]
    pub fn connection_state(&self) -> ConnectionState {
        self.state.lock().lifecycle.state().clone()
    }

    /// Drives a reconnect attempt through the SDK-003 lifecycle state machine.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] when the lifecycle rejects the transition.
    pub fn reconnect<J>(&self, jitter: &mut J) -> Result<Duration, SdkError>
    where
        J: ReconnectJitter + ?Sized,
    {
        self.state.lock().lifecycle.reconnect(jitter)
    }

    /// Marks the remote channel connected and builds subscription resume requests.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] if lifecycle transition or recovery state is invalid.
    pub fn connected(&self) -> Result<Vec<ResumeRequest>, SdkError> {
        // Compute the resume requests under the lock, then release it before any
        // transport I/O so concurrent callers do not spin-wait during resume calls.
        let requests = {
            let mut state = self.state.lock();
            let previous = state.lifecycle.state().clone();
            state.lifecycle.connected()?;
            let event = ConnectionEvent::new(previous, state.lifecycle.state().clone());
            state.pool.resume_requests_for_transition(&event)?
        };
        for request in &requests {
            let wire_request = WireResumeRequest::new(*request);
            self.transport.resume(&self.server_address, &wire_request)?;
        }
        Ok(requests)
    }

    /// Marks a subscription active and assigns it to the configured pool.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] if a subscription id overflows or pool assignment fails.
    pub fn track_subscription(&self) -> Result<SubscriptionId, SdkError> {
        let mut state = self.state.lock();
        let id = SubscriptionId::new(state.next_subscription);
        state.next_subscription =
            state
                .next_subscription
                .checked_add(1)
                .ok_or_else(|| SdkError::Store {
                    description: "subscription id overflow".to_string(),
                })?;
        state.pool.assign_subscription(id)?;
        Ok(id)
    }

    /// Records an acknowledged sequence for subscription recovery.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] if the subscription is not active in the pool.
    pub fn acknowledge(
        &self,
        subscription_id: SubscriptionId,
        sequence: u64,
    ) -> Result<(), SdkError> {
        let mut state = self.state.lock();
        state.pool.acknowledge(subscription_id, sequence)
    }

    /// Returns remote server address used by this handle.
    #[must_use]
    pub const fn server_address(&self) -> &ServerAddress {
        &self.server_address
    }
}

impl ChannelHandle for RemoteChannelHandle {
    type Subscription<M>
        = SdkSubscription<M>
    where
        M: DeserializeOwned;

    type ReplyFuture<'a, Resp>
        = ReadyResult<Resp>
    where
        Self: 'a,
        Resp: DeserializeOwned + 'a;

    fn publish<M>(&self, message: M) -> Result<PressureResponse, SdkError>
    where
        M: Serialize + SchemaValidate,
    {
        let request = WirePublishRequest::new(&self.channel_name, &message)?;
        self.transport.publish(&self.server_address, &request)
    }

    fn subscribe<M>(&self) -> Self::Subscription<M>
    where
        M: DeserializeOwned,
    {
        let outcome = self.track_subscription().and_then(|subscription_id| {
            let connection_id = self
                .state
                .lock()
                .pool
                .connection_for_subscription(subscription_id)
                .ok_or_else(|| connection_error("subscription was not assigned to the pool"))?;
            let request = WireSubscribeRequest::new(
                &self.channel_name,
                subscription_id,
                connection_id.get(),
            )?;
            self.transport.subscribe(&self.server_address, &request)
        });

        match outcome {
            Ok(()) => SdkSubscription::empty(),
            Err(error) => SdkSubscription::error(error),
        }
    }

    fn request_reply<Req, Resp>(&self, request: Req) -> ReadyResult<Resp>
    where
        Req: Serialize + SchemaValidate,
        Resp: DeserializeOwned,
    {
        let schema = Req::schema_metadata();
        let result = serialize_payload(&request).map(|payload| {
            core::hint::black_box((schema, payload));
        });
        ReadyResult::new(result.and_then(|()| {
            Err(SdkError::Protocol {
                description: "remote request/reply awaits protocol response integration"
                    .to_string(),
            })
        }))
    }
}

/// Conversation handle that communicates through SDK-internal wire protocol transport.
#[derive(Clone, Debug)]
pub struct RemoteConversationHandle {
    server_address: ServerAddress,
    conversation_id: ConversationId,
    lifecycle: Arc<Mutex<ConnectionLifecycle>>,
    transport: Arc<dyn RemoteTransport>,
}

impl RemoteConversationHandle {
    /// Creates a remote conversation handle from validated configuration.
    #[must_use]
    pub fn new(config: &RemoteConfig) -> Self {
        Self {
            server_address: config.server_address.clone(),
            conversation_id: config.conversation_id.clone(),
            lifecycle: Arc::new(Mutex::new(ConnectionLifecycle::new(
                config.reconnect_config,
            ))),
            transport: Arc::clone(&config.transport),
        }
    }

    /// Returns current lifecycle state from the SDK-003 state machine.
    #[must_use]
    pub fn connection_state(&self) -> ConnectionState {
        self.lifecycle.lock().state().clone()
    }

    /// Returns remote server address used by this handle.
    #[must_use]
    pub const fn server_address(&self) -> &ServerAddress {
        &self.server_address
    }
}

impl ConversationHandle for RemoteConversationHandle {
    type ReceiveFuture<'a, M>
        = ReadyResult<M>
    where
        Self: 'a,
        M: DeserializeOwned + 'a;

    type LifecycleStream = EmptyLifecycleStream;

    fn send<M>(&self, message: M) -> Result<(), SdkError>
    where
        M: Serialize,
    {
        let request = WireConversationRequest::new(&self.conversation_id, &message)?;
        self.transport
            .send_conversation(&self.server_address, &request)
    }

    fn receive<M>(&self) -> ReadyResult<M>
    where
        M: DeserializeOwned,
    {
        ReadyResult::new(Err(SdkError::Conversation {
            conversation_id: self.conversation_id.as_str().to_string(),
            description: "remote receive awaits protocol inbox integration".to_string(),
        }))
    }

    fn lifecycle(&self) -> Self::LifecycleStream {
        EmptyLifecycleStream
    }
}

/// Runtime-selected channel handle that keeps deployment differences behind configuration.
#[derive(Clone, Debug)]
pub enum SdkChannelHandle {
    /// Embedded direct in-process handle.
    Embedded(EmbeddedChannelHandle),
    /// Remote protocol-backed handle.
    Remote(RemoteChannelHandle),
}

impl SdkChannelHandle {
    /// Creates a channel handle selected by SDK configuration.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] if the selected handle cannot be initialized.
    pub fn new(config: &SdkConfig) -> Result<Self, SdkError> {
        match config {
            SdkConfig::Embedded(config) => Ok(Self::Embedded(EmbeddedChannelHandle::new(config))),
            SdkConfig::Remote(config) => Ok(Self::Remote(RemoteChannelHandle::new(config)?)),
        }
    }
}

impl ChannelHandle for SdkChannelHandle {
    type Subscription<M>
        = SdkSubscription<M>
    where
        M: DeserializeOwned;

    type ReplyFuture<'a, Resp>
        = ReadyResult<Resp>
    where
        Self: 'a,
        Resp: DeserializeOwned + 'a;

    fn publish<M>(&self, message: M) -> Result<PressureResponse, SdkError>
    where
        M: Serialize + SchemaValidate,
    {
        match self {
            Self::Embedded(handle) => handle.publish(message),
            Self::Remote(handle) => handle.publish(message),
        }
    }

    fn subscribe<M>(&self) -> Self::Subscription<M>
    where
        M: DeserializeOwned,
    {
        match self {
            Self::Embedded(handle) => handle.subscribe(),
            Self::Remote(handle) => handle.subscribe(),
        }
    }

    fn request_reply<Req, Resp>(&self, request: Req) -> ReadyResult<Resp>
    where
        Req: Serialize + SchemaValidate,
        Resp: DeserializeOwned,
    {
        match self {
            Self::Embedded(handle) => handle.request_reply(request),
            Self::Remote(handle) => handle.request_reply(request),
        }
    }
}

/// Runtime-selected conversation handle that keeps deployment differences behind configuration.
#[derive(Clone, Debug)]
pub enum SdkConversationHandle {
    /// Embedded direct in-process handle.
    Embedded(EmbeddedConversationHandle),
    /// Remote protocol-backed handle.
    Remote(RemoteConversationHandle),
}

impl SdkConversationHandle {
    /// Creates a conversation handle selected by SDK configuration.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] if the selected handle cannot be initialized.
    pub fn new(config: &SdkConfig) -> Result<Self, SdkError> {
        match config {
            SdkConfig::Embedded(config) => {
                Ok(Self::Embedded(EmbeddedConversationHandle::new(config)))
            }
            SdkConfig::Remote(config) => Ok(Self::Remote(RemoteConversationHandle::new(config))),
        }
    }
}

impl ConversationHandle for SdkConversationHandle {
    type ReceiveFuture<'a, M>
        = ReadyResult<M>
    where
        Self: 'a,
        M: DeserializeOwned + 'a;

    type LifecycleStream = EmptyLifecycleStream;

    fn send<M>(&self, message: M) -> Result<(), SdkError>
    where
        M: Serialize,
    {
        match self {
            Self::Embedded(handle) => handle.send(message),
            Self::Remote(handle) => handle.send(message),
        }
    }

    fn receive<M>(&self) -> ReadyResult<M>
    where
        M: DeserializeOwned,
    {
        match self {
            Self::Embedded(handle) => handle.receive(),
            Self::Remote(handle) => handle.receive(),
        }
    }

    fn lifecycle(&self) -> Self::LifecycleStream {
        EmptyLifecycleStream
    }
}
