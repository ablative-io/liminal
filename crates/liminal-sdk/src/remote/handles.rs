use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use serde::Serialize;
use serde::de::DeserializeOwned;
use spin::Mutex;

use crate::connection::{
    ConnectionEvent, ConnectionLifecycle, ConnectionPool, ConnectionState, DisconnectReason,
    ReconnectAttempt, ReconnectEvent, ResumeRequest, SubscriptionId,
};
use crate::embedded::{
    EmbeddedChannelHandle, EmbeddedConversationHandle, EmptyLifecycleStream, ReadyResult,
    SdkSubscription,
};
use crate::{
    ChannelHandle, ConversationHandle, ConversationId, DeliveryAck, PressureResponse,
    SchemaValidate, SdkError,
};

use super::config::SdkConfig;
use super::protocol::{
    RemoteTransport, WireConversationRequest, WirePublishRequest, WireResumeRequest,
    WireSubscribeRequest, deserialize_payload,
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
                lifecycle: ConnectionLifecycle::new(),
                pool: ConnectionPool::new(config.pool_config)?,
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

    /// Consumes one fresh event and starts one reconnect attempt immediately.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] while another attempt is still in flight.
    pub fn reconnect(&self, event: ReconnectEvent) -> Result<ReconnectAttempt, SdkError> {
        self.state.lock().lifecycle.reconnect(event)
    }

    /// Parks a failed attempt without arming a retry timer.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] unless a reconnect attempt is in flight.
    pub fn reconnect_failed(&self, reason: DisconnectReason) -> Result<(), SdkError> {
        self.state.lock().lifecycle.reconnect_failed(reason)
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
        ReadyResult::new(self.request_reply_blocking(&request))
    }
}

impl RemoteChannelHandle {
    /// Publishes a message with an idempotency key and returns a genuine delivery
    /// ack.
    ///
    /// The idempotency key drives dedup-on-delivery on the server: a re-publish of
    /// the same key is delivered to subscribers at most once. The returned
    /// [`DeliveryAck`] reports whether this publish was genuinely accepted by a
    /// subscriber ([`DeliveryAck::is_accepted`]), which a caller such as the aion
    /// outbox uses to treat the send as done only on real acceptance. This is
    /// distinct from the backpressure-only [`publish`](ChannelHandle::publish).
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] when the message cannot be serialized, the round trip
    /// fails, or the transport cannot produce a genuine delivery ack.
    pub fn publish_with_idempotency_key<M>(
        &self,
        message: &M,
        idempotency_key: &str,
    ) -> Result<DeliveryAck, SdkError>
    where
        M: Serialize + SchemaValidate,
    {
        let request =
            WirePublishRequest::with_idempotency_key(&self.channel_name, message, idempotency_key)?;
        self.transport
            .publish_with_delivery(&self.server_address, &request)
    }

    /// Performs a correlated request-reply round trip over the transport and
    /// deserializes the reply.
    ///
    /// The round trip rides the conversation request-reply path: the channel name
    /// is used as the conversation correlation id and subject, so the reply the
    /// server returns is matched back to this request by `conversation_id` on the
    /// single synchronous connection. Schema validation still runs on the request
    /// so a request-reply enforces the same typing contract as `publish`.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] when the request cannot be serialized, the round trip
    /// fails, or the reply cannot be deserialized into `Resp`.
    fn request_reply_blocking<Req, Resp>(&self, request: &Req) -> Result<Resp, SdkError>
    where
        Req: Serialize + SchemaValidate,
        Resp: DeserializeOwned,
    {
        let conversation_id = ConversationId::new(self.channel_name.clone());
        let wire_request = WireConversationRequest::new(&conversation_id, request)?;
        let reply = self
            .transport
            .request_reply_conversation(&self.server_address, &wire_request)?;
        deserialize_payload(&reply)
    }
}

/// Conversation handle that communicates through SDK-internal wire protocol transport.
#[derive(Clone, Debug)]
pub struct RemoteConversationHandle {
    server_address: ServerAddress,
    conversation_id: ConversationId,
    lifecycle: Arc<Mutex<ConnectionLifecycle>>,
    transport: Arc<dyn RemoteTransport>,
    /// Buffered reply bytes from the most recent [`request`](Self::request) round
    /// trip, drained by the next [`receive`](ConversationHandle::receive). The
    /// synchronous transport completes the round trip inside `request`, so the
    /// correlated reply is held here until the caller deserializes it.
    pending_reply: Arc<Mutex<Option<Vec<u8>>>>,
}

impl RemoteConversationHandle {
    /// Creates a remote conversation handle from validated configuration.
    #[must_use]
    pub fn new(config: &RemoteConfig) -> Self {
        Self {
            server_address: config.server_address.clone(),
            conversation_id: config.conversation_id.clone(),
            lifecycle: Arc::new(Mutex::new(ConnectionLifecycle::new())),
            transport: Arc::clone(&config.transport),
            pending_reply: Arc::new(Mutex::new(None)),
        }
    }

    /// Sends a typed request on this conversation and blocks for its correlated
    /// reply, buffering the reply for the next [`receive`](ConversationHandle::receive).
    ///
    /// This is the request leg of the conversation request-reply pattern. It is
    /// kept distinct from [`send`](ConversationHandle::send), which stays
    /// fire-and-forget (the server is silent on success): only `request` sets the
    /// reply-requested flag and waits for the server's correlated answer. The aion
    /// dispatch model (`send` request, then `receive` reply) maps onto a `request`
    /// followed by `receive`.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] when the request cannot be serialized or the round
    /// trip fails.
    pub fn request<Req>(&self, request: Req) -> Result<(), SdkError>
    where
        Req: Serialize,
    {
        let wire_request = WireConversationRequest::new(&self.conversation_id, &request)?;
        let reply = self
            .transport
            .request_reply_conversation(&self.server_address, &wire_request)?;
        *self.pending_reply.lock() = Some(reply);
        Ok(())
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
        ReadyResult::new(self.receive_blocking())
    }

    fn lifecycle(&self) -> Self::LifecycleStream {
        EmptyLifecycleStream
    }
}

impl RemoteConversationHandle {
    /// Drains and deserializes the correlated reply buffered by the most recent
    /// [`request`](Self::request).
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Conversation`] when no reply is pending (no `request`
    /// has completed since the last `receive`), or [`SdkError::Serialization`]
    /// when the buffered reply cannot be deserialized into `M`.
    fn receive_blocking<M>(&self) -> Result<M, SdkError>
    where
        M: DeserializeOwned,
    {
        let payload = self
            .pending_reply
            .lock()
            .take()
            .ok_or_else(|| SdkError::Conversation {
                conversation_id: self.conversation_id.as_str().to_string(),
                description: "no correlated reply is pending; call request before receive"
                    .to_string(),
            })?;
        deserialize_payload(&payload)
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
