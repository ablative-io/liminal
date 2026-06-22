#![allow(clippy::module_name_repetitions)]

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use core::fmt;
use core::marker::PhantomData;
use core::pin::Pin;
use core::task::{Context, Poll};

use futures_core::Stream;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{
    ChannelHandle, ConversationEvent, ConversationHandle, ConversationId, PressureResponse,
    SchemaMetadata, SchemaValidate, SdkError,
};

#[cfg(test)]
mod tests;

/// Type-erased view of a typed channel message passed to an embedded backend.
///
/// The embedded SDK path hands this reference directly to the in-process backend;
/// it never asks serde to produce bytes, protocol envelopes, or wire frames.
pub trait EmbeddedChannelMessage {
    /// Returns the compile-time schema metadata declared by the message type.
    #[must_use]
    fn schema_metadata(&self) -> SchemaMetadata;

    /// Returns the Rust type name used for diagnostics and backend routing.
    #[must_use]
    fn type_name(&self) -> &'static str;
}

impl<M> EmbeddedChannelMessage for M
where
    M: Serialize + SchemaValidate,
{
    fn schema_metadata(&self) -> SchemaMetadata {
        M::schema_metadata()
    }

    fn type_name(&self) -> &'static str {
        core::any::type_name::<M>()
    }
}

/// Type-erased view of a typed conversation message passed to an embedded backend.
pub trait EmbeddedConversationMessage {
    /// Returns the Rust type name used for diagnostics and backend routing.
    #[must_use]
    fn type_name(&self) -> &'static str;
}

impl<M> EmbeddedConversationMessage for M
where
    M: Serialize,
{
    fn type_name(&self) -> &'static str {
        core::any::type_name::<M>()
    }
}

/// Stream used by SDK handles when no typed messages are buffered locally.
pub struct SdkSubscription<M> {
    pending_error: Option<SdkError>,
    message: PhantomData<M>,
}

impl<M> SdkSubscription<M> {
    /// Creates an empty typed subscription stream.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            pending_error: None,
            message: PhantomData,
        }
    }

    /// Creates a subscription stream that surfaces a setup error to the caller.
    #[must_use]
    pub const fn error(error: SdkError) -> Self {
        Self {
            pending_error: Some(error),
            message: PhantomData,
        }
    }

    /// Returns true when this local subscription has no pending setup error.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.pending_error.is_none()
    }
}

impl<M> Default for SdkSubscription<M> {
    fn default() -> Self {
        Self::empty()
    }
}

impl<M> fmt::Debug for SdkSubscription<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SdkSubscription")
            .field("has_pending_error", &self.pending_error.is_some())
            .finish()
    }
}

impl<M> Unpin for SdkSubscription<M> {}

impl<M> Stream for SdkSubscription<M> {
    type Item = Result<M, SdkError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        core::hint::black_box(context.waker());
        Poll::Ready(self.pending_error.take().map(Err))
    }
}

/// Stream used when a conversation has no lifecycle events buffered locally.
#[derive(Clone, Debug, Default)]
pub struct EmptyLifecycleStream;

impl Unpin for EmptyLifecycleStream {}

impl Stream for EmptyLifecycleStream {
    type Item = ConversationEvent;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        core::hint::black_box((self, context.waker()));
        Poll::Ready(None)
    }
}

/// Ready future used by SDK handles that complete an operation synchronously.
pub struct ReadyResult<T> {
    result: Option<Result<T, SdkError>>,
}

impl<T> fmt::Debug for ReadyResult<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReadyResult")
            .field("is_ready", &self.result.is_some())
            .finish()
    }
}

impl<T> ReadyResult<T> {
    /// Creates a ready future from a result.
    #[must_use]
    pub const fn new(result: Result<T, SdkError>) -> Self {
        Self {
            result: Some(result),
        }
    }
}

impl<T> Unpin for ReadyResult<T> {}

impl<T> core::future::Future for ReadyResult<T> {
    type Output = Result<T, SdkError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        core::hint::black_box(context.waker());
        let Some(result) = self.result.take() else {
            return Poll::Ready(Err(SdkError::Protocol {
                description: "ready future polled after completion".to_string(),
            }));
        };

        Poll::Ready(result)
    }
}

/// Direct in-process channel backend used by embedded handles.
pub trait EmbeddedChannelBackend: fmt::Debug + Send + Sync {
    /// Publishes a typed message reference without protocol framing or wire encoding.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] if the in-process backend rejects the publish attempt.
    fn publish(&self, message: &dyn EmbeddedChannelMessage) -> Result<PressureResponse, SdkError>;
}

/// Direct in-process conversation backend used by embedded handles.
pub trait EmbeddedConversationBackend: fmt::Debug + Send + Sync {
    /// Sends a typed message reference without protocol framing or wire encoding.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] if the in-process backend rejects the send attempt.
    fn send(&self, message: &dyn EmbeddedConversationMessage) -> Result<(), SdkError>;
}

/// Minimal in-process channel backend that accepts messages immediately.
#[derive(Clone, Debug, Default)]
pub struct DirectEmbeddedChannelBackend;

impl EmbeddedChannelBackend for DirectEmbeddedChannelBackend {
    fn publish(&self, message: &dyn EmbeddedChannelMessage) -> Result<PressureResponse, SdkError> {
        let schema = message.schema_metadata();
        core::hint::black_box(&schema);
        Ok(PressureResponse::Accept)
    }
}

/// Minimal in-process conversation backend that accepts sends immediately.
#[derive(Clone, Debug, Default)]
pub struct DirectEmbeddedConversationBackend;

impl EmbeddedConversationBackend for DirectEmbeddedConversationBackend {
    fn send(&self, message: &dyn EmbeddedConversationMessage) -> Result<(), SdkError> {
        let type_name = message.type_name();
        core::hint::black_box(type_name);
        Ok(())
    }
}

/// Configuration for embedded SDK handles.
#[derive(Clone, Debug)]
pub struct EmbeddedConfig {
    /// Application-visible channel name.
    pub channel_name: String,
    /// Application-visible conversation identifier.
    pub conversation_id: ConversationId,
    /// Direct channel backend used for in-process publication.
    pub channel_backend: Arc<dyn EmbeddedChannelBackend>,
    /// Direct conversation backend used for in-process conversation sends.
    pub conversation_backend: Arc<dyn EmbeddedConversationBackend>,
}

impl EmbeddedConfig {
    /// Creates embedded configuration without requiring a server address.
    #[must_use]
    pub fn new(
        channel_name: impl Into<String>,
        conversation_id: impl Into<ConversationId>,
    ) -> Self {
        Self {
            channel_name: channel_name.into(),
            conversation_id: conversation_id.into(),
            channel_backend: Arc::new(DirectEmbeddedChannelBackend),
            conversation_backend: Arc::new(DirectEmbeddedConversationBackend),
        }
    }

    /// Replaces the direct in-process channel backend.
    #[must_use]
    pub fn with_channel_backend(mut self, backend: Arc<dyn EmbeddedChannelBackend>) -> Self {
        self.channel_backend = backend;
        self
    }

    /// Replaces the direct in-process conversation backend.
    #[must_use]
    pub fn with_conversation_backend(
        mut self,
        backend: Arc<dyn EmbeddedConversationBackend>,
    ) -> Self {
        self.conversation_backend = backend;
        self
    }
}

/// Channel handle that publishes through direct in-process references.
#[derive(Clone, Debug)]
pub struct EmbeddedChannelHandle {
    channel_name: String,
    backend: Arc<dyn EmbeddedChannelBackend>,
}

impl EmbeddedChannelHandle {
    /// Creates an embedded channel handle from direct in-process configuration.
    #[must_use]
    pub fn new(config: &EmbeddedConfig) -> Self {
        Self {
            channel_name: config.channel_name.clone(),
            backend: Arc::clone(&config.channel_backend),
        }
    }

    /// Returns the application-visible channel name.
    #[must_use]
    pub fn channel_name(&self) -> &str {
        self.channel_name.as_str()
    }
}

impl ChannelHandle for EmbeddedChannelHandle {
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
        self.backend.publish(&message)
    }

    fn subscribe<M>(&self) -> Self::Subscription<M>
    where
        M: DeserializeOwned,
    {
        SdkSubscription::empty()
    }

    fn request_reply<Req, Resp>(&self, request: Req) -> ReadyResult<Resp>
    where
        Req: Serialize + SchemaValidate,
        Resp: DeserializeOwned,
    {
        let schema = Req::schema_metadata();
        core::hint::black_box((&request, &schema));
        ReadyResult::new(Err(SdkError::Protocol {
            description: "embedded request/reply requires an in-process responder backend"
                .to_string(),
        }))
    }
}

/// Conversation handle that sends through direct in-process references.
#[derive(Clone, Debug)]
pub struct EmbeddedConversationHandle {
    conversation_id: ConversationId,
    backend: Arc<dyn EmbeddedConversationBackend>,
}

impl EmbeddedConversationHandle {
    /// Creates an embedded conversation handle from direct in-process configuration.
    #[must_use]
    pub fn new(config: &EmbeddedConfig) -> Self {
        Self {
            conversation_id: config.conversation_id.clone(),
            backend: Arc::clone(&config.conversation_backend),
        }
    }

    /// Returns the application-visible conversation identifier.
    #[must_use]
    pub const fn conversation_id(&self) -> &ConversationId {
        &self.conversation_id
    }
}

impl ConversationHandle for EmbeddedConversationHandle {
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
        self.backend.send(&message)
    }

    fn receive<M>(&self) -> ReadyResult<M>
    where
        M: DeserializeOwned,
    {
        ReadyResult::new(Err(SdkError::Conversation {
            conversation_id: self.conversation_id.as_str().to_string(),
            description: "embedded receive requires an in-process inbox backend".to_string(),
        }))
    }

    fn lifecycle(&self) -> Self::LifecycleStream {
        EmptyLifecycleStream
    }
}
