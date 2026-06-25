use alloc::string::String;
use core::future::Future;

use futures_core::Stream;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::SdkError;

/// Application-visible identifier for a conversation.
///
/// SDK callers use this value for correlation and lifecycle observation. It is
/// not a beamr process identifier and does not require callers to manage any
/// supervised runtime process directly.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ConversationId(String);

impl ConversationId {
    /// Creates a conversation identifier from an application-visible string.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the identifier as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl From<String> for ConversationId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&'static str> for ConversationId {
    fn from(value: &'static str) -> Self {
        Self::new(value)
    }
}

/// Lifecycle events emitted by a conversation.
///
/// Every event carries the application-visible conversation identifier so
/// lifecycle streams can be correlated without exposing process identifiers,
/// protocol frames, or transport details.
#[derive(Debug)]
pub enum ConversationEvent {
    /// The conversation was opened.
    Opened {
        /// Identifier for the conversation that emitted this event.
        conversation_id: ConversationId,
    },
    /// A message was observed within the conversation.
    Message {
        /// Identifier for the conversation that emitted this event.
        conversation_id: ConversationId,
    },
    /// The conversation has begun closing.
    Closing {
        /// Identifier for the conversation that emitted this event.
        conversation_id: ConversationId,
    },
    /// The conversation closed.
    Closed {
        /// Identifier for the conversation that emitted this event.
        conversation_id: ConversationId,
    },
    /// The conversation encountered an error.
    Error {
        /// Identifier for the conversation that emitted this event.
        conversation_id: ConversationId,
        /// Error reported for the conversation lifecycle.
        error: SdkError,
    },
}

/// Application-facing typed conversation API.
///
/// A conversation is the fundamental messaging unit in liminal. The handle lets
/// callers send typed messages, receive typed messages, and observe lifecycle
/// events without handling transport details or supervised runtime process IDs.
pub trait ConversationHandle: core::fmt::Debug + Send + Sync {
    /// Future returned by [`receive`](Self::receive) for message type `M`.
    type ReceiveFuture<'a, M>: Future<Output = Result<M, SdkError>> + 'a
    where
        Self: 'a,
        M: DeserializeOwned + 'a;

    /// Stream returned by [`lifecycle`](Self::lifecycle).
    type LifecycleStream: Stream<Item = ConversationEvent>;

    /// Sends a typed message within this conversation.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] when the concrete conversation implementation cannot
    /// serialize or transmit the message in the conversation context.
    fn send<M>(&self, message: M) -> Result<(), SdkError>
    where
        M: Serialize;

    /// Receives the next typed message from this conversation.
    ///
    /// The message type is owned after deserialization so callers never borrow
    /// buffers managed by an SDK implementation.
    ///
    /// # Errors
    ///
    /// The returned future resolves to [`SdkError`] when the concrete
    /// implementation cannot receive or deserialize the next message.
    fn receive<M>(&self) -> Self::ReceiveFuture<'_, M>
    where
        M: DeserializeOwned;

    /// Observes lifecycle events for this conversation.
    fn lifecycle(&self) -> Self::LifecycleStream;
}
