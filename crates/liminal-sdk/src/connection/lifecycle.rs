use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;
use core::pin::Pin;
use core::task::{Context, Poll};

use futures_core::Stream;

use crate::SdkError;

/// Application-visible lifecycle state for a remote SDK connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnectionState {
    /// The SDK is establishing a connection.
    Connecting,
    /// The SDK has an active connection.
    Connected,
    /// A caller-authorized real reconnect attempt is in progress.
    Reconnecting,
    /// The SDK is disconnected and will not become connected without reconnecting.
    Disconnected {
        /// Reason the SDK entered the disconnected state.
        reason: DisconnectReason,
    },
}

/// Reason a connection entered the disconnected state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DisconnectReason {
    /// The connection was closed intentionally.
    Normal,
    /// The connection closed because of an error.
    Error,
    /// The connection closed because a timeout elapsed.
    Timeout,
}

/// Event emitted after a connection lifecycle transition succeeds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionEvent {
    /// State before the transition.
    pub previous: ConnectionState,
    /// State after the transition.
    pub current: ConnectionState,
}

impl ConnectionEvent {
    /// Creates a connection transition event.
    #[must_use]
    pub const fn new(previous: ConnectionState, current: ConnectionState) -> Self {
        Self { previous, current }
    }
}

/// Stream wrapper for observing connection lifecycle events.
///
/// Concrete SDK clients can wrap their runtime-specific event stream in this
/// type while exposing a stable SDK item type of [`ConnectionEvent`].
pub struct ConnectionEvents<S> {
    inner: S,
}

impl<S> ConnectionEvents<S> {
    /// Wraps a stream that yields connection transition events.
    #[must_use]
    pub const fn new(inner: S) -> Self {
        Self { inner }
    }

    /// Returns a shared reference to the wrapped stream.
    #[must_use]
    pub const fn inner(&self) -> &S {
        &self.inner
    }

    /// Unwraps the runtime-specific event stream.
    #[must_use]
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: Clone> Clone for ConnectionEvents<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S> fmt::Debug for ConnectionEvents<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("ConnectionEvents").finish()
    }
}

impl<S> Stream for ConnectionEvents<S>
where
    S: Stream<Item = ConnectionEvent> + Unpin,
{
    type Item = ConnectionEvent;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let stream = &mut self.as_mut().get_mut().inner;
        Pin::new(stream).poll_next(context)
    }
}

type ConnectionObserver = Box<dyn FnMut(&ConnectionEvent) + Send>;

/// Owns the SDK connection lifecycle state and emits validated transitions.
pub struct ConnectionLifecycle {
    state: ConnectionState,
    observers: Vec<ConnectionObserver>,
}

impl ConnectionLifecycle {
    /// Creates a lifecycle in the [`ConnectionState::Connecting`] state.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: ConnectionState::Connecting,
            observers: Vec::new(),
        }
    }

    /// Returns the current connection state.
    #[must_use]
    pub const fn state(&self) -> &ConnectionState {
        &self.state
    }

    /// Registers an observer that is called after each successful transition.
    pub fn observe(&mut self, observer: impl FnMut(&ConnectionEvent) + Send + 'static) {
        self.observers.push(Box::new(observer));
    }

    /// Transitions from [`ConnectionState::Disconnected`] to connecting.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] when the lifecycle is not disconnected.
    pub fn connect(&mut self) -> Result<(), SdkError> {
        match self.state {
            ConnectionState::Disconnected { .. } => {
                self.transition(ConnectionState::Connecting);
                Ok(())
            }
            _ => Err(invalid_transition(&self.state, "Connecting")),
        }
    }

    /// Transitions from connecting or reconnecting to connected.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] when the lifecycle is disconnected or already connected.
    pub fn connected(&mut self) -> Result<(), SdkError> {
        match self.state {
            ConnectionState::Connecting | ConnectionState::Reconnecting => {
                self.transition(ConnectionState::Connected);
                Ok(())
            }
            _ => Err(invalid_transition(&self.state, "Connected")),
        }
    }

    /// Records that an externally authorized real reconnect attempt started.
    ///
    /// This method has no delay, timer, retry counter, or authority producer. The
    /// remote participant entrypoint obtains one-use attempt authority from
    /// `liminal-protocol` before calling the transport.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] when reconnecting from the current state is invalid.
    pub fn reconnect_started(&mut self) -> Result<(), SdkError> {
        match self.state {
            ConnectionState::Connecting | ConnectionState::Connected => {
                self.transition(ConnectionState::Reconnecting);
                Ok(())
            }
            ConnectionState::Reconnecting => Err(invalid_transition(&self.state, "Reconnecting")),
            ConnectionState::Disconnected { .. } => {
                Err(invalid_transition(&self.state, "Reconnecting"))
            }
        }
    }

    /// Transitions to disconnected with the supplied reason.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] when the lifecycle is already disconnected.
    pub fn disconnect(&mut self, reason: DisconnectReason) -> Result<(), SdkError> {
        match self.state {
            ConnectionState::Connecting
            | ConnectionState::Connected
            | ConnectionState::Reconnecting => {
                self.transition(ConnectionState::Disconnected { reason });
                Ok(())
            }
            ConnectionState::Disconnected { .. } => {
                Err(invalid_transition(&self.state, "Disconnected"))
            }
        }
    }

    fn transition(&mut self, next: ConnectionState) {
        let previous = core::mem::replace(&mut self.state, next);
        let event = ConnectionEvent::new(previous, self.state.clone());

        for observer in &mut self.observers {
            observer(&event);
        }
    }
}

impl Default for ConnectionLifecycle {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ConnectionLifecycle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectionLifecycle")
            .field("state", &self.state)
            .field("observers", &self.observers.len())
            .finish()
    }
}

fn invalid_transition(previous: &ConnectionState, requested: &str) -> SdkError {
    connection_error(format!(
        "invalid connection transition from {previous:?} to {requested}"
    ))
}

const fn connection_error(description: String) -> SdkError {
    SdkError::Connection { description }
}

#[cfg(test)]
mod tests;
