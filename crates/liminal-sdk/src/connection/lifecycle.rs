#![allow(clippy::module_name_repetitions)]

use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;
use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;

use futures_core::Stream;

use crate::SdkError;

/// Application-visible lifecycle state for a remote SDK connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnectionState {
    /// The SDK is establishing a connection.
    Connecting,
    /// The SDK has an active connection.
    Connected,
    /// The SDK is attempting to reconnect after a disruption.
    Reconnecting {
        /// Zero-based reconnect attempt counter for this disruption.
        attempt: u32,
    },
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

/// Configures exponential reconnect backoff.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReconnectConfig {
    /// Initial retry delay before exponential growth is applied.
    pub base_delay: Duration,
    /// Maximum delay used for the exponential component before jitter is added.
    pub max_delay: Duration,
}

impl ReconnectConfig {
    /// Creates a reconnect configuration from base and maximum delays.
    #[must_use]
    pub const fn new(base_delay: Duration, max_delay: Duration) -> Self {
        Self {
            base_delay,
            max_delay,
        }
    }

    /// Computes `min(base_delay * 2^attempt, max_delay)` before jitter.
    #[must_use]
    pub fn capped_delay(self, attempt: u32) -> Duration {
        let base_nanos = self.base_delay.as_nanos();
        let max_nanos = self.max_delay.as_nanos();

        if base_nanos == 0 || max_nanos == 0 {
            return Duration::ZERO;
        }

        let multiplier = 1_u128.checked_shl(attempt).unwrap_or(u128::MAX);
        let scaled_nanos = base_nanos.saturating_mul(multiplier);
        duration_from_nanos(core::cmp::min(scaled_nanos, max_nanos))
    }

    /// Computes the retry delay for an attempt using an injected random jitter source.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] if the jitter source returns a value above 50% of the
    /// capped exponential delay.
    pub fn retry_delay<J>(self, attempt: u32, jitter: &mut J) -> Result<Duration, SdkError>
    where
        J: ReconnectJitter + ?Sized,
    {
        let capped_delay = self.capped_delay(attempt);
        let jitter_delay = jitter.jitter(attempt, capped_delay);
        self.retry_delay_with_jitter(attempt, jitter_delay)
    }

    /// Computes the retry delay for an attempt using a precomputed jitter value.
    ///
    /// This helper is useful for deterministic tests and for transport layers
    /// that produce randomness externally. The jitter value must be between zero
    /// and 50% of the capped exponential delay.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] if `jitter` is greater than 50% of the capped delay.
    pub fn retry_delay_with_jitter(
        self,
        attempt: u32,
        jitter: Duration,
    ) -> Result<Duration, SdkError> {
        let capped_delay = self.capped_delay(attempt);
        let jitter_limit = capped_delay / 2;

        if jitter > jitter_limit {
            return Err(connection_error(format!(
                "reconnect jitter {jitter:?} exceeds 50% limit {jitter_limit:?}"
            )));
        }

        Ok(capped_delay.checked_add(jitter).unwrap_or(Duration::MAX))
    }
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self::new(Duration::from_millis(100), Duration::from_secs(30))
    }
}

/// Source of random reconnect jitter.
///
/// Implementations must return a random value from zero through 50% of the
/// supplied capped delay. The SDK validates that upper bound before using the
/// value so reconnection never falls back to a fixed retry interval.
pub trait ReconnectJitter: fmt::Debug {
    /// Returns random jitter for a reconnect attempt and capped base delay.
    fn jitter(&mut self, attempt: u32, capped_delay: Duration) -> Duration;
}

type ConnectionObserver = Box<dyn FnMut(&ConnectionEvent) + Send>;

/// Owns the SDK connection lifecycle state and emits validated transitions.
pub struct ConnectionLifecycle {
    state: ConnectionState,
    reconnect_config: ReconnectConfig,
    next_reconnect_attempt: u32,
    observers: Vec<ConnectionObserver>,
}

impl ConnectionLifecycle {
    /// Creates a lifecycle in the [`ConnectionState::Connecting`] state.
    #[must_use]
    pub fn new(reconnect_config: ReconnectConfig) -> Self {
        Self {
            state: ConnectionState::Connecting,
            reconnect_config,
            next_reconnect_attempt: 0,
            observers: Vec::new(),
        }
    }

    /// Returns the current connection state.
    #[must_use]
    pub const fn state(&self) -> &ConnectionState {
        &self.state
    }

    /// Returns the reconnect backoff configuration.
    #[must_use]
    pub const fn reconnect_config(&self) -> ReconnectConfig {
        self.reconnect_config
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
            ConnectionState::Connecting | ConnectionState::Reconnecting { .. } => {
                self.next_reconnect_attempt = 0;
                self.transition(ConnectionState::Connected);
                Ok(())
            }
            _ => Err(invalid_transition(&self.state, "Connected")),
        }
    }

    /// Transitions to reconnecting and returns the next retry delay.
    ///
    /// The first reconnect attempt after a successful connection uses attempt
    /// zero. Each subsequent reconnect attempt increments the counter until a
    /// successful [`Self::connected`] transition resets it.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] when reconnecting from the current state is invalid or
    /// when the jitter source exceeds the allowed jitter range.
    pub fn reconnect<J>(&mut self, jitter: &mut J) -> Result<Duration, SdkError>
    where
        J: ReconnectJitter + ?Sized,
    {
        match self.state {
            ConnectionState::Connecting
            | ConnectionState::Connected
            | ConnectionState::Reconnecting { .. } => {
                let attempt = self.next_reconnect_attempt;
                let delay = self.reconnect_config.retry_delay(attempt, jitter)?;
                self.next_reconnect_attempt = attempt.saturating_add(1);
                self.transition(ConnectionState::Reconnecting { attempt });
                Ok(delay)
            }
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
            | ConnectionState::Reconnecting { .. } => {
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
        Self::new(ReconnectConfig::default())
    }
}

impl fmt::Debug for ConnectionLifecycle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectionLifecycle")
            .field("state", &self.state)
            .field("reconnect_config", &self.reconnect_config)
            .field("next_reconnect_attempt", &self.next_reconnect_attempt)
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

fn duration_from_nanos(nanos: u128) -> Duration {
    const NANOS_PER_SECOND: u128 = 1_000_000_000;

    let seconds = nanos / NANOS_PER_SECOND;
    let subsecond_nanos = nanos % NANOS_PER_SECOND;

    let Ok(seconds) = u64::try_from(seconds) else {
        return Duration::MAX;
    };
    let Ok(subsecond_nanos) = u32::try_from(subsecond_nanos) else {
        return Duration::MAX;
    };

    Duration::new(seconds, subsecond_nanos)
}

#[cfg(test)]
mod tests;
