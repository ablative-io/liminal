use alloc::format;
use alloc::vec::Vec;

use crate::SdkError;

use super::{
    ConnectionEvent, ConnectionLifecycle, ReconnectConfig, ResumeRequest, SubscriptionId,
    SubscriptionRecovery,
};

/// Caller-supplied connection pool sizing and resource configuration.
///
/// This type deliberately has no [`Default`] implementation: pool sizing must be
/// supplied by the caller, builder, or runtime so the SDK never bakes in a hidden
/// connection-count default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConnectionPoolConfig {
    /// Maximum number of remote connections managed by this pool.
    pub max_connections: usize,
    /// Per-connection operation timeout, in milliseconds.
    pub timeout_millis: u64,
    /// Per-connection inbound buffer size.
    pub buffer_size: usize,
}

impl ConnectionPoolConfig {
    /// Creates pool configuration from caller-supplied values.
    #[must_use]
    pub const fn new(max_connections: usize, timeout_millis: u64, buffer_size: usize) -> Self {
        Self {
            max_connections,
            timeout_millis,
            buffer_size,
        }
    }

    /// Validates caller-supplied pool configuration.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] when no connection can be allocated.
    pub fn validate(self) -> Result<Self, SdkError> {
        if self.max_connections == 0 {
            return Err(SdkError::Connection {
                description: "connection pool max_connections must be greater than zero".into(),
            });
        }

        Ok(self)
    }
}

/// Stable identifier for an internally managed pooled connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PoolConnectionId(usize);

impl PoolConnectionId {
    /// Creates a pooled connection identifier.
    #[must_use]
    pub const fn new(value: usize) -> Self {
        Self(value)
    }

    /// Returns the numeric connection slot.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0
    }
}

/// Assignment returned when a subscription is placed on a pooled connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubscriptionAssignment {
    /// Subscription assigned to the pool.
    pub subscription_id: SubscriptionId,
    /// Connection that owns the subscription.
    pub connection_id: PoolConnectionId,
}

#[derive(Debug)]
struct PoolConnection {
    id: PoolConnectionId,
    subscription_count: usize,
    lifecycle: ConnectionLifecycle,
    recovery: SubscriptionRecovery,
}

impl PoolConnection {
    fn new(id: PoolConnectionId, reconnect_config: ReconnectConfig) -> Self {
        Self {
            id,
            subscription_count: 0,
            lifecycle: ConnectionLifecycle::new(reconnect_config),
            recovery: SubscriptionRecovery::new(),
        }
    }
}

/// Configurable pool for remote SDK connections.
#[derive(Debug)]
pub struct ConnectionPool {
    config: ConnectionPoolConfig,
    connections: Vec<PoolConnection>,
}

impl ConnectionPool {
    /// Creates a pool with exactly the caller-supplied connection count.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] if the supplied configuration is invalid.
    pub fn new(
        config: ConnectionPoolConfig,
        reconnect_config: ReconnectConfig,
    ) -> Result<Self, SdkError> {
        let config = config.validate()?;
        let mut connections = Vec::with_capacity(config.max_connections);

        for slot in 0..config.max_connections {
            connections.push(PoolConnection::new(
                PoolConnectionId::new(slot),
                reconnect_config,
            ));
        }

        Ok(Self {
            config,
            connections,
        })
    }

    /// Returns the caller-supplied pool configuration.
    #[must_use]
    pub const fn config(&self) -> ConnectionPoolConfig {
        self.config
    }

    /// Returns the caller-supplied maximum connection count.
    #[must_use]
    pub const fn max_connections(&self) -> usize {
        self.config.max_connections
    }

    /// Returns the number of managed connection slots.
    #[must_use]
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    /// Assigns a subscription to the least-loaded pooled connection.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] if the pool has no available connection entries.
    pub fn assign_subscription(
        &mut self,
        subscription_id: SubscriptionId,
    ) -> Result<SubscriptionAssignment, SdkError> {
        if let Some(existing) = self.connection_for_subscription(subscription_id) {
            return Ok(SubscriptionAssignment {
                subscription_id,
                connection_id: existing,
            });
        }

        let connection = self
            .connections
            .iter_mut()
            .min_by_key(|connection| (connection.subscription_count, connection.id))
            .ok_or_else(|| SdkError::Connection {
                description: "connection pool has no connections".into(),
            })?;

        connection.subscription_count = connection.subscription_count.saturating_add(1);
        connection.recovery.track_subscription(subscription_id);

        Ok(SubscriptionAssignment {
            subscription_id,
            connection_id: connection.id,
        })
    }

    /// Records an acknowledged sequence for the connection that owns a subscription.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] when the subscription is not active in this pool.
    pub fn acknowledge(
        &mut self,
        subscription_id: SubscriptionId,
        sequence: u64,
    ) -> Result<(), SdkError> {
        let connection = self.connection_for_subscription_mut(subscription_id)?;
        connection.recovery.acknowledge(subscription_id, sequence);
        Ok(())
    }

    /// Removes a subscription assignment and its recovery state.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] when the subscription is not active in this pool.
    pub fn unsubscribe(&mut self, subscription_id: SubscriptionId) -> Result<(), SdkError> {
        let connection = self.connection_for_subscription_mut(subscription_id)?;
        connection.recovery.unsubscribe(subscription_id);
        connection.subscription_count = connection.subscription_count.saturating_sub(1);
        Ok(())
    }

    /// Builds subscription resume requests for every pooled connection on reconnect.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] if any active subscription cannot compute a resume sequence.
    pub fn resume_requests_for_transition(
        &self,
        event: &ConnectionEvent,
    ) -> Result<Vec<ResumeRequest>, SdkError> {
        let mut requests = Vec::new();
        for connection in &self.connections {
            requests.extend(connection.recovery.resume_requests_for_transition(event)?);
        }
        Ok(requests)
    }

    /// Returns the connection assigned to a subscription, if it is active.
    #[must_use]
    pub fn connection_for_subscription(
        &self,
        subscription_id: SubscriptionId,
    ) -> Option<PoolConnectionId> {
        self.connections
            .iter()
            .find(|connection| connection.recovery.is_active(subscription_id))
            .map(|connection| connection.id)
    }

    /// Returns the number of active subscriptions on a connection.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] if the connection identifier is not part of this pool.
    pub fn subscription_count(&self, connection_id: PoolConnectionId) -> Result<usize, SdkError> {
        self.connection(connection_id)
            .map(|connection| connection.subscription_count)
    }

    /// Returns recovery state for a pooled connection.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] if the connection identifier is not part of this pool.
    pub fn recovery(
        &self,
        connection_id: PoolConnectionId,
    ) -> Result<&SubscriptionRecovery, SdkError> {
        self.connection(connection_id)
            .map(|connection| &connection.recovery)
    }

    /// Returns lifecycle state for a pooled connection.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] if the connection identifier is not part of this pool.
    pub fn lifecycle(
        &self,
        connection_id: PoolConnectionId,
    ) -> Result<&ConnectionLifecycle, SdkError> {
        self.connection(connection_id)
            .map(|connection| &connection.lifecycle)
    }

    fn connection(&self, connection_id: PoolConnectionId) -> Result<&PoolConnection, SdkError> {
        self.connections
            .iter()
            .find(|connection| connection.id == connection_id)
            .ok_or_else(|| SdkError::Connection {
                description: format!("unknown pooled connection {}", connection_id.get()),
            })
    }

    fn connection_for_subscription_mut(
        &mut self,
        subscription_id: SubscriptionId,
    ) -> Result<&mut PoolConnection, SdkError> {
        self.connections
            .iter_mut()
            .find(|connection| connection.recovery.is_active(subscription_id))
            .ok_or_else(|| SdkError::Connection {
                description: format!("unknown subscription {}", subscription_id.get()),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::{ConnectionState, DisconnectReason};

    #[test]
    fn invalid_pool_size_is_rejected() {
        let config = ConnectionPoolConfig::new(0, 10, 16);

        assert!(ConnectionPool::new(config, ReconnectConfig::default()).is_err());
    }

    #[test]
    fn subscriptions_are_distributed_across_connections() -> Result<(), SdkError> {
        let config = ConnectionPoolConfig::new(2, 10, 16);
        let mut pool = ConnectionPool::new(config, ReconnectConfig::default())?;

        let first = pool.assign_subscription(SubscriptionId::new(1))?;
        let second = pool.assign_subscription(SubscriptionId::new(2))?;
        let third = pool.assign_subscription(SubscriptionId::new(3))?;

        assert_ne!(first.connection_id, second.connection_id);
        assert_eq!(third.connection_id, first.connection_id);
        assert_eq!(pool.subscription_count(first.connection_id)?, 2);
        assert_eq!(pool.subscription_count(second.connection_id)?, 1);
        Ok(())
    }

    #[test]
    fn multiple_subscriptions_share_configured_connections() -> Result<(), SdkError> {
        let config = ConnectionPoolConfig::new(1, 20, 32);
        let mut pool = ConnectionPool::new(config, ReconnectConfig::default())?;

        let first = pool.assign_subscription(SubscriptionId::new(10))?;
        let second = pool.assign_subscription(SubscriptionId::new(11))?;

        assert_eq!(first.connection_id, second.connection_id);
        assert_eq!(pool.max_connections(), 1);
        assert_eq!(pool.subscription_count(first.connection_id)?, 2);
        Ok(())
    }

    #[test]
    fn pooled_recovery_builds_resume_requests_on_reconnect() -> Result<(), SdkError> {
        let config = ConnectionPoolConfig::new(2, 10, 16);
        let mut pool = ConnectionPool::new(config, ReconnectConfig::default())?;
        let first = SubscriptionId::new(21);
        let second = SubscriptionId::new(22);
        let event = ConnectionEvent::new(
            ConnectionState::Reconnecting { attempt: 0 },
            ConnectionState::Connected,
        );

        pool.assign_subscription(first)?;
        pool.assign_subscription(second)?;
        pool.acknowledge(first, 4)?;

        let requests = pool.resume_requests_for_transition(&event)?;

        assert_eq!(
            requests,
            vec![ResumeRequest::new(first, 5), ResumeRequest::new(second, 0)]
        );
        Ok(())
    }

    #[test]
    fn unsubscribe_removes_assignment() -> Result<(), SdkError> {
        let config = ConnectionPoolConfig::new(2, 10, 16);
        let mut pool = ConnectionPool::new(config, ReconnectConfig::default())?;
        let subscription_id = SubscriptionId::new(31);
        let assignment = pool.assign_subscription(subscription_id)?;

        pool.unsubscribe(subscription_id)?;

        assert_eq!(pool.connection_for_subscription(subscription_id), None);
        assert_eq!(pool.subscription_count(assignment.connection_id)?, 0);
        assert!(pool.unsubscribe(subscription_id).is_err());
        Ok(())
    }

    #[test]
    fn non_reconnect_transition_does_not_resume() -> Result<(), SdkError> {
        let config = ConnectionPoolConfig::new(2, 10, 16);
        let mut pool = ConnectionPool::new(config, ReconnectConfig::default())?;
        let event = ConnectionEvent::new(
            ConnectionState::Connected,
            ConnectionState::Disconnected {
                reason: DisconnectReason::Normal,
            },
        );

        pool.assign_subscription(SubscriptionId::new(41))?;

        assert!(pool.resume_requests_for_transition(&event)?.is_empty());
        Ok(())
    }
}
