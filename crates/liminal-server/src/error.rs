use std::net::SocketAddr;

/// Error taxonomy for standalone liminal server deployment failures.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// The configuration file could not be read or parsed.
    #[error("configuration load failed: {message}")]
    ConfigLoad { message: String },

    /// The configuration file was read but failed semantic validation.
    #[error("configuration validation failed: {message}")]
    ConfigValidation { message: String },

    /// The server could not bind its configured listener address.
    #[error("listener bind failed for {address}: {source}")]
    ListenerBind {
        /// Address the server attempted to bind.
        address: SocketAddr,
        /// Underlying operating-system bind failure.
        #[source]
        source: std::io::Error,
    },

    /// The server listener failed while accepting an inbound connection.
    #[error("listener accept failed: {message}")]
    ListenerAccept { message: String },

    /// A server→client push reply slot was dropped before a correlated reply
    /// arrived — the connection closed (the prompt worker-death signal). Distinct
    /// from [`Self::PushReplyTimeout`] so consumers can tell a worker that DIED
    /// (fast failover) from one that is merely SLOW, by type rather than message.
    #[error(
        "push correlation {correlation_id} did not complete: the connection closed before sending a correlated push reply"
    )]
    PushReplyDisconnected {
        /// Correlation id of the push whose reply will never arrive.
        correlation_id: u64,
    },

    /// A server→client push reply did not arrive within the awaiter's timeout —
    /// the worker is still connected but did not reply in time.
    #[error(
        "push correlation {correlation_id} did not complete: no correlated push reply arrived within the timeout"
    )]
    PushReplyTimeout {
        /// Correlation id of the push that timed out.
        correlation_id: u64,
    },

    /// The server could not join the configured beamr distribution cluster.
    #[error("cluster join failed: {message}")]
    ClusterJoin { message: String },

    /// Cluster state propagation through beamr distribution failed.
    #[error("cluster sync failed: {message}")]
    ClusterSync { message: String },

    /// Graceful shutdown did not drain within the configured timeout.
    #[error("shutdown timed out: {message}")]
    ShutdownTimeout { message: String },

    /// Durable state could not be flushed during graceful shutdown.
    #[error("shutdown flush failed: {message}")]
    ShutdownFlush { message: String },

    /// The health endpoint failed to start or serve requests.
    #[error("health endpoint failed: {message}")]
    HealthEndpoint { message: String },
}
