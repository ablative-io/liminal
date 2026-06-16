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
    #[error("listener bind failed: {message}")]
    ListenerBind { message: String },

    /// The server listener failed while accepting an inbound connection.
    #[error("listener accept failed: {message}")]
    ListenerAccept { message: String },

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
