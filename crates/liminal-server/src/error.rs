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

    /// Durable participant-incarnation startup or allocation failed before its
    /// result could be published.
    #[error("participant incarnation {phase} failed: {message}")]
    ParticipantIncarnation {
        /// Exact server seam that failed.
        phase: &'static str,
        /// Underlying durable or bounded-bridge diagnostic.
        message: String,
    },

    /// Startup found durable connection-fate work that the Decision A/C producer must complete
    /// before listener or semantic-service publication.
    #[error(
        "participant startup requires recovery of {open_count} connection-fate Opens beginning at sequence {first_open_sequence}"
    )]
    ConnectionFateRecoveryRequired {
        /// Number of replay-validated unmatched Opens.
        open_count: usize,
        /// Lowest unmatched Open sequence.
        first_open_sequence: u64,
    },

    /// The durable server-incarnation namespace has no successor.
    #[error("participant server-incarnation namespace is exhausted")]
    ServerIncarnationExhausted,

    /// Production participant startup restore failed: the durable
    /// conversation streams could not be scanned or replayed, so the
    /// server-scope capacity ledger cannot be made exact and the server
    /// refuses to start over state it cannot account for.
    #[error("participant startup restore failed: {message}")]
    ParticipantStartupRestore {
        /// Underlying durable, replay, or bridge diagnostic.
        message: String,
    },

    /// The current durable server incarnation has no collision-free connection
    /// ordinal left, so the accepted socket was not admitted.
    #[error(
        "connection incarnation exhausted for server incarnation {attempted_server_incarnation}"
    )]
    ConnectionIncarnationExhausted {
        /// Server incarnation whose complete ordinal suffix was examined.
        attempted_server_incarnation: u64,
    },

    /// A frame requested an operation the configured services profile does not
    /// serve (e.g. ordinary publish/subscribe/conversation traffic against the
    /// capability-scoped worker front door). Server-internal taxonomy, not wire
    /// vocabulary: the connection process renders it as the operation's existing
    /// typed error frame with this error's text as the message.
    #[error("{operation} is not supported by the {profile} services profile")]
    UnsupportedOperation {
        /// Human-readable description of the refused operation.
        operation: String,
        /// The configured services profile that refused it.
        profile: &'static str,
    },

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
    /// the worker is still connected but did not reply in this wait quantum. This
    /// is BENIGN: the reply slot survives untouched and the caller may re-arm
    /// [`PushReplyAwaiter::receive`](crate::server::connection::PushReplyAwaiter::receive)
    /// indefinitely. It is not a worker-death signal.
    #[error(
        "push correlation {correlation_id} did not complete: no correlated push reply arrived within the timeout"
    )]
    PushReplyTimeout {
        /// Correlation id of the push whose wait quantum elapsed.
        correlation_id: u64,
    },

    /// A server→client push carrying an explicit reply deadline (via
    /// [`push_to_connection_with_deadline`](crate::server::connection::ConnectionSupervisor::push_to_connection_with_deadline))
    /// reached that deadline before a correlated reply arrived. Unlike
    /// [`Self::PushReplyTimeout`] this is TERMINAL: the reply slot has been
    /// removed and its §5 `max_pending_pushes_per_connection` cap admission
    /// released. Returned PROMPTLY once the deadline is due — a `receive` call
    /// does not hold a due expiry until its caller's quantum ends, so the
    /// terminal outcome is independent of how the caller polls. Distinct
    /// variant so callers classify by type, not message.
    #[error(
        "push correlation {correlation_id} did not complete: its reply deadline passed before a correlated push reply arrived"
    )]
    PushReplyExpired {
        /// Correlation id of the push whose reply deadline passed.
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

    /// A new connection was refused because the configured `max_connections`
    /// cap (§5) is already reached. The listener drops the freshly accepted
    /// stream, refusing the connection, rather than admitting an unbounded fleet.
    #[error(
        "connection refused: the configured max_connections limit of {limit} live connections is reached"
    )]
    ConnectionLimitReached {
        /// The configured `max_connections` cap that was hit.
        limit: usize,
    },

    /// An admission-time per-connection cap (§5) refused an operation: too many
    /// subscriptions, conversations, in-flight pushes, or pending conversation
    /// replies on one connection. The connection process renders it as the
    /// operation's existing typed error frame with this text as the message.
    #[error("{operation} refused: the per-connection {cap} limit of {limit} is reached")]
    ConnectionCapReached {
        /// Human-readable description of the refused operation.
        operation: String,
        /// The name of the cap that refused it (a `limits.*` config key).
        cap: &'static str,
        /// The configured cap value that was hit.
        limit: usize,
    },
}
