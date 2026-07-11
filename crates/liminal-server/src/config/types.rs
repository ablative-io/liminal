use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crate::ServerError;

/// Declarative configuration for the standalone liminal server wrapper.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// Socket address where the standalone server will listen for client traffic.
    pub listen_address: SocketAddr,
    /// Socket address where the health endpoint server will listen for probes.
    pub health_listen_address: SocketAddr,
    /// Maximum time to allow existing connections to drain during graceful shutdown.
    pub drain_timeout_ms: u64,
    /// Channel topology definitions declared by the operator.
    pub channels: Vec<ChannelDef>,
    /// Declarative routing rules that connect configured channels.
    pub routing_rules: Vec<RoutingRuleDef>,
    /// Optional filesystem location for durable server state.
    pub persistence_path: Option<PathBuf>,
    /// Optional beamr distribution cluster membership configuration.
    pub cluster: Option<ClusterConfig>,
    /// Optional connection authentication configuration.
    ///
    /// When present, every client `Connect` handshake must carry a matching
    /// `auth_token`; when absent the server is open (byte-identical to the
    /// pre-auth behaviour). Not an ACL system — a single shared bearer token.
    #[serde(default)]
    pub auth: Option<AuthConfig>,
    /// Service construction profile. Absent `[services]` (or an absent `profile`
    /// key within it) defaults to `"full"`, so existing deployments build exactly
    /// what they build today.
    #[serde(default)]
    pub services: ServicesConfig,
    /// Operational bounds (§5). Absent `[limits]` (or any absent key within it)
    /// defaults to the certifying-pair-signed numbers, so an operator who sets
    /// nothing still runs bounded — "unlimited-by-silence is no longer a legal
    /// state" (§5). Every value is a hard cap enforced by a typed refusal; a
    /// zero (or otherwise invalid) value is a config validation error, never a
    /// silent "unlimited".
    #[serde(default)]
    pub limits: LimitsConfig,
}

impl ServerConfig {
    /// Returns the configured graceful-shutdown drain timeout.
    #[must_use]
    pub const fn drain_timeout(&self) -> Duration {
        Duration::from_millis(self.drain_timeout_ms)
    }
}

/// Declarative channel definition loaded from server configuration.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelDef {
    /// Unique channel name used by routing rules and operators.
    pub name: String,
    /// Filesystem path to a JSON Schema document that validates every message
    /// published to this channel.
    ///
    /// The path is resolved relative to the directory containing the config file
    /// (absolute paths are used verbatim). Config validation reads and parses the
    /// referenced document and stores the result in [`Self::loaded_schema`]; a
    /// missing file, an unreadable file, or a file that is not valid JSON is an
    /// accumulated validation error that stops startup.
    ///
    /// `None` means the channel has no schema: it keeps the permissive empty
    /// schema (`{}`) that accepts any JSON payload.
    #[serde(default)]
    pub schema_ref: Option<PathBuf>,
    /// Whether this channel requires durable persistence.
    pub durable: bool,
    /// Schema document loaded and parsed from [`Self::schema_ref`] during config
    /// validation. Populated only by [`crate::config::validate`]; a directly
    /// constructed [`ChannelDef`] that skips validation carries `None` here and is
    /// therefore built with the permissive empty schema regardless of
    /// [`Self::schema_ref`]. Never deserialized from the config file.
    #[serde(skip)]
    pub loaded_schema: Option<LoadedSchema>,
}

/// A channel's JSON Schema document as loaded from disk during config validation.
///
/// Carries both the parsed document (fed to the validation engine when the channel
/// is built) and the raw file bytes (hashed into the protocol schema id advertised
/// at subscribe time, so an SDK deriving ids from the same schema bytes converges).
#[derive(Debug, Clone)]
pub struct LoadedSchema {
    /// Raw bytes of the schema file, hashed to derive the protocol schema id.
    pub bytes: Vec<u8>,
    /// Parsed JSON Schema document, fed to the channel's validation engine.
    pub document: serde_json::Value,
}

/// Declarative routing rule definition loaded from server configuration.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingRuleDef {
    /// Source channel name from which messages are routed.
    pub source_channel: String,
    /// Target channel name to which matching messages are routed.
    pub target_channel: String,
    /// Optional predicate expression that filters routed messages.
    pub predicate: Option<String>,
}

/// Default beamr distribution handshake cookie, used when the operator does not
/// configure one. Mirrors beamr's own [`beamr::distribution::DEFAULT_COOKIE`].
pub const DEFAULT_COOKIE: &str = "beamr-cookie";

/// Beamr distribution cluster configuration for standalone deployment.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    /// Unique node name advertised to the beamr distribution cluster.
    pub node_name: String,
    /// Socket address this node binds for inbound distribution links from peers.
    ///
    /// This is distinct from [`ServerConfig::listen_address`] (the client wire
    /// port): a clustered node listens on two ports — one for clients, one for
    /// peer distribution traffic.
    pub listen_address: SocketAddr,
    /// Seed node socket addresses used to join an existing cluster.
    pub seed_nodes: Vec<SocketAddr>,
    /// Shared distribution handshake cookie. Every node in a cluster MUST use the
    /// same cookie or the OTP handshake is rejected. Defaults to
    /// [`DEFAULT_COOKIE`] when omitted.
    #[serde(default = "default_cookie")]
    pub cookie: String,
}

fn default_cookie() -> String {
    DEFAULT_COOKIE.to_owned()
}

/// Connection authentication configuration.
///
/// A single shared bearer token compared (constant-time) against the `auth_token`
/// carried on every client `Connect` handshake. This is the table-stakes access
/// gate, not an ACL system: one token grants full access, its absence (no `[auth]`
/// section) leaves the server open.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    /// Shared secret token a client must present in its `Connect` handshake. Must
    /// be non-empty when the `[auth]` section is present (an empty token is a
    /// config validation error, since it would gate nothing).
    pub token: String,
}

/// Service construction profile selection (D2).
///
/// The `profile` value is carried as a raw string here rather than a typed enum so
/// an unrecognised value is a config *validation* error with a helpful message
/// (via [`Self::profile`]) rather than an opaque deserialization failure — matching
/// how every other semantic config check surfaces. Absent `profile` defaults to
/// `"full"`.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServicesConfig {
    /// Construction profile: `"full"` (the default, unchanged behaviour) or
    /// `"worker-front-door"` (capability-scoped worker deployments).
    #[serde(default = "default_service_profile")]
    pub profile: String,
}

impl Default for ServicesConfig {
    fn default() -> Self {
        Self {
            profile: default_service_profile(),
        }
    }
}

impl ServicesConfig {
    /// Resolves the raw `profile` string into a typed [`ServiceProfile`].
    ///
    /// # Errors
    /// Returns [`ServerError::ConfigValidation`] when the value is not a recognised
    /// profile.
    pub fn profile(&self) -> Result<ServiceProfile, ServerError> {
        ServiceProfile::parse(&self.profile)
    }
}

fn default_service_profile() -> String {
    ServiceProfile::FULL.to_owned()
}

/// Operational bounds (§5, scout Q4 — rule-2 items).
///
/// Each field is a hard per-scope cap with a typed refusal and a
/// certifying-pair-signed default (the numbers below are §5's). The struct is
/// the single wire surface for `[limits]`; [`LimitsConfig::validate`] rejects any
/// zero value as a typed config error (a zero cap would gate nothing — the exact
/// unlimited-by-silence state §5 outlaws). Defaults come from the `default_*`
/// free functions so an absent key resolves to the signed number, not zero.
#[derive(Debug, Clone, Copy, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitsConfig {
    /// Total live connections the listener admits before refusing (§5: 256 — a
    /// worker-bus, an order of magnitude above any observed fleet).
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    /// Subscriptions one connection may hold (§5: 32).
    #[serde(default = "default_max_subscriptions_per_connection")]
    pub max_subscriptions_per_connection: usize,
    /// Open conversations one connection may hold (§5: 32).
    #[serde(default = "default_max_conversations_per_connection")]
    pub max_conversations_per_connection: usize,
    /// In-flight server→client correlated pushes per connection (§5: 32).
    #[serde(default = "default_max_pending_pushes_per_connection")]
    pub max_pending_pushes_per_connection: usize,
    /// Entries in the per-connection pending-reply table (§1.2(3b)/§5: 32 —
    /// distinct from server-push slots).
    #[serde(default = "default_max_pending_conversation_replies_per_connection")]
    pub max_pending_conversation_replies_per_connection: usize,
    /// Per-conversation sub-cap that confines tombstone ambiguity to its own
    /// conversation (§1.2(3b)/§5: 8). Pending entries count against BOTH this and
    /// the connection table; tombstones against THIS alone.
    #[serde(default = "default_max_pending_replies_per_conversation")]
    pub max_pending_replies_per_conversation: usize,
    /// One shared inbox-byte budget per connection, spent across ALL its
    /// subscription inboxes (§5: 4 MiB — deliberately mirroring the outbound 4 MiB
    /// bound). Accounting unit: serialized envelope bytes as admitted, charged at
    /// enqueue and released at dequeue.
    #[serde(default = "default_max_connection_inbox_bytes")]
    pub max_connection_inbox_bytes: usize,
    /// Per-inbox envelope-count secondary fairness trip (§5: 256) — stops one
    /// subscription starving its siblings inside the shared byte budget; no longer
    /// load-bearing for the signed bound.
    #[serde(default = "default_max_subscription_inbox_depth")]
    pub max_subscription_inbox_depth: usize,
}

impl LimitsConfig {
    /// §5 default: total live connections before the listener refuses.
    pub const DEFAULT_MAX_CONNECTIONS: usize = 256;
    /// §5 default: subscriptions per connection.
    pub const DEFAULT_MAX_SUBSCRIPTIONS_PER_CONNECTION: usize = 32;
    /// §5 default: open conversations per connection.
    pub const DEFAULT_MAX_CONVERSATIONS_PER_CONNECTION: usize = 32;
    /// §5 default: in-flight server pushes per connection.
    pub const DEFAULT_MAX_PENDING_PUSHES_PER_CONNECTION: usize = 32;
    /// §5 default: pending-reply table entries per connection.
    pub const DEFAULT_MAX_PENDING_CONVERSATION_REPLIES_PER_CONNECTION: usize = 32;
    /// §5 default: per-conversation pending-reply sub-cap.
    pub const DEFAULT_MAX_PENDING_REPLIES_PER_CONVERSATION: usize = 8;
    /// §5 default: shared per-connection inbox byte budget (4 MiB).
    pub const DEFAULT_MAX_CONNECTION_INBOX_BYTES: usize = 4 * 1024 * 1024;
    /// §5 default: per-inbox envelope-count fairness trip.
    pub const DEFAULT_MAX_SUBSCRIPTION_INBOX_DEPTH: usize = 256;

    /// Validates the caps: every value must be non-zero (a zero cap gates nothing
    /// — the unlimited-by-silence state §5 outlaws). Errors are accumulated into
    /// `errors` (one per offending field) so an operator sees every bad cap at
    /// once, matching the rest of config validation.
    pub(crate) fn collect_errors(&self, errors: &mut Vec<String>) {
        let checks: [(&str, usize); 8] = [
            ("max_connections", self.max_connections),
            (
                "max_subscriptions_per_connection",
                self.max_subscriptions_per_connection,
            ),
            (
                "max_conversations_per_connection",
                self.max_conversations_per_connection,
            ),
            (
                "max_pending_pushes_per_connection",
                self.max_pending_pushes_per_connection,
            ),
            (
                "max_pending_conversation_replies_per_connection",
                self.max_pending_conversation_replies_per_connection,
            ),
            (
                "max_pending_replies_per_conversation",
                self.max_pending_replies_per_conversation,
            ),
            (
                "max_connection_inbox_bytes",
                self.max_connection_inbox_bytes,
            ),
            (
                "max_subscription_inbox_depth",
                self.max_subscription_inbox_depth,
            ),
        ];
        for (field, value) in checks {
            if value == 0 {
                errors.push(format!(
                    "limits.{field}: must be greater than zero (a zero cap would be \
                     unlimited-by-silence, which §5 forbids)"
                ));
            }
        }
    }
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_connections: default_max_connections(),
            max_subscriptions_per_connection: default_max_subscriptions_per_connection(),
            max_conversations_per_connection: default_max_conversations_per_connection(),
            max_pending_pushes_per_connection: default_max_pending_pushes_per_connection(),
            max_pending_conversation_replies_per_connection:
                default_max_pending_conversation_replies_per_connection(),
            max_pending_replies_per_conversation: default_max_pending_replies_per_conversation(),
            max_connection_inbox_bytes: default_max_connection_inbox_bytes(),
            max_subscription_inbox_depth: default_max_subscription_inbox_depth(),
        }
    }
}

const fn default_max_connections() -> usize {
    LimitsConfig::DEFAULT_MAX_CONNECTIONS
}
const fn default_max_subscriptions_per_connection() -> usize {
    LimitsConfig::DEFAULT_MAX_SUBSCRIPTIONS_PER_CONNECTION
}
const fn default_max_conversations_per_connection() -> usize {
    LimitsConfig::DEFAULT_MAX_CONVERSATIONS_PER_CONNECTION
}
const fn default_max_pending_pushes_per_connection() -> usize {
    LimitsConfig::DEFAULT_MAX_PENDING_PUSHES_PER_CONNECTION
}
const fn default_max_pending_conversation_replies_per_connection() -> usize {
    LimitsConfig::DEFAULT_MAX_PENDING_CONVERSATION_REPLIES_PER_CONNECTION
}
const fn default_max_pending_replies_per_conversation() -> usize {
    LimitsConfig::DEFAULT_MAX_PENDING_REPLIES_PER_CONVERSATION
}
const fn default_max_connection_inbox_bytes() -> usize {
    LimitsConfig::DEFAULT_MAX_CONNECTION_INBOX_BYTES
}
const fn default_max_subscription_inbox_depth() -> usize {
    LimitsConfig::DEFAULT_MAX_SUBSCRIPTION_INBOX_DEPTH
}

/// Which connection-services adapter the server constructs (D2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceProfile {
    /// Full channel/conversation/durability services — the default. Constructs the
    /// haematite store, channel supervisor, conversation supervisor, and dedup
    /// cache exactly as before.
    Full,
    /// Capability-scoped worker front door: the connection supervisor only, with no
    /// channel/conversation/haematite machinery. Backs worker registration,
    /// correlated push/reply, and notifier-consumed reserved publishes; ordinary
    /// channel and conversation frames are rejected with a typed error frame.
    WorkerFrontDoor,
}

impl ServiceProfile {
    /// Config value selecting the full-service profile.
    pub const FULL: &'static str = "full";
    /// Config value selecting the worker-front-door profile.
    pub const WORKER_FRONT_DOOR: &'static str = "worker-front-door";

    /// Parses a `[services] profile` value into a typed profile.
    ///
    /// # Errors
    /// Returns [`ServerError::ConfigValidation`] for any value other than
    /// [`Self::FULL`] or [`Self::WORKER_FRONT_DOOR`].
    pub fn parse(value: &str) -> Result<Self, ServerError> {
        match value {
            Self::FULL => Ok(Self::Full),
            Self::WORKER_FRONT_DOOR => Ok(Self::WorkerFrontDoor),
            other => Err(ServerError::ConfigValidation {
                message: format!(
                    "services.profile: unknown profile '{other}'; expected \"{}\" or \"{}\"",
                    Self::FULL,
                    Self::WORKER_FRONT_DOOR
                ),
            }),
        }
    }
}
