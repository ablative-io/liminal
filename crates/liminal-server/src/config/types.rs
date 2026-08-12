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
    ///
    /// A fresh boot surfaces `ConfigValidation` unless this path's PARENT
    /// directory already exists: the haematite engine creates the store dir
    /// exactly one level below a pre-existing parent it can fence (never
    /// `create_dir_all` — deliberate upstream hardening). Create the parent;
    /// the engine fences into it.
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
    /// Optional WebSocket transport acceptor (LP-WS-TRANSPORT R1).
    ///
    /// When present the server binds a sibling WebSocket listener carrying the
    /// canonical liminal wire protocol (one binary message per canonical frame)
    /// alongside the main TCP listener. When absent NO HTTP/WebSocket listener
    /// is started and the server behaves byte-identically to the pre-WebSocket
    /// build. Every field inside is a deployment decision; the origin allow-list
    /// FAILS CLOSED (an absent or empty list refuses every Origin-bearing
    /// upgrade) and the keepalive ping interval is disabled unless explicitly
    /// configured.
    #[serde(default)]
    pub websocket: Option<WebSocketConfig>,
    /// Participant lifecycle activation (LP gap closure, Part B).
    ///
    /// When present the server installs the production participant semantic
    /// handler and advertises the participant capability bit on every
    /// connection. Every field inside is REQUIRED and carries NO default:
    /// participant lifecycle values are deployment decisions, and an absent
    /// field is a typed startup error rather than an assumed number. When the
    /// section is absent the participant capability stays disabled and the
    /// server behaves byte-identically to the pre-activation build.
    #[serde(default)]
    pub participant: Option<ParticipantConfig>,
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

/// WebSocket transport acceptor configuration (`[websocket]`, LP-WS-TRANSPORT R1).
///
/// The sibling WebSocket route is an explicit opt-in: the section itself must be
/// present for any HTTP/WebSocket listener to start, and inside it the listen
/// address and the single exact upgrade path are required with no defaults.
///
/// The deployment TLS contract (tear ruling Q1) is raw `ws://` behind a named
/// TLS-terminating proxy that owns public `wss://` and certificates; liminal
/// grows no TLS stack. Origin validation nonetheless belongs to this acceptor:
/// [`Self::allowed_origins`] is the explicit allow-list checked on every
/// Origin-bearing upgrade, and there is NO default list — absent or empty
/// configuration fails closed for browser-origin upgrades while a native client
/// that sends no `Origin` header may still upgrade (F6).
///
/// OPERATOR NOTE — the same deployment contract covers the pre-upgrade window
/// (domain-owner ruling, 2026-07-18): the fronting proxy must ALSO enforce
/// pre-upgrade read timeouts, handshake concurrency limits, and connection
/// rate limits. Between TCP accept and a completed WebSocket upgrade this
/// listener does not count the socket against `[limits] max_connections` and
/// applies no read deadline of its own (only the fixed request-head size
/// bound), so a deployment that exposes this port without the named proxy is
/// out of contract on untrusted networks. A named handshake read-deadline
/// config plus an in-flight handshake cap derived from the configured
/// `max_connections` value is the ledgered post-demo hardening.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebSocketConfig {
    /// Socket address the WebSocket acceptor binds. Required; distinct from the
    /// main wire listener, the health listener, and any cluster listener.
    pub listen_address: SocketAddr,
    /// The single exact HTTP request path that accepts WebSocket upgrades.
    /// Required; must start with `/`. Every other path — and every ordinary
    /// HTTP request — receives a small fixed non-success response and closes.
    pub path: String,
    /// Explicit browser-origin allow-list checked on every Origin-bearing
    /// upgrade (F6). Entries are compared byte-exact against the request's
    /// serialized `Origin` header value (RFC 6454 ASCII serialization, e.g.
    /// `https://app.example.com`). Absent or empty means NO browser origin is
    /// accepted (fail closed); native clients sending no `Origin` header are
    /// unaffected.
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    /// Q-A transport-liveness keepalive: the server-side WebSocket Ping
    /// interval in milliseconds. This is a precise LAW-1 carve-out — liveness
    /// pings never mint application events, never re-arm application state, and
    /// never serve as a source of truth; failure detection remains the socket's
    /// typed terminal events. The bound is one ping per interval per
    /// connection, so the idle cost is `interval x connection-count`. Absent
    /// means pings are DISABLED, accepting proxy-idle-disconnect churn as the
    /// documented consequence. A configured zero is a validation error.
    #[serde(default)]
    pub ping_interval_ms: Option<u64>,
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
    /// Per-inbox envelope-count secondary fairness trip — stops one subscription
    /// starving its siblings inside the shared byte budget. See
    /// [`LimitsConfig::DEFAULT_MAX_SUBSCRIPTION_INBOX_DEPTH`] for why the default
    /// is what it is; the short version is that this cap is the CRUDE bound and
    /// [`LimitsConfig::max_connection_inbox_bytes`] is the real one.
    #[serde(default = "default_max_subscription_inbox_depth")]
    pub max_subscription_inbox_depth: usize,
    /// Per-slice cap on `Deliver` frames one connection may enqueue across all of
    /// its subscriptions, before the scheduler moves on to its peers.
    ///
    /// Operator-visible because P0 #55 proved it decides whether a subscriber
    /// behind a burst survives — but see
    /// [`LimitsConfig::DEFAULT_DELIVERY_SLICE_BUDGET`] for why the DEFAULT is not
    /// raised. Raising it trades one starvation for another, and the measurement
    /// that showed it moves the outcome could not see the starvation it causes.
    #[serde(default = "default_delivery_slice_budget")]
    pub delivery_slice_budget: usize,
    /// Runtime-registered channels this deployment admits.
    ///
    /// # Why this cap departs the uniform pattern
    ///
    /// Every other field here carries `#[serde(default = "…")]` naming a §5
    /// constant, because each of those numbers is a signed §5 bound. **There is
    /// no signed §5 bound for channel count**, and inventing one is barred: a
    /// number nobody certified, presented in the same shape as eight numbers
    /// somebody did, is a forged citation. So the type is `Option<usize>` and
    /// the serde default is the ABSENCE itself (`None`), never a value —
    /// `#[serde(default)]` here resolves a missing key to "no bound declared",
    /// which is a different statement from any number.
    ///
    /// Nor may this be a `usize` with a large default: unbounded-by-default is
    /// not a bound, it is the gap wearing a number.
    ///
    /// `None` refuses every runtime channel registration with a typed error
    /// naming this key, so a deployment that wants runtime registration declares
    /// its own bound and a deployment that never registers needs no config
    /// change at all. The cap bounds RUNTIME-registered channels only:
    /// `[[channels]]` entries are the bound the operator already wrote.
    ///
    /// `Some(0)` is a validation error like every other zero cap here — see
    /// [`LimitsConfig::collect_errors`].
    #[serde(default)]
    pub max_channels: Option<usize>,
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
    /// Default per-inbox envelope-count fairness trip.
    ///
    /// # Why 4096 and not the §5-era 256 (P0 #55)
    ///
    /// A subscription inbox is bounded TWICE: by
    /// [`Self::DEFAULT_MAX_CONNECTION_INBOX_BYTES`] (4 MiB, shared across all of
    /// one connection's inboxes) and by this envelope COUNT. Memory is the real
    /// resource, so the byte budget is the bound that should bind and this one is
    /// meant to be a fairness backstop behind it.
    ///
    /// Which one binds first is decided by record size. For a connection holding a
    /// single subscription, the crossover — the record size at which the two caps
    /// trip together — is `4 MiB / depth`:
    ///
    /// | depth | crossover record size  | binds first for a 1 KiB record |
    /// |-------|------------------------|--------------------------------|
    /// | 256   | 4194304/256 = 16 KiB   | the COUNT (at 256 KiB, 6% of 4 MiB) |
    /// | 4096  | 4194304/4096 = 1 KiB   | the BYTES (as intended)        |
    ///
    /// At 256 the count cap therefore binds for every record smaller than 16 KiB —
    /// which is nearly all real traffic — and it binds at a small fraction of the
    /// memory the connection is actually permitted: at a 164-byte record, 256
    /// envelopes is 41 KiB, ~1% of the 4 MiB budget. A replay burst is thousands of
    /// SMALL records, which is precisely the shape that hits the crude bound while
    /// the real bound sits untouched. That is the P0: a live subscriber shed for
    /// exceeding a fairness trip it reached at 1% of its memory allowance.
    ///
    /// At 4096 the crossover falls to 1 KiB, so bytes bind for realistic records
    /// and this cap returns to backstop duty.
    ///
    /// Honest bound on that arithmetic: the byte budget is per CONNECTION and this
    /// cap is per SUBSCRIPTION, so with `S` subscriptions sharing one connection
    /// the effective crossover is `4 MiB / S / depth`. The table above is the `S=1`
    /// case. A connection holding many subscriptions of small records can still
    /// reach the count cap first — which is exactly the sibling-starvation case
    /// this cap exists to catch.
    pub const DEFAULT_MAX_SUBSCRIPTION_INBOX_DEPTH: usize = 4096;
    /// Default per-slice `Deliver` budget for one connection — the source of truth
    /// for [`crate::config::LimitsConfig::delivery_slice_budget`] and for the
    /// delivery pump's own `DELIVERY_SLICE_BUDGET`.
    ///
    /// # Why this is NOT raised (P0 #55)
    ///
    /// This is the cross-connection FAIRNESS bound: it caps how long one connection
    /// may hold a shared scheduler thread before its peers get a turn. Raising it
    /// makes a burst subscriber's own drain finish in fewer scheduling round trips,
    /// and the P0 #55 2x2 measured exactly that — 0/192 boots lost a subscriber at
    /// 256 against roughly half of them at 32.
    ///
    /// That experiment does not license raising the default. It ran two or three
    /// connections, so the peer starvation a raise would CAUSE was unobservable BY
    /// CONSTRUCTION: with no queue of waiting peers there is nothing for a longer
    /// slice to delay. What was measured is that this knob moves the outcome, not
    /// that moving it is safe.
    ///
    /// So the number stays 32 and becomes an operator DECISION instead: a
    /// deployment that knows its connection count and its burst shape can raise it
    /// deliberately. The default does not make that trade on anyone's behalf.
    /// The P0's actual fix is [`Self::DEFAULT_MAX_SUBSCRIPTION_INBOX_DEPTH`].
    pub const DEFAULT_DELIVERY_SLICE_BUDGET: usize = 32;

    /// Validates the caps: every value must be non-zero (a zero cap gates nothing
    /// — the unlimited-by-silence state §5 outlaws). Errors are accumulated into
    /// `errors` (one per offending field) so an operator sees every bad cap at
    /// once, matching the rest of config validation.
    pub(crate) fn collect_errors(&self, errors: &mut Vec<String>) {
        let checks: [(&str, usize); 9] = [
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
            // A zero here would make the delivery pump enqueue nothing on any
            // slice — every subscription silently stalled forever, which is the
            // unlimited-by-silence failure wearing the opposite sign.
            ("delivery_slice_budget", self.delivery_slice_budget),
        ];
        for (field, value) in checks {
            if value == 0 {
                errors.push(zero_cap_error(field));
            }
        }
        // `max_channels` is checked BESIDE the array rather than inside it,
        // because it is the one optional cap and the array's element type has no
        // way to say "absent". Forcing it in would need a stand-in value for
        // `None` — a number standing where a declaration is not — and any
        // stand-in would either invent a bound or make an undeclared cap look
        // declared. So the shape stays honest and the RULE stays uniform: a
        // declared zero is refused with the same message as the other eight.
        //
        // The parenthetical about unlimited-by-silence is the other eight caps'
        // ground; for this one a zero would refuse every registration rather
        // than admit every registration. The message is kept identical anyway:
        // an operator reading nine cap errors should learn one rule — zero is
        // not a legal cap value anywhere in this section — not two.
        if self.max_channels == Some(0) {
            errors.push(zero_cap_error("max_channels"));
        }
    }
}

/// The typed refusal for a zero-valued cap, shared by every field in `[limits]`
/// so the wording cannot drift between them.
fn zero_cap_error(field: &str) -> String {
    format!(
        "limits.{field}: must be greater than zero (a zero cap would be \
         unlimited-by-silence, which §5 forbids)"
    )
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
            delivery_slice_budget: default_delivery_slice_budget(),
            // No signed bound exists for this cap, so the default is the
            // absence of a declaration — never a number.
            max_channels: None,
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
const fn default_delivery_slice_budget() -> usize {
    LimitsConfig::DEFAULT_DELIVERY_SLICE_BUDGET
}

/// Participant lifecycle configuration (`[participant]`).
///
/// Present iff the deployment activates the participant protocol. Every field
/// is required — serde carries no defaults here, so a missing field fails
/// config loading with a typed error naming the field, and
/// [`ParticipantConfig::collect_errors`] rejects semantically impossible
/// values during the same accumulated validation pass as the rest of the
/// config. All values are deployment-owner decisions (no assumed defaults).
///
/// Every field here is consumed by the live production handler. Frontier and
/// retention limits are required inputs; there are no deployment defaults.
#[derive(Debug, Clone, Copy, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParticipantConfig {
    /// Complete participant wire-frame limit (`WF`) negotiated with every
    /// participant-capable connection. Must be at least the protocol's
    /// minimum complete frame; enforced by the shared codec at service
    /// construction and pre-checked during config validation.
    pub wire_frame_limit: u64,
    /// Secret-bearing attach/enrollment receipt lifetime in milliseconds.
    pub attach_receipt_ttl_ms: u64,
    /// Non-secret receipt-provenance lifetime in milliseconds. Must be at
    /// least `attach_receipt_ttl_ms` (provenance explains the receipt and
    /// cannot expire first).
    pub receipt_provenance_ttl_ms: u64,
    /// Reporting threshold for server-wide live secret-bearing receipts
    /// (stage-8 shared scope `LiveReceiptServer`).
    ///
    /// Lane p0-39: this scope is NOT an admission gate. It is where an honest
    /// third party would meet a number someone else's churn consumed, and no
    /// configured refusal is tolerable there — retention is bounded by
    /// `attach_receipt_ttl_ms` alone. This number is a TRIPWIRE: when
    /// in-window occupancy reaches it, the server counts the observation and
    /// warns on the rising edge. Nothing is ever refused because of it.
    /// Setting it higher than the deployment's plausible steady state is the
    /// point; a churn storm is what it is meant to disclose.
    pub live_receipt_server_report_threshold: u64,
    /// Per-participant WINDOW SIZE for live secret-bearing receipts (stage-8
    /// scope `LiveReceiptParticipant`).
    ///
    /// Lane p0-39: this is a bound on retention, not a refusal threshold. At
    /// a full window the participant's own oldest live receipt is displaced
    /// and the new one lands, so a rotation can never be refused by the very
    /// receipt it is about to end. Occupancy never exceeds this value.
    pub max_live_attach_receipts_per_participant: u64,
    /// Reporting threshold for server-wide retained non-secret provenance
    /// fingerprints (stage-8 shared scope `ProvenanceServer`).
    ///
    /// Lane p0-39: a tripwire, never a gate — see
    /// [`Self::live_receipt_server_report_threshold`]. Retention here is
    /// bounded by `receipt_provenance_ttl_ms` alone.
    ///
    /// Board #37 (ruling 2026-08-12): a fingerprint occupies from the moment
    /// the client proves it possesses the secret its receipt minted through
    /// that receipt's own provenance deadline — not from the minting commit.
    /// A receipt whose delivery was never observed classifies through its
    /// window but is never counted here, so an enrol-and-crash cycle leaves
    /// no provenance residue behind it.
    pub receipt_provenance_server_report_threshold: u64,
    /// Reporting threshold for per-conversation provenance fingerprints
    /// (stage-8 shared scope `ProvenanceConversation`). A tripwire, never a
    /// gate: one participant's churn must not refuse the next participant of
    /// the same conversation.
    pub receipt_provenance_per_conversation_report_threshold: u64,
    /// Per-participant WINDOW SIZE for provenance fingerprints (stage-8 scope
    /// `ProvenanceParticipant`).
    ///
    /// Lane p0-39: the participant's retained fingerprints — one per
    /// committed rotation, plus its proven enrollment fingerprint — are held
    /// to this many, oldest displaced first. Per-participant pressure is
    /// self-inflicted, so the number bounds memory without refusing: the
    /// (N+1)th honest fingerprint always lands, including on cold replay from
    /// durable state. Occupancy never exceeds this value.
    pub max_receipt_provenance_per_participant: u64,
    /// Server-wide identity-slot limit (the contract's
    /// `max_retired_identity_slots` server scope): the total number of
    /// participant identities — live or retired — mintable across ALL
    /// conversations. Enrollment refuses with server-scope
    /// `IdentityCapacityExceeded` (tested BEFORE the conversation scope)
    /// when every slot is reserved.
    pub max_retired_identity_slots_server: u64,
    /// Per-CONVERSATION identity limit `I` (the contract's half-open
    /// `0..=I` bound on permanent participant ordinals — the conversation
    /// scope of `max_retired_identity_slots`, NOT a per-participant
    /// reservation). Enrollment assigns monotone participant indices in
    /// `0..I` within one conversation and refuses with conversation-scope
    /// `IdentityCapacityExceeded` when occupancy reaches this value; slots
    /// and ids are never reused. The server-wide companion is
    /// [`Self::max_retired_identity_slots_server`].
    pub identity_slots: u64,
    /// Maximum entries one observer-recovery handshake batch may name.
    pub observer_recovery_max_entries: u64,
    /// Semantic conversations one connection may track — the protocol's
    /// signed connection-conversation limit. Consumed on BOTH of its contract
    /// paths: the stage-6 capacity gate every conversation-scoped semantic
    /// operation runs (register row 5641) and the observer-recovery batch
    /// preflight (register row 5642), over one shared per-connection
    /// dispatch map.
    pub max_semantic_conversations_per_connection: u64,
    /// Maximum canonical entries in one ordinary retained-record row.
    pub max_ordinary_record_entries: u64,
    /// Maximum canonical bytes in one ordinary retained-record row.
    pub max_ordinary_record_bytes: u64,
    /// Maximum canonical entries in one generated marker row.
    pub max_generated_marker_entries: u64,
    /// Maximum canonical bytes in one generated marker row.
    pub max_generated_marker_bytes: u64,
    /// Entry component of the mandatory transaction envelope `Q`.
    pub mandatory_transaction_bound_entries: u64,
    /// Byte component of the mandatory transaction envelope `Q`.
    pub mandatory_transaction_bound_bytes: u64,
    /// Entry component of the full recovery claim `K`.
    pub full_recovery_claim_entries: u64,
    /// Byte component of the full recovery claim `K`.
    pub full_recovery_claim_bytes: u64,
    /// Total retained durable entry capacity per conversation.
    pub retained_capacity_entries: u64,
    /// Total retained canonical-byte capacity per conversation.
    pub retained_capacity_bytes: u64,
    /// Maximum retained causal-record rows restored for one conversation.
    pub max_retained_record_rows: u64,
    /// Maximum closure churn cycles in one episode.
    pub closure_episode_churn_limit: u64,
}

impl ParticipantConfig {
    /// Accumulates semantic validation errors for the participant section.
    ///
    /// Zero is rejected wherever it would be unlimited-by-silence, gate
    /// nothing, or violate a protocol precondition; the TTL ordering mirrors
    /// the protocol's own frozen configuration precedence.
    pub(crate) fn collect_errors(&self, errors: &mut Vec<String>) {
        // The receipt/identity block follows the contract's frozen nine-field
        // validation order: both TTLs, the five receipt/provenance caps, the
        // server identity limit, then the conversation identity limit.
        let nonzero: [(&str, u64); 23] = [
            ("wire_frame_limit", self.wire_frame_limit),
            ("attach_receipt_ttl_ms", self.attach_receipt_ttl_ms),
            ("receipt_provenance_ttl_ms", self.receipt_provenance_ttl_ms),
            (
                "live_receipt_server_report_threshold",
                self.live_receipt_server_report_threshold,
            ),
            (
                "max_live_attach_receipts_per_participant",
                self.max_live_attach_receipts_per_participant,
            ),
            (
                "receipt_provenance_server_report_threshold",
                self.receipt_provenance_server_report_threshold,
            ),
            (
                "receipt_provenance_per_conversation_report_threshold",
                self.receipt_provenance_per_conversation_report_threshold,
            ),
            (
                "max_receipt_provenance_per_participant",
                self.max_receipt_provenance_per_participant,
            ),
            (
                "max_retired_identity_slots_server",
                self.max_retired_identity_slots_server,
            ),
            ("identity_slots", self.identity_slots),
            (
                "observer_recovery_max_entries",
                self.observer_recovery_max_entries,
            ),
            (
                "max_semantic_conversations_per_connection",
                self.max_semantic_conversations_per_connection,
            ),
            (
                "max_ordinary_record_entries",
                self.max_ordinary_record_entries,
            ),
            ("max_ordinary_record_bytes", self.max_ordinary_record_bytes),
            (
                "max_generated_marker_entries",
                self.max_generated_marker_entries,
            ),
            (
                "max_generated_marker_bytes",
                self.max_generated_marker_bytes,
            ),
            (
                "mandatory_transaction_bound_entries",
                self.mandatory_transaction_bound_entries,
            ),
            (
                "mandatory_transaction_bound_bytes",
                self.mandatory_transaction_bound_bytes,
            ),
            (
                "full_recovery_claim_entries",
                self.full_recovery_claim_entries,
            ),
            ("full_recovery_claim_bytes", self.full_recovery_claim_bytes),
            ("retained_capacity_entries", self.retained_capacity_entries),
            ("retained_capacity_bytes", self.retained_capacity_bytes),
            ("max_retained_record_rows", self.max_retained_record_rows),
        ];
        for (field, value) in nonzero {
            if value == 0 {
                errors.push(format!("participant.{field}: must be greater than zero"));
            }
        }
        if self.receipt_provenance_ttl_ms < self.attach_receipt_ttl_ms {
            errors.push(
                "participant.receipt_provenance_ttl_ms: must be at least \
                 attach_receipt_ttl_ms (provenance cannot expire before the receipt it explains)"
                    .to_owned(),
            );
        }
        if self.full_recovery_claim_entries != self.mandatory_transaction_bound_entries {
            errors.push(
                "participant.full_recovery_claim_entries: must equal \
                 mandatory_transaction_bound_entries"
                    .to_owned(),
            );
        }
        if self.full_recovery_claim_bytes != self.mandatory_transaction_bound_bytes {
            errors.push(
                "participant.full_recovery_claim_bytes: must equal \
                 mandatory_transaction_bound_bytes"
                    .to_owned(),
            );
        }
        if !(2..=u64::from(u32::MAX)).contains(&self.closure_episode_churn_limit) {
            errors.push(
                "participant.closure_episode_churn_limit: must be in 2..=u32::MAX".to_owned(),
            );
        }
        self.collect_unit2_derived_errors(errors);
    }

    fn collect_unit2_derived_errors(&self, errors: &mut Vec<String>) {
        if self
            .max_retained_record_rows
            .checked_mul(self.identity_slots)
            .is_none()
        {
            errors.push("participant.UNIT2_MAX_LIVE_RECIPIENT_OBLIGATIONS: max_retained_record_rows * identity_slots overflows u64".to_owned());
        }
    }
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
