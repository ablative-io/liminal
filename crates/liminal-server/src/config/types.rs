use std::net::SocketAddr;
use std::path::PathBuf;

/// Declarative configuration for the standalone liminal server wrapper.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// Socket address where the standalone server will listen for client traffic.
    pub listen_address: SocketAddr,
    /// Socket address where the health endpoint server will listen for probes.
    pub health_listen_address: SocketAddr,
    /// Channel topology definitions declared by the operator.
    pub channels: Vec<ChannelDef>,
    /// Declarative routing rules that connect configured channels.
    pub routing_rules: Vec<RoutingRuleDef>,
    /// Optional filesystem location for durable server state.
    pub persistence_path: Option<PathBuf>,
    /// Optional beamr distribution cluster membership configuration.
    pub cluster: Option<ClusterConfig>,
}

/// Declarative channel definition loaded from server configuration.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelDef {
    /// Unique channel name used by routing rules and operators.
    pub name: String,
    /// Schema reference used to validate messages for this channel.
    pub schema_ref: String,
    /// Whether this channel requires durable persistence.
    pub durable: bool,
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

/// Beamr distribution cluster configuration for standalone deployment.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    /// Unique node name advertised to the beamr distribution cluster.
    pub node_name: String,
    /// Seed node socket addresses used to join an existing cluster.
    pub seed_nodes: Vec<SocketAddr>,
}
