use std::ffi::{OsStr, OsString};
use std::net::SocketAddr;
use std::path::PathBuf;

use crate::ServerError;

use super::types::ServerConfig;

const ENV_PREFIX: &str = "LIMINAL_";
const LISTEN_ADDRESS: &str = "LIMINAL_LISTEN_ADDRESS";
const HEALTH_LISTEN_ADDRESS: &str = "LIMINAL_HEALTH_LISTEN_ADDRESS";
const DRAIN_TIMEOUT_MS: &str = "LIMINAL_DRAIN_TIMEOUT_MS";
const PERSISTENCE_PATH: &str = "LIMINAL_PERSISTENCE_PATH";
const CLUSTER_NODE_NAME: &str = "LIMINAL_CLUSTER_NODE_NAME";
const CLUSTER_SEED_NODES: &str = "LIMINAL_CLUSTER_SEED_NODES";

/// Applies supported `LIMINAL_` environment variable overrides to a config.
///
/// Absent variables leave the supplied configuration unchanged. Supported
/// variables are `LIMINAL_LISTEN_ADDRESS`, `LIMINAL_HEALTH_LISTEN_ADDRESS`,
/// `LIMINAL_DRAIN_TIMEOUT_MS`, `LIMINAL_PERSISTENCE_PATH`,
/// `LIMINAL_CLUSTER_NODE_NAME`, and `LIMINAL_CLUSTER_SEED_NODES`.
///
/// # Errors
///
/// Returns [`ServerError`] when a present environment variable cannot be parsed
/// or targets cluster configuration that was not declared in the file.
pub fn apply_env_overrides(config: ServerConfig) -> Result<ServerConfig, ServerError> {
    apply_env_overrides_from(config, std::env::vars_os())
}

pub(crate) fn apply_env_overrides_from<I>(
    mut config: ServerConfig,
    variables: I,
) -> Result<ServerConfig, ServerError>
where
    I: IntoIterator<Item = (OsString, OsString)>,
{
    for (key, value) in variables {
        let Some(key) = key.to_str() else {
            continue;
        };

        if !key.starts_with(ENV_PREFIX) {
            continue;
        }

        match key {
            LISTEN_ADDRESS => {
                config.listen_address = parse_socket_addr(LISTEN_ADDRESS, &value)?;
            }
            HEALTH_LISTEN_ADDRESS => {
                config.health_listen_address = parse_socket_addr(HEALTH_LISTEN_ADDRESS, &value)?;
            }
            DRAIN_TIMEOUT_MS => {
                config.drain_timeout_ms = parse_u64(DRAIN_TIMEOUT_MS, &value)?;
            }
            PERSISTENCE_PATH => {
                config.persistence_path = Some(PathBuf::from(value));
            }
            CLUSTER_NODE_NAME => {
                let node_name = env_string(CLUSTER_NODE_NAME, &value)?;
                cluster_required(&mut config, CLUSTER_NODE_NAME)?.node_name = node_name;
            }
            CLUSTER_SEED_NODES => {
                let seed_nodes = parse_seed_nodes(&value)?;
                cluster_required(&mut config, CLUSTER_SEED_NODES)?.seed_nodes = seed_nodes;
            }
            _ => {}
        }
    }

    Ok(config)
}

fn parse_socket_addr(name: &str, value: &OsStr) -> Result<SocketAddr, ServerError> {
    let value = env_string(name, value)?;
    value.parse::<SocketAddr>().map_err(|error| {
        config_load(format!(
            "environment variable {name} must be a socket address: {error}"
        ))
    })
}

fn parse_u64(name: &str, value: &OsStr) -> Result<u64, ServerError> {
    let value = env_string(name, value)?;
    value.parse::<u64>().map_err(|error| {
        config_load(format!(
            "environment variable {name} must be an unsigned integer: {error}"
        ))
    })
}

fn parse_seed_nodes(value: &OsStr) -> Result<Vec<SocketAddr>, ServerError> {
    let value = env_string(CLUSTER_SEED_NODES, value)?;
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }

    value
        .split(',')
        .enumerate()
        .map(|(index, candidate)| parse_seed_node(index, candidate))
        .collect()
}

fn parse_seed_node(index: usize, candidate: &str) -> Result<SocketAddr, ServerError> {
    let candidate = candidate.trim();
    if candidate.is_empty() {
        return Err(config_load(format!(
            "environment variable {CLUSTER_SEED_NODES} contains an empty seed node at position {}",
            index + 1
        )));
    }

    candidate.parse::<SocketAddr>().map_err(|error| {
        config_load(format!(
            "environment variable {CLUSTER_SEED_NODES} contains invalid seed node '{}' at position {}: {error}",
            candidate,
            index + 1
        ))
    })
}

fn env_string(name: &str, value: &OsStr) -> Result<String, ServerError> {
    value.to_str().map(str::to_owned).ok_or_else(|| {
        config_load(format!(
            "environment variable {name} contains non-Unicode data"
        ))
    })
}

fn cluster_required<'a>(
    config: &'a mut ServerConfig,
    name: &str,
) -> Result<&'a mut super::types::ClusterConfig, ServerError> {
    config
        .cluster
        .as_mut()
        .ok_or_else(|| ServerError::ConfigValidation {
            message: format!(
                "environment variable {name} requires a [cluster] section in the configuration file"
            ),
        })
}

const fn config_load(message: String) -> ServerError {
    ServerError::ConfigLoad { message }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::net::SocketAddr;
    use std::path::{Path, PathBuf};

    use crate::ServerError;

    use super::apply_env_overrides_from;
    use crate::config::types::{ChannelDef, ClusterConfig, RoutingRuleDef, ServerConfig};
    use crate::config::{load_from_file, validate};

    fn socket(address: &str) -> Result<SocketAddr, Box<dyn std::error::Error>> {
        Ok(address.parse()?)
    }

    fn sample_config() -> Result<ServerConfig, Box<dyn std::error::Error>> {
        Ok(ServerConfig {
            listen_address: socket("127.0.0.1:8080")?,
            health_listen_address: socket("127.0.0.1:8081")?,
            drain_timeout_ms: 30_000,
            channels: vec![ChannelDef {
                name: "orders".to_owned(),
                schema_ref: "schemas/orders.json".to_owned(),
                durable: true,
            }],
            routing_rules: vec![RoutingRuleDef {
                source_channel: "orders".to_owned(),
                target_channel: "orders".to_owned(),
                predicate: None,
            }],
            persistence_path: Some(PathBuf::from("/tmp")),
            cluster: Some(ClusterConfig {
                node_name: "node-a".to_owned(),
                seed_nodes: vec![socket("127.0.0.1:9000")?],
            }),
        })
    }

    fn env_pair(name: &str, value: &str) -> (OsString, OsString) {
        (OsString::from(name), OsString::from(value))
    }

    fn write_temp_config(contents: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let path = std::env::temp_dir().join(format!(
            "liminal-server-env-pipeline-{}.toml",
            std::process::id()
        ));
        std::fs::write(&path, contents)?;
        Ok(path)
    }

    fn remove_temp_file(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        if path.exists() {
            std::fs::remove_file(path)?;
        }
        Ok(())
    }

    #[test]
    fn listen_address_override_replaces_file_value() -> Result<(), Box<dyn std::error::Error>> {
        let config = sample_config()?;
        let config = apply_env_overrides_from(
            config,
            vec![env_pair("LIMINAL_LISTEN_ADDRESS", "0.0.0.0:9090")],
        )?;

        assert_eq!(config.listen_address, socket("0.0.0.0:9090")?);

        Ok(())
    }

    #[test]
    fn health_listen_address_override_replaces_file_value() -> Result<(), Box<dyn std::error::Error>>
    {
        let config = sample_config()?;
        let config = apply_env_overrides_from(
            config,
            vec![env_pair("LIMINAL_HEALTH_LISTEN_ADDRESS", "0.0.0.0:9191")],
        )?;

        assert_eq!(config.health_listen_address, socket("0.0.0.0:9191")?);

        Ok(())
    }

    #[test]
    fn drain_timeout_override_replaces_file_value() -> Result<(), Box<dyn std::error::Error>> {
        let config = sample_config()?;
        let config =
            apply_env_overrides_from(config, vec![env_pair("LIMINAL_DRAIN_TIMEOUT_MS", "1250")])?;

        assert_eq!(config.drain_timeout_ms, 1250);

        Ok(())
    }

    #[test]
    fn persistence_path_override_replaces_file_value() -> Result<(), Box<dyn std::error::Error>> {
        let config = sample_config()?;
        let config = apply_env_overrides_from(
            config,
            vec![env_pair("LIMINAL_PERSISTENCE_PATH", "/var/lib/liminal")],
        )?;

        assert_eq!(
            config.persistence_path.as_deref(),
            Some(Path::new("/var/lib/liminal"))
        );

        Ok(())
    }

    #[test]
    fn cluster_overrides_replace_existing_cluster_values() -> Result<(), Box<dyn std::error::Error>>
    {
        let config = sample_config()?;
        let config = apply_env_overrides_from(
            config,
            vec![
                env_pair("LIMINAL_CLUSTER_NODE_NAME", "node-b"),
                env_pair(
                    "LIMINAL_CLUSTER_SEED_NODES",
                    "127.0.0.1:9100, 127.0.0.1:9200",
                ),
            ],
        )?;

        let Some(cluster) = config.cluster else {
            return Err("cluster config should remain present".into());
        };
        assert_eq!(cluster.node_name, "node-b");
        assert_eq!(cluster.seed_nodes.len(), 2);
        assert_eq!(cluster.seed_nodes[0], socket("127.0.0.1:9100")?);
        assert_eq!(cluster.seed_nodes[1], socket("127.0.0.1:9200")?);

        Ok(())
    }

    #[test]
    fn absent_environment_variables_leave_config_unchanged()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = sample_config()?;
        let original_address = config.listen_address;
        let original_health_address = config.health_listen_address;
        let original_drain_timeout_ms = config.drain_timeout_ms;
        let original_path = config.persistence_path.clone();
        let original_cluster_name = config
            .cluster
            .as_ref()
            .map(|cluster| cluster.node_name.clone());

        let config = apply_env_overrides_from(config, Vec::new())?;

        assert_eq!(config.listen_address, original_address);
        assert_eq!(config.health_listen_address, original_health_address);
        assert_eq!(config.drain_timeout_ms, original_drain_timeout_ms);
        assert_eq!(config.persistence_path, original_path);
        assert_eq!(
            config
                .cluster
                .as_ref()
                .map(|cluster| cluster.node_name.clone()),
            original_cluster_name
        );

        Ok(())
    }

    #[test]
    fn invalid_listen_address_override_returns_config_load()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = sample_config()?;
        let result = apply_env_overrides_from(
            config,
            vec![env_pair("LIMINAL_LISTEN_ADDRESS", "not-a-socket")],
        );

        assert!(matches!(result, Err(ServerError::ConfigLoad { .. })));

        Ok(())
    }

    #[test]
    fn invalid_health_listen_address_override_returns_config_load()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = sample_config()?;
        let result = apply_env_overrides_from(
            config,
            vec![env_pair("LIMINAL_HEALTH_LISTEN_ADDRESS", "not-a-socket")],
        );

        assert!(matches!(result, Err(ServerError::ConfigLoad { .. })));

        Ok(())
    }

    #[test]
    fn invalid_drain_timeout_override_returns_config_load() -> Result<(), Box<dyn std::error::Error>>
    {
        let config = sample_config()?;
        let result = apply_env_overrides_from(
            config,
            vec![env_pair("LIMINAL_DRAIN_TIMEOUT_MS", "not-a-number")],
        );

        assert!(matches!(result, Err(ServerError::ConfigLoad { .. })));

        Ok(())
    }

    #[test]
    fn cluster_override_without_cluster_section_returns_validation_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut config = sample_config()?;
        config.cluster = None;
        let result = apply_env_overrides_from(
            config,
            vec![env_pair("LIMINAL_CLUSTER_NODE_NAME", "node-b")],
        );

        assert!(matches!(result, Err(ServerError::ConfigValidation { .. })));

        Ok(())
    }

    #[test]
    fn file_then_env_then_validate_pipeline_gives_env_precedence()
    -> Result<(), Box<dyn std::error::Error>> {
        let toml = r#"
listen_address = "127.0.0.1:8080"
health_listen_address = "127.0.0.1:8081"
drain_timeout_ms = 30000
persistence_path = "/tmp"

[[channels]]
name = "orders"
schema_ref = "schemas/orders.json"
durable = true

[[routing_rules]]
source_channel = "orders"
target_channel = "orders"
"#;
        let path = write_temp_config(toml)?;
        let config = load_from_file(&path)?;
        let config = apply_env_overrides_from(
            config,
            vec![env_pair("LIMINAL_LISTEN_ADDRESS", "0.0.0.0:9090")],
        )?;
        validate(&config)?;
        remove_temp_file(&path)?;

        assert_eq!(config.listen_address, socket("0.0.0.0:9090")?);

        Ok(())
    }
}
