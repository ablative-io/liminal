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
const CLUSTER_LISTEN_ADDRESS: &str = "LIMINAL_CLUSTER_LISTEN_ADDRESS";
const CLUSTER_COOKIE: &str = "LIMINAL_CLUSTER_COOKIE";
const AUTH_TOKEN: &str = "LIMINAL_AUTH_TOKEN";
const WEBSOCKET_LISTEN_ADDRESS: &str = "LIMINAL_WEBSOCKET_LISTEN_ADDRESS";
const WEBSOCKET_PATH: &str = "LIMINAL_WEBSOCKET_PATH";
const WEBSOCKET_ALLOWED_ORIGINS: &str = "LIMINAL_WEBSOCKET_ALLOWED_ORIGINS";
const WEBSOCKET_PING_INTERVAL_MS: &str = "LIMINAL_WEBSOCKET_PING_INTERVAL_MS";

/// Applies supported `LIMINAL_` environment variable overrides to a config.
///
/// Absent variables leave the supplied configuration unchanged. Supported
/// variables are `LIMINAL_LISTEN_ADDRESS`, `LIMINAL_HEALTH_LISTEN_ADDRESS`,
/// `LIMINAL_DRAIN_TIMEOUT_MS`, `LIMINAL_PERSISTENCE_PATH`,
/// `LIMINAL_CLUSTER_NODE_NAME`, `LIMINAL_CLUSTER_SEED_NODES`,
/// `LIMINAL_CLUSTER_LISTEN_ADDRESS`, `LIMINAL_CLUSTER_COOKIE`,
/// `LIMINAL_AUTH_TOKEN`, `LIMINAL_WEBSOCKET_LISTEN_ADDRESS`,
/// `LIMINAL_WEBSOCKET_PATH`, `LIMINAL_WEBSOCKET_ALLOWED_ORIGINS`, and
/// `LIMINAL_WEBSOCKET_PING_INTERVAL_MS`.
///
/// Unlike the cluster and websocket overrides — which refuse to fabricate a
/// `[cluster]`/`[websocket]` section that the file did not declare (a
/// partially-specified section is unsafe) —
/// `LIMINAL_AUTH_TOKEN` MAY create the `[auth]` section when the file omits it.
/// The auth section is a single scalar secret with no other required fields, so a
/// deployment can inject the token purely from the environment (the idiomatic way
/// to supply a secret) without also pinning it in a committed config file; an empty
/// value still fails the later non-empty validation, so this cannot silently create
/// a no-op gate.
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
            CLUSTER_LISTEN_ADDRESS => {
                let listen_address = parse_socket_addr(CLUSTER_LISTEN_ADDRESS, &value)?;
                cluster_required(&mut config, CLUSTER_LISTEN_ADDRESS)?.listen_address =
                    listen_address;
            }
            CLUSTER_COOKIE => {
                let cookie = env_string(CLUSTER_COOKIE, &value)?;
                cluster_required(&mut config, CLUSTER_COOKIE)?.cookie = cookie;
            }
            WEBSOCKET_LISTEN_ADDRESS => {
                let listen_address = parse_socket_addr(WEBSOCKET_LISTEN_ADDRESS, &value)?;
                websocket_required(&mut config, WEBSOCKET_LISTEN_ADDRESS)?.listen_address =
                    listen_address;
            }
            WEBSOCKET_PATH => {
                let path = env_string(WEBSOCKET_PATH, &value)?;
                websocket_required(&mut config, WEBSOCKET_PATH)?.path = path;
            }
            WEBSOCKET_ALLOWED_ORIGINS => {
                let allowed_origins = parse_allowed_origins(&value)?;
                websocket_required(&mut config, WEBSOCKET_ALLOWED_ORIGINS)?.allowed_origins =
                    allowed_origins;
            }
            WEBSOCKET_PING_INTERVAL_MS => {
                let interval = parse_u64(WEBSOCKET_PING_INTERVAL_MS, &value)?;
                websocket_required(&mut config, WEBSOCKET_PING_INTERVAL_MS)?.ping_interval_ms =
                    Some(interval);
            }
            AUTH_TOKEN => {
                // A single scalar secret is allowed to create the `[auth]` section
                // even when the file omits it (see the module-level note); an empty
                // value is left to fail the non-empty auth validation, not rejected
                // here, so precedence and error reporting stay uniform.
                let token = env_string(AUTH_TOKEN, &value)?;
                config.auth = Some(super::types::AuthConfig { token });
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

/// Parses the comma-separated `LIMINAL_WEBSOCKET_ALLOWED_ORIGINS` list. An
/// empty value yields the empty (fail-closed) list; entries are trimmed and an
/// empty entry between commas is a typed error rather than a silently dropped
/// origin. Semantic origin-shape checks stay in config validation so file- and
/// environment-sourced lists are held to the identical rules.
fn parse_allowed_origins(value: &OsStr) -> Result<Vec<String>, ServerError> {
    let value = env_string(WEBSOCKET_ALLOWED_ORIGINS, value)?;
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .enumerate()
        .map(|(index, candidate)| {
            let candidate = candidate.trim();
            if candidate.is_empty() {
                Err(config_load(format!(
                    "environment variable {WEBSOCKET_ALLOWED_ORIGINS} contains an empty origin \
                     at position {}",
                    index + 1
                )))
            } else {
                Ok(candidate.to_owned())
            }
        })
        .collect()
}

/// Mirrors [`cluster_required`]: the `[websocket]` section has two required
/// fields with no defaults, so the environment may adjust a declared section but
/// must never fabricate a partially-specified acceptor.
fn websocket_required<'a>(
    config: &'a mut ServerConfig,
    name: &str,
) -> Result<&'a mut super::types::WebSocketConfig, ServerError> {
    config
        .websocket
        .as_mut()
        .ok_or_else(|| ServerError::ConfigValidation {
            message: format!(
                "environment variable {name} requires a [websocket] section in the configuration \
                 file"
            ),
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
                schema_ref: None,
                durable: true,
                loaded_schema: None,
            }],
            routing_rules: vec![RoutingRuleDef {
                source_channel: "orders".to_owned(),
                target_channel: "orders".to_owned(),
                predicate: None,
            }],
            persistence_path: Some(PathBuf::from("/tmp")),
            cluster: Some(ClusterConfig {
                node_name: "node-a".to_owned(),
                listen_address: socket("127.0.0.1:9000")?,
                seed_nodes: vec![socket("127.0.0.1:9001")?],
                cookie: "test-cookie".to_owned(),
            }),
            auth: None,
            services: crate::config::types::ServicesConfig::default(),
            limits: crate::config::types::LimitsConfig::default(),
            participant: None,
            websocket: None,
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
    fn cluster_listen_address_and_cookie_overrides_replace_values()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = sample_config()?;
        let config = apply_env_overrides_from(
            config,
            vec![
                env_pair("LIMINAL_CLUSTER_LISTEN_ADDRESS", "127.0.0.1:9500"),
                env_pair("LIMINAL_CLUSTER_COOKIE", "override-cookie"),
            ],
        )?;

        let Some(cluster) = config.cluster else {
            return Err("cluster config should remain present".into());
        };
        assert_eq!(cluster.listen_address, socket("127.0.0.1:9500")?);
        assert_eq!(cluster.cookie, "override-cookie");

        Ok(())
    }

    #[test]
    fn cluster_listen_address_override_without_cluster_section_returns_validation_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut config = sample_config()?;
        config.cluster = None;
        let result = apply_env_overrides_from(
            config,
            vec![env_pair("LIMINAL_CLUSTER_LISTEN_ADDRESS", "127.0.0.1:9500")],
        );

        assert!(matches!(result, Err(ServerError::ConfigValidation { .. })));

        Ok(())
    }

    #[test]
    fn auth_token_override_replaces_existing_token() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = sample_config()?;
        config.auth = Some(crate::config::types::AuthConfig {
            token: "file-token".to_owned(),
        });

        let config =
            apply_env_overrides_from(config, vec![env_pair("LIMINAL_AUTH_TOKEN", "env-token")])?;

        let auth = config.auth.ok_or("auth section should remain present")?;
        assert_eq!(auth.token, "env-token");

        Ok(())
    }

    #[test]
    fn auth_token_override_creates_missing_auth_section() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut config = sample_config()?;
        config.auth = None;

        let config =
            apply_env_overrides_from(config, vec![env_pair("LIMINAL_AUTH_TOKEN", "env-token")])?;

        // Unlike cluster overrides, a scalar auth secret MAY fabricate the section.
        let auth = config.auth.ok_or("auth section should have been created")?;
        assert_eq!(auth.token, "env-token");

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
durable = true

[[routing_rules]]
source_channel = "orders"
target_channel = "orders"
"#;
        let path = write_temp_config(toml)?;
        let config = load_from_file(&path)?;
        let mut config = apply_env_overrides_from(
            config,
            vec![env_pair("LIMINAL_LISTEN_ADDRESS", "0.0.0.0:9090")],
        )?;
        validate(&mut config, path.parent())?;
        remove_temp_file(&path)?;

        assert_eq!(config.listen_address, socket("0.0.0.0:9090")?);

        Ok(())
    }

    // ---- LP-WS-TRANSPORT R1.1: [websocket] environment overrides ----

    fn sample_config_with_websocket() -> Result<ServerConfig, Box<dyn std::error::Error>> {
        let mut config = sample_config()?;
        config.websocket = Some(crate::config::types::WebSocketConfig {
            listen_address: socket("127.0.0.1:8082")?,
            path: "/liminal".to_owned(),
            allowed_origins: Vec::new(),
            ping_interval_ms: None,
        });
        Ok(config)
    }

    #[test]
    fn websocket_overrides_replace_declared_section_values()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = sample_config_with_websocket()?;
        let config = apply_env_overrides_from(
            config,
            vec![
                env_pair("LIMINAL_WEBSOCKET_LISTEN_ADDRESS", "0.0.0.0:9292"),
                env_pair("LIMINAL_WEBSOCKET_PATH", "/bridge"),
                env_pair(
                    "LIMINAL_WEBSOCKET_ALLOWED_ORIGINS",
                    "https://a.example.com, https://b.example.com",
                ),
                env_pair("LIMINAL_WEBSOCKET_PING_INTERVAL_MS", "15000"),
            ],
        )?;
        let websocket = config.websocket.ok_or("websocket section missing")?;
        assert_eq!(websocket.listen_address, socket("0.0.0.0:9292")?);
        assert_eq!(websocket.path, "/bridge");
        assert_eq!(
            websocket.allowed_origins,
            vec![
                "https://a.example.com".to_owned(),
                "https://b.example.com".to_owned()
            ]
        );
        assert_eq!(websocket.ping_interval_ms, Some(15_000));
        Ok(())
    }

    #[test]
    fn websocket_override_without_declared_section_is_refused()
    -> Result<(), Box<dyn std::error::Error>> {
        for (name, value) in [
            ("LIMINAL_WEBSOCKET_LISTEN_ADDRESS", "0.0.0.0:9292"),
            ("LIMINAL_WEBSOCKET_PATH", "/bridge"),
            ("LIMINAL_WEBSOCKET_ALLOWED_ORIGINS", "https://a.example.com"),
            ("LIMINAL_WEBSOCKET_PING_INTERVAL_MS", "15000"),
        ] {
            let config = sample_config()?;
            let result = apply_env_overrides_from(config, vec![env_pair(name, value)]);
            let Err(ServerError::ConfigValidation { message }) = result else {
                return Err(format!("{name}: fabricating [websocket] must be refused").into());
            };
            assert!(
                message.contains("[websocket]"),
                "{name}: expected a section-required error, got: {message}"
            );
        }
        Ok(())
    }

    #[test]
    fn websocket_empty_origin_list_override_is_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = sample_config_with_websocket()?;
        let config = apply_env_overrides_from(
            config,
            vec![env_pair("LIMINAL_WEBSOCKET_ALLOWED_ORIGINS", "")],
        )?;
        let websocket = config.websocket.ok_or("websocket section missing")?;
        assert!(websocket.allowed_origins.is_empty());
        Ok(())
    }

    #[test]
    fn websocket_origin_list_with_empty_entry_is_refused() -> Result<(), Box<dyn std::error::Error>>
    {
        let config = sample_config_with_websocket()?;
        let result = apply_env_overrides_from(
            config,
            vec![env_pair(
                "LIMINAL_WEBSOCKET_ALLOWED_ORIGINS",
                "https://a.example.com,,https://b.example.com",
            )],
        );
        let Err(ServerError::ConfigLoad { message }) = result else {
            return Err("an empty origin entry must be a typed load error".into());
        };
        assert!(
            message.contains("empty origin"),
            "expected an empty-origin error, got: {message}"
        );
        Ok(())
    }
}
