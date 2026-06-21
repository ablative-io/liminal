use std::collections::{BTreeMap, BTreeSet};

use crate::ServerError;

use super::types::ServerConfig;

/// Validates a fully loaded server configuration before startup.
///
/// Validation is intentionally limited to deterministic semantic checks and
/// filesystem metadata inspection. It does not bind sockets, connect to peers,
/// or perform any other network I/O.
///
/// # Errors
///
/// Returns [`ServerError::ConfigValidation`] containing all discovered validation
/// errors when the configuration is not safe to use for startup.
pub fn validate(config: &ServerConfig) -> Result<(), ServerError> {
    let mut errors = Vec::new();

    validate_listen_address(config, &mut errors);
    validate_channels(config, &mut errors);
    validate_routing_rules(config, &mut errors);
    validate_persistence_path(config, &mut errors);
    validate_cluster(config, &mut errors);

    if errors.is_empty() {
        Ok(())
    } else {
        Err(ServerError::ConfigValidation {
            message: errors.join("; "),
        })
    }
}

fn validate_listen_address(config: &ServerConfig, errors: &mut Vec<String>) {
    if config.listen_address.port() == 0 {
        errors.push("listen_address: port must be non-zero".to_owned());
    }
}

fn validate_channels(config: &ServerConfig, errors: &mut Vec<String>) {
    let mut seen = BTreeSet::new();
    let mut duplicates = BTreeSet::new();

    for channel in &config.channels {
        let name = channel.name.trim();
        if name.is_empty() {
            errors.push("channels.name: channel name must not be empty".to_owned());
            continue;
        }

        if !seen.insert(name.to_owned()) {
            duplicates.insert(name.to_owned());
        }
    }

    if !duplicates.is_empty() {
        let names = duplicates.into_iter().collect::<Vec<_>>().join(", ");
        errors.push(format!("channels.name: duplicate channel names: {names}"));
    }
}

fn validate_routing_rules(config: &ServerConfig, errors: &mut Vec<String>) {
    let channel_names = config
        .channels
        .iter()
        .map(|channel| channel.name.as_str())
        .collect::<BTreeSet<_>>();

    for (index, rule) in config.routing_rules.iter().enumerate() {
        let source = rule.source_channel.trim();
        if source.is_empty() {
            errors.push(format!(
                "routing_rules[{index}].source_channel: source channel must not be empty"
            ));
        } else if !channel_names.contains(source) {
            errors.push(format!(
                "routing_rules[{index}].source_channel: unknown channel '{source}'"
            ));
        }

        let target = rule.target_channel.trim();
        if target.is_empty() {
            errors.push(format!(
                "routing_rules[{index}].target_channel: target channel must not be empty"
            ));
        } else if !channel_names.contains(target) {
            errors.push(format!(
                "routing_rules[{index}].target_channel: unknown channel '{target}'"
            ));
        }
    }
}

fn validate_persistence_path(config: &ServerConfig, errors: &mut Vec<String>) {
    let Some(path) = config.persistence_path.as_deref() else {
        return;
    };

    match std::fs::metadata(path) {
        Ok(metadata) => {
            if !metadata.is_dir() {
                errors.push(format!(
                    "persistence_path '{}': path must be an existing directory",
                    path.display()
                ));
            } else if metadata.permissions().readonly() {
                errors.push(format!(
                    "persistence_path '{}': path is not writable",
                    path.display()
                ));
            }
        }
        Err(error) => {
            errors.push(format!(
                "persistence_path '{}': path is unreachable: {error}",
                path.display()
            ));
        }
    }
}

fn validate_cluster(config: &ServerConfig, errors: &mut Vec<String>) {
    let Some(cluster) = config.cluster.as_ref() else {
        return;
    };

    if cluster.node_name.trim().is_empty() {
        errors.push("cluster.node_name: node name must not be empty".to_owned());
    }

    let mut seed_node_counts = BTreeMap::new();
    for (index, seed_node) in cluster.seed_nodes.iter().enumerate() {
        if seed_node.port() == 0 {
            errors.push(format!(
                "cluster.seed_nodes[{index}]: seed node port must be non-zero"
            ));
        }
        seed_node_counts
            .entry(seed_node.to_string())
            .and_modify(|count| *count += 1)
            .or_insert(1_usize);
    }

    let duplicates = seed_node_counts
        .into_iter()
        .filter_map(|(seed_node, count)| (count > 1).then_some(seed_node))
        .collect::<Vec<_>>();

    if !duplicates.is_empty() {
        errors.push(format!(
            "cluster.seed_nodes: duplicate seed nodes: {}",
            duplicates.join(", ")
        ));
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::ServerError;

    use super::validate;
    use crate::config::types::{ChannelDef, ClusterConfig, RoutingRuleDef, ServerConfig};

    static NEXT_TEMP_DIR_ID: AtomicU64 = AtomicU64::new(0);

    fn socket(address: &str) -> Result<SocketAddr, Box<dyn std::error::Error>> {
        Ok(address.parse()?)
    }

    fn sample_config() -> Result<ServerConfig, Box<dyn std::error::Error>> {
        Ok(ServerConfig {
            listen_address: socket("127.0.0.1:8080")?,
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
            persistence_path: None,
            cluster: Some(ClusterConfig {
                node_name: "node-a".to_owned(),
                seed_nodes: vec![socket("127.0.0.1:9000")?],
            }),
        })
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        let id = NEXT_TEMP_DIR_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "liminal-server-validation-{label}-{}-{id}",
            std::process::id()
        ))
    }

    fn config_validation_message(result: Result<(), ServerError>) -> String {
        let Err(ServerError::ConfigValidation { message }) = result else {
            return String::new();
        };
        message
    }

    #[test]
    fn valid_config_passes_validation() -> Result<(), Box<dyn std::error::Error>> {
        let config = sample_config()?;

        validate(&config)?;

        Ok(())
    }

    #[test]
    fn invalid_listen_address_reports_field_name() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = sample_config()?;
        config.listen_address = socket("127.0.0.1:0")?;

        let message = config_validation_message(validate(&config));

        assert!(message.contains("listen_address"));
        assert!(message.contains("port"));

        Ok(())
    }

    #[test]
    fn duplicate_channel_names_are_listed() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = sample_config()?;
        config.channels.push(ChannelDef {
            name: "orders".to_owned(),
            schema_ref: "schemas/orders-v2.json".to_owned(),
            durable: false,
        });

        let message = config_validation_message(validate(&config));

        assert!(message.contains("duplicate"));
        assert!(message.contains("orders"));

        Ok(())
    }

    #[test]
    fn unreachable_persistence_path_reports_path() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = sample_config()?;
        let path = unique_temp_dir("missing");
        config.persistence_path = Some(path.clone());

        let message = config_validation_message(validate(&config));

        assert!(message.contains("persistence_path"));
        assert!(message.contains(&path.display().to_string()));

        Ok(())
    }

    #[test]
    fn file_persistence_path_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = sample_config()?;
        let path = unique_temp_dir("file");
        fs::write(&path, "not a directory")?;
        config.persistence_path = Some(path.clone());

        let message = config_validation_message(validate(&config));
        fs::remove_file(&path)?;

        assert!(message.contains("persistence_path"));
        assert!(message.contains("directory"));

        Ok(())
    }

    #[test]
    fn multiple_validation_errors_are_reported_together() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut config = sample_config()?;
        let missing_path = unique_temp_dir("multi-missing");
        config.listen_address = socket("127.0.0.1:0")?;
        config.channels.push(ChannelDef {
            name: "orders".to_owned(),
            schema_ref: "schemas/orders-v2.json".to_owned(),
            durable: false,
        });
        config.persistence_path = Some(missing_path.clone());

        let message = config_validation_message(validate(&config));

        assert!(message.contains("listen_address"));
        assert!(message.contains("duplicate channel names: orders"));
        assert!(message.contains(&missing_path.display().to_string()));

        Ok(())
    }

    #[test]
    fn routing_rules_reference_configured_channels() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = sample_config()?;
        config.routing_rules[0].target_channel = "unknown".to_owned();

        let message = config_validation_message(validate(&config));

        assert!(message.contains("routing_rules[0].target_channel"));
        assert!(message.contains("unknown"));

        Ok(())
    }
}
