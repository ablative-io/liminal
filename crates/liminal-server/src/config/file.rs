use std::path::Path;

use crate::ServerError;

use super::env::apply_env_overrides;
use super::types::ServerConfig;
use super::validation::validate;

/// Loads a server configuration from a TOML file.
///
/// # Errors
///
/// Returns [`ServerError::ConfigLoad`] when the file cannot be read, the TOML is
/// malformed, or strict deserialization rejects an unknown field.
pub fn load_from_file(path: impl AsRef<Path>) -> Result<ServerConfig, ServerError> {
    let path = path.as_ref();
    let contents = std::fs::read_to_string(path).map_err(|error| ServerError::ConfigLoad {
        message: format!(
            "failed to read configuration file '{}': {error}",
            path.display()
        ),
    })?;

    toml::from_str::<ServerConfig>(&contents).map_err(|error| ServerError::ConfigLoad {
        message: format!(
            "failed to parse configuration file '{}': {error}",
            path.display()
        ),
    })
}

pub(crate) fn load_config(path: impl AsRef<Path>) -> Result<ServerConfig, ServerError> {
    let path = path.as_ref();
    let config = load_from_file(path)?;
    let mut config = apply_env_overrides(config)?;
    // Channel `schema_ref` paths are resolved relative to the directory holding
    // the config file, so validation loads each schema from there.
    validate(&mut config, path.parent())?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::ServerError;

    use super::load_from_file;

    static NEXT_TEMP_FILE_ID: AtomicU64 = AtomicU64::new(0);

    fn valid_toml() -> &'static str {
        r#"
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
predicate = "true"

[cluster]
node_name = "node-a"
listen_address = "127.0.0.1:9000"
seed_nodes = ["127.0.0.1:9001"]
"#
    }

    fn temp_config_path(label: &str) -> PathBuf {
        let id = NEXT_TEMP_FILE_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "liminal-server-{label}-{}-{id}.toml",
            std::process::id()
        ))
    }

    fn write_temp_config(
        label: &str,
        contents: &str,
    ) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let path = temp_config_path(label);
        fs::write(&path, contents)?;
        Ok(path)
    }

    fn remove_temp_file(path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
        if path.exists() {
            fs::remove_file(path)?;
        }
        Ok(())
    }

    #[test]
    fn valid_toml_parses_into_server_config() -> Result<(), Box<dyn std::error::Error>> {
        let path = write_temp_config("valid", valid_toml())?;
        let config = load_from_file(&path)?;
        remove_temp_file(&path)?;

        assert_eq!(config.listen_address.to_string(), "127.0.0.1:8080");
        assert_eq!(config.health_listen_address.to_string(), "127.0.0.1:8081");
        assert_eq!(config.drain_timeout_ms, 30_000);
        assert_eq!(config.channels.len(), 1);
        assert_eq!(config.channels[0].name, "orders");
        assert_eq!(config.routing_rules.len(), 1);
        assert_eq!(
            config.persistence_path.as_deref(),
            Some(std::path::Path::new("/tmp"))
        );
        let cluster = config
            .cluster
            .as_ref()
            .ok_or("cluster section should be present")?;
        assert_eq!(cluster.node_name, "node-a");
        assert_eq!(cluster.listen_address.to_string(), "127.0.0.1:9000");
        assert_eq!(cluster.seed_nodes.len(), 1);
        // The cookie is omitted from the fixture, so it must fall back to the
        // shared default rather than parse-failing.
        assert_eq!(cluster.cookie, crate::config::types::DEFAULT_COOKIE);

        Ok(())
    }

    #[test]
    fn websocket_section_parses_and_absent_section_stays_none()
    -> Result<(), Box<dyn std::error::Error>> {
        // Absent section: no websocket configuration exists at all.
        let absent_path = write_temp_config("ws-absent", valid_toml())?;
        let absent = load_from_file(&absent_path)?;
        remove_temp_file(&absent_path)?;
        assert!(absent.websocket.is_none());

        // Present section: every field parses, including the optional
        // keepalive interval and origin allow-list.
        let toml = format!(
            "{}\n[websocket]\nlisten_address = \"127.0.0.1:8090\"\npath = \"/liminal\"\n\
             allowed_origins = [\"https://app.example.com\"]\nping_interval_ms = 30000\n",
            valid_toml()
        );
        let path = write_temp_config("ws-present", &toml)?;
        let config = load_from_file(&path)?;
        remove_temp_file(&path)?;
        let websocket = config.websocket.ok_or("websocket section should parse")?;
        assert_eq!(websocket.listen_address.to_string(), "127.0.0.1:8090");
        assert_eq!(websocket.path, "/liminal");
        assert_eq!(
            websocket.allowed_origins,
            vec!["https://app.example.com".to_owned()]
        );
        assert_eq!(websocket.ping_interval_ms, Some(30_000));

        // Minimal section: origins default to the fail-closed empty list and
        // the keepalive stays disabled.
        let minimal = format!(
            "{}\n[websocket]\nlisten_address = \"127.0.0.1:8091\"\npath = \"/liminal\"\n",
            valid_toml()
        );
        let minimal_path = write_temp_config("ws-minimal", &minimal)?;
        let minimal_config = load_from_file(&minimal_path)?;
        remove_temp_file(&minimal_path)?;
        let websocket = minimal_config
            .websocket
            .ok_or("minimal websocket section should parse")?;
        assert!(websocket.allowed_origins.is_empty());
        assert_eq!(websocket.ping_interval_ms, None);
        Ok(())
    }

    #[test]
    fn missing_file_returns_config_load() {
        let path = temp_config_path("missing");
        let result = load_from_file(&path);

        assert!(matches!(result, Err(ServerError::ConfigLoad { .. })));
    }

    #[test]
    fn malformed_toml_returns_config_load_with_parse_details()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = write_temp_config("malformed", "listen_address =")?;
        let result = load_from_file(&path);
        remove_temp_file(&path)?;

        assert!(matches!(result, Err(ServerError::ConfigLoad { .. })));
        let Err(ServerError::ConfigLoad { message }) = result else {
            return Ok(());
        };
        assert!(message.contains("parse"));

        Ok(())
    }

    #[test]
    fn unknown_fields_return_config_load() -> Result<(), Box<dyn std::error::Error>> {
        let toml = format!("{}\nunknown_field = true\n", valid_toml());
        let path = write_temp_config("unknown", &toml)?;
        let result = load_from_file(&path);
        remove_temp_file(&path)?;

        assert!(matches!(result, Err(ServerError::ConfigLoad { .. })));
        let Err(ServerError::ConfigLoad { message }) = result else {
            return Ok(());
        };
        assert!(message.contains("unknown") || message.contains("unexpected"));

        Ok(())
    }
}
