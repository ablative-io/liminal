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
    let config = load_from_file(path)?;
    let config = apply_env_overrides(config)?;
    validate(&config)?;
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
seed_nodes = ["127.0.0.1:9000"]
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
        assert_eq!(config.channels.len(), 1);
        assert_eq!(config.channels[0].name, "orders");
        assert_eq!(config.routing_rules.len(), 1);
        assert_eq!(
            config.persistence_path.as_deref(),
            Some(std::path::Path::new("/tmp"))
        );
        assert!(config.cluster.is_some());

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
