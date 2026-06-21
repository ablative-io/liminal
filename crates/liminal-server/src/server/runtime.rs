use std::path::Path;

use crate::ServerError;
use crate::config::file::load_config;

/// Starts the server deployment wrapper for the supplied configuration path.
///
/// SRV-001 establishes the binary-to-library boundary. Configuration loading,
/// listener startup, shutdown, clustering, and health endpoints are implemented
/// by later server briefs.
///
/// # Errors
///
/// Returns [`ServerError`] when a later server lifecycle phase fails.
pub fn run(config_path: &Path) -> Result<(), ServerError> {
    if config_path.as_os_str().is_empty() {
        return Err(ServerError::ConfigLoad {
            message: "configuration path is empty".to_owned(),
        });
    }

    let config = load_config(config_path)?;

    tracing::debug!(
        config_path = %config_path.display(),
        listen_address = %config.listen_address,
        "liminal server configuration validated"
    );

    Ok(())
}
