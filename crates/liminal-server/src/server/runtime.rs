use std::path::Path;

use crate::ServerError;
use crate::config::file::load_config;
use crate::health::{ReadinessState, SharedReadinessState, start_health_server};

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

    let readiness = SharedReadinessState::new(ReadinessState::default());
    let health_server = start_health_server(config.health_listen_address, readiness.clone())?;
    readiness.set_config_loaded(true);
    readiness.set_cluster_configured(config.cluster.is_some());

    tracing::debug!(
        config_path = %config_path.display(),
        listen_address = %config.listen_address,
        health_listen_address = %health_server.local_addr(),
        "liminal server configuration validated"
    );

    health_server.shutdown()?;

    Ok(())
}
