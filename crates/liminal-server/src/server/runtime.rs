use std::path::Path;

use crate::ServerError;
use crate::config::file::load_config;
use crate::health::{ReadinessState, SharedReadinessState, start_health_server};
use crate::server::connection::ConnectionSupervisor;
use crate::server::listener::ServerListener;
use crate::server::shutdown::{ShutdownHandle, register_signal_handlers, run_shutdown_sequence};

/// Starts the server deployment wrapper for the supplied configuration path.
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
    let shutdown_handle = ShutdownHandle::new();
    let signal_registration = register_signal_handlers(shutdown_handle.clone())?;
    let connection_supervisor = ConnectionSupervisor::from_config(&config)?;
    let mut listener = ServerListener::bind(&config, connection_supervisor)?;
    readiness.set_config_loaded(true);
    readiness.set_listener_bound(true);
    readiness.set_cluster_configured(config.cluster.is_some());

    tracing::debug!(
        config_path = %config_path.display(),
        listen_address = %config.listen_address,
        health_listen_address = %health_server.local_addr(),
        "liminal server configuration validated"
    );

    tracing::info!(
        listen_address = %listener.local_addr(),
        health_listen_address = %health_server.local_addr(),
        "liminal server started"
    );

    shutdown_handle.wait();
    readiness.set_listener_bound(false);

    let supervisor = listener.supervisor();
    let shutdown_result = run_shutdown_sequence(&mut listener, &supervisor, config.drain_timeout());
    drop(signal_registration);
    health_server.shutdown()?;
    shutdown_result
}
