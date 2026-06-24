use std::path::Path;
use std::sync::Arc;

use crate::ServerError;
use crate::cluster::{self, ClusterHandle};
use crate::config::file::load_config;
use crate::config::types::ClusterConfig;
use crate::health::{ReadinessState, SharedReadinessState, start_health_server};
use crate::server::connection::ConnectionSupervisor;
use crate::server::connection::services::{ChannelCluster, LiminalConnectionServices};
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

    // Build the library-backed services once so we can reach the shared (possibly
    // clustered) channel supervisor before the connection supervisor takes
    // ownership of the services as a trait object.
    let services = Arc::new(LiminalConnectionServices::from_config(&config)?);
    let channel_cluster = services.channel_cluster().clone();
    let connection_supervisor = ConnectionSupervisor::with_services(services)?;

    // SRV-005: start clustering on the channel-supervisor scheduler when a
    // [cluster] section is configured. The returned handle owns the inbound
    // distribution listener and the membership poll loop; it must outlive the
    // server and is torn down in the shutdown sequence below.
    let cluster_handle = match config.cluster.as_ref() {
        Some(cluster_config) => Some(start_cluster(&channel_cluster, cluster_config)?),
        None => None,
    };

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

    // Tear the cluster down before draining connections: stop accepting peer
    // links and halt the membership poll loop. Each node shuts down independently
    // (no cluster-wide coordinated shutdown — that boundary belongs to SRV-004).
    if let Some(mut cluster_handle) = cluster_handle {
        cluster_handle.shutdown();
    }

    let supervisor = listener.supervisor();
    let shutdown_result = run_shutdown_sequence(&mut listener, &supervisor, config.drain_timeout());
    drop(signal_registration);
    health_server.shutdown()?;
    shutdown_result
}

/// Starts clustering on the shared channel supervisor's scheduler (SRV-005).
///
/// Installs the cluster `sync` as the supervisor's [`ClusterObserver`] so channel
/// subscribe/unsubscribe/publish events drive process-group membership and
/// cross-node fan-out.
fn start_cluster(
    channel_cluster: &ChannelCluster,
    cluster_config: &ClusterConfig,
) -> Result<ClusterHandle, ServerError> {
    let resolver = channel_cluster
        .resolver()
        .cloned()
        .ok_or_else(|| ServerError::ClusterJoin {
            message: "clustering configured but channel supervisor has no distribution resolver"
                .to_owned(),
        })?;
    let scheduler = channel_cluster.supervisor().scheduler();
    let supervisor = channel_cluster.supervisor().clone();
    cluster::start(&scheduler, resolver, cluster_config, move |sync| {
        supervisor.install_observer(Arc::new(sync));
    })
}
