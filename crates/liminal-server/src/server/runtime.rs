use std::path::Path;
use std::sync::Arc;

use crate::ServerError;
use crate::cluster::{self, ClusterHandle};
use crate::config::file::load_config;
use crate::config::types::{ClusterConfig, ServiceProfile};
use crate::health::{ReadinessState, SharedReadinessState, start_health_server};
use crate::server::connection::ConnectionSupervisor;
use crate::server::connection::services::{
    ChannelCluster, LiminalConnectionServices, build_connection_services,
};
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

    // Enable metrics for this process before the health server accepts scrapes,
    // so `/metrics` renders the server families. Standalone liminal library users
    // never call this, so the registry gate stays off for them.
    crate::metrics::init();

    let readiness = SharedReadinessState::new(ReadinessState::default());
    let health_server = start_health_server(config.health_listen_address, readiness.clone())?;
    let shutdown_handle = ShutdownHandle::new();
    let signal_registration = register_signal_handlers(shutdown_handle.clone())?;

    // The configured [auth] token must ride along here: these call sites build
    // services themselves (full mode reaches the shared channel cluster first;
    // the worker front door builds no cluster at all) and so cannot use
    // `from_config`, which is the only other place the token is wired.
    let auth_token = config
        .auth
        .as_ref()
        .map(|auth| auth.token.clone().into_bytes());

    // D2: the service profile selects which connection-services stack is built.
    // Full mode is byte-for-byte the previous construction path (build services,
    // reach the shared channel cluster, start clustering when configured). The
    // worker front door constructs the connection supervisor over the
    // capability-scoped adapter and NOTHING else — no channel/conversation/haematite
    // services, and therefore no distribution cluster (config validation rejects a
    // `[cluster]` section under this profile, so none can be present here).
    let (connection_supervisor, cluster_handle) = match config.services.profile()? {
        ServiceProfile::Full => {
            let services = Arc::new(LiminalConnectionServices::from_config(&config)?);
            let channel_cluster = services.channel_cluster().clone();
            let connection_supervisor =
                ConnectionSupervisor::with_services_and_auth(services, auth_token)?;

            // SRV-005: start clustering on the channel-supervisor scheduler when a
            // [cluster] section is configured. The returned handle owns the inbound
            // distribution listener and the membership poll loop; it must outlive the
            // server and is torn down in the shutdown sequence below.
            readiness.set_cluster_configured(config.cluster.is_some());
            let cluster_handle = match config.cluster.as_ref() {
                Some(cluster_config) => {
                    Some(start_cluster(&channel_cluster, cluster_config, &readiness)?)
                }
                None => None,
            };
            (connection_supervisor, cluster_handle)
        }
        ServiceProfile::WorkerFrontDoor => {
            let services = build_connection_services(&config)?;
            let connection_supervisor =
                ConnectionSupervisor::with_services_and_auth(services, auth_token)?;
            readiness.set_cluster_configured(false);
            (connection_supervisor, None)
        }
    };

    let mut listener = ServerListener::bind(&config, connection_supervisor)?;
    readiness.set_config_loaded(true);
    readiness.set_listener_bound(true);

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
///
/// On the success path this marks cluster membership as established on `readiness`
/// (G2) via [`cluster::start`]'s `on_established` hook, so a clustered server's
/// `/ready` endpoint transitions from 503 to 200 once the cluster stack is up.
/// Every early return here (missing resolver, listener bind failure, no reachable
/// seed) leaves the flag unset, so `/ready` stays 503.
fn start_cluster(
    channel_cluster: &ChannelCluster,
    cluster_config: &ClusterConfig,
    readiness: &SharedReadinessState,
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
    let readiness = readiness.clone();
    cluster::start(
        &scheduler,
        resolver,
        cluster_config,
        move |sync| {
            supervisor.install_observer(Arc::new(sync));
        },
        move || readiness.set_cluster_membership_established(true),
    )
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::{ChannelCluster, ClusterConfig, SharedReadinessState, start_cluster};
    use crate::ServerError;
    use crate::health::{ClusterReadiness, ReadinessCondition, ReadinessState, readiness_check};
    use crate::server::connection::services::LiminalConnectionServices;

    /// A channel cluster with NO distribution resolver — the shape produced when a
    /// server was built without a `[cluster]` section. `start_cluster` must reject
    /// it before touching `cluster::start`, so its `on_established` hook never runs.
    fn unclustered_channel_cluster() -> Result<ChannelCluster, ServerError> {
        Ok(LiminalConnectionServices::empty()?
            .channel_cluster()
            .clone())
    }

    fn clustered_but_unmet_readiness() -> SharedReadinessState {
        SharedReadinessState::new(ReadinessState::new(
            true,
            true,
            ClusterReadiness::Configured {
                membership_established: false,
            },
        ))
    }

    fn sample_cluster_config() -> Result<ClusterConfig, Box<dyn std::error::Error>> {
        let listen_address: SocketAddr = "127.0.0.1:0".parse()?;
        Ok(ClusterConfig {
            node_name: "node-under-test@127.0.0.1".to_owned(),
            listen_address,
            seed_nodes: Vec::new(),
            cookie: "runtime-test-cookie".to_owned(),
        })
    }

    #[test]
    fn failed_cluster_start_leaves_membership_unestablished()
    -> Result<(), Box<dyn std::error::Error>> {
        let readiness = clustered_but_unmet_readiness();
        let channel_cluster = unclustered_channel_cluster()?;
        let config = sample_cluster_config()?;

        // Missing-resolver failure path: start_cluster returns Err before the
        // established hook can fire.
        let result = start_cluster(&channel_cluster, &config, &readiness);
        assert!(
            result.is_err(),
            "start_cluster must fail without a distribution resolver"
        );

        // The readiness flag stays unset, so /ready still lists the unmet gate.
        let status = readiness_check(&readiness.snapshot());
        assert!(
            !status.ready,
            "readiness must remain not-ready after a failed start"
        );
        assert!(
            status
                .unmet_conditions
                .contains(&ReadinessCondition::ClusterMembershipEstablished),
            "cluster membership gate must stay unmet after a failed start"
        );

        Ok(())
    }
}
