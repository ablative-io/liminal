//! SRV-005: the shared (optionally clustered) channel supervisor used by the
//! server's connection services.
//!
//! Extracted from `services.rs` to keep that file within the 500-line limit. A
//! single [`ChannelCluster`] holds the one [`ChannelSupervisor`] every configured
//! channel runs on, plus the [`ClusterResolver`] shared with that supervisor's
//! scheduler when clustering is configured. Building it is the only place the
//! choice between a clustered and a plain supervisor is made.

use std::sync::Arc;

use liminal::channel::{ChannelRestartPolicy, ChannelSupervisor};

use crate::ServerError;
use crate::cluster::discovery::{ClusterResolver, as_resolver};
use crate::config::types::ClusterConfig;

/// The shared channel supervisor backing every configured channel (SRV-005).
///
/// Plus the cluster resolver when clustering is configured. All channels run on
/// this ONE supervisor's scheduler so they share the clustered distribution
/// transport; the resolver is the SAME instance handed to that scheduler, kept
/// here so the cluster can dial seeds and learn peer names on it.
#[derive(Clone, Debug)]
pub struct ChannelCluster {
    supervisor: ChannelSupervisor,
    resolver: Option<Arc<ClusterResolver>>,
}

impl ChannelCluster {
    /// The shared channel supervisor.
    #[must_use]
    pub const fn supervisor(&self) -> &ChannelSupervisor {
        &self.supervisor
    }

    /// The cluster resolver, present only when clustering is configured.
    #[must_use]
    pub const fn resolver(&self) -> Option<&Arc<ClusterResolver>> {
        self.resolver.as_ref()
    }
}

/// The channel restart policy used for server-hosted channels. Matches the
/// library default (one-for-one with a bounded budget) and is passed explicitly
/// because the distribution-enabled constructor takes a policy.
fn server_channel_policy() -> ChannelRestartPolicy {
    ChannelRestartPolicy::default()
}

/// Builds the shared channel supervisor (SRV-005).
///
/// When `cluster` is `Some`, the supervisor's scheduler is distribution-enabled
/// with the configured node identity and cookie, and a [`ClusterResolver`] is
/// created and shared between the scheduler and the returned [`ChannelCluster`].
/// When `None`, an ordinary non-clustered supervisor is built.
///
/// # Errors
/// Returns [`ServerError`] when the underlying scheduler cannot start.
pub fn build_channel_cluster(
    cluster: Option<&ClusterConfig>,
) -> Result<ChannelCluster, ServerError> {
    let Some(cluster) = cluster else {
        let supervisor =
            ChannelSupervisor::with_policy(server_channel_policy()).map_err(|error| {
                ServerError::ConfigValidation {
                    message: format!("failed to start channel supervisor: {error}"),
                }
            })?;
        return Ok(ChannelCluster {
            supervisor,
            resolver: None,
        });
    };
    let resolver = Arc::new(ClusterResolver::new());
    let supervisor = ChannelSupervisor::with_distribution(
        cluster.node_name.clone(),
        node_creation(),
        cluster.cookie.clone(),
        as_resolver(Arc::clone(&resolver)),
        server_channel_policy(),
    )
    .map_err(|error| ServerError::ClusterJoin {
        message: format!("failed to start clustered channel supervisor: {error}"),
    })?;
    Ok(ChannelCluster {
        supervisor,
        resolver: Some(resolver),
    })
}

/// A non-zero node-incarnation value for beamr distribution. Derived from the
/// process start time so two incarnations of the same node name on one host get
/// distinct creations; never zero (beamr reserves 0 for "unknown creation").
fn node_creation() -> u32 {
    let since_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    // Fold the 64-bit seconds into 31 bits (the low word, masked positive) so the
    // creation is a stable-per-process, non-zero value; the exact bits do not
    // matter, only that incarnations differ and none is zero.
    let folded = u32::try_from(since_epoch & u64::from(u32::MAX)).unwrap_or(u32::MAX);
    (folded & 0x7fff_ffff).max(1)
}
