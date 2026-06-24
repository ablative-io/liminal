//! SRV-005 R1: seed-node discovery over beamr distribution.
//!
//! Beamr does not run EPMD; distribution callers supply a [`NodeResolver`] that
//! maps a node *name* to a socket address. A liminal operator configures seed
//! *addresses*, not names — the peer's real distribution name is only learned
//! from the authenticated handshake. We bridge the two with [`ClusterResolver`]:
//!
//! * Each configured seed gets a synthetic dial label (`seed-{i}@{addr}`) mapped
//!   to its address. Discovery dials those labels.
//! * [`ConnectionManager::connect`] re-keys the connection table by the name the
//!   peer advertises in the handshake — the synthetic label is throwaway and
//!   never becomes a table key, so it cannot collide with a real node name.
//! * After a successful dial we learn the peer's real name and register
//!   `real_name -> addr` in the resolver, so beamr can re-dial that peer by name
//!   later (e.g. an outbound send after a transient drop) without us re-deriving
//!   the address.
//!
//! The SAME resolver instance is handed to the channel-supervisor scheduler's
//! [`DistributionConfig`] and used here to dial seeds, so every distribution
//! component resolves names consistently.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use beamr::atom::AtomTable;
use beamr::distribution::connection::ConnectionManager;
use beamr::distribution::resolver::{NodeResolver, ResolveError, ResolveFuture, Resolver};

/// A distribution node resolver that learns `name -> address` mappings at runtime.
///
/// Pre-seeded with synthetic dial labels for each configured seed and extended
/// with each peer's real handshake name as connections are established.
#[derive(Debug, Default)]
pub struct ClusterResolver {
    nodes: RwLock<HashMap<String, SocketAddr>>,
}

impl ClusterResolver {
    /// Creates an empty resolver.
    #[must_use]
    pub fn new() -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
        }
    }

    /// Registers (or replaces) a `name -> address` mapping.
    pub fn register(&self, name: impl Into<String>, address: SocketAddr) {
        self.nodes
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(name.into(), address);
    }

    fn lookup(&self, name: &str) -> Option<SocketAddr> {
        self.nodes
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(name)
            .copied()
    }
}

impl NodeResolver for ClusterResolver {
    fn resolve<'a>(&'a self, name: &'a str) -> ResolveFuture<'a> {
        let result = self.lookup(name).ok_or(ResolveError::NotFound);
        Box::pin(async move { result })
    }
}

/// The synthetic dial label for the seed at position `index`.
#[must_use]
fn seed_label(index: usize, address: SocketAddr) -> String {
    format!("seed-{index}@{address}")
}

/// Builds a [`ClusterResolver`] pre-seeded with a synthetic dial label per seed
/// address and returns it alongside those labels (in seed order).
///
/// The resolver is returned as a concrete `Arc<ClusterResolver>` so the caller
/// can both hand it to the scheduler as a [`Resolver`] and keep registering
/// learned peer names on it; [`as_resolver`] performs the trait-object coercion.
#[must_use]
pub fn seed_resolver(seeds: &[SocketAddr]) -> (Arc<ClusterResolver>, Vec<String>) {
    let resolver = Arc::new(ClusterResolver::new());
    let labels = register_seed_labels(&resolver, seeds);
    (resolver, labels)
}

/// Registers a synthetic dial label per seed onto an EXISTING resolver.
///
/// Returns those labels in seed order. Used when the resolver was already built
/// and shared with the scheduler, so seed dialing resolves on the very same
/// instance the scheduler uses for every other lookup.
pub fn register_seed_labels(resolver: &ClusterResolver, seeds: &[SocketAddr]) -> Vec<String> {
    let mut labels = Vec::with_capacity(seeds.len());
    for (index, address) in seeds.iter().enumerate() {
        let label = seed_label(index, *address);
        resolver.register(label.clone(), *address);
        labels.push(label);
    }
    labels
}

/// Coerces a concrete cluster resolver into the shared distribution [`Resolver`]
/// handle the scheduler's `DistributionConfig` expects.
#[must_use]
pub fn as_resolver(resolver: Arc<ClusterResolver>) -> Resolver {
    resolver
}

/// Outcome of attempting to connect to the configured seeds (R1).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SeedConnectOutcome {
    /// Number of seeds dialed.
    pub attempted: usize,
    /// Number of seeds that completed the distribution handshake.
    pub connected: usize,
}

impl SeedConnectOutcome {
    /// True when at least one seed was reachable, or there were no seeds to dial
    /// (a lone bootstrap node forms a cluster of one and is not a join failure).
    #[must_use]
    pub const fn is_satisfied(&self) -> bool {
        self.attempted == 0 || self.connected > 0
    }
}

/// Dials every seed `label` through `connections`, learning each reachable
/// peer's real name into `resolver` (R1).
///
/// An unreachable seed is logged at warn level and skipped — discovery is
/// non-fatal per-seed. The caller decides whether the aggregate outcome
/// (no seed reachable when seeds were configured) is fatal.
pub async fn connect_seeds(
    connections: &ConnectionManager,
    resolver: &Arc<ClusterResolver>,
    atoms: &AtomTable,
    labels: &[String],
) -> SeedConnectOutcome {
    let mut outcome = SeedConnectOutcome {
        attempted: labels.len(),
        connected: 0,
    };
    for label in labels {
        match connections.connect(label).await {
            Ok(connection) => {
                let address = connection.peer_addr();
                if let Some(name) = atoms.resolve(connection.node()).map(str::to_owned) {
                    resolver.register(name.clone(), address);
                    tracing::info!(
                        seed_label = %label,
                        peer = %name,
                        peer_addr = %address,
                        "connected to cluster seed node"
                    );
                } else {
                    tracing::info!(
                        seed_label = %label,
                        peer_addr = %address,
                        "connected to cluster seed node"
                    );
                }
                outcome.connected += 1;
            }
            Err(error) => {
                tracing::warn!(
                    seed_label = %label,
                    error = %error,
                    "cluster seed node unreachable at startup; continuing with reachable seeds"
                );
            }
        }
    }
    outcome
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::{ClusterResolver, SeedConnectOutcome, as_resolver, seed_label, seed_resolver};
    use beamr::distribution::resolver::{NodeResolver, ResolveError};
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    fn resolve_now(resolver: &ClusterResolver, name: &str) -> Result<SocketAddr, ResolveError> {
        let waker = Waker::from(Arc::new(NoopWake));
        let mut context = Context::from_waker(&waker);
        let mut future = resolver.resolve(name);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => result,
            Poll::Pending => panic!("cluster resolver future should be ready immediately"),
        }
    }

    fn socket(address: &str) -> SocketAddr {
        address.parse().expect("valid socket address")
    }

    #[test]
    fn seed_resolver_maps_each_seed_to_a_synthetic_label() {
        let seeds = vec![socket("127.0.0.1:9000"), socket("127.0.0.1:9001")];
        let (resolver, labels) = seed_resolver(&seeds);

        assert_eq!(labels.len(), 2);
        assert_eq!(labels[0], seed_label(0, seeds[0]));
        assert_eq!(labels[1], seed_label(1, seeds[1]));
        assert_eq!(resolve_now(&resolver, &labels[0]), Ok(seeds[0]));
        assert_eq!(resolve_now(&resolver, &labels[1]), Ok(seeds[1]));
    }

    #[test]
    fn resolver_learns_real_peer_names() {
        let resolver = ClusterResolver::new();
        assert_eq!(
            resolve_now(&resolver, "node-b@host"),
            Err(ResolveError::NotFound)
        );
        resolver.register("node-b@host", socket("127.0.0.1:9100"));
        assert_eq!(
            resolve_now(&resolver, "node-b@host"),
            Ok(socket("127.0.0.1:9100"))
        );
    }

    #[test]
    fn as_resolver_coerces_to_shared_handle() {
        let (resolver, _labels) = seed_resolver(&[socket("127.0.0.1:9000")]);
        let shared = as_resolver(Arc::clone(&resolver));
        // The coerced handle resolves the same mappings.
        let waker = Waker::from(Arc::new(NoopWake));
        let mut context = Context::from_waker(&waker);
        let mut future = shared.resolve("seed-0@127.0.0.1:9000");
        let outcome = match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => result,
            Poll::Pending => panic!("future should be ready"),
        };
        assert_eq!(outcome, Ok(socket("127.0.0.1:9000")));
    }

    #[test]
    fn outcome_is_satisfied_when_no_seeds_or_some_connected() {
        assert!(
            SeedConnectOutcome {
                attempted: 0,
                connected: 0
            }
            .is_satisfied()
        );
        assert!(
            SeedConnectOutcome {
                attempted: 3,
                connected: 1
            }
            .is_satisfied()
        );
        assert!(
            !SeedConnectOutcome {
                attempted: 2,
                connected: 0
            }
            .is_satisfied()
        );
    }
}
