//! SRV-005 R2/R3/R4: cluster membership by POLLING beamr's connection table,
//! plus the [`start`] entry point and the [`ClusterHandle`] that owns the
//! cluster's background resources.
//!
//! ## Why polling, not the connection-down hook
//!
//! Beamr's connection manager has a SINGLE connection-down callback slot, and
//! the scheduler already owns it: on node down it calls
//! `PgRegistry::purge_remote_node`, which is exactly the R6 remote-subscription
//! cleanup this cluster needs for free. Registering our own callback would
//! REPLACE that one and break R6. So membership never touches the hook — it
//! derives joins and departures by diffing successive snapshots of
//! [`ConnectionManager::connected_nodes`]:
//!
//! * R2/R3 join: a peer appears in `connected_nodes()` after a successful
//!   connect or accepted handshake; the first poll that sees it logs a join and
//!   notifies sync (which backfills its local subscriptions to the newcomer).
//! * R3 graceful leave / R4 failure: when a peer's TCP link drops, beamr removes
//!   it from the table (and, via the scheduler's hook, purges its remote pg
//!   members — R6). The next poll sees it gone, logs a departure, and notifies
//!   sync. The two observers (our poll, beamr's hook) read the same table
//!   independently with no contention.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use beamr::atom::{Atom, AtomTable};
use beamr::distribution::connection::{AcceptHandle, ConnectionManager};
use beamr::scheduler::Scheduler;

use crate::ServerError;
use crate::cluster::discovery::{self, ClusterResolver};
use crate::cluster::sync::ClusterSync;
use crate::config::types::ClusterConfig;

/// Interval between membership polls. Node-down handling for R6 does NOT depend
/// on this cadence (beamr's own hook drives the pg purge synchronously on the
/// drop); the poll only drives membership logging and R5 peer-join backfill, so
/// a sub-second cadence keeps the cluster view fresh without busy-spinning.
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// A membership transition computed by diffing two connection snapshots.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MembershipDelta {
    /// Peers that appeared since the previous snapshot.
    pub joined: Vec<Atom>,
    /// Peers that disappeared since the previous snapshot.
    pub left: Vec<Atom>,
}

impl MembershipDelta {
    /// True when no peer joined or left.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.joined.is_empty() && self.left.is_empty()
    }
}

/// Tracks cluster peers by polling the distribution connection table.
#[derive(Clone)]
pub struct Membership {
    inner: Arc<MembershipInner>,
}

struct MembershipInner {
    connections: ConnectionManager,
    atoms: Arc<AtomTable>,
    peers: Mutex<BTreeSet<Atom>>,
}

impl std::fmt::Debug for Membership {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Membership")
            .field("peer_count", &self.peers().len())
            .finish()
    }
}

impl Membership {
    /// Creates a membership tracker over `connections` with an empty peer set.
    #[must_use]
    pub fn new(connections: ConnectionManager, atoms: Arc<AtomTable>) -> Self {
        Self {
            inner: Arc::new(MembershipInner {
                connections,
                atoms,
                peers: Mutex::new(BTreeSet::new()),
            }),
        }
    }

    /// The currently-tracked peers, sorted by atom index.
    #[must_use]
    pub fn peers(&self) -> Vec<Atom> {
        self.lock_peers().iter().copied().collect()
    }

    /// The currently-tracked peers as resolved node-name strings.
    #[must_use]
    pub fn peer_names(&self) -> Vec<String> {
        self.peers()
            .into_iter()
            .filter_map(|peer| self.inner.atoms.resolve(peer).map(str::to_owned))
            .collect()
    }

    /// Polls the connection table once, updates the tracked peer set, and returns
    /// the join/leave delta since the previous poll.
    #[must_use]
    pub fn poll_once(&self) -> MembershipDelta {
        let current: BTreeSet<Atom> = self
            .inner
            .connections
            .connected_nodes()
            .into_iter()
            .collect();
        let mut tracked = self.lock_peers();
        let joined: Vec<Atom> = current.difference(&tracked).copied().collect();
        let left: Vec<Atom> = tracked.difference(&current).copied().collect();
        *tracked = current;
        drop(tracked);
        MembershipDelta { joined, left }
    }

    fn name(&self, peer: Atom) -> String {
        self.inner
            .atoms
            .resolve(peer)
            .map_or_else(|| format!("<atom {peer:?}>"), str::to_owned)
    }

    fn lock_peers(&self) -> std::sync::MutexGuard<'_, BTreeSet<Atom>> {
        self.inner
            .peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Owns the cluster's live background resources. Dropping it stops the membership
/// poll loop and tears down the inbound distribution listener.
pub struct ClusterHandle {
    accept: AcceptHandle,
    poll: Option<PollLoop>,
    membership: Membership,
    /// The runtime that drove cluster bring-up and that the inbound accept loop
    /// keeps running on. It MUST outlive the listener: the accept and per-link
    /// read tasks are spawned onto this runtime's handle, so dropping it would
    /// abort them and silently stop accepting peers. Kept here so it lives for
    /// the cluster's whole lifetime. Dropped last (fields drop in declaration
    /// order) so the listener and poll loop wind down before the runtime does.
    _runtime: Arc<tokio::runtime::Runtime>,
}

impl std::fmt::Debug for ClusterHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClusterHandle")
            .field("listen_addr", &self.accept.local_addr())
            .field("membership", &self.membership)
            .finish_non_exhaustive()
    }
}

impl ClusterHandle {
    /// The address the distribution listener bound for inbound peer links.
    #[must_use]
    pub fn listen_addr(&self) -> SocketAddr {
        self.accept.local_addr()
    }

    /// The membership tracker, for inspection and tests.
    #[must_use]
    pub const fn membership(&self) -> &Membership {
        &self.membership
    }

    /// Stops the poll loop and the inbound listener. Idempotent.
    pub fn shutdown(&mut self) {
        if let Some(poll) = self.poll.take() {
            poll.stop();
        }
        self.accept.shutdown();
    }
}

impl Drop for ClusterHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// The background membership poll thread and its stop flag.
struct PollLoop {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl PollLoop {
    fn start(membership: Membership, sync: ClusterSync) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_for_thread = Arc::clone(&stop);
        let handle = std::thread::Builder::new()
            .name("liminal-cluster-membership".to_owned())
            .spawn(move || {
                run_poll_loop(&membership, &sync, &stop_for_thread);
            })
            .ok();
        Self { stop, handle }
    }

    fn stop(mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn run_poll_loop(membership: &Membership, sync: &ClusterSync, stop: &AtomicBool) {
    while !stop.load(Ordering::SeqCst) {
        apply_delta(membership, sync, membership.poll_once());
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// Logs and dispatches a single membership delta (R3/R4/R5).
fn apply_delta(membership: &Membership, sync: &ClusterSync, delta: MembershipDelta) {
    for peer in delta.joined {
        let name = membership.name(peer);
        tracing::info!(peer = %name, peers = ?membership.peer_names(), "cluster peer joined");
        // R5: re-advertise our local subscriptions to the newcomer — a fresh
        // pg.join only broadcasts on the insert edge, so a node that joins after
        // our subscribers already registered would otherwise never learn them.
        sync.on_peer_join(peer);
    }
    for peer in delta.left {
        let name = membership.name(peer);
        // R4: a lost peer is a warning; R6 cleanup of its remote pg members has
        // already happened via beamr's connection-down hook (purge_remote_node).
        tracing::warn!(peer = %name, peers = ?membership.peer_names(), "cluster peer left");
        sync.on_peer_leave(peer);
    }
}

/// Starts clustering on the channel-supervisor `scheduler` (SRV-005).
///
/// Steps, in order:
/// 1. Bind the inbound distribution listener (so peers can dial us) BEFORE we
///    dial seeds, mirroring beamr's own bring-up order.
/// 2. Dial each configured seed (R1); an unreachable seed is non-fatal, but if
///    seeds were configured and none was reachable we return
///    [`ServerError::ClusterJoin`].
/// 3. Build the membership tracker and the subscription sync, install sync as
///    the channel-supervisor's observer, and spawn the membership poll loop.
///
/// `resolver` MUST be the same [`ClusterResolver`] handed to the scheduler's
/// `DistributionConfig` (so handshake-learned names resolve everywhere).
///
/// `on_established` is invoked exactly once, on the success path, at the moment
/// this node's cluster machinery is up: the listener is bound, the seed-dial pass
/// has completed under the non-fatal policy above (zero seeds is a valid
/// single-node bootstrap), and membership plus sync are built and installed. It
/// signals per-node cluster readiness (G2) and is NOT called on any error path.
///
/// # Errors
/// Returns [`ServerError::ClusterJoin`] when the listener cannot bind or when no
/// configured seed was reachable.
pub fn start(
    scheduler: &Arc<Scheduler>,
    resolver: Arc<ClusterResolver>,
    config: &ClusterConfig,
    install_observer: impl FnOnce(ClusterSync),
    on_established: impl FnOnce(),
) -> Result<ClusterHandle, ServerError> {
    // Typed absence (beamr 0.14 honest-None surface): a scheduler composed
    // WITHOUT distribution cannot join a cluster — refused at bring-up, the
    // same refuse-at-birth posture readiness composition uses (plan §2).
    let connections =
        scheduler
            .try_distribution_connections()
            .ok_or_else(|| ServerError::ClusterJoin {
                message: "scheduler was composed without a distribution service; \
                      cluster membership requires one"
                    .to_owned(),
            })?;
    let atoms = Arc::clone(scheduler.atom_table());
    let pg = scheduler.pg_registry();
    let local_node = atoms.intern(&config.node_name);

    // Register a synthetic dial label per seed onto the SHARED resolver the
    // scheduler already uses, so seed dialing resolves on that same instance.
    let labels = discovery::register_seed_labels(&resolver, &config.seed_nodes);

    // A multi-thread runtime that drives cluster bring-up AND stays alive for the
    // cluster's lifetime: the inbound accept loop and the per-link read tasks are
    // spawned onto this runtime, so it must outlive the listener. A current-thread
    // runtime would also deadlock the bring-up handshake (the outbound connect and
    // the inbound accept must interleave reads/writes concurrently).
    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|error| ServerError::ClusterJoin {
                message: format!("failed to build cluster runtime: {error}"),
            })?,
    );
    // Bind this runtime to the distribution connection manager so the accept and
    // read lifecycle tasks run on it (and survive for the cluster's lifetime),
    // rather than on any transient ambient runtime.
    connections.set_runtime_handle(runtime.handle().clone());

    let accept = runtime
        .block_on(scheduler.start_distribution_listener(config.listen_address))
        .map_err(|error| ServerError::ClusterJoin {
            message: format!(
                "failed to bind cluster distribution listener on {}: {error}",
                config.listen_address
            ),
        })?;

    let outcome = runtime.block_on(discovery::connect_seeds(
        &connections,
        &resolver,
        &atoms,
        &labels,
    ));
    if !outcome.is_satisfied() {
        return Err(ServerError::ClusterJoin {
            message: format!(
                "no configured seed node was reachable ({} attempted)",
                outcome.attempted
            ),
        });
    }

    let membership = Membership::new(connections.clone(), Arc::clone(&atoms));
    let sync = ClusterSync::new(pg, Arc::clone(&atoms), connections, local_node, resolver);
    install_observer(sync.clone());

    // Seed the tracked set from the connections established during discovery and
    // log the initial membership (R2), backfilling our state to each peer.
    apply_delta(&membership, &sync, membership.poll_once());
    tracing::info!(
        node_name = %config.node_name,
        peers = ?membership.peer_names(),
        "cluster membership established"
    );

    // G2: the node's cluster stack is now up (listener bound, seed-dial pass
    // done, membership + sync installed). Signal established readiness. This is
    // per-node liveness of the cluster machinery, NOT quorum: a single-node
    // bootstrap with zero reachable peers is legitimately established.
    on_established();

    let poll = PollLoop::start(membership.clone(), sync);
    Ok(ClusterHandle {
        accept,
        poll: Some(poll),
        membership,
        _runtime: runtime,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::{Membership, MembershipDelta};
    use beamr::atom::AtomTable;
    use beamr::distribution::connection::ConnectionManager;
    use beamr::distribution::resolver::StaticResolver;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn empty_manager(atoms: &Arc<AtomTable>) -> ConnectionManager {
        ConnectionManager::new(
            Arc::clone(atoms),
            Arc::new(StaticResolver::new(HashMap::new())),
            "test-cookie",
            "local@127.0.0.1",
            1,
        )
    }

    #[test]
    fn delta_is_empty_by_default() {
        assert!(MembershipDelta::default().is_empty());
    }

    #[test]
    fn first_poll_of_empty_table_yields_no_peers() {
        let atoms = Arc::new(AtomTable::with_common_atoms());
        let membership = Membership::new(empty_manager(&atoms), Arc::clone(&atoms));
        let delta = membership.poll_once();
        assert!(delta.is_empty());
        assert!(membership.peers().is_empty());
    }

    #[test]
    fn peer_names_resolve_through_the_atom_table() {
        let atoms = Arc::new(AtomTable::with_common_atoms());
        let membership = Membership::new(empty_manager(&atoms), Arc::clone(&atoms));
        // No connections, so no names — but the accessor must not panic.
        assert!(membership.peer_names().is_empty());
    }
}
