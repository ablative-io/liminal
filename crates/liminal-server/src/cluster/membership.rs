//! SRV-005 R2/R3/R4 + SRV-008: cluster membership driven by beamr's ORDERED
//! connection events, plus the [`start`] entry point and the [`ClusterHandle`]
//! that owns the cluster's background resources.
//!
//! ## The source: an atomic initial view, then ordered deltas
//!
//! Membership is TOLD, never sampled. [`Membership::new`] arms
//! `ConnectionManager::subscribe_connection_events_with_snapshot`, beamr's
//! blessed late-subscriber path. Before that call returns, our callback is
//! invoked with a synthetic `Up` for every currently live peer, and the
//! registration completes — all under beamr's event-dispatch gate, so no real
//! event can interleave between the snapshot and the registration. The tracker
//! therefore starts from a race-free initial view and continues on an ordered
//! stream that misses no session and repeats none:
//!
//! * INV-ALTERNATION — per node the delivered events are `Up(g1) Down(g1)
//!   Up(g2) …` with strictly increasing generations, so a set insert on `Up` and
//!   a set remove on `Down` is the whole state machine.
//! * INV-EXACTLY-ONCE — one `Up` and one `Down` per generation, so there are no
//!   duplicates to dedupe and nothing to coalesce.
//! * INV-SYNC — delivery is synchronous with the transition, so the tracked set
//!   is already correct when the call that caused the transition returns.
//!
//! ## Why we still never take beamr's single connection-down slot
//!
//! Beamr's connection manager has a SINGLE legacy connection-down callback slot,
//! and the scheduler already owns it: on node down it calls
//! `PgRegistry::purge_remote_node`, which is exactly the R6 remote-subscription
//! cleanup this cluster needs for free. Registering our own callback would
//! REPLACE that one and break R6. Membership never touches the slot; it uses the
//! multi-subscriber hub instead, and INV-SCHED-FIRST guarantees the scheduler's
//! composed subscriber (pg-purge included) runs before ours — so a peer's remote
//! pg members are already purged by the time we observe its departure.
//!
//! ## Why the callback hands off instead of acting
//!
//! beamr's INV-SUB-DISCIPLINE binds every subscriber: callbacks MUST NOT block,
//! MUST NOT perform socket I/O, and MUST capture only `Weak` handles — "a
//! blocked callback stalls reads, writes, heartbeats, accepts AND concurrent
//! transition callers for EVERY peer". R5's join backfill
//! ([`ClusterSync::on_peer_join`]) writes frames to the newcomer's socket, so it
//! categorically cannot run on the delivery thread.
//!
//! So the callback does the smallest possible amount of work — update the peer
//! set under a short-hold mutex (explicitly permitted), push the resulting delta
//! onto a local FIFO, and signal a condvar — and a consumer thread owned by this
//! module runs the logging and the backfill off the delivery thread.
//!
//! ### Overflow rule: lossless, unbounded, loud depth
//!
//! The handoff FIFO never drops and never blocks its producer, and that pair of
//! constraints forces it to be unbounded:
//!
//! * Dropping is inadmissible. beamr's INV-NO-REPLAY means a discarded join
//!   delta is gone forever — the newcomer would permanently miss its R5
//!   backfill — and a discarded leave delta would permanently corrupt the
//!   tracked set.
//! * Blocking the producer is forbidden by INV-SUB-DISCIPLINE, and would stall
//!   every peer's I/O, not just this one's.
//!
//! Growth is bounded in practice because an entry is produced only by a real
//! connection-table transition and the consumer does nothing but drain. The
//! residual risk is disclosed rather than silent: [`Membership::queue_high_water`]
//! reports the deepest the FIFO has ever been, and the consumer warns once if a
//! single drain ever exceeds [`EFFECT_QUEUE_WARN_DEPTH`].
//!
//! The authoritative peer set is NOT behind the FIFO — it is mutated in the
//! callback — so [`Membership::peers`] cannot be skewed by consumer lag. The
//! FIFO carries only the effects: join logging, R5 backfill, leave logging.

use std::collections::{BTreeSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError, Weak};
use std::thread::JoinHandle;

use beamr::atom::{Atom, AtomTable};
use beamr::distribution::connection::{AcceptHandle, ConnectionManager};
use beamr::distribution::connection_events::{ConnectionEvent, SubscriberId};
use beamr::scheduler::Scheduler;

use crate::ServerError;
use crate::cluster::discovery::{self, ClusterResolver};
use crate::cluster::sync::ClusterSync;
use crate::config::types::ClusterConfig;

/// Depth at which a single drain of the membership effect FIFO is reported as
/// pathological. Not a capacity: the queue is lossless and nothing is discarded
/// at or above this depth — it exists so unbounded growth is loud instead of
/// silent. A real cluster produces one entry per connection transition, so a
/// drain this deep means the consumer is being starved and an operator should
/// know.
const EFFECT_QUEUE_WARN_DEPTH: usize = 1024;

/// A membership transition carried from the event source to the consumer.
///
/// beamr delivers one transition at a time (INV-ALTERNATION, INV-EXACTLY-ONCE),
/// so a delta produced by the source names exactly one peer on exactly one side.
/// The batching shape is retained because it is the consumer-facing surface and
/// the initial view legitimately yields several.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MembershipDelta {
    /// Peers that joined.
    pub joined: Vec<Atom>,
    /// Peers that left.
    pub left: Vec<Atom>,
}

impl MembershipDelta {
    /// True when no peer joined or left.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.joined.is_empty() && self.left.is_empty()
    }
}

/// Tracks cluster peers from beamr's ordered connection-event stream.
///
/// Cloning shares one tracker: the arm, the peer set, the effect FIFO, and the
/// counters all live behind a single `Arc`.
#[derive(Clone)]
pub struct Membership {
    inner: Arc<MembershipInner>,
}

/// The pending effects and the consumer's terminal flag, under one mutex so a
/// single condvar covers both "work arrived" and "shut down".
#[derive(Default)]
struct EffectQueue {
    pending: VecDeque<MembershipDelta>,
    shutdown: bool,
}

struct MembershipInner {
    connections: ConnectionManager,
    atoms: Arc<AtomTable>,
    /// The authoritative peer set. Written in the subscriber callback under this
    /// short-hold mutex so readers are correct the instant a transition returns.
    peers: Mutex<BTreeSet<Atom>>,
    queue: Mutex<EffectQueue>,
    wake: Condvar,
    /// The live subscription, taken on shutdown so unsubscribing is idempotent.
    subscription: Mutex<Option<SubscriberId>>,
    events_observed: AtomicU64,
    consumer_wakes: AtomicU64,
    source_snapshots: AtomicU64,
    queue_high_water: AtomicUsize,
    depth_warned: AtomicBool,
}

impl MembershipInner {
    /// The subscriber callback body. INV-SUB-DISCIPLINE: no blocking, no socket
    /// I/O, no logging, no allocation beyond one small delta — only two
    /// short-hold mutexes, a few relaxed atomics, and a condvar signal.
    fn observe(&self, event: ConnectionEvent) {
        self.events_observed.fetch_add(1, Ordering::Relaxed);
        let node = event.node();
        let delta = {
            let mut tracked = self.peers.lock().unwrap_or_else(PoisonError::into_inner);
            match event {
                ConnectionEvent::Up(_) if tracked.insert(node) => MembershipDelta {
                    joined: vec![node],
                    left: Vec::new(),
                },
                ConnectionEvent::Down(_) if tracked.remove(&node) => MembershipDelta {
                    joined: Vec::new(),
                    left: vec![node],
                },
                // INV-ALTERNATION rules the redundant cases out upstream; if one
                // ever arrives the set is already right and there is no effect to
                // run, so it is absorbed rather than double-counted.
                _ => return,
            }
        };
        let depth = {
            let mut queue = self.queue.lock().unwrap_or_else(PoisonError::into_inner);
            queue.pending.push_back(delta);
            queue.pending.len()
        };
        self.queue_high_water.fetch_max(depth, Ordering::Relaxed);
        self.wake.notify_one();
    }
}

impl Drop for MembershipInner {
    /// Detach from the event hub when the last tracker handle goes away, so a
    /// dropped tracker leaves no registration behind on a still-live manager.
    fn drop(&mut self) {
        let id = self
            .subscription
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(id) = id {
            self.connections.unsubscribe_connection_events(id);
        }
    }
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
    /// Arms a membership tracker on `connections`.
    ///
    /// The subscription is established here, not later: before this returns, the
    /// callback has been handed a synthetic `Up` for every live peer under
    /// beamr's dispatch gate, so the tracker's initial view is atomic with
    /// respect to the ordered stream that follows. Peers already connected by
    /// seed discovery therefore appear immediately, and their join effects are
    /// queued for the consumer.
    #[must_use]
    pub fn new(connections: ConnectionManager, atoms: Arc<AtomTable>) -> Self {
        let inner = Arc::new(MembershipInner {
            connections,
            atoms,
            peers: Mutex::new(BTreeSet::new()),
            queue: Mutex::new(EffectQueue::default()),
            wake: Condvar::new(),
            subscription: Mutex::new(None),
            events_observed: AtomicU64::new(0),
            consumer_wakes: AtomicU64::new(0),
            source_snapshots: AtomicU64::new(0),
            queue_high_water: AtomicUsize::new(0),
            depth_warned: AtomicBool::new(false),
        });

        // Weak, never Arc: INV-SUB-DISCIPLINE requires it, and it is also what
        // breaks the cycle (the manager owns the hub, the hub owns this
        // callback, and this tracker owns a handle to the manager).
        let weak: Weak<MembershipInner> = Arc::downgrade(&inner);
        inner.source_snapshots.fetch_add(1, Ordering::Relaxed);
        let id = inner
            .connections
            .subscribe_connection_events_with_snapshot(move |event| {
                if let Some(inner) = weak.upgrade() {
                    inner.observe(event);
                }
            });
        *inner
            .subscription
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(id);

        Self { inner }
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

    /// How many connection events this tracker has been handed. Flat means the
    /// backend told us nothing, which is the only reason membership may be flat.
    #[must_use]
    pub fn events_observed(&self) -> u64 {
        self.inner.events_observed.load(Ordering::Relaxed)
    }

    /// How many times the effect FIFO has been drained — the consumer-side wake
    /// count. A drain runs only because a delta was pushed or shutdown was
    /// signalled; spurious condvar wakeups are absorbed by the wait predicate and
    /// perform no work, so they are not counted. Includes the one synchronous
    /// drain that applies the initial view at bring-up.
    #[must_use]
    pub fn consumer_wakes(&self) -> u64 {
        self.inner.consumer_wakes.load(Ordering::Relaxed)
    }

    /// How many times this tracker has asked the backend for an initial view.
    /// Exactly one per arm, for the lifetime of the tracker — there is no
    /// cadence, so this never grows again.
    #[must_use]
    pub fn source_snapshots(&self) -> u64 {
        self.inner.source_snapshots.load(Ordering::Relaxed)
    }

    /// The deepest the lossless effect FIFO has ever been. The disclosed bound on
    /// the unbounded-queue tradeoff.
    #[must_use]
    pub fn queue_high_water(&self) -> usize {
        self.inner.queue_high_water.load(Ordering::Relaxed)
    }

    /// Effects queued but not yet applied by the consumer.
    #[must_use]
    pub fn pending_effects(&self) -> usize {
        self.lock_queue().pending.len()
    }

    /// Blocks until there is work or shutdown. `None` means the consumer is done:
    /// shutdown was signalled and every queued effect has already been handed
    /// out. There is no timeout and no flag sampling — the thread is TOLD.
    fn wait_for_effects(&self) -> Option<Vec<MembershipDelta>> {
        let mut queue = self
            .inner
            .wake
            .wait_while(self.lock_queue(), |queue| {
                queue.pending.is_empty() && !queue.shutdown
            })
            .unwrap_or_else(PoisonError::into_inner);
        self.inner.consumer_wakes.fetch_add(1, Ordering::Relaxed);
        if queue.pending.is_empty() {
            return None;
        }
        Some(queue.pending.drain(..).collect())
    }

    /// Takes whatever is queued right now without blocking, counting the pass.
    /// Used once at bring-up to apply the atomic initial view on the starting
    /// thread, before the continuation is consumed.
    fn take_pending(&self) -> Vec<MembershipDelta> {
        self.inner.consumer_wakes.fetch_add(1, Ordering::Relaxed);
        self.lock_queue().pending.drain(..).collect()
    }

    /// Signals the consumer to finish and wakes it, even with nothing pending.
    fn signal_shutdown(&self) {
        self.lock_queue().shutdown = true;
        self.inner.wake.notify_all();
    }

    /// Detaches from the event hub. Idempotent.
    fn unsubscribe(&self) {
        let id = self
            .inner
            .subscription
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(id) = id {
            self.inner.connections.unsubscribe_connection_events(id);
        }
    }

    fn warn_once_on_depth(&self, depth: usize) {
        if depth >= EFFECT_QUEUE_WARN_DEPTH
            && !self.inner.depth_warned.swap(true, Ordering::Relaxed)
        {
            tracing::warn!(
                depth,
                high_water = self.queue_high_water(),
                "cluster membership effect queue is unusually deep; the queue is \
                 lossless so nothing was discarded, but the consumer is being starved"
            );
        }
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
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn lock_queue(&self) -> std::sync::MutexGuard<'_, EffectQueue> {
        self.inner
            .queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// Owns the cluster's live background resources. Dropping it stops the membership
/// consumer, detaches from the event source, and tears down the inbound
/// distribution listener.
pub struct ClusterHandle {
    accept: AcceptHandle,
    consumer: Option<MembershipConsumer>,
    membership: Membership,
    /// The runtime that drove cluster bring-up and that the inbound accept loop
    /// keeps running on. It MUST outlive the listener: the accept and per-link
    /// read tasks are spawned onto this runtime's handle, so dropping it would
    /// abort them and silently stop accepting peers. Kept here so it lives for
    /// the cluster's whole lifetime. Dropped last (fields drop in declaration
    /// order) so the listener and consumer wind down before the runtime does.
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

    /// Stops the membership consumer, detaches from the event source, and stops
    /// the inbound listener. Idempotent.
    pub fn shutdown(&mut self) {
        if let Some(consumer) = self.consumer.take() {
            consumer.stop();
        }
        self.membership.unsubscribe();
        self.accept.shutdown();
    }
}

impl Drop for ClusterHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// The thread that applies membership effects off beamr's delivery thread.
///
/// It owns no timer and samples no flag: it blocks on the tracker's condvar and
/// runs only when a delta is pushed or shutdown is signalled.
struct MembershipConsumer {
    membership: Membership,
    handle: Option<JoinHandle<()>>,
}

impl MembershipConsumer {
    /// Applies whatever the atomic initial view produced, on the CALLING thread,
    /// before any continuation is consumed. Returns the peers it admitted so
    /// bring-up can log them.
    fn prime(membership: &Membership, sync: &ClusterSync) {
        for delta in membership.take_pending() {
            apply_delta(membership, sync, delta);
        }
    }

    /// Spawns the consumer for the ordered continuation.
    fn start(membership: Membership, sync: ClusterSync) -> Self {
        let membership_for_thread = membership.clone();
        let handle = std::thread::Builder::new()
            .name("liminal-cluster-membership".to_owned())
            .spawn(move || {
                run_consumer(&membership_for_thread, &sync);
            })
            .ok();
        Self { membership, handle }
    }

    /// Wakes the consumer even with nothing pending, and joins it.
    fn stop(mut self) {
        self.membership.signal_shutdown();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn run_consumer(membership: &Membership, sync: &ClusterSync) {
    while let Some(batch) = membership.wait_for_effects() {
        membership.warn_once_on_depth(batch.len());
        for delta in batch {
            apply_delta(membership, sync, delta);
        }
    }
}

/// Logs and dispatches a single membership delta (R3/R4/R5).
///
/// The one funnel every membership fact passes through, whether it came from the
/// atomic initial view or from an ordered delta. Runs on the consumer, never on
/// beamr's delivery thread — `on_peer_join` writes to a socket, which
/// INV-SUB-DISCIPLINE forbids in a subscriber callback.
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
        // already happened via beamr's connection-down hook (purge_remote_node),
        // which INV-SCHED-FIRST guarantees ran before this subscriber saw the
        // event at all.
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
/// 3. Arm the membership event source and build the subscription sync, install
///    sync as the channel-supervisor's observer, apply the atomic initial view,
///    and start the consumer for the ordered continuation.
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

    // Arming the source captures the peers seed discovery just established as an
    // atomic initial view (synthetic Ups delivered under beamr's dispatch gate),
    // and queues their join effects for the consumer.
    let membership = Membership::new(connections.clone(), Arc::clone(&atoms));
    let sync = ClusterSync::new(pg, Arc::clone(&atoms), connections, local_node, resolver);
    install_observer(sync.clone());

    // Apply the initial view synchronously, before any continuation is consumed:
    // log the initial membership (R2) and backfill our state to each peer (R5).
    MembershipConsumer::prime(&membership, &sync);
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

    let consumer = MembershipConsumer::start(membership.clone(), sync);
    Ok(ClusterHandle {
        accept,
        consumer: Some(consumer),
        membership,
        _runtime: runtime,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::{Membership, MembershipDelta};
    use beamr::atom::AtomTable;
    use beamr::distribution::connection::{AcceptHandle, ConnectionManager};
    use beamr::distribution::connection_events::{ConnectionEvent, ConnectionGeneration};
    use beamr::distribution::resolver::StaticResolver;
    use std::collections::HashMap;
    use std::sync::Arc;

    const COOKIE: &str = "srv008-membership-cookie";
    const DIALER_NAME: &str = "dialer@127.0.0.1";
    const PEER_NAME: &str = "peer@127.0.0.1";

    fn empty_manager(atoms: &Arc<AtomTable>) -> ConnectionManager {
        ConnectionManager::new(
            Arc::clone(atoms),
            Arc::new(StaticResolver::new(HashMap::new())),
            "test-cookie",
            "local@127.0.0.1",
            1,
        )
    }

    /// A live loopback pair of REAL connection managers — real sockets, real OTP
    /// handshake, no mock. `dialer` is the manager whose membership the tests arm;
    /// the peer just listens.
    ///
    /// This is the deterministic barrier SRV-008 needs. beamr's INV-SYNC says that
    /// when the call causing a transition returns, every event it produced has
    /// already been delivered to every subscriber — so a `connect` that returns Ok
    /// is, on the DIALER's side, a completed delivery of the session's `Up`. No
    /// sleep and no retry loop are needed to observe a membership change.
    struct LivePair {
        runtime: tokio::runtime::Runtime,
        dialer: ConnectionManager,
        dialer_atoms: Arc<AtomTable>,
        /// Kept alive: dropping the peer manager or its accept handle would tear
        /// the link down underneath the assertions.
        _peer: ConnectionManager,
        _accept: AcceptHandle,
    }

    impl LivePair {
        fn new() -> Self {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("build loopback test runtime");

            let peer_atoms = Arc::new(AtomTable::with_common_atoms());
            let peer = ConnectionManager::new(
                peer_atoms,
                Arc::new(StaticResolver::new(HashMap::new())),
                COOKIE,
                PEER_NAME,
                1,
            );
            peer.set_runtime_handle(runtime.handle().clone());
            let accept = runtime
                .block_on(peer.listen("127.0.0.1:0".parse().expect("loopback addr")))
                .expect("peer binds a loopback listener");

            let mut routes = HashMap::new();
            routes.insert(PEER_NAME.to_owned(), accept.local_addr());
            let dialer_atoms = Arc::new(AtomTable::with_common_atoms());
            let dialer = ConnectionManager::new(
                Arc::clone(&dialer_atoms),
                Arc::new(StaticResolver::new(routes)),
                COOKIE,
                DIALER_NAME,
                1,
            );
            dialer.set_runtime_handle(runtime.handle().clone());

            Self {
                runtime,
                dialer,
                dialer_atoms,
                _peer: peer,
                _accept: accept,
            }
        }

        /// Dials the peer and returns once the session is installed in the
        /// dialer's table AND its connection event has been delivered (INV-SYNC).
        fn connect(&self) {
            self.runtime
                .block_on(self.dialer.connect(PEER_NAME))
                .expect("loopback handshake succeeds");
        }

        /// Closes the dialer's link. INV-SYNC again: `disconnect_node` returns
        /// only after the `Down` has reached every subscriber.
        fn disconnect(&self) {
            let node = self.dialer_atoms.intern(PEER_NAME);
            assert!(
                self.dialer.disconnect_node(node),
                "the live loopback link must be closable"
            );
        }
    }

    #[test]
    fn delta_is_empty_by_default() {
        assert!(MembershipDelta::default().is_empty());
    }

    /// SRV-008 R5 tombstone replacement. `first_poll_of_empty_table_yields_no_peers`
    /// asserted the retired sampler's contract directly; the empty-initial-view
    /// coverage it carried is preserved here through the event source instead.
    /// Arming over an empty table must yield an empty view, no queued effects,
    /// and exactly one initial-view acquisition — never a repeated one.
    #[test]
    fn empty_initial_view_yields_no_peers() {
        let atoms = Arc::new(AtomTable::with_common_atoms());
        let membership = Membership::new(empty_manager(&atoms), Arc::clone(&atoms));

        assert!(membership.peers().is_empty());
        assert_eq!(
            membership.events_observed(),
            0,
            "an empty table synthesizes no catch-up event"
        );
        assert_eq!(membership.pending_effects(), 0);
        assert_eq!(
            membership.source_snapshots(),
            1,
            "arming asks for the initial view exactly once"
        );
    }

    /// The atomic initial view is what makes a late arm safe: a peer that was
    /// already live before the tracker existed is delivered as a synthetic `Up`
    /// under beamr's dispatch gate, so the tracker never has to go looking.
    #[test]
    fn a_late_arm_sees_a_live_peer_in_its_initial_view() {
        let pair = LivePair::new();
        pair.connect();

        let membership = Membership::new(pair.dialer.clone(), Arc::clone(&pair.dialer_atoms));

        assert_eq!(
            membership.peers().len(),
            1,
            "the initial view must contain the peer that was already live"
        );
        assert_eq!(membership.events_observed(), 1);
        assert_eq!(
            membership.pending_effects(),
            1,
            "the initial view's join effect is queued for the consumer, not run \
             on beamr's delivery thread"
        );
    }

    /// INV-ALTERNATION end to end on real sockets: join, leave, and rejoin are
    /// observed in that order, each released by a real transition rather than by
    /// a cadence. The tracked set is correct at every step with no waiting.
    #[test]
    fn join_leave_and_rejoin_are_observed_in_order() {
        let pair = LivePair::new();
        let membership = Membership::new(pair.dialer.clone(), Arc::clone(&pair.dialer_atoms));
        let peer = pair.dialer_atoms.intern(PEER_NAME);

        pair.connect();
        assert_eq!(membership.peers(), vec![peer], "join is visible at once");

        pair.disconnect();
        assert!(
            membership.peers().is_empty(),
            "leave is visible at once, with no sampling in between"
        );

        pair.connect();
        assert_eq!(membership.peers(), vec![peer], "rejoin is visible at once");

        let effects = membership.take_pending();
        assert_eq!(
            effects,
            vec![
                MembershipDelta {
                    joined: vec![peer],
                    left: Vec::new(),
                },
                MembershipDelta {
                    joined: Vec::new(),
                    left: vec![peer],
                },
                MembershipDelta {
                    joined: vec![peer],
                    left: Vec::new(),
                },
            ],
            "the consumer receives the transitions in the order they happened"
        );
        assert_eq!(
            membership.events_observed(),
            3,
            "exactly one event per transition — no duplicates to dedupe"
        );
    }

    /// The handoff FIFO is lossless: every queued effect survives to the consumer
    /// and the depth is reported, which is the whole of the documented overflow
    /// rule (nothing is ever discarded, so there is no drop path to test).
    #[test]
    fn queued_effects_are_lossless_and_report_their_depth() {
        let atoms = Arc::new(AtomTable::with_common_atoms());
        let membership = Membership::new(empty_manager(&atoms), Arc::clone(&atoms));

        for index in 0_u64..64 {
            let peer = atoms.intern(&format!("peer-{index}@127.0.0.1"));
            membership.inner.observe(ConnectionEvent::up(
                peer,
                ConnectionGeneration::from_raw(index + 1),
                1,
            ));
        }

        assert_eq!(
            membership.pending_effects(),
            64,
            "no membership effect may be discarded"
        );
        assert_eq!(membership.queue_high_water(), 64);
        assert_eq!(membership.peers().len(), 64);

        let drained = membership.take_pending();
        assert_eq!(
            drained.len(),
            64,
            "every queued effect reaches the consumer"
        );
        assert_eq!(membership.pending_effects(), 0);
        assert_eq!(
            membership.queue_high_water(),
            64,
            "the high-water mark is a disclosure, not a counter that resets"
        );
    }

    /// A redundant event cannot double-count: the set is already right, so there
    /// is no effect to run and nothing is queued. INV-ALTERNATION rules these out
    /// upstream; this pins that liminal absorbs rather than amplifies one.
    #[test]
    fn a_redundant_event_queues_no_effect() {
        let atoms = Arc::new(AtomTable::with_common_atoms());
        let membership = Membership::new(empty_manager(&atoms), Arc::clone(&atoms));
        let peer = atoms.intern(PEER_NAME);

        membership.inner.observe(ConnectionEvent::up(
            peer,
            ConnectionGeneration::from_raw(1),
            1,
        ));
        membership.inner.observe(ConnectionEvent::up(
            peer,
            ConnectionGeneration::from_raw(2),
            1,
        ));

        assert_eq!(membership.peers(), vec![peer]);
        assert_eq!(
            membership.pending_effects(),
            1,
            "a repeated Up for a tracked peer must not queue a second join effect"
        );
    }

    /// R3: explicit shutdown wakes and releases the consumer even when no
    /// membership event is pending. Nothing polls a stop flag — the waiter is
    /// told.
    #[test]
    fn shutdown_wakes_a_consumer_with_nothing_pending() {
        let atoms = Arc::new(AtomTable::with_common_atoms());
        let membership = Membership::new(empty_manager(&atoms), Arc::clone(&atoms));
        assert_eq!(membership.pending_effects(), 0, "nothing is pending");

        let waiter = membership.clone();
        let joined = std::thread::spawn(move || waiter.wait_for_effects());
        membership.signal_shutdown();

        assert!(
            joined.join().expect("the waiter thread joins").is_none(),
            "shutdown must release a consumer that has no work"
        );
    }

    /// SRV-008 deletion check — absence proof over this module's own production
    /// source, mirroring the accept-path guard in `listener.rs`. The retired
    /// polling family must never come back.
    #[test]
    fn membership_source_has_no_retired_poll_family() {
        const SOURCE: &str = include_str!("membership.rs");
        let production = SOURCE.split("mod tests").next().unwrap_or(SOURCE);
        for forbidden in [
            "POLL_INTERVAL",
            "poll_once",
            "thread::sleep",
            "PollLoop",
            "run_poll_loop",
            "connected_nodes",
        ] {
            assert!(
                !production.contains(forbidden),
                "retired membership poll-family source `{forbidden}` reappeared"
            );
        }
    }

    /// RED PIN (SRV-008 R3) — the polling-cadence defect, pinned.
    ///
    /// A membership tracker is armed over a real connection manager BEFORE any
    /// peer link exists. Then a peer link is genuinely established: `connect`
    /// returns only after beamr has installed the session and delivered its
    /// connection event to every subscriber (INV-SYNC). An event-driven tracker
    /// has therefore already been TOLD, and reports the peer with no sleep, no
    /// retry, and no call into a sampling entry point.
    ///
    /// The polling tracker cannot: its set only moves when someone samples the
    /// connection table, so on the current implementation this reports zero peers
    /// and the membership change is observable only at the next 250ms tick. That
    /// gap IS the defect SRV-008 retires.
    #[test]
    fn armed_membership_observes_a_join_without_sampling() {
        let pair = LivePair::new();
        let membership = Membership::new(pair.dialer.clone(), Arc::clone(&pair.dialer_atoms));

        pair.connect();

        assert_eq!(
            membership.peers().len(),
            1,
            "an armed membership source must observe the join the instant beamr \
             installs it, without any sampling of the connection table"
        );
    }

    #[test]
    fn peer_names_resolve_through_the_atom_table() {
        let atoms = Arc::new(AtomTable::with_common_atoms());
        let membership = Membership::new(empty_manager(&atoms), Arc::clone(&atoms));
        // No connections, so no names — but the accessor must not panic.
        assert!(membership.peer_names().is_empty());
    }
}
