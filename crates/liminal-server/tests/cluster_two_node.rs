#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
//! SRV-005 acceptance: two real liminal nodes over loopback, clustered through
//! beamr distribution (NOT mocked). Each node runs its own distribution-enabled
//! channel supervisor and a real cluster started via [`cluster::start`], with the
//! two nodes cross-seeded so they connect to each other.
//!
//! These tests exercise the real cross-node path end to end:
//! * a subscription on node A becomes a remote pg member visible on node B;
//! * a publish on node B reaches node A's subscriber inbox;
//! * a node that joins late is backfilled with the existing subscriptions (R5);
//! * dropping a node purges its remote members and survivors still deliver (R6).
//!
//! SRV-008 changed how membership itself is learned, and these tests are written
//! to that mechanism. Membership is no longer sampled on a cadence: a node's
//! tracker is armed from beamr's atomic initial view and continues on the
//! ordered connection-event stream, delivered synchronously with the transition
//! (INV-SYNC). So a node that DIALS its seeds knows them the instant
//! `cluster::start` returns and is asserted with no waiting at all; only the
//! ACCEPTING side is genuinely concurrent with the test thread, and the wait
//! there is on the socket handshake, never on a poll interval.

use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::{Duration, Instant};

use liminal::channel::{ChannelConfig, ChannelHandle, ChannelMode, ChannelSupervisor, Schema};
use liminal_server::cluster::{self, ClusterHandle};
use liminal_server::config::types::ClusterConfig;
use liminal_server::health::{
    ClusterReadiness, ReadinessState, SharedReadinessState, readiness_check,
};

const COOKIE: &str = "srv005-loopback-cookie";

/// Waits for a genuinely asynchronous CROSS-NODE effect — an inbound handshake
/// completing on another node's runtime, a pg update crossing the wire, a
/// message reaching an inbox — until it holds or the deadline elapses.
///
/// Since SRV-008 this is no longer used to wait out a membership poll interval.
/// Nothing on the membership path has a cadence to wait for: once a transition
/// happens, beamr delivers it to the tracker synchronously. What remains
/// genuinely concurrent is the network and the other node's runtime, which is
/// what this bounds.
fn eventually(timeout: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if condition() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Grabs a free loopback port by binding and immediately dropping a listener.
fn free_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read ephemeral addr");
    drop(listener);
    addr
}

/// A running cluster node: its clustered channel supervisor and live handle.
struct Node {
    supervisor: ChannelSupervisor,
    handle: ClusterHandle,
    listen_addr: SocketAddr,
    /// The same readiness handle a real server's health endpoint reads (G2): the
    /// cluster start flips its membership-established gate through the real
    /// `on_established` hook, so `/ready` payloads mirror the node's cluster stack.
    readiness: SharedReadinessState,
}

impl Node {
    /// Starts a node named `node_name`, listening on `listen_addr`, seeded with
    /// `seeds`. The cluster `sync` is installed as the supervisor's observer so
    /// channel subscribe/publish drive the distributed process group.
    fn start(node_name: &str, listen_addr: SocketAddr, seeds: Vec<SocketAddr>) -> Self {
        let resolver = Arc::new(cluster::discovery::ClusterResolver::new());
        let supervisor = ChannelSupervisor::with_distribution(
            node_name.to_owned(),
            1,
            COOKIE.to_owned(),
            cluster::discovery::as_resolver(Arc::clone(&resolver)),
            liminal::channel::ChannelRestartPolicy::default(),
        )
        .expect("clustered supervisor starts");

        let config = ClusterConfig {
            node_name: node_name.to_owned(),
            listen_address: listen_addr,
            seed_nodes: seeds,
            cookie: COOKIE.to_owned(),
        };
        // Start with the cluster gate unmet, exactly as a fresh server does before
        // its cluster stack comes up; the `on_established` hook flips it on success.
        let readiness = SharedReadinessState::new(ReadinessState::new(
            true,
            true,
            ClusterReadiness::Configured {
                membership_established: false,
            },
        ));
        let scheduler = supervisor.scheduler();
        let supervisor_for_observer = supervisor.clone();
        let readiness_for_hook = readiness.clone();
        let handle = cluster::start(
            &scheduler,
            resolver,
            &config,
            move |sync| {
                supervisor_for_observer.install_observer(Arc::new(sync));
            },
            move || readiness_for_hook.set_cluster_membership_established(true),
        )
        .expect("cluster starts");

        Self {
            supervisor,
            handle,
            listen_addr,
            readiness,
        }
    }

    /// True once this node's `/ready` gate reports the cluster stack established.
    fn readiness_ready(&self) -> bool {
        readiness_check(&self.readiness.snapshot()).ready
    }

    /// A channel handle on this node's clustered supervisor (empty schema, so any
    /// payload is accepted — matching the server's channel construction).
    fn channel(&self, name: &str) -> ChannelHandle {
        let schema = Schema::new(serde_json::json!({})).expect("empty schema");
        ChannelHandle::with_supervisor(
            ChannelConfig::new(name.to_owned(), schema, ChannelMode::Ephemeral),
            self.supervisor.clone(),
        )
    }

    fn peer_count(&self) -> usize {
        self.handle.membership().peers().len()
    }

    /// The node's membership tracker, for the SRV-008 idle-cost counters.
    const fn membership(&self) -> &liminal_server::cluster::Membership {
        self.handle.membership()
    }

    /// Shuts down this node's scheduler, closing its distribution connections so
    /// peers observe it going down (R4/R6). Used by the node-departure test to
    /// simulate the node process exiting.
    fn shutdown(&self) {
        self.supervisor.shutdown();
    }
}

/// Asserts, with NO waiting, that a node that dialed seeds already knows them.
///
/// This is the SRV-008 event barrier at its sharpest. `cluster::start` completes
/// the seed dial and only then arms the membership source, and arming delivers a
/// synthetic `Up` for every peer already live under beamr's dispatch gate. So by
/// the time `Node::start` returns, the dialer's view is complete. Under the
/// retired sampler this assertion could not have been made: the view stayed
/// empty until a tick sampled the table.
fn assert_dialer_knows_its_seeds(node: &Node, expected: usize) {
    assert_eq!(
        node.peer_count(),
        expected,
        "a node that dialed its seeds must know them the instant cluster start \
         returns — the initial view is atomic, not sampled"
    );
}

/// Waits for the ACCEPTING side of a link, the only genuinely concurrent half:
/// the inbound handshake completes on the accepting node's own runtime. Once it
/// does, beamr delivers the event to that node's tracker synchronously — so this
/// bounds the socket, not a cadence.
fn await_inbound_membership(node: &Node, expected: usize) {
    assert!(
        eventually(Duration::from_secs(10), || node.peer_count() >= expected),
        "the accepting node should observe the inbound link as a peer"
    );
}

/// Both sides of a freshly cross-seeded pair: `b` dialed `a`, so `b` is exact and
/// immediate and only `a` has to wait for its accept.
fn await_mutual_membership(a: &Node, b: &Node) {
    assert_dialer_knows_its_seeds(b, 1);
    await_inbound_membership(a, 1);
}

#[test]
fn cluster_start_flips_node_readiness_to_established() {
    // Zero-seed single-node bootstrap: no peer is required for this node's cluster
    // stack to be "established" (G2 semantics — per-node liveness, not quorum). A
    // successful start must flip /ready from 503 to 200.
    let addr_a = free_port();
    let node_a = Node::start("node-a@127.0.0.1", addr_a, vec![]);
    assert!(
        node_a.readiness_ready(),
        "a successful zero-seed cluster start must mark the node ready"
    );

    // And with two cross-seeded nodes, each independently reaches ready.
    let addr_b = free_port();
    let node_b = Node::start("node-b@127.0.0.1", addr_b, vec![addr_a]);
    await_mutual_membership(&node_a, &node_b);
    assert!(node_a.readiness_ready(), "node A stays ready");
    assert!(
        node_b.readiness_ready(),
        "node B reaches ready after its own successful start"
    );
}

#[test]
fn subscription_on_a_is_visible_as_remote_member_on_b() {
    let addr_a = free_port();
    let addr_b = free_port();
    let node_a = Node::start("node-a@127.0.0.1", addr_a, vec![]);
    let node_b = Node::start("node-b@127.0.0.1", addr_b, vec![addr_a]);
    await_mutual_membership(&node_a, &node_b);

    // Subscribe on A; B must observe A's subscriber as a remote pg member of the
    // "orders" group (the channel name), proving pg.join propagated cross-node.
    let channel_a = node_a.channel("orders");
    let _subscription = channel_a.subscribe().expect("subscribe on A");

    let channel_b = node_b.channel("orders");
    assert!(
        eventually(Duration::from_secs(10), || {
            !node_b_remote_members(&node_b, "orders").is_empty()
        }),
        "node B should see A's subscription as a remote pg member"
    );
    // The remote member's node is A.
    let members = node_b_remote_members(&node_b, "orders");
    assert_eq!(members.len(), 1, "exactly one remote subscriber");
    drop(channel_b);
}

#[test]
fn publish_on_b_reaches_subscriber_on_a() {
    let addr_a = free_port();
    let addr_b = free_port();
    let node_a = Node::start("node-a@127.0.0.1", addr_a, vec![]);
    let node_b = Node::start("node-b@127.0.0.1", addr_b, vec![addr_a]);
    await_mutual_membership(&node_a, &node_b);

    let channel_a = node_a.channel("orders");
    let subscription = channel_a.subscribe().expect("subscribe on A");

    // Wait until B has learned A's subscription before publishing.
    assert!(
        eventually(Duration::from_secs(10), || {
            !node_b_remote_members(&node_b, "orders").is_empty()
        }),
        "B should learn A's subscription before publishing"
    );

    let channel_b = node_b.channel("orders");
    let payload = br#"{"order":"cross-node-1"}"#.to_vec();
    channel_b.publish(&payload).expect("publish on B");

    let received = eventually(Duration::from_secs(10), || {
        matches!(subscription.try_next(), Ok(Some(_)))
            || a_inbox_has_payload(&subscription, &payload)
    });
    assert!(received, "A's subscriber should receive B's publish");
}

#[test]
fn late_joiner_is_backfilled_with_existing_subscriptions() {
    let addr_a = free_port();
    let addr_c = free_port();
    // Node A starts alone and gets a subscriber BEFORE C exists.
    let node_a = Node::start("node-a@127.0.0.1", addr_a, vec![]);
    let channel_a = node_a.channel("events");
    let _subscription = channel_a.subscribe().expect("subscribe on A");

    // Now C joins, seeded at A. A is TOLD that C joined — beamr delivers the
    // `Up` for the accepted link to A's membership subscriber, which hands it to
    // A's consumer, which backfills A's pre-existing "events" subscription to C
    // (R5). The join barrier is the event, not a tick: C's own view is exact and
    // immediate, and A's is asserted from the moment its accept completes.
    let node_c = Node::start("node-c@127.0.0.1", addr_c, vec![addr_a]);
    assert_dialer_knows_its_seeds(&node_c, 1);
    await_inbound_membership(&node_a, 1);

    assert!(
        eventually(Duration::from_secs(10), || {
            !node_b_remote_members(&node_c, "events").is_empty()
        }),
        "late joiner C should be backfilled with A's existing subscription"
    );
}

#[test]
fn dropping_a_node_purges_its_remote_members_and_survivors_still_deliver() {
    let addr_a = free_port();
    let addr_b = free_port();
    let addr_c = free_port();
    let node_a = Node::start("node-a@127.0.0.1", addr_a, vec![]);
    let node_b = Node::start("node-b@127.0.0.1", addr_b, vec![addr_a]);
    // Each dialer's initial view is asserted exactly, at the moment it is armed —
    // B before C exists, C once it has dialed both seeds. The accepting sides
    // then catch up over their sockets.
    await_mutual_membership(&node_a, &node_b);

    let node_c = Node::start("node-c@127.0.0.1", addr_c, vec![addr_a, addr_b]);
    assert_dialer_knows_its_seeds(&node_c, 2);
    await_inbound_membership(&node_a, 2);
    await_inbound_membership(&node_b, 2);

    // A and C both subscribe to "orders". B sees two remote members.
    let channel_a = node_a.channel("orders");
    let sub_a = channel_a.subscribe().expect("subscribe on A");
    let channel_c = node_c.channel("orders");
    let sub_c = channel_c.subscribe().expect("subscribe on C");

    assert!(
        eventually(Duration::from_secs(10), || {
            node_b_remote_members(&node_b, "orders").len() >= 2
        }),
        "B should see remote members from both A and C"
    );

    // Drop A entirely — simulating the node going away. Every handle that holds
    // A's scheduler alive must be dropped too (a subscription handle owns an
    // `Arc<Scheduler>`), so A's connections actually close and B's read loop sees
    // EOF. beamr's connection-down hook then purges A's remote members from every
    // group (R6) with no liminal code on the path; C remains a member.
    drop(sub_a);
    drop(channel_a);
    node_a.shutdown();
    drop(node_a);

    assert!(
        eventually(Duration::from_secs(15), || {
            node_b_remote_members(&node_b, "orders").len() == 1
        }),
        "B should purge A's remote member after A drops, leaving only C"
    );

    // Surviving subscriber on C still receives a publish from B.
    let channel_b = node_b.channel("orders");
    let payload = br#"{"order":"after-a-dropped"}"#.to_vec();
    channel_b
        .publish(&payload)
        .expect("publish on B after A dropped");

    let received = eventually(Duration::from_secs(10), || {
        a_inbox_has_payload(&sub_c, &payload)
    });
    assert!(
        received,
        "C's surviving subscriber should still receive B's publish after A dropped"
    );
}

/// SRV-008 R6 oracle — the idle-cost bound on the membership source, proven
/// against a LIVE cluster rather than a sleeping one.
///
/// Two real nodes are connected and their membership is then held stable. Across
/// the soak the membership consumer must take ZERO further wakes and ask for
/// ZERO further initial views, and be handed ZERO further events: with nothing
/// happening to membership, the membership unit costs nothing.
///
/// Per the estate's idle-cost fixture law that claim is only worth something if
/// the process is demonstrably alive, so the same soak drives real cross-node
/// traffic and asserts that UNRELATED counters GROW while the unit's counters
/// stay flat. Publishes are issued and cross-node deliveries are counted — real
/// sockets, real read loops, real heartbeats, real pg propagation, all working —
/// and the membership counters do not move. The retired sampler could not have
/// passed this: it woke on a 250ms cadence, so the soak below would have ticked
/// it a dozen times over with no membership traffic at all.
#[test]
fn stable_membership_source_has_zero_consumer_wakes() {
    let addr_a = free_port();
    let addr_b = free_port();
    let node_a = Node::start("node-a@127.0.0.1", addr_a, vec![]);
    let node_b = Node::start("node-b@127.0.0.1", addr_b, vec![addr_a]);
    await_mutual_membership(&node_a, &node_b);

    // A subscriber on A and a publisher on B, so the soak has real work to do.
    let channel_a = node_a.channel("soak");
    let subscription = channel_a.subscribe().expect("subscribe on A");
    assert!(
        eventually(Duration::from_secs(10), || {
            !node_b_remote_members(&node_b, "soak").is_empty()
        }),
        "B should learn A's subscription before the soak begins"
    );

    // Baseline only once bring-up has fully settled, so the flat window is a
    // genuinely quiescent one rather than a race with the consumer.
    let membership = node_a.membership();
    assert!(
        eventually(Duration::from_secs(10), || membership.pending_effects()
            == 0),
        "the membership consumer should have applied bring-up before the soak"
    );
    let wakes_at_rest = membership.consumer_wakes();
    let snapshots_at_rest = membership.source_snapshots();
    let events_at_rest = membership.events_observed();

    // The soak: three seconds of real cross-node traffic — twelve intervals of
    // the retired 250ms cadence.
    let channel_b = node_b.channel("soak");
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut published = 0_u64;
    let mut delivered = 0_u64;
    while Instant::now() < deadline {
        let payload = format!(r#"{{"soak":{published}}}"#).into_bytes();
        channel_b
            .publish(&payload)
            .expect("publish on B during the soak");
        published += 1;
        while let Ok(Some(_)) = subscription.try_next() {
            delivered += 1;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    while let Ok(Some(_)) = subscription.try_next() {
        delivered += 1;
    }

    // The unrelated counters GREW: this was a live process under load.
    assert!(
        published >= 50,
        "the soak must actually drive the cluster (published {published})"
    );
    assert!(
        delivered > 0,
        "cross-node delivery must GROW across the soak, or the flat membership \
         counters below prove only that the test slept (published {published}, \
         delivered {delivered})"
    );

    // The membership unit's counters stayed FLAT.
    assert_eq!(
        membership.consumer_wakes(),
        wakes_at_rest,
        "a stable membership source must wake its consumer zero times"
    );
    assert_eq!(
        membership.source_snapshots(),
        snapshots_at_rest,
        "the initial view is acquired once at arm and never again — no cadence"
    );
    assert_eq!(
        membership.events_observed(),
        events_at_rest,
        "no membership traffic means no membership events"
    );
    assert_eq!(
        membership.pending_effects(),
        0,
        "nothing queued, because nothing happened"
    );
    assert_eq!(
        node_a.peer_count(),
        1,
        "and the view is still correct at the end of the soak"
    );
}

/// Remote pg members of `channel` as seen by `node`, read directly from the
/// node's scheduler pg registry (the same registry beamr's purge writes).
fn node_b_remote_members(node: &Node, channel: &str) -> Vec<beamr::distribution::pg::RemoteMember> {
    let scheduler = node.supervisor.scheduler();
    let atoms = scheduler.atom_table();
    let pg = scheduler.pg_registry();
    let group = atoms.intern(channel);
    pg.remote_members(pg.default_scope(), group)
}

/// Drains `subscription` looking for `payload`, returning true once seen. Used by
/// the delivery assertions; each call consumes at most the queued messages.
fn a_inbox_has_payload(
    subscription: &liminal::channel::SubscriptionHandle,
    payload: &[u8],
) -> bool {
    while let Ok(Some(envelope)) = subscription.try_next() {
        if envelope.payload == payload {
            return true;
        }
    }
    false
}

// Keep `listen_addr` reachable from the Node Debug surface for diagnostics.
impl std::fmt::Debug for Node {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Node")
            .field("listen_addr", &self.listen_addr)
            .finish_non_exhaustive()
    }
}
