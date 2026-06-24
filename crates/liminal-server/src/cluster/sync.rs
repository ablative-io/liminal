//! SRV-005 R5/R6: cross-node subscription propagation and message fan-out.
//!
//! Each channel maps to a beamr process group (the channel name interned into an
//! atom in the default pg scope). A local subscriber joining a channel joins
//! that pg group; beamr broadcasts the membership to connected peers, so every
//! node knows which peers hold subscribers for which channel. Publishing then
//! consults the group's REMOTE members and forwards the published envelope to
//! each one over the existing distribution link.
//!
//! * R5 propagation: [`ClusterSync::on_subscribe`] -> `pg.join`; the broadcast is
//!   beamr's, not ours.
//! * R5 backfill: a fresh `pg.join` only broadcasts on the insert edge, so a peer
//!   that joins the cluster AFTER our subscribers registered would never learn
//!   them. [`ClusterSync::on_peer_join`] re-sends a pg-join control frame for each
//!   of our local members directly to the newcomer.
//! * R5 delivery: [`ClusterSync::on_publish`] sends the envelope (encoded by
//!   [`liminal::channel::encode_envelope`]) as a beamr `SEND` to each remote
//!   member's pid. On that member's home node the frame lands in the subscriber
//!   process's mailbox, which decodes it back into its inbox.
//! * R6 cleanup: when a peer drops, beamr's connection-down hook calls
//!   `purge_remote_node`, so its remote members vanish from every group with no
//!   liminal code on the path. [`ClusterSync::on_peer_leave`] only logs.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use beamr::atom::{Atom, AtomTable};
use beamr::distribution::connection::ConnectionManager;
use beamr::distribution::control::{encode_pg_update_frame, encode_send_frame};
use beamr::distribution::pg::{PgRegistry, PgUpdate, RemoteMember};
use beamr::native::ProcessContext;
use beamr::term::Term;

use crate::cluster::discovery::ClusterResolver;
use liminal::channel::{ClusterObserver, encode_envelope};
use liminal::envelope::Envelope;

/// Propagates channel subscriptions and fans published messages out across the
/// cluster via beamr process groups (SRV-005 R5/R6).
#[derive(Clone)]
pub struct ClusterSync {
    inner: Arc<SyncInner>,
}

struct SyncInner {
    pg: Arc<PgRegistry>,
    atoms: Arc<AtomTable>,
    connections: ConnectionManager,
    /// This node's distribution atom — the node component every locally-joined
    /// member carries on the wire.
    local_node: Atom,
    /// The same resolver the scheduler uses, retained so a future enhancement can
    /// register learned peer addresses; held to keep the discovery/scheduler
    /// resolver identity explicit at this seam.
    _resolver: Arc<ClusterResolver>,
    /// Local subscriptions, keyed by channel group atom, each holding the set of
    /// local subscriber pids. The source of truth for R5 peer-join backfill.
    local: Mutex<HashMap<Atom, Vec<u64>>>,
}

impl std::fmt::Debug for ClusterSync {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClusterSync")
            .field("local_node", &self.inner.local_node)
            .finish_non_exhaustive()
    }
}

impl ClusterSync {
    /// Builds a sync over the scheduler's pg registry and distribution
    /// connections. `local_node` is this node's distribution atom (interned from
    /// the configured node name in the SAME atom table).
    #[must_use]
    pub fn new(
        pg: Arc<PgRegistry>,
        atoms: Arc<AtomTable>,
        connections: ConnectionManager,
        local_node: Atom,
        resolver: Arc<ClusterResolver>,
    ) -> Self {
        Self {
            inner: Arc::new(SyncInner {
                pg,
                atoms,
                connections,
                local_node,
                _resolver: resolver,
                local: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// The default pg scope atom.
    fn scope(&self) -> Atom {
        self.inner.pg.default_scope()
    }

    /// Interns a channel name into its pg group atom.
    fn group(&self, channel: &str) -> Atom {
        self.inner.atoms.intern(channel)
    }

    /// Remote members of `channel`'s group (for publish fan-out and tests).
    #[must_use]
    pub fn remote_targets(&self, channel: &str) -> Vec<RemoteMember> {
        let group = self.group(channel);
        self.inner.pg.remote_members(self.scope(), group)
    }

    /// Records a local subscriber pid under `group` for backfill.
    fn record_local(&self, group: Atom, pid: u64) {
        let mut local = self.lock_local();
        let pids = local.entry(group).or_default();
        if !pids.contains(&pid) {
            pids.push(pid);
        }
        drop(local);
    }

    /// Forgets a local subscriber pid under `group`.
    fn forget_local(&self, group: Atom, pid: u64) {
        let mut local = self.lock_local();
        if let Some(pids) = local.get_mut(&group) {
            pids.retain(|candidate| *candidate != pid);
            if pids.is_empty() {
                local.remove(&group);
            }
        }
    }

    /// A snapshot of every local `(group, pid)` membership for backfill.
    fn local_memberships(&self) -> Vec<(Atom, u64)> {
        let local = self.lock_local();
        local
            .iter()
            .flat_map(|(group, pids)| pids.iter().map(move |pid| (*group, *pid)))
            .collect()
    }

    /// Sends an encoded envelope to one remote member's pid as a beamr `SEND`.
    fn send_to_member(&self, member: RemoteMember, frame_bytes: &[u8]) {
        // The SEND control targets the member's pid_number; on the member's home
        // node that maps to the local subscriber process, whose mailbox receives
        // the binary payload.
        let Some(to_pid) = Term::try_pid(member.pid_number) else {
            tracing::warn!(
                pid_number = member.pid_number,
                "remote member pid out of immediate range; skipping cross-node delivery"
            );
            return;
        };
        let mut context = ProcessContext::new();
        let Ok(payload) = context.alloc_binary(frame_bytes) else {
            tracing::warn!("failed to allocate cross-node envelope payload");
            return;
        };
        let Ok(frame) = encode_send_frame(
            Term::atom(beamr::atom::Atom::OK),
            to_pid,
            payload,
            &self.inner.atoms,
        ) else {
            tracing::warn!("failed to encode cross-node send frame");
            return;
        };
        self.write_frame(member.node, &frame);
    }

    /// Writes a pre-encoded distribution frame to `node`'s connection, if live.
    fn write_frame(&self, node: Atom, frame: &[u8]) {
        let Some(connection) = self.inner.connections.get_connection(node) else {
            // No live link: the peer departed between snapshot and send, or its
            // membership is stale. Nothing to do — R6 cleanup removes it shortly.
            return;
        };
        write_raw_blocking(&connection, frame);
    }

    /// Re-sends a pg-join control frame for one local member to a single node
    /// (R5 backfill). Mirrors the frame beamr broadcasts on a fresh `pg.join`,
    /// but targeted at the newcomer only.
    fn backfill_member(&self, node: Atom, group: Atom, pid: u64) {
        let update = PgUpdate::Join {
            scope: self.scope(),
            group,
            pid,
        };
        if let Ok(frame) = encode_pg_update_frame(update, self.inner.local_node, &self.inner.atoms)
        {
            self.write_frame(node, &frame);
        } else {
            tracing::warn!("failed to encode cluster backfill frame");
        }
    }

    fn lock_local(&self) -> std::sync::MutexGuard<'_, HashMap<Atom, Vec<u64>>> {
        self.inner
            .local
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl ClusterObserver for ClusterSync {
    fn on_subscribe(&self, channel: &str, subscriber_pid: u64) {
        let group = self.group(channel);
        // pg.join broadcasts the membership to connected peers (R5 propagation).
        self.inner.pg.join(self.scope(), group, subscriber_pid);
        self.record_local(group, subscriber_pid);
        tracing::debug!(
            channel = %channel,
            pid = subscriber_pid,
            "advertised local subscription to cluster"
        );
    }

    fn on_unsubscribe(&self, channel: &str, subscriber_pid: u64) {
        let group = self.group(channel);
        self.inner.pg.leave(self.scope(), group, subscriber_pid);
        self.forget_local(group, subscriber_pid);
        tracing::debug!(
            channel = %channel,
            pid = subscriber_pid,
            "withdrew local subscription from cluster"
        );
    }

    fn on_publish(&self, channel: &str, envelope: &Envelope) {
        let targets = self.remote_targets(channel);
        if targets.is_empty() {
            return;
        }
        let frame_bytes = encode_envelope(envelope);
        for member in targets {
            self.send_to_member(member, &frame_bytes);
        }
    }
}

impl ClusterSync {
    /// R5 backfill: re-advertise every local subscription to a newly-joined peer.
    pub fn on_peer_join(&self, node: Atom) {
        for (group, pid) in self.local_memberships() {
            self.backfill_member(node, group, pid);
        }
    }

    /// R6 is automatic (beamr purges the departed node's remote members via its
    /// connection-down hook). This logs the cleanup for operators.
    pub fn on_peer_leave(&self, node: Atom) {
        let name = self
            .inner
            .atoms
            .resolve(node)
            .map_or_else(|| format!("<atom {node:?}>"), str::to_owned);
        tracing::info!(
            peer = %name,
            "peer departed; its remote subscriptions were purged by beamr"
        );
    }
}

/// Writes `frame` to `connection`, driving the async write to completion off any
/// ambient runtime. The membership poll thread that calls this owns no tokio
/// runtime, so a fresh current-thread runtime drives the single write — the same
/// shape as beamr's own synchronous distribution-send bridge.
fn write_raw_blocking(
    connection: &Arc<beamr::distribution::connection::DistConnection>,
    frame: &[u8],
) {
    let connection = Arc::clone(connection);
    let frame = frame.to_vec();
    let write = async move {
        let _ = connection.write_raw(&frame).await;
    };
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        if matches!(
            handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            tokio::task::block_in_place(|| handle.block_on(write));
            return;
        }
    }
    match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime.block_on(write),
        Err(error) => tracing::warn!(error = %error, "failed to build cluster send runtime"),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::ClusterSync;
    use crate::cluster::discovery::ClusterResolver;
    use beamr::atom::AtomTable;
    use beamr::distribution::connection::ConnectionManager;
    use beamr::distribution::pg::PgRegistry;
    use beamr::distribution::resolver::StaticResolver;
    use liminal::channel::ClusterObserver;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn sync_fixture() -> (ClusterSync, Arc<PgRegistry>, Arc<AtomTable>) {
        let atoms = Arc::new(AtomTable::with_common_atoms());
        let pg = Arc::new(PgRegistry::new(&atoms));
        let connections = ConnectionManager::new(
            Arc::clone(&atoms),
            Arc::new(StaticResolver::new(HashMap::new())),
            "test-cookie",
            "local@127.0.0.1",
            1,
        );
        let local_node = atoms.intern("local@127.0.0.1");
        let resolver = Arc::new(ClusterResolver::new());
        let sync = ClusterSync::new(
            Arc::clone(&pg),
            Arc::clone(&atoms),
            connections,
            local_node,
            resolver,
        );
        (sync, pg, atoms)
    }

    #[test]
    fn subscribe_joins_the_channel_pg_group() {
        let (sync, pg, atoms) = sync_fixture();
        sync.on_subscribe("orders", 42);
        let group = atoms.intern("orders");
        assert_eq!(pg.local_members(pg.default_scope(), group), vec![42]);
    }

    #[test]
    fn unsubscribe_leaves_the_channel_pg_group() {
        let (sync, pg, atoms) = sync_fixture();
        sync.on_subscribe("orders", 42);
        sync.on_unsubscribe("orders", 42);
        let group = atoms.intern("orders");
        assert!(pg.local_members(pg.default_scope(), group).is_empty());
    }

    #[test]
    fn local_memberships_track_subscriptions_for_backfill() {
        let (sync, _pg, _atoms) = sync_fixture();
        sync.on_subscribe("orders", 1);
        sync.on_subscribe("orders", 2);
        sync.on_subscribe("events", 3);
        let mut memberships = sync.local_memberships();
        memberships.sort_by_key(|(group, pid)| (*group, *pid));
        assert_eq!(memberships.len(), 3);
        // After unsubscribing the only member of a group, the group is dropped.
        sync.on_unsubscribe("events", 3);
        let remaining = sync.local_memberships();
        assert_eq!(remaining.len(), 2);
        assert!(remaining.iter().all(|(_, pid)| *pid == 1 || *pid == 2));
    }

    #[test]
    fn remote_targets_empty_without_remote_members() {
        let (sync, _pg, _atoms) = sync_fixture();
        sync.on_subscribe("orders", 1);
        assert!(sync.remote_targets("orders").is_empty());
    }

    #[test]
    fn remote_targets_reflect_applied_remote_joins() {
        let (sync, pg, atoms) = sync_fixture();
        let group = atoms.intern("orders");
        let remote_node = atoms.intern("node-b@127.0.0.1");
        pg.apply_remote_join(pg.default_scope(), group, remote_node, 99, 0);
        let targets = sync.remote_targets("orders");
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].node, remote_node);
        assert_eq!(targets[0].pid_number, 99);
    }
}
