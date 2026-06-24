# SRV-005 clustering — FINAL implementation plan (beamr 0.8.3, no substrate gaps)

Build cluster/{discovery,membership,sync,mod}.rs in crates/liminal-server/src/. Distribution layer is COMPLETE.
Worktree has a dev [patch.crates-io] beamr→local 0.8.3 (uncommitted — do NOT commit it). liminal-server already
builds against local beamr 0.8.3 (verified).

## TWO DOMINANT CONSTRAINTS (honor exactly)
- **A — connection-down hook is SINGLE-SLOT, already taken by beamr's pg-purge** (scheduler/mod.rs:763-769; ConnectionDownHook::register
  is register-or-REPLACE, connection.rs:94-104). **liminal-server MUST NOT call register_connection_down** — it would overwrite
  pg-purge and BREAK R6. → membership derives node-down by POLLING connected_nodes() + diffing.
- **B — pg transport + channel subscribers must be on the SAME scheduler.** Subscriber pids live on the channel-supervisor
  scheduler (subscription.rs SubscriptionHandle::spawn(core.scheduler(),..)). pg.join must run on the scheduler owning that pid.
  → the CLUSTERED scheduler must BE the channel-supervisor scheduler. Cluster attaches to ChannelSupervisor::scheduler()
  (supervisor.rs:133). connection-supervisor + conversation-supervisor schedulers stay non-clustered.

## Verified beamr 0.8.3 symbols
- SchedulerConfig{ node_name, creation, distribution: Some(DistributionConfig{ resolver, cookie }) } (scheduler/mod.rs:88-107; cookie default DEFAULT_COOKIE)
- Scheduler::start_distribution_listener(&self, addr)->io::Result<AcceptHandle> (mod.rs:990; keep AcceptHandle alive — drop aborts accept)
- ConnectionManager::connect(&self, node_name:&str)->Result<Arc<DistConnection>,ConnectError> (connection.rs:518; resolves name→addr, handshakes, keys table by result.remote_name())
- Scheduler::distribution_connections()->ConnectionManager (mod.rs:980); ConnectionManager::connected_nodes()->Vec<Atom> (connection.rs:443)
- Scheduler::pg_registry()->Arc<PgRegistry> (mod.rs:997); PgRegistry::join/leave(scope,group,pid) (pg.rs:154,175, auto-broadcasts);
  remote_members(scope,group)->Vec<RemoteMember{node,pid_number,serial}> (pg.rs:201,23); local_members (pg.rs:191);
  purge_remote_node(node) (pg.rs:287, ALREADY wired to the down-hook = R6 free)
- ProcessContext::pg_facility() (context/mod.rs:1081)

## 1. Integration seam (cluster attaches to channel-supervisor scheduler)
- channel/supervisor.rs:111 — add ChannelSupervisor::with_distribution(node_name,creation,cookie,resolver,policy): builds Scheduler::new
  with SchedulerConfig{ node_name:Some, creation:Some, distribution:Some(DistributionConfig{resolver,cookie}), ..default }. Keep new() for non-clustered.
- PREREQUISITE REFACTOR: services.rs:155 from_config_with_store today creates a SEPARATE supervisor per channel (services.rs:171-182
  via ChannelHandle::new). Change to ONE shared ChannelSupervisor (recommend a ChannelRegistry, registry.rs:46 with_supervisor) so all
  channels share one scheduler = the cluster transport. When config.cluster.is_some() build it via with_distribution(...), else new().
  Expose scheduler()->Arc<Scheduler> + a subscription-enumeration seam.
- server/runtime.rs:28 — after services built, before wait() (line 47), gate on config.cluster.is_some() → cluster::start(...) passing
  channel-supervisor Arc<Scheduler>, scheduler.pg_registry(), scheduler.distribution_connections(), the channel enum handle, &ClusterConfig.
  Keep returned ClusterHandle (owns AcceptHandle + membership poll task) alive; drop in shutdown (runtime.rs:50-54).
- cluster::start: (1) start_distribution_listener(listen_addr).await? [bind first]; (2) discovery::connect_seeds(&conns,&cluster).await;
  (3) Membership::start(conns, local_atom); (4) sync::install(pg, channel_enum, membership); (5) return ClusterHandle.
  async-from-sync: use block_on on a temp current-thread runtime (mirror block_on_distribution_send in supervision_integration.rs).

## 2. R1 discovery.rs (node_name↔addr scheme RESOLVED via handshake)
- ClusterConfig.seed_nodes: Vec<SocketAddr> (no names). Build a synthetic dialing label per seed (e.g. "seed-{i}@{addr}"), register in a
  custom NodeResolver (impl beamr::distribution::resolver::NodeResolver, resolver.rs:46) mapping label→addr, connect(label). After handshake
  the table re-keys to the seed's REAL name (result.remote_name()) — synthetic label is throwaway, never the key, no collision.
- local node name = ClusterConfig.node_name (→ SchedulerConfig.node_name). cookie = ClusterConfig.cookie (ADD field, default DEFAULT_COOKIE).
- ADD ClusterConfig.listen_address: SocketAddr (distribution port, distinct from ServerConfig client port) — nowhere to bind without it.
- The resolver built here MUST be the SAME instance passed to SchedulerConfig.distribution.resolver (build in seam step 0, share to with_distribution + discovery).
- fns: seed_resolver(seeds)->(Arc<dyn NodeResolver+Send+Sync>, Vec<String> labels); async connect_seeds(conns,labels) — per label connect, Ok→info, Err→warn+continue (R1 non-fatal).

## 3. R2/R3/R4 membership.rs (POLL connected_nodes, COMPOSES with pg-purge — do NOT register a hook)
- struct Membership{inner:Arc<MembershipInner{connections,peers:Mutex<BTreeSet<Atom>>,local:Atom}>} Clone-cheap.
  start(conns,local)->Self spawns poll thread; peers()->Vec<Atom>; poll_once()->MembershipDelta{joined,left} diffs connected_nodes() vs tracked.
- R2: local registered by SchedulerConfig.node_name; peer appears in connected_nodes() after connect/accept → first poll = joined.
- R3: poll loop (~250ms-1s, or piggyback supervisor.rs:73 reap cadence) diffs, updates peers, info! each transition; joined→sync.on_peer_join(node) (R5 backfill); left→sync.on_peer_leave(node).
- R4: peer TCP drop → beamr marks down + removes from table (connection.rs:594/610/624) → disappears from connected_nodes() → next poll = left;
  SIMULTANEOUSLY beamr's hook calls purge_remote_node (R6). Two independent observers of the same table — no contention.
- Tests: 2 nodes connect → both peers() include each other within a poll; drop one → survivor peers() drops it + logs left + pg remote_members for it empty (beamr purge fired).

## 4. R5/R6 sync.rs (channel = pg group; cross-scheduler RESOLVED by Constraint B)
- Each channel name = a pg group in default scope. Subscriber pid (on the clustered=channel-supervisor scheduler) joins → propagates. NO cross-scheduler delivery needed.
- SUBSCRIPTION-ENUMERATION SEAM: add a SubscriptionObserver to the channel layer (channel/types.rs): subscribe_inner (types.rs:331) has registration.pid()
  (subscription.rs:72); invoke observer.on_subscribe(channel,pid) after core.subscribe, observer.on_unsubscribe(channel,pid) in unsubscribe (types.rs:343).
- struct ClusterSync{pg:Arc<PgRegistry>,scope:Atom,atoms:Arc<AtomTable>,membership:Membership}:
  on_subscribe(ch,pid)→pg.join(scope,intern(ch),pid) [R5 propagate, auto-broadcast via DistSender non-blocking]; on_unsubscribe→pg.leave;
  on_peer_join(node)→R5 BACKFILL (re-broadcast local set to new node — see risk 3); remote_targets(ch)->Vec<RemoteMember> for publish fan-out.
- Publish fan-out: channel publish path consults remote_targets(ch) → send envelope to each RemoteMember external pid via SchedulerDistributionSendFacility::send_remote (normal external-pid `!`, same scheduler).
- R6 peer departure: AUTOMATIC — beamr purge_remote_node removes all that node's RemoteMembers; remote_targets drops them; local subs see no error. NO liminal code needed for R6.
- Tests: subscribe on A → B remote_members(ch) includes A's external pid; publish on B reaches A's subscriber inbox; peer-join backfill delivers A's pre-existing subs to late C; drop A → B remote_members(ch) drops A + B local subs still receive.

## 5. R7 mod.rs — declarations + re-exports ONLY (no logic). Put start()/ClusterHandle in membership.rs, re-export, to keep mod.rs pure.

## RISKS / decisions (implementer + reviewer)
1. Single shared ChannelSupervisor refactor = PREREQUISITE (touches LIM-002 wiring services.rs:171-182). Real refactor, not just new files.
2. ClusterConfig MISSING 2 fields: cookie:String (default DEFAULT_COOKIE) + listen_address:SocketAddr. Update config/types.rs:60, env.rs, validation.rs, tests.
3. R5 backfill on peer-join is SYNC's job: pg.join broadcasts only on the INSERT edge; a new node won't learn pre-existing memberships. Implement explicit
   backfill in sync (re-send local members to the new node) OR request a small PgRegistry::resend_local_to_node(node) beamr helper. ONLY req not free. PREFER sync-side (keep beamr stable).
4. async-from-sync: block_on (mirror block_on_distribution_send); keep AcceptHandle alive for server lifetime.
5. membership poll cadence: pick interval or piggyback reap cadence. node-down for R6 is NOT poll-dependent (beamr hook); poll only drives logging + R5 backfill.
6. DO NOT call register_connection_down from liminal-server (Constraint A) — overwrites pg-purge, breaks R6. HIGHEST-RISK mistake.
7. Publish fan-out reachability: confirm the channel actor's ProcessContext has the send facility (same scheduler, so external-pid send works — verify).
8. Use scheduler.distribution_connections() exclusively, NEVER net_kernel:connect_node / the NetKernel manager.
