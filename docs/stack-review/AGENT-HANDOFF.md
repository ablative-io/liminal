# liminal — incoming-agent handoff (from Artemis Peach, beamr domain)

*2026-07-07. For the agent picking up liminal work. I own beamr next door
(`../beamr`) and wrote the July 2026 liminal review docs in this directory —
read `liminal-review.md` (state), `liminal-ledger.md` (work map), and
`liminal-assets-pack.md` (review prompts, verification rules, standing
orders) before touching anything. This file is the delta: sequencing,
coordination points with in-flight beamr work, and house rules.*

*I am not reachable from your session directly — I'm in the team's Meridian
channel (`stack-devs`) as Artemis Peach; you are not. Route coordination
through Annabel, or leave notes in-repo (a `docs/stack-review/NOTES-*.md`
file committed to main works; I watch this repo).*

## Do these in order

1. **Version convergence first: bump the beamr pin 0.11.0 → 0.12.0**
   (`crates/liminal-server/Cargo.toml` and wherever else beamr is pinned)
   before any feature work. The 0.12.0 changelog is additive over 0.11
   (`spawn_link_closure` is the only new surface) — haematite did the same
   bump today and found it mechanical. Gate: full workspace test suite +
   clippy clean.

2. **A1 backpressure wiring** (`liminal-ledger.md` §A1) is the
   frame-critical item — frame F-3a's acceptance criterion depends on it.
   **Design doc before code**: Defer redelivery semantics (who buffers, who
   retries, when Defer escalates to Reject, interaction with durable
   channels and dedup) per `liminal-assets-pack.md` §3. Acceptance is
   "asserted, not assumed": tests must show Defer emission under real
   slow-subscriber load, not just exercise the decision function.

3. **A2 server schema wiring** (small, self-contained) is the best warm-up
   item if you want one before A1: load `schema_ref` from channel config
   instead of building every channel with `Schema::new(json!({}))`.

## Coordination points with in-flight beamr work (mine, active now)

I am currently wiring beamr's cross-node link/exit controls onto the wire
and replacing the single-slot connection-down hook with a multi-subscriber
connection **events** API (down + up, generation-tagged). Specs land in
`../beamr/docs/DIST-CONTROL-WIRE-SPEC.md` and
`../beamr/docs/CONN-EVENTS-HOOK-SPEC.md`. Consequences for you:

- **Do NOT call `register_connection_down`.** It is a single
  replace-on-register slot and beamr's own pg-purge owns it
  (beamr `scheduler/mod.rs:876`); registering evicts pg cleanup and
  liminal's `on_peer_leave` is log-only (`sync.rs:251-262`) — you'd
  silently leak remote subscribers cluster-wide. Keep the 250ms poll for
  now.
- **Your 250ms poll's real job is join-backfill** (`membership.rs:234-241`),
  not down-detection — down cleanup is beamr's pg-purge. My new hook
  includes connection-**up** events (fired after the connection is
  installed, so `get_connection` works inside the callback) precisely so
  liminal can delete the poll once it adopts the new API. That adoption is
  a post-0.12-bump follow-up, not part of item 1.
- **Compat promise from me**: everything liminal 0.11.0 imports is treated
  as a frozen surface in my changes — `ConnectionManager::{connected_nodes,
  get_connection, set_runtime_handle}`, `DistConnection::write_raw`,
  `PgRegistry::{join, leave, default_scope, remote_members,
  apply_remote_join}`, `control::{encode_pg_update_frame,
  encode_send_frame}`, `Scheduler::{distribution_connections, atom_table,
  pg_registry, start_distribution_listener}`, `RemoteMember` fields,
  `ConnectionDownEvent{node, reason}`. pg purge-on-node-down semantics are
  preserved unchanged. If a bump ever breaks compilation anyway, that's a
  bug on my side — flag it via Annabel.
- **External pids**: beamr's `Term::try_pid` range-skip silently drops
  cross-node deliveries today. I'm adding decode-side node validation
  beamr-side. Don't extend the skip pattern in liminal's cluster code, and
  don't work around pid misrouting locally — report it.

## Constraints that are easy to trip

- **C1 sync bridge** (`liminal-ledger.md` §C1): the durability bridge is a
  `block_on` with an 8-poll bound — sound only while `HaematiteStore`
  completes on first poll. Do not introduce (or depend on) any store
  implementation that might not complete immediately; if you need async
  storage, the bridge must be replaced first (design doc, tracked against
  haematite's roadmap).
- **Workspace lints are law and bypasses are banned**: `unsafe_code`,
  `unwrap/expect/panic` are `deny` workspace-wide. Do not add `#[allow]`
  attributes to get past them — design errors as values. 500-LOC file
  limit, **200 for mod.rs/lib.rs/main.rs**.
- **3-way conformance harness**: any change to connection lifecycle,
  recovery, pressure vocabulary, or conversation lifecycle updates
  `tests/conformance/scenarios.json` + all three harnesses
  (rust/gleam/typescript) in the same PR. A scenario added to one SDK only
  is a review reject.
- **Payload discipline** (actor pattern): term payloads never travel
  through beamr mailboxes — data rides the shared queues, mailboxes carry
  wakeup atoms. Link-before-forward on every new send path.
- **OCC/durability**: in-memory state advances only after successful
  append; cursors are "absent == 0", never store physical zero;
  `release_claim` never clobbers a stored receipt.

## Honest-docs note

The design checklists under `docs/design/` show `done:false` for shipped
work — trust git and the July 2026 review docs, not the checklist JSONs
(ledger item F). If you touch an area with a stale checklist, reconcile or
delete the checklist as part of the change.
