# Changelog

All notable changes to liminal are recorded here. Versions follow semver.
Each crate is versioned and published independently (`liminal-rs`,
`liminal-server`, `liminal-protocol`, `liminal-sdk`); a release entry names
the versions it actually moves.

## liminal-protocol 0.7.1 — 2026-08-18

`liminal-protocol` 0.7.0 → 0.7.1; no other crate moves.

**An additive cut that closes silent in-tree drift.** The tree's protocol had
carried one public item the published 0.7.0 does not:
`ClientParticipantAggregate::lost_credential_attach` (added by the #195
peek-verdict work; total drift measured at client.rs +43/−0, nothing removed
or resignatured). The gap was latent — every in-workspace build resolves the
path crate — and surfaced the first time a standalone consumer needed the
symbol: `liminal-sdk` 0.8.0's package verification builds against PUBLISHED
dependencies and failed on the missing method. Additive-only under 0.x is a
patch; `^0.7.0` requirements resolve this release without edits.

### Added

- `ClientParticipantAggregate::lost_credential_attach` — peek at a retained
  credential-attach request on the client aggregate (`client.rs`).

## liminal-sdk 0.8.0 — 2026-08-18

`liminal-sdk` 0.7.0 → 0.8.0; no other crate moves.

**A breaking release with zero code change.** No liminal-sdk item was added,
removed or resignatured, and the crate's own API names neither moved
dependency. The break is transitive: the optional `embedded` feature carries
`liminal-rs` and `liminal-server`, which moved to 0.6.0 / 0.9.0 in the
release below. A consumer with `embedded` enabled resolves the new family
through this release; published 0.7.0's `liminal-rs ^0.5.5` edge cannot
unify with 0.6.0 and would fork a consumer's lock two-versions (measured at
frame's lock, where the duplicate guard refuses the fork by design).

### Changed

- Version only. The `embedded` feature's edges now require `liminal-rs`
  0.6.0 and `liminal-server` 0.9.0 (checksums in the entry below).

## 0.6.0 — 2026-08-18

`liminal-rs` 0.5.5 → 0.6.0, `liminal-server` 0.8.2 → 0.9.0;
`liminal-protocol` 0.7.0 and `liminal-sdk` 0.7.0 (both unchanged).

**This is a breaking release, and the break is inherited rather than authored.**
No liminal item was added, removed or resignatured; the wire protocol did not
move; no contract text changed. What moved is the major version of two
dependencies whose types liminal carries in its own public API, which under
0.x semver makes this a minor bump for both crates rather than a patch.

### Changed

- **beamr 0.17.1 → 0.19.1 and haematite 0.8.3 → 0.9.0, in one motion.** The two
  pins move together and may never move apart: haematite's public API carries
  beamr types across the crate boundary, so two beamr versions in one tree are
  two incompatible types with identical names. haematite is pinned exactly
  (`=0.9.0`) for that reason. Pinned by published artifact:

  | crate | version | sha256 |
  |---|---|---|
  | beamr | 0.19.1 | `bd7d2a8b408452efead5933fb672bee795b64666873f2aa6dbf717451c8b2dd7` |
  | haematite | 0.9.0 | `60b8566825e2647cc4b1b25395972d06eb722b66f2fe398de9651c191c9135e1` |
  | gleam-types | 0.4.4 | `d584956cc629238409467c899f7fb219bb60bfb78afef3a715842c6bed87f9e2` |

  gleam-types rides along because beamr 0.19.1 requires `^0.4.4`.

- **Scheduler construction now declares its native-BIF surface.** beamr 0.19
  added a required `NativeBifs` argument to `Scheduler::new` and
  `Scheduler::with_services`, with no `Default` and no `From`, so every embedder
  must state its answer. All three of liminal's schedulers declare
  `NativeBifs::none()`, which is correct because none of them loads bytecode:
  the two hand-built actor modules carry empty function tables, the connection
  scheduler registers no module at all, and the six opcodes liminal emits
  (`Label`, `Wait`, `RemoveMessage`, `LoopRec`, `CallOnly`, `CallExt`) contain
  no arithmetic, comparison or type guard — the only place the declaration can
  bite. **This is an internal change with no observable effect**; it is recorded
  because it is the entire compile-forced surface of the beamr major bump.

### Why this is breaking, stated precisely

Both published crates expose the moved dependencies in public signatures, so a
consumer that names those types must recompile against the new majors:

- `liminal-rs` — `haematite::ApiError` is a variant of the public
  `DurabilityError` (`durability/error.rs:10`); `HaematiteStore::new` takes
  `Arc<haematite::EventStore>` (`durability/store.rs:87`);
  `ChannelSupervisor::scheduler` returns `Arc<beamr::Scheduler>`
  (`channel/supervisor.rs:193`); `ParticipantStatus::exit_reason` is a public
  field of type `Option<beamr::ExitReason>` (`conversation/types.rs:210`).
- `liminal-server` — beamr types are public throughout `cluster::{discovery,
  membership, sync}` (e.g. `MembershipDelta::joined: Vec<Atom>` at
  `cluster/membership.rs:104`, `membership::start` taking `&Arc<Scheduler>` at
  `:542`) and via `ConnectionSupervisor::scheduler`
  (`server/connection/supervisor.rs:249`). haematite, by contrast, is fully
  encapsulated in this crate — private functions and `cfg(test)` only — so its
  bump reaches `liminal-server` only transitively through `liminal-rs`.

`liminal-protocol` and `liminal-sdk` name neither dependency and are not
resignatured, so their versions do not move.

## 0.5.1 — 2026-07-29

`liminal-rs` 0.5.1, `liminal-server` 0.5.1, `liminal-sdk` 0.5.1;
`liminal-protocol` 0.3.2 (unchanged).

All changes below are additive or internal: no public item was added, removed
or resignatured in `liminal-rs` or `liminal-server`, the wire protocol did not
move, and the readiness contract did not move. That is why this is a patch
release and not a minor one.

### Added

- **The server logs.** `liminal-server` installs a stderr `tracing` subscriber
  at startup (`ae3abd0` red, `ba43b92` fix). Before this the binary ran from
  boot to shutdown printing nothing, and its 120 `tracing` events — connection
  failures, drain timeouts, durable-flush failures among them — were dropped by
  the no-op global dispatcher. Filtered by `RUST_LOG`; the default when the
  variable is unset is `warn,liminal_server=info,liminal=info`. Colour is
  emitted only when stderr is a terminal, so piped output stays byte-clean
  (`925b863`).
- **An empty `RUST_LOG` means total silence, including `error` events.**
  `RUST_LOG=""` is a valid but empty directive set — it is *not* "use the
  default". This is upstream `env-filter` semantics and is kept deliberately.
  Unset the variable rather than emptying it if you want the default back.
- **Bootable from a repository checkout.** `config/liminal.example.toml` is a
  complete, commented, working configuration, kept honest by a test that parses
  it through the real loader (`7bd2c23` red, `93f114d` fix). README gains a
  Usage quickstart covering the run line, the thirteen `LIMINAL_*` overrides,
  the health and metrics routes, and the `persistence_path` trap: the directory
  must already exist and is never created for you, so a path typo fails startup
  instead of quietly minting a new directory.
  **Stated precisely, because "shipped" would overclaim it: that file lives at
  the repository root, outside every crate, so it is NOT carried inside the
  published `liminal-server` crate.** An operator who installs from crates.io
  gets a binary that requires `--config` with five mandatory keys and no
  defaults, and must copy the example out of the repository to write one. The
  quickstart's `cargo build -p liminal-server` line is likewise a workspace
  command and assumes a checkout. Making the *published crate* self-sufficient
  is follow-up work and is not claimed here.

### Changed

- **Cluster membership is driven by ordered events, not polling** (SRV-008;
  `9d87e32` red, `25e8ed5` fix). The poll interval, sampling entry point, poll
  thread and snapshot/diff change detection are gone; membership now arms
  beamr's connection-event stream with an atomic initial view. Effects still run
  through the same `apply_delta` funnel. **All of the deleted machinery was
  private — no public item changed**, which is why this is not a breaking
  change. The one observable difference is a strict improvement: a node that
  dials its seeds previously had an empty membership view until a poll tick
  sampled the table, and now has an exact view the instant `Node::start`
  returns. That removes a staleness window; it does not change a contract.

### Fixed

- **SDK: one named 5 s setup deadline across all three readers** (SDK-010;
  red pins `847bfb3`, fix `b54cf7b`). The push, TCP-subscription, and
  WebSocket-subscription readers now run every post-connect setup read under
  the single named `SETUP_TIMEOUT` of 5 s — the estate's already-ratified
  value, generalized and named — replacing the unnamed 100 ms reader-poll
  quantum that had been serving as the setup deadline by accident; steady-state
  reads block rather than poll. Operator-visible consequence: a peer whose
  control-frame reply latency exceeds 100 ms no longer fails setup — retrying
  a 100 ms window is not equivalent to one long window. This cures the frame
  announcer's 1-in-2 boot crash.

### Internal

Neither item below is observable to anyone installing from crates.io. Both are
recorded because a reader diffing 0.5.0 against this tag finds production files
changed under them, and an entry that does not account for those files makes
that reader repeat the investigation that produced this paragraph.

- **The build toolchain is pinned at 1.97.1** (`cb1bb50`), and the pin carried
  behaviour-identical lint and trybuild collateral across seventeen files
  (`358742b`) — four of them production: `durability/bridge.rs` and
  `routing/function/execute/actor.rs` in `liminal-rs`, `cluster/discovery.rs`
  and `server/participant/production/outbox_replay.rs` in `liminal-server`.
  The edits are manual no-op wakers replaced with `std`'s `Waker::noop()`,
  `map_or_else(f, identity)` narrowed to `unwrap_or_else(f)`, `loop`/`let-else`
  rewritten as `while-let`, `Duration` millisecond literals in tests written as
  whole seconds, and re-blessed trybuild stderr (same error code, same span,
  wording only). **The minimum supported Rust version did not move: it remains
  1.85, as `Cargo.toml` and the README both still state** — `Waker::noop()`
  stabilized in 1.85, so the collateral stays inside the declared floor.
  `rust-toolchain.toml` lives at the repository root, outside every crate, so
  like the example config it is not carried inside any published crate: it
  binds anyone building from a checkout and nobody building against the
  registry.
- **`liminal-protocol` is not republished, and its source is not identical to
  published 0.3.2.** The crate took one behaviour-identical edit from that same
  collateral — the `map_or_else`/`unwrap_or_else` narrowing in
  `wire/server_codec.rs`, plus a redundant pattern binding dropped in a test.
  No public item changed and the encoding did not move, so 0.3.2 is left
  standing rather than re-cut; "unchanged" in the header above refers to the
  version, which is what the other three crates depend on.

### Notes

- **Non-Rust SDK claims remain untrue in this cut.** The Gleam SDK's FFI
  module exists nowhere in the repo, so every Gleam I/O call fails at load;
  the TypeScript `Channel`/`Connection` API still dead-ends on a missing
  transport. Frame consumes the Rust SDK, so the cure above is unaffected.
  Truing these claims is in flight (front-door fix wave, Leg D) and lands in
  the following release.
- **The tag gap stands, deliberately.** Release tags stop at `liminal-v0.2.3`;
  0.4.x and 0.5.0 remain untagged (ledger A-6). Retro-tagging history is out
  of scope for an unblocking release; the item stays open for a calm day.
- This entry records SDK-010 at its landing (`b54cf7b`): the change shipped
  after 0.5.0's notes were cut and is recorded here, dated to this release,
  never backdated.

## 0.5.0 — 2026-07-28

`liminal-rs` 0.5.0, `liminal-server` 0.5.0, `liminal-sdk` 0.5.0;
`liminal-protocol` 0.3.2 (unchanged).

### Deployment note — upgrade servers BEFORE workers

`WorkerRegister` now carries a trailing activity census. An old worker
registering against a new server is safe by construction (the absent census
decodes as empty — the pre-contract shape). A NEW worker registering against
an old (≤0.4.1) server fails the connection loudly: the census bytes are
refused as leftover payload. Upgrade every server before any worker.

### Added

- **Protocol: worker activity census on `WorkerRegister`.** A trailing field
  after `identity`: u32 descriptor count, then per descriptor three
  length-prefixed strings (`name`, `input_schema_json`, `output_schema_json`).
  An empty census identifies a pre-contract worker. Compatibility rides
  trailing-bytes detection (`PayloadReader::is_finished`); this consumes the
  frame's ONE trailing-bytes extension slot — any future field on this frame
  must ride a `ProtocolVersion` gate, never a second sniff. Pinned both ways:
  old-shaped frame → empty census, and non-empty census exact round-trip.

### Fixed

- **Server: Detached-flavor candidate-lane drain is faithful detach
  finalization** (S-16), with live-drain, live-resume-with-parked-replay,
  mixed-flavor ordering, and unclean-restart pins (S-18).

## 0.4.1 — 2026-07-23

`liminal-rs` 0.4.1, `liminal-server` 0.4.1, `liminal-sdk` 0.4.1;
`liminal-protocol` 0.3.2 (additive).

### Added

- **SDK: explicit flush surface** (`SDK-PUSH-FLUSH` r2). `PushClient::flush()`
  awaits the server's verdict for every response-eliciting publish written
  before the call, bounded by a 5 s budget; `close()` = flush-then-graceful-
  half-close. `FlushOutcome { failures, unresolved, mode }`:
  `failures.is_empty() && unresolved == 0` is the ONLY proven-accepted shape;
  budget expiry is a normal caller-inspected outcome, never an `Err`.
  Rejections carry the raw wire `{reason_code, message}` verbatim. Publishes
  to the reserved observability channel elicit no server response by design
  and sit outside the flush contract — on a server run without a notifier,
  an observability-channel publish falls through to channel machinery, DOES
  elicit a response, and flush fails loudly with the response-count-mismatch
  mechanism error: designed, and operator-visible.
- **Protocol: candidate-lane pending-terminal drain owner operation**
  (additive; old logs remain byte-compatible).

### Fixed

- **Server: restore-window publish tear.** A valid publish that encountered a
  crash-restored `PendingFinalization(Died)` residence tore the connection
  with a Fatal invariant error. The server now drains that pending terminal
  as one durable candidate transaction (terminal record, retention, candidate
  deletion, binding-slot release) and the publish COMMITS after the drain —
  proven live-socket end-to-end including a real unclean restart replaying
  the drain row.
- **rs: subscriber-spawn trap_exit race.** The subscriber process is now
  spawned via beamr 0.16.1's `spawn_native_trap_exit` (flag set before the
  process is runnable), retiring the once-per-battery `NoCaller` failure on
  high-core hosts.

## 0.4.0 — 2026-07-23

Haematite 0.7.0 uptake: `liminal-rs` 0.4.0, `liminal-server` 0.4.0, and
`liminal-sdk` 0.4.0 in lockstep. `liminal-protocol` stays 0.3.1 — untouched
by this line.

### Changed

- **haematite `0.6.2` → `0.7.0`** (the sole dependency change). Every
  `DatabaseConfig` construction site gains `executor_threads: None`
  (haematite's auto-sizing default); no other haematite surface is exercised
  differently. Existing v1/unstamped stores open under `V1_DEFAULT` forever;
  databases **created** by this version are stamped haematite
  `ON_DISK_FORMAT_VERSION = 2` and cannot be opened by earlier haematite
  binaries.

### Why 0.4.0 rather than 0.3.4

- `liminal-rs`: haematite is a **public dependency** —
  `liminal::durability::DurabilityError::StoreError(haematite::ApiError)`
  carries a haematite type in a public field, and
  `HaematiteStore::new(Arc<haematite::EventStore>)` takes one — so a
  haematite major-class bump is breaking-class for this crate.
- `liminal-sdk`: its public surface exposes `liminal::protocol` types
  (`DeliveredMessage::schema_id() -> SchemaId` on both transports), so it
  follows `liminal-rs`'s break.
- `liminal-server`: API-compatible, but a compatible-range upgrade must not
  silently begin creating format-2 stores that the operator's previous
  binary cannot open. The no-downgrade format shift rides the major-class
  number instead of hiding in a patch.

## 0.3.3 — 2026-07-23

Delivery-integrity release: `liminal-server` 0.3.3 and `liminal-sdk` 0.3.3.
`liminal-rs` stays 0.3.2 and `liminal-protocol` stays 0.3.1 (untouched by
this line — zero of the fix commits modify either crate).

### Fixed

Teardown-window delivery loss (DEFECT A): events published fire-and-forget
immediately before an embedded server's shutdown were lost through two
unfenced teardown windows, first detected by a downstream storeless
consumer that published a burst into its own shutdown.

- **SDK (A-i):** `PushClient::drop` now closes gracefully — it shuts the
  write half and drains pending `PublishAck`s to the server's FIN rather
  than closing with unread bytes, so a fire-and-forget burst is no longer
  stranded by the RST that made the server's kernel discard publish frames
  it had not yet read.
- **Server (A-ii):** `run_shutdown_sequence` now runs a TOLD flush barrier
  between stop-accepting and the shutdown `Disconnect` broadcast — parking
  (bounded by `drain_timeout`, no polling) until every accepted publish has
  fanned out to its subscriber's socket — so the `Disconnect` can no longer
  overtake an in-flight delivery.

While-dead publish delivery loss (DEFECT B1): a publish accepted while a
subscriber was connection-lost-Detached-but-resumable never reached that
subscriber's resumed replay — the recipient snapshot admitted only
live-`Bound` slots, so the accepted-then-lost record minted no durable
obligation for the resumable peer.

- **Server (B1):** the produced-record recipient snapshot now admits
  `Bound | Detached` slots, keyed on map presence (a cleanly-Left peer is
  removed from `authority.slots`, so departed identities remain excluded).
  A Detached recipient's obligation is durably installed and PARKED — owed
  no live delivery tell (it has no connection to notify), it replays on the
  subscriber's `CredentialAttach` resume — so an accepted while-dead publish
  now reaches the resumed session.

### Behavior change (carried from 0.3.2, release-note flag)

- **W2 obligation-debt dispatch reports peer connection loss eagerly.** On
  a peer connection loss, obligation-debt dispatch delivers a typed
  `ResponderFailed { NoConnection }` on the request surface AND exactly one
  `PeerFailed` lifecycle item on each subscribed surface — exactly-once per
  exact target, a designed W2 invariant per `W2-OBLIGATION-DEBT-DISPATCH.md`'s
  dedup-and-notify-once rule (oracle-guarded by
  `published_obligation_tells_exact_live_dispatch_once` and
  `dispatch_impact_unions_multi_effect_targets`) — where prior versions were
  silent until the requester's deadline. This is intended behavior being
  flagged for release notes, not a fix.

## 0.3.2 — 2026-07-23

Dependency convergence release, no API changes: beamr 0.15.4 → 0.16.0 and
haematite 0.6.1 → 0.6.2. beamr 0.16.0 carries four interpreter/BIF fixes
that are hot-path once `gleam_erlang` loads (cross-process local `send/2`
delivery, `func_info` raising catchable `function_clause` instead of
spinning, bare-atom `if_clause`, boxed-reference `demonitor/1`) plus the
breaking selector-shadow removal — liminal's actor tiers assemble no
selector opcodes, so the removal is inert here (verified by instruction
census at the 0.16.0 uptake). Ships with `liminal-protocol` 0.3.1.

## liminal-protocol 0.3.1 — 2026-07-23

Additive release (no removed or changed public items — verified by diff
against the 0.3.0 release commit): the W2 obligation-debt dispatch
surface (`ObligationDebtDispatchState`/`Transition`/`Decision`,
`decide_obligation_debt_dispatch`, `scalar_audit_for_recipient_endpoint`,
debt-owner coupling at the delivery seam). Published because
`liminal-server` 0.3.2 consumes this surface — the gap was caught by
`cargo publish`'s tarball verify, which builds against the registry
rather than workspace paths.

## liminal-protocol 0.3.0 — 2026-07-21

W1b durable connection-fate sources land: every participant binding now
records an exact Died / Ordinary / Recovered / Detached fate row, flushed
before transport teardown and replayed identically live and cold.

Breaking API changes (the reason this is 0.3.0, not 0.2.2):

- Public `Clone`/`Copy` removed from the fenced-attach proof surface, the
  public recovered-fate method is now private, and
  `DetachedCredentialRecovery::fenced_attach` is no longer public — attach
  commit now splits operational state from a single non-cloneable fate
  token, so a proof cannot be minted, reused, or forked twice
  (compile-fail-tested via trybuild).
- `VerifiedAttachCommit<F>` is lifetime-free.
- New public `ServerError::ParticipantServiceFatal`.
- New sealed fate/finalizer authority surface and marker-source APIs.

## 0.2.4 — 2026-07-13

The release that retires the idle-CPU burn. Three payloads, one publish.

### 1. Idle connections park instead of busy-spinning (the host-resource incident)

Before this release, an idle connection was **permanently runnable**: the
connection process returned `Continue` after every drain, so the connection
scheduler's workers never slept. On a host with a handful of idle
connections this burned whole cores — measured at **~350–427% CPU with 11
idle workers** on the machine that surfaced it, and independently measured
in aion's embedded front door at **~140% with zero workers connected, plus
~30–50% per connected worker**.

Connections now register their socket with beamr's readiness service and
return `NativeOutcome::Wait`, waking only on a real event (inbound bytes,
writable-after-blocked, subscription publish, control/push, reply
availability, reply-deadline expiry, EOF/HUP, shutdown). An idle connection
now costs **zero slices and zero wakes**.

Consumers embedding liminal (aion's worker front door among them) inherit
the cure by bumping this pin and rebuilding — no code change and no config
change is required.

Pinned so it cannot regress: the former busy-spin assertion is inverted into
its own tombstone (`idle_connection_slice_count_is_flat_across_soak`), and
the scheduler census asserts exactly one readiness poll thread.

**Requires beamr 0.14.0** (readiness service; `readiness` feature named
explicitly in the manifest rather than inherited from beamr's defaults).

### 2. A push's reply deadline belongs to the push, not to the caller's poll (G7)

`PushReplyAwaiter::receive(timeout)`'s `timeout` is a **wait quantum only** —
an elapsed poll is a benign re-arm and never cancels the reply slot. A caller
polling `receive(1s)` in a re-arm loop no longer sees a false worker-death
when its handler simply runs longer than one poll quantum.

Restored contract (this shape existed in 0.2.3 and was broken on unreleased
main only — **no published release ever carried the defect**):

- The default slot lifetime is reclaimed by **reply-consumed or
  connection-close**; the `max_pending_pushes_per_connection` cap bounds
  abandonment.
- **New, additive:** `ConnectionSupervisor::push_to_connection_with_deadline`
  attaches an explicit per-push reply deadline, resolving to the new typed
  `ServerError::PushReplyExpired`. Expiry is evaluated host-side and lazily —
  **no timer thread, no sweeper, zero idle cost.**
- **Publication invariant:** an `Err` from either push method guarantees no
  `Push` control was ever published (the client never saw it); an `Ok`
  promises *admission*, and the awaiter's outcome carries the delivery truth.
- The poll quantum never changes the protocol outcome: a deadlined push waits
  the earlier of the caller's quantum and its own deadline, so the terminal
  result is identical however the caller polls.

`push_to_connection` is behaviourally unchanged on the no-deadline path.

### 3. Dependency graph: two beamr copies, named rather than hidden

liminal depends directly on **beamr 0.14.0** (connection/channel schedulers,
`readiness` + `cooperative` features) while **haematite 0.4.1** pulls its own
**beamr 0.13.0** transitively for the durable event store. The two never
exchange a type: haematite fully encapsulates its beamr behind
`EventStore`/`Database`/`ApiError`, none of which expose a beamr type across
liminal's boundary, and the copies compile with disjoint feature sets. There
is **no runtime cost, no idle resident state, and no correctness surface** to
the split — it is bloat and version skew, not a defect.

Re-unification onto a single beamr line is a **haematite-side change** (beamr
types cross haematite's public sync surface, making it a major-version bump
there) and rides the next haematite release. It is **deliberately deferred,
not overlooked**.

### Also in this release

- **D2 — worker front door:** a capability-scoped services profile constructs
  only what it serves (an embedder needing connections alone no longer builds
  the durable store, channel, and conversation schedulers).
- **D3 — ephemeral store lifecycle:** temp-dir stores are owned by a guard and
  removed on last-handle drop (they previously leaked, 276 directories deep on
  the incident host).
- **D4 — conversation/finalization repair:** teardown is non-blocking and
  idempotent on every path; an exit watcher makes participant death
  observable rather than silently leaking the conversation.
- **G4 — oversize frames:** a frame larger than the free kernel send buffer is
  no longer truncated on the wire (it previously desynchronized the client
  decoder permanently); pinned by a 512 KiB regression through a forced
  WouldBlock boundary.
- **Typed caps** (`[limits]`): connections, subscriptions, conversations,
  pending pushes, pending replies (per connection and per conversation),
  connection inbox bytes, subscription inbox depth — each refused by type at
  admission rather than absorbed silently.

### Upgrading

Nothing is required beyond the version bump. The one API signature change is
additive-with-a-companion: `ConnectionServices::subscribe` carries an
`Option<InboxInstall>` so a bounded, wakeable subscription inbox can be
installed at construction; `ChannelHandle::subscribe` is unchanged, and
`subscribe_with_install` is the additive entry point.
