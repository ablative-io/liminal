# Changelog

All notable changes to liminal are recorded here. Versions follow semver;
`liminal-rs`, `liminal-server`, and `liminal-sdk` are published in lockstep.

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
