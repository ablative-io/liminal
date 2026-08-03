# Runtime channel registration — design brief v0

**Lane opening. Hermes Crumpet, 2026-08-03. Status: DRAFT — frames the design
round; decides nothing the round has not read.**

Commissioned by the BUS-FOLLOWS-RECORD design gate (Cally Ray, DM `a404d58d`,
2026-08-03): the manifold surface's bus roster becomes a projection of its
registry's record, and that design is a three-layer lane whose first layer is
this one — a liminal capability that does not exist today. Layer 2 is
frame-host plumbing (published crate, its own release discipline); layer 3 is
the manifold projection (bridge-driven, per the BUS-FOLLOWS-RECORD seam-1
ruling). This brief is layer 1 only.

## The measured ground

At `ca66543`, the server builds its channel map once at boot and can never
change it:

- `crates/liminal-server/src/server/connection/services.rs:254` — a plain
  `HashMap<String, ConfiguredChannel>` field.
- `:337-409` — the map is built in the constructor loop from
  `config.channels`; the sole `channels.insert` in the crate is `:369`, inside
  that loop.
- No interior mutability; every other touch of the map is a read. (Derived
  independently at two seats: mine, then re-derived and extended by Cally Ray
  before the gate.)
- `:967` (publish) and `:1051` (subscribe) — the two PRODUCTION refusal
  sites: an unknown name refuses with `channel '<name>' is not configured`.
  A third occurrence of the string at `:559` sits inside `#[cfg(test)]
  subscribe_handle_for_test` and is not production surface — the earlier
  three-site count (this brief's v0, and the gate derivation alike) counted
  the STRING, not the production construct. Corrected at the design round
  (Cally Ray, F1, 2026-08-03), verified at both seats.

So "the roster is boot-frozen" is not a policy today — it is a structural
property of one field. This lane changes that property on purpose, and the
design round's job is to decide what the new property is and what it costs.

## The consumed interface (ruled elsewhere; binding here)

These are not open questions. They were ruled at the BUS-FOLLOWS-RECORD gate
and this design must satisfy them:

1. **Refuse-unless-known survives.** An unknown channel name is refused by
   name, loudly, exactly as today. Registration moves the authority that can
   make a name known; it does not soften the door.
2. **Quiesce, never yank** (seam-2). The capability must support a channel
   state in which new publishes are refused with a named reason (e.g.
   "archived") while existing subscribers keep their stream until they
   disconnect and new subscribes are refused. Channel *removal* out from under
   a live subscriber is not a thing this design provides. Whether hard
   removal exists at all (for empty, quiesced channels) is an open question
   below — quiesce is the ruled floor.
3. **The ordering law** (PROJECT-BEFORE-SEAT or CARRIER-WAITS, gated as
   CARRIER-WAITS with three conditions). The conditions bind the *consumer*
   (manifold's carriers), but they constrain this layer's refusal surface: a
   caller must be able to distinguish "this name is not registered" from any
   other failure cheaply and reliably, because a waiting carrier keys its
   backoff on exactly that answer, and its truth stream reports which state
   it is in. The refusal must therefore stay typed/structured on the wire and
   in the embedded API — not a string a consumer greps.
4. **Semver honesty.** Registration is new public surface on liminal-server.
   The version implication is decided at the cut, in the open, under the
   discipline the 0.4.0 arc re-taught (a public break under an unbumped
   version is a lying surface). Nothing in this lane pre-decides the number.

## What has to be true of any design the round accepts

- **The map's concurrency story is explicit.** Today's field is read by live
  connection paths with no synchronization because it never changes. Any
  mutable design states its synchronization (actor-owned registry, lock
  discipline, copy-on-write snapshot — the round chooses) and prices the read
  path: the subscribe/publish hot path currently pays zero for the roster
  being static, and whatever it pays afterwards is named with a bound and a
  pinning test (idle-cost rules apply in full).
- **Registered channels are real channels.** Schema resolution, durable vs
  ephemeral mode, and cluster-supervisor attachment all happen in the boot
  loop today (`:337-376`). A runtime-registered channel goes through the same
  construction — same schema handling, same supervisor — or the difference is
  a named refusal (e.g. if durable registration at runtime is out of scope for
  v1, the API refuses it by name rather than quietly building a different
  kind of channel).
- **Boot config keeps meaning what it means.** The `channels = [...]` list
  becomes a seed, not a lie: the design states whether boot-configured and
  runtime-registered channels are distinguishable afterwards, and what a
  restart does (registered channels are gone unless something re-registers
  them — that is the *expected* contract for layer 3, whose projector
  re-projects from the record on every boot, but it must be said, not
  assumed).
- **Idle cost is bounded and pinned.** Each registered channel is an actor
  plus retained state. The design names the per-channel idle cost, what
  bounds the aggregate (a cap? whose?), and lands a pinning test in the
  keepalive-honest form (unrelated counters grow while the unit's stay flat).

## Open questions for the design round

- **Q1 — Where does the API live?** The consumer is frame-host, which embeds
  the server in-process. The primary surface is therefore a Rust API on the
  embedded server handle. Is there ALSO a wire-level admin surface? A wire
  surface has authentication implications the embedded surface does not
  (adjacent: the SubscriptionStream auth-token defect, task #2), and v1 can
  refuse to have one. The round decides; the brief's lean is embedded-only v1.
- **Q2 — Who may register?** In embedded mode the host process is the
  authority and the answer may be "whoever holds the handle". If a wire
  surface ever exists, this question reopens with teeth.
- **Q3 — Registration semantics on name collision.** Registering an existing
  name: refuse by name, or idempotent success if the configuration is
  identical? The projector (layer 3) re-projects on boot and after
  reconnects, so idempotent-if-identical is the lean; a same-name different
  config registration is a refusal either way.
- **Q4 — Does deregistration exist at all in v1?** Quiesce is ruled. Hard
  removal of a quiesced, subscriber-free channel is convenience, not
  requirement, and every removal path is a new class of race. Lean: no
  removal in v1; quiesce + restart covers layer 3's needs.
- **Q5 — Quiesce as a state machine.** Active → quiesced("reason") — is it
  one-way? Can a projector un-quiesce (un-archive exists on the record?
  BUS-FOLLOWS-RECORD says archive is one-way on the record, so one-way here
  matches; said, not assumed).
- **Q6 — The version number.** Decided at the cut. The round's only
  obligation is to keep the surface small enough that the honest number is
  the intended one.

## What this brief does not do

No API signatures, no module layout, no implementation order — those belong
to the design round, which has not read this yet. No claims about
liminal-protocol: the current read is that registration is a server-crate
surface and the wire protocol is untouched (no new frames; refusals already
travel), and the round must verify that read rather than inherit it.
