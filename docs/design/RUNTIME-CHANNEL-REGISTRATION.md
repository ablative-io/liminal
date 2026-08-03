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

---

# Part II — Design v1

**Hermes Crumpet, 2026-08-03. Status: DESIGN v1 — ACCEPTED at the design round
with amendment v1a. Every file:line below was read at the lane worktree's bytes
before it was written down (`96e342b` for v1, re-verified for v1a). NOTHING IN
PART II IS OPEN: the round's one structural finding (F-v1-1) is resolved in
§II.5(d) and all eight of v1's LEANs are ruled. §II.12 is the register.**

Part I's open questions Q1–Q5 are closed by the round; Q6 (the version number)
stays open by design and §II.10 prices it rather than answering it. Part II adds
what Part I refused to invent: signatures, the state machine, the
synchronisation, the error vocabulary, and the tests that make the whole thing
falsifiable.

**Amendment v1a (2026-08-03)** carries the round's verdict into the body rather
than appending it: F-v1-1's mechanism is written up in §II.5(d) as decided, and
each ruling replaces the LEAN it settles in the section that owned it. Two
rulings changed the design — L5 closed the aggregate-cap gap, L8 reversed the
enumerator omission — and both are recorded as reversals in §II.12.

## II.1 — The measured ground, extended

Part I's census counted the refusal sites. This is the complete census of the
map itself — every touch of the `channels` field, because the concurrency design
in §II.4 must account for all of them:

| Site | Kind | Path |
| --- | --- | --- |
| `services.rs:254` | declaration — `HashMap<String, ConfiguredChannel>` | — |
| `services.rs:369` | the SOLE insert | constructor loop |
| `services.rs:409`, `:430` | struct-literal initialisers | `from_config_with_store_via`, `empty` |
| `services.rs:963` | read (`.get`) | `publish` |
| `services.rs:1048` | read (`.get`) | `subscribe` |
| `services.rs:1151` | full iteration | `flush_durable_state` |
| `services.rs:556` | read (`.get`) | `#[cfg(test)] subscribe_handle_for_test` |

Three production reads, one production write, one production iteration, and the
write happens only inside the constructor. That is why the read path pays
nothing today, and it is the property §II.4 has to spend.

Two further facts the design turns on, neither of them stated in Part I:

- **The channel actor is spawned lazily, not at construction.**
  `ChannelHandle::with_supervisor` is infallible and stores
  `ChannelActorState` with an empty `OnceLock`
  (`crates/liminal/src/channel/types.rs:181-187`, `:94-98`); the actor process is
  created on first use by `ChannelActorState::core`
  (`types.rs:127-142`) calling `ChannelSupervisor::spawn_channel`
  (`crates/liminal/src/channel/supervisor.rs:205-216`). A registered,
  never-published, never-subscribed channel therefore owns **zero** beamr
  processes. §II.8 is built on this.
- **`flush_durable_state` already covers whatever is in the map.** It iterates
  the map and flushes every entry whose mode is `Durable`
  (`services.rs:1151-1162`), reading the mode off the live handle
  (`handle.config().mode`). A runtime-registered durable channel is flushed at
  shutdown with no new wiring — which is one concrete sense in which "registered
  channels are real channels" is structural rather than promised.

The liminal library already contains a `ChannelRegistry`
(`crates/liminal/src/channel/registry.rs:27-30`) with `create`/`lookup`/`list`/
`close` over a `Mutex<HashMap<String, ChannelHandle>>`. The server does not use
it (zero references in `crates/liminal-server/src`), and v1 does not adopt it:
it has no schema-id column, no durable-store column, no state machine, and its
`close` (`registry.rs:114-123`) is exactly the hard removal the round refused.
It is named here so a later reader does not mistake its absence for an oversight.

## II.2 — The embedded API surface

**Where it hangs.** On `LiminalConnectionServices` as inherent methods —
the same shape and the same seam as the existing runtime-mutation API,
`register_responder`/`unregister_responder` (`services.rs:499-522`), which take
`&self`, use interior mutability, and return `Result`. The type is publicly
exported (`crates/liminal-server/src/server/connection.rs`, the
`pub use services::{… LiminalConnectionServices …}` re-export) and the embedding
host already constructs it directly before handing it to
`ConnectionSupervisor::with_services(Arc<dyn ConnectionServices>)`
(`crates/liminal-server/src/server/connection/supervisor.rs:110`). The host keeps
its own `Arc<LiminalConnectionServices>` and coerces a clone into the trait
object for the supervisor; registration is then called on the concrete `Arc`.

**The REGISTRATION vocabulary does not go on the `ConnectionServices` trait.**
The trait is public and implemented outside this file — `WorkerFrontDoorServices`
implements it at
`crates/liminal-server/src/server/connection/worker_front_door.rs:56`. A required
method breaks every external implementor; a defaulted one would put
*register/quiesce* vocabulary on a profile that serves no channels at all
(`supports_channel_operations` → `false`, `services.rs:246-248`). Inherent
methods on the one adapter that owns a channel map is the honest placement for
registration.

That objection is scoped to registration and **does not carry to the
access-refusal path.** Admission is hot-path vocabulary the worker profile
already answers — refusing channel operations is precisely what
`supports_channel_operations` exists to say (`services.rs:246-248`). §II.5(d)
therefore adds one *additive defaulted* trait method for admission, and the two
placements are consistent rather than in tension: authority-moving APIs are
inherent, per-frame admission is on the trait. Ruled explicitly by the stack lead
at the design round.

```rust
/// A channel to register at runtime. Mirrors exactly what the boot loop
/// consumes (`services.rs:338-375`) minus the config-file concerns.
pub struct ChannelRegistration {
    /// Channel name — the roster key.
    pub name: String,
    /// Raw JSON Schema bytes. `None` means the permissive empty schema `{}`,
    /// identical to a boot channel with no `schema_ref`
    /// (`services_schema.rs:20`, `:33-36`). The protocol schema id is derived
    /// from THESE bytes by the same FNV-1a derivation the boot path uses
    /// (`services_schema.rs:46-59`), so an SDK deriving ids from schema bytes
    /// converges on it exactly as it does for a boot channel.
    pub schema_bytes: Option<Vec<u8>>,
    /// Durable vs ephemeral, the same bit `ChannelDef::durable` carries
    /// (`crates/liminal-server/src/config/types.rs:103-104`).
    pub durable: bool,
}

/// Whether a registration created the channel or found it already identical.
/// Distinguishable on purpose: a projector's truth stream reports which.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Registered {
    Created,
    AlreadyIdentical,
}

/// Where a roster entry came from. Behaviourally inert; see §II.7.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChannelOrigin {
    BootConfigured,
    RuntimeRegistered,
}

/// The probe's typed answer. The carrier-waits protocol keys its backoff on
/// this and nothing else.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChannelStatus {
    NotRegistered,
    Active {
        origin: ChannelOrigin,
        mode: liminal::channel::ChannelMode,
        schema: liminal::protocol::SchemaId,
    },
    Quiesced {
        reason: String,
        origin: ChannelOrigin,
        mode: liminal::channel::ChannelMode,
    },
}

impl LiminalConnectionServices {
    /// Registers `spec` on the live roster. Idempotent when an entry of the
    /// same name has an IDENTICAL configuration (§II.6); refuses typed and by
    /// differing field otherwise.
    ///
    /// # Errors
    /// Returns [`ChannelRegistryError`] when the name exists with a different
    /// configuration, the schema bytes do not parse or compile, durable
    /// initialisation over the shared store fails, or the roster is unavailable.
    pub fn register_channel(
        &self,
        spec: &ChannelRegistration,
    ) -> Result<Registered, ChannelRegistryError>;

    /// Moves `name` from active to quiesced with a named `reason`. ONE-WAY.
    /// New publishes and new subscribes are refused afterwards, carrying the
    /// reason; existing subscribers keep their stream (§II.3).
    ///
    /// # Errors
    /// Returns [`ChannelRegistryError`] when the name is not registered, is
    /// already quiesced under a DIFFERENT reason, or the roster is unavailable.
    pub fn quiesce_channel(
        &self,
        name: &str,
        reason: impl Into<String>,
    ) -> Result<(), ChannelRegistryError>;

    /// Cheap typed probe: one roster read plus one atomic load. Touches no
    /// actor and therefore CANNOT spawn one — see §II.8.
    ///
    /// # Errors
    /// Returns [`ChannelRegistryError::RosterUnavailable`] only.
    pub fn channel_status(&self, name: &str)
        -> Result<ChannelStatus, ChannelRegistryError>;

    /// The whole roster: one minimal descriptor per entry, sorted by name.
    /// Touches no actor (same constraint as `channel_status`).
    ///
    /// # Errors
    /// Returns [`ChannelRegistryError::RosterUnavailable`] only.
    pub fn registered_channels(&self)
        -> Result<Vec<ChannelDescriptor>, ChannelRegistryError>;
}

/// One roster entry, minimally. Deliberately NOT the full status: an
/// enumeration is a census instrument, and a census needs names, origins, and
/// states — not schemas.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelDescriptor {
    pub name: String,
    pub origin: ChannelOrigin,
    pub state: ChannelState,
}

/// The state machine's two states, as a value (§II.3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChannelState {
    Active,
    Quiesced { reason: String },
}
```

**RULED — the enumerator is IN v1** (L8, overruling this design's original
omission). The grounds are the estate's census law, and they are decisive: a
by-name probe can confirm that every *expected* name is present, but it can
**never** detect an unexpected extra. Seam-5 ruled that layer 3 verifies against
a MOVING roster; verification with no enumerator is a sweep with no population
denominator, which is an instrument shape the estate forbids outright. It is also
cheap — one read lock under §II.4, `n` `Arc` derefs, no actor contact — so the
only thing the original omission bought was a smaller diff.

**`channel_status` must not use the library's existing observability.**
`ChannelHandle::subscriber_count` (`types.rs:529-531`) and
`ChannelHandle::close` (`types.rs:538-540`) both route through
`ChannelHandle::core` (`types.rs:543-545`), which SPAWNS the actor on demand.
A probe built on `subscriber_count` would make polling the status of an idle
channel materialise its actor — turning a read into a side effect and breaking
§II.8's bound. The probe reads the roster entry's own recorded fields and
nothing else.

**Construction goes through one function, by structure.** The boot loop's body
(`services.rs:344-375`) is extracted to a single
`fn build_configured_channel(name, resolved, durable, &store, &supervisor)
-> Result<ConfiguredChannel, …>`, and both the boot loop and
`register_channel` call it. This is the "registered channels are real channels"
obligation discharged as construction rather than as a promise: there is no
second place a channel can be built, so a runtime channel cannot drift into
being a different kind of object. Schema resolution likewise reuses
`resolve_channel_schema`'s exact two branches (`services_schema.rs:30-41`),
lifted to take the loaded bytes directly instead of a `ChannelDef`.

**Durable registration is IN scope**, not refused: it is the same
`ChannelHandle::new_durable_with_supervisor` call over the same shared store
(`services.rs:354-365`, `types.rs:234-245`), and its cost is named in §II.8.

## II.3 — The state machine, and the quiesce race

```
              register_channel
   (absent) ─────────────────────▶ Active
                                     │
                                     │ quiesce_channel(reason)
                                     ▼
                                  Quiesced(reason)          [terminal in v1]
```

Two states, one transition, no edge back. `Quiesced` is terminal within a
process lifetime: there is no un-quiesce (§II.9), and no removal (§II.9). The
only exit is process restart, which drops the whole runtime roster (§II.7).

Semantics of `Quiesced(reason)`:

- **New publish** — refused, typed, carrying `reason`.
- **New subscribe** — refused, typed, carrying `reason`.
- **Existing subscriptions** — untouched. Nothing revokes a
  `SubscriptionHandle`; the actor's subscriber list is not walked; no EXIT is
  sent. A subscriber that already holds a stream keeps receiving anything the
  actor still delivers, and its stream ends when it unsubscribes or its
  connection closes (`services.rs:1079-1081`, `apply.rs:527-536`).
- **The channel actor keeps running.** Quiesce is a roster-level admission
  decision, not an actor command. `ChannelHandle::close` (`types.rs:538-540`) is
  never called.
- **Re-quiesce is idempotent only if the reason is IDENTICAL** (L3 ruled). Same
  reason → `Ok(())`, mirroring §II.6's idempotent-if-identical registration.
  Different reason → `ChannelRegistryError::AlreadyQuiesced { name, reason }`
  carrying the reason already on record. A reason is written once (the
  `OnceLock` below); silently keeping the first while reporting success on the
  second would tell the caller its reason took effect when it did not, and
  silently replacing it would lose a recorded cause. Refusing is the only option
  that neither loses nor lies.

### The race, decided

**Decision: the subscribe is ADMITTED. The linearisation point is the state
read, not the completion of `handle.subscribe()`.**

The subscribe path today reads the map and then calls into the library
(`services.rs:1047-1070`). Under §II.4 the read becomes an *admission* — a
roster lookup plus one acquire-load of the entry's state — and the library call
happens after it, outside any lock. A `quiesce_channel` that commits between
those two points therefore does not stop the subscription: it lands, the
subscriber gets its stream, and it belongs to the "existing subscribers keep
their stream" clause.

The grounds:

1. **The alternative costs the hot path a lock held across an actor
   round-trip.** Refusing the raced subscribe requires the admission decision and
   `ChannelHandle::subscribe_with_install` (`types.rs:424`) to be atomic with
   respect to quiesce — i.e. a per-channel lock held across a call that reaches
   the beamr scheduler. That is the exact price §II.4 exists to avoid, paid on
   every subscribe forever to close a window measured in microseconds and opened
   only by an operation the round already calls rare.
2. **The other alternative is the yank the round refused.** Admitting and then
   revoking is removal of a live subscriber's stream under a different name.
3. **The admitted subscriber is not a correctness problem.** It is by
   construction inside the ruled-legal class. Quiesce forbids *new publishes*;
   a subscriber with no publisher receives nothing.

**The cost, said aloud:** `quiesce_channel` returning `Ok(())` does NOT mean
"no new subscriber can appear on this channel". There is a bounded window — one
in-flight `subscribe` call — in which one more can. A consumer that needs
"nobody is attached" must observe attachment directly; it cannot infer it from
quiesce's return. This is a real weakening of what a naive reader would assume
quiesce means, and it is the price of keeping the read path lock-free across the
library boundary.

Pinned by `quiesce_admits_a_subscribe_that_read_active` — §II.9, test 2.

## II.4 — The map: concurrency mechanism and the hot path's price

**Chosen: `RwLock<HashMap<String, Arc<ConfiguredChannel>>>` for the roster, with
per-entry atomic state.** The read guard is held only long enough to clone out
one `Arc<ConfiguredChannel>` — never across a library call.

```rust
struct LiminalConnectionServices {
    channels: std::sync::RwLock<HashMap<String, Arc<ConfiguredChannel>>>,
    // … unchanged fields …
}

struct ConfiguredChannel {
    handle: ChannelHandle,
    protocol_schema: ProtocolSchemaId,
    origin: ChannelOrigin,
    /// `ACTIVE` or `QUIESCED`. Written once, by CAS.
    state: std::sync::atomic::AtomicU8,
    /// Set STRICTLY BEFORE `state` flips (Release), so any reader that
    /// observes `QUIESCED` (Acquire) can read the reason.
    quiesce_reason: std::sync::OnceLock<String>,
}
```

**The state machine is not in the map.** Only `register_channel` takes the write
lock. `quiesce_channel` takes a *read* lock to find the entry and then does a
`compare_exchange` on that entry's `state` — so the one operation the round most
wants to be safe never blocks a reader at all. The `Arc` value type also makes
the write cheap in the only way that matters: an entry can be handed out and
outlive a concurrent roster mutation.

**What the hot path pays after this change**, per `publish` (`services.rs:963`)
and per `subscribe` (`services.rs:1048`) frame:

1. one uncontended `RwLock` read acquire + release (two atomic RMWs on one
   shared word),
2. one `Arc` clone (one relaxed atomic increment) and its later decrement —
   paid by `subscribe` and the test-only roster read; `publish` does NOT pay
   it (see the correction below),
3. one `Acquire` load of the entry's `state` byte,
4. one fallible branch for lock poisoning.

Against what the same frame already does: `publish` performs a haematite dedup
claim through the durability bridge when an idempotency key is present
(`services.rs:975-986`), a `publish_with_delivery` into the channel actor
(`:988-992`), and a dedup receipt write (`:1015-1026`); `subscribe` performs
schema negotiation (`:1053-1058`) and a `subscribe_with_install` actor
round-trip (`:1063-1067`). The added cost is nanoseconds on a path already
measured in actor hops and store writes. It is not free, and the honest
statement is that the roster read goes from *zero* to *bounded and small*, not
that nothing changed.

**Correction (found at build step 3, folded at lane close 2026-08-04 on the
stack lead's binding word — a named correction, not silent absorption):** item
2 was written as if the `Arc` clone were a new cost on every converted site.
At the bytes, the pre-refactor `publish` already cloned a `ChannelHandle` out
of the map (`.map(|configured| configured.handle.clone())`, the old
`services.rs:963-969` — confirmed independently at the design gate's own
F2-round read), so for `publish` the conversion SUBSTITUTES an
`Arc<ConfiguredChannel>` clone for a `ChannelHandle` clone and the genuinely
new cost is the lock discipline alone (items 1, 3, 4). `subscribe` and the
test-only roster read each pay one genuinely new `Arc` clone. The
flip-measurement trigger below is unchanged: it was stated against the shared
lock word, which every converted path touches regardless of who pays the
clone.

**The one real hazard, named:** every connection process publishing on *any*
channel now touches the same `RwLock` word. Reader-reader never blocks, but the
cacheline is shared fleet-wide. If a hot-path measurement ever shows that line
contended, the fix is the snapshot design below — and the measurement that would
flip the decision is: publish throughput on N ≥ 16 concurrently-publishing
connections degrading against the same build with the roster read hoisted out.
Say that number before running it, not after.

**Alternatives, priced and rejected:**

- *Actor-owned registry.* Every publish and subscribe becomes a message send and
  a reply wait on the shared scheduler. It converts two pointer-chases into two
  scheduler round-trips on the most-travelled path in the server. Rejected on
  cost, not on taste.
- *Copy-on-write snapshot (`arc-swap`).* The best read path available: a
  lock-free load, no shared RMW, writers clone the map. It is genuinely better
  under contention. Rejected for v1 because it adds a workspace dependency
  (`arc-swap` is absent from the `[workspace.dependencies]` block in the root
  `Cargo.toml`) to optimise a line nobody has yet measured as hot, and the
  hand-rolled equivalent is barred outright — `unsafe_code = "deny"` at
  `Cargo.toml`'s `[workspace.lints.rust]`. Named as the pre-decided upgrade path
  so a future contention finding has somewhere to go.
- `Mutex<Arc<HashMap<…>>>`. Strictly worse than `RwLock`: it serialises readers.

**Precedent.** `RwLock` is already the workspace's answer to this shape at
`crates/liminal/src/routing/table.rs`, `crates/liminal/src/routing/group.rs`,
`crates/liminal/src/metrics/registry.rs`, `.../registry/families.rs`, and
`crates/liminal-server/src/cluster/discovery.rs`. Poisoning is handled the way
this file already handles it for the responder registry — mapped to a typed
error rather than unwrapped (`services.rs:539-545`), which the workspace's
`unwrap_used`/`expect_used`/`panic = "deny"` lints require anyway.

**Iteration.** `flush_durable_state` (`services.rs:1151`) takes a read lock,
clones the `Arc`s into a `Vec`, drops the guard, and flushes — so a shutdown
flush never holds the roster lock across a durable write.

## II.5 — The error taxonomy

Today "this channel is not configured" is a `ServerError::ListenerAccept`
carrying a formatted string (`services.rs:967`, `:1051`) — the same variant used
for dedup-bridge failures (`:577`, `:581`), publish failures (`:1005`),
subscribe failures (`:1069`), conversation spawn failures (`:1105`), protocol
failures (`:1219`), and a poisoned responder lock (`:542`). It is a catch-all
(`crates/liminal-server/src/error.rs:27-28`). A consumer can distinguish
"not configured" only by grepping the message. v1 mints types instead.

### (a) Lane-local error enums

Two, in a new module `server::connection::channel_registry`. They are split
because they have disjoint call sites and only one of them reaches the wire:
`ChannelRegistryError` answers the host holding the handle, and
`ChannelAccessError` answers a frame. The second is re-exported alongside the
trait, because §II.5(d) puts it in `ConnectionServices`' own signature.

```rust
/// Failures of the REGISTRATION APIs (§II.2). Never reaches the wire.
#[derive(Debug, thiserror::Error)]
pub enum ChannelRegistryError {
    /// The name exists with a DIFFERENT configuration. `field` names the first
    /// field that differs, by type — never a string a consumer must parse.
    #[error("channel '{name}' is already registered with a different {field}")]
    AlreadyRegistered { name: String, field: ChannelConfigField },

    /// Quiesce or probe named a channel that is not on the roster.
    #[error("channel '{name}' is not registered")]
    NotRegistered { name: String },

    /// Re-quiesce under a different reason. Quiesce is one-way and its reason
    /// is written once; a second reason would silently lose one of them.
    #[error("channel '{name}' is already quiesced: {reason}")]
    AlreadyQuiesced { name: String, reason: String },

    /// The schema bytes did not parse as JSON, or did not compile as a JSON
    /// Schema (`Schema::new`, services.rs:346-348).
    #[error("channel '{name}' schema rejected: {message}")]
    SchemaRejected { name: String, message: String },

    /// `new_durable_with_supervisor` failed over the shared store
    /// (services.rs:360-365).
    #[error("durable channel '{name}' could not be initialized: {message}")]
    DurableInitFailed { name: String, message: String },

    /// Runtime registration was attempted with no `limits.max_channels`
    /// declared. Refused rather than admitted: unbounded-by-default is not a
    /// bound (§II.8). Carries the config key so the operator is told what to
    /// declare, not merely that something is missing.
    #[error(
        "runtime channel registration refused: no {cap} is configured; \
         a deployment that registers channels at runtime must declare its bound"
    )]
    CapNotConfigured { cap: &'static str },

    /// The declared `limits.max_channels` is already reached. Shaped on
    /// `ServerError::ConnectionCapReached` (error.rs:189-197): the key name and
    /// the configured value, both carried.
    #[error("channel registration refused: the {cap} limit of {limit} is reached")]
    CapReached { cap: &'static str, limit: usize },

    /// The roster lock is poisoned.
    #[error("channel roster unavailable: {message}")]
    RosterUnavailable { message: String },
}

/// The fields compared for configuration identity (§II.6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChannelConfigField {
    Mode,
    SchemaId,
    SchemaDocument,
}

/// The HOT-PATH admission refusal. Produced by the roster admission funnel,
/// consumed by publish/subscribe, rendered to the wire with a reason code.
#[derive(Debug, thiserror::Error)]
pub enum ChannelAccessError {
    #[error("channel '{name}' is not registered")]
    NotRegistered { name: String },

    #[error("channel '{name}' is quiesced: {reason}")]
    Quiesced { name: String, reason: String },

    #[error("channel roster unavailable: {message}")]
    RosterUnavailable { message: String },
}
```

**No new `ServerError` variant.** `ServerError` is public and exhaustive — no
`#[non_exhaustive]` at `error.rs:6-7` — so a new variant breaks every downstream
`match`. §II.10 prices that trade openly; the lane-local enums buy the same
typing without it.

### (b) The wire reason code

`ChannelAccessError` carries `reason_code()` in the shape the repo already
uses — the `ProtocolError` precedent at
`crates/liminal/src/protocol/error.rs:56-72` (stable associated `u16` consts) and
`:76-88` (a `const fn reason_code` matching each variant to its const),
consumed at `apply.rs:364` and `:375`.

**RULED — the consts live in `liminal-protocol`** (L1 granted). Two values,
additive public consts on that crate; no new enum variant anywhere, so nothing
breaks:

```rust
// liminal-protocol
/// The named channel is not on the server's roster.
pub const CHANNEL_NOT_REGISTERED_CODE: u16 = 0x0101;
/// The named channel is quiesced; its reason travels in the frame's message.
pub const CHANNEL_QUIESCED_CODE: u16 = 0x0102;
```

```rust
// liminal-server
impl ChannelAccessError {
    #[must_use]
    pub const fn reason_code(&self) -> u16 {
        match self {
            Self::NotRegistered { .. } => CHANNEL_NOT_REGISTERED_CODE,
            Self::Quiesced { .. }      => CHANNEL_QUIESCED_CODE,
            Self::RosterUnavailable { .. } => SERVER_ERROR_CODE,
        }
    }
}
```

`RosterUnavailable` keeps `SERVER_ERROR_CODE` (`apply.rs:26`): it is an internal
fault, not a statement about the channel. `CHANNEL_QUIESCED_CODE` stands (L2
granted) — without it a quiesced publish falls back to the undifferentiated
`0xFFFF` and the grep problem simply reopens for the second state.

**Why liminal-protocol and not liminal-server.** `liminal-sdk` — the crate a
wire client actually uses — depends on `liminal-protocol` and NOT on
`liminal-server` (`crates/liminal-sdk/Cargo.toml`, `[dependencies]`). A code
minted server-side is a code every SDK client hardcodes as a literal. The stack
lead added the sharper form of the same observation: the SDK sees the **liminal**
crate only *optionally* (`liminal = { workspace = true, optional = true }`, same
manifest), so `ProtocolError`'s home is not a shared surface either — which is
one more reason the new codes belong in liminal-protocol rather than beside the
codes they sit next to numerically. Part I's closing paragraph read the wire
protocol as untouched; that read is right about *frames* and wrong about
*vocabulary*.

**CONDITION on L1 — the band map rides the same commit.** Two crates minting
`u16` reason codes with no shared registry collide someday, and the collision is
silent: a client reads a number that means one thing to the crate that sent it
and another to the crate that documented it. The commit that adds the consts
records, beside them, the complete band map:

| Band | Owner | Currently minted in |
| --- | --- | --- |
| `0x0000–0x00FF` | protocol layer — parse, negotiation, auth | `ProtocolError`'s consts, `crates/liminal/src/protocol/error.rs:56-72` (`0x0001`–`0x0009` in use) |
| `0x0100–0x01FF` | server layer — channel-roster refusals | `liminal-protocol` (this lane; `0x0101`, `0x0102` in use) |
| `0xFFFF` | undifferentiated server error | `SERVER_ERROR_CODE`, `apply.rs:26` — the complete site census is `:254`, `:432`, `:456`, `:515`, `:551`, `:629`, `:710` |

The map is the fence. It is not documentation of a decision already safe; it is
the thing that makes the decision safe, which is why it may not land in a later
commit.

**Why the roster band is not inside `ProtocolError`'s.** A name that is not
registered is an application-layer statement about server state, not a parse,
negotiation, or auth failure. Putting it in the protocol band would make the
band mean nothing.

### (c) The probe

`channel_status` (§II.2) is the third leg. It answers `NotRegistered` /
`Active{…}` / `Quiesced{reason,…}` with no publish, no subscribe, and no actor
contact. The carrier-waits protocol backs off on `NotRegistered`, stops on
`Quiesced`, and proceeds on `Active`; each is a distinct constructor, so the
consumer's truth stream reports which one it saw without a string in sight.
`registered_channels` (§II.2) is its census companion: the probe answers about a
name the caller already suspects, the enumerator answers about names the caller
does not.

### (d) Crossing the trait boundary — the admission probe

**Finding F-v1-1 (stack lead, verified at both seats): §II.5(b) cannot be
applied as this design originally wrote it.** `apply.rs` does not receive a
`ChannelAccessError`. It receives a `ServerError`, because that is what the
PUBLIC `ConnectionServices` trait returns — `publish` at `services.rs:169-174`
and `subscribe` at `services.rs:189-194`, both
`Result<_, ServerError>` — and the trait is implemented outside this file
(`worker_front_door.rs:56`, with its own `publish` at `:57-71` and `subscribe`
at `:73-82`). Three routes were considered and two are barred:

- Adding a `source` or a code field to `ServerError::ListenerAccept`
  (`error.rs:27-28`) is exactly as breaking as a new variant on an exhaustive
  public enum. Barred (§II.10, Path B).
- Recovering the code by inspecting the error's *message* inside a helper is the
  grep defect wearing a type's coat. Barred by name.

**RULED — the ADMISSION-PROBE-BEFORE form.** One additive, defaulted method on
the trait:

```rust
/// Which channel operation is asking. `Copy`, so the hot path allocates
/// nothing to ask (contrast `ServerError::UnsupportedOperation`'s owned
/// `operation: String`, error.rs:104-110 — that one is built on a refusal
/// path, this one is consulted on every frame).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChannelOperation { Publish, Subscribe }

pub trait ConnectionServices: std::fmt::Debug + Send + Sync {
    // … existing methods unchanged …

    /// Whether `channel` admits `operation` right now.
    ///
    /// Consulted by the connection process BEFORE the operation is delegated,
    /// so a roster refusal is typed at the moment of the decision and carries
    /// its own wire reason code. The default admits everything: an adapter
    /// with no roster has nothing to say here, and its own refusals travel as
    /// service errors exactly as they do today.
    ///
    /// # Errors
    /// Returns [`ChannelAccessError`] when the roster refuses the operation.
    fn admit_channel(
        &self,
        operation: ChannelOperation,
        channel: &str,
    ) -> Result<(), ChannelAccessError> {
        let _ = (operation, channel);
        Ok(())
    }
}
```

**No implementor breaks.** The default body returns `Ok(())`, so every existing
`impl` — in-repo and downstream — compiles untouched. The trait already carries
two defaulted methods of exactly this kind (`participant_service` →`None` at
`services.rs:157-159`, `supports_channel_operations` → `true` at `:246-248`), so
this is the file's own established shape for additive trait surface, not a new
device.

**Who overrides.** `LiminalConnectionServices` overrides it with the real roster
read — one §II.4-priced lookup: lock acquire/release, `Arc` clone or state load,
poison branch. `WorkerFrontDoorServices` inherits the default `Ok(())` and that
is correct, not an oversight: its refusals are **profile** statements, not
**roster** statements. It has no roster to consult, and its `publish`/`subscribe`
already refuse with `UnsupportedOperation` text through the service error path
(`worker_front_door.rs:57-71`, `:73-82`). Making a profile refusal wear a
roster reason code would be its own typed lie.

**How `apply.rs` uses it.** `publish_response` (`apply.rs:408-436`) and
`subscribe_response` (`apply.rs:438-519`) consult `admit_channel` first:

- **Refused** → emit `PublishError`/`SubscribeError` carrying the
  `ChannelAccessError`'s own `reason_code()` (`0x0101`/`0x0102`) and its own
  reason text, and **do not call the service at all**. No publish is attempted
  against a quiesced channel; no subscribe reaches the actor.
- **Admitted** → proceed exactly as today. If the service then returns `Err`,
  the frame carries today's EXACT bytes: `SERVER_ERROR_CODE`
  (`apply.rs:26`, sites `:432` and `:515`) and the preserved message string.

Degraded, never lying. That is the whole shape of it.

**Ground 1 — why not query the roster in the `Err` arm.** Classifying after the
fact means re-reading the roster AFTER an opaque failure has already happened.
A publish that failed in the dedup bridge (`services.rs:975-986`), in the actor
(`:988-992`), or in schema negotiation (`:1053-1058`) — on a channel that was
CONCURRENTLY quiesced — would come back from that second read as "quiesced" and
be stamped `0x0102`. The wire would then carry a typed, confident, wrong cause.
The probe-before form cannot lie by construction: the code is emitted at the
moment of the admission decision, by the component that made it, from the value
it decided on. There is no second read to disagree with the first.

**Ground 2 — the price, said aloud: the happy path reads the roster TWICE.**
Once at admission in `apply.rs`, and once inside the service's own lookup
(`services.rs:963`, `:1048`), which STAYS. It stays because it is the guard at
the library boundary: `LiminalConnectionServices::publish` is public and callable
without going through `apply.rs` at all, and a guard that only exists in the
caller is not a guard. This is defense in depth, not redundancy to be optimised
away, and a later reader who deletes the inner check as "already done upstream"
will have removed the only check that holds for direct callers. The cost is the
§II.4 read, doubled — two lock acquire/release pairs and two `Arc` touches on a
path that already performs a haematite dedup claim and an actor round-trip. Same
framing as §II.4: nanoseconds against actor hops. Small, real, and named.

**Ground 3 — the attribution window, said aloud.** Admission and delegation are
not atomic; the roster can move between them. A publish that passes admission and
is then refused by the service — because quiesce landed in between — surfaces as
`0xFFFF` plus the preserved string, not as `0x0102`. **A reader must not assume
every roster refusal carries the new codes.** The codes are a reliable *positive*
signal (a `0x0101` always means the roster said so) and NOT a complete one
(`0xFFFF` does not prove the roster was fine). This is the same candour §II.3
owes about the quiesce race, and it has the same root: the design refuses to hold
a lock across the library boundary, and pays for that refusal in windows it names
rather than windows it hides.

**Ground 4 — why this does not contradict §II.2.** §II.2 keeps *registration*
vocabulary off the trait. That objection was about authority-moving APIs on a
profile that owns no channels. Access refusal is different in kind: it is
hot-path vocabulary the worker profile already answers, via
`supports_channel_operations` (`services.rs:246-248`). The stack lead ruled the
distinction explicitly, and it is recorded here so a later reader does not read
§II.2 and §II.5(d) as an inconsistency the design failed to notice.

## II.6 — Identical configuration: the compared field set

Registering a name that already exists compares exactly three fields. All three
must match for `Ok(Registered::AlreadyIdentical)`; the first mismatch returns
`AlreadyRegistered { name, field }` naming it.

| # | Field | Type & equality | Ground |
| --- | --- | --- | --- |
| 1 | **mode** | `liminal::channel::ChannelMode`, derives `PartialEq` (`types.rs:59-65`) | The `durable` bit selects `ChannelMode::Durable` vs `Ephemeral` at `services.rs:349-353` and selects the constructor at `:354-368`. Read back off the live entry as `handle.config().mode` (`types.rs:249-251`), the same accessor `flush_durable_state` uses (`services.rs:1152`). |
| 2 | **protocol schema id** | `liminal::protocol::SchemaId`, derives `PartialEq`/`Eq`/`Hash` (`crates/liminal/src/protocol/envelope.rs:7-8`) | Stored on the entry (`services.rs:1170`, set at `:373`), content-addressed from the RAW schema bytes by FNV-1a spread over 32 bytes (`services_schema.rs:38`, `:46-59`). This is the id advertised to subscribers at `services.rs:1054`, so a change here changes what every future subscriber negotiates. |
| 3 | **schema document** | `serde_json::Value`, `PartialEq` | The parsed document fed to `Schema::new` (`services.rs:344-348`), read back as `handle.config().schema.definition()` (`crates/liminal/src/channel/schema.rs:75`, field at `:42`). |

**Why field 3 exists when field 2 already covers the schema.** The protocol id
is a 64-bit FNV-1a digest spread across 32 bytes (`services_schema.rs:46-59`,
`:63-71`) — non-cryptographic, and only 64 bits of entropy however wide the id
is. Comparing the parsed document too means a digest collision cannot silently
accept a different schema as identical. Fields 2 and 3 also fail in opposite
directions on purpose: two byte sequences that parse to the same `Value` but
differ in whitespace produce DIFFERENT ids, and that must refuse — because the
id is on the wire.

**Why `Schema` itself is not compared.** `liminal::channel::Schema` derives only
`Clone, Debug` (`schema.rs:39-45`). It is not `PartialEq`, and its `SchemaId`
(`schema.rs:69`, a fresh `Uuid` per `Schema::new`, `schema.rs:15`, `:53`) is
identity, not content — two schemas built from identical bytes carry different
ones. Comparing it would refuse every idempotent re-registration.

**What is deliberately NOT compared, and why:**

- **The name.** It is the roster key. Identity by name alone is precisely the
  failure the round forbade; it is a precondition of the comparison, not a
  member of it.
- **The supervisor.** `cluster.supervisor().clone()` (`services.rs:358`, `:367`)
  is ONE server-wide supervisor built once at `services.rs:336` — SRV-005
  requires every channel to share it (`types.rs:217-223`). It cannot differ
  between two registrations in one process, so comparing it is theatre.
- **The durable store.** `Arc::clone(&durable_store)` (`services.rs:357`) is
  likewise the one store built at `services.rs:296`. Same argument.
- **`schema_ref`.** A config-file path resolved relative to the config file
  during validation (`config/types.rs:90-102`). It is not a runtime input at
  all; `ChannelRegistration` carries bytes.
- **`origin`.** See §II.7.

**RULED — the origin never flips** (L4 granted). An identical registration
against a `BootConfigured` entry returns `AlreadyIdentical` and the entry stays
`BootConfigured`. Flipping it would make the entry lie about its restart fate
(§II.7): it survives restart because the config file still lists it, whatever a
runtime call said about it. It also matters for the cap: a flip would move an
entry into the counted population (§II.8) without a channel having been created.

## II.7 — Boot config as a seed; the restart contract

**Boot-configured and runtime-registered channels ARE distinguishable**, via
`ChannelOrigin` on the entry and reported by `channel_status`. Behaviourally
they are identical in every other respect: same construction function (§II.2),
same shared supervisor, same shared store, same refusal semantics, same shutdown
flush. The origin tag exists for exactly one reason — **it is the only field
whose value predicts what a restart does** — and a projector that cannot ask
"which of these must I re-register?" cannot verify its own projection. That is
worth one byte per entry.

**The restart contract:** *a runtime-registered channel does not survive a
process restart.* Nothing persists the roster. The map is rebuilt from
`config.channels` alone (`services.rs:337-376`), so after a restart every
`RuntimeRegistered` entry is simply absent, and publish/subscribe on its name
refuse with `NotRegistered` exactly as they would for a name that never existed.
This is the *expected* contract: layer 3's projector re-projects from its record
on every boot, so the roster is derived state and rebuilding it is the normal
path, not a recovery path.

**The half that survives, said aloud.** For a *durable* runtime-registered
channel, the roster entry is gone but the DATA is not:
`new_durable_with_supervisor` recovers each partition's next-sequence counter
from the store at construction (`types.rs:234-245`, documented at `:194-198`).
Re-registering the same name with the same config after a restart therefore
resumes that channel's log where it left off rather than conflicting at sequence
zero. The roster is ephemeral; the log is not. A reader who takes "gone after
restart" to mean "the data is gone" has it wrong, and would double-write.

Pinned by `runtime_registered_channels_are_absent_after_restart` — §II.9, test 1.

## II.8 — Idle cost

**Per-channel idle cost, from the actor implementation:**

- **Registered, never used: zero processes, zero threads, zero timers.**
  `ChannelHandle::with_supervisor` (`types.rs:181-187`) stores a
  `ChannelActorState` whose `core` is an empty `OnceLock` (`types.rs:96`); the
  actor is spawned by `ChannelActorState::core` (`types.rs:127-142`) on first
  use. Memory: one `String` key, one `Arc<ConfiguredChannel>` allocation, a
  `ChannelConfig` (name `String`, a `Schema` = three `Arc`s and a `SchemaId`,
  `schema.rs:40-45`; a `ChannelMode`), an `AtomicU8`, an empty `OnceLock`, an
  `AtomicU32`, and a `ProtocolSchemaId` (32 bytes,
  `protocol/envelope.rs:12`) — hundreds of bytes, not kilobytes, plus the schema
  document and compiled validator shared behind their `Arc`s.
- **After first use: exactly one beamr process on the SHARED supervisor
  scheduler** (`supervisor.rs:205-216`; the supervisor is the single one built at
  `services.rs:336`) plus one `ChannelActorCore` — a `Schema`, a subscriber
  `Vec`, a closed flag, a command `VecDeque`, a pid slot, a restart lock, and a
  counter (`crates/liminal/src/channel/actor/mod.rs:47-62`, `:72-84`).
- **An idle actor does no work.** It is a `trap_exit` bytecode process whose
  single NIF either drains one queued command or handles a trapped
  `{EXIT, pid, reason}` from a dead subscriber
  (`actor/mod.rs:1-19`). It is delivery-driven and EXIT-driven: no timer, no
  poll, no keepalive. An idle channel costs scheduler occupancy, not scheduler
  cycles.
- **Registration-time cost, not idle cost, for durable channels:**
  `new_durable_with_supervisor` recovers per-partition sequence counters from
  the store, which is O(stream length) in store reads (`types.rs:194-198`,
  `:234-245`). A durable `register_channel` over a long-lived stream is a
  measurably slow call. It is on the registering host's thread, not on any
  connection's path.

**What bounds the aggregate: `limits.max_channels`, declared by the operator.**
`config.channels` is operator-authored and finite; a runtime API removes that
bound, so v1 restores it explicitly (L5 ruled — this was the largest gap in
design v1 and it closes here).

```rust
pub struct LimitsConfig {
    // … the existing eight caps, unchanged …
    /// Runtime-registered channels this deployment admits. NO DEFAULT: a
    /// deployment that registers channels at runtime declares its own bound.
    pub max_channels: Option<usize>,
}
```

**`Option`, with no default value.** Every other cap on `LimitsConfig` carries a
`#[serde(default = "…")]` and a documented §5 constant
(`crates/liminal-server/src/config/types.rs:286-320`, constants at `:324-338`),
because each of those numbers is derived from a signed §5 bound. There is no such
bound for channel count, and the estate bars inventing one. Nor may the field be
`usize` with a large default — unbounded-by-default is not a bound, it is the
gap wearing a number.

**Absent ⇒ registration refuses, typed.** `register_channel` with no
`limits.max_channels` configured returns
`ChannelRegistryError::CapNotConfigured { cap: "limits.max_channels" }`
(§II.5(a)), carrying the key so the operator is told what to declare rather than
that something is missing. A host that wants runtime registration declares its
bound; a deployment that never registers loses nothing and needs no config
change, so this is not a compatibility break for any existing config file.

**Present ⇒ enforced, in the existing shape.** Past the cap,
`CapReached { cap, limit }` — modelled on `ServerError::ConnectionCapReached`
(`error.rs:189-197`), which carries the `limits.*` key name and the configured
value for exactly this reason, and matching how the per-connection subscription
cap already refuses (`apply.rs:452-466`). A configured `Some(0)` is a config
validation error, joining the eight caps already checked non-zero by
`LimitsConfig::collect_errors` (`config/types.rs:344-375`) under the rule that
"a zero cap gates nothing — the unlimited-by-silence state §5 outlaws".

**The counting predicate, stated: the cap bounds `RuntimeRegistered` entries
ONLY.** Boot-configured channels are the operator's own authored bound — they are
in the file the operator wrote — and counting them would make the same number
mean two different things depending on how the deployment was configured. A cap
whose population is ambiguous is a count-domain defect, so the population is
named here and not left to the implementation: `count(entries where origin ==
RuntimeRegistered) < max_channels` is the admission predicate, evaluated under
the write lock that performs the insert (§II.4), so the check and the insert
cannot race. §II.6's rule that the origin never flips is what keeps this
population well-defined over time.

Pinned by `registered_idle_channels_spawn_no_actor` — §II.9, test 3.

## II.9 — The three pinned tests

All three fixtures configure `limits.max_channels` (§II.8). Registration refuses
`CapNotConfigured` without it, so the cap is a precondition of every test that
registers anything — stated once here rather than rediscovered three times.

### Test 1 — the restart contract

`runtime_registered_channels_are_absent_after_restart`

1. Build services over a config listing one boot channel `boot`, with a
   `persistence_path` in a `tempdir` and `limits.max_channels = Some(4)` (§II.8
   — without it every `register_channel` in this test refuses
   `CapNotConfigured`, so the cap is part of the fixture, not scenery), via
   `from_config_with_store` (`services.rs:311`).
2. `register_channel` a durable `runtime`. Publish to both; both succeed.
3. Drop the services (running the durable flush, `services.rs:1150-1164`).
4. Rebuild from the SAME config and the SAME store path.
5. Assert: `channel_status("boot")` is `Active{origin: BootConfigured, …}`;
   `channel_status("runtime")` is `NotRegistered`; a publish to `runtime`
   refuses with `ChannelAccessError::NotRegistered` and reason code
   `CHANNEL_NOT_REGISTERED_CODE`.
6. **Positive control** (without it the test cannot tell a real restart contract
   from a broken rebuild): re-`register_channel` `runtime` with the identical
   spec on the rebuilt services; assert `Registered::Created`, assert the publish
   now succeeds, and assert the durable stream RESUMED rather than restarted —
   the §II.7 half that survives.

### Test 2 — the quiesce race

`quiesce_admits_a_subscribe_that_read_active`

The interleave is pinned at the linearisation point, not by racing threads. Two
threads and a sleep can pass by luck and would prove nothing about which point
is the decision; this exercises the decision directly.

1. Register `orders`; call the admission funnel for a subscribe and hold the
   admitted `Arc<ConfiguredChannel>` — this is the "read Active" step, made
   explicit.
2. `quiesce_channel("orders", "archived")` → `Ok(())`.
3. Complete the subscribe from the held entry
   (`handle.subscribe_with_install`, `types.rs:424`). Assert it SUCCEEDS.
4. Assert the resulting stream is live: publish through the handle directly and
   assert the subscriber receives it — proving "keeps their stream" is delivery,
   not just a non-error.
5. **Both directions.** A subscribe admitted AFTER the quiesce refuses with
   `ChannelAccessError::Quiesced { reason: "archived", .. }`; a publish through
   the ordinary `ConnectionServices::publish` path (`services.rs:956`) refuses
   the same way. Without this arm, step 3 passing would be consistent with
   quiesce doing nothing at all.

### Test 3 — idle cost, keepalive-honest

`registered_idle_channels_spawn_no_actor`

1. Register 16 channels; touch none of them.
2. Register one control channel; publish M messages to it with one subscriber
   attached.
3. Assert the **unrelated counters grow**: the control channel's
   `PublishOutcome.delivered` is true for each publish, its subscriber received
   M envelopes, and the process-wide `publish_accepted` counter
   (`crates/liminal-server/src/metrics.rs:64`, called at `services.rs:1031`)
   advanced by M.
4. Assert the **unit's counters stay flat**: each of the 16 idle channels has no
   spawned actor.

**The instrument does not exist yet, and step 4 is why.** Every public
`ChannelHandle` accessor that could answer "is there an actor?" —
`subscriber_count` (`types.rs:529-531`), `close` (`types.rs:538-540`) — routes
through `core()` (`types.rs:543-545`), which SPAWNS one. Observing the property
would destroy it.

**RULED — `ChannelHandle::is_actor_spawned(&self) -> bool` on the liminal
crate** (L6 granted): new public surface on liminal (0.5.1), additive, minor.
It cannot be `#[cfg(test)]` — liminal-server's tests cannot see liminal's
test-gated items — so it is a real API, and the ruling attaches three conditions
that keep it one:

1. **Its doc-comment states that it NEVER spawns, and why it exists.** The point
   is spawn-state observability: an actor that materialises lazily
   (`types.rs:127-142`) has an observable spawned/unspawned state, and a library
   that offers no way to read it forces every consumer to choose between not
   knowing and destroying the thing it wanted to know. That is a genuine gap in
   the handle's surface, not test theatre with a public modifier on it.
2. **It reads `ChannelActorState.core.get().is_some()` (`types.rs:96`) and
   touches nothing else.** No `core()`, no supervisor call, no `ensure_running`
   (`supervisor.rs:227`) — that last one matters, because `ensure_running`
   RESTARTS a dead actor, so a naive implementation would not merely observe
   the state but repair it.
3. **Test 3 keeps both arms.** The grow arm (step 3) and the flat arm (step 4)
   are one instrument; either alone proves nothing. A flat arm with no grow arm
   cannot distinguish "the idle channels spawned nothing" from "the harness
   measured nothing at all".

It means **this lane touches the liminal library crate, not only
liminal-server** — a real widening of the cut's blast radius, priced in §II.10.

**The gap this test does NOT close.** It bounds *spawn by registration*. It does
not census scheduler occupancy, because no OS-level process census is available:
`services.rs:1330-1332` records that beamr's scheduler-inventory API is on that
project's branch and not yet consumable from liminal. When it lands, this test
gains the stronger assertion; until then the honest claim is "registration
spawns nothing", not "N registered channels occupy nothing".

## II.10 — Semver pricing for the cut

**Decided at the cut, not here.** What follows is the trade laid out.

**Path A — the one this design takes.** New public types and inherent methods on
`LiminalConnectionServices`, two lane-local error enums, additive `u16` consts,
no signature changes, no `ConnectionServices` trait change, and
`ConfiguredChannel` stays private (`services.rs:1167`) so its new fields are not
public surface. On liminal-server (0.5.1) this is purely additive: **minor**.

Three things Path A must watch, each of which could quietly make the number
dishonest:

1. **The refusal STRING.** Today an unknown channel yields
   `ServerError::ListenerAccept` whose message is
   `channel '<name>' is not configured` (`services.rs:967`, `:1051`), and the
   connection process puts that text on the wire (`apply.rs:433`, `:516`). Any
   consumer distinguishing this case does so by grepping it — that is the exact
   defect this lane fixes. **Recommendation: preserve that text byte-for-byte**
   and put the discrimination entirely in the reason code. Then no string
   consumer breaks, the fix is purely additive, and the number stays minor.
   Changing the wording buys nothing and costs a behavioural break.
2. **The wire reason code.** `reason_code` for a not-registered publish or
   subscribe moves from `0xFFFF` to `0x0101`. `0xFFFF` means "some server error"
   (`apply.rs:26`), so no correct client can have keyed on it for this case —
   but it is wire-observable and must be named in the release notes, not
   discovered.
3. **The `admit_channel` trait method.** Additive with a default body
   (§II.5(d)), so no implementor breaks and it stays inside "minor". It is the
   one place where a mistake would not: a method added *without* a default body
   breaks every downstream implementor of a public trait, silently converting
   this into Path B's price by a different route.

**RULED — this is a THREE-CRATE cut** (L1), each leg additive and each priced at
its own cut:

| Crate | Version now | What this lane adds | Honest number |
| --- | --- | --- | --- |
| `liminal-server` | 0.5.1 | registration/quiesce/probe/enumerator APIs, two lane-local enums, `admit_channel` (defaulted), `limits.max_channels` | minor — additive |
| `liminal-protocol` | 0.4.0 | two `pub const u16` reason codes + the band map (§II.5(b)) | minor — additive, no enum variant |
| `liminal` | 0.5.1 | `ChannelHandle::is_actor_spawned` (§II.9) | minor — additive |

Three additive-minor legs is a wider cut than design v1 assumed, and the width
is the honest consequence of two findings — the SDK cannot see liminal-server,
and the spawn state cannot be observed without destroying it — rather than scope
creep. Each leg is separately releasable; none of them is a break.

**Path B — rejected, priced openly.** A typed `ServerError::ChannelNotRegistered`
variant. `ServerError` is public and exhaustive: no `#[non_exhaustive]` at
`error.rs:6-7`. Adding a variant breaks every downstream `match` that names its
variants — major-class on a `0.x` crate, i.e. the minor is the breaking
boundary and liminal-server moves to a new breaking series. It buys nothing the
lane-local enums do not: the refusal is already typed at the API boundary and
already differentiated on the wire. Declined, and declined in the open, because
"we did not add a `ServerError` variant" is only honest if the alternative was
weighed rather than avoided.

**RULED — no `#[non_exhaustive]` on the new enums** (L7 granted explicitly).
The estate's recorded refusal of the attribute covers the 268 public enums in
`crates/liminal-protocol/src` (`docs/gates/EXHAUSTIVENESS-REFUSAL.md`, Cally Ray,
2026-08-03), on the ground that causes must travel by type and a forced `_ =>`
arm un-makes that discipline.

Its *scope* is liminal-protocol, so it did not decide these enums by itself. The
stack lead **judged its grounds to extend here**, and the judgement is recorded
rather than assumed: the ground — a wildcard arm past a future variant is a
silent policy change that will compile, will run, and will route an unconsidered
cause into whichever branch the wildcard happened to land in — applies
identically to a carrier matching on `ChannelAccessError`. That carrier is
exactly the consumer whose backoff turns on the discrimination this lane exists
to provide.

This is the judged-not-blanket shape the refusal document reserves for itself: a
named person extended named grounds to a named new surface, on the record, at the
moment the surface was designed. It is not the attribute being refused everywhere
by default, and a future enum elsewhere in the estate inherits the grounds, not
the verdict.

## II.11 — What v1 does not do

- **No channel removal.** Quiesce is the ruled floor and v1 stops there. Removal
  of even a quiesced, subscriber-free channel is a new class of race (a name
  freed while a subscribe is in flight against it) and buys layer 3 nothing that
  quiesce plus restart does not.
- **No un-quiesce.** `Quiesced` is terminal in-process. This matches
  BUS-FOLLOWS-RECORD's one-way archive on the record, and it means the state
  byte is written once, by CAS, which is why the state machine needs no lock.
  Un-archiving is a restart plus a re-registration.
- **No wire-level admin surface — REFUSED BY NAME.** There is no frame, no
  reason code, and no code path by which a connected client registers or
  quiesces a channel. Registration is a Rust API on an in-process handle. The
  authentication consequences a wire surface would carry (adjacent: the
  `SubscriptionStream` auth-token defect, task #2) are consequences v1 does not
  take on. Part I's Q2 — "who may register?" — is therefore answered "whoever
  holds the `Arc<LiminalConnectionServices>`", which is the host process, and it
  reopens with teeth the day a wire surface is proposed.
- **No per-channel authorisation.** No ACL, no ownership, no quota per
  registrant. Every holder of the handle is fully privileged.
- **No aggregate cap that the SERVER invents.** `limits.max_channels` is ruled
  (§II.8) but it is `Option` with no default: liminal does not choose a number,
  it requires the operator to. A deployment that declares nothing cannot
  register at runtime — which is a refusal, not an unbounded roster.
- **No schema evolution through registration.** `ChannelHandle` can evolve a
  live schema (`types.rs:391`, `schema.rs:117`), and re-registering with
  different schema bytes REFUSES (§II.6) rather than evolving. Registration
  creates channels; it does not modify them. The two mechanisms are unrelated in
  v1, and the comparison in §II.6 reads the AS-REGISTERED document
  (`handle.config().schema`, unaffected by evolution) so an evolved channel does
  not start refusing its own original config.
- **No metrics.** No per-channel gauge, no registration counter. The existing
  metrics are process-wide (`metrics.rs:48-73`) and v1 adds none.
- **`admit_channel` is not an admin surface.** It is a trait method the
  connection process calls on the way to a publish or a subscribe. No frame
  reaches it; no client invokes it. The wire-admin refusal above is unaffected
  by it.

*(Design v1 also proposed omitting the roster enumerator. That omission was
OVERRULED — `registered_channels()` is in v1; see §II.2.)*

## II.12 — The ruling record

Design v1 was ACCEPTED at the design round (2026-08-03) with one structural
finding and all eight LEANs ruled. Nothing in Part II is open. The register below
is the record; each ruling's substance lives in its own section and is not
restated here.

### The structural finding

**F-v1-1 — the typed refusal could not cross the trait boundary as written.**
Raised and verified at both seats. `apply.rs` receives `ServerError` through the
public `ConnectionServices` trait (`services.rs:169-174`, `:189-194`), so
§II.5(b)'s wire code had no route to the frame. RESOLVED by the
admission-probe-before form, ruled by the stack lead as seam owner and written up
as decided in **§II.5(d)**, together with the four grounds: why not the `Err`-arm
query, the doubled roster read on the happy path, the attribution window, and why
§II.2's placement objection does not carry.

### The eight LEANs, ruled

| # | Subject | Ruling | Now in |
| --- | --- | --- | --- |
| L1 | Reason-code consts' home | **GRANTED** — `liminal-protocol`; three-crate cut accepted, each leg additive-minor. **CONDITION:** the three-band map rides the SAME commit as the consts | §II.5(b), §II.10 |
| L2 | `CHANNEL_QUIESCED_CODE` `0x0102` | **GRANTED** — stands | §II.5(b) |
| L3 | Different-reason re-quiesce | **GRANTED** — refuses `AlreadyQuiesced`; identical reason is idempotent | §II.3, §II.5(a) |
| L4 | Origin on identical re-registration | **GRANTED** — never flips | §II.6 |
| L5 | Aggregate cap | **RULED — THE GAP CLOSES.** `limits.max_channels`, `Option`, no default; absent ⇒ `CapNotConfigured`; population = `RuntimeRegistered` entries only | §II.5(a), §II.8 |
| L6 | `is_actor_spawned` | **GRANTED** — additive minor on liminal, under three conditions | §II.9 |
| L7 | `#[non_exhaustive]` | **GRANTED EXPLICITLY** — none. The refusal doc's grounds JUDGED to extend here; a judged extension on the record | §II.10 |
| L8 | Roster enumerator | **OVERRULED** — `registered_channels()` is IN v1: a by-name probe can never detect an unexpected extra | §II.2, §II.11 |

Two of these changed the design rather than confirming it. L5 closed what this
document called its own largest gap, and did it by refusing to invent a number —
the cap is required, not defaulted, so a deployment that wants runtime
registration states its own bound and one that does not is unaffected. L8
reversed an omission whose only justification had been a smaller diff, on the
census ground that verification without a population denominator is not
verification. Both are recorded here as reversals, not as things the design got
right, because a design document that quietly absorbs its own corrections stops
being evidence of anything.
