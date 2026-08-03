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

**Hermes Crumpet, 2026-08-03. Status: DESIGN v1 — written under the design
round's rulings (stack lead's gate, 2026-08-03). Every file:line below was read
at `96e342b` in the lane worktree before it was written down. Anything the round
did not rule is marked LEAN.**

Part I's open questions Q1–Q5 are closed by the round; Q6 (the version number)
stays open by design and §II.10 prices it rather than answering it. Part II adds
what Part I refused to invent: signatures, the state machine, the
synchronisation, the error vocabulary, and the tests that make the whole thing
falsifiable.

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

**Not on the `ConnectionServices` trait.** The trait is public and implemented
outside this file (`WorkerFrontDoorServices`). A required method breaks every
external implementor; a defaulted method puts channel-registration vocabulary on
a profile that serves no channels at all (`supports_channel_operations` →
`false`, `services.rs:246-248`). Inherent methods on the one adapter that owns a
channel map is the honest placement.

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
}
```

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
2. one `Arc` clone (one relaxed atomic increment) and its later decrement,
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
because they have disjoint call sites and only one of them reaches the wire.

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
consumed at `apply.rs:364` and `:375`:

```rust
impl ChannelAccessError {
    /// The named channel is not on the roster.
    pub const CHANNEL_NOT_REGISTERED_CODE: u16 = 0x0101;
    /// The named channel is quiesced.            [LEAN — see below]
    pub const CHANNEL_QUIESCED_CODE: u16 = 0x0102;

    #[must_use]
    pub const fn reason_code(&self) -> u16 { … }
}
```

`RosterUnavailable` keeps `SERVER_ERROR_CODE` (`apply.rs:26`): it is an internal
fault, not a statement about the channel.

**Band.** `0x0000–0x00FF` is the protocol layer's (`0x0001`–`0x0009` in use,
`protocol/error.rs:56-72`). `0xFFFF` is the server's undifferentiated
"something failed" (`apply.rs:26`, used at `:254`, `:432`, `:456`, `:515`,
`:551`, `:629`, `:710` — the complete census). `0x0100–0x01FF` is reserved here
for server-layer channel-roster refusals: a name that is not registered is an
application-layer statement about the roster, not a parse, negotiation, or auth
failure, so it does not belong in `ProtocolError`'s vocabulary.

**Where it is minted, and the finding that complicates it.** The consts above
sit in the new server module. But `liminal-sdk` — the crate a wire client
actually uses — depends on `liminal-protocol` and NOT on `liminal-server`
(`crates/liminal-sdk/Cargo.toml`'s `[dependencies]`). A code minted server-side
is therefore a code every SDK client hardcodes as a literal. Part I's closing
paragraph read the wire protocol as untouched; that read is right about *frames*
and wrong about *vocabulary*. **LEAN:** the honest home for the two consts is
`liminal-protocol`, as additive public consts (no new enum variant, so no break;
additive on a `0.4.0` crate). The round ruled the code's existence, not its
crate. Recorded for the cut, because it moves which crates the version question
touches.

**Where it is applied.** `publish_response` (`apply.rs:429-434`) and
`subscribe_response` (`apply.rs:512-517`) currently hardcode `SERVER_ERROR_CODE`.
Both learn to ask a `ChannelAccessError` for its code when the failure is one,
and keep `SERVER_ERROR_CODE` otherwise. This is the only change to `apply.rs`.

### (c) The probe

`channel_status` (§II.2) is the third leg. It answers `NotRegistered` /
`Active{…}` / `Quiesced{reason,…}` with no publish, no subscribe, and no actor
contact. The carrier-waits protocol backs off on `NotRegistered`, stops on
`Quiesced`, and proceeds on `Active`; each is a distinct constructor, so the
consumer's truth stream reports which one it saw without a string in sight.

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

**LEAN — re-registering a boot-configured channel.** An identical registration
against a `BootConfigured` entry returns `AlreadyIdentical` and the origin does
NOT flip to `RuntimeRegistered`. Flipping it would make the entry lie about its
restart fate (§II.7): it survives restart because the config file still lists
it, whatever a runtime call said about it. The round did not rule this.

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

**What bounds the aggregate: nothing, today.** `config.channels` is
operator-authored and finite; a runtime API removes that bound. **LEAN:** a
`limits.max_channels` cap on `LimitsConfig`, refusing registration past it with
a typed variant, following the `ConnectionCapReached` precedent
(`error.rs:189-197`) and the existing per-connection caps
(`apply.rs:452-466`). The round ruled that the aggregate must be bounded and did
not rule *whose* cap it is; until it does, v1's bound is host-side discipline,
which is not a bound. This is the largest LEAN in the document and the cut
should not close without an answer.

Pinned by `registered_idle_channels_spawn_no_actor` — §II.9, test 3.

## II.9 — The three pinned tests

### Test 1 — the restart contract

`runtime_registered_channels_are_absent_after_restart`

1. Build services over a config listing one boot channel `boot`, with a
   `persistence_path` in a `tempdir`, via `from_config_with_store`
   (`services.rs:311`).
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
would destroy it. **LEAN:** add
`ChannelHandle::is_actor_spawned(&self) -> bool` reading
`ChannelActorState.core.get().is_some()` (`types.rs:96`) without touching
`core()`. It cannot be `#[cfg(test)]`: liminal-server's tests cannot see
liminal's test-gated items. So it is new public surface on liminal (0.5.1),
additive, minor — and it means **this lane touches the liminal library crate,
not only liminal-server**. That is a real widening of the cut's blast radius and
belongs in §II.10's pricing.

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
3. **The liminal-side test seam.** §II.9's `is_actor_spawned` is additive public
   surface on liminal (0.5.1) — minor there, but it makes this a two-crate cut,
   and three-crate if the reason-code consts move to liminal-protocol (0.4.0,
   also additive, also minor).

**Path B — rejected, priced openly.** A typed `ServerError::ChannelNotRegistered`
variant. `ServerError` is public and exhaustive: no `#[non_exhaustive]` at
`error.rs:6-7`. Adding a variant breaks every downstream `match` that names its
variants — major-class on a `0.x` crate, i.e. the minor is the breaking
boundary and liminal-server moves to a new breaking series. It buys nothing the
lane-local enums do not: the refusal is already typed at the API boundary and
already differentiated on the wire. Declined, and declined in the open, because
"we did not add a `ServerError` variant" is only honest if the alternative was
weighed rather than avoided.

**LEAN — `#[non_exhaustive]` on the new enums.** Not ruled. The estate has a
recorded refusal of the attribute for the 268 public enums in
`crates/liminal-protocol/src` (`docs/gates/EXHAUSTIVENESS-REFUSAL.md`, Cally Ray,
2026-08-03), on the ground that causes must travel by type and a forced `_ =>`
arm un-makes that discipline. That refusal is scoped to liminal-protocol, so it
does not decide these enums — but its ground applies identically here: a carrier
that wildcards its way past a future `ChannelAccessError` variant is a silent
policy change waiting to happen. **Lean: no `#[non_exhaustive]`, consistent with
the recorded ruling.** The round should say so explicitly rather than let the
scope gap decide by default.

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
- **No aggregate cap.** See §II.8's LEAN. This is a gap, not a decision.
- **No schema evolution through registration.** `ChannelHandle` can evolve a
  live schema (`types.rs:391`, `schema.rs:117`), and re-registering with
  different schema bytes REFUSES (§II.6) rather than evolving. Registration
  creates channels; it does not modify them. The two mechanisms are unrelated in
  v1, and the comparison in §II.6 reads the AS-REGISTERED document
  (`handle.config().schema`, unaffected by evolution) so an evolved channel does
  not start refusing its own original config.
- **No roster listing on the probe.** `channel_status` answers about one name.
  A `registered_channels() -> Vec<ChannelDescriptor>` enumerator is obvious and
  cheap under §II.4's read lock, but the round did not ask for it and a
  projector reconciling by name does not need it. **LEAN: omit from v1.**
- **No metrics.** No per-channel gauge, no registration counter. The existing
  metrics are process-wide (`metrics.rs:48-73`) and v1 adds none.

## II.12 — Every LEAN in this document

Collected so the round can rule them in one pass:

1. §II.5(b) — the two reason-code consts belong in `liminal-protocol`, not
   liminal-server, because `liminal-sdk` cannot see liminal-server. Makes this a
   three-crate cut.
2. §II.5(b) — `CHANNEL_QUIESCED_CODE` (`0x0102`). The round ruled a distinct
   not-registered code; a quiesced publish would otherwise fall back to the
   undifferentiated `0xFFFF`, which re-opens the grep problem for the second
   state.
3. §II.5(a) — `AlreadyQuiesced` on a re-quiesce with a DIFFERENT reason;
   identical reason is idempotent success, mirroring registration.
4. §II.6 — an identical re-registration of a boot-configured channel does not
   flip its origin.
5. §II.8 — `limits.max_channels`. The aggregate is unbounded without it.
   **The largest gap in this design.**
6. §II.9 — `ChannelHandle::is_actor_spawned`, new public surface on the liminal
   crate, without which test 3's step 4 cannot be written at all.
7. §II.10 — no `#[non_exhaustive]` on the new enums, consistent with the
   recorded liminal-protocol refusal whose scope does not reach them.
8. §II.11 — no `registered_channels()` enumerator in v1.
