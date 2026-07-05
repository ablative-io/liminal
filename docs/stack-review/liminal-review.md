# liminal — Review

*State as of v0.2.2 (`liminal-rs` on crates.io, July 2026; pins beamr 0.11.0,
haematite 0.4.0). Verdict: the core is real and more sophisticated than the
README suggests, but several advertised guarantees are seams waiting for
their last wire.*

## The actual runtime model

Conversations and channels are genuine beamr processes — but **not** via the
high-level `Actor` API. Each is a hand-assembled bytecode module (a
LoopRec/CallExt receive loop whose single import is a `process_command_nif`)
running with `trap_exit`. The reason: only bytecode-side
`ProcessContext.link_facility()` can link to a *pre-existing* pid, and
linking to already-spawned participants/subscribers is the whole point.

The load-bearing pattern throughout the library: **payloads never travel
through beamr mailboxes locally.** Real data moves through shared
`Arc<Mutex<VecDeque>>` command queues; the owning process is woken with an
atom via `Scheduler::enqueue_atom_message`. (Exception: remote frames arrive
as beamr binary messages.) Participants and subscribers are `spawn_native`
`NativeHandler` processes. The only place the high-level `spawn_actor` API is
used is routing-function execution (supervised, panic→Crashed,
timeout→Kill).

Crash detection is the real thing: process links, trapped EXIT tuples, and a
dispatch layer that achieves sub-millisecond crash-to-reroute with a strict
**link-before-forward** invariant (exit notifiers register before send; the
register/signal race is closed under one state lock).

Consumer-group dispatch: each message spawns a fresh conversation actor with
`CrashPolicy::RouteToNext`, linked to the chosen consumer; an EXIT within a
250ms handoff window triggers re-selection excluding the crashed consumer.
Note: "delivered" = absence of EXIT for 250ms, not a positive ack.

## Durability (event-sourced onto haematite)

Behind an async `DurableStore` trait (production impl wraps
`haematite::EventStore`):

- **Channel messages**: per-partition streams (`"{channel}:{partition}"`),
  optimistic per-partition sequences, envelope codec.
- **Conversations**: per-step event log (`MessageReceived`,
  `ProcessingStarted`, `StepCompleted{step,output}`, …) replayed into state;
  redelivery resolves to `Skip` / `ResumeFrom(step)` / `Start` — a crashed
  handler resumes at the exact step, which is the redelivery-idempotency
  brain.
- **Consumer cursors**: CAS-backed offsets with an "absent == 0" contract
  (never store physical zero) and refuse-regression checkpoints; batched
  checkpoint policies.
- **Dedup**: claim/complete cache with tombstone release and TTL sweep;
  at-most-once receipt guard.

Ordering discipline is clean: in-memory state advances only after a
successful append — no partial-persist divergence on a single stream. There
is **no cross-stream transaction**: a channel append, cursor checkpoint, and
dedup completion are independent; crashes between them produce redelivery,
which the dedup/`RedeliveryDecision` layer is designed to absorb.

## Wire protocol

10-byte header (`type | flags | stream_id:u32 | payload_len:u32`, BE),
control frames on stream 0, forward-compatible unknown-frame skipping.
Frame types cover connect/subscribe/publish/conversation lifecycles, in-band
Accept/Defer/Reject, Ping/Pong, and the newer server-push inversion
(Push/PushReply) + worker registration. Version negotiation on handshake.
Schema at the wire layer is a 32-byte content hash compared for exact
equality — **actual JSON Schema validation lives in the channel layer**
(jsonschema crate, validate + apply-defaults, additive evolution without
subscriber disconnect).

## Server and clustering

The standalone server is production-shaped: accept loop → **one supervised
beamr native process per TCP connection**, graceful drain, separate health
endpoint with readiness gating, haematite-backed durable channels with
dedup-on-delivery. Clustering uses **beamr's distribution layer directly**:
each channel is a beamr pg process group; cross-node publish sends encoded
SEND frames to remote members; membership is **poll-based at 250ms**
(deliberate — beamr's single connection-down hook slot is owned by pg-purge);
node-down purge itself is instant via the beamr hook.

## Solid vs seam

**Production-shaped**: core channels (schema validation + defaults at
publish), conversations (phase machine, crash policies, `ask()` pattern),
consumer-group dispatch, the durability layer, the wire protocol, the server
including clustering, the Rust SDK's TCP transport (handshake, reconnect,
resume machinery, push client), the 3-way conformance harness, and the
freshly-landed aion observability drain (`aion.observability.v1` tap,
consumed by the connection notifier *before* fan-out — deliberately outside
all channel guarantees).

**Seams and stubs** (each is finished-looking API over an unbuilt core):

1. **Backpressure is not wired into the publish path.** The full
   Accept/Defer/Reject machinery exists — `CapacityTracker` decision rule,
   `PressureMonitor`/`PressureEnforcer` policy actions, in-band Defer frames
   with message ids — but `ChannelActorCore::apply_publish` fans out
   unconditionally. Nothing consults a tracker.
2. **Server-side schema validation is stubbed.** Channels are built with an
   empty schema (`Schema::new(json!({}))`); the config's `schema_ref` is
   loaded but never read. The validation engine exists and works — the
   server just doesn't feed it.
3. **The Gleam SDK cannot connect.** Its transport is declared as
   `@external(erlang, "liminal_sdk_ffi", …)` — and `liminal_sdk_ffi` does
   not exist anywhere (no rustler NIF, no .erl shim). Only the pure
   lifecycle/recovery state machines are live (and conformant).
4. **SDK embedded mode is a scaffold** — a no-op backend trait, not an
   embedding of the core actors.
5. **TS SDK is bring-your-own-transport** — it ships the real Rust protocol
   codec compiled to WASM (byte-identical framing), but no socket; wasm
   artifacts are built on demand, not checked in.
6. Routing rules are parsed from server config but no routing engine exists;
   the causal orderer is a composable stage nothing invokes automatically;
   server dedup is wired in-memory (not persisted across restarts).

**Docs caveat**: the design checklists under `docs/design/` show `done:false`
for work that visibly shipped — trust git history and source presence, not
the checklist JSONs. VISION.md is unusually honest (dated ⚠️ status notes on
every aspirational section).

## Latent traps

- **The durability sync bridge** (`block_on`, bounded at 8 polls) is only
  sound because `HaematiteStore` completes on the first poll (in-memory
  engine). A real-I/O haematite backend must replace the bridge or every
  durable path fails — loudly (`DidNotComplete`), but completely.
- **`CausalContext::child_of` silently truncates ancestry to one hop** —
  use `child_of_context` for transitive ordering. `happened_before` is
  explicit-chain membership, not vector clocks.
- **Cross-node delivery silently skips** remote pids that don't fit
  `Term::try_pid`'s immediate range (warn log only).
- Every subsystem **spins its own beamr scheduler by default**; cross-node
  delivery requires subscribers to live on the distribution-owning
  scheduler. Cluster sync spins a fresh tokio current-thread runtime per
  cross-node write (allocation footgun under fan-out load).
- Wire codec truncates timestamps to milliseconds; subscriber predicates are
  closures, so **remote publishes bypass predicate filtering**; conversation
  restart is unbounded while channel restarts are budgeted (asymmetric);
  default cluster cookie is `"beamr-cookie"`.
