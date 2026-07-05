# liminal — Full Domain Ledger

*The complete work list and opportunity map, beyond the Frame v1 critical
path. Companion to `liminal-review.md` (state assessment) and
`liminal-assets-pack.md` (pipeline toolkit). State as of v0.2.2, July 2026.
Sizing in briefs.*

## A. Finish-the-seam tier (machinery exists, last wire missing)

### A1. Backpressure wiring (S–M: 2–4 briefs) — on the Frame v1 path
Attach `CapacityTracker` to the publish path (`ChannelActorCore::
apply_publish` currently fans out unconditionally), define Defer
redelivery semantics (who buffers, who retries, when does Defer escalate
to Reject), and wire `PressureEnforcer` policy actions to something real.
The wire-level Defer frames and the decision rule both exist. **Unlocks:**
frame F-3a's acceptance criterion; honest protocol-level backpressure
instead of an aspirational README claim.

### A2. Server schema wiring (S: 1–2 briefs)
Read `schema_ref` from channel config, load the JSON Schema, build channels
with it (today: `Schema::new(json!({}))`). The validation engine
(jsonschema, defaults, additive evolution) works — the server just never
feeds it. **Unlocks:** schema-validated channels in server mode; closes a
README-vs-reality gap.

### A3. Routing engine (M: 3–5 briefs)
`routing_rules` parse from server config but no engine consumes them. The
predicate plane (`RoutingTable`, `Predicate`/`FieldPath`, compiler) exists
as a pure data structure. Wire config → table → the publish path.
**Unlocks:** source→target predicate routing at the server, which Mako-style
applications will want the moment two components share a bus.

### A4. Causal ordering v2 (M, design-first)
`CausalContext::child_of` silently truncates ancestry to one hop (use
`child_of_context` — documented but a live footgun); the `CausalOrderer`
enforces only direct-parent order and requires manual orphan pumping;
nothing invokes it automatically. Decide: promote to an opt-in channel
stage with automatic pumping, adopt vector clocks, or deprecate the
one-hop constructor. **Interacts:** frame conversation patterns; aion's
event ordering (which has its own total order — don't build two).

## B. Integration tier (decision-dependent, rulings #4/#5)

### B1. Embedded/frame-conv integration (M, jointly owned with frame)
If ruling #4 = embedded: frame-conv builds on the `liminal` core crate
sharing one beamr scheduler (`new_durable_with_supervisor` /
`with_distribution` exist; make shared-scheduler the blessed path with a
constructor that doesn't require knowing the internals). The SDK's no-op
embedded backend then stays a test seam, honestly labeled.

### B2. Gleam native surface (M: 3–6 briefs, ruling-dependent)
Gleam components on beamr should reach liminal in-runtime: a BIF/FFI
surface into the channel supervisor on the shared scheduler. The designed
NIF→TCP path (`liminal_sdk_ffi`, which does not exist) serves external
Gleam-on-OTP users — build it only if that audience materializes.
**Unlocks:** typed Gleam pub/sub for frame components with zero
serialization overhead (same process graph).

### B3. TS SDK transport (S–M: 2–3 briefs)
Ship a WebSocket transport + prebuilt wasm artifacts. The SDK is
API-complete with the real Rust codec compiled to wasm; it just has no
socket. **Interacts:** beamr's browser transport (C2 in the beamr ledger) —
if the mesh speaks WS, the TS SDK and the server should share framing
decisions. Do these two briefs against one design doc.

## C. Durability tier

### C1. Async-ready durability bridge (M, prerequisite-tracked)
The sync bridge (`block_on`, 8-poll bound) is only sound while
`HaematiteStore` completes on first poll. Before haematite grows a
genuinely-async backend (Apollo has this on his watchlist), replace the
bridge: either a dedicated durability thread with a submission queue, or
make the channel/conversation APIs honestly async. **This is the one item
that turns from "hygiene" to "everything is broken" the day the store
changes** — track it against haematite's roadmap, not liminal's.

### C2. Durability hygiene (S each)
Persist the server dedup cache (in-memory today — restart forgets
idempotency); snapshot/compaction story for long conversation logs
(recovery is O(log length), batched 1024); `resume` over TCP (currently
"re-subscribe to trigger replay").

## D. Cluster tier

- `on_peer_leave` only logs; coordinated cluster shutdown deferred
  (SRV-004) — both needed before multi-node production use.
- Per-cross-node-write tokio runtime spin-up in `sync.rs` — replace with a
  shared runtime handle (allocation footgun under fan-out).
- `Term::try_pid` range skip silently drops cross-node deliveries — encode
  as proper external pids instead of skipping (joint item with beamr's pid
  encoding rules).
- Membership staleness: 250ms polling is a workaround for beamr's
  single-slot connection-down hook; adopt the multi-subscriber hook when
  it lands (beamr ledger C3) and delete the poll.

## E. Protocol tier

- Schema negotiation is exact-hash equality; define compatibility rules
  (additive-evolution acceptance) so a client with schema v_n can subscribe
  to v_n+1 channels when the evolution was additive.
- One-blocking-request-per-socket in `request_reply` — multiplex over
  stream ids (the stream table already exists).
- Wire ids are FNV-1a hashes of names (collision-possible, not negotiated) —
  move to server-assigned ids at subscribe time.
- Timestamp truncation to ms on the wire — document as contract or carry
  micros.

## F. Tech debt / honesty items

- Design checklists under `docs/design/` show `done:false` for shipped work
  — reconcile or delete; they currently mislead (bit me during
  orientation; trust git, not the JSONs).
- README/VISION aspirational claims (durable replay, Gleam routing) now
  partially true — refresh the ⚠️ status blocks to match v0.2.2 reality.
- The observability drain seam (`aion.observability.v1` + default-false
  notifier tap) is the template for cross-layer taps — extract the pattern
  into a doc before frame copies it ad hoc.

## Cross-domain synergies

1. **Backpressure × aion worker dispatch**: Defer signals reaching aion's
   scheduler would let workflow dispatch slow down instead of queueing
   blind — worth a joint design note with Vesper's #197 retry work.
2. **Conversation patterns × frame-conv**: request-response, subscription,
   pub/sub, workflow-handle (the frame Phase 3 quartet) map 1:1 onto
   existing liminal primitives + aion for durability — the frame-conv
   crate is mostly *shaping*, not new machinery.
3. **Observability drain × norn NOI**: agent transcripts already ride this
   seam; frame's per-component telemetry should reuse it rather than
   inventing a second tap.
4. **Cluster membership × #146**: liminal should become a consumer of
   haematite's durable membership state instead of maintaining its own
   view, once CSOT phases land.

## Explicitly deferred
Gleam SDK NIF (unless external audience appears), vector clocks (A4 may
resolve simpler), vectored/zero-copy publish path, protocol compression.
