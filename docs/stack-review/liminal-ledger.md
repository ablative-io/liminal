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

### A2. Server schema wiring — DONE 2026-07-07 (5ee2062 + bddc9c9)
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

- **Signed resource bound — host-held fd duplicate (park-flip, 2026-07-12,
  domain-owner review N1):** every live connection holds ONE extra fd — the
  supervisor's `fd_guard` dup that keeps the socket alive until the single
  record-removal funnel ACKs readiness deregistration (dereg while the fd
  is live, never after reuse). Bound: +1 fd per live connection, capped by
  `max_connections` (256 at cap). Same cost-shape as beamr commit 4's
  signed "one-extra-fd-per-live-connection teardown-dup budget" §6 line —
  this entry is liminal's mirror of that signature: named bound, deliberate,
  not a leak.
- Design checklists under `docs/design/` show `done:false` for shipped work
  — reconcile or delete; they currently mislead (bit me during
  orientation; trust git, not the JSONs).
- README/VISION aspirational claims (durable replay, Gleam routing) now
  partially true — refresh the ⚠️ status blocks to match v0.2.2 reality.
- The observability drain seam (`aion.observability.v1` + default-false
  notifier tap) is the template for cross-layer taps — extract the pattern
  into a doc before frame copies it ad hoc.

## G. Confirmed defects (2026-07-07 orientation pass, Hermes Crumpet)

*Appended per stack-devs agreement — existing item ids above are stable.
All four verified against code. ALL FOUR FIXED 2026-07-07: G1 (fb9985a), G2 (50b53eb), G3 (e2b8769), G4 (e1b847d — absorbed into the H1 outbound writer). G1 and G4 carry coordination commitments: ping
Vesper (aion) before either fix lands so the aion cross-node failover
proofs re-run.*

### G1. Durable-channel restart sequence conflict — FIXED 2026-07-07 (fb9985a; test-first, pre-fix SequenceConflict observed verbatim)
`liminal-server` rebuilds durable channels with `next_sequences = [0]`
(`services.rs:230-241` → `channel/storage.rs:198`) instead of calling
`recover_durable_channel` (`recovery.rs:46-55`, exists, tested, never
wired). Against an existing `persistence_path`, haematite's OCC refuses
the first post-restart publish with `SequenceConflict` — deterministic,
per Apollo: `expected_seq` must derive from the store, never process
memory. The restart test (`services_r5_tests.rs:62-86`) only reads.
**Order:** pin with a failing restart-then-publish test first (after the
0.12.1 bump), then wire recovery; the A1 defer-semantics doc designs
against the recovered contract, not the accidental one.

### G2. Clustered readiness never ready — FIXED 2026-07-07 (50b53eb)
`set_cluster_membership_established(true)` has no production caller —
only `set_cluster_configured(true)` runs (`runtime.rs:52`); the setter is
exercised solely by a unit test (`checks.rs:344-345`). Any server with a
`[cluster]` section serves 503 on `/ready` forever. Wire membership's
first-peer (or zero-seed bootstrap) signal into `SharedReadinessState`.

### G3. Conversation boot links dead participants — FIXED 2026-07-07 (e2b8769)
Conversation-actor boot errors if *any* configured participant pid is dead
(`conversation/actor/beam.rs:128-154`), unlike channel boot which prunes
dead subscribers (`channel/actor/mod.rs:294-316`). A conversation that
survived a participant crash under a non-`Fail` policy cannot restart its
own actor. Fix direction: prune-and-record, matching channel semantics.

### G4. Non-blocking `write_all` truncates large frames — FIXED 2026-07-07 (e1b847d, H1 outbound writer) → was: permanent
connection desync (M — root-caused from aion live report)
`write_frame` calls `write_all` on the connection socket
(`process.rs:669-683`) that the listener sets non-blocking
(`listener.rs:100`). `write_all` aborts on `WouldBlock` *after a partial
write*: any server-originated frame exceeding free kernel sndbuf
(~73KB delivered / ~96KB lost, empirically — boundary moves with buffer
state) goes out truncated. Server warns + cancels the push slot; the
client decoder waits on the declared `payload_length` and then consumes
all subsequent frames as continuation bytes — the connection silently
ghosts until "worker lost" timeout and sweeper deregistration. Affects
every server-originated frame path; pushes are where it bites (aion
transcript drains). **Fix shape:** per-connection outbound buffer drained
cooperatively in the slice loop; tear the connection down on
unrecoverable partial write; loud max-frame error at enqueue time.
**Interim mitigation (aion-side, communicated):** keep push payloads well
under ~64KB or chunk transcript events.

### G0. Substrate constraint (transient, blocks G1's test + A1 load tests)
crates.io beamr 0.11.0 **and** 0.12.0 ship LIFO owner run-queues
(beamr `run_queue.rs:47-50`): a `Continue`-spinning native starves
co-resident pids. `ConnectionProcess` busy-polls by design
(`process.rs:88-100`) on a shared 4-thread scheduler → **liminal-server on
published beamr is unreliable beyond 4 concurrent connections.** Fix is
beamr `d147fc6` + `9710912` (FIFO owner pop + regression tests), merged
2026-07-07, unpublished. Pin bump targets **0.12.1 ≥ 9710912** (Artemis
preparing; publish gated on Tom). Do not pin crates.io 0.12.0.

### G7. Push-reply slot cancelled on the first receive timeout — FIXED 2026-07-12 (unreleased-main-only regression; never shipped)

**Scope: unreleased main only — this defect existed between the D4 leak-repair
wave and this fix; no published liminal release ever carried it.** The
documented 0.2.3 push-reply contract was: `PushReplyAwaiter::receive(timeout)`'s
`timeout` is a WAIT QUANTUM only — "each elapsed poll is a benign re-arm, never a
failure." The D4 leak-repair wave (`supervisor.rs` `resolve_timeout` →
`runtime.cancel_push`) silently overloaded that parameter to ALSO be the reply
slot's lifetime: the FIRST elapsed `receive` cancelled the reserved slot. A
consumer that polls `receive(1s)` in an unbounded re-arm loop (aion's dispatch
path) then saw its re-armed `receive` find the sender dropped →
`PushReplyDisconnected` → a FALSE lost-worker at ~poll+ε while the worker was
healthy and its handler merely ran longer than one poll quantum.

**Found by aion re-proofs, root-caused by counter-experiment** (the counter-test
inverted the assertion the D4 wave had pinned, `pending_push_count == 0` after a
timeout, and showed a slow-but-healthy handler dying at the first quantum). The
leak the D4 cancel-on-timeout guarded is now bounded anyway by the §5
`max_pending_pushes_per_connection` cap (32 — count-and-insert under the
push-slot registry mutex, the cap being the per-pid slot count, released on
slot removal; the CAS reservation is the `max_connections` CONNECTION
admission, a different cap), so the cancel earned nothing and cost the
contract.

**Ruling (domain owner + consumer seats, on the record):** a caller's poll
quantum must NEVER change the protocol outcome. The `timeout` was doing two jobs
(wait quantum AND slot lifetime); they split — the slot's lifetime belongs to the
PUSH (the reply's deadline is a property of the request), the receive timeout is
just how long THIS caller waits THIS time. Mirrors the certified R1(vi)
pending-reply table design (deadline at admission, reclamation by operation
lifecycle).

**Restored + strengthened contract:** (1) `receive`'s elapsed timeout returns the
`PushReplyTimeout` variant WITHOUT cancelling and NEVER cancels — re-arm works
indefinitely; a reply already in the channel is delivered rather than reported as
a timeout. (2) DEFAULT slot lifetime is the 0.2.3 shape exactly: no per-slot
deadline; the slot is reclaimed only by reply-consumed or connection-close
(`cancel_pushes_for_connection`). Rule 2 signs cost NUMBERS, so no default
deadline number was invented; the §5 push cap bounds abandonment. (3) Additive
NEW capability: `push_to_connection_with_deadline(pid, payload, deadline)` attaches
an explicit per-push reply deadline; at expiry the slot resolves to the new typed
`ServerError::PushReplyExpired`, is removed, and its §5 cap admission released.
Expiry is evaluated HOST-SIDE and LAZILY (at `receive` touch points and at
connection close at the latest) — no timer thread, no sweeper, no parked-process
wake; an abandoned-and-never-polled deadlined push resolves at connection close.
This laziness is deliberate: zero idle cost. `push_to_connection` stays
byte-compatible on the no-deadline path. (4) A late `PushReply` frame for an
expired/removed slot is a harmless no-op (`resolve_push` benign missing-slot
discard), pinned by test.

**Regression pins:** `elapsed_receive_polls_are_benign_rearms` (the inverted D4
assertion — every elapsed poll leaves the slot reserved),
`receive_poll_quantum_never_changes_protocol_outcome` (real connection, reply
after ≥3 elapsed quanta, delivered byte-exact, no disconnect),
`explicit_deadline_expires_slot_and_releases_cap`,
`late_reply_after_expiry_is_a_harmless_noop`,
`poll_quantum_independence_yields_the_same_outcome`,
`concurrent_enumeration_during_polling_does_not_change_outcome`.

**Adversarial Sol round hardening (same branch, pre-merge):** the deadlined
receive now waits `min(caller quantum, time until deadline)` and re-evaluates
reply-first-then-expiry on every wake, so the terminal outcome of a deadlined
push is quantum-independent and a due expiry returns promptly (the quantum is a
max wait, not a promise to block); the no-deadline receive never touches the
slot registry (behaviour-compatible with 0.2.3 — no contention or poison
exposure on the unchanged API); a close-vs-register race that could strand a
slot past connection close is walled off (record removal ordered before the
close's push sweep + a post-enqueue registration re-check — exactly one side
always observes a racing slot); reclamation paths (expiry, reply delivery,
cancellation, close sweep) recover a poisoned registry guard and complete
their removals while admission stays fail-closed; a dead runtime reads as
DISCONNECTED, never a benign timeout; and an extreme deadline duration is
refused as a typed error instead of an `Instant` panic. Round 2 added the
PUBLICATION INVARIANT (S7): push registration runs insert -> confirm ->
publish, so an `Err` from `push_to_connection{,_with_deadline}` guarantees no
`Push` control was published — the confirm-after-enqueue order could report
failure for a push the client had already received and answered. Round 3
closed the invariant's last hole (S8): a failed wake does not prove the queued
control was never consumed (a live control drain can pop it in the
insert->wake window), so publication on that branch is disambiguated BY
OBSERVATION — the failed-wake rollback returning "removed" proves unpublished
(typed `Err`); "already gone" proves a drain consumed it (only `pop_control`
also removes queue entries, and the removal key embeds the push's unique
correlation id), and the call returns `Ok`: `Ok` promises ADMISSION, not
delivery — the awaiter's outcome is the delivery truth. Pins:
`same_deadlined_schedule_yields_same_outcome_for_any_quantum`,
`overdue_deadlined_receive_returns_expired_promptly`,
`no_deadline_receive_never_blocks_on_registry_lock`,
`close_racing_push_registration_never_strands_slot_or_cap`,
`poisoned_registry_still_reclaims_and_expires`,
`dropped_runtime_yields_disconnected_not_timeout`,
`duration_max_deadline_is_refused_without_slot_leak`,
`public_deadlined_push_expires_promptly_over_real_connection`,
`public_deadlined_push_reply_after_deadline_instant_still_wins`,
`close_between_insert_and_confirm_publishes_nothing`,
`err_from_public_push_publishes_nothing_after_close`,
`failed_wake_rollback_err_publishes_nothing`,
`control_consumed_before_failed_wake_reads_ok_then_disconnected`.

## H. Server direction (2026-07-07)

Product scoping + phase-1 build plan live in `docs/design/SERVER-DIRECTION.md`
(thesis: the runtime supervises your consumers). H-wave LANDED 2026-07-07
(9f869b3..5b43f6e): H1 delivery pump + G4 outbound writer (Frame::Deliver
0x19, headroom-aware pump, SDK SubscriptionStream), H2/G1 restart recovery,
H3 /metrics, H4 auth token (gate on every frame + production wiring).
Deferred minors recorded from Fable review: greedy per-slice budget in
HashMap order (latent, v1 = one sub per connection); per-skipped-frame
timeout re-arm in try_receive_once on shared transports; single frame
> 4 MiB outbound buffer = teardown (spec-inherent; publish-side size cap is
a candidate follow-up); Deliver envelope carries payload+schema+seq only
(causal metadata + publisher identity deferred, documented). Post-0.12.1
tail: pin bump -> A1-1 -> A1-2/3/4 -> A3.

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
