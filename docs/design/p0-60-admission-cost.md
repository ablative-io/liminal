# p0-60 — admission cost grows with conversation history

Board #60. Every committed `RecordAdmission` costs time proportional to the
conversation's whole durable history. Measured slope 1.153 ms/record over 4,585
appends (zero failures), intercept 9.9 ms. A conversation at N≈1,510 pays
1.75 s per admission; one admission alone crosses a 5 s socket window at
N≈4,328. The ceiling is arithmetic, not load.

Every `file:line` below is at rev `9c22b84` unless another rev is written.

## 1. Where the cost is

Two independent multipliers compose.

### 1a. The post-append reconcile replays the conversation from sequence 0

`with_conversation_reconciliation`
(`crates/liminal-server/src/server/participant/production/handler.rs:482`)
runs the caller's operation, and when that operation appended anything
(`handler.rs:526-527`) it calls `replay_and_repair`
(`handler.rs:532`, defined `handler.rs:594`) — a cold replay of the entire
durable log, inside the per-conversation authority mutex, before the correlated
terminal response may be published.

`replay_and_repair` costs, per call, four full stream passes:

| pass | site | over |
| --- | --- | --- |
| outbox pre-validation | `handler.rs:600` (`restore_cursor().validate_all()`) | whole Unit 2 extension stream |
| base-log schema validation | `ops_session_replay.rs:33` → `validate_operation_schema_inner` `ops_session_replay.rs:519` | whole base log |
| base-log replay | `ops_session_replay.rs:39-106` | whole base log |
| outbox merge | `ExtensionMerge` cursor, `outbox_replay.rs:57-118` | whole Unit 2 extension stream |

So an admission at history N decodes and re-runs N durable rows twice and walks
the extension stream twice.

### 1b. `read_from` pushes no page limit down, so each full pass is O(N²)

`HaematiteStore::read_from`
(`crates/liminal/src/durability/store.rs:118-132`) calls
`EventStore::read_from(stream_key, offset)`, which materialises **every** event
with seq ≥ offset — key and value copied across the shard-actor boundary
(`haematite-0.8.1/src/api/event_store.rs:186-239`, allocating at
`shard/actor/native.rs:309`, `db/helpers.rs:28-38`, `event_store.rs:224`) — and
only then truncates to `limit` (`store.rs:130`).

Replay pages at `READ_BATCH_SIZE = 64` (`production/log.rs:29`) and
`UNIT2_OUTBOX_RESTORE_BATCH_ROWS = 64` (`production/outbox_log.rs:24`). A full
read of an N-row stream therefore scans

    N + (N-64) + (N-128) + … ≈ N² / 128

engine rows to deliver N. The page limit is honoured at the wrong end of the
call.

## 2. What the reconcile is re-establishing — the load-bearing answer

The obvious reading is that the reconcile is defensive: recompute the authority
from durable truth so the live owner cannot drift from the cold owner (the
doc comment at `handler.rs:465-468` says exactly that). That reading is
incomplete, and the incomplete reading is what makes the cost look removable by
a cheap equivalence proof.

**The replay is not a verification pass on the admission path. It is the only
writer of the admission's durable Unit 2 extension row.**

- The live admission commit `persist_record_commit`
  (`ops_frontier.rs:326-392`) appends the base-log row (`ops_frontier.rs:355-357`),
  then mutates `install_frontier` / `committed_admissions` /
  `observe_replayed_position` / `advance_log_head`
  (`ops_frontier.rs:370-377`) and **stages** the projection into the dispatch
  accumulator (`ops_frontier.rs:378-387`).
- It never touches `authority.outbox`, and it never appends an `OutboxRow`. The
  only two sites that assign `authority.outbox` are
  `outbox_replay.rs:176` and `outbox_replay.rs:306`, both inside a merge that
  only a replay constructs.
- The extension row is written by `ExtensionMerge::apply_boundary`'s repair
  branch (`outbox_replay.rs:120-152`, the append at `outbox_replay.rs:137-142`):
  the replay reaches the just-appended base row, `project_committed_source`
  produces the expected `OutboxRow` (`ops_session_replay.rs:87-92`), the
  extension cursor is at confirmed EOF because nothing wrote that row yet, and
  the "repair" appends it.

So the from-zero shape is incidental. `ConversationAuthority::replay` is a
cold-boot function reused as a commit-completion function; it happens to
recompute the prefix on the way to doing the one piece of work the commit
actually still owes.

The codebase already contains the other half of the pattern: `MarkerAck`
appends its own extension row live (`ops_acks.rs:458`,
`block_on(outbox_log.append(&row, extension_sequence))??`) and its handler arm
deliberately ignores the base appender (`handler_semantic.rs:163`). `MarkerAck`
pays no from-zero replay. `RecordAdmission` does.

**Invariant re-established:** `owner ≡ replay_and_repair(durable log)` — the
live owner equals what a cold load of the same bytes would produce, *including*
a durable Unit 2 extension row for every committed source.

**Minimal carried state that makes the suffix sufficient:** the live owner
itself, plus the outbox's own extension head (`ConversationOutbox`'s
`next_extension_sequence`, `outbox.rs:174`), plus the projection `OutboxRow`
for the appended source — which the live path **already computes**, via the
same `project_committed_source` the replay uses
(`production/dispatch_impact.rs:47`). Nothing about the prefix is needed. The
prefix is re-derived only because the writer of the extension row lives inside
the cold-boot function.

## 3. Designs considered

### 3a. Snapshot the pre-operation authority and replay only the suffix — REJECTED

The textbook incremental reconcile: hold `replay(0..H)` from before the
operation, fold rows `[H, H')` onto it. Provably equivalent by induction,
because the fold is literally the same code over the same rows.

It requires cloning `ConversationAuthority` before the operation runs, and that
is **barred at compile time by the protocol's own law**, not merely absent:

- `ParticipantConversation` is "intentionally not `Clone`: at most one owner may
  prepare the next event ordinal", enforced by a `compile_fail` doctest
  (`crates/liminal-protocol/src/lifecycle/conversation.rs:40-50`).
- `LiveFrontierOwner` carries two `compile_fail` doctests, one forbidding
  `clone()` and one forbidding field-splicing
  (`crates/liminal-protocol/src/lifecycle/operations/live_frontier.rs:47-64`).
- `ObligationDebtDispatchState` (`lifecycle/obligation_dispatch.rs:29`),
  `ConversationOutbox` (`production/outbox.rs:161`), `Slot`
  (`production/state.rs:122`), `FateOccurrenceRouter`
  (`production/fate_occurrence.rs:107`) and
  `ObserverProgressWitnessState` (`production/observer_progress.rs:407`) are all
  non-`Clone` by the same move-only discipline.

Move-only ownership is how this system makes "exactly one owner may mint the
next ordinal" a compile error rather than a review note. Deriving `Clone`
through that layer to buy a performance fix trades a structural safety property
for a constant factor, and the constant factor is not even good: the snapshot
would be O(state), and `committed_admissions` (`state.rs:228`) holds one entry
per committed admission in the retained window, so the clone stays Θ(N).
Rejected on both counts.

### 3b. Trust the live apply and delete the reconcile — REJECTED

If live and cold post-states were always equal, the reconcile would be
redundant and removable, pinned by an equivalence oracle. They are not equal:
§2 shows the live apply leaves the durable extension row unwritten and the
outbox owner untouched. The reconcile is the mechanism that makes them equal,
not a check that they already are. Deleting it loses durable state.

### 3c. Complete the appended source in place — DESIGNED, NOT LANDED IN THIS LANE

Give `RecordAdmission` the same commit-completion `MarkerAck` already has:

1. The live commit computes its projection once (it already does —
   `record_produced_source`, `production/dispatch_impact.rs:40-52`).
2. It appends that `OutboxRow` to the extension stream at the outbox's own
   `next_extension_sequence`, and applies it to the live `ConversationOutbox`,
   under the same conversation lock, before the terminal response is published.
3. The post-append from-zero replay is then no longer the writer of anything,
   and the reconcile for that source becomes O(1): no base-log pages, no
   extension-stream pages, no re-decode of the prefix.

Cost per admission drops from Θ(N) durable rows to O(1). The load-end passes
that the reconcile also performs (`repair_pending_specific_fates`,
`reconcile_observer_progress`, `prune_expired_provenance`, the ledger fold,
`reconcile_load_end_marker_anchors`, `validate_replayed_seal` —
`handler.rs:594-670`, `ops_session_replay.rs:107-161`) are load-time repairs of
crash residue; a source committed in-process under the lock has no crash
residue to repair, and the observer-progress witnesses the live path already
accumulates (`begin/end_observer_progress_source`) are reconciled directly.

Equivalence would be pinned, not argued: generated histories driven through the
live path, then cold-replayed from the durable bytes into a fresh handler, and
the two compared.

**Why it is not in this lane.** The live commit path and the replay path do not
merely disagree about who writes the extension row; they disagree about
observer-progress accounting. `begin_observer_progress_source` /
`end_observer_progress_source` (`state.rs:457`, `state.rs:463`) bracket every
row applied by a replay (`ops_session_replay.rs:78,93`, `outbox_replay.rs:88,116,143,148`)
and are called from **no live commit site at all**. The witness state the
handler reconciles (`handler.rs:623`, `take_observer_progress_witnesses`) is
therefore produced by the replay, not by the live apply — a second thing the
from-zero pass is the sole producer of, on the same footing as the extension
row.

Landing 3c means giving all five sources that append a base row and produce a
projection (`record_produced_source` callers: `ops_frontier.rs:308`,
`ops_frontier.rs:378`, `ops_session.rs:127`, `ops_attach.rs:178`,
`ops_enroll.rs:204`) a live commit-completion that reproduces the replay's
extension write AND its observer-progress bracketing exactly — the property
`tests_w1b_umbrella::fate_live_and_cold_replay_produce_identical_witnesses_and_state`
already exists to guard. That is a refactor of the Unit 2 / observer-progress
seam, not an edit, and shipping a half of it that greens a row-count pin while
quietly changing observer-progress semantics would be a fix that closes a
different failure. It is scoped here and left red on purpose (§6).

Two smaller economies were examined and rejected as NOT free. The outbox
pre-validation walk (`handler.rs:600`) and the base-log schema validation walk
(`ops_session_replay.rs:33`) each duplicate a stream the following pass walks
again, but both exist to establish that the whole stream decodes BEFORE any
state is installed — a fail-fast ordering the suite pins by name
(`tests_w3_restore::midstream_outbox_decode_failure_preserves_typed_error_and_publishes_no_state`).
Deleting either halves the constant by changing error semantics, which is a
different lane's decision.

### 3d. Push the page limit into the store — LANDED IN THIS LANE

`HaematiteStore::read_from` builds a bounded key window and asks the engine for
exactly that window:

    from = encode_stream_key(stream_key, offset + 1)                  // engine seq is 1-based
    to   = encode_stream_key(stream_key, offset + 1 + limit)          // exclusive
    database().range_routed(stream_key, &from, &to)

`encode_stream_key` is `stream_key || 0x00 || seq.to_be_bytes()`
(`haematite-0.8.1/src/api/event_store.rs:375-381`), so byte order is sequence
order and the window is exact. `Database::range_routed`
(`haematite-0.8.1/src/api/kv.rs:229`) routes on the stream key — the same
co-location `EventStore` itself uses — and merges committed tree with WAL
buffer, which is the identical mechanism `read_event_entries_from`
(`haematite-0.8.1/src/db.rs:212-221`) uses with an unbounded upper bound. Both
symbols are public and `encode_stream_key` is already imported by the file
(`crates/liminal/src/durability/store.rs:146`). No haematite change is needed.

Two behaviours of the old path must not be lost: the `HistoryCompacted` verdict
raised when `offset == 0` and the stream is empty-but-counter-set
(`event_store.rs:232-238`), and the exact answer when TTL expiry or compaction
leaves a hole inside the key window. Both are preserved by a single rule:

> If the bounded window returns exactly `limit` entries, return them.
> Otherwise delegate to the unbounded engine path and truncate.

A full window is provably the same answer the old code gave — the window
contains `limit` live events, and the ordering of encoded keys means those are
exactly the first `limit` live events at or after `offset`. Any short window
falls through to literally the old code, so its answer (and its error) is the
old answer. In the common case the fall-through happens once per full stream
read, at the end-of-stream page, where the residual suffix is shorter than one
page.

Result: a full stream read goes from ≈N²/128 engine rows to N; a suffix read
costs its suffix.

## 4. The checkpoint artifact — decision and its catch-up implication

**Decision: no durable checkpoint. The design keeps no persisted snapshot and
introduces no new stream.**

3c does not need one. It does not carry a reconcile cursor forward at all: it
removes the reason to re-derive the prefix, rather than caching the prefix.
The only cursor involved is one the system already persists — the extension
stream's own physical sequence, owned by `ConversationOutbox`
(`outbox.rs:174`) and re-derived at load by the merge that already runs.

That is the honest answer, and it costs something, stated plainly:

- **Boot / first-touch cost is unchanged.** The first touch of a conversation
  still cold-replays it from sequence 0 (`handler.rs:506-511`). At N=4,585 that
  is one full replay, once per conversation per process, amortised over the
  process lifetime — and after 3d it is O(N) rather than O(N²).
- A durable checkpoint would fix boot cost too. It is not in this lane.

**If a checkpoint is built later, it should be built as the catch-up artifact,
not as a private reconcile cache.** The participant protocol today has no
non-lossy catch-up: no snapshots, no attach-at-tail, no range reads — a fresh
member replays from seq 1
(estate capture `/Users/tom/Developer/ablative/docs/briefs/manifold-extensions-and-faces-capture-20260810.md`,
Doc 4 legs 1-2, and Doc 1's scaffold-path item). A reconcile checkpoint and a
catch-up snapshot are the same object seen from two sides: "the conversation
state as of sequence S, sufficient to continue from S without reading
[0, S)". Requirements that follow, and that any future checkpoint in this
codebase should be held to:

1. **Versioned and self-describing.** A leading schema-version byte, refused
   loudly on mismatch — the precedent is the Unit 2 extension codec
   (`outbox_log.rs:26`, `OutboxLogError::SchemaVersion` /
   `MixedSchemaVersions`, `outbox_log.rs:36-47`), not the JSON envelope.
2. **Anchored to a base-log sequence**, so "state as of S" is checkable against
   the log rather than trusted.
3. **Derived, never authoritative.** Deleting every checkpoint must leave the
   system correct and only slower — the same delete-and-replay contract the
   estate already requires of extension derived state. A checkpoint that can
   make a conversation unloadable has replaced durable truth with a cache.
4. **Reader-facing, not writer-private.** The catch-up user is a *member*
   joining at S, not the authority completing its own commit. A format designed
   only to reseed the local authority will not serve a joining member, and the
   second consumer is the one that justifies the artifact's existence.

The mechanical consequence of 4 is why this lane does not build it: the write
path does not need it (3c removes the need), so a checkpoint built here would
have exactly one consumer and no pressure on its format from the consumer that
actually matters. It should be designed against the catch-up requirement, in
the lane that has that requirement.

## 5. What each leg does NOT fix

- **3d (paging pushdown)** removes the quadratic, not the linearity. Per-admission
  cost stays Θ(N) rows decoded and re-run until 3c lands; 3d only stops the
  engine from copying the suffix once per page.
- **3c (complete in place)** does not touch boot cost, first-touch cost after a
  process restart, or the cost of loading a conversation that has never been
  touched in this process. Those remain one full replay each, now O(N).
- **Neither** bounds `committed_admissions` (`state.rs:228`), which grows with
  admissions in the retained op-log window. It is memory, not store I/O, and it
  is the next ceiling after this one.
- **Neither** gives the participant protocol catch-up. §4 is a design position,
  not a delivery.

## 6. State of this lane, and what is left red

**Landed:** §3d. A full stream read costs N engine rows instead of ≈N²/128,
pinned by counts inside `HaematiteStore` because the quantity is invisible from
outside it (`crates/liminal/src/durability/store.rs`, module
`paged_read_shape_tests`). This removes the quadratic. It does not move the
ceiling: it makes each of the four passes cheaper, not fewer.

**Not landed:** §3c, the fix that moves the ceiling.

**Left mechanical rather than prose.** The defect is pinned by
`tests_p0_60_admission_cost`, which asserts that one admission's durable read
cost grows by exactly `FULL_PASSES_PER_COMMIT` (4) rows per record of history,
measured at three histories so the linearity is pinned too, not just a
difference. The number 4 is §1a's table of passes, arriving independently from
an instrument that knows nothing about the table.

Those two pins are green today **because the defect is present**, and they say
so in their own names and doc comments. The lane that lands §3c inverts them to
`growth == 0`. A green there today means the ceiling is still where the field
measured it: 1.75 s/admission at N≈1,510, single-admission-fatal at N≈4,328.
