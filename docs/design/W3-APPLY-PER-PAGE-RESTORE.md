# W3 — apply-per-page outbox restore design brief

Base: `liminal` at `eb3ae30` on `docs/w3-apply-per-page-restore`.

Revision: r1, 2026-07-19. The Unit 2 spec of record is
`0cdff85:docs/design/F0C-UNIT2-SERVERPUSH-PRODUCER.md`, SHA-256
`98f9130faa175f323206eb6640e9f625ab6385dc285e11f5b2774fb306b5de6a`.
All repository anchors in this revision were checked against the base bytes.

## Goal

Make Unit 2 extension/outbox restore consume and apply one decoded durable page
at a time. Peak transient extension-row materialization must be bounded by the
signed page size, independent of total append-only outbox history, without
changing replay order, repair, errors, durable bytes, restored state, or the
point at which restored state becomes observable.

## 1. Pinned gap and memory consequence

### 1.1 Signed disposition

Wiring-ledger lane W3 records the signed conformance gap verbatim:

> spec:570 total-restore-streaming: read_all materializes the full decoded stream; only the 64-row page size is enforced.

That is the W3 deliverable and trigger recorded at
`docs/design/WIRING-LEDGER.md:49-59`. The property being repaired is the Unit 2
signed-values claim that `UNIT2_OUTBOX_RESTORE_BATCH_ROWS = 64` has the
worst-case cost “One batch holds 64 encoded extension/outbox rows plus decode
state; total restore remains streaming”
(`0cdff85:docs/design/F0C-UNIT2-SERVERPUSH-PRODUCER.md:560-570`).

### 1.2 The current bytes do materialize the complete decoded stream

`OutboxLog::read_all` returns `Vec<(u64, OutboxRow)>`, initializes one `rows`
vector, reads store pages with `UNIT2_OUTBOX_RESTORE_BATCH_ROWS`, and pushes
every decoded row into that same vector before returning it
(`crates/liminal-server/src/server/participant/production/outbox_log.rs:287-336`).
The page-size constant is 64
(`crates/liminal-server/src/server/participant/production/outbox_log.rs:19-24`),
but the vector is never drained between durable reads. The current production
caller first receives that complete vector and then moves it into
`ConversationAuthority::replay`
(`crates/liminal-server/src/server/participant/production/handler.rs:250-268`;
`crates/liminal-server/src/server/participant/production/ops_session.rs:270-282`).
The premise is therefore true at the pinned bytes.

Let `H` be the number of physical extension rows and `D(H)` the sum of their
decoded owned allocations, including nested records, recipients, and payloads.
Current peak restore memory includes all `H` decoded rows simultaneously:
transient decoded-history memory is `Theta(D(H))` and therefore grows with the
complete outbox history. A 64-row `read_from` limit changes I/O page size; it
does not cap this accumulation. For an append-only history, memory can continue
to grow even when live retained obligations remain within their separate
configuration bounds.

## 2. Production entry-point census

There is exactly one direct production caller of `OutboxLog::read_all`; the
other matches are test inspection helpers. Its three production entry routes
are all owned by the same `replay_and_repair` path:

| kind | production file:line | role |
|---|---|---|
| direct caller | `crates/liminal-server/src/server/participant/production/handler.rs:256` | `replay_and_repair` reads the complete extension stream before replay. |
| startup route | `crates/liminal-server/src/server/participant/production/handler.rs:121-141` | `restore_all_conversations` calls `replay_and_repair` for every registered conversation before installing its owner. |
| cold-first-touch route | `crates/liminal-server/src/server/participant/production/handler.rs:183-195` | An absent conversation owner is replayed under the conversation-cell lock. |
| post-commit reconciliation route | `crates/liminal-server/src/server/participant/production/handler.rs:206-220` | After a v2 source barrier advances the log head, the same replay repairs/reconciles Unit 2 and replaces the owner; failure clears it. |

W3 changes the one direct caller and the replay seam it feeds. All three routes
must use that one page-wise implementation; no route may retain `read_all` as a
fallback.

## 3. Page-reader contract

### 3.1 One move-only restore cursor

Replace the production `read_all -> Vec<all rows>` contract with one move-only
restore cursor owned by the cold replay transaction. The cursor owns:

1. the next expected physical extension sequence;
2. the schema version established by the first row, retained across page
   boundaries;
3. EOF state; and
4. at most one decoded page of `(physical_sequence, OutboxRow)` values.

A page read still calls the durable store with
`UNIT2_OUTBOX_RESTORE_BATCH_ROWS`. It checks physical contiguity, missing or
mixed schema versions, the exact schema-v1 version, decode completeness, and
checked sequence advancement using the same rules now at
`crates/liminal-server/src/server/participant/production/outbox_log.rs:293-332`.
It returns no aggregate history collection.
The cursor may request the next durable page only after ownership of every row
in the current decoded page has moved into the restoring merge or been dropped
on failure. It must not prefetch a second decoded page.

The encoded store entries for the current read, decoder scratch, the existing
base-log page, cursor fields, and the staged final authority are fixed with
respect to total historical row count under the already signed configuration.
The only transient decoded extension-history buffer is one page. Thus, for any
history size `H`, its resident decoded stream rows are at most
`min(H, UNIT2_OUTBOX_RESTORE_BATCH_ROWS)` rather than `H`.

### 3.2 EOF is testimony, not an empty local queue

The current merge repairs a missing projection only when the durable extension
is an exact prefix and no later row is present
(`crates/liminal-server/src/server/participant/production/outbox_replay.rs:46-117`).
With page-wise input, exhausting a page does not prove that condition. The merge
must ask the cursor for the next page after fully applying the current one and
may append a missing projection only after the cursor has returned durable EOF.
A nonempty next page preserves the current conflict rule: a missing projection
before a later physical row is corruption, not a repair opportunity.

The cursor's checked next physical sequence at EOF becomes the optimistic
extension head. It replaces the current `rows.len()` derivation at
`crates/liminal-server/src/server/participant/production/outbox_replay.rs:20-33`;
no unchecked length conversion or guessed head is permitted.

## 4. Application, determinism, and visibility

### 4.1 Ordered apply-per-page merge

The restoring transaction starts with the same empty local
`ConversationAuthority` and `ConversationOutbox`. It merges literal v2 base rows
and physical extension rows in the same order as today: all extension rows tied
to a base boundary in physical extension-sequence order, then the next v2 row.
The current base replay is already paged and applies each operation before
advancing (`crates/liminal-server/src/server/participant/production/ops_session.rs:279-336`).
W3 makes the extension side equally incremental.

For each decoded extension page, the merge SHALL:

1. preserve the durable physical sequence and row order exactly;
2. advance base replay only as required to reach each row's recorded boundary;
3. run the existing expected-projection equality, marker-ack replay, and
   `ConversationOutbox::apply_row` transitions for each row
   (`crates/liminal-server/src/server/participant/production/outbox_replay.rs:63-95`);
4. relinquish the complete page; and only then
5. request the next extension page.

No page boundary is a semantic boundary. It may split rows tied to one base
head, and the merge must continue that tie across pages without publication,
repair, reset, reordering, or a second interpretation of the row.

### 4.2 Determinism-preservation invariant

**W3-D1 — page-partition invariance.** For every durable base/extension input
accepted by today's `read_all`-fed replay, page-by-page application must produce
a final restored state byte-identical to today's result. Durable repair bytes,
extension head, owner facts, observer projections, capacity contribution, and
subsequent ordered push bytes must also be identical. For every input rejected
today, W3 must reject with the same typed outcome. Changing the placement of a
page boundary must change none of those observations.

The implementation reuses the existing projection and apply transitions; it
must not introduce a page-level sort, fold summary, duplicate-elision rule, or
alternate decoder. `ExtensionMerge::finish` remains the only step that installs
the fully validated outbox into the local authority
(`crates/liminal-server/src/server/participant/production/outbox_replay.rs:120-136`).

### 4.3 State-visibility boundary

Decoded pages apply only to a staged, function-local authority while the
conversation-cell lock excludes operations. They are not successive published
owners. At startup, the owner is installed only after `replay_and_repair`
returns
(`crates/liminal-server/src/server/participant/production/handler.rs:121-143`).
On cold first touch, installation likewise occurs only after it returns
(`crates/liminal-server/src/server/participant/production/handler.rs:183-195`).
Post-commit reconciliation replaces the old owner only on complete success and
clears it on failure
(`crates/liminal-server/src/server/participant/production/handler.rs:206-220`).
The cell lock is released after those decisions at
`crates/liminal-server/src/server/participant/production/handler.rs:236`.

Observer reconciliation and the server-capacity fold are also outside
`ConversationAuthority::replay`, after its complete return
(`crates/liminal-server/src/server/participant/production/handler.rs:269-297`).
W3 must keep them after durable EOF, final merge checks,
and the last applied page. No observer target, capacity contribution,
conversation owner, replay delivery, or repair wake may witness a page prefix.
This is the state-visibility boundary: all externally reachable restore effects
remain after the final page and successful finish, exactly where the current
complete-vector path places them.

## 5. Error and failure atomicity contract

### 5.1 Today's typed decode outcome

Today `read_all` checks the leading/cross-row schema version, invokes the
singular `decode_row`, and propagates its exact `OutboxLogError` with `?`
(`crates/liminal-server/src/server/participant/production/outbox_log.rs:308-330`).
`decode_row` itself returns the typed missing version, schema version,
unexpected-end, unknown-tag, invalid-selector, trailing-byte, and related codec
variants
(`crates/liminal-server/src/server/participant/production/outbox_log.rs:26-104`;
`crates/liminal-server/src/server/participant/production/outbox_log/codec.rs:61-115`).
The local all-rows vector is dropped on that error. At the production boundary,
`replay_and_repair` maps the unchanged `OutboxLogError` through
`outbox_log_error`
(`crates/liminal-server/src/server/participant/production/handler.rs:256-258`),
which yields `ParticipantSemanticError::Internal` with
`participant Unit 2 extension log failed: {error}`
(`crates/liminal-server/src/server/participant/production/handler.rs:412-415`).
No owner assignment follows.

### 5.2 Required page-wise behavior

A decode failure in any page, including a row after one or more successfully
applied pages, SHALL return the same concrete `OutboxLogError` variant and the
same production `ParticipantSemanticError` mapping as today. The staged
authority, staged outbox, current page, and restore cursor are dropped. No
partially applied conversation owner, capacity contribution, observer state,
or publication work survives.

Nor may page streaming create an earlier durable side effect. Projection repair
remains forbidden until durable EOF is proven as section 3.2 requires. A later
malformed row therefore fails before any “missing tail” repair can append.
Read failures, sequence failures, mixed versions, apply failures, and final
future-boundary failures obey the same unpublished-staging rule. W3 adds no
resume checkpoint and no partial-restore recovery format.

## 6. Signed values and arithmetic

No new numeric production bound is introduced.

| signed value | value | W3 use and worst-case cost |
|---|---:|---|
| `UNIT2_OUTBOX_RESTORE_BATCH_ROWS` | `64` | Reuse the landed signed constant at `crates/liminal-server/src/server/participant/production/outbox_log.rs:21-22` as the sole extension restore page size. At most one decoded page is resident, plus encoded current-page input, existing base-page state, and fixed-with-respect-to-history restore overhead. |

Every new sequence, resident-row accounting value, fixture history length, or
byte/row derivation must come from this named value or an existing signed config
input using checked conversion and checked arithmetic. Overflow is a typed
failure. No measured value may be copied into a literal, and no second page-size
constant, “safety” allowance, or hidden prefetch bound may be added.

## 7. Acceptance items

These four items are the W3 acceptance census. Existing item-24 and item-30
oracles rerun unchanged; the two new names are normative implementation test
names.

| item | oracle | required proof |
|---:|---|---|
| 1 | `cold_reopen_reconciles_and_replays_all_record_shapes` | Rerun the landed item-24 real-socket suite unchanged. It constructs all mapped source shapes, verifies decoded v2/extension source census and a `MarkerAckCommitted` interleaving, cold-reopens the same disk, and compares every replayed main and marker `ParticipantDelivery` exactly and in order (`crates/liminal-server/src/server/participant/production/e2e_cold_all_shapes.rs:293-350,353-424,427-451`). This pins full-shape replay determinism, not merely successful startup. |
| 2 | `leave_discharge_replays_deterministically_across_the_commit_boundary` | Rerun the landed item-30 suite unchanged. It exercises crash after both Leave flushes and crash between the v2 Left flush and extension append; reconciliation must recreate byte-identical extension history and owner state across cuts. It also pins preserved source/audit bytes, unchanged extension row count/head/payloads on a second restore, discharged leaver obligations, and the signed live-payload bound (`crates/liminal-server/src/server/participant/production/e2e_leave_commit_boundary.rs:135-187,203-309,311-331`). |
| 3 | `outbox_restore_peak_decoded_rows_never_exceeds_one_page` | Build a valid history longer than two complete pages, with its length derived by checked arithmetic from `UNIT2_OUTBOX_RESTORE_BATCH_ROWS`. A `cfg(test)`-gated accounting seam on decoded-page ownership records current and peak resident decoded stream rows, decrementing only when page ownership is consumed or dropped. Assert a full page is observed, peak never exceeds the signed constant, no second page overlaps it, and final restored facts match the ordinary path. The seam must instrument real decode/page ownership, not estimate allocations from read-call count; it is absent from production builds. |
| 4 | `midstream_outbox_decode_failure_preserves_typed_error_and_publishes_no_state` | Place at least one complete valid page before a next-page row containing only the named schema-version byte, which currently yields `OutboxLogError::UnexpectedEnd { field: "row_kind" }` through the codec at `crates/liminal-server/src/server/participant/production/outbox_log/codec.rs:65-77`. Prove the earlier page was applied to staging, then assert that exact log error and unchanged production wrapper, zero published conversation/capacity/observer prefix, and unchanged durable extension bytes with no repair append. |

The peak oracle's accounting seam follows the established test-only seam rule:
it may be `cfg(test)`-gated, but production control flow and ownership must be the
same code the oracle exercises. No ignored test, allocator-RSS guess, sleep, or
history-size proxy satisfies item 3.

## 8. Trigger and main exposure

**HARD trigger — this brief's implementation must land before any deployment with unbounded outbox history; until then the 64-row page bound plus bounded-history configuration is the operative protection.**

### Main-exposure note

The gap is **not latent-by-configuration for the participant-enabled full
profile**. `[participant]` is optional, but when present it activates production
and every field is required with no default
(`crates/liminal-server/src/config/types.rs:422-435`). The exhaustive,
unknown-field-denying `ParticipantConfig` has finite live retention inputs
`retained_capacity_entries`, `retained_capacity_bytes`, and
`max_retained_record_rows`, but no total append-only extension-history row or
byte cap (`crates/liminal-server/src/config/types.rs:509-517`).
`max_retained_record_rows` bounds restored causal rows, not historical
`Produced`, `AckAdvanced`, and marker-ack audit rows. The outbox handle is
explicitly append-only and exposes append/read behavior without a history
cutoff
(`crates/liminal-server/src/server/participant/production/outbox_log.rs:251-288`).

The worker-front-door profile cannot expose the gap because validation rejects
`[participant]` there and directs the deployment to `profile = "full"`
(`crates/liminal-server/src/config/validation.rs:524-529`). A full-profile
participant deployment, however, has no schema-enforced total-history cap, so
sufficiently long-lived traffic can make `H` unbounded even while live retention
is bounded. Until W3 lands, “bounded-history configuration” in the HARD clause
must therefore be an explicit deployment/operational bound on accumulated
outbox history; the current participant config fields do not silently supply
one.

## 9. Honesty, costs, and non-goals

### 9.1 What W3 does not do

- It does not change `UNIT2_OUTBOX_RESTORE_BATCH_ROWS = 64`, negotiate a page
  size, or add another numeric restore bound.
- It does not stream `read_all`-style inspection helpers or any non-restore
  reader. Only the production restore/reconcile path in section 2 changes.
- It does not alter canonical row encoding, schema version, physical sequence,
  base/extension merge ordering, marker-ack semantics, projection repair, or
  append/flush barriers.
- It does not alter live-path outbox accounting, recipient obligations, retained
  payload limits, acknowledgement/Leave discharge, scheduler behavior, socket
  publication, or SDK behavior.
- It does not compact, truncate, delete, or otherwise bound physical durable
  history. Apply-per-page restore makes restore memory safe as history grows; it
  is not physical reclamation.
- It does not publish partial progress, add a restore checkpoint, or permit
  service startup over a corrupt suffix.

### 9.2 Cost statement

W3 adds **no idle or steady-state cost**. The cursor exists only while startup,
cold first touch, or post-commit reconciliation is already replaying a
conversation. There is no resident task, timer, poll, background queue, extra
live-path branch, or steady-state allocation. Each extension row is still read
and decoded once; the existing durable page cadence is retained, while decoded
page ownership is released earlier. The accounting seam is `cfg(test)`-gated
and contributes no production atomics, counters, or memory.

### 9.3 Ownership and deferrals

No part of the normative W3 contract is deferred: page-wise production wiring,
both new oracles, and unchanged item-24/item-30 gates land together. **Hermes
(liminal seat and W3 lane owner)** owns that implementation, with the HARD
trigger in section 8. Non-restore-reader streaming and physical audit
compaction are non-goals, not dormant seams introduced by W3. If either later
becomes required, its sponsor must first add a ledger row naming its consumer,
owner, trigger, and oracle floor; this brief does not mint an unowned road back.

## 10. Walls

- **WALL-ONE-PAGE:** no production path retains decoded rows from two extension
  pages or reconstructs a complete history collection under another name.
- **WALL-DETERMINISM:** W3-D1 is exact; page boundaries cannot alter final state,
  durable repair bytes, errors, ordering, or publication.
- **WALL-ATOMIC-VISIBILITY:** all page application is staged; authority,
  observer, capacity, and publication boundaries remain after final EOF and
  successful finish.
- **WALL-ONE-DECODER:** use the canonical schema-v1 decoder and existing typed
  errors. No compatibility decoder, default, alias, or page-local version reset.
- **WALL-NO-MAGIC-NUMBERS:** reuse the signed 64-row constant; derive and check
  every new count or bound.
- **WALL-YG-560:** forward-only ordinary commits; no merge, rebase, cherry-pick,
  or pull.
- **WALL-NO-PUBLISH:** no package publish or version tag.

## 11. Revision record

| revision | date | record |
|---|---|---|
| r1 | 2026-07-19 | Pinned the current full-history accumulation and all production entry routes; specified the one-page cursor, EOF-gated repair, page-partition determinism, post-final-page visibility and unchanged typed-error contract; named the four acceptance oracles; recorded signed arithmetic, main exposure, costs, non-goals, ownership, trigger, and walls. |
