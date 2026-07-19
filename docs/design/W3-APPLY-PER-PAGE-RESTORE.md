# W3 — apply-per-page outbox restore design brief

Base: `liminal` branch at `af202b7`; production anchors remain the bytes from
`eb3ae30`. The amended wiring ledger is `origin/main@45cd066`.

Revision: r2, 2026-07-19. This is the complete pre-review fold of r1. The Unit 2
spec of record remains
`0cdff85:docs/design/F0C-UNIT2-SERVERPUSH-PRODUCER.md`, SHA-256
`98f9130faa175f323206eb6640e9f625ab6385dc285e11f5b2774fb306b5de6a`.
All repository anchors in this revision were checked against the cited bytes.

## Goal

Remove only the duplicate, complete decoded-extension `Vec` built by
`OutboxLog::read_all`. Restore SHALL validate the complete extension stream one
bounded page at a time, then re-read and apply it one bounded page at a time,
without changing error precedence, replay order, repair, durable bytes, restored
state, or the point at which restored state becomes observable.

W3 does **not** bound total restore memory as history grows. The restored
`ConversationOutbox` itself retains history-linear indexes; bounding those is
ledger lane W7. W3 narrows one transient duplicate from the full decoded stream
to one page in each of two non-overlapping passes while the retained authority
remains `Theta(history)`.

## 1. Pinned gap, narrowed scope, and honest memory consequence

### 1.1 Signed gap and amended disposition

The conformance-gap text remains verbatim:

> spec:570 total-restore-streaming: read_all materializes the full decoded stream; only the 64-row page size is enforced.

The Unit 2 signed-values table says
`UNIT2_OUTBOX_RESTORE_BATCH_ROWS = 64` and claims one batch plus decode state
while total restore remains streaming
(`0cdff85:docs/design/F0C-UNIT2-SERVERPUSH-PRODUCER.md:560-570`). The amended
ledger narrows W3 to removing “the duplicate aggregate materialization (the
`read_all` Vec) ONLY,” explicitly rejects r1's safe-for-unbounded-history claim,
and opens W7 for the retained authority indexes
(`45cd066:docs/design/WIRING-LEDGER.md:49-85`).

### 1.2 The duplicate aggregate exists

`OutboxLog::read_all` returns `Vec<(u64, OutboxRow)>`, initializes one `rows`
vector, reads with `UNIT2_OUTBOX_RESTORE_BATCH_ROWS`, and pushes every decoded
row into that same vector before returning it
(`crates/liminal-server/src/server/participant/production/outbox_log.rs:287-336`).
The production caller receives the complete vector before invoking
`ConversationAuthority::replay`
(`crates/liminal-server/src/server/participant/production/handler.rs:250-268`;
`crates/liminal-server/src/server/participant/production/ops_session.rs:270-282`).
The signed constant is 64
(`crates/liminal-server/src/server/participant/production/outbox_log.rs:19-24`),
but that controls the store request, not the accumulating vector.

Let `H` be physical extension-row count and `D(H)` the decoded owned bytes of
those rows. Today the duplicate aggregate contributes `Theta(D(H))` transient
memory in addition to the authority being restored. W3 replaces that duplicate
with at most one decoded page in either pass; it does not change the asymptotic
memory of the complete restore because of section 1.3.

### 1.3 W7 boundary: the authority remains history-linear

The restored `ConversationOutbox` contains `source_batches`, `ack_sources`, and
`all_obligations` beside its live-record structures
(`crates/liminal-server/src/server/participant/production/outbox.rs:122-138`).
Their current growth paths are byte-evident:

- every new `Produced` source stores canonical row bytes in `source_batches`
  (`crates/liminal-server/src/server/participant/production/outbox.rs:205-252`);
- every projected recipient sequence is inserted into `all_obligations`
  (`crates/liminal-server/src/server/participant/production/outbox.rs:262-270`); and
- every accepted ack source is inserted into `ack_sources`
  (`crates/liminal-server/src/server/participant/production/outbox.rs:298-325`).

Ack and retirement discharge remove recipients from live `records`, reclaim
empty live records, and reduce live charges
(`crates/liminal-server/src/server/participant/production/outbox.rs:330-395`).
They do not remove the three historical indexes. Fully
acknowledged/reclaimed traffic can therefore leave zero live records while the
three indexes continue to grow with history. Total retained authority memory is
`Theta(history)` with or without W3, exactly as ledger W7 records
(`45cd066:docs/design/WIRING-LEDGER.md:73-85`).

**Narrowed W3 memory invariant.** At no time may W3 retain the complete decoded
extension stream in a second aggregate. At most one decoded extension page is
resident in the validation pass or the application pass, and those passes do
not overlap. The staged authority and its W7 indexes are explicitly outside
that narrowed bound and must be measured honestly rather than called fixed
restore overhead.

## 2. Production entry-point and W7-inheritance census

There remains exactly one direct production call to `OutboxLog::read_all`, but
there are **four** owning production routes into the `replay_and_repair` path:

| kind | production file:line | route and visibility trace |
|---|---|---|
| direct caller | `crates/liminal-server/src/server/participant/production/handler.rs:256` | `replay_and_repair` reads the extension before authority replay. |
| route 1 — startup | `crates/liminal-server/src/server/participant/production/handler.rs:121-143` | `restore_all_conversations` replays each registered conversation, installs the completed owner at line 141, then releases its cell lock. |
| route 2 — ordinary cold first touch | `crates/liminal-server/src/server/participant/production/handler.rs:183-195` | An absent owner is replayed under the conversation-cell lock and installed only after complete return. |
| route 3 — post-commit reconciliation | `crates/liminal-server/src/server/participant/production/handler.rs:206-236` | A crossed v2 barrier replays/reconciles under the same lock, replaces the owner on complete success, clears it on failure, and only then releases the lock. |
| route 4 — ObserverRecovery pre-pass | `crates/liminal-server/src/server/participant/production/handler_semantic.rs:312-314`; `crates/liminal-server/src/server/participant/production/handler_observer.rs:39-53,345-365` | `ClientRequest::ObserverRecovery` runs `ensure_tracking_from_log` for each named conversation before observer classification. An absent owner takes the conversation-cell lock, calls `replay_and_repair`, installs the completed owner at line 364, and releases the lock at line 365. |

Route 4 is reachable in production: an operation failure or failed post-commit
reconciliation sets the owner to `None`, leaving durable truth for the next
touch
(`crates/liminal-server/src/server/participant/production/handler.rs:218-233`).
All four routes SHALL use the same bounded
validate-then-apply implementation. No route may preserve `read_all` or a
complete-history fallback.

### Coordinator census finding — W7 inherits route 4

**Yes: the ObserverRecovery pre-pass materializes and touches all three W7
history-linear indexes when its owner is absent.** The pre-pass calls
`replay_and_repair`
(`crates/liminal-server/src/server/participant/production/handler_observer.rs:357-364`),
which invokes `ConversationAuthority::replay`
(`crates/liminal-server/src/server/participant/production/handler.rs:250-268`).
Replay constructs an `ExtensionMerge`, applies every physical extension row to
its `ConversationOutbox`, and installs that outbox only at finish
(`crates/liminal-server/src/server/participant/production/ops_session.rs:270-349`;
`crates/liminal-server/src/server/participant/production/outbox_replay.rs:20-33,71-95,120-136`).
Those applications execute the insertions in section 1.3. W7's future route
census therefore inherits ObserverRecovery's absent-owner pre-pass. This r2
records the byte finding; the coordinator, not W3, owns the corresponding
ledger amendment.

## 3. Bounded validate-then-apply reader contract

### 3.1 One-page cursor, instantiated twice

Replace `read_all -> Vec<all rows>` with a move-only extension restore cursor.
Each cursor instance owns:

1. the next expected physical sequence;
2. the schema version established by its first row, retained across page cuts;
3. explicit EOF state; and
4. at most one decoded page of `(physical_sequence, OutboxRow)`.

Every nonempty read validates physical contiguity, missing/mixed schema version,
the exact schema-v1 version, canonical decode, trailing bytes, and checked
sequence advancement using the same rules now in `read_all`
(`crates/liminal-server/src/server/participant/production/outbox_log.rs:293-330`).
A cursor must release or drop the complete current
page before requesting another. It must not prefetch. Restore creates one cursor
for validation, drops it at confirmed EOF, and creates a fresh cursor for
application; pages from the two passes can never coexist.

### 3.2 Pass 1 — validate the complete extension before semantics

The validation pass streams every physical extension row through the canonical
`decode_row`, performs exactly the stream/codec checks that `read_all` performs
today, and discards each page before reading the next. It creates no
`ConversationAuthority`, does not read/apply v2 semantic operations, does not
compare expected projections, and cannot repair or publish anything.

Only after this pass receives the explicit empty-read EOF confirmation may the
application pass start. Thus every extension decode/sequence/schema error is
selected before any base/extension semantic error, preserving today's
“decode-everything-first” ordering at
`crates/liminal-server/src/server/participant/production/handler.rs:256-268`.

### 3.3 Pass 2 — re-stream and apply one page at a time

The application pass re-reads and re-decodes the validated extension from
physical sequence zero. It builds the same local empty authority and
`ConversationOutbox`, stream-merges v2 and extension rows in physical order,
and consumes every current page before requesting another. It uses the existing
expected-projection comparison, marker-ack replay, and
`ConversationOutbox::apply_row`; no summary or alternate interpretation is
introduced
(`crates/liminal-server/src/server/participant/production/outbox_replay.rs:63-95`).

The application cursor's checked next physical sequence after confirmed EOF is
the optimistic extension head. Missing-tail reconciliation is permitted only
after that confirmation. A later physical row still makes a missing earlier
projection corruption, as today
(`crates/liminal-server/src/server/participant/production/outbox_replay.rs:97-115`).

### 3.4 Durable EOF is an empty read

`DurableStore::read_from` promises entries beginning at an offset **up to** the
requested limit; it does not promise that a nonterminal read fills the limit
(`crates/liminal/src/durability/store.rs:31-37`). Therefore a short nonempty page
is not EOF. After every nonempty page, including a short one, each pass advances
the checked offset and reads again. Durable EOF is established only when
`read_from` returns an empty vector at that next offset.

Both passes perform that terminal empty read. Application cannot declare an
exact prefix, append a repair, finish the outbox, or publish state based only on
`page_len < UNIT2_OUTBOX_RESTORE_BATCH_ROWS`. This deliberately replaces the
current short-page break at
`crates/liminal-server/src/server/participant/production/outbox_log.rs:300-334`.

## 4. Application, determinism, precedence, and visibility

### 4.1 Boundary state survives page cuts

The semantic merge order remains: every extension row at a base head in physical
extension-sequence order, then the next v2 base row. The base side is already
paged
(`crates/liminal-server/src/server/participant/production/ops_session.rs:279-336`).
A decoded extension page is only an ownership unit, never a semantic boundary.

State for the current base-head group, including whether its exact expected
projection has been seen, must survive release of one extension page and loading
of the next. Today `projection_seen` governs exact projection acceptance and
whether repair is legal
(`crates/liminal-server/src/server/participant/production/outbox_replay.rs:55-105`).
Multiple MarkerAck rows can
share the unchanged `self.next_log_sequence` base head while receiving distinct
physical extension sequences (`crates/liminal-server/src/server/participant/production/ops_acks.rs:328-344`). A page cut through a group containing its
`Produced` row and same-head MarkerAck rows must neither reset
`projection_seen` nor create an apparent empty-prefix repair opportunity.

### 4.2 Determinism and exact-error-precedence invariants

**W3-D1 — page-partition invariance.** For every input accepted today, the final
restored state, durable repair bytes/head, authority facts, observer projections,
capacity contribution, and ordered push bytes are byte-identical regardless of
store page lengths or physical page cuts.

**W3-D2 — decode-before-semantics precedence.** For every multiply-invalid
input, the validation pass selects the exact first extension stream/codec error
that today's complete `read_all` selects before `ConversationAuthority::replay`
can observe a base/extension semantic conflict. A later-page malformed suffix
therefore beats an earlier-page projection conflict. This ordering is a
preserved contract, not a new priority: the two-pass implementation is a zero
observable contract change.

**W3-D3 — narrowed materialization.** The duplicate decoded-stream residency is
at most `UNIT2_OUTBOX_RESTORE_BATCH_ROWS` rows in pass 1 or pass 2, never both.
This invariant says nothing about the history-linear authority measured for W7.

`ExtensionMerge::finish` remains the only installation of the fully checked
outbox into the local authority
(`crates/liminal-server/src/server/participant/production/outbox_replay.rs:120-136`).

### 4.3 State-visibility boundary across all four routes

No validation page creates semantic state. Application pages mutate only a
staged function-local authority while the owning conversation cell is locked.
Observer reconciliation and server-capacity folding remain after complete
authority replay
(`crates/liminal-server/src/server/participant/production/handler.rs:269-297`),
hence after validation EOF, application EOF, and merge finish.

The ordinary route installation points remain those in section 2. For
ObserverRecovery, `ensure_tracking_from_log` installs and unlocks only after
`replay_and_repair` succeeds
(`crates/liminal-server/src/server/participant/production/handler_observer.rs:345-365`);
only after every pre-pass returns does `apply_observer_recovery` lock/restore
and classify the observer aggregate
(`crates/liminal-server/src/server/participant/production/handler_observer.rs:46-75`).
A validation or application error therefore prevents owner-prefix publication
and prevents the recovery
batch from entering observer classification. W3 must not move owner, observer,
capacity, delivery, or repair-wake visibility into either page loop.

## 5. Error and failure atomicity contract

### 5.1 Today's typed outcome and precedence

`read_all` checks physical sequence and schema, invokes the singular canonical
`decode_row`, and propagates its exact `OutboxLogError`
(`crates/liminal-server/src/server/participant/production/outbox_log.rs:293-330`;
`crates/liminal-server/src/server/participant/production/outbox_log/codec.rs:61-115`).
The production boundary maps that error through `outbox_log_error` at
`crates/liminal-server/src/server/participant/production/handler.rs:256-258`,
yielding
`ParticipantSemanticError::Internal` with
`participant Unit 2 extension log failed: {error}` at
`crates/liminal-server/src/server/participant/production/handler.rs:412-415`.
Because the whole vector is decoded before `ConversationAuthority::replay`
starts, any extension decode error has precedence over every semantic conflict,
including one represented by earlier physical rows.

### 5.2 Required two-pass failures

A validation-pass error returns the same concrete `OutboxLogError` variant and
same production wrapper as today. The application pass never starts, no repair
append occurs, and no owner/observer/capacity prefix exists. A payload containing
only `OUTBOX_SCHEMA_VERSION` still yields
`OutboxLogError::UnexpectedEnd { field: "row_kind" }` through
`crates/liminal-server/src/server/participant/production/outbox_log/codec.rs:65-77`
and the unchanged handler mapping.

Pass 2 re-runs the same stream checks while applying to unpublished staging.
Any read, decode, sequence, semantic, or final-boundary failure drops the staged
authority and current page. Repair remains forbidden until pass 2's empty-read
EOF. W3 adds no resume checkpoint, partial authority, compatibility decoder, or
alternate error. The two passes preserve the old result for valid, singly
invalid, and multiply invalid durable states.

## 6. Signed values, accounting, and I/O cost

No new numeric production bound is introduced.

| signed value | value | W3 use and honest worst-case statement |
|---|---:|---|
| `UNIT2_OUTBOX_RESTORE_BATCH_ROWS` | `64` | Reuse the landed constant at `crates/liminal-server/src/server/participant/production/outbox_log.rs:21-22` for both cursor instances. Each pass holds at most one decoded extension page; the passes do not overlap. The authority remains history-linear under W7. |

Every sequence, resident-row counter, fixture history length, prefix shift, and
byte/count derivation must use checked conversion/arithmetic from this value or
an existing signed config input. No second page size, measured literal, hidden
prefetch allowance, or asserted W7 memory bound is permitted.

Compared with current restore and r1's rejected single-pass design, r2 reads and
decodes each extension row twice: once to validate and once to apply. Each pass
also performs a terminal empty read, and a store returning short nonempty pages
may require more calls because “up to limit” is not “fill to limit.” Exact call
count is backend-page-shape dependent; the contract is two complete sequential
passes plus one empty EOF confirmation per pass, not a guessed formula.

## 7. Acceptance items — complete nine-oracle census

The first four names are retained from r1; items 5–9 close the five pre-review
requirements. Every asynchronous proof uses deterministic store/barrier seams,
not sleeps or RSS guesses.

| item | oracle | required proof |
|---:|---|---|
| 1 | `cold_reopen_reconciles_and_replays_all_record_shapes` | Rerun landed item 24 unchanged. It verifies decoded source census and marker interleaving, cold-reopens the same disk, and compares each main/marker `ParticipantDelivery` exactly and in order (`crates/liminal-server/src/server/participant/production/e2e_cold_all_shapes.rs:293-350,353-424,427-451`). |
| 2 | `leave_discharge_replays_deterministically_across_the_commit_boundary` | Rerun landed item 30 unchanged. It compares both Leave crash cuts, byte-identical repaired extension history and owner state, and second-restore row-count/head/payload idempotency (`crates/liminal-server/src/server/participant/production/e2e_leave_commit_boundary.rs:135-187,203-309,311-331`). |
| 3 | `outbox_restore_peak_decoded_rows_never_exceeds_one_page` | Build more than two full pages using checked arithmetic from the signed constant. A `cfg(test)` accounting seam labels validation and application page ownership, records current/peak decoded rows in both passes, proves each page is released before the next and pass 1 is released before pass 2, and asserts each pass's peak is no greater than the signed page size. It explicitly excludes retained authority indexes from the narrowed page-buffer metric. |
| 4 | `midstream_outbox_decode_failure_preserves_typed_error_and_publishes_no_state` | Put a complete valid page before a next-page payload containing only the named schema-version byte. Assert validation returns the exact `UnexpectedEnd { field: "row_kind" }` and unchanged production wrapper before application begins, with no repair, owner, observer, capacity, or durable-byte change. |
| 5 | `restore_retained_authority_counts_are_measured_and_ledgered` | Restore more than two pages of fully acknowledged/reclaimed history. Assert the live-record/charge fixture is reclaimed, then use a `cfg(test)` deep-accounting seam to measure and emit a machine-readable count and retained owned bytes separately for `source_batches`, `ack_sources`, and `all_obligations`. The implementation fold records those measurements as W7 evidence. W3 asserts measurement completeness, not an upper bound or constancy claim. |
| 6 | `observer_recovery_restores_absent_owner_page_wise_with_exact_errors` | Use the existing operation-error/failed-reconciliation transition to leave a durable conversation owner absent, then reach restore only through `ClientRequest::ObserverRecovery`. A valid case proves both passes, correct owner/capacity/observer installation, and normal classification only after the pre-pass. A malformed-suffix case proves the same concrete `OutboxLogError` and `ParticipantSemanticError` wrapper as the ordinary routes, no owner prefix, and no observer-batch classification/publication. |
| 7 | `later_page_decode_failure_takes_precedence_over_earlier_semantic_conflict` | Construct the reviewer's dual fault: a page-1 row that would produce an expected-projection semantic conflict plus a later page containing only the schema-version byte. Assert the later `UnexpectedEnd { field: "row_kind" }` wins through `crates/liminal-server/src/server/participant/production/outbox_log/codec.rs:65-77` to `crates/liminal-server/src/server/participant/production/handler.rs:412-415`, exactly as decode-everything-first does today; the semantic conflict is never applied. |
| 8 | `same_base_head_group_straddling_page_cut_replays_without_repair` | Using checked arithmetic from the signed page size, place a valid group containing its exact `Produced` row and multiple unchanged-base-head MarkerAck rows across the physical cut. Shift the derived prefix across every interior group position so both sides of the cut are exercised. Assert no repair append; exact extension bytes/head; complete owner, capacity, and observer facts; and exact ordered pushes. |
| 9 | `short_nonempty_pages_with_remaining_rows_do_not_terminate_restore` | Back both passes with an instrumented `DurableStore` fixture that returns short nonempty pages while rows remain. Assert restore continues from each checked offset until an empty read, consumes the complete stream in both passes, and produces the same final facts/bytes as full-sized pages without premature repair. |

The page and retained-authority accounting seams may be `cfg(test)`-gated. They
must instrument the real cursor/container ownership exercised by production
control flow. Production builds carry no counters or allocation tags.

## 8. Shared trigger and main exposure

The amended r1.1 ledger trigger is carried verbatim:

> HARD, SHARED WITH W7 — before any deployment with unbounded outbox history, BOTH W3 and W7 must be discharged. Stated on both rows so neither landing alone can be read as unblocking unbounded history.

This shared trigger is normative (`45cd066:docs/design/WIRING-LEDGER.md:60-62,73-85`).
Landing W3 alone removes a duplicate transient aggregate but does not make an
unbounded-history deployment safe. Landing W7 alone while `read_all` remains
would retain the duplicate full decoded stream. Both lanes must discharge.

### Main-exposure note

The gap is not latent-by-configuration for the participant-enabled full
profile. `[participant]` is optional but all of its fields are required and it
has no defaults (`crates/liminal-server/src/config/types.rs:422-435`). Its
history-related configuration bounds live retained entries/bytes and retained
causal rows, not total append-only extension history
(`crates/liminal-server/src/config/types.rs:509-517`). The outbox is append-only
and exposes no history cutoff
(`crates/liminal-server/src/server/participant/production/outbox_log.rs:251-288`).
Worker-front-door rejects `[participant]` and directs users to
`profile = "full"`
(`crates/liminal-server/src/config/validation.rs:524-529`), but a
full-profile participant deployment has no schema-enforced total-history cap.
Bounded-history operation remains required until both W3 and W7 land.

## 9. Honesty, costs, non-goals, and deferral

### 9.1 Narrowed claim and non-goals

- W3 removes only the duplicate aggregate decoded-stream materialization.
- W3 does not make total restore memory bounded or safe as history grows. The
  authority remains `Theta(history)` because of W7's three indexes.
- W3 does not change the 64-row signed page size, row encoding/schema, physical
  sequence, merge ordering, projection/repair semantics, live outbox accounting,
  ack/Leave discharge, scheduler, transport, or SDK behavior.
- W3 does not stream non-restore inspection helpers and does not compact,
  truncate, delete, or physically bound durable history.
- W3 adds no partial restore, resume checkpoint, page-level publication,
  compatibility decoder, or new error precedence.

### 9.2 Cost statement

W3 adds no idle or steady-state cost: no resident task, timer, poll, background
queue, live-path branch, or steady-state allocation. Its cost is paid on
startup, cold first touch, post-commit reconciliation, and the ObserverRecovery
absent-owner pre-pass. Relative to today, extension restore performs a second
complete read/decode pass and one explicit empty EOF confirmation per pass;
short backend pages can add read calls in both passes. This is intentional cost
for exact error precedence. Peak duplicate decoded-page residency is lower, but
the history-linear authority remains allocated. Test accounting is `cfg(test)`
only and adds no production atomics or tags.

### 9.3 W7 deferral satisfies no-row-no-dormancy

The authority-index bound is deliberately deferred to **W7 — Authority restore
history-linear indexes**, not hidden inside W3. Its named consumer is any
deployment with unbounded outbox history; its owner is **Hermes**; its trigger is
the exact shared HARD trigger in section 8; and its oracle floor is a design-first
bounding brief plus the retained-authority measurement family
(`45cd066:docs/design/WIRING-LEDGER.md:73-85`). Index compaction/reconstruction
touches ack and conflict semantics and is not safely foldable into W3. W7
inherits all four restore routes, including the ObserverRecovery pre-pass found
in section 2.

No part of narrowed W3 is deferred: both production passes, all four route
owners, exact error precedence, EOF behavior, and the complete nine-oracle floor
land together.

## 10. Walls

- **WALL-NARROWED-SCOPE:** W3 claims only removal of the duplicate full decoded
  aggregate; it does not claim a bounded total restore.
- **WALL-TWO-PASS:** all extension decode/stream validation completes page-wise
  before any semantic application; application then re-streams page-wise.
- **WALL-ONE-PAGE-PER-PASS:** no pass retains decoded rows from two pages, the
  passes do not overlap, and no complete history collection is rebuilt under
  another name.
- **WALL-EXACT-PRECEDENCE:** later extension decode failures retain today's
  precedence over earlier semantic conflicts.
- **WALL-EMPTY-READ-EOF:** a short nonempty page never proves EOF; only an empty
  read at the checked successor offset does.
- **WALL-ALL-ROUTES:** startup, ordinary cold first touch, post-commit
  reconciliation, and ObserverRecovery absent-owner pre-pass use one contract.
- **WALL-ATOMIC-VISIBILITY:** owner, observer, capacity, repair, and publication
  boundaries remain after validation EOF, application EOF, and finish.
- **WALL-NO-MAGIC-NUMBERS:** derive and check every page count, fixture shift,
  sequence, and measurement; reuse the signed constant.
- **WALL-YG-560:** forward-only ordinary commits; no merge, rebase, cherry-pick,
  or pull.
- **WALL-NO-PUBLISH:** no package publish or version tag.

## 11. Revision record

| revision | date | record |
|---|---|---|
| r1 | 2026-07-19 | Initial W3 brief: one-page single-pass design, three-route census, four-oracle floor, and an incorrect claim that final authority overhead was fixed with respect to history. |
| r2 | 2026-07-19 | Pre-review fold (`not_ready`, four majors plus one minor), disposition counted against all five findings: (1) MAJOR memory claim narrowed to duplicate aggregate only, opened/cross-referenced W7, added retained-index measurement; (2) MAJOR fourth ObserverRecovery route added with visibility/error oracle and W7-inheritance finding; (3) MAJOR error precedence preserved exactly by bounded validate-then-apply two-pass with both-pass measurement and dual-fault oracle; (4) MAJOR page-cut gap closed by same-base-head straddle oracle; (5) MINOR EOF defined only by an empty read and short-page oracle. Updated two-pass/EOF costs, shared trigger, honesty, walls, and full nine-oracle census. |
