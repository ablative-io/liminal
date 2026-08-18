# P65 — the live owner's memory grows for its whole process lifetime

Board #65. Board #60 §5 named the next ceiling and left it standing:

> **Neither** bounds `committed_admissions`, which grows with admissions in the
> retained op-log window. It is memory, not store I/O, and it is the next
> ceiling after this one.

This lane MEASURED that ceiling, pinned it mechanically, and **did not bound
it**. Every candidate structure hit a stop clause that belongs to the seat, not
to a build. §4 states each one and what ruling would release it.

Every `file:line` below is at rev `cc81488` (branch `p65-live-owner-growth`,
worktree of `stack/liminal`) unless another rev is written.

## 0. What the lane delivers

- A measurement of every in-memory structure on the live-owner path that grows
  monotonically and has no shedding site (§1) — **twelve**, not the one the
  board named.
- Four pins in `tests_p65_live_owner_growth` holding those measurements in
  place, each with its positive control inside the fixture (§2).
- One CORRECTION: the observer-progress witness vector, named in the dispatch
  as part of the admission growth family, does not grow with admissions on
  either the live or the restored path (§3).
- No bound. §4 is the reason, per structure.

## 1. The measurement

Method: `ConversationAuthority` (`production/state.rs:197`) and every struct it
owns were walked field by field. A field is listed here when it is a
collection, it gains entries on a committed-source path, and no site removes,
retains, clears or truncates it. "No shedding site" below is a statement about
the whole crate, not about the file the field lives in.

### 1a. Unbounded — no shedding site anywhere

| # | Field | Inserted by | Read by |
| --- | --- | --- | --- |
| 1 | `committed_admissions` `state.rs:282` | `persist_record_commit` `ops_frontier.rs:432`; `publish_replayed_record_admission` `ops_frontier.rs:725` | `answer_committed_record_admission` `ops_frontier.rs:146,165,184` |
| 2 | `ConversationOutbox::source_batches` `outbox.rs:166` | `apply_produced` `outbox.rs:292` | `apply_produced` `outbox.rs:249` |
| 3 | `ConversationOutbox::all_obligations` `outbox.rs:168` | `install_record` `outbox.rs:306` | `recipient_ack_obligations` `outbox/selection.rs:57` |
| 4 | `ConversationOutbox::ack_sources` `outbox.rs:167` | `apply_ack` `outbox.rs:363` | `apply_ack` `outbox.rs:344` |
| 5 | `ConversationOutbox::retired` `outbox.rs:172` | `discharge_retired` `outbox.rs:425` | `apply_produced` `outbox.rs:259` |
| 6 | `ConversationOutbox::ack_frontiers` `outbox.rs:170` | `install_record` `outbox.rs:310`; `apply_ack` `outbox.rs:365` | `durable_ack_through` `outbox/selection.rs:119` |
| 7 | `ConversationOutbox::marker_ack_frontiers` `outbox.rs:171` | `apply_marker_ack` `outbox.rs:395` | `dispatch_after` `outbox/selection.rs:97` |
| 8 | `ObserverProgressWitnessState::witnesses` `observer_progress.rs:460` | `record_at` `observer_progress.rs:550` | `record_at` `:530`; `witnesses()` `:563`; `complete_live_commit` `handler.rs:664`; `replay_and_repair` `handler.rs:728` |
| 9 | `ObserverProgressWitnessState::occurrences` `observer_progress.rs:462` | `record_at` `observer_progress.rs:547` | `record_at` `observer_progress.rs:536` |
| 10 | `ObserverProgressWitnessState::lineage_progress` `observer_progress.rs:463` | `record_at` `observer_progress.rs:548` | `record_at` `observer_progress.rs:541` |
| 11 | `FateOccurrenceRouter::occurrences` `fate_occurrence.rs:109` (owned at `state.rs:240`) | `insert_primary` `fate_occurrence.rs:246` | `select_finalizer` `:263`; `route_specific` `:312`; `state` `:366` |
| 12 | `ConversationAuthority::retired` `state.rs:242` | `persist_leave` `ops_leave.rs:247`; `replay_leave` `ops_leave.rs:407` | `classify_leave` `ops_leave.rs:129,200`; `capacity_contribution` `occupancy.rs:189` |

Rows 6, 7, 10, 11 and 12 are keyed by participant or by lineage, so they are
bounded by the participant population rather than by history. They are listed
because the population itself is unbounded over a long-lived conversation
(every enrollment mints a new permanent `ParticipantId`) and because none of
them is erased when the participant leaves — #12 says so in its own doc
("permanent … tombstones"). Rows 1–5, 8 and 9 grow with TRAFFIC, and those are
the ceiling.

Two of them contradict a comment standing next to them:

- **#2 `source_batches` against `records`.** `records` (`outbox.rs:165`) IS
  reclaimed once every recipient has discharged (`reclaim_empty_records`,
  `outbox.rs:458`). `source_batches`, one field above it, holds the canonical
  BYTES of every produced batch and is reclaimed nowhere. The live owner keeps
  a second copy of the conversation's entire payload traffic.
- **#3 `all_obligations` against the same neighbour.** `discharge_through`
  (`outbox.rs:406`), `discharge_retired` (`outbox.rs:427`) and
  `reclaim_empty_records` (`outbox.rs:458`) all move `records` or
  `next_live_obligations`; none touches `all_obligations`. A fully discharged,
  reclaimed record leaves its per-recipient sequence behind forever.
- **#11 `FateOccurrenceRouter`** is documented at `state.rs:238-239` as
  retaining "active keys, never a copy of history". No `remove`, `retain` or
  `clear` exists on `occurrences` anywhere in the crate. The doc describes an
  intent the code does not implement.

### 1b. Bounded — a shedding site exists (recorded so the boundary is named)

| Field | Shed by | Bound |
| --- | --- | --- |
| `offered_markers` `state.rs:227` | `commit_marker_ack` `ops_acks.rs:472` | outstanding unacked markers |
| `tokens` `state.rs:244` | `release_drained_binding_slot` `ops_terminal_drain.rs:406` | live enrollments |
| `slots` `state.rs:233` | `commit_drained_detached_slot` `ops_terminal_drain.rs:427` | live slots |
| `pending_specific_fates` `state.rs:235` | `complete_pending_specific_fate` `binding_fate_completion.rs:136` | open intents |
| `prepared_ordinary_finalizers` `state.rs:237` | `complete_prepared_ordinary_finalizer` `binding_fate_completion.rs:441` | open finalizers |
| `Slot::attach_provenance` `state.rs:179` | `install_attach_receipt` `ops_attach.rs:601`; `prune_expired_provenance` `occupancy.rs:64` | configured displacement window |
| `LiveFrontierOwner::retained_charges` `live_frontier.rs:72` | `install_binding_fate_transition` `live_frontier/binding_fate_transition.rs:68` | `retained_record_limit` |

`offered_markers` is called out because the dispatch named it as part of the
growth family. It has a shedding site and is bounded by unacked markers, not by
history — a liveness question, not a growth one. It is not a #65 structure.

### 1c. The numbers

From `gate-logs/p65-live-owner-growth/red-proof-inverted-to-the-bound.log`,
across histories 32 → 96 (64 further admissions of 256 payload bytes each,
two enrolled participants, no drain — both histories sit under
`max_retained_record_rows = 1_024`):

| structure | at N=32 | at N=96 | slope |
| --- | --- | --- | --- |
| `committed_admissions` entries | 32 | 96 | **exactly 1.00 per admission** |
| `source_batches` entries | 34 | 98 | 1.00 per admission (+2 enrollments) |
| `all_obligations` sequences | 33 | 97 | 1.00 per admission (+1) |
| retained batch BYTES | — | — | ≥ 16,384 over the 64, i.e. ≥ one full copy of every record body |
| `observer_progress_witnesses` | 0 | 0 | **0.00 — see §3** |

The slope on `committed_admissions` is exactly one, not merely positive: the
map is a permanent copy of the conversation's admission history, one key per
record, with no reuse and no displacement.

## 2. The pins

`crates/liminal-server/src/server/participant/production/tests_p65_live_owner_growth.rs`,
registered at `production/mod.rs:172`.

| pin | holds |
| --- | --- |
| `every_committed_admission_permanently_occupies_the_dedup_map` | slope on #1 is exactly 1 entry/admission |
| `the_outbox_retains_every_produced_batch_body_for_the_owners_life` | #2 grows in entries AND in bytes, floored by the payloads themselves |
| `discharged_records_leave_their_obligation_sequences_behind` | #3 grows one sequence per admission |
| `admissions_project_no_observer_progress_witnesses_on_either_path` | the §3 correction, live and restored |

They are green today **because the growth is present**, on the exact precedent
`tests_p0_60_admission_cost` set: that module's pins asserted #60's defect and
were inverted by the lane that fixed it (`tests_p0_60_admission_cost.rs:251`).
A lane that bounds one of these structures inverts its pin here to the ceiling
it chose. A green is the statement "the ceiling is still where #65 measured
it".

**Red proof.** Each growth pin was inverted to the bound it would assert after
a fix (`growth == 0`) and run at base:
`gate-logs/p65-live-owner-growth/red-proof-inverted-to-the-bound.log`,
`TRUE_EXIT=101`, three of four FAILED by name. The fourth
(`admissions_project_no_observer_progress_witnesses_on_either_path`) stayed
green under inversion because zero IS its measured value — which is the §3
correction, not an instrument failure. The green form is
`gate-logs/p65-live-owner-growth/p65-pins.log`, `TRUE_EXIT=0`.

**Positive controls.** Every pin proves, through the same probe and inside the
same fixture, that `next_seq` and `next_log_sequence` moved by at least the
admissions it claims to have driven. This is load-bearing rather than
ceremonial: a fixture that admitted nothing produces flat counters, so the
FLAT assertions the bounding lane will write here would pass vacuously on a
dead fixture. The control is what makes the future inversion a measurement.

## 3. The correction — the witness vector does not grow with admissions

The dispatch named `observer_progress_witnesses` as part of the admission
growth family. **It measures at zero**, at both histories, on the live path and
after a cold restore.

The reason is structural. An ordinary `RecordAdmission` projects no observer
progress at all: the bracketing calls that produce witnesses
(`begin/end_observer_progress_source`, `state.rs:571,577`) sit on the replay of
progress-bearing sources — binding fates, leaves, marker acks — and board #60
§2 records that they are "called from no live commit site at all". An
admission-only history contains no such source, so the vector stays empty
whether or not the owner has been restored.

This is stop clause 5, and it is reported rather than built around: a bound
written for this structure on the admission path would have been a bound for
growth that does not happen.

**What the correction does NOT clear.** `ObserverProgressWitnessState` still
has no shedding site on any path (#8, #9, #10 above), and board #60 §3c
deliberately converted this vector from a drain to a borrow
(`state.rs:588-597`, in the accessor's own doc) so that a commit would not have
to replay the log to rebuild it. Under a workload that DOES project observer
progress — fates, leaves, marker acks — the growth family is live and this lane
did not measure it. Worse than memory: `complete_live_commit` hands the WHOLE
slice to `reconcile_observer_progress` on every commit (`handler.rs:664`), so
after such a workload the per-commit fold is Θ(sources so far). Board #60
removed a Θ(N) durable read from the commit path; a Θ(N) in-memory fold is
still on it. **Named residue for a fate/ack-shaped lane, not a cleared
structure.**

## 4. Why no bound landed — the stop clauses, per structure

### 4a. `committed_admissions` — STOP: contract text (clause 2)

Bounding this map narrows the A2/A4 dedup window. The participant contract
fixes that window explicitly:

> RETENTION HONESTY: the dedup window is the retained op-log window. A
> re-present arriving after its witness row is compacted commits a second copy
> exactly as today. The window is NAMED, not silent. **No server-side op-log
> compaction exists at the time of this amendment**, so the boundary is not
> mechanically armable as a pin; it is declared here and must gain a pin when
> compaction gains an implementation.
> — `docs/design/PARTICIPANT-CONTRACT.md:474-479`

Read at the bytes, that sentence is why the map is unbounded and why a build
cannot bound it. The contract does not say "the window is 1,024 records"; it
says the window IS the op log, and then records that nothing compacts the op
log. So today the declared window and the whole durable history are the same
object, and the implementation is CONFORMING. Introducing a narrower dedup
window makes the two diverge — it is a change to that sentence, and A2 is a
ratified two-key amendment (`PARTICIPANT-CONTRACT.md:557`).

A second edge sits under the same map: A4's same-participant body-conflict
refusal reads it (`ops_frontier.rs:165`). Evicting an entry silently converts a
typed `attempt_token_body_conflict` refusal into a fresh commit. That behaviour
is pinned by
`tests_a4_body_conflict::a_cross_participant_token_hit_still_commits_with_no_refusal`,
so any eviction policy also engages stop clause 4.

**Honesty — the lazy re-derivation that looks like a way out, and is not.**
The map is genuinely re-derivable: `publish_replayed_record_admission`
(`ops_frontier.rs:725`) is literally the re-derivation function, and it rebuilds
the map from durable rows on every replay. So a bounded cache with lazy
re-derivation on miss would preserve today's answers exactly. It must not be
built, and the reason is arithmetic rather than taste: **a normal admission IS
a dedup miss.** Every honest first-time admission would take the miss path and
pay an O(N) durable scan to prove the token is new — reintroducing precisely
the Θ(N) per-admission cost that board #60 §3c removed, and turning
`tests_p0_60_admission_cost::admission_cost_does_not_grow_with_history` red.
The cheap-looking cache is a straight regression of the lane before this one.

Bounding this map without that regression needs a durable token→sequence index
— new durable state, and p0-60 §4 already ruled that a checkpoint artifact
belongs to the lane with the catch-up requirement, not to this side of the
seam. **Seat ruling needed:** does the estate want a narrower, named dedup
window (contract edit + A4 pin change), or a durable index (new stream)?

### 4b. `source_batches` and `all_obligations` — STOP: non-re-derivable eviction (clause 3)

These are the strongest bound candidates and the closest to free. Both have a
neighbour, `records`, that IS reclaimed on full discharge
(`reclaim_empty_records`, `outbox.rs:458`), and neither is reclaimed with it.
The shape of a fix is obvious — shed alongside `records`.

What stops it here is that neither eviction is provably answer-preserving from
this lane's evidence:

- `source_batches` is read as a conflicting-source idempotence check
  (`apply_produced`, `outbox.rs:249`): a source sequence re-produced with
  DIFFERENT canonical bytes must be refused. Evicting a discharged source
  removes the evidence for that refusal. Whether a source can be re-produced
  after discharge is a durability question about the Unit 2 extension stream's
  replay contract, not a fact this lane measured.
- `all_obligations` is read by `recipient_ack_obligations`
  (`outbox/selection.rs:57`) and by the capacity measurements
  (`outbox/selection.rs:210-250`). If any consumer reads it as a CUMULATIVE
  count rather than an outstanding one, shedding changes reported capacity
  numbers rather than just memory.

Both are answerable, and both answers are rulings about what the outbox owes
after discharge. **Seat ruling needed:** may a fully discharged, reclaimed
record's source batch and obligation sequences be dropped, and if so, is the
post-discharge re-production of a source a refusable event or an impossible
one?

### 4c. `FateOccurrenceRouter::occurrences` — STOP: doc/code divergence (clause 3)

`state.rs:238-239` says it "retains active keys, never a copy of history".
There is no removal site. Either the doc is wrong or the code is; deciding
which is a correctness question about four-class occurrence routing, and the
router's conflict detection (`FateOccurrenceConflict`) is exactly the kind of
guard that a wrong answer disables silently.

## 5. Honesty section (no-silent-tradeoffs)

Stated plainly, per the house rule:

1. **This lane bounds nothing.** Memory growth is unchanged at every structure.
   The deliverable is measurement and pins.
2. **The pins cost battery time.** Four pins, ~150 s wall clock, dominated by
   two 96-admission histories each built through the full dispatch seam. They
   run in the default battery. Cheaper histories were rejected: a slope needs
   two points far enough apart that fixed footprint cancels, and 32/96 is
   already the minimum that puts the 1.00 slope beyond rounding.
3. **The pins measure entry counts and owned bytes, not RSS.** Entry count is
   the honest proxy for structures whose element size is fixed;
   `source_batches` is measured in BYTES because its elements are record bodies
   and an entry count would understate it by the size of the payload. No pin
   claims a process-level memory figure.
4. **Only the admission axis was measured.** Fates, leaves, marker acks and
   attach/detach traffic each drive other rows in §1a and were not exercised.
   §3's correction is scoped to admissions and says so.
5. **The re-derivation cost is stated where it bites** (§4a): re-derivable does
   not mean cheap to re-derive, and for the dedup map the miss path is the
   common path.

## 6. What is left red, and for whom

Nothing is left red. The four pins are green because the growth they measure is
present; the battery's only failures remain the declared `tests_f8_marker_poison`
instrument pair.

Left for the seat, in the order a lane would take them:

1. **`source_batches` / `all_obligations`** (§4b) — smallest ruling, largest
   byte win, no contract surface. The likely first bounding lane.
2. **`FateOccurrenceRouter`** (§4c) — a doc/code divergence to resolve before
   it is relied upon.
3. **The observer witness family under a fate/ack workload** (§3) — unmeasured,
   and carries a Θ(N) per-commit fold as well as memory.
4. **`committed_admissions`** (§4a) — needs a contract decision or a durable
   index. It is the structure the board named and the one furthest from a
   build.
