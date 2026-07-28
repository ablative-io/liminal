# Wiring ledger — dormant machinery and its roads back

- **Revision:** r1.10, 2026-07-28 — **doc-truing sweep**. No lane opened or
  closed by ruling; every row authored at `eb3ae30` (2026-07-19) was re-derived
  at the bytes against main `1a0b60c`, and each stale row gained a dated
  correction block. Corrections are recorded FORWARD: no original claim was
  erased, so every corrected row carries both what it said and what is true.
  Two rows were found **stale at authorship** (W5, W6) — see the W5 lesson.
  New dated-annotation section at the foot for facts the ledger predates.
  (Prior: r1.9, 2026-07-20 — W1b scope EXTENDED to Detached sources, W1b
  pre-review findings 3+5.) Owner of the ledger: Waffles
  (coordination seat).
  Lane owner unless stated otherwise: Hermes Crumpet (liminal seat).
- **Why this exists:** the F-0c Unit 2 fold minted the unwired-seam sweep as a
  mandatory discipline: every entry point a branch adds either has a production
  caller at bytes, or is declared dormant with a **named future consumer**. This
  ledger is the companion discipline — the register that keeps every dormant
  declaration honest with a trigger, an owner, and an oracle floor. It also
  answers the standing ask from Annabel's machine (via Hermes's consolidation)
  to enumerate liminal's unwired machinery, and Tom's go: the wiring program
  continues.

## The two rules every lane obeys

1. **Wire-with-oracle.** A lane is done when the seam has a production caller
   AND a named oracle test proving the wired behaviour. Wiring without its
   oracle fails the tear. (Minted in the Unit 2 fold; standing here for every
   future lane.)
2. **No row, no dormancy.** A seam may ship dormant only if it has a row here
   (or in a successor register) carrying: named future consumer, build trigger,
   owner, oracle floor. A dormant seam without a row is a finding at any tear.
   (The scheduled-road-back rule, Tom-ratified 2026-07-19.)

## Lanes

### W1 — PHASED r1.6 into W1a (wire what exists) and W1b (fate sources)

The W1 pre-review (session c769ccf3) found the ruled disposition
UNIMPLEMENTABLE for three of the four arms, verified at the coordination
seat: the production `StoredOperation` enum has NO Died/Ordinary/Recovered
variants (`log.rs` — zero grep hits), the replay match has no BindingFate
branch (`ops_session.rs`), and `BindingFateOperation` is a protocol codec
with no production durable home. No source rows exist for those fates to
flush at the §8 barrier, and cold replay cannot reconstruct their
projections. The r1.5 "Advance is the only new persisted output, no new
row shape" disposition is EXPRESSLY AMENDED: it stands for W1a and is
superseded for the fate sources, whose creation is W1b's design scope.

#### W1a — CLOSED r1.8 (landed 8ce73bf)

Torn at the coordination seat 2026-07-20: eight-oracle census verbatim
(one hit each), plain-arm projection removed from the protocol with the
trybuild compile-fail fixture proving it, refusals mutation-free via
durable observer-rows equality, witness vector = the pre-existing
projection queue enriched with typed provenance (lineage map is
participant-bounded — no W7-species materialization), docs commit exactly
one (§9.1, Apollo's phrasing verbatim). Battery green my hands
(fmt/check/clippy/test workspace). The §8 reconcile-conformance
disclosure-class gap on main is REPAIRED by this landing. Tear rider
carried to W1b's first touch: the four-counter tuple assertion in
`same_participant_ack_lineage_regression_refuses_before_observer_mutation`
compares freshly-declared zeros to zeros (tautological decoration; the
durable-rows equality is the real proof) — wire the counters or delete
the tuple.

##### Correction to the r1.8 W1a tear rider (2026-07-28 sweep)

**DISCHARGED — by deletion, the option the rider itself offered.** The rider
read "wire the counters or delete the tuple". The tautological four-counter
tuple (four freshly-declared zeros compared to `(0,0,0,0)`) was real at
`b867ec8` (`tests_w1a.rs:332-339` at that tree) and was removed by `38a7900`
(2026-07-21, "test(server): discharge W1b tear rider census"). The test
`same_participant_ack_lineage_regression_refuses_before_observer_mutation`
survives at `production/tests_w1a.rs:308-332`, ending on the durable
observer-rows equality assert (`:327-331`) — the real proof, with the
decoration gone.

Not established by this sweep: the name-by-name mapping behind the r1.8
"eight-oracle census verbatim". `tests_w1a.rs` carries six `#[test]` fns both
at `b867ec8` and today; the remaining two names were not reconcilable from the
bytes. Left untouched rather than guessed.

#### W1a — original row (historical)
- **Scope:** the canonical Leave producer ruling (one producer per fate,
  single-presentation oracle, r1.5) applied to the ONLY wireable arm; PLUS
  the §8 reconcile-conformance repair: production today silently tolerates
  nonmonotone and disagreeing sources (`record_observer_progress_projection`
  stores a max while queueing all values, `state.rs:267-275`; reconcile
  continues on `current >= presented`, `handler_observer.rs:275-290`) where
  §8 requires a loud refusal. Same species as the W3 row-R gap —
  DISCLOSURE-CLASS on main independent of W1, severity recorded here, home
  ruled: the validation lands WITH W1a (same reconcile path).
- **Oracle floor:** single-presentation oracle for the surviving Leave
  producer (fails if both arms present); refusal oracles including
  PER-LINEAGE regression and unsupported ahead-Advance arms (r1.7: a
  globally-decreasing sequence can be legal multi-participant history —
  per-participant cursors, no global floor, `ops_acks.rs:162-207`; the
  W1a validation model is per-lineage monotonicity + running-maxima
  witness, final progress = max over the witness set); cold-repair oracle
  makes `apply_observer_recovery` the FIRST touch; Leave-duplicate coverage
  via structural-absence check + `cfg(test)` duplicate-injection seam.
- **Owner:** Hermes (brief r2 folds findings 2, 3, 5 at his seat).

#### W1b — Died/Ordinary/Recovered/Detached durable source rows (design-first)
- **Scope EXTENDED r1.9 (pre-review findings 3+5, verified at the
  coordination seat):** orderly close today leaves durable participant
  bindings `Bound` — `Frame::Disconnect` returns `Close` with no binding
  disposition (`apply.rs`), while the exact `clean_disconnect` /
  `server_shutdown` Detached producers sit dormant
  (`binding.rs:721-753`). That was a live no-row-no-dormancy violation at
  the bytes; this extension is its repair-in-place. Rationale for extend
  over successor row: a fate-sources lane that leaves the commonest fate
  (orderly close) undispositioned is incoherent, the schema/replay/witness
  design work is shared, and the brief's Pending-Detached oracles gain a
  real producer instead of being cut.
- **What is missing:** the fate `StoredOperation` variants (now four
  fate classes), their replay transitions, and their §8 flush barriers —
  never built.
- **Named consumer:** the full §8 crash-fate window repair (what W1's
  original row wrongly presumed already had sources).
- **Trigger:** design brief at Hermes's seat, its own review round — the
  open decisions (schema version, migration-or-refusal rule, which live
  paths emit Died rows at all) are design decisions, not fold riders.
- **Oracle floor:** set by the W1b design brief; at minimum per-fate
  append/replay/flush oracles and the cold-reconstruction path.
- **Owner:** Hermes. (No-row-no-dormancy: this row is the road back.)

##### Correction to W1b (2026-07-28 sweep, verified at main `1a0b60c`)

Not an `eb3ae30` row — this is r1.9 (2026-07-20) — but the sweep found it stale
and the same discipline applies. Verdict: **true when written, repaired the very
next day.** All three of the row's open claims are closed at the bytes:

- **"`Frame::Disconnect` returns `Close` with no binding disposition
  (`apply.rs`)" — FALSE now.** `crates/liminal-server/src/server/connection/apply.rs:58-60`
  returns `FrameAction::CloseWithFate(ConnectionFateClass::CleanDisconnect)`.
  `CloseWithFate` is defined at `connection/state.rs:204` / `:222` and consumed
  at `connection/process.rs:370` (`finish_fate_close`) and `:986-987`, with the
  WS leg at `connection/websocket/process.rs:383,:523`. Orderly close therefore
  lands `BindingState::Detached` (`lifecycle/binding.rs:598-606`), not a
  retained `Bound`. Changed by `ebb8aaa` (2026-07-21) — the sole commit
  touching `CloseWithFate` in `apply.rs`.
- **"the exact `clean_disconnect` / `server_shutdown` Detached producers sit
  dormant (`binding.rs:721-753`)" — FALSE now, on both the location and the
  dormancy.** They are at `lifecycle/binding.rs:735` and `:761` (the r1.9 line
  range no longer covers `server_shutdown`), and carry four production callers:
  `production/connection_fate_rows.rs:47` and `:62` (live append),
  `production/connection_fate_replay.rs:33` and `:34` (cold replay). Reached
  from production via `connection_fate.rs:247`, with
  `ConnectionFateClass::ServerShutdown` raised at `connection/process.rs:724`
  and `connection/websocket/process.rs:900`. The module is declared with no
  `cfg(test)` gate. Wired by `79a5ca6` / `c06bda8` / `1b03e50`, all 2026-07-21.
  **The no-row-no-dormancy violation this extension existed to repair is
  repaired.**
- **"the fate `StoredOperation` variants … never built" — FALSE now.** All four
  are live durable tags in
  `crates/liminal-server/src/server/participant/production/log_v3.rs`:
  `Died { row: StoredDied }` (:38-40), `Detached { row: StoredDetached }`
  (:42-44), `Ordinary { row: StoredOrdinaryFate, event }` (:46-50),
  `Recovered { row: StoredRecoveredFate, event }` (:52-56), under a doc comment
  at `:17-20` stating "The four fate variants are deliberately distinct durable
  tags." Their replay transitions exist too:
  `binding_fate_completion.rs:351-390` (`replay_specific_fate`, Ordinary and
  Recovered) and `connection_fate_replay.rs` (Died and Detached). Landed by
  `87caef4` (2026-07-20, "feat(server): add W1b durable fate substrate") with
  follow-ons `12353f6` / `f346d2a` / `b653aa3` / `cd21da2` (2026-07-21),
  `e25fa72` (2026-07-22), `4c6fa9b` (2026-07-23), `44686d8` (2026-07-24).

Method finding worth keeping, because it is the W5 failure mode in a
politer form: the W1 phasing preamble's evidence — "the production
`StoredOperation` enum has NO Died/Ordinary/Recovered variants (`log.rs` — zero
grep hits)" — was **literally true and substantively misleading even when
written.** `log.rs:306` is only `pub(super) type StoredOperation =
StoredOperationV3;`; the grammar lives in `log_v3.rs`. A zero-hit grep against
an alias is not an absence proof. Recorded, not corrected — the r1.9 conclusion
(the variants did not exist yet) happened to be right.

**Whether W1b is discharged is a ruling, not a sweep finding.** This sweep
records the bytes only; the lane's closure is the owner's call.

##### W1b — RULED CLOSED (Waffles the Terrible, ledger owner, 2026-07-28)

The third leg the closure was held on — the §8 flush barriers — is evidenced
at the artifact level and the ruling is made. Closing evidence, re-walked at
the owner's hands on the two load-bearing hops: every production fate-row
append funnels through the synchronous bridge into
`production/log.rs:238-248`, where `store.append` is immediately followed by
`self.store.flush().await?` (:248 — "the flush is the durability barrier the
caller's pending shell commit waits behind"); at the Died/Detached site
(`connection_fate.rs:256`) and through `commit_through_barrier`
(`barrier.rs:138/:149`) for Ordinary/Recovered, ALL in-memory state advance is
textually and causally after the `?`-propagated append, so a failed flush
aborts before anything moves. Same append-then-flush shape as the observer log
(`observer.rs:88-91/:98`) and outbox log (`outbox_log.rs:580-583/:590`) —
class parity, not a weaker discipline. The terminal disk hop is haematite
0.7.0's atomic commit point (temp → fsync → rename → dir fsync). §8's second
leg (the `ObserverRow::Advance` append/flush + cold reconcile) is wired at
`handler.rs:366-372` → `handler_observer_reconcile.rs:285-295`. The row's own
oracle floor — per-fate append/replay/flush oracles and the cold-reconstruction
path — is discharged by `tests_w1b_source_flush.rs` (`source_flush_precedes_advance`
:123 with its `SourceCutAppender` fault injection; entry points :222/:227/:232).
The earlier grep's zero hits are explained by file selection:
`connection_fate_rows.rs` constructs rows and never appends; `log_v3.rs` is
schema grammar; the flush is one module over.

**Not verified by this sweep:** the 2026-07-24 Detached-drain landing
(`6d09bae`) does **not** wire these producers — `bbb3ace` / `6f3febb` /
`4a5de6a` touch `ops_session_replay.rs`, `ops_terminal_drain.rs` and their
suites only, none of `connection_fate_rows.rs` / `connection_fate_replay.rs` /
`apply.rs`. The candidate-lane terminal drain is a separate seam; the
clean_disconnect/server_shutdown wiring predates it by three days. Stated
because the two are easy to conflate.

#### Original W1 row (historical, premise superseded by the phasing above)
- **What sits dormant:** the `BindingFate` observer projection arms
  (`Died` / `Ordinary` / `Recovered` / `LeaveCommit`), landed with Unit 2,
  zero production callers (declared in the Unit 2 Census A, verified at my
  tear of `7a9b2cb`).
- **Named consumer (NARROWED r1.5, W1 ground scout):** the §8
  observer-progress crash-window repair. The prior "crash repository reads
  the four projections" premise was payload-false at the bytes: all four
  arms surrender the same sealed two-field
  `ObserverProgressProjection { conversation_id, new_observer_progress }`
  (`liminal-protocol/src/lifecycle/observer_recovery.rs`) — fate class,
  cause, participant, and epoch are ERASED, so progress repair is all the
  projection can drive. Full crash-fate persistence (what typed source rows,
  if any, are preserved) must be an EXPLICIT section of the W1 brief — ruled
  a required brief section, not smuggled into a projection that cannot carry
  it. (Same discipline as the W3 r1.1 narrowing.)
- **Canonical-producer rule (r1.5):** production already consumes a
  semantically duplicate leave projection via `LiveLeaveCommit`
  (`live_frontier.rs`, wired at `ops_leave.rs`) while this row names the
  dormant `LeaveCommit` arm. The W1 brief must rule ONE canonical producer
  per fate, with an oracle proving single-presentation (tolerance of double
  presentation by `current >= presented` is an accident, not a design);
  this row's arm naming amends to whichever producer survives.
- **Trigger:** the crash-window repair consumer moving to production use.
- **Oracle floor:** per-arm projection tests (each fate arm drives its
  projection and asserts the projected row; no shared fixture shortcuts).
- **Normative §8 source (r1.5):** the Unit 2 brief is on main as historical
  record at `docs/design/F0C-UNIT2-SERVERPUSH-PRODUCER.md` (brought verbatim
  from preserved branch tip `0cdff85`; content sha256 `98f9130f…`). Lane
  briefs cite the main path.

##### Correction to the original W1 row (2026-07-28 sweep, verified at main `1a0b60c`)

Audit verdict: **accurate when authored, STALE SINCE.** The "zero production
callers" census was correct at `eb3ae30` (2026-07-19 15:52) — no fate-arm
projection call existed anywhere in `crates/liminal-server/` at that tree. It
is no longer correct:

- **`LeaveCommit` arm — DELETED**, not wired. Removed by `1c30f16`
  (2026-07-19 23:20, "Remove duplicate Leave projection producer"), roughly
  seven hours after this row was written, discharging the r1.5
  canonical-producer rule in favour of `LiveLeaveCommit`
  (`operations/live_frontier.rs:745`, wired at
  `production/ops_leave.rs:580`). Enforced by the trybuild compile-fail fixture
  `crates/liminal-server/tests/trybuild/plain_leave_projection_removed.rs`,
  registered at `production/tests_w1a.rs:263`.
- **`Died` — WIRED, two production callers**: `production/connection_fate_rows.rs:163`
  (live append) and `production/connection_fate_replay.rs:161` (cold replay).
- **`Recovered` — WIRED, one production caller**:
  `production/binding_fate_completion.rs:256`, inside
  `append_recovered_binding_fate` (:239).
- **`Ordinary` — STILL ZERO production callers.** This is the only surviving
  part of the original claim. `append_ordinary_binding_fate`
  (`binding_fate_completion.rs:192-238`) deliberately does not call it — the
  Died source row already carried the progress — and
  `MeasuredBindingFate::observer_progress_projection`
  (`operations/binding_fate.rs:43-48`), the only other route in, has callers
  only in `operations/binding_fate_tests.rs`. **W1 is NOT discharged; it has
  narrowed to the Ordinary arm alone.**
- **A FIFTH arm exists that this row never named:**
  `DetachedBindingTransition::observer_progress_projection`
  (`lifecycle/binding.rs:586`), with two production callers
  (`connection_fate_rows.rs:138`, `connection_fate_replay.rs:36`). It did not
  exist at `eb3ae30`.

Wiring commits, all 2026-07-21: `79a5ca6` (append connection fate sources),
`ebb8aaa` (fold live connection fates before teardown), `c06bda8` (complete
measured binding fates), `1b03e50` (replay durable fate sources), `8c48c95`
(repair terminal fate witnesses).

The r1.5 sealed-projection amendment is **still true at the bytes** —
`ObserverProgressProjection` remains the two private fields
`conversation_id` / `new_observer_progress` with a `pub(in crate::lifecycle)`
constructor and no `Clone` (`lifecycle/observer_recovery.rs:22-37`). One
qualification for whoever picks up the Ordinary arm: the *rationale* ("progress
repair is all the projection can drive") no longer describes production, because
a typed sidecar now travels beside every projection —
`ObserverProgressSourceMetadata::died(…)` / `::detached(…)` /
`::recovered_binding_fate(…)` (`connection_fate.rs:376-401`,
`connection_fate_replay.rs:174-184`, `binding_fate_completion.rs:283-290`). The
erasure is real in the struct and routed around in production.

Citation-quality note, recorded not corrected: this row and the r1.5 amendment
both cite `lifecycle/observer_recovery.rs` for the arms. The arms have never
lived there — that file holds the projection struct only; the arms are in
`binding.rs` and `edge.rs`. The citation was loose at authorship.

### W2 — Nonzero-debt ack-obligations pair
- **What sits dormant:** the nonzero-debt ack obligations pair landed with
  Unit 2; its scalar sibling is equally uncalled (Census A verified this is
  NOT the item-28 relocation pattern — genuinely awaiting its consumer).
- **Named consumer:** the dispatch arm that consumes obligation debt at
  delivery decision time.
- **Trigger:** the dispatch arm's build (first unit that schedules deliveries
  against obligation debt).
- **Oracle floor:** dispatch-arm tests exercising both the nonzero-debt path
  and the scalar path against the same fixture, asserting they cannot diverge.

#### Correction to W2 (2026-07-28 sweep, verified at main `1a0b60c`)

Audit verdict: **accurate when authored, STALE SINCE 2026-07-22. Wired at the
bytes, undischarged in this ledger — the register was simply never updated.**

The pair the row names (per `docs/design/W2-OBLIGATION-DEBT-DISPATCH.md:47-55`)
is `apply_nonzero_participant_ack_with_obligations`
(`liminal-protocol/src/lifecycle/operations/nonzero_participant_ack.rs:313`)
and its scalar sibling `apply_nonzero_participant_ack` (same file, `:289`).
Both now have production callers inside `select_conforming_nonzero_ack`
(`liminal-server/src/server/participant/production/ops_nonzero_ack.rs:350`) —
the obligation-aware member at `:353`, the scalar at `:382`. Production
reachability: `select_conforming_nonzero_ack` is called at
`ops_nonzero_ack.rs:120` and `:204`, both inside the `pub(super)` method
`ConversationAuthority::apply_nonzero_ack_with_impact` (`:84`); the module is
declared with no `cfg(test)` gate (`production/mod.rs:57`). A third helper,
`scalar_audit_for_recipient_endpoint` (`nonzero_participant_ack.rs:340`), is
production-called at `ops_nonzero_ack.rs:365`.

**The oracle floor is met in the strongest available form — in production, not
only in tests.** `ops_nonzero_ack.rs:388-393` returns
`StateError::invariant("nonzero ack obligation and scalar selectors diverged")`
when the two selectors disagree: the "asserting they cannot diverge" clause is
enforced on every live ack, not merely fixture-checked.

Wired by `e25fa72` (2026-07-22, "feat(w2): consume both nonzero ack paths") —
the sole commit introducing either symbol into `crates/liminal-server/`.
Formal discharge of the lane is the owner's ruling, not this sweep's.

##### W2 — RULED CLOSED (Waffles the Terrible, ledger owner, 2026-07-28)

Closed on the owner's own byte-walk of `select_conforming_nonzero_ack`: both
selectors production-called, the divergence invariant returned when they
disagree. The oracle floor asked for fixture tests asserting non-divergence; a
production invariant enforced on every live ack is the stronger form and
supersedes the fixture formulation. The trigger (the dispatch arm consuming
obligation debt, `e25fa72`) has occurred.

### W3 — Apply-per-page restore (row R) — CLOSED r1.4 (landed 9dca3a3)
- **Closure (2026-07-20, coordination-seat tear):** production restore now
  runs the bounded validate-then-apply two-pass (`bbe25d0`), the eleven-oracle
  census landed in `tests_w3_restore.rs` (`b31fc6d`, accounting `9dca3a3`),
  and `read_all` survives only as a `#[cfg(test)]` frozen pre-W3 reference
  with no production selector or fallback. Battery green at the tear
  (fmt/check/clippy/test, oracle census 0 absent, floor files byte-identical).
  Equivalence claim on record: stable-read durable-state equivalence (the
  two-pass adds fallible production reads — loud-and-earlier failure as a
  design position), expressly superseding the earlier "zero observable
  contract change" phrasing; see the brief's supersession record.
- **What sat open:** `spec:570 total-restore-streaming` — `read_all`
  materialized the full decoded stream; only the 64-row page size was
  enforced. Disclosed in the Unit 2 declaration under its own line;
  disposition Tom-ratified (disclose-with-teeth).
- **Scope (narrowed 2026-07-19, W3 pre-review finding 1):** W3 removes the
  duplicate aggregate materialization (the `read_all` Vec) ONLY. The brief's
  original "safe for unbounded history" claim was FALSE at the bytes — the
  restored authority itself retains history-linear indexes (see W7). W3
  does NOT alone discharge the unbounded-history trigger.
- **Named consumer:** restore path under unbounded outbox history.
- **Trigger:** HARD, SHARED WITH W7 — before any deployment with unbounded
  outbox history, BOTH W3 and W7 must be discharged. Stated on both rows so
  neither landing alone can be read as unblocking unbounded history.
- **Oracle floor:** the apply-per-page brief's acceptance re-runs the 24/30
  determinism oracles (the ratified floor), PLUS a retained-authority-counts
  oracle measuring the narrowed claim (what memory the restored authority
  actually holds), not asserting it. Error precedence for multiply-invalid
  durable states is preserved EXACTLY via a bounded validate-then-apply
  two-pass (one-page peak, zero observable contract change) — ruled at the
  coordination seat 2026-07-19; a new error order would have been a contract
  change needing Tom.
- **Owner:** Hermes (brief r2 fold in flight).

#### Correction to W3 (2026-07-28 sweep, verified at main `1a0b60c`)

Audit verdict on the ORIGINAL claim: **accurate when authored, stale ninety-five
minutes later the same day, and correctly closed at r1.4.** At `eb3ae30`
(2026-07-19 15:52) `read_all` was ungated production code accumulating every
decoded row into one `Vec` across pages
(`git show eb3ae30:…/production/outbox_log.rs:287-288` — "Reads and decodes the
complete stream before any owner is published"). `bbe25d0` landed 17:27 the
same day and is not an ancestor of `eb3ae30`. The row was honest; the lane was
simply fast.

The r1.4 closure claims **verify at the bytes**:
- bounded validate-then-apply two-pass in production — pass 1
  `block_on(outbox_log.restore_cursor().validate_all())` at
  `production/handler.rs:441` inside `replay_and_repair` (:435); pass 2
  `ConversationAuthority::replay(…)` at `:444-449`, driving `ExtensionMerge`
  over a streaming `OutboxRestoreCursor` (`production/outbox_replay.rs:23-42`,
  cursor at `:25`, constructed `:38`). One page held at a time —
  `outbox_log.rs:385-393`, `load_page` at `:418` reading at most
  `UNIT2_OUTBOX_RESTORE_BATCH_ROWS` into one `VecDeque` (`:435`).
- `read_all` survives only as a frozen `#[cfg(test)]` reference — attribute at
  `outbox_log.rs:599`, doc "Production restore has no selector or fallback to
  this implementation". Its one caller outside `tests_*` files,
  `replay_aggregate_reference`, is itself `#[cfg(test)]` (`handler.rs:501`).
  **No production selector or fallback survives.**
- `bbe25d0` / `b31fc6d` / `9dca3a3` all exist, all 2026-07-19, subjects
  matching their stated roles.

**One imprecision in the r1.4 closure sentence, corrected forward.** It reads
"the eleven-oracle census landed in `tests_w3_restore.rs`". At the bytes,
**nine** of the eleven landed there (`tests_w3_restore.rs` carries nine
`#[test]` fns — `:90`, `:115`, `:153`, `:242`, `:304`, `:344`, `:452`, `:528`,
`:556`; the file's other seven `fn`s are helpers). Census items 1 and 2 are
what `docs/design/W3-APPLY-PER-PAGE-RESTORE.md:443-444` calls "Rerun landed
item 24/30 unchanged" — pre-existing oracles living elsewhere, both verified
present: `cold_reopen_reconciles_and_replays_all_record_shapes`
(`production/e2e_cold_all_shapes.rs:455`) and
`leave_discharge_replays_deterministically_across_the_commit_boundary`
(`production/e2e_leave_commit_boundary.rs:439`). **A location defect, not a
coverage defect** — all eleven named oracles exist. The closure stands.

### W7 — Authority restore history-linear indexes (opened by W3 pre-review)
- **What sits open:** the restored `ConversationOutbox` retains
  history-linear indexes — `source_batches` / `ack_sources` /
  `all_obligations` (`outbox.rs:124-137`; inserts `:205-252`, `:298-325`,
  `:262-270`; reclamation removes live records only, `:330-395`). Restore
  memory is Θ(history) with or without W3.
- **Route census (r1.2):** FOUR restore routes reach these indexes, not
  three — the ObserverRecovery pre-pass on absent owner runs
  `replay_and_repair` (`handler_observer.rs:357-364`) →
  `ConversationAuthority::replay` (`handler.rs:250-268`) → full
  `ConversationOutbox` reconstruction (`ops_session.rs:270-349`,
  `outbox_replay.rs:20-33,71-95,120-136`), reconstructing all three
  indexes **for the corresponding row shapes** (r1.3 precision:
  `apply_row` branches by kind — only `Produced` rows feed
  `source_batches`/`all_obligations` (`outbox.rs:205-270`), only
  `AckAdvanced` feeds `ack_sources` (`:298-325`), `MarkerAck` feeds
  none). Four-route inheritance SETTLED — survived the independent
  re-trace (W3 re-review, session 16e12546). W7's bounding design must
  cover all four routes, and any all-three-coverage fixture must
  construct the row classes that actually feed each index.
- **Named consumer:** any deployment with unbounded outbox history.
- **Trigger:** HARD, SHARED WITH W3 — before any deployment with unbounded
  outbox history, BOTH W3 and W7 must be discharged. Stated on both rows so
  neither landing alone can be read as unblocking unbounded history.
  (r1.3: wording now verbatim on this row — the prior by-reference form
  violated the byte-parity requirement of the r1.1 ruling itself.)
- **Oracle floor:** its own bounding design brief (index compaction /
  reconstruction touches ack + conflict semantics — a design-first lane,
  NOT foldable into W3); acceptance includes the retained-authority-counts
  oracle family measuring each index under bounded and unbounded fixtures.
- **Owner:** Hermes.

#### W7 line-citation drift (2026-07-28 sweep) — SUBSTANCE ACCURATE, CITATIONS STALE

The row is **still true and W7 is still open.** Only its coordinates moved. All
three history-linear indexes remain, now at
`production/outbox.rs:166` (`source_batches`), `:167` (`ack_sources`), `:168`
(`all_obligations`) — struct `ConversationOutbox` opens `:162`, restore
initializers `:190-192`. The row's `:124-137` now points at the
`LiveRecipientObligationsExceeded` / `BoundOverflow` error variants. Supporting
citations likewise: `source_batches` insert `:292` (conflict check `:249`),
`all_obligations` insert `:306` in `install_record` (:302), `ack_sources`
insert `:363` (conflict check `:344`); reclamation `discharge_through` `:400`,
`discharge_retired` `:421`, `reclaim_empty_records` `:451`,
`recompute_next_live` `:467`. The load-bearing fact is unchanged:
`reclaim_empty_records` (`:451-465`) removes from `self.records` only, and none
of the three indexes is pruned anywhere in the file. **Restore memory is still
Θ(history), the HARD shared W3/W7 trigger still binds, and W3's landing alone
still does not unblock unbounded history.**

### W4 — LAW-1 polling retirement
- **What sits open:** the polling seams LAW-1 retires, board item since
  Hermes's catch (see `docs/design/LAW1-POLLING-RETIREMENT.md`).
- **Named consumer:** the event-driven replacements the LAW-1 design names.
- **Trigger:** next liminal maintenance window after the wiring lanes W1/W2
  open (sequencing at Hermes's seat).
- **Oracle floor:** per LAW-1 doc — absence proofs (no polling observed under
  the doc's named workloads), not just presence of the new path.

#### Correction to W4 (2026-07-28 sweep, verified at main `1a0b60c`)

Audit verdict: **accurate when authored, STALE SINCE 2026-07-22 — but only
PARTIALLY. W4 is five-ninths discharged, not discharged.** The row's flat "what
sits open" no longer describes the tree, and reading it as still-fully-open is
as wrong as reading the landings as closure.

**The scope partition the row does not carry.** `docs/design/W4-LAW1-POLLING-RETIREMENT.md`
(r5, 2026-07-22 — the successor brief for the first buildable wave; the row
cites only the older open-ended skeleton `LAW1-POLLING-RETIREMENT.md`) censuses
nine production families F1–F9 (§2.1, `:132-144`) and rules **F1–F5 only** into
W4-NOW in three legs (§3, `:177-194`). F6–F9 plus candidates C1–C9 are
expressly out.

**RETIRED (F1–F5), landed 2026-07-22:**

| family | seam | retiring commit |
| --- | --- | --- |
| F1 | main TCP listener accept loop | `e76d5af` — F1 main listener blocking-accept + interrupt + EMFILE shed (W4 leg 1) |
| F2 | WebSocket listener accept loop | `64e122c` — F2 websocket listener blocking-accept + interrupt + EMFILE shed (W4 leg 1) |
| F3 | health accept loop | `772922f` — retire health accept WouldBlock/sleep poll for blocking-accept + interrupt |
| F4+F5 | shutdown drain + force-close settle loops | `778f1f4` — retire drain/settle poll loops for TOLD completion (W4 leg 3) |

The oracle floor's **absence proofs are honoured**: `server/listener.rs:345-356`
is a source-scanning guard asserting `ACCEPT_IDLE_BACKOFF`,
`TRANSIENT_ERROR_BACKOFF`, `set_nonblocking`, `thread::sleep` and
`ErrorKind::WouldBlock` are all absent from the accept path, with `:472`
pinning "shutdown must interrupt the accept wait promptly, not sleep-poll";
`server/shutdown.rs:337` is `drain_source_has_no_reap_count_sleep_loop`,
asserting `FORCE_CLOSE_POLL_INTERVAL` absent (`:345`). Note the health endpoint
**moved**: `server/health/endpoint.rs` → `health/endpoint.rs:23-25`.

**STILL OPEN at the bytes (F6–F9), unchanged:**

> **Same-day forward correction — F6 RETIRED at `048e17a` (2026-07-28).** The
> sweep's F6 bullet below was true at its base `1a0b60c`; the SRV-008 lane
> landed hours later and retired it: membership now rides beamr's ordered
> connection events (`subscribe_connection_events_with_snapshot`), red pin
> `armed_membership_observes_a_join_without_sampling` + committed red log
> `gate-logs/extras/red-srv008.log`, zero-wake soak oracle
> `stable_membership_source_has_zero_consumer_wakes`, and a source-guard
> tombstone (`membership_source_has_no_retired_poll_family`) asserting
> `POLL_INTERVAL` / `poll_once` / `thread::sleep` / `run_poll_loop` absent from
> production source. **W4 is now six-ninths; F7–F9 remain** (F8+F9 are
> SDK-010's dispatched scope under the PUSH-HANDSHAKE-DEADLINE ruling; F7 is
> unclaimed).

- **F6** cluster membership — `cluster/membership.rs:44`
  `const POLL_INTERVAL: Duration = Duration::from_millis(250)`; `run_poll_loop`
  at `:225-228` calls `membership.poll_once()` then `std::thread::sleep`;
  spawned `:211`. **[RETIRED same day — see the correction above.]**
- **F7** channel command-reply liveness — `crates/liminal/src/channel/actor/wait.rs:24`
  `const LIVENESS_POLL = 10ms`; `poll_reply` loops `recv_timeout` at `:76-83`.
  (Path moved from `liminal-server` to `liminal`.)
- **F8** SDK TCP push reader — `liminal-sdk/src/remote/tcp/push_client.rs:56`
  `READER_POLL_TIMEOUT = 100ms`, armed `:574`.
- **F9** SDK TCP subscription reader — `liminal-sdk/src/remote/tcp/subscription.rs:47`,
  armed `:230`.

**The brief itself was never updated after the build.**
`W4-LAW1-POLLING-RETIREMENT.md:5-8` still says "It is a **docs-only lane** …
**it does not claim any replacement is implemented**", and its last commit is
`c506147` (2026-07-22), before all four implementation commits. Anyone reading
that brief for lane state will be wrong in the opposite direction from this
row. Flagged, not edited — the brief is not this sweep's file.

**Amended trigger, forward:** the row's trigger ("next maintenance window after
W1/W2 open") is spent for F1–F5. F6–F9 need their own wave and their own
sequencing ruling; the skeleton `LAW1-POLLING-RETIREMENT.md` remains the
open-ended program register and is **itself stale at codebase pin `ce8814d`**
(`:1-7`), as the W4 brief already notes.

### W5 — LP-CLIENT SDK riders — CLOSED BEFORE THIS ROW WAS AUTHORED (audited 2026-07-28)

**The row stays. The lesson is why.** Both riders were already fixed on main
when this row was written. The row is not retired quietly — it is the ledger's
own worked example of the failure mode it must never repeat.

**Failure mode: authored from stale notes one day after the fix landed.** The
row entered the ledger at `eb3ae30` (2026-07-19 15:52). `fb11ff6`
("fix(protocol): validate durable abandonment records") landed 2026-07-18
05:57 — thirty-four hours earlier — and closed BOTH riders in one commit. The
notes the row was transcribed from were the Phase C landing notes; nobody
re-derived the claims at the bytes before writing "what sits open". Every row
in this register states a *present-tense* fact about the tree, so every row
must be re-derived at the bytes at authorship. Carried notes are not evidence.

#### Original row (historical — the claim as authored)
- **What sits open:** (a) `decode_abandonment` any-request gap;
  (b) pre-existing `unreachable!()` at `inbound.rs:140`.
- **Named consumer:** the SDK leg that hardens client decode paths.
- **Trigger:** first SDK hardening pass, or immediately if either surfaces in
  a production trace.
- **Oracle floor:** (a) a decode-abandonment test per request shape;
  (b) the `unreachable!()` replaced with a typed refusal + a test that reaches
  the formerly-unreachable arm.

#### Correction (2026-07-28, verified at main `1a0b60c`)

- **Rider (a) — `decode_abandonment` any-request gap: CLOSED by `fb11ff6`
  (2026-07-18), NARROWED further by `811b52d` (2026-07-18).** `fb11ff6`
  replaced the unconditional `Ok(ParticipantFrame::ClientRequest(request))`
  arm with a match guard admitting only
  `RecordAdmission(_) | ObserverRecovery(_)`, and added a sibling arm returning
  the typed `ClientResumeRecordDecodeError::InvalidAbandonmentRequest {
  request: request.discriminant() }` for everything else. `811b52d` then
  narrowed the admitted set again. **On main today the guard is
  ObserverRecovery-only**, at
  `crates/liminal-protocol/src/client/resume_decode.rs:279-293` — accepting
  guard `:280-281`, typed-refusal arm `:289-293`. The gap does not exist and
  has not existed since 2026-07-18.
- **Rider (b) — the `unreachable!()` at `inbound.rs:140`: DELETED by `fb11ff6`,
  not replaced.** It was a **never-reachable destructuring artifact**, not a
  live hazard: the code called `inbound_refusal(…)`, which returns
  `ClientInboundDecision::Refused` unconditionally, then destructured the
  result with `let ClientInboundDecision::Refused(refusal) = decision else {
  unreachable!() };` — and immediately discarded the pieces into a differently
  typed refusal. `fb11ff6` removed the whole nine-line block, so the arm the
  oracle floor asked for a test against no longer exists to be reached.
  `811b52d` removed a further eighteen lines from the same function. There is
  **no `unreachable!()` anywhere in
  `crates/liminal-protocol/src/client/inbound.rs`** today.
- **Oracle floor — satisfied, but count it honestly.**
  `crates/liminal-protocol/src/client/rider_tests.rs` (created by `fb11ff6`,
  45 lines, unchanged since) carries **two** `#[test]` oracles over a shared
  token-bearing-abandonment fixture:
  `canonical_decode_rejects_token_bearing_abandonment` (:22) and
  `canonical_restore_rejects_token_bearing_abandonment` (:34) — the decode
  entry point and the restore entry point, both asserting the exact typed
  `InvalidAbandonmentRequest { request: ClientDiscriminant::EnrollmentRequest }`.
  The floor asked for "a decode-abandonment test per request shape";
  `ClientRequest` has eight variants (`wire/request.rs:122-139`) and the tests
  exercise **one** of the seven now-rejected shapes. The other six are covered
  **structurally, not per-shape**: the accepting arm is a `matches!` guard on a
  single variant, so every other variant falls to the catch-all refusal arm by
  construction — a stronger proof than a per-shape census, but a different one
  than the floor's words. Recorded precisely so the difference is visible.
  (This sweep is doc-only and ran no cargo; the counts above are read off the
  bytes, not off a test run.)
- **Named consumer / trigger:** both moot. The riders were closed by the
  protocol crate's own hardening before any "first SDK hardening pass" existed.

### W6 — Browser conversation surface
- **What sits open:** request-reply and conversations remain Rust-side only;
  the browser SDK deliberately ships publish-with-receipt + subscription only
  (recorded in the Iridium authoring draft §5.4 as a chosen non-wait).
- **Named consumers:** the frame authoring arc (edit proposals as
  conversation facts, when Tom rules T1–T4) and a Meridian-in-frame surface
  (two-way conversation UI) — both in the applications conversation opened
  with Tom 2026-07-19.
- **Trigger:** whichever named consumer Tom greenlights first.
- **Oracle floor:** browser conversation tests mirroring the Rust transport's
  conversation suite (same semantics, wire-level parity asserts).

#### Correction to W6 (2026-07-28 sweep, verified at main `1a0b60c`)

Audit verdict: **STALE AT AUTHORSHIP — the second instance of the W5 failure
mode in this same commit.** The row's "what sits open" was already contradicted
by the tree it was written against, and the row text is byte-identical at
`eb3ae30` and at HEAD, so nobody has re-derived it since.

**The claim:** "request-reply and conversations remain Rust-side only; the
browser SDK deliberately ships publish-with-receipt + subscription only".

**At `eb3ae30`'s own tree,** `sdks/liminal-ts/src/index.ts` already exported
`RequestReplyMetadata` (:7), `RequestReplyOptions` (:8), `openConversation`
(:36) and eight conversation types (:38-46). `conversation.ts` had been in the
tree since `adf9165` (2026-06-26, "SDK-008: TypeScript SDK — conversations,
backpressure, connection, codegen"), and the last change to `index.ts` before
the ledger was `d4c82d6` at **13:04** on 2026-07-19 — the ledger landed at
**15:52** the same day. Nothing has changed the API surface since: the only two
commits under `sdks/liminal-ts/src` after 2026-07-19 are `7ba8c07`
(2026-07-20, wasm glue import) and `c2e4a1a` (2026-07-22, lazy default WASM
source hook).

**What is actually open — the row's real content, re-derived.** The browser SDK
**exports request-reply and conversation APIs but ships no concrete transport
that wires them.** `Channel.requestReply` is declared at `channel.ts:67` and
implemented at `:135-152`, delegating to `ChannelTransport.requestReply`
(`:41`); `openConversation` (`conversation.ts:118-121`) returns
`TransportConversation<T>` (`:128`). But `ChannelTransport` and
`ConversationTransport` are interfaces with **no shipped implementation** — the
default is `missingTransport` (`channel.ts:193-203`), whose `requestReply`
throws "Channel transport is not configured; connect the SDK before using
transport operations" (`:200-201`). The one concrete wire transport that ships,
`LiminalFeedSource` (`feed-source.ts:44`), offers `subscribe` (:74),
`requestSnapshot` (:126) and `publish` → `FeedPublishReceipt` (:145-154) —
i.e. publish-with-receipt + subscription, exactly as the row's conclusion said,
for a reason the row got wrong.

**Why the distinction matters for the lane, not just the record:** the row
implies the browser conversation lane must design and build an API surface. It
must not — the surface exists and is public (`@ablative/liminal` 0.3.3). The
lane is a **transport** lane: implement `ConversationTransport` /
`ChannelTransport.requestReply` over the WS wire and pin them. Named consumers,
trigger and oracle floor are unaffected and stand as written.

**Unverifiable, left untouched:** the row's citation "recorded in the Iridium
authoring draft §5.4 as a chosen non-wait". No Iridium draft exists in this
repository — `grep -rli iridium docs/` matches only this ledger. The citation
may be sound against a document held elsewhere; this sweep cannot confirm or
refute it and does not touch it.

## Dated annotations — 2026-07-28 (facts this ledger predates)

The ledger body above was written between 2026-07-19 and 2026-07-20. These rows
record what happened afterwards that bears on the lanes, so no future reader
has to infer it. Annotations only — **nothing here opens, closes, or rules a
lane.**

### A-1 — W4 leg landings (2026-07-22)

`e76d5af`, `64e122c`, `772922f`, `778f1f4` retired families F1–F5 of
`docs/design/W4-LAW1-POLLING-RETIREMENT.md`. Detail, including the F6–F9
remainder and the absence-proof guards, is in the W4 correction block above.
Recorded here as well so the annotation section is a complete index of the
post-authorship record.

### A-2 — 0.5.0 released (`2a15a23`, 2026-07-28)

`release: 0.5.0 — WorkerRegister activity census (servers before workers)`.
Lockstep `liminal-rs` / `liminal-sdk` / `liminal-server` 0.4.1 → 0.5.0;
`liminal-protocol` unchanged at 0.3.2. A version-metadata commit only — six
files, no code.

The census is a trailing field on `WorkerRegister` after `identity`: a u32
descriptor count, then three length-prefixed strings per descriptor (`name`,
`input_schema_json`, `output_schema_json`); an empty census identifies a
pre-contract worker. **One-direction wire break, operationally load-bearing:**
an old worker against a new server is safe by construction, but a new worker
against a ≤0.4.1 server fails the connection loudly, so **every server upgrades
before any worker**. The 0.5.0 CHANGELOG carries the deployment note.

### A-3 — PARTICIPANT-CONTRACT R18 RATIFIED (main `1a0b60c`, 2026-07-28)

**Both keys of the LAW 3 two-key gate turned 2026-07-28** — Hermes Crumpet
(liminal domain owner) and Waffles the Terrible (reviewer of record,
coordinator seat). The header Status block moved from "DRAFT R18 — … not yet
ratified" to "R18 RATIFIED AS FROZEN SOURCE MATERIAL"
(`docs/design/PARTICIPANT-CONTRACT.md:3-8`), with the closing gate posture
updated at `:6378-6381` and `:6396-6402`. The 2026-07-15 freeze posture is
unchanged; the frozen body is untouched.

Ratification carries **three named carve-outs** — text inside them is ratified
only as recorded in the Status block, never as written in the body
(`:9-33`):

1. **Occurrence-array machinery (~lines 2560–2715)** — defective;
   **FORBIDDEN to transcribe** (`LP-EXTRACTION-GOAL.md` Fix 2); replaced in
   `crates/liminal-protocol` by the per-participant cursor-fact map and the
   `ParticipantCursorProgress` repayment edge.
2. **Attach-clears-the-detach-cell text (~lines 1596–1645)** — defective as
   written; superseded by Fix 1's fourth `Terminalized` cell variant.
3. **R-A2/R-C0 "binding-slot release" (lines 912–914, 1605–1607, 1629–1631)** —
   ratified AS SCOPED by the 2026-07-23 drain-extension tear's flavor ruling:
   for `Died`, release = erasure of slot and token; for `Detached`, release =
   the slot's transition OUT of pending residence into committed
   `BindingState::Detached`, **slot and enrollment token PRESERVED**.

**Amendment A1 (§0.12, response/push ordering on one participant connection,
landed `fec354f`) is NOT covered by `1a0b60c`** — it rides its own key and, at
`1a0b60c`, remained "decided-by-amendment, pending the reviewer-of-record key
(**Vesper Lynd**)".

> **Same-day forward correction — A1 RATIFIED at `26f4bf2`.** This sweep is
> based on `1a0b60c` (2026-07-28 23:23), and `26f4bf2` ("docs(contract): A1 key
> record — reviewer-of-record key turned, every gate closed") landed on main at
> **23:30, seven minutes later** — after this branch was cut, so the paragraph
> above was true when written and is superseded now. Vesper Lynd (reviewer of
> record) ratified amendment A1 at the bytes on 2026-07-28. Her verdict record
> is folded into §0.12's status; status surfaces only (head Status block, §0.12
> status, the `«RESPONSE-PUSH-ORDER»` socket row, closing gate posture) — the
> frozen body is still untouched. Scope note carried with her verdict: the
> correlation slot's *shape* was verified, not an exhaustive pipelining-refusal
> proof — **pipelining clients sit outside the contract until R-D3**.
> **With both `1a0b60c` and `26f4bf2`, every key on PARTICIPANT-CONTRACT.md is
> turned and no gate remains open.**

### A-4 — R18's OBSERVES rows that W4 made stale (ledgered here, not edited there)

The frozen body is not edited, so the §1 evidence table still describes the
pre-W4 tree. Recorded here so no reader mistakes a frozen observation for a
current one. **Coordinates corrected:** the brief for this sweep placed these
rows at `:360-368`; those lines are §0.12 amendment-A1 prose. `## 1. The
verified gap` begins at `:369` and the evidence table runs `:384-393`. Read at
the bytes, 2026-07-28:

| contract line | OBSERVES | status |
| --- | --- | --- |
| `:384` | "The current listener already has the banned polling shape." | **STALE** — F1 retired by `e76d5af` (2026-07-22) |
| `:385` | "Cluster membership is polled." | accurate at the sweep's base `1a0b60c`; **STALE since `048e17a` same day** — F6 retired by SRV-008 (see the W4 correction block) |
| `:386` | "Health accept is polled." | **STALE** — F3 retired by `772922f`; the cited path `server/health/endpoint.rs` also moved to `health/endpoint.rs` |
| `:387` | "Shutdown drain and settle are polled." | **STALE** — F4+F5 retired by `778f1f4` |
| `:388` | "Channel command reply liveness is polled." | accurate — F7, `channel/actor/wait.rs:24,:76-83` (path moved to `crates/liminal`) |
| `:389` | "The SDK push reader polls for local shutdown." | accurate — F8, `tcp/push_client.rs:56,:574` |
| `:390` | "The SDK subscription reader polls for local shutdown." | accurate — F9, `tcp/subscription.rs:47,:230` |
| `:391` | "The synchronous durability bridge repeatedly polls without honoring a waker." | accurate in substance — `durability/bridge.rs:52` (`MAX_POLLS = 8`), loop + `yield_now` at `:87-93`; cited line ranges drifted |
| `:392` | "The push-reply public wait contract blesses caller-side timeout re-arm." | accurate in substance — `receive` now at `supervisor.rs:807`, `receive_deadlined` still loops `try_take_reply` + `recv_timeout` at `:849-877`; cited range `533-636` drifted |
| `:393` | "Subscription setup samples a total deadline through repeated read timeouts." | accurate — `tcp/subscription.rs:389-393` |

Three rows stale, seven still accurate (two of those with drifted line
citations). All three stale rows went stale on 2026-07-22 — a week after the
07-15 freeze — which is precisely why they are annotated here instead of fixed
there.

### A-5 — Core / participant codec-doctrine fork (no rule violated)

Two crates take **opposite doctrines on trailing bytes**, deliberately, in
partitioned domains:

- **Core (`crates/liminal`, published `liminal-rs`)** — absence of trailing
  bytes is a valid legacy shape. `decode_worker_register_payload`
  (`src/protocol/codec/known.rs:285`) sniffs `reader.is_finished()` at `:298`
  and yields an empty census for a pre-contract worker
  (`PayloadReader::is_finished` at `src/protocol/codec/payload.rs:309-311`).
- **Participant (`crates/liminal-protocol`)** — any trailing byte is a hard
  decode failure, never an extension point. `DecodeClass::CanonicalEncoding`
  ("Complete selected shape contains trailing bytes",
  `src/wire/tags.rs:265-266`), enforced at `src/wire/codec.rs:365-367` and in
  `Reader::finish` at `:1003-1006`, server direction at
  `src/wire/server_codec.rs:370`, pinned by
  `trailing_truncated_and_zero_generation_classes_are_stable`
  (`src/wire/codec_tests.rs:230`), which appends exactly one byte and asserts
  the refusal.

**No rule is violated.** The domains do not share frames: the core codec serves
the legacy worker/broker wire, the participant codec serves the extracted
lifecycle wire. **The core side is already fenced by a ceiling comment at the
seam** — `known.rs:295-297`, verbatim:

```
// This sniff consumes WorkerRegister's one trailing-bytes extension slot. Any
// future field appended to this frame must use a ProtocolVersion gate, never a
// second sniff: another optional tail is indistinguishable from census bytes.
```

Recorded here because the fork is the kind of thing a future reader discovers
mid-lane and mistakes for a defect. It is a partition with a named ceiling, and
the ceiling has been spent once.

### A-6 — Release tags stop at `v0.2.3` (ledger NOTE only — the act is Tom's seat)

`git tag --list` returns exactly three tags — `liminal-v0.2.0` (`71b4273`),
`liminal-v0.2.1` (`6ac1e36`), `liminal-v0.2.3` (`288b997`, 2026-07-09). There
is no `v0.2.2`, and **nothing for 0.3.x, 0.4.x or 0.5.0**; the 0.5.0 release
commit `2a15a23` is untagged. (Naming convention is `liminal-vX.Y.Z`, not
`vX.Y.Z`.)

**This is a note, not a lane and not a task.** Tagging is part of the
publish/release act, which is human-gated at Tom's seat. Nothing in this sweep
or this ledger authorises anyone else to cut a tag. Recorded so the gap is
visible to whoever holds that seat.

### A-7 — RESIDUE ROW: three test-helper reap loops survive the W4 retirement

**Micro-rider candidate.** Not a LAW-1 violation — the W4 brief pre-blesses
this class at `:127-129` ("The `#[cfg(test)]` sleeps surfaced by the sweep …
are test scaffolding, not production loops") — but they are reap/sleep loops
that outlived the production seams they mirrored, and they are the shape a
future sweep will re-flag if nobody writes them down.

- `crates/liminal-server/src/server/connection/supervisor_tests.rs:2710-2723` —
  `wait_for_cleanup`, a non-`#[test]` helper: 2s deadline,
  `supervisor.reap_crashed_connections()` then `thread::sleep(10ms)`. A sibling
  helper of the same shape sits at `:2696-2708`.
- `crates/liminal-server/tests/sdk_tcp_e2e.rs:1035` — reap-then-sleep(10ms)
  loop against `notifier.unregistered_calls()`, bounded by `CONNECT_TIMEOUT`.
- `crates/liminal-server/tests/ws_transport_e2e.rs:116` — `wait_for_active`,
  reap-then-sleep(10ms) loop against `active_connection_count()`.

(Both `tests/` paths are under `crates/liminal-server/`, not the repo root.)

- **Named consumer:** whichever wave next touches connection-supervisor test
  scaffolding — most naturally the F6–F9 W4 successor wave.
- **Trigger:** micro-rider, foldable into any lane already editing these files;
  no standalone dispatch warranted.
- **Oracle floor:** replace each poll with the same TOLD notification the
  production path now uses, and keep the existing assertions green — an
  equivalence obligation, not new coverage.
- **Owner:** unassigned. Recorded under no-row-no-dormancy so the residue has a
  row rather than sitting nameless.

## Companion registers (not duplicated here)

- **Frame danglers:** the decisions audit (2026-07-19, coordination seat)
  enumerates the frame-side named-outs; its remediation runs under the same
  no-row-no-dormancy rule. Live roads back already opened from it: the
  editable-Iridium arc (design draft r0.1), the operable-console arc (send
  landed 2026-07-19), motion-feel pass (queued, Tom's eyes on result).
- **D7–D11 attribution re-head:** held at Tom's desk (which-did-you-rule),
  not a wiring lane.

## Standing

Lanes W1, W2, W4: Hermes picks up on his word — "ready to pick up
wiring-program lanes the moment the ledger names them." This document is that
naming. W3's brief drafts at his seat per the ratified disposition. W5 queues
behind the current SDK arcs. W6 waits on Tom's application ruling.

**Standing as re-derived by the 2026-07-28 sweep** (the paragraph above is left
as written; this is the forward correction). Read against the bytes at main
`1a0b60c`, and **subject to the owner's ruling — a sweep records, it does not
close**:

- **W1** — narrowed to the **`Ordinary` arm alone** (still zero production
  callers). Died and Recovered wired 2026-07-21; `LeaveCommit` deleted
  2026-07-19; a fifth `Detached` arm arrived wired.
- **W1a** — CLOSED, and its r1.8 tear rider **discharged** by `38a7900`.
- **W1b** — all three open claims repaired at the bytes 2026-07-20/21
  (`87caef4`, `ebb8aaa`, `79a5ca6`, `c06bda8`, `1b03e50`). **RULED CLOSED
  2026-07-28** on the flush-barrier evidence — see the closure block in the
  row.
- **W2** — **wired at the bytes** since `e25fa72` (2026-07-22), including the
  divergence refusal in production. **RULED CLOSED 2026-07-28** — the
  production invariant supersedes the fixture formulation.
- **W3** — CLOSED r1.4; closure verifies, with one census-location imprecision
  corrected forward.
- **W4** — **five-ninths done** at the sweep's base; **six-ninths since
  `048e17a` same day** (F6 retired by SRV-008). F1–F6 retired; **F7–F9
  remain** — F8+F9 dispatched as SDK-010, F7 unclaimed. Not dischargeable as
  written.
- **W5** — **CLOSED before the row was authored** (`fb11ff6`, 2026-07-18). Row
  retained as this register's worked example of stale-at-authorship.
- **W6** — still open, but it is a **transport** lane, not an API lane: the
  browser SDK's request-reply and conversation surface already ships public;
  what is missing is a concrete `ConversationTransport` /
  `ChannelTransport.requestReply`. Still waits on Tom's application ruling.
- **W7** — still open and unchanged in substance; line citations refreshed. The
  HARD shared W3/W7 trigger on unbounded outbox history still binds.
- **A-7** — new residue row (three test-helper reap loops), unassigned.
