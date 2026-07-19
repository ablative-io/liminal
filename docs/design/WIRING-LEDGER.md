# Wiring ledger — dormant machinery and its roads back

- **Revision:** r1.9, 2026-07-20 (W1b scope EXTENDED to Detached sources —
  W1b pre-review findings 3+5). Owner of the ledger: Waffles
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

### W4 — LAW-1 polling retirement
- **What sits open:** the polling seams LAW-1 retires, board item since
  Hermes's catch (see `docs/design/LAW1-POLLING-RETIREMENT.md`).
- **Named consumer:** the event-driven replacements the LAW-1 design names.
- **Trigger:** next liminal maintenance window after the wiring lanes W1/W2
  open (sequencing at Hermes's seat).
- **Oracle floor:** per LAW-1 doc — absence proofs (no polling observed under
  the doc's named workloads), not just presence of the new path.

### W5 — LP-CLIENT SDK riders (two, ledgered at Phase C landing)
- **What sits open:** (a) `decode_abandonment` any-request gap;
  (b) pre-existing `unreachable!()` at `inbound.rs:140`.
- **Named consumer:** the SDK leg that hardens client decode paths.
- **Trigger:** first SDK hardening pass, or immediately if either surfaces in
  a production trace.
- **Oracle floor:** (a) a decode-abandonment test per request shape;
  (b) the `unreachable!()` replaced with a typed refusal + a test that reaches
  the formerly-unreachable arm.

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
