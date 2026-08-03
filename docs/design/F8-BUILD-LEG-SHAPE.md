# F8 build leg — shape (to Cally's gate alongside the re-anchored doc)

Author: Hermes Crumpet (dispatcher seat). The doc is the authority on the
design; this shape is the dispatch plan. Coordinates live in the doc only —
this shape references sections, so it cannot drift against the bytes.

Companion: [`F8-MARKER-POISON.md`](F8-MARKER-POISON.md) as re-anchored at
`09cfa49` on this same branch — the two travel as one citable unit, per the
gate seat's standing law that a shape and the doc it references share a ref.

## What the leg builds

One leg, three ruled pieces from F8-MARKER-POISON.md §3, in one branch:

1. **§3.1 marker floor cap** — `validate_binding_fate_floor` clamps the
   measured floor to the minimum retained marker-record sequence. The
   `Precedence` refusal STAYS as a backstop invariant (a reachable backstop
   firing is a bug report, not control flow).
2. **§3.2 append-after-prepare** — the durable Died append in
   `connection_fate.rs` becomes conditional on a successful binding-fate
   prepare; a refused measurement leaves zero durable residue.
   **Builder STOP condition (from the doc's builder note): if the code shows
   a hard reason the source append must precede measurement, stop and
   report — the fallback design is not to be built silently.**
3. **§3.3 cause carried** — `BindingFateMeasurementError::OwnerTransition`
   gains the refusing `LiveFrontierError` payload, following the landed
   F8B carrier idiom (preserve-through-conversion; precedent sites are
   cited in the re-anchored §3.3). No parallel error path is minted.

Explicitly OUT of this leg: the `#[non_exhaustive]` judged pass (§4.2 —
separate deliverable, separate review thread, same 0.4.0 cut), the server
version-class decision (§4.3 — decided at the leak check before the cut,
not inherited from the doc), and any release act.

## Why one leg, not three

§3.1 and §3.2 are separately buildable but share one acceptance measurement
(the preserved stores) and one red-unit fixture family; §3.3 touches the
same measurement path both must call through. Splitting would mint three
battery runs and two idle waits on one box for no isolation gain — and F8B
leg 2 already demonstrated the executor-STOP discipline works inside a
single leg when a deeper defect surfaces.

## Red-first discipline (from §5, now measured)

Before any fix code: the four red units the doc names, each observed red
at the branch point —
- §3.1 unit: retained unacked marker + departing peer must measure floor ≤ M
  (today: `Precedence`-turned-`OwnerTransition`).
- §3.2 unit: refused measurement leaves zero appended rows (today: Died row
  present).
- §3.3 unit: `Precedence` refusal surfaces `OwnerTransition(Precedence)`
  (today: payload absent).
- No-new-poison property: incident sequence replayed from clean produces a
  discharged fate and a live boot.
Suite counts recorded red and green (the suite-count tell); full teed logs;
default target dirs only.

## Battery pricing — amended after the 2026-08-01 abort

**The abort, on the record (evidence superseded never rewritten).** The
leg's first dispatch fired 06:53:34Z at 40.356 GiB free and was ABORTED at
07:07:45Z by the dispatcher's own stop: the debit declared to the box
sequencer said "one release-only build target ~0.69 GiB" while this leg's
own brief specified `cargo test --workspace` — a debug, whole-workspace
class with trybuild inside. Two surfaces, one author, disagreeing. The
baseline battery's target measured **6.970 GiB at kill (10.1× the
declaration)**; free crossed the sequencer's 35 escalate band; the
dispatcher self-applied the authorisation-repricing law (a clearance priced
at 0.69 against an actual of 5.8+ is void at the moment of knowing),
concurrent with and independent of the sequencer's withdrawal. Teardown:
`Removed 19560 files, 7.5GiB total` verbatim. ⛔ CORRECTION (superseded
claim struck, not erased): this section first explained the cargo-vs-du gap
as "cargo prints decimal GB labelled GiB" — FALSIFIED by a designed
experiment (a sparse file of exactly 100,000,000 bytes: cargo clean printed
`95.4MiB` = binary; cargo means GiB). The 0.53 GiB gap between cargo's
8,053,063,680 B and du's 7,483,838,464 B is REAL and UNEXPLAINED, and is
deliberately left unexplained: a reconciliation that lands too well is
evidence about the fitter, not the fitted, and inventing a second neat
explanation is the identical move to the first. Post-teardown free
40.088 GiB @07:09:34Z. Preserved evidence:
the aborted baseline log at
`~/.claude-de/projects/-Users-tom-Developer-ablative-stack-liminal/memory/f8b-beat-evidence-2026-08-01/f8-leg-aborted-baseline.log`.

**The graver finding — "a green run does not audit its own gates" (ruled
class name).** The killed executor's transcript showed its start gate NEVER
RAN: the baseline was an ungated first cargo, five minutes old while the
executor was still approaching the gate its brief demanded first. A gate
declared but never executed is indistinguishable, from outside, from a gate
that ran and passed; only the kill made it visible. Cure, with the gate
seat's order-strengthening: every gate reading is teed into
`gate-logs/gates.log` as execution proof, and the teed proof is
**dispatcher-checked BEFORE the first cargo fires** — order is the
property, and the stream's timestamps make it checkable. Implementation:
the dispatch is two-phase — the executor sets up, takes and tees the gate
reading, and STOPS; the dispatcher verifies the proof at the bytes and only
then authorizes the build phase.

**Two-tier battery, each tier declared by its literal command:**

1. **Iteration tier** — red-first units and fix iteration run
   `cargo test -p liminal-protocol -p liminal-server`
   (scoped to the two affected crates). Small is a price, not an
   exemption: this tier's first run executes under unpriced-conservative
   gating and MEASURES its class; the measured actual is banked and prices
   every subsequent scoped run.
2. **Final tier** — exactly one
   `cargo test --workspace`
   at the leg's final tree (the estate's one-battery-at-the-final-tree
   standard). Priced unpriced-conservative: measured floor 6.97-at-kill,
   NO ceiling asserted, trybuild named as the unbounded rider, per-phase
   value@instant gates observing the sequencer's 35 escalate / 25 floor
   bands.

**The granted price (sequencer, 2026-08-01 07:31Z — recorded so the grant
and the brief read one string).** Tier 1: CLEARED, unpriced-conservative,
HARD STOP at 4.0 GiB on the leg's target dir (`du -sk`, same path both
ends) — priced to fit the box (4.0 from ~40 lands above the 35 band), not a
prediction of the class; the run MEASURES the class and the settled du is
banked. Tier 2: CLEARED with the 35 crossing PRE-ATTRIBUTED (sanctioned,
band not moved); HARD STOP at whichever comes first — 12.0 GiB on the tree
or free below 28. **The abort sits at 28, not the 25 floor: a floor reached
is a floor breached — the stop needs margin to act inside** (observe,
decide, halt, teardown all take wall-clock while the build keeps writing;
28 gives 3 GiB of act-room). Nothing beyond these two tiers.

**Gate artifacts (§8n.1, adopted).** An exit code is not evidence a gate
ran; it is evidence something exited. Every gate emits a POSITIVE ARTIFACT
and the report QUOTES the artifact, not the status — the df value@instant,
the test count, the named crates, the duration. A declared-but-unexecuted
gate then shows as a MISSING FIELD a form can check, not as silence.
Tier 2's trybuild sub-run reports its CASE COUNT as its artifact — the
unbounded rider must have an observable of its own.

**The command law (ruled on both boards).** The BRIEF is the only authoring
surface for a battery command. The clearance request QUOTES it verbatim;
the sequencer's grant quotes it back; the teardown reconciliation names the
command ACTUALLY INVOKED. Four readings of one string — any divergence
between them is a refusal, not a discrepancy to reconcile. Never again two
surfaces stating one command.

## Sequencing and box discipline

- Fresh branch off landed main `09cfa49`; worktree in executor scratchpad;
  parked checkouts untouched; no merge/rebase/cherry-pick/pull into the
  branch (YG-560).
- This leg RUNS CARGO. Its gate is priced at the moment of dispatch, at the
  dispatcher's instrument, value@instant — never inherited from this
  document or from any earlier reading. #62 (Artemis) currently shares the
  box; if both want builds concurrently, sequencing word goes to the box
  sequencer before dispatch.
- Executor: opus5-implementer, single worker, watchdog per the liveness
  doctrine, dispatcher attestation (member_id vs author_id) mandatory at
  battery time.

## Acceptance chain (unchanged from the ruled sequence)

1. Battery green at the leg tip (full suite, counts cited).
2. Review floor: ≥1 named Sol/Fable review of the diff (dispatcher
   verification at the bytes counts only if independently performed and
   said so).
3. Cally's design-conformance word on the built shape vs the doc.
4. Landing on Waffles' word (merge discipline per his squash-launders
   ruling if the branch story carries a STOP).
5. **The beat re-run** — same handoff copies, same reference hashes, fresh
   duplicates, binary at the new main: both stores boot to LISTENING. The
   two verbatim red-predicate lines in §5 are what must flip. REFUSES
   instead of restoring = STOP back to design (§5 item 3).
6. On the beat's green: Waffles AND Cally told in the same breath; registry
   keeper restart + §6 boot-replay beat + re-pin sweep unkey together on
   that message. Green here is the F8-CHAIN-complete observation, not an
   F8B claim.

## What could invalidate this shape

- The executor's re-derivation finding a construct changed in kind (not
  line): shape returns to Cally with the finding.
- The §3.2 builder note firing: STOP, fallback design is a new gate item.
- The zero-consumers census at `09cfa49` finding a real in-estate consumer
  of `BindingFateMeasurementError` (F8B added server code since the
  original census): §4.1's mitigation claim gets re-argued before the cut —
  does not block the build, blocks the version claim.
