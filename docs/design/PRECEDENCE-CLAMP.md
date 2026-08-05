# The Precedence clamp — a floor that cannot cross a marker, r3

**Status: dispatch-ready. Tom ruled "get this fixed" (2026-08-05) after the
third estate brick in six days. Hermes ruled the shape and reviews the build;
Waffles authored this and holds the landing gates.** This brief fixes the
defect that has bricked three manifold estates. It does NOT build the
abandonment path — that stays Hermes's ruling, and this brief's specimen gate
is what decides whether it is needed at all.

## What exists, measured

Three estates have been permanently unbootable from the same failure:
2026-07-31, 2026-08-01 (`intent 82` at conversation 3), and 2026-08-05
(`intent 329` at conversation 7). Each refuses identically on every boot,
forever. The verbatim refusal from the third:

```
participant incarnation connection-fate handler recovery failed: Open 329
failed before Complete: ... binding-terminal admission refused: Precedence
```

The mechanism, read at the bytes by both of us independently:

- **The split.** `validate_binding_fate_floor`
  (`liminal-protocol/src/lifecycle/operations/binding_fate.rs:436`) computes
  `resulting_floor` through one `floor_transition` call whose five arguments
  are **none of them derived from `marker_records`**.
- **The enforcement.** `prepare_binding_fate_transition`
  (`claim_frontier/binding_fate_transition.rs:38-44`) then refuses that very
  floor with `Precedence` whenever a retained marker sits strictly below it.
  One half computes in ignorance of the rule the other half enforces.
- **The second enforcer.** The same marker rule is enforced independently at
  `install_finalized_binding_fate_floor` (same file, `:77+`), reached through
  `complete_pending_died_ordinary_finalizer` (`binding_fate.rs:127`). A fix
  applied only at the measurement site leaves the finalizer path exposed.
- **The lost cause.** `validate_binding_fate_measurement`
  (`binding_fate.rs:374`) collapses the inner error with
  `.map_err(|_| BindingFateMeasurementError::OwnerTransition)`. A refusal that
  cannot name the invariant it hit cost Hermes a day in July and Waffles a
  morning today.
- **THE TRAP.** `cap_floor` (`algebra/floor.rs:26-33`) is
  `if base_result > cap_floor { base_result } else { cap_floor }` — that is
  `max`, a floor-**raiser**, despite its name. Passing the lowest marker
  through it yields `max(base, marker)`, which still sits above the marker in
  exactly the poisoning case, and would raise floors in cases that work today.
  A builder told "bound the floor by the marker, make it pass" lands there
  naturally. That is why this paragraph exists.

Why the rule is right and must not be weakened: `install_binding_fate_transition`
**prunes** retained records below the resulting floor. The `Precedence` refusal
exists to stop a floor advance from silently eating a marker. The defect is not
the rule; it is that nothing prevents an unsatisfiable floor from being proposed.

## Requirements

- **M1 — one marker-aware minting point, AND a floor that is still legal when
  it is enforced.** The resulting floor is minted in exactly one place that
  knows about `marker_records`. That is necessary and **not sufficient**, for a
  reason read at Hermes's bytes and confirmed at Waffles's:
  `PendingDiedOrdinaryFinalizer` (`binding_fate.rs:81-84`) carries a **frozen**
  `resulting_floor: DeliverySeq`, fixed at measurement time (`:288`, `:340`).
  `complete_pending_died_ordinary_finalizer` (`:120-127`) destructures that
  stored value and replays it into `install_finalized_binding_fate_floor`, which
  re-checks it against the **current** marker set. The server holds that
  finalizer across a durable boundary
  (`liminal-server/src/server/participant/production/state.rs:117`) — the very
  window M5 exists to close. So a floor correctly clamped at mint time can be
  crossed by a marker admitted afterwards, and the second enforcer refuses a
  floor that was legal when it was computed.
  The build must therefore deliver **one** of:
  **(a)** re-clamp at finalization against the current marker set — re-mint
  rather than replay; or
  **(b)** a demonstrated argument, in the code and in the report, that no marker
  can be admitted in that interval.
  (a) is the structural answer and is preferred. (b) is acceptable only if
  actually proven; asserted is not proven. Getting this wrong looks exactly like
  success: the measurement path is fixed, G2 passes both paths in a quiet test,
  and the finalizer path stays live in production under concurrency — a fourth
  occurrence with a fix already in the tree.
- **M1a — the re-mint's admissible interval, or (a) swaps one brick for
  another.** `install_finalized_binding_fate_floor` refuses on **two** conditions,
  and clamping downward against markers only bounds one of them. Verbatim:

  ```rust
  let retained_end = u128::from(self.sequence.ledger().high_watermark()) + 1;
  if resulting_floor < self.retained_floor || resulting_floor > retained_end {
      return Err(LiveFrontierTransitionError::ResultingFrontier);
  }
  ```

  A re-mint that clamps only downward can land **below** the current
  `retained_floor` — which has moved by ordinary means since measurement — and
  is refused as `ResultingFrontier`: the same permanent brick under a different
  variant name, produced by a builder implementing (a) exactly as written.
  So the re-minted floor must land in
  **[current `retained_floor`, min(lowest_retained_marker_seq, high_watermark + 1)]**,
  with **both ends read at finalization time, not at mint time** — the upper end
  moves too, since `retained_end` derives from the current high watermark.
  **The subsumed case is a decision, not an accident:** if the current
  `retained_floor` has already advanced past the measured floor, the fate's floor
  is subsumed and installing the older value is meaningless. Handle it as an
  explicit success / no-op. It must not be allowed to fall into the `<` branch
  and refuse.
- **M1b — a required question, answered from the code, not from G3.** Is the
  subsumed-floor refusal an **independent, marker-free brick path**? It needs no
  marker at all — only that the floor advanced by ordinary projection while a
  finalizer sat pending across the durable boundary. Neither Hermes nor Waffles
  claims it is reachable; the interval's concurrency is untraced. The build must
  answer it from the code and say so in the report. If it is reachable, it is a
  second way to produce a permanent refusal on the same durable-Died-row
  mechanism, and our three specimens may not all be the marker story — which
  would be the difference between fixing this class and fixing most of it.
- **M2 — a true clamp, not `cap_floor`.** The minted floor is
  `min(computed_floor, lowest_retained_marker_seq)`. It must NOT be routed
  through `cap_floor`, for the reason stated above; if a new algebra helper is
  needed, add one and name it for what it does. The code states in a comment
  that `cap_floor` raises and this clamp lowers, so the next reader cannot make
  the same mistake.
  Monotonicity is safe and must be asserted, not assumed:
  `install_binding_fate_transition` retains only markers `>= resulting_floor`,
  so the lowest retained marker is never below `retained_floor`, and the clamp
  can therefore never drive a floor backwards.
  **The clamp target is the marker itself, not `marker - 1`.** The enforcer
  refuses on `record.delivery_seq < resulting_floor` — strictly below — so
  `resulting_floor == lowest_marker_seq` is admissible, and it is consistent
  downstream because `install_binding_fate_transition` retains markers
  `>= resulting_floor`, so a marker sitting exactly at the floor survives.
  "Clamp to just below the marker, to be safe" is the natural defensive reflex
  and it silently destroys legal floor advances: a green build in which the
  floor quietly stops moving.
  **The empty marker set is the majority case and must be stated, not inferred:**
  no retained markers ⇒ no clamp ⇒ the computed floor passes through
  byte-identical. A `min` over an empty set is otherwise resolved by a guess.
- **M3 — answer the reuse question out loud.**
  `ordinary_record_projection.rs:728-750` already does this properly for its own
  invariant: compute a base floor, run it through
  `search_capacity_floor(facts, &marker_state, …)`, recompute. That machinery is
  capacity/credit-shaped and precedence is a different invariant — the two are
  NOT asserted interchangeable here. The build must decide **reuse it or clamp
  beside it**, and record the reasoning in the code. A builder guessing is a
  brief failure, not a builder failure.
- **M4 — the refusal names its invariant.** Replace the lossy collapse at
  `binding_fate.rs:374` so the caller can tell `Precedence` from every other
  `LiveFrontierTransitionError`. This is not cosmetic: it is the difference
  between a one-hour diagnosis and a one-day one, measured twice now.
- **M5 — measure before you commit.** `liminal-server` durably appends the Died
  row before the measurement that can refuse it, so a refusal becomes permanent
  poison. Measure first and commit after, or make both one durable act. Endorsed
  by Hermes without further ruling; server-side ordering.
- **M6 — the regression that would have caught it.** A test that constructs a
  retained marker **below** the naively-computed floor and asserts the fate
  **admits** rather than refuses. Red before green: it must fail on today's tree
  and pass on the fixed one, with the red evidence committed.
- **M7 — the property family extended.** `properties.rs:198`'s floor family
  covers the clamp, including the degenerate case.

## Fences

- **F1 — no abandonment is built here.** Part C — a sanctioned
  complete-or-abandon at recovery — is Hermes's ruling and is explicitly out of
  scope. He has ruled that the protocol does **not** forbid it (durable
  abandonment atoms already exist at `client.rs:347-380`:
  `RestoredExpectedOperationAbandonment` / `TokenlessAfterCrash`, minted at a
  restore boundary, carried losslessly, taken exactly once), so it remains
  available as a shape. Whether it is *needed* is what G3 decides.
- **F2 — the poisoned stores are evidence, not scratch.** ⛔ The three poisoned
  estates are the only specimens of this class in existence. Every gate runs
  against **copies**. No binary, fixed or otherwise, is ever booted against an
  original. ⛔ The `.manifold-backup-*` originals stay untouched — that is the
  one thing here that cannot be undone. Preserved at
  `apps/manifold/.manifold-backup-20260805-intent329`,
  `.manifold-backup-20260801-intent82`,
  `.manifold-backup-20260731-spine-poison`, plus
  `.manifold-poisoned-parts-20260801`.
- **F2a — copy wholesale, never look inside.** These store trees hold real
  payloads: private conversations belonging to real people. Copy them as opaque
  directories. Do **not** `cat`, `grep`, `strings`, or otherwise inspect their
  contents, and do not include store contents in any report. The only things
  that may be reported from a specimen are the **boot outcome** and the
  **verbatim refusal line**.
- **F2b — a narrow, named exception to a standing fence.** Executors are barred
  from `/Users/tom/Developer/ablative/apps/manifold/` as standing estate policy.
  This brief lifts that fence for exactly one act: copying the four paths named
  in F2 to the builder's own scratch directory, and building the manifold binary
  for G3. Nothing else under that path may be read, written, moved or deleted —
  and the live `.manifold/` estate is not in the exception at all. Stated here,
  by the author, so no builder discovers a block and improvises around it.
- **F3 — nothing publishes.** No version bump, no tag, no crates.io release.
  Consuming this from manifold is a separate act behind Tom's own word. G3's
  local `[patch.crates-io]` override is gate machinery and is not landed.
- **F4 — the degenerate case is correct semantics.** When the lowest marker
  equals `retained_floor`, the floor does not advance. A marker pins the floor;
  that is the whole point of the rule. Nothing may "work around" it.
- **F5 — no widening anywhere else.** The clamp changes the outcome only where
  a floor would previously have been refused. Floors in marker-free cases are
  byte-identical to today's.

## Gates

- **G1 (red first)** — a failing test reproducing `Precedence` from the
  measurement path on today's tree; committed as red evidence before any fix.
- **G2 (the fix admits)** — M6's regression passes, and the finalizer path
  (`complete_pending_died_ordinary_finalizer`) is exercised too, not just the
  measurement path. Two enforcers, two proofs.
- **G3 (THE SPECIMEN GATE — this is the one that matters)** — build the
  manifold binary against the patched liminal (local path override) and boot it
  against a **copy** of each of the three poisoned stores. Record, per specimen,
  whether it boots clean or still latches, verbatim.
  This settles the open question: Hermes's leading hypothesis is that what is
  durable in a poisoned store is the Died row plus a pending fate intent — that
  `complete_pending_specific_fate` re-runs the protocol measurement rather than
  replaying a persisted bad floor — in which case **M1 un-bricks every existing
  poisoned estate on its next boot and no abandonment path is needed at all.**
  It is a hypothesis, not a finding, and the full replay path is untraced. G3 is
  how it becomes one or the other. Both outcomes are a result: if the copies
  boot, we state hand-recovery-is-unnecessary as a property; if they latch,
  Part C is real and Hermes rules the abandon semantics with the client-side
  atom as template.
  **G3 requires a positive control, and is invalid without it.** If the
  `[patch.crates-io]` override silently fails to apply, the specimen boots the
  OLD liminal and latches exactly as it always did — and we would read that as
  "the copies latch ⇒ Part C is real" and build an abandonment path nobody
  needs. That is a false green wearing a red coat, and it is the tool-absence
  class from the measurement catalogue: **the control must exercise the same
  predicate as the run.** Before believing either outcome, prove the booted
  binary actually contains the clamp. The cheapest honest form is to boot a copy
  under **both** the unpatched and the patched build and show the behaviour
  differs — which also makes a clean boot self-evidencing rather than merely
  hoped for. A run that cannot show the difference reports "inconclusive", never
  a verdict.
  **Report the refusal variant verbatim, per specimen.** M4's error preservation
  makes `Precedence` and `ResultingFrontier` distinguishable for the first time,
  so a latching specimen must say WHICH it is. That costs nothing and it is how
  M1b's question gets a second, independent answer from live evidence: if any
  specimen latches on `ResultingFrontier` rather than `Precedence`, the
  marker-free path is not hypothetical and the three specimens are not one
  story. (M4 was argued on diagnosis time; it has become load-bearing for the
  specimen gate.)
- **G4 (no regressions)** — full workspace battery fresh on the final tree:
  `cargo fmt --check`, `clippy --workspace --all-targets -D warnings`, the
  complete test suite. No `#[allow]`, no `#[ignore]`, no skips.
- **G5 (nothing else moved)** — floors computed in marker-free scenarios are
  unchanged from today's tree, demonstrated rather than asserted.

## What this brief does not do

No abandonment path (F1). No publish (F3). No change to the `Precedence` rule
itself — it is correct and stays. No recovery of any live estate: that is a
separate act, and which act it is depends entirely on what G3 reports.

## Why this is worth doing properly

The genesis history of the currently-bricked estate ends with a restoration note
written on 2026-08-01, after the second occurrence, recording that the defect
owner then had two independent trigger cases. The record has spent four days
telling us we were treating the symptom. This is the brief that stops writing
that note.
