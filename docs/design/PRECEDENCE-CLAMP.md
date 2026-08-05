# The Precedence clamp — a floor that cannot cross a marker, r1

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

- **M1 — one marker-aware minting point.** The resulting floor is minted in
  exactly one place that knows about `marker_records`, and **both** enforcement
  sites (`prepare_binding_fate_transition`, `install_finalized_binding_fate_floor`)
  draw from it. Correctness lives in the structure, not in two functions each
  remembering a rule.
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
  original. Preserved at
  `apps/manifold/.manifold-backup-20260805-intent329`,
  `.manifold-backup-20260801-intent82`,
  `.manifold-backup-20260731-spine-poison`, plus
  `.manifold-poisoned-parts-20260801`.
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
