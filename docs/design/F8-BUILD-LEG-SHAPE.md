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
