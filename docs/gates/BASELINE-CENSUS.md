# Gate baseline census — the denominator main never carried

The 0.5.1 release gate was briefed as "the count must equal main's exactly"
when no committed count existed on main to compare against — the gap was
worked around by running the battery twice on one box (sound, not
repeatable). This file is the control that closes it: a future gate re-runs
the command below at its own tree and compares its numbers against these,
so drift is attributable to a tree change rather than to an unremembered
difference in how the counting was done. A pin no runner mechanically
compares is an instruction, not a control — the block below is written to be
consumed, not admired.

## The pin (machine-consumable)

```
baseline_tree      = c9218279a5c187c977161ea3d1d9eb2e07d6379b
baseline_tree_short = c921827
summary_line       = Summary [ 130.895s] 1764 tests run: 1764 passed (3 slow, 1 leaky), 0 skipped
tests_started      = 1764
tests_ok           = 1764
tests_failed       = 0
tests_ignored      = 0
suites             = 36
toolchain          = rustc 1.97.1 (8bab26f4f 2026-07-14)
cargo              = cargo 1.97.1 (c980f4866 2026-06-30)
toolchain_pin      = rust-toolchain.toml (tree-pinned)
machine            = Annabel's box (Annabels-MacBook-Pro, aarch64-apple-darwin)
operator           = Mercury Toast (5b70322e-e7a9-451c-91ca-a3dfa7b05bda)
measured_at        = 2026-07-29T21:21:29Z..21:25:08Z (claim window, pid 58281)
command            = NEXTEST_EXPERIMENTAL_LIBTEST_JSON=1 cargo nextest run --workspace --cargo-quiet --message-format libtest-json-plus --no-fail-fast
runner             = canon r3 + five disclosed deltas, as-run sha256 82ea8eac3f9e0bf576d784bc20fa1c410fd48a94ebfc765aeb6d816abcbc49f9
evidence_branch    = evidence/main-baseline-battery @ 67810be (off c921827)
sibling_run        = evidence/release-0.5.1-battery @ fbf444c (off 62d9b80, identical counts)
```

Reconciliation held at the source run, four ways: test-level started (1764)
equals ok (1764) + failed (0) + ignored (0); the 36 suite-level sums agree;
nextest Summary totals agree; JSON `ignored` equals Summary `skipped` (0).

## How to consume it

Run the pinned command at the tree under gate, under the pinned toolchain,
and compare `tests run` / `passed` / `skipped` against the block. If the diff
from `baseline_tree` to the gated tree touches zero `.rs` files, the counts
MUST be identical and drift in either direction is RED. If `.rs` files moved,
the expected delta must be reasoned per change (the per-leg pattern: each
gate names its own additions), never pattern-matched.

## Known limits, carried with the number

- **Doc tests are not covered** — the canonical battery script has no
  doc-test leg (the runner's own disclosure line).
- **The census quiet is a BOUNDARY claim, not a throughout claim** — the
  runner censuses at start and end and samples nothing during the gate legs.
- **`pgrep` on this box is blind to root-owned processes** (probe: `pgrep -x
  launchd` silent while `ps -p 1` sees it); the census population is
  same-user processes, which is where compiles live. Disclosed, not assumed
  away.
- **The baseline run carried "1 leaky"** (its sibling release-tip run carried
  none): `liminal-rs routing::dispatch::tests::registration_helper_constructs_consumer_state`
  was marked LEAK once. Diagnosis at lane entry
  `922cedb3-7bf3-459a-b700-bcc20b0dbf65`: the instrument was proven
  two-directionally, the leak is UNREPRODUCED at 338 direct executions of the
  named test (3214 total test executions across contexts), the test and its
  whole crate contain zero child-process spawn sites, and the observation is
  carried as a known-unknown — not closed, not diagnosed into a theory.
  "Leaky" is not a count term; it does not affect this denominator.
  Disposition (lane entry `39142c52-a6bf-4248-b4ba-630061f70717`): carried as
  known-unknown, no battery slot spent — a structural impossibility beats
  another sample. **TRIPWIRE, binding on future sweeps:** if `Command::new` or
  `process::Command` ever appears anywhere under `crates/liminal`, the
  disposition EXPIRES and the observation re-opens; likewise on any recurrence
  of LEAK in any future battery, on any test.
