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

**Each field has exactly one role. A consumer that compares a RECORD field
false-REDs on every run, forever, on every box** — `summary_line` alone carries
a wall-clock duration. The partition is part of the pin, not commentary on it:

| Role | Fields | Meaning |
|------|--------|---------|
| **COMPARE** | `tests_started`, `tests_ok`, `tests_failed`, `tests_ignored`, `suites` | The predicate. Drift in either direction is RED. |
| **REQUIRE** | `baseline_tree`, `toolchain`, `cargo`, `command` | Preconditions. A mismatch does not fail the gate — it makes the comparison **void**, which must be reported as such and never as a pass. |
| **RECORD** | `summary_line`, `machine`, `operator`, `measured_at`, `runner`, `evidence_branch`, `sibling_run` | Provenance. Print, never compare. |

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

## ⚠️ How to extract the numbers — the raw stream is TWO POPULATIONS SUMMED

`libtest-json-plus` emits `"event":"started"` for **suites as well as tests**.
Counting the raw event yields a plausible, wrong number. Measured at the
committed evidence (`origin/evidence/main-baseline-battery @ 67810be`,
`gate-evidence/nextest.json.log`, 594364 bytes, 3600 lines):

```
grep -c '"event":"started"'   ->  1800   <- WRONG: 1764 tests + 36 suites
grep -c '"type":"test"'       ->  3528   =  1764 x 2 (started + ok)
total lines                       3600   =  3528 + 72 (36 suites x 2)
```

1764 + 36 = 1800 and 3528 + 72 = 3600, both closing with no remainder.
**Filter on `"type":"test"` BEFORE counting events.** 1800 is the dangerous
value precisely because it looks like a test count and would survive review as
a new baseline. A count with a stated denominator can still be a count of two
things added together — the partition is a separate obligation from the count
(Cally Ray, ratified estate-wide 2026-07-29 at topic entry `27c11415`).

## How to consume it

Run the pinned command at the tree under gate, under the pinned toolchain,
and compare `tests run` / `passed` / `skipped` against the block. If the diff
from `baseline_tree` to the gated tree touches zero `.rs` files, the counts
MUST be identical and drift in either direction is RED. If `.rs` files moved,
the expected delta must be reasoned per change (the per-leg pattern: each
gate names its own additions), never pattern-matched.

## Known limits, carried with the number

- **⚠️ SIX ARTIFACTS IN THE CITED EVIDENCE BRANCHES ARE ZERO BYTES**, on
  **both** `evidence/main-baseline-battery @ 67810be` and
  `evidence/release-0.5.1-battery @ fbf444c` (blob sizes from
  `git ls-tree -r -l`): `gate-evidence/nextest.extract.jsonl`,
  `check.extract.jsonl`, `clippy.extract.jsonl`, `census-at-start.txt`,
  `census-at-end.txt`, `gate-logs/fmt.log`. Two consequences, and they are not
  the same severity. **The extracts are the machine-readable layer a consumer
  naturally reaches for, and they are empty** — the numbers in this pin are
  derivable, but only from the 594KB `nextest.json.log` beside them, so read
  that and not the file whose name says `extract`. **The two census files are
  worse in kind: an empty census cannot distinguish "no actors" from "could
  not look", and it renders as QUIET** — the tool-absence class, failing
  toward harm. Neither defect touches the counts, which reconcile four ways
  above; both are disclosed rather than repaired here because repairing them
  means re-running a battery, and the counts do not need one.
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
  another sample.
  **UPGRADED 2026-07-30 from unreproduced to CLASSIFIED, and the two are
  different logical objects.** The baseline run (tree `c921827`) carried the
  leak; the sibling run (tree `62d9b80`) did not. **The `.rs` delta between
  those two trees is ZERO** — `git diff --numstat c921827 62d9b80 -- '*.rs'`
  reports 0 files, +0/-0; the whole delta is 7 files, all manifests and docs
  (`CHANGELOG.md`, `Cargo.lock`, `README.md`, four `Cargo.toml`). **Zero `.rs`
  delta is NOT sufficient on its own — a `Cargo.lock` move can change the code
  that actually runs — so the lock was checked too: its delta is 3 lines,
  `version = "0.5.0"` → `"0.5.1"` on liminal's own three path crates, with no
  third-party dependency moving at all**; the manifests carry nothing but the
  same self-version strings. The executable code is therefore identical across
  the two runs (the sole reachable difference is `CARGO_PKG_VERSION`, which
  cannot leak a process), and the
  observation appears in exactly one of them. **By the intersection rule —
  present in both ⇒ tree-determined, present in one ⇒ not tree-determined —
  the leak falls in the symmetric difference and is NOT tree-determined.**
  That is positive proof of non-determinism, whereas 338 clean executions were
  only failure to reproduce, and no number of clean runs proves absence. **The
  same-tree premise is the load-bearing half and it is MEASURED here, not
  assumed** — an intersection argument over two runs of *different* trees
  proves nothing at all. **TRIPWIRE, binding on future sweeps:** if `Command::new` or
  `process::Command` ever appears anywhere under `crates/liminal`, the
  disposition EXPIRES and the observation re-opens; likewise on any recurrence
  of LEAK in any future battery, on any test.
