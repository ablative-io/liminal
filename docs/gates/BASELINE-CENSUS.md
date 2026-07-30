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
(Cally Ray, §7 of topic entry `27c11415`, ratified estate-wide 2026-07-29 and
still standing; **note that Amendment One of that same entry was later
withdrawn in full at `ab13367a` — cite the section, not the entry**).

## How to consume it

**The runner is `scripts/baseline-compare.py`, and it reads this file** — so
the pin stays the single source of truth and cannot drift from its consumer:

```
python3 scripts/baseline-compare.py <nextest.json.log> \
    [--toolchain "<rustc -V of this run>"] [--tree <sha>] [--expect-total N]
```

Exit codes: **0 PASS · 1 RED · 2 instrument/usage error · 3 VOID.** It parses
the JSON rather than matching text, so the suite/test population split and the
compact-vs-spaced byte form cannot mislead it. It **refuses** rather than
reports on an empty stream, an unparseable line, or a run that disagrees with
itself — a dead producer and an empty world are the same number, so a zero is
never compared. `--expect-total` is the stated-in-advance denominator for a
tree whose `.rs` files moved, and it is echoed in the verdict.

Proven both ways against committed evidence before first use: the baseline and
release-tip streams PASS; a toolchain mismatch VOIDs; `--expect-total 1772`
against the 1764 run REDs naming `-8`; empty, corrupt, and five-tests-missing
streams are each refused with the correct diagnosis.

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
  `census-at-end.txt`, `gate-logs/fmt.log`.

  **⚠️ CORRECTED 2026-07-30 — I FILED THESE AS SIX DEFECTS AND THEY ARE THREE
  DIFFERENT THINGS. I read a state (six empty files) and inferred a rule (six
  faults), which is the same error I withdrew four hours earlier in
  `RELEASE-RECORD.md` §4.** Measured at the evidence, not re-read from my own
  note:

  | Artifact | Measurement | Class |
  |---|---|---|
  | `gate-logs/fmt.log` | **belongs to a DIFFERENT RUN — see the two-runs finding below** | **NOT PART OF THIS BATTERY AT ALL** |
  | `clippy.extract.jsonl` | `clippy.json.log` = 374 lines, **zero `compiler-message` records** | **LEGITIMATELY EMPTY, NOT ASSERTED** — clean run ⇒ empty extract is right, but nothing records that it was *observed* |
  | `check.extract.jsonl` | `check.json.log` = 374 lines, **zero diagnostics** | same |
  | `nextest.extract.jsonl` | **1764 tests ran** | **BROKEN** — empty is impossible here |
  | `census-at-start.txt` / `census-at-end.txt` | — | **BROKEN, AND WORST IN KIND:** an empty census cannot distinguish *no actors* from *could not look*, and it renders as QUIET — the tool-absence class, failing toward harm |

- **🔴 THE CITED EVIDENCE BRANCH CONTAINS TWO DIFFERENT RUNS, AND NOTHING IN IT
  SAYS SO.** Found 2026-07-30 while checking a claim I had made about one of
  these files. `gate-evidence/` and `gate-logs/` describe **different
  batteries, on different boxes, with different toolchains**:

  | | `gate-evidence/*` | `gate-logs/report.json`, `fmt.log`, `clippy.log`, `tests.log` |
  |---|---|---|
  | when | **2026-07-29** 21:21→21:29Z | **2026-07-28** 05:09→05:17Z |
  | who / where | **Mercury Toast, Annabel's box**, pid 58281 | **`Toms-MacBook-Pro.local`** |
  | branch | the baseline battery | **`feat/handshake-protocol`** |
  | commit | **`c921827`** (this pin's `baseline_tree`) | **`4c2a4d8`** — an *ancestor*, verified with `git merge-base --is-ancestor` |
  | rustc | **1.97.1** (the pinned toolchain) | **1.92.0** |

  **⇒ ANYONE READING `gate-logs/report.json` FOR THIS BATTERY'S PROVENANCE GETS
  THE WRONG BOX, THE WRONG BRANCH, THE WRONG COMMIT AND THE WRONG TOOLCHAIN** —
  and it is a valid, complete, internally consistent document, so nothing about
  it looks wrong. **The counts are unaffected**: they derive from
  `gate-evidence/nextest.json.log` (594364 bytes, the 07-29 run), not from
  `gate-logs/tests.log` (203667 bytes, the 07-28 run) — **the size split is
  itself the tell.**

  **★ THIS IS A CLASS NO FRAMING, TRAILER, OR SIZE GUARD CATCHES.** Every such
  check answers *"did the producer finish writing this?"* — and this artifact's
  producer finished perfectly, **a day earlier, on another machine.**
  ⇒ **A COMPLETENESS CHECK CANNOT ANSWER AN IDENTITY QUESTION. The frame must
  carry the RUN'S IDENTITY — tree sha, claim pid, `started_at` — and the
  consumer must compare it to the run it believes it is reading.** Without
  that, a stale artifact is indistinguishable from a fresh one by construction.
  **Whether these files were carried deliberately or left behind, nothing in
  the branch distinguishes them, and only `report.json`'s own metadata reveals
  it** — which is to say, only by reading the thing you were trying to trust.

  **⇒ AND THE FILE-SIZE POINT BELOW STILL STANDS ON ITS OWN: THE DISCRIMINATOR
  IS NOT FILE SIZE, IT IS WHETHER EMPTY IS A LEGAL OUTCOME FOR THAT PRODUCER** — and for the two that are legal, nothing in the
  evidence says the emptiness was measured rather than suffered. **A blanket
  size guard would false-RED the clean clippy and check runs**, which is why
  the fix is a trailer asserting an explicit count and not a non-zero check.
  The numbers in this pin are derivable only from the 594KB
  `nextest.json.log`, so read that and not the file whose name says `extract`.
  None of this touches the counts, which reconcile four ways above; disclosed
  rather than repaired because repairing means re-running a battery, and the
  counts do not need one.
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
  observation appears in exactly one of them. **An observation that varies
  across two executions of identical code is not caused by that code.** That
  is the whole argument and it rests on no ruling: the leak is NOT
  tree-determined.
  That is positive proof of non-determinism, whereas 338 clean executions were
  only failure to reproduce, and no number of clean runs proves absence. **The
  identical-code premise is the load-bearing half and it is MEASURED here in
  two independent ways (`.rs` delta and lock delta), not assumed** — the same
  argument over two runs of *different* code proves nothing, because the
  difference is then equally explained by the change itself. That precondition
  is what failed elsewhere in the estate on this same night and cost a
  stack-lead ruling its full retraction (`ab13367a`), which is why it is
  measured here rather than asserted. **TRIPWIRE, binding on future sweeps:** if `Command::new` or
  `process::Command` ever appears anywhere under `crates/liminal`, the
  disposition EXPIRES and the observation re-opens; likewise on any recurrence
  of LEAK in any future battery, on any test.
