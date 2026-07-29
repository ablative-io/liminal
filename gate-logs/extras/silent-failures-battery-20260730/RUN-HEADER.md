# Leg E landing battery — run header (committed BEFORE the run)

Seat: Horus Ham and Cheese (ecad4cb7-b79b-4cb1-a1e7-8ee629ea610b). Machine:
Dean's box (the shared battery venue). Branch: fix/silent-failures @ cb6b953
(base c921827).

## Pre-stated expected denominator (binding: stated before, compared after)

- Expected tests RUN: **1785**
- Expected SKIPPED: **2** (the two `#[ignore]` AMP amplifier e2e tests)
- Derivation: `cargo nextest list --workspace` on this exact tree, pre-run.
- CONTRAST, named not absorbed: the base evidence f88d3f1 records 1806/0/42 at
  tree 35c9b61 — a different tree AND liminal's own gates harness, which counts
  differently from canon r3's nextest leg. This run is judged against the
  pre-statement above; disagreement between achieved and stated is RED.

## Post-run reconciliation asserted (disagreement is RED, not a wobble)

- JSON stream vs Summary line: test-level `started` == `ok + failed + ignored`
- JSON `ignored` == Summary `skipped`

## Environment contract (both halves)

- Required SET: nothing (no DATABASE_URL anywhere in this repo; no CI workflows)
- Required UNSET, re-confirmed at launch in this run's env-contract.txt:
  AMP_ITERS, AMP_PEERS, AMP_BURNERS (unset = the amplifier BODY does not run —
  set/unset changes body execution, not test presence), CONFORMANCE_RESULTS_DIR,
  RUST_LOG, and zero LIMINAL_* variables.

## Harness identity and convention pin set

- Canon r3 byte-verbatim: entry e269d2c9, body sha ff831516…, script sha
  bf404f6a… (wrapper refuses other bytes). r4 exists but is HELD (blocking
  release-path defect) — this run is r3 + disclosed wrapper deltas, as briefed.
- Wrapper package (Hermes-confirmed shape): leg-e-battery.sh, five disclosed A5
  deltas in its header citing a621f353 (+ fe983cc6 scope-of-claim correction,
  + the absent-without-own-release ordering test). The wrapper ships itself into
  this directory (wrapper-as-run.sh) and writes WRAPPED.marker — records an
  unwrapped launch cannot produce.
- Claim discipline: one claim at /tmp/ablative-gate-battery.claim; census (not
  claim) is the quiet-floor proof; batteries serialized one-at-a-time by the
  dispatcher (Seth Crackers) — the acquisition race is structurally absent at
  this venue, cited alongside the census per the dispatcher's instruction.
- Exit semantics: 0 = COMPLETE (verdict from extracts + A6 count-match, never
  from the exit code), 4/5 = loud refusals, 6 = RUN VOID AS EVIDENCE (claim
  integrity violated — a detector, not a red).

## Model disclosure

Implementation subagents: batch 1 default-inherit (Fable), batches 2-3 pinned
opus5-implementer. Sweep agents: read-only Explore type.
