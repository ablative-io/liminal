# Cross-SDK conformance suite (SDK-009)

Verifies that the Rust, Gleam, and TypeScript SDKs produce **identical
observable outcomes** for the behaviours in `scenarios.json`: connection
lifecycle, subscription recovery, backpressure vocabulary, and conversation
lifecycle. This enforces design principle P6 (cross-SDK behavioural parity).

Each SDK ships a harness that drives the scenarios against its own **public
API** and writes a machine-readable result file. `compare.py` then checks that
every SDK observed the same values (and that each matched its expectation).

## Layout

- `scenarios.json` — the shared, declarative scenario fixtures.
- `compare.py` — cross-SDK comparison; exits non-zero on any divergence.
- `results/` — harness outputs (`<sdk>.json`). Regenerated on every run and
  git-ignored: they are transient IPC between the harnesses and `compare.py`,
  not source.

## Running

Each harness writes `<sdk>.json` into the directory named by
`CONFORMANCE_RESULTS_DIR`. Point all three at the same directory, then compare:

```sh
export CONFORMANCE_RESULTS_DIR="$PWD/tests/conformance/results"

# Rust
cargo test -p liminal-sdk --test conformance

# TypeScript
( cd sdks/liminal-ts && npm run test )

# Gleam (the harness prints the result JSON on stdout)
( cd sdks/liminal-gleam && gleam test ) \
  | grep '^{"sdk":"gleam"' > "$CONFORMANCE_RESULTS_DIR/gleam.json"

# Compare across all three
python3 tests/conformance/compare.py
```

A zero exit and `conformance comparison passed: N scenarios matched across 3
SDKs` means the SDKs are in parity. Any divergence is reported with the SDK,
scenario, and expected-vs-observed values, and exits non-zero.
