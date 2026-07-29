---
name: shared-target-dir-battery-races
description: Concurrent cargo batteries from different branches sharing one CARGO_TARGET_DIR execute WRONG binaries — per-lane target dirs or serialize; suite-count drift is the tell
metadata: 
  node_type: memory
  type: project
  originSessionId: 6bfc46aa-2bfa-43f8-a284-0ba8a3713539
  modified: 2026-07-22T13:10:28.271Z
---

Found 2026-07-22 during the W4 leg-2 fold: two builders on different branches (leg-2 fix worktree + receipts-fix worktree) both ran batteries against the shared `CARGO_TARGET_DIR=.../liminal/target`. Cargo's flock serializes BUILDS but not build-vs-run: my `cargo test` built the leg-2 binary, released the lock, the other lane rebuilt from ITS branch overwriting the artifact, then my harness executed the other branch's bytes — my "battery" ran 5 fewer tests (exactly the leg-2 additions; server lib showed the pre-leg count 549) and earlier produced transient unnamed "reds" that were cross-binary contamination, not flakes.

**Why:** artifact paths are branch-agnostic; content fingerprints govern rebuilds, not execution — the binary on disk at exec time is whoever built last.

**How to apply:** (1) RULED RESOLUTION (Tom, 2026-07-22, estate-wide): target directories are DEFAULTS ONLY — never set CARGO_TARGET_DIR; each worktree's own default `target/` provides the cross-branch isolation and dies with the worktree. Full cold builds per worktree = accepted cost. (2) The tell is SUITE-COUNT DRIFT: passed+failed totals lower than the branch's expected count = wrong binary ran; always compare totals against the expected count before trusting a green. (3) Unexplained transient reds during concurrent lane work are contamination-class until proven otherwise — get names before calling flake. (4) Waffles serializes his tear batteries for the flake-manufacture reason; defaults-only is the harder correctness version of the same law. See [[worker-liveness-deadlines]], [[liminal-repo-state]].
