#!/bin/bash
set -u
SCRATCH=/private/tmp/claude-501/-Users-annabel-Developer-ablative-stack-liminal/5b70322e-e7a9-451c-91ca-a3dfa7b05bd9/scratchpad
RUNNER=/Users/annabel/Developer/ablative/stack/liminal/.worktrees/leg-sdk/gate-evidence/canon-r3-local-d4.as-run.sh
export SEAT_NAME="Mercury Toast"
export MEMBER_ID=5b70322e-e7a9-451c-91ca-a3dfa7b05bda
unset AMP_ITERS AMP_PEERS AMP_BURNERS CONFORMANCE_RESULTS_DIR RUST_LOG DATABASE_URL CARGO_TARGET_DIR
for v in $(env | sed -n 's/^\(LIMINAL_[^=]*\)=.*/\1/p'); do unset "$v"; done
run_one() {
  wt=$1; label=$2
  export EVIDENCE_DIR="$wt/gate-evidence"
  mkdir -p "$EVIDENCE_DIR"
  WORKTREE="$wt" RUN_LABEL="$label" bash "$SCRATCH/release-battery-header.sh" 2>&1 | tee "$EVIDENCE_DIR/header.log"
  hrc=${PIPESTATUS[0]}
  if [ "$hrc" != "0" ]; then echo "HEADER FAILED ($hrc) for $label — NO LAUNCH"; return 90; fi
  ( cd "$wt" && bash "$RUNNER" 2>&1 | tee "$EVIDENCE_DIR/gate-run.log" )
  rc=$?
  echo "RUN $label runner-exit=$rc $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  return $rc
}
run_one /Users/annabel/Developer/ablative/stack/liminal/.worktrees/release-baseline main-baseline || { echo "RUN1 STOPPED rc=$?"; exit 1; }
run_one /Users/annabel/Developer/ablative/stack/liminal/.worktrees/release-0.5.1 release-tip || { echo "RUN2 STOPPED rc=$?"; exit 2; }
echo "BOTH RUNS COMPLETE $(date -u +%Y-%m-%dT%H:%M:%SZ)"
