#!/bin/bash
# Same reaper-proof shape as run-battery-detached.sh, for the clippy leg.
set -u
WT=/Users/tom/Developer/ablative/stack/liminal-wt-dep-currency
LOG="$WT/gate-logs/p-dep-currency-latest/clippy.log"
{
  echo "DETACHED_LAUNCH pid=$$ at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  cd "$WT" || { echo "SENTINEL_DONE TRUE_EXIT=CDFAIL"; exit 1; }
  cargo clippy --workspace --all-targets 2>&1
  echo "TRUE_EXIT=$?"
  echo "SENTINEL_DONE at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "$LOG" 2>&1
