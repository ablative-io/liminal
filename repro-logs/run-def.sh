#!/bin/sh
# Drives the three fidelity arms at 40 fresh-server iterations each, one arm
# per cargo invocation so libtest's single filter string can select it.
set -u
cd /Users/tom/Developer/ablative/stack/liminal/.claude/worktrees/agent-adaf5ddccd511edb6
for arm in arm_d arm_e arm_f; do
  LIMINAL_WS_REPRO_ITERS=40 cargo test -p liminal-server --test ws_parked_delivery_e2e \
    -- --test-threads=1 --nocapture "$arm" > "repro-logs/07-${arm}-40x.log" 2>&1
  echo "TRUE_EXIT=$?" >> "repro-logs/07-${arm}-40x.log"
done
echo ALL_ARMS_DONE
