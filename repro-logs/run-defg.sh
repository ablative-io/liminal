#!/bin/sh
# Fidelity arms at 40 fresh-server iterations each (arm G at 6, it costs a
# 65s park per iteration), one arm per cargo invocation so libtest's single
# filter string can select it.
set -u
cd /Users/tom/Developer/ablative/stack/liminal/.claude/worktrees/agent-adaf5ddccd511edb6
for arm in arm_d arm_e arm_f; do
  LIMINAL_WS_REPRO_ITERS=40 cargo test -p liminal-server --test ws_parked_delivery_e2e \
    -- --test-threads=1 --nocapture "$arm" > "repro-logs/08-${arm}-40x.log" 2>&1
  echo "TRUE_EXIT=$?" >> "repro-logs/08-${arm}-40x.log"
done
LIMINAL_WS_REPRO_ITERS=6 cargo test -p liminal-server --test ws_parked_delivery_e2e \
  -- --test-threads=1 --nocapture arm_g > repro-logs/08-arm_g-6x.log 2>&1
echo "TRUE_EXIT=$?" >> repro-logs/08-arm_g-6x.log
echo ALL_ARMS_DONE
