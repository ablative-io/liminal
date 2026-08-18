#!/bin/bash
# Starved-runner discrimination protocol, per the p0-60 precedent (liminal
# 15e6ef7): solo re-runs of the two unexpected reds from the dep-hop battery,
# recorded with load readings, terminal sentinel stamped. Launched detached
# (host-reaper law). The battery's red lines stay in the ledger either way.
set -u
WT=/Users/tom/Developer/ablative/stack/liminal-wt-dep-hop
LOG="$WT/gate-logs/dep-hop-beamr0171-haematite083/flake-reruns.log"
cd "$WT" || exit 1
{
  echo "RERUN_PROTOCOL start $(date -u +%Y-%m-%dT%H:%M:%SZ) loadavg=$(sysctl -n vm.loadavg)"
  for i in 1 2 3 4 5; do
    echo "== inbox_overflow solo run $i loadavg=$(sysctl -n vm.loadavg)"
    cargo test -p liminal-server --test subscription_e2e inbox_overflow_sheds_the_offending_subscription_without_tearing_down_the_connection 2>&1 | grep -E "^test |test result:"
    echo "RUN${i}_EXIT=$?"
  done
  echo "== subscription_e2e full suite loadavg=$(sysctl -n vm.loadavg)"
  cargo test -p liminal-server --test subscription_e2e 2>&1 | grep -E "^test |test result:"
  echo "SUITE_EXIT=$?"
  for i in 1 2 3; do
    echo "== starvation mixed-fate solo run $i loadavg=$(sysctl -n vm.loadavg)"
    cargo test -p liminal-server --test subscription_starvation_e2e b_two_websocket_subscribers_on_one_boot_share_the_same_fate 2>&1 | grep -E "^test |test result:|MIXED-FATE"
    echo "MFRUN${i}_EXIT=$?"
  done
  echo "== starvation full suite loadavg=$(sysctl -n vm.loadavg)"
  cargo test -p liminal-server --test subscription_starvation_e2e 2>&1 | grep -E "^test |test result:"
  echo "STARVE_SUITE_EXIT=$?"
  echo "SENTINEL_DONE at $(date -u +%Y-%m-%dT%H:%M:%SZ) loadavg=$(sysctl -n vm.loadavg)"
} > "$LOG" 2>&1
