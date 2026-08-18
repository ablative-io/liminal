#!/bin/bash
# Reaper-proof battery launcher for the dep-hop lane (Waffles's host-reaper
# finding 8f903cd4: plain backgrounded session-shell children get reaped in
# waves at ~15min; setsid-detached survivors documented). This script is
# started via `setsid` so it reparents away from the session shell, waits for
# the load window Waffles ruled (1-min load < 15), runs the full battery with
# --no-fail-fast, and stamps a terminal sentinel line either way. The seat
# watches the log file; nothing long-lived stays attached to the session.
set -u
WT=/Users/tom/Developer/ablative/stack/liminal-wt-dep-hop
LOG="$WT/gate-logs/dep-hop-beamr0171-haematite083/seat-battery.log"
{
  echo "DETACHED_LAUNCH pid=$$ at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  while :; do
    L=$(sysctl -n vm.loadavg | awk '{print $2}')
    if [ "$(echo "$L < 15" | bc)" = 1 ]; then
      echo "LOAD_OK $L at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
      break
    fi
    sleep 60
  done
  cd "$WT" || { echo "SENTINEL_DONE TRUE_EXIT=CDFAIL"; exit 1; }
  cargo test --workspace --no-fail-fast 2>&1
  echo "TRUE_EXIT=$?"
  echo "SENTINEL_DONE at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "$LOG" 2>&1
