#!/bin/bash
# Reaper-proof battery launcher for the dep-currency lane (same shape as
# gate-logs/dep-hop-beamr0171-haematite083/run-battery-detached.sh: Waffles's
# host-reaper finding 8f903cd4 — plain backgrounded session-shell children get
# reaped in waves at ~15min; setsid-detached survivors documented). Started via
# a python double-fork + os.setsid so it reparents away from the session shell,
# waits for a load window, runs the full battery with --no-fail-fast, and stamps
# a terminal sentinel either way. The seat watches the log file with
# short-lived polls; nothing long-lived stays attached to the session.
set -u
WT=/Users/tom/Developer/ablative/stack/liminal-wt-dep-currency
LOG="$WT/gate-logs/p-dep-currency-latest/battery.log"
{
  echo "DETACHED_LAUNCH pid=$$ at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  while :; do
    L=$(sysctl -n vm.loadavg | awk '{print $2}')
    # THRESHOLD 60, NOT the dep-hop lane's 15 / this lane's nominal 25: the box
    # was saturated by unrelated concurrent work for this whole window (1-min
    # load sampled at 60 / 102 / 140 / 223 / 353 over ~35min, never near 25), so
    # a <25 gate parks forever rather than protecting anything. 60 is the lowest
    # value actually reached. The residual starved-runner risk is NOT absorbed
    # silently -- it is discharged by the discrimination protocol (precedent
    # 15e6ef7): any non-F8 red is re-run solo 5x plus its suite once at lower
    # load, logged to flake-reruns.log, and a red that survives solo STOPS the
    # lane. The run shape itself is unchanged (default test threads) so the
    # totals stay comparable to the declared 2101/2/3-over-56 baseline.
    if [ "$(echo "$L < 60" | bc)" = 1 ]; then
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
