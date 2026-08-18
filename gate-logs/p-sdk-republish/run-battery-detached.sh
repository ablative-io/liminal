#!/bin/bash
# Reaper-proof battery launcher for the liminal-sdk 0.8.0 republish lane (queue
# line 10; version+CHANGELOG only off 8a36db0). Same shape as
# gate-logs/p-dep-currency-latest/run-battery-detached.sh -- Waffles's
# host-reaper finding 8f903cd4: plain backgrounded session-shell children get
# reaped in waves at ~15min, so this is started via a python double-fork +
# os.setsid and reparents to PID 1. Nothing long-lived stays attached to the
# session; the seat watches the logs with short-lived polls.
#
# GATES, IN ORDER:
#   (a) the battery-queue TOKEN FILE -- entries 1..9 all marked DONE
#   (b) a QUIET BOX -- 1-min load < 25 on two samples 60s apart
#   (c) cargo test --workspace --no-fail-fast -j4   -> battery.log
#   (d) cargo clippy --workspace --all-targets -j4  -> clippy.log
#   (e) SENTINEL_DONE
#   (f) cargo clean of THIS worktree's target
set -u

WT=/Users/tom/Developer/ablative/stack/liminal-wt-sdk-republish
GD="$WT/gate-logs/p-sdk-republish"
UNITLOG="$GD/unit.log"
BATLOG="$GD/battery.log"
CLIPLOG="$GD/clippy.log"
TOKEN=/Users/tom/Developer/ablative/docs/tracking/battery-queue-20260818.md
LOAD_MAX=25

ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }

# Extract one numbered entry's block from the token file: from the line that
# starts "N. " up to (not including) the next line that starts with a number.
entry_block() {
  awk -v n="$1" '
    $0 ~ "^"n"\\. "                                   { inblock = 1 }
    inblock && /^[0-9]+\. / && $0 !~ "^"n"\\. "        { inblock = 0 }
    inblock                                           { print }
  ' "$TOKEN"
}

# A line is a real DONE marker only if "DONE" is followed by an ISO date.
#
# ⚠ A BARE `grep DONE` IS WRONG HERE and this is not hypothetical: entry 3's
# prose contained the phrase "lines 1-2 DONE + own quiet check", which a bare
# predicate reads as entry 3 being finished. That would have fired this battery
# into the middle of someone else's slot. The date anchor is the discriminator.
# Proven both ways against the live file before arming: entries 1 and 2 (real
# markers, "DONE 2026-08-18T07:57:57Z") matched, entries 3 and 4 (unfinished,
# one of them carrying the decoy phrase) did not.
entry_done() {
  entry_block "$1" | grep -qE 'DONE[[:space:]]+[0-9]{4}-[0-9]{2}-[0-9]{2}'
}

one_min_load() { sysctl -n vm.loadavg | awk '{print $2}'; }

{
  echo "DETACHED_LAUNCH pid=$$ ppid=$PPID at $(ts)"
  echo "WORKTREE=$WT"
  echo "BRANCH=$(cd "$WT" && git rev-parse --abbrev-ref HEAD 2>/dev/null)"
  echo "HEAD=$(cd "$WT" && git rev-parse HEAD 2>/dev/null)"
  echo "TOKEN=$TOKEN"
  echo "LOAD_MAX=$LOAD_MAX"
  echo "EXPECTED baseline 2101 passed / 2 failed / 3 ignored over 56 suites;"
  echo "EXPECTED TRUE_EXIT=101 -- the 2 reds are the DECLARED F8 instruments"
  echo "  tests_f8_marker_poison::a_refused_connection_fate_leaves_no_durable_residue"
  echo "  tests_f8_marker_poison::the_incident_sequence_reboots_into_a_discharged_fate_and_a_live_server"
  echo "These two are NEVER to be fixed. Any OTHER red is a real finding."

  # ---- GATE (a): the token file ------------------------------------------
  echo "GATE_A_WAIT token file, entries 1..9 must all be DONE, at $(ts)"
  while :; do
    if [ ! -r "$TOKEN" ]; then
      echo "GATE_A_TOKEN_UNREADABLE at $(ts) -- refusing to fire blind"
      sleep 60
      continue
    fi
    pending=""
    for n in 1 2 3 4 5 6 7 8 9; do
      entry_done "$n" || pending="$pending $n"
    done
    if [ -z "$pending" ]; then
      echo "GATE_A_OK all of 1..9 DONE at $(ts)"
      break
    fi
    echo "GATE_A_PENDING entries:$pending at $(ts)"
    sleep 60
  done

  # ---- GATE (b): a quiet box, sustained ----------------------------------
  echo "GATE_B_WAIT 1-min load < $LOAD_MAX on two samples 60s apart, at $(ts)"
  while :; do
    L1=$(one_min_load)
    if [ "$(echo "$L1 < $LOAD_MAX" | bc)" = 1 ]; then
      echo "GATE_B_SAMPLE1 load=$L1 at $(ts) -- confirming in 60s"
      sleep 60
      L2=$(one_min_load)
      if [ "$(echo "$L2 < $LOAD_MAX" | bc)" = 1 ]; then
        echo "GATE_B_OK load1=$L1 load2=$L2 at $(ts)"
        break
      fi
      echo "GATE_B_LOST load1=$L1 load2=$L2 at $(ts) -- not sustained, re-arming"
    else
      echo "GATE_B_BUSY load=$L1 at $(ts)"
      sleep 60
    fi
  done

  cd "$WT" || { echo "SENTINEL_DONE TRUE_EXIT=CDFAIL at $(ts)"; exit 1; }

  # ---- GATE (c): the battery ---------------------------------------------
  # ⚠ $? after a pipe is TEE's status, not cargo's -- the pipeline-exit
  # false-green. PIPESTATUS[0] is the only honest reading here.
  echo "BATTERY_START at $(ts)"
  cargo test --workspace --no-fail-fast -j4 2>&1 | tee "$BATLOG"
  TEST_EXIT=${PIPESTATUS[0]}
  echo "TRUE_EXIT=$TEST_EXIT" | tee -a "$BATLOG"
  echo "BATTERY_END at $(ts)"

  # ---- GATE (d): clippy ---------------------------------------------------
  echo "CLIPPY_START at $(ts)"
  cargo clippy --workspace --all-targets -j4 2>&1 | tee "$CLIPLOG"
  CLIPPY_EXIT=${PIPESTATUS[0]}
  echo "TRUE_EXIT=$CLIPPY_EXIT" | tee -a "$CLIPLOG"
  echo "CLIPPY_END at $(ts)"

  # ---- GATE (e): the sentinel --------------------------------------------
  echo "TEST_TRUE_EXIT=$TEST_EXIT CLIPPY_TRUE_EXIT=$CLIPPY_EXIT"
  echo "SENTINEL_DONE at $(ts)"

  # ---- GATE (f): disk, in the same breath --------------------------------
  # Runs LAST, after every log above is safely written. The logs live in
  # gate-logs/ (in-tree), never under target/, so this cannot eat them. Only
  # this worktree's own target is touched.
  echo "CLEAN_START at $(ts)"
  cargo clean
  echo "CLEAN_DONE rc=$? at $(ts)"
} > "$UNITLOG" 2>&1
