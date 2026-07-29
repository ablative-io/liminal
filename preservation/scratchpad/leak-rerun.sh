#!/bin/bash
set -u
EV=/private/tmp/claude-501/-Users-annabel-Developer-ablative-stack-liminal/5b70322e-e7a9-451c-91ca-a3dfa7b05bd9/scratchpad/leak-diag/evidence
WT=/Users/annabel/Developer/ablative/stack/liminal/.worktrees/release-baseline
CLAIM=/tmp/ablative-gate-battery.claim
L="$EV/ledger.txt"
note() { echo "$(date -u +%H:%M:%SZ) $*" | tee -a "$L"; }
unset RUST_LOG CARGO_TARGET_DIR
n=0
while :; do
  if ( set -o noclobber; printf 'seat=Mercury Toast\nmember_id=5b70322e-e7a9-451c-91ca-a3dfa7b05bda\npid=%s\nstarted_at=%s\nphase=running\n' "$$" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$CLAIM" ) 2>/dev/null; then
    trap 'if [ "$(sed -n "s/^pid[[:space:]]*[=:][[:space:]]*//p" "$CLAIM" 2>/dev/null)" = "$$" ]; then rm -f "$CLAIM"; note "claim released (own claim, pid $$)"; else note "RELEASE ANOMALY — VOIDING signal"; fi' EXIT INT TERM HUP
    note "rerun claim acquired pid=$$"; break
  fi
  hp="$(sed -n 's/^pid[[:space:]]*[=:][[:space:]]*//p' "$CLAIM" 2>/dev/null)"
  case "$hp" in (''|*[!0-9]*) : ;; (*) ps -p "$hp" >/dev/null 2>&1 || { cat "$CLAIM" >> "$EV/stale-claim.txt"; rm -f "$CLAIM"; continue; } ;; esac
  n=$((n+1)); [ $n -ge 80 ] && { note "claim ceiling"; exit 4; }
  sleep 15
done
cd "$WT"
F='test(=routing::dispatch::tests::registration_helper_constructs_consumer_state)'
BI=0; BL=0; BT=0; BR=0
for i in $(seq 1 300); do
  OUT=$(NEXTEST_EXPERIMENTAL_LIBTEST_JSON=1 cargo nextest run -p liminal-rs --lib --cargo-quiet -E "$F" --message-format libtest-json-plus 2>&1)
  BI=$((BI+1))
  S=$(echo "$OUT" | grep -oE "[0-9]+ tests? run: [0-9]+ passed" | head -1)
  RUNC=$(echo "$S" | grep -oE "^[0-9]+")
  [ "$RUNC" = "1" ] || { BR=$((BR+1)); echo "iter $i: $S" >> "$EV/phaseB2-anomalies.txt"; }
  BT=$((BT+${RUNC:-0}))
  echo "$OUT" | grep -q " LEAK " && { BL=$((BL+1)); echo "iter $i:" >> "$EV/phaseB2-leaks.txt"; echo "$OUT" | grep " LEAK " >> "$EV/phaseB2-leaks.txt"; }
done
note "PHASE B2 (solo x$BI, fixed filter): leaks=$BL tests-executed=$BT recon-failures=$BR"
DI=0; DL=0; DT=0
for i in $(seq 1 5); do
  OUT=$(NEXTEST_EXPERIMENTAL_LIBTEST_JSON=1 cargo nextest run -p liminal-rs --lib --cargo-quiet --no-fail-fast --message-format libtest-json-plus 2>&1)
  DI=$((DI+1))
  S=$(echo "$OUT" | grep -oE "[0-9]+ tests? run: [0-9]+ passed" | head -1)
  RUNC=$(echo "$S" | grep -oE "^[0-9]+")
  DT=$((DT+${RUNC:-0}))
  echo "iter $i: $S" >> "$EV/phaseD2-summaries.txt"
  echo "$OUT" | grep -q " LEAK " && { DL=$((DL+1)); echo "$OUT" | grep " LEAK " >> "$EV/phaseD2-leaks.txt"; }
done
note "PHASE D2 (full lib x$DI): leak-iterations=$DL tests-executed=$DT"
note "=== RERUN COMPLETE ==="
