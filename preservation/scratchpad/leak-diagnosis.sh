#!/bin/bash
# LEAK DIAGNOSIS — run-1 "1 leaky" on registration_helper_constructs_consumer_state.
# Phase A: instrument positive control (deliberate leaker must read LEAK, clean must read PASS).
# Phase B: solo loop of the named test. Phase C: module-context loop. Phase D: full-lib runs.
# Machine: Annabel's box | Operator: Mercury Toast (5b70322e-e7a9-451c-91ca-a3dfa7b05bda)
set -u
D=/private/tmp/claude-501/-Users-annabel-Developer-ablative-stack-liminal/5b70322e-e7a9-451c-91ca-a3dfa7b05bd9/scratchpad/leak-diag
WT=/Users/annabel/Developer/ablative/stack/liminal/.worktrees/release-baseline
CLAIM=/tmp/ablative-gate-battery.claim
MEMBER=5b70322e-e7a9-451c-91ca-a3dfa7b05bda
rm -rf "$D"; mkdir -p "$D/evidence"
EV="$D/evidence"; L="$EV/ledger.txt"
note() { echo "$(date -u +%H:%M:%SZ) $*" | tee -a "$L"; }
unset RUST_LOG AMP_ITERS AMP_PEERS AMP_BURNERS CONFORMANCE_RESULTS_DIR CARGO_TARGET_DIR
for v in $(env | sed -n 's/^\(LIMINAL_[^=]*\)=.*/\1/p'); do unset "$v"; done

# claim (rule 2): everything below compiles or loops compiled binaries
n=0
while :; do
  if ( set -o noclobber; printf 'seat=Mercury Toast\nmember_id=%s\npid=%s\nstarted_at=%s\nphase=running\n' "$MEMBER" "$$" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$CLAIM" ) 2>/dev/null; then
    trap 'if [ "$(sed -n "s/^pid[[:space:]]*[=:][[:space:]]*//p" "$CLAIM" 2>/dev/null)" = "$$" ]; then rm -f "$CLAIM"; note "claim released (own claim, pid $$)"; elif [ -f "$CLAIM" ]; then note "RELEASE: claim NOT ours — left; VOIDING signal"; else note "RELEASE: NO CLAIM on file — VOIDING signal"; fi' EXIT INT TERM HUP
    note "claim acquired pid=$$"; break
  fi
  hp="$(sed -n 's/^pid[[:space:]]*[=:][[:space:]]*//p' "$CLAIM" 2>/dev/null)"
  case "$hp" in (''|*[!0-9]*) note "holder unparseable -> HELD" ;; (*)
    if ps -p "$hp" >/dev/null 2>&1; then note "live holder $hp; yielding"; else
      note "stale claim (pid $hp dead) — recording+clearing"; cat "$CLAIM" >> "$EV/stale-claim.txt"; rm -f "$CLAIM"; continue; fi ;; esac
  n=$((n+1)); [ $n -ge 80 ] && { note "claim ceiling — ABORT"; exit 4; }
  sleep 15
done

# ---- Phase A: instrument control ----
mkdir -p "$D/instrument/src" "$D/instrument/tests"
printf '[package]\nname="leakprobe"\nversion="0.0.1"\nedition="2021"\n' > "$D/instrument/Cargo.toml"
echo '' > "$D/instrument/src/lib.rs"
cat > "$D/instrument/tests/probe.rs" <<'EOF'
#[test]
fn deliberate_leaker() {
    // child inherits our stderr and outlives the test process
    std::process::Command::new("sleep").arg("5").spawn().expect("spawn");
}
#[test]
fn clean_control() { assert_eq!(1 + 1, 2); }
EOF
(cd "$D/instrument" && NEXTEST_EXPERIMENTAL_LIBTEST_JSON=1 cargo nextest run --cargo-quiet --no-fail-fast --message-format libtest-json-plus > "$EV/instrument.json" 2> "$EV/instrument.stderr")
LEAKLINE=$(grep -E "LEAK" "$EV/instrument.stderr" | head -2)
PASSLINE=$(grep -E "PASS.*clean_control" "$EV/instrument.stderr" | head -1)
if echo "$LEAKLINE" | grep -q "deliberate_leaker" && [ -n "$PASSLINE" ]; then
  note "INSTRUMENT CONTROL PASS: deliberate leaker marked LEAK, clean marked PASS. Tool line: $(echo "$LEAKLINE" | head -1 | sed 's/^ *//')"
else
  note "INSTRUMENT CONTROL FAILED: leaker/clean not discriminated — STOP CLASS. stderr tail: $(tail -3 "$EV/instrument.stderr" | tr '\n' ' ')"
fi

cd "$WT"
TEST_FILTER='test(=liminal$routing::dispatch::tests::registration_helper_constructs_consumer_state)'

# ---- Phase B: solo loop x200 ----
B_ITERS=0; B_LEAKS=0; B_TESTS=0; B_RECON_FAIL=0
for i in $(seq 1 200); do
  OUT=$(NEXTEST_EXPERIMENTAL_LIBTEST_JSON=1 cargo nextest run -p liminal-rs --lib --cargo-quiet -E "$TEST_FILTER" --message-format libtest-json-plus 2>&1)
  B_ITERS=$((B_ITERS+1))
  S=$(echo "$OUT" | grep -oE "Summary \[[^]]*\] [0-9]+ tests? run: [0-9]+ passed[^,]*, [0-9]+ skipped" | head -1)
  RUNC=$(echo "$S" | grep -oE "[0-9]+ tests? run" | grep -oE "^[0-9]+")
  PASSC=$(echo "$S" | grep -oE "[0-9]+ passed" | grep -oE "^[0-9]+")
  [ "$RUNC" = "1" ] && [ "$PASSC" = "1" ] || { B_RECON_FAIL=$((B_RECON_FAIL+1)); echo "iter $i recon fail: $S" >> "$EV/phaseB-anomalies.txt"; }
  B_TESTS=$((B_TESTS+${RUNC:-0}))
  if echo "$OUT" | grep -q " LEAK "; then B_LEAKS=$((B_LEAKS+1)); echo "iter $i LEAK:" >> "$EV/phaseB-leaks.txt"; echo "$OUT" | grep " LEAK " >> "$EV/phaseB-leaks.txt"; fi
done
note "PHASE B (solo x$B_ITERS): leaks=$B_LEAKS tests-executed=$B_TESTS recon-failures=$B_RECON_FAIL"

# ---- Phase C: module-context loop x30 ----
C_ITERS=0; C_LEAKS=0; C_TESTS=0; C_RECON_FAIL=0
for i in $(seq 1 30); do
  OUT=$(NEXTEST_EXPERIMENTAL_LIBTEST_JSON=1 cargo nextest run -p liminal-rs --lib --cargo-quiet -E 'test(routing::dispatch::tests)' --message-format libtest-json-plus 2>&1)
  C_ITERS=$((C_ITERS+1))
  S=$(echo "$OUT" | grep -oE "Summary \[[^]]*\] [0-9]+ tests? run: [0-9]+ passed[^,]*, [0-9]+ skipped" | head -1)
  RUNC=$(echo "$S" | grep -oE "[0-9]+ tests? run" | grep -oE "^[0-9]+")
  PASSC=$(echo "$S" | grep -oE "[0-9]+ passed" | grep -oE "^[0-9]+")
  [ -n "$RUNC" ] && [ "$RUNC" = "$PASSC" ] || { C_RECON_FAIL=$((C_RECON_FAIL+1)); echo "iter $i recon fail: $S" >> "$EV/phaseC-anomalies.txt"; }
  C_TESTS=$((C_TESTS+${RUNC:-0}))
  if echo "$OUT" | grep -q " LEAK "; then C_LEAKS=$((C_LEAKS+1)); echo "iter $i:" >> "$EV/phaseC-leaks.txt"; echo "$OUT" | grep " LEAK " >> "$EV/phaseC-leaks.txt"; fi
done
note "PHASE C (module x$C_ITERS): leak-iterations=$C_LEAKS tests-executed=$C_TESTS recon-failures=$C_RECON_FAIL"

# ---- Phase D: full liminal-rs lib x3 (battery-adjacent context) ----
D_ITERS=0; D_LEAKS=0; D_TESTS=0
for i in $(seq 1 3); do
  OUT=$(NEXTEST_EXPERIMENTAL_LIBTEST_JSON=1 cargo nextest run -p liminal-rs --lib --cargo-quiet --no-fail-fast --message-format libtest-json-plus 2>&1)
  D_ITERS=$((D_ITERS+1))
  S=$(echo "$OUT" | grep -oE "Summary \[[^]]*\] [0-9]+ tests? run: [0-9]+ passed[^,]*, [0-9]+ skipped" | head -1)
  RUNC=$(echo "$S" | grep -oE "[0-9]+ tests? run" | grep -oE "^[0-9]+")
  D_TESTS=$((D_TESTS+${RUNC:-0}))
  echo "iter $i: $S" >> "$EV/phaseD-summaries.txt"
  if echo "$OUT" | grep -q " LEAK "; then D_LEAKS=$((D_LEAKS+1)); echo "iter $i:" >> "$EV/phaseD-leaks.txt"; echo "$OUT" | grep " LEAK " >> "$EV/phaseD-leaks.txt"; fi
done
note "PHASE D (full lib x$D_ITERS): leak-iterations=$D_LEAKS tests-executed=$D_TESTS"
note "=== DIAGNOSIS RUNS COMPLETE ==="
