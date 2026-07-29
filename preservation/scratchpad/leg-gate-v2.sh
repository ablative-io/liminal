#!/bin/bash
# Gate battery runner — CLAIM CONVENTION v2 + amended gate script (A4/A5,
# completion marker, pipestatus per leg).
# PINNED REVISION SET (build target, named per re-brief 17:03Z — do not track
# the live lane; drift arrives as a re-brief from the lane owner):
#   4b8b38e1 (anchor, sha256 2785feab…3110 verified at this seat)
#   e903b4ad (Amendment 1) · c6d998bc (Amendment 2) · aa92a18c (Addendum)
#   91ba17f9 (Vesper's withdrawal) · c3ee8385 (ratification)
#
# v2 shape: claim FIRST at the fixed path, THEN drain-wait UNDER the held claim
# (30s samples, 60-minute ceiling, every sample recorded). Refusal ONLY on
# timeout, loud and recorded. Refuse-on-sight is retired. The quiet CENSUS is
# the load-bearing control cited in evidence; the claim orders participants.
#
# Usage: WORKTREE=<abs path> LEG_LABEL=<label> bash leg-gate-v2.sh
# Machine: Annabel's box (Annabels-MacBook-Pro) | Operator: Mercury Toast (5b70322e-e7a9-451c-91ca-a3dfa7b05bda)
set -u
CLAIM=/tmp/ablative-gate-battery.claim
WORKTREE="${WORKTREE:?set WORKTREE}"
LEG_LABEL="${LEG_LABEL:?set LEG_LABEL}"
SAMPLE_SECS=30
CEILING_SECS=3600

echo "=== RUNNER v2 START $(date -u +%Y-%m-%dT%H:%M:%SZ) | leg=$LEG_LABEL ==="
echo "machine: Annabel's box (Annabels-MacBook-Pro) | operator: Mercury Toast (…bda)"

# --- Phase 1: acquire the claim (yield to a live holder; record+clear a stale one) ---
DEADLINE=$(( $(date +%s) + CEILING_SECS ))
while :; do
  STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  # Amendment 1 item 1: claim body carries `phase` — "draining" until the quiet
  # census passes, flipped to "running" when the battery starts, so other seats
  # can read whether the box-freeze is a wait or a run (and stale-claim
  # forensics learn what the holder was doing when it died).
  if ( set -o noclobber; printf 'seat=Mercury Toast\nmember_id=5b70322e-e7a9-451c-91ca-a3dfa7b05bda\npid=%s\nstarted_at=%s\nleg=%s\nphase=draining\n' "$$" "$STARTED_AT" "$LEG_LABEL" > "$CLAIM" ) 2>/dev/null; then
    trap 'if rm "$CLAIM" 2>/dev/null; then echo "=== CLAIM RELEASED $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="; else echo "=== CLAIM RELEASE FAILED OR ALREADY GONE — LOUD: verify /tmp/ablative-gate-battery.claim manually, record on lane ==="; fi' EXIT INT TERM HUP
    echo "=== CLAIM TAKEN $(date -u +%Y-%m-%dT%H:%M:%SZ) pid=$$ phase=draining ==="
    break
  fi
  HOLDER_PID=$(sed -n 's/^pid=//p' "$CLAIM" 2>/dev/null)
  # Liveness probe via ps, not `kill -0`: kill -0 answers EPERM for a live
  # process owned by another user, which would read a LIVE claim as stale —
  # the worst failure this convention has (Seth's flag 2, c527324b).
  if [ -n "$HOLDER_PID" ] && ! ps -p "$HOLDER_PID" > /dev/null 2>&1; then
    echo "=== STALE CLAIM (holder pid $HOLDER_PID dead) — contents below MUST BE POSTED VERBATIM TO THE LANE (d3ee85ac) BY THE OPERATOR before evidence cites this run; a run-log copy alone is NOT a lane record (rule 5 floor, Hermes's correction 17:00Z) ==="
    cat "$CLAIM"
    # Rule-5 floor: failure to clear must be LOUD, never a silent fall-through.
    rm "$CLAIM" || { echo "=== FAILURE TO CLEAR STALE CLAIM — LOUD ABORT (manual clear + lane record required) ==="; exit 6; }
    continue
  fi
  if [ "$(date +%s)" -ge "$DEADLINE" ]; then
    echo "=== REFUSED ON TIMEOUT (acquisition): live holder after ${CEILING_SECS}s — loud recorded outcome ==="
    cat "$CLAIM" 2>/dev/null
    exit 4
  fi
  echo "--- yield-sample $(date -u +%H:%M:%SZ): live claim held by pid ${HOLDER_PID:-unknown}, holding ${SAMPLE_SECS}s ---"
  sleep "$SAMPLE_SECS"
done

# --- Phase 2: drain-wait under the held claim — census is the load-bearing control ---
while :; do
  # Census = the load-bearing control (rule 6). Exact-name match over the six
  # compile binaries (Phoebus/Apollo set): a substring regex misses clippy-driver
  # and rustdoc, which carry neither "cargo" nor "rustc" in their names.
  FOREIGN=$(pgrep -lx "cargo|rustc|cargo-nextest|cargo-clippy|clippy-driver|rustdoc" 2>/dev/null)
  if [ -z "$FOREIGN" ]; then
    echo "=== CENSUS QUIET $(date -u +%Y-%m-%dT%H:%M:%SZ): zero foreign cargo/rustc — proceeding ==="
    # Drain complete: flip the held claim's phase to "running" (Amendment 1).
    printf 'seat=Mercury Toast\nmember_id=5b70322e-e7a9-451c-91ca-a3dfa7b05bda\npid=%s\nstarted_at=%s\nleg=%s\nphase=running\n' "$$" "$STARTED_AT" "$LEG_LABEL" > "$CLAIM"
    echo "=== CLAIM PHASE → running $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
    break
  fi
  if [ "$(date +%s)" -ge "$DEADLINE" ]; then
    echo "=== REFUSED ON TIMEOUT (drain): foreign compiles persisted past ceiling — loud recorded outcome ==="
    echo "$FOREIGN"
    exit 5
  fi
  echo "--- drain-sample $(date -u +%H:%M:%SZ) (recorded): foreign compile activity present ---"
  echo "$FOREIGN" | head -5
  sleep "$SAMPLE_SECS"
done

# --- Phase 3: amended gate ---
cd "$WORKTREE" || exit 2
echo "=== GATE RUN start $(date -u +%Y-%m-%dT%H:%M:%SZ) | HEAD=$(git rev-parse HEAD) | branch=$(git branch --show-current) ==="
echo "=== toolchain: $(rustc --version) | $(cargo --version) ==="
echo "=== load START: $(uptime) ==="

echo "=== LEG 1: cargo check ==="
cargo check \
  --workspace \
  --all-targets \
  --message-format=json \
  --keep-going |
jq -c '
  select(.reason == "compiler-message")
  | select(.message.level == "error" or .message.level == "warning")
  | {
      type: "clippy",
      level: .message.level,
      file: (
        [.message.spans[]? | select(.is_primary)][0].file_name
        // .message.spans[0]?.file_name
        // "unknown"
      ),
      line: (
        [.message.spans[]? | select(.is_primary)][0].line_start
        // .message.spans[0]?.line_start
        // null
      ),
      column: (
        [.message.spans[]? | select(.is_primary)][0].column_start
        // .message.spans[0]?.column_start
        // null
      ),
      lint: (.message.code.code // "compile-error"),
      message: .message.message
    }
'
echo "=== LEG 1 pipestatus: ${PIPESTATUS[*]} | load: $(uptime) ==="

echo "=== LEG 2: cargo clippy ==="
cargo clippy \
  --workspace \
  --all-targets \
  --message-format=json \
  --keep-going \
  -- \
  -D warnings |
jq -c '
  select(.reason == "compiler-message")
  | select(.message.level == "error" or .message.level == "warning")
  | {
      type: "clippy",
      level: .message.level,
      file: (
        [.message.spans[]? | select(.is_primary)][0].file_name
        // .message.spans[0]?.file_name
        // "unknown"
      ),
      line: (
        [.message.spans[]? | select(.is_primary)][0].line_start
        // .message.spans[0]?.line_start
        // null
      ),
      column: (
        [.message.spans[]? | select(.is_primary)][0].column_start
        // .message.spans[0]?.column_start
        // null
      ),
      lint: (.message.code.code // "compile-error"),
      message: .message.message
    }
'
echo "=== LEG 2 pipestatus: ${PIPESTATUS[*]} | load: $(uptime) ==="

echo "=== LEG 3: cargo nextest (A4: no --all-targets) ==="
NEXTEST_EXPERIMENTAL_LIBTEST_JSON=1 \
  cargo nextest run \
    --workspace \
    --cargo-quiet \
    --message-format libtest-json-plus \
    --no-fail-fast |
jq -c '
  select(.type == "test" and .event == "failed")
  | {
      type: "nextest",
      test: .name,
      stdout: (.stdout // ""),
      message: "test failed"
    }
'
echo "=== LEG 3 pipestatus: ${PIPESTATUS[*]} | load: $(uptime) ==="
echo "=== load END: $(uptime) ==="
echo "=== GATE RUN end $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
