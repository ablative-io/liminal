#!/bin/bash
# Leg A gate battery — claim-file protocol (Athena hardened form) + amended gate
#   A4: nextest leg drops --all-targets (clippy leg KEEPS it)
#   A5: no 2>/dev/null anywhere — raw stderr flows to the teed log
#   Completion marker: nextest Summary line MUST appear or the run is RED.
# Machine: Annabel's box (Annabels-MacBook-Pro) | Operator: Mercury Toast (5b70322e-e7a9-451c-91ca-a3dfa7b05bda)
set -u
CLAIM=/tmp/ablative-gate-battery.claim
WORKTREE=/Users/annabel/Developer/ablative/stack/liminal/.worktrees/leg-boot

echo "=== QUIET-BOX PREFLIGHT $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
echo "machine: Annabel's box (Annabels-MacBook-Pro) | operator: Mercury Toast (…bda)"
uptime
pgrep -fl "cargo" | grep -v "pgrep" || echo "preflight: no cargo activity on box"

# Atomic claim: refuse on live holder — the claim file is the arbiter.
if ( set -o noclobber; printf 'seat=Mercury Toast\nmember_id=5b70322e-e7a9-451c-91ca-a3dfa7b05bda\npid=%s\nstarted_at=%s\nleg=B(fix/boot-from-shipped)\n' "$$" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$CLAIM" ) 2>/dev/null; then
  trap 'rm -f "$CLAIM"; echo "=== CLAIM RELEASED $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="' EXIT INT TERM
  echo "=== CLAIM TAKEN $(date -u +%Y-%m-%dT%H:%M:%SZ) pid=$$ ==="
else
  echo "=== REFUSED: live claim holder ==="
  cat "$CLAIM"
  exit 3
fi

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
