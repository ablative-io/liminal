#!/bin/zsh
set -u
TC="$1"
OUT="$2"
cd /Users/annabel/Developer/ablative/stack/liminal/.worktrees/sdk-010
: > "$OUT"
run() {
  echo "=================================================================" >> "$OUT"
  echo "\$ cargo $TC $*" >> "$OUT"
  cargo $TC "$@" >> "$OUT" 2>&1
  local code=$?
  echo "EXIT=$code" >> "$OUT"
  echo "GATE cargo $TC $* -> EXIT=$code"
}
run fmt --all -- --check
run check --workspace --all-targets
run clippy --workspace --all-targets -- -D warnings
run test --workspace
run check -p liminal-protocol --target wasm32-unknown-unknown --no-default-features
run check -p liminal-sdk --target wasm32-unknown-unknown --no-default-features
