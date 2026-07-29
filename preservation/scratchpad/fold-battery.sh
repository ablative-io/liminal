#!/bin/zsh
# NEUTRALIZED 2026-07-29 ~18:45Z by Mercury Toast (…bda) — claim-blind compile launcher
# found in the estate-wide sweep (Cally directive via Hermes 18:39Z). Predates the claim
# convention; superseded by canon r3 + deltas. Original bytes preserved beside this file
# as fold-battery.sh.pre-neutralize.bytes. Refusal at launch, not a label (Artemis's law).
echo "RETIRED: this launcher is claim-blind (no /tmp/ablative-gate-battery.claim preflight). Use the canon runner. Original: fold-battery.sh.pre-neutralize.bytes" >&2
exit 86
# SDK-010 fold battery — merged tree, pinned toolchain, main checkout, default target dir
set -u
OUT="$1"
cd /Users/annabel/Developer/ablative/stack/liminal
: > "$OUT"
{
  echo "TOOLCHAIN: $(rustc --version) / $(cargo --version) / clippy: $(cargo clippy --version)"
  echo "TREE: $(git rev-parse --short HEAD) (merge of main 519a47f + feat/sdk010-reader-deadline 91244dd), clean: $(git status --short | grep -v '^?? .worktrees' | wc -l | tr -d ' ') dirty lines"
  echo "BOX: Annabel's Mac, main checkout, default target dir (CARGO_TARGET_DIR unset: ${CARGO_TARGET_DIR:-confirmed-unset})"
  echo "FRESHNESS: crate roots touched before clippy"
} >> "$OUT"
touch crates/*/src/lib.rs 2>/dev/null
run() {
  echo "=== GATE: cargo $* ===" >> "$OUT"
  cargo "$@" >> "$OUT" 2>&1
  local code=$?
  echo "EXIT: $code" >> "$OUT"
  echo "GATE cargo $* -> EXIT=$code"
}
run fmt --all -- --check
run check --workspace --all-targets
run clippy --workspace --all-targets -- -D warnings
run test --workspace
run check -p liminal-protocol --target wasm32-unknown-unknown --no-default-features
run check -p liminal-sdk --target wasm32-unknown-unknown --no-default-features
{
  echo "### totals:"
  grep '^test result:' "$OUT" | awk '{p+=$4; f+=$6} END {print p " passed / " f " failed / " NR " suites"}'
} >> "$OUT"
echo "BATTERY COMPLETE"
