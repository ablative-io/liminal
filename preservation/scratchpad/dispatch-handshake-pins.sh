#!/bin/zsh
# NEUTRALIZED 2026-07-29 ~18:45Z by Mercury Toast (…bda) — claim-blind compile launcher
# found in the estate-wide sweep (Cally directive via Hermes 18:39Z). Predates the claim
# convention; superseded by canon r3 + deltas. Original bytes preserved beside this file
# as dispatch-handshake-pins.sh.pre-neutralize.bytes. Refusal at launch, not a label (Artemis's law).
echo "RETIRED: this launcher is claim-blind (no /tmp/ablative-gate-battery.claim preflight). Use the canon runner. Original: dispatch-handshake-pins.sh.pre-neutralize.bytes" >&2
exit 86
set -euo pipefail
umask 077

MODE="dev"
PRESET="test-hardening"
EFFORT="high"

WORKSPACE="$HOME/Developer/ablative/stack/liminal/.worktrees/handshake-protocol"
SKILL_DIR="$HOME/Developer/ablative/stack/liminal/.claude/skills/norn"
NORN_STATE_HOME="${NORN_HOME:-$HOME/.norn}"
ALLOWED_TOOLS="read,search,lsp,bash,write,edit,apply_patch,action_log"

SCHEMA_PATH="$SKILL_DIR/schemas/dev.schema.json"
BASE_INSTRUCTIONS="$SKILL_DIR/instructions/dev/base.md"
PRESET_INSTRUCTIONS="$SKILL_DIR/instructions/dev/$PRESET.md"
APPENDED_SYSTEM_PROMPT="$(cat "$BASE_INSTRUCTIONS" "$PRESET_INSTRUCTIONS")"
ENVELOPE_DIR="$NORN_STATE_HOME/delegations"
mkdir -p "$ENVELOPE_DIR"
RESULT_FILE="$(mktemp "$ENVELOPE_DIR/claude-dev-handshake-pins.XXXXXX")"
SESSION_NAME="claude-dev-handshake-pins-$(date -u +%Y%m%dT%H%M%SZ)-$$"

PROMPT=$(cat <<'TASKEOF'
Delegation mode: dev
Specialist preset: test-hardening
Repository: worktree .worktrees/handshake-protocol on branch feat/handshake-protocol @ 8eda999 (off liminal main 6d09bae). Small bounded lane: TWO named codec pins + ONE decode-site comment. FIRST ACTIONS: verify `git rev-parse --short HEAD` = 8eda999 and clean status; state both.

CONTEXT: commit 8eda999 added a trailing activity census to the WorkerRegister frame (u32 count + per-descriptor name/input_schema_json/output_schema_json), taken from a rescue snapshot. Compatibility rides on trailing-bytes sniffing: crates/liminal/src/protocol/codec/known.rs::decode_worker_register_payload reads through `identity` then checks `reader.is_finished()` — finished ⇒ empty census (pre-contract worker); else reads count + descriptors. The encoder (codec.rs::write_worker_register_payload) writes the count UNCONDITIONALLY. THE SEAT HAS RULED: the sniff stays (no ProtocolVersion gating) — your job is the pins and the ceiling comment, NOT re-deciding this.

WORK, exactly three items in crates/liminal/src/protocol/:
1. PIN A (forward tolerance), named `worker_register_old_shape_decodes_to_empty_census` in codec/tests.rs: construct a literal OLD-LAYOUT WorkerRegister payload — the byte sequence a pre-census encoder produced, ending exactly after `identity` with NO census count — and decode it through the real decode path. Assert it decodes successfully to a WorkerRegister whose `activities` is EMPTY and whose other fields are exact. Build the old bytes deterministically: either hand-assemble with the same length-prefix helpers the codec uses, or encode a new frame with empty activities and TRUNCATE the trailing 4-byte zero count — whichever you choose, assert your old-shape construction is byte-exact at the layer you built it (no guessed offsets). Also assert the discriminator's other edge: an empty-census NEW frame (with its 4 zero bytes) still decodes to empty activities — the two byte shapes converge on the same value.
2. PIN B (round-trip), named `worker_register_nonempty_census_round_trips_exactly` in codec/tests.rs: a WorkerRegister with ≥2 descriptors (distinct names, non-trivial JSON schema strings incl. one empty-string schema) round-trips encode→decode with EXACT equality on every descriptor field, and encoded_len matches the encoder's actual output length.
3. COMMENT at the sniff site in known.rs (immediately above the `is_finished` branch): state that this sniff CONSUMES WorkerRegister's one trailing-bytes extension slot — any future field appended to this frame MUST ride a ProtocolVersion gate, never a second sniff, because a second optional trailing field would be indistinguishable from census bytes. Two or three lines, matching the file's comment voice.

GATES at your final commit, genuine exit codes, CARGO_TARGET_DIR=/Users/annabel/Developer/ablative/stack/liminal/target: cargo fmt --all -- --check; cargo check --workspace --all-targets; cargo clippy --workspace --all-targets -- -D warnings; cargo test --workspace; cargo check -p liminal-protocol --target wasm32-unknown-unknown --no-default-features; cargo check -p liminal-sdk --target wasm32-unknown-unknown --no-default-features. Known disclose-not-fix: SDK wasm dead-code warning (WirePublishRequest.idempotency_key). If an unrelated test flakes under parallel execution, rerun standalone + full workspace to confirm green and disclose; never modify it.

LAWS: YG-560 (no merge/rebase/cherry-pick/pull); no publish, no tag; no production code changes beyond the comment (a pin exposing a production defect = STOP with exact bytes, do not fix); no lint suppression; no sleep-based proofs; NO DEFERRALS; forward-only; commit and push when green (one commit is fine).

REPORT (structured): how you built the old-shape bytes and why they're exact; per-pin assertions; final HEAD + pushed + clean; full gate results with exits + per-suite counts; any STOP.
TASKEOF
)

norn --print \
  --model gpt-5.6-sol \
  --reasoning-effort "$EFFORT" \
  --fast \
  --working-dir "$WORKSPACE" \
  --workspace-root "$WORKSPACE" \
  --allowed-tools "$ALLOWED_TOOLS" \
  --append-system-prompt "$APPENDED_SYSTEM_PROMPT" \
  --session-name "$SESSION_NAME" \
  --quiet \
  --output-schema "$SCHEMA_PATH" \
  --output-format json \
  >"$RESULT_FILE" <<<"$PROMPT"

SESSION_ID="$(jq -er '.session_id' "$RESULT_FILE")"
printf 'Norn session: %s\n' "$SESSION_ID" >&2
printf 'Norn envelope: %s\n' "$RESULT_FILE" >&2
jq -e 'if .stop.reason == "completed" then .output else error("handshake pins leg did not complete; inspect envelope") end' "$RESULT_FILE"
