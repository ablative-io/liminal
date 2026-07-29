#!/bin/bash
# 0.5.1 release-battery PRE-CLAIM HEADER — all four assertions BEFORE the claim is taken.
# Usage: WORKTREE=<abs path> RUN_LABEL=<main-baseline|release-tip> bash release-battery-header.sh
# Machine: Annabel's box (Annabels-MacBook-Pro) | Operator: Mercury Toast (5b70322e-e7a9-451c-91ca-a3dfa7b05bda)
set -u
WORKTREE="${WORKTREE:?set WORKTREE}"
RUN_LABEL="${RUN_LABEL:?set RUN_LABEL}"
RUNNER=/Users/annabel/Developer/ablative/stack/liminal/.worktrees/leg-sdk/gate-evidence/canon-r3-local-d4.as-run.sh
EXPECT_HASH=82ea8eac3f9e0bf576d784bc20fa1c410fd48a94ebfc765aeb6d816abcbc49f9
# Member id derived from the SERVER's author_id stamp on my own lane post cb5caed7
# (member-kind vs member-kind; session id ends bd9 and must never appear here).
MEMBER_EXPECT=5b70322e-e7a9-451c-91ca-a3dfa7b05bda
fail() { echo "HEADER ASSERTION FAILED: $1 — NO LAUNCH; report to dispatcher"; exit 90; }

echo "=== RELEASE BATTERY HEADER $(date -u +%Y-%m-%dT%H:%M:%SZ) | run=$RUN_LABEL ==="
echo "machine: Annabel's box (Annabels-MacBook-Pro) | operator: Mercury Toast ($MEMBER_EXPECT)"

echo "--- assertion 1: Seth's two-directional tool preflight ---"
for t in pgrep ps sed; do command -v "$t" >/dev/null || fail "$t does not resolve"; echo "resolved: $t -> $(command -v $t)"; done
# Probe within the census's own population: same-user processes. First attempt used
# root-owned launchd and REFUSED (correct loudness, wrong probe): pgrep here is blind
# to root-owned processes (pgrep -x launchd = silence while ps -p 1 shows launchd)
# but discriminates same-user processes, and every compile on this box runs as this
# user — the exact population the census samples. Disclosed, not assumed.
sleep 397 & PROBE=$!
LIVEPID=$(pgrep -x sleep | grep -x "$PROBE" || true)
kill "$PROBE" 2>/dev/null
[ -n "$LIVEPID" ] || fail "pgrep -x sleep did not return our own live child $PROBE (census would read QUIET on a broken pgrep)"
echo "pgrep discriminates (same-user population): pgrep -x sleep -> own child pid $LIVEPID (real pid, not silence)"
echo "disclosed limit: pgrep blind to ROOT-owned processes on this box (launchd probe silent; ps -p 1 sees it); census population = same-user processes, which is where compiles live"
ps -p $$ >/dev/null 2>&1 || fail "ps -p on own live shell reads DEAD (pid_alive would clear a live claim)"
echo "ps discriminates ALIVE: ps -p $$ (own shell) -> ALIVE"
if ps -p 999999 >/dev/null 2>&1; then fail "ps -p 999999 reads ALIVE (ps not discriminating)"; fi
echo "ps discriminates DEAD: ps -p 999999 -> DEAD"

echo "--- assertion 2: denominator identity (two-run form; per-run record) ---"
cd "$WORKTREE" || fail "worktree missing"
echo "tree under battery: $(git rev-parse HEAD) ($(git log --oneline -1 | head -c 80))"
echo "range c921827..62d9b80: $(git -C /Users/annabel/Developer/ablative/stack/liminal diff --name-only c921827..62d9b80 | wc -l | tr -d ' ') paths, $(git -C /Users/annabel/Developer/ablative/stack/liminal diff --name-only c921827..62d9b80 | grep -c '\.rs$' || true) .rs files"
echo "identity: run-1 (c921827) count MUST equal run-2 (62d9b80) count exactly; drift either way = RED, both numbers reported, no diagnosis"

echo "--- assertion 3 (pre-registered): four-way reconciliation asserted at THIS run post-Summary ---"
echo "started == ok+failed+ignored; suite totals agree; nextest totals agree; JSON ignored == Summary skipped"

echo "--- assertion 4 (pre-registered): exits from COMPLETE.marker = PIPESTATUS[0] x3, not Summary alone ---"

echo "--- runner pin: hash + mode ---"
ACTUAL=$(shasum -a 256 "$RUNNER" | awk '{print $1}')
echo "runner: $RUNNER"
echo "sha256: $ACTUAL"
[ "$ACTUAL" = "$EXPECT_HASH" ] || fail "runner hash mismatch (expected $EXPECT_HASH) — STOP, send value to dispatcher, no reasoning about which copy is right"
echo "mode: committed 100644 (non-executable) -> invoked as: bash <path>"

echo "--- MEMBER_ID kind check (load-bearing: r3 line 20 takes env unvalidated) ---"
[ "${MEMBER_ID:-}" = "$MEMBER_EXPECT" ] || fail "MEMBER_ID env is '${MEMBER_ID:-unset}', not the server-derived member id (session-id contamination would stamp the dispatcher's seat)"
echo "MEMBER_ID == server-derived member id (…bda): OK"

echo "--- launch-env absences ---"
for v in AMP_ITERS AMP_PEERS AMP_BURNERS CONFORMANCE_RESULTS_DIR RUST_LOG DATABASE_URL; do [ -z "${!v:-}" ] || fail "$v is set"; done
LIM=$(env | grep -c '^LIMINAL_' || true); [ "$LIM" = "0" ] || fail "LIMINAL_* present"
echo "confirmed absent: AMP_ITERS/AMP_PEERS/AMP_BURNERS, CONFORMANCE_RESULTS_DIR, RUST_LOG, LIMINAL_*, DATABASE_URL; SET: nothing"

echo "=== HEADER COMPLETE $(date -u +%Y-%m-%dT%H:%M:%SZ): all pre-claim assertions PASSED — claim may be taken ==="
