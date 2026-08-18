#!/bin/sh
# Full workspace battery. TRUE exit is captured INSIDE the brace group, so it
# is the test runner's code and not a pipeline's.
cd /Users/tom/Developer/ablative/stack/liminal/.claude/worktrees/agent-aeccf347c111168f9 || exit 1
{
  cargo test --workspace --no-fail-fast 2>&1
  echo "TRUE_EXIT=$?"
} > gate-logs/p65-live-owner-growth/battery.log 2>&1
grep -E "^TRUE_EXIT=" gate-logs/p65-live-owner-growth/battery.log
