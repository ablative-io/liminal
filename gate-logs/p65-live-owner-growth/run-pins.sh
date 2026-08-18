#!/bin/sh
# Runs the #65 growth pins and records the TRUE exit code, not the pipeline's.
cd /Users/tom/Developer/ablative/stack/liminal/.claude/worktrees/agent-aeccf347c111168f9 || exit 1
LOG="gate-logs/p65-live-owner-growth/$1"
shift
{
  cargo test "$@" 2>&1
  echo "TRUE_EXIT=$?"
} > "$LOG" 2>&1
tail -14 "$LOG"
