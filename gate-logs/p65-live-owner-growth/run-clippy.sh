#!/bin/sh
# Clippy over all targets, TRUE exit captured inside the brace group.
cd /Users/tom/Developer/ablative/stack/liminal/.claude/worktrees/agent-aeccf347c111168f9 || exit 1
{
  cargo clippy --all-targets 2>&1
  echo "TRUE_EXIT=$?"
} > gate-logs/p65-live-owner-growth/clippy.log 2>&1
tail -20 gate-logs/p65-live-owner-growth/clippy.log
