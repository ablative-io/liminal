#!/bin/sh
# Reconciles the battery totals BY NAME: passed/failed/ignored summed across
# every suite, the suite count, and the failing tests listed by name.
cd /Users/tom/Developer/ablative/stack/liminal/.claude/worktrees/agent-aeccf347c111168f9 || exit 1
LOG=gate-logs/p65-live-owner-growth/battery.log
awk '/^test result:/ {
  for (i = 1; i <= NF; i++) {
    if ($(i+1) ~ /^passed/)  p += $i
    if ($(i+1) ~ /^failed/)  f += $i
    if ($(i+1) ~ /^ignored/) g += $i
  }
  s++
}
END { printf "passed=%d failed=%d ignored=%d suites=%d\n", p, f, g, s }' "$LOG"
echo "--- failures by name ---"
awk '/^failures:$/ {inblock = 1; next} /^test result:/ {inblock = 0} inblock && /^    [a-z]/ {print}' "$LOG" | sort -u
echo "--- true exit ---"
grep -E "^TRUE_EXIT=" "$LOG"
