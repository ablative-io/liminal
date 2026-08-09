#!/bin/zsh
# Sums a cargo battery's `test result:` lines. Counts come ONLY from those lines
# -- never from a per-test tally, never from memory. Prints the number of result
# lines it summed, because a total is meaningless without its denominator: a
# battery that lost a binary reports a smaller total AND fewer result lines, and
# only the second number gives it away.
#   $1 = log path
set -u
log="$1"
print -- "log: $log"
grep -c '^test result:' "$log" | read lines
print -- "result lines: $lines"
awk '/^test result:/ {
       for (i = 1; i <= NF; i++) {
         if ($(i+1) ~ /^passed/)  { p += $i }
         if ($(i+1) ~ /^failed/)  { f += $i }
         if ($(i+1) ~ /^ignored/) { g += $i }
       }
     }
     END { printf "passed=%d failed=%d ignored=%d\n", p, f, g }' "$log"
print -- "--- cargo TRUE exit ---"
grep 'GATE TRUE EXIT' "$log"
print -- "--- failing binaries ---"
grep '^test result: FAILED' "$log"
