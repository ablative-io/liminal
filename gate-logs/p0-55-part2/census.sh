#!/bin/zsh
# Boot census over a starvation-pin log: for each pin arm, how many FRESH BOOTS
# lost a subscriber. Counts come only from the per-iteration outcome lines the
# pins print, never from a summary line.
#   $1 = log path
set -u
log="$1"
for tag in BURST MIXED-FATE; do
  total=$(grep -c "^$tag PIN iteration" "$log")
  lost=$(grep -c "^$tag PIN iteration.*ok=false" "$log")
  print "$tag: lost=$lost boots=$total"
done
print -- "---cargo TRUE exits ---"
grep 'TRUE EXIT' "$log"
print -- "---summed test result lines ---"
grep '^test result:' "$log"
