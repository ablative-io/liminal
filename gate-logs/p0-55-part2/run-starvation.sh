#!/bin/zsh
# P0 #55 part 2 census runner: repeats the starvation pin binary N times, teeing
# every line and recording cargo's TRUE exit code separately from tee's.
#
# `--include-ignored`, NOT `--ignored`: the latter runs ONLY ignored tests, so
# once part 2 un-ignored these two pins it selected nothing and reported
# "0 passed; 2 filtered out" with exit 0 — a green from a runner that ran no test.
# `--include-ignored` runs the pins whether or not they carry the attribute, so
# the same script measures both sides of the fix.
#
#   $1 = number of runs
#   $2 = log path (relative to the worktree root)
#   $3 = optional value for LIMINAL_STARVATION_ITERS (boots per pin per run)
set -u
runs="$1"
log="$2"
: > "$log"
if [[ $# -ge 3 ]]; then
  export LIMINAL_STARVATION_ITERS="$3"
  print "LIMINAL_STARVATION_ITERS=$3" | tee -a "$log"
fi
for run in $(seq 1 "$runs"); do
  print "########## STARVATION RUN $run ##########" | tee -a "$log"
  cargo test -p liminal-server --test subscription_starvation_e2e -- --include-ignored --nocapture 2>&1 | tee -a "$log"
  print "STARVATION RUN $run TRUE EXIT=${pipestatus[1]}" | tee -a "$log"
done
