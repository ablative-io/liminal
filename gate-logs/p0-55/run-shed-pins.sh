#!/bin/zsh
# Repeats the shed-visibility pins N times, teeing the full output and the TRUE
# cargo exit code (not tee's). $1 = run count, $2 = log path.
set -u
runs="$1"
log="$2"
: > "$log"
for run in $(seq 1 "$runs"); do
  print "########## SHED PIN RUN $run ##########" | tee -a "$log"
  cargo test -p liminal-server --test subscription_shed_visibility_e2e -- --nocapture 2>&1 | tee -a "$log"
  cargo_exit=${pipestatus[1]}
  print "SHED PIN RUN $run TRUE EXIT=$cargo_exit" | tee -a "$log"
done
