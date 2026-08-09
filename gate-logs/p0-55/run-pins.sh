#!/bin/zsh
# Repeats the P0 #55 pins N times, teeing the full output. $1 = run count,
# $2 = log path. Reports the TRUE exit code of cargo (not tee's).
set -u
runs="$1"
log="$2"
: > "$log"
for run in $(seq 1 "$runs"); do
  print "########## PIN RUN $run ##########" | tee -a "$log"
  cargo test -p liminal-server --test subscription_starvation_e2e -- --nocapture 2>&1 | tee -a "$log"
  cargo_exit=${pipestatus[1]}
  print "PIN RUN $run TRUE EXIT=$cargo_exit" | tee -a "$log"
done
