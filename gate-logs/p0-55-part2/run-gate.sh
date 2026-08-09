#!/bin/zsh
# Full workspace battery, teed, with cargo's TRUE exit code recorded separately
# from tee's. $1 = log path.
set -u
log="$1"
: > "$log"
cargo test --workspace --no-fail-fast 2>&1 | tee -a "$log"
print "GATE TRUE EXIT=${pipestatus[1]}" | tee -a "$log"
