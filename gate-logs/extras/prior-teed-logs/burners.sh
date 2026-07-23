#!/bin/zsh
# 8 CPU burners
for i in $(seq 1 8); do ( while :; do :; done ) & echo $! >> /tmp/teardown_burners.pids; done
