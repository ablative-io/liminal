#!/usr/bin/env python3
"""Counts READY wake fires per connection pid, per arm-I iteration.

The starvation ratchet's signature: the connection that keeps up drains its
inbox to empty on every slice, so every arrival is a fresh empty-to-non-empty
edge and it is EXPLICITLY woken for essentially every envelope. The connection
that falls behind never empties its inbox again, so it never earns another edge
and gets exactly one wake for the whole burst.
"""
import re
import sys
import collections

lines = open(sys.argv[1], errors="replace").read().split("\n")
marks = [i for i, l in enumerate(lines) if re.search(r"ARM I iteration (\d+):", l)]
start = 0
for end in marks:
    label = re.search(r"ARM I iteration (\d+): ok=(\w+)", lines[end])
    part = lines[start:end]
    fires = collections.Counter(
        re.search(r"PROBE\[fire\] t=\d+ pid=(\d+)", l).group(1)
        for l in part
        if "PROBE[fire]" in l
    )
    slices = collections.Counter(
        re.search(r"PROBE\[(ws|tcp)-slice-begin\] t=\d+ pid=(\d+)", l).group(2)
        for l in part
        if "-slice-begin]" in l
    )
    overflow = sum(1 for l in part if "PROBE[overflow]" in l)
    print(
        f"iteration {label.group(1):>2} ok={label.group(2):<5} "
        f"fires={dict(sorted(fires.items()))} slices={dict(sorted(slices.items()))} "
        f"overflow_refusals={overflow}"
    )
    start = end + 1
