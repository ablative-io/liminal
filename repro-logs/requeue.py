#!/usr/bin/env python3
"""Measures how long a connection waits to run again, split by HOW the previous
slice ended.

Two re-entry routes exist:
  * `park` -> the process waits for an explicit wake (a READY atom from the
    inbox notifier, socket readiness, or a timer).
  * `continue_*` -> the slice self-requeued: the final probe saw work still
    pending, so the process is runnable RIGHT NOW and needs no wake at all.

The follow-up question is whether the websocket connection is starved of
SCHEDULING or of WAKES, and this is the measurement that separates them: if
self-requeue latency is comparable to wake latency, the connection is being
scheduled fairly and the wake edge is the problem; if self-requeue latency is
far worse, the re-queue itself is the bottleneck.
"""
import re
import statistics
import sys
from collections import defaultdict

begin = re.compile(r"PROBE\[(ws|tcp)-slice-begin\] t=(\d+) pid=(\d+)")
end = re.compile(r"PROBE\[(ws|tcp)-slice-end\] t=(\d+) pid=(\d+) outcome=(\w+)")

gaps = defaultdict(list)
widths = defaultdict(list)
pending_begin = {}
last_end = {}

for line in open(sys.argv[1], errors="replace"):
    hit = begin.search(line)
    if hit:
        key = (hit.group(1), hit.group(3))
        now = int(hit.group(2))
        pending_begin[key] = now
        if key in last_end:
            prior_t, prior_outcome = last_end.pop(key)
            delta = now - prior_t
            # Fresh-server iterations restart the world; a multi-second gap is
            # an iteration boundary, not a scheduling delay.
            if 0 <= delta < 2_000_000:
                route = "park->wake" if prior_outcome == "park" else "self-requeue"
                gaps[(key[0], route)].append(delta)
        continue
    hit = end.search(line)
    if hit:
        key = (hit.group(1), hit.group(3))
        now = int(hit.group(2))
        if key in pending_begin:
            widths[key[0]].append(now - pending_begin.pop(key))
        last_end[key] = (now, hit.group(4))


def show(label, values):
    if not values:
        print(f"  {label:<28} n=0")
        return
    values = sorted(values)
    print(
        f"  {label:<28} n={len(values):<6} "
        f"median={statistics.median(values):>9.0f}us  "
        f"p90={values[int(len(values) * 0.9)]:>9}us  max={values[-1]:>9}us"
    )


print("SLICE WIDTH (hypothesis b: slice cost)")
for transport in sorted(widths):
    show(f"{transport} slice width", widths[transport])
print()
print("RE-ENTRY LATENCY (hypothesis a: slice cadence)")
for key in sorted(gaps):
    show(f"{key[0]} {key[1]}", gaps[key])
