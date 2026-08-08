#!/usr/bin/env python3
"""Per-iteration burst-window analysis.

The burst window is from the first pump entry that finds a non-empty inbox to
the shed (or to the last pump entry). Only inside that window is a connection
actually racing the channel actor, so only inside it do slice counts and gaps
mean anything — the aggregate over a whole test run is dominated by idle phases
and by the publisher connection's long spans.

Reports, per iteration and per connection: slices inside the window, frames
drained, the gaps between consecutive slices, and how many of the connection's
READY wakes were GENUINE (fired by the empty-to-non-empty edge) versus fired by
the overflow refusal path, which only starts once the subscription is already
doomed.
"""
import re
import statistics
import sys
from collections import defaultdict

lines = open(sys.argv[1], errors="replace").read().split("\n")
bounds = [i for i, l in enumerate(lines) if re.search(r"ARM I iteration \d+:", l)]

begin = re.compile(r"PROBE\[(ws|tcp)-slice-begin\] t=(\d+) pid=(\d+)")
end = re.compile(r"PROBE\[(ws|tcp)-slice-end\] t=(\d+) pid=(\d+) outcome=(\w+)")
enter = re.compile(r"PROBE\[pump-enter\] t=(\d+) depths=\[\((\d+), (\d+)\)\]")
exit_ = re.compile(r"PROBE\[pump-exit\] t=(\d+) drained=(\d+) budget=\d+ depths=\[\((\d+), (\d+)\)\]")
fire = re.compile(r"PROBE\[fire\] t=(\d+) pid=(\d+)")
overflow = re.compile(r"PROBE\[overflow\] t=(\d+)")
header = re.compile(r"ARM I iteration (\d+): ok=(\w+)")

start = 0
for stop in bounds:
    tag = header.search(lines[stop])
    part = lines[start:stop]
    start = stop + 1

    first_overflow = min(
        (int(m.group(1)) for m in (overflow.search(l) for l in part) if m), default=None
    )
    busy = [
        (int(m.group(1)), int(m.group(2)))
        for m in (enter.search(l) for l in part)
        if m and int(m.group(3)) > 0
    ]
    if not busy:
        print(f"iteration {tag.group(1)} ok={tag.group(2)}: no busy pump window")
        continue
    window_start = busy[0][0]
    window_end = max(t for t, _ in busy)

    slices = defaultdict(list)
    opens = {}
    for line in part:
        m = begin.search(line)
        if m:
            opens[(m.group(1), m.group(3))] = int(m.group(2))
            continue
        m = end.search(line)
        if m:
            key = (m.group(1), m.group(3))
            began = opens.pop(key, None)
            if began is not None and window_start <= began <= window_end:
                slices[key].append((began, int(m.group(2)), m.group(4)))

    drained = defaultdict(int)
    for line in part:
        m = exit_.search(line)
        if m and window_start <= int(m.group(1)) <= window_end:
            drained[m.group(3)] += int(m.group(2))

    fires = defaultdict(lambda: [0, 0])
    for line in part:
        m = fire.search(line)
        if m:
            when, pid = int(m.group(1)), m.group(2)
            late = first_overflow is not None and when >= first_overflow
            fires[pid][1 if late else 0] += 1

    print(f"iteration {tag.group(1)} ok={tag.group(2)} window={window_end - window_start}us")
    for key in sorted(slices, key=lambda k: (k[0], int(k[1]))):
        runs = sorted(slices[key])
        if len(runs) < 1:
            continue
        gaps = [runs[i + 1][0] - runs[i][1] for i in range(len(runs) - 1)]
        genuine, late = fires.get(key[1], [0, 0])
        print(
            f"   {key[0]:3} pid={key[1]:>2} slices={len(runs):>4} "
            f"drained={drained.get(key[1], 0):>4} "
            f"gap_med={(statistics.median(gaps) if gaps else 0):>8.0f}us "
            f"gap_max={(max(gaps) if gaps else 0):>8}us "
            f"wakes: genuine={genuine:>4} post_overflow={late:>4}"
        )
