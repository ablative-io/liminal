#!/usr/bin/env python3
"""Segments a probe log into arm-I iterations and summarises each transport's
slice timeline: how many slices, how long each one took, and how long the gaps
between them were.

The (a)-vs-(b) discriminator is exactly these two columns. Hypothesis (a) SLICE
CADENCE predicts big GAPS between slices; hypothesis (b) SLICE COST predicts
wide slices with negligible gaps.
"""
import re
import sys
from collections import defaultdict

PATH = sys.argv[1]
WANT = sys.argv[2] if len(sys.argv) > 2 else None

begin = re.compile(r"PROBE\[(ws|tcp)-slice-begin\] t=(\d+) pid=(\d+)")
end = re.compile(r"PROBE\[(ws|tcp)-slice-end\] t=(\d+) pid=(\d+) outcome=(\w+)")
pump_enter = re.compile(r"PROBE\[pump-enter\] t=(\d+) depths=\[(.*?)\] held=(\d+)")
pump_exit = re.compile(r"PROBE\[pump-exit\] t=(\d+) drained=(\d+) budget=(\d+) depths=\[(.*?)\]")
pump_held = re.compile(r"PROBE\[pump-held\] t=(\d+) subscription_id=(\d+)")
overflow = re.compile(r"PROBE\[overflow\] t=(\d+) cause=(\w+) queued=(\d+)")
shed = re.compile(r"PROBE\[shed\] t=(\d+) subscription_id=(\d+)")
iteration = re.compile(r"ARM I iteration (\d+): ok=(\w+) :: (.*)")

segments = []
current = []
for line in open(PATH, encoding="utf-8", errors="replace"):
    hit = iteration.search(line)
    current.append(line)
    if hit:
        segments.append((int(hit.group(1)), hit.group(2), hit.group(3), current))
        current = []

for index, ok, detail, lines in segments:
    if WANT is not None and str(index) != WANT:
        continue
    print(f"===== iteration {index} ok={ok}")
    print(f"      {detail.strip()[:150]}")
    opens = {}
    slices = defaultdict(list)   # (transport, pid) -> [(start, end, outcome)]
    events = []
    for line in lines:
        hit = begin.search(line)
        if hit:
            opens[(hit.group(1), hit.group(3))] = int(hit.group(2))
            continue
        hit = end.search(line)
        if hit:
            key = (hit.group(1), hit.group(3))
            start = opens.pop(key, None)
            if start is not None:
                slices[key].append((start, int(hit.group(2)), hit.group(4)))
            continue
        for pattern, tag in ((pump_enter, "pump-enter"), (pump_exit, "pump-exit"),
                             (pump_held, "pump-held"), (overflow, "overflow"),
                             (shed, "shed")):
            hit = pattern.search(line)
            if hit:
                events.append((int(hit.group(1)), tag, line.strip()))
                break

    for key in sorted(slices, key=lambda k: (k[0], int(k[1]))):
        runs = slices[key]
        if not runs:
            continue
        widths = [e - s for s, e, _ in runs]
        gaps = [runs[i + 1][0] - runs[i][1] for i in range(len(runs) - 1)]
        parked = sum(1 for _, _, o in runs if o == "park")
        span = runs[-1][1] - runs[0][0]
        print(f"  {key[0]:3} pid={key[1]:>3} slices={len(runs):4} span={span:>8}us "
              f"width med={sorted(widths)[len(widths)//2]:>6}us max={max(widths):>7}us "
              f"total={sum(widths):>8}us | gap med={(sorted(gaps)[len(gaps)//2] if gaps else 0):>6}us "
              f"max={(max(gaps) if gaps else 0):>8}us total={sum(gaps):>8}us | parks={parked}")

    events.sort()
    marks = [e for e in events if e[1] in ("overflow", "shed", "pump-held")]
    print(f"  pump-held={sum(1 for e in events if e[1]=='pump-held')} "
          f"overflow={sum(1 for e in events if e[1]=='overflow')} "
          f"shed={sum(1 for e in events if e[1]=='shed')}")
    if marks:
        print(f"  first mark: {marks[0][2][:120]}")
        print(f"  last  mark: {marks[-1][2][:120]}")
    # The pump slices that actually moved frames, per subscription.
    moved = [e for e in events if e[1] == "pump-exit" and "drained=0 " not in e[2]]
    print(f"  pump-exit slices with drained>0: {len(moved)}")
    for entry in moved[:8]:
        print(f"    {entry[2][:130]}")
