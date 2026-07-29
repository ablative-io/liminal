#!/usr/bin/env python3
"""Compare a nextest run against the pinned gate baseline.

`docs/gates/BASELINE-CENSUS.md` states that a pin no runner mechanically
compares is an instruction rather than a control. Until this script existed,
that file was an instruction. This is the runner.

It reads the pin from the census itself, so the census stays the single source
of truth and cannot drift from its consumer.

Three field roles, taken from the census and enforced here:

  COMPARE   counts; drift in either direction is RED
  REQUIRE   preconditions; a mismatch makes the comparison VOID -- neither a
            pass nor a failure, because the question was never validly asked
  RECORD    provenance; printed, never compared

Exit codes: 0 PASS, 1 RED, 2 instrument/usage error, 3 VOID.

VOID exists because a gate with only pass and fail has nowhere to put "I could
not validly compare", and silently converts that into one of the two.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

PASS, RED, ERROR, VOID = 0, 1, 2, 3

COMPARE_FIELDS = ("tests_started", "tests_ok", "tests_failed", "tests_ignored", "suites")
REQUIRE_FIELDS = ("baseline_tree", "toolchain", "cargo", "command")
PIN_LINE = re.compile(r"^(?P<key>[a-z_]+)\s*=\s*(?P<value>.+?)\s*$")


class InstrumentError(Exception):
    """The measurement could not be taken. Never reported as a comparison result."""


@dataclass(frozen=True)
class Counts:
    tests_started: int
    tests_ok: int
    tests_failed: int
    tests_ignored: int
    suites: int


def read_pin(census: Path) -> dict[str, str]:
    """Extract the fenced ``key = value`` block from the census."""
    if not census.is_file():
        raise InstrumentError(f"census not found: {census}")
    pin: dict[str, str] = {}
    inside = False
    for raw in census.read_text(encoding="utf-8").splitlines():
        if raw.startswith("```"):
            # The pin is the first fenced block; stop at its closing fence.
            if inside:
                break
            inside = True
            continue
        if not inside:
            continue
        match = PIN_LINE.match(raw)
        if match:
            pin[match.group("key")] = match.group("value")
    missing = [f for f in COMPARE_FIELDS if f not in pin]
    if missing:
        raise InstrumentError(f"census pin is missing COMPARE fields: {', '.join(missing)}")
    return pin


def count_stream(stream_path: Path) -> Counts:
    """Count test-level events, refusing to guess past anything unparseable.

    The raw event stream is two populations summed: ``libtest-json-plus`` emits
    ``started`` for suites as well as tests, so an unfiltered count yields
    1800 where the truth is 1764. Filtering on ``type`` is the whole point of
    doing this in a parser instead of a grep.
    """
    if not stream_path.is_file():
        raise InstrumentError(f"nextest json stream not found: {stream_path}")
    tally: Counter[tuple[str, str]] = Counter()
    unparseable = 0
    first_bad = 0
    for number, raw in enumerate(stream_path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            unparseable += 1
            if unparseable == 1:
                first_bad = number
            continue
        kind, name = event.get("type"), event.get("event")
        if isinstance(kind, str) and isinstance(name, str):
            tally[(kind, name)] += 1
    if unparseable:
        raise InstrumentError(
            f"{unparseable} unparseable line(s) in {stream_path}, first at line {first_bad}; "
            "a partially readable stream is not a measurement"
        )
    counts = Counts(
        tests_started=tally[("test", "started")],
        tests_ok=tally[("test", "ok")],
        tests_failed=tally[("test", "failed")],
        tests_ignored=tally[("test", "ignored")],
        suites=tally[("suite", "started")],
    )
    if counts.tests_started == 0:
        raise InstrumentError(
            "zero test-started events: a dead producer and an empty world are the same "
            "number, so this is refused rather than compared"
        )
    return counts


def reconcile(counts: Counts) -> list[str]:
    """Internal consistency. A tool disagreeing with itself is a failed premise."""
    problems = []
    accounted = counts.tests_ok + counts.tests_failed + counts.tests_ignored
    if counts.tests_started != accounted:
        problems.append(
            f"started ({counts.tests_started}) != ok+failed+ignored ({accounted}); "
            f"{counts.tests_started - accounted} test(s) emitted no terminal event"
        )
    if counts.suites == 0:
        problems.append("zero suite-started events while test events exist")
    return problems


def check_preconditions(pin: dict[str, str], observed: dict[str, str]) -> list[str]:
    """REQUIRE mismatches void the comparison; they do not fail it."""
    mismatches = []
    for field in REQUIRE_FIELDS:
        want, got = pin.get(field), observed.get(field)
        if got is None or want is None:
            continue
        if want != got:
            mismatches.append(f"{field}: pin {want!r} != run {got!r}")
    return mismatches


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("stream", type=Path, help="nextest libtest-json-plus log from the tree under gate")
    parser.add_argument(
        "--census",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "docs" / "gates" / "BASELINE-CENSUS.md",
        help="the census holding the pin (default: docs/gates/BASELINE-CENSUS.md)",
    )
    parser.add_argument("--toolchain", help="rustc version string of this run, for the REQUIRE check")
    parser.add_argument("--tree", help="tree sha under gate; compared to baseline_tree for the REQUIRE check")
    parser.add_argument(
        "--expect-total",
        type=int,
        help="expected test total when the gated tree changes .rs files; must be stated "
        "before the run and is echoed in the verdict",
    )
    args = parser.parse_args()

    try:
        pin = read_pin(args.census)
        counts = count_stream(args.stream)
    except InstrumentError as failure:
        print(f"ERROR  {failure}", file=sys.stderr)
        return ERROR

    print("RECORD (printed, never compared)")
    for field in ("summary_line", "machine", "operator", "measured_at", "runner"):
        if field in pin:
            print(f"  pin {field:14s} = {pin[field]}")

    observed = {}
    if args.toolchain:
        observed["toolchain"] = args.toolchain
    if args.tree:
        observed["baseline_tree"] = args.tree

    mismatches = check_preconditions(pin, observed)
    if mismatches:
        print("\nVOID  precondition mismatch -- the comparison was never validly asked:")
        for line in mismatches:
            print(f"  {line}")
        print("  This is neither a pass nor a failure. Re-run under the pinned conditions.")
        return VOID

    problems = reconcile(counts)
    if problems:
        print("\nERROR  the run disagrees with itself:", file=sys.stderr)
        for line in problems:
            print(f"  {line}", file=sys.stderr)
        return ERROR

    expected_started = args.expect_total if args.expect_total is not None else int(pin["tests_started"])
    expected = {
        "tests_started": expected_started,
        "tests_ok": expected_started,
        "tests_failed": int(pin["tests_failed"]),
        "tests_ignored": int(pin["tests_ignored"]),
        "suites": int(pin["suites"]),
    }
    if args.expect_total is not None:
        print(f"\nCOMPARE against STATED expected total {args.expect_total} (pin says {pin['tests_started']})")
    else:
        print(f"\nCOMPARE against the pin at {pin.get('baseline_tree_short', pin['baseline_tree'])}")

    drift = []
    for field in COMPARE_FIELDS:
        want, got = expected[field], getattr(counts, field)
        flag = "ok " if want == got else "RED"
        print(f"  {flag} {field:14s} pin {want:6d}   run {got:6d}")
        if want != got:
            drift.append(f"{field}: expected {want}, got {got} ({got - want:+d})")

    if drift:
        print("\nRED  count drift; every difference must be reasoned, not waved through:")
        for line in drift:
            print(f"  {line}")
        return RED

    print("\nPASS  counts identical to the pin.")
    return PASS


if __name__ == "__main__":
    sys.exit(main())
