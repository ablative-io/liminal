#!/usr/bin/env python3
"""Refuse a BREAKING liminal-protocol bump that silently spends the
`#[non_exhaustive]` ride.

WHY THIS IS A GATE AND NOT A TASK
---------------------------------
This started as task #34, "add #[non_exhaustive] in the next cut". A task whose
trigger is somebody else's scheduling decision is not a task -- it sits in a
list looking parked while getting more expensive. A gate attached to that
decision fires by itself.

THE SEMVER FACT, READ FROM A FILE RATHER THAN RECALLED
------------------------------------------------------
`share/doc/rust/html/cargo/reference/semver.html` (ships with the toolchain),
section "Major: adding non_exhaustive to an existing enum, variant, or struct
with no private fields":

    "Pattern matching on non-exhaustive structs requires `..` and matching on
     enums does not count towards exhaustiveness."

  Mitigation, verbatim: "Mark structs, enums, and enum variants as
  #[non_exhaustive] when first introducing them, rather than adding
  #[non_exhaustive] later on."

Two consequences, and the second is the one that sets this gate's trigger:

  1. Adding the attribute later is a MAJOR change, and has been since the very
     first publish. The classification flipped ONCE; it does not compound per
     release. So "every release makes it worse" is the wrong mechanism. What
     actually decays is blast radius, scope, and the number of breaking cuts
     left to ride on.

  2. ⇒ THE ATTRIBUTE CAN ONLY RIDE A BREAKING BUMP. For a 0.x crate the
     breaking boundary is the MINOR: cargo reads `0.3.2` as `>=0.3.2, <0.4.0`,
     so 0.3.2 -> 0.3.3 is a COMPATIBLE update that every consumer takes
     automatically. Shipping the attribute in a patch bump would break them
     inside a range they were promised was safe. The gate must therefore fire
     on the breaking-series bump, NOT on "the next version bump".

  (Same 0.x arithmetic measured directly at the registry this same night:
   frame-core's `^0.1.0` admits `>=0.1.0, <0.2.0`.)

WHAT IT DOES
------------
Compares the in-tree liminal-protocol version against the highest PUBLISHED
version. If the tree has moved to a new breaking series, the decision must be
recorded one of two ways -- never left implicit:

  * the public enums carry `#[non_exhaustive]`, or
  * a written refusal exists naming what was accepted.

Exit codes match the other gates in this directory:
    0 PASS   not a breaking bump, or the decision is recorded
    1 RED    a breaking bump that spends the ride with no decision on file
    2 ERROR  instrument or usage fault
    3 VOID   could not validly compare (registry unreachable)
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tomllib
import urllib.error
import urllib.request

PASS, RED, ERROR, VOID = 0, 1, 2, 3

CRATE = "liminal-protocol"
MANIFEST = "crates/liminal-protocol/Cargo.toml"
SRC = "crates/liminal-protocol/src"
REFUSAL = "docs/gates/EXHAUSTIVENESS-REFUSAL.md"

INDEX = "https://index.crates.io"
UA = "liminal-exhaustiveness-gate (github.com/tomWhiting/liminal)"
POSITIVE_CONTROL = "serde"


def series(version: str) -> tuple[int, int]:
    """The breaking-compatibility series cargo actually uses.

    0.x  -> the MINOR is the breaking boundary  -> (0, minor)
    >=1  -> the MAJOR is                        -> (major, 0)
    """
    m = re.match(r"^(\d+)\.(\d+)", version)
    if not m:
        raise ValueError(f"unparseable version {version!r}")
    major, minor = int(m.group(1)), int(m.group(2))
    return (0, minor) if major == 0 else (major, 0)


def fetch_json_lines(crate: str, timeout: float) -> list[dict]:
    n = crate.lower()
    path = (f"1/{n}" if len(n) == 1 else f"2/{n}" if len(n) == 2
            else f"3/{n[0]}/{n}" if len(n) == 3 else f"{n[:2]}/{n[2:4]}/{n}")
    req = urllib.request.Request(f"{INDEX}/{path}", headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return []
        raise RuntimeError(f"{crate}: HTTP-{e.code}") from e
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        raise RuntimeError(f"{crate}: registry unreachable ({e})") from e
    # The payload must parse to be useful -- an error page does not.
    return [json.loads(l) for l in body.decode().splitlines() if l.strip()]


def census() -> tuple[int, int]:
    """(pub enum declarations, #[non_exhaustive] occurrences) in protocol src.

    Counted from `git grep` at HEAD, and PARTITIONED: only lines whose first
    non-space token is `pub enum` count, so doc-comment prose and re-export
    lines cannot inflate the number. That partition exists because the figure
    carried in prose for a week ("20+") was wrong by an order of magnitude.
    """
    def grep(pattern: str) -> list[str]:
        out = subprocess.run(
            ["git", "grep", "-h", pattern, "HEAD", "--", SRC],
            capture_output=True, text=True, check=False)
        if out.returncode not in (0, 1):
            raise RuntimeError(f"git grep failed: {out.stderr.strip()}")
        return out.stdout.splitlines()

    enums = sum(1 for l in grep("pub enum") if l.lstrip().startswith("pub enum"))
    attrs = sum(1 for l in grep("non_exhaustive")
                if l.lstrip().startswith("#[non_exhaustive]"))
    # Positive control: the instrument must find a token that IS there. A zero
    # from a blind grep and a true absence are the same integer otherwise.
    control = sum(1 for l in grep("pub struct")
                  if l.lstrip().startswith("pub struct"))
    if control == 0:
        raise RuntimeError("positive control failed: 0 `pub struct` in "
                           f"{SRC} -- grep is blind, refusing to report a zero")
    print(f"  census (positive control: {control} `pub struct` found)")
    return enums, attrs


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Refuse a breaking liminal-protocol bump that spends the "
                    "#[non_exhaustive] ride with no decision on file.")
    ap.add_argument("--timeout", type=float, default=20.0)
    args = ap.parse_args()

    try:
        with open(MANIFEST, "rb") as fh:
            tree_version = tomllib.load(fh)["package"]["version"]
    except (FileNotFoundError, KeyError, tomllib.TOMLDecodeError) as e:
        print(f"ERROR: cannot read {CRATE} version from {MANIFEST}: {e}",
              file=sys.stderr)
        return ERROR

    try:
        control = fetch_json_lines(POSITIVE_CONTROL, args.timeout)
        if len(control) < 2:
            print(f"VOID: positive control {POSITIVE_CONTROL} returned "
                  f"{len(control)} versions -- EMPTY-200 or mirror fault",
                  file=sys.stderr)
            return VOID
        published = fetch_json_lines(CRATE, args.timeout)
    except RuntimeError as e:
        print(f"VOID: {e}", file=sys.stderr)
        return VOID
    except json.JSONDecodeError as e:
        print(f"ERROR: index response is not JSON ({e}) -- an error page does "
              f"not parse", file=sys.stderr)
        return ERROR

    live = [p["vers"] for p in published if not p["yanked"]]
    if not live:
        print(f"ERROR: {CRATE} has no unyanked published version to compare "
              f"against", file=sys.stderr)
        return ERROR

    def full(v: str) -> tuple[int, ...]:
        return tuple(int(x) for x in re.findall(r"\d+", v)[:3])

    try:
        # Order by the FULL version, not by series -- ordering by series alone
        # ties across 0.3.0/0.3.1/0.3.2 and prints the wrong one as "highest
        # published". The verdict would still be right and the displayed fact
        # wrong, which is its own defect.
        highest = max(live, key=full)
        tree_s, pub_s = series(tree_version), series(highest)
        enums, attrs = census()
    except (ValueError, RuntimeError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return ERROR

    print(f"exhaustiveness-gate: {CRATE}")
    print(f"  in tree   {tree_version}  (breaking series {tree_s})")
    print(f"  published {highest}  (breaking series {pub_s}), "
          f"{len(live)} unyanked of {len(published)}")
    print(f"  {enums} `pub enum` declarations, {attrs} `#[non_exhaustive]`")

    if tree_s <= pub_s:
        print(f"\nPASS: not a breaking bump ({tree_s} <= {pub_s}). The ride is "
              f"still available.")
        print(f"  Standing debt: {enums} public enums carry {attrs} "
              f"attributes. This is an UPPER BOUND on the exposed surface -- "
              f"module visibility and re-export reachability are unresolved.")
        return PASS

    if attrs > 0:
        print(f"\nPASS: breaking bump {pub_s} -> {tree_s}, and the attribute "
              f"is present ({attrs} occurrences). Ride taken.")
        return PASS

    try:
        with open(REFUSAL, encoding="utf-8") as fh:
            body = fh.read().strip()
        if len(body) < 200:
            raise ValueError("refusal file is too short to name anything")
        print(f"\nPASS: breaking bump {pub_s} -> {tree_s} with no attribute, "
              f"but a written refusal exists at {REFUSAL} "
              f"({len(body)} chars). Decision is on file.")
        return PASS
    except (FileNotFoundError, ValueError):
        pass

    print(f"\nRED: BREAKING BUMP {pub_s} -> {tree_s} SPENDS THE RIDE.")
    print(f"  {CRATE} moves to a new breaking series and {enums} public enums "
          f"still carry {attrs} `#[non_exhaustive]`.")
    print(f"  Adding it later is a MAJOR change (cargo semver reference, "
          f"'adding non_exhaustive to an existing enum'), so it can only ride "
          f"a breaking bump -- and this is one.")
    print(f"  Resolve by EITHER adding the attribute OR writing {REFUSAL} "
          f"naming what was accepted and why.")
    return RED


if __name__ == "__main__":
    sys.exit(main())
