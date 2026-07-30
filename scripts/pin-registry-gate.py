#!/usr/bin/env python3
"""Refuse a lockfile whose resolved versions were never verified at the registry.

This is the gate `docs/gates/RELEASE-RECORD.md` names as UNBUILT. That file's
banner says a document tells a careful reader what to check, and the strong
half is a gate that refuses a pin whose resolved version was never verified at
the registry. This is that half.

What it checks, for every registry-sourced package in Cargo.lock:

  1. the exact resolved version EXISTS at index.crates.io
  2. it is not YANKED  (a yank has no git record and can land after your pin)

Both are release-record facts that no manifest, tag, or commit subject can
answer -- see the six-document table in RELEASE-RECORD.md sec.1.

CONTROLS RUN IN-BAND, NOT BESIDE (RELEASE-RECORD.md sec.2). A registry sweep
without controls returns a confident, uniform, wrong answer:

  POSITIVE  serde                             must be HTTP-200, many versions
  NEGATIVE  a nonsense crate name             must be HTTP-404, not EMPTY-200

Without the negative arm, an interposing proxy handing back empty 200s reads as
"every crate you asked about is unpublished" -- and the positive arm alone
cannot tell those apart. Both must pass or the run is VOID, never PASS.

We use the sparse INDEX endpoint, never the download endpoint: crates.io 302s
blindly on downloads for nonexistent versions, so a negative control there
passes falsely. A control that cannot fail is decoration.

And per Athena's law, the strongest check here is the one we needed anyway:
the index response MUST parse as newline-delimited JSON with the fields we
read. An error page does not parse. That control cannot be forgotten because
forgetting it means not doing the task.

Exit codes, matching scripts/baseline-compare.py:
    0 PASS   every resolved version exists and is unyanked
    1 RED    at least one is absent or yanked
    2 ERROR  instrument or usage fault (unreadable lock, unparseable response)
    3 VOID   could not validly compare (controls failed, network unreachable)

VOID is a distinct verdict on purpose. A gate with only PASS/FAIL has nowhere
to put "I could not look" and silently picks one of them.
"""

from __future__ import annotations

import argparse
import json
import sys
import tomllib
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

PASS, RED, ERROR, VOID = 0, 1, 2, 3

INDEX = "https://index.crates.io"
UA = "liminal-pin-registry-gate (github.com/tomWhiting/liminal)"

POSITIVE_CONTROL = "serde"
NEGATIVE_CONTROL = "zzq-liminal-nonexistent-crate-xyz"


class Void(Exception):
    """The comparison could not validly be made."""


class Fault(Exception):
    """The instrument is broken or misused."""


def index_path(name: str) -> str:
    """crates.io sparse-index layout. Lowercased; length-dependent prefixing."""
    n = name.lower()
    if len(n) == 1:
        return f"1/{n}"
    if len(n) == 2:
        return f"2/{n}"
    if len(n) == 3:
        return f"3/{n[0]}/{n}"
    return f"{n[:2]}/{n[2:4]}/{n}"


def fetch(name: str, timeout: float) -> tuple[int, bytes]:
    """Return (status, body). 404 is a real answer, not an error."""
    req = urllib.request.Request(
        f"{INDEX}/{index_path(name)}", headers={"User-Agent": UA}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        # Unreachable is NOT "absent". Never let a network fault read as a
        # missing crate -- that is the failure this whole file exists to stop.
        raise Void(f"{name}: registry unreachable ({e})") from e


def entries(name: str, body: bytes) -> list[dict]:
    """Parse newline-delimited JSON. This IS the anti-error-page control."""
    out = []
    for i, line in enumerate(body.decode("utf-8", "replace").splitlines(), 1):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as e:
            raise Fault(f"{name}: index line {i} is not JSON ({e}) -- "
                        f"an error page does not parse") from e
        if "vers" not in obj or "yanked" not in obj:
            raise Fault(f"{name}: index line {i} lacks vers/yanked -- "
                        f"not a crates.io index document")
        out.append(obj)
    return out


def run_controls(timeout: float) -> None:
    """Prove the instrument can SEE and can DISCRIMINATE, in this run."""
    status, body = fetch(POSITIVE_CONTROL, timeout)
    if status != 200:
        raise Void(f"positive control {POSITIVE_CONTROL} returned {status}, "
                   f"expected 200 -- the instrument cannot see")
    versions = entries(POSITIVE_CONTROL, body)
    if len(versions) < 2:
        # An EMPTY-200 from a mirror or proxy lands exactly here. A crate
        # known to have hundreds of versions returning one or none means the
        # transport is lying, not that serde was unpublished.
        raise Void(f"positive control {POSITIVE_CONTROL} returned "
                   f"{len(versions)} versions -- EMPTY-200 or mirror fault")

    status, _ = fetch(NEGATIVE_CONTROL, timeout)
    if status != 404:
        raise Void(f"negative control {NEGATIVE_CONTROL} returned {status}, "
                   f"expected 404 -- the instrument cannot discriminate "
                   f"absent from present")

    print(f"  POSITIVE  {POSITIVE_CONTROL:<34} HTTP-200  "
          f"{len(versions)} versions")
    print(f"  NEGATIVE  {NEGATIVE_CONTROL:<34} HTTP-404")


def load_lock(path: str) -> tuple[list[dict], list[dict]]:
    try:
        with open(path, "rb") as fh:
            lock = tomllib.load(fh)
    except FileNotFoundError as e:
        raise Fault(f"lockfile not found: {path}") from e
    except tomllib.TOMLDecodeError as e:
        raise Fault(f"lockfile is not valid TOML: {e}") from e

    pkgs = lock.get("package")
    if not pkgs:
        # An empty package list and an unread lockfile are the same emptiness.
        # A zero is never compared -- refuse instead.
        raise Fault(f"{path} declares no packages -- refusing to report a "
                    f"clean sweep over nothing")

    registry = [p for p in pkgs if "registry+" in p.get("source", "")]
    local = [p for p in pkgs if "source" not in p]
    if not registry:
        raise Fault(f"{path} has {len(pkgs)} packages but none are "
                    f"registry-sourced -- nothing this gate can check")
    return registry, local


def check(pkg: dict, timeout: float) -> tuple[str, str, str]:
    """-> (verdict, name, detail). verdict in {ok, absent, yanked}."""
    name, want = pkg["name"], pkg["version"]
    status, body = fetch(name, timeout)
    if status == 404:
        return "absent", name, f"{want}: crate not in index at all (HTTP-404)"
    if status != 200:
        raise Void(f"{name}: HTTP-{status} -- neither present nor absent")

    found = [e for e in entries(name, body) if e["vers"] == want]
    if not found:
        # Athena's absence law: a yank never removes the entry, it sets a
        # flag. So an absent entry can only mean never-published -- the
        # competing explanation is required by the mechanism to leave a mark.
        return "absent", name, f"{want}: crate exists, this version never published"
    if found[0]["yanked"]:
        return "yanked", name, f"{want}: YANKED at the registry"
    return "ok", name, want


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Refuse a pin whose resolved version was never verified "
                    "at the registry.")
    ap.add_argument("lockfile", nargs="?", default="Cargo.lock")
    ap.add_argument("--jobs", type=int, default=8,
                    help="concurrent index requests (default 8)")
    ap.add_argument("--timeout", type=float, default=20.0)
    args = ap.parse_args()

    try:
        registry, local = load_lock(args.lockfile)
    except Fault as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return ERROR

    print(f"pin-registry-gate: {args.lockfile}")
    print(f"  {len(registry)} registry-sourced, {len(local)} path/workspace "
          f"(not checkable here: "
          f"{', '.join(p['name'] for p in local) or 'none'})")
    print("controls, in-band:")

    try:
        run_controls(args.timeout)
    except Void as e:
        print(f"VOID: {e}", file=sys.stderr)
        return VOID
    except Fault as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return ERROR

    print(f"checking {len(registry)} resolved versions...")
    try:
        with ThreadPoolExecutor(max_workers=args.jobs) as pool:
            results = list(pool.map(lambda p: check(p, args.timeout), registry))
    except Void as e:
        print(f"VOID: {e}", file=sys.stderr)
        return VOID
    except Fault as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return ERROR

    bad = [(v, n, d) for v, n, d in results if v != "ok"]

    # Advisory only: duplicates are legal, but they are the mechanism
    # RELEASE-RECORD.md sec.4 is about -- a published requirement that cannot
    # accept the version the tree moved to, keeping the old copy alongside.
    seen: dict[str, list[str]] = {}
    for p in registry:
        seen.setdefault(p["name"], []).append(p["version"])
    dupes = {k: sorted(v) for k, v in seen.items() if len(v) > 1}
    if dupes:
        print(f"\nadvisory -- {len(dupes)} crate(s) resolved at >1 version "
              f"(legal; the double-copy mechanism, not a failure):")
        for k, v in sorted(dupes.items()):
            print(f"  {k}: {', '.join(v)}")

    if bad:
        print(f"\nRED: {len(bad)} of {len(registry)} resolved versions "
              f"failed verification")
        for verdict, name, detail in sorted(bad):
            print(f"  [{verdict.upper()}] {name} {detail}")
        return RED

    print(f"\nPASS: all {len(registry)} resolved versions exist at "
          f"index.crates.io and none are yanked")
    print("  NOTE: this is a claim about the registry AT THIS OBSERVATION "
          "TIME, not about any tree. A yank can land after this run with no "
          "commit anywhere (RELEASE-RECORD.md sec.1, the coordinate law).")
    return PASS


if __name__ == "__main__":
    sys.exit(main())
