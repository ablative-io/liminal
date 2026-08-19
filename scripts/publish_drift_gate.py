#!/usr/bin/env python3
"""The publish-drift gate (row 486643cf).

Predicate: for every publishable workspace member whose manifest version is
ALREADY on the index, the tree's packaged bytes must equal the published
bytes. A crate whose version is not yet published PASSES this face (the
ceremony changelog leg owns that path). Any fetch/transport failure is
FATAL, never "not published" — an absence is a measurement of the
instrument until a positive control proves otherwise.

Exit codes: 0 = every published crate byte- and dep-equal · 1 = drift RED
somewhere (unmeasured crates named alongside; RED dominates because proven
drift is actionable regardless) · 2 = no drift proven but at least one
crate unmeasurable. Either way nonzero refuses. `--self-test` runs the
instrument / negative / positive-byte / positive-dep controls, each
contained (a Fatal in one control fails that control, never aborts the
rest), and exits 0 only if every control behaves.

The byte compare excludes the GENERATED Cargo.toml (cargo-version
normalization) but its dependency tables are compared semantically — the
generated manifest is the only file where workspace-inherited requirement
changes materialize, so excluding it wholesale would blind the gate to
exactly the drift class its row was minted for (found by this script's own
first execution, 2026-08-19).

Design: ablative/docs/design/liminal-publish-drift-gate-20260819.md.
"""

import hashlib
import json
import shutil
import subprocess
import sys
import tarfile
import tempfile
import contextlib
import io
import tomllib
import urllib.request
from pathlib import Path

USER_AGENT = "hermes-crumpet-liminal-seat (dean@ablative.com.au)"
INDEX_BASE = "https://index.crates.io"
CRATE_BASE = "https://static.crates.io/crates"
# Files excluded from the BYTE comparison, each for a stated reason:
# .cargo_vcs_info.json carries the packaging commit's sha (differs by
# construction on any new commit); the GENERATED Cargo.toml is normalized
# by the packaging cargo's own version (Cargo.toml.orig IS compared).
# The generated Cargo.toml is NOT thereby uninspected: it is the ONLY file
# where workspace-inherited requirements materialize (Cargo.toml.orig keeps
# `workspace = true` byte-identical under a workspace-pin change), so its
# dependency tables are compared SEMANTICALLY via dep_map() — a wholesale
# exclusion here would swallow exactly the drift class this gate hunts.
EXCLUDED = {".cargo_vcs_info.json", "Cargo.toml"}


class Fatal(Exception):
    """Instrument failure: the gate could not measure. Never a verdict."""


def index_path(name: str) -> str:
    n = len(name)
    if n == 1:
        return f"1/{name}"
    if n == 2:
        return f"2/{name}"
    if n == 3:
        return f"3/{name[0]}/{name}"
    return f"{name[:2]}/{name[2:4]}/{name}"


def fetch(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.read()
    except Exception as error:  # noqa: BLE001 - every failure is FATAL here
        raise Fatal(f"fetch failed for {url}: {error}") from error


def published_cksum(name: str, version: str) -> str | None:
    """The index row's cksum for (name, version), or None if unpublished.

    A 404 for the whole crate file means the crate has never been published
    under this name — that is a real absence only because the index serves
    404 as its documented not-found answer; any other failure is Fatal.
    """
    url = f"{INDEX_BASE}/{index_path(name)}"
    try:
        body = fetch(url)
    except Fatal as error:
        if "404" in str(error):
            return None
        raise
    for line in body.decode("utf-8").splitlines():
        row = json.loads(line)
        if row["vers"] == version:
            if row.get("yanked"):
                # Yanked bytes are still published bytes: the comparison
                # still applies (a tree drifting under a yanked version is
                # still drift), so return the cksum rather than None.
                pass
            return row["cksum"]
    return None


def workspace_publish_set(workspace_root: Path) -> list[tuple[str, str, Path]]:
    """(name, version, crate_dir) for every member without publish = false.

    Read fresh from the manifests at every run — a publish list carried in
    prose is a memory of the manifests, not a reading of them.
    """
    root_manifest = (workspace_root / "Cargo.toml").read_text()
    members: list[str] = []
    in_members = False
    for raw in root_manifest.splitlines():
        line = raw.strip()
        if line.startswith("members"):
            in_members = True
        if in_members:
            if '"' in line:
                members.append(line.split('"')[1])
            if line.endswith("]"):
                break
    crates = []
    for member in members:
        manifest_path = workspace_root / member / "Cargo.toml"
        text = manifest_path.read_text()
        if "publish = false" in text:
            continue
        name = version = None
        for raw in text.splitlines():
            line = raw.strip()
            if line.startswith("name = ") and name is None:
                name = line.split('"')[1]
            if line.startswith("version = ") and version is None:
                version = line.split('"')[1]
            if name and version:
                break
        if not name or not version:
            raise Fatal(f"{manifest_path}: could not read name/version")
        crates.append((name, version, workspace_root / member))
    if not crates:
        raise Fatal("manifest reading produced an empty publish set")
    return crates


def extract_crate(crate_bytes: bytes, dest: Path) -> Path:
    dest.mkdir(parents=True, exist_ok=True)
    archive = dest / "archive.crate"
    archive.write_bytes(crate_bytes)
    with tarfile.open(archive, "r:gz") as tar:
        tar.extractall(dest, filter="data")
    roots = [p for p in dest.iterdir() if p.is_dir()]
    if len(roots) != 1:
        raise Fatal(f"crate archive extracted to {len(roots)} roots, expected 1")
    return roots[0]


def file_map(root: Path) -> dict[str, bytes]:
    mapping = {}
    for path in sorted(root.rglob("*")):
        if path.is_file():
            relative = str(path.relative_to(root))
            if relative in EXCLUDED:
                continue
            mapping[relative] = path.read_bytes()
    if "Cargo.toml.orig" not in mapping:
        raise Fatal(
            f"{root}: no Cargo.toml.orig in package — tarball predates the "
            "current packaging format; refusing to guess"
        )
    return mapping


def dep_map(manifest_bytes: bytes) -> dict[tuple[str, str], dict]:
    """(table, dep-name) -> normalized entry from a GENERATED Cargo.toml.

    Both sides of the comparison are cargo-generated manifests, so table
    shapes match; entries are normalized (bare string -> {"version": ...},
    feature lists sorted) to be immune to formatting, not to meaning.
    """
    data = tomllib.loads(manifest_bytes.decode("utf-8"))
    tables: list[tuple[str, dict]] = []
    for kind in ("dependencies", "dev-dependencies", "build-dependencies"):
        if kind in data:
            tables.append((kind, data[kind]))
    for target, target_data in data.get("target", {}).items():
        for kind in ("dependencies", "dev-dependencies", "build-dependencies"):
            if kind in target_data:
                tables.append((f"target.{target}.{kind}", target_data[kind]))
    mapping: dict[tuple[str, str], dict] = {}
    for table, deps in tables:
        for dep_name, entry in deps.items():
            if isinstance(entry, str):
                entry = {"version": entry}
            entry = dict(entry)
            if "features" in entry:
                entry["features"] = sorted(entry["features"])
            mapping[(table, dep_name)] = entry
    return mapping


def package_local(workspace_root: Path, name: str, scratch: Path) -> Path:
    """cargo package --no-verify: resolution only, no compile."""
    result = subprocess.run(
        [
            "cargo", "package", "--no-verify", "--allow-dirty",
            "-p", name, "--target-dir", str(scratch / "target"),
        ],
        cwd=workspace_root,
        capture_output=True,
        text=True,
        timeout=300,
    )
    if result.returncode != 0:
        raise Fatal(f"cargo package -p {name} failed:\n{result.stderr[-2000:]}")
    packages = list((scratch / "target" / "package").glob(f"{name}-*.crate"))
    if len(packages) != 1:
        raise Fatal(f"expected exactly one packaged crate for {name}, found {len(packages)}")
    return packages[0]


def compare(
    name: str,
    version: str,
    published: dict[str, bytes],
    local: dict[str, bytes],
    published_manifest: bytes,
    local_manifest: bytes,
) -> list[str]:
    problems = []
    for path in sorted(set(published) | set(local)):
        if path not in local:
            problems.append(f"missing from tree package: {path}")
        elif path not in published:
            problems.append(f"extra in tree package: {path}")
        elif published[path] != local[path]:
            problems.append(f"differs: {path} (published {len(published[path])}B, tree {len(local[path])}B)")
    published_deps = dep_map(published_manifest)
    local_deps = dep_map(local_manifest)
    for key in sorted(set(published_deps) | set(local_deps)):
        table, dep_name = key
        if key not in local_deps:
            problems.append(f"dep missing from tree package: [{table}] {dep_name}")
        elif key not in published_deps:
            problems.append(f"dep added in tree package: [{table}] {dep_name} = {local_deps[key]}")
        elif published_deps[key] != local_deps[key]:
            problems.append(
                f"dep differs: [{table}] {dep_name} — published {published_deps[key]}, tree {local_deps[key]}"
            )
    return problems


def run_gate(workspace_root: Path) -> int:
    """0 = every published crate byte- and dep-equal; 1 = drift PROVEN
    somewhere (unmeasured crates, if any, are named alongside); 2 = no drift
    proven but at least one crate could not be measured. RED dominates FATAL
    because proven drift is actionable regardless of what else could not be
    measured; a no-RED run with any FATAL must not read as green. Either way
    nonzero refuses."""
    reds = 0
    fatals: list[str] = []
    with tempfile.TemporaryDirectory(prefix="drift-gate-") as scratch_str:
        scratch = Path(scratch_str)
        for name, version, _crate_dir in workspace_publish_set(workspace_root):
            try:
                cksum = published_cksum(name, version)
                if cksum is None:
                    print(f"PASS {name} {version}: not on the index — the changelog leg governs the cut")
                    continue
                crate_bytes = fetch(f"{CRATE_BASE}/{name}/{name}-{version}.crate")
                actual = hashlib.sha256(crate_bytes).hexdigest()
                if actual != cksum:
                    raise Fatal(
                        f"{name} {version}: downloaded crate sha256 {actual} != index cksum "
                        f"{cksum} — transport damage, NOT a tree verdict"
                    )
                published_root = extract_crate(crate_bytes, scratch / f"pub-{name}")
                local_crate = package_local(workspace_root, name, scratch / f"pkg-{name}")
                local_root = extract_crate(local_crate.read_bytes(), scratch / f"loc-{name}")
                problems = compare(
                    name,
                    version,
                    file_map(published_root),
                    file_map(local_root),
                    (published_root / "Cargo.toml").read_bytes(),
                    (local_root / "Cargo.toml").read_bytes(),
                )
            except Fatal as error:
                # Contained per crate so the rest still get measured; the
                # inability is preserved in the exit rule, never absorbed.
                fatals.append(f"{name} {version}: {error}")
                print(f"FATAL {name} {version} (instrument, not verdict): {error}")
                continue
            if problems:
                reds += 1
                print(f"RED {name} {version}: SILENT IN-TREE DRIFT under a published version")
                for problem in problems:
                    print(f"  {problem}")
                print(
                    "  lawful exits: bump the version (cut a release that owns this "
                    "delta) or revert the delta. Silence is not an exit."
                )
            else:
                print(f"GREEN {name} {version}: tree bytes equal published bytes")
    if fatals:
        print(f"UNMEASURED: {len(fatals)} crate(s) — a green over these would be vacuous")
    if reds:
        return 1
    return 2 if fatals else 0


def self_test(workspace_root: Path) -> int:
    """Positive / negative / instrument controls. 0 only if every control behaves."""
    failures = []

    # Instrument control 1: a wrong index host must be FATAL, not PASS.
    global INDEX_BASE  # noqa: PLW0603 - controls deliberately break the instrument
    real_index = INDEX_BASE
    INDEX_BASE = "https://index.invalid.crates.example"
    try:
        published_cksum("liminal-rs", "0.6.0")
        failures.append("instrument-1: broken index host produced a verdict instead of Fatal")
    except Fatal:
        print("control instrument-1 OK: broken index host is FATAL")
    finally:
        INDEX_BASE = real_index

    # Instrument control 2: corrupted download bytes must FATAL at the cksum
    # step (exercised through the same predicate the gate uses).
    cksum = published_cksum("liminal-rs", "0.6.0")
    if cksum is None:
        failures.append("instrument-2: liminal-rs 0.6.0 missing from index (positive-control substrate gone)")
    else:
        corrupted = fetch(f"{CRATE_BASE}/liminal-rs/liminal-rs-0.6.0.crate") + b"x"
        if hashlib.sha256(corrupted).hexdigest() == cksum:
            failures.append("instrument-2: corruption not detected by sha256 (impossible)")
        else:
            print("control instrument-2 OK: corrupted bytes fail the cksum predicate")

    # Positive + negative controls need cargo package on a scratch copy. Every
    # control runs the REAL gate captured and contained: a Fatal inside one
    # control is that control's FAILURE, never an abort of the remaining
    # controls (an aborted control would read as a passed control).
    def captured_gate(root: Path) -> tuple[int | None, str]:
        buffer = io.StringIO()
        try:
            with contextlib.redirect_stdout(buffer):
                verdict = run_gate(root)
        except Fatal as error:
            buffer.write(f"FATAL escaped run_gate: {error}\n")
            verdict = None
        return verdict, buffer.getvalue()

    print("control negative: running the real gate over the clean tree")
    verdict, output = captured_gate(workspace_root)
    print(output, end="")
    if verdict != 0:
        failures.append(
            f"negative: clean tree did not report green (exit {verdict}) — either the "
            "gate is broken or the tree really has drifted; investigate WHICH before "
            "trusting anything, the two are different emergencies"
        )
    else:
        print("control negative OK: clean tree green")

    # Positive 1 — source-byte drift: doctor one source file of a published
    # crate and require RED naming THAT FILE for THAT CRATE. Exit 1 alone is
    # not enough: pre-existing drift elsewhere would satisfy it vacuously.
    with tempfile.TemporaryDirectory(prefix="drift-positive-") as copy_str:
        doctored = Path(copy_str) / "ws"
        shutil.copytree(
            workspace_root, doctored,
            ignore=shutil.ignore_patterns("target", ".git", "gate-logs", ".claude"),
        )
        victims = sorted((doctored / "crates" / "liminal" / "src").rglob("*.rs"))
        if not victims:
            failures.append("positive-byte: no source file found to doctor")
        else:
            victim = victims[0]
            victim.write_bytes(victim.read_bytes() + b"\n// drift-gate positive control\n")
            victim_rel = victim.relative_to(doctored / "crates" / "liminal")
            print(f"control positive-byte: doctored {victim.relative_to(doctored)} by one line")
            verdict, output = captured_gate(doctored)
            print(output, end="")
            if verdict != 1:
                failures.append(f"positive-byte: gate exit {verdict}, wanted 1 (drift RED)")
            elif f"differs: {victim_rel}" not in output:
                failures.append(
                    f"positive-byte: exit 1 but the doctored file {victim_rel} is not "
                    "named — the red belongs to something else (vacuous pass refused)"
                )
            else:
                print("control positive-byte OK: doctored file named RED")

    # Positive 2 — workspace-pin drift, the class invisible to the byte
    # compare: doctor the workspace serde requirement (resolvable either way)
    # and require a dep-differs RED naming serde. This exercises the SAME
    # predicate (dep_map over generated manifests) the gate uses.
    with tempfile.TemporaryDirectory(prefix="drift-positive-dep-") as copy_str:
        doctored = Path(copy_str) / "ws"
        shutil.copytree(
            workspace_root, doctored,
            ignore=shutil.ignore_patterns("target", ".git", "gate-logs", ".claude"),
        )
        root_manifest = doctored / "Cargo.toml"
        text = root_manifest.read_text()
        needle = 'serde = { version = "1",'
        if needle not in text:
            failures.append(
                "positive-dep: workspace serde pin not found to doctor — the "
                "substrate moved; update this control, do not skip it"
            )
        else:
            root_manifest.write_text(text.replace(needle, 'serde = { version = "1.0",', 1))
            print('control positive-dep: doctored workspace serde req "1" -> "1.0"')
            verdict, output = captured_gate(doctored)
            print(output, end="")
            dep_lines = [
                line for line in output.splitlines()
                if line.strip().startswith("dep differs:") and " serde " in line
            ]
            if verdict != 1:
                failures.append(f"positive-dep: gate exit {verdict}, wanted 1 (drift RED)")
            elif not dep_lines:
                failures.append(
                    "positive-dep: exit 1 but no dep-differs line names serde — the "
                    "workspace-pin class is still invisible (vacuous pass refused)"
                )
            else:
                print("control positive-dep OK: workspace-pin drift named RED")

    if failures:
        print("SELF-TEST FAILURES:")
        for failure in failures:
            print(f"  {failure}")
        return 2
    print("SELF-TEST: all controls behaved")
    return 0


def main() -> int:
    workspace_root = Path(__file__).resolve().parent.parent
    try:
        if "--self-test" in sys.argv:
            return self_test(workspace_root)
        return run_gate(workspace_root)
    except Fatal as error:
        print(f"FATAL (instrument, not verdict): {error}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
