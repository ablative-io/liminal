#!/usr/bin/env bash
# check-published-graph.sh — the post-publish graph census.
#
# A publish confirmation is a census of the DEPENDENCY GRAPH, not just the
# bytes: byte-identical src/ does not imply an equivalent graph, because the
# edges live in a manifest GENERATED at publish time from workspace state
# outside any crate directory. This instrument exists because published
# liminal-sdk 0.5.1 froze a liminal-protocol 0.3.2 edge that put TWO wire-type
# crates into any binary that also held server 0.5.2 — caught by a consumer
# reading their lockfile, not by any check on our side.
#
# What it does: resolves the four published crates TOGETHER in a scratch
# manifest against the real registry, then fails if the lock holds more than
# one version of any liminal-family crate.
#
# Verdict vocabulary (the third-verdict law: a verdict is only information if
# the tree can change it):
#   exit 0  = PASS     — one version of each family crate in the joint graph
#   exit 1  = FAIL     — duplicate family crate: the published set is unsafe
#                        to co-resolve; fix is a new publish, not a retry
#   exit 77 = BLOCKED  — environment refused (network/registry/cargo absent);
#                        says nothing about the tree, do not read as FAIL
#
# Usage: tools/gates/release/check-published-graph.sh <liminal-rs-ver> <protocol-ver> <server-ver> <sdk-ver>
# Run it as a mandatory step after every publish wave; keep its output with
# the gate logs.

set -u

if [ "$#" -ne 4 ]; then
  echo "usage: $0 <liminal-rs-version> <liminal-protocol-version> <liminal-server-version> <liminal-sdk-version>" >&2
  exit 77
fi

LIMINAL_V="$1"; PROTOCOL_V="$2"; SERVER_V="$3"; SDK_V="$4"

command -v cargo >/dev/null 2>&1 || { echo "BLOCKED: cargo not on PATH" >&2; exit 77; }

SCRATCH="$(mktemp -d "${TMPDIR:-/tmp}/liminal-graph-census.XXXXXX")" || { echo "BLOCKED: mktemp failed" >&2; exit 77; }
trap 'rm -rf "$SCRATCH"' EXIT

mkdir -p "$SCRATCH/src"
: > "$SCRATCH/src/lib.rs"
cat > "$SCRATCH/Cargo.toml" <<EOF
[package]
name = "liminal-graph-census"
version = "0.0.0"
edition = "2021"

[dependencies]
liminal-rs = "=${LIMINAL_V}"
liminal-protocol = "=${PROTOCOL_V}"
liminal-server = "=${SERVER_V}"
liminal-sdk = "=${SDK_V}"
EOF

# Resolution only — no build. cargo writes Cargo.lock or tells us why not.
GEN_OUT="$(cd "$SCRATCH" && cargo generate-lockfile 2>&1)"
GEN_EXIT=$?
if [ "$GEN_EXIT" -ne 0 ]; then
  # A resolution failure against the live registry is ambiguous between "the
  # published set cannot co-resolve" (a FAIL-shaped fact) and "the registry
  # was unreachable" (a BLOCKED-shaped one). Discriminate on the error text.
  echo "$GEN_OUT"
  if printf '%s' "$GEN_OUT" | grep -qiE 'failed to select a version|conflict'; then
    echo "FAIL: the published set does not co-resolve" >&2
    exit 1
  fi
  echo "BLOCKED: lock generation failed for a non-resolution reason (network/registry?)" >&2
  exit 77
fi

echo "== liminal-family entries in the joint lock =="
awk '
  /^name = / { name=$3; gsub(/"/,"",name) }
  /^version = / {
    ver=$3; gsub(/"/,"",ver)
    if (name ~ /^liminal(-rs|-protocol|-server|-sdk)?$/) print name, ver
    name=""
  }
' "$SCRATCH/Cargo.lock" | sort | tee "$SCRATCH/family.txt"

DUPES="$(cut -d' ' -f1 "$SCRATCH/family.txt" | sort | uniq -d)"
if [ -n "$DUPES" ]; then
  echo "FAIL: duplicate liminal-family crate(s) in the joint graph: $DUPES" >&2
  exit 1
fi

COUNT="$(wc -l < "$SCRATCH/family.txt" | tr -d ' ')"
if [ "$COUNT" -ne 4 ]; then
  echo "FAIL: expected exactly 4 family entries in the lock, found $COUNT (vacuous-census guard)" >&2
  exit 1
fi

echo "PASS: 4 family crates, one version each — the published wave co-resolves to a single graph"
exit 0
