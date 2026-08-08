#!/usr/bin/env bash
#
# Arm this clone's commit-msg hook, and PROVE it is armed.
#
# WHY THIS EXISTS: `.git/config` is not versioned and `.githooks/` is. The hook
# travels with the tree; the WIRING does not. Every clone, every worktree, every
# re-clone after a move starts unarmed, and nothing announces it.
#
# ---------------------------------------------------------------------------
# WHY AN ABSOLUTE core.hooksPath IS FRAGILE -- three failure modes, measured.
# ---------------------------------------------------------------------------
#
# This repo was previously armed with an ABSOLUTE hooksPath pointing at one
# checkout's `.githooks`. That works until it silently does not:
#
# 1. THE MOVE IS SILENT ON EVERY CHANNEL. If the directory an absolute
#    hooksPath names stops existing -- the repo is renamed, moved, re-cloned to
#    another path, or checked out on another machine -- git DOES NOT WARN. It
#    does not exit nonzero. It does not print to stderr. It just runs no hooks.
#    Measured in a throwaway repo: with the hooks directory present the hook
#    fired and refused the commit (exit 1); with the SAME config and the
#    directory renamed away, the identical commit SUCCEEDED at exit 0 with zero
#    bytes of diagnostic, and the commit landed.
#    => The failure mode of a missing hooksPath is indistinguishable from
#       "the hook ran and approved". An absent control reads exactly like a
#       passing one, which is the whole reason this script verifies by OUTCOME
#       instead of trusting that the config was written.
#
# 2. IT DEFEATS LINKED WORKTREES, WHICH IS WORSE THAN IT SOUNDS. Worktrees
#    share one config, so an absolute hooksPath aims EVERY worktree at ONE
#    checkout's copy of the hook. A branch that edits `.githooks/commit-msg` is
#    then never validated by its own edit -- its commits are checked by whatever
#    version happens to be sitting in the main checkout. Measured: with a
#    RELATIVE hooksPath a linked worktree ran its OWN hook; with an absolute one
#    it ran the main checkout's.
#    => A hook change cannot be tested by the branch that makes it.
#
# 3. IT IS NOT SHAREABLE. An absolute path is true for exactly one filesystem.
#    It cannot be documented, copied into a README, or handed to a second
#    machine without being wrong.
#
# A RELATIVE hooksPath fixes all three: git resolves it against the top level of
# the working tree, so it follows the tree through moves, re-clones and
# worktrees. (Verified: from a subdirectory git reports the hooks path as
# `../.githooks` and the hook still fires -- resolution is anchored to the tree
# root, not to the shell's current directory.)
#
# ---------------------------------------------------------------------------
# USAGE
# ---------------------------------------------------------------------------
#   tools/arm-hooks.sh            arm (write the config), then verify
#   tools/arm-hooks.sh --verify   verify only -- WRITES NOTHING
#   tools/arm-hooks.sh --help     this header
#
# Exit 0 only if the hook is armed AND proven live. Any doubt exits nonzero
# with the reason on stderr.

set -euo pipefail

readonly HOOKS_DIR_REL=".githooks"
readonly HOOK_NAME="commit-msg"

MODE="arm"
case "${1-}" in
    --verify) MODE="verify" ;;
    --help | -h)
        grep '^#' "$0" | sed 's/^# \{0,1\}//'
        exit 0
        ;;
    "") ;;
    *)
        echo "arm-hooks: unknown argument: $1 (try --help)" >&2
        exit 2
        ;;
esac

die() {
    echo "" >&2
    echo "arm-hooks: FAILED -- $1" >&2
    shift
    for line in "$@"; do
        echo "  $line" >&2
    done
    echo "" >&2
    exit 1
}

# ---- 0. We must be inside a working tree. A relative hooksPath is meaningless
#         in a bare repo, so refuse rather than write a config that cannot work.
if [ "$(git rev-parse --is-inside-work-tree 2>&1)" != "true" ]; then
    die "not inside a git working tree" \
        "A relative core.hooksPath resolves against the working tree root," \
        "so there is nothing for it to resolve against here."
fi

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

# ---- 1. ARM. Skipped entirely under --verify.
if [ "$MODE" = "arm" ]; then
    echo "arm-hooks: setting core.hooksPath to $HOOKS_DIR_REL (relative, in-repo)"
    git config --local core.hooksPath "$HOOKS_DIR_REL"
fi

# ---- 2. VERIFY BY OUTCOME. Everything below asks git and the filesystem what
#         is actually true. None of it trusts step 1 to have worked, because the
#         failure this script exists to prevent is precisely a config that looks
#         set and resolves to nothing.

configured="$(git config --get core.hooksPath || true)"
if [ -z "$configured" ]; then
    die "core.hooksPath is not set" \
        "Run this script with no arguments to set it."
fi

# 2a. The value must be RELATIVE. An absolute path that happens to exist on this
#     machine today is the exact state this script was written to retire, so it
#     is a failure even though the hook would currently fire.
case "$configured" in
    /*)
        die "core.hooksPath is ABSOLUTE: $configured" \
            "It names one filesystem and breaks silently on any move, re-clone" \
            "or second machine, and it aims every linked worktree at one" \
            "checkout. Re-run this script with no arguments to make it relative."
        ;;
esac

# 2b. Ask GIT to resolve it -- not our own string arithmetic. This is the same
#     resolution git itself will use when it goes looking for a hook.
resolved="$(git rev-parse --git-path hooks)"
if [ ! -d "$resolved" ]; then
    die "git resolves core.hooksPath to a directory that does not exist: $resolved" \
        "This is the silent-no-hooks state: git would run no hooks at all and" \
        "would report nothing while doing it."
fi

# 2c. The resolved directory must be THE ONE IN THIS TREE. Compared as physical
#     directories, not as strings, so symlinks and `..` cannot fake a match.
resolved_phys="$(cd "$resolved" && pwd -P)"
expected_phys="$(cd "$repo_root/$HOOKS_DIR_REL" 2>/dev/null && pwd -P || true)"
if [ -z "$expected_phys" ]; then
    die "the in-repo hooks directory is missing: $repo_root/$HOOKS_DIR_REL" \
        "It is tracked -- a checkout that lacks it is incomplete."
fi
if [ "$resolved_phys" != "$expected_phys" ]; then
    die "core.hooksPath resolves outside this tree" \
        "resolved: $resolved_phys" \
        "expected: $expected_phys" \
        "A per-worktree override (extensions.worktreeConfig) can do this." \
        "Check: git config --show-origin --get-all core.hooksPath"
fi

# 2d. The hook file must exist, be a regular file, and be EXECUTABLE. git skips
#     a non-executable hook in silence, which lands in the same failure class as
#     a missing directory.
hook_path="$resolved_phys/$HOOK_NAME"
if [ ! -f "$hook_path" ]; then
    die "hook file missing: $hook_path"
fi
if [ ! -x "$hook_path" ]; then
    die "hook file is NOT EXECUTABLE: $hook_path" \
        "git skips a non-executable hook without saying so." \
        "Fix: chmod +x $HOOKS_DIR_REL/$HOOK_NAME && git update-index --chmod=+x $HOOKS_DIR_REL/$HOOK_NAME"
fi

# ---- 3. PROVE THE HOOK DISCRIMINATES, BOTH WAYS. An executable file at the
#         right path is still only a claim that the control works. Run it on a
#         sample it MUST refuse and a sample it MUST accept. A hook that passes
#         everything and a hook that refuses everything are both broken, and
#         only a two-way probe can tell either from a working one.
probe_dir="$(mktemp -d)"
trap 'rm -rf "$probe_dir"' EXIT

# Must be REFUSED: a doubled space mid-line, the scar a shell substitution
# leaves when it deletes a backticked word.
printf 'probe  scar\n' > "$probe_dir/bad"
if "$hook_path" "$probe_dir/bad" > "$probe_dir/bad.out" 2>&1; then
    die "the hook ACCEPTED a message it must refuse" \
        "Sample: 'probe  scar' (doubled space mid-line)." \
        "The hook is present but not discriminating -- it would pass anything."
fi

# Must be ACCEPTED: ordinary prose, no scar, no spliced path or timestamp.
printf 'probe clean line\n' > "$probe_dir/good"
if ! "$hook_path" "$probe_dir/good" > "$probe_dir/good.out" 2>&1; then
    die "the hook REFUSED a message it must accept" \
        "Sample: 'probe clean line'." \
        "A control that refuses everything gets bypassed with --no-verify on" \
        "reflex, which is worse than no control. Output follows:" \
        "$(cat "$probe_dir/good.out")"
fi

echo "arm-hooks: OK"
echo "  core.hooksPath = $configured (relative)"
echo "  resolves to    = $resolved_phys"
echo "  hook           = $HOOK_NAME (executable, refuses a scar, accepts clean prose)"
