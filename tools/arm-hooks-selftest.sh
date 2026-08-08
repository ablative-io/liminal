#!/usr/bin/env bash
#
# Self-test for tools/arm-hooks.sh.
#
# arm-hooks.sh exists to prove the commit-msg hook is armed. This proves
# arm-hooks.sh can tell armed from unarmed -- otherwise it is an instruction
# wearing a verifier's clothes, and a verifier that never returns nonzero is
# indistinguishable from one that works.
#
# Every case runs in a THROWAWAY repo under mktemp. This script NEVER touches
# the configuration of the repo it is run from: the only `git config` writes it
# makes are inside the temporary clone it creates and deletes.
#
#   tools/arm-hooks-selftest.sh
#
# Exit 0 if every case behaved as specified, nonzero otherwise.

set -uo pipefail

repo_root="$(git rev-parse --show-toplevel)"
readonly repo_root

work="$(mktemp -d)"
readonly work
trap 'rm -rf "$work"' EXIT

readonly R="$work/clone"
readonly SAVE="$work/save"
mkdir -p "$SAVE"

pass=0
fail=0
check() { # label expected actual
    if [ "$2" = "$3" ]; then
        echo "  PASS  $1 (exit $3)"
        pass=$((pass + 1))
    else
        echo "  FAIL  $1 (expected $2, got $3)"
        fail=$((fail + 1))
    fi
}

fresh() {
    rm -rf "$R"
    mkdir -p "$R/tools"
    git init -q "$R"
    git -C "$R" config user.email selftest@example.invalid
    git -C "$R" config user.name selftest
    cp -R "$repo_root/.githooks" "$R/.githooks"
    cp "$repo_root/tools/arm-hooks.sh" "$R/tools/arm-hooks.sh"
    chmod +x "$R/tools/arm-hooks.sh"
    git -C "$R" add -A
    git -C "$R" commit -q --no-verify -m "seed"
}

echo "== 1. arm a clean clone =="
fresh
(cd "$R" && ./tools/arm-hooks.sh > /dev/null)
check "arm succeeds" 0 $?
check "value is relative" ".githooks" "$(git -C "$R" config --get core.hooksPath)"

echo "== 2. --verify is read-only =="
before="$(git -C "$R" config --get core.hooksPath)"
(cd "$R" && ./tools/arm-hooks.sh --verify > /dev/null)
check "verify succeeds" 0 $?
check "verify wrote nothing" "$before" "$(git -C "$R" config --get core.hooksPath)"

echo "== 3. unset config is caught =="
git -C "$R" config --unset core.hooksPath
(cd "$R" && ./tools/arm-hooks.sh --verify > /dev/null 2>&1)
check "unset detected" 1 $?

echo "== 4. an ABSOLUTE path that EXISTS is still refused =="
git -C "$R" config core.hooksPath "$R/.githooks"
(cd "$R" && ./tools/arm-hooks.sh --verify > /dev/null 2>&1)
check "absolute refused even though the hook would fire" 1 $?

echo "== 5. the silent state: relative value, directory gone =="
git -C "$R" config core.hooksPath .githooks
mv "$R/.githooks" "$R/.githooks-moved"
(cd "$R" && ./tools/arm-hooks.sh --verify > /dev/null 2>&1)
check "missing hooks directory detected" 1 $?
mv "$R/.githooks-moved" "$R/.githooks"

echo "== 6. a non-executable hook is caught =="
chmod -x "$R/.githooks/commit-msg"
(cd "$R" && ./tools/arm-hooks.sh --verify > /dev/null 2>&1)
check "non-executable detected" 1 $?
chmod +x "$R/.githooks/commit-msg"

echo "== 7. both-ways discrimination on the hook itself =="
cp "$R/.githooks/commit-msg" "$SAVE/commit-msg"
printf '#!/bin/sh\nexit 0\n' > "$R/.githooks/commit-msg"
chmod +x "$R/.githooks/commit-msg"
(cd "$R" && ./tools/arm-hooks.sh --verify > /dev/null 2>&1)
check "a hook that accepts everything is caught" 1 $?
printf '#!/bin/sh\nexit 1\n' > "$R/.githooks/commit-msg"
chmod +x "$R/.githooks/commit-msg"
(cd "$R" && ./tools/arm-hooks.sh --verify > /dev/null 2>&1)
check "a hook that refuses everything is caught" 1 $?
cp "$SAVE/commit-msg" "$R/.githooks/commit-msg"
chmod +x "$R/.githooks/commit-msg"

echo "== 8. green again once restored =="
(cd "$R" && ./tools/arm-hooks.sh --verify > /dev/null)
check "restored clone verifies" 0 $?

echo "== 9. end to end: the armed hook gates a real commit =="
touch "$R/probe-file"
git -C "$R" add probe-file
printf 'selftest  scar\n' > "$work/msg-bad"
git -C "$R" commit --cleanup=verbatim -F "$work/msg-bad" > /dev/null 2>&1
check "a real commit carrying a scar is REFUSED" 1 $?
printf 'selftest clean subject\n' > "$work/msg-good"
git -C "$R" commit --cleanup=verbatim -F "$work/msg-good" > /dev/null 2>&1
check "a real clean commit is ACCEPTED" 0 $?

echo "== 10. resolution is anchored to the tree root, not the shell =="
mkdir -p "$R/deep/er"
(cd "$R/deep/er" && ../../tools/arm-hooks.sh --verify > /dev/null)
check "verify from a subdirectory" 0 $?

echo ""
echo "arm-hooks-selftest: pass=$pass fail=$fail"
[ "$fail" -eq 0 ]
