#!/bin/bash
# 0.5.1 STRANGER-INSTALL WALK — P1-P6 positive, N1-N6 negative (each must FAIL CLOSED).
# Fresh CARGO_HOME, installs from crates.io only. Never touches the registry write-side.
# Machine: Annabel's box (Annabels-MacBook-Pro) | Operator: Mercury Toast (5b70322e-e7a9-451c-91ca-a3dfa7b05bda)
set -u
WALK=/private/tmp/claude-501/-Users-annabel-Developer-ablative-stack-liminal/5b70322e-e7a9-451c-91ca-a3dfa7b05bd9/scratchpad/stranger-walk
REPO=/Users/annabel/Developer/ablative/stack/liminal
TAGWT=$REPO/.worktrees/publish-0.5.1
CLAIM=/tmp/ablative-gate-battery.claim
MEMBER=5b70322e-e7a9-451c-91ca-a3dfa7b05bda
rm -rf "$WALK"; mkdir -p "$WALK/evidence"
EV="$WALK/evidence"
LEDGER="$EV/ledger.txt"
export CARGO_HOME="$WALK/cargo-home"
unset AMP_ITERS AMP_PEERS AMP_BURNERS CONFORMANCE_RESULTS_DIR RUST_LOG CARGO_TARGET_DIR
for v in $(env | sed -n 's/^\(LIMINAL_[^=]*\)=.*/\1/p'); do unset "$v"; done
note() { echo "$(date -u +%H:%M:%SZ) $*" | tee -a "$LEDGER"; }
PASS=0; FAILED=0
verdict() { # $1=id $2=PASS|FAIL $3=detail
  if [ "$2" = PASS ]; then PASS=$((PASS+1)); else FAILED=$((FAILED+1)); fi
  note "[$1] $2 — $3"
}

note "=== STRANGER WALK START | fresh CARGO_HOME=$CARGO_HOME ==="

# ---- claim (rule 2): P1/P2 compile ----
n=0
while :; do
  if ( set -o noclobber; printf 'seat=Mercury Toast\nmember_id=%s\npid=%s\nstarted_at=%s\nphase=running\n' "$MEMBER" "$$" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$CLAIM" ) 2>/dev/null; then
    trap 'if [ "$(sed -n "s/^pid[[:space:]]*[=:][[:space:]]*//p" "$CLAIM" 2>/dev/null)" = "$$" ]; then rm -f "$CLAIM"; note "claim released (own claim, pid $$)"; elif [ -f "$CLAIM" ]; then note "RELEASE: claim NOT ours — left in place (VOIDING signal per A5)"; else note "RELEASE FOUND NO CLAIM — taken mid-run; VOIDING signal per A5"; fi' EXIT INT TERM HUP
    note "claim acquired pid=$$"
    break
  fi
  hp="$(sed -n 's/^pid[[:space:]]*[=:][[:space:]]*//p' "$CLAIM" 2>/dev/null)"
  case "$hp" in (''|*[!0-9]*) note "holder unparseable -> HELD; waiting" ;; (*)
    if ps -p "$hp" >/dev/null 2>&1; then note "live holder pid=$hp; yielding"; else
      note "STALE CLAIM (pid $hp dead) — recording verbatim then clearing"; cat "$CLAIM" >> "$EV/stale-claim.txt"; rm -f "$CLAIM"; continue; fi ;;
  esac
  n=$((n+1)); [ $n -ge 80 ] && { note "claim wait ceiling — ABORT"; exit 4; }
  sleep 15
done

# ---- P1: library consumer from the registry ----
mkdir -p "$WALK/consumer/src"
cat > "$WALK/consumer/Cargo.toml" <<'EOF'
[package]
name = "stranger-consumer"
version = "0.0.1"
edition = "2021"

[dependencies]
liminal = { package = "liminal-rs", version = "0.5.1" }
EOF
cat > "$WALK/consumer/src/lib.rs" <<'EOF'
#[allow(unused_imports)]
use liminal::channel::ChannelHandle;
#[allow(unused_imports)]
use liminal::conversation::ConversationHandle;
EOF
if (cd "$WALK/consumer" && cargo build --quiet) > "$EV/p1-build.log" 2>&1; then
  verdict P1 PASS "README package-alias dep + both use lines compile from crates.io"
else verdict P1 FAIL "consumer build failed; see p1-build.log"; fi

# ---- P2: cargo install from registry ----
if cargo install liminal-server@0.5.1 --root "$WALK/installroot" --quiet > "$EV/p2-install.log" 2>&1; then
  verdict P2 PASS "cargo install liminal-server@0.5.1 from registry"
else verdict P2 FAIL "install failed; see p2-install.log"; fi
BIN="$WALK/installroot/bin/liminal-server"

# force-fetch sdk .crate for P4 coverage of all three
mkdir -p "$WALK/sdkfetch/src"; echo 'fn main(){}' > "$WALK/sdkfetch/src/main.rs"
printf '[package]\nname="sdkfetch"\nversion="0.0.1"\nedition="2021"\n[dependencies]\nliminal-sdk = "=0.5.1"\n' > "$WALK/sdkfetch/Cargo.toml"
(cd "$WALK/sdkfetch" && cargo fetch --quiet) >> "$EV/p2-install.log" 2>&1 || note "sdk fetch failed (P4 sdk coverage reduced)"

# ---- release claim before the non-compiling remainder ----
if [ "$(sed -n 's/^pid[[:space:]]*[=:][[:space:]]*//p' "$CLAIM" 2>/dev/null)" = "$$" ]; then rm -f "$CLAIM"; note "claim released early (compiles done, pid $$)"; trap - EXIT INT TERM HUP; else note "EARLY RELEASE: claim state unexpected — $(cat "$CLAIM" 2>/dev/null || echo ABSENT)"; fi

# ---- P3: unpack the FETCHED .crate ----
mkdir -p "$WALK/unpack"
for c in liminal-rs liminal-sdk liminal-server; do
  CR=$(find "$CARGO_HOME/registry/cache" -name "$c-0.5.1.crate" | head -1)
  if [ -n "$CR" ]; then tar xzf "$CR" -C "$WALK/unpack"; note "unpacked $c from $(basename "$CR")"; else note "no cached .crate for $c"; fi
done
SV="$WALK/unpack/liminal-server-0.5.1"
if [ -d "$SV/src" ] && ls "$SV/src"/*.rs >/dev/null 2>&1 || [ -f "$SV/src/main.rs" ]; then
  A1="absent"; A2="absent"
  [ -e "$SV/config/liminal.example.toml" ] && A1="PRESENT"
  [ -e "$SV/rust-toolchain.toml" ] && A2="PRESENT"
  if [ "$A1" = absent ] && [ "$A2" = absent ]; then
    verdict P3 PASS "server sources present; config/liminal.example.toml ABSENT and rust-toolchain.toml ABSENT (ruled+disclosed; asserted positively; NOT a stop)"
  else verdict P3 FAIL "shipping state changed: example.toml=$A1 toolchain=$A2 — a future-release visibility tripwire fired"; fi
else verdict P3 FAIL "server .crate sources missing"; fi

# ---- P4: published bytes vs tag b250da9 ----
P4BAD=0
for c in liminal-rs:liminal liminal-sdk:liminal-sdk liminal-server:liminal-server; do
  pkg="${c%%:*}"; dir="${c#*:}"
  U="$WALK/unpack/$pkg-0.5.1"; T="$TAGWT/crates/$dir"
  [ -d "$U" ] || { note "P4: $pkg not unpacked, skipped"; continue; }
  diff -r "$U" "$T" \
    --exclude Cargo.toml --exclude Cargo.toml.orig --exclude .cargo_vcs_info.json \
    --exclude Cargo.lock --exclude .cargo-ok \
    > "$EV/p4-diff-$pkg.txt" 2>&1
  rc=$?
  if [ $rc -eq 0 ]; then note "P4: $pkg == tag bytes (outside cargo-generated files)"
  else
    if grep -qE '\.rs' "$EV/p4-diff-$pkg.txt"; then P4BAD=1; note "P4: $pkg HAS .rs DIFFERENCES — STOP CLASS; see p4-diff-$pkg.txt"
    else note "P4: $pkg non-rs differences only; see p4-diff-$pkg.txt"; fi
  fi
done
[ $P4BAD -eq 0 ] && verdict P4 PASS "published sources byte-match tag b250da9 across all unpacked crates (cargo-generated files excluded)" || verdict P4 FAIL ".rs difference between published crate and tag — STOP: loud report, no registry action from this seat"

# ---- P5: stranger's real path — repo example config boots the installed binary ----
git -C "$REPO" show b250da9:config/liminal.example.toml > "$WALK/liminal.example.toml" 2>>"$LEDGER"
"$BIN" --config "$WALK/liminal.example.toml" > "$EV/p5-stdout.log" 2> "$EV/p5-stderr.log" &
SRV=$!
BOOT=fail
for i in $(seq 1 40); do
  if curl -m 2 -sS -o /dev/null -w '%{http_code}' http://127.0.0.1:8081/health 2>/dev/null | grep -q 200; then BOOT=ok; break; fi
  kill -0 $SRV 2>/dev/null || break
  sleep 0.5
done
if [ "$BOOT" = ok ]; then verdict P5 PASS "installed binary boots against the repo's example config (the CHANGELOG's stranger path)"
else verdict P5 FAIL "server did not become healthy on example config; see p5-stderr.log"; fi

# ---- P6: health surface + startup logging (on the P5 server) ----
if [ "$BOOT" = ok ]; then
  H=$(curl -m 3 -sS http://127.0.0.1:8081/health); HC=$(curl -m 3 -sS -o /dev/null -w '%{http_code}' http://127.0.0.1:8081/health)
  R=$(curl -m 3 -sS http://127.0.0.1:8081/ready); RC=$(curl -m 3 -sS -o /dev/null -w '%{http_code}' http://127.0.0.1:8081/ready)
  MC=$(curl -m 3 -sS -o /dev/null -w '%{http_code}' http://127.0.0.1:8081/metrics)
  MT=$(curl -m 3 -sS -o /dev/null -w '%{content_type}' http://127.0.0.1:8081/metrics)
  NC=$(curl -m 3 -sS -o /dev/null -w '%{http_code}' http://127.0.0.1:8081/nope)
  PC=$(curl -m 3 -sS -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:8081/health)
  echo "health=$HC:$H ready=$RC:$R metrics=$MC:$MT other=$NC post=$PC" > "$EV/p6-routes.txt"
  OK=1
  [ "$HC" = 200 ] && echo "$H" | grep -q '"status":"healthy"' || OK=0
  [ "$RC" = 200 ] && echo "$R" | grep -q '"ready":true' || OK=0
  [ "$MC" = 200 ] || OK=0
  echo "$MT" | grep -q 'text/plain' && echo "$MT" | grep -q 'version=0.0.4' || OK=0
  [ "$NC" = 404 ] || OK=0
  [ "$PC" = 405 ] || OK=0
  if [ -s "$EV/p5-stderr.log" ]; then LOGOK=1; else LOGOK=0; fi
  if [ $OK -eq 1 ] && [ $LOGOK -eq 1 ]; then
    verdict P6 PASS "routes 200/200/200(text/plain;version=0.0.4)/404/405 and server LOGS to stderr on startup under default filter"
  else verdict P6 FAIL "routes OK=$OK stderr-nonempty=$LOGOK; see p6-routes.txt / p5-stderr.log"; fi
  kill $SRV 2>/dev/null; wait $SRV 2>/dev/null
else verdict P6 FAIL "no healthy server to probe"; kill $SRV 2>/dev/null; fi

# ---- negative controls: each MUST fail closed ----
mkneg() { python3 - "$1" "$2" <<'PYEOF'
import sys,re
src=open('/private/tmp/claude-501/-Users-annabel-Developer-ablative-stack-liminal/5b70322e-e7a9-451c-91ca-a3dfa7b05bd9/scratchpad/stranger-walk/liminal.example.toml').read()
mode=sys.argv[1]; out=sys.argv[2]
if mode=='unknown': src += '\nnonsense_key_nobody_declared = 1\n'
elif mode=='persist': src += '\npersistence_path = "/private/tmp/claude-501/-Users-annabel-Developer-ablative-stack-liminal/5b70322e-e7a9-451c-91ca-a3dfa7b05bd9/scratchpad/stranger-walk/does-not-exist/nested"\n'
elif mode=='sameport': src = src.replace('health_listen_address = "127.0.0.1:8081"','health_listen_address = "127.0.0.1:8080"')
elif mode.startswith('omit:'):
    key=mode.split(':',1)[1]
    if key=='channels': src=re.sub(r'(?m)^\[\[channels\]\]$','[[channels_disabled]]',src)
    elif key=='routing_rules': src=re.sub(r'(?m)^\[\[routing_rules\]\]$','[[routing_rules_disabled]]',src)
    else: src=re.sub(r'(?m)^'+key+r' = .*$','',src)
open(out,'w').write(src)
PYEOF
}
negrun() { # $1=id $2=desc $3=configfile-or-NONE $4=extra-env-RUSTLOG(0/1)
  local id="$1" desc="$2" cfg="$3"
  local out="$EV/$id.log"
  if [ "$cfg" = NONE ]; then "$BIN" > "$out" 2>&1 & local p=$!
  else "$BIN" --config "$cfg" > "$out" 2>&1 & local p=$!; fi
  local alive=0; sleep 3; kill -0 $p 2>/dev/null && alive=1
  if [ $alive -eq 1 ]; then kill $p 2>/dev/null; wait $p 2>/dev/null; verdict "$id" FAIL "$desc — server STAYED UP (control passed when it must fail)"
  else wait $p 2>/dev/null; verdict "$id" PASS "$desc — failed closed (log: $id.log)"; fi
}
negrun N1 "no --config" NONE
mkneg unknown "$WALK/n2.toml"; negrun N2 "unknown key = startup error" "$WALK/n2.toml"
mkneg persist "$WALK/n3.toml"; negrun N3 "persistence_path at nonexistent dir" "$WALK/n3.toml"
if [ -e "$WALK/does-not-exist" ]; then verdict N3b FAIL "server CREATED the missing directory"; else note "N3b: directory NOT created (correct)"; grep -l "unreachable" "$EV/N3.log" >/dev/null 2>&1 && note "N3 message contains 'unreachable'" || note "N3 message text: $(tail -1 "$EV/N3.log" 2>/dev/null | head -c 120)"; fi
mkneg sameport "$WALK/n4.toml"; negrun N4 "listen == health_listen" "$WALK/n4.toml"
# N5: valid config, RUST_LOG empty string -> TOTAL SILENCE (server runs; stderr must stay empty)
RUST_LOG="" "$BIN" --config "$WALK/liminal.example.toml" > "$EV/N5.stdout.log" 2> "$EV/N5.stderr.log" &
N5P=$!
sleep 4
if kill -0 $N5P 2>/dev/null; then
  if [ -s "$EV/N5.stderr.log" ] || [ -s "$EV/N5.stdout.log" ]; then verdict N5 FAIL "RUST_LOG=\"\" produced OUTPUT (must be total silence)"; else verdict N5 PASS "RUST_LOG=\"\" = total silence including startup events"; fi
  kill $N5P 2>/dev/null; wait $N5P 2>/dev/null
else
  verdict N5 FAIL "server died under RUST_LOG=\"\" (see N5 logs)"
fi
for key in listen_address health_listen_address drain_timeout_ms channels routing_rules; do
  mkneg "omit:$key" "$WALK/n6-$key.toml"; negrun "N6-$key" "mandatory key $key omitted" "$WALK/n6-$key.toml"
done

note "=== WALK COMPLETE: PASS=$PASS FAIL=$FAILED ==="
exit 0
