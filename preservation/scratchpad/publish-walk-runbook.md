# 0.5.1 PUBLISH WALK — STAGED, NOT AUTHORIZED
# HELD on: Waffles' funnel-release pointer (topic entry) or Tom's word with a readable entry id.
# Hermes's checklist c2c1c87a taken verbatim; STOP conditions binding throughout.
# Tree: tag liminal-v0.5.1 -> b250da998acaecd3d8dadae033c33e8e315fea89 (= main, verified, annotated tag fbc74ee).
# Step 0 probes DONE 21:38Z (read-only): protocol 0.3.2 live/unyanked; trio 0.5.1 ABSENT x3 (expected).
# Token check NOT yet done (no credential hunting before the pointer).

UA='ablative-stack-release-probe (annabel@ablative.com.au)'
WT=/Users/annabel/Developer/ablative/stack/liminal   # publish from main checkout AT THE TAG (verify rev first)

## STEP 0 (at authorization time)
# - git -C $WT rev-parse HEAD  == b250da9  (checkout tag if main moved)
# - cargo login state: attempt `cargo publish --dry-run -p liminal-rs --locked` will surface token absence at publish step;
#   check `test -f ~/.cargo/credentials.toml` — if absent: STOP, tell Hermes. No improvised credential path.
# - Claim preflight before ANY cargo invocation (dry-run compiles): /tmp/ablative-gate-battery.claim absent or hold.
#   Publishing compiles (verify builds) => Athena slot request first? Publish verify-builds are compiles on this box:
#   RULE: request Athena's word for the publish window (no launcher starts without her word).

## PER CRATE C in ORDER: liminal-rs -> liminal-sdk -> liminal-server
# 3a: cargo publish -p C --locked --dry-run          (claim-preflight first)
# 3b: cargo publish -p C --locked
# 3c: PROBE (never trust cargo's exit):
#     curl -sS -H "User-Agent: $UA" https://crates.io/api/v1/crates/C/0.5.1  -> num=0.5.1, yanked=false
#     cargo new --lib /tmp/probe-C && cd /tmp/probe-C
#     cargo add C@=0.5.1 && cargo generate-lockfile
#     grep -A1 'name = "C"' Cargo.lock  -> version = "0.5.1"
# 3d: NEGATIVE: cargo add C@=0.5.2 MUST FAIL. If it succeeds: STOP CHAIN.
# Each crate fully confirmed live before the next begins. sdk needs rs live; server needs rs+sdk live.

## STRANGER-INSTALL (part of "live"): fresh CARGO_HOME=$(mktemp -d)
# POSITIVE:
#  - README:19 snippet verbatim: liminal = { package = "liminal-rs", version = "0.5.1" } + the two use lines -> compiles
#  - cargo install liminal-server@0.5.1 -> succeeds
#  - boot w/ config: /health 200, /ready 200, /metrics 200 text/plain version=0.0.4
#  - server LOGS to stderr on startup under default filter (silent server = Leg A cure did not ship -> STOP)
# TARBALL both directions: config/liminal.example.toml and rust-toolchain.toml ABSENT from published crate
#  (ruled + disclosed; assert positively; DO NOT STOP on it)
# NEGATIVE CONTROLS (each MUST fail; a pass = walk failure):
#  N1 no --config -> fail
#  N2 unknown key -> startup ERROR
#  N3 persistence_path nonexistent dir -> "path is unreachable", never created
#  N4 listen == health_listen -> fail
#  N5 RUST_LOG="" -> TOTAL silence incl. errors
#  N6 omit any of 5 mandatory keys -> fail
# STOP CONDITION: any positive fails or any negative passes -> report, stop, nothing registry-facing posted.
# Evidence -> TOPIC, plain text, no box-drawing. Registry line is HERMES's to post, never mine.
