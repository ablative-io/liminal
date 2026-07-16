# HANDOFF — liminal participant-lifecycle delivery

Written 2026-07-16 by Waffles (Claude/Fable seat, usage-constrained). This
document is self-contained: a fresh model at any provider, with access to this
repository, can continue the delivery from exactly here. Read this file fully
before doing anything. The four companion files it references live in this
same directory (`docs/design/` in the liminal repo,
`/Users/tom/Developer/ablative/liminal`).

## What this project is (60 seconds)

Liminal is a messaging bus. The conversation participant lifecycle — attach,
detach, leave, crash, recovery, under three guarantees (bounded memory,
exactly-once effects across crashes/retries, zero polling) — was designed in a
6,331-line frozen document: `docs/design/PARTICIPANT-CONTRACT.md` at commit
`55856ae3c53206f9c662e6815650dfc67a89ce85`. That document is READ-ONLY source
material. Its core algebra and wire register were verified sound by multiple
independent examinations; two confirmed defects were found and their fixes are
MANDATED as design constraints (below). The delivery decision: the rules move
into a shared Rust crate, `crates/liminal-protocol`, consumed by both
`crates/liminal-server` and `crates/liminal-sdk`, so the two sides cannot
drift and correctness is enforced by the compiler and tests, not by document
review. NO new design documents are to be authored. NO open-ended review
loops: gates are `cargo build` / `cargo clippy -- -D warnings` / `cargo test`,
plus at most TWO rounds of independent review per checkpoint, then a human
ruling.

## The plan of record (four files, this directory)

1. `LP-ROADMAP.md` — master plan: phases, review protocol (with an embedded
   ready-to-use review prompt), gate commands, contingencies, risks. THE
   authority on process questions.
2. `LP-EXTRACTION-GOAL.md` — phase A input (the crate). Contains the two
   mandated design fixes with exact type shapes.
3. `LP-SERVER-BINDING-GOAL.md` — phase B input (server binds the crate).
   Discovery-first, with a hard stop if the server's existing wire format
   conflicts with the contract register.
4. `LP-SDK-GOAL.md` — phase C input (SDK receive side + release prep; publish
   and tags are human-gated, never done by a work agent).

## Exact current state (verified 2026-07-16)

- **Phase A checkpoint 1 is BUILT and gate-verified.** Worktree
  `.worktrees/liminal-protocol`, branch `feature/liminal-protocol`, commit
  `83996e73388b807632330fcc5f063b8d1e26d459`, worktree clean. Six commits; the
  brief is the first (`a240510`). Modules: `algebra`, `lifecycle`, `outcome`,
  `wire`.
- **Gates all green, run independently by Waffles on that exact commit**
  (never trust a work agent's own pass-claim — rerun on final bytes): build
  exit 0; `cargo clippy -p liminal-protocol --all-targets -- -D warnings`
  exit 0 after a fresh `cargo clean -p liminal-protocol` (the
  `Checking liminal-protocol v0.1.0` recompile line was captured — cached
  greens don't count); tests 127/127. The `Terminalized` detach-cell variant
  is present across `lifecycle/{attach,detach,lookup}.rs` and
  `lifecycle/cursor_facts_tests.rs` exists.
- **Checkpoint 1 independent review is IN FLIGHT.** A first Sol (gpt-5.6)
  review dispatched 2026-07-15 20:42Z was killed by mistake ~28 minutes in (a
  timestamp misread — comparable reviews complete in ~20–30 min). A
  replacement with identical scope was dispatched 21:17Z (norn session
  `claude-review-lp-cp1-r2-20260715T211739Z`, envelope in
  `~/.norn/delegations/`). If its envelope contains a verdict, use those
  findings. If it produced nothing, run the review per the prompt below —
  that prompt is the complete task, no other context needed.
- Phase A2 (the 56 acceptance-case tests, algebra fixtures, property tests)
  has NOT started — it is gated on checkpoint-1 review per the brief.
- Phases B and C have not started.
- Nothing from this delivery is merged to `main` yet.

## Immediate next task: checkpoint-1 correctness review

Perform (or dispatch to a strong model) an adversarial correctness review of
`crates/liminal-protocol` at `83996e7`, read-only. The full review
prompt — scope, attack order, verdict rules, what is and is not a finding — is
embedded in `LP-ROADMAP.md` under "Review protocol", and a checkpoint-1
specific variant is preserved verbatim below so nothing depends on any other
tool:

- Phase-2 material (56 cases) being absent is NOT a finding.
- Attack order: (1) detach cell — `TerminalizedDetachCell` constructible ONLY
  from the `Terminalized` variant; attach transitions `Committed→Terminalized`
  never `→Empty`; token lookup order matches the frozen doc ~1930–1965 with
  the Terminalized arm added. (2) per-participant cursor accounting — build
  two-participant same-suffix ack histories; every legal ack must serialize,
  cursors never regress; check the shipped blocker test encodes the real
  overflow history (nonzero-debt episode, observer held, both members stepping
  the same suffix), not a weaker cousin. (3) algebra vs doc ~2884–2930 —
  recompute `B = S + ((I−C)×marker_max)`, `d' = max(0, B'+Q+K'−cap)` with
  `d'<=Q` as a refusal not a clamp, floor rule
  `F' = max(F, preferred_floor, cap_floor)` with `m = H'` on empty membership,
  u128 widening in the doc's canonical order, using the doc's own fixture
  numbers. (4) wire register vs the doc's R-D1 tables — 8 contiguous client
  requests, 37 contiguous server values `0x0100..0x0124`, 2 pushes, exact
  field lists, no `Option` where the doc declares required fields, the
  ten-field `sequence_budget` on every `ConversationSequenceExhausted`.
  (5) the seven stored-edge transitions vs the doc's completion-orderings
  table ~2933–2941 — every named outcome constructible only from states
  carrying its data; intended dead ends (`DetachedMarkerRelease`,
  `DetachedCursorRelease`) typed with sole successors; ack selector
  thresholds per doc ~2291–2300. (6) test honesty — tests assert document
  values, not implementation echoes; no `unwrap`/`expect`/`panic` outside
  `#[cfg(test)]`.
- Verdict `ready | ready_with_comments | not_ready`, findings as concrete fix
  items (file, line, what, why), every equation recomputed by the reviewer.

Findings go to the building agent (the goal session that built the crate, or
any competent replacement) as fix items; gates rerun after fixes; maximum two
review rounds, then escalate to Tom. On `ready`/`ready_with_comments`-with-
fixes-applied: phase A2 proceeds per `LP-EXTRACTION-GOAL.md`, and phase B may
start in parallel per `LP-ROADMAP.md`.

## How to verify anything (the gates)

```bash
cd /Users/tom/Developer/ablative/liminal/.worktrees/liminal-protocol
export CARGO_TARGET_DIR=/Users/tom/Developer/ablative/liminal/target
git rev-parse HEAD           # must equal the declared commit
git status --porcelain       # must be empty
cargo build -p liminal-protocol
cargo clean -p liminal-protocol && cargo clippy -p liminal-protocol --all-targets -- -D warnings
cargo test -p liminal-protocol
```

The `cargo clean` before clippy matters: cargo's freshness is content-based,
so a cached green proves nothing about the current lint posture; a fresh
`Checking liminal-protocol` line is the proof.

## The two mandated design fixes (deviations from the frozen doc — the doc is WRONG here)

1. **Detach cell has FOUR variants** (`Empty | Pending | Committed |
   Terminalized`). The doc's three-variant cell plus "attach clears the cell"
   makes its own promised post-attach `TerminalizedDetachCell` replay response
   (old `committed_binding_epoch` included) unproducible. Attach transitions
   `Committed → Terminalized`; the response type is constructible only from
   `Terminalized`. Full shape in `LP-EXTRACTION-GOAL.md`.
2. **No fixed occurrence array.** The doc's R18 `successor_milestones` /
   `R_max`/`E_max`/`O_max` slot machinery (~2560–2715) under-reserves
   participant-scoped cursor facts (two members acking the same suffix
   legally need more facts than it holds — confirmed by construction). It is
   FORBIDDEN to transcribe. Cursor-progress facts are keyed
   `(participant_index, boundary)`, accounted per participant; ordering
   coverage is carried by typed transitions + property tests.

If any review disputes a mandated fix, the mandate wins unless the finding
shows the mandate itself broken — in that case STOP and get a human ruling.

## Standing rules (non-negotiable, from the project owner)

- No new design/prose documents. Deliverables are code, tests, doc-comments,
  and declarations (text in the final message/commit, not files).
- Never touch `main` or switch branches on the main checkout; all work in
  `.worktrees/<name>` on feature branches; `CARGO_TARGET_DIR` always set to
  the repo's own `target/` — never build in temp directories.
- Commit + push after every completed unit. First commit of any new phase
  branch is that phase's brief file.
- No `unsafe`; no `unwrap`/`expect`/`panic` outside test code; clippy
  `-D warnings` always.
- Never re-implement a rule the crate owns in server or SDK code; gaps are
  reported, not patched around. No silent fallbacks anywhere — fail loudly.
- No polling (LAW-1). Never add a polling loop, timer-poll, or busy-wait.
- Publish, tags, and merges to main are human-gated (Tom, or Waffles when
  available).
- Review loops are capped at two rounds per checkpoint. Work agents never
  self-certify past a gate; verification is always rerun by the
  reviewer/lead on the declared commit's final bytes.

## History and deeper context (only if needed)

`LP-ROADMAP.md` carries the compressed reasoning for the document→crate pivot
and the risk register (server wire-compat cutover is the big unknown — phase
B's discovery stage exists for it and includes a mandatory stop-for-ruling).
The frozen contract document itself is the semantic authority for everything
not covered by the two mandated fixes. The git history of branch
`design/participant-contract` records the document's 18-round examination
history; it is background only — do not reopen it.
