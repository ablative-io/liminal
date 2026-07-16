# liminal-protocol — roadmap, reasoning, and review protocol

Written 2026-07-15 by Waffles. This is the master record for the participant-
lifecycle delivery, written so the arc can proceed even if my usage runs out.
Companion files: `LP-EXTRACTION-GOAL.md` (phase A launch input, exists),
`LP-SERVER-BINDING-GOAL.md` (phase B launch input), `LP-SDK-GOAL.md` (phase C
launch input).

## Why this shape (the reasoning, compressed)

The participant lifecycle was specified in `docs/design/PARTICIPANT-CONTRACT.md`
(frozen at `55856ae3c53206f9c662e6815650dfc67a89ce85`). Eighteen-plus rounds of
adversarial examination proved the core algebra and wire register sound but
kept finding defects in the document's own proof machinery — because prose has
no compiler. Final cold exam (2026-07-15) found two real blockers, both
hand-verified: (1) the R18 fixed occurrence array under-reserves participant-
scoped cursor facts (doc ~2560–2715); (2) a successful attach destroys the only
data the promised `TerminalizedDetachCell` replay response needs (doc
~1578–1634 vs ~4728–4752 and ~5671). Both defect classes are caught mechanically
by types and tests. Decision (Tom, 2026-07-15): the rules move into a shared
Rust crate — `crates/liminal-protocol` — consumed by both `liminal-server` and
`liminal-sdk`, so drift is impossible and correctness is enforced by cargo on
every commit, free, forever. The document stays frozen as source material. No
new design documents. No exam loops ever again — gates are compiler, clippy,
tests, plus one bounded review lens per checkpoint.

## Phase map

- **A — extraction** (goal agent, Tom launches; input `LP-EXTRACTION-GOAL.md`):
  `crates/liminal-protocol` on branch `feature/liminal-protocol`. Phase A1 =
  wire register + algebra + state machine + the two blocker regression tests →
  checkpoint pause. Phase A2 = the 56 acceptance cases as tests + algebra
  fixtures + property tests → final declaration.
- **B — server binding** (input `LP-SERVER-BINDING-GOAL.md`): `liminal-server`
  consumes the crate. Discovery first (map what the server currently does),
  then bind storage + transport to the typed state machine. Depends on A1
  reviewed; can start before A2 completes.
- **C — SDK + release** (input `LP-SDK-GOAL.md`): `liminal-sdk` receive-side
  completes against the crate. Release/publish/tags are LEAD-GATED — the goal
  agent prepares, never publishes.
- **D — later, out of scope now**: LAW-1 polling retirement in the server
  (classified families from the contract's sweep), aligned to the crate's
  no-polling transitions. Board item, not part of this delivery.

Order: A1 → review → (A2 ∥ B) → review B → C → release. A2 and B can run in
parallel because A2 only adds tests to the crate while B consumes its API; if
the A2 agent needs an API fix, B rebases — acceptable cost, sequential if it
gets noisy.

## Review protocol (bounded — this replaces exam loops)

At each declared checkpoint, exactly this, at most TWO rounds, then escalate to
Tom for a scope ruling rather than a third round:

1. **Gates** (anyone can run; owner: Waffles if available, else Tom):

```bash
cd /Users/tom/Developer/ablative/liminal/.worktrees/liminal-protocol
export CARGO_TARGET_DIR=/Users/tom/Developer/ablative/liminal/target
cargo build -p liminal-protocol
cargo clippy -p liminal-protocol --all-targets -- -D warnings
cargo test -p liminal-protocol
```

   All three must pass on the DECLARED commit (verify `git rev-parse HEAD`
   matches the declaration; worktree clean). Never accept the agent's
   pass-claim — rerun on final bytes.

2. **One Sol correctness lens** (read-only). Use the norn skill
   (`~/.claude/skills/norn`, mode `review`, preset `correctness`, model
   gpt-5.6-sol, effort `high` — xhigh only if a finding is disputed). The task
   prompt, ready to paste (fill `<COMMIT>`):

   > Adversarial correctness review of crates/liminal-protocol at commit
   > <COMMIT> (branch feature/liminal-protocol, worktree
   > .worktrees/liminal-protocol). This crate is extracted from the frozen
   > design document docs/design/PARTICIPANT-CONTRACT.md @ 55856ae with two
   > MANDATED deviations recorded in docs/design/LP-EXTRACTION-GOAL.md (a
   > fourth Terminalized detach-cell variant; per-participant cursor-progress
   > accounting replacing the document's fixed occurrence array). Attack, in
   > order: (1) the detach cell — verify TerminalizedDetachCell is
   > constructible ONLY from the Terminalized variant, attach transitions
   > Committed→Terminalized never →Empty, and the token lookup order matches
   > doc ~1930–1965 with the Terminalized arm added; (2) the per-participant
   > cursor accounting — construct two-participant same-suffix ack histories
   > and verify every legal ack serializes and cursors never regress; (3) the
   > algebra functions against doc ~2884–2930 — recompute B, d', floor rule,
   > u128 widening order, using the document's own fixture numbers; (4) the
   > wire register against the document's R-D1 tables — exact tags
   > (8 requests, 37 server values 0x0100..0x0124, 2 pushes), exact field
   > lists, no Option fields where the document declares required fields, the
   > ten-field sequence_budget; (5) the seven stored-edge transitions against
   > the completion-orderings table doc ~2933–2941 — every named outcome
   > constructible, only from states carrying its data; intended dead ends
   > (DMR, DCursor) typed with sole successors; (6) test honesty — tests
   > assert document values, not implementation echoes. Verdict ready /
   > ready_with_comments / not_ready with exact file:line findings and
   > recomputed arithmetic. Read-only; modify nothing.

   Findings go back to the building agent as concrete fix items (file, line,
   what, why). Re-run gates after fixes. That's one round.

3. **Merge** (Waffles's seat; if unavailable, Tom): squash-or-merge per repo
   convention, branch → main, after gates green on the final commit. Never
   force-push, never merge on a pass-claim.

## Phase A checkpoint specifics (what I would check by hand)

Hot spots from my exam knowledge, in priority order — whoever reviews, look
here first: the detach-cell transition table (the original defect site); the
two-participant ack test (must be the exact overflow history: nonzero debt,
observer held, both members stepping the same suffix); `d' <= Q` enforced as a
refusal not a clamp; floor rule applied on EVERY membership-changing
transition with `m = H'` on empty membership; wire discriminants byte-exact
(the doc's u16 values are normative); `AckGap`/`AckRegression`/`AckNoOp`
selector thresholds (doc ~2291–2300: `<` regression, `==` no-op, `>` committed
only when contiguously available); the `ConversationSequenceExhausted`
ten-field payload appearing everywhere that outcome appears.

## Contingency — if Waffles is out of usage

Everything above is runnable without me: Tom runs the three gate commands,
launches the Sol review with the embedded prompt via the norn skill, relays
findings to the goal agent verbatim, re-runs gates, merges on green. The two
things that need judgment if they arise: (a) a goal agent wanting to deviate
from a brief — the answer is no; it reports the conflict in its declaration
and the deviation gets ruled on explicitly; (b) a Sol finding disputing a
MANDATED deviation — the mandate wins unless the finding shows the mandate
itself broken, in which case stop and wait for a human/Waffles ruling. When my
usage returns I re-verify whatever landed in my absence — record hashes.

## Risks, stated honestly

- **Server binding is the least-known phase**: nobody has mapped current
  `liminal-server` participant handling against the contract. Phase B's
  discovery step exists precisely for this; its declaration may legitimately
  say "the binding is bigger than estimated" — that's information, not
  failure.
- **Wire compatibility**: the contract's register (0x0100..0x0124) may be new
  rather than matching what the server speaks today. If the server has an
  existing incompatible wire format, phase B must state it in discovery and
  Tom rules on migration vs cutover. Do not silently support both.
- **A2/B parallelism** can create rebase noise on the crate — fall back to
  sequential if more than one rebase round is needed.
- **The 56-case transcription** may surface more document defects (cases that
  can't pass against the fixed design). Expected and fine: the test lands with
  the failing assertion documented in the declaration, and the FIXED behavior
  wins — the document is source material, not authority, where it conflicts
  with the two mandated fixes.
