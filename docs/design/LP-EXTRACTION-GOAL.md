# liminal-protocol — extraction goal session

You are building `crates/liminal-protocol` in the liminal repository: the shared
protocol crate for conversation participant lifecycle. The server
(`crates/liminal-server`) and the SDK (`crates/liminal-sdk`) will both consume
it, so the rules are defined once, in types, and the two sides cannot drift.

Your source material is `docs/design/PARTICIPANT-CONTRACT.md` at commit
`55856ae3c53206f9c662e6815650dfc67a89ce85` (6,331 lines, SHA-256
`88da20fa11d878885b92b6782ca129121f6c74c574ca2962dbf1be17521d6312`). That
document is FROZEN, READ-ONLY source material. You never edit it, and you never
author any new design document. Your deliverable is code: types, functions,
tests, and doc-comments. Where this brief mandates a deviation from the
document, the deviation wins and you record it in a doc-comment citing this
brief.

## Ground rules

- Work ONLY in a new worktree at `.worktrees/liminal-protocol`, branch
  `feature/liminal-protocol`, created from current `main`. Never touch `main`
  or any other branch or worktree.
- Set `CARGO_TARGET_DIR` to the repository's own `target/` directory. Never
  build into any temporary directory.
- Your FIRST commit on the branch is this brief file
  (`docs/design/LP-EXTRACTION-GOAL.md`), unmodified, as the document of record.
- Commit and push after every completed unit of work
  (`feat(protocol): ...`/`test(protocol): ...`). Durability beats tidiness.
- Match the workspace's existing lint posture (workspace lints, edition,
  formatting). No `unsafe`. No `unwrap`/`expect`/`panic` outside `#[cfg(test)]`
  code. `cargo clippy --all-targets -- -D warnings` must pass at every
  checkpoint. Public items carry doc-comments.
- You never modify `liminal-server` or `liminal-sdk` in this session, beyond
  adding the new crate to the workspace `Cargo.toml` members.
- No new prose documents, no exam rounds, no self-designed review loops. Your
  gates are the compiler, clippy, and the test suite. When they are green for
  the declared scope, you declare and stop.

## Two mandated design fixes

These are confirmed defects in the source document. Do NOT transcribe the
document's version of these two areas.

**Fix 1 — detach cell gets a fourth variant.** The document's identity-slot
detach cell (lines ~1578–1634) has `Empty | Pending | Committed`, and says a
successful attach "clears" the cell — yet its case 53 (lines ~4728–4752) and
the R-D1 `StaleAuthority` wire row (line ~5671) require a post-attach replay of
the old detach token to return `TerminalizedDetachCell` carrying the OLD
`committed_binding_epoch`. That data does not survive the clear; the promise is
unproducible. In the crate, the cell is:

```
Empty
| Pending { token, participant_id, request_generation, request_verifier,
            committed_binding_epoch, admission_order, refused_epoch }
| Committed { token, participant_id, request_generation, request_verifier,
              committed_binding_epoch, detached_delivery_seq }
| Terminalized { token, participant_id, request_generation, request_verifier,
                 committed_binding_epoch }
```

A successful attach transitions `Committed → Terminalized` (never to `Empty`).
Leave replaces the cell under tombstone precedence exactly as the document
describes. The `TerminalizedDetachCell` response type must be constructible
ONLY from the `Terminalized` variant — encode that in the type signatures, so
the original defect is a compile error by construction. Token lookup order
(document lines ~1930–1965) extends naturally: an exact token against
`Terminalized` returns the `TerminalizedDetachCell` result; everything else
follows the document.

**Fix 2 — do not transcribe the fixed occurrence-array machinery.** The
document's R18 occurrence bound and slot layout (lines ~2560–2715: the
`successor_milestones` array, `R_max`/`E_max`/`O_max`, `O_base`, the ordinal
partition) is defective: cursor-progress facts are participant-scoped (the slot
key itself carries `participant_index`), but the layout reserves one
cursor-progress group per record shared across all participants. Two bound
members acking the same retained suffix legally require more facts than the
array holds. Do not port any of it. Instead: cursor-progress facts are keyed
`(participant_index, boundary)` and accounted per participant. Completion-
ordering coverage is carried by the typed state machine transitions plus the
property tests in this brief — not by a serialized slot array. Record the
deviation and the reason in the module doc-comment.

Everything else in the document — the algebra, the wire register, the
lifecycle rules, the lookup order, the worked examples — was verified sound by
multiple independent examinations and is transcribed faithfully.

## Phase 1 — the core (checkpoint 1)

1. **Crate scaffold.** `crates/liminal-protocol`, added to workspace members.
   Modules roughly: `wire` (message types + tags + encode/decode), `algebra`
   (capacity/floor/debt functions), `lifecycle` (states + transitions),
   `outcome` (the outcome taxonomy).
2. **Wire register.** Every client request (8 contiguous), every server value
   (`0x0100..0x0124`, 37 contiguous), both pushes, with exact selectors, tags,
   and field lists from the document's R-D1 register (the big outcome/schema
   tables in the ~5,500–5,800 region). Tagged unions become Rust enums with the
   exact discriminants; "no optional field bag exists" means no `Option` fields
   where the document declares required fields. Include the
   `ConversationSequenceExhausted` payload with its canonical ten-field
   `sequence_budget`.
3. **Algebra.** Pure functions with the document's exact formulas (lines
   ~2884–2930): `B = S + ((I − C) × marker_max)`; zero-debt admission
   `B + Q + K <= cap`; mandatory-class checks
   `d' = max(0, B' + Q + K_remaining' − cap)`, `B' + K_remaining' <= cap`,
   `d' <= Q`; floor rule `F' = max(F, preferred_floor, cap_floor)` with
   `m = H'` when membership becomes empty. All operands widened to u128 in the
   document's canonical suboperation order (stated immediately after line
   ~2717). Unit-test each formula with the document's own fixture numbers.
4. **Lifecycle state machine.** Typed states: binding states, the four-variant
   detach cell (Fix 1), membership/tombstone states, and the seven stored-edge
   kinds (`ObserverProjection`, `PhysicalCompaction`, `MarkerDelivery`,
   `ParticipantCursorProgress`, `DetachedCredentialRecovery`,
   `DetachedMarkerRelease`, `DetachedCursorRelease`). Typed transitions from
   the document's completion-orderings table (lines ~2933–2941) and the token
   lookup order (lines ~1930–1965). Every transition returns a typed outcome;
   every outcome the document names must be constructible, and only from states
   that actually carry its data. Where the document declares an intended dead
   end (DMR, DCursor), the type carries that intent and names the sole
   successor.
5. **Tests one and two — the confirmed blockers, as permanent regression
   tests.**
   - *Detach*: a test module demonstrating the `Terminalized` flow end to end —
     detach commits, attach terminalizes, exact-token replay returns
     `TerminalizedDetachCell` with the old epoch; plus a doc-comment in that
     module stating why the three-variant version cannot compile against the
     response type (the compile-refusal is enforced by the signatures from
     Fix 1).
   - *Occurrence*: a test with two bound participants acking the same retained
     suffix step by step during a nonzero-debt episode — every ack must be
     serializable under the per-participant accounting. This is the exact
     history that overflowed the document's array.

**Checkpoint 1 declaration:** the crate compiles, clippy is clean at
`-D warnings`, phase-1 tests green. Push, then state: commit hash, module
inventory, test count. PAUSE for external review before phase 2 begins —
review findings arrive as concrete fix items, you apply them, then proceed.

## Phase 2 — the corpus

6. **The 56 acceptance cases** (document's acceptance-case section, cases
   contiguous 1–56) transcribed as tests: exact pre-states, operations, and
   expected outcomes with the document's exact values. Where a case exercises
   the two fixed areas, the test asserts the FIXED behavior and cites this
   brief in a comment. Skip nothing; a case you cannot make pass is reported in
   your declaration with the failing assertion, never deleted or weakened.
7. **The algebra fixtures** scattered through the document (e.g. the capacity
   walk near line ~3472, the required-capacity vector near ~4196, the
   byte-budget walks near ~5129) as unit tests with the exact printed numbers.
8. **Property tests** (proptest, matching however the workspace already does
   randomized testing — if it doesn't, add proptest as a dev-dependency):
   ack-ordering interleavings across 1..=4 participants (every legal
   interleaving serializes; cursors never regress; floor monotonicity holds),
   and crash-replay (any transition replayed from its pre-state produces the
   identical outcome — pure functions make this cheap).

**Final declaration:** final commit hash, total test count, the full
`cargo build` / `cargo clippy --all-targets -- -D warnings` / `cargo test`
outputs summarized, plus a list of any case that needed interpretation beyond
transcription, with the document lines and the interpretation taken.

## What happens after (not your scope)

External review of the state machine and the deviations (Sol lens plus
Waffles's gates), then the server binds the crate, then SDK-receive, then
release. You do none of that in this session.
