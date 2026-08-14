# Fossil-reclaim lane — A7 operator re-issue + reconcile coherence

Lane branch `fossil-reclaim-a7`, cut from landed main `19ea485`. Two legs, one
lane, because both were opened by the same word and both gate browser visitors
on the live estate.

**Authority.** Tom's verbatim ruling, relayed at meridian `d3c7f892`
(2026-08-14, Waffles from Tom's desk after hitting the seating refusal in his
own browser): "I don't want to accept anything. That's not fine. We need to
fix everything." Both fixes are proper fixes; no accepted costs. Review gate:
Waffles reviews this brief plus §0.18 r0 before dispatch; the §0.18 TEXT
ratifies later at the two-key sitting with Cally (A6 precedent: mechanism may
land gated while text ratification pends).

**Specimens.** Meridian `819dfdff` (Waffles, 2026-08-14). Board rows: #74
(leg 1), #76 (leg 2). Preserved evidence at Waffles's end, flagged never-sweep
in his day file: pre-retry store copy
`manifold/.manifold-evidence-20260814-marker-authority` (950M) plus both boot
logs (`.manifold/kernel-boot-20260814-1547.log:1020`, `-1602.log:2198`).

---

## Leg 1 — #74: `OperatorCredentialReissue` (A7 §0.18 r0)

The normative spec is §0.18 of `PARTICIPANT-CONTRACT.md` on this branch. Build
to it exactly; where this brief and §0.18 disagree, §0.18 wins and the
disagreement comes back to the seat as a flag, not a silent choice (the §0.15
literal-range lesson: a contract defect is caught red and flagged, never
patched unilaterally, never silently complied with).

**Sites** (all citations @19ea485 unless noted):

- Operator surface: `crates/liminal-server/src/health/endpoint.rs` (the
  `GET /unloadable-conversations` mount, `UNLOADABLE_PATH` at `:19`) gains the
  re-issue route. Same trust plane, same serving discipline. Exact route name
  and body shape are build decisions; the inputs are fixed by §0.18
  (`conversation_id`, `participant_id`, `expected_current_generation`).
- Serialized operation: participant production gains the re-issue operation at
  the one serialized participant-state point (the lane that runs enrollment
  and credential attach). All four §0.18 guards evaluate there; every refusal
  mutation-free.
- Durable row: one new stored-operation row type for the re-issue (verifier
  and generation, never a secret body). Follow the store's schema-evolution
  discipline (`OperationSchemaPhase` in
  `crates/liminal-server/src/server/participant/production/ops_session_replay.rs`
  region — the replay reads phased pages; a new row type must replay
  identically on the crash boundaries in §0.18 acceptance 3).
- Protocol crate: **untouched**. `CredentialRecoveryLost` already exists at
  `crates/liminal-protocol/src/outcome/local.rs:142` and is the entry state,
  not a wire change. Lane assertion at gate time: `git diff --stat
  19ea485..HEAD -- crates/liminal-protocol` is EMPTY. If the build discovers a
  genuine protocol need, STOP and return to the seat — that reopens the
  breaking-window question and is not this lane's to decide.
- SDK: no wire change. Re-entry is §0.18 item 5 — the member installs the
  issued credential durably and performs an ordinary attach. If the SDK's
  durable-credential load path already covers installation (manifold writes
  the member's credential store), ship zero SDK delta and SAY SO in the land
  message; if a small install/exit-terminal helper is genuinely needed, it is
  additive and named.

**Pins** (§0.18 acceptance frame is the floor, not the ceiling):

1. The e2e fossil shape (acceptance 1) — this is the specimen's shape and the
   red fixture's stand-in: enroll → attach to G → response lost + receipt
   expired → `EnrollmentKnown` → re-issue → attach with issued secret →
   G+1 issued / G+2 bound / dead secret `StaleAuthority`.
2. All four guard refusals red-proven both ways with before/after state
   census (acceptance 2).
3. Replay equivalence across both crash boundaries of the re-issue row
   (acceptance 3).
4. The withheld control (acceptance 4): without re-issue the fossil is
   refused forever — pinned.
5. No-polling audit (acceptance 5).

**Field proof** (after land + Waffles consume): operator re-issues @compose's
identity on `#registry.ops`, installs the credential, compose takes its seat,
a browser-guest seating commits. That closes #74 in the field; the lane does
not wait on it to land.

---

## Leg 2 — #76: reconcile coherence at anchor retirement

**The defect** (mechanism read at 19ea485, confirmed against both boot logs):
the orphan reconcile retires marker-delivery anchors from the frontier but
strands the paired durable marker obligation in the outbox. Live, the
surviving obligation forces a re-offer (`record_publication_offer`,
`crates/liminal-server/src/server/participant/production/handler_semantic.rs:453-498`;
the `offered_markers` insert at `:492` is guarded by
`outbox.is_marker_obligation` at `:482-484`), and the ack for that offer walks
the offered=Some arm of `apply_marker_ack_with_impact`
(`.../ops_acks.rs:296-311`) into `marker_delivery_progress`, which correctly
finds no authority and refuses at the invariant
(`.../marker_progress.rs:76-78`) — fail closed, estate down.

**The fix**: retiring an anchor retires or re-derives the paired outbox
obligation and any offered state, at BOTH reconcile sites:

- load-end: `reconcile_load_end_marker_anchors`
  (`.../ops_session_replay.rs:149-163`);
- replay-retry: `retry_replay_after_orphan_reconcile`
  (`.../ops_frontier.rs:524-548`).

**Laws for this leg:**

- The invariant at `marker_progress.rs:76-78` is UNMODIFIED. It is correct
  for genuine divergence; a fix that widens, softens, or special-cases it is
  refused. Coherence is restored on the reconcile side only.
- If restoring coherence requires changing what the reconcile itself retires
  (i.e., the fix wants to change refusal/restoration semantics rather than
  ledger coherence), STOP — that is a contract question for the seat.
- #45 (live-window re-orphaning for the process lifetime) is a SIBLING, not
  this leg: do not claim it closed; if the coherence carrier naturally serves
  the live path too, say so and leave #45's closure to its own measurement.

**Pins:**

1. Constructed orphan shape (the #45 manufacture: participant erasure or
   record retirement stranding an anchor whose obligation survives), boot,
   first live marker ack answers the benign arm — RED-PROVEN against the
   unfixed tree, where it must die at the exact invariant string ("stored
   MarkerAck has no matching marker delivery authority"), TRUE EXIT.
2. The quiet-estate arm: NO commits between ready and the first marker ack.
   Pre-fix this replays identically every boot — the crash-loop shape (the
   one-boot "self-heal" is an accident of traffic). Post-fix, first boot
   clean. This is the pin that makes "not an accepted migration cost" a
   measurement instead of a sentence.
3. The witness-row path still works: a store whose record carries a
   post-reconcile committed row loads clean and the replay-retry still fires
   exactly once (the existing behaviour Waffles's boot 2 proved, pinned so
   the fix cannot regress it).
4. On-disk arm for at least the quiet-estate pin (the breaking-window Leg 4
   discipline: instrument control proving the disk store actually engaged).

**Field cross-check** (operator-side, not a lane gate): Waffles's preserved
950M store copy boots clean on the fixed tree, first boot, no invariant. His
box, his hands, after consume.

---

## Lane discipline

- Per-lane branch (this one), YG-560: no merge/rebase/cherry-pick/pull into
  the branch; conflict = STOP.
- Dispatch: `opus5-implementer` subagents, 2-3 concurrent max, one leg per
  agent; seat verifies every claim at its own hands (gates re-run, bytes
  read) before land.
- Gates: full workspace battery (`cargo test --workspace --no-fail-fast`,
  teed, `TRUE EXIT` echoed) at the lane tip; baseline 2055/2/3 over 56 plus
  exactly the new pins, reconciled by name; failures = the declared F8 pair
  BY NAME only. `cargo clippy --all-targets` clean at the seat's own hands.
  Logs in-tree under `gate-logs/fossil-reclaim/`.
- Red proofs committed to `gate-logs/fossil-reclaim/` before their fixes'
  greens, per standing practice.
- Land: merge to main only after Waffles's review of this brief + §0.18 r0
  and the usual gates; publish is NOT this lane's concern (server-only delta
  rides the next wave unless Tom's word says otherwise).
