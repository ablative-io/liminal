# Breaking-window build — A5 + A4 + StateUnavailable{source}

Lane: `breaking-window-a5-a4` off `b200718` (= tag `liminal-v0.7.0`).
Authority: window opened by Tom's word 2026-08-13 (relayed, meridian msg
`83499209`, "that window is open now, so may as well be going"). A4 §0.15
obligation 3 (seat owns arming the window) is discharged by that word.
Contract authority: `docs/design/PARTICIPANT-CONTRACT.md` §0.15 (A4,
RATIFIED @c3c7c7f) and §0.16 (A5, RATIFIED @d9585b5), both read at
`b200718` for this brief. Window selection for A5 obligation 2 was made BY
MEASUREMENT: strict-refuse decoders at published-client bytes
(`gate-logs/a5-decoder-measurement/RESULTS.md@3b8c370`).

## Site census (all re-measured at b200718)

| Anchor | Site @b200718 | Contract cite |
|---|---|---|
| Seam (`apply_live_transition`) | `crates/liminal-protocol/src/lifecycle/claim_frontier.rs:2397` | :2397-2409@c3c7c7f |
| Wrapper `apply_enrollment_frontier` | `crates/liminal-protocol/src/lifecycle/operations/live_frontier.rs:1113` | :1113@c3c7c7f |
| Wrapper `apply_attach_frontier` | `crates/liminal-protocol/src/lifecycle/operations/live_frontier.rs:1165` | :1165@c3c7c7f |
| Wrapper `apply_detach_frontier` | `crates/liminal-protocol/src/lifecycle/operations/live_frontier.rs:1268` | :1268@c3c7c7f |
| Attach flatten | `crates/liminal-server/src/server/participant/production/ops_attach.rs:562` | :518-525@c3c7c7f (drifted) |
| Detach flatten (`detach_commit`) | `.../production/ops_session.rs:240` | :237-240@c3c7c7f |
| Subsequent-enrollment flatten | `.../production/ops_enroll.rs:382` | :379-385@c3c7c7f (drifted) |
| `DrainFirst` clearing write | `.../production/ops_frontier.rs:225` | :225@c3c7c7f |
| Dormant `PresentedRefusal::detach()` | `.../production/presented_refusal.rs:95` (`#[expect(dead_code)]` at :89-94) | :91@c3c7c7f (drifted) |
| A2 wide-range dedup arm | `.../production/ops_frontier.rs:130-154` | `ops_frontier.rs:138-146`@c3c7c7f family |
| Push registry (0x0200/0x0201) | `crates/liminal-protocol/src/wire/tags.rs:158-162` | measured |
| `AttemptTokenBodyConflict` (0x0101) | `crates/liminal-protocol/src/wire/tags.rs:82`, `wire/response.rs:59` | measured |

## Leg 1 — protocol wire surface (BREAKING, priced)

1. New push `0x0202 MarkerSettled { conversation_id, refused_epoch }` —
   registry row, body codec, both-direction codec tests, push acceptance.
2. New refusal value `MarkerSettlementBackpressure { conversation_id,
   refused_epoch }` admitted for the attach and detach request families.
3. New refusal value `EnrollmentSettlementBackpressure { conversation_id }`
   admitted for the enrollment request family. NO epoch field, by ruling.
4. `AttemptTokenBodyConflict` gains `RecordAdmission` in its
   admitted-request set (§0.15: today admits only
   `CredentialAttachRequest|LeaveRequest`).
5. Codec tests must cover unknown-value strict-refuse both directions
   (registry `0xFFFF` fixture stays permanent).
6. Golden trace (`docs/wire/golden-trace/`, live pin): if the capture is
   invalidated by the new registry rows, re-derive per its own manifest
   procedure — never hand-edit bytes.

## Leg 2 — server A5 mechanics (§0.16 law)

- Discriminate `Precedence` clearing conditions at the SEAM's wrappers
  (law attaches to any wrapper of `apply_live_transition`, present or
  future — no call-site enumeration): binding-terminal → existing
  `ObserverBackpressure` row (answering marker-candidate with it is
  OUTLAWED — promises an `ObserverProgressed` nothing sends);
  marker-candidate-awaiting-drain → new rows per wrapper; armed
  fenced-recovery → excluded by census (#13), tripwire stands.
- Wire delivery via `PresentedRefusal` Class B exit; bring `detach()`
  alive (drop the `#[expect(dead_code)]`).
- `0x0202 MarkerSettled` fires at the clearing write
  (`ops_frontier.rs:225` DrainFirst arm, and boot drain), CONNECTION-SCOPED
  to connections refused in this process lifetime. NEVER to a connection
  refused at the enrollment wrapper.
- Obligation 1 — restoration proof: pin proving post-refusal state
  identity with pre-request state at the carrier's granularity, in-memory
  finalizer state explicitly included (`into_parts` alone measured
  insufficient). RED-PROVEN against a build that skips restoration.
- Obligation 3 — no broadcast: scoping pinned; the lazy
  every-connection-on-the-conversation push is outlawed.
- Obligation 4 — stage-order honesty: document + pin observed stage order
  at the new row's sites (stage 11 before 9/10 note extends).
- Adjacency flag (Waffles, in §0.16 status block): condition-2
  persist-the-waiting-state retry discipline must not extend #195's
  orphan window — the pin must SAY so.
- refused_epoch is load-bearing: stage-11 retry matches it against
  `MarkerSettled`'s.

## Leg 3 — server A4 mechanics (§0.15 law) + SDK shape

- Same-participant same-token different-payload committed match inside the
  retained op-log window → typed `AttemptTokenBodyConflict::RecordAdmission`
  refusal, commits NOTHING, consumes no `transaction_order` major; site =
  after authority verification, before order allocation (the A2 site).
- TWO RANGES (obligation 1): refusal probes its OWN presenter-scoped range
  `(token, [0x00;32], presenter) ..= (token, [0xFF;32], presenter)`; the
  wide range at `ops_frontier.rs:130-154` REMAINS warn-and-fall-through
  for cross-participant. One widened arm violates the amendment regardless
  of test results.
- Obligation 2 — latency: measure the presenter-scoped-vs-wide lookup
  differential and bound or unify it; record numbers in the lane evidence.
- Retention honesty: inherits A2's pin-when-compaction-exists obligation.
- SDK: `StateUnavailable { source }` breaking shape (#62 Leg B deferral)
  on the sdk error surface.

## Gate discipline

Baseline `2028/2/1 over 55` (battery = `cargo test --workspace
--no-fail-fast` teed, `echo "TRUE EXIT: $?"`; TRUE EXIT 101 expected —
declared F8 pair `tests_f8_marker_poison::{a_refused_connection_fate_leaves_no_durable_residue,
the_incident_sequence_reboots_into_a_discharged_fate_and_a_live_server}`
propagates; reconcile failures BY NAME never count). New pins enumerated
by name in the land record; every red proven against its own fix.
`cargo fmt --all` is UNSAFE at this repo — never run it. Publish after
land: reader first (protocol-changes-land-at-both-ends), versions ruled
per the three-classes law at publish time.

## Seat rulings after Leg 1 (2026-08-13, Hermes)

Leg 1 landed at `071a48e` + `a3cc02a`; verified at the seat (`cargo test -p
liminal-protocol` re-run: 586/0 over 9 targets, TRUE EXIT 0; registry rows,
origin arms, and type shapes read at the bytes).

1. **`AttemptTokenBodyConflict::RecordAdmission` carries no `AttemptConflict`
   selector — ACCEPTED.** Measured basis: the committed-identity key at
   `answer_committed_record_admission` is `(token, payload_fingerprint,
   participant_id)` — generation is not in it, so no selector variant is
   constructible on that arm. §0.15 mandates no selector; minting
   `AttemptConflict::Payload` would be unratified wire surface. Register
   precedent (per-variant field presence) followed.
2. **`SettlementEpoch = u64` as its own alias — ACCEPTED.** A settlement
   epoch must never read as comparable to observer progress; the type-level
   firewall is the cheapest pin of that.
3. **`carries_origin` as the complement of the origin-free set — ACCEPTED,
   verified at the bytes.** Single source of truth (the complement of
   `origin_is_valid`'s all-false set), with the why-not-a-window stated in
   its doc. Golden-trace instrument + walkthrough updated at the seat to the
   complement form (capture behavior identical — all captured tags
   `<= 0x0124`); rev-pinned `@339e81a` citations kept as historical.
4. **Correlation gap — closure REQUIRED IN THIS LANE, client-side, no new
   wire fields.** A settlement refusal landing as `ForeignResponse` at the
   client would convert lawful presentation back into a client error and
   defeat the amendment. The ratified rows are fixed (no participant/token
   fields may be added — that would be a new amendment), so the closure is
   client-side correlation: match the settlement refusal against the
   pending request slot by conversation + request family. Assigned to
   Leg 2's scope; the Leg 1 pin
   (`settlement_refusals_carry_no_correlating_request_identity`) inverts
   when closed — rework it into the closure's positive pin.

## Seat rulings after Leg 2 (2026-08-13, Hermes)

Leg 2 landed at `c13698a`; verified at the seat: three-crate gate re-run at
my hands — only failures are the declared F8 pair BY NAME, TRUE EXIT 101
(expected form); registry/classifier/closure structures read at the bytes;
red evidence in `gate-logs/breaking-window/leg2-*.log`.

1. **`PrecedenceCondition::Unclassified` — ACCEPTED, with a measurement
   owed.** An honest fourth variant beats forcing foreign sites into the
   family's three conditions. OWED (Leg 3): measure whether `Unclassified`
   is REACHABLE through any of the three wrappers. If unreachable, pin the
   tripwire (first reachable construction voids the flatten). If reachable,
   STOP — that is a §0.16 premise gap ("Precedence has THREE clearing
   conditions") and goes back to the two-key holders before land.
2. **`PresentedRefusal::enrollment()` — ACCEPTED.** The third wrapper's row
   needs a carrier; §0.16 named only `detach()` because only `detach()`
   existed to name. Same register-bound pattern, no bare `ServerValue` path.
3. **Position-allocator restoration — ACCEPTED, no contract change
   needed.** §0.16 obligation 1 states the granularity as "the carrier's
   own granularity" with finalizer state "explicitly included" — a floor,
   not a ceiling. Restoring MORE consumed authority the build discovered
   (allocators; `high_watermark + 2` otherwise) is compliance, not
   deviation. Recorded for the land message so the key-holders see it.

**Gap closures assigned to Leg 3** (Leg 2's honest remainder):
(a) obligation 3's dedicated registry pin — exercise
`register_settlement_waiter` + `fire_settlements` directly, two live
inboxes on one conversation, one refused one not, exactly one wake;
(b) end-to-end wake proof over a real transport — a refused client
receives `MarkerSettled` after the `DrainFirst` clearing write, an
uninvolved client on the same conversation receives nothing;
(c) the drain-then-retry round trip if the ack-contiguity harness allows —
otherwise name it in the land record as not covered and why;
(d) ruling 1's reachability measurement.

## Seat record after Leg 4 (2026-08-13, Hermes) — the disk-store question CLOSED BY MEASUREMENT

Two-step resolution, both at the bytes:
1. **Base repro (b200718, isolated worktree):** the window + DrainFirst
   clearing write on a disk store is GREEN at base, deterministically, at
   both the semantic seam and the full connection stack. Hunk-by-hunk diff
   classification: NO durable byte shape, append call, or write ordering
   changed on the drain path in this lane.
2. **Lane-tip 2×2 (0dc3163):** {disk, ephemeral} × {refused attach, none}
   — ALL FOUR GREEN, disk+refused-attach repeated 8× total including a
   tracing-subscriber arm matching the original log's format. Every
   mechanism that reaches haematite's `InvalidNode` was driven and
   excluded (second concurrent writer → refused at open by the store's
   own lock; reused directory → clean reopen; dropped TempDir → fails at
   open, differently). The original failure log's "second stream" is the
   same store failure surfacing at teardown, not independent evidence.

VERDICT: **the recorded failure does not reproduce under matched
conditions.** The harness that produced it was never committed and left
no residue, so what it actually ran is unknowable. The claim is retired
from prose and converted to a shipped measurement:
`the_settlement_wake_reaches_the_refused_connection_and_no_other_on_disk`
(a5_settlement_wake_e2e.rs, landed 7571267) runs the identical sequence
disk-backed, with an instrument control proving the arm really runs a
persistent database (config.json presence; red-proven — pointing the body
at `None` passes the sequence while the control fires, TRUE EXIT 101,
`gate-logs/breaking-window/leg4-shipped-disk-arm-red-control-runs-ephemeral.log`).
Honest caveat, carried: 8 greens cannot exclude a rare haematite-level
flake; if one ever recurs it is a store-layer bug and the shipped arm is
the tripwire that catches it.

Also at Leg 4: gate 1565/2/3 over 45 (delta vs 145027f exactly +1, the
new test), clippy --all-targets zero across all three crates, restoration
pins green unmodified, no src file touched.
