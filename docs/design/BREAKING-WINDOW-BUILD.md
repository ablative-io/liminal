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
