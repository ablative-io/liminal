# Wiring ledger — dormant machinery and its roads back

- **Revision:** r1, 2026-07-19. Owner of the ledger: Waffles (coordination seat).
  Lane owner unless stated otherwise: Hermes Crumpet (liminal seat).
- **Why this exists:** the F-0c Unit 2 fold minted the unwired-seam sweep as a
  mandatory discipline: every entry point a branch adds either has a production
  caller at bytes, or is declared dormant with a **named future consumer**. This
  ledger is the companion discipline — the register that keeps every dormant
  declaration honest with a trigger, an owner, and an oracle floor. It also
  answers the standing ask from Annabel's machine (via Hermes's consolidation)
  to enumerate liminal's unwired machinery, and Tom's go: the wiring program
  continues.

## The two rules every lane obeys

1. **Wire-with-oracle.** A lane is done when the seam has a production caller
   AND a named oracle test proving the wired behaviour. Wiring without its
   oracle fails the tear. (Minted in the Unit 2 fold; standing here for every
   future lane.)
2. **No row, no dormancy.** A seam may ship dormant only if it has a row here
   (or in a successor register) carrying: named future consumer, build trigger,
   owner, oracle floor. A dormant seam without a row is a finding at any tear.
   (The scheduled-road-back rule, Tom-ratified 2026-07-19.)

## Lanes

### W1 — BindingFate observer projections
- **What sits dormant:** the `BindingFate` observer projection arms
  (`Died` / `Ordinary` / `Recovered` / `LeaveCommit`), landed with Unit 2,
  zero production callers (declared in the Unit 2 Census A, verified at my
  tear of `7a9b2cb`).
- **Named consumer:** the §8 crash-fate arms of the durability design — these
  projections are what the crash repository reads when it goes production.
- **Trigger:** the crash repository moving to production use.
- **Oracle floor:** per-arm projection tests (each fate arm drives its
  projection and asserts the projected row; no shared fixture shortcuts).

### W2 — Nonzero-debt ack-obligations pair
- **What sits dormant:** the nonzero-debt ack obligations pair landed with
  Unit 2; its scalar sibling is equally uncalled (Census A verified this is
  NOT the item-28 relocation pattern — genuinely awaiting its consumer).
- **Named consumer:** the dispatch arm that consumes obligation debt at
  delivery decision time.
- **Trigger:** the dispatch arm's build (first unit that schedules deliveries
  against obligation debt).
- **Oracle floor:** dispatch-arm tests exercising both the nonzero-debt path
  and the scalar path against the same fixture, asserting they cannot diverge.

### W3 — Apply-per-page restore (row R) — SCOPE NARROWED r1.1
- **What sits open:** `spec:570 total-restore-streaming` — `read_all`
  materializes the full decoded stream; only the 64-row page size is enforced.
  Disclosed in the Unit 2 declaration under its own line; disposition
  Tom-ratified (disclose-with-teeth).
- **Scope (narrowed 2026-07-19, W3 pre-review finding 1):** W3 removes the
  duplicate aggregate materialization (the `read_all` Vec) ONLY. The brief's
  original "safe for unbounded history" claim was FALSE at the bytes — the
  restored authority itself retains history-linear indexes (see W7). W3
  does NOT alone discharge the unbounded-history trigger.
- **Named consumer:** restore path under unbounded outbox history.
- **Trigger:** HARD, SHARED WITH W7 — before any deployment with unbounded
  outbox history, BOTH W3 and W7 must be discharged. Stated on both rows so
  neither landing alone can be read as unblocking unbounded history.
- **Oracle floor:** the apply-per-page brief's acceptance re-runs the 24/30
  determinism oracles (the ratified floor), PLUS a retained-authority-counts
  oracle measuring the narrowed claim (what memory the restored authority
  actually holds), not asserting it. Error precedence for multiply-invalid
  durable states is preserved EXACTLY via a bounded validate-then-apply
  two-pass (one-page peak, zero observable contract change) — ruled at the
  coordination seat 2026-07-19; a new error order would have been a contract
  change needing Tom.
- **Owner:** Hermes (brief r2 fold in flight).

### W7 — Authority restore history-linear indexes (opened by W3 pre-review)
- **What sits open:** the restored `ConversationOutbox` retains
  history-linear indexes — `source_batches` / `ack_sources` /
  `all_obligations` (`outbox.rs:124-137`; inserts `:205-252`, `:298-325`,
  `:262-270`; reclamation removes live records only, `:330-395`). Restore
  memory is Θ(history) with or without W3.
- **Named consumer:** any deployment with unbounded outbox history.
- **Trigger:** HARD, SHARED WITH W3 (see W3's trigger wording verbatim).
- **Oracle floor:** its own bounding design brief (index compaction /
  reconstruction touches ack + conflict semantics — a design-first lane,
  NOT foldable into W3); acceptance includes the retained-authority-counts
  oracle family measuring each index under bounded and unbounded fixtures.
- **Owner:** Hermes.

### W4 — LAW-1 polling retirement
- **What sits open:** the polling seams LAW-1 retires, board item since
  Hermes's catch (see `docs/design/LAW1-POLLING-RETIREMENT.md`).
- **Named consumer:** the event-driven replacements the LAW-1 design names.
- **Trigger:** next liminal maintenance window after the wiring lanes W1/W2
  open (sequencing at Hermes's seat).
- **Oracle floor:** per LAW-1 doc — absence proofs (no polling observed under
  the doc's named workloads), not just presence of the new path.

### W5 — LP-CLIENT SDK riders (two, ledgered at Phase C landing)
- **What sits open:** (a) `decode_abandonment` any-request gap;
  (b) pre-existing `unreachable!()` at `inbound.rs:140`.
- **Named consumer:** the SDK leg that hardens client decode paths.
- **Trigger:** first SDK hardening pass, or immediately if either surfaces in
  a production trace.
- **Oracle floor:** (a) a decode-abandonment test per request shape;
  (b) the `unreachable!()` replaced with a typed refusal + a test that reaches
  the formerly-unreachable arm.

### W6 — Browser conversation surface
- **What sits open:** request-reply and conversations remain Rust-side only;
  the browser SDK deliberately ships publish-with-receipt + subscription only
  (recorded in the Iridium authoring draft §5.4 as a chosen non-wait).
- **Named consumers:** the frame authoring arc (edit proposals as
  conversation facts, when Tom rules T1–T4) and a Meridian-in-frame surface
  (two-way conversation UI) — both in the applications conversation opened
  with Tom 2026-07-19.
- **Trigger:** whichever named consumer Tom greenlights first.
- **Oracle floor:** browser conversation tests mirroring the Rust transport's
  conversation suite (same semantics, wire-level parity asserts).

## Companion registers (not duplicated here)

- **Frame danglers:** the decisions audit (2026-07-19, coordination seat)
  enumerates the frame-side named-outs; its remediation runs under the same
  no-row-no-dormancy rule. Live roads back already opened from it: the
  editable-Iridium arc (design draft r0.1), the operable-console arc (send
  landed 2026-07-19), motion-feel pass (queued, Tom's eyes on result).
- **D7–D11 attribution re-head:** held at Tom's desk (which-did-you-rule),
  not a wiring lane.

## Standing

Lanes W1, W2, W4: Hermes picks up on his word — "ready to pick up
wiring-program lanes the moment the ledger names them." This document is that
naming. W3's brief drafts at his seat per the ratified disposition. W5 queues
behind the current SDK arcs. W6 waits on Tom's application ruling.
