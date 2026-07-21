# W2 — obligation-debt dispatch arm

**Revision r1 — design-first brief, 2026-07-21**

This brief rules the first production arm that makes participant-delivery
scheduling conditional on protocol-owned obligation debt. It is a docs-only
lane: it specifies the build and its acceptance oracles; it does not claim that
the arm is already implemented.

## 0. Authority, pin, and binding lane law

The byte pin for every ground fact in this brief is liminal `23acdea`
(`23acdea0c390d4238a9ad1dcdd02cd60a85ffcbd`). At that commit the wiring
ledger identifies itself as **r1.9, 2026-07-20**
(`docs/design/WIRING-LEDGER.md:1-6`). Its two binding rules are:

1. **Wire-with-oracle:** a lane is complete only with a production caller and a
   named behavior oracle (`docs/design/WIRING-LEDGER.md:16-21`).
2. **No row, no dormancy:** a dormant seam requires a ledger row carrying its
   named consumer, trigger, owner, and oracle floor
   (`docs/design/WIRING-LEDGER.md:22-25`).

The controlling W2 row is quoted byte-for-byte, including its Markdown line
breaks:

> ### W2 — Nonzero-debt ack-obligations pair
> - **What sits dormant:** the nonzero-debt ack obligations pair landed with
>   Unit 2; its scalar sibling is equally uncalled (Census A verified this is
>   NOT the item-28 relocation pattern — genuinely awaiting its consumer).
> - **Named consumer:** the dispatch arm that consumes obligation debt at
>   delivery decision time.
> - **Trigger:** the dispatch arm's build (first unit that schedules deliveries
>   against obligation debt).
> - **Oracle floor:** dispatch-arm tests exercising both the nonzero-debt path
>   and the scalar path against the same fixture, asserting they cannot diverge.

Source: `docs/design/WIRING-LEDGER.md:135-144`.

There is no premise contradiction at the pin. The server has a participant
publication pump and a durable-obligation selector, but neither decision reads
`ClosureDebt` or calls either member of the W2 pair; therefore W2 extends an
existing delivery-decision path rather than inventing one or drafting around an
already-wired pair.

## Ground survey — bytes at `23acdea`

### G1. The dormant pair and the distinct item-28 relocation

The W2 pair is the two public nonzero-closure-debt selectors over the same
`NonzeroDebtCursorEpisode`:

| seam | exact input/output and computation | caller state at the pin |
|---|---|---|
| obligation-aware path: `apply_nonzero_participant_ack_with_obligations` | Accepts presented identity, binding, receiving epoch, `ParticipantAck`, `RecipientAckObligations`, and `NonzeroDebtCursorEpisode`; returns `NonzeroParticipantAckDecision`. It selects `AckAvailability::Obligations` and delegates to the common selector (`crates/liminal-protocol/src/lifecycle/operations/nonzero_participant_ack.rs:285-321`). | `git grep -n -w apply_nonzero_participant_ack_with_obligations -- '*.rs'` finds only its definition and the two re-export lines at `crates/liminal-protocol/src/lifecycle/operations/nonzero_participant_ack.rs:291-307`, `crates/liminal-protocol/src/lifecycle/operations/mod.rs:69-73`, and `crates/liminal-protocol/src/lifecycle/mod.rs:230-235`: **zero production callers and zero test callers**. |
| scalar sibling: `apply_nonzero_participant_ack` | Accepts the same authority, request, and episode but a scalar `contiguously_available_through`; returns the same decision type. It selects `AckAvailability::Contiguous` and delegates to the same selector (`crates/liminal-protocol/src/lifecycle/operations/nonzero_participant_ack.rs:247-283`). | `git grep -n -w apply_nonzero_participant_ack -- '*.rs'` finds definition/re-exports and protocol tests only; the definition is at `crates/liminal-protocol/src/lifecycle/operations/nonzero_participant_ack.rs:267-283` and representative test calls are at `crates/liminal-protocol/src/lifecycle/operations/nonzero_participant_ack_tests.rs:289-316`: **zero production callers**. |

The common selector owns lookup precedence, aggregate validation, clone-before-
selection, and the single commit/refusal mapping. Only its availability arm
differs: the scalar route calls `NonzeroDebtCursorEpisode::acknowledge`, while
the obligation route calls
`NonzeroDebtCursorEpisode::acknowledge_with_obligations`
(`crates/liminal-protocol/src/lifecycle/operations/nonzero_participant_ack.rs:314-379`).
The lower obligation method admits a forward endpoint only when sealed
recipient testimony contains it; absent endpoints are gaps, and testimony
context disagreement is an authorization/invariant error
(`crates/liminal-protocol/src/lifecycle/cursor_facts.rs:647-675`). The scalar
lower method instead caps its scalar by the episode high watermark and admits
any endpoint at or below that value
(`crates/liminal-protocol/src/lifecycle/cursor_facts.rs:613-645`).

Item 28 is deliberately not this pair. Production `ConversationAuthority::
apply_ack` seals `RecipientAckObligations` from the outbox and routes the
**zero-debt** selector `apply_participant_ack_with_obligations`
(`crates/liminal-server/src/server/participant/production/ops_acks.rs:29-57,93-133`).
That function has its own production caller at `ops_acks.rs:122-128` and its
contract is defined in
`crates/liminal-protocol/src/lifecycle/operations/participant_ack.rs:242-330`.
The similarly named scalar `apply_participant_ack` is a different zero-debt
Unit 1 boundary (`participant_ack.rs:210-239`). This byte distinction confirms
the ledger's item-28 assertion.

### G2. Where participant delivery is decided today

There are two delivery pumps, but only one is W2's seam. Generic subscription
`Frame::Deliver` is independently drained by `service_subscriptions`; it polls
subscription inboxes, owns a separate budget, and does not own participant
acknowledgements (`crates/liminal-server/src/server/connection/delivery.rs:83-181`).
W2 does not attach there.

Participant `ServerPush` delivery is decided in this chain:

1. A semantic request completes, then `InstalledParticipantService::handle`
   calls `notify_ready` for its conversation
   (`crates/liminal-server/src/server/participant/dispatch.rs:522-558`).
   `notify_ready` resolves currently bound incarnations and coalesces the
   conversation into each exact live connection inbox
   (`crates/liminal-server/src/server/participant/dispatch.rs:462-476`).
2. The registry holds weak inbox/waker handles and fires READY only on the
   empty-to-nonempty transition
   (`crates/liminal-server/src/server/participant/publication.rs:267-305,338-382`).
3. The TCP slice runs participant pushes after inbound/reply work and before
   generic subscriptions and socket drain
   (`crates/liminal-server/src/server/connection/process.rs:136-200,257-286`).
   The WebSocket route calls the same transport-neutral pump; the pump itself
   shares only `DeliverySink` across transports
   (`crates/liminal-server/src/server/connection/participant_delivery.rs:1-23,150-164`).
4. `service_one_conversation` resumes an exact held head first; otherwise its
   sole semantic decision call is `InstalledParticipantService::next_publication`
   (`crates/liminal-server/src/server/connection/participant_delivery.rs:341-373`).
   Production acquires the one conversation cell, verifies a binding for the
   calling connection incarnation, derives offered progress from the exact
   binding or durable ack, and asks `ConversationOutbox::delivery_after` for the
   next obligation (`crates/liminal-server/src/server/participant/production/handler_semantic.rs:179-220`).
5. Only successful enqueue records volatile offered progress. Current-room
   pressure retains the exact encoded head; oversize is typed; durable ack is
   untouched (`crates/liminal-server/src/server/connection/participant_delivery.rs:374-407`).

The exact W2 seam is therefore **inside production `next_publication`, before
`delivery_after` returns a publication**, not in socket code, the registry, or
the generic subscription pump. That point already holds the conversation lock
and sees binding, durable ack, outbox, and selected recipient in one authority
snapshot (`handler_semantic.rs:185-219`).

### G3. Where obligation debt is recorded and readable

The term has two related but non-interchangeable byte representations:

* **Durable recipient obligations** are owned solely by
  `ConversationOutbox`: record installation adds per-recipient sequence entries
  and increments `live_recipient_obligations`; ack and permanent retirement
  discharge entries, reclaim empty live payload, and recompute each recipient's
  next live sequence
  (`crates/liminal-server/src/server/participant/production/outbox.rs:274-307,310-419`).
  Delivery reads the least live entry after offered progress through
  `delivery_after`, while ack selection seals the complete durable testimony
  through `recipient_ack_obligations`
  (`crates/liminal-server/src/server/participant/production/outbox.rs:421-483`).
* **Protocol closure debt** is the nonzero `ClosureDebt` carried by
  `NonzeroDebtCursorEpisode`; its exact value is readable through `debt`, and
  the episode also owns participant cursors, retained suffix, floor, and cursor
  facts (`crates/liminal-protocol/src/lifecycle/cursor_facts.rs:580-611` and
  `:552-556`). A server `CursorEpisodeRepository` can durably record a start
  containing that debt and replay scalar ack commands
  (`crates/liminal-server/src/server/participant/cursor_repository.rs:20-65,98-221`).
  Exact-name grep finds `CursorEpisodeRepository`, `CursorEpisodeStart`, and
  `CursorAckCommand` only in that module and
  `cursor_repository_tests.rs`; it is not installed in production authority.

Thus current production can read recipient-obligation debt under the
conversation lock but has no authoritative installed bridge from the live
frontier's closure state to `next_publication`. W2 must add that bridge as part
of the arm; it may not infer closure debt from outbox counts.

### G4. W1b adjacency

W1b has landed four fate classes in the production authority: Died and Detached
primary source rows, followed where selected by Ordinary or Recovered
completion. The completion path consumes its pending move-only fate token,
measures through protocol selectors, and appends the specific row after the
owning Died source (`crates/liminal-server/src/server/participant/production/binding_fate_completion.rs:1-20,38-122`).
Connection fate is routed as a bounded `ConnectionFateWorkItem`; post-Open
incompleteness is the process-wide `ParticipantServiceFatal::
ConnectionFateIntentIncomplete`, surfaced by `ParticipantSemanticError::
ServiceFatal` (`crates/liminal-server/src/server/participant/dispatch.rs:99-158,203-234`).

This is adjacent, not an alternate W2 owner. The installed service delegates
`handle_connection_fate` directly and does not call the ordinary request
`notify_ready` wrapper (`crates/liminal-server/src/server/participant/dispatch.rs:498-510,522-544`).
W2 must therefore compose fate-produced debt changes into its explicit event
surface, while leaving W1b source selection, finalizer order, and fatal policy
unchanged. The crash/restart ruling below specifies that composition.

## 1. The unit — single owner and exact seam

## 2. Event-driven only — the no-polling law

## 3. Debt semantics

## 4. Both paths, one truth

## 5. Idle cost and honesty

## 6. Crash and restart

## 7. Acceptance oracle census

## 8. Scope walls

## 9. Revision record
