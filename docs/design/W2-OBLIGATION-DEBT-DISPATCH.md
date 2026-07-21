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

### 1.1 Ruled shape

W2 adds one **obligation-debt dispatch arm** to `ConversationAuthority`. It is
not a worker, queue, scheduler, or second pump. It is a protocol-owned decision
called synchronously by the existing participant pump's production
`next_publication` implementation immediately before today's
`ConversationOutbox::delivery_after` selection. The current call and selection
are at
`crates/liminal-server/src/server/connection/participant_delivery.rs:341-373`
and
`crates/liminal-server/src/server/participant/production/handler_semantic.rs:179-220`.

The production owner SHALL keep a coupled `ObligationDebtDispatchState` beside
the existing outbox and live frontier:

* `Clear` means protocol `ClosureState::Clear` and no nonzero episode;
* `Owed(NonzeroDebtCursorEpisode)` means the episode's `ClosureDebt` exactly
  equals the live frontier's `ClosureState::Owed` debt and both describe the
  same conversation, participant cursors, retained suffix, and floor; and
* no absent, stale, or independently reconstructed episode is permitted.

The bridge is installed only from protocol-owned post-transition/replay output.
The live frontier already exposes its complete closure accounting at
`crates/liminal-protocol/src/lifecycle/operations/live_frontier.rs:160-175`,
and the episode exposes debt, suffix, and facts at
`crates/liminal-protocol/src/lifecycle/cursor_facts.rs:552-611`; server code
SHALL compare those typed values, not derive debt from an outbox count.

The protocol crate SHALL own a total `decide_obligation_debt_dispatch` selector
with `Permit`, `Defer(DebtDispatchDeferral)`, and
`Invariant(DebtDispatchInvariant)` results. Production `next_publication`
provides one locked snapshot: closure/episode state, selected participant and
binding, durable ack, and the outbox's least next obligation. Only `Permit`
continues to publication construction.

### 1.2 Ownership, lock, and ordering law

| decision | permanent ruling | acceptance source |
|---|---|---|
| single owner | The existing per-conversation `Mutex<Option<ConversationAuthority>>` is the only mutable owner. No dispatch mutex, debt cache, channel worker, or background task is added. Production already takes this cell around binding/outbox selection (`crates/liminal-server/src/server/participant/production/handler_semantic.rs:185-219`). | `obligation_debt_dispatch_seams_before_delivery_after_under_one_owner` |
| atomic decision | Hold the conversation lock from reading closure/episode state through selecting or deferring the exact next durable obligation. Release it before encoding, socket work, registry notification, or READY firing. The registry currently clones weak handles before locking an inbox (`crates/liminal-server/src/server/participant/publication.rs:338-378`); W2 SHALL NOT reverse that order. | `obligation_debt_dispatch_never_unlocks_between_debt_and_obligation_selection` |
| pump order | Preserve answer/control → participant push → generic subscription → outbound drain. The existing TCP order is byte-visible at `crates/liminal-server/src/server/connection/process.rs:159-200`; both transports continue to use the one participant pump (`crates/liminal-server/src/server/connection/participant_delivery.rs:1-23`). | `obligation_debt_dispatch_preserves_pump_order_on_both_transports` |
| held head | A held frame is revalidated against exact binding **and current debt dispatch permission** before enqueue. Today's held path revalidates binding at `crates/liminal-server/src/server/connection/participant_delivery.rs:349-359`; W2 extends that same check and never trusts a pre-fate debt verdict. | `held_obligation_revalidates_binding_and_debt_before_offer` |

## 2. Event-driven only — the no-polling law

**Board-binding law: redesign it to be TOLD.** The arm may run only because a
typed debt-change event, a socket-writable event for an already-held permitted
head, or startup recovery handed it known work. It SHALL NOT discover debt by a
periodic probe, timer, scheduler slice, outbox scan, conversation sweep, retry
sleep, or self-requeue after `Defer`.

### 2.1 Exact wake vocabulary and producers

`ObligationDebtChanged { conversation_id, cause }` is a coalescing notification,
not durable authority. Its cause is exhaustive:

| cause | producer and durability point | recipients told |
|---|---|---|
| `Published` | The operation/outbox owner emits after the v2 source and complete outbox `Produced` batch are flushed and installed. Existing outbox installation creates obligations at `crates/liminal-server/src/server/participant/production/outbox.rs:262-307`; wake-before-flush is forbidden. | Re-resolve current bound recipients with live obligations under the conversation lock, then release the lock and notify their exact incarnations. |
| `Acknowledged` | The ack owner emits after the selected zero- or nonzero-debt ack row, outbox `AckAdvanced`, episode transition, and installed durable cursor all agree. Existing zero-debt ack seals outbox testimony before selection at `crates/liminal-server/src/server/participant/production/ops_acks.rs:41-57`; W2 adds the parallel nonzero commit and one post-install tell. | The acknowledging binding if another obligation is now eligible, plus any binding whose dispatch permission changed at the same debt transition. |
| `ExpiredOrRetired` | There is no wall-clock obligation TTL at the pin. The only W2 producer is durable permanent retirement/discharge (Leave or a W1b terminal/finalizer effect) after its source rows and outbox discharge install. Retirement currently removes every recipient entry at `crates/liminal-server/src/server/participant/production/outbox.rs:363-379`. A future real expiry may join this cause only at its own durable commit point; W2 does not add a timer. | Surviving current bindings whose next obligation or debt permission changed; never the retired/stale incarnation. |
| `DebtTransitioned` | Any protocol operation, marker progression, or W1b fate/finalizer that changes `ClosureState` emits only after the operation/fate source and resulting frontier/episode are durable and installed. W1b completion already orders its specific row after Died source ownership (`crates/liminal-server/src/server/participant/production/binding_fate_completion.rs:38-122`). | Exact current bindings selected from the poststate. A transition to zero MUST tell all conversations previously deferred by that episode. |
| `RecoveryReady` | Cold replay/reconciliation emits once per restored conversation after base rows, outbox rows, W1b fate rows, closure state, and the episode agree, but before participant service publication, scheduler/listener publication, or admission. | Every restored current binding with a permitted durable obligation; no registry lookup is required before a live connection later registers, so attach/registration also performs the one exact replay tell. |

A producer may coalesce multiple causes for one conversation before the inbox is
drained. The inbox already coalesces conversation ids and fires READY only on
empty→nonempty (`crates/liminal-server/src/server/participant/publication.rs:338-382`).
Cause is diagnostic; every decision rereads durable authority.

### 2.2 Wake decisions and absence proofs

| condition | required action | acceptance source |
|---|---|---|
| publish creates first/current obligation | Tell exact live bindings once after flush; a parked connection reaches the existing READY path. | `published_obligation_tells_exact_live_dispatch_once` |
| ack, retirement, or closure transition changes eligibility | Tell from the committing owner; duplicate causes coalesce, and stale incarnations receive nothing. | `ack_retirement_and_debt_transition_tell_without_polling` |
| decision is `Defer` | Consume the ready item without encoding, budget debit, held head, or requeue. Only a later typed cause may wake it. | `deferred_debt_does_not_self_requeue_or_advance_idle_slices` |
| source inspection | Production contains no interval/tick/sleep/sweep path into `decide_obligation_debt_dispatch`; the only call graph is inbox work → existing pump → `next_publication`. | `dispatch_source_has_no_timer_sweep_or_periodic_probe` |
| execute-to-wait race | Preserve both TCP and WebSocket final probes. The TCP probe currently checks participant inbox work before `Wait` (`crates/liminal-server/src/server/connection/process.rs:652-694`), and READY fires on the empty→nonempty edge (`crates/liminal-server/src/server/participant/publication.rs:364-381`). | `debt_tell_between_drain_and_wait_is_seen_by_final_probe` |

Requeue caused by a known nonempty queue and remaining/budget-deferred permitted
work is not polling. Requeue after debt `Defer` is polling and is forbidden.
Socket writable readiness may resume an existing held permitted head, but §1.2
requires a fresh binding/debt check first.

## 3. Debt semantics

### 3.1 Total decision table

| locked prestate | decision | effect |
|---|---|---|
| `Clear`, no next durable recipient obligation | `Defer(NoObligation)` | No work is retained or requeued; a later `Published`/attach tell is required. |
| `Clear`, exact current binding and next durable obligation | `Permit` | Preserve today's least-sequence selection and offered-progress rule from `handler_semantic.rs:193-218`. Zero-debt ack remains the distinct item-28 path. |
| `Owed(episode)`, next obligation is in the episode retained suffix and strictly after that participant's episode cursor | `Permit` | Schedule that exact obligation. Its later ack MUST route through the obligation-aware nonzero selector. `NonzeroDebtCursorEpisode::retains` defines suffix membership (`crates/liminal-protocol/src/lifecycle/cursor_facts.rs:591-605`). |
| `Owed(episode)`, next obligation is above the episode candidate high watermark | `Defer(BeyondDebtHighWatermark)` | Do not offer newer work while the nonzero episode seals an older retained suffix. A durable episode/debt transition must tell the arm. |
| `Owed(episode)`, outbox obligation is below the retained floor, at/below durable cursor, names another participant/binding, or episode debt differs from closure debt | `Invariant(...)` | Fail the participant semantic service loudly; do not turn durable disagreement into a wire refusal or silently fall back to zero debt. |
| poststate transitions `Owed` → `Clear` | `Clear` plus `DebtTransitioned` | Remove the episode only with the durable transition, tell all previously deferred current bindings, and resume from outbox durable ack—not volatile old-binding offer state. |

`ClosureDebt` is lifecycle state, not a byte/CPU token bucket. Permit does not
subtract debt on socket enqueue; only a protocol transition may change it.
Likewise, recipient-obligation count is not a substitute for closure debt. The
outbox separately tracks and bounds live obligation count and encoded bytes
(`crates/liminal-server/src/server/participant/production/outbox.rs:274-305,538-592`).

### 3.2 Pressure, budget, and refusal typing

The arm consumes no new budget. A fresh **permitted** push debits the existing
shared `UNIT2_PUSH_SLICE_BUDGET = 32`; participant and observer work already
share that counter (`crates/liminal-server/src/server/connection/participant_delivery.rs:22-23,150-178`).
A debt deferral consumes zero encode budget. Current-room pressure remains an
exact held head, and oversize remains a typed pump fault
(`crates/liminal-server/src/server/connection/participant_delivery.rs:46-88,374-407`).

| boundary | type | acceptance source |
|---|---|---|
| lifecycle says not yet dispatchable | `DebtDispatchDeferral`; internal scheduling result, never a client error | `nonzero_debt_defers_outside_retained_suffix_and_permits_member` |
| durable closure/episode/outbox disagreement | `DebtDispatchInvariant` → `ParticipantSemanticError::Internal`, or the already-latched service fatal | `debt_dispatch_invariant_never_falls_back_or_fabricates_wire_refusal` |
| sink lacks current room | existing held-head result; not debt and not teardown | `debt_refusal_and_pressure_holdback_remain_distinct` |
| zero transition | one told wake and ordinary budgeted continuation | `debt_zero_transition_releases_deferred_obligation` |

## 4. Both paths, one truth

### 4.1 Production disposition

The **obligation-aware**
`apply_nonzero_participant_ack_with_obligations` is the authoritative nonzero
ack path. Under the conversation lock, the owner seals
`RecipientAckObligations` from the same outbox prestate used by dispatch, passes
the installed episode, and applies a commit only after its operation and outbox
barriers succeed. Server code does not repeat endpoint membership; the protocol
function owns it (`crates/liminal-protocol/src/lifecycle/operations/nonzero_participant_ack.rs:285-307`).

The scalar sibling `apply_nonzero_participant_ack` SHALL also gain a real
production call, but never authority over sparse obligations. For every
obligation-aware result whose classification did not depend on rejecting an
absent endpoint—committed exact endpoint and pre-availability lookup/authority
refusals—the arm invokes the scalar sibling on the **same immutable prestate**,
using the sealed outbox's reconciled scalar audit. Exact decision inequality is
`DebtDispatchInvariant::ScalarDivergence`; it fails before append or mutation.
An obligation-only `AckGap` is not sent through the scalar audit because the
scalar type cannot represent legal internal sequence holes. That is a loud type
boundary, not permission to choose the scalar result.

This conditional conformance call is the scalar sibling's production consumer;
it removes dormancy without weakening Unit 2 endpoint membership. The common
implementation already funnels both availability forms through one lookup,
validation, mutation, floor, and decision mapping
(`crates/liminal-protocol/src/lifecycle/operations/nonzero_participant_ack.rs:309-379`).
The build SHALL add a protocol helper that returns the scalar audit value
alongside sealed obligations so the server cannot guess it, mirroring the
current outbox pair at
`crates/liminal-server/src/server/participant/production/outbox.rs:447-483`.

### 4.2 Mandatory non-divergence floor

| shared fixture | both calls required | equality | acceptance source |
|---|---|---|---|
| one nonzero episode, exact current binding, sealed obligations, and an ack ending on an actual obligation | obligation-aware call and scalar sibling receive the same identity, binding, epoch, request, cloned episode prestate, and reconciled scalar; compare the complete `NonzeroParticipantAckDecision` and committed episode/frontier effect | byte/typed equality; either difference is a failure | `nonzero_debt_obligation_and_scalar_commit_cannot_diverge` |
| the same fixture with one pre-availability authority refusal injected | both calls receive the same refused prestate and request; neither may mutate | exact refusal discriminant and unchanged owner equality | `nonzero_debt_obligation_and_scalar_refusal_cannot_diverge` |

The tests SHALL call both public entry points, not merely their shared private
helper. Sparse non-obligation endpoint coverage remains separate and must prove
the obligation path returns `AckGap` while the scalar result is never selected;
that boundary is pinned by
`nonzero_debt_sparse_gap_never_selects_scalar_fallback`.

## 5. Idle cost and honesty

### 5.1 Permanent idle bound

With no `ObligationDebtChanged`, no socket readiness, no inbound request, no
pending reply, and no held head, W2 performs **zero scheduler slices, zero debt
selector calls, zero authority-lock acquisitions, zero outbox probes, and zero
wake fires** after the connection parks. Memory is one coupled debt state per
loaded conversation plus the already bounded/coalesced conversation id in an
existing connection inbox; there is no per-tick or per-idle-allocation cost.
The named pin is
`obligation_debt_dispatch_idle_has_zero_slice_probe_and_wake_growth`.

### 5.2 Loud tradeoffs and design refusals

| item | permanent disclosure |
|---|---|
| commit-path duplicate decision | Eligible nonzero commits and common pre-availability refusals run both public selectors on cloned immutable input. This is intentional bounded CPU for ledger-mandated non-divergence; it performs no second append and cannot choose the scalar result. |
| coalescing | Multiple debt changes may collapse to one conversation wake. This sacrifices cause-by-cause observability, not correctness, because the decision rereads one locked durable poststate. |
| duplicates | Crash or reattach may offer the same unacked obligation again. Current durable ack, not socket offer, is authority; Unit 2 already records offered progress as volatile per binding (`crates/liminal-server/src/server/participant/publication.rs:19-34`). |
| no wall-clock expiry | The pin has retirement discharge but no obligation TTL. W2 refuses to invent a periodic expiry scan. Any future expiry requires a durable event producer and the same TOLD interface. |
| no periodic anything | Any implementation containing a debt timer, sweep, interval, unconditional scheduler continuation, or deferred self-requeue is a **design refusal** and must be escalated to the board; it cannot be accepted under this brief. |

### 5.3 Gate-freshness rider

W2 does not move transport authentication or participant request gating. It
does touch a post-gate delivery claim: current binding eligibility. Therefore
no binding, debt, or permission verdict may survive unlock/relock or held-head
resumption without revalidation. The existing held path already rechecks exact
binding (`crates/liminal-server/src/server/connection/participant_delivery.rs:349-359`);
W2 extends it to debt and pins the result with
`held_obligation_revalidates_binding_and_debt_before_offer` from §1.2. No cached
claim may bypass W1b fate rows or `ParticipantServiceFatal`.

## 6. Crash and restart

Durable rows and reconstructed protocol state are authority; wake notifications,
ready inboxes, offered cursors, and held frames are volatile. Startup SHALL
complete existing base/outbox reconciliation, W1b fate replay/finalization,
closure-state replay, and nonzero-episode replay before it emits
`RecoveryReady` or publishes participant service. The current cursor repository
replays scalar commands from an append-only start/ack stream
(`crates/liminal-server/src/server/participant/cursor_repository.rs:145-221`),
but W2 SHALL replace/integrate that dormant island with the conversation
authority's operation ordering and obligation-aware replay; it may not recover a
second independent episode and race it against the live frontier.

### 6.1 Crash-cut table

| cut | restart obligation | acceptance source |
|---|---|---|
| before a debt/outbox/ack/fate row flush | Restore the prior state; no tell or delivery attributable only to the uncommitted candidate. | `crash_before_debt_flush_restores_prior_dispatch_state` |
| after durable change and install, before `ObligationDebtChanged` | Replay reconstructs the new coupled state and one `RecoveryReady`/registration tell makes eligible work visible before admission. | `crash_after_debt_flush_before_tell_recovers_ready_work` |
| after tell, before enqueue | Volatile ready work may vanish; recovery reselects the same least unacked obligation from durable ack, with no skip. | `crash_after_tell_before_enqueue_replays_same_obligation` |
| after enqueue, before ack commit | Offer testimony does not discharge debt. Restart/reattach may duplicate the same obligation, and the obligation-aware ack still accepts the durable endpoint. | `crash_after_enqueue_before_ack_reoffers_and_accepts_endpoint` |
| after nonzero ack row but before outbox/episode coupling is fully durable | The ordered barrier/reconciliation either completes the exact coupled commit or fails startup loudly; it never publishes one side. | `crash_between_nonzero_ack_barriers_reconciles_one_coupled_commit` |

### 6.2 W1b connection-fate composition

When a connection fate occurs with a held or in-flight publication, W1b remains
first owner of Died/Detached and any Ordinary/Recovered finalizer. The W2 arm
receives only the resulting post-flush `ExpiredOrRetired`/`DebtTransitioned`
tell. A held frame for the old binding is discarded on fresh validation; an
unacked durable obligation survives for a later current binding unless the
fate's durable retirement semantics discharged it. Current held publication
already checks binding before offer
(`crates/liminal-server/src/server/connection/participant_delivery.rs:349-359`),
and current reattach selection restarts from durable ack when binding epoch
changes (`crates/liminal-server/src/server/participant/production/handler_semantic.rs:193-218`).

If W1b latches `ParticipantServiceFatal::ConnectionFateIntentIncomplete`, the
arm MUST produce no publication, wake-induced mutation, or scalar audit until
startup/non-crash recovery clears the service-level condition. The fatal is
already a process-wide typed semantic error
(`crates/liminal-server/src/server/participant/dispatch.rs:125-158`). W2 neither
catches it nor translates it to a participant wire response.

| fate boundary | acceptance source |
|---|---|
| stale held/in-flight head is rejected, durable unacked work follows the post-fate binding/retirement result | `connection_fate_drops_stale_head_and_replays_from_durable_ack` |
| Died/Detached plus Ordinary/Recovered replay completes before a recovery tell and cannot double-present a delivery | `w1b_fate_replay_precedes_recovery_ready_dispatch` |
| latched participant fatal blocks dispatch and is never downgraded | `participant_service_fatal_blocks_obligation_dispatch` |

## 7. Acceptance oracle census

## 8. Scope walls

## 9. Revision record
