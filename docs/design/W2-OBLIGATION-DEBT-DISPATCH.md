# W2 — obligation-debt dispatch arm

**Revision r4 — design-first brief, 2026-07-21**

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

Item 28 is deliberately not this pair. Production
`ConversationAuthority::apply_ack` seals `RecipientAckObligations` from the
outbox and routes the
**zero-debt** selector `apply_participant_ack_with_obligations`
(`crates/liminal-server/src/server/participant/production/ops_acks.rs:29-57,93-133`).
That function has its own production caller at
`crates/liminal-server/src/server/participant/production/ops_acks.rs:122-128`
and its contract is defined in
`crates/liminal-protocol/src/lifecycle/operations/participant_ack.rs:242-330`.
The similarly named scalar `apply_participant_ack` is a different zero-debt
Unit 1 boundary
(`crates/liminal-protocol/src/lifecycle/operations/participant_ack.rs:210-239`).
This byte distinction confirms
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
snapshot
(`crates/liminal-server/src/server/participant/production/handler_semantic.rs:185-219`).

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
  `crates/liminal-protocol/src/lifecycle/cursor_facts.rs:552-556`). A server
  `CursorEpisodeRepository` can durably record a start
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

The production owner SHALL install one protocol-owned
`ObligationDebtDispatchState` **in place of** the bare frontier field, not beside
it:

* `Clear(LiveFrontierOwner)` means protocol `ClosureState::Clear` and carries no
  episode;
* `Owed(CoupledObligationDebtOwner)` move-couples one `LiveFrontierOwner` and one
  `NonzeroDebtCursorEpisode`, even when no recipient obligation is presently
  dispatchable; and
* no public constructor, server-side splice, absent episode, or independently
  replayed episode is permitted.

The installed variant SHALL follow the operation's **resulting**
`ClosureState`, never its route name. A returned `Clear` always consumes any old
episode and installs `Clear(frontier)`; a returned `Owed` always constructs or
updates the complete coupled episode and installs `Owed(coupled)`. Initial
enrollment is proof that a route cannot imply a variant: its closure calculation
selects either `Clear` or `Owed`
(`crates/liminal-protocol/src/lifecycle/enrollment_closure.rs:431-445`). It is
also legal for first enrollment to return `Owed` with no next obligation for
the enrolling participant. The `Enrolled` projection identifies that
participant as sender
(`crates/liminal-server/src/server/participant/production/outbox_projection.rs:60-72`),
and recipient construction excludes the sender
(`crates/liminal-server/src/server/participant/production/outbox_projection.rs:365-372`).
`Owed` therefore means closure debt exists; it does not assert that every bound
participant has a current outbox recipient entry.

This is a necessary r2 correction, not a claim about landed bytes. At the pin,
`LiveFrontierOwner` contains only claim frontiers, closure accounting, retained
charges, and a retained limit
(`crates/liminal-protocol/src/lifecycle/operations/live_frontier.rs:65-71`),
while hard observer progress is separately owned by production authority
(`crates/liminal-server/src/server/participant/production/state.rs:169-218`).
The episode additionally owns its observer position, candidate watermark,
capacity floor, computed floor, participant cursors, and cursor facts
(`crates/liminal-protocol/src/lifecycle/cursor_facts.rs:295-314`). A comparison
of debt scalars cannot make those owners exact.

The protocol representation SHALL therefore widen the episode's participant
entry from bound-only `BoundParticipantCursor`—whose bytes contain only active
epoch and cursor (`crates/liminal-protocol/src/lifecycle/cursor_facts.rs:156-210`)—
to a private binding-tagged entry that can preserve the same permanent
participant, cursor, and facts across `Bound` and `Detached(last_epoch)`. This
matches the frontier's already-tagged binding domain
(`crates/liminal-protocol/src/lifecycle/claim_frontier.rs:76-125`). Only the
`Bound(exact_epoch)` arm authorizes an ack or publication. Temporary binding
fates preserve participant-scoped facts; permanent `Left` removes that
participant and all its facts only after its retirement discharge commits.

### 1.1.1 Mandatory coupled transition bridge

Every operation that can touch either half consumes `ObligationDebtDispatchState`
and returns one state. Production may not take and reinstall a bare
`LiveFrontierOwner` while the state is `Owed`. Every row obeys the variant law
above: an exact `Clear` result has no episode, while an exact `Owed` result has a
complete episode. The bridge is exhaustive:

| route | protocol-owned coupled output | fact/binding disposition | acceptance source |
|---|---|---|---|
| ordinary or fenced enrollment/attach/rebind | Branch on the operation's resulting closure: `Clear` installs only its exact frontier; `Owed` installs that frontier plus a complete episode with `Bound(new_epoch, durable_cursor)`. Rebind proves the same permanent participant and nonregressing cursor. First `Owed` enrollment is valid even when sender filtering leaves no next obligation. | Preserve that participant's facts across temporary detach; a first enrollment starts empty facts. Never fabricate an episode on the `Clear` branch. | `enrollment_clear_or_owed_and_no_obligation_are_total`; `temporary_fate_preserves_cursor_facts_and_rebinds_exact_epoch` |
| Died | Install the exact Died frontier/closure output. A `Clear` output has no episode; an `Owed` output carries `Detached(last_dead_epoch, cursor)` in its complete episode. | Preserve cursor and all facts on the `Owed` branch; reject an epoch or cursor mismatch before append. | `coupled_debt_owner_covers_every_w1b_fate_and_finalizer_route` |
| explicit/orderly Detached | Install the exact Detached frontier/closure output. A `Clear` output has no episode; an `Owed` output carries `Detached(last_epoch, cursor)`. | Same conditional preservation and mismatch refusal as Died. | `coupled_debt_owner_covers_every_w1b_fate_and_finalizer_route` |
| Ordinary | Consume the exact Died-owned Ordinary fate, verify the detached participant/epoch, and install its resulting frontier floor, charges, closure accounting, and closure-selected owner variant as one value. | On `Owed`, install episode floor/cap and observer input and preserve keyed facts except where the floor transition marks an already-covered fact; on `Clear`, consume the episode. | `coupled_debt_owner_covers_every_w1b_fate_and_finalizer_route` |
| Recovered | Consume the exact recovered-epoch fate and install its resulting frontier floor, charges, closure accounting, and closure-selected owner variant as one value. | On `Owed`, the dead recovered epoch remains Detached with facts preserved for exact rebind; on `Clear`, no episode survives. | `coupled_debt_owner_covers_every_w1b_fate_and_finalizer_route` |
| each enclosing Ordinary/Recovered finalizer, including pending storage/cursor-release completion | Consume the finalizer authority and return both the updated frontier and updated episode. If the protocol returns `ClosureState::Clear`, consume the episode and return `Clear`; if debt remains, return a fully updated `Owed`. | No server field copy or scalar debt patch is allowed. | `coupled_debt_owner_covers_every_w1b_fate_and_finalizer_route` |
| marker ack | Preserve the input owner variant because marker selection has no closure-state input and frontier accounting preserves the existing state. `Clear(frontier)` becomes `Clear(updated_frontier)` with no episode. `Owed(coupled)` advances the matching frontier and episode participant cursor/facts and remains `Owed`. | Advance the exact protocol participant cursor and consume covered facts only on the `Owed` branch; never create an episode on `Clear`. Durable outbox ack remains unchanged and is reconciled by §3.1. | `marker_ack_preserves_owner_variant_and_reconciles_dispatch_cursor` |
| nonzero normal ack | Apply the existing `NonzeroParticipantAckCommit`'s cursor/floor result to both halves in one barrier, then obey its resulting closure: consume the episode on `Clear`, otherwise install the complete resulting `Owed`. | Existing obligation-aware fact record/consume semantics remain authoritative. | `nonzero_debt_ack_row_replays_obligation_aware_commit` |
| `Left` | Install the frontier retirement and outbox discharge, then remove the participant and its facts from the episode; clear the episode only if closure state clears. | Permanent removal only; no later rebind. | `coupled_debt_owner_covers_every_w1b_fate_and_finalizer_route` |
| any remaining frontier/observer/floor/debt transition | A total protocol method returns matching frontier, observer, floor/cap, participant, and fact state or a typed invariant. | No route may preserve one half implicitly. | `coupled_debt_owner_covers_every_w1b_fate_and_finalizer_route` |

The marker split is mandatory. `apply_marker_ack` receives identity, binding,
epoch, request, and marker proof—but no closure state
(`crates/liminal-protocol/src/lifecycle/operations/marker_ack.rs:208-214`)—and
marker frontier accounting copies the existing closure state
(`crates/liminal-protocol/src/lifecycle/operations/live_frontier/state.rs:61-75`).
Route-name inference would therefore manufacture an episode on a legal `Clear`
commit.

The need for this bridge is byte-grounded: W1b's landed transition updates only
frontiers, retained charges, and closure accounting
(`crates/liminal-protocol/src/lifecycle/operations/live_frontier/binding_fate_transition.rs:15-107`),
and production Ordinary/Recovered completion appends then reinstalls only that
owner (`crates/liminal-server/src/server/participant/production/binding_fate_completion.rs:192-235,239-292`). No landed episode API removes, detaches, rebinds, or applies
those floor transitions. W2 adds those protocol-owned consuming APIs and their
storage form; it does not pretend the current types already compose.

The protocol crate SHALL also own a total
`decide_obligation_debt_dispatch` selector with `Permit`,
`Defer(DebtDispatchDeferral)`, and `Invariant(DebtDispatchInvariant)` results.
Production `next_publication` provides one locked coupled state, selected
participant/binding, the reconciled cursor defined by §3.1, and the outbox's
least obligation strictly after that cursor. Only `Permit` continues to
publication construction.

### 1.2 Ownership, lock, and ordering law

| decision | permanent ruling | acceptance source |
|---|---|---|
| single owner | The existing per-conversation `Mutex<Option<ConversationAuthority>>` remains the only server mutable owner; its bare `frontier` field is replaced by the move-only protocol `ObligationDebtDispatchState`. No dispatch mutex, debt cache, reverse index, channel worker, or background task is added. Production already takes this cell around binding/outbox selection (`crates/liminal-server/src/server/participant/production/handler_semantic.rs:185-219`). | `obligation_debt_dispatch_seams_before_delivery_after_under_one_owner` |
| atomic decision | Hold the conversation lock from reading closure/episode state through selecting or deferring the exact next durable obligation. Release it before encoding, socket work, registry notification, or READY firing. The registry currently clones weak handles before locking an inbox (`crates/liminal-server/src/server/participant/publication.rs:338-378`); W2 SHALL NOT reverse that order. | `obligation_debt_dispatch_never_unlocks_between_debt_and_obligation_selection` |
| pump order | Preserve answer/control → participant push → generic subscription → outbound drain. The existing TCP order is byte-visible at `crates/liminal-server/src/server/connection/process.rs:159-200`; both transports continue to use the one participant pump (`crates/liminal-server/src/server/connection/participant_delivery.rs:1-23`). | `obligation_debt_dispatch_preserves_pump_order_on_both_transports` |
| held head | A held frame is revalidated against exact binding **and current debt dispatch permission** before enqueue. Today's held path revalidates binding at `crates/liminal-server/src/server/connection/participant_delivery.rs:349-359`; W2 extends that same check and never trusts a pre-fate debt verdict. | `held_obligation_revalidates_binding_and_debt_before_offer` |

## 2. Event-driven only — the no-polling law

**Board-binding law: redesign it to be TOLD.** The arm may run only because a
committed operation returned a typed dispatch impact, or socket writable
readiness resumed an already-held permitted head. Startup replay and connection
registration are **not** wake sources. The arm SHALL NOT discover debt by a
periodic probe, timer, scheduler slice, outbox scan, conversation sweep, retry
sleep, registration catch-up, or self-requeue after `Defer`.

### 2.1 Exact impact vocabulary and producers

Every semantic operation returns its wire value together with
`DispatchImpact::{Unchanged, Changed { conversation_id, effects }}` from the
conversation owner. `effects` is a nonempty map
`DispatchEffect -> Set<DispatchTarget>`, where `DispatchTarget` is the exact
poststate `(participant_id, binding_epoch)` and therefore carries the current
connection incarnation. An effect's target set may be empty; no true effect may
be dropped merely because no current binding can be told.

The map is constructed under the conversation lock from the operation's exact
prestate, committed transition, and installed poststate. It is returned only
after every relevant operation, outbox, fate/finalizer, and coupled-state row
is flushed and installed. It is **not** inferred later from a request kind or a
poststate reread. After unlock, the wrapper takes the set union of all effect
targets, deduplicates exact targets, and notifies each once. There is no scalar
precedence rule. The eventual pump still rereads durable authority before
selection; that freshness check does not reconstruct lost effects or targets.

| effect | exhaustive producer set and durability point | targets recorded |
|---|---|---|
| `Published` | Any committed `Produced` batch that adds at least one live recipient obligation, after source and batch flush/install. Existing installation creates obligations at `crates/liminal-server/src/server/participant/production/outbox.rs:262-307`. A zero-recipient batch has no `Published` effect. | Exact poststate bound recipients of newly live obligations. |
| `Acknowledged` | A committed zero- or nonzero-debt normal ack after its full barrier, and every committed marker ack after its extension/member/frontier barrier. Existing zero-debt selection seals testimony at `crates/liminal-server/src/server/participant/production/ops_acks.rs:41-57`; marker durability is at `crates/liminal-server/src/server/participant/production/ops_acks.rs:332-367`. | The acknowledging current binding when another obligation or dispatch verdict may now be selected, plus any exact current target whose reconciled dispatch key changed. |
| `BindingChanged` | Committed enrollment/attach/rebind, Died, or Detached after binding and any Owed episode tag agree. | Only exact current poststate bindings affected by the change; never a detached/dead epoch. A committed rebind is the sole replay tell after restart. |
| `EpisodeChanged` | Observer/candidate/cap/debt transition; Ordinary or Recovered; every enclosing finalizer; an Owed marker ack; and any other operation whenever cursor, floor, `H'`, observer, cap, facts, debt, or owner variant changes—**whether or not `ClosureState` changes**. Marker ack preserves accounting state while advancing cursor (`crates/liminal-protocol/src/lifecycle/operations/live_frontier/state.rs:61-75`; `crates/liminal-protocol/src/lifecycle/operations/live_frontier.rs:1203-1250`). W1b floor accounting also preserves closure state (`crates/liminal-protocol/src/lifecycle/operations/live_frontier/state.rs:33-58`), and Ordinary/Recovered have no `Produced` row (`crates/liminal-server/src/server/participant/production/outbox_projection.rs:49-60`), so this effect is mandatory when those poststate fields change. A Clear marker has `Acknowledged`, not a fabricated episode effect. | Exact current poststate bindings whose coupled dispatch key may have changed. |
| `Retired` | Committed `Left` only, after its source batch and permanent outbox discharge install. The source validator selects retirement only for `Left` (`crates/liminal-server/src/server/participant/production/outbox/validation.rs:9-25`), and discharge removes that participant's live obligations (`crates/liminal-server/src/server/participant/production/outbox.rs:363-379`). | Surviving exact current bindings affected by discharge; never the retired identity. |

The effect vocabulary is closed, but a commit may carry any truthful subset:

| overlapping commit | required effect entries; no precedence | notification union | acceptance source |
|---|---|---|---|
| enrollment/attach/rebind | `BindingChanged`; also `Published` iff its `Produced` batch adds a live recipient obligation; also `EpisodeChanged` iff the owner variant, episode, facts, or debt dispatch state changes | Union exact new/current binding targets with produced-recipient and episode-change targets. First Owed enrollment with only its filtered sender has no `Published` entry. | `dispatch_impact_unions_multi_effect_targets` |
| marker ack | `Acknowledged` on both owner variants; also `EpisodeChanged` when Owed episode cursor/facts change | Union and deduplicate; Clear never invents `EpisodeChanged`. | `dispatch_impact_unions_multi_effect_targets`; `marker_ack_preserves_owner_variant_and_reconciles_dispatch_cursor` |
| normal ack | `Acknowledged`; also `EpisodeChanged` for every actual coupled cursor/fact/debt/variant change | Union acknowledging and all newly eligible exact targets. | `dispatch_impact_unions_multi_effect_targets` |
| Died/Detached/Ordinary/Recovered or enclosing finalizer | Include `BindingChanged`, `EpisodeChanged`, and `Published` independently whenever that committed transition has the corresponding effect. | Union all exact surviving poststate targets; omit only effects that did not occur. | `dispatch_impact_unions_multi_effect_targets`; `dispatch_impact_covers_state_preserving_marker_and_w1b_changes` |
| `Left`/terminal commit | `Retired`; also `Published` when the same source adds live survivor obligations; also `EpisodeChanged` when surviving coupled debt/episode state changes; `BindingChanged` only for another binding actually changed by that commit | Union surviving poststate targets. Retirement discharge never targets the retired identity. | `dispatch_impact_unions_multi_effect_targets` |

There is no `Expired` effect and no durable expiry operation at this pin. A
future expiry must first land its own durable operation and then revise this
closed enum; prose anticipation is not a producer.

The current wrapper is expressly replaced. Today
`InstalledParticipantService::handle` calls `notify_ready` after every
successful semantic return for any request with a conversation id, without a
commit/eligibility verdict
(`crates/liminal-server/src/server/participant/dispatch.rs:522-558`). W2 removes
that `request_conversation_id`-driven notification. The wrapper notifies the
union only for `DispatchImpact::Changed`; a `Changed` map whose union is empty
fires no READY. Refusals, no-ops, idempotent replays, and commits with no listed
effect return `Unchanged`. Semantic fixtures default to `Unchanged`, never to
an inferred effect or tell.

A changed conversation may coalesce behind one inbox entry. The inbox already
coalesces conversation ids and fires READY only on empty→nonempty
(`crates/liminal-server/src/server/participant/publication.rs:338-382`).
Coalescing loses per-effect counts after the truthful union is notified, not
state, because selection rereads the latest coupled authority.

### 2.2 Wake decisions and absence proofs

| condition | required action | acceptance source |
|---|---|---|
| publish creates first/current obligation | Tell exact live bindings once after flush; another connection and every pre-flush cut see nothing. | `published_obligation_tells_exact_live_dispatch_once` |
| normal ack, bind/fate, marker ack, W1b floor/finalizer, debt-zero, or retirement changes the dispatch key | The committing owner returns a `Changed` effect map; marker and W1b rows are covered even when closure state is byte-equal. | `dispatch_impact_covers_state_preserving_marker_and_w1b_changes` |
| one commit has publish/bind/ack/episode/retirement effects in combination | Record every true effect with its exact target set; notify the deduplicated union after unlock. No scalar cause or precedence may discard a target. | `dispatch_impact_unions_multi_effect_targets` |
| refusal, no-op, idempotent replay, or unrelated successful semantic return | Return `Unchanged`; remove the r1 unconditional wrapper tell. | `semantic_noop_refusal_and_unchanged_commit_emit_no_dispatch_tell` |
| decision is `Defer` | Consume ready work without encode, budget debit, held head, self-requeue, or W2 wake. Only a later committed `Changed` may tell it. | `deferred_debt_does_not_self_requeue_or_create_debt_wakes` |
| source inspection | No W2 interval/tick/sleep/sweep/registration path reaches `decide_obligation_debt_dispatch`; the only authority call graph is changed inbox work → existing pump → `next_publication`. | `dispatch_source_has_no_timer_sweep_or_periodic_probe` |
| restart and new connection registration | Replay reconstructs passively; TCP/WebSocket register before socket service; only later committed bind impact tells the exact conversation, with no sweep or reverse index. | `registration_is_passive_and_committed_bind_tells_without_sweep` |
| execute-to-wait race | Preserve both TCP and WebSocket final probes. The TCP probe checks participant inbox work before `Wait` (`crates/liminal-server/src/server/connection/process.rs:652-694`), and READY fires on empty→nonempty (`crates/liminal-server/src/server/participant/publication.rs:364-381`). | `debt_tell_between_drain_and_wait_is_seen_by_final_probe` |

Requeue caused by known permitted work left behind by fairness/budget is not
polling. Requeue after debt `Defer` is polling and forbidden. Socket writable
readiness may resume a held permitted head only after §1.2 fresh validation.

### 2.3 Restart and registration: passive register, committed bind tells

Registration performs no replay lookup. The landed registry accepts only
incarnation, inbox, and waker; it owns no conversation identities
(`crates/liminal-server/src/server/participant/publication.rs:279-306`). Adding
catch-up there would require the forbidden authority sweep or a new reverse
index. W2 does neither.

Instead, startup eagerly restores every conversation before constructing the
installed service (`crates/liminal-server/src/server/participant/production/handler.rs:81-115`;
`crates/liminal-server/src/server/connection/services.rs:385-407`), and W1b
unclean-restart repair completes before incarnation authority becomes Ready
(`crates/liminal-server/src/server/connection/incarnation.rs:156-170`). TCP and
WebSocket then register the unique new connection incarnation before servicing
its first socket request
(`crates/liminal-server/src/server/connection/process.rs:153-159,401-418`;
`crates/liminal-server/src/server/connection/websocket/process.rs:182-191,690-709`).
The later committed enrollment/attach/rebind already owns the exact conversation
id and returns an effect map containing `BindingChanged` plus every other true
effect; its target union tells the registered inbox and replays after §3.1's
reconciled cursor. There is no register-before-snapshot race,
because registration takes no snapshot and bind is ordered after register.
This ordering is pinned by
`registration_is_passive_and_committed_bind_tells_without_sweep`.

## 3. Debt semantics

### 3.1 Total decision table

Selection reconciles the two landed cursor meanings before asking for an
obligation. Under the conversation lock, let `protocol_cursor` be the exact
current participant cursor from the frontier; in `Owed`, require the episode
participant cursor to equal it. Let `offered_cursor` contribute only when its
binding epoch is still exact. Production computes

`dispatch_after = max(outbox.durable_ack_through, protocol_cursor, offered_cursor)`

and calls `delivery_after(participant, dispatch_after)`. This extends today's
raw durable-ack/offered choice at
`crates/liminal-server/src/server/participant/production/handler_semantic.rs:206-213`.
A delivery returned at or below `dispatch_after` is an invariant; a raw durable
outbox ack below `protocol_cursor` is not, provided operation commit/replay has
validated the cursor advance. No cursor is written by this read.

This carve-out is exact to marker semantics. Production appends/applies
`MarkerAckCommitted`, advances the durable member, and installs the frontier
(`crates/liminal-server/src/server/participant/production/ops_acks.rs:332-367`),
but outbox application advances durable ack only for `AckAdvanced` and treats
`MarkerAckCommitted` as a deliberate no-op
(`crates/liminal-server/src/server/participant/production/outbox.rs:199-206`).
When a later ordinary catch-up actually commits, it is visibly a distinct
`AckAdvanced` row
(`crates/liminal-server/src/server/participant/production/tests_marker_ack.rs:304-314`),
but W2 MUST NOT promise that catch-up. An ordinary ack exactly at the
marker-advanced member cursor returns `AckNoOp` before any commit
(`crates/liminal-protocol/src/lifecycle/operations/participant_ack.rs:210-220`),
and production appends `AckAdvanced` only on the selector's `Commit` arm
(`crates/liminal-server/src/server/participant/production/ops_acks.rs:164-215`).
Only a future ordinary ack strictly above that cursor which actually commits,
or permanent retirement, can discharge the old accounting entries.

Commit/replay validation still requires non-marker cursor advances to have
their matching `AckAdvanced`; only validated marker provenance permits the
protocol cursor to lead raw outbox ack. Live and cold selection use the same
`dispatch_after`, so fate/rebind cannot resurrect a marker-covered obligation.
This is eligibility reconciliation only: it performs no outbox mutation and
claims no accounting consumption.

| locked prestate | decision | effect | acceptance source |
|---|---|---|---|
| `Clear`, no recipient obligation strictly after `dispatch_after` | `Defer(NoObligation)` | No work is retained or requeued; a later committed publish/bind/ack impact may tell it. | — |
| `Clear`, exact current binding and next obligation strictly after `dispatch_after` | `Permit` | Preserve today's least-sequence selection and current-epoch offered-progress rule. Zero-debt ack remains the distinct item-28 path. | — |
| `Owed(coupled)`, exact bound participant/epoch, but no obligation strictly after `dispatch_after` | `Defer(NoObligation)` | This is legal for first `Owed` enrollment whose sender is filtered and after marker reconciliation **skips** every stale raw outbox candidate for delivery. The read consumes and writes nothing; retained accounting remains governed by the marker row below. Do not invent an obligation or self-requeue. | `enrollment_clear_or_owed_and_no_obligation_are_total`; `marker_ack_preserves_owner_variant_and_reconciles_dispatch_cursor` |
| `Owed(coupled)`, exact bound participant/epoch, and testified next obligation with `cursor < delivery_seq <= H'` | `Permit`, **including when `delivery_seq < resulting_floor`** | Schedule that exact obligation. Its later ack routes through the obligation-aware nonzero selector. Endpoint testimony and `H'`, not retained-floor membership, own legality. | `nonzero_debt_permits_testified_below_floor_and_defers_above_high_watermark` |
| `Owed(coupled)`, exact testified next obligation with `delivery_seq > H'` | `Defer(BeyondCandidateHighWatermark)` | The active episode cannot authorize that endpoint. Consume ready work without a held head or self-requeue; a later committed coupled-state impact must tell it. | `nonzero_debt_permits_testified_below_floor_and_defers_above_high_watermark` |
| validated marker commit leaves raw outbox durable ack below the matching protocol cursor | Reconcile delivery through `dispatch_after`; retain outbox accounting until a real discharge | Do not redispatch an obligation at/below the marker-advanced cursor and do not mutate outbox ack, `records`, `all_obligations`, live-recipient count, or charged bytes. An equal-cursor ordinary ack is `AckNoOp`; only a later strictly advancing committed `AckAdvanced` or retirement discharges those entries. | `marker_ack_preserves_owner_variant_and_reconciles_dispatch_cursor`; `marker_covered_outbox_accounting_stays_bounded_until_real_discharge` |
| `delivery_after` returns at/below `dispatch_after`; frontier/episode participant cursors differ in `Owed`; a non-marker cursor advance lacks its required `AckAdvanced`; participant/epoch or testimony context differs; or closure debt differs inside the coupled owner | `Invariant(...)` | Fail the participant semantic service loudly; do not fabricate a wire refusal or silently fall back to zero debt. | `debt_dispatch_invariant_never_falls_back_or_fabricates_wire_refusal` |
| `Owed(coupled)` participant is Detached | `Defer(NoCurrentBinding)` | No incarnation is eligible. Exact rebind owns the later tell; registration never scans for it. | — |
| poststate transitions `Owed` → `Clear` | `Clear` plus a post-commit dispatch impact | Consume the episode with the durable transition, tell exact currently bound recipients whose eligibility changed, and resume after the same reconciled cursor—not volatile old-binding offer state. | `debt_zero_transition_releases_deferred_obligation` |

The floor is a **retention/physical-compaction fact, not ack endpoint
eligibility**. `retains` checks only `resulting_floor <= delivery_seq <= H'`
(`crates/liminal-protocol/src/lifecycle/cursor_facts.rs:582-596`). The
obligation-aware path instead checks exact recipient testimony
(`crates/liminal-protocol/src/lifecycle/cursor_facts.rs:647-675`), then commits
any testified endpoint strictly above the cursor and at/below `H'` without a
floor test (`crates/liminal-protocol/src/lifecycle/cursor_facts.rs:708-744`).
The constructor independently accepts floor/cap/observer state
(`crates/liminal-protocol/src/lifecycle/cursor_facts.rs:434-505`), and
`floor_transition` can raise the resulting floor to `cap_floor` above the
minimum cursor (`crates/liminal-protocol/src/algebra/floor.rs:9-34`). Therefore
the acceptance fixture `H'=100, observer=100, floor=cap_floor=25, cursor=0,
obligation=10` MUST permit and later commit even though `retains(10)` is false.

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
| testified endpoint above `H'` defers; testified endpoint below physical floor but above cursor permits | `DebtDispatchDeferral` is internal scheduling, never a client error; floor is not an eligibility predicate | `nonzero_debt_permits_testified_below_floor_and_defers_above_high_watermark` |
| closure/episode disagreement, invalid outbox selection, or an unproved non-marker cursor lead | `DebtDispatchInvariant` → `ParticipantSemanticError::Internal`, or the already-latched service fatal; validated marker lag handled by §3.1 is expressly not an invariant | `debt_dispatch_invariant_never_falls_back_or_fabricates_wire_refusal` |
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

| durable decision | permanent ruling | acceptance source |
|---|---|---|
| distinct nonzero ack row | Extend the current tagged v3 operation grammar with `NonzeroDebtAck { request, receiving_epoch, contiguously_available_through, event }`; `event` is the canonical existing `ConversationOperation::NonzeroDebtAck` projection. Do not overload `ZeroDebtAck`, change old tags, or use the standalone cursor repository. The current grammar has only `ZeroDebtAck` at `crates/liminal-server/src/server/participant/production/log_v3.rs:15-72`, while the protocol already projects the nonzero commit and cursor-fact key at `crates/liminal-protocol/src/lifecycle/aggregate_commit.rs:315-332`. Replay reconstructs the episode and recipient obligations at that row boundary, reruns the obligation-aware selector and scalar conformance domain, byte-checks `event` and the scalar audit, then applies exactly one coupled episode/frontier/outbox commit. Unknown tags remain a loud old-binary refusal; existing v3 bytes are not reinterpreted. | `nonzero_debt_ack_row_replays_obligation_aware_commit` |

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
| one sparse recipient index with an internal conversation-sequence gap and a request ending on the absent sequence | call the obligation-aware path as authority; the scalar result may be observed only as non-authoritative diagnostic input | obligation path returns `AckGap`; no scalar result is selected, appended, or used to mutate | `nonzero_debt_sparse_gap_never_selects_scalar_fallback` |

The tests SHALL call both public entry points, not merely their shared private
helper. The sparse row is deliberately outside the equality domain because a
scalar cannot represent exact endpoint membership.

## 5. Idle cost and honesty

### 5.1 Permanent idle bound

The permanent claim is **zero debt-attributable work**, not zero transport
slices. A configured WebSocket maps `ping_interval_ms` to a live interval
(`crates/liminal-server/src/server/connection/websocket/listener.rs:50-58`),
arms timer-driven READY wakes (`crates/liminal-server/src/server/connection/websocket/process.rs:70-95,770-815`), and writes a Ping when due
(`crates/liminal-server/src/server/connection/websocket/process.rs:624-659`).
Each such slice increments the total slice counter and calls the shared
participant pump before keepalive servicing
(`crates/liminal-server/src/server/connection/websocket/process.rs:178-220`).
Those supported transport slices make r1's absolute zero-slice bound false, but
they are not debt polling.

| idle configuration | permanent W2 bound | permitted existing work | acceptance source |
|---|---|---|---|
| TCP or WebSocket, no changed dispatch impact, empty participant inbox, no held participant head | Zero W2 READY fires, zero `decide_obligation_debt_dispatch` calls, zero conversation-authority lock acquisitions for debt selection, zero outbox delivery probes, and zero W2 allocations. | Socket/readiness/reply machinery may run for its own typed events. The participant pump returns after the empty inbox before reaching authority (`crates/liminal-server/src/server/connection/participant_delivery.rs:159-173`). | `obligation_debt_dispatch_idle_has_zero_debt_attributable_work` |
| WebSocket with keepalive configured under the same empty W2 state | The same W2 counters stay flat across multiple ping intervals. Total scheduler-slice and Ping counters MUST grow, proving the test does not hide the supported timer. | Timer READY, shared-pump empty-inbox check, Ping, drain, and timer re-arm only. | `obligation_debt_dispatch_idle_has_zero_debt_attributable_work` |
| debt-deferred conversation after its ready item is consumed | No self-requeue, W2 wake, selector retry, or held head until a committed changed impact arrives. A keepalive slice may still observe the empty inbox. | Unrelated transport events only. | `deferred_debt_does_not_self_requeue_or_create_debt_wakes` |

W2 adds only one move-coupled protocol debt state per loaded conversation plus
an already bounded/coalesced conversation id while genuinely ready. Existing
outbox accounting may remain charged for marker-covered records as disclosed in
§5.2; zero debt-attributable work does not mean zero retained durable state. No
per-W2-tick state, timer, reverse index, or idle allocation exists.

### 5.2 Loud tradeoffs and design refusals

| item | permanent disclosure |
|---|---|
| commit-path duplicate decision | Eligible nonzero commits and common pre-availability refusals run both public selectors on cloned immutable input. This is intentional bounded CPU for ledger-mandated non-divergence; it performs no second append and cannot choose the scalar result. |
| marker-covered outbox accounting | W2 chooses **legal, bounded accounting divergence**, not a synthetic ack commit. Read-time `dispatch_after` suppresses redispatch but leaves the landed `records`, `all_obligations`, `live_recipient_obligations`, and charged bytes intact. Installation increments those structures (`crates/liminal-server/src/server/participant/production/outbox.rs:274-307`); only a real strictly advancing `apply_ack` or retirement removes recipients (`crates/liminal-server/src/server/participant/production/outbox.rs:310-379`). Each retained recipient obligation continues to count against `max_live_recipient_obligations = max_retained_record_rows × identity_slots`, and prospective Produced batches fail with the existing `LiveRecipientObligationsExceeded` when the count would exceed that bound (`crates/liminal-server/src/server/participant/production/outbox/limits.rs:7-25,29-60`). The charge survives idle and restart, may cause capacity failure earlier than delivery eligibility suggests, and creates no wake, retry, selector call, or timer. An equal-cursor ordinary ack remains `AckNoOp`; discharge is not promised before a later strictly advancing committed ack or `Left`. Acceptance source: `marker_covered_outbox_accounting_stays_bounded_until_real_discharge`. |
| coalescing | Multiple effect-target unions may collapse to one conversation wake after exact targets are notified. This sacrifices per-effect counts, not targets or correctness, because each target's decision rereads one locked durable poststate. |
| duplicates | Crash or reattach may offer the same unacked obligation again. Across fate/restart, §3.1's reconciled durable/protocol cursor—not an old socket offer—is authority; Unit 2 already records offered progress as volatile per binding (`crates/liminal-server/src/server/participant/publication.rs:19-34`). A current-epoch offer still contributes to live `dispatch_after`. |
| no wall-clock expiry | The pin has retirement discharge but no obligation TTL. The closed effect is `Retired` only. A future expiry requires its own durable operation, an explicit revision adding an `Expired` effect, and the same TOLD interface; W2 refuses anticipatory vocabulary and periodic scans. |
| no W2 periodic anything | Any implementation adding a **W2 debt** timer, sweep, interval, unconditional scheduler continuation, registration catch-up, or deferred self-requeue is a **design refusal** and must be escalated to the board. Existing WebSocket keepalive timers remain transport-only; they may enter an empty-inbox pump slice but cannot call the debt selector. |

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

Durable rows and reconstructed protocol state are authority; dispatch impacts,
ready inboxes, offered cursors, and held frames are volatile. Startup SHALL
complete base/outbox reconciliation, every W1b fate/finalizer, and coupled
frontier/episode replay before publishing participant service, but emits **no
startup or registration tell**. The current cursor repository replays scalar
commands from an append-only start/ack stream
(`crates/liminal-server/src/server/participant/cursor_repository.rs:145-221`);
W2 replaces/integrates that dormant island into the conversation operation
ordering and obligation-aware coupled replay. It may not recover a second
episode or race it against the frontier. After restart, only a committed
post-registration bind impact makes a current incarnation eligible (§2.3).

### 6.1 Crash-cut table

| cut | restart obligation | acceptance source |
|---|---|---|
| before a debt/outbox/ack/fate row flush | Restore the prior state; no tell or delivery attributable only to the uncommitted candidate. | `crash_before_debt_flush_restores_prior_dispatch_state` |
| after durable change and install, before `DispatchImpact::Changed` is notified | Cold replay reconstructs the exact coupled state but does not sweep or tell. W1b repairs dead bindings; the next connection registers, then its committed rebind impact selects the least obligation strictly after §3.1's reconciled cursor. | `crash_after_debt_flush_before_tell_rebind_replays_ready_work` |
| after tell, before enqueue | Volatile ready work may vanish with the old connection. Restart remains passive; the later registered connection's committed rebind selects the same least obligation after the reconciled cursor without skip or marker resurrection. | `crash_after_tell_before_enqueue_rebind_replays_same_obligation` |
| after enqueue, before ack commit | Offer testimony does not discharge debt. Restart/reattach may duplicate the same obligation, and the obligation-aware ack still accepts the durable endpoint. | `crash_after_enqueue_before_ack_reoffers_and_accepts_endpoint` |
| after nonzero ack row but before outbox/coupled-owner state is fully durable | Ordered barrier/reconciliation either completes the exact coupled commit or fails startup loudly; it never publishes one half. | `crash_between_nonzero_ack_barriers_reconciles_one_coupled_commit` |

### 6.2 W1b connection-fate composition

When a connection fate occurs with a held or in-flight publication, W1b remains
first owner of Died/Detached and any Ordinary/Recovered finalizer. The coupled
operation returns a post-flush map containing every true `BindingChanged`,
`EpisodeChanged`, `Published`, or `Retired` effect and the union of their exact
targets; only committed `Left` may include `Retired`. A held frame for the old binding is
discarded on fresh validation; an unacked durable obligation survives for a
later current binding unless permanent Left discharged it. Current held publication
already checks binding before offer
(`crates/liminal-server/src/server/connection/participant_delivery.rs:349-359`),
and current reattach selection falls back to durable outbox ack when binding
epoch changes
(`crates/liminal-server/src/server/participant/production/handler_semantic.rs:193-218`).
W2 replaces that raw fallback with §3.1's `dispatch_after`, so a validated marker
cursor survives fate/rebind even though the outbox's ack scalar intentionally
lags.

If W1b latches `ParticipantServiceFatal::ConnectionFateIntentIncomplete`, the
arm MUST produce no publication, wake-induced mutation, or scalar audit until
startup/non-crash recovery clears the service-level condition. The fatal is
already a process-wide typed semantic error
(`crates/liminal-server/src/server/participant/dispatch.rs:125-158`). W2 neither
catches it nor translates it to a participant wire response.

| fate boundary | acceptance source |
|---|---|
| stale held/in-flight head is rejected; the least obligation after the reconciled durable/protocol cursor follows the post-fate binding/retirement result without marker resurrection | `connection_fate_drops_stale_head_and_replays_after_reconciled_cursor` |
| Died/Detached plus Ordinary/Recovered replay completes before service publication; no replay tell fires, and the later committed bind cannot double-present a delivery | `w1b_fate_replay_precedes_bind_triggered_dispatch` |
| latched participant fatal blocks dispatch and is never downgraded | `participant_service_fatal_blocks_obligation_dispatch` |

## 7. Acceptance oracle census

The build is not accepted unless every row below exists under its exact name.
This is the single census table: **every name derives from a design-table row
in §§1–6**, and each appears exactly once in this census. No ignored test,
sleep-based test, log-only assertion, or mock that bypasses the production
`next_publication`/ack call graph satisfies a row.

| # | exact oracle | design source | required observation |
|---:|---|---|---|
| 1 | `obligation_debt_dispatch_seams_before_delivery_after_under_one_owner` | §1.2 single owner | The production call occurs under the conversation owner before `delivery_after`; no second owner exists. |
| 2 | `obligation_debt_dispatch_never_unlocks_between_debt_and_obligation_selection` | §1.2 atomic decision | A barrier race cannot pair one debt snapshot with another outbox/binding snapshot; registry/socket locks are absent while the conversation lock is held. |
| 3 | `obligation_debt_dispatch_preserves_pump_order_on_both_transports` | §1.2 pump order | TCP and WebSocket both select response/control before participant work, then generic delivery/drain, through the shared pump. |
| 4 | `held_obligation_revalidates_binding_and_debt_before_offer` | §§1.2, 5.3 held/gate rider | Change fate or debt while a head is held; resume rejects the stale verdict and neither offers nor advances it. |
| 5 | `published_obligation_tells_exact_live_dispatch_once` | §2.2 publish wake | Post-flush publish wakes the exact current incarnation once; another connection and pre-flush cut see nothing. |
| 6 | `dispatch_impact_covers_state_preserving_marker_and_w1b_changes` | §2.2 exhaustive change impacts | Normal ack, binding/fate, marker ack, W1b floor/finalizer, debt-zero, publish, and retirement changes each return a typed impact; marker/W1b coverage does not depend on a `ClosureState` delta. |
| 7 | `deferred_debt_does_not_self_requeue_or_create_debt_wakes` | §§2.2, 5.1 defer | A debt-deferred conversation leaves no held/ready work and creates no W2 wake or retry; unrelated transport slices may still occur. |
| 8 | `dispatch_source_has_no_timer_sweep_or_periodic_probe` | §2.2 source absence | Structural call-graph/grep absence plus runtime counters prove no timer, interval, sweep, sleep, unconditional continue, or periodic dispatch probe. |
| 9 | `debt_tell_between_drain_and_wait_is_seen_by_final_probe` | §2.2 race | Inject a tell after drain and before final probe on TCP and WebSocket; neither parks over pending debt work. |
| 10 | `nonzero_debt_permits_testified_below_floor_and_defers_above_high_watermark` | §§3.1–3.2 total selector | The explicit `H'=100`, floor/cap=25, cursor=0, obligation=10 fixture permits below-floor testimony, while an endpoint above `H'` defers without encode/budget mutation. |
| 11 | `debt_dispatch_invariant_never_falls_back_or_fabricates_wire_refusal` | §3.2 invariant | Debt/episode/outbox disagreement produces the typed internal/fatal arm, no zero-debt selector call, no wire value, and no mutation. |
| 12 | `debt_refusal_and_pressure_holdback_remain_distinct` | §3.2 pressure | Debt defer creates no frame/head; current-room pressure retains the exact permitted frame and resumes only on writable readiness plus fresh validation. |
| 13 | `debt_zero_transition_releases_deferred_obligation` | §§2.1, 3.1 zero transition | Durable `Owed`→`Clear`, then its tell, schedules the least unacked obligation exactly once under the existing budget. |
| 14 | `nonzero_debt_obligation_and_scalar_commit_cannot_diverge` | §4.2 mandatory floor | Both public nonzero entry points consume the same exact-endpoint fixture and produce equal decisions and equal post-commit episode/frontier state. |
| 15 | `nonzero_debt_obligation_and_scalar_refusal_cannot_diverge` | §4.2 mandatory floor | Both public entry points consume the same pre-availability refusal fixture, return equal typed refusal, and preserve byte-equal owner state. |
| 16 | `nonzero_debt_sparse_gap_never_selects_scalar_fallback` | §4.2 sparse boundary | Obligation-aware selection returns `AckGap` for an absent endpoint across legal internal gaps; scalar output is neither authoritative nor appended. |
| 17 | `nonzero_debt_ack_row_replays_obligation_aware_commit` | §4.1 durable decision | The distinct nonzero tag persists canonical event/scalar audit, replay reconstructs obligations and the episode at its row boundary, and exactly one coupled commit results. |
| 18 | `obligation_debt_dispatch_idle_has_zero_debt_attributable_work` | §5.1 idle table | With empty W2 state, W2 wake/selector/authority/outbox/allocation counters stay flat; a configured WebSocket fixture simultaneously proves transport slice/Ping counters grow. |
| 19 | `crash_before_debt_flush_restores_prior_dispatch_state` | §6.1 cut 1 | Cut before flush restores the exact prior closure/episode/outbox state and emits no candidate-derived work. |
| 20 | `crash_after_debt_flush_before_tell_rebind_replays_ready_work` | §6.1 cut 2 | Cut after durable install but before tell; cold replay is passive, then post-registration committed rebind selects the least obligation after the reconciled cursor without a sweep. |
| 21 | `crash_after_tell_before_enqueue_rebind_replays_same_obligation` | §6.1 cut 3 | Lose the volatile tell and old connection; passive restart plus committed rebind selects the same least obligation after the reconciled cursor without skip or marker resurrection. |
| 22 | `crash_after_enqueue_before_ack_reoffers_and_accepts_endpoint` | §6.1 cut 4 | Lose the connection after enqueue; restart/reattach reoffers the same obligation and the obligation-aware nonzero ack commits it. |
| 23 | `crash_between_nonzero_ack_barriers_reconciles_one_coupled_commit` | §6.1 cut 5 | Every barrier cut yields either old state, exactly one coupled ack/episode/outbox commit, or loud startup failure—never mixed published authority. |
| 24 | `connection_fate_drops_stale_head_and_replays_after_reconciled_cursor` | §6.2 fate | W1b fate invalidates the old held/in-flight head; the surviving post-fate binding resumes after the reconciled durable/protocol cursor, or retirement discharges it. |
| 25 | `w1b_fate_replay_precedes_bind_triggered_dispatch` | §6.2 fate/restart | Died/Detached and selected Ordinary/Recovered rows/finalizers finish before service publication; replay emits no tell, and later committed bind cannot present a pre-fate head. |
| 26 | `participant_service_fatal_blocks_obligation_dispatch` | §6.2 fatal | A latched `ParticipantServiceFatal` prevents selection, enqueue, wake-induced mutation, and scalar audit and is never converted to a wire refusal. |
| 27 | `temporary_fate_preserves_cursor_facts_and_rebinds_exact_epoch` | §1.1.1 attach/rebind bridge | Temporary detach/death preserves the participant cursor and fact map; exact nonregressing rebind changes only the binding tag/epoch and restores eligibility. |
| 28 | `coupled_debt_owner_covers_every_w1b_fate_and_finalizer_route` | §1.1.1 W1b bridge | Every route follows its resulting closure: `Clear` has no episode and `Owed` has one complete episode; no bare-owner route remains. |
| 29 | `enrollment_clear_or_owed_and_no_obligation_are_total` | §§1.1–1.1.1, 3.1 enrollment branches | Exercise initial enrollment resulting in `Clear`, `Owed` with another recipient, and `Owed` with sender-filtered no obligation; owner shape and selector result are defined in all three. |
| 30 | `marker_ack_preserves_owner_variant_and_reconciles_dispatch_cursor` | §§1.1.1, 3.1 marker bridge/read | Marker ack preserves `Clear` without an episode or advances both Owed cursors; with raw outbox ack behind, live/cold/post-fate reads skip marker-covered endpoints without mutating accounting. |
| 31 | `semantic_noop_refusal_and_unchanged_commit_emit_no_dispatch_tell` | §2.2 exclusive impacts | Refusal, no-op, idempotent replay, and successful unchanged commit fire no READY; the request-success wrapper has no unconditional notify path. |
| 32 | `registration_is_passive_and_committed_bind_tells_without_sweep` | §2.3 register/bind ordering | TCP/WebSocket register before socket service, registration reads no conversations, and the later exact committed bind tells replay work without a scan or reverse index. |
| 33 | `dispatch_impact_unions_multi_effect_targets` | §§2.1–2.2 effect map/union | Enrollment/attach, Owed marker/normal ack, W1b finalizer, and Left fixtures that combine effects retain every effect-specific exact target; notification is the deduplicated union with no scalar precedence or poststate reconstruction. |
| 34 | `marker_covered_outbox_accounting_stays_bounded_until_real_discharge` | §§3.1, 5.2 accounting honesty | After marker ack, delivery skips endpoints at/below protocol cursor while outbox records/obligations/count/bytes remain charged across idle and restart; equal-cursor ordinary ack is `AckNoOp`, the configured count bound is enforced, and only a later strictly advancing committed ack or retirement discharges. |

### 7.1 Acceptance mechanics

The tear SHALL perform all of the following:

1. exact-name census: one implementation for each of the 34 names above and no
   absent or duplicate census row;
2. production-caller grep: both
   `apply_nonzero_participant_ack_with_obligations` and
   `apply_nonzero_participant_ack` have a non-test call in the W2 ack arm, while
   item 28 remains on `apply_participant_ack_with_obligations`;
3. effect-map matrix: overlapping enrollment/attach, Owed marker/normal ack,
   W1b finalizer, and Left fixtures assert every effect-specific target and the
   deduplicated notification union; no test substitutes a scalar precedence;
4. marker-accounting fixture: after marker ack, assert delivery suppression,
   unchanged outbox accounting, equal-cursor `AckNoOp`, exact configured count
   failure, flat idle counters, cold equality, and discharge only by a later
   strictly advancing committed ack or retirement;
5. absence grep plus both idle fixtures: no W2 timer/sweep/poll/register entry;
   W2 counters stay flat with keepalive disabled and enabled, while the enabled
   fixture proves total WebSocket slice/Ping counters grow;
6. narrow W2 tests, then repository-standard formatting, check, clippy, and
   workspace tests; and
7. crash matrix and both transports: every §6 cut and TCP/WebSocket final-probe
   race is exercised, not inferred from one transport.

## 8. Scope walls and lens questions

### 8.1 Hard walls

| inside W2 | expressly outside W2 |
|---|---|
| One synchronous debt selector at the existing participant publication seam; coupled authority/replay state; explicit post-commit publish/ack/binding/episode/retirement effect maps; legal bounded marker-covered accounting; nonzero ack routing; the 34 oracles above. | A new scheduler, worker, durable queue, transport, outbox, connection registry, reverse index, registration catch-up, or generic subscription path. Existing generic subscription delivery is a separate pump (`crates/liminal-server/src/server/connection/delivery.rs:83-181`). |
| The no-polling law for this arm and its exact producers. | Wholesale LAW-1 polling retirement. W4 remains its own named lane and oracle floor (`docs/design/WIRING-LEDGER.md:212-219`); W2 neither claims nor blocks its unrelated replacements. |
| Composition with W1b post-fate state and fatal routing. | Reopening W1b fate classification, schema, source order, finalizer ownership, presentation, or `ParticipantServiceFatal`. Those landed bytes remain owned by the W1b path (`crates/liminal-server/src/server/participant/production/binding_fate_completion.rs:38-188`; `crates/liminal-server/src/server/participant/dispatch.rs:99-158`). |
| Bounded live outbox reads required for one locked decision. | W7 history-linear index compaction/reconstruction. The ledger keeps that as a separate design-first lane (`docs/design/WIRING-LEDGER.md:180-210`). |
| Internal liminal protocol/server types and participant `ServerPush`. | Any Apollo orchestration, UI policy, or haematite-facing API/surface. Those layers may consume a later stable liminal contract; W2 adds no cross-project payload, command, status, compatibility shim, or presentation semantics. |
| Existing gated binding authority revalidated at offer. | New auth claims, capability gates, wire variants, SDK surface, or browser conversation support. The browser surface remains separately ledgered (`docs/design/WIRING-LEDGER.md:231-241`). |

YG-560 is forward-only here: the build changes current authority and callers;
it does not add dual old/new runtime modes, migrate by fallback, publish a tag,
or retain a dormant compatibility branch.

### 8.2 Questions **FOR THE LENS**

These are review obligations, not deferred design choices. The rulings above
stand unless the lens answers **no** with contradicting bytes; a no blocks build
dispatch and returns the brief for revision rather than licensing an
implementation guess.

1. Do §§1.1–1.1.1 derive the owner variant from every operation's resulting
   `ClosureState`, with no episode on `Clear`, one complete episode on `Owed`,
   and defined first-enrollment outcomes for Clear, Owed-with-recipient, and
   Owed-without-obligation?
2. Does §3.1 reconcile validated marker cursor lead identically live, cold, and
   after fate/rebind, while leaving durable outbox ack unchanged and retaining a
   loud invariant for every unproved non-marker cursor split?
3. Does the concrete §3 fixture (`H'=100`, floor/cap=25, cursor=0,
   obligation=10) permit dispatch and commit through exact testimony, while
   only an endpoint above `H'` defers?
4. Does §4's production scalar domain—exact committed endpoints and common
   pre-availability refusals—satisfy the ledger's same-fixture non-divergence
   floor without weakening sparse endpoint membership?
5. Does every overlapping producer return one truthful nonempty effect map,
   retain every effect-specific exact target, and notify the deduplicated union
   without scalar precedence or poststate target reconstruction?
6. Does passive replay plus register-before-socket and committed-bind-after-
   register close every restart cut without a conversation sweep, reverse
   index, registration lookup, or pre-admission tell?
7. Do TCP and WebSocket retain equivalent final-probe and held-head freshness
   barriers, and does the configured-keepalive idle fixture show growing
   transport slice/Ping counters alongside flat W2 counters?
8. Is the closed effect vocabulary exactly `Published`, `Acknowledged`,
   `BindingChanged`, `EpisodeChanged`, and `Retired`, with no expiry branch or
   unconditional successful-request notification left in production?

## 9. Revision record

| revision | date | byte/ledger pin | record |
|---|---|---|---|
| r1 | 2026-07-21 | liminal `23acdea0c390d4238a9ad1dcdd02cd60a85ffcbd`; `WIRING-LEDGER.md` r1.9, 2026-07-20 | Initial design-first ruling for the W2 obligation-debt dispatch arm: exact existing seam and single owner; TOLD-only wake producers; debt permit/defer/invariant semantics; production disposition of both nonzero selectors with same-fixture oracle floor; idle-cost refusal; W1b crash/fate composition; 26-oracle census; scope walls and lens questions. |
| r2 | 2026-07-21 | same liminal/ledger pin | Folds the complete round-2 **5 MAJOR + 2 minor** array. **Major — Adjudication 2 NO, below-floor arm rejects a landed-legal endpoint:** §3 separates physical retention floor from exact testified endpoint eligibility and pins the H=100/floor=25/cursor=0/obligation=10 commit. **Major — Adjudication 1 NO, W1b has no coupled episode transition:** §§1.1–1.1.1 replace the bare frontier with a protocol move-coupled owner, widen episode binding state, and exhaustively rule Died/Detached/Ordinary/Recovered/finalizer/marker/ack/Left transitions. **Major — Adjudication 4 NO, wake vocabulary is neither exhaustive nor exclusive:** §§2.1–2.2 replace successful-request notification with operation-owned `DispatchImpact`, cover state-preserving marker/W1b changes, and require `Unchanged` for refusal/no-op/unchanged commits. **Major — Adjudication 5 NO, registration cannot exact-tell without sweep/index:** §§2.3 and 6 select passive registration plus register-before-socket and committed-bind tells; no reverse index, sweep, or recovery tell. **Major — permanent zero-slice bound false under WebSocket keepalive:** §5.1 narrows permanently to zero debt-attributable work and requires growing transport slice/Ping counters in the keepalive fixture. **Minor — Adjudication 7 NO, cause must be Retired:** §§2.1 and 5.2 close vocabulary on `Retired`; expiry requires a future durable operation and brief revision. **Minor — two census names lacked table origins:** §§4.2 and 5.1 give the sparse-gap and idle names table rows; §7 requires all 31 names to be table-derived. |
| r3 | 2026-07-21 | same liminal/ledger pin | Folds the complete round-3 **2 MAJOR + 1 minor** array. **Major — coupled bridge has no coherent Clear/Owed result for enrollment and marker commits:** §§1.1–1.1.1 derive every owner variant from the operation's resulting closure, keep Clear episode-free, preserve marker's input variant, and explicitly permit first Owed enrollment with no recipient obligation; §3.1 makes that real state `Defer(NoObligation)` and adds `enrollment_clear_or_owed_and_no_obligation_are_total`. **Major — Owed marker ack creates the outbox/cursor disagreement declared fatal:** §§1.1.1 and 3.1 advance the Owed protocol cursor while leaving durable outbox ack unchanged, compute `dispatch_after` from durable ack/protocol/current-offer cursors after validating marker provenance, retain fatal treatment for non-marker splits, and require live/cold/post-fate equivalence; §§6.1–6.2 resume after that reconciled cursor, with `marker_ack_preserves_owner_variant_and_reconciles_dispatch_cursor` and the renamed fate oracle. **Minor — scalar DispatchImpact cause cannot represent multi-effect commits:** §§2.1–2.3 replace the scalar with a nonempty `DispatchEffect -> Set<DispatchTarget>` map built under lock, enumerate overlapping enrollment/attach, marker/normal ack, W1b finalizer, and Left effects, and notify the deduplicated exact-target union with no precedence or poststate reconstruction; §§5.2 and 6.2 compose that rule through coalescing/fate, and §7 closes a 33-oracle table-derived census including `dispatch_impact_unions_multi_effect_targets`. |
