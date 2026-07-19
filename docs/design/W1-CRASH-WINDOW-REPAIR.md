# W1 — BindingFate observer-progress crash-window repair design brief

Base: `liminal` main at `de4f987`.

Revision: r1, 2026-07-19. Normative ledger: `WIRING-LEDGER.md` r1.5.
The Unit 2 source of record is
`docs/design/F0C-UNIT2-SERVERPUSH-PRODUCER.md`, SHA-256
`98f9130faa175f323206eb6640e9f625ab6385dc285e11f5b2774fb306b5de6a`.
Every repository anchor in this revision was opened and checked against the
base bytes.

## Goal

Wire lane W1 to one consumer only: the Unit 2 §8 observer-progress
crash-window repair. A committed fate may surrender its exact
`ObserverProgressProjection` into the existing source-barrier, observer-log,
and replay/reconciliation path. W1 does not turn that two-field projection
into crash-fate storage, does not add a polling reader, and does not bypass the
liminal durability adapter.

This is the r1.5 narrowed contract. The ledger says all four original arms
surrender the same sealed progress payload, requires an explicit full-fate
persistence ruling, and requires one canonical producer per fate
(`docs/design/WIRING-LEDGER.md:28-57`).

## 1. Consumer statement

### 1.1 Named consumer and positive capability

The named consumer is exactly **the §8 observer-progress crash-window repair**.
For each committed source, the selected producer supplies a sealed
`ObserverProgressProjection` containing only `conversation_id` and
`new_observer_progress`. Its constructor is lifecycle-private, the type is
move-only, and consuming code can read only those two values
(`crates/liminal-protocol/src/lifecycle/observer_recovery.rs:10-50`).

The projection CAN drive all and only these actions:

1. record the protocol-measured progress beside the replayed conversation
   authority;
2. present that progress to the serialized observer aggregate after the typed
   source append/flush barrier;
3. append/flush an existing `ObserverRow::Advance` when durable observer
   progress is lower; and
4. replay the typed source at startup or cold first touch and repair an absent
   `Advance` before authority or observer-handshake publication.

The existing observer log already has the v1 `Advance { conversation_id,
observer_progress }` row, appends and flushes it through `DurableStore`, and
restores it through the protocol aggregate
(`crates/liminal-server/src/server/participant/production/observer.rs:19-49,66-140,144-178`).
W1 therefore adds no observer row kind and no observer schema version.

### 1.2 Negative capability: no fate reconstruction

The projection CANNOT identify or reconstruct:

- whether its producer was `Died`, ordinary binding fate, recovered binding
  fate, or Leave;
- a Died cause;
- the affected participant;
- the dead or ended binding epoch; or
- any source-row audit beyond the two progress fields.

Those facts are erased by projection. They cannot be recovered from
`conversation_id` plus a numeric progress value, from record delivery, from a
maximum, or from observer-log history. W1 MUST NOT infer them. The protocol's
ordinary/recovered durable operation body, by contrast, retains participant,
epoch, and measured floor, showing why source truth and progress projection
are different objects
(`crates/liminal-protocol/src/lifecycle/operation_event.rs:413-490`).

### 1.3 Normative Unit 2 §8 ordering and crash cuts

W1 conforms to these operative lines verbatim:

> For each advancing source, the source append/flush barrier completes first.
> While the observer owner remains exclusively serialized, production presents
> that exact projection to `decide_progress_advance`, appends/flushes
> `ObserverRow::Advance`, commits the protocol transaction, and only then may
> publish the fired `ObserverProgressed`. Startup and cold first touch replay every
> participant source, reconcile any missing `Advance`, and restore the observer
> aggregate before publishing conversation authority or admitting an observer
> handshake. An already equal-or-greater exact durable Advance satisfies
> reconciliation; disagreement or a nonmonotone source refuses loudly. Thus a
> crash after a participant source barrier but before its Advance is repaired
> before an observer can observe stale authority.

Source: `docs/design/F0C-UNIT2-SERVERPUSH-PRODUCER.md:516-526`.

> The advance path shares that same non-cancellable owner critical section. After
> `ObserverRow::Advance` flushes, it commits the protocol transaction and transfers
> the exact fired payload to the associated live inbox before unlock. The payload
> rides `encode_server_push`, READY, and the shared signed push-slice budget, but
> not ParticipantAck fan-out. A dead or missing handle drops only that targeted
> wake; no other connection receives it, no socket handoff becomes receipt, and no
> semantic terminal response is displaced.

Source: `docs/design/F0C-UNIT2-SERVERPUSH-PRODUCER.md:538-544`.

> The deciding crash cuts are:
>
> - before `Arms` flush, there is no arm;
> - after `Arms` flush but before volatile registration, a process crash destroys
>   the only target socket, and reattach either re-arms or observes progressed;
> - after `Advance` flush but before socket handoff, a process crash leaves durable
>   progress, so the next handshake observes progressed; and
> - reattach versus advance serializes under the observer owner: advance-first
>   yields a progressed response, while reattach-first installs the one live
>   target that receives the fired payload.

Source: `docs/design/F0C-UNIT2-SERVERPUSH-PRODUCER.md:546-555`.

These quotations are the behavioral contract, not historical commentary.

## 2. Boundary statement

**Boundary statement.** W1 consumes
`liminal_protocol::lifecycle::ObserverProgressProjection` and persists/replays
it through liminal's `DurableStore`/observer-log machinery. W1 does **not** call
haematite branch/WAL crash-gates directly. There are zero liminal call sites
for that crash-gate API: the storage boundary available to lifecycle code is
`DurableStore` (`crates/liminal/src/durability/store.rs:20-57`), its production
adapter delegates the bounded append/read/CAS/scan/flush surface to
`EventStore` (`crates/liminal/src/durability/store.rs:60-191`), bootstrap
constructs `EventStore` and wraps it in `HaematiteStore`
(`crates/liminal-server/src/server/connection/services.rs:926-927`), and even
the cfg(test) crash fixture imports only `DurableStore` plus protocol transition
types
(`crates/liminal-server/src/server/participant/crash_repository.rs:8-17`).

No W1 production module imports a haematite branch, WAL, fault-injection, or
crash-gate type. Deterministic crash tests inject at liminal's source-append,
observer-append, owner, and weak-target seams. Haematite-specific engine
behavior remains behind `HaematiteStore`.

## 3. Full crash-fate persistence

### 3.1 Ruled disposition

**The observer-progress projection is the ONLY new persisted output of W1.**
More precisely, W1 may cause an existing observer-v1 `Advance` row to be
appended where one was missing; it creates no new row shape, stream, repository,
codec, schema version, or duplicated source log.

Typed fate information remains in the durable source home that already owns
it, to the extent that home preserves it:

- ordinary and recovered fate operation inputs retain participant, dead epoch,
  and measured floor in `BindingFateOperation`; both classes deliberately map
  to that same body, so the body does not become a full producer-class/cause
  audit (`crates/liminal-protocol/src/lifecycle/operation_event.rs:413-490`);
- production Leave persists request/verifier, receiving epoch, committed Left
  order/sequence, ended epoch, and prior terminal sequence before recording its
  progress projection
  (`crates/liminal-server/src/server/participant/production/ops_leave.rs:187-217`);
- Died cause and committed-versus-pending placement are represented by the
  typed transition inputs. The older repository demonstrates such a codec, but
  it is a cfg(test)-only fixture, not a production durable home
  (`crates/liminal-server/src/server/participant/crash_repository.rs:51-79,521-550,645-705`;
  `crates/liminal-server/src/server/participant/mod.rs:19-24`).

W1 neither strips facts from an existing source row nor promises that every
source row already contains every possible fate fact. It simply refuses to
copy those facts into observer progress. In particular, the two-field
projection is never evidence of cause, participant, epoch, or fate class.

### 3.2 Separate-lane rule for a future full-fate repository

A future consumer that needs queryable full crash-fate history requires a
**separate wiring lane** with its own ledger row, named consumer, trigger,
owner, typed source schema, migration/refusal policy, and oracle floor. It may
reuse validated codec/replay behavior only after that behavior is placed behind
the aggregate owner described in §8. It cannot be smuggled into W1, and it
cannot ship dormant without a row. This applies even if the test-only crash
repository looks reusable.

The byte evidence supports, rather than contradicts, this disposition: the
projection lacks the fields, the production observer log already has the one
required output row, and the richer crash codec is explicitly test-gated.

## 4. Canonical producer per fate

### 4.1 One-producer rule

For one typed source fate in one replay/application pass, exactly one canonical
producer may present observer progress. Replaying the same durable source in a
later startup or cold-repair pass remains required and idempotent; two producer
arms presenting the same source in one pass is forbidden.

The identity of a fate follows its committed lifecycle occurrence, not merely
the Rust wrapper currently in hand. In particular, `OrdinaryBindingFate`
consumes a committed Died terminal
(`crates/liminal-protocol/src/lifecycle/attach.rs:164-188`), and
`RecoveredBindingFate` is the recovered-epoch interpretation of that committed
death (`crates/liminal-protocol/src/lifecycle/edge.rs:1007-1056`). A direct Died
source uses the Died arm. An ordinary or recovered source uses its later sealed
fate arm and MUST NOT also present the input terminal through the Died arm.
These are mutually exclusive routes for one fate occurrence, not cumulative
progress producers.

| fate/source class | canonical producer | disposition |
|---|---|---|
| direct committed Died terminal | `DiedBindingTransition::observer_progress_projection` (`crates/liminal-protocol/src/lifecycle/binding.rs:605-616`) | Survives for a direct Died source only. `Pending` returns no projection. Do not also call it when the terminal is consumed into an ordinary/recovered fate source. |
| ordinary binding fate | `OrdinaryBindingFate::observer_progress_projection` (`crates/liminal-protocol/src/lifecycle/edge.rs:912-946`) | Survives and projects the measured `resulting_floor`; suppresses presentation by its input Died terminal. |
| recovered binding fate | `RecoveredBindingFate::observer_progress_projection` (`crates/liminal-protocol/src/lifecycle/edge.rs:1088-1122`) | Survives and projects the measured `resulting_floor`; suppresses presentation by its input Died terminal. |
| permanent Leave | `LiveLeaveCommit::observer_progress_projection` (`crates/liminal-protocol/src/lifecycle/operations/live_frontier.rs:210-229`) | Survives; it is already the production producer. |
| permanent Leave duplicate | `LeaveCommit::observer_progress_projection` (`crates/liminal-protocol/src/lifecycle/membership.rs:489-525`) | Does not survive W1; remove the dormant projection method rather than retain a second producer. |

### 4.2 Leave survivor ruling

`LiveLeaveCommit` is the canonical Leave producer. The plain `LeaveCommit` is
an intermediate: both settled and pending live-frontier functions consume it
with `into_parts`, validate the committed Left row against retention and
closure accounting, and return `LiveLeaveCommit` with the executable
`LiveFrontierOwner`
(`crates/liminal-protocol/src/lifecycle/operations/live_frontier.rs:255-321,324-400`).
The actual production path receives that complete value, projects before
`into_parts`, and carries the projection in `PreparedLeaveCommit`
(`crates/liminal-server/src/server/participant/production/ops_leave.rs:407-425`).
It appends the typed `Left` source first and records the projection afterward;
replay follows the same protocol transition and records the same canonical
producer
(`crates/liminal-server/src/server/participant/production/ops_leave.rs:187-217,258-303`).

Therefore calling the plain `LeaveCommit` arm as well would present the same
committed Left sequence twice. The current reconciliation branch accepting
`current >= presented` is replay/idempotency tolerance
(`crates/liminal-server/src/server/participant/production/handler_observer.rs:275-290`),
not authorization for duplicate producers. The single-presentation oracle in
§10 MUST be instrumented before that tolerance and MUST fail if both Leave
arms, or any two producer labels, present one source in the same pass.

Coordinator note: the ledger's `LeaveCommit` arm naming should amend to the
surviving `LiveLeaveCommit`; this brief does not edit the coordination-owned
ledger.

## 5. Projection timing and Died pending finalization

Projection extraction and projection presentation are two ordered moments:

1. while the typed source value is still owned, call its canonical projection
   method and carry the move-only result beside that prepared source;
2. append and flush the typed source;
3. only after that barrier, record/present the projection to the observer repair
   path; and
4. append/flush `Advance` before committing/firing observer publication.

The first step does not make progress durable. The source barrier is the
publication point. A source append/flush failure drops/aborts the prepared
projection and publishes no `Advance` or wake.

`DiedBindingTransition::Pending` projects `None`; only `Committed` reads the
terminal delivery sequence
(`crates/liminal-protocol/src/lifecycle/binding.rs:596-616`). The constructors
show that Pending owns a reserved transaction order and cause but no committed
delivery sequence, whereas Committed owns the terminal delivery sequence
(`crates/liminal-protocol/src/lifecycle/binding.rs:659-683`). Accordingly:

- the initial Pending source does not advance observer progress;
- W1 MUST NOT substitute the pending transaction order, next sequence, guessed
  maximum, or later record observation;
- if a crash occurs while finalization is still pending, replay restores the
  pending state and emits no `Advance`;
- the projection is minted only when a later durable finalization produces the
  committed Died terminal; and
- a crash after that finalization source flush but before `Advance` is exactly
  the §8 repair cut and is repaired on startup/cold first touch.

The committed restoration path validates the Died cause and reconstructs the
committed terminal at its exact delivery sequence
(`crates/liminal-protocol/src/lifecycle/binding.rs:864-920`).

## 6. Consumer shape: source-fed plus replay-repair

### 6.1 Candidate 1 selected — event-fed at the source barrier

W1 is **TOLD by the source barrier**. Each operation-specific owner extracts
the canonical projection while it still owns the typed fate, appends/flushes
the source, then calls
`ConversationAuthority::record_observer_progress_projection`. That method
updates the authority's measured progress and queues the move-only projection;
`take_observer_progress_projections` drains the queue in participant-log order
(`crates/liminal-server/src/server/participant/production/state.rs:159-164,267-281`).

The handler already detects a crossed source barrier by advancement of
`next_log_sequence`, immediately invokes `replay_and_repair` under the same
conversation lock, and withholds the correlated terminal response until that
returns. On reconciliation failure it clears the live owner so durable truth
is replayed next touch
(`crates/liminal-server/src/server/participant/production/handler.rs:168-241`).
No new background worker or notification API is required.

An implementation-private prepared source may carry `{ typed source,
projection }` across the append boundary. It must be move-only, must not expose
a raw projection constructor, and must not become a second authority. Existing
operation-specific prepared commits are the pattern; no public wrapper is
required.

### 6.2 Candidate 2 selected — startup and cold/first-touch replay-repair

W1 is also **TOLD by replay entry**. Startup enumerates registered
conversations and completes `replay_and_repair` before installing each owner
(`crates/liminal-server/src/server/participant/production/handler.rs:90-146`).
An absent owner on ordinary first touch takes the same path before the operation
runs; a crossed live barrier takes it again before the response escapes
(`crates/liminal-server/src/server/participant/production/handler.rs:184-223`).

`replay_and_repair` rebuilds typed authority, drains replayed projections,
ensures observer tracking, and reconciles progress before capacity fold or
owner publication
(`crates/liminal-server/src/server/participant/production/handler.rs:244-300`).
Reconciliation restores the observer owner if needed, validates conversation
identity, calls `decide_progress_advance`, appends/flushes `Advance`, commits,
and only then offers a fired wake
(`crates/liminal-server/src/server/participant/production/handler_observer.rs:224-337`).

Live application and replay MUST use the same canonical producer per source.
Ordinary fate restoration revalidates exact ordinary origin and committed Died
provenance; recovered fate restoration revalidates fenced-attach provenance,
participant, epoch, and floor
(`crates/liminal-protocol/src/lifecycle/storage.rs:1439-1480,1609-1659`).
Replay must project those restored typed values, never decode progress by hand.

### 6.3 Standalone cursor reader rejected

A standalone durable cursor reader is REJECTED. `DurableStore` offers bounded
reads but no subscription/wake operation
(`crates/liminal/src/durability/store.rs:20-57`). No explicit wake tells a
separate cursor that a participant source changed. It could discover change
only by periodic read, timer, scan, sweep, or synthetic probe.

LAW-1 says, verbatim, that a timer whose job is “check whether something
changed” is wrong and must be redesigned to be TOLD; it also classifies timer,
poll, sweep, scan, heartbeat, backoff, periodic reap, read-timeout wake,
stop-flag sampling, and synthetic probes as non-conforming change discovery
(`docs/design/LAW1-POLLING-RETIREMENT.md:9-22`). The selected source-barrier and
replay-entry triggers are already explicit events. A cursor reader would add
latency, another owner, and banned idle work without adding correctness.

## 7. Wiring points

The implementation MUST make the extraction point and the after-flush
presentation point visually distinct. These are the four arm-specific points
at the current bytes:

| fate | where the typed value is still owned | exact projection point and landing rule |
|---|---|---|
| Died | `ActiveBinding::finish_died` produces the complete `DiedBindingTransition` for all four Died causes (`crates/liminal-protocol/src/lifecycle/binding.rs:659-683,756-801`). At the pin, the only server repository operation receiving this value is the cfg(test) fixture's `commit_crash`; it is not a legal production owner (`crates/liminal-server/src/server/participant/crash_repository.rs:220-240,429-471`; module gate at `crates/liminal-server/src/server/participant/mod.rs:19-24`). | The production aggregate's Died source operation calls `DiedBindingTransition::observer_progress_projection` while it owns the transition, carries `Some` only for Committed, appends/flushes its existing typed source, then lands the value in `record_observer_progress_projection` (`crates/liminal-server/src/server/participant/production/state.rs:267-275`). It MUST NOT promote the fixture to create that owner. |
| ordinary binding fate | `AttachCommit::ordinary_binding_fate` consumes exact ordinary attach authority plus its committed Died terminal and returns `OrdinaryBindingFate` (`crates/liminal-protocol/src/lifecycle/attach.rs:164-188`). `decide_ordinary_binding_fate_operation` still owns and returns the fate intact through the source decision (`crates/liminal-protocol/src/lifecycle/aggregate_commit.rs:288-299`). | Call the ordinary arm before moving the fate into the source decision; after that operation's existing append/flush, feed the carried projection to `record_observer_progress_projection`. Replay calls the same arm on the restored typed fate before drain/reconcile. |
| recovered binding fate | `FencedAttachCommit::recovered_binding_fate` validates the exact recovered participant/epoch and returns `RecoveredBindingFate` (`crates/liminal-protocol/src/lifecycle/edge.rs:1007-1056`). `decide_recovered_binding_fate_operation` owns and returns it intact through the source decision (`crates/liminal-protocol/src/lifecycle/aggregate_commit.rs:301-313`). | Call the recovered arm before moving the fate into the source decision; after its existing append/flush, feed the carried projection to `record_observer_progress_projection`. Replay restores provenance and calls the same arm. |
| Leave | Production's `finish_leave_transition` owns `LiveLeaveCommit`, calls its projection, then consumes it with `into_parts` (`crates/liminal-server/src/server/participant/production/ops_leave.rs:407-425`). | This call already exists. Preserve source append before record at `crates/liminal-server/src/server/participant/production/ops_leave.rs:187-217` and replay record at `crates/liminal-server/src/server/participant/production/ops_leave.rs:258-303`; remove, do not call, the plain `LeaveCommit` duplicate. |

The Died row is intentionally explicit about the current absence of a
production repository owner. W1 does not solve that absence by enabling
`ParticipantCrashRepository`. The production fate operation must live behind
the same `ConversationAuthority`/aggregate boundary as the other operation
sources and must use its existing typed source persistence. If that source
operation is not present in an implementation dispatch, the W1 build is
incomplete; a test-only caller does not satisfy wire-with-oracle.

## 8. Ownership and failure atomicity

### 8.1 One aggregate owner

`ConversationAuthority` is the sole live production owner of one
conversation's protocol state and observer-projection queue
(`crates/liminal-server/src/server/participant/production/state.rs:123-165`).
Participant module policy says the older crash/cursor/detach repositories
remain test-only until their operations move **behind the aggregate**
(`crates/liminal-server/src/server/participant/mod.rs:1-6`).

Therefore any useful codec or replay behavior borrowed from
`ParticipantCrashRepository` is moved behind `ConversationAuthority` (or its
aggregate-owned operation implementation). Promoting the cfg(test) module,
constructing a second lifecycle state, or writing an independent crash stream
from connection teardown is FORBIDDEN. Storage helpers may be stateless;
lifecycle selection and installed authority remain singular.

### 8.2 Failure and refusal behavior

- Failure before the typed source flush publishes neither `Advance` nor wake.
- Failure after source flush but before/during `Advance` leaves committed source
  truth and returns a typed internal durability fault; startup/cold replay
  repairs it.
- A wrong projection conversation, untracked conversation, aggregate refusal,
  observer append failure, nonmonotone source, source decode/provenance failure,
  or arithmetic exhaustion is loud and typed. No default, saturation-as-policy,
  fabricated refusal value, or best-effort skip is allowed.
- Observer-head advancement uses checked arithmetic, as the existing
  reconciliation does
  (`crates/liminal-server/src/server/participant/production/handler_observer.rs:300-331`).
- A durable `Advance` equal to or above a replayed presentation may satisfy
  idempotent reconciliation. It does not waive source validation and does not
  legalize two canonical producers.
- After `Advance` flush, a missing arm association is a successful drop of that
  targeted wake only. `publish_fired_observer` removes only the matching
  conversation target and treats no target as `Ok(())`
  (`crates/liminal-server/src/server/participant/production/handler_observer.rs:379-409`).
  The target itself is weak; a dead inbox returns `Ok(false)` without broadcast
  (`crates/liminal-server/src/server/participant/publication.rs:70-115`).

## 9. Durability-subsystem premise and no-polling precision

The durability premise is exact: **durable-channel recovery IS
production-wired**. Both durable channel constructors call `recover_durable`
(`crates/liminal/src/channel/types.rs:189-245`), and that function drives
`recover_durable_channel` through the durability bridge
(`crates/liminal/src/channel/types.rs:564-588`).

The generic `ConsumerCursor`, generic `DurableConversation`, and
`DedupSweeper` remain unwired into production runtime ownership at this pin;
their concrete declarations are
`crates/liminal/src/durability/cursor.rs:3-18`,
`crates/liminal/src/durability/conversation.rs:156-184`, and
`crates/liminal/src/durability/dedup/sweep.rs:39-75`. Their source census has
module/test consumers, but no production W1 entry point. W1 does not change
that status.

`DedupSweeper` is interval-configured, scan-based TTL cleanup. It MUST stay out
of W1 change discovery. It is neither an observer-progress notification source
nor a substitute for source-barrier/replay events. Scheduling it to discover
fates would violate LAW-1 and would conflate dedup expiry with participant
lifecycle truth.

## 10. Acceptance oracles — complete nine-name census

Every arm fixture is independent: it constructs its own exact protocol
provenance and asserts its own projected row. No shared shortcut may manufacture
an `ObserverProgressProjection`, and no test may derive the expected value by
calling the method under test. Asynchronous tests use deterministic append,
flush, owner, and weak-target gates; no sleeps or eventual polling.

| item | exact oracle | required proof |
|---:|---|---|
| 1 | `committed_died_binding_transition_projects_terminal_delivery_sequence` | Independently construct a committed Died transition and assert conversation plus exact committed terminal `delivery_seq`. |
| 2 | `pending_died_binding_transition_projects_no_observer_progress` | Independently construct Pending Died and assert `None`; prove no source order or guessed next sequence is substituted. |
| 3 | `ordinary_binding_fate_projects_measured_resulting_floor` | Independently construct ordinary attach provenance, exact committed Died terminal, and a measured floor distinct from nearby cursor/terminal values; assert that exact floor. |
| 4 | `recovered_binding_fate_projects_measured_resulting_floor` | Independently construct fenced-recovery provenance and recovered fate with a distinguishing measured floor; assert that exact floor. |
| 5 | `live_leave_commit_projects_committed_left_delivery_sequence` | Exercise the actual settled and pending production-survivor shape and assert the committed `Left` sequence; do not call the plain `LeaveCommit` arm. |
| 6 | `one_committed_fate_has_exactly_one_canonical_projection_presentation` | Instrument the pre-tolerance presentation boundary with fate-occurrence identity plus producer label. Each pass must present once from its table-selected producer. Fixtures enabling raw Died plus Ordinary, raw Died plus Recovered, or plain plus live Leave must each fail with count two; `current >= presented` cannot hide any duplicate. Later independent cold replay remains allowed as a new pass. |
| 7 | `live_source_flush_precedes_observer_advance_flush` | Through the real production handler, gate source append/flush and observer `Advance` append/flush independently for each newly wired source class. Before source flush there is no Advance or wake. After source flush, an Advance failure returns typed fault and no wake; successful Advance flush precedes publication. |
| 8 | `cold_first_touch_repairs_missing_advance_before_authority_publication` | Cut after source append/flush and before Advance, discard live owners, and enter only through startup and ordinary cold first touch. Each route replays the typed source, appends one exact missing Advance, restores observer state, and only then installs authority/admit handshake; a second replay appends nothing. Include Pending Died as the negative no-Advance arm. |
| 9 | `dead_or_missing_weak_observer_handle_drops_only_targeted_wake` | Cut after Advance flush before handoff, and separately drop the weak inbox before publication. Durable progress survives; only the matching wake is absent. No broadcast, other-target removal, semantic-response displacement, or retry/poll occurs, and reattach observes progressed or re-arms under observer-owner order. |

The implementation fold MUST place these exact names in the appropriate
protocol and production suites. Existing broader Unit 2 test 25 does not waive
the independent per-arm floor or the strengthened single-presentation proof.

## 11. Honesty, cost, non-goals, and deferrals

### 11.1 Idle and steady-state cost

Expected idle and no-fate steady-state cost is **none beyond the existing
paths**: zero new task, thread, timer, poll, sweep, cursor, queue, wake source,
allocation, store read, or application slice. Across `N` idle conversations,
W1's added application wake/read ceiling is `0 × N = 0`.

On a committed fate, W1 adds only the selected projection extraction/queueing
and the existing observer reconciliation work. A missing Advance costs one
observer append/flush and any exact targeted wake already prescribed by §8.
Startup and cold first touch already pay `replay_and_repair`; W1 adds the newly
recognized typed sources to that fold, not another traversal. Test
instrumentation is cfg(test)-only.

### 11.2 Non-goals

- no fate-class, cause, participant, or epoch reconstruction from projections;
- no full-fate repository and no promotion of the crash fixture;
- no new persisted schema beyond an existing observer-v1 Advance produced by
  the ruled disposition;
- no protocol wire/schema change and no observer targeting schema change;
- no haematite branch/WAL/crash-gate surface;
- no generic cursor/conversation wiring and no DedupSweeper scheduling;
- no SDK, transport, ParticipantAck, recipient-outbox, or socket receipt change;
- no new public API beyond the minimum internal wiring carrier, if one is
  required; and
- no tolerance-based defense of duplicate canonical producers.

### 11.3 Deferred items with owner and trigger

1. **Queryable full crash-fate history.** Owner: a coordinator-assigned future
   participant lifecycle/storage lane, not W1. Trigger: a named product consumer
   requires fate class/cause/participant/epoch queries not answerable from its
   existing typed source home. Before implementation, add a wiring-ledger row
   carrying consumer, trigger, owner, schema/migration contract, and oracle
   floor.
2. **Generic cursor/conversation runtime consumers.** Owner: their future named
   durability lanes. Trigger: a production feature selects those abstractions.
   W1 creates no implied road and grants no timer-based implementation.
3. **Dedup TTL scheduling.** Owner: the dedup maintenance/LAW-1 lane. Trigger:
   explicit expiry maintenance receives an event-driven schedule and idle-cost
   signoff. It can never be triggered to discover observer progress.

Nothing inside the narrowed progress-repair contract is deferred: canonical
producer selection, Died Pending timing, source-fed presentation,
startup/cold-first-touch repair, ownership, and all nine oracles land together.

## 12. Walls

- **WALL-NARROWED-CONSUMER:** W1 drives only §8 observer-progress crash-window
  repair; projection payload is never fate reconstruction.
- **WALL-FULL-FATE-DISPOSITION:** the only W1 durable output is an existing
  observer-v1 Advance; a full-fate repository requires a separate ledger lane.
- **WALL-CANONICAL-PRODUCER:** one source fate has one producer per pass.
  `LiveLeaveCommit` survives; the duplicate plain `LeaveCommit` projection does
  not.
- **WALL-SINGLE-PRESENTATION-ORACLE:** duplicate producer presentation fails
  before `current >= presented` tolerance can mask it.
- **WALL-SOURCE-BEFORE-ADVANCE:** the typed source append/flush completes before
  projection presentation and observer Advance append/flush.
- **WALL-DIED-FINALIZATION:** Pending Died projects no progress. Only committed
  finalization may project its exact terminal delivery sequence.
- **WALL-TWO-TOLD-PATHS:** live source-barrier events and startup/cold replay
  entries are the only change-discovery triggers.
- **WALL-NO-POLLING:** no timer, cursor loop, sweep, scan, heartbeat, backoff,
  read timeout, stop-flag sample, or synthetic probe discovers progress.
- **WALL-AGGREGATE-OWNER:** reused repository behavior moves behind
  `ConversationAuthority`; the cfg(test) fixture never becomes a second owner.
- **WALL-LIMINAL-DURABILITY-BOUNDARY:** W1 uses `DurableStore` and observer-log
  machinery, never a direct haematite crash-gate API.
- **WALL-NO-PROTOCOL-SCHEMA:** no protocol wire/schema or observer-v1 schema
  change.
- **WALL-NO-NEW-PUBLIC-API:** internal move-only wiring only; no raw projection
  constructor or public fate repository.
- **WALL-CHECKED-ARITHMETIC:** sequence/head/count arithmetic and conversions are
  checked; no saturating fallback is used as correctness policy.
- **WALL-TYPED-REFUSAL:** corruption, provenance disagreement, nonmonotone
  progress, append failure, owner mismatch, and overflow fail loudly and typed;
  no silent fallback or fabricated protocol response.
- **WALL-DROPPED-WAKE:** a dead/missing weak handle drops only its targeted wake
  after durable Advance; it never broadcasts or rolls progress back.
- **WALL-DOCS-ONLY-BRIEF:** r1 changes this design document only; implementation
  follows under a separately reviewed fold.

## 13. Revision record

| revision | date | record |
|---|---|---|
| r1 | 2026-07-19 | Initial W1 consumer brief against ledger r1.5: narrowed projections to §8 progress repair; quoted source-before-Advance, cold repair, weak-target, and crash-cut law; ruled the liminal/haematite boundary; made full-fate persistence explicit; selected `LiveLeaveCommit` and removal of the duplicate plain Leave producer; fixed Died Pending timing; selected source-fed plus startup/cold replay-repair; rejected cursor polling; pinned aggregate ownership, durability-subsystem premise, nine exact oracles, costs, deferrals, and walls. |
