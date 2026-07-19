# W1a — Leave-survivor wiring and observer reconcile-conformance repair

Brief base: `docs/w1-crash-window-repair@f7dff3d`. Source-code anchors remain
at `de4f987`; `origin/main@8d9ee23` adds the normative r1.6 phase ruling and
no source changes used by this brief.

Revision: r2, 2026-07-19. Normative ledger:
`8d9ee23:docs/design/WIRING-LEDGER.md`, r1.6. The Unit 2 source of record is
`docs/design/F0C-UNIT2-SERVERPUSH-PRODUCER.md`, SHA-256
`98f9130faa175f323206eb6640e9f625ab6385dc285e11f5b2774fb306b5de6a`.
Every repository anchor in this revision was reopened against the cited bytes.

Path decision: keep `docs/design/W1-CRASH-WINDOW-REPAIR.md`. The r1.6 ledger
retitles and phases the lane but names no replacement document path. Retitling
the heading to W1a while preserving the path keeps the r1 review and r2
supersession in one auditable history.

## Goal

Close only wireable lane W1a:

1. make `LiveLeaveCommit` the sole permanent-Leave observer-progress producer,
   remove the dormant plain `LeaveCommit` projection arm, and prove one
   presentation per Leave occurrence; and
2. repair the existing §8 reconciliation path so historical-prefix idempotency
   is accepted only after exact source-witness validation, while decreasing,
   disagreeing, and unsupported-ahead progress refuses loudly before the
   numeric tolerance.

W1a does not invent the absent Died/Ordinary/Recovered production sources.
Their `StoredOperation` variants, flush barriers, replay transitions, canonical
occurrence routing, and migration contract are the separate design-first W1b
lane queued behind W1a.

## 1. r1.6 phase ruling, scope, and severity

### 1.1 Normative phase

The r1.6 ledger records that production has no Died, Ordinary, or Recovered
`StoredOperation` variant, no BindingFate replay branch, and no durable home
from which cold replay could reconstruct those projections. It expressly
phases W1 into W1a and W1b
(`8d9ee23:docs/design/WIRING-LEDGER.md:29-40`). The byte evidence is exact:
`StoredOperation` has Genesis, Enrolled, Attached, Detached, ZeroDebtAck,
MarkerDrained, RecordAdmission, and Left only
(`crates/liminal-server/src/server/participant/production/log.rs:150-222`),
and the exhaustive production replay match has no Died or BindingFate branch
(`crates/liminal-server/src/server/participant/production/ops_session.rs:441-517`).

The W1a scope is exactly the ledger's two-part ruling:

- the canonical Leave survivor and its single-presentation proof; and
- reconcile-conformance validation on the existing §8 path.

The ledger calls Leave the only wireable original arm and assigns both pieces
to W1a (`8d9ee23:docs/design/WIRING-LEDGER.md:42-57`). W1a validation applies
to every projection already reaching that shared reconcile path—existing
attach, detach, ack, marker-ack, and Leave sources—not only to Leave.

### 1.2 Disclosure-class gap on main

This is a **DISCLOSURE-CLASS gap on main, the same species as W3 row-R,
independent of W1a's implementation**. Today
`record_observer_progress_projection` silently stores a maximum while retaining
all presentations, and reconciliation silently continues whenever
`current >= presented`
(`crates/liminal-server/src/server/participant/production/state.rs:267-280`;
`crates/liminal-server/src/server/participant/production/handler_observer.rs:275-290`).
Section 8 instead requires disagreement and a nonmonotone source to refuse
loudly.

The coordination seat has recorded severity and assigned the repair to W1a
because it owns the same reconcile path
(`8d9ee23:docs/design/WIRING-LEDGER.md:45-51`). The posture is disclosure with
teeth: no separate escalation blocks this r2 paper fold, but W1a cannot close
until the production repair and its refusal oracles land. The existing behavior
is not relabeled safe, latent, or “close enough.”

### 1.3 r2 supersession statement

R2 supersedes these r1 claims:

- that all four fate projections were implementable in one lane;
- that Ordinary and Recovered were both later interpretations of an input Died
  terminal;
- that a pending Died later finalized by producing a new
  `DiedBindingTransition`; and
- that `current >= presented` could remain the general test for historical
  prefix acceptance.

The corrected contracts are in §§4, 5, 7, and 8.

## 2. Consumer statement and normative §8 contract

### 2.1 W1a consumer

The consumer remains exactly the Unit 2 §8 observer-progress crash-window
repair. `ObserverProgressProjection` is move-only, lifecycle-constructed, and
contains only `conversation_id` and `new_observer_progress`
(`crates/liminal-protocol/src/lifecycle/observer_recovery.rs:10-50`). It can
drive durable progress reconciliation. It cannot reconstruct fate class,
cause, participant, binding epoch, or source identity. W1a therefore carries
source/occurrence metadata beside the projection inside the server; it never
derives that metadata from the two-field payload.

### 2.2 Operative source/Advance and cold-repair lines

W1a conforms to these Unit 2 lines verbatim:

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

“Equal-or-greater exact” is not permission for an arbitrary higher number. R2
makes “exact” executable: the durable value must be the Track baseline or equal
a progress value surrendered by a validated source prefix in this complete
replay pass. Section 5 defines that witness rule.

### 2.3 Operative weak-target and crash-cut lines

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

## 3. Persistence disposition after phasing

### 3.1 W1a: r1.5 no-new-row disposition stands

For W1a, the observer-v1 `Advance { conversation_id, observer_progress }` is
the only additional durable output that repair may append. W1a adds no
participant `StoredOperation`, observer row kind, stream, schema version,
compatibility decoder, or repository. The observer log already persists and
restores Track, Advance, and Arms through `DurableStore`
(`crates/liminal-server/src/server/participant/production/observer.rs:19-49,66-140,144-178`).
Production Leave already persists its complete `Left` replay row before
recording the projection
(`crates/liminal-server/src/server/participant/production/ops_leave.rs:187-217`).

The source-witness metadata in §5 is replay-pass state derived from existing
durable rows. It is not serialized into the observer log and does not weaken
source validation.

### 3.2 W1b: r1.5 no-new-row disposition is expressly superseded

The r1.5 “Advance only; no new source row” disposition does **not** apply to the
three absent fate sources. The r1.6 revision header expressly amends it, and the
phase record says it stands for W1a but is superseded for W1b
(`8d9ee23:docs/design/WIRING-LEDGER.md:3-6,31-40`). W1b must design durable
source rows before its projections can have a source barrier or cold replay.
R1's statement that those typed values could simply remain in existing
production homes was false: `BindingFateOperation` is protocol codec machinery,
not a production `StoredOperation` home.

Exactly which fields and schema W1b persists remains open to W1b's own reviewed
design. W1a neither prejudges nor implements it.

## 4. Canonical permanent-Leave producer

### 4.1 Survivor ruling

`LiveLeaveCommit::observer_progress_projection` is the sole canonical permanent
Leave producer (`crates/liminal-protocol/src/lifecycle/operations/live_frontier.rs:210-229`).
The plain `LeaveCommit::observer_progress_projection` at
`crates/liminal-protocol/src/lifecycle/membership.rs:514-525` is removed in the
W1a implementation fold. It does not remain as a dormant alternate.

The reason is ownership at the actual production path. Both settled and pending
frontier functions consume the intermediate plain `LeaveCommit`, validate Left
retention/closure facts, and return the complete `LiveLeaveCommit` with its
executable `LiveFrontierOwner`
(`crates/liminal-protocol/src/lifecycle/operations/live_frontier.rs:255-321,324-400`).
Production owns that final value, calls its projection before `into_parts`, and
carries the projection with `PreparedLeaveCommit`
(`crates/liminal-server/src/server/participant/production/ops_leave.rs:407-425`).
The `Left` source append precedes record in live application, and replay
reconstructs the same surviving producer
(`crates/liminal-server/src/server/participant/production/ops_leave.rs:187-217,258-303`).

### 4.2 Leave occurrence and producer metadata

Each recorded Leave presentation carries an implementation-private witness:

- durable source identity: base-log sequence plus `Left` kind;
- occurrence identity: `{ conversation_id, participant_id, left_delivery_seq }`;
- producer identity: `LiveLeaveCommit`; and
- the sealed `ObserverProgressProjection`.

The metadata is taken from the typed `StoredLeave`/committed result while those
values are owned. It is not reconstructed from progress. Two presentations of
the same occurrence in one replay pass are a typed duplicate-producer error,
even when their numeric values are equal.

The structural half of acceptance proves the plain projection method is absent.
The dynamic half uses a cfg(test)-only injection seam: it obtains the projection
from a real `LiveLeaveCommit`, presents the same explicit occurrence/source a
second time with a distinct injected producer label, and requires rejection
before any observer-current comparison. Production carries no injection API or
counter.

## 5. Reconcile-conformance validation

### 5.1 Current defect

Current accumulation is lossy for validation: it replaces authority progress
with a maximum and separately queues all projections
(`crates/liminal-server/src/server/participant/production/state.rs:267-280`).
Current reconcile then examines each bare projection and treats every
`current >= presented` as satisfied
(`crates/liminal-server/src/server/participant/production/handler_observer.rs:257-290`).
Consequently a decreasing source pass, an unsupported durable value ahead of
all sources, or a durable value between source witnesses can pass silently.
That contradicts §8.

### 5.2 Replay-pass source witness

Replace the bare vector with an internal move-only
`ObserverProgressSourceWitness` per presentation. The exact spelling is an
implementation detail, but the owned facts are normative:

1. sealed projection;
2. checked replay ordinal in the conversation's actual merged replay order;
3. durable source identity and kind:
   - base source: operation-log sequence plus `StoredOperation` kind;
   - extension source: `{ base_log_head, extension_sequence }` plus extension
     kind;
4. occurrence identity and canonical producer label when the source has more
   than one structurally possible producer (Leave in W1a).

The base replay loop already passes each physical operation sequence into
`replay_operation`
(`crates/liminal-server/src/server/participant/production/ops_session.rs:441-448`).
MarkerAck rows already persist both ordering components
(`crates/liminal-server/src/server/participant/production/ops_acks.rs:328-344`)
and replay through their typed row
(`crates/liminal-server/src/server/participant/production/ops_acks.rs:365-452`).
W1a threads those existing facts to the record call; it does not invent a
parallel order.

Every current producer must supply a witness. Existing examples include attach
(`crates/liminal-server/src/server/participant/production/ops_attach.rs:240-265,315-319`),
detach
(`crates/liminal-server/src/server/participant/production/ops_session.rs:201-266`),
ack and MarkerAck
(`crates/liminal-server/src/server/participant/production/ops_acks.rs:162-207,299-358,365-452`),
and Leave
(`crates/liminal-server/src/server/participant/production/ops_leave.rs:187-217,258-303`).

### 5.3 Complete preflight before numeric tolerance

`record_observer_progress_projection` becomes fallible and preserves the last
accepted witness rather than computing a maximum. During participant replay it
validates, in participant-log order:

- projection conversation equals replay conversation;
- checked replay ordinals and durable source positions are strictly ordered;
- one occurrence has one canonical producer in the pass;
- projected progress is nondecreasing (`next >= previous`); and
- all source/occurrence fields agree with the typed row that supplied them.

An equal progress from two distinct, valid source occurrences is legal; a
second producer for one occurrence is not. A decreasing value is refused at
its source witness. No observer owner has been restored or mutated yet.

After the **complete** participant/outbox merge and semantic replay succeeds,
`replay_and_repair` drains one validated pass. Current control flow already
replays the complete authority before draining projections
(`crates/liminal-server/src/server/participant/production/handler.rs:251-275`).
Only then may reconciliation lock/restore the observer owner.

### 5.4 Exact historical-prefix witness rule

Let the validated nondecreasing progress sequence be `P = [p0, ..., pn]` and
let `A` be durable observer progress restored from the observer aggregate. Let
`T = pn` when the pass is nonempty, otherwise `T = 0`.

W1a selects this exact rule:

1. The explicit Track baseline `0` is a valid empty-prefix witness.
2. Any nonzero `A` is supported only if `A == pi` for at least one witness in
   the complete validated pass.
3. `A > T` is `AheadOfValidatedSourceMaximum` and refuses.
4. `0 < A <= T` with no exact `pi == A` is
   `AdvanceWithoutExactSourceWitness` and refuses. A numerically in-range value
   is not historical testimony.
5. Only after steps 1-4 succeed may the leading witnesses with progress
   `<= A` be classified as the already-durable historical prefix. This is the
   sole legal use of numeric tolerance.
6. Reconcile the remaining suffix in order. Append an Advance only for each
   strict increase above current; equal-progress later source occurrences have
   already been validated and require no duplicate row.
7. On complete success, restored observer progress MUST equal `T` exactly.

This is the no-new-observer-schema equivalent of durable source identity in an
Advance row: the observer number is accepted only as an exact member of a
complete, ordered, provenance-bearing source pass. Durable identity remains in
the participant/extension source; transient source identity proves which pass
and prefix make the number legal.

The empty-pass case is load-bearing: durable `A == 0` is valid, while any
nonzero Advance with no source witness refuses. A repeated progress value is
resolved to the greatest leading prefix at that value; because the pass is
nondecreasing, this never skips a lower later source.

### 5.5 Typed refusal and mutation boundary

Add a closed internal `ObserverProgressConformanceError` and carry it through a
dedicated `StateError` variant rather than a formatted invariant string. Its
minimum variants are:

- `ConversationMismatch`;
- `SourceOrder`;
- `DuplicateOccurrenceProducer`;
- `DecreasingSourceProgress`;
- `AheadOfValidatedSourceMaximum`;
- `AdvanceWithoutExactSourceWitness`; and
- `FinalProgressMismatch`.

The production boundary reports the typed conformance failure and publishes no
fabricated protocol value. “Refusal” here means replay/reconciliation refuses
the durable state before classification; it is never silent acceptance and
never a “close enough” `>=` comparison. It does not manufacture an
`InvalidObserverEpoch`, which is a request-specific protocol outcome
(`crates/liminal-protocol/src/wire/authority/records.rs:167-209`).

No observer append, transaction commit, arm removal, wake, conversation-owner
installation, capacity fold, or ObserverRecovery classification occurs before
complete source preflight and exact-witness validation. Once preflight passes,
the existing append/flush, checked head increment, transaction commit, and
publication order remains
(`crates/liminal-server/src/server/participant/production/handler_observer.rs:291-336`).

If a later Advance append fails after an earlier suffix Advance flushed, the
durable value is still an exact validated source prefix. The next replay may
resume from that exact witness. W1a adds no rollback fiction and no repair row
before full source validation.

## 6. Live, cold-first-touch, and weak-target behavior

### 6.1 Live Leave ordering

The live Leave path extracts the surviving projection while owning
`LiveLeaveCommit`, appends/flushes `StoredOperation::Left`, records its complete
source witness, and advances the source head
(`crates/liminal-server/src/server/participant/production/ops_leave.rs:187-217,407-425`).
The handler detects the crossed source barrier and runs `replay_and_repair`
before the correlated terminal response can escape
(`crates/liminal-server/src/server/participant/production/handler.rs:168-241`).
Source failure yields no Advance or wake. Advance failure leaves the committed
Left source for cold repair and yields no wake.

### 6.2 Startup and ObserverRecovery-first cold repair

Startup replays registered conversations before installing owners
(`crates/liminal-server/src/server/participant/production/handler.rs:90-146`).
Ordinary absent-owner first touch also replays under the conversation lock
(`crates/liminal-server/src/server/participant/production/handler.rs:184-223`).

The load-bearing W1a cold oracle uses neither route first. After cutting between
source flush and Advance, its **first touch is `apply_observer_recovery`**. That
method runs `ensure_tracking_from_log` for every named conversation before it
locks/restores and classifies the observer batch
(`crates/liminal-server/src/server/participant/production/handler_observer.rs:33-75`).
An absent owner reaches `replay_and_repair` before classification
(`crates/liminal-server/src/server/participant/production/handler_observer.rs:345-365`).
The oracle must show the missing Advance flushes and exact witness validation
completes before the handshake can be classified as progressed, armed, or
invalid.

### 6.3 Dead or missing weak target

After an Advance flush/commit, publication removes only the matching arm target;
a missing association is `Ok(())`
(`crates/liminal-server/src/server/participant/production/handler_observer.rs:379-409`).
The publication target owns a weak inbox; failed upgrade returns `Ok(false)`
and cannot broadcast
(`crates/liminal-server/src/server/participant/publication.rs:70-115`). W1a
preserves that rule after conformance validation: durable progress survives and
only that targeted volatile wake is dropped.

### 6.4 TOLD, never polled

Live source barriers and explicit startup/request first touches are the only
repair triggers. `DurableStore` has no notification operation
(`crates/liminal/src/durability/store.rs:20-57`), so a standalone cursor would
need timer/scan discovery. LAW-1 says a timer whose job is to check whether
something changed is wrong and must be redesigned to be TOLD
(`docs/design/LAW1-POLLING-RETIREMENT.md:9-22`). W1a adds no reader, timer,
sweep, scan, backoff, read timeout, synthetic probe, or `DedupSweeper` role.

## 7. Correct pending-finalization contract

R1's pending-Died story was false. `ActiveBinding::finish_died` runs once at the
initial transition and returns either Committed or Pending
`DiedBindingTransition`
(`crates/liminal-protocol/src/lifecycle/binding.rs:659-683`). Later
`PendingFinalization::commit` returns `CommittedBindingTerminal`, not another
`DiedBindingTransition`
(`crates/liminal-protocol/src/lifecycle/binding.rs:552-558`). Therefore a future
finalizer cannot be told to “call the Died arm when it returns” unless W1b first
designs an explicit route from the committed terminal/source row.

What W1a covers today is precise: Leave accepts a generic
`PendingFinalization`, assigns the pending terminal sequence and then the Left
sequence, and calls `commit_pending_leave_frontier`
(`crates/liminal-server/src/server/participant/production/ops_leave.rs:344-404`).
That protocol function consumes the pending terminal and returns one
`LiveLeaveCommit`
(`crates/liminal-protocol/src/lifecycle/operations/live_frontier.rs:324-400`).
Production projects the **Left sequence** from that final Leave commit
(`crates/liminal-server/src/server/participant/production/ops_leave.rs:407-425`).
Thus W1a covers a pending-Died terminal finalized atomically by Leave as one
canonical Leave occurrence. It does not additionally present the old pending
Died arm or the committed terminal sequence.

All other pending-finalizer routes and their source/projection choice belong to
W1b's mandatory routing table.

## 8. W1b deferral — Died/Ordinary/Recovered source design

### 8.1 Owner, consumer, and trigger

W1b is a real ledgered road back, not a W1a TODO. Its owner is **Hermes**. Its
named consumer is the full §8 crash-fate window repair. Its trigger is a
separate design brief at Hermes's seat with its own review round
(`8d9ee23:docs/design/WIRING-LEDGER.md:59-69`). It is queued behind W1a.

W1b owns the missing Died, Ordinary, and Recovered `StoredOperation` source
rows, replay transitions, source append/flush barriers, and cold
reconstruction. W1a MUST NOT add partial variants or a test-only production
caller.

### 8.2 Correct semantics W1b must inherit

Ordinary and Recovered are not symmetric:

- `AttachCommit::ordinary_binding_fate` consumes an exact
  `CommittedDiedTerminal` and measured floor
  (`crates/liminal-protocol/src/lifecycle/attach.rs:164-188`).
- `FencedAttachCommit::recovered_binding_fate` consumes only a
  `BindingFateObserved` event carrying participant, binding epoch, and resulting
  floor; it does **not** consume a Died terminal
  (`crates/liminal-protocol/src/lifecycle/edge.rs:1007-1056`).

R1's contrary sentence is superseded. The canonical occurrence identity
`(conversation_id, participant_id, binding_epoch)` needed to dispatch exactly
one of Died/Ordinary/Recovered is a W1b design decision. W1b must prove where
that tuple comes from on every live and replay path and must not infer a
Recovered Died input that does not exist.

### 8.3 Mandatory W1b decision tables

The W1b brief is incomplete unless it rules every row in both tables.

| durable-source decision | W1b must rule |
|---|---|
| schema/version | Whether the three variants extend the participant v2 stream or use a new version/stream; exact canonical fields and source identity for each fate. |
| migration or refusal | Exact behavior for existing v2 histories and old, missing, unknown, malformed, or mixed-version rows. No default, alias, dual interpretation, or silent omission. |
| Died emission census | Which live paths emit a Died source at all: connection loss, process killed, protocol error, unclean restart, and any other audited close path; which do not, and why. |
| ordinary emission | Where the committed Died terminal and ordinary attach authority meet, when its source flushes, and how replay rebuilds the sealed fate. |
| recovered emission | Where `BindingFateObserved` is durably represented, why no Died terminal is required, and how fenced provenance is replayed. |
| canonical occurrence routing | How `(conversation_id, participant_id, binding_epoch)` selects one producer and prevents Died/Ordinary/Recovered double presentation. |
| replay and repair | Which `replay_operation` branches reconstruct each projection before the same §5 conformance validator, including corruption and crash cuts. |

| pending-finalizer route | W1b must rule |
|---|---|
| pending terminal finalized by Leave | Inherited fact: W1a emits only the canonical LiveLeaveCommit Left projection; W1b must not add a second Died presentation. |
| pending Died finalized outside Leave | Name every production owner, durable row, resulting committed-terminal type, selected canonical projection producer, and §8 barrier. |
| pending Detached finalized outside Leave | State whether it can affect hard-observer progress and identify its producer or explicit non-producer disposition. |
| attach/recovery interaction | Census every route that consumes pending state during attach or fenced recovery and rule whether the fate source is Died, Ordinary, Recovered, or none. |
| restart restoration | Reconstruct pending cause/epoch/order, final committed terminal, and selected source without re-running `finish_died` or fabricating `DiedBindingTransition`. |

### 8.4 Required W1b per-arm oracle names

The r1.5 four-arm promise remains visible across the phase: Leave stays in
W1a; these three exact per-arm names are REQUIRED in W1b's floor:

1. `died_binding_transition_projects_terminal_sequence_only_when_committed` —
   independent Committed and Pending fixtures; Committed asserts exact terminal
   sequence and Pending asserts `None`.
2. `ordinary_binding_fate_projects_measured_resulting_floor` — independent
   ordinary attach plus exact committed-Died provenance, asserting a
   distinguishing measured floor.
3. `recovered_binding_fate_projects_measured_resulting_floor` — independent
   fenced-recovery plus BindingFateObserved provenance, asserting a
   distinguishing measured floor and no invented Died terminal.

W1b must add per-fate append/flush, replay, cold reconstruction, schema refusal,
and finalizer-routing oracles beyond this inherited per-arm minimum, as its
ledger row requires (`8d9ee23:docs/design/WIRING-LEDGER.md:59-69`).

## 9. Boundary, ownership, and durability precision

### 9.1 Liminal/haematite boundary

W1a consumes `liminal_protocol::lifecycle::ObserverProgressProjection` and
persists/replays through liminal's `DurableStore` and observer-log machinery.
It does not call haematite branch/WAL crash-gates. The lifecycle-facing surface
is `DurableStore` (`crates/liminal/src/durability/store.rs:20-57`), its
production adapter delegates to `EventStore`
(`crates/liminal/src/durability/store.rs:60-191`), and bootstrap wraps the
engine in `HaematiteStore`
(`crates/liminal-server/src/server/connection/services.rs:926-927`). Tests gate
liminal append/flush/owner seams, not engine-private gates.

### 9.2 One aggregate owner

`ConversationAuthority` remains the sole live production owner and owns the
projection-witness pass
(`crates/liminal-server/src/server/participant/production/state.rs:123-165`).
The older crash/cursor/detach repositories remain test-only until their
operations move behind the aggregate
(`crates/liminal-server/src/server/participant/mod.rs:1-24`). W1a does not
promote `ParticipantCrashRepository`; W1b inherits the same rule.

### 9.3 Durability subsystem premise

Durable-channel recovery is production-wired: both durable constructors call
`recover_durable`, which drives recovery through the durability bridge
(`crates/liminal/src/channel/types.rs:189-245,564-588`). Generic
`ConsumerCursor`, generic `DurableConversation`, and `DedupSweeper` remain
unwired into production ownership; W1a changes none of them. `DedupSweeper` is
interval-configured scan-based TTL cleanup
(`crates/liminal/src/durability/dedup/sweep.rs:39-75`) and remains outside
change discovery.

## 10. W1a acceptance oracles — complete seven-name census

Each exact name appears once. Tests use deterministic source/Advance barriers,
explicit owner state, and weak-target controls; no sleeps or polling.

| item | exact oracle | required proof |
|---:|---|---|
| 1 | `live_leave_commit_projects_left_sequence_for_settled_and_pending_paths` | Independent settled and pending fixtures exercise the surviving `LiveLeaveCommit`. Each asserts conversation and exact committed Left sequence. The pending fixture distinguishes terminal sequence from Left sequence and proves only Left is presented. |
| 2 | `leave_projection_has_one_surviving_producer_and_duplicate_injection_refuses` | Structural source/AST check proves the plain `LeaveCommit` projection method is absent. A cfg(test)-only seam obtains a real surviving projection, supplies explicit source/Leave-occurrence/producer metadata, injects a second producer for the same occurrence, and receives `DuplicateOccurrenceProducer` before observer restore or `current >= presented`. The oracle must fail if validation is removed or moved after tolerance. |
| 3 | `decreasing_source_progress_refuses_before_numeric_tolerance` | Replay two real source witnesses in increasing source order with decreasing projected progress. Assert exact `DecreasingSourceProgress`, no observer append/arm removal/wake, no owner/capacity publication, and no numeric-tolerance branch. |
| 4 | `unsupported_ahead_or_nonwitness_observer_advance_refuses_loudly` | Two arms: durable current above validated source maximum returns `AheadOfValidatedSourceMaximum`; durable current within the range but unequal to every source progress returns `AdvanceWithoutExactSourceWitness`. Include nonzero current with an empty source pass. All fail before append or classification. |
| 5 | `leave_source_flush_precedes_observer_advance_flush` | Gate Left append/flush and Advance append/flush independently through the real handler. Before Left flush there is no Advance/wake. After Left flush, Advance failure leaves the source durable, returns typed durability failure, and publishes no wake; success flushes Advance before publication. |
| 6 | `observer_recovery_first_touch_repairs_missing_advance_before_classification` | Cut after Left source flush and before Advance, discard owners, and make `apply_observer_recovery` the first touch. Its pre-pass replays, validates the exact source pass, appends one missing Advance, and only then classifies the handshake. A subsequent replay appends nothing. |
| 7 | `dead_or_missing_weak_observer_handle_drops_only_targeted_wake` | Cut after Advance flush before handoff and separately drop the weak inbox. Durable progress remains exact; only the targeted wake is absent. No broadcast, other-target removal, semantic-response displacement, timer, or retry occurs. |

The refusal tests attack the r1 gap directly. Passing only the live/cold happy
paths does not close W1a.

## 11. Honesty, costs, non-goals, and deferrals

### 11.1 Cost

W1a adds no idle task, thread, timer, poll, sweep, cursor, queue, store traversal,
or wake source. Across `N` idle conversations, added application wake/read cost
is `0 × N = 0`.

Each existing replayed projection gains bounded source/occurrence metadata and
constant-time adjacent-order/monotonic checks. The complete witness vector is
already bounded by the replayed projection count that the current vector
retains; W1a enriches each element rather than adding a second vector. Exact
membership of durable current can be decided during one linear scan of the
validated pass. No quadratic source lookup is allowed. Checked ordinals and
head increments refuse overflow.

A missing Advance retains the existing append/flush and targeted wake cost.
Test injection/instrumentation is cfg(test)-only.

### 11.2 Non-goals

- no Died/Ordinary/Recovered `StoredOperation` or live source path in W1a;
- no fate reconstruction from the two-field projection;
- no W1b schema/version/migration decision smuggled into this fold;
- no new W1a persisted row or observer schema change;
- no protocol wire refusal invented for durable-state corruption;
- no second lifecycle owner or promoted crash fixture;
- no haematite branch/WAL crash-gate surface;
- no generic cursor/conversation/sweeper wiring;
- no polling, compatibility shim, silent fallback, max-clamp, or “close enough”
  tolerance; and
- no new public API beyond internal wiring/error needs.

### 11.3 Deferred with owner and trigger

Only W1b's fate-source design is deferred from the original W1 premise. Its
owner, consumer, trigger, decision tables, and oracle minimum are complete in
§8 and pinned to the r1.6 row. Generic durability consumers and dedup scheduling
remain outside this lane and acquire no implied W1 road.

Nothing inside W1a is deferred: plain-Leave method removal, surviving Leave
metadata, complete-pass validation, exact historical-prefix witness, typed
refusals, live/cold ordering, ObserverRecovery-first proof, weak-target rule,
and all seven W1a oracles land together.

## 12. Walls

- **WALL-W1A-SCOPE:** W1a contains only canonical Leave plus reconcile
  conformance over currently wired sources.
- **WALL-W1B-PHASE:** Died/Ordinary/Recovered sources, schema, migration,
  emission census, occurrence routing, finalizers, and cold reconstruction stay
  in the ledgered design-first W1b lane.
- **WALL-SUPERSESSION:** W1a retains the r1.5 no-new-row rule; W1b expressly does
  not.
- **WALL-LIVE-LEAVE-SURVIVES:** `LiveLeaveCommit` is the only Leave projection
  producer; the plain method is structurally absent.
- **WALL-SINGLE-PRESENTATION:** occurrence/source/producer validation rejects a
  second Leave producer before all numeric tolerance.
- **WALL-COMPLETE-PREFLIGHT:** the complete replay pass validates source order,
  occurrence uniqueness, and nondecreasing progress before observer mutation.
- **WALL-EXACT-WITNESS:** durable progress is the zero Track baseline or equals
  a validated source progress; arbitrary in-range and ahead values refuse.
- **WALL-NO-MAX-HIDING:** accumulation never replaces validation with a maximum.
- **WALL-SOURCE-BEFORE-ADVANCE:** Left append/flush precedes Advance; Advance
  flush precedes fired publication.
- **WALL-OBSERVER-FIRST-TOUCH:** the cold oracle enters through
  `apply_observer_recovery` before any other touch or handshake classification.
- **WALL-PENDING-CORRECTION:** pending finalization returns
  `CommittedBindingTerminal`; W1a's pending route presents one Left projection,
  never a fabricated later Died transition.
- **WALL-TYPED-CONFORMANCE:** violations use the closed conformance error sum,
  never string-only acceptance, fabricated wire value, or a “close enough”
  comparison.
- **WALL-DROPPED-WAKE:** a dead/missing weak handle drops only its targeted wake
  after durable Advance.
- **WALL-AGGREGATE-OWNER:** witness and replay state remain behind
  `ConversationAuthority`; no second lifecycle repository.
- **WALL-LIMINAL-DURABILITY:** only `DurableStore`/observer-log seams; no direct
  haematite crash-gate API.
- **WALL-CHECKED-ARITHMETIC:** replay ordinals, source positions, heads, counts,
  and conversions are checked.
- **WALL-NO-POLLING:** no timer, cursor loop, scan, sweep, heartbeat, backoff,
  read timeout, stop-flag sampling, or synthetic probe discovers change.
- **WALL-DOCS-ONLY-R2:** this fold changes only this brief.

## 13. Pre-review findings disposition

| finding | severity | r2 disposition |
|---:|---|---|
| 1 | MAJOR | Folded the coordinator's PHASE ruling: retitled/rescoped to W1a, removed three absent fate-source wiring points/oracles from W1a, and created the pinned W1b owner/trigger/decision-table/oracle deferral. |
| 2 | MAJOR | Corrected the false Recovered-Died claim: Ordinary consumes `CommittedDiedTerminal`; Recovered consumes `BindingFateObserved` only. Assigned `(conversation_id, participant_id, binding_epoch)` canonical occurrence routing to W1b. |
| 3 | MAJOR | Corrected pending finalization: `finish_died` is one-shot, later commit returns `CommittedBindingTerminal`, and the wired pending route is Leave → `LiveLeaveCommit` → Left projection. Added W1b's mandatory finalizer-routing table. |
| 4 | MAJOR | Designed complete-pass monotonic/source/occurrence validation and the exact historical-prefix witness rule before `current >= presented`; added typed decreasing, ahead, nonwitness, and final-mismatch refusals and recorded disclosure-class severity. |
| 5 | MAJOR | Rebuilt the W1a census: ObserverRecovery is first cold touch; Leave duplicate proof combines structural absence with real-projection cfg(test) injection before tolerance; surviving Leave per-arm covers settled and pending; final census is seven exact names. |

All five findings are dispositioned. No r1 wiring claim for an absent fate
source survives in W1a.

## 14. Revision record

| revision | date | record |
|---|---|---|
| r1 | 2026-07-19 | Initial W1 brief against ledger r1.5. It correctly narrowed the consumer and selected the Leave survivor, but wrongly treated three absent production fate sources as one implementable lane, misstated Recovered provenance and pending-Died finalization, and lacked executable reconcile-conformance validation. |
| r2 | 2026-07-19 | Five-MAJOR pre-review fold under the r1.6 coordinator ruling at `8d9ee23`: retitled/rescoped to W1a; retained canonical LiveLeaveCommit and removed the plain arm; designed complete replay-pass witnesses and exact historical-prefix validation for the disclosure-class reconcile gap; corrected Recovered and pending-finalizer semantics; made ObserverRecovery the first cold touch; rebuilt the seven-name W1a census; and deferred Died/Ordinary/Recovered source schema, migration, emission, occurrence routing, finalizers, cold reconstruction, and three required per-arm names to ledgered W1b. |
