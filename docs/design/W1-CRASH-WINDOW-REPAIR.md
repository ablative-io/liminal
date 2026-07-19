# W1a — Leave-survivor wiring and observer reconcile-conformance repair

Brief base: `docs/w1-crash-window-repair@f7dff3d`. Source-code anchors remain
at `de4f987`; `origin/main@2a41a60` carries the normative r1.7 ledger, including
the r1.6 phase ruling and r1.7 per-lineage wording amendment, with no source
changes used by this brief.

Revision: r3, 2026-07-19. Normative ledger:
`2a41a60:docs/design/WIRING-LEDGER.md`, r1.7. The Unit 2 source of record is
`docs/design/F0C-UNIT2-SERVERPUSH-PRODUCER.md`, SHA-256
`98f9130faa175f323206eb6640e9f625ab6385dc285e11f5b2774fb306b5de6a`.
Every repository anchor in this revision was reopened against the cited bytes.

Path decision: keep `docs/design/W1-CRASH-WINDOW-REPAIR.md`. The r1.7 ledger
retains the retitled/phased lane but names no replacement document path. Retitling
the heading to W1a while preserving the path keeps the r1 review and r2/r3
supersessions in one auditable history.

## Goal

Close only wireable lane W1a:

1. make `LiveLeaveCommit` the sole permanent-Leave observer-progress producer,
   remove the dormant plain `LeaveCommit` projection arm, and prove one
   presentation per Leave occurrence; and
2. repair the existing §8 reconciliation path so the authority retains the
   maximum separately from its provenance-bearing source pass, accepts
   historical-prefix idempotency only from an exact source occurrence that
   established a running maximum, and refuses source-order, per-lineage,
   disagreement, and unsupported-ahead corruption before observer mutation.

W1a does not invent the absent Died/Ordinary/Recovered production sources.
Their `StoredOperation` variants, flush barriers, replay transitions, canonical
occurrence routing, and migration contract are the separate design-first W1b
lane queued behind W1a.

## 1. r1.7 ledger, phase ruling, scope, and severity

### 1.1 Normative phase

The r1.7 ledger retains r1.6's ruling that production has no Died, Ordinary, or
Recovered `StoredOperation` variant, no BindingFate replay branch, and no
durable home from which cold replay could reconstruct those projections. It
expressly phases W1 into W1a and W1b
(`2a41a60:docs/design/WIRING-LEDGER.md:29-40`). The byte evidence is exact:
`StoredOperation` has Genesis, Enrolled, Attached, Detached, ZeroDebtAck,
MarkerDrained, RecordAdmission, and Left only
(`crates/liminal-server/src/server/participant/production/log.rs:150-222`),
and the exhaustive production replay match has no Died or BindingFate branch
(`crates/liminal-server/src/server/participant/production/ops_session.rs:441-517`).

The W1a scope is exactly the ledger's two-part ruling:

- the canonical Leave survivor and its single-presentation proof; and
- reconcile-conformance validation on the existing §8 path.

The ledger calls Leave the only wireable original arm and assigns both pieces
to W1a (`2a41a60:docs/design/WIRING-LEDGER.md:42-61`). W1a validation applies
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
loudly. R3 interprets nonmonotonicity at the source-specific lineage, never
across unrelated participant cursors.

The coordination seat has recorded severity and assigned the repair to W1a
because it owns the same reconcile path
(`2a41a60:docs/design/WIRING-LEDGER.md:45-51`). The posture is disclosure with
teeth: no separate escalation blocks this r3 paper fold, but W1a cannot close
until the production repair and its refusal oracles land. The existing behavior
is not relabeled safe, latent, or “close enough.”

### 1.3 r2/r3 supersession statement

R2 supersedes these r1 claims:

- that all four fate projections were implementable in one lane;
- that Ordinary and Recovered were both later interpretations of an input Died
  terminal;
- that a pending Died later finalized by producing a new
  `DiedBindingTransition`; and
- that `current >= presented` could remain the general test for historical
  prefix acceptance.

R3 additionally supersedes r2's claims that the complete projection sequence is
globally nondecreasing, that its last value is authoritative truth, and that
calling `ensure_observer_tracked` before reconcile satisfies a no-mutation
preflight. Independent participant cursor lineages may legally project high
then lower values. The authoritative truth is the separately retained maximum,
and an absent observer Track is planned as `Untracked`, not appended before
conformance succeeds. R1.7 ratifies the coordinator wording amendment. Its
oracle-floor rule is, verbatim: “PER-LINEAGE regression and unsupported
ahead-Advance arms (r1.7: a globally-decreasing sequence can be legal
multi-participant history — per-participant cursors, no global floor,
`ops_acks.rs:162-207`; the W1a validation model is per-lineage monotonicity +
running-maxima witness, final progress = max over the witness set)”
(`2a41a60:docs/design/WIRING-LEDGER.md:52-60`). The brief uses that exact model.

The corrected contracts are in §§4, 5, 6, 7, and 8.

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
makes “exact” executable, and r3 corrects its granularity: durable progress must
be the Track baseline or equal the exact source occurrence that established a
running maximum in this complete replay pass. Section 5 defines that rule.

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
three absent fate sources. The r1.7 ledger retains r1.6's express amendment, and
the phase record says it stands for W1a but is superseded for W1b
(`2a41a60:docs/design/WIRING-LEDGER.md:3-6,29-40`). W1b must design durable
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

The structural half is a concrete compile-time check callable from the named
acceptance test: its body runs a `trybuild` compile-fail fixture that attempts to
call `LeaveCommit::observer_progress_projection`; the checked diagnostic proves
that method is absent. The dynamic half uses a cfg(test)-only injection seam: it
obtains the projection from a real `LiveLeaveCommit`, presents the same explicit
occurrence/source a second time with a distinct injected producer label, and
requires rejection before any observer-current comparison. Production carries
no injection API or counter.

## 5. Reconcile-conformance validation

### 5.1 Current defect and the legal heterogeneous counterexample

Current state already carries two different facts: a vector of surrendered
projections and a separately updated conversation maximum
(`crates/liminal-server/src/server/participant/production/state.rs:159-164,267-280`).
Current reconcile discards the vector's provenance, examines each bare value,
and treats every `current >= presented` as satisfied
(`crates/liminal-server/src/server/participant/production/handler_observer.rs:257-290`).
That permits an unsupported durable value ahead of all sources or between legal
running maxima.

R2's attempted cure—requiring the entire vector to be nondecreasing—was also
wrong. A committed ParticipantAck projects its request-local `through_seq`
(`crates/liminal-protocol/src/lifecycle/operations/participant_ack.rs:24-36`).
Admission compares that request only with the addressed participant's cursor
(`crates/liminal-protocol/src/lifecycle/operations/participant_ack.rs:183-213`),
and frontier application likewise finds and advances only that participant
(`crates/liminal-protocol/src/lifecycle/claim_frontier.rs:2516-2536`). Production
records every committed ack projection without a conversation-global floor
filter
(`crates/liminal-server/src/server/participant/production/ops_acks.rs:162-207`).
Therefore participant A acking through 100 and participant B later acking
through 50 legally yields source-order vector `[100, 50]`.

The separately retained maximum remains authoritative conversation progress.
It feeds ordinary-record projection
(`crates/liminal-server/src/server/participant/production/ops_frontier.rs:80-104`)
and detach lookup
(`crates/liminal-server/src/server/participant/production/ops_session.rs:40-69`).
R3 must enrich the vector for validation without replacing this maximum with
the pass's final element.

### 5.2 Provenance-bearing witness and one merged source order

Replace each bare vector element with an internal move-only
`ObserverProgressSourceWitness`. The exact Rust spelling is an implementation
detail, but these owned facts are normative:

1. sealed projection;
2. a transient checked merged replay ordinal assigned at the moment the source
   is visited in the conversation's actual base-plus-extension replay;
3. durable source identity and kind:
   - base source: operation-log sequence plus `StoredOperation` kind;
   - extension source: `{ base_log_head, extension_sequence }` plus extension
     kind;
4. source-specific lineage identity—for acknowledgment sources this includes
   the permanent participant id; and
5. occurrence identity and canonical producer label when more than one producer
   is structurally possible (Leave in W1a).

Base and extension positions are not compared as if their raw coordinate types
formed one numeric domain. The merge assigns one ordinal to every visited
source in **actual merged replay order**, using `checked_add`; witnesses compare
that ordinal, while retaining the raw base/extension coordinates as durable
identity. Thus a base row followed by its boundary extension rows and the next
base row has one explicit total order with overflow refusal. The replay loop
applies boundary 0, each base row, then that row's extension boundary before the
next base row
(`crates/liminal-server/src/server/participant/production/ops_session.rs:272-350`),
and MarkerAck rows already persist both extension ordering components
(`crates/liminal-server/src/server/participant/production/ops_acks.rs:328-344,365-452`).

Every current producer must supply a witness. Existing examples include attach
(`crates/liminal-server/src/server/participant/production/ops_attach.rs:240-265,315-319`),
detach
(`crates/liminal-server/src/server/participant/production/ops_session.rs:201-266`),
ack and MarkerAck
(`crates/liminal-server/src/server/participant/production/ops_acks.rs:162-207,299-358,365-452`),
and Leave
(`crates/liminal-server/src/server/participant/production/ops_leave.rs:187-217,258-303`).

### 5.3 Complete source validation is per lineage, not global

During complete replay, the fallible record path validates:

- projection conversation equals replay conversation;
- merged ordinals are strictly increasing and each raw source identity/kind is
  the one owned by the typed row at that ordinal;
- one occurrence has one canonical producer in the pass;
- each source satisfies its own typed transition and lineage monotonicity; and
- all occurrence, producer, and lineage fields agree with that typed source.

There is deliberately no `pi >= p(i-1)` check across heterogeneous lineages.
ParticipantAck and MarkerAck share the addressed participant-cursor lineage;
their own replayed transition may not regress that participant's cursor, but a
later source for a different participant may project any lower legal cursor.
Attach, detach, and Leave use the monotonic relation declared by their own typed
source/occurrence contract rather than borrowing the acknowledgment relation.
A same-lineage regression is corruption; `[A:100, B:50]` is not.

For every accepted witness, the authority updates its separate progress as
`authoritative_max = max(authoritative_max, pi)` and retains the witness in
source order. The maximum remains usable by later replayed record and detach
transitions, while the vector preserves proof. Equal or lower heterogeneous
sources are never filtered from the vector merely because they do not raise the
maximum.

After the **complete** participant/outbox merge and semantic replay succeeds,
`replay_and_repair` drains exactly one validated pass. Current control flow
already completes authority replay before draining projections
(`crates/liminal-server/src/server/participant/production/handler.rs:251-275`).
Only then may any observer owner be inspected or restored.

### 5.4 Exact running-maximum witness rule

Let `P = [p0, ..., pn]` be that validated source-order vector. Define
`M(-1) = 0`, `Mi = max(M(i-1), pi)`, and `max(empty) = 0`. Let `A` be durable
observer progress only when an observer Track exists.

**Corrected witness rule (normative, verbatim):** Let `P = [p0, ..., pn]` be the complete source-ordered vector after source-identity, source-order, occurrence, and source-specific/per-lineage validation, and let `Mi = max(p0, ..., pi)`. Keep `max(P)` separately as authoritative conversation progress. A nonzero durable observer progress `A` is supported only by an exact source occurrence `i` for which `pi == A` and `pi > M(i-1)` (with `M(-1) = 0`), so that occurrence established the running maximum. Select the greatest source-order prefix whose running maximum is `A`; validated lower or equal projections after the establishing occurrence remain in that prefix, and only later strict running maxima form the reconciliation suffix. Append `Advance` rows for those suffix maxima in source order. On success final durable observer progress MUST equal `max(P)` exactly. `A == 0` is the Track baseline; any ahead, in-range non-establishing, or source-less nonzero `A` refuses before observer mutation.

The operational consequences are exact:

1. `[100, 50]` supports durable `A = 100`: the first occurrence established
   `M0 = 100`, and the greatest prefix with running maximum 100 includes the
   later validated 50. It does not support `A = 50`, because that occurrence
   established no running maximum.
2. `A > max(P)` is `AheadOfValidatedSourceMaximum` and refuses.
3. `0 < A <= max(P)` without an exact maximum-establishing occurrence is
   `AdvanceWithoutRunningMaximumWitness` and refuses, even if `A` equals a lower
   source or lies numerically between two maxima.
4. Starting immediately after the greatest prefix with running maximum `A`,
   append one Advance for each later strict increase in `Mi`. Lower/equal
   sources remain validated history but require no row.
5. After all planned appends commit, durable observer progress and the
   authority's separate progress MUST both equal `max(P)` exactly.

This is the no-new-observer-schema equivalent of durable source identity in an
Advance row. Durable identity remains in participant/extension sources; the
transient witness proves which exact occurrence established each accepted
maximum and which complete source prefix makes the durable number legal.

### 5.5 Honest live-post-flush versus authoritative-replay split

The two validation sites have intentionally different authority:

| site | checks and effects | checks it MUST NOT claim |
|---|---|---|
| live post-flush | After the exact participant source append/flush, verify conversation, source kind/identity, occurrence, producer, and the just-committed typed transition's local lineage facts; assign its checked live visit position; retain the witness; update the authority maximum with `max`. | It does not possess a reconstructed complete history, does not compare or bless durable observer `A`, does not classify a historical prefix, and does not append an observer repair from this lightweight check alone. |
| complete replay pass | Reconstruct every base and extension source; assign checked merged ordinals in actual visit order; validate all source identities, occurrence uniqueness, canonical producers, and source-specific/per-lineage transitions; retain all witnesses and compute every `Mi` plus `max(P)`. | It does not use global projection monotonicity, the last projection as truth, or a pre-existing numeric `current >= presented` shortcut. |

The live handler still withholds its correlated response after the source barrier
and drives `replay_and_repair`, so only the authoritative complete pass may plan
observer mutation. The lightweight check catches immediate producer mistakes;
it is not a substitute for cold reconstruction.

### 5.6 Observer preflight states and one serialized plan

Current `replay_and_repair` drains projections, calls
`ensure_observer_tracked`, which may append `Track { 0 }`, and only then calls
reconcile
(`crates/liminal-server/src/server/participant/production/handler.rs:251-282`;
`crates/liminal-server/src/server/participant/production/handler_observer.rs:155-218`).
That violates the claimed no-mutation preflight and can leave Track residue for
a source pass that should refuse.

R3 replaces that ordering with one fused planner/executor. Only after §5.3
complete source validation succeeds does it lock the observer owner and, if
needed, restore the observer log **without mutation**. Inspection produces the
closed planning state:

- `Untracked` — no durable Track row for this conversation; or
- `Tracked(A)` — an existing Track and its exact durable progress `A`.

`Untracked` supplies virtual baseline 0 for planning only; it is not silently
inserted into the aggregate. While retaining the same observer lock and owner,
the planner validates the §5.4 witness rule and constructs the complete Track
plus running-maximum Advance sequence without appending anything. The result is
one of:

- **Refuse:** return the typed conformance failure and append **nothing**—no
  Track, Advance, transaction commit, arm removal, wake, owner installation,
  capacity fold, or handshake classification.
- **Commit from Untracked:** append/flush `Track { observer_progress: 0 }`,
  commit its track transaction and checked head increment, then append/flush
  and commit only the planned strict-running-maximum Advance suffix in source
  order.
- **Commit from Tracked(A):** append no Track; append/flush and commit only the
  planned suffix after the greatest prefix supported by `A`.

Track and every Advance execute under that same serialized observer owner.
Track must precede the first Advance, and the final Advance must precede fired
publication and ObserverRecovery classification. The standalone
`ensure_observer_tracked` repair call is not allowed before or beside this
plan: replay, the owner-present `ensure_tracking_from_log` branch, and the
post-enrollment response path must route absent-Track repair through the fused
complete-pass plan or prove that the plan already committed before returning.
Current ObserverRecovery first touch reaches those paths through
`ensure_tracking_from_log`
(`crates/liminal-server/src/server/participant/production/handler_observer.rs:339-375`).

The zero-source cases are explicit:

- enrolled `Untracked` plus `P = empty` succeeds with Track 0 only and final 0;
- enrolled `Tracked(0)` plus `P = empty` succeeds with no writes;
- enrolled `Tracked(A > 0)` plus `P = empty` refuses
  `AdvanceWithoutRunningMaximumWitness` with no writes; and
- an unenrolled conversation is not registered and cannot use the virtual
  baseline to create Track residue.

“Refuse appends nothing” applies to all source/preflight conformance failures.
After a successful plan begins executing, a durability failure may leave only
the exact flushed prefix—possibly Track 0 or earlier planned Advances. No
rollback fiction is introduced; the next complete replay validates and resumes
that prefix. Observer row heads use checked arithmetic throughout.

### 5.7 Typed refusal boundary

Add a closed internal `ObserverProgressConformanceError` and carry it through a
dedicated `StateError` variant rather than a formatted invariant string. Its
minimum variants are:

- `ConversationMismatch`;
- `SourceOrder`;
- `SourceIdentityMismatch`;
- `DuplicateOccurrenceProducer`;
- `SourceLineageRegression`;
- `AheadOfValidatedSourceMaximum`;
- `AdvanceWithoutRunningMaximumWitness`; and
- `FinalProgressMismatch`.

The production boundary reports the typed conformance failure and publishes no
fabricated protocol value. “Refusal” means replay/planning refuses durable state
before classification; it is never silent acceptance and never a “close
enough” global comparison. It does not manufacture an `InvalidObserverEpoch`,
which is a request-specific protocol outcome
(`crates/liminal-protocol/src/wire/authority/records.rs:167-209`).

Once a valid plan begins, existing per-row append/flush, checked head increment,
protocol transaction commit, and fired publication order remains
(`crates/liminal-server/src/server/participant/production/handler_observer.rs:291-336`).
On complete success, an explicit final equality check proves both observer and
authority progress equal `max(P)`; lower final element `pn` never lowers either.

## 6. Live, cold-first-touch, and weak-target behavior

### 6.1 Live Leave ordering

The live Leave path extracts the surviving projection while owning
`LiveLeaveCommit`, appends/flushes `StoredOperation::Left`, performs the
lightweight source-local witness check, and advances the source head
(`crates/liminal-server/src/server/participant/production/ops_leave.rs:187-217,407-425`).
The handler detects the crossed source barrier and runs the authoritative
complete `replay_and_repair` before the correlated terminal response can escape
(`crates/liminal-server/src/server/participant/production/handler.rs:168-241`).
Source failure yields no Track, Advance, or wake. Observer append failure leaves
only the exact committed source/observer prefix for cold repair and yields no
wake.

### 6.2 Startup and ObserverRecovery-first cold repair

Startup replays registered conversations before installing owners
(`crates/liminal-server/src/server/participant/production/handler.rs:90-146`).
Ordinary absent-owner first touch also replays under the conversation lock
(`crates/liminal-server/src/server/participant/production/handler.rs:184-223`).

The load-bearing W1a cold oracle uses neither route first. After retaining the
participant source but deleting/omitting its observer registration and Advance,
its **first touch is `apply_observer_recovery`**. That method runs
`ensure_tracking_from_log` for every named conversation before it locks/restores
and classifies the observer batch
(`crates/liminal-server/src/server/participant/production/handler_observer.rs:33-75`).
An absent owner reaches `replay_and_repair` before classification
(`crates/liminal-server/src/server/participant/production/handler_observer.rs:345-365`).
The oracle must show complete source/per-lineage validation first, then a
`Track { 0 }` flush, then the required running-maximum Advance flush, and only
then handshake classification. A refusal at source or preflight validation
must show neither Track nor Advance was appended.

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
(`2a41a60:docs/design/WIRING-LEDGER.md:63-73`). It is queued behind W1a.

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
ledger row requires (`2a41a60:docs/design/WIRING-LEDGER.md:63-73`).

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

## 10. W1a acceptance oracles — complete eight-name census

Each exact final name appears once. Tests use deterministic source/Track/Advance
barriers, explicit owner state, and weak-target controls; no sleeps or polling.

| item | exact oracle | required proof |
|---:|---|---|
| 1 | `live_leave_commit_projects_left_sequence_for_settled_and_pending_paths` | Independent settled and pending fixtures exercise the surviving `LiveLeaveCommit`. Each asserts conversation and exact committed Left sequence. The pending fixture distinguishes terminal sequence from Left sequence and proves only Left is presented. |
| 2 | `leave_projection_has_one_surviving_producer_and_duplicate_injection_refuses` | The test body invokes a `trybuild` compile-fail fixture that attempts the removed plain `LeaveCommit::observer_progress_projection` call. A cfg(test)-only seam then obtains a real surviving projection, supplies explicit source/Leave-occurrence/producer metadata, injects a second producer for the same occurrence, and receives `DuplicateOccurrenceProducer` before observer restore or numeric tolerance. |
| 3 | `same_participant_ack_lineage_regression_refuses_before_observer_mutation` | Supply two source-ordered real ParticipantAck projections with one permanent participant lineage and a lower second cursor through the cfg(test) conformance seam. Assert exact `SourceLineageRegression`. Start with no Track and assert the observer log receives neither Track nor Advance, with no arm removal, wake, owner/capacity publication, or classification. |
| 4 | `two_participant_high_then_lower_ack_replay_preserves_observer_maximum` | Commit and replay two real participants in source order, A through 100 then B through 50. Assert witness values `[100, 50]` are accepted, per-participant cursors remain exact, the authority maximum supplied to downstream record/detach state is 100, the durable observer finishes at 100, and replay appends no lowering row. |
| 5 | `unsupported_ahead_or_nonwitness_observer_advance_refuses_loudly` | Four arms: `A > max(P)` returns `AheadOfValidatedSourceMaximum`; `[100, 50]` with `A = 50` returns `AdvanceWithoutRunningMaximumWitness`; an in-range value equal to no source does likewise; and nonzero A with an empty pass does likewise. Every arm fails before append or classification. |
| 6 | `leave_source_flush_precedes_observer_advance_flush` | Gate Left append/flush and Advance append/flush independently through the real handler. Before Left flush there is no Track, Advance, or wake. After Left flush, observer append failure leaves only the exact durable prefix, returns typed durability failure, and publishes no wake; success flushes observer progress before publication. |
| 7 | `observer_recovery_first_touch_repairs_missing_advance_before_classification` | Retain the Left source with no observer Track or Advance, discard owners, and make `apply_observer_recovery` the first touch. Its pre-pass validates the complete source pass before writes, appends/flushes Track 0 before the required Advance, and completes both before classifying the handshake. Assert row order `Track < Advance < classification`; a subsequent replay appends nothing. |
| 8 | `dead_or_missing_weak_observer_handle_drops_only_targeted_wake` | Cut after Advance flush before handoff and separately drop the weak inbox. Durable progress remains exact; only the targeted wake is absent. No broadcast, other-target removal, semantic-response displacement, timer, or retry occurs. |

Rename map (old → new):

- decreasing_source_progress_refuses_before_numeric_tolerance →
  same_participant_ack_lineage_regression_refuses_before_observer_mutation.

The positive two-participant oracle is new; the other six names are unchanged.
The refusal tests attack the r1 gap directly. Passing only live/cold happy paths
does not close W1a.

## 11. Honesty, costs, non-goals, and deferrals

### 11.1 Cost

W1a adds no idle task, thread, timer, poll, sweep, cursor, queue, store traversal,
or wake source. Across `N` idle conversations, added application wake/read cost
is `0 × N = 0`.

Each existing replayed projection gains bounded per-element
source/occurrence/lineage metadata. One linear pass performs source-order,
uniqueness, per-lineage, running-maximum, exact-witness, and suffix planning;
lineage state uses keyed linear-space bookkeeping. No quadratic source lookup is
allowed. Checked ordinals and observer heads refuse overflow.

The witness vector itself is **history-linear in progress-producing sources**:
for `H_p` such sources, replay retains `Theta(H_p)` witnesses until reconcile.
That vector is pre-existing replay state—the current authority already owns
`Vec<ObserverProgressProjection>` plus its separate maximum
(`crates/liminal-server/src/server/participant/production/state.rs:159-164,267-280`).
R3 enriches those elements; it does not add a second, W3-style duplicate
materialization. This is the round-2 adjudication recorded in §13 finding 1,
and bounding it belongs to the ledgered W7 history-bounding lane, whose trigger
covers authority restore history-linear indexes
(`2a41a60:docs/design/WIRING-LEDGER.md:151-181`). W1a neither claims unbounded
history safety nor expands W3's narrowed duplicate-materialization scope.

A missing Track/Advance retains existing per-row append/flush and targeted wake
cost. Test injection/instrumentation and the compile-fail fixture are test-only.

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
§8 and pinned to the r1.7 row. Generic durability consumers and dedup scheduling
remain outside this lane and acquire no implied W1 road.

Nothing inside W1a is deferred: plain-Leave method removal, surviving Leave
metadata, complete per-lineage source validation, merged-order witnesses,
running-maximum exact-prefix planning, `Untracked | Tracked(A)` preflight,
typed refusals, live/cold ordering, ObserverRecovery-first proof, weak-target
rule, and all eight W1a oracles land together.

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
  occurrence uniqueness, and source-specific/per-lineage monotonicity before
  observer inspection; heterogeneous projections need not be nondecreasing.
- **WALL-EXACT-WITNESS:** nonzero durable progress equals the exact source
  occurrence that established its running maximum; lower/equal later sources
  are legal, while non-establishing, arbitrary in-range, and ahead values
  refuse.
- **WALL-MAX-AND-WITNESS:** authoritative conversation progress is `max(P)` and
  remains separate from the complete provenance-bearing source vector; neither
  may replace the other.
- **WALL-OBSERVER-PREFLIGHT:** `Untracked | Tracked(A)` is inspected only after
  complete source validation; a refused plan appends neither Track nor Advance.
- **WALL-SOURCE-BEFORE-ADVANCE:** Left append/flush precedes Track/Advance;
  Track, when absent, precedes Advance; Advance flush precedes publication.
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
- **WALL-HISTORY-HONESTY:** the witness vector is pre-existing history-linear
  replay state; W7 owns bounding and W1a makes no unbounded-history claim.
- **WALL-NO-POLLING:** no timer, cursor loop, scan, sweep, heartbeat, backoff,
  read timeout, stop-flag sampling, or synthetic probe discovers change.
- **WALL-DOCS-ONLY-R3:** this fold changes only this brief.

## 13. Findings disposition

### 13.1 Historical r2 pre-review

| finding | severity | r2 disposition and r3 status |
|---:|---|---|
| 1 | MAJOR | Folded the coordinator's PHASE ruling: retitled/rescoped to W1a, removed three absent fate-source wiring points/oracles from W1a, and created the pinned W1b owner/trigger/decision-table/oracle deferral. Retained. |
| 2 | MAJOR | Corrected the false Recovered-Died claim: Ordinary consumes `CommittedDiedTerminal`; Recovered consumes `BindingFateObserved` only. Assigned `(conversation_id, participant_id, binding_epoch)` canonical occurrence routing to W1b. Retained. |
| 3 | MAJOR | Corrected pending finalization: `finish_died` is one-shot, later commit returns `CommittedBindingTerminal`, and the wired pending route is Leave → `LiveLeaveCommit` → Left projection. Added W1b's mandatory finalizer-routing table. Retained. |
| 4 | MAJOR | Added complete source/occurrence validation and typed refusals, but its global-monotone/last-value prefix model was wrong. Superseded by §§5.1–5.5. |
| 5 | MAJOR | Made ObserverRecovery first cold touch and rebuilt the W1a floor. Its seven-name count and structural-check detail are superseded by §10's eight-name census and callable compile-fail mechanism. |

No r1 wiring claim for an absent fate source survives in W1a.

### 13.2 Round-2 re-review — exactly two new findings

| finding | severity | r3 disposition |
|---:|---|---|
| 1 | MAJOR | **Dispositioned.** Replaced global monotonicity and `pn` truth with source-specific/per-lineage validation, a checked transient ordinal over actual base/extension merge order, a separate authoritative `max(P)`, and exact maximum-establishing occurrence support. Added legal real `[A:100, B:50]` acceptance/final-max proof, renamed the regression oracle to one participant lineage, and now matches the r1.7 coordinator-floor wording ratified at `2a41a60:docs/design/WIRING-LEDGER.md:52-60`. Also records the residual adjudication: the witness vector is pre-existing, history-linear replay state rather than new W3-style duplicate materialization; W7 owns its bound (§11.1). |
| 2 | MAJOR | **Dispositioned.** Replaced pre-reconcile tracking mutation with explicit `Untracked | Tracked(A)` planning after complete source validation and read-only observer restore. A refused plan appends no Track or Advance; success appends Track 0 before planned running-maximum Advances under one serialized owner. Defined every empty-source case, extended a no-Track refusal arm, and made first-touch success prove `Track < Advance < classification`. |

Both and exactly both round-2 findings are dispositioned. The residual checks are
also closed: §5.2 defines cross-stream source order, §4.2/§10 name the callable
`trybuild` structural-absence mechanism, and §11.1 cites this adjudication for
the vector's history-linear ownership.

## 14. Revision record

| revision | date | record |
|---|---|---|
| r1 | 2026-07-19 | Initial W1 brief against ledger r1.5. It correctly narrowed the consumer and selected the Leave survivor, but wrongly treated three absent production fate sources as one implementable lane, misstated Recovered provenance and pending-Died finalization, and lacked executable reconcile-conformance validation. |
| r2 | 2026-07-19 | Five-MAJOR pre-review fold under the r1.6 coordinator ruling now carried by r1.7 at `2a41a60`: retitled/rescoped to W1a; retained canonical LiveLeaveCommit and removed the plain arm; designed complete replay-pass witnesses and an attempted exact historical-prefix validation for the disclosure-class reconcile gap; corrected Recovered and pending-finalizer semantics; made ObserverRecovery the first cold touch; rebuilt the seven-name W1a census; and deferred Died/Ordinary/Recovered source schema, migration, emission, occurrence routing, finalizers, cold reconstruction, and three required per-arm names to ledgered W1b. |
| r3 | 2026-07-19 | Two-MAJOR round-2 re-review fold under ratified ledger r1.7 at `2a41a60`: replaced the invalid global-monotone/last-value model with per-lineage source validation, checked merged replay order, separate authoritative maxima, running-maximum-establishing witnesses, lower/equal heterogeneous tolerance, and exact `max(P)` completion; split lightweight live checks from authoritative replay; fused absent-Track handling into `Untracked | Tracked(A)` no-mutation preflight and ordered Track-before-Advance execution; made structural absence executable; disclosed the pre-existing history-linear vector under W7; and rebuilt the eight-name W1a census while preserving W1b's three required names. |
