# PENDING-DRAIN-EMITTER — candidate-lane terminal drain + production ProcessKilled emitter

Revision: r1 (proposed-pending-tear — every decision in this document, without
exception, is `proposed-pending-tear`; nothing here is authorized for build
until the tear seat rules on it)

Base: main `14032ca` (liminal-server 0.3.3 + liminal-sdk 0.3.3). All liminal
file:line pins below were re-verified at that commit. Branch pins name their
branch and commit explicitly. Beamr pins were re-verified at the local beamr
checkout `fd71c5e` (crate `beamr` version 0.16.0, `crates/beamr/Cargo.toml:3`).

## 1. Scope

One design unifying two ruled problems, per the tear seat's collapse of two
lanes after a gate-order circularity was proven:

- **Problem 1 — the restore-window tear** (red-pinned, ruling confirmed): a
  crash-restored `PendingFinalization(Died)` residence makes every valid
  publish naming that participant tear the connection.
- **Problem 2 — the production ProcessKilled emitter** (§14 of
  `docs/design/W1B-FATE-SOURCES.md` = design of record; deferred-seam row at
  `docs/design/W1B-FATE-SOURCES.md:1132`; §14 heading at `:1128`): the
  beamr-to-participant exact exit-fate adapter.

## 2. Why one design — the relationship, precisely

The **drain** (Problem 1) closes the restore-window defect on its own: the
pending terminals it serves today are caused by `ConnectionLost` /
`UncleanServerRestart` (Died-source rows durable with
`StoredTerminalDisposition::Pending`; restore path
`crates/liminal-server/src/server/participant/production/connection_fate_replay.rs:73`
and `:85`).

The **emitter** (Problem 2) adds a **new entry source** into exactly that
pending-terminal set: actor-granular death in a live VM, where the socket never
drops. Its pending terminals are then served by the same drain.

That composition is why they are one design: the drain is the consumer-side
mechanism that makes any `PendingFinalization(Died)` entry source safe against
live traffic; the emitter is the first new producer that would be unsafe
without it. Built in the other order, the emitter would widen the very entry
set that today tears the connection (§5.1, row S-9).

## 3. Problem 1 — the restore-window tear

### 3.1 Mechanism (pinned trace, all at 14032ca)

During crash-restore, a participant slot resident `PendingFinalization(Died)`
(Died source row durable with disposition `Pending`, finalizer absent) causes:

1. Dispatch runs record admission: `select_record_admission` (the alias of
   `liminal_protocol`'s `apply_record_admission`,
   `crates/liminal-server/src/server/participant/production/ops_frontier.rs:14`,
   call at `:126`).
2. The decision is `RecordAdmissionDecision::DrainFirst` (`ops_frontier.rs:136`)
   because the earliest candidate is the victim's `BindingTerminal`.
3. The DrainFirst arm calls `persist_next_marker` (`ops_frontier.rs:142`,
   defined at `:167`), which is marker-only: the
   `ImmutableSequenceCandidate::BindingTerminal` arm is a hard invariant —
   `"DrainFirst selected a binding terminal instead of marker work"`
   (`ops_frontier.rs:482-484`).
4. The invariant surfaces as `ParticipantDispatch::Fatal`, which the connection
   layer maps to `FrameAction::Close`
   (`crates/liminal-server/src/server/connection/apply.rs:208-211`).
5. Durable truth replays the residence, so the tear repeats on every retry
   until a finalizer (Leave, fenced attach) happens by.

Red pin (branch `fix/restore-window-typed-refuse`, origin commit `3d66a89`):
`valid_publish_during_pending_finalization_residence_is_typed_refused`
(`crates/liminal-server/src/server/participant/production/tests_restore_window.rs:49`,
held `#[ignore]` at `:45`), asserting "must be refused, not committed" at
`:85`. The prestate census pin on the same branch:
`residence_frontier_census_sole_terminal_candidate_closure_clear` (`:121`) —
sole candidate is the victim's `BindingTerminal`, closure Clear, no observers.

### 3.2 Why TYPED-REFUSE is dead (ruled out, three walls, byte-verified)

1. **No truthful refusal body exists.** The R-D1 register's total simulation
   (register at `docs/design/PARTICIPANT-CONTRACT.md:5588`; the exhaustive
   "generic stages 2–13" selector at `:5633`) selects SUCCESS for this publish
   in the pinned prestate (census test above: sole candidate is the victim's
   terminal, closure Clear, no observers). Any constructible refusal fabricates
   bytes; in particular `ObserverBackpressure` would park the SDK forever on an
   `ObserverProgressed` that cannot arrive (no observers exist in the
   prestate; both wire values exist at
   `crates/liminal-protocol/src/wire/response.rs`).
2. **The contract prescribes drain, not refusal.** R-A2:
   "the lane drains candidates strictly by `admission_order` before caller
   **record admission**", with terminal-record append, retention transition,
   candidate deletion, and binding-slot release as one durable transaction and
   "No mailbox, absence inference, poll, sweep, or fabricated later cause"
   (`docs/design/PARTICIPANT-CONTRACT.md:909-914`; same prescription at `:756`
   — "or first drains those earlier candidates separately" — and in the Leave
   flow at `:2191-2193` — "Leave first drains every globally earlier candidate
   in a separate committed candidate transaction, strictly by the R-A2 tuple").
3. **The marker lane structurally rejects terminals.** `drain_next_marker`
   (`crates/liminal-protocol/src/lifecycle/operations/marker_drain.rs:202`;
   error contract at `:195-201` — errors "when the mandatory prefix …
   belongs to a binding terminal"; core selector
   `crates/liminal-protocol/src/lifecycle/claim_frontier.rs:3354`). Terminal
   commits today exist only inside participant-authorized finalizer
   transactions (Leave, fenced attach).

### 3.3 The ruled fix shape: candidate-lane terminal drain

When frontier selection encounters a binding terminal as the earliest
candidate (`DrainFirst` whose candidate is
`ImmutableSequenceCandidate::BindingTerminal`), the server drains it per R-A2
instead of faulting:

- One durable transaction per candidate: terminal-record append + retention
  transition + candidate deletion + binding-slot release
  (`PARTICIPANT-CONTRACT.md:912-914`). Multiple pending candidates drain
  strictly by `admission_order`, each an independently committed, crash-visible
  transaction (Leave precedent, `:2191-2193`).
- Fate completion is wired through `record_terminal_impact`
  (`crates/liminal-server/src/server/participant/production/connection_fate.rs:312`):
  committed-source projection publication, `record_binding_changed`,
  `record_episode_changed` — the same completion path finalizers use today.
- After the drain, admission proceeds for the caller exactly as the existing
  marker DrainFirst continuation does (`ops_frontier.rs:143`,
  `apply_record_admission_with_impact` re-entry).
- Mechanically, the drain lives beside `persist_next_marker` as a sibling
  terminal path selected on the candidate variant; `drain_next_marker` itself
  stays marker-only (row S-3). No new frontier decision variant is proposed.

Driver: **the publish that encounters the terminal** — plus the existing
finalizers (Leave, fenced attach), which remain valid and unchanged. Nothing
polls; no timer; no sweep (§7, LAW-1).

### 3.4 Redrawn pin assertion (tear-seat design note, resolved here)

Under drain semantics, the correct end-state of the pinned scenario is that
the publish **commits after the drain**. The red pin's "refused, not
committed" assertion (`tests_restore_window.rs:85` on the held branch) embodies
the dead typed-refuse ruling and must be redrawn; its repro half (crash-restore
residence + valid publish naming the victim) stays valid verbatim.

Redrawn assertion (row S-2):

1. Dispatch returns `RecordCommitted` — not a refusal, not `Fatal`; the
   connection stays open (no `FrameAction::Close`).
2. Exactly one Died terminal record was appended for the victim; the candidate
   is deleted; the binding slot is released (`slots.remove` semantics — the
   sole slot-removal precedent is Leave's
   `crates/liminal-server/src/server/participant/production/ops_leave.rs:289`).
3. The committed record's recipient set excludes the drained victim: `produced`
   (`crates/liminal-server/src/server/participant/production/outbox_projection.rs:359`)
   owes records only to slot-present recipients (live `Bound` or resumable
   `Detached`, contract comment at `:365-372`), and the drained slot is absent
   by construction. In the pinned prestate the victim may be the sole other
   participant; the record then commits with an empty recipient set, which is
   legal today.
4. Post-commit durable replay shows no residual candidate; a repeat publish
   commits without any drain work (idempotence of the closure).
5. The census pin (`:121`) keeps its prestate role unchanged.

### 3.5 Inherited live cold-restart SOCKET PIN obligation

The lane owes the belt-and-suspenders end-to-end pin (row S-4): a real unclean
server restart (process killed, not graceful), restore with the pending
residence, a **live-socket** client publish naming the victim, the correct
post-drain outcome observed **client-side** (`RecordCommitted` on the wire, no
connection close), and the server demonstrably keeps serving (subsequent
operations on the same and other connections succeed). Existing socket/cold
fixtures (`e2e_socket_fixture.rs`, `e2e_cold_*` under
`crates/liminal-server/src/server/participant/production/`) are the harness
precedents; the pin itself is new.

## 4. Problem 2 — the production ProcessKilled emitter

### 4.1 Design of record

§14 of `docs/design/W1B-FATE-SOURCES.md` (heading `:1128`) is the design of
record; the seam row at `:1132` reads: production ProcessKilled
participant-binding emitter; consumer = beamr-to-participant exact exit-fate
adapter; owners = Artemis (beamr API) + this repo (liminal adapter); oracle
floor = "exact forced process exit opens one bounded intent and appends
ProcessKilled once; other classes cannot select it; live/cold agree".

Artemis's half is **discharged**: no new beamr API is needed (facts below).

**Incarnation ruling (tear seat, final):** §14's "carrying connection
incarnation" is the intent reading — *pid-correlatable-at-delivery*. The
adapter maintains the pid → binding-incarnation map at spawn/attach; beamr
carries pid + reason only. Layering forbids beamr knowing liminal
incarnations.

### 4.2 Beamr 0.16.0 facts

> **Citation of record: pinned pending Artemis's §14 trigger-check post.** Her
> forwarded findings slot in here as the authoritative citation when they
> arrive. The pins below are this worker's local re-verification at beamr
> checkout `fd71c5e` (crate 0.16.0) and support the design but do not
> substitute for her post. Vesper's four-signature exit surface may already
> satisfy §14's trigger; her post decides.

Re-verified locally (paths under `crates/beamr/src/scheduler/`):

- `Scheduler::subscribe_exit_events` (`execution.rs:213`) returns a bounded
  **single-subscriber** `ExitEventSubscription` — "Exactly one subscription can
  be created for a scheduler's lifetime; later calls return `None`" (doc at
  `execution.rs:198-211`); the publisher holds a set-once sender
  (`exit_events.rs:154`).
- `ExitEvent::Exited { pid: u64, reason: ExitReason }` (`exit_events.rs:22-30`).
- The outcome is published **before** the event, so an immediate
  `Scheduler::take_exit_outcome(pid)` (`execution.rs:194`) yields the exact
  `(ExitReason, OwnedTerm)` pair (`exit_events.rs:23-24`; `execution.rs:200-202`).
  Outcomes are retained until consumed even if tombstones/notifications are
  evicted (`execution.rs:209-211` doc).
- The publisher is `try_send` on a bounded queue (`exit_events.rs:157`),
  capacity `EXIT_EVENT_CAPACITY = 1_024` (`exit_events.rs:18`); overflow sets a
  flag surfaced as a typed `ExitEvent::Lagged` marker (`exit_events.rs:37`,
  overflow store at `:163`) with the documented drain-then-`take_exit_outcome`
  recovery — no outcome is discarded.

### 4.3 Adapter design

- **Entry semantics.** On `Exited { pid, reason }` for a pid mapped to a live
  binding incarnation, the adapter immediately calls `take_exit_outcome(pid)`
  for the exact pair, opens **one bounded intent**, and appends the
  ProcessKilled Died-source row **once** — with disposition `Pending`. That is,
  the emitter **widens the `PendingFinalization(Died)` entry set**; it does not
  finalize. Finalization flows through the existing finalizers or the §3 drain,
  and thus through `record_terminal_impact` (`connection_fate.rs:312`)
  identically to today's Died sources (row S-8).
- **Distinct additive intent.** ProcessKilled is its own intent class (M-shape
  architecture). The trybuild wall
  `production_connection_fate_cannot_select_process_killed`
  (`crates/liminal-server/src/server/participant/production/tests_w1b_connection_fate.rs:422`)
  **stays**: production connection fate cannot select ProcessKilled; the
  emitter is its sole selector, and the emitter cannot select the
  connection-fate classes (row S-10).
- **Driver.** The adapter is TOLD by beamr's event delivery (blocking recv on
  the bounded channel). No polling anywhere in the path (§7).

### 4.4 pid-reuse ABA — stated and pinned (row S-6)

Hazard: a stale pid → incarnation map entry misattributes an exit — either an
old process's late exit lands on a newer incarnation that reused the pid, or a
mapping for an already-finalized incarnation swallows a fresh exit.

Designed closure — **remove-on-exit-delivery**, plus insert hygiene:

- Consuming an exit for pid P (including during Lagged recovery) removes P's
  map entry in the same adapter step: one delivered exit consumes exactly one
  mapping; a second event for P cannot re-select the dead incarnation.
- Insert at spawn/attach asserts vacancy. If P is occupied, the adapter first
  resolves the old entry via `take_exit_outcome(P)` (retained-until-consumed
  makes this total), appends its pending terminal, removes it, then inserts —
  never a silent replace (the TOCTOU `Registry::insert` cousin fenced in beamr
  0.15.4 is the anti-pattern).
- This is a designed candidate closure, **not an assumption** that beamr never
  reuses pids; whether beamr pids can in fact recur is left to Artemis's post
  (§8).

Specified pin: an adapter-level test that delivers exit(P) for incarnation I1,
re-maps P to incarnation I2, delivers exit(P) again, and asserts I1 gained
exactly one ProcessKilled append (append-once), I2's exit attributes to I2
only, and the occupied-insert path performs resolve-then-insert. Adapter-level
because pid reuse cannot be forced from liminal; if Artemis's post proves pids
never recur, the pin still stands as map-hygiene coverage.

## 5. The five banked brief obligations

**(a) DrainFirst interaction.** Emitter-created pending terminals appear in
frontier selection as `BindingTerminal` candidates exactly like restore-window
residencies — same `DrainFirst` classification (`ops_frontier.rs:136`), same
candidate lane, same R-A2 ordering. The §3 drain serves both uniformly; the
emitter introduces no new frontier decision and no new drain variant. Without
the drain, an emitter entry would reproduce the §3.1 tear against live
traffic in a live VM — the composition argument of §2 and the ordering of
row S-9.

**(b) B1 no-regression witness.** The emitter flows through
`record_terminal_impact` (`connection_fate.rs:312`) — no new projection or
impact path. `produced`'s recipient filter (live `Bound` OR resumable
`Detached`, `outbox_projection.rs:359` with contract comment `:365-372`) and
present-`Detached` parking (`record_published_projection`,
`crates/liminal-server/src/server/participant/production/dispatch_impact.rs:56`,
parking comment `:80-82`) are provably unchanged: neither half of this design
edits either file. Pinning tests that must stay green untouched:
`while_dead_publish_reaches_resumed_replay`
(`tests_w1b_while_dead_replay.rs:100`),
`clean_leave_departed_mints_no_obligation` (`tests_w1b_while_dead_replay.rs:236`),
`recipient_snapshot_is_postcommit_bound_and_resumable_detached_minus_sender`
(`tests_unit2_recipient_snapshot.rs:54`).

**(c) Subscription claim.** The embedding server claims beamr's ONE exit-event
subscription (set-once sender; `subscribe_exit_events` returns `None` on any
later call, `execution.rs:213`). Stated explicitly: anything else in liminal's
embedding that ever wants exit events multiplexes BEHIND the adapter — the
adapter owns the subscription for the scheduler's lifetime and is the only
component that may hold it (row S-5). The multiplex facility itself is an
interface commitment only, not built here (§8).

**(d) Lagged handling is MANDATORY adapter logic.** Capacity is 1,024
(`exit_events.rs:18`). On observing `ExitEvent::Lagged` (`exit_events.rs:37`)
the adapter performs the documented recovery: drain the marker, then call
`take_exit_outcome(pid)` for every pid it currently tracks in the map,
appending pending terminals for each recovered exit (append-once per
incarnation preserved by remove-on-exit-delivery). No outcome is discarded by
beamr, so recovery is total (row S-7).

**(e) pid-reuse ABA.** Stated and pinned in §4.4 (row S-6).

## 6. Hard constraints honored (all ruled, none renegotiated here)

- §14 oracle floor: exact forced process exit opens ONE bounded intent and
  appends ProcessKilled ONCE; other classes cannot select it; live/cold agree
  (`W1B-FATE-SOURCES.md:1132`).
- Trybuild wall `production_connection_fate_cannot_select_process_killed`
  stays (`tests_w1b_connection_fate.rs:422`).
- The crash repository stays TEST-ONLY.
- No frame-dependency claim anywhere in this design (F-3a premise is dead; the
  emitter's value is actor-granular death in a live VM where the socket never
  drops).
- Emitter is HARD-GATED behind the restore-window (drain) lane and Artemis's
  beamr exit-event sizing, her pacing (rows S-9, S-13).

## 7. LAW-1 / no-polling and idle-cost honesty

The drain is driven by the publish that encounters the terminal (plus the
existing finalizers); the emitter is driven by beamr's event delivery. Nothing
polls, no timer, no sweep — matching R-A2's own text: "No mailbox, absence
inference, poll, sweep, or fabricated later cause participates"
(`PARTICIPANT-CONTRACT.md:913-914`).

Idle-cost statement: with no process exits and no pending terminals there is
**zero** background work — the adapter blocks on a bounded channel recv, and
drain logic executes only inside publish/finalizer dispatch. A pending
terminal with no subsequent traffic simply remains durable until a publish,
Leave, or fenced attach encounters it; this design claims no liveness beyond
that (see §8).

## 8. Honesty — deliberately not solved here

1. **Standalone pending-terminal finalizer** stays deferred (its own seam row,
   `W1B-FATE-SOURCES.md:1133`): a pending terminal that no traffic ever
   touches is finalized only by Leave/attach/drain-on-publish. No sweeper is
   added, by law.
2. **Beamr pid-reuse ground truth** is not established here; §4.4's closure is
   designed defensively and pinned at the adapter level. Artemis's post is the
   citation of record.
3. **Artemis's §14 trigger-check post** had not arrived at r1 commit; §4.2 is
   pinned pending it, on worker-local byte re-verification only.
4. **The behind-the-adapter multiplex** (obligation (c)) is an interface
   commitment, not a built facility.
5. **Exact refusal-vs-commit behavior for publishes racing a concurrent
   in-flight drain transaction** is left to the build's serialization story
   (dispatch is already serialized per conversation authority today); no new
   concurrency claim is made.
6. **ProcessKilled wire vocabulary** (the exact `DiedCause`/terminal-kind
   variant naming) is left to the build under the semver row S-14; this doc
   fixes semantics, not spelling.

## 9. «SOCKET» decision register

| id | decision | status |
|---|---|---|
| S-1 | Typed-refuse is dead; the fix is the R-A2 candidate-lane terminal drain: on `DrainFirst` selecting a `BindingTerminal`, drain it (terminal append + retention transition + candidate deletion + slot release, one durable transaction per candidate, strictly by `admission_order`, completion via `record_terminal_impact`), then proceed to caller admission | proposed-pending-tear |
| S-2 | Redrawn pin: the pinned scenario's publish COMMITS after the drain; repro half kept verbatim; assertion set per §3.4 (commit, no close, one terminal append, slot released, victim excluded from recipients, replay clean) | proposed-pending-tear |
| S-3 | The drain is a sibling terminal path beside `persist_next_marker` in the existing DrainFirst arm; `drain_next_marker` stays structurally marker-only; no new frontier decision variant | proposed-pending-tear |
| S-4 | Lane closure requires the live cold-restart SOCKET pin of §3.5 (real unclean restart, live-socket publish, client-observed post-drain commit, server keeps serving) | proposed-pending-tear |
| S-5 | The liminal embedding's adapter claims beamr's single exit-event subscription; all other liminal-side consumers multiplex behind the adapter | proposed-pending-tear |
| S-6 | pid → binding-incarnation map lives in the adapter (pid-correlatable-at-delivery); remove-on-exit-delivery + resolve-before-occupied-insert closes the pid-reuse ABA; adapter-level pin per §4.4 | proposed-pending-tear |
| S-7 | Lagged handling is mandatory adapter logic: drain marker, then `take_exit_outcome` over all tracked pids; capacity 1,024 | proposed-pending-tear |
| S-8 | The emitter only WIDENS the `PendingFinalization(Died)` entry set (append-once pending row); finalization is exclusively existing finalizers + the S-1 drain, all through `record_terminal_impact`; `produced`/parking untouched with the §5(b) pinning tests as witness | proposed-pending-tear |
| S-9 | Sequencing: the drain builds FIRST and is standalone-buildable (closes the red-pinned defect with zero beamr dependency); the emitter follows, hard-gated behind the landed drain + Artemis's sizing — an emitter without the drain widens a live tear and is forbidden | proposed-pending-tear |
| S-10 | ProcessKilled stays a DISTINCT additive intent; the trybuild wall stays; the emitter is its sole selector and selects nothing else | proposed-pending-tear |
| S-11 | Crash repository stays test-only; no production dependency introduced by either half | proposed-pending-tear |
| S-12 | No frame dependency is claimed anywhere (F-3a premise dead); the emitter's value claim is actor-granular death in a live VM, socket never drops | proposed-pending-tear |
| S-13 | Emitter pacing: gated behind restore-window lane and Artemis's beamr exit-event sizing, at her pacing; §4.2 citation of record is her forthcoming trigger-check post | proposed-pending-tear |
| S-14 | Semver: drain = server-only defect-class behavior change (tear → commit), no wire schema change, no protocol/SDK bump — server minor class. Emitter = additive wire vocabulary (new Died/terminal cause variant visible in projections) — protocol additive minor + server minor, SDK decode addition; exact spelling deferred to build (§8.6) | proposed-pending-tear |

## 10. Semver / compatibility detail for S-14

- **Drain:** changes observable behavior only in states that today terminate
  the connection via an invariant tear (`ops_frontier.rs:482-484` →
  `apply.rs:208-211`). No request or response shape changes; no SDK change; no
  protocol change. Implies a server minor (0.4.0-class under house style for
  behavior-visible fixes; the tear seat may rule patch-class since the changed
  path is today a defect).
- **Emitter:** a ProcessKilled cause must be projectable to SDKs (today's
  `DiedCause` mapping at `outbox_projection.rs:178` shows the projection
  surface), so it is an additive protocol change: protocol minor, server
  minor, SDK decode addition. Cold/live replay equivalence is part of the §14
  oracle floor, not an extra compat claim.

## 11. Revision record

| rev | date (UTC) | change |
|---|---|---|
| r1 | 2026-07-23 | Initial unified design: candidate-lane terminal drain + ProcessKilled emitter; all decisions proposed-pending-tear; §4.2 pinned pending Artemis's trigger-check post |
