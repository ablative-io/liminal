# PENDING-DRAIN-EMITTER — candidate-lane terminal drain + production ProcessKilled emitter

Revision: r4 (PROPOSED, pending tear — this revision is the emitter design
round dispatched 2026-07-23: pin rebase to the 0.4.1 release tree, the
S-14 supersession record, the LIM-DETACHED-PENDING Detached-drain ruling
(§3A), and the ASK-6 answer (§8A). r3 was TORN 2026-07-23 — all fifteen
sockets approved, drain build authorized; the drain has since LANDED and
RELEASED, see §3.0.)

Base: main `703591d` (0.4.1 release tree — liminal-server 0.4.1,
liminal-sdk 0.4.1, liminal 0.4.1, liminal-protocol 0.3.2). Every liminal
file:line pin below was re-verified at that commit this round (r3 was
based at `14032ca`; the full pin survey ran at r4 and drifted pins are
corrected in place). Beamr pins in §4.2 remain quoted at the r3 surfaces
(published 0.16.0 lineage `5ebf94d`, local checkout `fd71c5e`); the
estate's beamr is now 0.16.2 — §8.7 makes re-corroboration at the
then-current beamr pin an explicit emitter-build precondition.

## 1. Scope

One design unifying two ruled problems, per the tear seat's collapse of two
lanes after a gate-order circularity was proven — extended at r4 with the
Detached-flavor ruling and the ASK-6 answer:

- **Problem 1 — the restore-window tear** (Died flavor): **CLOSED.** Built,
  landed, and released in liminal-server 0.4.1 / liminal-protocol 0.3.2
  (§3.0). §3 stands as the design record of what landed.
- **Problem 1b — LIM-DETACHED-PENDING** (the Detached sibling, ruled here at
  r4, §3A): a crash-restored pending-**Detached** first candidate still tears
  Fatal. The ruling on its drain semantics is made in this round; the fix
  builds later as a bounded drain-extension lane.
- **Problem 2 — the production ProcessKilled emitter** (§14 of
  `docs/design/W1B-FATE-SOURCES.md` = design of record; deferred-seam row at
  `docs/design/W1B-FATE-SOURCES.md:1132`; §14 heading at `:1128`): the
  beamr-to-participant exact exit-fate adapter. Still design-of-record here;
  not built.
- **ASK-6** (frame, durable admission idempotency): answered at §8A —
  sequenced OUT with reasoning recorded, per the dispatch's
  no-silence requirement.

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
set that today tears the connection (§5.1, row S-9). At r4 the ordering has
resolved: the drain is landed, so the emitter's hard gate S-9 is satisfied on
the Died side. The emitter mints **Died-flavored** rows only, so it does not
widen the still-open Detached set of §3A — the Detached extension and the
emitter build are independent lanes (§3A.6).

## 3. Problem 1 — the restore-window tear (Died flavor) — LANDED RECORD

### 3.0 Status: BUILT, LANDED, RELEASED (r4 note)

The S-1/S-2/S-3/S-4 build landed on main via `392e4d2` (protocol owner op),
`87db3bb` (server drain), `acd8eb7` (0.4.1 release); released in
liminal-server 0.4.1 with liminal-protocol 0.3.2. As-landed deltas against the
r3 text, recorded per the never-rewrite-evidence rule:

- **The DrainFirst arm now routes through `persist_drain_first`**
  (`crates/liminal-server/src/server/participant/production/ops_frontier.rs:142`,
  router at `ops_terminal_drain.rs:47-61`): `Marker` → `persist_next_marker`,
  `BindingTerminal` → `persist_terminal_drain`. r3 described the sibling path
  conceptually; the router function is the landed spelling. The marker-lane
  backstop invariant stays (`ops_frontier.rs:482-484`, now inside
  `canonical_marker_bytes` at `:469`).
- **Identity erasure went further than §3.4 pt 2 spelled**: the drain has no
  Leave request and cannot fabricate retirement authority, so
  `release_drained_binding_slot` (`ops_terminal_drain.rs:288-291`) removes the
  slot AND the enrollment-token mapping — later probes answer
  `ParticipantUnknown`; re-enrollment with the same token mints a fresh
  identity (doc comment `:278-287`). Ratified at the landing tear
  (deviation 3, full identity erasure). This is **Died-only** semantics —
  §3A rules the opposite for Detached.
- **The redrawn pin landed as a suite**, not one test:
  `tests_restore_window.rs` on main (495 lines) —
  `valid_publish_during_pending_finalization_residence_commits_after_drain`
  (`:76`), recipient exclusion (`:191`), repeat-publish idempotence (`:227`),
  leave-shaped finalizer transaction (`:276`), sole-candidate bound (`:378`),
  census pin moved to `:459-460`. The r3-cited evidence branch
  `fix/restore-window-typed-refuse` @ `3d66a89` is superseded and sweepable.
- **The S-4 e2e pin** landed as its own file `e2e_terminal_drain.rs`
  (`live_socket_publish_drains_pending_terminal_then_unclean_restart_serves`,
  `:161`): live-socket publish → wire `RecordCommitted`, no close, drained row
  durable, server keeps serving, then a **real unclean restart** and replay
  assertions (victim slot gone `:219`, no surviving candidate `:222-239`).
- **Protocol surface**: `LiveFrontierOwner::drain_pending_terminal`
  (`crates/liminal-protocol/src/lifecycle/operations/live_frontier.rs:385`,
  public, with `DrainedPendingTerminal` / `PendingTerminalDrainRefused`) and
  the crate-private `ClaimFrontiers::drain_first_binding_terminal`
  (`crates/liminal-protocol/src/lifecycle/claim_frontier.rs:3454`, sibling of
  `drain_next_marker_core` at `:3354`). This forced the protocol bump S-14
  said would not happen — supersession recorded at §9/S-14 and §10.

### 3.1 Mechanism (pinned trace, re-verified at 703591d)

During crash-restore, a participant slot resident `PendingFinalization(Died)`
(Died source row durable with disposition `Pending`, finalizer absent) caused:

1. Dispatch runs record admission: `select_record_admission` (the alias of
   `liminal_protocol`'s `apply_record_admission`,
   `crates/liminal-server/src/server/participant/production/ops_frontier.rs:14`,
   call at `:126`).
2. The decision is `RecordAdmissionDecision::DrainFirst` (`ops_frontier.rs:136`)
   because the earliest candidate is the victim's `BindingTerminal`.
3. (Pre-0.4.1) the DrainFirst arm was marker-only and faulted on a terminal
   candidate; (0.4.1) the arm drains it per §3.3 and admission proceeds.
4. Pre-0.4.1 the invariant surfaced as `ParticipantDispatch::Fatal`, which the
   connection layer maps to `FrameAction::Close`
   (`crates/liminal-server/src/server/connection/apply.rs:208-211`).
5. Durable truth replays the residence, so the tear repeated on every retry
   until a finalizer (Leave, fenced attach) happened by.

### 3.2 Why TYPED-REFUSE is dead (ruled out, three walls, byte-verified)

1. **No truthful refusal body exists.** The R-D1 register's total simulation
   (register at `docs/design/PARTICIPANT-CONTRACT.md:5588`; the exhaustive
   "generic stages 2–13" selector at `:5633`; both re-verified at 703591d)
   selects SUCCESS for this publish in the pinned prestate (census pin: sole
   candidate is the victim's terminal, closure Clear, no observers). Any
   constructible refusal fabricates bytes; in particular
   `ObserverBackpressure` would park the SDK forever on an
   `ObserverProgressed` that cannot arrive (no observers exist in the
   prestate). r4 pin correction: `ObserverBackpressure` is at
   `crates/liminal-protocol/src/wire/response.rs:863`; `ObserverProgressed`
   is a server-push value at `crates/liminal-protocol/src/wire/push.rs:149` —
   r3's "both at wire/response.rs" was imprecise.
2. **The contract prescribes drain, not refusal.** R-A2:
   "the lane drains candidates strictly by `admission_order` before caller
   **record admission**", with terminal-record append, retention transition,
   candidate deletion, and binding-slot release as one durable transaction and
   "No mailbox, absence inference, poll, sweep, or fabricated later cause"
   (`docs/design/PARTICIPANT-CONTRACT.md:909-914`; same prescription at `:756`
   — "or first drains those earlier candidates separately" — and in the Leave
   flow at `:2191-2193`).
3. **The marker lane structurally rejects terminals.** `drain_next_marker`
   (`crates/liminal-protocol/src/lifecycle/operations/marker_drain.rs:202`;
   error contract at `:195-201`; core selector `claim_frontier.rs:3354`).

### 3.3 The ruled fix shape: candidate-lane terminal drain (as landed)

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
  `record_episode_changed` — the same completion path finalizers use. Landed
  call site: `ops_terminal_drain.rs:118-125`.
- After the drain, admission proceeds for the caller exactly as the existing
  marker DrainFirst continuation does (`ops_frontier.rs:143`,
  `apply_record_admission_with_impact` re-entry).
- The drain row is the drain-flavored `StoredDied` append —
  `StoredDied.drained: Option<StoredDrainedTerminal>` (`log_v3.rs:409`,
  provenance `{pending_source_sequence, finalizer_presentation}` at `:414`),
  finalizer vocabulary `StoredPendingDiedFinalizer::Drained`
  (`log_v3.rs:474`); cold replay routes by shape (`replay_died_row`,
  `ops_terminal_drain.rs:136-145`).
- `drain_next_marker` stays marker-only (row S-3); no new frontier decision
  variant exists.

Driver: **the publish that encounters the terminal** — plus the existing
finalizers (Leave, fenced attach), which remain valid and unchanged. Nothing
polls; no timer; no sweep (§7, LAW-1).

### 3.4 Redrawn pin assertion — DISCHARGED

Landed as the `tests_restore_window.rs` suite and `e2e_terminal_drain.rs`
listed in §3.0; all five r3 assertion points (commit not refusal, one terminal
append + slot released, victim excluded from recipients, replay-clean
idempotence, census prestate role) are covered there.

### 3.5 Inherited live cold-restart SOCKET PIN obligation — DISCHARGED

Landed as `e2e_terminal_drain.rs` (§3.0).

## 3A. Problem 1b — LIM-DETACHED-PENDING: the Detached-flavor ruling (NEW at r4)

### 3A.1 The defect, pinned at 703591d

Candidate selection is flavor-blind: `ImmutableSequenceCandidate::BindingTerminal`
carries only sequence/order/owner — no Detached/Died flavor
(`crates/liminal-protocol/src/lifecycle/claim_frontier.rs:553-562`) — and the
`DrainFirst` selector gates nothing upstream
(`crates/liminal-protocol/src/lifecycle/operations/record_admission.rs:688-698`).
A crash-restored pending-**Detached** first candidate therefore reaches the
landed drain, whose sole flavor gate refuses it server-side:

```rust
let PendingFinalization::Died(died) = pending else {
    return Err(StateError::invariant(
        "terminal drain candidate is not a pending Died residence",
    ));
};
```

(`ops_terminal_drain.rs:325-329`, inside `validate_drain_candidate` at
`:300`). The `StateError` propagates as `ParticipantDispatch::Fatal` →
`FrameAction::Close` (`apply.rs:208-211`) and durable truth replays the
residence — the same tear-on-every-retry class §3.1 closed for Died. The
0.3.2 protocol ops are already flavor-agnostic:
`drain_pending_terminal(PendingFinalization, ..)` (`live_frontier.rs:385`)
and `drain_first_binding_terminal` (`claim_frontier.rs:3454`) never inspect
the flavor. The defect is entirely the server-side gate plus the missing
Detached completion semantics — which is why the RULING, not the build, is
this round's obligation.

### 3A.2 What mints a pending-Detached residence

- **Blocked explicit detach**: `start_blocked_detach`
  (`crates/liminal-protocol/src/lifecycle/detach.rs:511-545`) — a verified
  detach that cannot commit immediately produces
  `BindingState::PendingFinalization(PendingFinalization::Detached(..))`
  (`detach.rs:526-528`).
- **Server shutdown**: the contract's shutdown row — shutdown "persists every
  remaining terminal as its bounded `PendingFinalization`"
  (`PARTICIPANT-CONTRACT.md:603`), exercised by
  `tests_w1b_pending_detached_leave.rs:203-210` (cause
  `StoredDetachedCause::ServerShutdown`).
- **Restore**: `replay_explicit_pending_detached`
  (`connection_fate_replay.rs:55-138`) re-admits the Pending terminal and
  restores the slot **keeping its member and detach token** (`:132-136`) —
  no erasure at restore.

Durable shape: `StoredDetached { disposition: Pending, .. }`
(`log_v3.rs:446-453`, disposition enum `:366-371`) — today with **no**
finalizer/drained vocabulary (contrast `StoredDied`, §3.3).

### 3A.3 THE RULING (S-16): Detached drain = faithful detach finalization — never Died-style erasure

On `DrainFirst` selecting a Detached-flavored pending `BindingTerminal`, the
candidate-lane drain performs the **detach's own completion** as one durable
R-A2 transaction per candidate, strictly by `admission_order`:

1. **Committed Detached terminal append**, cause preserved in the
   `StoredDetachedCause` domain (`CleanDeregister` / `ServerShutdown` —
   `log_v3.rs` Detached-cause vocabulary). Never a `StoredDied` row: the
   Died cause domain cannot express a Detached cause, and a Died terminal
   would fabricate a death that did not happen.
2. **End state = committed `BindingState::Detached`** — the protocol already
   owns this exact completion: `complete_pending_detach`
   (`detach.rs:553-577`) commits the pending Detached to
   `CommittedBindingTerminal::Detached`, keeps the member
   (`:565-567`), and sets `binding_state: BindingState::Detached` (`:573`).
3. **Slot and enrollment token PRESERVED; the participant remains
   exact-secret resumable.** The contract makes Detached membership durable —
   "membership and cursor remain durable" (`PARTICIPANT-CONTRACT.md:596`) —
   and the client contract retains the attach secret precisely so "the
   complete client record must remain capable of a later credential attach
   after restart" (`crates/liminal-protocol/src/client.rs:54-55`; state table
   `:86`: Detached + exact-secret attach → `Bound`). Died-style erasure
   (`release_drained_binding_slot`'s `slots.remove` + token retain-out,
   `ops_terminal_drain.rs:288-291`) is **FORBIDDEN** for the Detached flavor:
   it would answer `ParticipantUnknown` to a participant the contract
   promises is resumable, and silently orphan the durable membership.
4. **Post-drain the victim is a parked recipient.** `produced` owes records
   to committed-`Detached` slots (`outbox_projection.rs:359`, match arms
   `:377-380`, contract comment `:363-372` — "connection-lost-but-resumable"),
   with `record_published_projection` parking the live tell
   (`dispatch_impact.rs:56`, `:80-82`). So the encountering publish's
   committed record **INCLUDES** the drained-to-Detached victim as a parked
   recipient — the exact opposite of the Died drain's recipient exclusion.
   This is not a new mechanism: it is what committed Detached already means.
5. **Completion via `record_terminal_impact`** (`connection_fate.rs:312`) and
   observer-progress presentation mirroring the landed
   `record_drain_presentation` (`ops_terminal_drain.rs:255`) — no new
   projection or impact path.

**Reading of R-A2's "binding-slot release" for this flavor, stated for the
tear**: for Died, release ratified as identity erasure (no resume claim
survives a death with no retirement authority). For Detached, this ruling
reads "release" as the slot's transition OUT of the pending residence into
committed `Detached` — the candidate is deleted and the lane unblocks, but
the slot persists because the contract gives a Detached participant a
standing resume claim (`:596`, `client.rs:54-55/:86`). The contract's own
phase table lists phase-0 terminals as "Pending or direct `Detached`/`Died`"
(`:660`) and prescribes candidate-lane drain transactions for
shutdown-minted pending-Detached explicitly ("Later candidate-lane
transactions drain one such terminal at a time", `:779-781`; generic
pending-fate drain `:716-718`). If the tear seat reads "release" otherwise,
that is a contract fork to resolve at the tear — flagged here per
stop-and-ask, not silently interpreted.

### 3A.4 Typed-refuse is dead for Detached too

Walls 2 and 3 of §3.2 are flavor-neutral verbatim: the contract prescribes
drain for pending-Detached (`:603`, `:716-718`, `:779-781`, `:909-914`) and
the marker lane rejects any binding terminal. Wall 1 (the R-D1 register
selects SUCCESS) keys off candidate/observer/closure state, not terminal
flavor, so it holds on its face — but its Detached-prestate instantiation is
**UNVERIFIED at 703591d** and becomes a build census obligation (S-18 pin 6),
mirroring the Died census pin.

### 3A.5 Durable schema + replay requirements (S-17 — semantics fixed, spelling to build)

The Detached drain row must be: (i) **distinguishable by shape** in cold
replay, mirroring `replay_died_row`'s `row.drained.is_some()` routing
(`ops_terminal_drain.rs:136-145`); (ii) **additive** in the durable schema
(serde default / skip-if-none, as `StoredDied.drained` was — old rows replay
unchanged); (iii) cause-preserving in the `StoredDetachedCause` domain;
(iv) carrying drain provenance equivalent to `StoredDrainedTerminal`
(`pending_source_sequence`, finalizer presentation). Whether that is a
`drained` field on `StoredDetached` or a sibling source/finalizer widening
is the build's spelling decision, reviewed at its tear. Precedent that
Detached finalization rows already exist for other finalizers:
`StoredComposedTerminalKind::Detached` (`log_v3.rs:300-302`) and the
Leave-finalized pending-Detached path (`ops_leave.rs:337-338`,
`tests_w1b_pending_detached_leave.rs:71-94`).

### 3A.6 Sequencing and non-interaction (S-16/S-9 composition)

The Detached extension is a bounded server-side drain-extension lane: zero
beamr dependency, and — on current evidence — likely zero protocol-crate
change (the 0.3.2 ops already accept both flavors; the gate and schema are
server-side). Its semver class is ruled at its release tear (§10). The
**emitter is independent of it**: the emitter mints Died-flavored rows only
(§4), so landing the emitter neither widens nor depends on the Detached set.
Neither lane blocks the other; both are gated on THIS r4 tear.

### 3A.7 Pins owed by the extension build (S-18, red-first per the runbook)

1. **Red pin first**: crash-restored pending-Detached residence + valid
   publish naming the victim → today `Fatal` at the `:325-329` gate; the red
   run is committed as evidence before the fix.
2. **Commits after drain**: same scenario → `RecordCommitted`, no
   `FrameAction::Close`.
3. **Post-drain state**: victim slot PRESENT as committed
   `BindingState::Detached`; enrollment token still maps (probes do NOT
   answer `ParticipantUnknown`); exactly one committed Detached terminal,
   cause preserved.
4. **Recipient semantics**: the committed record's recipient set INCLUDES the
   drained-to-Detached victim as a parked recipient (contrast the Died
   drain's exclusion pin at `tests_restore_window.rs:191`).
5. **Resumability — the decisive assertion**: post-drain exact-secret attach
   → `Bound`, and the parked publication replays to the resumed victim (the
   `while_dead_publish_reaches_resumed_replay` precedent,
   `tests_w1b_while_dead_replay.rs:100`).
6. **Census**: R-D1 total-simulation selects SUCCESS in the Detached
   prestate (closes §3A.4's UNVERIFIED flag; mirrors the Died census pin at
   `tests_restore_window.rs:459-460`).
7. **Replay-clean idempotence**: durable replay shows no residual candidate;
   a repeat publish commits without drain work.
8. **Mixed-flavor ordering**: one Died + one Detached pending candidate drain
   strictly by `admission_order`, each in its own transaction with its own
   flavor semantics.
9. **e2e analog** (tearer sizes it, S-4 precedent): ServerShutdown-minted
   pending-Detached, real restart, live-socket publish → wire
   `RecordCommitted`, then a live resume of the victim.

## 4. Problem 2 — the production ProcessKilled emitter

### 4.1 Design of record

§14 of `docs/design/W1B-FATE-SOURCES.md` (heading `:1128`) is the design of
record; the seam row at `:1132` reads: production ProcessKilled
participant-binding emitter; consumer = beamr-to-participant exact exit-fate
adapter; owners = Artemis (beamr API) + Hermes (liminal adapter — the row's
owner column was updated on main since r3); oracle floor = "exact forced
process exit opens one bounded intent and appends ProcessKilled once; other
classes cannot select it; live/cold agree".

Artemis's half is **discharged**: no new beamr API is needed (facts below).

**Incarnation ruling (tear seat, final):** §14's "carrying connection
incarnation" is the intent reading — *pid-correlatable-at-delivery*. The
adapter maintains the pid → binding-incarnation map at spawn/attach; beamr
carries pid + reason only. Layering forbids beamr knowing liminal
incarnations.

**r4 finding — the wire vocabulary already exists end-to-end.** r3 §8.6
deferred "ProcessKilled wire spelling" to the build; the survey at 703591d
shows there is nothing to spell: `DiedCause::ProcessKilled` is a wire variant
today (`crates/liminal-protocol/src/wire/push.rs:37`, enum at `:33`, with
`CloseCause` mapping at `:48`), the stored cause exists
(`StoredDiedCause::ProcessKilled`), the projection arm exists
(`outbox_projection.rs:179`), and the codec decodes it
(`wire/codec.rs:737-740`, entry `:679`). The SDK forwards the decoded push
opaquely (`crates/liminal-sdk/src/remote/participant.rs:412`) — no SDK
change. **The emitter build is therefore a production SOURCE only**: a new
producer that appends `StoredDiedCause::ProcessKilled` rows with disposition
`Pending`. It adds no wire variant, which re-classes its semver (§10) and
supersedes the S-14 emitter half (§9). Build precondition: §8.8's
production-unreachability census.

### 4.2 Beamr 0.16.0 facts

> **Citation of record:** Artemis Peach, stack-devs channel, 2026-07-23
> ~02:15Z, "ASK-4 BEAMR SIZING ANSWER". Tear-seat ruled: this sizing record IS
> the §14 citation of record; no separate trigger-check post is coming. Its
> substance: `subscribe_exit_events` (`execution.rs:213`) returns a bounded
> single-subscriber `ExitEventSubscription`, single subscription per scheduler
> lifetime (set-once/OnceLock sender); `ExitEvent::Exited { pid, reason:
> ExitReason }` (`exit_events.rs:22-30`); the outcome is published BEFORE the
> event, so an immediate `take_exit_outcome(pid)` yields the exact
> `(ExitReason, OwnedTerm)` pair (`execution.rs:194`); the publisher is
> `try_send` on a bounded queue (`exit_events.rs:157`) of capacity 1,024,
> overflow → `Lagged` marker with the documented
> drain-then-`take_exit_outcome` recovery, no outcome discarded — all
> ancestor-verified into the published 0.16.0 release lineage (`5ebf94d`).
> Her record is the authority; the pins below are r3's local corroboration at
> checkout `fd71c5e` and match it. (r4 note: the estate's beamr is now
> 0.16.2; these API facts are re-corroborated at the then-current beamr pin
> as an emitter-build precondition, §8.7.)

Corroborated locally at r3 (paths under `crates/beamr/src/scheduler/`):

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
  for the exact pair and applies the **ExitReason entry filter** (below, row
  S-15). Only a filter-selected reason opens **one bounded intent** and appends
  the ProcessKilled Died-source row **once** — with disposition `Pending`; a
  non-entering reason consumes the outcome and cleans the pid map, nothing
  more. For entering reasons, that is,
  the emitter **widens the `PendingFinalization(Died)` entry set**; it does not
  finalize. Finalization flows through the existing finalizers or the §3 drain,
  and thus through `record_terminal_impact` (`connection_fate.rs:312`)
  identically to today's Died sources (row S-8).
- **Distinct additive intent.** ProcessKilled is its own intent class (M-shape
  architecture). The trybuild wall
  `production_connection_fate_cannot_select_process_killed`
  **stays**: production connection fate cannot select ProcessKilled; the
  emitter is its sole selector, and the emitter cannot select the
  connection-fate classes (row S-10). r4 pin refresh: the harness test is now
  `process_killed_has_no_production_participant_binding_emitter`
  (`crates/liminal-server/src/server/participant/production/tests_w1b_connection_fate.rs:420`,
  `compile_fail` fixture wired at `:423`).
- **Driver.** The adapter is TOLD by beamr's event delivery (blocking recv on
  the bounded channel). No polling anywhere in the path (§7).

**The ExitReason taxonomy** (source: Artemis Peach, stack-devs channel,
2026-07-23 ~04:15Z taxonomy delta; her pins at published 0.16.0 bytes,
corroborated locally at r3): `ExitReason` is a CLOSED six-variant `Copy` enum
with no payload (`crates/beamr/src/process/types.rs:229-243`): `Normal`,
`Kill`, `Killed`, `Error`, `NoConnection`, `NoProc`; `exit_reason_from_term`
rejects any non-six atom as badarg
(`crates/beamr/src/native/process_bifs/mod.rs:348-359`). Partition:

- `Killed` — THE forced-exit terminal. The `Kill` → `Killed` conversion is
  centralized in `link::terminal_reason`
  (`crates/beamr/src/supervision/link.rs:159-164`) and applied before
  `terminate()` at every site — `Killed` is what the event carries.
- `Kill` — signal vocabulary; should never appear on an event, but treated as
  forced-if-ever-observed (defense, not a reachability claim — no
  `unreachable!()`).
- `Normal` — normal completion.
- `Error` — crash class ("Placeholder error exit until error terms land",
  `process/types.rs:236`), plus two defensive producers:
  `exit_reason_from_status` (`scheduler/execution/core.rs:1625-1630` at
  `5ebf94d`; the non-`Exited` arm yields `Error`) and the VM-failure path at
  `scheduler/timer_integration.rs:82`.
- `NoConnection` / `NoProc` — distribution vocabulary that can become a local
  terminal reason via an untrapped link signal.

**The entry filter (row S-15).** The emitter selects ProcessKilled for
`Killed`, and for `Kill` defensively (forced-if-ever-observed); `Normal`,
`Error`, `NoConnection`, `NoProc` DO NOT enter the emitter — each named
explicitly in a TOTAL match over all six variants with NO wildcard arm, at
**every consumed-exit site** (event-delivered, Lagged-recovered §5(d), and
occupied-insert-resolved §4.4 — the consumed-exit invariant). Rationale
(coverage-table discipline): when upstream widens the taxonomy — `Error` is a
self-declared placeholder — the filter breaks compilation instead of silently
misclassifying. Crash-class deaths (`Error`) and distribution-caused deaths
(`NoConnection`/`NoProc`) are DELIBERATELY not absorbed into ProcessKilled —
"exact forced exit" means exact (§14 oracle floor); until a crash-exit
emitter earns its own seam row (§8.3), those deaths surface exactly as they
do today (connection-fate/finalizers) — no regression, no new claim.

### 4.4 pid-reuse ABA — stated and pinned (row S-6)

Hazard: a stale pid → incarnation map entry misattributes an exit — either an
old process's late exit lands on a newer incarnation that reused the pid, or a
mapping for an already-finalized incarnation swallows a fresh exit.

Designed closure — **remove-on-exit-delivery**, plus insert hygiene, both
governed by the consumed-exit invariant (tear fold T1, r3): **EVERY consumed
exit — event-delivered, Lagged-recovered, or occupied-insert-resolved — passes
the S-15 entry filter; entering reasons append once, non-entering reasons
consume the outcome and clean the map, no exception.**

- Consuming an exit for pid P (including during Lagged recovery, and
  regardless of the S-15 entry-filter outcome) removes P's map entry in the
  same adapter step: one delivered exit consumes exactly one mapping; a second
  event for P cannot re-select the dead incarnation. Map hygiene is
  independent of whether the reason entered the emitter.
- Insert at spawn/attach asserts vacancy. If P is occupied, the adapter first
  resolves the old entry via `take_exit_outcome(P)` (retained-until-consumed
  makes this total), passes the resolved reason through the S-15 filter per
  the consumed-exit invariant above (a stale `Normal` exit consumes-and-cleans,
  never mints ProcessKilled; entering reasons append their pending terminal
  once), removes the entry, then inserts — never a silent replace (the TOCTOU
  `Registry::insert` cousin fenced in beamr 0.15.4 is the anti-pattern).
- This is a designed candidate closure, **not an assumption** that beamr never
  reuses pids; whether beamr pids can in fact recur is left to Artemis's post
  (§8).

Specified pin: an adapter-level test that delivers exit(P) for incarnation I1,
re-maps P to incarnation I2, delivers exit(P) again, and asserts I1 gained
exactly one ProcessKilled append (append-once), I2's exit attributes to I2
only, and the occupied-insert path performs resolve-then-insert. Adapter-level
because pid reuse cannot be forced from liminal; if beamr pids are ever proven
never to recur, the pin still stands as map-hygiene coverage.

### 4.5 Attribution and losslessness limits (design-grade caveats)

1. **Attribution is BY-REASON, not by-mechanism.** `exit(Pid, killed)` is
   expressible as a trappable signal, so an untrapped `killed` atom terminates
   the target with `Killed` absent any true kill (same as OTP). The emitter
   attributes the REASON the exit carries, not forensic provenance of who or
   what produced it — stated explicitly as a limit of this design.
2. **Losslessness.** At variant level, yes: the sole designed collapse is
   `Kill` → `Killed` in `link::terminal_reason`
   (`supervision/link.rs:159-164`). WITHIN `Error`, no: the `OwnedTerm` from
   `take_exit_outcome` is the captured final-result term
   (`scheduler/mod.rs:741-760`, NIL default), not a reason encoding; crash
   detail lives in the companion stores `take_exit_error` /
   `take_exit_exception` (`execution.rs:185-197` per Artemis's record; locally
   the independence contract is documented at `execution.rs:189-193` with
   `take_exit_error` at `:221-223`), each consumable without consuming the
   outcome. The entry filter needs only the variant; the companions are noted
   in case sub-`Error` attribution ever matters — it does not in this design,
   because `Error` does not enter.

## 5. The five banked brief obligations

**(a) DrainFirst interaction.** Emitter-created pending terminals appear in
frontier selection as `BindingTerminal` candidates exactly like restore-window
residencies — same `DrainFirst` classification (`ops_frontier.rs:136`), same
candidate lane, same R-A2 ordering. The landed §3 drain serves them
uniformly; the emitter introduces no new frontier decision and no new drain
variant. Without the drain, an emitter entry would reproduce the §3.1 tear
against live traffic in a live VM — the composition argument of §2 and the
ordering of row S-9 (now satisfied: the Died drain is landed and released).

**(b) B1 no-regression witness.** The emitter flows through
`record_terminal_impact` (`connection_fate.rs:312`) — no new projection or
impact path. `produced`'s recipient filter (live `Bound` OR resumable
`Detached`, `outbox_projection.rs:359`, arms `:377-380`, contract comment
`:363-372`) and present-`Detached` parking (`record_published_projection`,
`dispatch_impact.rs:56`, parking comment `:80-82`) are provably unchanged:
neither half of this design edits either file. Pinning tests that must stay
green untouched: `while_dead_publish_reaches_resumed_replay`
(`tests_w1b_while_dead_replay.rs:100`),
`clean_leave_departed_mints_no_obligation` (`tests_w1b_while_dead_replay.rs:236`),
`recipient_snapshot_is_postcommit_bound_and_resumable_detached_minus_sender`
(`tests_unit2_recipient_snapshot.rs:54-55`). (All three re-verified present
at 703591d.)

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
`take_exit_outcome(pid)` for every pid it currently tracks in the map. Every
recovered exit passes the same S-15 entry filter: entering reasons append
their pending terminal (append-once per incarnation preserved by
remove-on-exit-delivery); non-entering reasons consume the outcome and clean
the map only. No outcome is discarded by beamr, so recovery is total
(row S-7).

**(e) pid-reuse ABA.** Stated and pinned in §4.4 (row S-6).

## 6. Hard constraints honored (all ruled, none renegotiated here)

- §14 oracle floor: exact forced process exit opens ONE bounded intent and
  appends ProcessKilled ONCE; other classes cannot select it; live/cold agree
  (`W1B-FATE-SOURCES.md:1132`). "Exact forced" is enforced at entry by the
  S-15 ExitReason filter (§4.3).
- Trybuild wall `production_connection_fate_cannot_select_process_killed`
  stays (`tests_w1b_connection_fate.rs:420-423`, fixture unchanged).
- The crash repository stays TEST-ONLY.
- No frame-dependency claim anywhere in this design (F-3a premise is dead; the
  emitter's value is actor-granular death in a live VM where the socket never
  drops).
- Emitter pacing: the drain gate of S-9 is satisfied (landed); Artemis's
  sizing citation stands (S-13); the remaining build preconditions are §8.7
  and §8.8.

## 7. LAW-1 / no-polling and idle-cost honesty

The drain is driven by the publish that encounters the terminal (plus the
existing finalizers); the emitter is driven by beamr's event delivery. The §3A
Detached extension changes nothing here: same drivers, no timer, no sweep —
matching R-A2's own text: "No mailbox, absence inference, poll, sweep, or
fabricated later cause participates" (`PARTICIPANT-CONTRACT.md:913-914`).

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
   added, by law. (This covers the Detached flavor identically.)
2. **Beamr pid-reuse ground truth** is not established here and is not
   addressed by either cited Artemis record; §4.4's closure is designed
   defensively and pinned at the adapter level.
3. **A crash-exit emitter** (`Error`-class deaths, and any
   `NoConnection`/`NoProc` absorption) would be its own future seam with its
   own row — named here as EXCLUDED under no-row-no-dormancy, not implicitly
   deferred. Until such a row exists, those deaths surface exactly as they do
   today (connection-fate/finalizers): no regression, no new claim.
4. **The behind-the-adapter multiplex** (obligation (c)) is an interface
   commitment, not a built facility.
5. **Exact refusal-vs-commit behavior for publishes racing a concurrent
   in-flight drain transaction** is left to the build's serialization story
   (dispatch is already serialized per conversation authority today); no new
   concurrency claim is made.
6. ~~ProcessKilled wire vocabulary deferred to build~~ — **SUPERSEDED at r4**:
   the vocabulary exists end-to-end today (§4.1); there is nothing to spell.
7. **Beamr 0.16.2 re-corroboration** (NEW at r4): the §4.2 API facts are
   pinned at 0.16.0 bytes; the estate has since published 0.16.1 and 0.16.2.
   No adapter-facing change is CLAIMED for either — but that is unverified,
   so re-corroborating §4.2's four facts at the then-current beamr pin is an
   emitter-build precondition, recorded in the build declaration.
8. **ProcessKilled production-unreachability census** (NEW at r4): the wire
   variant existing today is safe only if no production path appends
   `StoredDiedCause::ProcessKilled` before the emitter. The trybuild wall
   guards connection-fate selection, but an exhaustive append-site census at
   the build's base commit is owed in the build declaration (the r4 survey
   judged it likely-unproduced, not proven).
9. **ASK-6 / durable admission idempotency** is deliberately OUT — §8A.

## 8A. ASK-6 — durable admission idempotency: SEQUENCED OUT, with reasoning (NEW at r4)

**The ask** (text of record: frame branch
`origin/feat/f3b-pubsub-workflow` @ `4767f14`,
`docs/upstream/liminal-asks-2026-07-23.md`, ASK-6; read at that surface this
round): `RecordAdmission`'s attempt token is per-attempt correlation identity
only — at liminal-server 0.4.1 a live binding admits a reused token as a NEW
record whether the body is identical or different, and the refusal cannot be
expressed: protocol 0.3.2's `AttemptTokenBodyConflict` carries only
`CredentialAttach` and `Leave` arms (verified at 703591d:
`crates/liminal-protocol/src/wire/response.rs:59-86`). The ask is the missing
`RecordAdmission` arm plus receipt-provenance-window enforcement on live
bindings — same token + same body echoes the original commit; same token +
different body refuses typed — i.e. what Leave's durable token already
provides. Liminal pre-acknowledges the leg: `client.rs` rule 15
(`crates/liminal-protocol/src/client.rs:42-46` — "A later
sealed-transport-context SDK leg may add outbound attempt tokens and lift
this restriction"), with rule 14's pending-window-crash companion
(`:40-41`). The ask's crash-recovery sharpening: resume-replay excludes the
crashed publisher's own record in every placement, so kill-during-submission
is typed-possible-duplicate BY CONSTRUCTION at the current substrate — the
durable token is the only path to crash-safe publish-once. Filed
**NON-GATING** by the coordination seat: frame builds on the honest weaker
contract, with characterization suites
(`crates/frame-conv/tests/pattern_pubsub.rs`,
`crates/frame-conv/tests/characterize_admission_recovery.rs`) pinning 0.4.1
truth and tripping loudly the day liminal lands anything.

**The answer (S-19): OUT of this design and out of the emitter build lane —
sequenced as its own future design-first round.** Reasoning, recorded per the
dispatch's no-silence requirement:

1. **Zero mechanism overlap.** ASK-6 lives on the client→server admission
   request path (wire response vocabulary, server receipt-provenance
   enforcement, SDK outbound-token leg). The emitter lives on server-side
   death-source production (beamr exit events → `StoredDied` pending rows);
   the Detached extension lives in the server drain. No shared mechanism, no
   shared invariants, no shared files beyond the protocol crate as a
   compilation unit.
2. **Different change class.** ASK-6 is a wire-schema addition (a new
   `AttemptTokenBodyConflict` arm) plus a new durable server window plus the
   rule-15 SDK leg — protocol minor + server + SDK, a full contract
   strengthening that deserves its own R-D1-grade design pass (replay/dedup
   semantics, window bounds, idle-cost of the receipt window under
   no-silent-tradeoffs). The emitter, post-§4.1, is server-behavior only.
   Bundling them would couple unrelated semver classes and hold the estate's
   one live known defect (LIM-DETACHED-PENDING, §3A) hostage to a
   client-idempotency feature.
3. **Non-gating by the filing itself.** Frame proceeds on the characterized
   weaker contract; no consumer is blocked. The trip-pins make a later
   landing loud at frame's tree — the announcement mechanism already exists,
   so sequencing out loses nothing silently.
4. **Non-interference commitment (binding on the emitter and Detached
   builds):** neither lane touches the `RecordAdmission` request path, the
   response vocabulary, or the receipt window; rule 15's "later leg" remains
   open exactly as written. Nothing in this design forecloses or prejudges
   the ASK-6 shape.
5. **No-row-no-dormancy:** sequencing out without a row would be silent
   deferral. This round therefore proposes a named board row — "durable
   admission idempotency (frame ASK-6): `AttemptTokenBodyConflict::RecordAdmission`
   arm + live-binding receipt-provenance-window enforcement + the rule-15
   sealed-transport outbound-token SDK leg; design-first; its own round" —
   for Tom's desk at this r4 tear. The row's existence, not this paragraph,
   is what keeps the ask alive; if the tear declines the row, that decision
   gets recorded here in r5.

## 9. «SOCKET» decision register

| id | decision | status |
|---|---|---|
| S-1 | Typed-refuse is dead; the fix is the R-A2 candidate-lane terminal drain: on `DrainFirst` selecting a `BindingTerminal`, drain it (terminal append + retention transition + candidate deletion + slot release, one durable transaction per candidate, strictly by `admission_order`, completion via `record_terminal_impact`), then proceed to caller admission | ruled (torn 2026-07-23, T1 folded) — **DISCHARGED: landed `87db3bb`/`392e4d2`, released 0.4.1** |
| S-2 | Redrawn pin: the pinned scenario's publish COMMITS after the drain; repro half kept verbatim; assertion set per r3 §3.4 | ruled — **DISCHARGED: landed as the `tests_restore_window.rs` suite (§3.0)** |
| S-3 | The drain is a sibling terminal path beside `persist_next_marker`; `drain_next_marker` stays structurally marker-only; no new frontier decision variant | ruled — **DISCHARGED: landed as the `persist_drain_first` router (§3.0)** |
| S-4 | Lane closure requires the live cold-restart SOCKET pin | ruled — **DISCHARGED: landed as `e2e_terminal_drain.rs` (§3.0)** |
| S-5 | The liminal embedding's adapter claims beamr's single exit-event subscription; all other liminal-side consumers multiplex behind the adapter | ruled (torn 2026-07-23, T1 folded) — **AMENDED 2026-07-23 (tear seat, on the S-5 fork survey, `docs/design/PK-EMITTER-S5-FORK-SURVEY.md`)**: the subscription is already claimed at supervisor construction by the W4 reclaim reactor (`supervisor.rs:1055`, sole drainer `:2661-2670`), so "the adapter claims it" is unimplementable as written. Re-spelled, conditional on the build proceeding at all (Finding A′ routed to Tom): **the adapter is an EXTENSION of the reclaim reactor** — one component, one subscription, the S-15 total filter inside it, outcome consumption unified on S-7's take semantics (the reactor's non-consuming Lagged-path peek at `:2849` re-spelled to take). The multiplex-behind-adapter option is dead (builds what §5(c)/§8.4 exclude); a separate participant scheduler is dead (no substrate) |
| S-6 | pid → binding-incarnation map lives in the adapter (pid-correlatable-at-delivery); remove-on-exit-delivery (regardless of S-15 entry-filter outcome) + resolve-before-occupied-insert closes the pid-reuse ABA, under the §4.4 consumed-exit invariant: EVERY consumed exit (event-delivered, Lagged-recovered, occupied-insert-resolved) passes the S-15 filter — entering reasons append once, non-entering consume-and-clean, no exception; adapter-level pin per §4.4 | ruled (torn 2026-07-23, T1 folded) — standing |
| S-7 | Lagged handling is mandatory adapter logic: drain marker, then `take_exit_outcome` over all tracked pids, each recovered exit passing the S-15 entry filter; capacity 1,024 | ruled (torn 2026-07-23, T1 folded) — standing |
| S-8 | The emitter only WIDENS the `PendingFinalization(Died)` entry set (append-once pending row); finalization is exclusively existing finalizers + the S-1 drain, all through `record_terminal_impact`; `produced`/parking untouched with the §5(b) pinning tests as witness | ruled (torn 2026-07-23, T1 folded) — standing |
| S-9 | Sequencing: the drain builds FIRST; the emitter follows, hard-gated behind the landed drain + Artemis's sizing | ruled — **drain gate SATISFIED at 0.4.1; remaining preconditions §8.7/§8.8** |
| S-10 | ProcessKilled stays a DISTINCT additive intent; the trybuild wall stays; the emitter is its sole selector and selects nothing else | ruled (torn 2026-07-23, T1 folded) — standing; wall pin refreshed (`tests_w1b_connection_fate.rs:420-423`) |
| S-11 | Crash repository stays test-only; no production dependency introduced by either half | ruled (torn 2026-07-23, T1 folded) — standing |
| S-12 | No frame dependency is claimed anywhere (F-3a premise dead); the emitter's value claim is actor-granular death in a live VM, socket never drops | ruled (torn 2026-07-23, T1 folded) — standing |
| S-13 | Emitter pacing: Artemis's ~02:15Z sizing record is the §4.2 citation of record; no separate trigger-check post | ruled (torn 2026-07-23, T1 folded) — standing, with §8.7's re-corroboration precondition |
| S-14 | Semver prediction (r3): drain = no protocol/SDK bump; emitter = additive wire vocabulary | **SUPERSEDED BY REALITY at r4** — recorded per evidence-wins: the drain's owner op is public protocol-crate lifecycle API and forced protocol 0.3.1→0.3.2 (no wire change — r3 conflated wire schema with protocol-crate API); the emitter needs NO new wire vocabulary (§4.1) and re-classes as server-behavior. Final versions ruled at each build's release tear (§10) |
| S-15 | ExitReason entry filter: ProcessKilled for `Killed`, and `Kill` defensively; `Normal`, `Error`, `NoConnection`, `NoProc` never enter — each named in a TOTAL match over all six variants with NO wildcard arm, applied at every consumed-exit site (event-delivered, Lagged-recovered, occupied-insert-resolved); non-entering exits still consume the outcome and clean the pid map | ruled (torn 2026-07-23, T1 folded) — standing |
| S-16 | **NEW r4** — LIM-DETACHED-PENDING ruling: Detached-flavored candidate drain = faithful detach finalization (§3A.3): committed Detached terminal in the `StoredDetachedCause` domain, end state committed `BindingState::Detached`, slot + enrollment token PRESERVED, participant stays exact-secret resumable, victim becomes a parked `produced` recipient; Died-style identity erasure FORBIDDEN for this flavor; R-A2 "binding-slot release" read as release-from-pending-residence (reading flagged for the tear, §3A.3) | proposed r4 — pending tear |
| S-17 | **NEW r4** — Detached drain row requirements: distinguishable-by-shape replay, additive durable schema, cause-preserving, drain provenance equivalent to `StoredDrainedTerminal`; exact spelling to the build (§3A.5) | proposed r4 — pending tear |
| S-18 | **NEW r4** — Detached extension pins (§3A.7): red-first defect pin, commits-after-drain, slot/token preserved + NOT ParticipantUnknown, victim included-and-parked in recipients, post-drain resume replays parked publication, R-D1 Detached-prestate census, replay-clean idempotence, mixed-flavor ordering, e2e analog (tearer sizes) | proposed r4 — pending tear |
| S-19 | **NEW r4** — ASK-6 sequenced OUT of this design and the emitter/Detached builds, with §8A's recorded reasoning, the binding non-interference commitment, and a proposed named board row for its own design-first round | proposed r4 — pending tear |

## 10. Semver / compatibility detail (r4, superseding r3's §10)

- **Drain (landed, historical record):** liminal-protocol 0.3.1→0.3.2
  (additive public lifecycle API: `drain_pending_terminal`,
  `DrainedPendingTerminal`, `PendingTerminalDrainRefused`; zero `wire/`
  changes), liminal-server 0.4.1. The SDK's 0.4.1 rode the family release,
  not the drain.
- **Detached extension (§3A):** expected server-only — the 0.3.2 protocol ops
  already accept both flavors and the gate + schema are server-side; if the
  build's spelling stays inside `liminal-server`, no protocol bump. Ruled at
  its release tear.
- **Emitter:** NO new wire vocabulary (§4.1) — expected server-behavior class
  (a new production source + adapter). Any protocol bump would come only from
  new public protocol-crate API the build turns out to need, per the S-14
  lesson (protocol-crate API ≠ wire schema — both are bump surfaces). Ruled
  at its release tear, per the dispatch.
- Cold/live replay equivalence is part of the §14 oracle floor, not an extra
  compat claim.

## 11. Revision record

| rev | date (UTC) | change |
|---|---|---|
| r1 | 2026-07-23 | Initial unified design: candidate-lane terminal drain + ProcessKilled emitter; all decisions proposed-pending-tear; §4.2 pinned pending Artemis's trigger-check post |
| r2 | 2026-07-23 | §4.2 citation of record + ExitReason entry filter per Artemis's taxonomy delta; filter proposed Killed\|Kill→ProcessKilled, others never |
| r3 | 2026-07-23 | Tear fold T1: consumed-exit invariant stated in §4.4 and cited in S-6. Torn APPROVE all fifteen sockets; drain build (S-1/S-2/S-3/S-4) authorized on this fold |
| r4 | 2026-07-23 | The emitter design round (dispatched post-0.4.1): rebased to main `703591d`; §3.0 landed record (S-1..S-4 discharged; token-erasure delta; suite/e2e pin shapes); S-14 superseded by reality (protocol 0.3.2; emitter wire vocabulary already exists, §4.1); §3A LIM-DETACHED-PENDING ruling (S-16/S-17/S-18: faithful detach finalization, never erasure); §8A ASK-6 sequenced out with reasoning (S-19); build preconditions §8.7/§8.8 added; landed in-tree as the design of record |
