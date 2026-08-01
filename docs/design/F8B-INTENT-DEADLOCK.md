# F8B — a durable connection-fate intent deadlocks the server against its own drain

**Design pass.** Author: Hermes Crumpet (liminal seat — designs, cannot compile).
Incident owner: Waffles the Terrible (Manifold spine, 2026-08-01), second
production restore failure in two days. **Every coordinate below was
re-verified at this seat at `11020d8`.** Execution goes to an executor seat;
**the code is the authority where this note and the code disagree — stop and
report, do not improvise.**

> ⚠️ **READ THIS AT HEAD.** A commit hash citing this file names the tree it
> read, not the current ruling.

**Sibling document.** This is incident 2. Incident 1 is
[`F8-MARKER-POISON.md`](F8-MARKER-POISON.md), whose design is certified and
stays intact for its own review thread. One plan, two documents, two
separable reviews (§8 states exactly what F8's green does and does not cover).
**F8 leads the plan; F8B is the second cut off the same wound.**

Versions: liminal-server 0.5.1 · liminal-protocol 0.3.2 · haematite 0.7.0.

---

## 1. THE INCIDENT, ONE PARAGRAPH

A participant's binding terminal is *pending* — it consumed its transaction
order but not its delivery sequence, and it sits in the conversation's
immutable-candidate lane awaiting a drain. A second participant on the same
conversation is then SIGKILLed. The transport reads EOF, classifies the
fate `ConnectionLost`, and the connection supervisor **durably appends and
flushes an `Open` intent, and only then calls the handler**
(`open_connection_fate` at `supervisor.rs:2296-2297`; `handle_connection_fate`
at `:2298`). The handler's terminal admission is refused with
`BindingTerminalAdmitError::Precedence` — *because the lane is occupied* —
and the server treats that refusal as corruption: it latches a **process-wide
fatal** and initiates shutdown (`:2299-2305` → `complete_connection_fate_fatal`
at `:2254` → `activate_fatal_shutdown` at `:2250`). The durable `Open` is
already on disk with no `Complete`. **Every subsequent boot then dies in
`ConnectionIncarnationAuthority::startup`, at the bare `?` on
`connection/incarnation.rs:95`, before the listener is ever bound.** And the
recovery that would discharge the intent cannot succeed, because the lane is
still occupied and **nothing at boot drains it**: the only caller of the drain
is record admission — a publish — and a publish requires a listener that boot
never reaches. The store is dead by construction, deterministically, on every
boot, forever.

**This is not F8.** F8's poison is a *marker* the floor computation cannot
satisfy; F8B's poison is a *lane occupant* the transition refuses to cross,
with **no floor input anywhere on the path** (§4). F8's fix, landed verbatim,
leaves this store dead.

## 2. THE ERROR CHAIN, END TO END — MEASURED

The operator sees one line. It is produced by six collapses:

| # | Seat | Coordinate |
|---|---|---|
| 1 | `LiveFrontierTransitionError::Precedence` raised because the candidate lane is non-empty | `claim_frontier.rs:2450-2456` (`apply_pending_binding_terminal`) / `:2380-2386` (`apply_live_transition`) |
| 2 | mapped to `LiveFrontierError::Precedence` | `live_frontier.rs:1052`, via `map_frontier_error` `:1538-1541` |
| 3 | mapped to `BindingTerminalAdmitError::Precedence` | `binding_terminal.rs:288`, via `map_live_frontier_error` `:334-337` |
| 4 | **flattened to a formatted string** in `StateError::Invariant` | `connection_fate.rs:471-473`, `"binding-terminal admission refused: {error:?}"` |
| 5 | wrapped in `ParticipantSemanticError::Internal` twice — once by `state_error`, once by the `"{fatal}: {error}"` join | `handler.rs:643-647`; `connection_fate_dispatch.rs:46` |
| 6 | surfaced as `ServerError::ParticipantIncarnation` with `phase: "connection-fate handler recovery"` and `"Open {} failed before Complete: {error}"` | `error.rs:32`; `connection/incarnation.rs:89-94` |

**Step 4 is the load-bearing loss, and it is a different loss from F8's.**
F8's tax (`binding_fate.rs:373`, `.map_err(|_| OwnerTransition)`) discards the
cause entirely. Step 4 keeps the cause *as text* — `StateError::Invariant`
carries only `message: String` (`state.rs:273-276`). Six distinct
`BindingTerminalAdmitError` variants (`binding_terminal.rs:280-293`) leave
this seam indistinguishable except by substring. The repository already
depends on that substring: **two tests assert on it**
(`tests_restore_window.rs:424`, `tests_restore_window_detached.rs:945`,
both matching `"binding-terminal admission refused"`).

That matters for the fix, not just for diagnosis: §6.3's park-not-fatal
decision must ask "was this refusal lane occupancy?", and it cannot be
allowed to answer by matching its own error text. See **R-TYPED-REFUSAL**.

## 3. THE THREE DEFECTS — MEASURED

### 3.1 The durable `Open` precedes its own validation (and it is RIGHT to)

`SupervisorInner::complete_connection_fate` (`supervisor.rs:2275-2315`):

- `:2296-2297` — `authority.open_connection_fate(..)`. This is durable **and
  flushed**: the wrapper's failure phases are `"connection-fate Open bridge"`
  / `"connection-fate Open persistence"` (`connection/incarnation.rs:284-291`)
  and the stream writes through `append_and_flush`
  (`incarnation_stream.rs:926`).
- `:2298` — `service.handle_connection_fate(intent.work_item())`. First
  validation of anything.
- `:2306` — `Complete` only on success.
- `:2299-2305` — on handler failure: `complete_connection_fate_fatal`
  (`:2254-2272`) → `latch_connection_fate_intent_incomplete` (`:2233-2252`)
  → `activate_fatal_shutdown` (`:2250`, `:2219-2223`).

**⚖️ RULING R2 — THE ORDERING STAYS. This is a WAL, not F8 §3.2's append.**
The durable `Open` exists *precisely* so that a process death between
classification and handling re-runs the fate at boot. Reordering it to
validate-before-`Open` would create a **lost-fate window**: the spine dies
between admitting the terminal and journalling the intent, and the
participant's death is then never durably recorded at all. That trades a
loud permanent refusal for silent loss of a terminal event — strictly worse,
and unobservable.

Contrast, explicitly, with **F8 §3.2**, where the reorder *is* correct: that
append (`connection_fate.rs:256`) is the *result* row, not a journal, and the
measurement that can refuse it runs at `:368` — after it. Here the sequence
at the same file is already correct: `admit_terminal` at **`:245`** precedes
`appender.append(..)` at **`:256`**, so a `Precedence` refusal at this layer
leaves **zero participant-log residue** and the part-consumed owner is
discarded (`handler.rs:395-419`, `*owner = None`, "replay durable truth next
touch").

**The residue is one layer up, in the intent journal, and the journal is
supposed to be there. The fix is not to remove the journal. The fix is to
make recovery able to make progress.**

### 3.2 A designed, tested, reachable refusal is treated as corruption

The refusal at `connection_fate.rs:468-473` is not a corruption signal. It is
a **structural boundary the repository designed, tested, and documented** —
the candidate lane admits one occupant at a time:

- `tests_restore_window.rs:378` `second_pending_terminal_cannot_join_the_candidate_lane`
  drives exactly this shape ("the still-bound peer's connection dies too, at
  the same retention cap that forced the first terminal to pend", `:402-404`),
  asserts the refusal at `:424`, and its failure message tells a *future
  builder* to "extend the drain coverage" (`:416-419`).
- `tests_restore_window_detached.rs:937-951`
  (`require_candidate_lane_refusal`) is the Died/Detached twin, same
  instruction (`:940-942`).

Both tests bless the *protocol's* refusal. **Nobody checked what the server
does with that refusal after it has already journalled a durable `Open`.** The
answer, measured, is: latch process-wide fatal and shut down
(`supervisor.rs:2299-2305`, `:2250`). A designed refusal became a
process-killer and then a permanent boot-killer.

F8 §3.1 wrote the standing clause: *"a reachable backstop firing is a bug
report, not control flow."* **Tonight proved a second backstop reachable from
production.** §6.5 enumerates which `Precedence` sources remain backstops and
which become flow.

### 3.3 Boot recovery has no drain step and no repair-vs-refuse decision

`ConnectionIncarnationAuthority::startup`
(`connection/incarnation.rs:53`), on the `RecoveryRequired` branch
(`:78-105`):

```
for intent in intents {                                    // :86
    handler.handle_connection_fate(intent.work_item())     // :88
        .map_err(|error| ServerError::ParticipantIncarnation {
            phase: "connection-fate handler recovery",     // :90
            message: format!("Open {} failed before Complete: {error}", ..),
        })?;                                               // :95  ← bare ?
    block_on(recovery.complete(intent.open_sequence))       // :96
    ...
}
let resumed = block_on(recovery.finish_startup())           // :106
```

**One bad intent aborts the whole loop.** `recovery.complete` (`:96`) and
`finish_startup` (`:106`) are unreachable; `startup` returns `Err`;
`SupervisorInner::new` propagates at `supervisor.rs:1003-1009`; the listener
is never bound. There is no repair branch, no skip branch, no quarantine
branch — **there is one branch, and it propagates.** Deterministic on every
boot.

**And the deadlock is closed, MEASURED.** The candidate lane is emptied by
exactly one function, `persist_drain_first`
(`ops_terminal_drain.rs:71-86`), and it has exactly one production caller:
`ops_frontier.rs:142`, inside `apply_record_admission_with_impact` — the
`RecordAdmissionDecision::DrainFirst` arm of **record admission**, i.e. a
publish (grep for `persist_drain_first` over `crates/` returns three hits:
the definition, that call, and a census-test string literal).

```
drain needs a publish
  → publish needs a listener
    → listener needs incarnation startup to return Ok
      → startup needs the recovery of Open 82 to succeed
        → recovery needs terminal admission
          → admission needs the lane empty
            → the lane needs a drain
```

Permanent by construction, at every boot, with no in-band remedy.

**AMENDED AT THE FOUNDATION LEG'S LANDING — the deadlock is NOT boot-only.
RED-A (`9f549b9`, rc 101) measured it live at the handler seat with no
restart involved:** a lane-occupancy refusal latched the process-wide
participant fatal at the dispatch seat (`connection_fate_dispatch.rs`, the
pre-fix `:37-38`), and that fatal then refused **the draining publish
itself** —

```
Error: "dispatch did not respond: Fatal(Semantic(ServiceFatal(
    ConnectionFateIntentIncomplete { open_sequence: 101, ... })))"
```

— so R-PARK-DRAIN's sole producer was unreachable the moment the condition
it exists to reverse first occurred. The live half of the cycle needed no
reboot to close; §6.4's convergence assertion (c) was unsatisfiable by
construction while the latch stood. The foundation leg's dispatch-seat
change (this branch: lane-occupancy `Precedence` returns typed without
latching, per §6.5's never-process-fatal ruling applied at that seat)
removes the live half; the boot half above stands until R-BOOT-DRAIN lands.

### 3.4 The tax: partial application has no defined semantics

`apply_connection_fate_with_impacts`
(`connection_fate_dispatch.rs:12-50`) iterates
`work_item.tracked_conversations` (`:20`) and **returns on the first failure**
(`:48`). Conversations earlier in the iteration have already durably
committed their rows; there is no rollback and no record of how far the fate
got. A fate over three conversations refused at the third leaves two
committed, and the recovery at `connection/incarnation.rs:88` re-runs **the
whole work item**. Whether that re-run converges or double-applies is
currently **undefined by the code and unasserted by any test**. §6.4 names it.

## 4. WHY F8'S FIX DOES NOT COVER THIS — MEASURED

Both incidents are "boot refuses forever". They fail at **different boot
stages, on different mechanisms, with different error types.**

| | Incident 1 (F8) | Incident 2 (F8B) |
|---|---|---|
| Failing stage | handler construction | connection-incarnation startup |
| Path | `services.rs:385-393` → `handler.rs:137` `restore_all_conversations` → `:250` `replay_and_repair` → `:462` `repair_pending_specific_fates` | `supervisor.rs:1003` → `connection/incarnation.rs:95` |
| Surfaced as | `ServerError::ParticipantStartupRestore` (`error.rs:70-74`) | `ServerError::ParticipantIncarnation` (`error.rs:32`) |
| Mechanism | floor computed blind to retained markers | candidate lane occupied; transition refuses to cross it |
| Floor input on the refusing path? | yes — that *is* the defect | **no — none, anywhere** |

The last row is the whole argument. The guard that fires in incident 2 is
`claim_frontier.rs:2450-2456`:

```rust
if !self.sequence.immutable_candidates.is_empty()
    || self.sequence.recovery.is_some()
    || !self.order.immutable_candidates.is_empty()
    || self.order.recovery.is_some()
{
    return Err(Box::new((self, LiveFrontierTransitionError::Precedence)));
}
```

Four boolean occupancy tests. **No floor, no marker sequence, no cursor.**
F8 §3.1's marker cap — clamping the measured floor to the minimum retained
marker record — is **provably inert on this path**: there is nothing here for
a floor to be compared against.

Nor does F8's repair reach the lane. `repair_pending_specific_fates`
(`binding_fate_completion.rs:296-320`) touches exactly two maps —
`prepared_ordinary_finalizers` (`:300-307`) and `pending_specific_fates`
(`:308-319`) — and runs to completion during handler construction, **before**
incarnation startup even begins. It never reads `immutable_candidates` and
never reads `recovery`.

**Conclusion: F8's fix landed verbatim leaves incident-2's store dead on
every boot. F8's acceptance green is not evidence about incident 2.** §8
carries this as a mandatory amendment to F8 §5.

## 5. THE LANE, MEASURED — WHAT CAN OCCUPY IT, WHAT CAN CLEAR IT

The remedies in §6 are only sound if the lane's reachable shapes are known.
They are, and the answer is narrower than expected.

**5.1 What the lane can hold.** `ImmutableSequenceCandidate`
(`claim_frontier.rs:553-565`) has two variants: `BindingTerminal` (`:555-562`)
and `Marker` (`:564`). Three occupancy classes bear on the refusal, and one
of them turns out to be unreachable in combination:

1. **N marker candidates.** Minted in bulk during ordinary record projection
   (`claim_frontier.rs:2902-2907`, `extend`).
2. **Exactly one pending binding terminal.** Created when the retained-record
   cap cannot admit the terminal's row (`binding_terminal.rs:187-192`) **and**
   hard observer progress is behind the candidate's delivery sequence
   (`:212-217`); if the cap is full and observer progress is *not* behind, the
   admission refuses `RetainedRecordLimit` outright (`:231`) and nothing
   pends.
3. **An armed fenced-attach recovery block** (`sequence.recovery` /
   `order.recovery`, `claim_frontier.rs:891/901`, `:1067/1076`).

**MIXED LANES ARE UNREACHABLE — MEASURED.** A marker cannot be minted while
any candidate occupies the lane: `preflight_ordinary_sequence_owners`
refuses `OrdinaryProjectionError::SequenceRelocation` when
`immutable_candidates` is non-empty (`claim_frontier.rs:3956-3962`), and it
gates every ordinary projection (`:2875-2877`). Symmetrically, a binding
terminal cannot pend while any candidate is present — that is the guard at
`:2450-2456`, and it is exactly what the two blessing tests in §3.2 assert.
**So a lane holds either N markers, or exactly one binding terminal, never
both.** The design below relies on this and the builder must re-prove it.

**5.2 What can clear each occupant.**

- **Marker head** — `drain_next_marker` (`marker_drain.rs:202-208`) →
  `drain_next_marker_core` (`claim_frontier.rs:3354`). It checks candidate
  presence (`:3357`), head flavor (`:3360-3362`), sequence adjacency
  (`:3369`), and order allocation (`:3372`). **It does not read `recovery`.**
  It removes exactly the head (`:3399`), so N markers need N drains.
- **Binding-terminal head** — `drain_pending_terminal`
  (`live_frontier.rs:385-457`) → `drain_first_binding_terminal`
  (`claim_frontier.rs:3454`) → `validate_first_terminal_candidate`
  (`:3516-3531`), which refuses `Precedence` when **any recovery block is
  armed** (`:3524-3526`) or when the lane does not hold **exactly one**
  candidate (`:3527`, `let [first] = ..`).
- **Recovery block** — consumed only by `apply_live_fenced_attach`
  (`claim_frontier.rs:2507-2545`), which requires a *live* fenced attach by
  the detached participant. Nothing else clears it.

**5.3 The two answers this forces (R7's open question, answered at the
bytes).**

**(i) Observer-progress-permanently-behind does NOT block the drain — NO.**
`drain_pending_terminal` takes no observer-progress input at all, and its
own doc states the retained-row cap is *deliberately* not re-checked
(`live_frontier.rs:369-378`: "the terminal pended exactly because the cap
could not admit its row at fate time … the drain honors the deferred
reservation even while the suffix rests at its cap"). The observer-progress
test at `binding_terminal.rs:212` gates **admission**, not the drain. So the
condition that *creates* the occupant cannot *preserve* it. A boot drain over
a terminal-occupied lane is unconditionally available.

**(ii) An armed recovery block DOES outlive boot drain — YES.**
`validate_first_terminal_candidate:3524-3526` refuses the terminal drain
outright when `recovery.is_some()`, and the only consumer of a recovery block
is a live fenced attach (`:2507`) — which requires the listener boot has not
reached. **A store whose lane holds a pending binding terminal *and* an armed
recovery block is not repairable by the boot drain in §6.2.** That shape is
carried honestly in §9, with the verdict it must produce, not papered over.

## 6. THE FIX — FIVE NAMED REQUIREMENTS

### 6.1 R2 restated as a build constraint

**⛔ Do not reorder `supervisor.rs:2296-2298`.** The durable `Open` is the
retry token and must stay ahead of the handler. Any patch that makes the
`Open` conditional on successful handling is a **STOP back to design** — it is
the lost-fate trade named in §3.1. (F8 §3.2's reorder is a different append at
a different layer; do not generalise one to the other.)

### 6.2 R-BOOT-DRAIN — boot recovery drains the lane before it replays intents

**Requirement.** Before any retained connection-fate `Open` is replayed, boot
recovery drains each restored conversation's immutable-candidate lane to
empty, using the existing machinery (`persist_drain_first`,
`ops_terminal_drain.rs:71`), which today has no boot caller.

**Placement — RULED.** In `restore_all_conversations`
(`handler.rs:237-262`), immediately after the per-conversation
`replay_and_repair` returns (`:250`) and before the owner is installed at
`:257`. **Not inside `replay_and_repair`**, even though F8's repair lives
there and the appender is already in scope: `replay_and_repair` has **four
live callers** (`handler.rs:350`, `:373`, `:401`;
`handler_observer.rs:170`) besides the boot path, so draining there changes
live behaviour under the guise of a boot fix. The boot drain must be
boot-only.

This lands the drain **after** handler construction (F8's repair included)
and **before** `connection/incarnation.rs:86`'s replay loop, because handler
construction (`services.rs:385-393`, `handler.rs:137`) completes before
`ConnectionIncarnationAuthority::startup` is called at `supervisor.rs:1003`.

**Wiring — named, per R3(a).**

- *Appender*: construct `LogAppender { log, registry: &self.registry,
  conversation_id }` exactly as `replay_and_repair` does at
  `handler.rs:456-460`; the `OperationLog` is already in scope in
  `restore_all_conversations` at `:249`.
- *Impact*: a fresh `DispatchImpactAccumulator::new()`
  (`dispatch_impact.rs:118`, const, no arguments) per drain, **discarded**.
  Boot has no connection to receive an impact — the listener is not bound
  until after `SupervisorInner::new` returns.
- *Observer progress*: the drain records new projections
  (`ops_terminal_drain.rs:115-118`, `:120-126`, `:356`), so the boot drain
  must re-run the same reconciliation `replay_and_repair` performs at
  `handler.rs:464-477` **after** draining. Reusing the drained owner without
  it leaves the handler's observer state behind its own durable log.
  ⚠️ **This is the sharpest wiring edge in the requirement; a builder who
  finds the reconciliation not re-runnable from this seat stops and reports.**
  *Review round 1 CLOSED it buildable as written:*
  `record_observer_progress_projection` (`state.rs:344-362`, called from
  `ops_terminal_drain.rs:356`) records into the exact state
  `take_observer_progress_witnesses` surrenders, and
  `reconcile_observer_progress` (`handler_observer_reconcile.rs:34`) is a
  handler method reachable from `restore_all_conversations`' scope at
  `:250-257`. One residual for the builder's first read: the witness state
  has `begin_source`/`end_source` visit bracketing (`state.rs:386`, `:392`)
  — whether boot-drain records must sit inside a bracketed visit is a
  one-read check, and the STOP above covers it.
- *Loop*: drain the **head** repeatedly until
  `authority.frontier()` (`state.rs:409`) reports
  `.frontiers().sequence().immutable_candidates()` empty — N markers need N
  drains (`claim_frontier.rs:3399`).

**R-BOOT-VERDICT (companion).** Every boot drain attempt ends in a **named
verdict**, never a bare `?`: `drained` (lane emptied) · `already-empty` ·
`refused-recovery-armed` (§5.3(ii)) · `refused-shape` (any other drain
refusal). The first two proceed to replay. The last two **refuse the boot
loudly, naming the conversation, the candidate shape, and this document** —
they are the honest "we cannot repair this" answer, and they must be
distinguishable in a log from the deadlock they replace. A silent skip is
forbidden.

**Red-first tests.**
1. A restored conversation whose lane holds one pending binding terminal and
   whose durable incarnation stream holds an unmatched `Open`: boot reaches
   listening. *Fails today* — boot returns
   `ParticipantIncarnation{phase: "connection-fate handler recovery"}`.
2. A restored conversation whose lane holds two marker candidates: boot
   empties the lane in two drains and reaches listening. *Fails today* — no
   boot caller exists.
3. A restored conversation whose lane holds a pending terminal **and** an
   armed recovery block: boot refuses with the `refused-recovery-armed`
   verdict naming the conversation — **not** with today's chain, and **not**
   silently. *Fails today* — the verdict does not exist.

### 6.3 R-PARK — live lane-occupancy `Precedence` parks, it does not fatal

**Requirement.** When `handle_connection_fate` fails because the candidate
lane is occupied, the connection supervisor **parks** the intent instead of
latching a process-wide fatal. The durable `Open` **is** the retry token —
R2 already ruled it stays — so parking costs no new durable state. The
connection is torn down, the spine stays up, and the intent is discharged
when the lane drains.

Concretely, at `supervisor.rs:2298-2305`: the fatal branch splits. Genuine
corruption keeps `complete_connection_fate_fatal` (`:2254`); lane occupancy
returns a **connection-scoped** error and leaves the `Open` unmatched for
recovery. The classes are separated by **type**, per R-TYPED-REFUSAL below.

**R-TYPED-REFUSAL (blocking prerequisite).** The park/fatal decision must be
made on a **typed** refusal, never on a substring of a formatted message.
Today the cause dies at `connection_fate.rs:471-473`, which formats
`BindingTerminalAdmitError` into `StateError::Invariant{message: String}`
(`state.rs:273-276`); six variants (`binding_terminal.rs:280-293`) become one
string, and the repository already string-matches it in two tests
(`tests_restore_window.rs:424`,
`tests_restore_window_detached.rs:945`). **A park decision taken on
`error.contains("binding-terminal admission refused")` is not a fix; it is the
same defect wearing a remedy's clothes, and it will be refused in review.**
The minimum shape: a dedicated typed carrier for the refusal that survives
`connection_fate.rs` → `handler.rs:643-647` →
`connection_fate_dispatch.rs:46` → `connection/incarnation.rs:89-94`. This is
F8 §3.3's requirement (`OwnerTransition(LiveFrontierError)`) applied to a
**second, independent** seam — same disease, different site, and F8's patch
does not reach it.

**Companion obligation (review round 1, required):** the two existing
substring assertions — `tests_restore_window.rs:424` and
`tests_restore_window_detached.rs:945` — convert to **typed** assertions **in
the same change** as the typed carrier. Left as substrings they stay
load-bearing in the suite and the seam regrows with the tests blessing it: a
check for the loud variant covering nothing of the silent one.

**R-PARK-LOUD (named requirement — a silent park is fate loss on a delay).**
A park is loud **at park time**, not at drain time and not by inference:

- **A log line at the parking site**, at `warn` or above, carrying
  `open_sequence`, `conversation_id`, `connection_incarnation`, the typed
  refusal, and the word "parked". The precedent and the seat both exist:
  `supervisor.rs:2261` already emits
  `tracing::error!(open_sequence, phase, %error, "durable connection-fate
  intent is incomplete")` from the adjacent branch. **Note for the builder:
  there is no `tracing::warn!`/`tracing::error!` anywhere under
  `server/participant/production/` — the instrument belongs at the supervisor
  seat, where the intent and its sequence are in scope.**
- **A monotonic counter of currently-parked intents**, decremented on
  discharge, readable without a debugger. In-repo precedent for the shape:
  `Arc<AtomicU64>` with an accessor and a test that asserts it —
  `listener.rs:34`, `:108-110`, `:252` (`shed_count.fetch_add`), surfaced
  again at `health/endpoint.rs:126-127` and asserted at `:865`. A parked-fate
  count belongs in the same family. ⚠️ Note honestly: that is a *private*
  accessor plus a health-endpoint read, **not** a public metrics surface —
  the requirement is "a log reader hits it without hunting", and the counter
  is the second half of that, not a substitute for the log line.

*Red-first test:* park one intent behind an occupied lane; assert **both**
that the counter reads 1 and that the emitted event carries the
`open_sequence` and the typed refusal. *Fails today* — neither exists, and the
process is dead before anything could read them.

**R-PARK-DRAIN (named requirement — the reversing producer, named and cited).**
The thing that parks names the thing that un-parks. **The producer is the
drain-completion site: `ops_frontier.rs:142`**, the sole production caller of
`persist_drain_first`, on the `RecordAdmissionDecision::DrainFirst` arm of
record admission. Its schedule is **the next successful publish on that
conversation** — no timer, no poll (LAW-1; and see
`docs/design/LAW1-POLLING-RETIREMENT.md`). The exact reversing observation is
available in-place at that seat: immediately after `:142` returns `Ok`, read
`authority.frontier()` (`state.rs:409`) →
`.frontiers().sequence().immutable_candidates()`; **empty ⇒ the lane just
became drainable, replay the parked intents for this conversation.** For a
binding-terminal occupant the lane holds exactly one candidate (§5.1), so one
drain empties it and the observation fires on the first publish after the
park.

*Red-first test:* park an intent behind an occupied lane, publish once,
assert the parked intent is discharged (`Complete` durable, counter back to
0) **without any restart and without any timer advancing**. *Fails today* —
the park does not exist and the process is dead.

**⚠️ THE INTERIM, STATED HONESTLY.** If the executor finds the event wiring
disproportionate for this cut, the fallback permitted by R3(b) is
**boot-only remedy §6.2 plus the live fatal downgraded to a
connection-scoped error** — spine stays up, fate parks, next publish drains,
parked intent replayed at the following restart. **In that interim a parked
fate is invisible to consumers until the next publish or restart.**
R-PARK-LOUD is **not** waivable in the interim: it is precisely what makes
the interim honest instead of silent, and it is the operator's only signal
that a consumer is looking at a stale liveness view. R-PARK-DRAIN's producer
must still be named in the code comment at the parking site even if the
automatic replay is deferred.

**Dispatch rule (review round 1):** the build dispatch brief DECLARES which
shape it builds — interim or full — before the leg starts. The full shape's
conversation→parked-opens lookup (parked intents live at the supervisor
layer; the drain observation fires at the conversation layer) is a
**design question that returns to the design seat** — a builder does not
mint a supervisor-layer index on their own authority.

### 6.4 R-IDEMPOTENT-PREFIX — re-running a partly-applied fate converges

**Requirement.** `apply_connection_fate_with_impacts`
(`connection_fate_dispatch.rs:12-50`) must be **idempotent in its success
prefix**: re-running a work item whose earlier conversations already committed
their fate rows is a **no-op for those conversations**, not an error and not a
double-apply. Without this, both §6.2 and §6.3 are unsafe — boot replay and
park-replay both re-run whole work items over partly-applied state.

*Red-first test:* a connection fate over three tracked conversations, refused
at the third; re-run the identical work item and assert (a) conversations 1
and 2 gain **no** additional durable rows, (b) conversation 3 completes, (c)
the re-run returns `Ok`. *Fails today* — the semantics are undefined and
unasserted; `:48` returns on first failure with no rollback and no progress
marker.

### 6.5 R3 boundary — which `Precedence` sources stay backstops

F8 §3.1's clause stands: *a reachable backstop firing is a bug report, not
control flow.* **The population, measured (review round 1): exactly TEN raise
sites of `LiveFrontierTransitionError::Precedence` in `liminal-protocol`,
every one classified below — none implicit.** After this cut they partition
as follows.

**Becomes FLOW (expected, handled, never process-fatal):**

- `apply_pending_binding_terminal` `Precedence` from a **non-empty
  immutable-candidate lane** (`claim_frontier.rs:2455`, guard `:2450-2456`)
  → parked (§6.3), drained at boot (§6.2). *This is the one tonight proved
  reachable.*
- `apply_live_transition` `Precedence` from the same occupancy (`:2385`,
  guard `:2380-2386`) reaching record admission → already flow today: it is
  `RecordAdmissionDecision::DrainFirst` (`ops_frontier.rs:136-142`).

**Becomes a NAMED REFUSAL (loud, honest, neither flow nor bug report):**

- `validate_first_terminal_candidate`'s **armed-recovery** refusal
  (`claim_frontier.rs:3525`) — at boot this is exactly R-BOOT-VERDICT's
  `refused-recovery-armed` arm (§6.2, §9.2): the store refuses to boot
  naming the reason. A firing on the *live* drain path (through
  `ops_frontier.rs:142`) is report-worthy — a publish-path drain should
  never meet an armed recovery block outside the fenced-attach window.

**Stays a BACKSTOP (a firing is a bug report):**

- `apply_live_fenced_attach`'s two **missing-recovery-block** raises
  (`claim_frontier.rs:2523` sequence, `:2526` order) — a fenced attach
  without its own reserved blocks is a genuine authority defect.
- `apply_live_fenced_attach`'s third raise (`:2542`): the lane is
  **occupied and not the pending-fenced-terminal shape** the attach is
  entitled to finalize — distinct semantics from the two above (the blocks
  exist; the lane's occupant is wrong for this attach). Not the boot-drain's
  problem: boot performs no fenced attach (§9.2).
- `validate_first_terminal_candidate`'s **non-singleton-lane** refusal
  (`claim_frontier.rs:3528`) — §5.1 shows mixed and multi lanes are
  unreachable by construction; a firing means that construction broke, and
  the two tests in §3.2 exist to catch exactly that. **Do not "fix" this by
  widening the drain**; report it.
- `validate_first_terminal_candidate`'s third raise (`:3536`): the head
  candidate destructures as **not a binding terminal** (i.e. a `Marker`
  reached the terminal-drain validator) — the mirror of the mis-selection
  reasoning above, same rule: report, never widen.
- F8 §3.1's two marker-crossing raises
  (`claim_frontier/binding_fate_transition.rs:43`, `:91`) — unchanged, and
  untouched by this document.

*Population-purity clause:* `drain_next_marker_core`'s refusal at
`claim_frontier.rs:3360-3362` raises **`BindingTerminalFirst`, not
`Precedence`** — it is a sibling mis-selection backstop but sits OUTSIDE this
population; named here so the ten-site count reads pure.

## 7. PROTOCOL SURFACE — SERVER-ONLY (R6)

**F8B needs no `liminal-protocol` change.** Measured, requirement by
requirement:

- **§6.2 R-BOOT-DRAIN** wires an existing protocol-owned drain
  (`persist_drain_first`, `ops_terminal_drain.rs:71`;
  `drain_pending_terminal`, `live_frontier.rs:385`, already `pub`;
  `drain_next_marker`, `marker_drain.rs:202`, already `pub`) from a new
  **server** caller in `handler.rs`. No protocol signature moves.
- **§6.3 R-PARK / R-PARK-LOUD / R-PARK-DRAIN** are entirely inside
  `liminal-server` (`supervisor.rs`, `handler.rs`, `ops_frontier.rs`).
- **§6.4 R-IDEMPOTENT-PREFIX** is inside
  `connection_fate_dispatch.rs`.
- **R-TYPED-REFUSAL** is the one to watch. The *source* type
  (`BindingTerminalAdmitError`) is already public and already carries
  `Precedence` (`binding_terminal.rs:280-293`) — nothing there needs to
  change. The loss is at the **server's own** `StateError`
  (`state.rs:273-276`), a server type. **If the executor can carry the cause
  by adding a `StateError` variant, F8B stays protocol-free.** If the
  executor finds it must change a protocol type instead, that is a **STOP and
  report**, because it changes the release story below.

**Consequence, and it is the useful one: F8B is decoupled from the 0.4.0
cut.** F8's `OwnerTransition(LiveFrontierError)` payload forces
liminal-protocol 0.3.2 → 0.4.0 and drags the `#[non_exhaustive]` gate with it
(F8 §4.1-§4.2). F8B, as designed, is a **liminal-server patch-class
behaviour fix** and can land on either side of that boundary. Server bump
class is decided by the same leak check F8 §4.3 names — **do not inherit this
note's suggestion as the decision.**

## 8. VERIFICATION — THREE FIXTURES, THREE DEFECT LEGS

**No aggregate green.** Each fixture names the leg it proves; a green that
cannot say which leg it proved has proved nothing.

**⚠️ This section is a release gate for downstream consumers (manifold-node
first). Keep the bullets exact.**

1. **Incident-1 store — marker-poison leg.**
   `apps/manifold/.manifold-backup-20260731-spine-poison/`. **F8's own
   acceptance, unchanged** (F8 §5: old binary fails `ParticipantStartupRestore`
   identically; fixed binary reaches listen with P1's marker still retained).
   Proves: F8 §3.1 floor clamp + §3.2 append ordering. **Proves nothing about
   F8B** — different stage, different mechanism (§4).

2. **Incident-2 store — intent-deadlock leg.**
   `apps/manifold/.manifold-backup-20260801-intent82`.
   - *Negative control:* old binary (0.5.1) on a copy fails with the exact
     quoted chain — `ServerError::ParticipantIncarnation`, `phase:
     "connection-fate handler recovery"`, message containing `Open 82 failed
     before Complete` and `binding-terminal admission refused` — on **every**
     boot. This proves the copy still carries the deadlock.
   - *The fix:* fixed binary on the same copy **boots to listening**, the
     lane drained, `Open 82` matched by a durable `Complete`.
   - Proves: **R-BOOT-DRAIN** (the lane emptied without a publish) **and**
     **R-PARK / R-TYPED-REFUSAL** (the admission that refused is retried
     rather than treated as corruption).
   - ⚠️ **The store is not in this repository** — `git` at `11020d8` has no
     `apps/manifold/.manifold-backup-*` path. The executor takes the fixture
     from Waffles and confirms its identity before running; a fixture that
     cannot be produced is a **STOP**, not a skip.

3. **Synthetic red-first repro — trigger end-to-end.** Enroll a participant,
   drive the conversation to the pending-terminal shape (retained-record cap
   full, hard observer progress behind — `binding_terminal.rs:187-192`,
   `:212-217`), then **SIGKILL a second participant's process by exact PID**
   mid-session (trigger precision per Waffles: the real kill *was* SIGKILL by
   exact PID; the transport path is EOF → `ConnectionFateClass::ConnectionLost`
   at `process.rs:209`, `:325`, `:351` and `websocket/process.rs:340`, `:360`
   → `process.rs:440-442` → `supervisor.rs:2275` → Died at
   `connection_fate.rs:505-509`). Restart the broker. **Old binary refuses;
   new binary listens.** Proves: the whole chain from real trigger to real
   recovery, without depending on a preserved store.

**Plus the per-requirement red-first units** listed inline in §6.2, §6.3, and
§6.4 — each must be committed **failing** against the current binary before
its fix lands.

**⛔ RULED: refusal-instead-of-restore at fixture 2 or 3 is a STOP back to
design.** It would mean the boot drain does not reach the lane the way this
note believes, and the design comes back here rather than being widened in
place.

**Mandatory amendment to F8.** F8 §5 gains a note stating that F8's green does
not cover incident 2 and citing this document. That amendment is applied in
the same change as this file.

## 9. HONESTY — WHAT STAYS UNFIXED

1. **A parked fate is invisible to consumers until the next publish or
   restart.** R-PARK-LOUD makes it visible to the *operator* at park time; it
   does not make it visible to a *subscriber*, who continues to see a
   participant that the server privately knows is dead. In the interim shape
   of §6.3 this window is unbounded on a quiet conversation. **This is the
   design's largest remaining hole and it is deliberate** — closing it means
   surfacing a not-yet-durable terminal, which is a different (and worse)
   correctness trade.

2. **A lane occupied by a pending terminal *and* an armed recovery block is
   not repairable by the boot drain.** Measured, §5.3(ii):
   `validate_first_terminal_candidate:3524-3526` refuses the drain while
   `recovery.is_some()`, and the only consumer of a recovery block is a live
   fenced attach (`claim_frontier.rs:2507`) that boot cannot perform.
   R-BOOT-VERDICT's `refused-recovery-armed` verdict is the *honest* answer,
   not a repair: such a store still refuses to boot, but it refuses **naming
   the reason and this document** instead of dying six collapses downstream.
   Whether that shape is reachable in production is **not established** —
   this note establishes only that if it occurs, the remedy does not cover
   it. Establishing reachability is out of scope here and should be a
   separate question.

3. **The observer-progress leg of the same question is CLOSED, negative.**
   Observer-progress-permanently-behind cannot preserve a lane occupant: the
   drain reads no observer progress and deliberately does not re-check the
   retained cap (`live_frontier.rs:369-378`). Recorded so a future reader
   does not re-open it.

4. **The fatal-shutdown path is retained, and the discrimination is by type,
   not by judgement.** `complete_connection_fate_fatal`
   (`supervisor.rs:2254`) stays for genuinely-corrupt classes — §6.5's
   backstop list. The discriminator is **R-TYPED-REFUSAL**, and the design is
   only as good as that type: a builder who cannot carry the type and falls
   back to string matching has built a different, worse design. Named here so
   the review can check for it.

5. **`liminal-protocol` is unchanged by choice, not by proof of impossibility
   (§7).** If R-TYPED-REFUSAL turns out to require a protocol type, F8B stops
   being decoupled from the 0.4.0 cut and the release story in §7 is wrong.

6. **The incident-2 store is not in this repository** (§8, fixture 2). Every
   claim in §1-§4 about *this* incident's shape is derived from the code
   paths, not from re-reading its bytes at this seat. The code coordinates
   are measured at `11020d8`; the store's contents are Waffles' evidence.

7. **§3.4's partial-application defect is named and required (§6.4) but its
   blast radius is not measured.** How many production fates span more than
   one tracked conversation is unknown at this seat.

## 10. WHAT THIS NOTE IS NOT

Not compiled, not tested, written at a seat that can do neither. Every
coordinate is MEASURED at `11020d8` and will drift; the builder re-anchors
before building. This document is the mechanism for **incident 2 only** —
[`F8-MARKER-POISON.md`](F8-MARKER-POISON.md) owns incident 1, and the two
are reviewed on separate threads even though they land as one plan.
Downstream: manifold-node is the willing early adopter and the live victim of
both incidents; its re-pin sweep is Waffles' to route.
