# ProcessKilled emitter build — mandatory reality survey: STOP (S-5 contract fork)

Lane: `feat/process-killed-emitter` off landed main `6d09bae`.
Status: **STOPPED before any code** per the dispatch's survey STOP-condition.
No adapter, no red pin, no test, and no production line was written; this
survey record is the lane's sole artifact. The design of record is
`docs/design/PENDING-DRAIN-EMITTER.md` (r4); the seam row is
`docs/design/W1B-FATE-SOURCES.md:1132`.

## 1. Finding A — S-5 cannot be implemented as written (the fork)

S-5 (ruled, torn 2026-07-23): "the liminal embedding's adapter claims beamr's
single exit-event subscription; all other liminal-side consumers multiplex
behind the adapter." At `6d09bae` that subscription is **already claimed, at
supervisor construction, by the W4 leg-1 reclamation reactor**, on the same —
and only — production beamr scheduler in liminal-server:

- One production scheduler exists: `SupervisorInner::new` constructs the
  connection scheduler
  (`crates/liminal-server/src/server/connection/supervisor.rs:1017-1027`,
  `Scheduler::with_services`, `CONNECTION_SCHEDULER_THREADS`). No other
  production `Scheduler` construction exists in liminal-server. (The `liminal`
  crate's channel/conversation subsystems construct their own schedulers —
  `crates/liminal/src/channel/supervisor.rs:152`,
  `crates/liminal/src/conversation/actor.rs:289` — but those host channel and
  conversation actors, never participant bindings.)
- The subscription is claimed immediately at construction:
  `match scheduler.subscribe_exit_events()` (`supervisor.rs:1055`), moved into
  the detached `liminal-connection-reclaim` thread (`supervisor.rs:1063-1067`)
  running `run_reclaim_reactor` (`supervisor.rs:949-972`; doc `:929-948`:
  "It blocks on beamr's sole exit-event subscription") for the scheduler's
  lifetime. The `None` arm (`supervisor.rs:1072-1079`) logs that
  external-termination reclamation would then have **no TOLD exit source** —
  the codebase already treats losing this subscription as a degradation.
- beamr allows exactly one subscription per scheduler lifetime; later calls
  return `None` — re-corroborated at the consumed pin (§3 below). A second
  subscription for the adapter is impossible; the dispatch forbids improvising
  one.
- The reactor also **consumes and discards every exit outcome**: it is the
  self-described "sole drainer" (`supervisor.rs:2661-2662`), calling
  `drop(scheduler.take_exit_outcome(pid))` per delivered exit
  (`supervisor.rs:2670`). So even behind the current reactor,
  retained-until-consumed cannot serve the adapter: the exact
  `(ExitReason, OwnedTerm)` pair the adapter's S-15 filter must consume is
  already taken and dropped.
- All production beamr processes on that scheduler are connection hosts:
  `spawn_native` at `supervisor.rs:1132` (TCP) and `supervisor.rs:1182`
  (LP-WS-TRANSPORT sibling). Participant sessions ride connection processes
  (`crates/liminal-server/src/server/connection/process.rs:919`,
  `state.participant_session`). There is no scheduler on which "the
  participant processes run" that is not this one.

Conclusion: the ruled S-5 spelling — "the adapter claims it" — is
unimplementable at `6d09bae` without either subsuming or restructuring the
existing reactor. That is a contract fork on a torn socket; it is not the
builder's to re-rule.

### Finding A′ — the substrate gap (second prong, stated honestly)

The adapter's S-6 map is inserted "at spawn/attach" (design §4.1/§4.4). At
`6d09bae` **no production component spawns a beamr process correlated to a
participant binding**. The only mappable pid is the connection host process —
and a forced kill of a connection host drops its socket
(`ConnectionProcess::Drop` releases the stream, `process.rs:882-906`) and is
already classed as connection-fate teardown (`ConnectionFateClass` carries
`CleanDisconnect`/`ServerShutdown`/`ConnectionLost`/`ProtocolError`,
`crates/liminal-server/src/server/participant/dispatch.rs:102-111`). Mapping
connection pids into the emitter would make one death produce two competing
fates (ProcessKilled via the adapter, ConnectionLost via connection fate) for
the same binding — a hazard the design does not adjudicate, because its value
claim is "actor-granular death in a live VM, where the socket never drops"
(§2, S-12): a participant-actor substrate that does not exist in this tree
yet. Which pids the production map would ever contain — and who calls insert —
is unresolved and belongs with the fork ruling.

### One adjacent wrinkle both fork options must reconcile

The reactor's current `Lagged` recovery (`supervisor.rs:962-968` →
`reap_crashed`, `supervisor.rs:2821-2860`) uses non-consuming
`peek_exit_reason` (`supervisor.rs:2849`) and never takes outcomes, while the
adapter contract (S-7) requires `take_exit_outcome` over all tracked pids.
Under a sole-drainer regime the merged component must define take-vs-peek
ownership so no outcome is leaked (retained forever) or double-consumed.

## 2. Honest options (for the tear seat — no build proceeds until ruled)

1. **The adapter IS an extension of the reactor.** One component, one
   subscription, claimed where it is claimed today. The reclaim loop grows the
   S-6 pid→binding map, the S-15 total filter at every consumed-exit site, and
   the emitter append (routed into participant production via a service seam
   parallel to `handle_connection_fate`). Requires the tear seat to re-spell
   S-5 ("reactor and adapter are one exit-fate component"); keeps LAW-1/TOLD
   and single-subscription intact; must unify outcome consumption (§1
   wrinkle) and preserve W4 reclamation semantics byte-for-byte.
2. **The reactor multiplexes BEHIND a new adapter.** The adapter claims the
   subscription (moving the claim out of `SupervisorInner::new`); reclamation
   becomes the first consumer multiplexed behind it. Implements S-5's letter,
   but **builds the multiplex facility that §5(c)/§8.4 explicitly exclude
   from this build** ("interface commitment only, not built here") — a scope
   amendment the tear seat must grant; same outcome-consumption unification
   owed; W4 reclamation delivery guarantees (weak handles, drop-synchronous
   release, one `remove()` funnel) must be preserved exactly.
3. **A separate participant-actor scheduler** — named only to dismiss: no
   production participant processes exist at `6d09bae` (Finding A′); inventing
   that substrate here would be a silent architecture change, forbidden by the
   dispatch.

Either viable option additionally needs the Finding A′ ruling: what the
production pid→binding-incarnation map contains before a participant-actor
substrate exists, and how a connection-host kill avoids double-fating.

## 3. §8.7 re-corroboration at the pin liminal actually consumes

Pin: liminal `6d09bae` consumes **beamr 0.16.1** — workspace `Cargo.toml:32`
(`beamr = { version = "0.16.1", features = ["readiness"] }`) and `Cargo.lock`
(`beamr 0.16.1`, registry, checksum `e8d90748…1099`). Verified at the exact
registry bytes cargo compiles:
`~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/beamr-0.16.1`.
(The estate's beamr checkout is ahead — 0.16.2 lineage; the early-warning
pass at `293d368` is corroborating but NOT the consumed surface. These are the
consumed bytes.)

**Wake obligation (tear seat, 2026-07-23, recorded at the hold):** the pin
above is the consumed 0.16.1 registry bytes. beamr 0.16.2 (the memory-safety
patch) queue-jumped after this pin was taken — when the lane wakes, verify
whether the workspace pin should advance to 0.16.2 and record which it is
before building.

All four §4.2 facts hold at 0.16.1, at the same line numbers the design cites:

1. **Single subscription**: `Scheduler::subscribe_exit_events`
   (`src/scheduler/execution.rs:213`; doc `:198-211` — "Exactly one
   subscription can be created for a scheduler's lifetime; later calls return
   `None`"); set-once sender (`src/scheduler/exit_events.rs:146`,
   `self.sender.set(sender).ok()?`).
2. **Event shape**: `ExitEvent::Exited { pid: u64, reason: ExitReason }`
   (`exit_events.rs:22-30`).
3. **Outcome before event; retained until consumed**: "Every process exit …
   publishes `Exited` only after `take_exit_outcome` can observe its retained
   outcome"; `take_exit_outcome(pid) -> Option<(ExitReason, OwnedTerm)>`
   (`execution.rs:194`); "retained until consumed even when their legacy
   tombstones or event notifications are evicted" (`execution.rs:209-211`);
   companion stores consumed independently (`take_exit_error` `:221-223`,
   `take_exit_exception` below it).
4. **Bounded queue + typed Lagged, no outcome discarded**:
   `EXIT_EVENT_CAPACITY = 1_024` (`exit_events.rs:18`); publisher `try_send`
   (`exit_events.rs:157`) with overflow flag (`:163`) surfaced as
   `ExitEvent::Lagged` (`:37`) — "No outcome is discarded … recover by calling
   `take_exit_outcome` for the process identifiers they track" (`:32-36`).

Taxonomy: `ExitReason` is a closed six-variant payload-free `Copy` enum —
`Normal, Kill, Killed, Error, NoConnection, NoProc`
(`src/process/types.rs:229-243`); `exit_reason_from_term` rejects any other
atom as badarg (`src/native/process_bifs/mod.rs:348-359`); `Kill → Killed` is
centralized in `link::terminal_reason` (`src/supervision/link.rs:159-164`).

## 4. §8.8 formal census — ProcessKilled construction sites at 6d09bae

Method: `grep -rn ProcessKilled` over all crates; every non-test-file hit
classified. **Verdict: no production path mints a ProcessKilled fate today.**

Production fate-row constructors mint only three Died causes:
`crates/liminal-server/src/server/participant/production/connection_fate_rows.rs:81`
(`ConnectionLost`), `:99` (`ProtocolError`), `:113` (`UncleanServerRestart`).
There is no ProcessKilled constructor in the production fate-row domain.

Every other site, classified:

| site | class |
|---|---|
| `production/binding_fate_completion.rs:438` (`stored_died_cause`) | translation helper, callers `binding_fate_completion.rs:205` and `ops_terminal_drain.rs:521,:625` — completing/draining an ALREADY-durable pending Died fate; cause originates from rows minted by `connection_fate_rows.rs`, so ProcessKilled is unreachable through it today |
| `production/connection_fate_replay.rs:306` | replay of an existing durable row; does not mint |
| `production/outbox_projection.rs:179,:304-308` | stored→wire projection of existing rows |
| `production/ops_attach_finalizer.rs:107` (`stored_composed_cause`) | fenced-attach finalizer: cause comes from `pending.close_cause()` (`:81`) and the client-presented composed terminal is VALIDATED against the selected pending finalizer (`:82-91`, mismatch → invariant refusal). **The inbound nuance, classified honestly:** a client can spell `CloseCause::ProcessKilled` on the wire here, but it can only echo an existing pending fate's cause, never mint one; since no production path mints a ProcessKilled pending fate, this arm is production-inert today (defensive totality, kept) |
| `production/fenced_attach_codec.rs:367` | durable codec for stored composed terminals |
| `production/fenced_attach_terminal.rs:198` | equivalence predicate between stored domains |
| `production/log_v3.rs:306,:342,:352` | the stored-domain enum declarations themselves |
| `production/outbox_log/codec.rs:198,:292` | outbox durable codec encode/decode of existing rows |
| `participant/crash_repository.rs:57,:452,:649,:658` | **`#[cfg(test)]`** module (`participant/mod.rs:19-20`) — compiled out of production (S-11 holds) |
| `liminal-protocol/src/wire/push.rs:37,:53` | wire vocabulary (`DiedCause::ProcessKilled` + `CloseCause` mapping) — declaration, not production minting |
| `liminal-protocol/src/wire/tags.rs:192,:212,:233` | tag registry |
| `liminal-protocol/src/wire/codec.rs:738` | Died-record decode inside `decode_server_push` (`:676`, reached via `:391`) — the ParticipantDelivery push decode leg (SDK/client side); produces an in-memory wire value, appends nothing durable |
| `liminal-protocol/src/lifecycle/binding.rs:783` | the `process_killed` transition method itself — the emitter's eventual protocol entry point; production callers today are replay-only (`connection_fate_replay.rs:306`) |
| `liminal-protocol/src/lifecycle/binding.rs:849,:906` | `pub(super)` restore helpers (`restore_pending_finalization`, `restore_committed_terminal`) — replay translation from an already-durable cause |
| `liminal-server/tests/trybuild/production_connection_fate_cannot_select_process_killed.rs:4` | the S-10 wall fixture (with harness `production/tests_w1b_connection_fate.rs:420-423`) — verified present and untouched |

## 5. Dispatch-required statements

- **Semver:** no build occurred; no semver event. (Had the build proceeded,
  the reactor fork's Option 2 stays server-behavior; Option 1 likewise; no
  wire or protocol-crate API need surfaced in this survey.)
- **Deviations:** none silent. The one deliberate act beyond reading is this
  committed survey record. The red-first pin was NOT written — the dispatch
  orders red-first only "assuming no STOP".
- **Unverified:** whether beamr pids can recur (design §8.2, unchanged);
  server-side inbound handling of a hostile `ServerPush`-tagged frame was not
  chased past the decode-≠-mint classification in §4.
