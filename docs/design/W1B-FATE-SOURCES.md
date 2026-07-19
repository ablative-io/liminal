# W1b — durable Died / Ordinary / Recovered fate sources

Revision: r1, 2026-07-19. Design-first lane; this revision is docs-only.

Normative ledger: `docs/design/WIRING-LEDGER.md` r1.8 at `9c01f97`. Source pin:
`9c01f97`. Every repository citation below was reopened against those bytes.

## 0. Goal, owner, consumer, and inherited facts

W1b gives the three missing binding fates durable production homes. It extends
one existing participant operation stream, replays the rows through the one
`ConversationAuthority`, and feeds their selected observer projections into the
landed W1a provenance pass. It does not create another fate repository.

The ledger found the structural hole: `StoredOperation` has no Died, Ordinary,
or Recovered variant, the production replay match has no such branch, and the
protocol `BindingFateOperation` has no production durable home
(`docs/design/WIRING-LEDGER.md:28-39,79-89`). W1a deliberately did not invent
those sources; its persistence disposition assigns their rows, barriers, replay,
and cold reconstruction to this lane
(`docs/design/W1-CRASH-WINDOW-REPAIR.md:177-207,574-645`).

- **Owner:** Hermes.
- **Named consumer:** the full Unit 2 §8 crash-fate observer-progress repair.
- **Build trigger:** approval of this design in its own review round, followed by
  one W1b implementation lane.
- **Completion:** all production entry points and every oracle in §9 land
  together. A protocol-only test caller or one row without replay/repair is not
  completion.

The inherited semantic facts are fixed:

1. `DiedBindingTransition::Committed` owns an exact terminal sequence and its
   projection; `Pending` owns no progress yet
   (`crates/liminal-protocol/src/lifecycle/binding.rs:596-628`).
2. `ActiveBinding::finish_died` runs exactly once and returns Committed or
   Pending (`crates/liminal-protocol/src/lifecycle/binding.rs:659-685`). A later
   `PendingFinalization::commit` returns `CommittedBindingTerminal`, not another
   `DiedBindingTransition` (`crates/liminal-protocol/src/lifecycle/binding.rs:552-559`).
3. Ordinary fate consumes an `AttachCommit`, the exact
   `CommittedDiedTerminal`, and a measured resulting floor
   (`crates/liminal-protocol/src/lifecycle/attach.rs:164-188`).
4. Recovered fate consumes only a `BindingFateObserved` event. It validates the
   participant and newly recovered epoch and carries the measured resulting
   floor; no Died terminal is an input
   (`crates/liminal-protocol/src/lifecycle/edge.rs:1007-1056`).
5. `BindingFateOperation` seals conversation, participant, last-dead epoch, and
   resulting floor (`crates/liminal-protocol/src/lifecycle/operation_event.rs:413-490`).
   Its canonical shell codec writes participant, epoch, and floor in a 40-byte
   body (`crates/liminal-protocol/src/lifecycle/conversation_codec.rs:35-38,147-151`).
   The codec deliberately does not identify Ordinary versus Recovered, so the
   production row kind must.
6. Unit 2 requires source append/flush before Advance, cold first-touch repair
   before authority or handshake publication, and loud disagreement
   (`docs/design/F0C-UNIT2-SERVERPUSH-PRODUCER.md:505-526`). A dead target drops
   only its targeted wake after Advance (`:528-555`).
7. W1a already landed provenance witnesses, read-only planning, and the fused
   executor. W1b extends those inputs; it does not replace that machinery.

## 1. Current byte ground truth

The participant log writes `SCHEMA_VERSION = 2`, serializes a version beside
one tagged `StoredOperation`, appends at the optimistic head, and flushes
(`crates/liminal-server/src/server/participant/production/log.rs:25-30,111-136,139-148`).
The current enum has exactly Genesis, Enrolled, Attached, Detached, ZeroDebtAck,
MarkerDrained, RecordAdmission, and Left (`:150-222`). Replay visits base rows in
physical sequence, brackets each row with one checked W1a source visit, and then
merges the extension boundary (`crates/liminal-server/src/server/participant/production/ops_session.rs:274-355`).
Its exhaustive operation match ends with RecordAdmission, MarkerDrained, and
Left and has no fate branch (`:447-524`).

`ConversationAuthority` is the sole production owner. It owns the shell,
frontier, outbox, slots, log head, authoritative progress, and the enriched W1a
witness vector (`crates/liminal-server/src/server/participant/production/state.rs:127-169`).
The older `ParticipantCrashRepository` is test-only
(`crates/liminal-server/src/server/participant/mod.rs:3-6,19-24`). Its useful
precedent is narrow: persist transition inputs, replay through protocol methods,
use a tagged cause/disposition, reject bad version/order, and install a committed
terminal in membership (`crates/liminal-server/src/server/participant/crash_repository.rs:302-344,429-471,537-550,645-705`).
Its separate stream and owner do not move to production.

## 2. Decision A — live Died-emission census

### 2.1 Decision

One new event-driven `ParticipantSemanticHandler` connection-fate entry point
receives the exact durable `ConnectionIncarnation`, the connection's sorted
tracked conversation ids, and one typed close class before those connection
facts are dropped. The installed production handler takes each existing
per-conversation mutex independently, finds slots whose active epoch belongs to
that incarnation, and runs the protocol-owned Died producer. It appends and
flushes one `StoredOperation::Died` per selected active epoch through the same
`OperationLog` barrier used by participant requests. It never holds the
connection registry lock while taking a conversation lock or flushing.

For one connection with several conversations, each conversation is an
independent durable transaction. A failure stops that connection's fate fold,
returns a typed internal failure, discards any touched in-memory owner, and
leaves already flushed conversation prefixes for ordinary cold replay. There is
no fictitious all-conversation atomic commit.

| Died producer | production event at the bytes | W1b ruling: durable Died row, trigger, owner, lock |
|---|---|---|
| `connection_lost` | TCP EOF and read failure are classified in `service_socket`; hard write/drain failures use the same crash funnel (`crates/liminal-server/src/server/connection/process.rs:197-210,293-337`). WebSocket clean transport completion, abrupt read/I/O loss, and peer Close reach the sibling cleanup paths (`crates/liminal-server/src/server/connection/websocket/process.rs:292-345,381-390`). | **Yes for loss of a bound participant transport.** Before participant-publication deregistration and tracked-map destruction, the connection process calls the fate entry point with `ConnectionLost`. The production handler owns the transition under the conversation mutex and appends/flushes Died. A liminal `Frame::Disconnect` is a clean bow-out (`connection/apply.rs:35-59`) and server `ForceClose` is orderly shutdown (`connection/process.rs:658-675`); neither is relabeled ConnectionLost. WebSocket Close without a liminal Disconnect is transport loss for participant bindings even though the socket process exits normally. |
| `process_killed` | The only structural linked-EXIT surface at present belongs to application `ConversationActor` participants, not durable participant bindings (`crates/liminal-server/src/server/connection/conversation.rs:1-7,173-188,239-267`). External connection-process reap has no public nonblocking exit-reason accessor (`crates/liminal-server/src/server/connection/supervisor.rs:2036-2069`). | **No production Died row. Explicitly out of scope.** Mapping either surface would assign a cause without a participant-binding event. The network binding is covered by its actual transport-loss event, or by unclean-restart repair if the server process dies. The dormant protocol producer remains ledgered in §11 with a named consumer and trigger. |
| `protocol_error` | TCP canonical decode returns a terminal protocol error for every error except incomplete/truncated buffering (`crates/liminal-server/src/server/connection/process.rs:865-873`). WebSocket turns malformed/truncated complete binary messages and contract violations into terminal failures (`crates/liminal-server/src/server/connection/websocket/process.rs:410-452`). Participant dispatch also distinguishes typed response-and-close from fatal internal failure (`crates/liminal-server/src/server/participant/dispatch.rs:404-465`). | **Yes, only for a terminating decode or protocol-state refusal after a binding exists.** That classified edge calls the fate entry point with `ProtocolError` before close. Pre-auth failures have no bound slot and append nothing. Store/observer failure, encoder failure, readiness failure, and pressure failure are internal/transport faults, not protocol evidence; they do not fabricate this cause. |
| `unclean_server_restart` | The durable incarnation stream replays, appends the new server startup, flushes it, and exposes the resulting header (`crates/liminal-server/src/server/participant/incarnation_stream.rs:270-301`). The participant handler currently restores all registered conversations before it can publish them (`crates/liminal-server/src/server/participant/production/handler.rs:79-146`). | **Yes.** Composition first completes the incarnation-stream startup barrier, then constructs/restores the participant handler with that current server incarnation. Under each conversation mutex, every still-Bound epoch owned by a prior incarnation runs `unclean_server_restart`; the cause's prior incarnation is derived from that exact epoch (`crates/liminal-protocol/src/lifecycle/binding.rs:783-801`). Died flush, outbox reconciliation, and W1a observer repair complete before the owner is installed or the service is exposed. |

Every included path checks the active epoch against the supplied connection
incarnation. An already Detached/Pending slot, an epoch bound to another
incarnation, a duplicate occurrence, or a conversation that the connection
never tracked cannot append a row. Cleanup remains idempotent: connection Drop
may release volatile resources, but it does not perform a second fate commit.

### 2.2 Alternatives considered

- **Append from `Drop`: rejected.** TCP and WebSocket Drop are non-fallible
  backstops after the handler can no longer report durability failure
  (`connection/process.rs:813-833`; `connection/websocket/process.rs:945-959`).
- **Treat every `ExitReason::Error` as ProcessKilled: rejected.** It collapses
  decode, socket, pressure, registry, and external-scheduler failures and invents
  evidence the supervisor says it cannot recover.
- **Synthesize Died only at the next attach: rejected.** It leaves the §8 source
  barrier absent during the crash window and lets stale authority publish.
- **Scan for dead connections: rejected by LAW-1.** Change is delivered by the
  connection event or startup restore, never discovered by timer, sweep, scan,
  heartbeat, backoff, read-timeout wake, stop-flag sample, or synthetic probe
  (`docs/design/LAW1-POLLING-RETIREMENT.md:9-22`).

### 2.3 Byte-evidence conclusion

Three producers have an exact production event and therefore gain the durable
Died append. ProcessKilled has no truthful participant-binding event and gains
none. “No row” is sound there because the full crash-fate consumer needs exact
binding provenance, not an approximate supervisor label; transport loss and
unclean restart already cover the network server's observable binding deaths.

## 3. Decision B — `StoredOperation` extension and replay

### 3.1 Decision: exact v3 row shapes

`StoredOperation` gains these literal serde tags and no aliases:

```text
Died { row: StoredDied }
Ordinary { row: StoredOrdinaryFate, event: Vec<u8> }
Recovered { row: StoredRecoveredFate, event: Vec<u8> }
```

The exact nested fields are normative:

| row | exact fields | reason |
|---|---|---|
| `StoredDied` | `participant_id`; `binding_epoch: StoredBindingEpoch`; `cause: StoredDiedCause`; `terminal_order`; `disposition: Committed { terminal_seq } | Pending` | These are exactly the `ActiveBinding`, cause, and committed-or-pending `BindingTerminalDisposition` inputs to `finish_died`. A tagged disposition, not `Option<terminal_seq>`, makes Pending versus malformed Committed unambiguous. `StoredDiedCause` is the closed four-arm cause sum; UncleanServerRestart additionally stores `prior_server_incarnation` and replay requires it to equal the epoch-derived value. Died has no fabricated `BindingFateOperation` shell bytes. |
| `StoredOrdinaryFate` | `participant_id`; `last_dead_binding_epoch`; `died_source_sequence`; `died_cause`; `died_transaction_order`; `died_terminal_seq`; `resulting_floor` | `AttachCommit::ordinary_binding_fate` consumes the complete exact committed-Died provenance plus the measured floor. Duplicating the terminal audit beside its source reference is intentional cross-checking, not a second owner. The `event` is the canonical `BindingFateOperation::from_ordinary` shell bytes. |
| `StoredRecoveredFate` | `participant_id`; `prior_binding_epoch`; `recovered_binding_epoch`; `fenced_attach_source_sequence`; `marker_delivery_seq`; `resulting_floor` | These fields identify the exact `FencedAttachCommit` and its `BindingFateObserved` input. There is deliberately no Died cause, Died order, or Died terminal sequence: recovered fate consumes none. The `event` is the canonical `BindingFateOperation::from_recovered` shell bytes. |

Conversation id remains the stream identity and is also revalidated against the
protocol-produced value, as current request-bearing rows do. `StoredBindingEpoch`
continues to encode server incarnation, connection ordinal, and nonzero
capability generation and uses the existing checked reconstruction
(`crates/liminal-server/src/server/participant/production/log.rs:502-527`).

The aggregate's current slot gains one move-only, bounded current-binding fate
provenance value: EnrollmentOrigin, OrdinaryAttach authority, or FencedAttach
proof. It is produced by live commit and reconstructed by the same replay helper.
It lives inside `ConversationAuthority::slots`; it is not serialized separately,
cloneable, globally indexed, or exposed through a second repository. Fenced
attach support must stop refusing every marker-bearing attach and must retain the
protocol-produced `FencedAttachCommit`; current production's unconditional
marker-bearing refusal is at
`crates/liminal-server/src/server/participant/production/ops_attach.rs:97-103`.

### 3.2 Decision: canonical live producer per fate

The three row kinds have three typed production moments. They are not socket
codecs and are never appended merely because a server branch has a matching
name:

| row | canonical live producer | trigger, owner, lock, and barrier |
|---|---|---|
| Died | the one connection-fate/startup adapter ruled in §2, consuming the exact current `ActiveBinding` through one of the four cause methods | A classified included connection event or prior-incarnation Bound restore. `ConversationAuthority` owns the transition under its per-conversation mutex. Died append/flush is the initial source barrier; Committed may then offer its projection, while Pending offers none. |
| Ordinary | the aggregate binding-fate operation at the point where the retained ordinary `AttachCommit` authority and the exact earlier `CommittedDiedTerminal` meet | The Died terminal must already be durable and source-referenced. Under the same conversation mutex, the protocol transition measures `resulting_floor`, production calls `decide_ordinary_binding_fate_operation`, and the Ordinary row plus canonical shell event append/flush before closure-state installation or observer handling. The server never computes a floor from its observer maximum. |
| Recovered | the aggregate fenced-fate operation at the point where retained `FencedAttachCommit` provenance receives the protocol-produced `BindingFateObserved` | Under the conversation mutex, the protocol/storage edge supplies participant, recovered epoch, and measured floor. Production calls `recovered_binding_fate` and `decide_recovered_binding_fate_operation`, then appends/flushes Recovered plus its canonical event before applying the returned closure authority or observer handling. No Died terminal is requested, synthesized, or passed. |

The protocol aggregate already has distinct sealed Ordinary and Recovered event
barriers (`crates/liminal-protocol/src/lifecycle/aggregate_commit.rs:288-313`).
W1b moves both behind `ConversationAuthority` and its existing `DurableAppend`;
it does not expose a raw public fate-row constructor. A Died append that has no
later Ordinary/Recovered row is complete legal history. Conversely, an
Ordinary/Recovered transition is not “repaired” by guessing it from row absence:
only its typed live event may create its base row. If its append/flush fails, no
closure state or projection publishes, the in-memory owner is discarded, and
the operation remains uncommitted. Next touch replays only the flushed prefix;
the candidate may run again only if its typed event is delivered again, never
because a timer or absence heuristic guesses it. A process restart classifies
any still-Bound old epoch through the unclean-restart path.

### 3.3 Decision: replay transitions

The production replay match gains three arms and calls the same helpers as live
application:

1. **Died:** require a Bound slot whose participant and epoch match; reconstruct
   the tagged disposition; invoke exactly the stored cause producer once; check
   every terminal/pending field; install `transition.binding_state()`. For
   Committed, install the terminal into membership exactly as the test-only
   precedent does, project the sealed terminal sequence, derive the Died outbox
   record, and observe the checked order/sequence. Pending installs no terminal,
   no outbox Died record, and no observer candidate.
2. **Ordinary:** resolve the referenced earlier committed Died row and the
   retained ordinary-attach authority; cross-check cause, epoch, order, and
   terminal sequence; invoke `ordinary_binding_fate` with the stored measured
   floor; mint and byte-check `BindingFateOperation::from_ordinary`; apply the
   protocol-owned closure transition; then offer its sealed projection to the
   occurrence router in §5.
3. **Recovered:** resolve the referenced fenced attach and cross-check old epoch,
   recovered epoch, and marker sequence; construct only the exact
   `BindingFateObserved(participant, recovered_epoch, resulting_floor)` event;
   invoke `recovered_binding_fate`; mint and byte-check
   `BindingFateOperation::from_recovered`; apply the recovered closure transition;
   then offer its sealed projection to the occurrence router. Replay never calls
   `finish_died` to fabricate a terminal for this arm.

All numeric source references, replay heads, ordinals, order/sequence advances,
and conversions use checked arithmetic. Unknown cause/disposition tags,
forward/self references, epoch mismatch, source mismatch, event-byte drift, or a
protocol refusal is a typed restore failure before owner or observer publication.

### 3.4 Decision: W1a witness join

Every canonical candidate passes through §5's occurrence router before the
existing `record_observer_progress_projection`. A selected candidate adds one
base source kind, occurrence, producer, and lineage to the existing W1a types at
`observer_progress.rs:45-72,74-177`; it then uses the existing conversation,
source-order, uniqueness, and per-lineage checks at `:352-431`.

| fate candidate | source identity | occurrence identity | producer | lineage key |
|---|---|---|---|---|
| Died Committed | base log sequence + Died kind | `(conversation_id, participant_id, binding_epoch)` plus exact `terminal_seq` audit | Died transition | `(conversation_id, ParticipantTerminal, participant_id)` |
| Ordinary | base log sequence + Ordinary kind | `(conversation_id, participant_id, last_dead_binding_epoch)` plus referenced Died source/terminal audit | Ordinary binding fate | `(conversation_id, BindingFateFloor, participant_id)` |
| Recovered | base log sequence + Recovered kind | `(conversation_id, participant_id, recovered_binding_epoch)` plus fenced-attach source audit | Recovered binding fate | `(conversation_id, BindingFateFloor, participant_id)` |

`BindingFateFloor` is a new source-specific W1a lineage class. It prevents a
measured fate floor from being compared as though it were a terminal delivery
sequence, while retaining per-participant monotonicity across fate-floor
sources. The occurrence key is shared across the three candidates even though
lineage classes differ.

### 3.5 Alternatives considered and byte evidence

- **Persist only `BindingFateOperation`: rejected.** Its sealed body erases fate
  class, Died cause/order/terminal, fenced prior epoch, marker provenance, and
  source references (`operation_event.rs:413-490`). It cannot replay the typed
  transition.
- **Promote `ParticipantCrashRepository`: rejected.** It is behind `#[cfg(test)]`
  and would become a second lifecycle owner.
- **Put the rows in the Unit 2 extension stream: rejected.** These are primary
  participant lifecycle transitions, not outbox-only projections. Base replay
  must rebuild them before extension reconciliation.
- **Persist raw constructed outcomes: rejected.** Existing log discipline stores
  transition inputs and canonical event audit, then re-runs protocol rules
  (`log.rs:1-10`). W1b keeps that discipline.

## 4. Decision C — schema version and migration-or-refusal

### 4.1 Decision

Bump participant operation `SCHEMA_VERSION` from 2 to 3. The stream key remains
`liminal:participant-production:<conversation_id>`; no parallel stream is added.
All new appends write v3.

The actual current gate first deserializes only `StoredEntryVersion`, compares
its byte to the one compile-time version, and only then deserializes the complete
entry (`crates/liminal-server/src/server/participant/production/log.rs:90-108,139-148`).
W1b preserves the two-stage gate but makes the second stage explicit by version:

- version 2 decodes through a frozen `StoredEntryV2`/`StoredOperationV2` containing
  exactly the eight current variants and converts them losslessly into the live
  replay input;
- version 3 decodes through the expanded v3 enum;
- missing or malformed version/row returns typed serialization corruption;
- every other version returns `OperationLogError::SchemaVersion(actual)`;
- replay accepts an all-v2 stream or one contiguous v2 prefix followed by a v3
  suffix; a v2 row after the first v3 row returns a new typed
  `SchemaVersionTransition { sequence, previous: 3, actual: 2 }`;
- no row is defaulted, aliased, skipped, decoded under two schemas, or inferred
  from its JSON shape.

A v2-only conversation is a legal history with no Died/Ordinary/Recovered rows.
The new binary does not invent historical fates. W1a already treats source
absence explicitly: Untracked plans from virtual zero, an empty witness pass has
maximum zero, and only unsupported nonzero durable progress refuses
(`crates/liminal-server/src/server/participant/production/observer_progress_plan.rs:36-84`).
On a later real fate or unclean-restart repair, the first new append starts the
v3 suffix.

**Old binary reading new log:** the old v2 binary parses the v3 version prefix
and returns its existing typed `OperationLogError::SchemaVersion(3)` before it
tries to decode the expanded enum (`log.rs:101-106`). Startup/first touch fails;
it cannot silently omit a fate.

**New binary reading old log:** the frozen v2 decoder accepts and replays the
complete prefix. Absence of fate rows is history, not corruption. Missing,
unknown, malformed, or version-regressing bytes still refuse before authority,
outbox, observer, or socket publication.

### 4.2 Alternatives considered

- **Reject all v2 in the new binary:** rejected because it would make every
  existing legal history undeployable despite an unambiguous exact decoder.
- **Rewrite v2 in place:** rejected because the append-only `DurableStore`
  surface provides no rewrite/truncate contract; Unit 2 records the same fact
  (`docs/design/F0C-UNIT2-SERVERPUSH-PRODUCER.md:430-442`).
- **Leave the version at 2:** rejected because old readers could reach an unknown
  enum variant as generic serialization failure rather than the required schema
  refusal, and the schema claim would be false.
- **Serde default/alias or skip unknown variants:** rejected. That loses source
  barriers and violates the Unit 2 loud-migration wall
  (`docs/design/F0C-UNIT2-SERVERPUSH-PRODUCER.md:815-822`).

## 5. Decision D — canonical producer and occurrence key

### 5.1 Decision

The dispatch boundary owns one `FateOccurrenceKey`:

```text
(conversation_id, participant_id, binding_epoch)
```

The tuple comes only from the active/replayed binding authority plus the sealed
fate value; it is never inferred from observer progress. Each complete live or
cold pass keeps a checked per-conversation map from this key to the first
canonical presentation. Durable rows remain independently validated even when
they do not win presentation.

The producer rule is:

1. a Committed initial Died transition claims the key and presents its exact
   terminal sequence;
2. a Pending initial Died transition claims no presentation;
3. Ordinary or Recovered claims an unpresented key with its measured floor;
4. if an exact later Ordinary/Recovered row describes the same already-presented
   committed Died occurrence, it remains a durable crash-fate fact but emits no
   second observer witness;
5. any second Died, repeated same-class fate row, Ordinary-versus-Recovered
   conflict, epoch/source disagreement, or alternate producer for the key is a
   typed `FateOccurrenceConflict` before observer mutation.

Therefore **Died-then-Recovered for one epoch presents once, by Died**. This is
not numeric deduplication: the Died terminal sequence and Recovered measured
floor may differ. It is an explicit occurrence/provenance rule. Recovered still
replays its typed transition and event bytes and still proves its candidate
projection; it simply cannot double-present a key already claimed by a committed
Died. When no committed Died presentation exists—as required by Recovered's
no-terminal input contract—Recovered presents its measured floor once.

Live and cold use the same router. Live application does not call a projection
method directly around it; cold replay does not infer the winner from a numeric
maximum. The current W1a duplicate-producer check remains the final structural
backstop (`observer_progress.rs:400-425`), while the fate router owns the legal
Died-to-specific-fate relationship before a W1a witness is constructed.

### 5.2 Alternatives considered and byte evidence

- **Last row wins:** rejected; it changes already durable observer history and
  makes crash timing select a producer.
- **Largest progress wins:** rejected; terminal sequence and measured floor are
  different typed facts, not competing estimates.
- **Present both and rely on `max`: rejected.** The ledger expressly says numeric
  tolerance is not a design and requires one canonical producer
  (`docs/design/WIRING-LEDGER.md:107-113`).
- **Use `(participant, terminal_seq)`: rejected.** Recovered has no terminal
  input. The epoch is present in both Ordinary and Recovered typed provenance
  (`edge.rs:925-945,1101-1122`).

## 6. Decision E — pending-Died finalizer routing

### 6.1 Decision table

| state/route | canonical source and projection | replay rule | oracle/exclusion disposition |
|---|---|---|---|
| Initial `DiedBindingTransition::Committed` | Append Died Committed; Died arm presents its exact `terminal_seq`. | Replay invokes the stored cause once and checks the committed terminal. | Covered by census items 1, 8, and 11. |
| Initial `DiedBindingTransition::Pending` | Append Died Pending so cause/participant/epoch/order are durable; present **none** because the protocol method returns `None`. | Replay invokes the stored cause once with Pending and reconstructs `PendingFinalization`; it does not call `commit`. | Covered by census items 1, 8, and 20. |
| Pending Died finalized by Leave | `StoredOperation::Left` and `LiveLeaveCommit` are the sole enclosing source. Present the Left sequence, never the old terminal sequence and never a later Died arm. | Re-run the landed pending-Leave path. It allocates terminal then Left and records only `LiveLeaveCommit` (`crates/liminal-server/src/server/participant/production/ops_leave.rs:359-430`). | Covered by census item 21. |
| Pending Detached finalized by Leave | Same `Left`/`LiveLeaveCommit` source. Died is impossible because the pending cause is Detached. | Same generic `PendingFinalization` replay; cause remains typed and the Left row audits the prior terminal (`production/log.rs:332-342`). | Covered by census item 22. |
| Pending Died or Detached composed by fenced AttachCommit | The enclosing `StoredOperation::Attached` is canonical. `AttachCommit::observer_progress_projection` selects its composed terminal sequence (`crates/liminal-protocol/src/lifecycle/attach.rs:120-137`); no Died/Detached fate candidate is separately presented. | `verify_fenced_attach` requires exact pending state and sequence, then `commit_attach` calls `finalization.commit(sequence)` once and carries the composed terminal (`attach.rs:300-356,395-448`). The Attached row must persist/audit the pending-terminal sequence as part of fenced mode. | Covered by census item 23. |
| Ordinary fate after a committed Died | Ordinary row persists the exact Died reference and measured floor. The occurrence router presents Ordinary only if no committed Died presentation claimed the epoch. | Resolve the prior Died; never re-run `finish_died`; call `ordinary_binding_fate` once. | Covered by census items 2, 9, and 18. |
| Recovered fate during/after attach recovery | Recovered row is its enclosing canonical source when no committed Died presentation exists. It has no Died terminal input. | Reconstruct `FencedAttachCommit` and pass only `BindingFateObserved`. | Covered by census items 3, 10, and 19. |
| Standalone pending-terminal finalization | **Excluded. No production owner exists.** The only production callers are enclosing Leave; current production attach rejects pending state, and W1b admits it only through fenced AttachCommit. | No replay variant or public handler is added. A future standalone owner must receive a ledger row before code. | Covered by the structural exclusion in census item 24 and the road back in §11. |
| Restart with pending state | No new initial transition. Replay the Died Pending row to reconstruct cause/epoch/order, then route a later durable Left/Attached row through its enclosing source. | Never call `finish_died` twice and never fabricate a `DiedBindingTransition` from `CommittedBindingTerminal`. | Covered by census item 20. |

### 6.2 Alternatives considered

- **Call Died after every finalizer:** rejected by the return type at
  `binding.rs:552-559` and by the landed Leave path.
- **Let terminal sequence and Left/Attached both present:** rejected by the
  enclosing-source and single-presentation rules.
- **Add a generic `Finalized` row now:** rejected. There is no production caller,
  and it would create a dormant alternate owner rather than close W1b.

## 7. Decision F — W1a tear-rider discharge

### 7.1 Decision

Delete the four local zero counters and their tuple assertion from the landed
same-participant lineage regression. Do not “wire” them.

At the bytes, that test calls `ObserverProgressWitnessState` directly, asserts
the exact `SourceLineageRegression`, and compares durable observer rows before
and after. It then declares `arm_removals`, `wakes`, `owner_publications`, and
`classifications` as zero and compares them with a literal zero tuple
(`crates/liminal-server/src/server/participant/production/tests_w1a.rs:309-340`).
None of those four runtime observation points can execute in that unit-level
source-validation seam. Adding counters there would be test-only theater or
would broaden the test into a different handler fixture.

The W1b implementation's first touch of that file must remove lines 332-339,
retain the exact typed-error assertion and measured durable-row equality, and
leave the test enabled and otherwise uninverted. Existing handler-level W1a
oracles already own real arm removal, wake, publication, and classification
paths; this rider does not duplicate them. A source census verifies the tuple
and declarations are absent.

This decision generalizes into a standing lens: no W1b oracle may compare a
constant, a freshly declared fixed value, or values derived only from fixture
constants against another constant and call that observation. Assertions must
read protocol results, decoded durable rows, store heads, lock-visible owner
state, emitted publications, or instrumented production observation points.

### 7.2 Alternative considered

Wiring four handler counters into a direct witness-state test was rejected
because the test never owns `ObserverOwner`, `ParticipantPublicationRegistry`,
or handshake classification. Deleting the decoration is the narrow, honest
r1.8 rider discharge (`docs/design/WIRING-LEDGER.md:41-56`).

## 8. Ordering, crash cuts, and production integration

For each selected fate source:

1. acquire the existing per-conversation owner lock;
2. reconstruct the owner first if absent;
3. select and validate the typed protocol transition and occurrence;
4. append/flush the v3 source row at the checked base-log head;
5. install only the committed transition state and advance the checked head;
6. reconcile the exact Unit 2 outbox projection, if any;
7. run the existing complete replay pass so the W1a witness set, read-only
   `Untracked | Tracked(A)` plan, and fused executor repair any missing Advance;
8. only then release the owner for publication/reattach/observer classification.

The current handler already owns the per-conversation cell and repeats complete
replay/repair whenever a source head advances
(`crates/liminal-server/src/server/participant/production/handler.rs:168-241`).
W1b makes connection fate and startup restore enter that owner, rather than
adding another lock or bypass.

Crash cuts are exact:

- before source flush: no fate row, no transition installation, no Advance;
- after source flush but before outbox/Advance: cold first touch replays the fate
  row and repairs the missing projections before publication;
- after Advance flush but before wake: durable progress survives; only the exact
  volatile wake may be lost;
- after a Died Pending row: replay reconstructs Pending and waits for an
  event-driven enclosing finalizer; no task polls for one;
- during a v2-prefix/v3-suffix upgrade: the flushed prefix is authoritative;
  old binaries refuse at the first v3 row and new binaries resume by physical
  sequence.

The `handler_observer_reconcile` fused executor remains the only observer
mutation path. W1b may add source kinds/metadata and route new candidates into
it, but may not reimplement planning, append Track early, or publish around it.

## 9. Acceptance oracle census

Each exact name appears once in this brief. These names are the implementation
floor, not illustrative suggestions. Fixtures use deterministic append/flush
and first-touch gates; no sleeps, eventual assertions, or constant-versus-
constant claims are accepted.

| # | exact oracle | required proof |
|---:|---|---|
| 1 | `died_binding_transition_projects_terminal_sequence_only_when_committed` | Independent real Committed and Pending transitions; read the sealed projection. Committed equals the allocated terminal sequence and Pending is absent. |
| 2 | `ordinary_binding_fate_projects_measured_resulting_floor` | Consume an ordinary AttachCommit and exact committed-Died provenance with a distinguishing measured floor; read that floor from the sealed projection. |
| 3 | `recovered_binding_fate_projects_measured_resulting_floor` | Consume fenced provenance plus BindingFateObserved with a distinguishing measured floor; read it from the sealed projection and prove no Died terminal was constructed or passed. |
| 4 | `connection_lost_appends_died_source_before_transport_teardown` | For TCP EOF/read/write and WebSocket transport loss with a real bound epoch, observe Died append then flush before publication deregistration; clean Disconnect/ForceClose arms append no Died. |
| 5 | `protocol_error_appends_died_source_only_for_bound_terminal_refusal` | Malformed canonical input after binding appends ProtocolError Died; pre-auth, typed response, and internal durability/pressure failures do not fabricate that cause. |
| 6 | `unclean_restart_appends_prior_incarnation_died_before_owner_publication` | After incarnation startup, restore an old Bound epoch, observe exact prior-incarnation cause and source flush before handler/service publication; second restore appends nothing. |
| 7 | `process_killed_has_no_production_participant_binding_emitter` | Structural source census proves no application ConversationActor EXIT or reasonless supervisor reap calls the participant fate entry point; the dormant road-back row remains named. |
| 8 | `died_stored_operation_round_trips_and_replays_committed_and_pending` | Encode/decode both tagged dispositions; replay exact cause/participant/epoch/order, terminal only for Committed, binding state, member history, and checked head. |
| 9 | `ordinary_stored_operation_round_trips_and_replays_measured_fate` | Round-trip the full Died reference/audit, measured floor, and event; replay through ordinary authority and byte-check the re-minted shell event. |
| 10 | `recovered_stored_operation_round_trips_without_died_terminal` | Round-trip both epochs, fenced source, marker, and floor; replay through FencedAttachCommit/BindingFateObserved and byte-check the event with no Died terminal field or call. |
| 11 | `died_source_flush_precedes_observer_advance_and_cold_repair` | Gate Died source and Advance independently; before source flush there is no state/publication, after source-only crash first touch repairs Advance, then repeated replay is idempotent. |
| 12 | `ordinary_source_flush_precedes_observer_advance_and_cold_repair` | Same barrier/cold cut for Ordinary, reading its measured floor and durable source/event bytes. |
| 13 | `recovered_source_flush_precedes_observer_advance_and_cold_repair` | Same barrier/cold cut for Recovered, with fenced provenance and no invented Died input. |
| 14 | `old_v2_reader_refuses_v3_fate_row_with_typed_schema_version` | Run the frozen old reader on a v3 fate entry; observe SchemaVersion(3) before enum decoding or publication. |
| 15 | `v3_reader_accepts_v2_prefix_and_refuses_v2_after_v3` | Pure v2 and v2-prefix/v3-tail histories replay; a later v2 row returns the typed sequence-bearing transition error. |
| 16 | `missing_unknown_malformed_and_mixed_operation_versions_refuse_before_publication` | Independent missing, unknown, malformed, and illegal-mix fixtures produce their exact typed errors with no authority/outbox/observer/socket publication. |
| 17 | `fate_occurrence_key_presents_each_new_arm_at_most_once` | Died, Ordinary, and Recovered independent fixtures derive the exact conversation/participant/epoch key; a duplicate same-arm or cross-arm conflict cannot add a second W1a witness. |
| 18 | `died_then_recovered_same_epoch_presents_died_once` | Persist exact Died then compatible Recovered facts for one epoch; both typed transitions replay, the Recovered event remains durable, and the observed witness set contains one Died producer/value, not a numeric dedup. |
| 19 | `recovered_without_committed_died_presents_measured_floor_once` | A fenced Recovered occurrence with no committed Died presentation adds exactly one BindingFateFloor witness and one repair Advance when it establishes the maximum. |
| 20 | `pending_died_restart_restores_cause_epoch_order_without_refinish` | Cut after Died Pending flush; cold replay reconstructs identical Pending state and source head without calling finish_died twice, assigning terminal seq, or presenting progress. |
| 21 | `pending_died_finalized_by_leave_presents_only_live_leave_commit` | Start from real pending Died, finalize through Leave, and observe terminal then Left allocation but only one LiveLeaveCommit witness at the Left sequence. |
| 22 | `pending_detached_finalized_by_leave_presents_only_live_leave_commit` | Same enclosing-source proof for pending Detached; no Died producer or terminal-sequence projection appears. |
| 23 | `pending_terminal_composed_by_attach_presents_only_attached_source` | Exercise Died and Detached pending inputs through real fenced AttachCommit; the composed terminal is audited but only the enclosing Attached source presents. |
| 24 | `standalone_pending_finalizer_has_no_production_entry_point` | Type-aware/source census finds no production call that commits PendingFinalization outside Leave or fenced AttachCommit and no dormant StoredOperation alternate. |
| 25 | `same_participant_ack_lineage_regression_refuses_before_observer_mutation` | Retained landed test still reads exact SourceLineageRegression and unchanged durable observer rows after the decorative counter tuple is deleted. |
| 26 | `w1b_tear_rider_removes_tautological_four_counter_tuple` | Source census proves the four fixed-zero declarations and their zero-tuple assertion are absent from the retained regression; no replacement constant-only assertion exists in W1b tests. |
| 27 | `fate_live_and_cold_replay_produce_identical_witnesses_and_state` | For all three variants, compare measured decoded rows, typed authority state, witness metadata/order, outbox state, and final durable observer progress across live and cold paths. |

The repository-standard implementation gates are:

```text
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo check -p liminal-protocol --target wasm32-unknown-unknown --no-default-features
cargo check -p liminal-sdk --target wasm32-unknown-unknown --no-default-features
```

The wasm legs are mandatory if protocol fate types/codecs change.

## 10. Honesty, cost, and non-goals

### 10.1 Idle and active cost

W1b adds no task, thread, timer, polling loop, sweep, scan, cursor, heartbeat,
backoff, read timeout, stop-flag sampler, or synthetic probe. New rows are
event-driven appends on lifecycle paths that already use the participant
operation-log append/flush barrier: classified connection termination, typed
fate completion, and startup restore. Across `N` idle conversations, added
application wakes, reads, and appends are `0 × N = 0`.

One connection-fate event visits only the connection's signed-bounded tracked
conversation set, then only active slots bound to its exact incarnation. Work is
linear in those bounded facts. Each row adds bounded fixed metadata and one
canonical event byte vector where applicable. Replay retains the existing
history-linear W1a witness vector; W1b adds no duplicate history vector and
makes no unbounded-history claim.

### 10.2 Non-goals

- no production ProcessKilled guess from application actors or supervisor reap;
- no generic standalone pending-finalizer API or row;
- no second crash repository, stream, aggregate, lock hierarchy, or lifecycle
  interpretation;
- no rewrite of old v2 bytes and no physical compaction;
- no observer schema change, durable socket target, or broadcast wake;
- no new fate formula in server code: terminal sequence and measured floor come
  from protocol-owned values;
- no polling-based discovery or periodic retry;
- no change to Leave's landed canonical producer;
- no weakening, deletion, inversion, ignore, or replacement of landed tests;
  only the exact rider's tautological tuple is deleted;
- no claim that a successful socket handoff is receipt; and
- no direct haematite crash-gate API.

## 11. Deferred/excluded seams under no-row-no-dormancy

| seam | named future consumer | trigger | owner | oracle floor |
|---|---|---|---|---|
| Production `process_killed` participant-binding emitter | a beamr-to-participant exact exit-fate adapter | a public nonblocking beamr exit event that carries reason plus the exact connection incarnation and can be delivered before tracked binding facts are destroyed | Artemis owns the beamr API; Hermes owns the liminal adapter | exact forced-process exit appends ProcessKilled once; socket/decode/restart events cannot select it; live/cold state agrees |
| Standalone pending-terminal finalizer | a future aggregate-owned lifecycle operation that cannot be enclosed by Leave or AttachCommit | the first reviewed production caller requiring such a transition | Hermes | row/codec/replay/barrier plus single-presentation and restart oracles, added to the wiring ledger before implementation |

These are exclusions, not hidden TODOs. Everything else specified by this brief
lands in the W1b implementation. No additional fate-source item may ship dormant
without its own successor-register row.

## 12. Walls

- **WALL-W1B-ONE-OWNER:** all fate state, provenance, replay, and occurrence
  routing remain inside `ConversationAuthority`; never promote or copy the
  test-only crash repository.
- **WALL-W1B-SOURCE-BEFORE-ADVANCE:** source append/flush precedes outbox repair,
  observer Track/Advance, owner publication, classification, and wake.
- **WALL-W1B-TYPED-ROWS:** Died, Ordinary, and Recovered have distinct exact v3
  shapes; no default, alias, raw-outcome shortcut, or silent field omission.
- **WALL-W1B-RECOVERED-NO-DIED-INPUT:** Recovered replay passes only
  BindingFateObserved to recovered_binding_fate; no terminal is inferred.
- **WALL-W1B-FINISH-DIED-ONCE:** replay reconstructs the one stored initial
  transition; a later finalizer never fabricates another Died transition.
- **WALL-W1B-SINGLE-PRESENTATION:** one occurrence key has at most one observer
  witness; Died precedes and owns a compatible later Recovered presentation.
- **WALL-W1B-LIVE-COLD-CONSISTENT:** live and cold use the same transition,
  occurrence router, witness metadata, outbox projection, and fused observer
  executor.
- **WALL-W1B-W1A-STANDS:** W1a provenance validation, read-only preflight,
  running-maximum planning, and fused executor remain untouched except for
  adding the three source kinds, lineages, and calls that join them.
- **WALL-W1B-LOUD-VERSION:** v2 prefix compatibility is explicit; unknown,
  missing, malformed, or regressing versions refuse before publication; old v2
  readers reject v3.
- **WALL-W1B-CHECKED-ARITHMETIC:** log heads, source references, merged ordinals,
  orders, terminal/delivery sequences, counts, and conversions are checked.
- **WALL-W1B-TYPED-REFUSAL:** schema, source, occurrence, epoch, provenance,
  event-byte, and protocol-transition faults remain typed; no formatted-string
  fallback manufactures a wire value.
- **WALL-W1B-NO-POLLING:** only connection events, typed operation completion,
  startup, and request first touch trigger work.
- **WALL-W1B-DROPPED-WAKE:** after durable Advance, a dead/missing weak target
  drops only that exact wake and never broadcasts.
- **WALL-W1B-LANDED-TESTS:** never delete, ignore, invert, or weaken a landed
  test. Rider deletion removes only a constant-vs-constant assertion that proves
  nothing.
- **WALL-W1B-OBSERVATION-LENS:** no proposed oracle contains constant-vs-constant
  assertions; every assertion reads an actual production or durable observation.
- **WALL-W1B-DOCS-ONLY-R1:** this revision changes only this brief.

## 13. Revision record

| revision | date | record |
|---|---|---|
| r1 | 2026-07-19 | First W1b design brief against ledger r1.8: ruled the four-producer live census; exact Died/Ordinary/Recovered v3 row shapes and replay; v2-prefix/v3-tail compatibility with loud old/new refusal; occurrence identity and Died-first single presentation; complete pending-finalizer routing; W1a witness joins; required oracle census; idle/non-goal/deferred honesty; and deletion of the r1.8 tautological four-counter rider. |
