# W1b — durable Died / Ordinary / Recovered / Detached fate sources

Revision: r3, 2026-07-20. Design-first lane; this revision is docs-only.

Normative ledger: `docs/design/WIRING-LEDGER.md` r1.9 at `c77ce31`. Source pin:
`c77ce31`. The only repository change from the r1 source pin to this pin is the
ledger amendment; every source citation below was nevertheless reopened against
these bytes.

## 0. Goal, owner, consumer, and review disposition

W1b gives all four binding-fate classes durable production homes. It extends one
participant operation stream, replays every row through the one
`ConversationAuthority`, and feeds selected observer projections into the landed
W1a provenance pass. The r1.9 ledger expressly extends this lane from three to
four classes because clean Disconnect and orderly server shutdown currently
leave bindings Bound while the exact Detached producers are dormant. That was a
live no-row-no-dormancy violation, and Detached is repaired here at the same
schema, producer, replay, finalizer, occurrence, lineage, and oracle depth as
Died, Ordinary, and Recovered (`docs/design/WIRING-LEDGER.md:57-75`).

- **Owner:** Hermes.
- **Named consumer:** the full Unit 2 §8 crash-fate observer-progress repair.
- **Build trigger:** approval of this r3 design, followed by one W1b
  implementation lane.
- **Completion:** every production entry point, v3 row/decoder, replay path,
  barrier, and oracle in §12 lands together. Protocol-only callers or rows with
  no cold completion are not completion.

The r1 and r2 reviews were both **not ready**. R2's prior findings remain folded
in the decisions below; the round-2 re-review contributes this complete
six-element array (**5 NEW MAJOR + 1 minor**):

| round-2 finding | r3 disposition |
|---|---|
| MAJOR 1 — Pending-Died completion grammar | §§4.1, 4.5, 8, and 9 define one closed grammar. Ordinary names a lower Committed Died or exact Left/Fenced-Attached finalizer source; a Pending Died row never changes disposition. Recovered after Pending Died durably reserves/presents the occurrence, and the later finalizer consumes that reservation through a typed non-presenting transition. |
| MAJOR 2 — consume-once must revoke copyable proof | §5.3 removes Clone/Copy from `FencedAttachCommit`, moves it by value through verified attach/commit, stores one private recovered authority, and removes the old public fate-capable method. §13 records the required next-publish `liminal-protocol` 0.3.0 semver implication. |
| MAJOR 3 — selector candidate charge | §7.1 chooses a sealed two-stage prepare/admit API. The server computes one versioned canonical candidate charge after allocation is proposed; protocol validates conversation, participant, epoch, order, sequence, and one encoded entry before deciding Committed/Pending. Every refusal returns unchanged authority. |
| MAJOR 4 — Pending explicit Detached shape | §§4.1 and 7.2 partition ExplicitRequestCommitted, which retains the canonical shell event, from ExplicitRequestPending, which stores no event and re-mints the pending cell/outcome from exact request facts and observer baseline. |
| MAJOR 5 — Open intent live failure policy | §§3.2–3.4 and 11 choose participant-service/server-fatal escalation after Open. Startup replay is mandatory; Drop may release only volatile transport state and cannot complete/reclassify the intent. Simultaneously unmatched Opens are bounded by signed `limits.max_connections`. |
| MINOR — marker digest contract | §5.1 omits the digest. The exact durable marker source sequence plus full source-row/frontier validation is sole identity; replay mints one `ValidatedMarkerRecord` only after validation. |

## 1. Current byte ground truth and inherited semantics

The participant log writes schema v2, decodes and then discards the entry version,
returns only `(sequence, StoredOperation)`, appends at an optimistic head, and
flushes (`crates/liminal-server/src/server/participant/production/log.rs:90-135`).
Its current enum has Genesis, Enrolled, Attached, Detached, ZeroDebtAck,
MarkerDrained, RecordAdmission, and Left, with no Died/Ordinary/Recovered branch
(`:150-222`). Replay carries only `(sequence, operation)` across 64-row pages and
therefore cannot enforce a schema transition across a page boundary
(`crates/liminal-server/src/server/participant/production/ops_session.rs:274-342`).

The current request dispatcher has no binding-fate request or emitter; it only
matches the existing client request variants
(`crates/liminal-server/src/server/participant/production/handler_semantic.rs:221-309`).
The connection termination and startup paths likewise append no participant
fate rows today: TCP releases conversations directly on EOF/error/normal close
(`connection/process.rs:293-355,416-425`), ForceClose releases directly
(`:658-675`), and handler composition restores then publishes owners
(`participant/production/handler.rs:79-146`). W1b therefore **designs** new
callers; it does not describe them as already appending.

The fixed protocol facts are:

1. `DetachedBindingTransition` and `DiedBindingTransition` each have Committed
   and Pending states. Only Died currently exposes a projection method; W1b adds
   the identical sealed Committed-only method to Detached
   (`crates/liminal-protocol/src/lifecycle/binding.rs:574-628`).
2. `ActiveBinding::finish_detached` and `finish_died` take a caller-supplied
   `BindingTerminalDisposition`. Committed contains order and delivery sequence;
   Pending contains order only (`binding.rs:630-685`).
3. `clean_disconnect` is exactly CleanDeregister and `server_shutdown` is the
   other orderly Detached cause (`binding.rs:687-728,747-753`). ConnectionLost,
   ProcessKilled, ProtocolError, and UncleanServerRestart are the closed Died
   causes (`:756-801`).
4. Ordinary fate consumes an ordinary `AttachCommit`, the exact
   `CommittedDiedTerminal`, and a resulting floor
   (`crates/liminal-protocol/src/lifecycle/attach.rs:164-188`). Recovered fate
   consumes a `FencedAttachCommit` plus `BindingFateObserved`; no Died terminal
   is an input (`crates/liminal-protocol/src/lifecycle/edge.rs:1007-1056`).
5. The public `Event::binding_fate_observed` accepts a caller-supplied floor and
   performs no measurement (`edge.rs:1477-1488`). Production must not call it
   with a server-computed maximum.
6. `LiveFrontierOwner` is the move-only owner of `ClaimFrontiers`,
   `ClosureAccounting`, retained charges, and the retained-row limit
   (`lifecycle/operations/live_frontier.rs:37-67,110-149`). Cursor floor
   computation uses the post-transition minimum member cursor, candidate high
   watermark, hard observer progress, current floor, and cap floor
   (`lifecycle/cursor_facts.rs:430-505,558-624`). Those are the authoritative
   inputs to W1b's new protocol selector.
7. Unit 2 requires source append/flush before Advance, cold first-touch repair
   before owner or handshake publication, and loud disagreement
   (`docs/design/F0C-UNIT2-SERVERPUSH-PRODUCER.md:505-526`). A dead target drops
   only its targeted wake after Advance (`:528-555`).
8. `ConversationAuthority` remains the sole conversation owner; it contains the
   shell, move-only frontier, outbox, slots, checked allocators, observer
   witnesses, and durable observer progress
   (`participant/production/state.rs:127-169`). The test-only crash repository
   is precedent, never a production owner.

## 2. Decision A — closed connection classification and live producer census

### 2.1 One typed connection-fate entry point

`ParticipantSemanticHandler` gains one non-request method that consumes a
`ConnectionFateWorkItem`:

```text
ConnectionFateWorkItem {
    connection_incarnation,
    class: CleanDisconnect | ServerShutdown | ConnectionLost | ProtocolError,
    tracked_conversations: sorted Vec<ConversationId>,
}
```

The TCP and WebSocket process must preserve this closed class to the terminal
funnel. `FrameAction::Close` therefore becomes a typed close action rather than
one undifferentiated value. A liminal `Frame::Disconnect` selects
CleanDisconnect; ForceClose selects ServerShutdown; transport loss selects
ConnectionLost; a terminating decode/protocol-state refusal after binding
selects ProtocolError. Auth refusal, store failure, pressure failure, encoder
failure, and internal invariant failure do not masquerade as ProtocolError.

The process first durably opens the bounded intent in §3, then invokes the
participant handler, and only after intent completion may deregister publication,
drop the tracked map, or release conversations. Each conversation is locked
independently. The existing connection registry lock is never held while a
conversation lock or durable flush is held.

### 2.2 Died table

| Died cause | exact current event | ruling, owner, lock, trigger |
|---|---|---|
| ConnectionLost | TCP EOF/read/write loss reaches direct release today (`connection/process.rs:293-355`); WebSocket abrupt I/O/protocol transport loss does likewise (`connection/websocket/process.rs:292-369`). | The typed transport-loss funnel opens the connection intent before release. `ConversationAuthority` owns each matching Bound epoch under its per-conversation mutex and invokes `connection_lost`. TCP FIN and WebSocket Close without a liminal Disconnect remain transport loss because neither is the protocol's clean-deregister evidence. |
| ProtocolError | TCP canonical decode distinguishes incomplete buffering from terminal decode error; WebSocket complete malformed messages and contract violations are terminal (`connection/process.rs:865-873`; `connection/websocket/process.rs:410-473`). | Only a terminating protocol/decode refusal after a participant binding exists selects this cause. The same handler/owner/mutex invokes `protocol_error`; pre-auth and non-protocol internal failures append none. |
| UncleanServerRestart | Startup flushes the new server incarnation before exposing it; the participant handler restores before publication (`participant/incarnation_stream.rs:270-301`; `participant/production/handler.rs:79-146`). | Startup first resumes all open §3 intents using their stored original class, then under each conversation mutex invokes `unclean_server_restart` for any remaining prior-incarnation Bound epoch. Died, specific-fate completion, and observer repair finish before owner/service publication. |
| ProcessKilled | Application linked EXIT belongs to `ConversationActor`, not participant bindings; supervisor reap has no exact nonblocking connection exit reason (`connection/conversation.rs:173-188,239-267`; `connection/supervisor.rs:2036-2069`). | No production row. The exact future consumer, trigger, owner, and oracle remain in §14. Transport loss and restart cover observable network binding death without inventing this cause. |

### 2.3 Detached table — coequal, not an appendix

| Detached cause | exact current event | ruling, owner, lock, trigger |
|---|---|---|
| CleanDeregister / clean Disconnect | `Frame::Disconnect` is admitted pre-auth as a clean bow-out and currently returns an untyped Close (`crates/liminal-server/src/server/connection/apply.rs:35-59`). TCP and WebSocket then call normal close and release state (`connection/process.rs:416-425`; `connection/websocket/process.rs:480-495`). | The apply arm returns typed CleanDisconnect. If participant bindings exist, the process opens §3's intent before normal-close drain/release. Under each conversation mutex, the handler runs the exact dormant `ActiveBinding::clean_disconnect` producer and appends/flushed Detached. A pre-auth Disconnect with no binding opens no participant work. |
| ServerShutdown | TCP ForceClose sends shutdown, drains, releases, and finishes without participant terminalization (`connection/process.rs:658-675`); WebSocket has the parallel ForceClose arm (`connection/websocket/process.rs:808-825`). | ForceClose supplies typed ServerShutdown before either transport releases state. The same intent owner and conversation lock invoke `ActiveBinding::server_shutdown` for exact matching Bound epochs. NotifyShutdown alone does not terminalize; the actual orderly ForceClose trigger does. |
| Superseded | A successful superseding attach already commits the old `Detached(Superseded)` terminal inside the atomic Attached transition (`lifecycle/attach.rs:263-298,395-449`). | No standalone W1b Detached row. Superseded remains the closed Superseding mode of v3 Attached in §5; a second fate source would double-own the terminal. |
| Explicit Detach request | Production currently always allocates a committed terminal pair (`participant/production/ops_session.rs:120-128`). | The existing v3 Detached request source remains canonical, but §7 routes it through the same protocol disposition selector so its real observer-blocked Pending state is reachable and durable. Clean connection paths use ConnectionClose source mode, never a fabricated request. |

A slot not Bound to the supplied connection incarnation, an already Pending or
Detached slot, an untracked conversation, or an already completed occurrence
cannot append a second row. Drop remains a non-fallible resource backstop and
never appends (`connection/process.rs:813-833`;
`connection/websocket/process.rs:945-959`).

## 3. Decision B — bounded durable connection-fate intent

### 3.1 Why the r1 loop is insufficient

Appending independently for sorted conversations is not enough: a crash after
conversation `i` loses the volatile tail `i+1..K`. W1b therefore adds a durable
work intent before the first conversation transaction. This is an explicit
exception to the conversation-only source stream, not a second fate repository:
it carries work identity, not terminal/floor state; every actual fate remains in
the owning conversation's operation log.

The coordinator's bound is retained verbatim: **an unbounded intent row trades
volatile tail-loss for W7-species growth; the intent's size must be bounded by
the connection's tracked-conversation count with the bound named, and its
lifecycle (created when, completed when, reclaimed when) explicit.**

### 3.2 Owner, exact bound, and row grammar

The **one server-wide started incarnation-stream authority** owns the intent.
It already serializes durable startup/allocation inputs under one append head
(`participant/incarnation_stream.rs:1-19,220-248,421-470`). W1b bumps that event
codec from v1 to v2 with frozen-v1 prefix decoding and two new tagged inputs:

```text
OpenConnectionFate {
    connection_incarnation,
    class,
    declared_conversation_bound,
    conversations: sorted, unique Vec<ConversationId>,
}
CompleteConnectionFate { open_event_sequence }
```

The hard bound is the signed
`ParticipantConfig::max_semantic_conversations_per_connection`. The connection's
`ParticipantConnectionConversations` is a sorted set populated only after a
semantic commit and already bounded by that exact stage-6 limit
(`participant/dispatch.rs:27-74`; `config/types.rs:484-492`). Open refuses unless
`conversations.len() <= declared_conversation_bound ==` the live signed limit,
the ids are strictly increasing, and the incarnation/class are canonical. Thus
one Open row is at most one fixed header plus that connection's bounded tracked
conversation count—not an unbounded bag and not a server-global snapshot.

The incarnation authority mutex is acquired to append/flush Open and released
before any conversation mutex is taken. After all targets complete, it is
reacquired to append/flush Complete. This preserves one lock order and prevents
connection/conversation inversion.

The same authority owns a checked in-memory set of unmatched Open sequences. Its
server-wide hard bound is the startup-signed `LimitsConfig::max_connections`, the
existing cap on concurrently admitted live connections
(`crates/liminal-server/src/config/types.rs:270-284,316-340`). Startup admits no
new connections until it has completed every restored Open, so historical Opens
cannot overlap a new admitted generation. During live service each connection
can own at most one Open, and the listener can have at most `max_connections`
connections; an attempted second Open or over-bound set is a typed invariant
failure before append. Complete removes the sequence from this set.

### 3.3 Lifecycle and replay

1. **Created:** after the terminal event has an exact typed class and before
   publication deregistration, conversation release, or tracked-map destruction.
   Open flush is the fold's outer durability barrier.
2. **Completed per conversation:** in sorted order, restore/take the
   `ConversationAuthority`, find every Bound slot for the exact incarnation, and
   flush the Died or Detached row plus any immediately executable specific-fate
   row. A prior matching fate row is idempotent completion; mismatched class,
   epoch, or source is a typed conflict.
3. **Completed globally:** only after every listed conversation is either
   durably terminalized or proved to contain no matching Bound slot does the
   incarnation authority append/flush Complete.
4. **Replayed:** incarnation-stream replay keeps only unmatched Open rows in a
   checked map. Before startup publishes the participant service, it hands each
   open item to the same handler, processes the entire tail, appends Complete,
   then performs the remaining UncleanServerRestart pass. No polling or absence
   guess is involved; Open is positive durable work authority.
5. **Reclaimed:** Complete removes the intent from the replay-time/live active
   map immediately. Paired Open/Complete bytes remain in the append-only
   incarnation stream because `DurableStore` exposes append/read/CAS/scan/flush,
   not truncate or delete (`crates/liminal/src/durability/store.rs:20-58`). This
   is **logical reclamation, not a false physical-reclamation claim**. Page replay
   retains only concurrently unmatched bounded intents, never a vector of all
   historical completed intents. The two event rows add constant event count per
   connection to an already connection-linear allocation stream; the variable
   payload is bounded by the signed tracked-conversation cap.

A process failure after terminalizing a middle conversation leaves Open without
Complete. Restart observes prior rows for the prefix and durably terminalizes
every tail binding before service publication. Failure to finish any tail item
is startup-fatal and leaves the intent open for the next exact replay; it never
publishes a partial owner set.

### 3.4 Chosen live failure policy: fatal after Open

W1b chooses policy **(a)**. Once Open flushes, any non-idempotent per-conversation
protocol, lock, append, flush, replay, or observer-repair failure latches
`ParticipantServiceFatal::ConnectionFateIntentIncomplete { open_sequence,
conversation_id }`. The participant service atomically stops accepting semantic
requests, owner publication, observer classification, and new connection-intent
opens; the server stops accepting connections and exits through its normal fatal
composition path. A fresh startup must replay and Complete the Open before
re-exposing service. There is no live background runner, retry loop, or request
handoff, so the policy cannot silently depend on a task that the design did not
name.

After the fatal latch, transport Drop/release **may** close the socket, cancel
pending replies, deregister weak publication targets, and release volatile
connection actors: Open already owns the exact sorted conversation list. It may
not append Complete, erase/reclassify Open, append a different cause, publish any
conversation owner, or continue participant service. Before Open flush, by
contrast, Drop/release is forbidden because no durable authority owns the list.
A non-crash injected middle failure therefore first produces an observable fatal
latch and service refusal, then controlled restart completes every tail item from
Open; §12 names that fixture separately from abrupt process crash.

## 4. Decision C — four exact v3 fate rows, sources, and replay

### 4.1 `StoredOperationV3` grammar

The v3 enum has distinct exact tags and no aliases:

```text
Died { row: StoredDied }
Detached { row: StoredDetached }
Ordinary { row: StoredOrdinaryFate, event: Vec<u8> }
Recovered { row: StoredRecoveredFate, event: Vec<u8> }
```

The v3 Detached tag deliberately replaces the live v3 representation of the
current explicit-detach tag; frozen v2 retains its old shape and converts as
specified below.

| class | exact v3 fields | invariant |
|---|---|---|
| Died | `participant_id`; `binding_epoch`; `cause: ConnectionLost | ProcessKilled | ProtocolError | UncleanServerRestart { prior_server_incarnation }`; `terminal_order`; `disposition: Committed { terminal_seq } | Pending`; `connection_intent_sequence: Option<u64>`; `specific_fate_intent: None | Ordinary { attached_source_sequence } | Recovered { attached_source_sequence, prior_binding_epoch, marker_delivery_seq }` | Cause is closed. Restart incarnation equals the epoch-derived value. Connection causes reference Open; startup repair may use None. The specific intent is selected from the consume-once slot token before Died flush and is positive durable completion authority. |
| Detached | `participant_id`; `binding_epoch`; `cause: CleanDeregister | ServerShutdown`; `terminal_order`; `disposition: Committed { terminal_seq } | Pending`; `source: ExplicitRequestCommitted { request, secret_verified, verifier, receiving_epoch, event } | ExplicitRequestPending { request, secret_verified, verifier, receiving_epoch, observer_baseline } | ConnectionClose { connection_intent_sequence }` | ExplicitRequestCommitted is legal only with Committed and retains the existing canonical committed shell event. ExplicitRequestPending is legal only with Pending, stores **no event**, and re-mints the exact pending cell/outcome from inputs. ConnectionClose must reference matching Open. Superseded is forbidden because v3 Attached owns it. |
| Ordinary | `participant_id`; `last_dead_binding_epoch`; `ordinary_attached_source_sequence`; `terminal_source: DiedCommitted { died_source_sequence } | PendingDiedFinalized { died_source_sequence, finalizer: Left { source_sequence } | FencedAttached { source_sequence } }`; `committed_terminal_audit: { cause, transaction_order, terminal_seq }`; `resulting_floor` | DiedCommitted requires the lower Died row itself be Committed. PendingDiedFinalized requires the Died row remain Pending forever, resolves the exact lower finalizer row, and validates its committed terminal before consuming Ordinary authority. `event` is canonical `BindingFateOperation::from_ordinary` bytes. |
| Recovered | `participant_id`; `last_dead_binding_epoch`; `died_source_sequence`; `fenced_attached_source_sequence`; `prior_binding_epoch`; `marker_delivery_seq`; `resulting_floor`; `presentation: DiedCommittedOwns | RecoveredOwnsAndReservesFinalizer` | It consumes an earlier Died intent but no Died terminal protocol input. DiedCommittedOwns is non-presenting because committed Died already owns the occurrence. RecoveredOwnsAndReservesFinalizer is legal only after Pending Died: this row presents and durably reserves the occurrence, and a later exact finalizer must consume the reservation through §4.5's non-presenting mode. `event` is canonical `from_recovered` bytes. |

Conversation id remains stream identity and is revalidated against every
protocol-produced value. Every epoch uses checked `StoredBindingEpoch`
reconstruction (`participant/production/log.rs:502-527`). Forward/self
references, wrong class, wrong intent, event drift, cause/mode mismatch, unknown
tags, or unchecked arithmetic are typed restore failures before publication.

Frozen v2 Detached converts only as follows: its request/verifier/receiving epoch,
terminal order/sequence, and event become v3 ExplicitRequestCommitted +
CleanDeregister + Committed. There is no v2 Pending request or connection-close
row to invent.

### 4.2 Four canonical live producers

| row | exact production caller and typed input | owner, lock, trigger, barrier |
|---|---|---|
| Died | `ConversationAuthority::apply_connection_fate` receives one referenced Open work item, exact Bound slot, and §7 protocol terminal-admission decision; startup uses the same method with UncleanServerRestart | Conversation owner under its mutex; classified loss/protocol event or startup restore. Died append/flush precedes slot installation, specific completion, Advance, and publication. |
| Detached | The same method receives CleanDisconnect/ServerShutdown work, or existing `apply_detach` receives an explicit verified request; both consume §7's selector output | Same owner/mutex. Clean Disconnect, orderly ForceClose, or explicit request is the trigger. Detached append/flush precedes transition installation and projection. |
| Ordinary | `ConversationAuthority::complete_binding_fate_intent` receives `PendingSpecificFate::Ordinary { died_source, terminal_source, authority }`. `terminal_source` is either that Committed Died row or an exact lower Left/Fenced-Attached row that committed the terminal for an immutable Pending Died | Same owner/mutex. The trigger is successful Committed Died flush, or a real finalizer/replay satisfying a Pending Died intent. Protocol consumes the private ordinary authority and the terminal reconstructed from the selected source, measures the floor, and Ordinary flush precedes closure/observer handling. |
| Recovered | The same completion method receives `PendingSpecificFate::Recovered { died_source, recovered_authority }`, where `recovered_authority` is the sole private by-value authority emitted by §5's attach commit | Same owner/mutex. Successful Died flush or explicit-intent replay is the trigger. Protocol measures the floor and consumes the private authority. No caller retains `FencedAttachCommit`; no Died terminal is constructed or passed. Pending-Died presentation uses the durable reservation grammar in §4.5. |

### 4.3 Protocol-owned floor measurement

W1b adds one consuming protocol operation on `LiveFrontierOwner`, not a server
formula:

```text
prepare_binding_fate(owner, sealed_fate_token, terminal_source_if_ordinary)
    -> PreparedBindingFate { next_owner, fate, event }
```

It takes the move-only coupled frontiers/accounting/retained charges, the exact
slot participant and cursor from the sealed token, current hard observer
progress, candidate high watermark, current retained floor, and cap floor. It
removes/releases that binding in protocol state, recomputes the post-fate floor
with the existing floor transition, constructs `BindingFateObserved` internally,
and returns an Ordinary or Recovered fate plus the next owner. The server can
read `fate.resulting_floor()` to persist/audit it but cannot supply or override
it. Production stops calling the raw public constructor; it becomes crate-private
or remains unreachable from the production module. This closes the current hole
where the constructor accepts any caller number (`edge.rs:1477-1488`).

The Ordinary branch additionally consumes the exact committed Died terminal
reconstructed from its closed terminal-source tag. The Recovered branch consumes
only §5's private recovered authority and internally minted event; the old
fate-capable method on public `FencedAttachCommit` no longer exists. A protocol
refusal returns the unchanged boxed/private authority and no row is appended.

### 4.4 Died-to-specific-fate choreography

W1b chooses the permitted **durable pending intent completed on replay** design,
not fictitious two-row atomicity:

1. The consume-once slot token selects None/Ordinary/Recovered before the Died
   append. Died persists that exact intent and flushes.
2. Only after Died flush does `complete_binding_fate_intent` run the protocol
   measurement and append/flush the matching specific row.
3. The specific row consumes the intent by exact Died source sequence. A second
   consumer, wrong class, or wrong Attached source is a typed conflict.
4. A crash after Died flush but before the specific row leaves a positive open
   intent. Validate-first replay reconstructs the token and Died transition;
   after the complete durable stream is validated and applied, it appends the
   deterministic completion at the current checked head, flushes, replays that
   row, and only then publishes. This is not “row absent, guess fate”: the Died
   row explicitly commands one named completion.
5. A Pending Died row is immutable: its disposition never changes to Committed.
   Ordinary waits until a lower Left or Fenced-Attached finalizer commits the
   terminal, then persists that finalizer as its terminal source and audits the
   resulting terminal. It never labels the Died row Committed.
6. Recovered may complete immediately after Pending Died because it consumes no
   terminal. That Recovered row presents and durably reserves the occurrence;
   the later finalizer must encode and consume the reservation through a typed
   non-presenting transition. Silence or numeric suppression is forbidden.
7. A Died row with no specific token is complete history. Enrollment-origin
   bindings currently select None; no Ordinary authority is fabricated.

### 4.5 Closed Pending-Died completion grammar

There is one grammar, used by live application, validation, replay, and W1a:

| pending-Died branch | required durable sequence | presentation owner and validation |
|---|---|---|
| Ordinary finalized by Leave | Died Pending `<` Left with `pending_source_sequence = Died` `<` Ordinary with `PendingDiedFinalized::Left = Left` | Left commits the exact Died terminal and presents. Ordinary repeats cause/order/terminal sequence, resolves both lower rows, consumes the ordinary authority, and is non-presenting because Left owns the occurrence. The Died row remains Pending. |
| Ordinary finalized by Fenced Attached | Died Pending `<` Fenced Attached whose composed-terminal source is Died `<` Ordinary with `PendingDiedFinalized::FencedAttached = Attached` | Attached commits/audits the exact Died terminal and presents. Ordinary resolves the composed terminal and is non-presenting. The Died row remains Pending. |
| Recovered before Leave finalization | Died Pending `<` Recovered with `RecoveredOwnsAndReservesFinalizer` `<` Left with `ConsumeRecoveredReservation { recovered_source_sequence }` | Recovered presents its measured floor and reserves the occurrence key. Left still commits the terminal/member transition but the protocol returns `NonPresentingFinalizerCommit`; it has no projection accessor. Replay requires reciprocal source/key references. |
| Recovered before Fenced-Attached finalization | Died Pending `<` Recovered with reservation `<` Fenced Attached whose composed-terminal presentation is `ConsumeRecoveredReservation { recovered_source_sequence }` | Recovered presents. Attached consumes Pending and rotates state but returns the same typed non-presenting finalizer result. The composed terminal remains fully audited; no Attached terminal witness is constructed. |

V3 Left and Fenced Attached therefore carry a closed
`FinalizerPresentation = PresentEnclosing | ConsumeRecoveredReservation {
recovered_source_sequence }`. The consuming tag is legal only when the referenced
lower Recovered row names this exact Pending Died source, participant, epoch, and
`RecoveredOwnsAndReservesFinalizer` mode. `PresentEnclosing` is illegal after such
a reservation. The occurrence router validates this before constructing either
`LiveLeaveCommit`/Attached projection or the projection-free
`NonPresentingFinalizerCommit`; it does not construct a projection and then drop
it. An open reservation with no later finalizer is legal pending history.
Duplicate consumption, a finalizer claiming a missing/wrong reservation, or a
finalizer after a reservation that silently omits the tag is a typed
`FateOccurrenceConflict`.

### 4.6 Replay and W1a join at equal depth

| class | replay transition | source / occurrence audit / lineage |
|---|---|---|
| Died | Reconstruct exact Bound slot and §7 disposition, invoke stored cause once, validate state/terminal/order, and restore the explicit specific intent. Pending creates no terminal/projection. | base sequence + Died; `(conversation, participant, epoch)` plus terminal audit; `ParticipantTerminal` lineage for Committed. |
| Detached | Resolve ExplicitRequest or referenced ConnectionClose, invoke clean_disconnect/server_shutdown or committed/pending explicit detach exactly once, and validate terminal/cell/state. | base sequence + Detached; same occurrence plus cause/source/terminal audit; `ParticipantTerminal` lineage for Committed. |
| Ordinary | Resolve ordinary Attached and the closed terminal source. DiedCommitted resolves a committed Died; PendingDiedFinalized resolves immutable Pending Died plus exact lower Left/Fenced Attached and reconstructs its committed terminal. Re-run measurement and byte-check event. | base sequence + Ordinary; same occurrence plus Died/Attached/finalizer/terminal audits; `BindingFateFloor` lineage. |
| Recovered | Resolve earlier Died and fenced Attached, restore the private recovered authority, re-run measurement with no Died terminal, byte-check event, and validate DiedOwned versus reservation presentation mode. | base sequence + Recovered; same occurrence plus Died/Attached/marker/reservation audits; `BindingFateFloor` lineage. |

Every candidate enters §8's router before W1a's existing
`record_observer_progress_projection`. Detached gains the same sealed
Committed-only projection as Died. Pending rows contribute no candidate. The
existing W1a source-order, uniqueness, and per-lineage checks remain the final
backstop (`participant/production/observer_progress.rs:352-431`).

## 5. Decision D — complete fenced Attached v3 and consume-once authority

### 5.1 Closed mode shape

The common v3 allocation retains the current exact binding epoch, attach secret,
Attached order/sequence, receipt/provenance deadlines, and admitted clock. The
current v2 allocation has only an optional superseded terminal sequence as mode
evidence (`participant/production/log.rs:420-438`). V3 removes that ambiguous
option and requires one closed tag:

| `StoredAttachModeV3` | exact fields and validation |
|---|---|
| Ordinary | no mode payload; request marker must be None; prestate must be Detached with ordinary closure admission. |
| Superseding | `prior_binding_epoch`; `terminal_transaction_order`; `terminal_delivery_seq`; request marker None. Prior epoch is the exact Bound prestate; terminal order equals common Attached order; protocol verifies the indivisible Superseded/Attached handoff. |
| Fenced | `prior_binding_epoch`; `marker_delivery_seq`; `marker_source_sequence`; `proof: { detached_credential_recovery, predecessor_debt, fenced_resulting_floor, successor }`; `composed_terminal: None | Some { kind: Died | Detached, cause, transaction_order, delivery_seq, pending_source_sequence, presentation }` | Request marker equals the marker. Proof mirrors complete `FencedAttachCommitRestore`: predecessor, nonzero debt, participant, marker, old/new epochs, measured fenced floor, and restricted successor (`lifecycle/storage.rs:1538-1606`). The exact source sequence resolves one durable marker row; full frontier restoration must mint the one-use validation authority. Optional terminal must match Pending/source and §4.5 presentation mode; None requires Detached state/history (`lifecycle/attach.rs:300-357`). |

The Fenced proof is not just marker number + epochs. Existing
`FencedAttachCommit` also owns the restricted closure successor
(`lifecycle/edge.rs:955-1005`), and restoration validates participant, marker,
epoch, debt, predecessor, and successor. V3 persists every input needed to
re-run that transition. `StoredRecoveredFate` references this Attached row rather
than duplicating its full proof.

**Marker identity ruling: no digest.** Conversation stream identity plus the v3
`marker_source_sequence`, expected marker row kind, and marker delivery sequence
select one durable row. Replay decodes that complete source row and validates its
causal row/provenance/target binding/occurrence through total
frontier restore, and only then obtains the private `ValidatedMarkerRecord`.
That token cannot be fabricated from raw ids and is consumed after restore
(`lifecycle/claim_frontier.rs:1490-1541`). A hash would add an unversioned second
identity while still requiring this full validation, so no
`marker_record_digest` field or function exists.

### 5.2 Frozen-v2 Attached mapping

The frozen decoder preserves every field before conversion:

- marker None + `superseded_terminal_seq = None` maps to Ordinary;
- marker None + terminal Some maps to Superseding. The exact prior epoch is
  derived losslessly from the replay prestate Bound slot, and terminal order is
  the stored Attached order, as today's verified transition requires;
- marker Some cannot map to Fenced because v2 stores no predecessor debt,
  durable marker source, successor, or composed-terminal audit. Current
  production refuses every marker-bearing attach before allocation/commit
  (`participant/production/ops_attach.rs:97-103`), so no legal production v2
  history is lost. If such manually/corruptly written v2 bytes exist, replay
  returns typed `V2AttachedFencedProofUnavailable { sequence }`; it never invents
  a proof;
- terminal Some with a Detached prestate, terminal None with a Bound prestate,
  or any marker/terminal contradiction keeps the existing loud mode refusal.

V3 never infers mode from options or prestate: the tag is mandatory and every
redundant request/prestate field is cross-checked.

### 5.3 Consume exactly once

R2's split was insufficient at the bytes. `FencedAttachCommit` is publicly
Clone + Copy and exposes public `recovered_binding_fate(self, ...)`, while
`AttachMode::Fenced` merely borrows it; the pre-attach caller can retain a
fate-capable copy (`lifecycle/edge.rs:955-968,1007-1056`;
`lifecycle/attach.rs:215-222,300-357`). R3 changes ownership itself:

1. Remove `Clone` and `Copy` from public `FencedAttachCommit`; retain only Debug,
   PartialEq, and Eq. Its observational accessors borrow `&self`.
2. `verify_fenced_attach` accepts `proof: FencedAttachCommit` **by value** and
   returns a non-Clone `VerifiedAttachCommit<F>` whose private
   `AttachMode::Fenced` owns the proof, with no lifetime/borrow. A verification
   refusal returns an owning error wrapper containing the unchanged member and
   proof, never a copied proof.
3. `commit_attach` consumes that verified value by value. Fenced commit moves the
   proof's fate-capable state into one private `RecoveredBindingAuthority` field
   inside `AttachCommit`; Installed state gets only non-fate transition audit.
4. Remove the public `FencedAttachCommit::recovered_binding_fate`. Its logic moves
   to a crate-private consuming method on `RecoveredBindingAuthority`; only that
   authority can produce `RecoveredBindingFate`.
5. The final production boundary consumes once:

```text
AttachCommit::into_slot_and_fate(self)
    -> (InstalledAttachState<F, V>, SealedBindingFateToken)

SealedBindingFateToken = None | Ordinary(OrdinaryFateAuthority)
                              | Recovered(RecoveredBindingAuthority)
```

`InstalledAttachState` owns member, active binding, detach cell, Attached record,
outcome, and receipt facts. `ConversationAuthority` stores exactly one private
slot token and moves it into §4.3. On fate refusal it receives the same authority
back; on success it is consumed. No public/borrowing fate accessor remains.
Oracle 25 includes trybuild cases that attempt to retain the pre-attach proof,
copy/clone it, invoke recovered fate after moving it into verification, and call
the old public fate method; every case must fail to compile. Live and replay each
exercise one successful by-value chain.

## 6. Decision E — participant schema v3 and cross-page phase

All new participant appends use schema v3 on the existing conversation stream.
The decoder remains two-stage but returns the version rather than dropping it:

```text
DecodedOperation { sequence, schema_version, operation }
OperationSchemaPhase = V2Prefix | V3Suffix
read_page(from_sequence, phase)
    -> DecodedOperationPage { rows, next_phase }
```

The replay loop owns `phase` outside the page loop and supplies the returned phase
to the next call. Version 2 decodes only through frozen `StoredEntryV2` and the
exact eight-variant enum; version 3 decodes only through `StoredEntryV3`. V2Prefix
accepts v2 or transitions once to V3Suffix. V3Suffix rejects v2 with typed
`SchemaVersionTransition { sequence, previous: 3, actual: 2 }`. Unknown versions
remain `SchemaVersion(actual)`; missing/malformed version or row is typed
serialization corruption. No default, alias, skip, dual decode, or shape inference
is allowed.

The replay validator makes a first bounded-page pass carrying both schema phase
and fate grammar/occurrence state; only after that complete pass succeeds does
the existing bounded-page apply pass mutate reconstructed authority or reconcile
Advance. This preserves W3's page-bounded discipline while ensuring an
already-flushed extension row cannot be applied before a later base-log temporal
error is discovered.

The page-boundary fixture places v2 rows through sequence
`READ_BATCH_SIZE - 2`, a v3 row at the final slot of that page, and a v2 row at
the first slot of the next page. It must report the transition error at that
second-page sequence. A sibling fixture accepts a complete v2 page followed by a
v3 page. Current code cannot satisfy these because it drops version at decode and
carries only sequence/operation (`production/log.rs:90-108`;
`production/ops_session.rs:285-342`).

Old v2 binaries see the first v3 prefix and return their existing typed
`OperationLogError::SchemaVersion(3)` before enum decoding. New binaries accept a
contiguous v2 prefix. V2 histories contain no W1b fate rows and no historical
fates are synthesized; a later real event starts the v3 suffix.

## 7. Decision F — protocol-owned Committed-versus-Pending selector

### 7.1 Exact selector and production state

The Died/Detached cause methods require disposition as input; they do not decide
it (`binding.rs:630-685,756-801`). Allocation values are also inputs to the exact
candidate charge: today the server serializes a terminal-shaped row only after it
has order/sequence (`participant/production/frontier.rs:126-147`) and passes the
resulting `RetainedRecordCharge` into protocol frontier application
(`lifecycle/operations/live_frontier.rs:695-740`). W1b therefore chooses a sealed
two-stage API rather than allocating before an unpriced admission:

```text
LiveFrontierOwner::prepare_binding_terminal(
    active_binding,
    cause_class,
    next_transaction_order,
    next_delivery_sequence,
    hard_observer_progress,
)
 -> PreparedBindingTerminal { unchanged_owner, CandidateTerminalKey }
  | PrepareRefused { unchanged_owner, typed_reason }

BindingTerminalCandidateCharge {
    conversation_id,
    participant_id,
    binding_epoch,
    transaction_order,
    delivery_seq,
    encoding: ParticipantLifecycleV3CanonicalJson,
    charge: RetainedRecordCharge,
}

PreparedBindingTerminal::admit(candidate_charge)
 -> Commit { next_owner, CommittedBindingTerminalPosition }
  | Pending { next_owner, PendingBindingTerminalPosition, blocked_at_observer }
  | AdmitRefused { unchanged_owner, typed_reason }
```

Prepare validates authority and checked candidate order/sequence but mutates
nothing and consumes no allocator. Its sealed `CandidateTerminalKey` exposes only
the exact fields needed to encode the candidate. The server builds
`CanonicalLifecycleRowV3::BindingTerminal { conversation_id, participant_id,
binding_epoch, admission_order, delivery_seq }`, serializes it with the v3
canonical JSON encoder, converts byte length with checked arithmetic, and wraps
`ResourceVector::new(1, bytes)` in the keyed `RetainedRecordCharge`.
Storage framing stays server-owned, but the encoding tag makes the charge contract
versioned and prevents v2/v3 ambiguity.

Admit requires every repeated conversation/participant/epoch/order/sequence to
equal the sealed key, the charge's delivery/order keys to match, encoding to be
v3, and encoded entry count to be exactly one. It then uses the coupled
ClaimFrontiers/ClosureAccounting, retained charges/row limit, current retained and
cap floors, hard observer progress, and candidate charge. **Committed** consumes
order+sequence only when the row is admissible. **Pending** is only the
observer-blocked result, consumes order but not candidate sequence, and persists
the observed baseline. Wrong charge, capacity/arithmetic exhaustion, wrong
authority, or closure mismatch returns `AdmitRefused` with the original
`LiveFrontierOwner` and no allocator movement. Live and replay call the identical
v3 encoder and both stages; neither can hand protocol an unkeyed byte count.

Production passes only the sealed admitted result into `clean_disconnect`,
`server_shutdown`, or Died. The row stores the selected disposition and replay
re-runs prepare/encode/admit from the same prestate.

### 7.2 Required production changes

- Explicit detach stops unconditionally calling `allocate_position` and gains
  its real `start_blocked_detach` route; current code always commits and treats
  pending replay as impossible (`production/ops_session.rs:103-128`;
  `lifecycle/detach.rs:501-544`). Committed selects
  `ExplicitRequestCommitted` and persists the existing shell event. Pending
  selects `ExplicitRequestPending` and persists request, verifier,
  receiving epoch, terminal admission order, and exact `observer_baseline`, but
  **no event bytes**. Replay calls `start_blocked_detach` and validates its
  PendingFinalization, PendingDetach cell, refused epoch, and backpressure outcome
  against those inputs. It never invokes the committed aggregate event decision
  or fabricates a canonical event for a transition that produces none.
- Clean Disconnect and ServerShutdown use the same selector and v3 Detached
  ConnectionClose row. They are the new non-request Pending Detached producers.
- Died causes use the same selector; a Pending Died row carries its specific-fate
  intent and waits only where §4.4 requires a committed terminal.
- Attach no longer treats all PendingFinalization as an invariant violation
  (`production/ops_attach.rs:126-138`). Only Fenced mode may consume it, and v3
  Attached must carry the exact composed-terminal sequence/cause/source audit.
  Ordinary and Superseding still reject Pending.

This rule makes every Pending-Detached acceptance oracle reachable from actual
production state, without a test-only row constructor.

## 8. Decision G — occurrence identity and temporal order

### 8.1 One four-class occurrence router

The key remains:

```text
(conversation_id, participant_id, binding_epoch)
```

Died Committed and Detached Committed present their exact terminal sequence;
Pending presents none. Ordinary and Recovered present their measured floor only
when no earlier canonical presentation owns the key. Died and Detached conflict
for one epoch. Duplicate same-class rows, alternate producers, wrong source
references, or Ordinary-versus-Recovered conflict are typed
`FateOccurrenceConflict` before observer mutation.

A compatible specific-fate row after Died remains a required durable closure fact
but cannot double-present an already committed Died occurrence. If Died was
Pending, Recovered presents its floor and atomically records
`RecoveredOwnsAndReservesFinalizer`; a later Left/Fenced-Attached row must consume
that exact reservation through the projection-free finalizer transition in §4.5.
An Ordinary intent cannot complete until a terminal exists: its referenced
Left/Attached finalizer presents, while the later Ordinary row is a validated
non-presenting closure fact. These ownership choices are durable row tags, never
silent suppression in the router.

### 8.2 Enforced Died-before-specific order

Every live Ordinary/Recovered row references a lower Died source sequence and
consumes that row's exact specific intent. The validate-first pass rejects:

- Ordinary or Recovered with no earlier Died source;
- a source sequence at or after the specific row;
- Recovered followed by Died for the same epoch;
- Died after any earlier Recovered-only presentation;
- a duplicate Died after an extension Advance already names the Recovered
  witness;
- an Ordinary PendingDiedFinalized source whose Died is not Pending, whose
  finalizer is not lower, or whose committed terminal audit differs;
- a Leave/Fenced-Attached finalizer after reserved Recovered that selects
  PresentEnclosing, omits/wrongly consumes the reservation, or repeats its
  consumption; or
- a Died/specific class or Attached-token mismatch.

Thus the legal order is **Died flush → specific row flush → optional Advance**.
For Pending-Died Recovered, the specific flush also durably reserves the
occurrence; finalization later consumes that reservation without a projection.
No polling waits for a row, and no Died can appear after Recovered. Recovered
still receives no Died terminal input: source order/reservation and protocol
transition inputs are distinct contracts.

For Died then Recovered, both rows and transitions survive replay, but Committed
Died presents once. For Pending Died then Recovered, Recovered presents once and
either Leave or Fenced Attached later makes an explicit non-presenting state
transition. A fabricated Recovered-then-Died history refuses during preflight
before authority or extension application. If an Advance for that Recovered is
already durable, its bytes remain unchanged, no second witness is produced, and
no owner is published.

## 9. Decision H — finalizer routing for Died and Detached

| initial/source state | real finalizer and canonical presentation | durable/replay rule |
|---|---|---|
| Died Committed | Died terminal sequence; later compatible Ordinary/Recovered does not present twice | Died source flush opens/finishes its specific intent. Replay invokes Died once and consumes the intent once. |
| Died Pending | no initial presentation | Replay restores exact cause/epoch/order and open specific intent; its row remains Pending permanently. Ordinary waits for a named finalizer; Recovered may present only with a reservation. |
| Detached Committed from Disconnect/Shutdown/request | Detached terminal sequence | Replay invokes exact source/cause once and installs committed terminal/cell as applicable. |
| Detached Pending from Disconnect/Shutdown/request | no initial presentation | Replay restores exact pending cause/epoch/order and, for explicit request, its event-free pending cell/outcome. |
| Pending Died + Ordinary finalized by Leave | Left presents; Ordinary later presents none | Left commits the terminal. Ordinary's PendingDiedFinalized::Left tag resolves Left and audits that terminal; Died remains Pending. |
| Pending Died + Ordinary finalized by Fenced Attached | Attached presents; Ordinary later presents none | Composed terminal names Died source. Ordinary resolves exact Attached source/audit; Died remains Pending. |
| Pending Died + reserved Recovered finalized by Leave | Recovered already presented; Left is typed non-presenting | Left stores/consumes recovered source reservation and commits terminal/member state through `NonPresentingFinalizerCommit`. |
| Pending Died + reserved Recovered finalized by Fenced Attached | Recovered already presented; Attached composed terminal is typed non-presenting | Attached stores/consumes reservation, rotates state, and exposes no terminal projection. `commit_attach` consumes Pending once (`lifecycle/attach.rs:325-357,432-449`). |
| Pending Detached finalized by Leave | only Left presents | Same generic PendingFinalization commit. No Died or Detached second candidate is minted. |
| Pending Detached composed by Fenced Attached | only Attached presents | Fenced mode persists exact Detached cause/order/sequence/source and PresentEnclosing. |
| Restart with pending row | no new initial transition | Restore Died/Detached Pending plus any Recovered reservation, then route a later durable Left/Fenced Attached through the exact selected mode. Never call finish twice. |
| Standalone pending finalization | excluded | No public row/handler is added. A future owner needs a ledger row before implementation. |

Correction to r1: current `StoredLeave` does **not** audit cause. It stores request,
verifier, receiving epoch, Left order/sequence, ended binding epoch, and optional
prior terminal sequence only (`participant/production/log.rs:332-342`). V3 Left
adds `pending_source_sequence: Option<u64>` plus `finalizer_presentation`.
PresentEnclosing stores no reservation. ConsumeRecoveredReservation stores the
exact lower Recovered source. Replay resolves the Died/Detached pending source for
cause and, when present, the Recovered row for occurrence ownership; it
cross-checks ended epoch and committed terminal sequence. V2 Left converts with
no pending source and PresentEnclosing. Cause is never inferred from the optional
terminal number.

## 10. Decision I — W1a tear-rider discharge

Delete the four local zero counters and their tuple assertion from the landed
same-participant lineage-regression test. The test directly exercises witness
state, reads the exact typed refusal, and compares
durable observer rows. It cannot execute arm removal, wake, publication, or
classification; wiring counters there would be test-only theater
(`participant/production/tests_w1a.rs:309-340`).

Retain the typed `SourceLineageRegression`, durable-row equality, enabled test,
and all real handler oracles. A source census proves the declarations and tuple
are absent. No W1b test may compare a constant/fresh fixed value to another
constant and call it observation; every assertion reads protocol output, decoded
durable bytes, store head, owner state, publication, or an instrumented production
point.

## 11. Ordering, crash cuts, and integration

For a connection fold:

1. classify one exact close event;
2. append/flush bounded Open under the incarnation owner, then release that lock;
3. for each sorted conversation, take its existing owner mutex;
4. restore/validate complete base and extension history first if owner is absent;
5. run §7 terminal admission and the exact Died/Detached transition;
6. append/flush the v3 source through `OperationLog`/`DurableAppend`;
7. install committed transition state and, for Died, complete the explicit
   specific-fate intent, persist a Recovered reservation, or retain Ordinary for
   an exact finalizer;
8. run the existing W1a read-only plan and fused Advance executor, respecting any
   typed non-presenting finalizer result;
9. release the conversation mutex and continue;
10. after every target succeeds, append/flush Complete under the incarnation
    owner; only then deregister/release/publish close completion. Any failure
    after step 2 takes §3.4's fatal path instead.

For a specific fate, Died source flush always precedes Ordinary/Recovered flush,
which precedes its Advance. A Recovered reservation precedes and is consumed by
its finalizer. For a clean fate, Detached flush precedes its Advance.
`handler_observer_reconcile` remains the only observer mutation path.

Exact crash cuts:

- before Open flush: no durable fold authority and no connection state may be
  destroyed; return terminal failure while state remains owned;
- after Open flush before first conversation: the live owner must Complete, or
  any failure latches service/server fatal and startup replay owns the full list;
- after a non-crash middle failure: already flushed prefixes remain idempotent,
  participant operations refuse under the fatal latch, Drop may release only the
  volatile transport, and controlled restart completes every tail from Open;
- after an abrupt middle crash: startup performs the same tail completion before
  accepting a connection;
- after Died/Detached flush before state/Advance: cold replay restores the source
  and repairs before publication;
- after Died flush before Ordinary/Recovered: the explicit specific intent
  deterministically appends its completion on replay;
- after Pending Died with Ordinary intent: only an event-driven lower Left/Fenced
  Attached source unlocks completion; the Died row stays Pending;
- after Pending Died with Recovered intent: Recovered may flush/present/reserve;
  its later finalizer must consume the reservation without presentation;
- after specific source before Advance: cold first touch repairs exact measured
  progress;
- after Advance before wake: only the exact weak-target wake may be dropped;
- after all conversations before Complete: replay sees all rows, treats them as
  idempotent, and appends Complete;
- after Complete: no active intent remains; repeated startup performs no fold;
- at v2/v3 or page boundary: schema/fate preflight refuses before apply or
  publication.

## 12. Acceptance oracle census

Each exact name appears once in this brief. These 48 names are implementation
floor, not suggestions. Fixtures use deterministic append/flush gates and actual
observations; no sleeps, eventual assertions, or constant-versus-constant claims.

| # | exact oracle | required proof |
|---:|---|---|
| 1 | `died_binding_transition_projects_terminal_sequence_only_when_committed` | Independent real Committed/Pending transitions; read terminal projection versus absence. |
| 2 | `ordinary_binding_fate_projects_measured_resulting_floor` | Consume real ordinary authority and exact committed Died; read protocol-measured distinguishing floor. |
| 3 | `recovered_binding_fate_projects_measured_resulting_floor` | Consume fenced token and internally minted event; read measured floor and prove no Died terminal input. |
| 4 | `detached_binding_transition_projects_terminal_sequence_only_when_committed` | Add/read sealed Detached Committed projection; Pending is absent. |
| 5 | `clean_disconnect_appends_detached_source_before_transport_teardown` | Real Frame::Disconnect with Bound slots flushes CleanDeregister Detached before TCP/WS release; pre-auth no-binding appends none. |
| 6 | `server_force_close_appends_shutdown_detached_source_before_release` | TCP and WebSocket ForceClose flush ServerShutdown Detached before deregistration/release; NotifyShutdown alone does not. |
| 7 | `connection_lost_appends_died_source_before_transport_teardown` | TCP/WS loss flushes ConnectionLost Died; clean Disconnect and ForceClose select Detached instead. |
| 8 | `protocol_error_appends_died_source_only_for_bound_terminal_refusal` | Bound malformed input appends ProtocolError; pre-auth/internal/pressure paths do not. |
| 9 | `unclean_restart_appends_prior_incarnation_died_before_owner_publication` | After resuming Open intents, old remaining Bound epochs flush exact restart Died before service publication. |
| 10 | `process_killed_has_no_production_participant_binding_emitter` | Type-aware source census preserves the explicit exclusion/road back. |
| 11 | `died_stored_operation_round_trips_and_replays_committed_and_pending` | Both dispositions, all causes, connection ref, and specific intents round-trip/replay. |
| 12 | `detached_stored_operation_round_trips_request_disconnect_and_shutdown` | Assert distinct ExplicitRequestCommitted-with-event and ExplicitRequestPending-without-event shapes plus connection modes; replay the pending cell/outcome from observer baseline. |
| 13 | `ordinary_stored_operation_round_trips_and_replays_measured_fate` | Exercise DiedCommitted and PendingDiedFinalized via both lower Left and Fenced-Attached sources; validate terminal audits and byte-check event. |
| 14 | `recovered_stored_operation_round_trips_without_died_terminal` | Died ordering reference, full fenced source, and both presentation tags replay with no Died terminal field/call. |
| 15 | `died_source_flush_precedes_observer_advance_and_cold_repair` | Source-only cut repairs Advance and repeats idempotently. |
| 16 | `detached_source_flush_precedes_observer_advance_and_cold_repair` | Same cut for clean/shutdown Detached with real sealed projection. |
| 17 | `ordinary_source_flush_precedes_observer_advance_and_cold_repair` | Same cut for Ordinary and measured durable floor/event. |
| 18 | `recovered_source_flush_precedes_observer_advance_and_cold_repair` | Same cut for Recovered with fenced proof and no terminal input. |
| 19 | `connection_fate_intent_failure_on_middle_conversation_completes_every_tail_binding` | Open K real tracked conversations, fail after middle flush, restart, and observe each tail terminal row before Complete/publication. |
| 20 | `post_open_middle_failure_latches_service_fatal_and_startup_completes_tail` | Inject a live non-crash failure after a middle row; observe fatal latch/refusals, permitted volatile Drop only, then controlled startup tail completion and Complete. |
| 21 | `ordinary_completion_uses_protocol_floor_and_exact_production_caller` | Production instrumentation proves only complete_binding_fate_intent calls selector; stored floor equals protocol result, not observer maximum. |
| 22 | `recovered_completion_uses_protocol_floor_and_exact_production_caller` | Same for sole private recovered authority; raw caller-supplied constructor/public proof method is unreachable. |
| 23 | `died_specific_fate_intent_completes_after_source_only_crash` | Cut after Died flush, restart, append exactly one named Ordinary/Recovered completion, then publish. |
| 24 | `attached_v3_closed_modes_round_trip_complete_fenced_proof` | Round-trip all modes; Fenced resolves exact source row, mints one ValidatedMarkerRecord without digest, and checks predecessor/debt/successor/composed terminal. |
| 25 | `attached_v2_mapping_is_lossless_and_marker_rows_refuse_without_proof` | Map both legal v2 modes exactly; marker-bearing v2 returns the named typed proof-unavailable refusal. |
| 26 | `attach_commit_splits_operational_state_and_one_noncloneable_fate_token` | Trybuild proves pre-attach proof cannot be retained/copied/cloned or used for fate after move, old public method is inaccessible, and split cannot repeat; live/replay consume one chain. |
| 27 | `old_v2_reader_refuses_v3_fate_row_with_typed_schema_version` | Frozen old reader returns SchemaVersion(3) before enum decode/publication. |
| 28 | `v3_reader_accepts_v2_prefix_and_refuses_v2_after_v3` | Contiguous histories pass; regression reports exact sequence. |
| 29 | `v2_after_v3_across_operation_page_boundary_refuses_before_apply` | V3 at page tail then v2 at next-page head retains phase and refuses before authority/Advance apply. |
| 30 | `missing_unknown_malformed_and_mixed_operation_versions_refuse_before_publication` | Independent fixtures read exact typed errors and no publication. |
| 31 | `terminal_disposition_selector_commits_or_pends_from_protocol_state` | Two-stage prepare/encode/admit validates all candidate identities, v3 encoding, keyed charge, and one entry; real states select Committed/Pending and every refusal returns unchanged owner/allocators. |
| 32 | `fate_occurrence_key_presents_each_new_arm_at_most_once` | Four independent arms derive exact key; same/cross-class duplicates cannot add a second witness. |
| 33 | `died_then_recovered_same_epoch_presents_died_once` | Both durable transitions replay; Committed Died alone presents without numeric dedup. |
| 34 | `recovered_then_died_same_epoch_refuses_before_observer_mutation` | Reversed base rows fail validate-first grammar with unchanged owner/outbox/observer state. |
| 35 | `recovered_then_died_same_epoch_after_advance_flush_refuses_without_second_presentation` | Include durable Recovered Advance; reverse Died still refuses before apply/publication and durable rows remain unchanged. |
| 36 | `recovered_after_pending_died_presents_measured_floor_once` | Pending Died precedes RecoveredOwnsAndReservesFinalizer, which presents one measured floor and one durable reservation. |
| 37 | `pending_died_recovered_reservation_makes_leave_finalizer_non_presenting` | Real Pending Died + Recovered intent + Leave; Recovered presents/reserves, Left commits state while consuming reservation with no projection, live/cold agree. |
| 38 | `pending_died_recovered_reservation_makes_fenced_attach_finalizer_non_presenting` | Same grammar through real Fenced Attached; composed terminal is audited, reservation consumed, and no Attached terminal witness exists. |
| 39 | `pending_died_restart_restores_cause_epoch_order_without_refinish` | Source-only restart restores Pending/open intent without calling finish twice or assigning terminal seq. |
| 40 | `pending_detached_restart_restores_disconnect_or_shutdown_without_refinish` | Real selector-generated Pending Detached restores exact cause/source/order without projection. |
| 41 | `pending_died_finalized_by_leave_presents_only_live_leave_commit` | Pending Died without a Recovered reservation plus Leave presents only Left; Ordinary later references Left and remains non-presenting. |
| 42 | `pending_detached_finalized_by_leave_presents_only_live_leave_commit` | Real clean/shutdown Pending Detached plus Leave presents only Left. |
| 43 | `pending_terminal_composed_by_attach_presents_only_attached_source` | Pending Died/Detached without Recovered reservation through Fenced mode audits terminal and presents only Attached. |
| 44 | `standalone_pending_finalizer_has_no_production_entry_point` | Type-aware census finds only Leave/Fenced Attached and no alternate row. |
| 45 | `leave_finalizer_resolves_pending_source_without_claiming_stored_cause` | Decode Left without cause, resolve pending source and optional Recovered reservation, and compare source cause/epoch/terminal. |
| 46 | `same_participant_ack_lineage_regression_refuses_before_observer_mutation` | Retained test reads exact refusal and unchanged durable rows after decoration deletion. |
| 47 | `w1b_tear_rider_removes_tautological_four_counter_tuple` | Source census proves declarations/tuple absent and no constant-only replacement. |
| 48 | `fate_live_and_cold_replay_produce_identical_witnesses_and_state` | For all four classes, finalizer grammar branches, and intent failures, compare decoded rows, authority, reservations, witnesses, outbox, and observer progress. |

Repository gates for implementation are:

```text
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo check -p liminal-protocol --target wasm32-unknown-unknown --no-default-features
cargo check -p liminal-sdk --target wasm32-unknown-unknown --no-default-features
```

The wasm legs are mandatory because protocol fate/attach types change.

## 13. Honesty, semver, and bounded cost

### 13.1 Idle and bounded active cost

W1b adds no task, timer, polling loop, sweep, heartbeat, backoff, read-timeout
wake, stop-flag sampler, or synthetic probe. Across `N` idle conversations,
added wakes, reads, and appends remain zero.

The relevant lifecycle paths **do not append participant fate rows today**;
W1b makes them event-driven users of the existing `OperationLog` append/flush
barrier abstraction. For one close, let `K` be the actual tracked-conversation
count. `K <= max_semantic_conversations_per_connection`. Active work is one
bounded Open flush, at most `K` owner-lock acquisitions, and work only over slots
in each affected conversation whose Bound epoch matches the exact connection.
The per-conversation slot scan is bounded by signed `ParticipantConfig::identity_slots`
(`config/types.rs:468-492`). Each matching slot appends one Died or Detached row
and at most one specific-fate row; one Complete flush ends the fold. Thus cost is
bounded by the connection's tracked conversations and each conversation's signed
slot capacity, not total registered conversations and not a periodic scan.

Replay retains one active-map entry per unmatched connection intent, each with at
most K ids, and discards it on Complete. Fate rows add fixed bounded metadata;
Fenced Attached proof is a fixed product of existing bounded proof types. The
append-only bytes remain historical; r3 makes no physical compaction or
unbounded-history safety claim.

### 13.2 Published protocol semver

`FencedAttachCommit` is a public type in published `liminal-protocol` 0.2.0;
the repository currently declares 0.2.1
(`crates/liminal-protocol/Cargo.toml:1-4`; workspace dependency at
`Cargo.toml:27`). Removing public Clone/Copy and the public recovered-fate method
is a breaking API change even though it repairs authority soundness. Therefore a
W1b implementation landing implies **`liminal-protocol` 0.3.0 at the next
publish**. This brief flags that requirement for the coordinator's version list;
it does not decide the release train or edit package versions in this docs-only
revision.

## 14. Deferred/excluded seams under no-row-no-dormancy

| seam | named future consumer | trigger | owner | oracle floor |
|---|---|---|---|---|
| Production ProcessKilled participant-binding emitter | beamr-to-participant exact exit-fate adapter | public nonblocking beamr exit event carrying exact reason and connection incarnation before tracked facts disappear | Artemis owns beamr API; Hermes owns liminal adapter | exact forced process exit opens one bounded intent and appends ProcessKilled once; other classes cannot select it; live/cold agree |
| Standalone pending-terminal finalizer | future aggregate operation not enclosed by Leave/Fenced Attached | first reviewed production caller requiring it | Hermes | ledger row first; exact row/codec/replay/barrier/single-presentation/restart oracles |
| Physical reclamation/compaction of completed incarnation intent bytes | future bounded incarnation-history compaction lane | deployment requiring bounded durable incarnation history rather than bounded active state | Hermes with haematite owner | crash-safe compacted replay equivalent to complete append-only history; no open intent lost |

These are exclusions, not hidden implementation TODOs. Detached clean/shutdown
producers, Pending Detached production, bounded active intent replay, and
Ordinary/Recovered callers are **not** deferred.

## 15. Walls

- **WALL-W1B-FOUR-FATES:** Died, Detached, Ordinary, and Recovered have equal
  exact v3 source, replay, barrier, occurrence, lineage, and oracle coverage.
- **WALL-W1B-ONE-CONVERSATION-OWNER:** fate state and occurrence routing remain
  inside `ConversationAuthority`; the incarnation stream owns only bounded fold
  work identity.
- **WALL-W1B-BOUNDED-INTENT:** one Open list is sorted, unique, and no longer than
  `max_semantic_conversations_per_connection`; simultaneously unmatched Opens
  never exceed signed `limits.max_connections`; Complete logically reclaims one.
- **WALL-W1B-OPEN-FAILURE-FATAL:** after Open, any incomplete live fold latches
  participant/server fatal and requires startup replay; Drop can release only
  volatile transport state and cannot complete or reclassify the intent.
- **WALL-W1B-SOURCE-BEFORE-ADVANCE:** Open precedes per-conversation destruction;
  fate source precedes specific source; every source precedes Advance/publication.
- **WALL-W1B-PROTOCOL-FLOOR:** server code cannot choose a binding-fate floor;
  the consuming protocol selector computes it from coupled frontier state.
- **WALL-W1B-TERMINAL-CHARGE:** sealed prepare plus versioned keyed candidate
  charge precedes admit; mismatch/refusal returns unchanged owner and allocators.
- **WALL-W1B-SEALED-ATTACH:** public `FencedAttachCommit` is non-Clone/non-Copy,
  moves by value through attach, and yields one private recovered authority; no
  pre-attach caller retains a fate-capable proof.
- **WALL-W1B-MARKER-SOURCE:** exact source-row decode plus full frontier validation
  is sole marker identity; no digest duplicates it.
- **WALL-W1B-RECOVERED-NO-TERMINAL:** Recovered has a lower Died source reference
  for order but receives no Died terminal protocol input.
- **WALL-W1B-PENDING-DIED-GRAMMAR:** immutable Pending Died completes Ordinary
  through an exact finalizer source or lets Recovered reserve/present and forces
  the finalizer through a typed non-presenting transition.
- **WALL-W1B-PENDING-DETACHED-NO-EVENT:** ExplicitRequestPending stores/replays
  exact blocked inputs and outcome without fabricated committed event bytes.
- **WALL-W1B-DIED-FIRST:** Ordinary/Recovered cannot precede their named Died
  intent; Recovered-then-Died refuses before apply, including existing Advance.
- **WALL-W1B-FINISH-ONCE:** replay reconstructs each initial Died/Detached
  transition once; finalizers consume Pending once through enclosing sources.
- **WALL-W1B-SINGLE-PRESENTATION:** one occurrence has at most one observer
  witness; numeric max is never a dedup mechanism.
- **WALL-W1B-LIVE-COLD-CONSISTENT:** live, intent replay, and cold replay use the
  same protocol transitions, tokens, metadata, and fused observer executor.
- **WALL-W1B-LOUD-VERSION:** phase crosses pages; unknown/missing/malformed/
  regressing versions refuse before apply; old v2 readers reject v3.
- **WALL-W1B-CHECKED-ARITHMETIC:** stream heads, source refs, counts, orders,
  terminal/delivery sequences, page offsets, and conversions are checked.
- **WALL-W1B-NO-POLLING:** only classified close, typed operation completion,
  startup replay, and request first touch trigger work.
- **WALL-W1B-STORED-LEAVE-HONESTY:** Left never claims to store cause; it resolves
  cause from an exact pending source reference.
- **WALL-W1B-LANDED-TESTS:** no landed test is ignored/inverted/weakened; only the
  constant-only rider decoration is deleted.
- **WALL-W1B-OBSERVATION-LENS:** no proposed oracle proves constants with
  constants; every assertion reads real protocol/production/durable state.
- **WALL-W1B-DOCS-ONLY-R3:** this revision changes only this brief.

## 16. Revision record

| revision | date | record |
|---|---|---|
| r1 | 2026-07-19 | Initial three-fate brief against ledger r1.8. Pre-review verdict: not ready. |
| r2 | 2026-07-20 | Pins `c77ce31` / ledger r1.9 and folds the full findings array (**5 MAJOR + 1 minor**), the coordinator's **EXTEND** ruling for first-class Detached, and both coordinator notes (bounded intent owner/lifecycle; airtight fenced-Attached lens). Adds clean Disconnect/server-shutdown producers, durable bounded tail intent, exact Ordinary/Recovered caller and protocol floor selector, replay-completed Died-specific intent, closed v3 Attached modes and lossless v2 mapping, one-use fate token, cross-page schema phase, protocol disposition selector, honest StoredLeave source audit, enforced Died-before-Recovered including Advance case, 45-oracle renumbered census, and bounded active-cost statement. |
| r3 | 2026-07-20 | Keeps pin `c77ce31` and folds the complete round-2 six-element findings array (**5 NEW MAJOR + 1 minor**): immutable Pending-Died Ordinary finalizer sources and Recovered-owned reservation/non-presenting Leave+Fenced finalizers; by-value non-Clone/non-Copy `FencedAttachCommit` with private recovered authority and 0.3.0 next-publish semver flag; sealed two-stage terminal prepare/admit with exact v3 keyed candidate charge; event-free ExplicitRequestPending Detached; post-Open participant/server-fatal policy with `max_connections` unmatched bound and non-crash recovery; and source-row/full-validation marker identity with no digest. Extends the census from 45 to 48. |
