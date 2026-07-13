# Participant-domain wire/server contract — design draft R3

**Status: DRAFT — decisions made by this draft, not yet ratified.** This redraft
parks at the two-key gate: reviewer-of-record plus the liminal domain-owner pass
(Hermes Crumpet). “Decided-by-draft” below means that R3 selects one contract
shape for those keys to accept or refute; it does not grant implementation
authority before both keys turn.

**Author:** Vesper Lynd. **Drafter:** Sol worker.

## 0. Provenance, authority, and laws

### 0.1 Provenance

This draft is pinned to liminal commit
`ce8814daa748373d8ffc66b3ff1664f1697a5f4e`, confirmed as the merge base of this
design branch. Every repository citation retained from R1/R2 or added in R3 was
opened and re-verified in this checkout against that pin. A bare `path:line`
below therefore means that path and current line at that named commit; no
citation is inherited from an outline or review.

R1 originated from SDK-receive dispatch blocked-upstream, norn session
`256a81a0`, envelope `claude-dev-sdk-receive.8rss9K`. That worker reached the
transport-versus-participant-domain gap, stopped at the brief's valve, and
produced the evidence base retained here. That session remains the **RESUME
VEHICLE** once this contract lands.

R2 is the redraft required by second-key refusal tear `22923c37`, read from the
newest `claude-review-contract-tear.*` envelope. The delta outline decides the
five blockers. Where the tear classified a matter as a decision but the outline
expressly leaves it to an owner or evidence gate, the outline governs and the
tension is recorded in §0.3 and §7 rather than silently erased.

R3 is the targeted redraft required by the second key's refusal ruling of
2026-07-13 and full tear envelope `b1623e31`. That key confirmed that all five
R1 blockers are resolved in R2; they and §0.3 survive. R3 decides only the new
first-attach, compaction, membership, lifecycle-observer, concurrent-holder,
and citation surfaces. Where the tear offered alternatives or suggested a
possible owner gate, the governing R3 outline selects one candidate contract;
those tensions are recorded item by item in §0.4.

The corrected boundary survives R1: the server side has symmetric **transport**,
not a participant-domain contract. At the pin, transport parks on readiness,
has bounded inbox configuration with an installed READY notifier, and owns
correlated push-reply slots
(`crates/liminal-server/src/server/connection/process.rs:193-212`,
`crates/liminal-server/src/config/types.rs:226-236`,
`crates/liminal-server/src/server/connection/apply.rs:378-405`,
`crates/liminal-server/src/server/connection/supervisor.rs:224-240`). Those
surfaces do not jointly specify participant identity, participant cursor
authority, replay, or participant lifecycle verdicts: Push carries correlation
and opaque bytes, while Deliver has only a per-subscription sequence
(`crates/liminal/src/protocol/frame.rs:381-405`,
`crates/liminal/src/protocol/frame.rs:429-444`).

### 0.2 Binding laws

**LAW 1 — NO-POLLING (Tom, 2026-07-13 01:16Z, design constant).** No
application-layer poll loops anywhere in the product. Crash detection stays
event-driven: linked-EXIT in-VM, connection-fate cross-process. The heartbeat
option for no-FIN liveness is dead. The no-FIN bound is designed **inside**
event-driven machinery, not around it. If a design has a timer whose job is
“check whether something changed,” it is wrong: redesign it to be **TOLD**.
This law binds §§2-7, especially §3.

**LAW 2 — BELIEVED STATE IS NOT CITABLE STATE (tear-side law).** Every sentence
asserting the state of the liminal codebase must cite a verified file and line
at the named commit above, or must be a named socket in §7. This law binds the
whole document.

**LAW 3 — TWO-KEY GATE.** Nothing here is ratified until the reviewer key and
Hermes Crumpet's liminal domain-owner key both pass it. “Decided-by-draft” and
“requires” describe the candidate contract presented to those keys, never
implementation authority.

**Silence attack / provenance acceptance.** A reviewer refutes this section by
finding any repository-state sentence without a pinned citation, any citation
that does not support its sentence at the pin, or wording that mistakes a draft
decision for two-key ratification.

### 0.3 R1 → R2 changelog

Driver: second-key refusal tear `22923c37` and its full envelope findings.

| Refusal item | R2 change | Where |
|---|---|---|
| B1 — impossible zero-duplicate application delivery | Rewrote R-C3 in place to **at-least-once delivery, exactly-once marking**; exposed `(conversation_id, delivery_seq)` as the application idempotency key and made crash-window redelivery an acceptance case. | §4 R-C3 and its acceptance frame |
| B2 — unauthorized cursor construction | Closed `«PARTICIPANT-ID-ORIGIN»` by draft decision: the server mints an unforgeable attach secret, returns it exactly once, and authorizes cursor access before attach. | §4 R-C1; §7 |
| B3 — incoherent global sequence/cursor/recipient state | Made v1 conversations broadcast domains, made sequence allocation and record admission one commit, and closed lifecycle recipients. Targeted delivery is excluded. | §4 R-C2–R-C4; §5 R-D1; §6–§7 |
| B4 — lifecycle authority on the wrong trait | Kept `ConnectionNotifier` byte-untouched, introduced participant-domain `ParticipantLifecycle`, and separated worker-registration rollback from participant finalization. **R-A3's trait-widening recommendation is withdrawn.** | §2, especially R-A3 |
| B5 — SIGSTOP cannot be detected by TCP keepalive | Split host/network death from wedged-process liveness. SIGSTOP now must produce no false `ConnectionLost`; cursor stall is the only layer-local observable. | §3 R-B1–R-B5 and acceptance |
| M1 — existing listener violates LAW 1 | Named the current nonblocking-accept/sleep/reap loop and made its retirement a prerequisite of the server-side build. | §1 |
| M2 — protocol-1.0 exclusion was prose only | Required negotiated capability state, close on failed negotiation, and one grep-able outbound participant-frame construction/enforcement choke point. | §5 R-D2 |
| M3 — sockets were decisions in disguise | Audited every R1 socket, decided identity, ack shape, retention units, mux, and recipients, and added a `decided-by-draft / genuinely-open` column. **Recorded outline/tear tension:** the tear sought owner choices for no-FIN defaults and the legacy-resume migration now; the governing outline leaves `«NO-FIN-KERNEL-BOUND»` and `«RESUME-COMMENT-SERVER-MISMATCH»` genuinely open to platform evidence and owner ruling. | §7 |
| M4/minor — keepalive citation overreach | Removed the claim that the read span itself proves HUP and write errors; R-B1 now cites read EOF/error and outbound write failure separately and describes HUP as readiness confirmed by read outcome. | §3 R-B1 |

### 0.4 R2 → R3 changelog

Drivers: second-key refusal ruling of 2026-07-13 and full tear envelope
`b1623e31`.

| Refusal item | R3 change | Where |
|---|---|---|
| B1 — first-attach response loss | Replaced bare credential-free minting with the shared write-ahead attach transaction: a mandatory enrollment token idempotently returns the same participant and secret until secret proof invalidates that recovery path. **Outline/tear tension:** the tear left the completion rule open; the outline decides secret proof as the atomic invalidation point. | §4 R-C0–R-C1; §5 R-D1; §7 |
| B2 — permanent `HistoryCompacted` wedge | Made the delivered marker the ackable substitute for its named replaced interval; marker ack is explicit loss acceptance, advances the cursor atomically, and cannot make unseen payload count as delivered. **Outline/tear tension:** the tear allowed either reset or substitution; the outline selects substitution. | §4 R-C3–R-C5 and acceptance |
| B3 — membership versus attachment | Defined durable membership from mint, initial cursor zero and full-history entitlement, offline replay, and terminal Leave distinct from transient detach. | §§2 and 4 R-C2/R-C5; §7 |
| M1 — impossible exactly-once observer callback | Kept exactly one terminal lifecycle **record** in the sole-authority log, but changed observer delivery to at-least-once with exactly-once marking by `(conversation_id, delivery_seq)`; retained zero-event rollback. | §2 R-A2–R-A4 and acceptance |
| M2 — equal-holder supersession flapping | Rotates the attach secret on every credential-bearing successful attach and fences stale holders; the shared attempt token recovers a lost rotation response. Stale proof commits no record. **Outline/ruling tension:** the ruling allowed a genuine fencing-policy gate or bounded bearer-latest-wins; the outline decides rotation and leaves only total credential loss plus expiry/revocation open. | §4 R-C0–R-C1; §7 |
| M3/minor — incomplete capability-absence citation | Expanded the `ConnectionProcessState` citation through the complete struct. | §1 (`crates/liminal-server/src/server/connection/state.rs:18-59`) |

## 1. The verified gap

The evidence base was re-verified rather than copied:

| Claim | Verified evidence at `ce8814d…` | Contract consequence |
|---|---|---|
| Worker unregistration has neither participant identity nor cause. | `ConnectionNotifier` is explicitly a connection-keyed worker-registration hook and `on_worker_unregistered` carries only `pid` (`crates/liminal-server/src/server/connection/notifier.rs:1-25`, `crates/liminal-server/src/server/connection/notifier.rs:46-52`). | Participant lifecycle needs its own keyed abstraction; §2 does not widen this trait. |
| A notifier unregistration call is also used for rollback while the connection remains open. | If application registration succeeds but registry storage fails, `worker_register_response` calls `on_worker_unregistered(pid)` as compensation and returns a rejection (`crates/liminal-server/src/server/connection/apply.rs:205-213`). | That path must produce no participant event and cannot be assigned a fake close cause. |
| In-VM participant liveness is deliberate event doctrine. | The conversation resource says participant crash arrives through a trapped linked-EXIT notifier, “never by polling, sleeping, or a heartbeat,” and its waiter parks on that event (`crates/liminal-server/src/server/connection/conversation.rs:3-7`, `crates/liminal-server/src/server/connection/conversation.rs:43-49`). | §3 stays inside linked-EXIT, readiness, and connection-fate events. |
| TCP transport resume is refused, while its comment claims re-Subscribe replay. | `TcpRemoteTransport::resume` returns `SdkError::Protocol`, and its comment says re-issued Subscribe triggers durable replay (`crates/liminal-sdk/src/remote/tcp/mod.rs:190-207`). | The migration mismatch remains a named owner ruling; participant resume cannot reuse that claim silently. |
| The existing SDK recovery model is subscription-local and in memory. | `ResumeRequest` carries `subscription_id` and client-held `from_sequence`; `SubscriptionRecovery` stores acknowledgements in a `BTreeMap`/`Vec` and clears them on unsubscribe (`crates/liminal-sdk/src/connection/recovery.rs:33-58`, `crates/liminal-sdk/src/connection/recovery.rs:154-158`). | Participant recovery needs distinct participant/conversation types or an explicit legacy API migration. |
| Current server Subscribe accepts no cursor and unsubscribe resets delivery sequencing. | Subscribe takes stream/channel/schema inputs and constructs a new subscription; unsubscribe removes both delivery counter and held frame (`crates/liminal-server/src/server/connection/apply.rs:349-421`, `crates/liminal-server/src/server/connection/apply.rs:432-443`). | R-C2/R-C3 define a different durable domain and starting convention. |
| Push has no participant envelope; Deliver's sequence is narrower. | Push is `{ flags, stream_id, correlation_id, payload }` with opaque payload; Deliver documents `delivery_seq` as per-subscription, starting at 1 (`crates/liminal/src/protocol/frame.rs:381-405`, `crates/liminal/src/protocol/frame.rs:429-444`). | §5 adds participant frame types without repurposing either key. |
| Authentication cannot by itself authorize a participant cursor. | Server auth is one shared bearer token and expressly “not an ACL system” (`crates/liminal-server/src/config/types.rs:25-31`). | R-C1 adds a participant-scoped capability while leaving connection auth intact. |
| Current negotiation is not an outbound capability boundary. | Connection state records `authenticated` but no selected protocol/capability (`crates/liminal-server/src/server/connection/state.rs:18-59`). `connect_response` sets authenticated before negotiation and answers negotiation failure without closing (`crates/liminal-server/src/server/connection/apply.rs:281-295`). | R-D2 makes negotiation state and the outbound choke point structural. |
| The current listener already has the banned polling shape. | The listener defines sleep backoffs, loops over a nonblocking `accept`, polls shutdown, reaps crashed processes, and sleeps on `WouldBlock` (`crates/liminal-server/src/server/listener.rs:11-12`, `crates/liminal-server/src/server/listener.rs:125-147`). | The server-side build for this contract must retire that loop in favor of readiness/blocking notification plus explicit shutdown and process-exit wakes. Compliance cannot be claimed before then. |
| Existing caps have a signed configuration pattern. | `LimitsConfig` defines named hard caps and defaults, rejects zero by field name, and constructs explicit defaults (`crates/liminal-server/src/config/types.rs:193-203`, `crates/liminal-server/src/config/types.rs:257-316`). | Keepalive and retention extend this pattern rather than creating silent unlimited states. |

**`«RESUME-COMMENT-SERVER-MISMATCH»` is broader than a comment edit.** The
legacy public model is subscription-keyed and client-cursored, while R-C1/R-C3
are participant/conversation-keyed and server-cursored. §7 requires an owner to
choose distinct protocols or a versioned deprecation/removal; merely correcting
the comment cannot close the socket.

**LAW-1 prerequisite.** The participant implementation may not inherit the
listener loop above. Accept readiness, shutdown, and process exit must each wake
their owner through blocking/readiness/event notification. No periodic reap,
sleep-backoff change detector, or “temporary” polling adapter is conforming.

**Silence attack / gap acceptance.** Refute this section by identifying an
existing frame that jointly carries participant identity, conversation identity,
a durable authorized cursor, replay position, and lifecycle verdict, or by
showing server Subscribe consumes such a cursor. A nearby field with narrower
scope does not close the gap. Conversely, finding the named listener loop does
not refute R3; it proves the explicit retirement prerequisite remains unmet.

## 2. Section (a) — participant lifecycle at the participant boundary

### Proposed contract

**R-A1 — Typed cause, participant-domain owner.** Introduce `CloseCause` (final
name subject to domain-owner review) and a separate participant-domain observer,
working name `ParticipantLifecycle`. Its domain facts identify
`(participant_id, conversation_id, connection_incarnation)`; each committed
lifecycle record and observer callback also carries its canonical delivery key
`(conversation_id, delivery_seq)`:

- `Attached { participant_id, conversation_id, connection_incarnation }`;
- `Detached { ..., cause: CloseCause }` for an explicit detach, clean
  Disconnect, authorized superseding attach, or server-directed shutdown;
- `Died { ..., cause: CloseCause }` for connection or process failure; and
- `Left { participant_id, conversation_id }` for R-C2's explicit, durable,
  terminal membership transition, never for transient detach or connection loss.

`CloseCause` has at least these non-collapsible classes:

- `CleanDeregister`: explicit participant detach or clean protocol Disconnect.
  The current dispatcher classifies Disconnect as Close
  (`crates/liminal-server/src/server/connection/apply.rs:46-53`), and the process
  then follows the normal finish path
  (`crates/liminal-server/src/server/connection/process.rs:280-289`).
- `ConnectionLost`: TCP EOF/FIN without domain detach, kernel keepalive expiry,
  or fatal read/write failure. EOF and read error are distinct paths
  (`crates/liminal-server/src/server/connection/process.rs:238-269`), and fatal
  outbound drain errors tear the connection down
  (`crates/liminal-server/src/server/connection/process.rs:169-182`).
- `ProcessKilled`: trapped linked-EXIT or a locally known supervisor
  termination. Conversation actors already expose the trapped EXIT event
  (`crates/liminal-server/src/server/connection/conversation.rs:173-186`), but
  the external reap path cannot recover beamr's private exit reason
  (`crates/liminal-server/src/server/connection/supervisor.rs:1643-1658`), so
  `«EXTERNAL-EXIT-REASON»` still blocks complete fidelity.
- `ProtocolError`: decode or protocol-state refusal that terminates the
  connection. Decode refusal is distinct in the buffer path
  (`crates/liminal-server/src/server/connection/process.rs:695-710`).
- `Superseded`: an attach authorized by the same participant capability replaces
  an older incarnation under R-C1. It is an explicit participant-domain action,
  not evidence that the old transport died.
- `ServerShutdown`: the server deliberately ends an active participant binding
  during shutdown. It is not misreported as participant choice or transport
  failure.

The enum may carry typed detail, but no catch-all may erase these classes. EOF is
not proof of clean deregistration. An `Attached` event has no invented close
cause; terminal `Detached` and `Died` binding events do. `Left` is the typed
terminal membership fact and needs no fabricated close cause.

**R-A2 — Event mapping and one finalization authority.** Cause assignment occurs
where the authoritative event is received. The conversation log is the **sole
durable lifecycle authority**: the participant owner commits exactly one
terminal lifecycle record per active participant binding. Projection to an
observer is governed by R-A4 and is not a second exactly-once effect promise:

| Source event | Participant-domain result |
|---|---|
| Successful R-C1 attach commit | One `Attached` record; a failed/rolled-back attach commits none. |
| Explicit participant detach | `Detached(CleanDeregister)` for that binding; membership and cursor remain durable. |
| Explicit participant Leave | `Detached(CleanDeregister)` for an active binding, followed by one ordered `Left`; R-C2 retires the participant id and releases its cursor/retention claim atomically. |
| Clean protocol Disconnect | `Detached(CleanDeregister)` for every binding still active on that connection. |
| Authorized replacement by a newer incarnation | `Detached(Superseded)` for the old binding, then `Attached` for the new binding in conversation order. |
| TCP FIN/EOF, confirmed read error, fatal write error, or kernel keepalive expiry | `Died(ConnectionLost)` for every active binding on the connection. |
| Trapped linked-EXIT or locally known forced process termination | `Died(ProcessKilled)`; external reason detail remains gated by `«EXTERNAL-EXIT-REASON»`. |
| Terminating decode/protocol-state refusal | `Died(ProtocolError)`. |
| Server shutdown | `Detached(ServerShutdown)` unless the participant completes explicit detach first; shutdown is neither participant choice nor inferred transport death. |
| Worker-registration compensation in `crates/liminal-server/src/server/connection/apply.rs:205-213` | **No participant event.** The registration never became a participant binding. |

The connection owner publishes one typed connection-termination event to the
participant owner; the participant owner derives the keyed terminal events. It
does not infer cause from later absence and adds no detector, sweep, or timer.

**R-A3 — R1 recommendation explicitly withdrawn.** `ConnectionNotifier` remains
byte-for-byte API-compatible: do not widen `on_worker_unregistered(pid)`, do not
add participant or cause semantics to it, and do not use its compile failures as
a lifecycle inventory. It remains the worker-routing callback its source
specifies (`crates/liminal-server/src/server/connection/notifier.rs:20-52`).
`ParticipantLifecycle` is the only participant lifecycle delivery vehicle.

**R-A4 — Broadcast lifecycle records; honest observer projection.** Each
successful attach and each terminal participant event commits the corresponding
lifecycle record through R-C2. The record is the domain fact. `ParticipantLifecycle`
projects committed records **at least once with exactly-once marking**, keyed by
`(conversation_id, delivery_seq)`: it durably marks projection progress, may
redeliver after a crash, and never changes the key. The callback receives that
key and the same incarnation as the wire record. Applications requiring an
exactly-once observer effect must durably deduplicate or transactionally commit
against the key, exactly as under R-C3. Thus observer and participant delivery
make the same honest promise by the same mechanism; notification is neither a
second ordering authority nor an exactly-once callback promise.

### Silence-attacking acceptance frame

Exercise successful attach, explicit detach, explicit Leave, clean Disconnect,
EOF, read error, write error, kernel expiry, protocol error, linked-EXIT, local
force-close, authorized supersession, external termination, and server shutdown.
Assert exactly one correctly classified ordered lifecycle record for each
participant-domain finalization. Crash after record commit but before observer
notification: after recovery the event is observed at least once, and every
copy has the same `(conversation_id, delivery_seq)`. Crash after callback but
before projection-progress persistence: assert duplicate observation with that
same key and a dedupe-able single effect. Separately force the
worker-registration storage rollback at
`crates/liminal-server/src/server/connection/apply.rs:205-213`; assert the
worker compensation callback runs while **zero** participant events or records
are produced. A reviewer refutes this section by finding a fake cause on
rollback, an active binding with no terminal record, two terminal records for
one binding, a redelivery with a changed key, an exactly-once observer-effect
promise, or participant authority routed through `ConnectionNotifier`.

## 3. Section (b) — connection fate, with honest liveness classes

### Constraint first

**NO-POLLING:** no application-layer poll loops anywhere in the product. Crash
detection remains event-driven—linked-EXIT in-VM, connection-fate
cross-process. No heartbeat frame, periodic sweep, scan, synthetic liveness
write, or timer whose job is to ask whether state changed.

R3 retains two different liveness classes:

1. **Class 1 — host/network/TCP-path death.** Host down, network partition or
   black hole, socket close, FIN/EOF, and fatal socket errors are connection-fate
   failures. Kernel machinery may bound silent no-FIN detection.
2. **Class 2 — wedged process with a live TCP stack.** SIGSTOP, deadlock, and
   livelock are **excluded from connection-fate detection by design**. A live
   kernel may continue acknowledging TCP probes while userspace does nothing;
   this layer must not manufacture a death verdict.

### Proposed contract

**R-B1 — Kernel connection fate, Class 1 only.** Enable `SO_KEEPALIVE` on every
participant TCP socket and configure per-socket idle, probe interval, and probe
count. The kernel owns probe scheduling; userspace parks on the existing
readiness path (`crates/liminal-server/src/server/connection/process.rs:193-212`).
Read EOF/error is confirmed on the read path
(`crates/liminal-server/src/server/connection/process.rs:238-269`); fatal write
outcomes come from the outbound writer
(`crates/liminal-server/src/server/connection/outbound.rs:174-235`) and tear down
through the process drain path
(`crates/liminal-server/src/server/connection/process.rs:169-182`). HUP is not
claimed as an independent branch in those spans: readiness must be confirmed by
EOF or error. This corrects R1's citation overreach.

**R-B2 — Owned, certifying socket adapter.** Add a direct production `socket2`
dependency with the explicitly required feature set; the current server lists
`socket2` only under dev-dependencies
(`crates/liminal-server/Cargo.toml:11-36`), though the workspace declares 0.6
(`Cargo.toml:38-46`). One owned adapter configures accepted sockets before
connection-process spawn; current production setup only calls
`set_nonblocking(true)` there
(`crates/liminal-server/src/server/connection/supervisor.rs:744-765`). The
adapter must:

- validate nonzero idle seconds, interval seconds, and probe count against the
  target's range and granularity;
- set `SO_KEEPALIVE` plus all three target options exactly once;
- read back enablement and all three effective values; and
- refuse participant mode at startup or accept when exact certification is
  unavailable—never round, truncate, or silently fall back.

The fields extend the named/defaulted/nonzero `LimitsConfig` pattern verified in
§1. Exact signed defaults, platform mappings, and macOS/Linux worst-case formula
remain the genuine evidence socket `«NO-FIN-KERNEL-BOUND»` together with
`«KEEPALIVE-PORTABILITY»`; R3 does not manufacture numbers.

**R-B3 — Honest Class-1 bound.** The advertised detection bound is the supported
platform's documented worst case for the read-back configuration, including
option granularity and retry semantics. A black-holed peer is not promised
detection before that machinery expires; an independently authoritative EOF or
socket error may report earlier. Unsupported or uncertifiable platforms refuse
participant mode.

**R-B4 — Ordinary writes are additional events, never probes.** An application
send may surface connection failure immediately; the outbound writer treats a
zero write or unrecoverable error as fatal
(`crates/liminal-server/src/server/connection/outbound.rs:219-235`). The server
must not generate a send merely to test liveness.

**R-B5 — Class-2 truth is cursor stall, not death.** A wedged participant stops
acking, so its R-C3 cursor freezes and retention pressure accrues. If compaction
overtakes it, `HistoryCompacted` on eventual return is the truthful record. This
layer emits no `ConnectionLost`, no wall-clock liveness score, and no synthetic
probe for SIGSTOP/deadlock/livelock. Eviction, alerting, or other wedged-process
semantics belong to the layer above under `«WEDGED-PARTICIPANT-POLICY»`.

**R-B6 — Listener prerequisite is part of LAW-1 acceptance.** The listener's
current accept/sleep/reap loop named in §1 must be replaced by blocking or
readiness-driven accept, an explicit shutdown wake, and process-exit
notification. Retention, replay, and lifecycle paths may not add polling under a
different name.

### Explicit non-goals

- No application heartbeat frames, heartbeat sweeper, or synthetic liveness send.
- No periodic accept, reap, replay, cursor, or lifecycle scan.
- No wall-clock liveness score.
- No connection-fate claim for a wedged process with a responsive TCP stack.

### Silence-attacking acceptance frame

With certifiable test values, inject a Class-1 black hole/host-loss fault and
assert `ConnectionLost` no later than the documented kernel worst-case bound
plus scheduler/test tolerance; an authoritative earlier socket failure may win.
Then SIGSTOP a participant while its host and socket remain live: assert **no
`ConnectionLost`, no death lifecycle record, and no forced cursor advance**.
Resume it and observe either normal continuation or the same participant's
`HistoryCompacted` outcome if retention passed it. Inventory the listener and
all participant threads, timers, tasks, and writes: none may periodically ask
whether state changed. The false-positive assertion for Class 2 is as
load-bearing as Class-1 detection.

## 4. Section (c) — authorized registration and server-backed cursor/replay

### Proposed contract

**R-C0 — Idempotent attach transaction with a write-ahead client token.** Every
attach attempt—enrollment and credential-bearing reattach alike—carries a
client-generated, unguessable, single-purpose `attempt_token`. The SDK **must
durably persist the token before sending the attempt** and must re-present that
same token after response loss or a crash; no recovery algorithm can close every
send/commit/response crash window without that write-ahead obligation.

The server treats `(participant_scope, attempt_token)` as an idempotency key.
The concrete key is `(conversation_id, enrollment_token)` before mint and
`(conversation_id, participant_id, attempt_token)` thereafter. It durably
stores the committed result with the transaction. An exact duplicate returns a
byte-identical result and commits nothing new: no second capability, binding,
lifecycle record, rotation, cursor transition, or retention claim. A token
cannot authorize another participant or another operation. Enrollment-token
lifetime has R-C1's explicit proof boundary; a committed credential-bearing
attach result remains replayable with its presented old secret and same token
so loss of a rotation response is recoverable. No retry path polls.

**R-C1 — Retry-safe enrollment and rotating participant capability.** Every
successful attach returns a newly minted secret: enrollment creates the initial
secret, and every credential-bearing success rotates it. Connection
authentication runs first and remains the shared bearer gate; it does not become
an ACL. Participant cursor authority is a separate capability:

1. First attach is an **enrollment transaction** carrying mandatory
   `{ conversation_id, enrollment_token }`, where `enrollment_token` is R-C0's
   attempt token. There is no credential-free bare first-attach frame. In one
   commit the server mints unguessable `participant_id` and `attach_secret`,
   creates R-C2 membership with cursor `0`, binds and records `Attached`, stores
   the enrollment token in the durable participant record, and returns
   `{ participant_id, attach_secret }`. The secret is returned exactly once per
   enrollment transaction, never in a broadcast record.
2. Retrying that enrollment token before its proof boundary returns the same
   participant id and same secret byte-for-byte and commits nothing. A distinct
   token creates a distinct participant. Collision is a minting failure retried
   before publication; identity, sequence, and cursor state are never aliased.
3. The enrollment token remains a valid recovery path until the first successful
   credential-bearing attach proves the secret, demonstrating that the client
   durably holds it. That attach atomically invalidates the enrollment token.
   Any later replay of the old enrollment token is a terminal connection-level
   refusal and commits no record.
4. Every credential-bearing attach proves the **current** `attach_secret` before
   any cursor is read, replayed, advanced, or rebound. On success the same commit
   invalidates the presented secret, returns a fresh unguessable secret, and—if
   an incarnation is active—records exactly one ordered
   `Detached(Superseded)`/`Attached` handoff and fences the old incarnation from
   acking. One connection may hold many bindings under R-C5, but only one
   incarnation per `(participant_id, conversation_id)` is authoritative.
5. Rotation response loss does not strand the client: retrying with the
   invalidated old secret **and the same write-ahead attempt token** returns the
   byte-identical fresh-secret result and commits no second rotation or handoff.
   That old secret is accepted exclusively as the retry credential for that one
   committed attempt, never for a new attach.
6. A stale secret paired with a new attempt token is a terminal connection-level
   refusal. It has no cursor side effect, commits and broadcasts no lifecycle
   record, and creates no retention pressure. In a two-holder race, the first
   serialized valid attach rotates the secret; the second holder is stale and
   cannot flap ownership. The log contains at most one Superseded/Attached pair
   per genuine handoff.
7. Cursor and replay authority key on the verified participant-capability
   binding plus `conversation_id`, never on the shared bearer token alone.

This is a participant-scoped continuity capability, **not an ACL system**. The
shared token still gates the connection; the current attach secret gates only
identity continuity and that participant's cursor. Mapping humans, agents, or
external principals to participants belongs to aion/the layer above. If the
domain owner later prefers externally minted participant ids, the server still
mints and binds the secret; an external id cannot authorize a cursor by itself.
The only deferred reachable state is named in the narrowed
`«ATTACH-SECRET-LIFECYCLE»` socket.

**Domain-owner refutation target.** Hermes may prefer bearer-latest-wins
simplicity over rotation, but that is a different contract: equal holders can
then generate unbounded broadcast handoff traffic and retention pressure. A
ruling for that alternative must add an explicit event-driven admission/rate
bound, accept and specify the denial-of-service consequence, and replace the
rotation and no-record acceptance cases below; it may not silently delete the
fence or introduce a polling limiter.

**R-C2 — One atomic, gap-free conversation log.** The server is the sole writer
of a strictly increasing `delivery_seq` for each conversation. Admission,
retention, and sequence allocation are one commit: success appends exactly one
record at the next number; failure appends nothing and consumes no number.
Sequence exhaustion is a typed terminal refusal, never wraparound. The current
pump's narrower counter is assigned before enqueue and held with its frame to
preserve order (`crates/liminal-server/src/server/connection/delivery.rs:114-142`);
that implementation fact is not reused as the durable transaction.

A conversation is a **broadcast domain in v1**, with durable membership distinct
from transient attachment:

1. **Membership begins at mint and survives detach.** R-C1's enrollment commit
   makes the participant a conversation member. Attachment is only the
   connection-scoped binding. Any number of detaches, disconnects, deaths, or
   server restarts leave membership, cursor, and retention claim intact.
2. **Initial cursor is `0`.** A newly minted member has acknowledged nothing and
   is entitled from sequence `1`, including history committed before mint. Every
   **member** is entitled to every committed record of the conversation;
   entitlement is membership-scoped and is never attachment-filtered. If the
   retention floor has already passed sequence `1`, the late member's first
   delivery is R-C4's covering `HistoryCompacted` marker and its explicit-accept
   path, by composition of these two rules—not an undefined starting point.
3. **Offline records remain entitled.** Records committed while a member is
   detached are replayed in order on reattach by R-C5's existing
   `(cursor, watermark]` mechanism. The mechanism already supplied the replay;
   this rule supplies the previously missing entitlement.
4. **Leave is terminal and is not detach.** An authorized explicit Leave is one
   durable transaction: it commits the ordered R-A1 `Left` lifecycle record,
   ends membership, releases the participant cursor and retention claim, and
   permanently retires the `participant_id`. A subsequent attach for that id is
   a terminal connection-level refusal and commits no record. Detach merely ends
   an attachment; membership, cursor, and its R-B5 cursor-stall/retention
   pressure persist.

Records include application payloads, attach, detach, death with R-A1 cause,
Leave, and `HistoryCompacted`. Every member's entitled subsequence is therefore
the full per-conversation sequence, so cumulative gap checking has no legitimate
non-recipient gap. Targeted or filtered delivery inside a conversation is
excluded from v1 under `«TARGETED-DELIVERY»`, not left as a semantics hole.

**R-C3 — At-least-once delivery, exactly-once marking.** The server stores one
durable cumulative cursor per authorized `(conversation_id, participant_id)`.
It offers every entitled retained `(conversation_id, delivery_seq)` at least
once until cumulatively acknowledged; transport loss or reconnect may offer the
same pair again. If retention has already removed an unacknowledged record, the
server emits the explicit R-C4 compaction outcome rather than claiming delivery.

The ratified-by-draft ack shape is:

`ParticipantAck { conversation_id, through_seq }`.

It is explicit, never dependent on outbound application traffic. Ordinarily the
server accepts it only from the currently authorized incarnation and only when
every sequence from cursor + 1 through `through_seq` was made available
contiguously to that participant. R-C4 adds exactly one amendment: when that
interval is unavailable because compaction overtook the cursor, delivery of the
covering `HistoryCompacted` marker substitutes for the marker's declared
replaced interval. An ack through the marker's own `delivery_seq` is then legal,
means the client explicitly accepts the named loss, and atomically advances the
cursor to the marker sequence.

The server records that the marker was delivered to the authorized incarnation
before accepting that ack. It **never** accepts an ack spanning a compacted
interval whose covering marker was not delivered. Payload in the replaced
interval is not asserted or marked as delivered; the client acknowledges the
marker itself, which names what was lost. Until that explicit ack, the cursor
holds and R-B5's honest cursor-stall observable remains. The server persists any
new cursor before confirming acceptance. Re-acknowledging the current cursor is
an idempotent confirmed no-op; an uncovered skip or regression is refused. Each
cursor transition is therefore marked once even if confirmation is retried.

The SDK keeps an in-session confirmed-contiguous watermark. It suppresses a
redelivery only when it can prove `delivery_seq <= watermark`. It hands the
canonical idempotency key `(conversation_id, delivery_seq)` to the application.
After a crash between an application effect and durable ack persistence, the
same record is redelivered with the same key. **Exactly-once application effect
is not promised.** Applications that require it must durably deduplicate or
transactionally commit their effect against that key.

**R-C4 — Both-bounds retention and honest compaction.** The draft decides both
`max_retained_conversation_bytes` and
`max_retained_conversation_entries`, each nonzero with a signed default; compact
when either bound is reached. Bytes bound payload/storage and entries bound
small-record metadata. The fields extend the verified `LimitsConfig` pattern
(`crates/liminal-server/src/config/types.rs:193-203`,
`crates/liminal-server/src/config/types.rs:257-316`).

When a cursor precedes the retention floor and no covering marker is outstanding,
the server commits and broadcasts one sequenced
`HistoryCompacted { affected_participant_id, requested_after,
compacted_through, retention_floor }` record. `requested_after` is the held
cursor; `compacted_through` is the pre-marker conversation watermark, so the
marker at the next sequence explicitly replaces
`(requested_after, compacted_through]`. For example, cursor `10`, floor `20`, and
pre-marker watermark `29` produce a marker at `30` that names `11..29` as the
replaced interval. This avoids pretending that any unseen payload in that
interval was delivered.

For the affected participant the marker is the first attach/replay outcome. It
is offered at least once under its canonical `(conversation_id, delivery_seq)`;
delivery loss, ack-confirmation loss, or reattach redelivers the **same** marker
and does not commit a new one. If the client acks its sequence, R-C3 atomically
moves the cursor there and later records may flow. If it does not, the cursor
does not move. If retention later removes that unacked marker, a repeated
compaction cycle may commit a new covering marker from the still-held cursor,
but never silently jumps it. Live commits racing the marker are sequenced after
it, queued by R-C5, and cannot leak past the unaccepted reset boundary.

**R-C5 — Replay/live cutover and multi-conversation mux.** At attach, the server
establishes a sequencer watermark and subscribes the binding to later committed
records without a race. Normally it emits retained `(cursor, watermark]` in
order, then hands off once to live records `> watermark`. If the cursor is below
the floor—including cursor `0` for a late member—R-C4's covering marker is
(re)delivered instead; only its explicit ack crosses that boundary, after which
records later than the marker flow. Live commits arriving during ordinary replay
or marker acceptance are queued under the same admission bounds and wake the
connection by an event, never a scan. The current READY marker runs one full
slice and the quiet connection parks after a final probe
(`crates/liminal-server/src/server/connection/process.rs:648-658`,
`crates/liminal-server/src/server/connection/process.rs:193-212`). The exact
linearization mechanism remains `«REPLAY-LIVE-CUTOVER»`.

One connection may attach many conversations. Participant frames are demuxed by
`conversation_id`; their v1 `stream_id` is fixed to zero and carries no identity,
ordering, cursor, or correlation meaning. Per-conversation replay and ack
streams may interleave on the connection but each conversation's sequence order
must be preserved. Connection teardown finalizes every active binding through
R-A2.

### Silence-attacking acceptance frame

1. Deliver N, let the SDK confirm its contiguous watermark, duplicate N on the
   same session, and prove the guard suppresses the duplicate it can prove.
2. Apply N in the application, crash before ack persistence, reattach, and prove
   N is redelivered **with the identical `(conversation_id, delivery_seq)`**.
   The test expects this residual; it does not hide it behind “exactly once.”
3. Commit/abort every admission failure point and prove successful records are
   gap-free while failed admissions consume no sequence.
4. Attach three participants, commit payload and lifecycle records, and prove
   all three observe the same sequence. No recipient-filter branch exists in v1.
5. Race a live commit against replay's final retained record; prove no loss,
   duplicate marking, or reordering across the handoff.
6. Put cursor `10` below floor `20` with a covering marker at `30`: deliver the
   marker, ack `30`, prove the cursor becomes `30`, and prove subsequent live
   records flow. Lose marker delivery/ack confirmation and prove redelivery uses
   the same key with one cursor advance. Repeat compaction cycles and race live
   commits against marker acceptance without loss or leakage. Negatively, send
   ack `30` without delivering its marker and prove terminal refusal with cursor
   `10`; decline to ack a delivered marker and prove the cursor still holds. No
   unseen payload is marked as delivered in either case.
7. Attach two conversations to one connection, interleave their records, and
   prove demux by `conversation_id` with independent cumulative cursors.
8. Hold the system silent; replay advances only on attach, storage completion,
   ack, and committed-record events—never because a timer checks for work.
9. Commit enrollment and lose its response; retry the same write-ahead token and
   prove the byte-identical participant id/secret returns while the log contains
   exactly one `Attached` record and no ghost. Distinct enrollment tokens create
   distinct participants. After a successful secret-proving attach, replay the
   old enrollment token and prove terminal refusal with no committed record.
10. Mint a member after N records and prove replay from `1`, or marker-first R-C4
    recovery if the floor passed it. Detach at a cursor, commit offline records,
    reattach, and prove ordered replay. Then Leave: prove the retention claim is
    released (including observable floor advance when it was the last claim) and
    every later attach for the retired id is terminally refused with no record.
11. Race two holders of one current secret with distinct write-ahead attempt
    tokens: exactly one attach wins, the loser is terminally refused, and the log
    contains at most one Superseded/Attached pair. Lose the winner's rotation
    response and retry `(old_secret, same_attempt_token)`; prove a byte-identical
    fresh secret and one committed rotation. Try a stale secret with a new token;
    prove refusal with zero records, broadcasts, cursor changes, or retention
    claims.

A reviewer refutes R-C0/R-C3 by finding a claim of exactly-once application
effect, a redelivery whose idempotency key changes, a duplicate the in-session
guard could have proved but exposed, a ghost enrollment, cursor mutation before
attach authorization, a compaction ack without marker delivery, or payload
counted delivered only because the marker was accepted. A legitimate
non-recipient gap cannot refute cumulative ack because targeted v1 delivery does
not exist.

## 5. Section (d) — participant envelope and structural wire evolution

### Proposed contract

**R-D1 — Typed participant records.** Add versioned participant frame types for:

- enrollment attach with mandatory write-ahead `enrollment_token`, and
  credential-bearing attach with mandatory write-ahead `attempt_token`;
- attach result carrying the initial or freshly rotated `attach_secret`; duplicate
  attempts return the same result under R-C0, while secrets never enter broadcast
  records;
- participant delivery carrying `conversation_id`, optional
  `sender_participant_id`, `delivery_seq`, record kind, and opaque payload;
- explicit cumulative `ParticipantAck`; and
- typed attach/refusal/Leave/compaction outcomes.

Application records name a sender. Lifecycle records name the affected
participant and incarnation and carry attach/detach/death/Leave plus R-A1 cause.
`HistoryCompacted` names the affected participant, `requested_after`,
`compacted_through`, and retention floor. Every committed record has the R-C2
sequence and is entitled to all members in that order, whether attached or not.
`conversation_id`, not `stream_id`, is the mux key.

**R-D2 — One structural protocol-capability choke point.** Leave Push
byte-for-byte untouched and add participant frame discriminants beside it. Push
is a correlated transport with opaque payload
(`crates/liminal/src/protocol/frame.rs:381-405`), so its correlation lifetime is
not the participant log lifetime.

The server must store successful negotiated protocol/capabilities only after
negotiation succeeds. Failed authentication or version negotiation responds and
closes; it never leaves an application-admitted connection. Protocol 1.0 is the
only currently supported version
(`crates/liminal-server/src/server/connection/apply.rs:20-21`), and preserving an
unknown discriminant as `Frame::Unknown` is parsing behavior only
(`crates/liminal/src/protocol/codec/known.rs:43-54`).

Outbound enforcement is structural:

1. There is exactly one grep-able server construction site for outbound
   participant frames, adjacent to the outbound emitter.
2. That function requires the connection's unforgeable negotiated participant
   capability, accepts a typed domain record rather than an unrestricted
   `Frame`, constructs the participant discriminant, and enqueues it.
3. The generic outbound enqueue path rejects server-originated participant
   discriminants, so no producer can bypass the construction site with a raw
   `Frame`.
4. Replay, live delivery, lifecycle, compaction, attach responses, and close
   verdicts all call that site. Pre-handshake, auth/version error, force-close,
   and protocol-1.0 paths cannot obtain the capability.

This choke point sits at the common emission boundary: ordinary applied
responses currently reach `OutboundWriter::enqueue_frame` from `process_buffer`
(`crates/liminal-server/src/server/connection/process.rs:713-730`), delivery also
uses `enqueue_frame` (`crates/liminal-server/src/server/connection/delivery.rs:128-142`),
and that method is where frames are encoded
(`crates/liminal-server/src/server/connection/outbound.rs:124-153`). A second
participant-frame constructor or unchecked encode path is a contract failure.

**R-D3 — Correlation remains correlation.** `correlation_id` remains the Push ↔
PushReply key; PushReply echoes it and the server owns the one-shot reply slot
(`crates/liminal/src/protocol/frame.rs:381-405`,
`crates/liminal-server/src/server/connection/supervisor.rs:224-233`). Participant
`delivery_seq` and attach credentials never reuse it. A future participant
application reply needs an explicit relationship rather than overloading either
key.

**R-D4 — Payload opacity.** Payload remains uninterpreted application bytes. The
participant record wraps those bytes; it does not inspect or derive identity,
recipient, ordering, or lifecycle facts from them. That preserves the verified
opacity of Push payload (`crates/liminal/src/protocol/frame.rs:381-394`).

### Silence-attacking acceptance frame

From captured wire bytes alone, an SDK must attribute every participant record
to `(conversation_id, delivery_seq)`, identify sender or affected participant,
distinguish payload from lifecycle, and obtain the canonical idempotency key.
Mixed-version tests exercise every producer class—attach, replay, live,
lifecycle, compaction, auth/version refusal, force-close, and pre-handshake—and
prove participant discriminants never reach protocol 1.0. A source-structure
test/grep proves there is exactly one outbound participant-frame construction
site and that generic enqueue refuses those discriminants. Push/PushReply codec
vectors remain byte-identical.

## 6. Interactions and non-goals

### Interactions

**R-I1 — Resume becomes buildable, not silently compatible.** The current TCP
resume method is a typed refusal
(`crates/liminal-sdk/src/remote/tcp/mod.rs:190-207`). R-C0/R-C1/R-C3 and R-D1/R-D2
supply write-ahead attach, authorized cursor, replay, idempotency key, and verdict
facts for a later SDK surface. They do not authorize routing participant recovery through
the legacy subscription-local `ResumeRequest`; `«RESUME-COMMENT-SERVER-MISMATCH»`
must rule distinct protocols versus migration/deprecation first.

**R-I2 — Cally/F-3a gate.** The checked-in receive brief assigns the lane and
parks merge at reviewer-plus-domain-owner keys
(`docs/design/SDK-PARTICIPANT-RECEIVE.md:3-10`,
`docs/design/SDK-PARTICIPANT-RECEIVE.md:35-41`). It requires receive facts
including conversation, sender, payload, and sequence
(`docs/design/SDK-PARTICIPANT-RECEIVE.md:96-110`) and reattach from a server
cursor with typed compaction (`docs/design/SDK-PARTICIPANT-RECEIVE.md:128-136`).
The future version carrying that receive path therefore remains gated on this
contract and its implementation.

`«LIMINAL-SDK-VERSION»` is the unfilled future-release dependency gate. It is
distinct from the brief's filled sequencing gate
`«LIMINAL-BASE-VERSION: 0.2.4»`
(`docs/design/SDK-PARTICIPANT-RECEIVE.md:12-33`).

**R-I3 — Resume vehicle.** After both keys pass and the contract lands, norn
session `256a81a0` / envelope `claude-dev-sdk-receive.8rss9K` is the designated
resume vehicle. Until then it remains blocked upstream.

### Non-goals and explicit exclusions

- No implementation, schedule, or release promise in this document.
- No SDK API/ergonomics design beyond facts the wire must expose.
- No application payload schema.
- No ACL or human/agent identity mapping; that belongs above the participant
  continuity capability.
- No exactly-once application effect guarantee.
- No application heartbeat, poll, sweep, scan, synthetic probe, or wall-clock
  liveness score.
- No connection-fate detection for SIGSTOP/deadlock/livelock.
- No targeted or filtered delivery within a v1 conversation. V1 is broadcast;
  `«TARGETED-DELIVERY»` is a named future protocol design, not a current hole.
- Wire and server semantics only.

### Silence-attacking acceptance frame

A reviewer refutes this section by finding a hidden ACL claim, SDK ergonomics
commitment, exactly-once effect promise, targeted-delivery branch, schedule, or
liveness detector. The receive brief must be implementable from explicit R3
facts, except where §7 names a genuine gate.

## 7. Named sockets and decision register

R3 distinguishes decisions made by the draft from genuine unknowns. A
**decided-by-draft** row has one candidate answer and may change only by explicit
key refusal/revision. A **genuinely-open** row names unavailable platform
evidence, an implementation linearization mechanism, a layer-above policy, or
an owner migration decision. Open rows are not permission for implementations
to choose incompatible semantics silently.

| Socket | Status | R3 answer or reason openness is genuine | Closure/refutation evidence |
|---|---|---|---|
| `«ATTACH-TRANSACTION»` | **decided-by-draft** | Every attach has a mandatory client-generated token durably persisted before send; `(participant_scope, attempt_token)` is idempotent and duplicate results are byte-identical with no new commit. | Send/commit/response crash-window tests for enrollment and rotation; source/API proof that bare attach is structurally impossible. |
| `«PARTICIPANT-ID-ORIGIN»` | **decided-by-draft** | Server mints participant id and initial secret inside tokenized enrollment; later attach proves and atomically rotates the current secret before cursor access. External ids never authorize a cursor. | Authorization-before-cursor, collision, enrollment replay, rotation loss, stale-incarnation, and reconnect tests. |
| `«MEMBERSHIP-BOUNDARY»` | **decided-by-draft** | Membership begins at mint with cursor `0`, survives detach, entitles full history and offline commits, and ends only with terminal Leave that retires the id and releases retention. | Late-join/full-history, detach/commit/reattach, and Leave/floor-advance/refusal tests. |
| `«ACK-SHAPE»` | **decided-by-draft** | Explicit cumulative `ParticipantAck { conversation_id, through_seq }`; durable persist before confirmation. A delivered covering marker is the sole substitution for its named compacted interval. | Loss/retry/gap/regression tests plus refusal without marker delivery and proof unseen payload is not marked delivered. |
| `«COMPACTION-EXIT»` | **decided-by-draft** | `HistoryCompacted` names `requested_after..compacted_through`; acking the delivered marker explicitly accepts that loss and atomically advances to marker sequence. | Marker-loss/redelivery, repeated-cycle, concurrent-live, refusal-without-delivery, and post-ack flow tests. |
| `«RETENTION-UNITS»` | **decided-by-draft** | Both bytes and entries; compact when either signed nonzero bound is reached. | Aggregate byte/entry accounting and typed configuration/refusal tests. |
| `«MULTI-CONVERSATION-MUX»` | **decided-by-draft** | Yes. One connection carries many conversations, demuxed by `conversation_id`; participant `stream_id = 0` and has no semantic role. | Cross-conversation interleaving and independent-cursor tests. |
| `«LIFECYCLE-VERDICT-RECIPIENTS»` | **decided-by-draft** | Every member is entitled to every lifecycle/compaction record in the conversation's total order, including while detached. | Three-party attach/detach/death/Leave/compaction and offline replay races with identical sequences. |
| `«LIFECYCLE-OBSERVER-DELIVERY»` | **decided-by-draft** | The log is sole authority with exactly one terminal record per binding; observer projection is at-least-once with exactly-once marking by `(conversation_id, delivery_seq)`. | Commit-before-notify and notify-before-progress crash tests, stable-key dedupe, and zero-event registration rollback. |
| `«SUPERSESSION-FENCE»` | **decided-by-draft** | Every credential-bearing successful attach rotates the secret; only the current secret can hand off, while stale proof is a connection-level no-record refusal. | Two-holder race, single handoff pair, rotation-response replay, and stale-secret zero-record tests. |
| `«RESUME-COMMENT-SERVER-MISMATCH»` | **genuinely-open** | Owner must choose separate legacy subscription recovery versus version/deprecate/delete it for participant attach. Outline governs over the tear's preference to decide now; R3 nevertheless makes “comment-only fix” insufficient. | Owner ruling covering types, cursor owner, starting convention, persistence, release boundary, and removal/compatibility tests. |
| `«EXTERNAL-EXIT-REASON»` | **genuinely-open** | Current external reap cannot read the private beamr exit reason; complete `ProcessKilled` detail needs event/API plumbing, not inference. | Typed event payload or nonblocking termination-reason API; no scan substitute. |
| `«NO-FIN-KERNEL-BOUND»` | **genuinely-open** | Exact signed defaults and macOS/Linux worst-case formulas require platform evidence. The contract shape and refusal policy are decided; numbers are not invented. | Platform documentation, readback, and black-hole fault tests proving lower/worst-case behavior. |
| `«KEEPALIVE-PORTABILITY»` | **genuinely-open** | Supported target option/range/granularity mapping and refusal matrix require target validation. | Per-target set/readback and bounded fault tests; unsupported targets refuse. |
| `«REPLAY-LIVE-CUTOVER»` | **genuinely-open** | External behavior is fixed, but the atomic sequencer/storage/binding linearization mechanism depends on the selected durability backend. | Named linearization point and adversarial attach/replay/live/crash tests. |
| `«ATTACH-SECRET-LIFECYCLE»` | **genuinely-open (narrowed)** | Enrollment recovery and per-attach rotation are decided. The reachable deferred state is total client-side credential loss—current secret **and** any still-valid enrollment token both gone—requiring an operator re-issue authority; rotation-era expiry and revocation policy also remain open. | Threat model and authorized operator re-issue for total loss; atomic expiry/revocation rules that preserve R-C0 retry receipts; stale-secret and response-loss tests. |
| `«WEDGED-PARTICIPANT-POLICY»` | **genuinely-open** | Connection-fate exclusion is decided. Alerting/eviction for cursor stall belongs to the layer above and must not feed a false lifecycle verdict back into this layer. | Layer-owner policy demonstrating no polling detector and no false `ConnectionLost`. |
| `«TARGETED-DELIVERY»` | **genuinely-open (future only)** | V1 excludes it. A future version must redesign entitlement, cursor scanning, ack validation, and capability negotiation rather than weakening broadcast implicitly. | New versioned contract and mixed-version/recipient-gap tests; not a v1 implementation gate. |
| `«LIMINAL-SDK-VERSION-GATE-NAME»` | **decided-by-draft (owner-closed)** | Preserve both real gates: filled `«LIMINAL-BASE-VERSION: 0.2.4»` and unfilled future `«LIMINAL-SDK-VERSION»`. | Existing owner reconciliation and exact names in §6. |

**LAW 2 closes the evidence escape hatch:** an unresolved dependency is one of
the grep-able genuinely-open rows or it cannot support a codebase-state claim.
**LAW 1 closes the mechanism escape hatch:** none may be “solved” by a timer,
poll, sweep, scan, heartbeat, listener backoff, periodic reap, or synthetic
write whose job is to ask whether state changed.

### Silence-attacking acceptance frame

Search the document for `«` and require every result to appear in this register
or be one of the two SDK gate names explained here. For decided rows, search for
one normative answer in §§2-6. For genuinely-open rows, require the named owner
or evidence and refuse implementation where the gap is load-bearing. Search all
timers, wakes, loops, and tasks—including the listener—and prove each is driven
by admitted work, kernel connection fate, explicit shutdown/process exit, or an
existing domain event rather than change detection.

---

**Gate posture:** DESIGN DRAFT R3. The five R1 refusal blockers remain resolved,
and the three R2 blockers plus two majors are resolved by explicit draft
decisions; the socket register separates decisions from real unknowns. Reviewer
key plus Hermes Crumpet's liminal domain-owner key are still required; until both
turn, this document is not ratified and grants no implementation authority.
