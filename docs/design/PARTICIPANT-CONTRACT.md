# Participant-domain wire/server contract — design draft R9

**Status: DRAFT — decisions made by this draft, not yet ratified.** This redraft
parks at the two-key gate: reviewer-of-record plus the liminal domain-owner pass
(Hermes Crumpet). “Decided-by-draft” below means that R9 selects one contract
shape for those keys to accept or refute; it does not grant implementation
authority before both keys turn.

**Author:** Vesper Lynd. **Drafter:** Sol worker.

## 0. Provenance, authority, and laws

### 0.1 Provenance

This draft is pinned to liminal commit
`ce8814daa748373d8ffc66b3ff1664f1697a5f4e`, confirmed as the merge base of this
design branch. Every repository citation retained from R1–R8 or added in R9 was
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

R4 is the composition redraft required by second-key refusal tear `5de60e4c`,
read from envelope `claude-review-contract-r3.RMX45H`. The ruling confirms all
six R2 refusal items are resolved in form and mandates decisions—not new
mechanisms—at seven seams among receipts, Leave, observer projection, compaction,
and retention. R4 makes those decisions in place and records them in §0.4.

R5 is the failure-taxonomy redraft required by second-key refusal tear
`be2ca4fb`, read from envelope `claude-review-contract-r4-c.b11qea`. That ruling
confirms all seven R3 seams closed correctly and narrows refusal to five outcome,
bounded-provenance, backpressure, and SDK-transition decisions recorded in §0.4.

R6 is the boundary-completion redraft required by second-key refusal tear
`fb3cc175`, read from envelope `claude-review-contract-r5.Hy31yI`. That ruling
confirms credential-token provenance and all R4 decisions, and narrows refusal
to four enrollment/readiness/finalization/enumeration decisions in §0.4.

R7 is the oldest/newest composition redraft required by second-key refusal tear
`93998bf9`, read from envelope `claude-review-contract-r6.IfAExd`. That ruling
confirms the R6 enrollment index, pending-state bound, ordinary-admission
ambiguity, ack outcomes, size validation, and tombstone precedence, and narrows
refusal to the five sequence/readiness/detach/Retired/drain decisions in §0.4.

R8 is the boundary-field redraft required by max-effort second-key refusal tear
`e51e8e10`, read from envelope `claude-review-contract-r7.LhpOA7`. That ruling
confirms the R7 reserve arithmetic, reconnect linearization, detach cell,
token-bearing `Retired` paths, and unified admission order, and narrows refusal to
six marker/parking/epoch/ack/detach-field/provenance decisions in §0.4.

R9 is the whole-artifact closure redraft required by fresh max-effort second-key
refusal tear `021b672d`, read from envelope
`claude-review-contract-r8-final.HkIBRS`. That examiner was deliberately given no
round history and re-opened the complete wire contract. R9 closes the three
marker/ack/taxonomy blockers and three parked-state/byte-bound/detach-cell majors
recorded in §0.4 rather than treating earlier-round surfaces as settled.

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

### 0.4 R8 → R9 changelog

Driver: fresh whole-document second-key refusal tear `021b672d`, envelope
`claude-review-contract-r8-final.HkIBRS`.

| Refusal item | R9 decision | Where |
|---|---|---|
| B1 — marker append can evict another unaccepted marker forever | Added a signed configuration floor of maximum mandatory transaction plus every simultaneously outstanding marker, and a dynamic no-marker-eviction closure check. A triggering commit returns typed `MarkerClosureCapacityExceeded` before changing floor or membership unless its finite marker fixed point fits; each admitted append then strictly decreases the obligation count. | §4 R-C2/R-C4; §5 R-D1; acceptance 31/37 |
| B2 — ack bytes omit the authority they purport to classify | Both ack requests now carry `participant_id` and `capability_generation`. Their serialized commit point matches that presented tuple and the authoritative binding; tombstone lookup keys on the presented id, so delayed P bytes cannot advance replacement Q. | §4 R-C3; §5 R-D1 and `«ACK-SHAPE»`; acceptance 34/38 |
| B3 — unknown/unbound requests and ordinary sender authority are undefined | Chose separate non-secret `ParticipantUnknown` and `NoBinding` outcomes and an exhaustive lookup order. Ordinary admission now presents current id/generation, requires this connection's authoritative binding at the shared serialized point, derives `sender` rather than trusting it, and cannot allocate identity state. | §4 R-C1/R-C3; §5 R-D1; acceptance 21/39 |
| M1 — parked rows can orphan or drain nondeterministically | Added a durable per-conversation order and explicit row states, atomic cohort authorization, write-ahead in-flight transition, restart drain, cap-renegotiation rule, and credential-loss/retirement terminalization. Removed the impossible parked-ack path. | §2 R-A4; §5 R-D1; acceptance 32/40 |
| M2 — SDK persistence bound omits metadata and an undefined request cap | Added signed negotiated request-byte and full-row-byte caps, exact full-row accounting including keys/framing/token/authority, checked product validation, and a local oversized-request outcome. | §2 R-A4; §5 R-D1; acceptance 32 |
| M3 — pending detach has no token-bearing pre-sequence representation | Made the detach replay cell a tagged `Empty | Pending | Committed` durable cell. Pending owns the token without inventing a sequence; final append atomically resolves it to the real sequence, with stable same-token and typed different-token behavior. | §4 R-C0; §5 R-D1; acceptance 41 |

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
| Existing caps have a signed configuration pattern. | `LimitsConfig` defines named hard caps and defaults, rejects zero by field name, and constructs explicit defaults (`crates/liminal-server/src/config/types.rs:193-203`, `crates/liminal-server/src/config/types.rs:257-316`). | Keepalive, retention, receipt lifetime/count, and negotiated SDK parking extend this pattern rather than creating silent unlimited states. |

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
not refute R9; it proves the explicit retirement prerequisite remains unmet.

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
durable completed lifecycle history**: R-A2's bounded pending state records an
accepted fate only until the participant owner commits exactly one terminal
lifecycle record per active participant binding. Projection to an observer is
governed by R-A4 and is not a second exactly-once effect promise:

| Source event | Participant-domain result |
|---|---|
| Successful R-C1 attach commit | One `Attached` record; a failed/rolled-back attach commits none. |
| Explicit participant detach | `Detached(CleanDeregister)` for that binding; membership and cursor remain durable. |
| Explicit participant Leave | One ordered `Left` is both the active binding's terminal record and the terminal membership record; R-C2 retires the participant id, receipts, cursor, and retention claim atomically. |
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

**Pending finalization and one server-candidate order.** Each R-C0 identity
reservation owns at most one current-or-pending binding slot because replacement
fences the old incarnation; active plus pending bindings therefore cannot exceed
the signed identity-slot caps. A binding slot is not reusable until its terminal
record commits. When explicit detach has been accepted, clean Disconnect,
EOF/read/write failure, linked-EXIT or local process termination, shutdown, or
protocol error arrives and the log cannot append, one atomic durable transition
changes the binding to `PendingFinalization { participant_id, conversation_id,
incarnation, original_cause, event_kind, admission_order }`. For explicit detach,
that transaction also writes R-C0's token-bearing `detach_replay::Pending` with
its presented generation and refusal epoch; connection-fate candidates have no
client token. Transport/cursor authority ends immediately, but the original
cause/incarnation is preserved outside the log.

The conversation's serialized admission lane assigns `admission_order` from its
durable total commit order to **every not-yet-committed server-originated
candidate** when that candidate arises: terminal finalizations and R-C4
reconstructed `HistoryCompacted` markers share this one order. Each identity slot
owns at most one pending finalization and one pending marker cell, so the candidate
set is bounded by twice the signed identity-slot cap. Crash recovery reads it
directly. Observer progress or the operator recovery event wakes the finite set;
the lane drains candidates strictly by `admission_order` before caller **record
admission**. Append-free normal/marker acks remain independently admissible under
R-D1 and do not reorder the drain. Terminal-record append, retention transition,
candidate deletion, and binding-slot
release are one durable transaction. No mailbox, absence inference, poll, sweep,
or fabricated later cause participates.

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
against the key, exactly as under R-C3.

Each conversation stores a durable cumulative `observer_progress` initialized to
`0`. The projector examines records in sequence, advances across non-lifecycle
records without a callback, and advances across a lifecycle record only after
its at-least-once projection is durably marked. `observer_progress` is R-C4's
**hard retention claimant**: no compaction commit may remove a sequence above it.
If satisfying either retention cap would require that removal, every operation
listed in R-D1's fail-closed set returns typed
`ObserverBackpressure { conversation_id, backpressure_epoch,
observer_progress }`, commits no conversation-log record, and consumes no
sequence. An authoritative fate may only commit R-A2's bounded pending state.

Add signed nonzero
`max_parked_observer_requests_per_conversation`,
`max_participant_request_bytes`, and `max_parked_observer_row_bytes`, each with a
signed default advertised in negotiated participant capability state, extending
the verified named/defaulted/nonzero `LimitsConfig` pattern
(`crates/liminal-server/src/config/types.rs:193-203`,
`crates/liminal-server/src/config/types.rs:257-316`). The request-byte cap covers
the complete encoded participant request. The row-byte cap covers the complete
durable serialization: conversation and row keys, storage framing/version/checksum,
state and length fields, encoded request, any separately indexed attempt token,
participant id, presented generation/incarnation, epoch, retry metadata, and
commit-order key. Token and identifier widths are protocol constants. Startup and
negotiation reject a row cap smaller than `max_participant_request_bytes` plus the
schema's maximum metadata/framing bytes, and reject overflow of checked
`max_parked_observer_requests_per_conversation ×
max_parked_observer_row_bytes`. That checked product is the exact per-SDK,
per-conversation persistence ceiling; occupied bytes count each fully serialized
row, not only its request body.

Before sending any operation that can backpressure, the SDK first encodes it. An
oversized request returns local `SdkParticipantRequestTooLarge {
conversation_id, encoded_bytes, limit }`. Otherwise it atomically reserves one
full durable row against both the row ceiling and byte ceiling. If either is
full, it returns local `SdkObserverParkCapacityExceeded { conversation_id,
row_limit, rows_occupied, byte_limit, bytes_occupied, requested_row_bytes }`; the
request is not sent and creates no server state. A non-backpressure or terminal
unknown-fate transition releases the reservation (`RecordAdmissionUnknown` does
so); tokenized response loss keeps its one counted row only while exact token fate
can still be recovered. The SDK never retries on a timer.

Every row has a durable per-conversation `park_order` allocated monotonically in
the reservation transaction and one of `Reserved`,
`AwaitingObserverProgress { refused_epoch }`, `RetryAuthorized`, `InFlight`, or
`TokenFateRecovery`. Backpressure stores the exact encoded request, token where
present, authority, epoch, and all accounting fields in `AwaitingObserverProgress`.
A restarted SDK drains `RetryAuthorized`, `InFlight`, and `TokenFateRecovery`
strictly by `park_order`; before each network write it durably marks that row
`InFlight`. An `InFlight` crash uses the operation's attempt token, or returns
`RecordAdmissionUnknown` and deletes an untokenized ordinary row. It never assumes
that an unobserved send failed.

`ObserverProgressed { conversation_id, refused_epoch, observer_progress }` is a
connection-level control event, not a promise that a particular request now fits.
A backpressure epoch is the immutable `observer_progress` value at refusal: a
**conversation-wide progress generation** in the same checked, non-wrapping
sequence domain. Every refusal while that baseline is unchanged returns the same
epoch. Progress is monotonic, so epochs are ordered and never reused; a later
refusal after actual progress necessarily carries a greater epoch. No historical
epoch map is retained.

The server delivers the event once to **every live connection that received that
epoch's refusal**, including pre-enrollment and unbound replacement-attach
connections. One fixed-size `(conversation, epoch)` interest covers every bounded
SDK request parked at that baseline. The negotiated signed nonzero
`max_participant_conversations_per_connection` bounds attached conversations plus
refusal-only interests; a first conversation beyond it returns
`ConnectionConversationCapacityExceeded` before participant mutation. Interests
are discarded when the connection ends.

The one-shot reconnect handshake may carry
`observer_refusals: [{ conversation_id, refused_epoch }]`; duplicate conversation
entries are typed `InvalidObserverEpochList` and arm nothing. For each unique
entry, one serialized comparison uses these exhaustive branches:

- `refused_epoch < observer_progress`: return `ObserverProgressStatus {
  progressed: true, armed: false }` immediately and do not disturb any newer arm;
- equality: atomically **subscribe then snapshot**—install the replacement in the
  equal epoch's push-recipient set before reading the status it reports; or
- `refused_epoch > observer_progress`: return typed protocol error
  `InvalidObserverEpoch { presented_epoch, observer_progress }` and arm nothing.

Thus on equality either progress linearizes first and the comparison takes the
older branch, or arming linearizes first and that or any later progress queues
`ObserverProgressed`; progress between snapshot and response cannot be lost. The
handshake changes no participant state and is not a post-connect query.

One matching signal/status causes one local durable transaction to change the
**entire** matching `AwaitingObserverProgress` cohort to `RetryAuthorized` before
any member is sent. Restart before that transaction leaves the rows awaiting the
reconnect comparison; restart after it resumes the ordered drain. Each terminal
result deletes its row; repeated refusal rewrites that row under the newer shared
epoch without changing `park_order`. Duplicate/lost refusal responses at an
unchanged baseline return the same epoch. A crash between rows or after a send is
therefore resolved from durable state and cannot reverse the per-conversation
order.

A current-authority invalidation, typed stale/receipt-or-credential-expiry
verdict, `Retired`, or `CredentialRecoveryLost` transition atomically visits the
bounded rows for that authority. Rows known never to have committed—such as a
backpressured Leave or ordinary admission—terminalize as the received typed
stale/expired/retired outcome or local `CredentialRecoveryLost` and are deleted. An in-flight tokenized request, and an accepted pending detach, becomes
`TokenFateRecovery` only while its exact server fate remains recoverable; exact
token replay is a status recovery, not cursor authority. It terminalizes and is
deleted on stable committed, stale, retired, expiry, or
`StaleOrUnknownReceipt`; no old request is silently reissued under new authority.
There is no parked ack: normal and marker acks are append-free and never return
observer backpressure.

On restart or renegotiation, a lower advertised row/byte cap is incompatible when
current rows or bytes exceed it; the SDK returns local
`SdkParkingCapacityIncompatible { conversation_id, negotiated_row_limit,
rows_occupied, negotiated_byte_limit, bytes_occupied }`, sends no parked row, and
requires operator/configuration action. It does not grandfather an unbounded set
or discard rows. The only request-sized waiter is this explicitly capped parking
set; there is no unbounded/general outbox, hidden server request queue, poll, or
sweep.

An arbitrarily long observer outage therefore remains bounded: admissions refuse
and lifecycle facts remain reconstructible. A deterministic callback failure at
sequence K may freeze the conversation indefinitely; replacement/retirement
without advancing past K is the genuine owner socket
`«WEDGED-OBSERVER-POLICY»`. Implementations may not skip K, weaken the hard claim,
or invent automatic observer retirement.

### Silence-attacking acceptance frame

Exercise successful attach, explicit detach, explicit Leave, clean Disconnect,
EOF, read error, write error, kernel expiry, protocol error, linked-EXIT, local
force-close, authorized supersession, external termination, and server shutdown.
Assert exactly one correctly classified ordered lifecycle record for each
participant-domain finalization. Crash after record commit but before observer
notification: after recovery the event is observed at least once, and every
copy has the same `(conversation_id, delivery_seq)`. Crash after callback but
before projection-progress persistence: assert duplicate observation with that
same key and a dedupe-able single effect. Stop observers for two conversations,
admit until both byte and entry caps would be exceeded, and prove further commits
receive typed `ObserverBackpressure` without sequence consumption while every
unprojected lifecycle record remains retained. Recover one observer: its own
progress event permits only its conversation to admit again, with all pending
callbacks delivered at least once; no timer or sweep participates. At the cap,
submit a valid authoritative Leave, lose its `ObserverBackpressure` response, and
replay: prove the same epoch and no `Left`. Recover the observer, receive one
`ObserverProgressed`, retry the same Leave token once, and prove one `Left`.
At the cap, separately trigger explicit detach, Disconnect, EOF, read failure,
write failure, linked-EXIT/process termination, shutdown, and protocol error;
crash before observer recovery. For each, prove the bounded slot is durably
`PendingFinalization` with original cause/incarnation/order, the binding has no
remaining authority, progress wake appends exactly one correctly ordered record,
and no absence inference, timer, poll, or duplicate participates. Then make
callback K deterministically fail across restart; prove progress holds at K, all
appending operations fail closed, and only `«WEDGED-OBSERVER-POLICY»` can
authorize replacement—never a silent skip.
Separately force the worker-registration storage rollback at
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

R9 retains two different liveness classes:

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
`«KEEPALIVE-PORTABILITY»`; R9 does not manufacture numbers.

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

**R-C0 — Write-ahead idempotent participant transactions and bounded receipts.**
Every enrollment attach, credential-bearing attach, explicit detach, and Leave
carries a client-generated, unguessable, single-purpose attempt token. The SDK
**must durably persist the token before sending the request** and re-present that
same token after response loss or a crash. Attach keys are
`(conversation_id, enrollment_token)` before mint and
`(conversation_id, participant_id, attach_attempt_token)` thereafter; detach and
Leave use `(conversation_id, participant_id, detach_attempt_token)` and
`(conversation_id, participant_id, leave_attempt_token)`. All serialize on the
same participant state. A duplicate commits no second capability, binding,
lifecycle record, rotation, cursor transition, or retention claim.

**Bounded detach replay.** Each reserved identity slot has exactly one fixed-size
durable tagged cell for the current-or-most-recent binding:

- `Empty`;
- `Pending { token, participant_id, presented_generation,
  committed_incarnation, admission_order, refused_epoch }`; or
- `Committed { token, participant_id, committed_generation,
  committed_incarnation, detached_delivery_seq }`.

Acceptance of an explicit detach whose terminal record cannot yet append ends
transport/cursor authority and atomically changes the binding to R-A2
`PendingFinalization` **and** the cell to `Pending`; no delivery sequence exists or
is fabricated. Exact-token replay while pending returns the same
`ObserverBackpressure` baseline and pending identity without a second candidate.
A different token while pending returns non-secret
`DetachInProgress { participant_id, presented_generation,
committed_incarnation }`, commits nothing, and never reveals the stored token.
The eventual terminal append, floor transition, pending-candidate removal, and
`Pending → Committed` conversion are one transaction; that conversion records the
real allocated `detached_delivery_seq`. A crash before, during, or after it can
therefore expose only Pending or Committed, never a half-resolved sequence.

The cell—not live binding state—is the sole source of every stable
`DetachCommitted` echo, so binding-slot reuse or disappearance cannot alter its
generation, incarnation, or sequence. Stability lasts only until the identity's
next successful attach or Leave. Attach atomically clears the cell and
terminalizes its old token as `StaleAuthority { presented_generation,
presented_incarnation, current_generation, binding_state }`, where
`binding_state` is either `Bound { current_incarnation }` or `Detached`; no current
incarnation is fabricated for a gone replacement binding. Leave replaces the
cell with tombstone-precedence `Retired { participant_id, retired_generation }`.
Thus cycling stores one cell. After response loss the SDK replays while no newer
attach/Leave is durable. A newer `AttachBound` makes the SDK record
`AuthoritySuperseded` and never resend; later stale replay is not evidence detach
failed. `Retired` terminalizes while preserving operator identity.

Every live attach receipt contains `{ participant_id, capability_generation,
attach_secret, originating_connection_incarnation, receipt_expires_at }`. Replay
on its authoritative origin returns `Bound`; replay on another connection returns
`UnboundReceipt` with the same credential payload and never binds that connection.
Leave overrides every receipt with non-secret `Retired`.

**Bounded provenance.** Each committed credential-bearing attach token, and each
enrollment token during its exact-reason window, also has a non-secret fingerprint
record `{ token_fingerprint, participant_id, request_generation,
result_generation, terminal_reason, provenance_expires_at }`. While that record
exists, the server may claim exact provenance: an exact committed token whose
body was ended by a newer generation returns
`ReceiptExpired { reason: Superseded, ... }`; one ended by its deadline returns
`ReceiptExpired { reason: Deadline, ... }`. At the same old generation, a fresh
token absent from the complete in-window fingerprint set returns
`StaleAuthority`, proving no commit for that token. For a credential-bearing
token after the fingerprint deadline, both an exact old token and an unknown old
token return
`StaleOrUnknownReceipt { presented_generation, current_generation }`, which
expressly does **not** claim that a transaction committed. Enrollment instead
follows the lifetime mapping below. A retired identity's tombstone still wins and
returns `Retired`.

Receipt/provenance cost is signed and known before send. Add nonzero
`attach_receipt_ttl_ms`, `receipt_provenance_ttl_ms` (not shorter than the receipt
TTL), server-wide and per-participant live-receipt caps, and server-wide,
per-conversation, and per-participant provenance-fingerprint caps, all with signed
defaults advertised in negotiated participant capability state. They extend the
verified named/defaulted/nonzero `LimitsConfig` pattern
(`crates/liminal-server/src/config/types.rs:193-203`,
`crates/liminal-server/src/config/types.rs:257-316`). If either complete bounded
set lacks capacity, the candidate attach returns
`ReceiptCapacityExceeded { scope, limit }` before commit. A later authorized
proof, Leave, or receipt deadline removes the secret body; the non-secret
fingerprint remains only through its provenance deadline. Both cleanups are
admitted durable deadline events plus request-time checks—never a sweep.

**Bounded retirement identity.** Add signed nonzero server-wide and
per-conversation `max_retired_identity_slots`. Enrollment must reserve one slot
in both scopes before mint and returns
`IdentityCapacityExceeded { scope, limit }` if either is full. A live participant's
reservation counts against the cap; Leave converts that same reservation into
the permanent non-secret tombstone without another capacity decision. Slots are
not cleaned up in v1, preserving exact `Retired`, no-ghost enrollment replay, and
stable lost-response `LeaveCommitted` forever while bounding total authenticated
enroll/Leave churn. There is no tombstone sweep.

**Enrollment mapping exception.** The reserved identity slot owns one indexed
`(conversation_id, enrollment_token_fingerprint) → participant_id` mapping for
the full lifetime of the live identity and its tombstone; it is not provenance-
TTL state. After the secret receipt/provenance window, replay of that committed
token returns non-secret
`EnrollmentKnown { participant_id, current_generation }`, never remints and
never claims an exact expiry reason. The SDK uses any valid current-or-newer
credential it already holds, otherwise enters
`CredentialRecoveryLost` with the returned identity. A fresh token absent from
this lifetime index may enroll normally until the reserved identity cap.

The negotiated receipt TTL is normatively the deployment's **maximum supported
recovery outage**. If generation G is invalidated by a committed G+1 attach, its
response is lost, and the client returns after the secret body expired, no wire
outcome can restore G+1. On exact `ReceiptExpired` or later
`StaleOrUnknownReceipt` while the SDK has only invalid G, it durably enters
terminal `CredentialRecoveryLost { conversation_id, participant_id,
last_known_generation }`, preserves those identities, disables new attach,
conversation replay, and ack authority, and routes credential repair only to
`«ATTACH-SECRET-LIFECYCLE»` operator re-issue. R-A4's bounded exact-token status
recovery may still learn the fate of an already-sent transaction; it cannot
mutate cursor or membership under the lost credential. It never retries
periodically.

**R-C1 — Retry-safe enrollment, generation-ordered rotation, and binding.** Every
successful attach returns a newly minted secret: enrollment creates generation
`1`; every credential-bearing success checked-increments the monotonic
`capability_generation` and rotates the secret. Generation exhaustion is a typed
terminal refusal with no commit. Connection authentication remains the shared
bearer gate, not an ACL. Participant cursor authority is separate:

1. Enrollment carries mandatory `{ conversation_id, enrollment_token }`. There is
   no credential-free bare frame. One commit mints `participant_id`, generation
   `1`, and `attach_secret`; creates R-C2 membership at cursor `0`; binds the
   originating connection; records `Attached`; stores the enrollment fingerprint
   and live receipt; and returns typed `Bound` with that generation and secret.
   Distinct enrollment tokens create distinct participants; collision retries
   before publication and never aliases identity, sequence, or cursor state.
2. Enrollment or attach receipt replay follows R-C0. In particular, recovery on
   a replacement connection is never reported as attached: it returns
   `UnboundReceipt`. The SDK first atomically persists its non-stale generation
   and secret, then sends a **new** credential-bearing attach with a fresh
   write-ahead token. Only that new `Bound` result enables replay or ack authority
   on the replacement connection.
3. A credential-bearing attach presents `{ participant_id,
   capability_generation, attach_secret, attach_attempt_token }`. The server
   rechecks generation and secret at the serialized commit point before reading,
   replaying, advancing, or rebinding the cursor. Success increments generation,
   invalidates the presented secret, returns a fresh secret plus the persisted
   cursor, terminalizes R-C0's prior detach replay cell, and—if an incarnation is
   active—records exactly one ordered `Detached(Superseded)`/`Attached` handoff
   and fences the old incarnation.
4. Lost rotation response is recovered with the invalidated old secret and same
   token only while its receipt is live. R-C0 returns `Bound` on the still-current
   origin connection or `UnboundReceipt` elsewhere, with no second rotation or
   handoff. After completion/expiry it returns the typed non-secret outcome, never
   treats the old secret as a new attach credential.
5. The SDK persists results in this crash order: compare the received generation
   with the durable one; reject a lower generation as stale; require identical
   secret for an equal generation; for a greater generation atomically persist
   `{ participant_id, generation, secret, receipt_expires_at }` **before** marking
   the attempt complete or exposing binding/replay/ack authority. A crash at any
   point replays the write-ahead token. A delayed generation can never overwrite
   a newer secret.
6. A stale generation/secret with a new token is a terminal connection-level
   refusal with no cursor side effect, lifecycle record, broadcast, or retention
   pressure. In a two-holder race, the first serialized valid attach rotates;
   the second is stale. The log contains at most one Superseded/Attached pair per
   genuine handoff.
7. Cursor and replay authority key on the verified current-generation capability,
   authoritative connection binding, and `conversation_id`, never on a receipt
   or the shared bearer token alone.

**Exhaustive participant-reference lookup.** At the serialized participant-state
point, every decoded request that names a participant uses this precedence before
its operation-specific checks: (0) an exact bounded attempt-token receipt or
Pending/Committed status lookup returns its stable operation-specific result;
(1) otherwise a tombstone for the **presented**
`(conversation_id, participant_id)` returns operation-specific `Retired`; (2) no
live identity and no tombstone returns non-secret
`ParticipantUnknown { conversation_id, participant_id }`; (3) a live identity
whose current generation differs from the presented generation returns
`StaleAuthority`; (4) for a binding-required operation, a matching live generation
without this connection's current incarnation returns
`NoBinding { conversation_id, participant_id, presented_generation }`; and only
then (5) the operation executes. Credential attach and exact receipt/status lookup
do not require an existing binding, but all acks, detach, Leave, and ordinary
record admission do; server-driven replay runs only inside the binding established
by a successful attach and is not a separate participant request. `ParticipantUnknown` and `NoBinding`
are typed semantic outcomes: the connection remains open, the SDK terminalizes
that attempt, and neither outcome creates a receipt, cursor, candidate, identity,
log record, or other durable participant state. This explicit identity oracle is
acceptable only after the existing shared connection-authentication gate; neither
outcome contains a secret or another holder's binding/incarnation.

This is a participant-scoped continuity capability, **not an ACL system**. The
shared token gates the connection; the current generation and attach secret gate
identity continuity and that participant's cursor. Mapping humans, agents, or
external principals belongs above. If ids later originate externally, the
server still mints the generation/secret; an external id cannot authorize a
cursor. The remaining total-loss/operator re-issue state is named in
`«ATTACH-SECRET-LIFECYCLE»`.

**Domain-owner refutation target.** Hermes may prefer bearer-latest-wins
simplicity over rotation, but that is a different contract: equal holders can
then generate unbounded broadcast handoff traffic and retention pressure. A
ruling for that alternative must add an explicit event-driven admission/rate
bound, accept and specify the denial-of-service consequence, and replace the
rotation and no-record acceptance cases below; it may not silently delete the
fence or introduce a polling limiter.

**R-C2 — One atomic, gap-free conversation log with owed-record reserve.** The
server is the sole writer of a strictly increasing `delivery_seq`. Let `MAX` be
its maximum, `H` the high watermark, `T` active-or-pending bindings with unwritten
terminal records, `M` required-but-unwritten `HistoryCompacted` markers, and `L`
live members. Checked arithmetic preserves after every commit:

`MAX - H >= T + M + (L × T)`.

`T` and `M` are actual owed records. The conservative `L × T` budget guarantees
that each unavoidable terminal drain can create a marker requirement for every
live member without losing a sequence exit. For a candidate costing `k` records,
the transaction first computes its resulting floor, memberships, terminal claims,
and the complete R-C4 marker fixed point newly required by cursor-0 mint or floor
advance, then checks both this sequence invariant and R-C4's independent
entry/byte closure invariant. Sequence failure returns
`ConversationSequenceExhausted`; physical closure failure returns
`MarkerClosureCapacityExceeded`. Either refusal occurs before any participant,
floor, candidate, or sequence mutation. A required marker is never terminalized,
discarded, or removed before its sequenced append, and an appended-but-unaccepted
marker is never compacted.

Enrollment and attach-from-detached cost `1` and create `T`; enrollment into
compacted history also creates `M`. Ordinary records cost `1`; any floor advance
adds one `M` per newly overtaken member. Supersession costs exactly `2` contiguous
values: its terminal record consumes old `T`, `Attached` creates replacement `T`,
and net `T` is unchanged. A marker append costs `1` and removes one `M`. A
terminal detach, death, or bound Leave costs `1`, removes one `T`, and may convert
up to `L` released potential-budget values into newly required `M`, so it cannot
break the invariant. Active-to-`PendingFinalization` changes no count. Record
append, claim/marker changes, retention transition, and candidate deletion are
one commit; failures consume no number and sequence never wraps. The current
pump's narrower counter is assigned before enqueue and held with its frame to
preserve order (`crates/liminal-server/src/server/connection/delivery.rs:114-142`);
that fact is not reused as the durable transaction.

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
4. **Leave is terminal and is not detach.** The v1 authority is deliberately
   narrow: only the currently authoritative bound incarnation may send
   `LeaveRequest { participant_id, capability_generation, attach_secret,
   leave_attempt_token }`. Shared connection authentication and stale
   incarnations are insufficient; v1 defines no operator-initiated Leave. The SDK
   write-aheads the token under R-C0, and the server rechecks incarnation,
   generation, and secret at the same serialized participant-state linearization
   point used by attach.
5. **One Leave commit retires everything.** It appends one ordered R-A1 `Left`
   record, terminalizes the active binding and membership, invalidates the current
   secret, converts every outstanding enrollment/attach receipt to R-C0
   `Retired`, releases the member cursor/soft retention claim, and permanently
   tombstones `participant_id` in one commit. The tombstone retains the enrollment
   fingerprint and successful Leave token/result but no attach secret; it converts
   the retirement slot reserved by enrollment under R-C0 and cannot exceed either
   signed cap. Tombstone lookup precedes live capability validation: duplicate Leave with that token
   returns the stable `LeaveCommitted { retired_generation, left_delivery_seq }`
   and commits no second record even though the secret is now invalid; any other
   later enrollment/attach/detach/Leave replay returns
   `Retired { participant_id, retired_generation }` with no secret, binding, new
   identity, or record.
6. **Attach/Leave races have one order.** If attach linearizes first, it rotates
   generation and incarnation; the competing old-incarnation Leave fails typed
   `StaleAuthority` with no record. If Leave linearizes first, the participant and
   all receipts are terminal; the competing attach returns `Retired` with no
   binding or record. There is no state in which a stale binding retires the new
   holder. Detach merely ends an attachment; membership, cursor, and its R-B5
   cursor-stall/retention pressure persist.

Records include application payloads, attach, detach, death with R-A1 cause,
Leave, and `HistoryCompacted`. Every member's entitled subsequence is therefore
the full per-conversation sequence, so cumulative gap checking has no legitimate
non-recipient gap. Targeted or filtered delivery inside a conversation is
excluded from v1 under `«TARGETED-DELIVERY»`, not left as a semantics hole.

**R-C3 — At-least-once continuous delivery or explicit abandonment; exactly-once
marking.** The server stores one durable cumulative cursor per authorized
`(conversation_id, participant_id)`. While `cursor + 1 >= physical_floor`, it
offers every entitled retained `(conversation_id, delivery_seq)` at least once
until cumulatively acknowledged; transport loss or reconnect may offer the same
pair again. If `cursor + 1 < physical_floor`, continuous history is impossible:
the server offers R-C4's explicit abandonment marker instead of promising the
retained suffix. This is the deliberate R4 narrowing of R3's unconditional
retained-record offer.

Both ack shapes carry their authority explicitly:

- `ParticipantAck { conversation_id, participant_id,
  capability_generation, through_seq }`; and
- `MarkerAck { conversation_id, participant_id, capability_generation,
  marker_delivery_seq }`.

At the same serialized point as detach and Leave, the server applies R-C1's
presented-id lookup precedence, then requires the presented generation and this
connection's current bound incarnation to match. Tombstone lookup is therefore
on the id in the ack bytes, not an inferred current connection binding. A delayed
P ack received after Q binds on the same connection can only classify P; it can
never advance Q. For continuous history, the server then accepts the normal ack
only when every sequence from cursor + 1 through `through_seq` was made available
contiguously to that matched incarnation. For broken history, delivery of
`HistoryCompacted { abandoned_after, abandoned_through, ... }` authorizes exactly
one alternative: a `MarkerAck` naming the marker's own `delivery_seq` explicitly
abandons the entire named interval—including any records still physically
retained at or above the old floor—and atomically advances that matched
participant's cursor to the marker sequence. Those payloads are not asserted or
marked as delivered; the marker records the client's authorized relinquishment.

The server records marker delivery to the same current incarnation before
accepting that ack and refuses an ack spanning abandonment without it. Until the
client accepts, the cursor holds, no suffix payload flows, and R-B5's cursor-stall
observable remains. The server persists the new cursor before confirmation.
Re-acknowledging the current cursor under the same presented authority is an
idempotent confirmed no-op; an uncovered skip or regression is refused. Each
cursor transition is marked once even if confirmation is retried.

**Ordinary record admission authority.** Its wire request carries
`{ conversation_id, participant_id, capability_generation, payload }`; it carries
no caller-supplied `sender`. At the same serialized point used by ack, detach, and
Leave, R-C1's exhaustive lookup requires the current generation and this
connection's authoritative incarnation. Only then may admission derive the
record's `sender = participant_id`, run size/observer/sequence/marker-closure
checks, and commit. A stale, retired, unknown, or unbound request returns that
named outcome and commits nothing. Ordinary admission cannot mint or reserve an
identity, membership, cursor, receipt, candidate, or tombstone and therefore
cannot create durable conversation state outside an already reserved live
identity.

The SDK keeps an in-session confirmed-contiguous watermark. It suppresses a
redelivery only when it can prove `delivery_seq <= watermark`. It hands the
canonical idempotency key `(conversation_id, delivery_seq)` to the application.
After a crash between an application effect and durable ack persistence, the
same record is redelivered with the same key. **Exactly-once application effect
is not promised.** Applications that require it must durably deduplicate or
transactionally commit their effect against that key.

**R-C4 — Monotonic physical floor, soft members, hard observer, and honest
abandonment.** Retention has both
`max_retained_conversation_bytes` and
`max_retained_conversation_entries`, each nonzero with a signed default. Bytes
bound payload/storage and entries bound metadata. The fields extend the verified
`LimitsConfig` pattern
(`crates/liminal-server/src/config/types.rs:193-203`,
`crates/liminal-server/src/config/types.rs:257-316`). At participant-mode startup, let `I` be the configured per-conversation
`max_retired_identity_slots` (the permanent live-plus-retired reservation bounds
simultaneously live members); let `(Qe, Qb)` be the maximum entry/byte cost of a
mandatory transaction, the larger of supersession's ordered
`Detached`+`Attached` pair and any single pending-finalization, Leave, detach,
marker, or attach transaction; and let `(1, Bm)` be the maximum encoded cost of
one `HistoryCompacted` marker, all including keys and storage framing. Startup
uses checked arithmetic and fails configuration validation unless:

- `max_retained_conversation_entries >= Qe + (I × 1)`; and
- `max_retained_conversation_bytes >= Qb + (I × Bm)`.

Thus both caps admit one maximum mandatory transaction **in addition to** the
maximum one unaccepted-or-planned marker per live identity. `I`, both retention
caps, and the maximum encoded mandatory/marker sizes are signed, nonzero
participant capability values following the cited `LimitsConfig` pattern;
overflow is configuration failure, never saturation. Consequently mandatory
lifecycle operations have no reachable `RecordTooLarge`; only ordinary
caller-sized record admission may return it.

The physical floor is reproducible. Let `H'` be the high watermark after a
candidate record append; `F` the current first physically retained sequence
(`H + 1` when empty); `m` the minimum durable member cursor after membership
changes in that transaction, or `H'` when there are no members; and `o` the
conversation's R-A4 `observer_progress`. Floors use a domain with explicit
one-past-end `END = MAX + 1`; `END` is not allocatable as `delivery_seq` and is
represented by a wider checked value or enum, never wrapped. Define:

- `preferred_floor = min(m, o) + 1` (checked into the floor domain);
- `a` as the earliest delivery sequence of a live member's unaccepted marker, or
  `END` when none exists;
- `cap_floor` as the smallest floor at or above `F` whose retained suffix,
  including the candidate record and R-C4 closure headroom, satisfies both hard
  caps **without exceeding `a`**; and
- `F' = max(F, preferred_floor, cap_floor)`, valid only when `F' <= a`.

A successful log commit appends at `H'`, sets the physical floor to `F'`, and
atomically removes exactly the old prefix `[F, F')`. The floor never decreases.
Acceptance of that marker releases its anchor; Leave resolves all remaining
entitlement and releases a retiring member's anchor in the same retirement
transaction, rather than evicting a still-entitled marker. No other transition
may release it. A single record too large for either cap is refused before
sequence allocation.
Member claims are **soft**: `cap_floor` may exceed `m + 1`, and each member with
`cursor + 1 < F'` is overtaken and receives the marker path below. Observer
progress is **hard**: `cap_floor` may not exceed `o + 1`. If satisfying a cap
would cross that boundary, the whole candidate admission returns
`ObserverBackpressure`, removes nothing, and consumes no sequence. Persisted
observer progress recomputes the formula and emits the admission wake; no sweep
or poll does so.

An appended-but-unaccepted marker is a hard physical anchor: the floor may not
remove it, and its participant owns no second candidate until that marker is
accepted. Let `U` be the distinct live identities already owning an unaccepted or
planned marker. Every record admission preserves the checked closure envelope
`(Qe, Qb) + ((I - U) × (1, Bm))` beyond the actual retained suffix (which already
contains or budgets the `U` markers). This is closure headroom, separate from
observer hard retention and R-C2 sequence reserve; ordinary admission may
therefore receive `MarkerClosureCapacityExceeded` even when its own record fits.
It ensures a later mandatory transaction can materialize every still-possible
marker without first waiting for ordinary data to disappear.

Before **any** floor- or membership-triggering commit changes the floor, it
computes this finite marker fixed point. Start with participants already owning
an unaccepted marker or candidate plus every live member the proposed floor would
overtake. Simulate candidates in the shared `admission_order`, including each
maximum-size marker and each resulting capped-floor transition while pinning all
existing and simulated markers. If a simulated transition newly overtakes an
uncovered live member, add that member once and repeat. There are at most `I` live
identities and one marker owner per identity, so the set grows strictly at most
`I` times and then reaches a fixed point. At that point both dimensions must fit
the actual anchored suffix, every planned marker, the maximum marker cost for
every still-unrepresented identity slot, **and** `(Qe, Qb)` closure headroom
without evicting an unaccepted marker. Checked overflow or no satisfying floor
returns `MarkerClosureCapacityExceeded { conversation_id, dimension,
required, limit, marker_owners }` before changing floor, membership, candidates,
sequence, or participant state. Startup's `Q + I × marker` floor guarantees that
the bounded marker set itself is representable; the dynamic check accounts for
actual intervening retained records.

Only after both that closure check and R-C2's sequence check succeed does the same
transaction change the floor/membership and durably create one R-A2 candidate per
newly affected member with `{ affected_participant_id, admission_order }`.
Simultaneous candidates use `admission_order = (transaction_order,
ascending_participant_index)`, whose index is bounded by `I`, so the total order
cannot alias. Caller record admissions remain behind this candidate drain. A
candidate does **not** freeze the marker payload. On its actual append turn, the
same transaction recomputes `abandoned_after` from the then-current held cursor,
`abandoned_through` from the then-current pre-marker high watermark, and
`physical_floor_at_decision` from the then-current physical floor. It appends the
sequenced `HistoryCompacted { affected_participant_id, abandoned_after,
abandoned_through, physical_floor_at_decision }`, removes the candidate, and
preserves all marker anchors plus closure headroom atomically.

This is the termination argument: the pre-commit fixed point contains every
marker its own appends can induce; pinning prevents an append from evicting any
unaccepted marker, so each drain append removes exactly one `M` and creates zero
replacement obligations. With no caller admission interleaving, finite `M`
therefore decreases to zero in at most `I` appends. If that proof cannot be made
for a proposed trigger, the trigger is refused before floor change rather than
entering the drain. An earlier pending finalization is included in the marker
interval, while a later finalization follows the marker; both derive from the one
durable admission order rather than wake scheduling.

`abandoned_after` is the held cursor and `abandoned_through` is the pre-marker
watermark; the next sequence is the marker itself. These names deliberately
distinguish physical compaction from client-authorized abandonment. With cursor
`10`, physical floor `20`, and watermark `29`, marker `30` offers abandonment of
`11..29`: records `11..19` are physically gone, while retained entitled records
`20..29` are explicitly included in the offer. If the client accepts marker `30`,
none of `20..29` is delivered and the cursor becomes `30` by authorized
abandonment. If it declines, the cursor remains `10`, no `20..29` payload is
released out of order, and the stall remains visible. This is not a claim that
`20..29` was compacted or seen.

The marker is offered at least once under its canonical key. Delivery loss,
ack-confirmation loss, or reattach redelivers the same retained marker. The floor
cannot remove it before acceptance. Caller commits remain behind the shared
ordered candidate drain; subsequent admissions preserve its anchor and closure
headroom, and live delivery after an appended marker remains queued until marker
acceptance. Once accepted, its cursor transition and anchor release are atomic;
only a later floor transition may then create a new marker for that participant.
No timer retries either path.

Worked floor cases (assume the byte cap is not tighter than the stated entry
cap):

1. **Multiple claims.** At `H=100`, `F=1`, member cursors `{10, 40}`, and
   `o=100`, the preferred floor is `11`. If the caps require `cap_floor=25`, one
   commit removes `1..24`, sets `F'=25`, overtakes only cursor `10`, and leaves
   cursor `40` continuous.
2. **Below-cap Leave.** From `F=11`, cursors `{10, 40}`, and `o=100`, Leave by
   cursor `40` appends `Left` but leaves the preferred floor `11`; Leave by cursor
   `10` instead leaves cursor `40` as minimum and advances the floor to `41`.
3. **At-cap final Leave.** At `H=100` with one cursor-`0` member and `o=100`, the
   Leave commit appends `Left` at `101`, removes the released member claim, deletes
   the old retained prefix through `100`, and sets `F'=101`, retaining `Left` for
   the observer. When the observer marks `101`, the empty-log floor becomes `102`.
4. **Cursor-0 late member at cap.** Mint never lowers `F`. If mint appends
   `Attached` at `101` while `F=25`, the new cursor `0` is immediately overtaken;
   its first replay outcome is a marker offering abandonment `1..101`. Cap
   eviction may advance `F` further but may not cross `observer_progress + 1`.
   If the unprojected `Attached` blocks marker admission, the marker receives
   typed `ObserverBackpressure` until observer progress emits the retry wake;
   enrollment is never silently rolled back or polled.

**R-C5 — Replay/live cutover and multi-conversation mux.** At attach, the server
establishes a sequencer watermark and subscribes the binding to later committed
records without a race. Normally it emits retained `(cursor, watermark]` in
order, then hands off once to live records `> watermark`. If `cursor + 1` is
below the physical floor—including cursor `0` for a late member—R-C4's
abandonment marker is (re)delivered instead. Its explicit ack abandons through
the marker's pre-commit watermark; only records sequenced after the marker then
flow. Live commits arriving during ordinary replay or marker acceptance are
queued under the same admission bounds and wake the
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
6. Put cursor `10` below physical floor `20` at watermark `29`; marker `30`
   must name `abandoned_after=10`, `abandoned_through=29`, and floor `20`.
   Prove `20..29` are still physically retained before the choice. Accept marker
   `30`: prove none of `20..29` is delivered, the client explicitly records their
   abandonment, cursor becomes `30`, and later live records flow. Decline it:
   prove cursor remains `10` and `20..29` is not released out of order. Lose
   marker/ack confirmation and prove same-key redelivery with one cursor advance;
   ack `30` without marker delivery is refused. Repeat across compaction and live
   commit races.
7. Attach two conversations to one connection, interleave their records, and
   prove demux by `conversation_id` with independent cumulative cursors.
8. Hold the system silent; replay advances only on attach, storage completion,
   ack, committed-record, observer-progress, and admitted deadline events—never
   because a timer or sweep checks for work.
9. Commit enrollment and lose its response; retry on the origin connection and
   prove the same generation/credential payload returns with exactly one
   `Attached` record and no ghost. Distinct enrollment tokens create distinct
   participants. Replay after receipt expiry and prove typed `ReceiptExpired`
   cannot mint another participant.
10. Mint a member after N records and prove replay from `1`, or marker-first R-C4
    abandonment if the floor passed it. Detach at a cursor, commit offline
    records, reattach, and prove ordered replay. Leave and prove the formula's
    floor transition, retired id, and no-record refusal.
11. Race two holders of generation G with distinct write-ahead tokens: exactly
    one produces G+1, the loser is stale, and the log has at most one handoff
    pair. Lose the winner's response and replay its live receipt; prove one
    rotation. Try stale G with a new token; prove zero records, broadcasts,
    cursor changes, or retention claims.
12. Commit enrollment, then Leave before any secret-proving reattach; delayed
    exact committed enrollment-token replay returns `Retired` with the same
    participant id and retired generation, but no secret, binding, new identity,
    or record. Separately Leave with a live credential-attach receipt; replay the
    exact `(old_secret, same_token, old_generation)` and prove `Retired` carries
    that same participant id/retired generation with no secret, binding, or record.
13. Attempt Leave from the shared bearer alone, a stale incarnation, wrong
    generation, and wrong secret; each is refused with no record. Lose a valid
    Leave response and retry the same write-ahead token; prove one stable
    `LeaveCommitted` and one `Left`. Race valid attach and Leave at the serialized
    state: attach-first makes Leave `StaleAuthority`; Leave-first makes attach and
    all receipt replays `Retired`.
14. Commit attach on C1 and kill C1 before its response. Retry the token on C2:
    assert typed `UnboundReceipt`, persist its higher generation/secret, and
    assert C2 has no replay/ack authority. Send a fresh-token attach from C2,
    obtain `Bound`, then prove C2 replays and acks. No response in this sequence
    may ambiguously claim C2 was bound before the final commit.
15. Delay generation G's response until after G+1 is durably installed; prove the
    SDK rejects G and preserves G+1. Crash before and after atomic generation/
    secret persistence and recover by token replay. Exercise the negotiated
    receipt deadline immediately before and after expiry: before returns the live
    typed receipt; after returns `ReceiptExpired`, the secret body is gone, and
    no periodic expiry scan exists. Exceed the signed receipt-count cap and prove
    pre-commit refusal with no rotation.
16. Execute R-C4's four worked floor cases with byte and entry accounting:
    multiple claims `{10,40}`, below-cap Leave of each claimant, at-cap final
    cursor-0 Leave, and cursor-0 late mint at cap. Assert the exact removed prefix,
    monotonic `F'`, which member is overtaken, observer hard-stop refusal, marker
    fields, and the final empty-log floor after observer progress.
17. From generation G, let exact token T commit G+1 while fresh token U loses.
    Let T's receipt deadline remove its secret body with no G+2 attach. Inside the
    provenance window, T replay must be exact
    `ReceiptExpired { reason: Deadline }` while U is `StaleAuthority`. After T's
    fingerprint deadline, both return `StaleOrUnknownReceipt`; neither claims a
    commit, leaks a secret, binds, or writes a record.
18. Churn enroll/Leave until each conversation/server retirement-slot boundary.
    Every admitted enrollment has a reserved slot; lost Leave response still
    returns stable `LeaveCommitted`, retired enrollment replay cannot ghost-remint,
    and the next enrollment returns `IdentityCapacityExceeded` without minting.
19. Force `ObserverBackpressure` separately on enrollment attach, credential
    attach, explicit detach, ordinary record admission, and valid Leave. A known
    refusal parks for one `ObserverProgressed` cycle; normal and marker acks still
    commit/no-op. Lose an ordinary admission response and prove terminal
    `RecordAdmissionUnknown` with no resend. A poison callback reaches the socket.
20. Commit G+1, lose its response, crash the client past `receipt_expires_at`, GC
    the secret, then replay T. Assert non-secret `ReceiptExpired` (or
    `StaleOrUnknownReceipt` after provenance), terminal `CredentialRecoveryLost`
    preserving conversation/participant/G, and no automatic retry.
21. Drive generation to its maximum and prove `GenerationExhausted` is terminal,
    non-retryable, and no-commit. Exercise live-receipt and fingerprint-cap
    `ReceiptCapacityExceeded`, every reachable discriminant and every row of R-D1
    in both directions: each operation has only listed outcomes and every listed
    outcome has a trigger, exact fields, retryability, and SDK transition.
22. Keep enrolled P live past receipt and ordinary provenance GC. Replay its
    original enrollment token T: assert `EnrollmentKnown` identifies P/current
    generation without secret/remint. From identical unbound connection state,
    fresh token U enrolls a distinct participant until the identity cap.
23. Backpressure bound and unbound enrollment/replacement connections; prove each
    refusal recipient gets connection-level `ObserverProgressed`. Reconnect and
    obtain non-mutating status by conversation+epoch. Force two refusal/progress
    cycles. Reattach while Leave is parked: the old row terminalizes on its typed
    authority outcome and is not silently reissued under new authority. Prove no
    ack row exists and no polling occurs.
24. For every R-A2 fate while blocked, crash after durable `PendingFinalization`
    and before progress, then inject crashes at every terminal-drain write
    boundary. Recovery preserves original cause/incarnation/order and observes
    either the whole atomic append/retention/delete/release transaction or none;
    exactly one ordered terminal record and one released bounded slot result.
25. Refuse participant-mode startup when either retention cap is below its
    checked `Q + I × marker` floor, when either SDK byte cap is invalid, or when a
    checked product overflows. Exercise every explicit detach outcome; prove acks
    are never observer-backpressured and ordinary response loss remains
    at-most-once ambiguous. Re-run case 21's bidirectional enumeration.
26. With N active/pending binding claims and all current marker obligations,
    admit non-terminal records to reserve equality. Refuse ordinary, enrollment,
    detached-attach, floor-advancing, and two-value supersession candidates before
    they invade either owed class. Then deliver EOF/shutdown/protocol failure
    across all N bindings; prove every terminal and newly required marker drains
    through its reserved value without dropping a promised record.
27. Race reconnect handshake against observer progress in both linearization
    orders: status is already progressed or the replacement is armed and receives
    the push, never neither. Backpressure two concurrent local requests at one
    baseline: both receive one epoch, one matching signal authorizes one retry of
    each, and refusals after actual progress share a new epoch. Exceed the signed
    connection-conversation bound and prove pre-mutation capacity refusal.
28. Detach with token T and lose the response. Before another attach, T replay
    returns the same `DetachCommitted`. Successfully reattach with a fresh attach
    token, terminalize T as `AuthoritySuperseded`, then replay T and prove
    `StaleAuthority`, not a stable result or second record. Repeat cycles and prove
    one detach cell; after Leave, the old token returns `Retired`.
29. Decode post-Leave replay of the exact committed enrollment token and an exact
    committed credential token. Both return the same participant id and retired
    generation in `Retired`, and neither returns a secret, binds, mints, or writes.
30. First create a blocked marker candidate, then accept EOF into pending
    finalization, then wake on observer progress: marker appends first from its
    earlier order and recomputes fields before the terminal record. Repeat with
    EOF's candidate ordered first: terminal appends first and the later marker's
    recomputed `abandoned_through` includes it. In both runs the resulting sequence,
    abandonment interval, and cumulative observer projection match that order.
31. At `H=MAX-2`, `T=M=0`, and floor above `1`, attempt cursor-0 enrollment: its
    Attached+terminal+marker obligations exceed the remaining values, so the
    whole mint refuses `ConversationSequenceExhausted`. Separately make a candidate
    floor advance newly overtake members at the boundary; refuse that triggering
    commit atomically, preserve floor/cursors, and later append every owed marker.
32. Fill exactly `max_parked_observer_requests_per_conversation` full durable SDK
    rows and separately fill the checked byte ceiling with variable request sizes.
    Count keys, framing, token, authority, epoch, state, and order bytes. The next
    call returns local `SdkObserverParkCapacityExceeded` before send; a request
    above `max_participant_request_bytes` returns
    `SdkParticipantRequestTooLarge`. One progress event atomically authorizes the
    epoch cohort and drains it by durable `park_order`. Crash before cohort mark,
    between rows, after write-before-response, and after repeated refusal; each
    restart resumes deterministically without exceeding either ceiling.
33. Reconnect with an older, equal, and newer epoch. Older returns progressed and
    does not replace a newer arm; equal subscribes before snapshot and cannot lose
    the race; newer returns `InvalidObserverEpoch`. Duplicate conversation entries
    return `InvalidObserverEpochList`. Prove epochs equal refusal baselines and
    never alias after progress.
34. Encode normal and marker ack requests with conversation id, participant id,
    presented generation, and requested boundary. After Leave, deliver both delayed
    forms. Tombstone lookup on the presented id returns `Retired` with that id,
    presented generation/requested boundary, and retired generation, with no
    secret, binding, or live/current cursor; codec vectors match exactly.
35. Detach and replay before attach: `DetachCommitted` echoes generation,
    incarnation, and sequence from the cell after live binding state is gone.
    Reattach, terminalize that replacement binding, then replay the old token:
    `StaleAuthority` reports `binding_state: Detached` and no current incarnation.
36. Separately from case 17, commit T at G+1 and then a valid G+1→G+2 attach before
    T's deadline. T replay inside provenance is exactly
    `ReceiptExpired { reason: Superseded }`; case 17 alone proves `Deadline`.
37. Configure three live identities and try the former two-entry suffix. Startup
    rejects it because `2 < Qe + 3`. At the smallest valid cap, detach all three
    so one trigger needs three markers: preflight reaches the finite fixed point,
    all three append without evicting/replacing one another, `M` decreases
    `3→2→1→0`, and reattach can receive/accept its retained marker. Separately fill
    an anchored suffix so a new closure cannot fit; the triggering floor/membership
    commit returns `MarkerClosureCapacityExceeded` before changing anything.
38. Bind P, offer through N, then Leave P and bind Q on the same connection. Deliver
    delayed bytes for P's normal and marker acks after Q is also offered through N.
    Their explicit P/generation tuple returns P's `Retired` and Q's cursor is
    unchanged. Repeat generation rotation for one participant and obtain
    `StaleAuthority`, never an inferred-current ack.
39. For credential attach, receipt replay, detach, Leave, normal ack, marker ack,
    conversation replay, and ordinary admission, present a never-minted id and
    assert `ParticipantUnknown`. For every binding-required operation present a
    known current id/generation on an unbound or wrong connection and assert
    `NoBinding`. Then send stale and retired variants to exercise the full lookup
    precedence. Ordinary sender always equals the verified id and none of these
    failures allocates durable state.
40. Park ordered attach/ordinary/Leave rows, then invalidate, retire, and expire
    their authorities while they occupy every durable state. Known-no-commit rows
    terminalize and delete; only exact recoverable token fate enters bounded
    `TokenFateRecovery`. Inject `CredentialRecoveryLost`, crashes at each cohort
    and per-row boundary, and a lower-cap renegotiation; prove deterministic order,
    `SdkParkingCapacityIncompatible`, no orphan row, no old-authority reissue, and
    no ack row.
41. Backpressure explicit detach T and lose the response. Before progress, replay T
    and receive the same Pending/backpressure status with no sequence; send U and
    receive `DetachInProgress` without disclosure of T. Crash before and during the
    wake transaction. Exactly one atomic state is observed: Pending, or Committed
    with the real terminal sequence. Replay T after commit returns that stable
    `DetachCommitted`; no sentinel, duplicate candidate, or second record exists.

A reviewer refutes R-C0–R-C4 by finding an exactly-once application-effect
claim, changed delivery key, ghost enrollment, `Bound` on an unbound replacement
connection, secret returned after receipt expiry/Leave, stale-generation
overwrite, unauthorized or duplicate-record Leave, cursor mutation before
current binding proof, compaction ack without marker delivery, abandoned payload
called delivered, physical-floor transition that differs from the formula, or
compaction above `observer_progress`. A legitimate non-recipient gap cannot
refute cumulative ack because targeted v1 delivery does not exist.

## 5. Section (d) — participant envelope and structural wire evolution

### Proposed contract

**R-D1 — Exhaustive participant requests and outcomes.** Add versioned request
frames for enrollment, credential attach/receipt replay, explicit detach,
cumulative ack, Leave, marker ack, ordinary record admission, and non-mutating
observer-readiness status. Every outcome carries its named discriminant and
`conversation_id`; it echoes the request token when one exists, the **presented**
`participant_id` and `capability_generation` when the request contains them, and
the operation's requested sequence/generation. The cross-cutting lookup rows below
compose with every named operation before its operation-specific rows; together
they are exhaustive. No generic “proof/admission refusal” exists. Connection
authentication/version failures remain R-D2 pre-participant outcomes.

| Operation | Named outcome discriminant | Required fields | Retryability and required SDK transition |
|---|---|---|---|
| SDK-local participant encoding | `SdkParticipantRequestTooLarge` | conversation id, encoded request bytes, signed request-byte limit | Local terminal refusal for this call; reserve no row, send no frame, create no server state. |
| SDK-local observer-wait admission | `SdkObserverParkCapacityExceeded` | conversation id, signed row/byte limits, occupied rows/full serialized bytes, requested full row bytes | Local terminal refusal for this call; reserve no row, send no frame, create no server state. |
| SDK restart/cap renegotiation | `SdkParkingCapacityIncompatible` | conversation id, negotiated row/byte limits and occupied rows/full bytes | Send no parked row; preserve bounded rows and require operator/configuration action. |
| First participant operation or reconnect refusal entry for a conversation on this connection | `ConnectionConversationCapacityExceeded` | conversation id, signed connection-conversation limit | No participant mutation, interest arming, or readiness promise; use a connection with a free negotiated slot. |
| Credential attach, receipt/status replay, detach, Leave, normal ack, marker ack, or ordinary admission | `ParticipantUnknown` | presented conversation id and participant id; presented generation, token, and requested boundary when those fields exist in that operation | Terminal semantic outcome for this attempt; connection remains open and no participant/durable state is created. |
| New detach/Leave, normal ack, marker ack, or ordinary admission after exact-token status lookup misses | `NoBinding` | presented conversation id, participant id, and generation | Terminal for this attempt; connection remains open, no other binding is disclosed, and no state changes. |
| Any listed participant-id operation with a live generation mismatch | `StaleAuthority` | presented participant id/generation and current generation; operation-specific token/boundary when present | Terminal under the old authority; no state changes. |
| Any listed participant-id operation whose presented id has a tombstone | operation-specific `Retired` | presented participant id/generation, retired generation, and operation-specific token/boundary | Terminal tombstone result; no secret, binding, live cursor, or state change. |
| Any floor- or membership-triggering admission | `MarkerClosureCapacityExceeded` | conversation id, entry/byte dimension, checked required capacity, configured limit, marker-owner count | No floor, membership, participant, candidate, or sequence mutation; terminal for caller admission, while mandatory server candidates remain durably pending for an event-driven retry. |
| Enrollment attach | `EnrollBound` | enrollment token, participant id, generation `1`, secret, receipt/provenance deadlines | Success; persist before exposing `Bound`; replay same token only after unknown response. |
| Enrollment attach | `EnrollUnboundReceipt` | same credential fields, origin incarnation | Persist, then fresh-token credential attach; never expose binding. |
| Enrollment attach | `EnrollmentKnown` | enrollment token, participant id, current generation | Committed identity, no secret/binding; use a valid current-or-newer credential or enter `CredentialRecoveryLost`; never remint. |
| Enrollment attach | `ReceiptExpired` | token, participant id, result/current generations, `Deadline` or `Superseded` | Exact only inside the enrollment provenance window; use a valid current-or-newer credential or enter `CredentialRecoveryLost`. |
| Enrollment attach | `Retired` | token, participant id, retired generation | Terminal; preserve identity for operator record, no attach. |
| Enrollment attach | `ReceiptCapacityExceeded` | token, `scope`, `limit` | No SDK auto-retry; surface typed admission refusal. |
| Enrollment attach | `IdentityCapacityExceeded` | token, `scope`, `limit` | Terminal for new enrollment; no participant was minted. |
| Enrollment attach | `ObserverBackpressure` | token, observer progress, backpressure epoch | Persist `AwaitingObserverProgress`; retry once after matching `ObserverProgressed` or reconnect status. |
| Enrollment attach | `ConversationSequenceExhausted` | token, high watermark, remaining capacity, resulting `T`, `M`, and `L × T` budgets | Terminal for this mint; reserve check includes cursor-0 marker creation before identity or sequence mutation. |
| Credential attach | `AttachBound` | attach token, participant id, new generation/secret, deadlines, incarnation, persisted cursor | Success; generation-ordered persist, then expose binding; no ack parking state exists. |
| Credential attach | `AttachUnboundReceipt` | same credential fields, origin incarnation | Persist, then fresh-token attach; no replay/ack authority. |
| Credential attach | `ReceiptExpired` / `StaleOrUnknownReceipt` / `Retired` | attach token and fields defined above | Never retry same request; preserve newer credential or enter the specified terminal state. |
| Credential attach | `StaleAuthority` | token, presented/current generations | Terminal for this request; preserve current durable credential. |
| Credential attach | `GenerationExhausted` | token, participant id, current generation = maximum | Terminal, not retryable; enter `CapabilityGenerationExhausted`, preserve identity, no commit. |
| Credential attach | `ReceiptCapacityExceeded` / `ObserverBackpressure` / `ConversationSequenceExhausted` | token plus capacity/epoch fields; exhaustion adds high watermark, remaining capacity, resulting `T`/`M`/`L × T`, and required cost (`2` for supersession) | Capacity is surfaced; backpressure enters a shared epoch; reserve exhaustion precedes rotation, binding, floor, and marker changes. |
| Receipt replay | `Bound` / `UnboundReceipt` | exact token plus live receipt fields | Same-connection binding or persist-then-fresh-attach respectively. |
| Receipt replay | `ReceiptExpired` | exact fingerprint, reason, result/current generations | Exact only inside provenance window; not retryable; preserve newer credential or `CredentialRecoveryLost`. |
| Receipt replay | `StaleAuthority` | fresh token absent from complete in-window set, presented/current generations | Proves no commit for this token; not retryable. |
| Receipt replay | `StaleOrUnknownReceipt` | token, presented/current generations | Post-provenance ambiguity; claims no commit; no automatic retry. |
| Receipt replay | `Retired` | token, participant id, retired generation | Terminal; no secret or binding. |
| `DetachRequest` | `DetachCommitted` | detach token, participant id, cell-retained committed generation/incarnation and detached delivery seq | Success; echo comes only from `detach_replay::Committed`, never reused live-binding state; stable until next successful attach/Leave. |
| `DetachRequest` exact-token replay while cell is Pending | `ObserverBackpressure` | detach token, participant id, presented generation, committed incarnation, original epoch/progress; no delivery sequence | Stable pending status; create no second candidate and retain bounded token-fate recovery only while needed. |
| `DetachRequest` different token while cell is Pending | `DetachInProgress` | presented token, participant id, presented generation, committed incarnation; never the stored token | Terminal for the competing attempt; no state change or sequence. |
| `DetachRequest` | `StaleAuthority` | detach token, presented generation/incarnation, current generation, `binding_state: Bound { current_incarnation } | Detached` | Terminal after cell overwrite; absent live incarnation is represented by `Detached`, never a sentinel or retained fake current binding. |
| `DetachRequest` | `Retired` | detach token, participant id, retired generation | Terminal tombstone result after Leave; preserve operator identity, no record. |
| `DetachRequest` first accepted while append is blocked | `ObserverBackpressure` | detach token, generation/incarnation, epoch/progress | One atomic transition writes `PendingFinalization` plus `detach_replay::Pending`; progress wake atomically appends and converts Pending to Committed with the real sequence. |
| `ParticipantAck { conversation_id, participant_id, capability_generation, through_seq }` | `AckCommitted` | presented participant id/generation and requested `through_seq`, matched persisted cursor | Success only after tuple+binding match; advance SDK watermark. |
| `ParticipantAck` | `AckNoOp` | presented participant id/generation, requested boundary, unchanged matched cursor | Success; idempotent confirmation under the same authority. |
| `ParticipantAck` | `AckGap` / `AckRegression` / `StaleAuthority` | presented participant id/generation, requested/current cursor, reason | Terminal for this ack; do not advance SDK watermark. |
| `ParticipantAck` | `Retired` | presented participant id/generation, requested `through_seq`, retired generation | Tombstone lookup uses the presented id; no secret, binding, or live/current cursor is present. |
| `LeaveRequest` | `LeaveCommitted` | leave token, retired generation, `left_delivery_seq` | Success; terminal participant state. Duplicate token returns same result. |
| `LeaveRequest` | `StaleAuthority` / `Retired` | leave token, participant id, presented/current or retired generation | Terminal; `Retired` preserves operator identity but returns no secret, binding, or record. |
| `LeaveRequest` | `ObserverBackpressure` | leave token, generation, epoch/progress | Preserve valid authority/request; every counted parked row at that epoch retries once after progress. |
| `MarkerAck { conversation_id, participant_id, capability_generation, marker_delivery_seq }` | `MarkerAckCommitted` / `AckNoOp` | presented participant id/generation, requested marker seq, matched persisted cursor | Success only after tuple+binding match; record abandonment or idempotent confirmation. |
| `MarkerAck` | `MarkerNotDelivered` / `MarkerMismatch` / `StaleAuthority` | presented participant id/generation, marker/requested/current sequences, reason | Terminal for this ack; cursor holds. |
| `MarkerAck` | `Retired` | presented participant id/generation, requested marker sequence, retired generation | Tombstone lookup uses the presented id; no secret, binding, or live/current cursor is present. |
| `RecordAdmission { conversation_id, participant_id, capability_generation, payload }` | `RecordCommitted` | verified/derived sender participant id, assigned delivery seq | Success only after tuple+binding match; after response loss enter SDK `RecordAdmissionUnknown` and never resend automatically. |
| Ordinary record admission | `RecordTooLarge` / `ConversationSequenceExhausted` / `MarkerClosureCapacityExceeded` | measured cap or high watermark, remaining capacity, resulting `T`/`M`/`L × T`, or checked closure required/limit/owners | Terminal for this record; refusal consumes no sequence and preserves floor, identity, and every terminal/marker obligation. |
| Ordinary record admission | `ObserverBackpressure` | verified/derived sender, epoch/progress | A received refusal may park for one progress-cycle retry; a lost response is ambiguous and terminal. |
| Reconnect handshake status | `ObserverProgressStatus` | conversation id, refused epoch, current observer progress, `armed`, `progressed` | Older epoch returns progressed/unarmed; equal atomically arms then snapshots. Matching progress retries every bounded local row at that epoch. |
| Reconnect handshake status | `InvalidObserverEpoch` / `InvalidObserverEpochList` | conversation id, presented/current epoch or duplicate-entry detail | Typed protocol error; newer epoch or duplicate conversation entries arm nothing and mutate no participant state. |

A valid terminal detach or bound Leave never returns
`ConversationSequenceExhausted`: its active/pending claim owns one reserved value.
That outcome remains reachable only for non-terminal enrollment, attach,
supersession, and ordinary candidates before they invade the reserve. Each owed
marker already owns its `M` value; its physical append is instead protected by
R-C4 closure accounting.
Normal and marker acks never return `ObserverBackpressure`: they append no
record and may relieve retention pressure. Ordinary record admission is
deliberately at-most-once-ambiguous after response loss; unlike mandatory-token
operations, the SDK must not convert ambiguity into a duplicate. Applications
requiring deduplication supply and durably interpret an application-level key in
the opaque payload.

`ObserverProgressed` is a pushed connection event, not a polling invitation or
admission guarantee. Receipt and identity capacity refusals are terminal for
that request; no progress event is promised. `HistoryCompacted` carries affected
participant, `abandoned_after`, `abandoned_through`, and
`physical_floor_at_decision`. Participant delivery carries conversation,
optional sender, delivery sequence, record kind, and opaque payload. Every record
has R-C2 order and member entitlement. `conversation_id`, not `stream_id`, is the
mux key.

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
For every attach/detach/Leave exchange it must also recover the echoed attempt
token, capability generation, applicable receipt deadline, and typed
binding/terminal status; an
`UnboundReceipt` can never decode as `Bound`. Mixed-version tests exercise every
producer class—attach, Leave, replay, live,
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
facts for a later SDK surface. They do not authorize routing participant recovery
through the legacy subscription-local `ResumeRequest`; `«RESUME-COMMENT-SERVER-MISMATCH»`
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
- No lifecycle outbox or unbounded secret-bearing receipt, provenance-fingerprint,
  or retired-identity store.
- No application heartbeat, poll, sweep, scan, synthetic probe, receipt-expiry
  sweep, observer retry loop, or wall-clock liveness score.
- No connection-fate detection for SIGSTOP/deadlock/livelock.
- No targeted or filtered delivery within a v1 conversation. V1 is broadcast;
  `«TARGETED-DELIVERY»` is a named future protocol design, not a current hole.
- Wire and server semantics only.

### Silence-attacking acceptance frame

A reviewer refutes this section by finding a hidden ACL claim, SDK ergonomics
commitment, exactly-once effect promise, targeted-delivery branch, schedule, or
liveness detector. The receive brief must be implementable from explicit R9
facts, except where §7 names a genuine gate.

## 7. Named sockets and decision register

R9 distinguishes decisions made by the draft from genuine unknowns. A
**decided-by-draft** row has one candidate answer and may change only by explicit
key refusal/revision. A **genuinely-open** row names unavailable platform
evidence, an implementation linearization mechanism, a layer-above policy, or
an owner migration decision. Open rows are not permission for implementations
to choose incompatible semantics silently.

| Socket | Status | R9 answer or reason openness is genuine | Closure/refutation evidence |
|---|---|---|---|
| `«ATTACH-TRANSACTION»` | **decided-by-draft** | Attach, detach, and Leave use mandatory write-ahead tokens and one serialized participant state. The identity slot's tagged detach cell is `Empty`, token-bearing `Pending` without a sequence, or `Committed` with the real generation/incarnation/sequence; it is the sole stable-echo source until next attach/Leave. | All send/commit/response windows, same/different token while Pending, crash around Pending→Committed, echo after binding removal, stale replay after replacement death, detach loss across attach/Leave, replacement recovery, and terminal receipt tests. |
| `«RECEIPT-LIFETIME»` | **decided-by-draft** | Signed receipt TTL/count and non-secret provenance TTL/count caps bound both bodies and classifiers. Cleanup uses admitted deadlines; TTL is the maximum supported recovery outage and expiry may enter `CredentialRecoveryLost`. | Delayed generation, crash order, exact/unknown before/after provenance, cap exhaustion, composed outage, and no-sweep tests. |
| `«RETIRED-IDENTITY-BOUND»` | **decided-by-draft** | Enrollment reserves signed server/conversation identity slots; each slot owns the enrollment-token mapping for the live+tombstone lifetime, and Leave converts it to a permanent tombstone. Cap exhaustion refuses before mint, bounding churn without weakening `EnrollmentKnown`, `Retired`, or duplicate `LeaveCommitted`. | Post-GC enrollment T versus fresh U, boundary churn, no-ghost replay, lost Leave response, and pre-mint capacity refusal. |
| `«PARTICIPANT-ID-ORIGIN»` | **decided-by-draft** | Server mints participant id, generation 1, and initial secret inside tokenized enrollment; later attach proves current generation/secret and atomically increments/rotates before cursor access. External ids never authorize a cursor. | Authorization-before-cursor, collision, enrollment replay, generation ordering, rotation loss, and reconnect tests. |
| `«MEMBERSHIP-BOUNDARY»` | **decided-by-draft** | Membership begins at mint with cursor `0`, survives detach, entitles full history and offline commits, and ends only with authoritative tokenized Leave that retires id, receipts, and soft retention claim. | Late join, offline replay, Leave authority/idempotency/races, and floor-transition tests. |
| `«LEAVE-AUTHORITY»` | **decided-by-draft** | Only the current bound incarnation proving current generation/secret may Leave. Attach and Leave serialize: attach-first makes old Leave stale; Leave-first makes attach/receipts retired. V1 has no operator Leave. | Shared-bearer/stale-proof refusals, duplicate stable result, and both race orders. |
| `«ACK-SHAPE»` | **decided-by-draft** | Normal `ParticipantAck { conversation_id, participant_id, capability_generation, through_seq }` and `MarkerAck { conversation_id, participant_id, capability_generation, marker_delivery_seq }` carry presented authority. At one serialized point the server looks up the presented id, matches generation+binding, then validates continuity or named abandonment. Post-Leave uses the presented id's tombstone and never a live cursor. | P-ack-after-Q-bind ambiguity, same-id rotation, unknown/unbound/stale/Retired codec vectors, normal gap/regression, marker-delivery refusal, and proof abandoned payload is not marked delivered. |
| `«COMPACTION-EXIT»` | **decided-by-draft** | A pending marker shares the durable server-candidate admission order with finalizations; at append it recomputes `abandoned_after..abandoned_through` and the physical-floor snapshot. Acking abandons through that pre-marker watermark and advances to marker sequence. | Both marker/finalization orders, append-time fields, retained-suffix choice, marker loss/redelivery, repeated-cycle, concurrent-live, and post-ack flow tests. |
| `«RETENTION-UNITS»` | **decided-by-draft** | Both bytes and entries, with signed nonzero caps validated at `Q + I × maximum-marker`. The floor pins every live-owner unaccepted marker. A finite pre-commit fixed point budgets all marker owners plus mandatory-transaction headroom; failure is typed before floor/membership change, and each admitted marker append strictly reduces `M`. Sequence reserve independently covers terminal and marker obligations. | Exact byte/entry closure arithmetic, three owners versus a two-entry invalid cap, no-marker-eviction drain, closure refusal atomicity, `MAX-2` sequence boundary, multiple claims, observer stop, cap overtake, and Leave examples. |
| `«MULTI-CONVERSATION-MUX»` | **decided-by-draft** | Yes. One connection carries many conversations, demuxed by `conversation_id`; participant `stream_id = 0` and has no semantic role. | Cross-conversation interleaving and independent-cursor tests. |
| `«LIFECYCLE-VERDICT-RECIPIENTS»` | **decided-by-draft** | Every member is entitled to every lifecycle/compaction record in total order, including while detached, unless it explicitly accepts a named abandonment after compaction broke continuity. | Three-party lifecycle/compaction races, offline replay, and explicit-abandonment tests. |
| `«LIFECYCLE-OBSERVER-DELIVERY»` | **decided-by-draft** | The log is sole completed lifecycle history and observer progress is hard. Signed request, full-row, row-count, and checked-product caps bound the complete SDK parking serialization. One signal atomically authorizes an epoch cohort; durable `park_order` and write-ahead states determine restart drain. Credential loss terminalizes known fates and retains only bounded exact-token recovery. Acks never park. | Exact row/byte accounting, oversize and renegotiation refusal, cohort/per-row crash boundaries, credential loss/retirement/expiry in every state, old/equal/new epoch entries, and proof of no orphan or ack row. |
| `«SUPERSESSION-FENCE»` | **decided-by-draft** | Every credential-bearing successful attach checked-increments generation and rotates the secret; stale proof is a connection-level no-record refusal. | Two-holder generation race, single handoff pair, rotation receipt recovery, and stale-proof zero-record tests. |
| `«RESUME-COMMENT-SERVER-MISMATCH»` | **genuinely-open** | Owner must choose separate legacy subscription recovery versus version/deprecate/delete it for participant attach. Outline governs over the earlier tear's preference to decide now; R9 nevertheless makes “comment-only fix” insufficient. | Owner ruling covering types, cursor owner, starting convention, persistence, release boundary, and removal/compatibility tests. |
| `«EXTERNAL-EXIT-REASON»` | **genuinely-open** | Current external reap cannot read the private beamr exit reason; complete `ProcessKilled` detail needs event/API plumbing, not inference. | Typed event payload or nonblocking termination-reason API; no scan substitute. |
| `«NO-FIN-KERNEL-BOUND»` | **genuinely-open** | Exact signed defaults and macOS/Linux worst-case formulas require platform evidence. The contract shape and refusal policy are decided; numbers are not invented. | Platform documentation, readback, and black-hole fault tests proving lower/worst-case behavior. |
| `«KEEPALIVE-PORTABILITY»` | **genuinely-open** | Supported target option/range/granularity mapping and refusal matrix require target validation. | Per-target set/readback and bounded fault tests; unsupported targets refuse. |
| `«REPLAY-LIVE-CUTOVER»` | **genuinely-open** | External behavior is fixed, but the atomic sequencer/storage/binding linearization mechanism depends on the selected durability backend. | Named linearization point and adversarial attach/replay/live/crash tests. |
| `«ATTACH-SECRET-LIFECYCLE»` | **genuinely-open (narrowed)** | Receipt lifetime is decided and is not credential revocation. The negotiated TTL is the maximum recovery outage; lost response plus expiry normatively produces SDK `CredentialRecoveryLost` with preserved identity. Operator re-issue, post-receipt capability expiry, and revocation remain open. | Threat model and authorized operator re-issue from `CredentialRecoveryLost`; atomic expiry/revocation preserving generation, Retired, and no-polling rules. |
| `«WEDGED-PARTICIPANT-POLICY»` | **genuinely-open** | Connection-fate exclusion is decided. Alerting/eviction for cursor stall belongs to the layer above and must not feed a false lifecycle verdict back into this layer. | Layer-owner policy demonstrating no polling detector and no false `ConnectionLost`. |
| `«WEDGED-OBSERVER-POLICY»` | **genuinely-open** | A deterministic callback failure at sequence K truthfully freezes the conversation. Owner must define operator replacement or retirement that resumes from K without silently advancing observer progress, skipping lifecycle, polling, or weakening the hard claim. | Operator-authority ruling, same-key replay/replacement evidence, poison recovery tests, and no-skip audit. |
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

**Gate posture:** DESIGN DRAFT R9. The five R1 blockers, six R2 refusal items,
seven R3 composition findings, five R4 failure-taxonomy findings, four R5
boundary/completeness findings, five R6 composition findings, and six R7
marker/parking/field findings remain resolved. R9 closes the fresh whole-document
R8 refusal's three blockers and three majors by explicit decisions. The socket
register separates decisions from real unknowns. Reviewer key plus Hermes
Crumpet's liminal domain-owner key are still required; until both turn, this
document is not ratified and grants no implementation authority.
