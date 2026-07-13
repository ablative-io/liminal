# Participant-domain wire/server contract — design draft R10

**Status: DRAFT — decisions made by this draft, not yet ratified.** This redraft
parks at the two-key gate: reviewer-of-record plus the liminal domain-owner pass
(Hermes Crumpet). “Decided-by-draft” below means that R10 selects one contract
shape for those keys to accept or refute; it does not grant implementation
authority before both keys turn.

**Author:** Vesper Lynd. **Drafter:** Sol worker.

## 0. Provenance, authority, and laws

### 0.1 Provenance

This draft is pinned to liminal commit
`ce8814daa748373d8ffc66b3ff1664f1697a5f4e`, confirmed as the merge base of this
design branch. Every repository citation retained from R1–R9 or added in R10 was
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

R10 is the dual-final-examiner redraft required after **both** independent fresh
max-effort examiners refused commit `8d8dab0`. The dispatcher hand-verified all
fifteen defects. R10 closes the reserve, restart, precedence, direct-detach,
epoch, single-binding, global-bound, renegotiation, byte-ceiling, counter,
maximum-generation, anchored-recovery, taxonomy, acceptance, and LAW-1 inventory
decisions recorded in §0.4.

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

### 0.4 R9 → R10 changelog

Driver: dual independent fresh-examiner refusal of commit `8d8dab0`; every item
below was dispatcher-verified against that commit.

| Refusal item | R10 decision | Where |
|---|---|---|
| D1 — mandatory reserve re-required and candidate double-counted | Made `(Qe,Qb)` consumable exactly once by named mandatory classes. One bounded `ClosureDebt` cell records the post-commit shortfall; ordinary work preserves the full envelope, marker conversion is counted once, and pending mandatory work waits until event-driven progress repays debt. | §4 R-C2/R-C4; acceptance 25/37 |
| D2 — `Reserved` row has no restart edge | Reservation now persists the full request/authority; restart atomically changes known-never-sent `Reserved → RetryAuthorized`, then uses the same ordered sender and current-authority validation. | §2 R-A4; acceptance 32/40 |
| D3 — different token against Pending has two outcomes | Detach-cell classification is pre-binding step 0a/0b: exact token gets stable status; a different token against Pending gets only `DetachInProgress`; nonmatching Committed continues ordinary lookup. | §4 R-C0/R-C1; §5 R-D1; acceptance 41 |
| D4 — direct detach has no `Committed` producer | Immediate detach atomically appends, transitions floor, replaces the old cell, and writes `Committed` with the allocated sequence. | §4 R-C0; acceptance 35 |
| D5 — Pending detach reuses a consumed epoch | Pending replay compares cell epoch to current progress, drains first after progress, and if still blocked rewrites cell epoch plus interest atomically; SDK rows never demote to a consumed epoch. | §2 R-A4; §4 R-C0; acceptance 41 |
| D6 — two bindings on one connection/conversation are unaddressable | V1 permits one participant binding per `(connection_incarnation, conversation_id)`; different-id binding attempts return non-secret `ConnectionConversationBindingOccupied`. | §4 R-C1/R-C5; §5 R-D1; §7; acceptance 42 |
| D7 — parking is unbounded and unarmable across conversations | Added signed SDK-wide parked-conversation/row/byte caps, atomic scoped reservation, and durable interest-slot ownership; one-connection recovery validates parked conversations against recoverable slots. | §2 R-A4; §5 R-D1; acceptance 32/40 |
| D8 — renegotiation omits request/row maxima | Compatibility checks every aggregate and per-row/request dimension and reports the first offending dimension without sending or dropping rows. | §2 R-A4; §5 R-D1; acceptance 40 |
| D9 — product byte ceiling cannot independently bind | Added configurable signed per-conversation parked-byte cap below-or-equal to the checked row-count×row-size product; exact full-row bytes consume it. | §2 R-A4; acceptance 32 |
| D10 — durable order counters have no finite domain | Fixed `park_order` and `transaction_order` to checked `u64`, added typed terminal exhaustion outcomes, allowed only empty-set transactional reset of `park_order`, and counted its fixed eight bytes. | §2 R-A2/R-A4; §4 R-C2/R-C4; §5 R-D1; acceptance 43 |
| D11 — maximum generation strands an unretirable member | At `u64::MAX`, valid current credential authorizes tokenized unbound terminal Leave, releasing marker/membership claims and tombstoning without wrap or cursor authority. | §4 R-C1/R-C2; §5 R-D1; acceptance 44 |
| D12 — retained marker can make finalization permanent | Added fenced provisional recovery: a valid owner attach may accept its delivered retained marker and append the pending-finalization/Attached handoff in one durable transaction, releasing the anchor. | §4 R-C1/R-C3/R-C4; acceptance 45 |
| D13 — `RecordAdmissionUnknown` is absent from R-D1 | Added exact SDK-local fields and mandatory delete-row/never-resend transition. | §5 R-D1 |
| D14 — acceptance invents a replay request | Removed the nonexistent request and instead asserts that only `AttachBound` starts server-driven replay. | §4 acceptance 39 |
| D15 — LAW-1 prerequisite inventory omits three poll loops | Added pinned membership, health, and shutdown polling evidence, mandatory event-driven replacements, and named implementation-dependency sockets. | §1; §7 |

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
| Cluster membership is polled. | `PollLoop` repeatedly calls `membership.poll_once()` and sleeps for `POLL_INTERVAL` (`crates/liminal-server/src/cluster/membership.rs:198-228`). | `«MEMBERSHIP-EVENT-SOURCE»` must replace it with pushed membership-delta notification plus explicit shutdown wake before conformance. |
| Health accept is polled. | The health worker loops over nonblocking `accept`, reads a shutdown flag, and sleeps on `WouldBlock` (`crates/liminal-server/src/health/endpoint.rs:104-123`). | `«HEALTH-ACCEPT-SHUTDOWN-WAKE»` must use blocking/readiness accept and an explicit shutdown wake before conformance. |
| Shutdown drain and settle are polled. | Drain repeatedly reaps/counts active connections and sleeps; post-force-close repeats the same pattern (`crates/liminal-server/src/server/shutdown.rs:191-225`, `crates/liminal-server/src/server/shutdown.rs:229-244`). | `«SHUTDOWN-DRAIN-NOTIFICATION»` must replace both loops with connection-exit/drain completion notification and an admitted deadline event. |
| Existing caps have a signed configuration pattern. | `LimitsConfig` defines named hard caps and defaults, rejects zero by field name, and constructs explicit defaults (`crates/liminal-server/src/config/types.rs:193-203`, `crates/liminal-server/src/config/types.rs:257-316`). | Keepalive, retention, receipt lifetime/count, and negotiated SDK parking extend this pattern rather than creating silent unlimited states. |

**`«RESUME-COMMENT-SERVER-MISMATCH»` is broader than a comment edit.** The
legacy public model is subscription-keyed and client-cursored, while R-C1/R-C3
are participant/conversation-keyed and server-cursored. §7 requires an owner to
choose distinct protocols or a versioned deprecation/removal; merely correcting
the comment cannot close the socket.

**LAW-1 prerequisite inventory.** The participant implementation may not inherit
the listener, cluster-membership, health-accept, or shutdown drain/settle loops
above. Main and health accepts use blocking/readiness notification plus explicit
shutdown wakes; membership changes are pushed by an event source; connection exit
updates a drain completion primitive; an admitted shutdown deadline may race that
primitive but never samples it. Process exit likewise wakes its owner. The three
missing event-API dependencies are named in §7. No periodic reap, count/check,
`sleep` backoff, synthetic wake, or “temporary” polling adapter is conforming.

**Silence attack / gap acceptance.** Refute this section by identifying an
existing frame that jointly carries participant identity, conversation identity,
a durable authorized cursor, replay position, and lifecycle verdict, or by
showing server Subscribe consumes such a cursor. A nearby field with narrower
scope does not close the gap. Conversely, finding the named listener loop does
not refute R10; it proves the explicit retirement prerequisite remains unmet.

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

The conversation's serialized admission lane allocates one checked fixed-width
`u64 transaction_order` for every caller admission or server event entering its
durable total order, whether or not it creates a candidate, and assigns
`admission_order = (transaction_order, ascending_participant_index)` to each
candidate from that transaction. Values `0..=u64::MAX` are allocatable once. The
transaction allocating MAX atomically sets an exhausted flag; the **next** attempt
returns typed terminal `ConversationOrderExhausted { conversation_id, counter:
TransactionOrder, value: u64::MAX }` before candidate/log/participant mutation.
The major component never wraps, saturates, aliases, resets, or rebases; the minor
index is bounded by the signed identity-slot cap. A connection-fate
event at that boundary remains recorded in its existing bounded identity cell as
terminally unsequencable and requires operator shutdown; it is never relabeled or
polled. At one billion allocations per second, `2^64` values last about 584 years;
real deployments assess remaining values against their actual conversation-local
rate. This is physical unreachability context, not an infinity claim or a polling
monitor. Terminal finalizations and R-C4 reconstructed `HistoryCompacted` markers
share this one order. Each identity slot owns at most one pending finalization and
one pending marker cell, so the candidate set is bounded by twice the signed
identity-slot cap. Crash recovery reads it directly. Observer progress or the
operator recovery event wakes the finite set; the lane drains candidates strictly
by `admission_order` before caller **record admission**. Append-free normal/marker acks remain independently admissible under
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

Add these signed nonzero limits, each with a signed default advertised in
negotiated participant capability state under the verified `LimitsConfig` pattern
(`crates/liminal-server/src/config/types.rs:193-203`,
`crates/liminal-server/src/config/types.rs:257-316`):

- per conversation: `max_parked_observer_requests_per_conversation` (`N`) and
  `max_parked_observer_bytes_per_conversation` (`C`);
- per SDK: `max_parked_conversations` (`P`),
  `max_parked_observer_requests_per_sdk` (`G`), and
  `max_parked_observer_bytes_per_sdk` (`D`); and
- per row: `max_participant_request_bytes` (`R`) and
  `max_parked_observer_row_bytes` (`B`).

`R` covers the complete encoded participant request. `B` covers the complete
durable serialization: conversation and row keys, storage framing/version/checksum,
state and length fields, encoded request, any separately indexed attempt token,
participant id, presented generation/incarnation, epoch, retry metadata, and the
fixed eight-byte `u64 park_order`. Token and identifier widths are protocol
constants. The row is a fixed-layout tagged union: reservation charges the exact
encoded request plus the largest state-variant fields for that row, so later state
transitions cannot grow its charged full-row bytes. Startup/negotiation rejects `B` smaller than `R` plus the schema's
maximum metadata/framing bytes; checked-product overflow; `C > N × B`; or
`D > G × B`. The configured `C` and `D`, not those products, are independently
binding exact full-row byte ceilings. One-connection recovery additionally rejects
`P > max_participant_conversations_per_connection`. Thus every durable parked
conversation can own a recoverable interest slot.

Before sending an operation that can backpressure, the SDK encodes it. A request
above `R` returns local `SdkParticipantRequestTooLarge { conversation_id,
encoded_bytes, limit }`. For the first row of a conversation it atomically reserves
one durable parked-conversation/interest slot; that slot remains counted until the
last row is deleted. It then atomically reserves the full serialized row against
all applicable dimensions: per-conversation rows `N` and bytes `C`, SDK-wide
conversations `P`, rows `G`, and bytes `D`. Failure returns
`SdkObserverParkCapacityExceeded { scope: PerConversation | SdkWide, dimension:
Conversations | Rows | Bytes, conversation_id, limit, occupied, requested }`; no
row/frame/server state is created, and an empty newly reserved interest slot is
rolled back. A non-backpressure or terminal unknown-fate transition releases the
row (`RecordAdmissionUnknown` does so); deleting the last row releases its slot.
Tokenized response loss keeps one counted row only while exact token fate remains
recoverable. All table/set/counter storage is therefore bounded by `P`, `G`, `D`,
`N`, and `C`; no connection lifetime can accumulate another unbounded set.

`park_order` is a checked `u64` stored with every row. When a conversation's
parked-row set is empty, the next reservation transaction may reset its durable
counter to zero and allocate zero; this is its **only** reuse rule. Otherwise it
checked-increments. Values through `u64::MAX` are allocated once; allocating MAX
sets an exhausted flag, and the next nonempty-set reservation returns terminal
local `SdkParkOrderExhausted { conversation_id, counter: ParkOrder, value:
u64::MAX }`, reserves nothing, and never wraps, saturates, aliases, or rebases.
At a sustained billion allocations per second, exhausting all `2^64` values takes
about 584 years. Deployment-honest interpretation uses the actual per-conversation
allocation rate and remaining values; this arithmetic justifies a terminal edge,
not “infinite,” and creates no periodic monitor or rebase mechanism.
The reservation transaction persists the complete encoded request, token,
authority, byte accounting, and order in known-never-sent `Reserved`. The other
states are `AwaitingObserverProgress { refused_epoch }`, `RetryAuthorized`,
`InFlight`, and `TokenFateRecovery`.

Normal sending and restart use one lowest-`park_order` sender per conversation.
Before any network write it revalidates current authority and durably changes the
row to `InFlight`. On restart, one transaction changes every `Reserved` row to
`RetryAuthorized`: those rows are known never sent because no write may precede
the `InFlight` commit. They then enter the same ordered sender. Restart drains
`RetryAuthorized`, recovers `InFlight`/`TokenFateRecovery`, and never skips an
earlier blocked row. An `InFlight` crash uses the operation's attempt token; an
untokenized ordinary operation returns `RecordAdmissionUnknown`, deletes its row,
and is never resent. It never assumes that an unobserved write failed.

`ObserverProgressed { conversation_id, refused_epoch, observer_progress }` is a
connection-level control event, not a promise that a particular request now fits.
A backpressure epoch is the immutable `observer_progress` value at refusal in the
same checked non-wrapping sequence domain. Every refusal at an unchanged baseline
returns the same epoch; actual progress makes every later refusal strictly greater.
No historical epoch map is retained. An SDK that has durably consumed progress E
must reject any transition that would demote a row to
`AwaitingObserverProgress(E' <= E)`; it instead performs the serialized status or
operation retry that obtains a current baseline.

The server pushes the event to every live connection that received the epoch's
refusal, including pre-enrollment and unbound replacement-attach connections. One
fixed-size `(conversation, epoch)` interest covers all bounded rows at that
baseline. The SDK's durable interest slot is reserved before its first row, while
the server's connection-local recipient is installed on refusal or reconnect;
connection loss discards only the server copy, never the SDK reservation. The
negotiated signed `max_participant_conversations_per_connection` bounds attached
conversations plus refusal-only recipients; a first live-connection conversation
beyond it returns `ConnectionConversationCapacityExceeded` before mutation.

The one-shot reconnect handshake carries at most all `P` durable
`observer_refusals: [{ conversation_id, refused_epoch }]`; duplicate conversations
are `InvalidObserverEpochList`. For each unique entry, one serialized comparison is
exhaustive: older returns progressed/unarmed without disturbing a newer arm;
equal atomically subscribes then snapshots; newer returns
`InvalidObserverEpoch` and arms nothing. Because `P` is validated against the
recoverable connection slots, every Awaiting conversation can be included in one
handshake. Equality linearizes either after progress (older branch) or before it
(installed recipient receives the push), never between both.

One matching signal/status atomically changes the entire matching
`AwaitingObserverProgress` cohort to `RetryAuthorized` before any send. Restart
before that transaction repeats reconnect comparison; restart after it resumes
ordered drain. Terminal results delete rows. Repeated refusal may rewrite a row
only to a strictly newer epoch, without changing `park_order`. Crashes before
cohort marking, between rows, or after writes are determined by durable state.

Current-authority invalidation, stale/expiry, `Retired`, or
`CredentialRecoveryLost` atomically visits **all** bounded row states, including
`Reserved`. Known-never-committed rows terminalize with the typed authority result
and delete; an in-flight tokenized request or accepted pending detach becomes
`TokenFateRecovery` only while exact fate remains recoverable. Stable committed,
stale, retired, expiry, or `StaleOrUnknownReceipt` deletes it. No old request is
silently reissued under new authority. Normal and marker acks never park.

Restart/renegotiation checks every dimension before sending any parked row:
conversation and SDK-wide row/byte counts; parked-conversation count; each stored
request length against new `R`; each full serialized row length against new `B`;
and `P` against recoverable connection slots. Any violation returns
`SdkParkingCapacityIncompatible { scope, dimension, conversation_id,
park_order?, occupied_or_actual, negotiated_limit }`, preserving every row and
sending none until operator/configuration correction. `Reserved` rows and interest
slots participate. It never grandfather-discard rows. The only request-sized
waiter is this SDK-wide and per-conversation bounded set; every retry is TOLD by
startup, connection readiness, response, observer progress, or authority change,
never a timer, poll, or sweep.

**Idle-cost closure.** Per SDK, parked conversation/interest headers are bounded by
signed P, rows by signed G, and fully serialized bytes by signed D; per-conversation
subsets are additionally bounded by N/C and each row by R/B. Per server
conversation, `ClosureDebt` and `transaction_order` are fixed header fields,
marker/finalization/detach cells are bounded by signed I, retained log bytes/entries
by their signed caps, and connection-local binding/interest maps by signed
`max_participant_conversations_per_connection`. Resettable `park_order` is one
fixed header per currently parked conversation. No new R10 table, set, counter,
cell, recipient interest, or recovery token exists outside one of those bounds.

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

R10 retains two different liveness classes:

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
`«KEEPALIVE-PORTABILITY»`; R10 does not manufacture numbers.

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

**R-B6 — Product polling retirement is a LAW-1 prerequisite.** Every loop named
in §1 must retire: main/health accepts become blocking or readiness-driven with
explicit shutdown wakes; membership deltas are pushed; process/connection exit
signals drain completion; an admitted shutdown deadline races that notification.
The implementation dependencies are §7 sockets. Retention, replay, lifecycle,
D5 epoch rewrite, D11 unbound Leave, and D12 fenced recovery may not add polling
under another name.

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
`HistoryCompacted` outcome if retention passed it. Inventory main/health accepts,
cluster membership, shutdown drain/settle, and every participant thread, timer,
task, and write: none may periodically ask whether state changed; each named §1
loop is absent and its event/shutdown/deadline race is exercised. The false-positive assertion for Class 2 is as
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

An immediately admissible explicit detach performs terminal append, retention-
floor transition, old-cell replacement, binding release, and
`Empty | Committed(old) → Committed { token, participant_id,
committed_generation, committed_incarnation, detached_delivery_seq: allocated }`
in **one** durable transaction. Thus the ordinary success path, not only recovery,
is a normative `Committed` producer; a crash immediately before sees the old
binding/cell, and a crash after sees the complete new record/cell.

If the terminal record cannot yet append, acceptance ends transport/cursor
authority and atomically changes the binding to R-A2 `PendingFinalization` **and**
the cell to `Pending`; no delivery sequence exists or is fabricated. Detach-cell
classification precedes binding lookup: the exact token selects this Pending
status, while a different token against Pending returns only non-secret
`DetachInProgress { participant_id, presented_generation,
committed_incarnation }`, commits nothing, and never reveals the stored token. A
nonmatching token against `Committed` receives no special status and continues
through the ordinary tombstone/generation/binding order.

Exact-token Pending replay compares `refused_epoch` with current
`observer_progress` at the serialized point. On equality it returns the current
cell status unchanged. If progress is greater, it first attempts the ordered
candidate drain. Success performs terminal append, floor transition, candidate
removal, binding-slot release, and `Pending → Committed` with the real allocated
sequence in one transaction. If still blocked, one transaction replaces the
cell's `refused_epoch` with current progress, installs that epoch's recipient
interest, and returns `ObserverBackpressure` with the **new** cell epoch. The SDK
must not rewrite a row to an epoch at or below progress it already consumed. A
progress/replay race therefore either drains or arms the current epoch, never
reuses a spent wake. A crash before, during, or after either transaction exposes
only one complete Pending or Committed state.

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
`1`; every credential-bearing success checked-increments the fixed-width `u64`
`capability_generation` and rotates the secret. It never wraps, saturates, aliases,
or rebases. Attach at `u64::MAX` returns typed terminal `GenerationExhausted`
without mutation; the credential-bearing unbound terminal-Leave escape below
closes the identity machine. Connection authentication remains the shared bearer
gate, not an ACL. Participant cursor authority is separate.

V1 has exactly one participant binding per
`(connection_incarnation, conversation_id)`. Before enrollment mint or credential-
attach binding mutation, the serialized lane checks that slot. Empty permits the
operation; a slot for the same presented participant permits rotation/supersession;
a different participant—or enrollment, whose identity is not minted yet—returns
`ConnectionConversationBindingOccupied { conversation_id,
presented_participant_id: Option<ParticipantId> }`, with `None` for enrollment.
It reveals no occupying id, generation, incarnation, or binding state and commits
no receipt, identity, rotation, or record. Exact-token result lookup still
precedes this check. This invariant makes conversation-only delivery demux
unambiguous without adding recipient identity to every delivery frame:

1. Enrollment carries mandatory `{ conversation_id, enrollment_token }`. There is
   no credential-free bare frame. One commit mints `participant_id`, generation
   `1`, and `attach_secret`; creates R-C2 membership at cursor `0`; binds the
   originating connection; records `Attached`; stores the enrollment fingerprint
   and live receipt; and returns typed `Bound` with that generation and secret.
   Subject to the empty connection/conversation binding slot above, distinct
   enrollment tokens create distinct participants; when occupied, refusal happens
   before mint. Collision retries before publication and never alias identity,
   sequence, or cursor state.
2. Enrollment or attach receipt replay follows R-C0. In particular, recovery on
   a replacement connection is never reported as attached: it returns
   `UnboundReceipt`. The SDK first atomically persists its non-stale generation
   and secret, then sends a **new** credential-bearing attach with a fresh
   write-ahead token. Only that new `Bound` result enables replay or ack authority
   on the replacement connection.
3. A credential-bearing attach presents `{ participant_id,
   capability_generation, attach_secret, attach_attempt_token,
   accept_marker_delivery_seq?: Option<DeliverySeq> }`. The server
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

**Fenced provisional recovery at a retained marker.** This is the only attach path
that may combine cursor abandonment with binding. It is available only when the
presented current generation/secret is valid, no different live binding occupies
the connection/conversation slot, the participant owns the named retained
unaccepted marker, that marker was durably recorded as delivered to its last
authoritative incarnation, and ordinary handoff fails only for R-C4 closure
headroom. The request must present that marker's exact sequence in
`accept_marker_delivery_seq`; omission or mismatch follows ordinary attach and
cannot acknowledge anything.

One durable transaction revalidates all proofs; accepts the marker by advancing
the cursor to its sequence without claiming abandoned payload delivery; releases
the marker anchor; drains this identity's one earlier `PendingFinalization` when
present; allocates the contiguous terminal/`Attached` records (one or two, never
more than `(Qe,Qb)`); rotates generation/secret; recomputes the floor and
`ClosureDebt`; and binds the new incarnation. It returns `AttachBound` with
`accepted_marker_delivery_seq`. No provisional binding is externally visible and
no new record class exists. The marker component allocates no sequence; only the
pending terminal and `Attached` records do. Crash exposes either the entire old
marker/cursor/pending state or the complete new cursor/floor/records/credential/
binding. Invalid proof returns the existing marker/authority outcome with no
mutation. Retry uses the attach token and is driven only by response/connection/
observer events, never a timer.

**Finite generation domain.** At a sustained billion successful attaches per
second **for one participant**, reaching `u64::MAX` takes about 584 years; actual
per-participant attach rate makes the horizon longer. This deployment-honest
arithmetic explains why the terminal edge is physically remote, not absent. It
creates no wrap, rekey epoch, rebase, periodic monitor, or polling policy.

**Exhaustive participant-reference lookup.** At the serialized participant-state
point, every decoded request that names a participant uses this total precedence
before operation-specific checks:

0a. An exact bounded attempt-token receipt, exact `detach_replay::Pending` token,
    or exact `detach_replay::Committed` token returns its stable operation result.
0b. A Pending detach cell with a **different** detach token returns
    `DetachInProgress`; this classification precedes binding lookup. A nonmatching
    token against Committed gets no special result and continues below.
1. A tombstone for the **presented** `(conversation_id, participant_id)` returns
   operation-specific `Retired`.
2. No live identity and no tombstone returns non-secret
   `ParticipantUnknown { conversation_id, participant_id }`.
3. A live identity whose current generation differs from the presented generation
   returns `StaleAuthority`.
4. For a binding-required operation, a matching live generation without this
   connection's current incarnation returns
   `NoBinding { conversation_id, participant_id, presented_generation }`.
5. The operation executes its remaining proof/admission checks.

Credential attach and exact receipt/status lookup do not require an existing
binding. Normal/marker ack, new detach, ordinary Leave, and ordinary record
admission do. The sole additional exception is R-C2's generation-maximum unbound
terminal Leave, which proves the still-current secret and grants no cursor or
binding authority. Server-driven replay runs only inside a successful
`AttachBound`; it is not a request. `ParticipantUnknown` and `NoBinding` leave the
connection open and terminalize the attempt without receipt, cursor, candidate,
identity, record, or other durable state. The explicit identity oracle exists only
after shared connection authentication; neither outcome reveals another holder's
binding/incarnation.

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
and net `T` is unchanged. Fenced provisional recovery costs one `Attached` plus
its pending terminal when present (one or two contiguous values): marker acceptance
costs no sequence, the pending terminal consumes old `T`, and Attached creates new
`T`. A marker append costs `1` and removes one `M`. A terminal detach, death,
bound Leave, or max-generation unbound Leave costs `1`, removes its terminal or
membership claim, and may convert
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
4. **Leave is terminal and is not detach.** Ordinarily only the authoritative
   bound incarnation may send `LeaveRequest { participant_id,
   capability_generation, attach_secret, leave_attempt_token }`; shared connection
   authentication and stale incarnations are insufficient, and v1 defines no
   operator Leave. The one machine-closure exception applies when the live
   identity's current generation is exactly `u64::MAX`: the same request may be
   sent unbound and is authorized by exact current-generation/current-secret proof
   at the attach serialization point. It grants no binding, cursor, replay, ack,
   or fresh credential and may only retire. The SDK write-aheads the token.
   Success uses the existing mandatory-sequence/closure reserve, appends one
   ordered `Left`, releases any unaccepted-marker anchor plus membership/cursor,
   invalidates the secret, and mints the tombstone atomically. If append is
   temporarily observer-blocked, the identity's one bounded token-bearing pending
   finalization cell preserves this Leave for event-driven drain. This exception
   is as strong as attach proof but strictly terminal, so it does not weaken the
   binding rule for any operation that can gain cursor authority.
5. **One Leave commit retires everything.** It appends one ordered R-A1 `Left`
   record, terminalizes any active binding and the membership, invalidates current
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
two versioned routes: a current binding's `MarkerAck`, or R-C1's fenced provisional
attach by the same credential owner when ordinary handoff lacks closure headroom.
Both name the marker's own `delivery_seq`, explicitly abandon the entire interval—including records still physically retained at or above the old floor—and
atomically advance that participant's cursor to the marker sequence. Those
payloads are not asserted or marked as delivered.

For `MarkerAck`, the server requires delivery to the same current incarnation. For
fenced recovery, it requires the durable delivery fact to the participant's last
authoritative incarnation plus current-generation/current-secret proof. Every
other attempt spanning abandonment is refused. Until the
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
`crates/liminal-server/src/config/types.rs:257-316`). At participant-mode startup, let `I` be configured per-conversation
`max_retired_identity_slots`; `(1,Bm)` the maximum encoded entry/byte cost of one
`HistoryCompacted`; and `(Qe,Qb)` the maximum encoded cost of exactly these
**mandatory classes**: enrollment, the ordered supersession pair, Leave, immediate
detach terminal, marker append, and pending-finalization drain. R-C1 fenced
recovery is charged as its pending-finalization/supersession class. Keys, fixed
`u64 transaction_order`, and storage framing are included. Checked startup
validation requires:

- `max_retained_conversation_entries >= Qe + (I × 1)`; and
- `max_retained_conversation_bytes >= Qb + (I × Bm)`.

Overflow is configuration failure. No other transaction may consume `(Qe,Qb)`.
In particular, ordinary records and an ordinary detached credential attach must
preserve it. Mandatory records have no reachable `RecordTooLarge`; ordinary
caller-sized records may.

Each conversation header has one fixed-size durable
`ClosureDebt { entry_debt: 0..=Qe, byte_debt: 0..=Qb,
last_consuming_transaction_order }` cell, initialized to zero. It adds no independent
set cardinality. Let `S` be the actual retained suffix plus maximum encoded cost
for every planned-but-unwritten marker; let `U` be distinct live identities with
an unaccepted or planned marker. With zero debt, the **ordinary closure envelope**
for each dimension is:

`S + ((I - U) × marker_max) + Q <= configured_cap`.

Ordinary admission must leave that equation true and returns
`MarkerClosureCapacityExceeded` otherwise. Mandatory classes collectively consume
at most the one `Q` term: a later mandatory commit may use its unconsumed remainder
or reuse it after same-transaction prefix removal, but may never make debt exceed
Q. Its post-commit debt is

`d' = max(0, S' + ((I - U') × marker_max) + Q - configured_cap)`,

computed separately for entries/bytes after candidate, floor, and marker fixed
point. The candidate is already in `S'` and is never added again as another `Q`.
Commit requires both `S' + remaining_marker_reserve <= configured_cap` and
`d' <= Q`; it then stores `d'` atomically. At the minimum startup cap, a first
empty-conversation enrollment therefore appears once in `S'`, consumes that much
of Q as debt, and commits; it does not demand `candidate + Q + I`.

While either debt component is nonzero, ordinary caller admission returns the
existing `MarkerClosureCapacityExceeded`. A mandatory candidate may still commit
only when fixed-point prefix removal or remaining Q keeps both recomputed debt
components within Q and absolute fit holds; otherwise a caller request receives
that refusal and an already accepted server candidate remains only in its signed
identity cell. Marker append is
special only in accounting: it swaps one planned maximum marker reservation in
`S` for its actual record, so it may drain while debt exists and cannot increase
debt. Append-free ack, observer progress, cursor progress, marker acceptance, and
floor compaction recompute debt downward in their transaction; once both
components are zero the Q slot is restored. R-C1 provisional recovery and the
maximum-generation terminal Leave may run in debt only if their anchor release
makes post-commit debt strictly smaller and the absolute-fit check succeeds. All
waits are woken by those durable events, never a timer or scan.

The physical floor remains reproducible. Let `H'` be the candidate watermark;
`F` the current first retained sequence (`H+1` when empty); `m` the minimum member
cursor after membership changes, or `H'` with no members; `o` the hard
`observer_progress`; and `a` the earliest live-member unaccepted marker sequence,
or checked one-past-end `END=MAX+1`. `END` is never allocatable. Define
`preferred_floor=min(m,o)+1`; `cap_floor` as the smallest floor at least `F` that
fits actual records plus the applicable normal/debt envelope without exceeding
`a`; and `F'=max(F,preferred_floor,cap_floor)`, valid only when `F'<=a`.
Successful commit appends at `H'`, removes exactly `[F,F')`, and stores floor/debt
atomically. Marker acceptance, fenced provisional recovery, or Leave releases its
owner's anchor; nothing else does. Observer hard retention forbids
`cap_floor>o+1` and produces `ObserverBackpressure` before mutation. Member claims
remain soft. Persisted progress emits the wake; no poll does.

Before any floor/membership trigger changes state, its finite fixed-point preflight
starts with existing marker owners plus every member the proposed floor overtakes,
simulates candidates in `admission_order`, pins all simulated/unaccepted markers,
and adds each newly overtaken uncovered live identity once. At most `I` strict set
growth steps reach a fixed point. For ordinary work, the postcondition is the full
zero-debt envelope. For every mandatory class, the postcondition is the
absolute-fit plus bounded-`d'` equations above—candidate counted once regardless
of prior debt. For a marker
append, one planned slot becomes actual and the debt cannot grow. Failure returns
`MarkerClosureCapacityExceeded { conversation_id, dimension, required, limit,
marker_owners, entry_debt, byte_debt }` before floor, membership, candidate,
sequence, debt, or participant mutation.

After preflight and R-C2 sequence checks, one transaction changes floor/membership
and creates one candidate per newly affected member. Simultaneous candidates use
checked `admission_order=(u64 transaction_order,
ascending_participant_index)`. At append, a candidate recomputes
`abandoned_after`, `abandoned_through`, and `physical_floor_at_decision`, then
appends `HistoryCompacted` and removes itself. The candidate drain precedes caller
records.

**Finite termination under consumable reserve.** The trigger fixed point contains
every marker its own appends induce and absolute-fit reserves all of them. Each
marker append converts one reserved slot to one actual record, does not consume Q
a second time, cannot evict an unaccepted marker, creates no replacement, and
strictly changes `M→M-1` even if ClosureDebt is nonzero. With no caller admission
interleaving, `M<=I` reaches zero in at most `I` appends. Other pending mandatory
candidates wait in bounded cells until progress repays debt; they cannot steal a
marker slot. If the equations cannot prove this before a trigger, refusal occurs
before floor change. Earlier/later finalizations remain ordered by the shared
`admission_order` rather than wake scheduling.

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

One connection may attach many conversations, but R-C1 permits at most one
participant binding for each `(connection_incarnation, conversation_id)`.
Participant frames are therefore unambiguously demuxed by `conversation_id`; v1
`stream_id` is fixed to zero and carries no identity, ordering, cursor, or
correlation meaning. Per-conversation replay/ack streams may interleave, but each
conversation's order is preserved. A second different participant is refused
before mint/bind under `«MULTI-BINDING-PER-CONVERSATION»`; v1 neither adds recipient
id to deliveries nor silently multiplexes two cursors. Connection teardown
finalizes every active binding through R-A2.

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
   `Attached` record and no ghost. On distinct connections with empty conversation
   binding slots, distinct enrollment tokens create distinct participants; the
   occupied-slot case is acceptance 42. Replay after receipt expiry and prove typed `ReceiptExpired`
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
25. Refuse participant-mode startup below either checked `Q + I×marker`
    retention floor; on SDK product overflow, `C>N×B`, `D>G×B`, invalid row schema,
    or parked conversations above recoverable slots. Then start at **exact equality
    from an empty conversation**: first enrollment commits once, consumes Q into
    bounded `ClosureDebt`, and is not double-counted; cursor/observer progress
    repays debt. Exercise every detach outcome, prove acks never backpressure and
    ordinary response loss remains at-most-once ambiguous, then re-run case 21.
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
32. With varied row sizes, hit per-conversation row cap N while below byte cap C
    and verify exact full-row accounting. Separately configure `C<N×B` and hit C
    below N with large rows. Repeat at SDK-wide parked-conversation, row, and byte
    caps; every first-row conversation already owns an armable interest slot and
    scoped capacity fields name the limiting dimension. Oversize request returns
    `SdkParticipantRequestTooLarge`. Crash **exactly after `Reserved` commit and
    before first `InFlight`**: restart converts it to `RetryAuthorized`, revalidates
    authority, and sends it once in lowest `park_order`. Also crash before cohort
    mark, between rows, after write-before-response, and after repeated refusal;
    no path exceeds a cap, leaks a slot, or polls.
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
35. For an immediately appendable detach, crash immediately before and after the
    one direct append/floor/binding/cell transaction: observe either the old state
    or `Committed` with its allocated sequence, never an absent/partial producer.
    Replay echoes generation/incarnation/sequence after binding is gone. Reattach,
    terminalize the replacement, then replay old token: `StaleAuthority` reports
    `binding_state: Detached` with no current incarnation.
36. Separately from case 17, commit T at G+1 and then a valid G+1→G+2 attach before
    T's deadline. T replay inside provenance is exactly
    `ReceiptExpired { reason: Superseded }`; case 17 alone proves `Deadline`.
37. With `Qe=2,I=3`, reject entry cap 2 and accept exact minimum 5. From an empty
    conversation bootstrap first enrollment at equality; it commits once with
    debt 1, not a six-entry demand. Use distinct connections and event-driven
    progress to prepare three live detached members at the same cursor with empty
    suffix/debt zero. Append one Qe-sized mandatory transaction (debt 2), then a
    second Qe-sized mandatory trigger whose cap floor removes the first pair and
    overtakes all three; its fixed point owns all marker slots while debt stays 2.
    At cap 5, append all
    three by reserved-slot conversion: `M:3→2→1→0`, no marker eviction/replacement
    and no second Q charge; reattach receives/accepts its retained marker and
    progress repays debt. Separately make absolute fit impossible and prove typed
    pre-floor refusal leaves floor/membership/debt unchanged.
38. Bind P, offer through N, then Leave P and bind Q on the same connection. Deliver
    delayed bytes for P's normal and marker acks after Q is also offered through N.
    Their explicit P/generation tuple returns P's `Retired` and Q's cursor is
    unchanged. Repeat generation rotation for one participant and obtain
    `StaleAuthority`, never an inferred-current ack.
39. For credential attach, receipt replay, detach, Leave, normal ack, marker ack,
    and ordinary admission, present a never-minted id and assert
    `ParticipantUnknown`. For each binding-required operation present a current id/
    generation on an unbound/wrong connection and assert `NoBinding`; then exercise
    stale and retired precedence. No replay bytes appear before `AttachBound`, and
    unknown/stale/unbound attach outcomes start no server-driven replay. Ordinary
    sender always equals verified id; no failure allocates durable state.
40. Park ordered attach/ordinary/Leave rows in every state, including `Reserved`,
    then invalidate/retire/expire authority; known-no-commit rows delete and only
    exact recoverable fate enters bounded `TokenFateRecovery`. Crash immediately
    after reservation and at every cohort/per-row boundary. Renegotiate downward
    on every dimension; specifically retain a 900-byte row under new `B=500` while
    aggregate bytes remain below their cap. `SdkParkingCapacityIncompatible` names
    `RowBytes`, preserves all rows/interest slots, and sends none. Repeat for
    request maximum and parked-conversation count; prove deterministic order, no
    orphan/old-authority reissue/ack row, and no polling.
41. Backpressure detach T and lose response. Before progress, exact T returns
    Pending/current epoch/no sequence; different U uniquely returns
    `DetachInProgress` before `NoBinding`, without disclosing T. Race live replay T
    against progress in both orders: equality returns unchanged; greater progress
    drains first, or atomically rewrites cell+interest to the newer epoch. SDK never
    parks on a consumed epoch. Crash around every transaction and observe only
    complete Pending or Committed with real sequence. Replay T after commit is
    stable; a nonmatching token against Committed follows ordinary lookup.
42. Bind P to `(C,X)`. A fresh enrollment token on C/X returns
    `ConnectionConversationBindingOccupied` with no presented id; credential attach
    for Q returns it with only presented Q. Neither reveals P or mints/rotates/
    replays. Same-P rotation on C/X succeeds. Race P/Q binding attempts into an
    empty slot: one serialized winner binds and the different-id loser gets the
    typed outcome; codec vectors prove delivery remains conversation-only.
43. Test-seed nonempty parked and conversation order counters at `u64::MAX-1`.
    Allocate the final value, then prove `SdkParkOrderExhausted` and
    `ConversationOrderExhausted` at the next allocation with exact counter/scope/
    value fields, no wrap/mutation, and deterministic ordering of existing rows/
    candidates. Empty the parked set transactionally and prove only `park_order`
    resets to zero; `transaction_order` never does.
44. Seed one participant at generation `u64::MAX`, lose its connection, advance the
    floor so its unaccepted marker is retained, then send write-ahead unbound Leave
    with the exact max-generation current secret. It commits one `Left`, releases
    marker/membership/cursor claims, invalidates the secret, and returns stable
    `LeaveCommitted`/tombstone without binding or cursor authority. Wrong secret,
    lower generation, and fresh token after retirement remain typed/no-commit.
45. Deliver a retained marker to its owner and decline it. Inject enough
    death/credential-reattach cycles to consume all ordinary closure slack; make
    the next terminal fate `PendingFinalization`. A valid fresh attach token naming
    that marker executes fenced provisional recovery: one atomic transaction
    accepts the marker, releases anchor, drains the pending terminal, appends
    `Attached`, rotates credential, advances floor, and binds. Crash at every write
    boundary sees all-old or all-new; abandoned payload is never marked delivered,
    the marker allocates no sequence, and no timer/retry loop participates.

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
| SDK-local observer-wait admission | `SdkObserverParkCapacityExceeded` | `scope: PerConversation | SdkWide`, `dimension: Conversations | Rows | Bytes`, conversation id, signed limit, occupied, requested full amount | Local terminal refusal; atomically roll back empty first-interest reservation, reserve no row, send no frame, create no server state. |
| SDK-local park-order allocation | `SdkParkOrderExhausted` | conversation id, `counter: ParkOrder`, `value: u64::MAX` | Terminal while set nonempty; reserve/send nothing, preserve ordered rows; only empty-set transaction may reset. |
| SDK restart/cap renegotiation | `SdkParkingCapacityIncompatible` | scope, offending `dimension: ConversationRows | ConversationBytes | SdkConversations | SdkRows | SdkBytes | RequestBytes | RowBytes | RecoverableInterestSlots`, conversation id/park order when applicable, actual or occupied, negotiated limit | Preserve every row and interest slot, send none, require operator/config correction. |
| SDK-local untokenized response loss | `RecordAdmissionUnknown` | conversation id, presented participant id/generation, operation identity `OrdinaryRecordAdmission`, park order | Atomically delete the row/release last interest when applicable; terminal ambiguity and never resend automatically. |
| First participant operation or reconnect refusal entry for a conversation on this connection | `ConnectionConversationCapacityExceeded` | conversation id, signed connection-conversation limit | No participant mutation, interest arming, or readiness promise; use a connection with a free negotiated slot. |
| Enrollment or credential attach binding attempt | `ConnectionConversationBindingOccupied` | conversation id and `presented_participant_id: Option<ParticipantId>` (`None` for enrollment); no occupying identity fields | Terminal for this attempt; no mint/receipt/rotation/replay/binding/record; same-participant rotation remains eligible. |
| Any operation requiring `transaction_order` | `ConversationOrderExhausted` | conversation id, `counter: TransactionOrder`, `value: u64::MAX`, affected operation/candidate scope | Terminal, no wrap/rebase/alias and no log/participant mutation; preserve already bounded candidates for operator shutdown. |
| Credential attach, receipt/status replay, detach, Leave, normal ack, marker ack, or ordinary admission | `ParticipantUnknown` | presented conversation id and participant id; presented generation, token, and requested boundary when those fields exist in that operation | Terminal semantic outcome for this attempt; connection remains open and no participant/durable state is created. |
| New detach with **no Pending cell**, ordinary Leave except max-generation credential escape, normal ack, marker ack, or ordinary admission after exact-token lookup misses | `NoBinding` | presented conversation id, participant id, generation | Terminal; no other binding disclosed and no state change. Any Pending detach cell classified at step 0b instead. |
| Any listed participant-id operation with a live generation mismatch | `StaleAuthority` | presented participant id/generation and current generation; operation-specific token/boundary when present | Terminal under the old authority; no state changes. |
| Any listed participant-id operation whose presented id has a tombstone | operation-specific `Retired` | presented participant id/generation, retired generation, and operation-specific token/boundary | Terminal tombstone result; no secret, binding, live cursor, or state change. |
| Any closure-checked admission | `MarkerClosureCapacityExceeded` | conversation id, entry/byte dimension, checked required, configured limit, marker-owner count, entry/byte `ClosureDebt` | No floor, membership, participant, candidate, sequence, or debt mutation; terminal for caller admission, while accepted server candidates remain bounded for an event-driven retry. |
| Enrollment attach | `EnrollBound` | enrollment token, participant id, generation `1`, secret, receipt/provenance deadlines | Success; persist before exposing `Bound`; replay same token only after unknown response. |
| Enrollment attach | `EnrollUnboundReceipt` | same credential fields, origin incarnation | Persist, then fresh-token credential attach; never expose binding. |
| Enrollment attach | `EnrollmentKnown` | enrollment token, participant id, current generation | Committed identity, no secret/binding; use a valid current-or-newer credential or enter `CredentialRecoveryLost`; never remint. |
| Enrollment attach | `ReceiptExpired` | token, participant id, result/current generations, `Deadline` or `Superseded` | Exact only inside the enrollment provenance window; use a valid current-or-newer credential or enter `CredentialRecoveryLost`. |
| Enrollment attach | `Retired` | token, participant id, retired generation | Terminal; preserve identity for operator record, no attach. |
| Enrollment attach | `ReceiptCapacityExceeded` | token, `scope`, `limit` | No SDK auto-retry; surface typed admission refusal. |
| Enrollment attach | `IdentityCapacityExceeded` | token, `scope`, `limit` | Terminal for new enrollment; no participant was minted. |
| Enrollment attach | `ObserverBackpressure` | token, observer progress, backpressure epoch | Persist `AwaitingObserverProgress`; retry once after matching `ObserverProgressed` or reconnect status. |
| Enrollment attach | `ConversationSequenceExhausted` | token, high watermark, remaining capacity, resulting `T`, `M`, and `L × T` budgets | Terminal for this mint; reserve check includes cursor-0 marker creation before identity or sequence mutation. |
| Credential attach | `AttachBound` | attach token, participant id, new generation/secret, deadlines, incarnation, persisted cursor, optional accepted marker sequence | Success; generation-ordered persist, then expose binding/replay. Optional marker field is present only after atomic fenced recovery; no ack parking state exists. |
| Credential attach | `AttachUnboundReceipt` | same credential fields, origin incarnation | Persist, then fresh-token attach; no replay/ack authority. |
| Credential attach | `ReceiptExpired` / `StaleOrUnknownReceipt` / `Retired` | attach token and fields defined above | Never retry same request; preserve newer credential or enter the specified terminal state. |
| Credential attach | `StaleAuthority` | token, presented/current generations | Terminal for this request; preserve current durable credential. |
| Credential attach | `GenerationExhausted` | token, participant id, current generation `u64::MAX` | Terminal for attach with no commit/wrap; preserve current secret and enter `CapabilityGenerationExhausted`, from which only exact-token status or max-generation terminal Leave is allowed. |
| Credential attach with `accept_marker_delivery_seq` | `MarkerNotDelivered` / `MarkerMismatch` | token, participant id/generation, presented marker seq, retained/delivered marker seq when non-secret, reason | Terminal for this attach attempt; no marker accept, cursor, floor, rotation, record, or binding mutation. |
| Credential attach | `ReceiptCapacityExceeded` / `ObserverBackpressure` / `ConversationSequenceExhausted` | token plus capacity/epoch fields; exhaustion adds high watermark, remaining capacity, resulting `T`/`M`/`L × T`, and required cost (`2` for supersession) | Capacity is surfaced; backpressure enters a shared epoch; reserve exhaustion precedes rotation, binding, floor, and marker changes. |
| Receipt replay | `Bound` / `UnboundReceipt` | exact token plus live receipt fields | Same-connection binding or persist-then-fresh-attach respectively. |
| Receipt replay | `ReceiptExpired` | exact fingerprint, reason, result/current generations | Exact only inside provenance window; not retryable; preserve newer credential or `CredentialRecoveryLost`. |
| Receipt replay | `StaleAuthority` | fresh token absent from complete in-window set, presented/current generations | Proves no commit for this token; not retryable. |
| Receipt replay | `StaleOrUnknownReceipt` | token, presented/current generations | Post-provenance ambiguity; claims no commit; no automatic retry. |
| Receipt replay | `Retired` | token, participant id, retired generation | Terminal; no secret or binding. |
| `DetachRequest` | `DetachCommitted` | detach token, participant id, cell-retained committed generation/incarnation and detached delivery seq | Success; echo comes only from `detach_replay::Committed`, never reused live-binding state; stable until next successful attach/Leave. |
| `DetachRequest` exact-token replay while cell is Pending | `ObserverBackpressure` | detach token, participant id, presented generation, committed incarnation, **current cell epoch per rewrite rule**, current progress; no delivery sequence | Equal returns unchanged; greater progress drains first or atomically rewrites cell+interest to newer epoch. Never park on a consumed epoch or create a second candidate. |
| `DetachRequest` different token while cell is Pending | `DetachInProgress` | presented token, participant id, presented generation, committed incarnation; never the stored token | Terminal for the competing attempt; no state change or sequence. |
| `DetachRequest` | `StaleAuthority` | detach token, presented generation/incarnation, current generation, `binding_state: Bound { current_incarnation } | Detached` | Terminal after cell overwrite; absent live incarnation is represented by `Detached`, never a sentinel or retained fake current binding. |
| `DetachRequest` | `Retired` | detach token, participant id, retired generation | Terminal tombstone result after Leave; preserve operator identity, no record. |
| `DetachRequest` first accepted while append is blocked | `ObserverBackpressure` | detach token, generation/incarnation, epoch/progress | One atomic transition writes `PendingFinalization` plus `detach_replay::Pending`; progress wake atomically appends and converts Pending to Committed with the real sequence. |
| `ParticipantAck { conversation_id, participant_id, capability_generation, through_seq }` | `AckCommitted` | presented participant id/generation and requested `through_seq`, matched persisted cursor | Success only after tuple+binding match; advance SDK watermark. |
| `ParticipantAck` | `AckNoOp` | presented participant id/generation, requested boundary, unchanged matched cursor | Success; idempotent confirmation under the same authority. |
| `ParticipantAck` | `AckGap` / `AckRegression` / `StaleAuthority` | presented participant id/generation, requested/current cursor, reason | Terminal for this ack; do not advance SDK watermark. |
| `ParticipantAck` | `Retired` | presented participant id/generation, requested `through_seq`, retired generation | Tombstone lookup uses the presented id; no secret, binding, or live/current cursor is present. |
| Bound `LeaveRequest` | `LeaveCommitted` | leave token, participant id, retired generation, `left_delivery_seq` | Success; terminal participant state. Duplicate token returns same result. |
| Unbound `LeaveRequest` at generation `u64::MAX` with current secret | `LeaveCommitted` | leave token, participant id, presented/retired max generation, `left_delivery_seq`, `unbound_terminal_escape: true` | Terminal success only: release marker/membership/cursor, tombstone, persist stable result; never bind or grant replay/ack authority. |
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

A valid terminal detach, bound Leave, or max-generation unbound terminal Leave
never returns `ConversationSequenceExhausted`: its active/pending/membership claim
owns one reserved value.
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
liveness detector. The receive brief must be implementable from explicit R10
facts, except where §7 names a genuine gate.

## 7. Named sockets and decision register

R10 distinguishes decisions made by the draft from genuine unknowns. A
**decided-by-draft** row has one candidate answer and may change only by explicit
key refusal/revision. A **genuinely-open** row names unavailable platform
evidence, an implementation linearization mechanism, a layer-above policy, or
an owner migration decision. Open rows are not permission for implementations
to choose incompatible semantics silently.

| Socket | Status | R10 answer or reason openness is genuine | Closure/refutation evidence |
|---|---|---|---|
| `«ATTACH-TRANSACTION»` | **decided-by-draft** | Attach, detach, and Leave use mandatory write-ahead tokens and one serialized participant state. The identity slot's tagged detach cell is `Empty`, token-bearing `Pending` without a sequence, or `Committed` with the real generation/incarnation/sequence; it is the sole stable-echo source until next attach/Leave. | All send/commit/response windows, same/different token while Pending, crash around Pending→Committed, echo after binding removal, stale replay after replacement death, detach loss across attach/Leave, replacement recovery, and terminal receipt tests. |
| `«RECEIPT-LIFETIME»` | **decided-by-draft** | Signed receipt TTL/count and non-secret provenance TTL/count caps bound both bodies and classifiers. Cleanup uses admitted deadlines; TTL is the maximum supported recovery outage and expiry may enter `CredentialRecoveryLost`. | Delayed generation, crash order, exact/unknown before/after provenance, cap exhaustion, composed outage, and no-sweep tests. |
| `«RETIRED-IDENTITY-BOUND»` | **decided-by-draft** | Enrollment reserves signed server/conversation identity slots; each slot owns the enrollment-token mapping for the live+tombstone lifetime, and Leave converts it to a permanent tombstone. Cap exhaustion refuses before mint, bounding churn without weakening `EnrollmentKnown`, `Retired`, or duplicate `LeaveCommitted`. | Post-GC enrollment T versus fresh U, boundary churn, no-ghost replay, lost Leave response, and pre-mint capacity refusal. |
| `«PARTICIPANT-ID-ORIGIN»` | **decided-by-draft** | Server mints participant id, generation 1, and initial secret inside tokenized enrollment; later attach proves current generation/secret and atomically increments/rotates before cursor access. External ids never authorize a cursor. | Authorization-before-cursor, collision, enrollment replay, generation ordering, rotation loss, and reconnect tests. |
| `«MEMBERSHIP-BOUNDARY»` | **decided-by-draft** | Membership begins at mint with cursor `0`, survives detach, entitles full history and offline commits, and ends only with authoritative tokenized Leave that retires id, receipts, and soft retention claim. | Late join, offline replay, Leave authority/idempotency/races, and floor-transition tests. |
| `«LEAVE-AUTHORITY»` | **decided-by-draft** | Ordinarily only the bound incarnation with current generation/secret may Leave. At generation `u64::MAX` only, the same current credential authorizes unbound tokenized terminal retirement; it grants no cursor/binding authority. V1 has no operator Leave. | Shared-bearer/stale-proof refusals, duplicate stable result, attach/Leave races, and max-generation connection-loss/marker-anchor escape. |
| `«ACK-SHAPE»` | **decided-by-draft** | Normal `ParticipantAck { conversation_id, participant_id, capability_generation, through_seq }` and `MarkerAck { conversation_id, participant_id, capability_generation, marker_delivery_seq }` carry presented authority. At one serialized point the server looks up the presented id, matches generation+binding, then validates continuity or named abandonment. Post-Leave uses the presented id's tombstone and never a live cursor. | P-ack-after-Q-bind ambiguity, same-id rotation, unknown/unbound/stale/Retired codec vectors, normal gap/regression, marker-delivery refusal, and proof abandoned payload is not marked delivered. |
| `«COMPACTION-EXIT»` | **decided-by-draft** | A pending marker shares the durable server-candidate admission order with finalizations; at append it recomputes `abandoned_after..abandoned_through` and the physical-floor snapshot. Acking abandons through that pre-marker watermark and advances to marker sequence. | Both marker/finalization orders, append-time fields, retained-suffix choice, marker loss/redelivery, repeated-cycle, concurrent-live, and post-ack flow tests. |
| `«RETENTION-UNITS»` | **decided-by-draft** | Signed byte/entry caps start at `Q + I×marker`. Ordinary work preserves Q; named mandatory classes consume it into one bounded `ClosureDebt` cell, candidate counted once. Fixed-point absolute fit reserves every marker; each append swaps planned for actual and reduces M even in debt. Fenced recovery/max-generation Leave may release anchors while repaying debt. | Empty-conversation equality bootstrap, independently computed entry/byte debt, three-marker minimum-cap drain, no double count/eviction, provisional anchored recovery, `MAX-2` sequence boundary, and closure-refusal atomicity. |
| `«MULTI-CONVERSATION-MUX»` | **decided-by-draft** | Yes. One connection carries many conversations, demuxed by `conversation_id`; participant `stream_id = 0` and has no semantic role. | Cross-conversation interleaving and independent-cursor tests. |
| `«MULTI-BINDING-PER-CONVERSATION»` | **decided-by-draft (excluded in v1)** | At most one participant binds each `(connection_incarnation, conversation_id)`. A different-id enrollment/attach gets `ConnectionConversationBindingOccupied` containing only its presented optional id; same-id rotation is allowed. | Empty-slot race, enrollment with no presented id, Q-after-P refusal without P disclosure, same-P rotation, and conversation-only delivery codec. |
| `«LIFECYCLE-VERDICT-RECIPIENTS»` | **decided-by-draft** | Every member is entitled to every lifecycle/compaction record in total order, including while detached, unless it explicitly accepts a named abandonment after compaction broke continuity. | Three-party lifecycle/compaction races, offline replay, and explicit-abandonment tests. |
| `«LIFECYCLE-OBSERVER-DELIVERY»` | **decided-by-draft** | The log is sole completed lifecycle history and observer progress hard. Signed per-conversation and SDK-wide conversation/row/full-byte caps plus request/row maxima bound parking; durable first-row interest slots make all Awaiting conversations armable. `Reserved` restart, checked u64 order, cohort marking, authority loss, epoch monotonicity, and all-dimension renegotiation are explicit. Acks never park. | Independent row/byte/global cap hits, interest-slot recovery, Reserved crash, per-row downward incompatibility, near-max order, epoch races, credential loss, and no orphan/ack/poll. |
| `«SUPERSESSION-FENCE»` | **decided-by-draft** | Every credential-bearing successful attach checked-increments generation and rotates the secret; stale proof is a connection-level no-record refusal. | Two-holder generation race, single handoff pair, rotation receipt recovery, and stale-proof zero-record tests. |
| `«RESUME-COMMENT-SERVER-MISMATCH»` | **genuinely-open** | Owner must choose separate legacy subscription recovery versus version/deprecate/delete it for participant attach. Outline governs over the earlier tear's preference to decide now; R10 nevertheless makes “comment-only fix” insufficient. | Owner ruling covering types, cursor owner, starting convention, persistence, release boundary, and removal/compatibility tests. |
| `«MEMBERSHIP-EVENT-SOURCE»` | **genuinely-open implementation dependency** | External behavior is fixed: membership deltas must be pushed and shutdown must wake the source. The selected membership backend's non-poll event API is not yet chosen. | Backend event subscription/callback with ordered delta and explicit shutdown tests; delete `PollLoop`, `poll_once` cadence, and sleep. |
| `«HEALTH-ACCEPT-SHUTDOWN-WAKE»` | **genuinely-open implementation dependency** | External behavior is fixed: health accept blocks/uses readiness and explicit shutdown interrupts it. The concrete cross-platform wake mechanism is not selected. | Per-target accept/shutdown race tests with no WouldBlock sleep or shutdown-flag sampling loop. |
| `«SHUTDOWN-DRAIN-NOTIFICATION»` | **genuinely-open implementation dependency** | External behavior is fixed: connection exit updates a completion primitive raced against one admitted deadline; force-close uses the same notification. Concrete supervisor API is not selected. | Exit/drain notification API, crash/force-close/deadline races, and deletion of reap/count/sleep loops. |
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
timers, wakes, loops, and tasks—including main/health accept, membership, and
shutdown drain/settle—and prove each is driven by admitted work, kernel connection
fate, explicit shutdown/process exit, an admitted deadline, or an existing domain
event rather than change detection.

---

**Gate posture:** DESIGN DRAFT R10. All earlier-round decisions remain in force.
R10 closes the fifteen dispatcher-verified defects found by both independent final
examiners in R9: consumable reserve, Reserved restart, detach precedence/direct/
epoch paths, single binding, global parking and every renegotiation/byte dimension,
finite counters, maximum-generation retirement, fenced anchored recovery, missing
local taxonomy, executable acceptance, and complete LAW-1 inventory. The socket
register separates decisions from real implementation dependencies. Reviewer key plus Hermes
Crumpet's liminal domain-owner key are still required; until both turn, this
document is not ratified and grants no implementation authority.
