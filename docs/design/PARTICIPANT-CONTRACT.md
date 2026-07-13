# Participant-domain wire/server contract — design draft R11

**Status: DRAFT — decisions made by this draft, not yet ratified.** This redraft
parks at the two-key gate: reviewer-of-record plus the liminal domain-owner pass
(Hermes Crumpet). “Decided-by-draft” below means that R11 selects one contract
shape for those keys to accept or refute; it does not grant implementation
authority before both keys turn.

**Author:** Vesper Lynd. **Drafter:** Sol worker.

## 0. Provenance, authority, and laws

### 0.1 Provenance

This draft is pinned to liminal commit
`ce8814daa748373d8ffc66b3ff1664f1697a5f4e`, confirmed as the merge base of this
design branch. Every repository citation retained from R1–R10 or added in R11 was
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
decisions recorded in its change ledger.

R11 is the final-boundary redraft required by the latest integrated examiner
ruling. That ruling found three executable-acceptance defects and eight semantic
holes spanning sequence exits, closure debt, unbound Leave, finite ordering,
receipt binding, equal-generation secret proof, stale outbound work, binding
epoch identity, and three additional LAW-1 polling families. R11 closes all
items in §0.4 without changing the frozen laws or relaxing the two-key gate.

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

### 0.4 R10 → R11 changelog

Driver: latest integrated examiner ruling over the complete R10 artifact. Every
item below is closed in normative prose, exhaustive outcomes, and executable
acceptance rather than by an implementation placeholder.

| Refusal item | R11 decision | Where |
|---|---|---|
| E1 — sequence reserve does not pay a detached member's eventual `Left` | Every live member owns one flat exit claim `E=L`; the checked invariant is `MAX-H >= E+T+M+(L×T)+(L_other×E)`. Binding terminals remain separately charged, and bind/detach/Leave transitions consume or create the named claims atomically. | §4 R-C2; acceptance 26/46/47 |
| E2 — acceptance 37 starts from an unreachable detached-marker state | Replaced the premise with an API-reachable setup using distinct connections, real enrollment/ack/projection/compaction and supersession events, and explicit postcondition checks before the minimum-cap drain. | §4 acceptance 37 |
| E3 — finite-boundary tests invent production mutation APIs | Defined a test-only durability-backend seeding convention; generation, sequence, transaction-order, and park-order boundary cases use it explicitly, and no production request can set counters. | §4 acceptance preamble and 21/26/31/43/44/47 |
| S1 — marker acceptance can increase reserve and closure debt can circularly deny recovery | A marker owns one capacity credit from planning through physical compaction; acceptance cannot increase reserve. Detached attach is a mandatory closure class, and nonzero/full debt requires a persisted TOLD repayment edge under absolute-fit, byte/entry, and sequence checks. | §4 R-C4; acceptance 37/45/48/49 |
| S2 — unbound Leave exists only at generation maximum | Any detached live member with exact current generation/secret may tokenized-Leave. If an earlier binding terminal is pending, one atomic transaction appends that terminal first and `Left` second, including at full debt. | §4 R-C1/R-C2/R-C4; §5 R-D1; acceptance 44 |
| S3 — accepted finalizations can outlive `transaction_order` | Active bindings and live members hold consumable major-order claims. Marker candidates share their causal major order, candidate drains allocate no new major, and only work outside those claims can receive `ConversationOrderExhausted`. | §2 R-A2; §4 R-C2; §5 R-D1; acceptance 43 |
| S4 — three production polling families are absent from LAW-1 evidence | Added pinned evidence for channel-reply liveness polling and both SDK TCP reader timeout/stop-flag loops, mandated event races plus explicit shutdown wake, and registered three implementation sockets. | §1; §7; acceptance 50 |
| S5 — a live receipt can report `Bound` after the slot changed | `Bound` now requires the receipt's exact participant binding epoch still occupy the origin slot; otherwise exact replay is `UnboundReceipt`, even on the same physical connection. | §4 R-C0/R-C1; §5 R-D1; acceptance 42 |
| S6 — equal-generation wrong secret has no exhaustive classification | Every secret-bearing operation compares the secret after tombstone/identity and generation lookup; mismatch returns the existing `StaleAuthority` with equal presented/current generation and no side effect or new oracle. | §4 R-C1; §5 R-D1; acceptance 13/44 |
| S7 — queued participant bytes can cross detach/rebind | Queued work is keyed by participant plus binding epoch and revalidated at the sole final construction site; stale work is dropped; constructed bytes precede the same-writer detach/rebind response, while cross-stream handoff atomically retires the old SDK epoch. | §4 R-C5; §5 R-D2; acceptance 42 |
| S8 — connection incarnation does not identify same-connection rotations | Lifecycle, finalization, delivery, and receipt state use immutable `binding_epoch=(connection_incarnation, capability_generation)`, so same-connection rotation fences old work and yields distinct ordered lifecycle facts. | §2 R-A1/R-A2; §4 R-C1/R-C5; acceptance 42 |

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
| Channel command reply liveness is polled. | `LIVENESS_POLL` is 10ms and `poll_reply` repeatedly `recv_timeout`s, queries the process table, and samples the command deadline (`crates/liminal/src/channel/actor/wait.rs:21-24`, `crates/liminal/src/channel/actor/wait.rs:73-95`). | `«CHANNEL-REPLY-EVENT-RACE»` must race the reply, a pushed process-exit notification, and the one admitted command deadline; no repeated liveness query may remain. |
| The SDK push reader polls for local shutdown. | The reader installs a 100ms read timeout, maps timeout to `None`, and loops to re-read an atomic stop flag (`crates/liminal-sdk/src/remote/tcp/push_client.rs:52`, `crates/liminal-sdk/src/remote/tcp/push_client.rs:378-385`, `crates/liminal-sdk/src/remote/tcp/push_client.rs:460-510`). | `«SDK-PUSH-READER-SHUTDOWN-WAKE»` must race blocking/readiness input and an explicit local shutdown wake that interrupts the reader; socket silence is not a clock. |
| The SDK subscription reader polls for local shutdown. | The reader installs the same 100ms timeout and treats timeout as a chance to re-check its stop flag (`crates/liminal-sdk/src/remote/tcp/subscription.rs:47`, `crates/liminal-sdk/src/remote/tcp/subscription.rs:229-233`, `crates/liminal-sdk/src/remote/tcp/subscription.rs:327-382`). | `«SDK-SUBSCRIPTION-READER-SHUTDOWN-WAKE»` must race blocking/readiness input and an explicit local shutdown wake that interrupts the reader; no timeout/flag recheck loop may remain. |
| Existing caps have a signed configuration pattern. | `LimitsConfig` defines named hard caps and defaults, rejects zero by field name, and constructs explicit defaults (`crates/liminal-server/src/config/types.rs:193-203`, `crates/liminal-server/src/config/types.rs:257-316`). | Keepalive, retention, receipt lifetime/count, and negotiated SDK parking extend this pattern rather than creating silent unlimited states. |

**`«RESUME-COMMENT-SERVER-MISMATCH»` is broader than a comment edit.** The
legacy public model is subscription-keyed and client-cursored, while R-C1/R-C3
are participant/conversation-keyed and server-cursored. §7 requires an owner to
choose distinct protocols or a versioned deprecation/removal; merely correcting
the comment cannot close the socket.

**LAW-1 prerequisite inventory.** The participant implementation may not inherit
the listener, cluster-membership, health-accept, shutdown drain/settle, channel-
reply liveness, SDK push-reader, or SDK subscription-reader loops above. Main and
health accepts use blocking/readiness notification plus explicit shutdown wakes;
membership changes are pushed by an event source; connection exit updates a drain
completion primitive; and an admitted shutdown deadline may race that primitive
but never samples it. A command wait races reply, pushed process exit, and its one
admitted deadline. Each SDK reader races blocking/readiness socket input against
an explicit local shutdown wake. All six missing event-API dependencies are named
in §7. No periodic reap, count/check, timeout-as-wake, atomic-flag recheck, `sleep`
backoff, synthetic wake, or “temporary” polling adapter is conforming.

**Silence attack / gap acceptance.** Refute this section by identifying an
existing frame that jointly carries participant identity, conversation identity,
a durable authorized cursor, replay position, and lifecycle verdict, or by
showing server Subscribe consumes such a cursor. A nearby field with narrower
scope does not close the gap. Conversely, finding the named listener loop does
not refute R11; it proves the explicit retirement prerequisite remains unmet.

## 2. Section (a) — participant lifecycle at the participant boundary

### Proposed contract

**R-A1 — Typed cause, participant-domain owner.** Introduce `CloseCause` (final
name subject to domain-owner review) and a separate participant-domain observer,
working name `ParticipantLifecycle`. A binding is identified by the immutable
`BindingEpoch { connection_incarnation, capability_generation }` captured by its
attach commit. Rotating on the same physical connection therefore creates a new
epoch and can never reuse the old generation's lifecycle, receipt, finalization,
or queued-delivery identity. Binding-domain facts identify
`(participant_id, conversation_id, binding_epoch)`; each committed lifecycle
record and observer callback also carries its canonical delivery key
`(conversation_id, delivery_seq)`:

- `Attached { participant_id, conversation_id, binding_epoch }`;
- `Detached { ..., binding_epoch, cause: CloseCause }` for an explicit detach,
  clean Disconnect, authorized superseding attach, or server-directed shutdown;
- `Died { ..., binding_epoch, cause: CloseCause }` for connection or process
  failure; and
- `Left { participant_id, conversation_id,
  ended_binding_epoch: Option<BindingEpoch> }` for R-C2's explicit, durable,
  terminal membership transition, never for transient detach or connection loss.
  The option is `Some` only when the same commit terminalizes an active binding;
  an already-detached or already-finalized member has no live epoch to invent.

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
  an older binding epoch under R-C1, including on one connection incarnation. It is an explicit participant-domain action,
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
| Successful R-C1 attach commit | One `Attached` record naming the newly committed binding epoch; a failed/rolled-back attach commits none. |
| Explicit participant detach | `Detached(CleanDeregister)` names exactly that binding epoch; membership and cursor remain durable. |
| Explicit participant Leave | One ordered `Left` is both the active binding epoch's terminal record when bound and the terminal membership record. If a distinct old binding terminal is already pending, that exact record appends first in the same composition; R-C2 then retires id, receipts, cursor, and retention claim atomically. |
| Clean protocol Disconnect | `Detached(CleanDeregister)` for every binding still active on that connection. |
| Authorized replacement by a newer binding epoch, including rotation on the same connection incarnation | `Detached(Superseded)` for the exact old epoch, then `Attached` for the exact new epoch in conversation order. |
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
fences the old binding epoch; active plus pending bindings therefore cannot exceed
the signed identity-slot caps. A binding slot is not reusable until its terminal
record commits. When explicit detach has been accepted, clean Disconnect,
EOF/read/write failure, linked-EXIT or local process termination, shutdown, or
protocol error arrives and the log cannot append, one atomic durable transition
changes the binding to `PendingFinalization { participant_id, conversation_id,
binding_epoch, original_cause, event_kind, admission_order }`. For explicit
detach, that transaction also writes R-C0's token-bearing
`detach_replay::Pending` with its presented binding epoch and refusal epoch;
connection-fate candidates have no client token. Transport/cursor authority ends
immediately, but the exact original cause and binding epoch remain durable.

The conversation's serialized admission lane owns a checked fixed-width `u64`
major `transaction_order`. Persist it as `Option<u64>` so the exact number of
allocatable values is derivable in checked `u128`: `2^64` when absent and
`u64::MAX - high` otherwise. Values `0..=u64::MAX` allocate once and the major
never wraps, saturates, aliases, resets, or rebases. Candidate order is
`(transaction_order, candidate_phase, ascending_participant_index)`; the phase is
a fixed protocol enum and the participant index is bounded by the signed identity
cap. All candidates caused by one transaction—including every marker found by
R-C4's fixed point—share that major without aliasing.

Finite ordering is backed by consumable claims, not physical-longevity prose. Let
`A` be active binding epochs whose terminal fate has no assigned major and let
`X=L` be live members whose tokenized `Left` has no assigned major. Leave is never
accepted into a separate server pending state, so every stable live member retains
that exit claim. Checked arithmetic preserves the complete major-order reserve:

`order_remaining >= A + X`.

One `A` major orders the binding terminal **and every marker candidate in its R-C4
fixed point** by phase/participant minor; one `X` major similarly orders `Left` and
all exit-caused markers. Those marker reconstructions share the causal major and
never allocate another one. A bound Leave consumes one of its `A`/`X` majors and
releases the other; when a binding terminal was assigned earlier, composed Leave
uses that preserved old major for the terminal and consumes `X` for the later
`Left`. Marker append, marker acceptance, normal ack, status, and candidate drain
are continuations of already ordered work and allocate no major.

Any optional allocation whose post-state would make
`order_remaining < A + X` returns `ConversationOrderExhausted` with current high,
remaining values, reserved claims, and the operation's required/resulting claims,
even before numeric high reaches MAX. It mutates nothing and may not borrow a
claim. A terminal fate consumes `A` while assigning its immutable major and cannot
be refused for order exhaustion. Supersession pays an unreserved caller major,
uses it for the old terminal/new `Attached` pair, and transfers old `A` to the new
epoch. Enrollment pays its major and creates `A+1,X+1`; detached attach pays its
major and creates `A+1`. Both check the resulting expression before mint/bind.

Current pending finalizations and marker candidates already carry immutable
`admission_order`; draining one allocates no second major. Allocation of
`u64::MAX` is legal only when the post-transaction `A+X` is zero. Afterward the
conversation can still drain already ordered work and perform append-free ack/
status, but a new-major operation returns `ConversationOrderExhausted {
conversation_id, counter: TransactionOrder, value: u64::MAX,
order_remaining: 0, reserved_claims: 0 }` before mutation. An accepted terminal
fate can never become “unsequencable.” At one billion allocations per second,
`2^64` values last about 584 years; this is physical context, not an infinity
claim, monitor, or substitute for the invariant.

Each identity slot owns at most one pending finalization and one pending marker
cell, so the candidate set is bounded by twice the signed identity-slot cap.
Crash recovery reads it directly. Observer progress or the operator recovery
event wakes the finite set; the lane drains candidates strictly by
`admission_order` before caller **record admission**. Append-free normal/marker
acks remain independently admissible under R-D1 and do not reorder the drain.
Terminal-record append, retention transition, candidate deletion, and binding-
slot release are one durable transaction. No mailbox, absence inference, poll,
sweep, or fabricated later cause participates.

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
key and the same immutable binding epoch as the wire record. Applications requiring an
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
participant id, presented generation/binding epoch, refusal epoch, retry metadata, and the
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
conversation, `ClosureDebt`, its repayment-edge tag, and `transaction_order` are
fixed header fields; binding-epoch and order/exit-claim tags live in the already
bounded identity slots; marker/finalization/detach cells are bounded by signed I;
retained log bytes/entries by their signed caps; and connection-local binding/
interest maps by signed `max_participant_conversations_per_connection`. Resettable
`park_order` is one fixed header per currently parked conversation. No new R11
table, set, counter, cell, recipient interest, or recovery token exists outside
one of those bounds.

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
`PendingFinalization` with original cause/binding-epoch/order, the binding has no
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

R11 retains two different liveness classes:

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
`«KEEPALIVE-PORTABILITY»`; R11 does not manufacture numbers.

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
signals drain completion; and an admitted shutdown deadline races that
notification. Channel commands race reply, pushed process exit, and their one
admitted deadline. SDK push/subscription readers race socket readiness with an
explicit local shutdown wake, never a read timeout used to sample a stop flag.
The implementation dependencies are §7 sockets. Retention, replay, lifecycle,
epoch rewrite, detached Leave, fenced recovery, order/debt repayment, and reader
shutdown may not add polling under another name.

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
cluster membership, shutdown drain/settle, channel reply waits, both SDK TCP
readers, and every participant thread, timer, task, and write: none may
periodically ask whether state changed; each named §1 loop is absent and its
reply/process-exit/deadline, readiness/shutdown, or domain-event race is exercised. The false-positive assertion for Class 2 is as
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
- `Pending { token, participant_id, committed_binding_epoch,
  admission_order, refused_epoch }`; or
- `Committed { token, participant_id, committed_binding_epoch,
  detached_delivery_seq }`.

An immediately admissible explicit detach performs terminal append, retention-
floor transition, old-cell replacement, binding release, and
`Empty | Committed(old) → Committed { token, participant_id,
committed_binding_epoch, detached_delivery_seq: allocated }`
in **one** durable transaction. Thus the ordinary success path, not only recovery,
is a normative `Committed` producer; a crash immediately before sees the old
binding/cell, and a crash after sees the complete new record/cell.

If the terminal record cannot yet append, acceptance ends transport/cursor
authority and atomically changes the binding to R-A2 `PendingFinalization` **and**
the cell to `Pending`; no delivery sequence exists or is fabricated. Detach-cell
classification precedes binding lookup: the exact token selects this Pending
status, while a different token against Pending returns only non-secret
`DetachInProgress { participant_id, presented_generation,
committed_binding_epoch }`, commits nothing, and never reveals the stored token. A
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
binding epoch or sequence. Stability lasts only until the identity's next
successful attach or Leave. Attach atomically clears the cell and terminalizes
its old token as `StaleAuthority { presented_generation,
committed_binding_epoch, current_generation, binding_state }`, where
`binding_state` is either `Bound { current_binding_epoch }` or `Detached`; no
current epoch is fabricated for a gone replacement binding. Leave replaces the
cell with tombstone-precedence `Retired { participant_id, retired_generation }`.
Thus cycling stores one cell. After response loss the SDK replays while no newer
attach/Leave is durable. A newer `AttachBound` makes the SDK record
`AuthoritySuperseded` and never resend; later stale replay is not evidence detach
failed. `Retired` terminalizes while preserving operator identity.

Every live attach receipt contains `{ participant_id, capability_generation,
attach_secret, origin_binding_epoch, receipt_expires_at }`. Exact replay returns
`Bound` only when the origin connection/conversation slot still contains that
participant at that exact binding epoch. If the slot is empty, contains another
participant, or contains a later epoch—even on the same physical connection—replay
returns `UnboundReceipt` with the same credential payload and never mutates the
slot. This post-receipt binding comparison is non-secret and occurs after exact-
token lookup; it cannot rotate, detach, or disclose the current occupant. Leave
overrides every receipt with non-secret `Retired`.

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
without mutation. Any detached live member may instead use the current credential
for R-C2's tokenized unbound terminal Leave; generation maximum needs no special
escape. Connection authentication remains the shared bearer gate, not an ACL.
Participant cursor authority is separate.

For every secret-bearing credential attach or Leave, the serialized authority
check is exhaustive: after tombstone/identity lookup it compares generation, then
compares the presented secret in constant time. A generation mismatch or an
equal-generation secret mismatch returns the existing `StaleAuthority` carrying
presented and current generations. Equality of those fields identifies the latter
without a new discriminant or additional secret oracle. It commits no receipt,
order, cursor, binding, lifecycle record, candidate, or retention mutation.

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
   originating connection at epoch `(connection_incarnation, 1)`; records
   `Attached` with that epoch; stores the enrollment fingerprint and live receipt;
   and returns typed `Bound` with that generation, secret, and binding epoch.
   Subject to the empty connection/conversation binding slot above, distinct
   enrollment tokens create distinct participants; when occupied, refusal happens
   before mint. Collision retries before publication and never alias identity,
   sequence, or cursor state.
2. Enrollment or attach receipt replay follows R-C0. Recovery is reported as
   attached only while the exact receipt binding epoch still occupies its origin
   slot; every replacement, detach, or later same-connection rotation returns
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
   cursor and new `BindingEpoch`, and terminalizes R-C0's prior detach replay cell.
   If a binding epoch is active—even on this same connection incarnation—it
   records exactly one ordered `Detached(Superseded)`/`Attached` handoff naming
   the old/new epochs and fences every old-epoch operation and queued delivery.
4. Lost rotation response is recovered with the invalidated old secret and same
   token only while its receipt is live. R-C0 returns `Bound` only if the receipt's
   exact participant/binding epoch still occupies its origin slot, otherwise
   `UnboundReceipt`, with no second rotation or handoff. After completion/expiry it returns the typed non-secret outcome, never
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
authoritative binding epoch, and ordinary handoff fails only for R-C4 closure
headroom. The request must present that marker's exact sequence in
`accept_marker_delivery_seq`; omission or mismatch follows ordinary attach and
cannot acknowledge anything.

One durable transaction revalidates all proofs; accepts the marker by advancing
the cursor to its sequence without claiming abandoned payload delivery; releases
the marker anchor; drains this identity's one earlier `PendingFinalization` when
present; allocates that terminal and `Attached` in global admission order (one or
two records for this identity, never more than `(Qe,Qb)`); rotates
generation/secret; recomputes the floor,
marker capacity credit, `ClosureDebt`, and its repayment edge; and binds the new
binding epoch. Absolute fit plus `d'<=Q` is sufficient even when incoming debt is
full: anchor release must lower debt or persist the exact `ObserverProjection`
edge. It returns `AttachBound` with `accepted_marker_delivery_seq`. No provisional binding is externally visible and
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

0a. Exact-token lookup runs R-C0's receipt/cell state machine before ordinary
    identity lookup. An exact **live** attach receipt returns its stored credential
    payload plus the current `Bound`/`UnboundReceipt` classification; a terminalized
    or expired attach token returns only its currently retained non-secret R-C0
    outcome. An exact `detach_replay::Pending` or `detach_replay::Committed` token
    returns that cell's stable operation result.
0b. A Pending detach cell with a **different** detach token returns
    `DetachInProgress`; this classification precedes binding lookup. A nonmatching
    token against Committed gets no special result and continues below.
1. A tombstone for the **presented** `(conversation_id, participant_id)` returns
   operation-specific `Retired`.
2. No live identity and no tombstone returns non-secret
   `ParticipantUnknown { conversation_id, participant_id }`.
3. A live identity whose current generation differs from the presented generation
   returns `StaleAuthority`. If the operation carries `attach_secret`, an equal-
   generation constant-time mismatch also returns `StaleAuthority`; equal
   presented/current fields classify it without a new oracle.
4. For a binding-required operation, matching live authority without this
   connection's exact current binding epoch returns
   `NoBinding { conversation_id, participant_id, presented_generation }`.
5. The operation executes its remaining proof/admission checks.

Credential attach, exact receipt/status lookup, and R-C2 terminal Leave by an
already-detached member do not require an existing binding. Normal/marker ack,
new detach, bound Leave, and ordinary record admission do. Detached Leave proves
the exact still-current generation and secret and grants no cursor, replay,
binding, or fresh-credential authority. Server-driven replay runs only inside a successful
`AttachBound`; it is not a request. `ParticipantUnknown` and `NoBinding` leave the
connection open and terminalize the attempt without receipt, cursor, candidate,
identity, record, or other durable state. The explicit identity oracle exists only
after shared connection authentication; neither outcome reveals another holder's
binding epoch or connection incarnation.

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
terminal records, `M` required-but-unwritten `HistoryCompacted` markers, `L` live
members, `E=L` the flat member-exit claims, and
`L_other = if L == 0 { 0 } else { L - 1 }` computed in the checked wide domain.
Every live member owns exactly one sequence value for its eventual `Left`, whether
bound, detached, marker-blocked,
or at generation maximum. A bound member also keeps its separate binding-terminal
claim in `T`. Checked wide arithmetic preserves after every commit:

`MAX - H >= E + T + M + (L × T) + (L_other × E)`.

`E`, `T`, and `M` are durable owed-record claims. `E` and `T` remain separately
charged for a bound member even though one bound `Left` can discharge both; when a
prior terminal is pending they pay two distinct records. The first product
reserves a possible marker for all `L` members after each binding terminal; the
second reserves one for each of the `L_other` members remaining after an exit. In particular, one detached member with no binding or
marker has required reserve exactly `E=1`, so its `Left` may allocate `MAX` and
the post-commit high watermark is `H=MAX`; a pre-commit `H=MAX,E=1` state is
correctly unreachable. For a candidate costing `k` records, the transaction computes the
resulting membership, exit/terminal claims, floor, and complete R-C4 marker fixed
point, then checks this invariant and R-C4's independent entry/byte closure
invariant. Sequence failure returns `ConversationSequenceExhausted` before an
optional mint, attach, supersession, ordinary record, floor, candidate, or sequence
mutation. It is unreachable for a terminal event already backed by `E` or `T`.
A required marker is never discarded before append, and an appended-unaccepted
marker is never compacted.

The transition accounting is exhaustive:

- Enrollment costs one `Attached`, creates one `E` and one `T`, and includes every
  cursor-0 marker in resulting `M`; it commits only with all resulting claims.
- Attach from detached costs one and creates `T` while preserving that member's
  existing `E`. Supersession costs two contiguous values, consumes old `T`, creates
  replacement `T`, and leaves `E` unchanged. Both are optional and may be refused
  before mutation.
- A binding terminal costs one, consumes `T`, preserves `E`, and converts at most
  `L` released product values into `M`. Active-to-`PendingFinalization` changes no
  sequence count; its eventual append performs that conversion.
- A detached Leave costs one and consumes `E`. A bound Leave with no earlier
  pending terminal appends one `Left` that discharges both `T` and `E`. If the old
  binding terminal is already pending, R-C2 appends that record first and `Left`
  second, consuming exactly the two claims. Any exit can create markers for the
  remaining members from the product decrease without invading another claim.
- A marker append costs one and changes `M→M-1`. Fenced recovery costs one
  `Attached` plus its prior pending terminal when present; marker acceptance costs
  no sequence, old `T` is consumed when drained, new `T` is created, and `E`
  survives. Ordinary records cost one and every overtaken member enters `M` in the
  same preflight.

If a binding dies while Leave races, serialization yields either bound Leave's
single combined record, or the terminal record followed by detached Leave; it can
never lose or double-spend `E`/`T`. Thus bind→detach→bind→detach cycles preserve
one `E` throughout and create/consume exactly one `T` per binding. Record append,
claim/marker changes, retention transition, and candidate deletion are one commit;
failed optional work consumes no value and sequence never wraps. The current
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
4. **Leave is terminal and is not detach.** `LeaveRequest { participant_id,
   capability_generation, attach_secret, leave_attempt_token }` is legal either
   from the exact authoritative bound epoch or while the live member is detached.
   Both paths require constant-time proof of the exact current generation/secret;
   shared authentication, an old epoch, a stale generation, or an equal-generation
   wrong secret is insufficient, and v1 defines no operator Leave. The detached
   path grants no binding, cursor, replay, ack, or fresh credential and may only
   retire. The SDK write-aheads the token.

   With no prior binding terminal pending, success uses the member's flat `E`
   claim, appends one ordered `Left` (also discharging active `T` when bound),
   releases every marker anchor plus membership/cursor, invalidates the secret,
   and mints the tombstone atomically. Leave itself never creates a server pending
   cell. If hard observer progress is the blocker, `ObserverBackpressure` leaves
   the member/secret/binding unchanged and only the bounded SDK row waits; any
   independent closure-equation failure is the cross-cutting R-D1
   `MarkerClosureCapacityExceeded` with no automatic retry. If an
   earlier detach/death terminal occupies the identity cell, the lane preflights
   one atomic composition that drains all globally earlier candidates, appends
   that identity's original terminal record before
   its `Left`, releases `T` and `E`, applies floor/credit/debt changes, and writes
   the tombstone. R-C4 permits this composition at full debt under absolute fit
   because anchor/membership release leaves a persisted TOLD repayment edge. If
   observer hard progress still prevents absolute fit, the request remains
   unaccepted and returns `ObserverBackpressure`; the bounded SDK row retries on
   the pushed epoch. There is no circular server candidate or poll.

   Crash exposes all-old pending/member state or both ordered records plus the
   complete tombstone. Exact token replay after commit returns one stable
   `LeaveCommitted { retired_generation, prior_terminal_delivery_seq: Option,
   left_delivery_seq }`; the optional field is present only for the composed
   two-record path. This terminal proof is as strong as attach proof but cannot
   gain cursor authority.
5. **One Leave commit retires everything.** It appends exactly one ordered R-A1
   `Left` and, only when a prior binding terminal is pending, that distinct earlier
   terminal record in the same atomic composition. It terminalizes any active
   binding and the membership, invalidates current secret, converts every
   outstanding enrollment/attach receipt to R-C0
   `Retired`, releases the member cursor/soft retention claim, and permanently
   tombstones `participant_id` in one commit. The tombstone retains the enrollment
   fingerprint and successful Leave token/result but no attach secret; it converts
   the retirement slot reserved by enrollment under R-C0 and cannot exceed either
   signed cap. Tombstone lookup precedes live capability validation: duplicate
   Leave with that token returns the stable `LeaveCommitted {
   retired_generation, prior_terminal_delivery_seq: Option,
   left_delivery_seq }` and commits no second record even though the secret is now
   invalid; any other
   later enrollment/attach/detach/Leave replay returns
   `Retired { participant_id, retired_generation }` with no secret, binding, new
   identity, or record.
6. **Attach/Leave races have one order.** If attach linearizes first, it rotates
   generation and binding epoch; the competing old-epoch Leave fails typed
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
connection's exact current binding epoch to match. Tombstone lookup is therefore
on the id in the ack bytes, not an inferred current connection binding. A delayed
P ack received after Q binds—or after P rotates—on the same connection can only
classify the presented authority; it can never advance the newer cursor. For
continuous history, the server then accepts the normal ack only when every
sequence from cursor + 1 through `through_seq` was made available contiguously to
that matched binding epoch. For broken history, delivery of
`HistoryCompacted { abandoned_after, abandoned_through, ... }` authorizes exactly
two versioned routes: a current binding's `MarkerAck`, or R-C1's fenced provisional
attach by the same credential owner when ordinary handoff lacks closure headroom.
Both name the marker's own `delivery_seq`, explicitly abandon the entire interval—including records still physically retained at or above the old floor—and
atomically advance that participant's cursor to the marker sequence. Those
payloads are not asserted or marked as delivered.

For `MarkerAck`, the server requires delivery to the same current binding epoch.
For fenced recovery, it requires the durable delivery fact to the participant's
last authoritative binding epoch plus current-generation/current-secret proof. Every
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
connection's authoritative binding epoch. Only then may admission derive the
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
`HistoryCompacted`; and `(Qe,Qb)` the maximum encoded cost of one transaction in
exactly these **mandatory closure classes**: enrollment, attach from detached, the
ordered supersession pair, one-record Leave, composed pending-terminal-plus-`Left`,
immediate detach terminal, marker append, pending-finalization drain, and R-C1
fenced recovery. Keys, fixed `u64 transaction_order`/candidate phase, and storage
framing are included. Checked startup
validation requires:

- `max_retained_conversation_entries >= Qe + (I × 1)`; and
- `max_retained_conversation_bytes >= Qb + (I × Bm)`.

Overflow is configuration failure. No other transaction may consume `(Qe,Qb)`.
In particular, ordinary records preserve it. A detached credential attach is now
a named mandatory closure class: it may consume Q under the debt/repayment rule
below so a legal minimum configuration cannot circularly deny the only path that
can deliver and accept an already retained marker. Mandatory records have no
reachable `RecordTooLarge`; ordinary caller-sized records may.

Each conversation header has one fixed-size durable
`ClosureDebt { entry_debt: 0..=Qe, byte_debt: 0..=Qb,
last_consuming_transaction_order, repayment_edge }` cell, initialized to zero and
`None`. The fixed `repayment_edge` tag is one of `None`,
`MarkerDelivery { participant_id, binding_epoch, marker_ref }`,
`DetachedCredentialRecovery { participant_id, marker_ref }`, or
`ObserverProjection { through_seq }`; it adds no independent set cardinality.

Let `S_actual` be the actual encoded retained suffix. Let closure charge `S` equal
actual non-marker retained cost plus `marker_max` for every planned or physically
retained credited marker; therefore `S_actual<=S` in each dimension. Let `C` be distinct reserved
identity slots currently owning a **marker capacity credit**. A credit is acquired when a marker is planned and
survives candidate append, delivery, and acceptance until that exact marker is
physically compacted. It can end earlier only when Leave cancels a still-unwritten
candidate in the same transaction. Thus `C` may temporarily include a retired
identity whose accepted marker remains retained, and always satisfies `0<=C<=I`.
One identity cannot own a second marker candidate/record until physical compaction
(or same-transaction cancellation) releases its credit. With zero debt, the
ordinary envelope in each dimension is:

`S + ((I - C) × marker_max) + Q <= configured_cap`.

Marker acceptance changes only cursor/anchor state: `S_actual`, `S`, `C`, and
remaining marker reserve are identical before and after, so it is append-free and
can never return a capacity refusal. Planned→actual append retains the credit and
its `marker_max` charge in `S`; only `S_actual` changes from no record to actual
encoded cost, already bounded by that charge. Later physical compaction removes
actual cost from `S_actual` and exactly `marker_max` from `S` while releasing the
credit adds exactly one
`marker_max` to remaining reserve. The marker's contribution to closure required
capacity is therefore equal before and after compaction; any co-compacted non-
marker prefix can only reduce it. Debt cannot increase.

Ordinary admission must leave the zero-debt equation true and otherwise returns
`MarkerClosureCapacityExceeded`. Mandatory classes collectively consume at most
the one Q term. Their post-commit debt is, separately for entries and bytes,

`d' = max(0, S' + ((I - C') × marker_max) + Q - configured_cap)`.

The candidate appears once in `S'`, never again as Q. Commit requires absolute fit
`S' + ((I-C')×marker_max) <= configured_cap` and `d'<=Q`. At the minimum startup
cap, first enrollment therefore appears once in `S'`, consumes its actual cost as
debt, and does not demand `candidate + Q + I`.

While either debt component is nonzero, ordinary caller admission returns
`MarkerClosureCapacityExceeded`. A mandatory transaction may commit at any debt,
including `(Qe,Qb)`, only if absolute fit and the bounded equations hold and its
post-state either lowers debt or persists a valid repayment edge that needs no
capacity-consuming admission:

- detached attach establishes `MarkerDelivery`; storage completion, marker append,
  final emitter delivery, and marker ack are TOLD events, and all remain admissible
  while debt exists;
- a binding fate changes that edge to `DetachedCredentialRecovery`; the next exact-
  credential attach or Leave is itself a mandatory class backed by R-C2's `T`/`E`
  sequence claims and R-A2's `A`/`X` order claims;
- marker acceptance, fenced recovery, or Leave releases its anchor/member and uses
  `ObserverProjection` when only hard observer progress still prevents physical
  prefix removal; the already scheduled projection completion pushes that progress.

“Repayment edge” promises an ordinary-admission-independent TOLD path, not that an
external participant or observer will cooperate; a deliberate wedge remains the
named §7 policy. The fixed header stores the lexicographically first valid witness
when several exist. It names the exact participant/epoch/marker or observer
boundary, is recomputed/replaced in the same transaction as debt or witness
invalidation, and is cleared when both components reach zero. A mandatory candidate that fits but can neither lower debt nor preserve
one of these paths is refused before mutation; an already accepted ordered
candidate remains in its bounded cell. Marker append, append-free ack, observer
progress, cursor progress, and floor compaction may run while debt exists. No
repayment path polls, scans, or requires an ordinary record to make room.

The physical floor remains reproducible. Let `H'` be the candidate watermark;
`F` the current first retained sequence (`H+1` when empty); `m` the minimum member
cursor after membership changes, or `H'` with no members; `o` the hard
`observer_progress`; and `a` the earliest live-member unaccepted marker sequence,
or checked one-past-end `END=MAX+1`. `END` is never allocatable. Define
`preferred_floor=min(m,o)+1`; `cap_floor` as the smallest floor at least `F` whose
resulting `S` charge plus the applicable normal/debt envelope fits without
exceeding `a`; and `F'=max(F,preferred_floor,cap_floor)`, valid only when `F'<=a`.
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
marker_capacity_credits, marker_anchors, entry_debt, byte_debt,
repayment_edge }` before floor, membership, marker credit/anchor, candidate,
sequence, debt/edge, or participant mutation.

After preflight and R-C2 sequence checks, one transaction changes floor/membership
and creates one candidate per newly affected member. Simultaneous candidates use checked
`admission_order=(u64 transaction_order, candidate_phase,
ascending_participant_index)`. At append, a candidate recomputes
`abandoned_after`, `abandoned_through`, and `physical_floor_at_decision`, then
appends `HistoryCompacted` and removes itself. The candidate drain precedes caller
records.

**Finite termination under consumable reserve.** The trigger fixed point contains
every marker its own appends induce and absolute-fit reserves all of them. Each
marker append retains one capacity credit and its `marker_max` charge while
materializing one bounded actual record, does not consume Q twice, cannot evict an unaccepted marker, creates no
replacement, and strictly changes `M→M-1` even at full ClosureDebt. With no caller
admission interleaving, `M<=I` reaches zero in at most `I` appends. Acceptance
retains that same credit; only physical compaction releases it under the proven
nonincrease above. Other pending mandatory candidates wait in bounded cells until
a named repayment edge fires; they cannot steal a marker credit. If the equations
cannot prove this before a trigger, refusal occurs before floor change. Earlier/
later finalizations remain ordered by `admission_order`, not wake scheduling.

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

Every retained/live item before encoding is `ParticipantDeliveryWork {
conversation_id, intended_participant_id, intended_binding_epoch, delivery_seq,
record }`. The sole R-D2 participant-frame constructor revalidates immediately
before construction that the connection/conversation slot still contains that
participant at that exact epoch. Mismatch drops the stale work without cursor,
ack, or record mutation; it may never be retargeted to the slot's new occupant.

Detach, Leave, and same-connection rebind install an epoch fence and outbound
response barrier in the serialized handoff. All old-epoch work not yet constructed
fails the final check; bytes already constructed for that epoch are ahead of the
response in that connection's one writer. The operation response follows those
bytes. On successful rebind it names Q's new epoch, and Q's first delivery is
constructed after that response under the same epoch.

A cross-connection handoff claims no impossible byte ordering between two TCP
streams. The commit fences the old final constructor; after generation-ordered
persistence the SDK atomically retires the old connection/epoch before exposing
`AttachBound`, and subsequently arriving origin-stream bytes are old-epoch input,
never Q delivery. Reader retirement is driven by the explicit shutdown/fate wake,
not a drain poll. Thus no P work can be constructed after its per-connection
barrier or retargeted merely because a physical slot was reused.

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

**Finite-boundary test convention.** “Test-seed” below means a test-only
constructor on the durability backend that installs a version-valid, checksum-
valid complete snapshot before the server starts. It is unavailable to protocol
frames, SDK/public APIs, configuration, and production binaries. The fixture must
state every affected counter, claim, candidate, floor, cursor, debt, and epoch and
must satisfy all invariants on entry; the test then crosses the final boundary
through the ordinary production request/event path. Direct production mutation of
generation, `delivery_seq`, `transaction_order`, or `park_order` is forbidden.

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
13. Attempt Leave from the shared bearer alone, a stale binding epoch, wrong
    generation, and the correct generation with a wrong secret. The last two are
    exact `StaleAuthority` (equal presented/current generations for wrong secret);
    each refusal has no order/record/cursor side effect. Lose a valid
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
21. Test-seed a complete live identity at generation `u64::MAX`, then send an
    ordinary credential attach and prove `GenerationExhausted` is terminal,
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
    boundary. Recovery preserves original cause/binding-epoch/order and observes
    either the whole atomic append/retention/delete/release transaction or none;
    exactly one ordered terminal record and one released bounded slot result.
25. Refuse participant-mode startup below either checked `Q + I×marker`
    retention floor; on SDK product overflow, `C>N×B`, `D>G×B`, invalid row schema,
    or parked conversations above recoverable slots. Then start at **exact equality
    from an empty conversation**: first enrollment commits once, consumes Q into
    bounded `ClosureDebt`, and is not double-counted; cursor/observer progress
    repays debt. Exercise every detach outcome, prove acks never backpressure and
    ordinary response loss remains at-most-once ambiguous, then re-run case 21.
26. Test-seed a complete snapshot with N active/pending binding claims, at least
    one detached live member, all current marker obligations, and
    `MAX-H = E+T+M+(L×T)+(L_other×E)` exactly. Refuse ordinary, enrollment, detached attach,
    floor-advancing, and two-value supersession candidates before they invade any
    flat exit, binding-terminal, marker, or potential-marker claim. Then deliver
    EOF/shutdown/protocol failure across all active bindings and valid detached
    Leaves across all members. Prove every terminal, `Left`, and newly required
    marker receives only its reserved value and the final allocation may land on
    `MAX` without dropping a promised record.
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
31. Test-seed an empty-member snapshot at `H=MAX-2`, `E=T=M=0`, and floor above
    `1`, then use ordinary enrollment. Its `Attached` plus resulting `E`, `T`,
    cursor-0 `M`, `L×T`, and `L_other×E` claims exceed the one post-append value, so the
    whole mint refuses `ConversationSequenceExhausted`. Separately test-seed a
    valid near-boundary member snapshot and make a production floor trigger newly
    overtake members; refuse it atomically, preserve floor/cursors/claims, and
    later append every already owed marker.
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
    Replay echoes the committed binding epoch/sequence after binding is gone.
    Reattach, terminalize the replacement, then replay old token:
    `StaleAuthority` reports `binding_state: Detached` with no fabricated current
    binding epoch.
36. Separately from case 17, commit T at G+1 and then a valid G+1→G+2 attach before
    T's deadline. T replay inside provenance is exactly
    `ReceiptExpired { reason: Superseded }`; case 17 alone proves `Deadline`.
37. With `Qe=2,I=3`, reject entry cap 2 and accept exact minimum 5. Starting only
    from public enrollment on three distinct connections, after each mint drain
    every cursor-0 marker candidate, deliver and explicitly accept its marker,
    project every record, cumulatively ack the remaining contiguous records, and
    physically compact the suffix before the next mint. Assert after the third
    cycle three live **bound** members at one cursor, empty suffix, `M=0`, all
    marker credits free, and debt zero. (The first equality bootstrap commits once
    with debt 1, never a six-entry demand.) Supersede P1 in one Qe-sized production
    transaction and project it without member acks, producing debt 2. Supersede P2;
    its preflight cap-floor removes the first pair, overtakes all three members, and
    atomically creates three credited marker candidates while debt stays 2. At cap
    5 append them by planned→actual conversion, `M:3→2→1→0`, with no eviction,
    replacement, or second Q charge. Deliver/accept each retained marker on its
    existing binding, then observer/cursor progress physically compacts it,
    releases its credit, and repays debt. Separately make absolute fit impossible
    and prove typed pre-floor refusal leaves floor/membership/credits/debt unchanged.
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
42. Bind P to `(C,X)` and retain its live receipt. A fresh enrollment token on
    C/X returns `ConnectionConversationBindingOccupied` with no presented id;
    credential attach for Q returns it with only presented Q. Neither reveals P or
    mints/rotates/replays. Queue P delivery work, rotate P on the same C/X, and
    assert distinct old/new binding epochs plus exactly ordered
    `Detached(Superseded)`/`Attached`; the final constructor drops unconstructed
    old-epoch work and any already-emitted P bytes precede the rotation response
    barrier. Then detach P, bind Q on that same physical slot, and replay P's exact
    still-live receipt there: it is `UnboundReceipt`, never `Bound`. Queue more P
    work across that handoff; prove none is encoded after the barrier or delivered
    as Q and Q's first delivery is keyed to Q's epoch internally. Repeat a P
    credential handoff from C1 to C2: assert no order between the TCP streams, but
    atomically persist P's newer epoch, retire C1's reader, and reject every later
    C1 participant byte before exposing C2's `AttachBound`. Finally race P/Q into
    an empty slot: one winner binds and the different-id loser gets the typed
    outcome; codec vectors remain conversation-only.
43. For `park_order`, test-seed a nonempty set at `u64::MAX-1`, allocate MAX,
    prove the next reservation returns `SdkParkOrderExhausted`, then empty the set
    transactionally and prove only that counter resets. Separately test-seed one
    active member at `transaction_order=MAX-2` with exact
    `order_remaining=A+X=2`. Optional record/enrollment/attach work returns
    `ConversationOrderExhausted` with high/remaining/reserved/resulting fields and
    no mutation. A real connection fate consumes `A`, assigns `MAX-1` to its
    terminal plus every reconstructed marker, and may crash/restart before its
    already ordered candidate drains without another allocation. Exact-current
    detached Leave then consumes `X`, assigns MAX to `Left`, and leaves `A=X=0`.
    Every later new-major operation refuses, append-free work remains admissible,
    nothing wraps, and `transaction_order` never resets.
44. First detach a normal-generation member and prove exact-current write-ahead
    unbound Leave commits one `Left` with no binding/cursor grant. In a separate
    fixture, test-seed a live member at generation `u64::MAX` and, through
    production floor/fate events,
    retain its delivered-unaccepted marker, fill both debt components to Q, and
    leave its old binding terminal in `PendingFinalization`. Send exact-current
    unbound Leave: one absolute-fit transaction appends the preserved old terminal
    first and `Left` second, releases marker anchor/membership/cursor and `T`/`E`,
    persists the repayment edge or lowers debt, invalidates the secret, and writes
    the tombstone plus both sequence fields. Crash at every boundary and lose the
    response; observe all-old or all-new and stable one-time `LeaveCommitted`.
    Lower generation and equal-generation wrong secret are `StaleAuthority`; a
    fresh token after retirement is `Retired`, all with no mutation.
45. On C1 deliver a retained marker and decline it; fill entry and byte debt to
    Q and make C1's terminal fate pending. On C2, a valid fresh attach naming that
    marker executes fenced recovery at full debt: one transaction accepts the
    marker, retains its capacity credit, releases the anchor, drains the old
    terminal, appends `Attached`, rotates to C2's binding epoch, and lowers debt or
    stores the exact TOLD repayment edge. Kill C2 before that edge repays; the fate
    atomically changes it to `DetachedCredentialRecovery`, and exact-current C3
    attach or detached Leave remains mandatory/admissible under absolute fit.
    Crash before/after every C1→C2→death transition and response: observe all-old
    or all-new, no abandoned payload marked delivered, no second marker/sequence
    charge, and no timer or retry loop.
46. Through public operations enroll P (`E=1,T=1`), detach (`E=1,T=0`), reattach
    (`E=1,T=1`), and detach again (`E=1,T=0`); at each commit recompute every
    reserve term and prove no cycle creates, loses, or double-spends the flat exit
    claim. Then race bound Leave with connection death in both orders. Leave-first
    writes one `Left` and death sees `Retired`; death-first writes its exact-epoch
    terminal and detached Leave writes `Left` second. Inject crashes throughout and
    prove the mid-exit fate is exactly one of those two complete histories.
47. Exercise exact sequence exhaustion with a step-by-step E1 construction. Arm A
    test-seeds one detached member, zero bindings/markers, and `H=MAX-1`: required
    reserve is exactly `E=1`, so unbound Leave allocates `Left` at `MAX` and leaves
    zero claims—this is the `H=MAX`, zero-bindings arm. Arm B test-seeds one active
    member at `H=MAX-3`: `E=1,T=1,M=0,L×T=1,L_other×E=0`. Optional work refuses;
    EOF allocates `MAX-2`, consumes T, and creates one M at exact equality. Arm C
    starts from that resulting durable state: marker append allocates `MAX-1` and
    consumes M, then detached Leave allocates `MAX` and consumes E. Assert equality
    after each of the three named arms' mandatory allocations, no wrap, and exact
    `ConversationSequenceExhausted` fields for every optional invasion attempt.
48. At exact entry and byte capacity, create and append one marker with `C=1` and
    debt at its legal boundary. Snapshot `S`, `C`, remaining marker reserve, and
    both debts immediately before acceptance; `MarkerAck` must commit with all four
    capacity quantities unchanged and can never return a capacity outcome. Advance
    cursors/observer so physical compaction removes that marker: assert `C:1→0`,
    reserve increases by marker maximum, `S_actual` falls by actual encoded cost,
    `S` falls by marker maximum, and required capacity/debt does not increase. Crash on both sides of acceptance and
    compaction and observe only those complete states.
49. Configure the legal minimum `cap=Q+I×marker` with `I=1`. Via public operations
    detach the member, let cap-floor planning create and append its marker, and
    crash before the marker is delivered. Restart at full legal debt. A fresh exact-
    credential detached attach is a mandatory closure transaction: it fits once,
    persists `MarkerDelivery`, binds a new epoch, receives the retained same-key
    marker, accepts it, and uses observer/cursor progress to compact it, release the
    credit, and repay debt. Crash before/after attach and first delivery; recovery
    neither duplicates the attach nor permanently denies reconnect.
50. Replace and adversarially test all three newly cited LAW-1 families. Channel
    reply wait races reply, pushed process exit, and one command deadline in every
    order; no 10ms liveness query remains. On indefinitely silent push and
    subscription sockets, explicit local shutdown wakes and joins each reader
    without a 100ms read timeout or atomic stop-flag recheck; race shutdown with
    readable bytes, EOF, and decode error. A source-structure test rejects the old
    constants/timeout-as-wake loops and proves every wait parks until one named
    event, never a timer whose job is change detection.

A reviewer refutes R-C0–R-C5 by finding an exactly-once application-effect
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
| SDK-local observer-wait admission | `SdkObserverParkCapacityExceeded` | scope (`PerConversation` or `SdkWide`), dimension (`Conversations`, `Rows`, or `Bytes`), conversation id, signed limit, occupied, requested full amount | Local terminal refusal; atomically roll back empty first-interest reservation, reserve no row, send no frame, create no server state. |
| SDK-local park-order allocation | `SdkParkOrderExhausted` | conversation id, `counter: ParkOrder`, `value: u64::MAX` | Terminal while set nonempty; reserve/send nothing, preserve ordered rows; only empty-set transaction may reset. |
| SDK restart/cap renegotiation | `SdkParkingCapacityIncompatible` | scope; offending dimension (`ConversationRows`, `ConversationBytes`, `SdkConversations`, `SdkRows`, `SdkBytes`, `RequestBytes`, `RowBytes`, or `RecoverableInterestSlots`); conversation id/park order when applicable; actual or occupied; negotiated limit | Preserve every row and interest slot, send none, require operator/config correction. |
| SDK-local untokenized response loss | `RecordAdmissionUnknown` | conversation id, presented participant id/generation, operation identity `OrdinaryRecordAdmission`, park order | Atomically delete the row/release last interest when applicable; terminal ambiguity and never resend automatically. |
| First participant operation or reconnect refusal entry for a conversation on this connection | `ConnectionConversationCapacityExceeded` | conversation id, signed connection-conversation limit | No participant mutation, interest arming, or readiness promise; use a connection with a free negotiated slot. |
| Enrollment or credential attach binding attempt | `ConnectionConversationBindingOccupied` | conversation id and `presented_participant_id: Option<ParticipantId>` (`None` for enrollment); no occupying identity fields | Terminal for this attempt; no mint/receipt/rotation/replay/binding/record; same-participant rotation remains eligible. |
| Optional operation requiring an unreserved `transaction_order` major | `ConversationOrderExhausted` | conversation id, `counter: TransactionOrder`, current high/next value when any, `order_remaining`, `reserved_claims`, operation scope, required/resulting claims | Terminal for this attempt with no wrap/rebase/alias or mutation. Accepted terminal/exit obligations consume `A`/`X`; all marker candidates share their causal major, and already ordered candidates drain without another major. |
| Credential attach, receipt/status replay, detach, Leave, normal ack, marker ack, or ordinary admission | `ParticipantUnknown` | presented conversation id and participant id; presented generation, token, and requested boundary when those fields exist in that operation | Terminal semantic outcome for this attempt; connection remains open and no participant/durable state is created. |
| New detach with **no Pending cell**, Leave while a different live binding epoch exists, normal ack, marker ack, or ordinary admission after exact-token lookup misses | `NoBinding` | presented conversation id, participant id, generation | Terminal; no other binding disclosed and no state change. Detached exact-credential Leave is eligible; any Pending detach cell is classified at step 0b first. |
| Any listed participant-id operation with a live generation mismatch, or any secret-bearing operation with equal generation and wrong secret | `StaleAuthority` | presented participant id/generation and current generation; operation-specific token/boundary when present; no secret-derived detail | Terminal under invalid authority; equal generation fields classify secret mismatch without a new oracle. No state changes. |
| Any listed participant-id operation whose presented id has a tombstone | operation-specific `Retired` | presented participant id/generation, retired generation, and operation-specific token/boundary | Terminal tombstone result; no secret, binding, live cursor, or state change. |
| Any closure-checked admission | `MarkerClosureCapacityExceeded` | conversation id, entry/byte dimension, checked required, configured limit, marker capacity-credit count, anchor count, entry/byte `ClosureDebt`, repayment-edge tag | No floor, membership, credit, participant, candidate, sequence, edge, or debt mutation; terminal for every unaccepted request, with no SDK park or automatic retry. Already accepted server candidates and claimed terminal paths remain event-driven. Marker acceptance itself never has this outcome. |
| Enrollment attach | `EnrollBound` | enrollment token, participant id, generation `1`, secret, receipt/provenance deadlines, exact binding epoch | Success; persist before exposing `Bound`; replay same token only after unknown response. |
| Enrollment attach | `EnrollUnboundReceipt` | same credential fields and origin binding epoch | Persist, then fresh-token credential attach; never expose binding. |
| Enrollment attach | `EnrollmentKnown` | enrollment token, participant id, current generation | Committed identity, no secret/binding; use a valid current-or-newer credential or enter `CredentialRecoveryLost`; never remint. |
| Enrollment attach | `ReceiptExpired` | token, participant id, result/current generations, `Deadline` or `Superseded` | Exact only inside the enrollment provenance window; use a valid current-or-newer credential or enter `CredentialRecoveryLost`. |
| Enrollment attach | `Retired` | token, participant id, retired generation | Terminal; preserve identity for operator record, no attach. |
| Enrollment attach | `ReceiptCapacityExceeded` | token, `scope`, `limit` | No SDK auto-retry; surface typed admission refusal. |
| Enrollment attach | `IdentityCapacityExceeded` | token, `scope`, `limit` | Terminal for new enrollment; no participant was minted. |
| Enrollment attach | `ObserverBackpressure` | token, observer progress, backpressure epoch | Persist `AwaitingObserverProgress`; retry once after matching `ObserverProgressed` or reconnect status. |
| Enrollment attach | `ConversationSequenceExhausted` | token, high watermark, remaining capacity, resulting `E`, `T`, `M`, `L×T`, and `L_other×E` budgets | Terminal for this mint; reserve check includes the flat `Left`, binding terminal, and cursor-0 marker fixed point before identity or sequence mutation. |
| Credential attach | `AttachBound` | attach token, participant id, new generation/secret, deadlines, exact binding epoch, persisted cursor, optional accepted marker sequence | Success; generation-ordered persist, then expose binding/replay. Optional marker field is present only after atomic fenced recovery; no ack parking state exists. |
| Credential attach | `AttachUnboundReceipt` | same credential fields and origin binding epoch | Persist, then fresh-token attach; no replay/ack authority. |
| Credential attach | `ReceiptExpired` / `StaleOrUnknownReceipt` / `Retired` | attach token and fields defined above | Never retry same request; preserve newer credential or enter the specified terminal state. |
| Credential attach | `StaleAuthority` | token, presented/current generations | Terminal for this request; preserve current durable credential. |
| Credential attach | `GenerationExhausted` | token, participant id, current generation `u64::MAX` | Terminal for attach with no commit/wrap; preserve current secret and enter `CapabilityGenerationExhausted`, from which only exact-token status or exact-current terminal Leave (bound or detached) is allowed. |
| Credential attach with `accept_marker_delivery_seq` | `MarkerNotDelivered` / `MarkerMismatch` | token, participant id/generation, presented marker seq, retained/delivered marker seq when non-secret, reason | Terminal for this attach attempt; no marker accept, cursor, floor, rotation, record, or binding mutation. |
| Credential attach | `ReceiptCapacityExceeded` / `ObserverBackpressure` / `ConversationSequenceExhausted` | token plus capacity/epoch fields; exhaustion adds high watermark, remaining capacity, resulting `E`/`T`/`M`/`L×T`/`L_other×E`, and required record cost | Capacity is surfaced; backpressure enters a shared epoch; reserve exhaustion precedes rotation, binding, floor, and marker changes. Detached attach may consume closure Q but never sequence/order claims it did not reserve. |
| Receipt replay | `Bound` / `UnboundReceipt` | exact token plus live receipt fields and origin binding epoch | `Bound` only if the exact participant/epoch still occupies its origin slot; every empty/replaced/later-epoch slot, including on the same connection, is `UnboundReceipt`, then persist-and-fresh-attach. |
| Receipt replay | `ReceiptExpired` | exact fingerprint, reason, result/current generations | Exact only inside provenance window; not retryable; preserve newer credential or `CredentialRecoveryLost`. |
| Receipt replay | `StaleAuthority` | fresh token absent from complete in-window set, presented/current generations | Proves no commit for this token; not retryable. |
| Receipt replay | `StaleOrUnknownReceipt` | token, presented/current generations | Post-provenance ambiguity; claims no commit; no automatic retry. |
| Receipt replay | `Retired` | token, participant id, retired generation | Terminal; no secret or binding. |
| `DetachRequest` | `DetachCommitted` | detach token, participant id, cell-retained committed binding epoch and detached delivery seq | Success; echo comes only from `detach_replay::Committed`, never reused live-binding state; stable until next successful attach/Leave. |
| `DetachRequest` exact-token replay while cell is Pending | `ObserverBackpressure` | detach token, participant id, committed binding epoch, **current cell refusal epoch per rewrite rule**, current progress; no delivery sequence | Equal returns unchanged; greater progress drains first or atomically rewrites cell+interest to newer refusal epoch. Never park on a consumed epoch or create a second candidate. |
| `DetachRequest` different token while cell is Pending | `DetachInProgress` | presented token, participant id/generation, committed binding epoch; never the stored token | Terminal for the competing attempt; no state change or sequence. |
| `DetachRequest` | `StaleAuthority` | detach token, presented generation, committed binding epoch when retained, current generation, binding state (`Bound { current_binding_epoch }` or `Detached`) | Terminal after cell overwrite; absent live binding is `Detached`, never a sentinel or retained fake current epoch. |
| `DetachRequest` | `Retired` | detach token, participant id, retired generation | Terminal tombstone result after Leave; preserve operator identity, no record. |
| `DetachRequest` first accepted while append is blocked | `ObserverBackpressure` | detach token, binding epoch, refusal epoch/progress | One atomic transition writes binding-epoch-keyed `PendingFinalization` plus `detach_replay::Pending`; progress wake atomically appends and converts Pending to Committed with the real sequence. |
| `ParticipantAck { conversation_id, participant_id, capability_generation, through_seq }` | `AckCommitted` | presented participant id/generation and requested `through_seq`, matched persisted cursor | Success only after tuple+binding match; advance SDK watermark. |
| `ParticipantAck` | `AckNoOp` | presented participant id/generation, requested boundary, unchanged matched cursor | Success; idempotent confirmation under the same authority. |
| `ParticipantAck` | `AckGap` / `AckRegression` / `StaleAuthority` | presented participant id/generation, requested/current cursor, reason | Terminal for this ack; do not advance SDK watermark. |
| `ParticipantAck` | `Retired` | presented participant id/generation, requested `through_seq`, retired generation | Tombstone lookup uses the presented id; no secret, binding, or live/current cursor is present. |
| Bound `LeaveRequest` with exact current secret | `LeaveCommitted` | leave token, participant id, retired generation, ended binding epoch, `prior_terminal_delivery_seq: None`, `left_delivery_seq` | Success; terminal participant state. Duplicate token returns the identical result. |
| Detached `LeaveRequest` with exact current secret, at any generation | `LeaveCommitted` | leave token, participant id, presented/retired generation, no ended binding epoch, optional prior-terminal delivery seq, `left_delivery_seq` | Terminal success only: drain the preserved terminal first when present, release marker/membership/cursor, tombstone, and never bind or grant replay/ack authority. |
| `LeaveRequest` | `StaleAuthority` / `Retired` | leave token, participant id, presented/current or retired generation | Terminal; equal presented/current `StaleAuthority` means wrong secret without further detail. `Retired` returns no secret, binding, or new record. |
| `LeaveRequest` | `ObserverBackpressure` | leave token, generation, refusal epoch/progress, whether a prior terminal cell exists | Preserve valid authority/request only in the bounded SDK row and retry once after progress. Leave creates no server pending cell, whether or not an earlier binding terminal exists. |
| `MarkerAck { conversation_id, participant_id, capability_generation, marker_delivery_seq }` | `MarkerAckCommitted` / `AckNoOp` | presented participant id/generation, requested marker seq, matched persisted cursor | Success only after tuple+binding match; record abandonment or idempotent confirmation. |
| `MarkerAck` | `MarkerNotDelivered` / `MarkerMismatch` / `StaleAuthority` | presented participant id/generation, marker/requested/current sequences, reason | Terminal for this ack; cursor holds. |
| `MarkerAck` | `Retired` | presented participant id/generation, requested marker sequence, retired generation | Tombstone lookup uses the presented id; no secret, binding, or live/current cursor is present. |
| `RecordAdmission { conversation_id, participant_id, capability_generation, payload }` | `RecordCommitted` | verified/derived sender participant id, assigned delivery seq | Success only after tuple+binding match; after response loss enter SDK `RecordAdmissionUnknown` and never resend automatically. |
| Ordinary record admission | `RecordTooLarge` / `ConversationSequenceExhausted` / `MarkerClosureCapacityExceeded` | measured cap or high watermark, remaining capacity, resulting `E`/`T`/`M`/`L×T`/`L_other×E`, or checked closure required/limit/credits/anchors/debt edge | Terminal for this record; refusal consumes no sequence and preserves floor, identity, every exit/terminal/marker claim, credit, and debt edge. |
| Ordinary record admission | `ObserverBackpressure` | verified/derived sender, epoch/progress | A received refusal may park for one progress-cycle retry; a lost response is ambiguous and terminal. |
| Reconnect handshake status | `ObserverProgressStatus` | conversation id, refused epoch, current observer progress, `armed`, `progressed` | Older epoch returns progressed/unarmed; equal atomically arms then snapshots. Matching progress retries every bounded local row at that epoch. |
| Reconnect handshake status | `InvalidObserverEpoch` / `InvalidObserverEpochList` | conversation id, presented/current epoch or duplicate-entry detail | Typed protocol error; newer epoch or duplicate conversation entries arm nothing and mutate no participant state. |

A valid terminal detach or binding fate never returns
`ConversationSequenceExhausted` because `T` owns its value; any valid bound or
detached Leave never returns it because `E` owns `Left` (and `T` separately owns
an earlier pending terminal). That outcome remains reachable only for optional
enrollment, attach, supersession, ordinary, or floor-triggering candidates before
they invade the reserve. Each owed
marker already owns its `M` value; its physical append is instead protected by
R-C4 closure accounting.
Normal and marker acks never return `ObserverBackpressure`,
`MarkerClosureCapacityExceeded`, or `ConversationOrderExhausted`: they append no
record, allocate no major, and may relieve retention pressure. Ordinary record admission is
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
   capability and a typed participant outbound value, never an unrestricted
   `Frame`. For delivery work it performs R-C5's final exact participant/binding-
   epoch slot check immediately before constructing the discriminant; mismatch is
   a stale-work drop, not retargeting.
3. The generic outbound enqueue path rejects server-originated participant
   discriminants, so no producer can bypass the construction site with a raw
   `Frame`.
4. Replay, live delivery, lifecycle, compaction, attach responses, and close
   verdicts all call that site. Pre-handshake, auth/version error, force-close,
   and protocol-1.0 paths cannot obtain the capability.
5. Same-connection detach/rebind inserts R-C5's writer barrier at this boundary:
   old-epoch constructed bytes precede the response, stale queued values fail
   revalidation, and the new epoch's first construction follows. Cross-connection
   handoff instead fences the old constructor and relies on SDK epoch retirement;
   it asserts no ordering between independent TCP streams.

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
site, that it performs the final participant/binding-epoch revalidation, and that
generic enqueue refuses those discriminants. On one connection, queue P work,
cross detach/rebind, and bind Q: only constructed P bytes precede the response,
stale work drops, and Q's first construction uses Q's epoch. Repeat across two
connections: assert no cross-stream order, atomically retire P's old SDK epoch
before exposing Q's `AttachBound`, and reject every later old-stream delivery.
Push/PushReply codec vectors remain byte-identical.

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
liveness detector. The receive brief must be implementable from explicit R11
facts, except where §7 names a genuine gate.

## 7. Named sockets and decision register

R11 distinguishes decisions made by the draft from genuine unknowns. A
**decided-by-draft** row has one candidate answer and may change only by explicit
key refusal/revision. A **genuinely-open** row names unavailable platform
evidence, an implementation linearization mechanism, a layer-above policy, or
an owner migration decision. Open rows are not permission for implementations
to choose incompatible semantics silently.

| Socket | Status | R11 answer or reason openness is genuine | Closure/refutation evidence |
|---|---|---|---|
| `«ATTACH-TRANSACTION»` | **decided-by-draft** | Attach, detach, and Leave use mandatory write-ahead tokens and one serialized participant state. The identity slot's detach cell is `Empty`, token-bearing `Pending` without a sequence, or `Committed` with the exact binding epoch/sequence; it is the sole stable-echo source until next attach/Leave. | All send/commit/response windows, same/different token while Pending, crash around Pending→Committed, echo after binding removal, stale replay after replacement death, detach loss across attach/Leave, replacement recovery, and terminal receipt tests. |
| `«RECEIPT-LIFETIME»` | **decided-by-draft** | Signed receipt TTL/count and non-secret provenance TTL/count caps bound both bodies and classifiers. Cleanup uses admitted deadlines; TTL is the maximum supported recovery outage and expiry may enter `CredentialRecoveryLost`. | Delayed generation, crash order, exact/unknown before/after provenance, cap exhaustion, composed outage, and no-sweep tests. |
| `«RETIRED-IDENTITY-BOUND»` | **decided-by-draft** | Enrollment reserves signed server/conversation identity slots; each slot owns the enrollment-token mapping for the live+tombstone lifetime, and Leave converts it to a permanent tombstone. Cap exhaustion refuses before mint, bounding churn without weakening `EnrollmentKnown`, `Retired`, or duplicate `LeaveCommitted`. | Post-GC enrollment T versus fresh U, boundary churn, no-ghost replay, lost Leave response, and pre-mint capacity refusal. |
| `«PARTICIPANT-ID-ORIGIN»` | **decided-by-draft** | Server mints participant id, generation 1, and initial secret inside tokenized enrollment; later attach proves current generation/secret and atomically increments/rotates before cursor access. External ids never authorize a cursor. | Authorization-before-cursor, collision, enrollment replay, generation ordering, rotation loss, and reconnect tests. |
| `«MEMBERSHIP-BOUNDARY»` | **decided-by-draft** | Membership begins at mint with cursor `0`, survives detach, entitles full history and offline commits, and ends only with authoritative tokenized Leave that retires id, receipts, and soft retention claim. | Late join, offline replay, Leave authority/idempotency/races, and floor-transition tests. |
| `«LEAVE-AUTHORITY»` | **decided-by-draft** | Exact current generation/secret authorizes tokenized terminal Leave from the authoritative bound epoch or while detached, at every generation. Detached Leave grants no cursor/binding authority; when an earlier terminal is pending, one atomic ordered composition writes it then `Left`. V1 has no operator Leave. | Shared-bearer/stale/wrong-secret refusals, duplicate stable result, attach/Leave/death races, full-debt pending-terminal composition, and generation-maximum tests. |
| `«ACK-SHAPE»` | **decided-by-draft** | Normal `ParticipantAck { conversation_id, participant_id, capability_generation, through_seq }` and `MarkerAck { conversation_id, participant_id, capability_generation, marker_delivery_seq }` carry presented authority. At one serialized point the server looks up the presented id, matches generation plus the exact binding epoch, then validates continuity or named abandonment. Post-Leave uses the presented id's tombstone and never a live cursor. | P-ack-after-Q-bind ambiguity, same-id rotation, unknown/unbound/stale/Retired codec vectors, normal gap/regression, marker-delivery refusal, and proof abandoned payload is not marked delivered. |
| `«COMPACTION-EXIT»` | **decided-by-draft** | A pending marker shares the durable server-candidate admission order with finalizations; at append it recomputes `abandoned_after..abandoned_through` and the physical-floor snapshot. Acking abandons through that pre-marker watermark and advances to marker sequence. | Both marker/finalization orders, append-time fields, retained-suffix choice, marker loss/redelivery, repeated-cycle, concurrent-live, and post-ack flow tests. |
| `«RETENTION-UNITS»` | **decided-by-draft** | Signed byte/entry caps start at `Q + I×marker`. Ordinary work preserves Q; detached attach and every named mandatory class may consume it under absolute fit. A marker capacity credit survives planning through physical compaction, acceptance never increases reserve, and nonzero/full `ClosureDebt` stores an exact TOLD repayment edge. | Empty-conversation equality bootstrap, independently computed entry/byte debt, reachable three-marker drain, acceptance/compaction credit boundary, minimum-cap crash-before-delivery reattach, full-debt C1→C2→death, and closure-refusal atomicity. |
| `«MULTI-CONVERSATION-MUX»` | **decided-by-draft** | Yes. One connection carries many conversations, demuxed by `conversation_id`; participant `stream_id = 0` and has no semantic role. | Cross-conversation interleaving and independent-cursor tests. |
| `«MULTI-BINDING-PER-CONVERSATION»` | **decided-by-draft (excluded in v1)** | At most one participant binds each `(connection_incarnation, conversation_id)`. A different-id enrollment/attach gets `ConnectionConversationBindingOccupied` containing only its presented optional id; same-id rotation is allowed. | Empty-slot race, enrollment with no presented id, Q-after-P refusal without P disclosure, same-P rotation, and conversation-only delivery codec. |
| `«LIFECYCLE-VERDICT-RECIPIENTS»` | **decided-by-draft** | Every member is entitled to every lifecycle/compaction record in total order, including while detached, unless it explicitly accepts a named abandonment after compaction broke continuity. | Three-party lifecycle/compaction races, offline replay, and explicit-abandonment tests. |
| `«LIFECYCLE-OBSERVER-DELIVERY»` | **decided-by-draft** | The log is sole completed lifecycle history and observer progress hard. Signed per-conversation and SDK-wide conversation/row/full-byte caps plus request/row maxima bound parking; durable first-row interest slots make all Awaiting conversations armable. `Reserved` restart, checked u64 order, cohort marking, authority loss, epoch monotonicity, and all-dimension renegotiation are explicit. Acks never park. | Independent row/byte/global cap hits, interest-slot recovery, Reserved crash, per-row downward incompatibility, near-max order, epoch races, credential loss, and no orphan/ack/poll. |
| `«SUPERSESSION-FENCE»` | **decided-by-draft** | Every credential-bearing successful attach checked-increments generation, rotates the secret, and creates a distinct immutable binding epoch even on one physical connection. Final outbound construction drops old-epoch work behind the response barrier; stale proof is a no-record refusal. | Two-holder and same-connection rotation races, single handoff pair, exact-epoch receipt recovery, queued-work barrier, and stale-proof zero-record tests. |
| `«RESUME-COMMENT-SERVER-MISMATCH»` | **genuinely-open** | Owner must choose separate legacy subscription recovery versus version/deprecate/delete it for participant attach. Outline governs over the earlier tear's preference to decide now; R11 nevertheless makes “comment-only fix” insufficient. | Owner ruling covering types, cursor owner, starting convention, persistence, release boundary, and removal/compatibility tests. |
| `«MEMBERSHIP-EVENT-SOURCE»` | **genuinely-open implementation dependency** | External behavior is fixed: membership deltas must be pushed and shutdown must wake the source. The selected membership backend's non-poll event API is not yet chosen. | Backend event subscription/callback with ordered delta and explicit shutdown tests; delete `PollLoop`, `poll_once` cadence, and sleep. |
| `«HEALTH-ACCEPT-SHUTDOWN-WAKE»` | **genuinely-open implementation dependency** | External behavior is fixed: health accept blocks/uses readiness and explicit shutdown interrupts it. The concrete cross-platform wake mechanism is not selected. | Per-target accept/shutdown race tests with no WouldBlock sleep or shutdown-flag sampling loop. |
| `«SHUTDOWN-DRAIN-NOTIFICATION»` | **genuinely-open implementation dependency** | External behavior is fixed: connection exit updates a completion primitive raced against one admitted deadline; force-close uses the same notification. Concrete supervisor API is not selected. | Exit/drain notification API, crash/force-close/deadline races, and deletion of reap/count/sleep loops. |
| `«CHANNEL-REPLY-EVENT-RACE»` | **genuinely-open implementation dependency** | External behavior is fixed: a command wait parks on the reply, pushed process exit, and one admitted command deadline. The scheduler-to-waiter exit-notification primitive is not selected. | Reply/exit/deadline races in every order; delete `LIVENESS_POLL`, repeated `recv_timeout`, and process-table sampling. |
| `«SDK-PUSH-READER-SHUTDOWN-WAKE»` | **genuinely-open implementation dependency** | External behavior is fixed: the push reader blocks on/readiness-waits for socket input and explicit local shutdown interrupts that wait. The portable wake primitive is not selected. | Silent-socket data/EOF/error/shutdown races and join tests; delete `READER_POLL_TIMEOUT`, timeout-as-`None`, and stop-flag rechecks. |
| `«SDK-SUBSCRIPTION-READER-SHUTDOWN-WAKE»` | **genuinely-open implementation dependency** | External behavior is fixed: the subscription reader blocks on/readiness-waits for socket input and explicit local shutdown interrupts that wait. The portable wake primitive is not selected. | Silent-socket data/EOF/error/shutdown races and join tests; delete `READER_POLL_TIMEOUT`, timeout-as-`None`, and stop-flag rechecks. |
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
poll, sweep, scan, heartbeat, listener backoff, periodic reap, read-timeout wake,
stop-flag sampling, or synthetic write whose job is to ask whether state changed.

### Silence-attacking acceptance frame

Search the document for `«` and require every result to appear in this register
or be one of the two SDK gate names explained here. For decided rows, search for
one normative answer in §§2-6. For genuinely-open rows, require the named owner
or evidence and refuse implementation where the gap is load-bearing. Search all
timers, wakes, loops, and tasks—including main/health accept, membership,
shutdown drain/settle, channel reply wait, and both SDK readers—and prove each is
driven by admitted work, reply/data readiness, kernel connection fate, explicit
shutdown/process exit, one admitted deadline, or an existing domain event rather
than change detection.

---

**Gate posture:** DESIGN DRAFT R11. All earlier-round decisions remain in force.
R11 closes the latest examiner's E1–E3 and S1–S8 defects: flat member-exit
sequence/order reserve, executable boundary seeding, reachable minimum-cap tests,
marker capacity credit, full-debt repayment edges, general detached Leave and
pending-terminal composition, exact receipt/binding epochs, equal-generation
wrong-secret classification, final-emitter fencing, and the three additional
LAW-1 polling families. The socket register separates decisions from real
implementation dependencies. Reviewer key plus Hermes Crumpet's liminal domain-
owner key are still required; until both turn, this document is not ratified and
grants no implementation authority.
