# Participant-domain wire/server contract — design draft R14

**Status: DRAFT R14 — closes transferable-occupancy, one-quartet, marker-backing, parking-outcome, floor-envelope, SDK-latch, detach-verifier, reconnect-delay, sequence-payload, fixture, and pen defects; not yet ratified.** This redraft
parks at the two-key gate: reviewer-of-record plus the liminal domain-owner pass
(Hermes Crumpet). “Decided-by-draft” below means that R14 selects one contract
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

R12 is the specification-completeness redraft required after both independent
triple-lens examiners and the acceptance-executability lens refused R11 commit
`1e6aa99`. The mandate preserves the E/T reserve formula, case 47 arithmetic,
and all verified polling evidence; R12 closes the thirteen named classes without
re-deriving those surviving decisions. It makes repayment edges closed and
resource-backed, reserves recovery occupancy, makes the polling inventory
reproducible, validates recovery-handshake encoding, closes token fingerprints,
candidate order, Leave drains, restart/incarnation recovery, dispatch-slot
handoff, and all affected executable fixtures.

R13 is the refusal-closure redraft required after two independent max-effort
examiners and one blind executability examiner refused R12 commit `897be59`.
The owner hand-verified all fifteen deduplicated findings and ruled eight repair
classes. R13 separates Q debt from non-borrowable K, makes fenced recovery accept
the marker and terminate the attach/death cycle, gives every edge complete claims
and invalidators, makes composed Leave positional, extends LAW 1 across Rust and
all shipped SDK source, puts the Leave verifier in the tombstone, fixes recovery-
handshake widths/phase precedence, and re-derives every affected fixture. It does
not reopen the thirteen R12 mandate classes beneath those corrections.

R14 is the refusal-closure redraft required after two independent Sol examiners
and one blind human-lens examiner unanimously refused R13 commit `0bbcd56`.
The reviewing seat hand-verified all nineteen findings and consolidated them into
eleven ruled repair classes. R14 makes K componentwise transferable, permits one
quartet per debt episode, separates product-backed marker claims, closes initial
parking outcomes and the floor envelope, serializes every SDK parked-row action,
keeps detach non-secret-bearing, retires Rust/Gleam reconnect-delay re-arm,
canonicalizes sequence-budget payloads, and rebuilds every defective fixture. It
does not reopen the R13 classes beneath those corrections.

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

### 0.5 R11 → R12 changelog

Driver: the decisions-only r12 refusal mandate over commit `1e6aa99`. The E/T
formula and its boundary arithmetic are unchanged.

| Mandate class | R12 decision | Where |
|---|---|---|
| C1–C2 | Repayment edges are a closed successor machine whose stored fixed-point claims own sequence, candidate position, occupancy, and recovery headroom. | R-C2/R-C4; cases 37/48/49/51 |
| C3 | A reproducible pinned-source grep classifies the total LAW-1 result set; durability bridge, push-reply re-arm, and subscription setup have retirement sockets. | §§1, 3, 7; case 50 |
| C4 | Startup/renegotiation validate the maximum one-shot refusal list against request and wire maxima; no chunk protocol is adopted. | R-A4/R-D1; case 52 |
| C5 | Fenced-recovery death preserves its independent projection edge; detached marker-delivery death has its own arm. | case 45 |
| C6 | Every live token stores a canonical verifier; wrong-secret conflict is `StaleAuthority`, other body conflict is typed. | R-C0/R-C1/R-D1; case 53 |
| C7–C8 | Candidate phases/indexes are normative and every affected finite-boundary fixture is a complete snapshot. | R-A2; cases 26/31/43/47/54 |
| C9 | Globally earlier Leave drains are separately committed and Q-bounded; only the member's own preserved terminal plus `Left` is the two-record atomic core. | R-C2/R-C4; case 55 |
| C10 | Prior-incarnation bindings undergo claim-backed startup recovery; connection incarnation is fixed-width, durable, unique, and exhaustion-safe. | R-A1/R-A2/R-D1; cases 42/56 |
| C11 | Cross-connection handoff retires one SDK dispatch slot, never an unrelated physical reader. | R-C5/R-D2; case 42 |
| C12 | Bound Leave at exact E/T equality spends both claims in one `Left`. | case 47D |
| C13/global | Names, outcomes, idle cost, no-polling, shared definitions, and cross-references receive explicit whole-document audits. | §§5, 7 and final audits |

### 0.6 R12 → R13 changelog

Driver: the decisions-only r13 refusal mandate over commit `897be59`; all
fifteen underlying findings were owner-verified before drafting.

| Mandate class | R13 decision | Where |
|---|---|---|
| C1 | Closure debt borrows only Q; K is explicit, non-borrowable occupancy, with componentwise fit and value assertions. | R-C4; cases 25/37/44/45/48/49/51/56 |
| C2 | Fenced DCR accepts the marker and exits to observer position; complete attach/future-binding claims, optional ordinary-attach refusal, and a decreasing successor plan prevent cycles. | R-A2/R-C2/R-C4; cases 45/51 |
| C3 | The preserved-terminal/Left core is positional; an intervening tuple forces a separate own-terminal drain before one-record Leave. | R-A2/R-C2; case 55 |
| C4 | Every repayment row enumerates every witness invalidator; binding fate and Leave now close `ParticipantCursorProgress`. | R-C4; cases 48/56 |
| C5 | The pinned LAW-1 sweep covers all crate and SDK product source, extended timer/clock/wait/scan vocabulary, TS reconnect, and structural wait shapes. | §§1, 7; case 50 |
| C6 | The bounded tombstone permanently stores and charges the fixed Leave verifier; lookup step 0a is uniform before tombstone precedence. | R-C0/R-C1/R-D1; case 53 |
| C7 | Recovery-handshake arithmetic is u128 over u64 inputs and cannot overflow; lifecycle phase alone selects startup versus parked-row incompatibility. | R-A4/R-D1; case 52 |
| C8 | Cases 26/43/47/51/56 are arithmetically complete, and the fixture audit covers every test seed. | acceptance preamble and named cases |

### 0.7 R13 → R14 changelog

Driver: the decisions-only r14 refusal mandate over commit `0bbcd56`; all
nineteen underlying findings were hand-verified before drafting.

| Mandate class | R14 decision | Where |
|---|---|---|
| E1–E3 | Recovery charge transfers from K into B; one quartet is consumed once; marker claims name non-product M versus terminal/exit product backing. | R-C2/R-C4; cases 37/45/48/49/51/56 |
| E4–E5 | Initial parking-shape failures have one exhaustive outcome, and `cap_floor` uses exactly the committing transaction class's envelope. | R-A4/R-C4/R-D1; cases 25/37/47 |
| E6–E7 | One SDK-wide domain serializes validation with every parked-row action; detach remains non-secret-bearing with executable verifier arms. | R-A4/R-C0; cases 52/53 |
| E8 | The pinned sweep includes reconnect/retry/backoff/delay vocabulary and separately retires Rust/Gleam caller-side reconnect-delay re-arm. | §§1, 3, 7; case 50 |
| E9 | Every sequence exhaustion uses one canonical payload containing all scalar and product reserve terms. | R-C2/R-D1; cases 21/31/43/47/51/56 |
| E10–E11 | Startup/enrollment, finite drains, complete snapshots, edge fields, floor values, and the partial-handshake sentence are repaired in place. | cases 21/25/26/37/44/47/51/56; R-D1 |

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
| The synchronous durability bridge repeatedly polls without honoring a waker. | `block_on` constructs a no-op waker, polls up to eight times, and yields after `Pending` (`crates/liminal/src/durability/bridge.rs:29-40`, `crates/liminal/src/durability/bridge.rs:83-99`); production channel and server-dedup callers use it (`crates/liminal/src/channel/types.rs:340-368`, `crates/liminal-server/src/server/connection/services.rs:508-545`). | `«DURABILITY-BRIDGE-WAKE»` permits exactly one poll followed by a loud `Pending` error under the asserted synchronous backend, or a real parked executor honoring the future's waker. No no-op-waker retry/yield loop is conforming. |
| The push-reply public wait contract blesses caller-side timeout re-arm. | `PushReplyAwaiter::receive` describes elapsed polls as benign re-arms and implements timeout/`try_recv` looping (`crates/liminal-server/src/server/connection/supervisor.rs:533-636`); pinned tests repeatedly call it with short quanta (`crates/liminal-server/src/server/connection/supervisor_tests.rs:1211-1225`, `crates/liminal-server/src/server/connection/supervisor_tests.rs:1979-2004`). | `«PUSH-REPLY-AWAITER-EVENT-RACE»` replaces that blessed use with one SDK-call-site race among reply, connection fate, and one admitted deadline. An internal one-shot wait quantum may implement the race, but indefinite caller re-arm is not its contract. |
| Subscription setup samples a total deadline through repeated read timeouts. | `read_one_frame` takes 100ms reads and samples `Instant::now()` against `SETUP_TIMEOUT` (`crates/liminal-sdk/src/remote/tcp/subscription.rs:389-416`). | `«SDK-SUBSCRIPTION-SETUP-DEADLINE-RACE»` races handshake/Subscribe socket input against one total admitted setup deadline; no periodic clock sampling remains. |
| The shipped TypeScript SDK reconnects by an unbounded-default timer retry loop. | `reconnect` iterates attempts, sleeps for backoff, and retries `transport.open()`; omitted or negative `maxAttempts` becomes `Number.POSITIVE_INFINITY`, and default sleep uses `setTimeout` (`sdks/liminal-ts/src/connection.ts:240-264`, `sdks/liminal-ts/src/connection.ts:309-320`, `sdks/liminal-ts/src/connection.ts:336-339`). | `«SDK-RECONNECT-TIMER-LOOP»` replaces automatic timer retry with transport/connection-fate arming; a manual reconnect request remains TOLD, never a retry poll. |
| The Rust SDK's public lifecycle computes a reconnect delay and permits caller re-arm. | `ReconnectConfig` defaults from 100ms to 30s and computes capped exponential delay (`crates/liminal-sdk/src/connection/lifecycle.rs:115-195`); `reconnect()` accepts `Reconnecting`, advances its attempt counter, and returns another delay (`crates/liminal-sdk/src/connection/lifecycle.rs:277-300`). It computes only; it does not itself wait. | `«SDK-RECONNECT-DELAY-CONTRACT»` requires one fresh transport-fate event per returned delay/attempt and removes repeated timer re-arm as a public contract. |
| The Gleam SDK mirrors that reconnect-delay contract. | Its lifecycle stores reconnect configuration/counter and documents exponential backoff (`sdks/liminal-gleam/src/liminal/connection.gleam:47-58`); `reconnect` accepts `Reconnecting`, increments, and returns a delay (`sdks/liminal-gleam/src/liminal/connection.gleam:105-129`). It also performs no wait itself. | The same `«SDK-RECONNECT-DELAY-CONTRACT»` retirement applies; compute-only is not permission for callers to turn each result into a timer re-arm. |
| The durability API exposes a configured interval sweeper and scan. | `DedupSweeper` stores `sweep_interval` and `sweep_once` scans the store at caller-supplied time (`crates/liminal/src/durability/dedup/sweep.rs:39-76`); production source exports it (`crates/liminal/src/durability/dedup.rs:7-10`, `crates/liminal/src/durability/mod.rs:20`). | `«DEDUP-EXPIRY-EVENT-SOURCE»` replaces interval scanning with keyed admitted-expiry events or explicit mutation-triggered cleanup; no periodic full-store question is conforming. |
| Existing caps have a signed configuration pattern. | `LimitsConfig` defines named hard caps and defaults, rejects zero by field name, and constructs explicit defaults (`crates/liminal-server/src/config/types.rs:193-203`, `crates/liminal-server/src/config/types.rs:257-316`). | Keepalive, retention, receipt lifetime/count, and negotiated SDK parking extend this pattern rather than creating silent unlimited states. |

**`«RESUME-COMMENT-SERVER-MISMATCH»` is broader than a comment edit.** The
legacy public model is subscription-keyed and client-cursored, while R-C1/R-C3
are participant/conversation-keyed and server-cursored. §7 requires an owner to
choose distinct protocols or a versioned deprecation/removal; merely correcting
the comment cannot close the socket.

**LAW-1 prerequisite inventory — reproducible multi-language product sweep.**
R14 derives this inventory from the pin, not reviewer testimony. The product root
is every tracked path below `crates/*/src` and `sdks/**`, including TypeScript,
Gleam, and `wasm-src`. SDK `test/` and `tests/` directories are intentionally
excluded from the **product** claim, not represented as swept clean. In-source
Rust test modules/files remain visible and classify as test-only. These commands
first prove that the pathspecs match real files and print the root lines, then run
every expression independently so overlaps remain visible:

```bash
PIN=ce8814daa748373d8ffc66b3ff1664f1697a5f4e
PATHS=(':(glob)crates/*/src/**' ':(glob)sdks/**'
       ':(exclude,glob)sdks/*/test/**' ':(exclude,glob)sdks/*/tests/**')
git grep -I -l -E '.' "$PIN" -- "${PATHS[@]}" |
  sed -E 's/^[^:]+://' |
  awk -F/ '/^crates\// {print $1"/"$2"/src"; next}
           /^sdks\// {print $1"/"$2}' | sort -u
git grep -I -l -E '.' "$PIN" -- "${PATHS[@]}" | wc -l

for expression in recv_timeout try_recv poll yield_now sleep 'Instant::now' \
  'read_timeout|set_read_timeout|read_one_frame' wait_timeout park_timeout \
  sleep_until 'SystemTime::now' '\.elapsed[[:space:]]*\(' setTimeout setInterval
do
  git grep -n -E "$expression" "$PIN" -- "${PATHS[@]}" || true
done
git grep -n -E 'sleep' "$PIN" -- ':(glob)sdks/**/*.ts' \
  ':(exclude,glob)sdks/*/tests/**' || true
git grep -n -i -E '([[:alnum:]_]*(sweep|scan)[[:alnum:]_]*)' \
  "$PIN" -- "${PATHS[@]}" || true
git grep -n -E 'Condvar|\.wait[[:space:]]*\(' \
  "$PIN" -- "${PATHS[@]}" || true
for word in reconnect retry backoff delay; do
  git grep -n -i -E "([[:alnum:]_]*${word}[[:alnum:]_]*)" \
    "$PIN" -- "${PATHS[@]}" || true
done
```

The root enumeration prints `crates/liminal-sdk/src`,
`crates/liminal-server/src`, `crates/liminal/src`, `sdks/liminal-gleam`, and
`sdks/liminal-ts`; the nonempty-file count is `199`. The old six extended-root
counts are respectively `28, 2, 151, 12, 58, 96`. The timeout-read expression
has `29` results. The added counts in command order are `wait_timeout=0`,
`park_timeout=0`, `sleep_until=0`, `SystemTime::now=5`, `.elapsed(...)=10`,
`setTimeout=2`, and `setInterval=0`; TypeScript `sleep` has `5`. The case-
insensitive structural sweep/scan expression has `119`, and the explicit
Condvar/`.wait(` shape has `15`. The final underscore-tolerant vocabulary counts
are `reconnect=172`, `retry=43`, `backoff=27`, and `delay=109`. All 21 counts were
re-run at the pin over the same five roots with overlaps intentionally retained.
Zero is evidence because the preceding root enumeration proves the pathspecs matched.

The classification below is total over every returned line, including overlaps.
“Lexical” means a doc/comment/identifier/trait implementation or test-only body,
not an executing production wait. Every executable result belongs to exactly one
behavioral row.

| Complete grep-result class | Classification and reason |
|---|---|
| Main listener; cluster membership; health accept; shutdown drain/settle; channel reply wait; SDK push reader; SDK subscription reader | **Nonconforming prerequisite families already evidenced above.** Retire through the corresponding §7 event socket. |
| Durability `bridge::block_on`; `PushReplyAwaiter::receive` caller re-arm; subscription `read_one_frame` setup loop | **Nonconforming prerequisite families added by R12.** Retire through `«DURABILITY-BRIDGE-WAKE»`, `«PUSH-REPLY-AWAITER-EVENT-RACE»`, and `«SDK-SUBSCRIPTION-SETUP-DEADLINE-RACE»`. |
| TypeScript `Connection.reconnect`, its configurable/default sleep, and `setTimeout` implementation | **Nonconforming prerequisite family.** It is one default-infinite timer-driven retry loop; retire it through `«SDK-RECONNECT-TIMER-LOOP»`. |
| Rust `ConnectionLifecycle::reconnect`/`ReconnectConfig` and Gleam `connection.reconnect`/`ReconnectConfig` | **Nonconforming public re-arm contract, not an executing wait.** Each computes/returns another delay from `Reconnecting`; retire repeated timer re-arm through `«SDK-RECONNECT-DELAY-CONTRACT»`. Other reconnect/retry/backoff/delay matches are TS-family, event-driven state/recovery vocabulary, bounded admission retries, tests, or lexical names classified by the other behavioral rows. |
| `DedupSweeper`, `sweep_interval`, `sweep_once`, and the scan it initiates | **Nonconforming prerequisite family.** Retire interval/full-store change detection through `«DEDUP-EXPIRY-EVENT-SOURCE»`; a caller-supplied clock does not make scanning TOLD. |
| `Future::poll`/`Stream::poll_next` implementations in SDK lifecycle/embedded types | **Event-driven conformant.** These are executor-called callbacks honoring the supplied waker, not application loops. |
| One-shot `recv_timeout`/timeout reads in public SDK receive calls, actor request/reply, routing handoff, health request parsing, and connection setup | **Event-driven conformant only as one admitted wait/deadline.** No caller or callee may use timeout return as a re-arm/change-sampling loop; the violations above are carved out explicitly. |
| Conversation EXIT `try_recv`, pending-reply `try_recv`, process timer reads, causal-order/lifecycle/tracing `Instant`/`SystemTime`/`.elapsed` timestamps | **Event-driven conformant.** Each is read only on an already delivered scheduler/domain event, a final park probe, or records one timestamp; none schedules repeated observation. |
| Shutdown `Condvar::wait`; runtime completion wait | **Event-driven conformant.** Each parks until an explicit shutdown/completion notification. Supervisor barrier waits are `#[cfg(test)]`, not product waits. |
| Conversation-close pending-reply sweep; store `scan` trait/adapter and non-dedup scan mentions | **Event-driven/lexical conformant.** The close sweep runs synchronously on the close event; generic scan surfaces do not schedule themselves. The Dedup caller above is the carved-out interval family. |
| All matches under `*_tests.rs`, `*/tests.rs`, an in-file `#[cfg(test)]` module, docs/comments, field/type names, and negative assertions such as “no polling” | **Lexical/test-only.** They remain acceptance affordances. Tests may coordinate with sleeps/yields; no helper is a production liveness mechanism. SDK test directories are outside, and make no contribution to, the product-sweep claim. |

Consequently the participant implementation may inherit none of the thirteen named
nonconforming families. Main/health accepts use readiness plus explicit shutdown;
membership deltas and connection exit are pushed; deadline waits race one admitted
deadline; SDK readers race input with explicit shutdown; push reply races reply,
connection fate, and deadline; subscription setup races input with one total
deadline; and the durability bridge either completes on its sole synchronous poll
or parks on a real waker. SDK reconnect is armed only by a transport/connection-
fate or explicit manual-connect event. A Rust/Gleam returned delay is valid only
for the attempt armed by that fresh event; repeated re-arm without another fate
event is illegal. Dedup expiry is keyed by admitted
expiry/mutation events. No periodic reap, count/check, timeout-as-wake,
atomic-flag recheck, `sleep` backoff, no-op-waker repoll, synthetic wake, or
“temporary” polling adapter is conforming. Independently of spelling, the
structural audit must inspect every production loop/task/callback that combines a
clock, timeout, sleep, atomic/nonblocking probe, retry, sweep, or scan. Any wait
shape lacking a named TOLD trigger fails; a future lexical hit is unclassified
until this table or a new pinned evidence row classifies it.

**Silence attack / gap acceptance.** Refute this section by identifying an
existing frame that jointly carries participant identity, conversation identity,
a durable authorized cursor, replay position, and lifecycle verdict, or by
showing server Subscribe consumes such a cursor. A nearby field with narrower
scope does not close the gap. Conversely, finding the named listener loop does
not refute R12; it proves the explicit retirement prerequisite remains unmet.

## 2. Section (a) — participant lifecycle at the participant boundary

### Proposed contract

**R-A1 — Typed cause, participant-domain owner.** Introduce `CloseCause` (final
name subject to domain-owner review) and a separate participant-domain observer,
working name `ParticipantLifecycle`. A binding is identified by the immutable
`BindingEpoch { connection_incarnation, capability_generation }` captured by its
attach commit. This is the artifact's sole definition of **binding epoch**; R-C0,
R-C1, R-C5, and restart recovery cite it rather than redefining it. Rotating on
the same physical connection therefore creates a new epoch and can never reuse
the old generation's lifecycle, receipt, finalization, or queued-delivery
identity.

`connection_incarnation` is a fixed-width unsigned 128-bit wire/storage value
`(server_incarnation: u64, connection_ordinal: u64)`. Before participant mode
accepts connections, startup transactionally checked-increments and fsyncs the
persisted `server_incarnation`; it never wraps, rebases, or reuses a value. Before
a connection can negotiate participant capability, the server transactionally
allocates its checked-incrementing `connection_ordinal`. The pair is compared
against every still-live or durably referenced binding, receipt, work item, and
recovery row; a collision is never published and retries the next ordinal.
Exhaustion of either component returns the R-D1
`ConnectionIncarnationExhausted` startup/connection outcome and refuses new
participant connections without changing identity state. The fixed 16-byte
encoding is included in Qb, parked-row, receipt, and frame maxima. Thus the pair
is unique across every simultaneously or durably referenceable connection,
including server restarts, and is minted before first participant use.

Binding-domain facts identify
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
- `UncleanServerRestart { prior_server_incarnation }`: startup recovery found a
  durably active binding owned by a prior server incarnation. It is authoritative
  restart evidence, not a fabricated FIN, clean disconnect, or transport timeout.

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
| Server startup finds a persisted active binding from a prior `server_incarnation` | One claim-backed startup-recovery transaction revokes authority and records `Died(UncleanServerRestart { prior_server_incarnation })` directly or as `PendingFinalization`; it never invents FIN. |
| Worker-registration compensation in `crates/liminal-server/src/server/connection/apply.rs:205-213` | **No participant event.** The registration never became a participant binding. |

The connection owner publishes one typed connection-termination event to the
participant owner; the participant owner derives the keyed terminal events. It
does not infer cause from later absence and adds no detector, sweep, or timer.

**Prior-incarnation startup recovery.** Before participant traffic or receipt
replay is enabled, startup enumerates the bounded persisted binding slots once as
durable recovery input. For each epoch whose `server_incarnation` is not current,
one idempotent transaction immediately revokes transport/cursor authority,
consumes that epoch's already-owned `A` major and `T` sequence claim, records
`UncleanServerRestart`, and either appends the one terminal record where the
ordinary drain law permits or writes the exact ordered `PendingFinalization`.
The transaction also updates the bounded detach cell, ClosureDebt witness, and
its already-owned edge claims atomically. It creates no reserve domain and is not
refusable at full debt or at sequence/order equality. An exact receipt for the
recovered epoch subsequently fails R-C0's current-slot test and returns
`UnboundReceipt`.

The sole state outcome is R-D1 `BindingRecoveryCommitted { participant_id,
conversation_id, recovered_binding_epoch, cause, assigned_transaction_order,
finalization: Appended { delivery_seq } | Pending { admission_order },
repayment_edge }`; exact restart replay returns the same state and cannot append
a second terminal. A storage-commit failure keeps participant mode closed and
retries startup recovery on the next explicit startup attempt; it is not a wire
semantic outcome. Enumeration is bounded by signed identity slots and runs once
from startup input, never from a periodic scan. Each transaction is TOLD by
startup/storage completion and introduces no timer, poll, or sweep. Its only
durable state is the existing binding/finalization/detach/debt cells plus the
terminal record, so idle cost does not grow.

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
`(transaction_order, candidate_phase, ascending_participant_index)`. Its complete
canonical phase order is:

| Numeric phase | Protocol name | Record/candidate class |
|---:|---|---|
| 0 | `BindingTerminal` | Pending or direct `Detached`/`Died`, including startup recovery. |
| 1 | `MembershipExit` | `Left`; a bound `Left` that also terminalizes its binding appears only here, not twice. |
| 2 | `AttachLifecycle` | `Attached`, including enrollment, supersession, detached attach, and fenced recovery. |
| 3 | `OrdinaryRecord` | One admitted application record owned by the transaction. |
| 4 | `CompactionMarker` | Every `HistoryCompacted` candidate/record induced by the causal transaction. |

All binding terminals in a major therefore precede all attached/ordinary facts
and **all induced markers**; within one phase, ascending `participant_index`
decides order. Transaction-specific impossible classes are absent rather than
assigned another value: enrollment has `AttachLifecycle` then any markers;
supersession has `BindingTerminal`, `AttachLifecycle`, then markers; a multi-
binding fate has every terminal then every marker; and a bound one-record Leave
has `MembershipExit` then any exit markers. A positional composed preserved-terminal Leave uses the preserved old major for
phase 0 and the member's `X` major for phases 1 and 4 only when no unrelated
candidate tuple lies strictly between those majors. Otherwise R-C2 drains the
terminal at its old tuple and later commits one-record `Left` at X.

`participant_index` is the permanent ordinal of the identity reservation slot,
assigned once in `0..I` at enrollment. It is unique within the conversation,
persists through live membership and tombstone, and is never reused. All
candidates caused by one transaction—including every marker found by R-C4's
fixed point—share that major without aliasing. **Candidate-key uniqueness** is a
named invariant: no two live candidates or direct records may share the complete
`(transaction_order, candidate_phase, participant_index)` tuple. Commit and
restart reconstruction reject a duplicate as corrupt durable state rather than
choosing an order.

Finite ordering is backed by consumable claims, not physical-longevity prose.
This is the sole **reserved-counter-capacity** definition: every accepted future
obligation owns its required finite counter value before publication, and later
work refuses rather than borrow it. Let `A` be active binding epochs whose terminal fate has no assigned major and let
`X=L` be live members whose tokenized `Left` has no assigned major. Let `RO` be
edge-owned current-operation majors and `RA` edge-owned replacement-binding
terminal majors; both are zero or one because the one stored edge has a fixed
claim set. Leave is never accepted into separate server pending state, so every
stable live member retains its exit claim. Checked arithmetic preserves:

`order_remaining >= A + X + RO + RA`.

A DCR-capable edge owns `RO=1` for the recovery Attach transaction and `RA=1`
for the replacement epoch's future terminal. Recovery Attach consumes RO and
atomically transfers RA into A. No attach, fate, or Leave borrows these values.

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
transitions cannot grow its charged full-row bytes.

Define every recovery-handshake input and limit as an unsigned wire/configuration
`u64`: signed nonzero `P` is in `1..=u64::MAX`; `RF`, `RC(P)`, `RE`, `R`, and
`WF` are in `0..=u64::MAX`, with `RE>0`; and `RC(P)` is the codec-computed byte
width of the u64 list count (never an estimated element count). Compute

`RH(P) = u128(RF) + u128(RC(P)) + (u128(P) × u128(RE))`

in checked u128 and compare it to `u128(R)`/`u128(WF)`. For u64 inputs the
maximum three-term sum is at most `u128::MAX`, so arithmetic overflow is
unreachable and has no outcome discriminant. These are codec-schema maxima, not
runtime estimates; their fixed widths are included in R/Qb accounting.

Startup and first participant-capability negotiation first reject configuration
shape as `ParticipantParkingConfigurationInvalid { dimension, operands }`. Its
exhaustive dimensions are `RowSchemaBytes` for `B < R + maximum_metadata`;
`CheckedProduct` for overflow in any validated product; `RowBytesBound` for
`C > N × B`; `SdkBytesBound` for `D > G × B`; and `RecoverableSlots` for
`P > max_participant_conversations_per_connection`. `operands` carries the exact
left/right factors, checked result when one exists, actual, and limit appropriate
to the named dimension. These shape checks precede recovery-handshake size. Only
a shape-valid proposal then rejects `RH(P) > R` or `RH(P) > WF` as
`ParticipantRecoveryHandshakeTooLarge {
max_entries: P, framing_bytes: RF + RC(P), entry_bytes: RE, encoded_bytes:
RH(P), request_limit: R, wire_frame_limit: WF, dimension: RequestBytes |
WireFrameBytes }`. Thus every accepted configuration can encode the complete
one-shot recovery request. The configured `C` and `D`, not their products, remain
independently binding exact full-row byte ceilings. Checked validation does not
unacceptably constrain legal P: P already means the maximum conversations one
connection must recover atomically, so R12 deliberately adopts **no chunked
handshake** and introduces no per-chunk arming race.

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
recoverable connection slots **and** `RH(P)` is validated against both R and WF,
every Awaiting conversation can be included in one encodable handshake. Equality
linearizes either after progress (older branch) or before it (installed recipient
receives the push), never between both.

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

Lifecycle phase, not check order, selects the recovery-handshake outcome. The
**initial phase** is server startup validation or a connection's first participant-
capability negotiation, before it owns any durable parked row; it uses
`ParticipantParkingConfigurationInvalid` for shape and, only after shape passes,
`ParticipantRecoveryHandshakeTooLarge` for RH size. The **parked phase** is restart or
renegotiation whose serialized pre-validation snapshot contains at least one
parked row; it uses only `SdkParkingCapacityIncompatible`. The phase bit, row snapshot, validation, and configuration/handshake commit all
run in one SDK-wide serialization domain. That same domain contains every row
reservation or state mutation, the sender's `Reserved|RetryAuthorized→InFlight`
commit and write authorization, response/progress deletion, and every authority
or expiry visit. None can interleave, so both outcomes can never race for one
proposal and a rejected proposal can send or delete no snapshotted row. Restart/renegotiation with no
parked rows is initial phase.

In parked phase, validation checks every dimension before sending any row:
`RowSchemaBytes`, `CheckedProduct`, `RowBytesBound`, `SdkBytesBound`, and
`RecoverableSlots` configuration shape; conversation and SDK-wide row/byte counts;
parked-conversation count; each stored request length against new `R`; each full
serialized row length against new `B`; and `RH(P)` against new R and WF. The same
shape failures map to the correspondingly named
`SdkParkingCapacityIncompatible` dimensions, with exact operands. Any
violation returns `SdkParkingCapacityIncompatible { scope, dimension,
conversation_id, park_order?, occupied_or_actual, negotiated_limit,
max_recovery_entries?, recovery_framing_bytes?, recovery_entry_bytes? }`.
`dimension: RecoveryHandshakeRequestBytes | RecoveryHandshakeWireFrameBytes`
carries the recovery fields and exact u128 encoded size. Existing parked rows are
preserved even when a downward R/WF change would strand them; no partial
handshake is sent. Every violation preserves every row and sends none until
operator/configuration correction. `Reserved` rows and interest
slots participate. It never grandfather-discard rows. The only request-sized
waiter is this SDK-wide and per-conversation bounded set; every retry is TOLD by
startup, connection readiness, response, observer progress, or authority change,
never a timer, poll, or sweep.

**Idle-cost closure.** Per SDK, parked conversation/interest headers are bounded by
signed P, rows by signed G, and fully serialized bytes by signed D; per-conversation
subsets are additionally bounded by N/C and each row by R/B. Per server
conversation, `ClosureDebt`, its fixed-size edge-claim bitmap/counters, and
`transaction_order` are fixed header fields; K is unavailable occupancy inside the
existing signed cap, not stored bytes. Binding epoch, request verifier, and order/exit-claim tags live in already bounded
identity/receipt cells. Attach/enrollment verifier life is TTL/cap bounded;
detach verifier life is identity-cell bounded; Leave verifier bytes were reserved
in the identity slot and remain inside its permanent bounded tombstone. Startup recovery reuses binding/finalization/detach/
debt cells and creates only the already-owed terminal. Marker/finalization/detach
cells are bounded by signed I; retained log bytes/entries by their signed caps;
and connection-local binding/interest/dispatch maps by signed
`max_participant_conversations_per_connection`. Resettable
`park_order` is one fixed header per currently parked conversation. No new R12
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

R12 retains two different liveness classes:

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
`«KEEPALIVE-PORTABILITY»`; R12 does not manufacture numbers.

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
Push reply races reply/connection fate/one deadline; subscription setup races
input against one total deadline; and the durability bridge performs one asserted
synchronous poll or uses an executor honoring its waker. Rust/Gleam reconnect-delay results additionally require a fresh transport-fate
event per attempt; repeated caller timer re-arm is nonconforming. The implementation
dependencies are §7 sockets. Retention, replay, lifecycle, startup recovery,
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

**Canonical live-token verifier.** Every live token record stores a fixed-size
constant-time `request_verifier` over the protocol-versioned canonical request.
Canonicalization is the unique wire-field order with normalized optional tags.
Credential attach and Leave are the only secret-bearing operations: their verifier
combines a constant-time secret-proof verifier with a non-secret fingerprint over
operation discriminant, conversation, participant, presented generation, and
every operation field. Credential attach necessarily covers
`accept_marker_delivery_seq: Option<DeliverySeq>`. Enrollment covers its complete
non-secret body. Detach is deliberately non-secret-bearing: its cell verifier
covers generation and every detach operation field, while its exact binding epoch
plus current generation supplies authority. Adding a detach secret would change
the protocol and is forbidden. Padding, map order, or alternate encodings cannot create a second
canonical body.

Exact-token lookup checks this verifier before returning any receipt, cell, or
Leave tombstone result. A canonical match replays the stored result. A secret-
proof mismatch returns the existing `StaleAuthority` with equal presented/current
generation where applicable and reveals no receipt. With a matching secret proof,
any other canonical-body mismatch returns R-D1 `AttemptTokenBodyConflict { token,
operation, conversation_id, presented_participant_id?, presented_generation?,
presented_marker_delivery_seq?, conflict: Generation | MarkerDeliverySequence |
CanonicalBody }`; it mutates nothing and discloses neither stored body, credential,
nor receipt. `conflict` reports only which presented non-secret field class differs.
For a nonmatching token, ordinary tombstone precedence remains unchanged.

The verifier is stored with the already counted live receipt, pending detach cell,
bounded parked token-fate row, or successful Leave tombstone. Receipt TTL/caps
bound attach/enrollment verifiers; the identity-slot cell bounds detach. The
permanent retirement slot reserves the fixed maximum Leave verifier bytes at
enrollment, and Leave transfers that charge into its bounded tombstone without a
new capacity decision. The tombstone retains the non-reversible secret-proof
verifier, canonical fingerprint, token, and `LeaveCommitted` fields, never the
secret or body. Its lifetime is exactly the tombstone's permanent lifetime. This
verifier is distinct from R-C0's provenance fingerprint and creates no table,
sweep, or idle-cost dimension.

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
check is exhaustive. A live exact token first runs R-C0's constant-time verifier:
wrong secret is `StaleAuthority`, while matching-secret non-secret mismatch is
`AttemptTokenBodyConflict`. Otherwise, after tombstone/identity lookup, authority
compares generation and then the presented secret in constant time. A generation
mismatch or an equal-generation secret mismatch returns the one
`StaleAuthority` row carrying presented and current generations. Equality of
those fields identifies either wrong-secret path without a new discriminant or
additional oracle. Every refusal commits no receipt, order, cursor, binding,
lifecycle record, candidate, or retention mutation.

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
generation/secret; recomputes the floor, marker capacity credit, `ClosureDebt`,
and its repayment edge; and binds the new epoch. It consumes the edge's pre-owned
RS/RO and transfers RT/RA into that epoch's T/A. Absolute fit plus `d'<=Q` is
sufficient even at full incoming debt: because the named marker is accepted, the
successor is exact `ObserverProjection` or `PhysicalCompaction` (or `None` at zero
debt), never `MarkerDelivery`. It returns `AttachBound` with
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

0a. Exact-token lookup runs R-C0's receipt/cell/tombstone state machine before
    ordinary identity lookup. **Every** token-keyed state, including a successful
    Leave tombstone, first constant-time checks its stored canonical verifier:
    wrong secret is `StaleAuthority`; matching-secret body mismatch is
    `AttemptTokenBodyConflict`; only a byte-identical canonical body may return a
    stored result. A detach cell has no secret-proof arm: byte-identical body
    replays, a same-generation operation-body mismatch is
    `AttemptTokenBodyConflict`, and a generation mismatch is `StaleAuthority`. A matching live attach receipt returns its stored credential
    payload plus current `Bound`/`UnboundReceipt`; a terminalized or expired attach
    token returns only its retained non-secret R-C0 outcome; matching detach cells
    return their stable result; and a matching Leave tombstone returns its stable
    `LeaveCommitted`. A different token reaches ordinary tombstone precedence.
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
Let `RS` be edge-owned sequence values for the current recovery Attach record and
`RT` edge-owned sequence values for the replacement binding's future terminal;
each is zero or one. Every live member owns one value for eventual `Left`; a bound
member also keeps its separate terminal claim in `T`. Checked wide arithmetic
preserves after every commit:

`MAX - H >= E + T + M + RS + RT + (L × T) + (L × RT) + (L_other × E)`.

The canonical `SequenceBudget` payload is `{ high_watermark: H, remaining:
MAX-H, E, T, M, RS, RT, L_times_T: L×T, L_times_RT: L×RT,
L_other_times_E: L_other×E }`. Every `ConversationSequenceExhausted` outcome and
codec vector carries exactly this payload for its proposed resulting state; no row
may abbreviate away a zero term.

A DCR-capable edge owns `RS=1,RT=1`. Recovery Attach consumes RS and atomically
transfers RT into T; its pre/post right sides differ by exactly the one appended
record. A pre-attach fate plan also owns every conditional M value. No transition
borrows E/T/M/RS/RT or counts one numeric value in two terms.

R14 preserves the E/T/product terms and gives every preplanned marker exactly one
backing class. `NonProductM` markers are counted in `M` immediately.
`TerminalProduct { terminal: T | RT, affected_participant }` markers remain in
`L×T` or `L×RT`, and `ExitProduct { exit: E, remaining_participant }` markers
remain in `L_other×E`, until that causal terminal or exit fires; that transaction
atomically transfers each marker actually induced into `M` and releases every
unused conditional claim. `edge_claims.marker_sequence_values` records the exact
value, backing enum, causal `transaction_order`, phase-4 candidate position, and
participant index. Thus a value is never simultaneously M and product-backed,
and transfer is not a later sequence/order allocation. If the
post-state cannot include those claims, the **original optional operation** gets
`ConversationSequenceExhausted`, `ConversationOrderExhausted`, or
`MarkerClosureCapacityExceeded`—whichever first check in R-D1 precedence fails—
before mutation. This cites R-A2's sole **reserved-counter-capacity** definition: an accepted
obligation owns its finite counter values and later ordinary work cannot borrow
them; R-C2 applies that definition to sequence values without redefining it.

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
   `MarkerClosureCapacityExceeded` with no automatic retry.

   Leave first drains every globally earlier candidate in a separate committed
   candidate transaction, strictly by the R-A2 tuple. Each transaction is
   independently Q-bounded and crash-visible. A terminal transaction appends that
   terminal and atomically creates its complete same-major marker fixed point;
   those markers then drain in phase-4/index order before any later major. After
   every commit all invariants hold, so restart resumes at the lowest tuple; no
   transaction groups an unbounded set.

   If this member's pending terminal remains after that prefix, the two-record
   atomic core is legal **iff** no unrelated candidate tuple lies strictly between
   the terminal's preserved major and the member's X-major `Left`. In that
   positional case the core is exactly the old-major terminal then X-major `Left`.
   It alone evaluates one combined final floor: no intermediate floor is visible,
   and newly induced exit markers use the Left major. It releases T/E, applies
   floor/credit/debt changes, and writes the tombstone atomically.

   If an unrelated tuple lies between those majors, composition is forbidden.
   The member's own pending terminal drains first as a separate standard terminal
   transaction at its exact tuple, with all same-major markers draining after it;
   every intervening tuple then drains in order. Leave finally commits as the
   existing one-record `Left` transition of an already-detached member whose
   terminal is complete. Each transaction computes its own floor; “one combined
   final floor” does not apply. The stable receipt still records that separately
   committed terminal sequence.

   Observer blockage returns `ObserverBackpressure` before accepting the affected
   transaction; otherwise nonzero debt requires absolute fit and a resource-backed
   successor. The crash-visible order is the exact sequence of separate terminal/
   marker transactions, followed either by the indivisible composed pair or the
   one-record Left, then exit markers. A crash between transactions exposes that
   valid prefix; a crash around a composed core exposes all-old or both records
   plus the tombstone. Exact replay returns one stable `LeaveCommitted {
   retired_generation, prior_terminal_delivery_seq: Option, left_delivery_seq }`;
   the optional field is present whenever a prior terminal exists, whether it was
   committed separately or in the core. This proof grants no cursor authority.
5. **The Leave transaction retires everything.** It appends exactly one ordered
   R-A1 `Left`. A prior pending terminal is either in the positional composed core
   or was separately committed before this transaction; no terminal is duplicated.
   Leave terminalizes any active binding and the membership, invalidates current
   secret, converts every
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
`max_retired_identity_slots`; `marker_max=(1,Bm)` be the maximum encoded entry/
byte cost of one `HistoryCompacted`; and `Q=(Qe,Qb)` be the maximum encoded cost
of one transaction in exactly these **mandatory closure classes**: enrollment,
attach from detached, the ordered supersession pair, one-record Leave, the own-
pending-terminal plus `Left` two-record core, one earlier-candidate drain
transaction, immediate detach terminal, marker append, pending-finalization
drain, and R-C1 fenced recovery. Keys, fixed transaction/candidate order, the
16-byte connection incarnation, and storage framing are included.

Define componentwise transferable **recovery occupancy** `K=(Ke,Kb)=Q`. Q is
the only borrowable mandatory-transaction envelope; K is never debt. With no edge
and zero debt, `K_remaining=(0,0)` and all K is free. A newly stored nonzero-debt
edge holds `K_remaining=K`. When its guaranteed fenced recovery materializes
actual record charge `r=(re,rb)`, the same transaction adds r exactly once to S/B
and subtracts it componentwise from the incoming K claim:

`0 <= r <= K_remaining`, and `K_remaining' = K_remaining - r`.

The edge occupancy fields equal exact post-transfer `K_remaining`, never all K by
fiat. Transferred occupancy is already in B and is not free or counted again; no K
is reusable while debt remains. When repayment clears both debt components, the
edge and `K_remaining` clear atomically and all K becomes free. K covers binding
fate plus pending-terminal/recovery Attach while an anchored
marker remains retained. Checked startup validation is:

- `max_retained_conversation_entries >= (2 × Qe) + (I × 1)`; and
- `max_retained_conversation_bytes >= (2 × Qb) + (I × Bm)`.

Every sum/product uses checked wide arithmetic; overflow is
`ParticipantRetentionCapacityInvalid`. At empty equality, base marker reserve
plus free Q plus free K is exactly `I×marker + Q + K = 2Q + I×marker`.
Ordinary work consumes neither Q nor K. A mandatory transaction may borrow Q but
must leave K physically free for, or atomically held by, its edge. Mandatory
records have no reachable `RecordTooLarge`; ordinary caller-sized records may.

Each conversation header has one fixed-size durable
`ClosureDebt { entry_debt: 0..=Qe, byte_debt: 0..=Qb,
last_consuming_transaction_order, repayment_edge, edge_claims }`, initialized to
zero, `None`, and zero claims. `edge_claims { marker_sequence_values: [(value, backing_class, causal_tuple); I],
recovery_attach_sequence: Option<u64>, replacement_terminal_sequence:
Option<u64>, recovery_attach_order: Option<u64>, replacement_terminal_order:
Option<u64>, candidate_positions, successor_milestones, occupancy_entries,
occupancy_bytes }` is fixed-width. Candidate positions and successor milestones
are I-/enum-bounded bitmaps, never growing sets. Marker values identify their exact R-C2 `NonProductM`, `TerminalProduct`, or
`ExitProduct` backing; recovery fields are exactly RS/RT/RO/RA in the two reserve invariants; order/
positions are exact R-A2 tuples; and occupancy equals exact post-transfer
`K_remaining(edge)` in each dimension. Serialization includes every field and fixed maximum byte. No claim is
double-counted or implicit.

The complete `repayment_edge` enum is:

- `None`;
- `ObserverProjection { through_seq }`;
- `PhysicalCompaction { from_floor, through_seq }`;
- `MarkerDelivery { participant_id, binding_epoch, marker_delivery_seq }`;
- `ParticipantCursorProgress { participant_id, binding_epoch, through_seq,
  marker_delivery_seq: Option<DeliverySeq> }`; and
- `DetachedCredentialRecovery { participant_id, recovery_through_seq,
  marker_delivery_seq: Option<DeliverySeq>, prior_binding_epoch }`.

Every binding/sequence value in a tag is exact and immutable. A planned marker's
`marker_delivery_seq` is its pre-owned value even before append. On restart the
participant owner re-registers exactly the tagged storage/projection/compaction/
delivery/cursor/recovery interest and continues from its recorded claims; it
never scans for a substitute witness.

Let `S_actual` be the actual encoded retained suffix. Let closure charge `S` equal
actual non-marker retained cost plus `marker_max` for every planned or physically
retained credited marker; therefore `S_actual<=S` in each dimension. Let `C` be distinct reserved
identity slots currently owning a **marker capacity credit**. A credit is acquired when a marker is planned and
survives candidate append, delivery, and acceptance until that exact marker is
physically compacted. It can end earlier only when Leave cancels a still-unwritten
candidate in the same transaction. Thus `C` may temporarily include a retired
identity whose accepted marker remains retained, and always satisfies `0<=C<=I`.
One identity cannot own a second marker candidate/record until physical compaction
(or same-transaction cancellation) releases its credit. Write the componentwise retained baseline as

`B = S + ((I - C) × marker_max)`.

With zero debt, `edge=None`, K is free, and ordinary admission must preserve:

`B + Q + K <= configured_cap`.

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
`MarkerClosureCapacityExceeded`. Mandatory classes collectively borrow at most Q.
For a mandatory proposed post-state that stores an edge, let `B'` use the formula
above and let `K_remaining'` be its exact post-transfer componentwise occupancy.
Its debt and absolute-fit checks are:

`d' = max(0, B' + Q + K_remaining' - configured_cap)`,

`B' + K_remaining' <= configured_cap`, and `d' <= Q`.

Thus debt is exactly borrowed Q. An incoming recovery transaction of charge r
uses `B'=B_after_removal+r` and `K_remaining'=K_remaining-r`, so positive recovery
records fit without double-counting. Edge occupancy is not a free variable:
`edge_occupancy_claims=K_remaining'`. A no-edge post-state is legal only with
debt zero and `B'+Q+K<=cap`. The candidate appears once in S', never again as Q
or K. Commit also requires the complete R-C2/R-A2 RS/RT/RO/RA and marker claims.
At startup equality, let enrollment's one actual `Attached` charge be
`q_enroll=(1,b_enroll)` with `b_enroll<=Qb`. Then
`B'=I×marker+q_enroll`, `d'=max(0,B'+Q+K-cap)=q_enroll`, and
`B'+K=cap-Q+q_enroll`; enrollment is not an exact-Q transaction. A genuinely
exact-Q supersession pair reaches debt Q. In either walk the producer stores
`K_remaining=K`; later recovery charge transfers from that claim into B.

While either debt component is nonzero, ordinary caller admission returns
`MarkerClosureCapacityExceeded`. A mandatory transaction may commit at any debt,
including `(Qe,Qb)`, only if absolute fit and the bounded equations hold and its
post-state either lowers debt for everything consumed or stores one valid row of
this closed successor machine with all resources through its finite fixed point
already owned:

| Stored edge | TOLD event and exact witness | Complete invalidators and atomic successor/effect |
|---|---|---|
| `ObserverProjection` | Projection storage completion through exact `through_seq`. | Completion advances o, recomputes floor, materializes only preplanned M/positions, and selects the strict-plan suffix `PhysicalCompaction`, `MarkerDelivery`, or `ParticipantCursorProgress`. Fate, supersession, and Leave do not invalidate this observer witness. |
| `PhysicalCompaction` | Enabled storage compaction of exact `[from_floor, through_seq]`. | Exact or overlapping floor advance satisfies the covered prefix, releases only credits whose accepted markers are removed, lowers debt, and selects the earliest strict suffix. Participant lifecycle cannot invalidate a storage range. |
| `MarkerDelivery` | Candidate append/storage completion and final-emitter delivery of the exact marker to the exact binding epoch. | Delivery becomes `ParticipantCursorProgress`; detach/death makes the identity detached and becomes claim-backed `DetachedCredentialRecovery`; supersession atomically retargets exact delivery to its new epoch after full fixed-point preflight; Leave atomically releases the anchor and selects observer/compaction or clears debt. These are the complete invalidators. |
| `ParticipantCursorProgress` | Exact normal/marker ack from the named binding and through-sequence. | Ack advances only that cursor (marker ack also releases anchor) and selects observer/compaction. Detach/death atomically creates `DetachedCredentialRecovery` for the detached identity; supersession retargets the same through/marker witness to the new epoch after preflight; Leave itself performs anchor/debt effect and successor replacement in its absolute-fit transaction. These are the complete invalidators. |
| `DetachedCredentialRecovery` | Exact-current tokenized fenced attach or claim-backed Leave after prior fate. | Fenced attach atomically accepts the named marker, transfers its actual terminal/Attached charge from K_remaining into B, releases the anchor, consumes the episode's sole RS/RT/RO/RA quartet into Attach/new T/A, and selects independent `ObserverProjection` or `PhysicalCompaction`, **never `MarkerDelivery`**. Claim-backed Leave likewise transfers actual charge, spends E/X, releases the anchor, and selects observer/compaction or clears debt. Ordinary non-fenced attach while this anchored nonzero-debt edge exists is forbidden and returns `MarkerClosureCapacityExceeded` without mutation. Authority supersession before commit is `StaleAuthority` and leaves the edge current. These are the complete invalidators. |

MarkerAck therefore has two explicit ordinary orderings. If o is behind the
accepted marker, it atomically replaces `ParticipantCursorProgress` with
`ObserverProjection`. If o is already through the marker, it atomically stores
`PhysicalCompaction`; acceptance itself still leaves S_actual, S, C, and debt
unchanged. Projection without member ack similarly materializes its preclaimed
marker and moves to delivery/cursor progress rather than losing its witness.

A binding-fate event invalidating delivery and any event satisfying projection,
cursor, or compaction performs the named debt effect and successor replacement
in **the same durability transaction**. There is no crash point between witness
invalidation and successor commit. Any other event that can invalidate a witness
is a contract defect unless added as a row; `None` is legal iff both debt
components are zero.

**Resource-backed edge invariant.** Before storing any edge, the transaction
simulates every marker, invalidator, and every post-completion successor-selection
state in its finite plan; assigns marker backing, credit, causal tuple, the sole
RS/RT/RO/RA quartet, and exact K_remaining; and serializes the remaining-milestone
bitmap. `successor_milestones` is a fixed enum bitmap over
`ProjectionCompleted`, `CompactionCompleted`, `MarkerAppended`, `MarkerDelivered`,
`CursorProgressed`, `BindingFateObserved`, `SupersessionObserved`,
`FencedRecoveryCommitted`, `ClaimBackedLeaveCommitted`, and selection of each
repayment-edge tag or `None`. A transition consumes its event and selection bits
and may retain only a strict subset of the original bitmap; it can never add a bit. A later allocation that would consume an owned value,
position, credit, or byte/entry must transfer a still-fireable suffix atomically or
refuse before mutation. It may not borrow E/T/M/A/X or edge claims. At equality,
an edge fires entirely from those claims or the original optional producer was
refused by its first sequence/order/closure outcome.

Each debt episode stores at most one RS/RT/RO/RA quartet and consumes it exactly
once. An ordinary attach that could later need DCR must pre-own that death branch
before the attach commits; if sequence or order capacity cannot hold the quartet,
the attach refuses its first exhaustion outcome. Once DCR is anchored, ordinary
non-fenced attach is forbidden. Fenced attach consumes RS/RO and transfers RT/RA
into the new binding's T/A, but marker acceptance ends the DCR cycle and its
observer/compaction successor is independent of later binding fate. Claim-backed
Leave ends it directly. No transition re-endows a quartet, and consumed numeric
values are never reused.

**Closure/induction proof sketch.** Name the lexicographic decreasing measure
`μ=(successor_milestones_remaining, entry_debt+byte_debt, M)`. The debt-producing
base commit proves the componentwise Q/K equations and owns a finite milestone
suffix. Every table event consumes one milestone and can select only a strict
suffix; a clearing transition sets μ to zero. Within one floor preflight the named
measure `ν=I-|affected_identity_slots|` strictly decreases whenever a new marker
owner is discovered, so at most I steps close the marker set. Per-row invalidator
audit is complete as stated in the table: observer/storage witnesses have only
completion/range advance; delivery/cursor add fate, supersession, and Leave; DCR
adds all three attach/Leave outcomes. Each invalidation and successor replacement
is one transaction. Induction on μ therefore preserves: nonzero debt has exactly
one valid tag, exact `K_remaining`, complete claims, and no crash-visible invalid witness.

**Anchored recovery occupancy.** Whenever an edge is stored, preflight every
occupancy-consuming operation against the worst binding-fate plus pending-terminal-
and-detached-attach sequence with the marker anchor held. Exact K is unavailable
to supersession, another mandatory transaction, or ordinary work. For
`I=1,Qe=2`, the new legal minimum is `2×2+1=5` entries, not 3. With an anchored
marker, a proposed Qe-sized supersession is refused *before supersession* if its
complete successor fixed point would invade K; it may never commit and discover
after death that recovery cannot fit. The byte dimension is identical with Qb/Bm.

“Repayment edge” promises an ordinary-admission-independent TOLD path, not that an
external participant or observer will cooperate; a deliberate wedge remains the
named §7 policy. The fixed header stores the lexicographically first valid witness
when several exist. It names exact binding/sequence boundaries, is recomputed or
replaced in the same transaction as debt or witness invalidation, and is cleared
when both components reach zero. A mandatory candidate that cannot lower debt or
preserve a row uses its already-owned pending cell until its TOLD event; an optional
producer is refused before mutation. Marker append, append-free ack, observer
progress, cursor progress, and floor compaction may run while debt exists. No
repayment path polls, scans, or requires an ordinary record to make room.

The physical floor remains reproducible. Let `H'` be the candidate watermark;
`F` the current first retained sequence (`H+1` when empty); `m` the minimum member
cursor after membership changes, or `H'` with no members; `o` the hard
`observer_progress`; and `a` the earliest live-member unaccepted marker sequence,
or checked one-past-end `END=MAX+1`. `END` is never allocatable. Define
`preferred_floor=min(m,o)+1`. For candidate floor f, let `B_f` include every
removal and fixed-point marker charge and let `K_f` be exact post-transfer
K_remaining. Define the committing-class predicate
`Envelope(tx,f) = (B_f+Q+K<=cap and debt=0)` for ordinary transactions, and
`Envelope(tx,f) = (B_f+K_f<=cap and max(0,B_f+Q+K_f-cap)<=Q)` for mandatory
transactions. Then `cap_floor=min { f>=F | Envelope(tx,f) }`, never an envelope
from another transaction class; `F'=max(F,preferred_floor,cap_floor)`, valid only when `F'<=a`.
Successful commit appends at `H'`, removes exactly `[F,F')`, and stores floor/debt
atomically. Marker acceptance, fenced provisional recovery, or Leave releases its
owner's anchor; nothing else does. Observer hard retention forbids
`cap_floor>o+1` and produces `ObserverBackpressure` before mutation. Member claims
remain soft. No member is ever overtaken by `preferred_floor`, because
`min(m,o)+1 <= m+1`; every `HistoryCompacted` marker is caused by `cap_floor`
pressure or by enrollment whose cursor 0 was already below F. Persisted progress
emits the wake; no poll does.

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
persistence the SDK atomically retires only the old
`DispatchSlot=(connection_incarnation, conversation_id, participant_id,
binding_epoch)` before exposing `AttachBound`. Later origin-stream bytes for that
slot are dropped as old-epoch input, never Q delivery. The physical C1 reader
continues serving every other conversation/dispatch slot; only actual connection
fate drives whole-reader shutdown through the explicit wake. Thus no P work can
be constructed after its slot barrier or retargeted merely because a physical
slot was reused, while unrelated multiplexed delivery is uninterrupted. The
fixed-size dispatch map remains bounded by the negotiated per-connection
conversation cap and adds no unbounded state or polling.

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
An arm that omits any quantity read by its transition is a **suite defect**, not a
fixture default; H/F/o, L/E/T/M, every cursor/anchor/credit, A/X/order high,
ClosureDebt/edge claims, caps/S occupancy, binding epoch/generation/secret, and
all pending candidates/admission orders must be explicit.

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
21. Test-seed the complete generation-boundary snapshot below; `Kmax` is
    explicit fixture secret bytes and all omitted attempt-token indexes are empty:

    | Quantity | Exact pre-attach value |
    |---|---|
    | H/F/o; sequence budget | `H=10,F=11` empty, `o=10`; canonical budget `{ high=10, remaining=MAX-10,E=1,T=0,M=0,RS=0,RT=0,L×T=0,L×RT=0,L_other×E=0 }` |
    | Cursor/marker/candidates | P0 cursor 10; no anchor, credit, candidate, or admission order |
    | Order/debt | `A=0,X=1`, order high 10 with ample remaining; debt zero, edge `None`, all edge claims/milestones zero, K free |
    | Capacity | `I=1,Q=(2,2Bm),cap=(16,16Bm)`; `S_actual=S=0,C=0,B=(1,Bm)`, so `B+Q+K=(5,5Bm)`; `cap_floor=F=11` |
    | Identity/authority | P0 index 0, detached at generation `u64::MAX`, secret `Kmax`; no binding epoch, tombstone, receipt, fingerprint, detach cell, or live token verifier; target slot empty; receipt/fingerprint caps ample |

    Send an ordinary credential attach and prove `GenerationExhausted` is terminal,
    non-retryable, and leaves every table cell and canonical budget byte-identical.
    Exercise live-receipt and fingerprint-cap `ReceiptCapacityExceeded`, every
    reachable discriminant and every R-D1 row in both directions: each operation
    has only listed outcomes and every listed outcome has a trigger, exact fields,
    retryability, and SDK transition.
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
25. Exercise the complete initial-configuration matrix before any conversation
    state exists:

    | Arm | Exact inputs and failed predicate | Required outcome fields |
    |---|---|---|
    | Row schema | `R=100,metadata=20,B=119` | `ParticipantParkingConfigurationInvalid { dimension: RowSchemaBytes, actual:119, required:120 }` |
    | Product | checked u64 product `u64::MAX×2` | `dimension: CheckedProduct`, factors `u64::MAX/2`, operation Multiply, overflow |
    | Per-conversation bytes | `N=2,B=10,C=21` | `dimension: RowBytesBound`, factors/product `2/10/20`, actual 21 |
    | SDK bytes | `G=3,B=10,D=31` | `dimension: SdkBytesBound`, factors/product `3/10/30`, actual 31 |
    | Recovery slots | `P=6,connection_slots=5` | `dimension: RecoverableSlots`, actual 6, limit 5 |

    In every arm phase is initial, parked rows/bytes/conversations are zero, and no
    row, interest, handshake, or participant state exists. Shape failure precedes
    RH checks. With shape valid use `P=4,RF=1,RC(P)=1,RE=2`, hence
    `RH(P)=1+1+4×2=10`, and start with `R=WF=10`. Lower R, then WF, to 9 and assert
    only the two `ParticipantRecoveryHandshakeTooLarge` dimensions and those exact
    u128 operands.
    In parked phase repeat all five shape arms and assert the same operands under
    the corresponding `SdkParkingCapacityIncompatible` dimensions, preserving all
    rows and sending no row or partial handshake.

    Separately use `I=1,Q=K=(2,2Bm),marker=(1,Bm)`, reject caps below `(5,5Bm)`,
    then start an empty conversation at exact equality. Before enrollment,
    `S=0,C=0,B=(1,Bm)`, K is free, and `B+Q+K=cap=(5,5Bm)`. Let the one Attached record's
    actual charge be `q_enroll=(1,Bm)`. After enrollment,
    `S'=q_enroll,C'=0,B'=I×marker+q_enroll`, `K_remaining=K`,
    `d'=max(0,B'+Q+K-cap)=q_enroll`, and
    `B'+K=cap-Q+q_enroll`; `cap_floor=F` and the record is counted once. Exercise
    every detach outcome, prove acks never backpressure, and re-run case 21.
26. Test-seed the amended complete entry-equality snapshot below. Every record
    costs one entry; ordinary/marker/Left bytes are Bm, while P1's terminal is
    `(1,2Bm)` as stated below. Let `h=MAX-22`. The extra five reserve values versus
    r13 are the one episode's RS/RT/L×RT death branch required by E2.

    | Quantity | Exact entry value |
    |---|---|
    | H/F/o; retained charge | `H=h,F=h-1,o=h`; retained seq h-1,h; `S_actual=S=(2,2Bm),C=0` |
    | Caps/debt/K | `I=3,Q=K=(2,2Bm),cap=(7,7Bm),B=S+(I-C)×marker=(5,5Bm)`; debt Q because `5+2+2-7=2`; fit `B+K_remaining=7`; `K_remaining=K` |
    | Members/cursors | `L=E=3,T=2,M=0`; P0/P1 active and P2 detached; P1 cursor `F-1=h-2`, P0/P2 cursors h; no anchor/credit |
    | Canonical budget | `{ high:h,remaining:22,E:3,T:2,M:0,RS:1,RT:1,L×T:6,L×RT:3,L_other×E:6 }`; equality `22=3+2+0+1+1+6+3+6` |
    | Exact sequence claims | T owns h+1/h+3; E owns h+6/h+8/h+9; materialized T-products own h+2/h+4/h+5; materialized exit product owns h+7; released T-products own h+10..h+12; released exit-products own h+13..h+17; RS/RT/L×RT own h+18/h+19/h+20..MAX |
    | Order/edge | order high 100; A owns 101/102, X owns 103/104/105, RO/RA own 106/107; `PhysicalCompaction { from_floor:h-1, through_seq:h }`, one exact quartet and complete event/selection milestones, including both claim-backed Leave suffixes |
    | Identity/cells | P0 index0 epoch `(7,3;gen7)`/K0; P1 index1 epoch `(7,4;gen5)`/K1; P2 index2 detached gen4/K2; detach cells Empty; no candidate |

    The production construction and every committing-class floor calculation are:

    | Step | H/F/o and exact cap_floor | S/C/B; debt/K_remaining | Marker/product effect |
    |---|---|---|---|
    | P0 terminal | `H=h+1,F=h,o=h`; `cap_floor=h` removes h-1 | `S=3,C=1,B=5`; debt Q/K | P1 alone is overtaken; one T-product value transfers to M and the other two claims for this terminal release |
    | append/deliver/accept P1 marker | marker `h+2`; `F=h,o:h→h+2`; append/accept `cap_floor=h` | `S=3,C=1,B=5`; debt Q/K | M `1→0`; P1 cursor becomes h+2; anchor releases, credit remains |
    | P1 terminal `(1,2Bm)` | `H=h+3,F=h+2,o=h+2`; byte-driven `cap_floor=h+2` removes h and h+1 | `S=(4,5Bm),C=3,B=(4,5Bm)`; debt `(1,2Bm)`/K | P0/P2 are overtaken; two T-product values transfer to M and P1's credit-blocked product claim releases |
    | append P0/P2 markers | seq h+4,h+5; `F=h+2,o:h+2→h+5`; each `cap_floor=h+2` | `S=(4,5Bm),C=3,B=(4,5Bm)`; debt `(1,2Bm)`/K | M `2→1→0`; neither detached owner can accept; anchors remain |
    | Leave P0 | Left h+6; `F=h+4,o=h+5`; `cap_floor=h+4` removes accepted marker h+2 and terminal h+3 | before removal `B=(5,6Bm)`; after marker-neutral removal plus terminal charge `(1,2Bm)`, `S=(4,4Bm),C=3,B=(4,4Bm)`; debt `(1,1Bm)`/K | P0 anchor releases; one exit-product transfers to a P1 marker, three release, and two future-order values remain product-backed |
    | append P1 exit marker | marker h+7; `F=h+4,o:h+5→h+7`; `cap_floor=h+4` | `S=(4,4Bm),C=3,B=(4,4Bm)`; debt `(1,1Bm)`/K | M `1→0`; detached P1 cannot accept, so its anchor remains |
    | Leave P1 | Left h+8; `F=h+4,o=h+7`; `cap_floor=h+4` | `S=(5,5Bm),C=3,B=(5,5Bm)`; debt Q/K | P1 anchor releases; the final two exit-product values release because P2 already owns a credit |
    | Leave P2 | after projecting h+8, Left h+9; `F=h+7,o=h+8`; `cap_floor=h+7` removes released-anchor markers h+4/h+5 and Left h+6 | before removal `B=(6,6Bm)`; marker removals are B-neutral, then nonmarker removal gives `S=(3,3Bm),C=1,B=(5,5Bm)`; debt Q/K | final exit has no remaining product; P2 anchor releases |
    | final projection/compaction | `H=h+9=MAX-13,F'=H+1,o=H`; mandatory `cap_floor=h+7`, `preferred_floor=H+1` | removing marker h+7 and Left h+8/h+9 gives `S=0,C=0,B=(3,3Bm)`; debt zero, edge/claims clear, `K_remaining=0`, K free | remaining credit gone; no marker omitted |

    This is the tighter construction produced by the full rules. Exactly three
    terminal-induced markers materialize; only P1's first marker can be accepted
    before its binding terminal. Exactly one exit-induced replacement marker can
    reuse P1's credit in the same transaction that compacts its accepted marker;
    the other five exit-product values release. The drain is therefore
    `2 terminals + 3 Left + 3 terminal markers + 1 exit marker = 9` records,
    ending at `MAX-13`. The thirteen still-unallocated values are exactly three
    unused T-products, five unused exit-products, and the five-value RS/RT/L×RT
    branch released when claim-backed Leaves and final compaction clear debt.
    Every promised record either materializes or is named in one atomic
    release; preferred_floor overtakes nobody.
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
31. Test-seed both complete snapshots below; caps are deliberately ample so only
    sequence reserve can refuse:

    | Quantity | Enrollment arm | Floor-trigger arm |
    |---|---|---|
    | H/F/o | `H=MAX-2,F=MAX-1` empty, `o=H` | `H=MAX-1,F=H-1`, `o=H` |
    | L/E/T/M | `0/0/0/0` | `1/1/0/0` |
    | Canonical budget | pre `{high:MAX-2,remaining:2,E:0,T:0,M:0,RS:0,RT:0,L×T:0,L×RT:0,L_other×E:0}`; rejected enrollment projects `{high:MAX-1,remaining:1,E:1,T:1,M:1,RS:0,RT:0,L×T:1,L×RT:0,L_other×E:0}` | pre `{high:MAX-1,remaining:1,E:1,T:0,M:0,RS:0,RT:0,L×T:0,L×RT:0,L_other×E:0}`; rejected floor projects the same high with `M:1`, requiring 2 |
    | Cursors/anchors/credits | none | P0 cursor `F-1=H-2` (`cursor+1=F` before trigger); none/none |
    | A/X/order | `0/0`, order high absent | `0/1`, order high 100 with ample remaining values |
    | Debt/edge | `(0,0)`, `None`, zero claims | `(0,0)`, `None`, zero claims |
    | Caps/S | `I=1,Q=(2,2Bm)`, caps `(16,16Bm)`, `S=(0,0)` | `I=1,Q=(2,2Bm)`, caps `(7,16Bm)`; two retained entries, `S=(2,2Bm),C=0,B=(3,3Bm)`, so entry `B+Q+K=7` |
    | Identity/binding | no identity | P0 index 0 detached, generation 9, secret `K9`, no binding epoch |
    | Candidates | none | none |

    In the first arm use ordinary enrollment. Its `Attached` plus resulting E/T,
    cursor-0 `M`, `L×T`, and `L_other×E` claims exceed the one post-append value, so the
    whole mint refuses `ConversationSequenceExhausted` with all nine canonical
    budget fields. Separately test-seed a
    valid near-boundary member snapshot above and make a production floor trigger newly
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
37. With `Qe=2,I=3`, reject cap 6 and use entry cap 9. The complete
    capacity checkpoints (bytes use the same Bm multiples) are:

    | Step | H/F/o and cap_floor | S/C/B | debt; K_remaining/free |
    |---|---|---|---|
    | empty public setup | `30/31/30`; `cap_floor=31` | `0/0/3` | `0; 0/2` |
    | two ordinary records, projected | `32/31/32`; each `cap_floor=31` | `2/0/5`; `5+2+2=9` | `0; 0/2` |
    | supersede P1 | `34/31/32`; mandatory `cap_floor=31` | `4/0/7` | `d=2`; `2/0`, fit `7+2=9` |
    | supersede P2, remove seq 31..32, plan three markers | `36/33/32`; mandatory `cap_floor=33` | `7/3/7` | `d=2`; `2/0`, fit `7+2=9` |
    | append each marker | H 37,38,39; `F=33,o=32,cap_floor=33` | `7/3/7`, M `3→2→1→0` | `2; 2/0` |
    | accept/project/compact full prefix | final `F=40,o=39,cap_floor=33` | `0/0/3` | `0; 0/2`, edge None |

    At setup P0/P1/P2 are indexes 0/1/2, bound at generations 7/8/9 with distinct
    explicit secrets, empty detach cells, cursors 30, `L=E=T=A=X=3`, `M=RS=RT=RO=RA=0`,
    no candidates, and order high 20. Its canonical budget is
    `{high:30,remaining:MAX-30,E:3,T:3,M:0,RS:0,RT:0,L×T:9,L×RT:0,L_other×E:6}`.
    The first supersession allocates major21 and stores the sole quartet, giving
    RS/RT/RO/RA=1 and L×RT=3; the second allocates major22 and transfers three
    old-terminal products into M at exact phase-4 tuples `(22,CompactionMarker,0..2)`.
    Thus its post budget has `high:36,E:3,T:3,M:3,RS:1,RT:1,L×T:9,L×RT:3,
    L_other×E:6`; every unshown numeric value remains above 39 and is named by that
    payload, never an implicit fixture default. The first supersession proves the
    mandatory class may fit without removal; the second
    proves removal is selected by that same class. No recovery record materializes
    in these steps, so K_remaining stays 2 until repayment clears debt. Separately
    project `B'+K_remaining>9` and prove refusal preserves the complete row.
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
    credential handoff for conversation X from C1 to C2 while unrelated R on
    C1/conversation Y receives continuously: assert no order between TCP streams,
    atomically persist P's newer epoch, retire only P/X's exact C1 dispatch slot,
    and drop every later C1/X byte before exposing C2's `AttachBound`. C1's reader
    remains live; R/Y delivery is gap-free and no lifecycle event is emitted for R.
    Finally race P/Q into an empty slot: one winner binds and the different-id loser
    gets the typed outcome; codec vectors remain conversation-only. Add fixed-width
    incarnation vectors: same-connection rotation changes only generation; a new
    connection changes ordinal; restart increments server incarnation; reuse,
    collision, and either u64 exhaustion return `ConnectionIncarnationExhausted`
    without aliasing a live/receipt/work epoch.
43. Use these complete, independent snapshots:

    | Quantity | Park-order arm | Transaction-order arm |
    |---|---|---|
    | H/F/o; L/E/T/M | `1/2/1; 1/1/1/0` | `100/101/100; 1/1/1/0` |
    | Canonical budget | `{high:1,remaining:MAX-1,E:1,T:1,M:0,RS:0,RT:0,L×T:1,L×RT:0,L_other×E:0}` | `{high:100,remaining:MAX-100,E:1,T:1,M:0,RS:0,RT:0,L×T:1,L×RT:0,L_other×E:0}` |
    | Cursors/anchors/credits | P0 cursor 1 after its Attached was projected, acked, and compacted; none/none | P0 cursor 100; none/none |
    | A/X/order | `1/1`, order high 10 | `1/1`, order high `MAX-2`, remaining 2 |
    | Debt/edge | `(0,0)`, `None`, zero claims | same |
    | Caps/S | `I=1,Q=(2,2Bm)`, caps `(16,16Bm)`, `S_actual=S=0,C=0,B=(1,Bm)`, K free | same |
    | Identity | P0 index 0, epoch `(11,2; generation 4)`, secret `K4` | P0 index 0, epoch `(11,3; generation 5)`, secret `K5` |
    | Pending rows/candidates | one `Reserved` ordinary row at park order `MAX-1`: request 24 bytes, full row 48 bytes; `N=G=4,P=1,C=D=256,B=64,R=32`, occupied rows/conversations/bytes `1/1/48` with room for the MAX allocation and next attempted row; no server candidate | no parked row and no candidate |

    For `park_order`, allocate MAX, prove the next reservation returns
    `SdkParkOrderExhausted`, then empty the set transactionally and prove only that
    counter resets. In the second snapshot, exact
    `order_remaining=A+X=2`. Optional record/enrollment/attach work returns
    `ConversationOrderExhausted` with high/remaining/reserved/resulting fields;
    the independent sequence check serializes all nine canonical budget fields and
    no mutation. A real connection fate consumes `A`, assigns `MAX-1` to its
    terminal plus every reconstructed marker, and may crash/restart before its
    already ordered candidate drains without another allocation. Exact-current
    detached Leave then consumes `X`, assigns MAX to `Left`, and leaves `A=X=0`.
    Every later new-major operation refuses, append-free work remains admissible,
    nothing wraps, and `transaction_order` never resets.
44. First prove public detached Leave normally. Then test-seed this complete
    generation-MAX/full-debt positional snapshot (all entries cost Bm):

    | Quantity | Pre-Leave | Atomic post-Leave |
    |---|---|---|
    | H/F/o; floor | `h=MAX-6,F=h-2,o=h-3`; mandatory `cap_floor=F` | `H'=h+2,F'=F,o=h-3`; `cap_floor=F`, and preferred floor remains F |
    | Budget | `{high:h,remaining:6,E:1,T:1,M:0,RS:1,RT:1,L×T:1,L×RT:1,L_other×E:0}`; T/E/RS/RT/L×T/L×RT own h+1/h+2/h+3/h+4/h+5/MAX | `{high:h+2,remaining:4,E:0,T:0,M:0,RS:0,RT:0,L×T:0,L×RT:0,L_other×E:0}`; h+3..MAX release |
    | Cursor/marker | P0 cursor h-3; delivered-unaccepted marker, anchor+credit | membership/cursor/anchor released; the retained ownerless marker credit remains until compaction |
    | Order/candidates | `A=0,X=1,RO=1,RA=1`, order high 50; old terminal tuple `(50,BindingTerminal,0)`, RO/RA/X own 51/52/53; no candidate tuple lies between 50 and 53 | terminal then Left committed; no candidate; quartet order claims release |
    | Debt/edge | debt Q; anchored DCR with one quartet, complete milestones, `K_remaining=(2,2Bm)` | debt Q; exact `ObserverProjection { through_seq:h+2 }`, quartet consumed, `K_remaining=0` |
    | Capacity | `I=1,Q=(2,2Bm),cap=(5,5Bm)`; `S=B=(3,3Bm),C=1` | recovery charge `(2,2Bm)` transfers into `S=B=(5,5Bm),C=1`; fit `5+0=5` |
    | Identity | P0 index0 detached gen MAX/Kmax; pending old terminal; empty tombstone | retired tombstone with both sequence fields; secret invalid |

    Lower generation and wrong Leave secret are `StaleAuthority`; fresh post-retire
    token is `Retired`. Crash/lost-response sees all-old or all-new and stable
    `LeaveCommitted`.
45. Use `h=MAX-6`, cap `(5,5Bm)`, `I=1,Q=(2,2Bm)` and this
    complete full-debt DCR walk; every retained record costs Bm:

    | Quantity/step | Exact state |
    |---|---|
    | Pre H/F/o and authority | `H=h,F=h-2,o=h-3`; P0 cursor h-3, detached gen8/K8; delivered-unaccepted marker h with anchor+credit; old terminal Pending |
    | Pre budget/order | `{high:h,remaining:6,E:1,T:1,M:0,RS:1,RT:1,L×T:1,L×RT:1,L_other×E:0}`; T/RS/RT/L×T/L×RT/E own h+1/h+2/h+3/h+4/h+5/MAX. Order high50; RO/RA/X own 51/52/53; complete milestones |
    | Pre capacity/edge | `S_actual=S=(3,3Bm),C=1,B=(3,3Bm)`; debt Q; anchored DCR; `K_remaining=(2,2Bm)`, fit `3+2=5` |
    | Fenced recovery | charge `r=(2,2Bm)` appends terminal h+1 and Attached h+2, accepts marker h, and uses mandatory `cap_floor=F=h-2`; `S=B=(5,5Bm),C=1`, debt Q, `K_remaining=0`, fit 5; exact `ObserverProjection { through_seq:h+2 }` |
    | Post claims | `{high:h+2,remaining:4,E:1,T:1,M:0,RS:0,RT:0,L×T:1,L×RT:0,L_other×E:0}`; new T/L×T/E own h+3/h+5/MAX and unused h+4 releases. RO is consumed, RA transfers to A, X remains |
    | C2 fate before projection | H/F/o, S/C/B, K_remaining, and OP stay unchanged; C2 becomes bounded PendingFinalization, so no phantom quartet is created |
    | Repayment | project/ack through h+2, then compact to `F'=h+3` with mandatory `cap_floor=h-2` and preferred floor h+3: `S=0,C=0,B=(1,Bm)`, debt zero, edge/claims clear, `K_remaining=0`, K free `(2,2Bm)` |

    In a separate copy of the exact pre-state, ordinary non-fenced attach at
    anchored nonzero-debt DCR returns `MarkerClosureCapacityExceeded` with no
    mutation—no exhaustion loop exists. Fenced attach and claim-backed Leave remain
    fireable; each transfers its actual charge from K_remaining. Crash every
    boundary and assert the complete pre/post payload, milestone, occupancy, floor,
    authority, and candidate state.
46. Through public operations enroll P (`E=1,T=1`), detach (`E=1,T=0`), reattach
    (`E=1,T=1`), and detach again (`E=1,T=0`); at each commit recompute every
    reserve term and prove no cycle creates, loses, or double-spends the flat exit
    claim. Then race bound Leave with connection death in both orders. Leave-first
    writes one `Left` and death sees `Retired`; death-first writes its exact-epoch
    terminal and detached Leave writes `Left` second. Inject crashes throughout and
    prove the mid-exit fate is exactly one of those two complete histories.
47. Exercise exact sequence exhaustion from these complete snapshots; all
    entries cost Bm and every refusal emits the canonical `SequenceBudget`:

    | Quantity | Arm A | Arm B | Arm C (durable B result) | Arm D |
    |---|---|---|---|---|
    | H/F/o | `MAX-1/MAX/MAX-1` empty | `h=MAX-6; F=h-4; o=F` | `h+1; F'=F+1; o=F` | `MAX-3/MAX-2/MAX-3` empty |
    | L/E/T/M | `1/1/0/0`, RS/RT 0 | `1/1/1/0`, RS/RT 1/1 | `1/1/0/1`, RS/RT 1/1 | `1/1/1/0`, RS/RT 0 |
    | Cursor; anchor/credit | P0=H; none | P0=F-1; none | unchanged; planned P0 credit | P0=H; none |
    | A/X/RO/RA; order | `0/1/0/0`, high100 | `1/1/1/1`, ample high | `0/1/1/1`; marker uses fate major | `1/1/0/0`, high100 |
    | Debt/edge | zero/None/K free | Q; `ObserverProjection { through_seq:h }`, full quartet/milestones, K_remaining 2 | Q; same OP/quartet, K_remaining 2 | zero/None/K free |
    | Caps/S | A/D: `I=1,Q=(2,2Bm),cap=(10,10Bm),S=0`; B: cap `(8,8Bm)`, `S_actual=S=5,C=0,B=6`; C: `S_actual=5,S=6,C=1,B=6` | — | — | — |
    | Identity/candidates | P0 detached gen3/K3; none | P0 bound `(20,1;gen4)`/K4; none | P0 detached; marker candidate with exact product-backed value/tuple | P0 bound `(20,2;gen5)`/K5; none |

    Arm A allocates Left at MAX. In B the canonical pre-budget is
    `{high:h,remaining:6,E:1,T:1,M:0,RS:1,RT:1,L×T:1,L×RT:1,L_other×E:0}`.
    EOF appends at h+1 and transfers the L×T marker into M. Without removal the
    tentative `B'=7` fails mandatory fit `7+K_remaining(2)>8`; removing exactly
    the first retained entry and planning its credited marker yields `B'=6`, fit
    `6+2=8`, debt Q, and exact `cap_floor=F+1`. C's marker append changes only
    S_actual `5→6`, then M `1→0`. Its canonical post-budget is
    `{high:h+1,remaining:5,E:1,T:0,M:1,RS:1,RT:1,L×T:0,L×RT:1,L_other×E:0}`.
    Detached Leave later allocates its claimed value. Arm D remains
    `remaining=3=E+T+L×T`; bound Leave appends one Left at MAX-2 and discharges
    E/T. Assert exact payload fields, cap_floor, no wrap, and no hidden default.
48. At `I=1,Q=(2,2Bm),cap=(5,5Bm)`, use a retained prefix of two
    nonmarkers plus one delivered marker: exactly
    `S_actual=S=(3,3Bm),C=1,B=(3,3Bm)`, debt Q, and
    `K_remaining=(2,2Bm)`, so fit is `3+2=5`. Before and after MarkerAck, capacity
    is unchanged. Observer-behind selects exact `ObserverProjection`; projection
    selects `PhysicalCompaction { from_floor:F, through_seq:marker }`.
    Observer-ahead selects that PCP directly. Each edge carries the same exact
    milestones/occupancy. For the compaction step, `cap_floor=F` and
    `preferred_floor=marker+1`; removing both nonmarkers and the accepted marker
    gives `S_actual:S 3→0`, `C:1→0`, `B:3→1`, debt `2→0`, edge None,
    `K_remaining:2→0`, and K free `0→2`. Crash both sides of every commit; no
    acceptance can return a capacity outcome or expose an invalid witness.
49. At the minimum `I=1,Q=(2,2Bm),cap=(5,5Bm)`, assert these exact
    recovery checkpoints:

    | Step | S/C/B | debt/edge | K_remaining/free |
    |---|---|---|---|
    | empty | `0/0/1` | zero/None | `0/2` |
    | marker plus two retained nonmarkers | `3/1/3` | Q/claim-complete marker edge | `2/0` |
    | DCR fenced attach, charge `(1,Bm)` | `4/1/4` | Q/OP or PCP | `1/0`; fit `4+1=5` |
    | full-prefix compaction (`cap_floor=F`) | `0/0/1` | zero/None | `0/2` |

    Thus positive recovery charge transfers from K into B and repayment restores
    free K only when debt clears. For the A2 preflight, first use a marker-only
    zero-debt state `S=C=B=1`; an exact-Q supersession produces `S=B=3`, debt Q,
    K_remaining 2 and fit 5, with the one quartet pre-owned. Fate before delivery
    preserves that quartet in DCR; fenced recovery of pending terminal+Attached
    transfers charge 2, reaching `B=5,K_remaining=0`, fit 5. In a refusal copy use
    `S=B=3,C=1`, debt Q, K_remaining2: projected supersession `B'=5` fails
    because `5+2=7>5`, so all old values—including K_remaining2—remain unchanged. Repeat
    bytes and every crash boundary; no admitted path invents another quartet.
50. Replace and adversarially test all thirteen sweep-classified LAW-1
    families. Preserve prior arms and distinguish the TypeScript executing timer
    loop from `«SDK-RECONNECT-DELAY-CONTRACT»`: for Rust and Gleam, a fresh
    transport-fate event may authorize exactly one reconnect transition/returned
    delay; a second `Reconnecting→reconnect` call or caller timer re-arm without a
    new fate event is rejected. Manual connect is separately TOLD. Add dedup expiry
    notification. Re-run all 21 §1 counts over the five roots at the pin, including
    `172/43/27/109` for underscore-tolerant reconnect/retry/backoff/delay.
    Independently AST/CFG-audit every clock/timeout/sleep/probe/retry/sweep/scan
    loop or callback; every unmatched wait shape fails.
51. Test-seed two complete attach arms (all entries cost Bm). The equality
    arm uses the old projected post-state: `h=MAX-5`, post `H=MAX-4`. Its complete
    pre-state is detached P0 gen8/K8, cursor h, `F=h+1,o=h`, debt zero/None/K free,
    `I=1,Q=2,cap=5,S=0,C=0,B=1`, no candidates/cells. The projected canonical
    payload is `{high:MAX-4,remaining:4,E:1,T:1,M:1,RS:1,RT:1,L×T:1,L×RT:1,
    L_other×E:0}`, requiring 7; projected order remaining is 2 while
    `A+X+RO+RA=4`. The attach therefore refuses `ConversationSequenceExhausted`
    (and the independent order arm refuses `ConversationOrderExhausted`) with all
    exact fields.

    The commit arm moves farther from both maxima:

    | Quantity | Pre | Post attach |
    |---|---|---|
    | H/F/o | `h=MAX-8,F=h+1,o=h` | `H'=MAX-7,F'=h+1,o=h`, `cap_floor=F` |
    | Budget | `{high:h,remaining:8,E:1,T:0,M:0,RS:0,RT:0,L×T:0,L×RT:0,L_other×E:0}` | `{high:MAX-7,remaining:7,E:1,T:1,M:1,RS:1,RT:1,L×T:1,L×RT:1,L_other×E:0}` |
    | Order | high `MAX-5`, X=1 | high `MAX-4`; `RO/RA/A/X` own `MAX-3/MAX-2/MAX-1/MAX` |
    | Debt/edge | zero/None/K free | debt `(1,1Bm)`; `ObserverProjection { through_seq:MAX-7 }`; full quartet, exact milestones, K_remaining `(2,2Bm)` |
    | S/C/B | `0/0/(1,1Bm)` | `S_actual=(1,1Bm),S=(2,2Bm),C=1,B=(2,2Bm)`; fit `2+2<=5` |
    | Identity/candidates | detached P0 index0 gen8/K8, empty cells | epoch `(30,1;gen9)`/K9; marker candidate tuple; M=`MAX-6`, RS=`MAX-5`, RT=`MAX-4`, T=`MAX-3`, L×T=`MAX-2`, L×RT=`MAX-1`, E=`MAX` |

    Fire OP before ack and then fate in both orders. Every post-completion selection
    consumes only stored milestones; dead-epoch DCR uses the displayed sole quartet.
    Crash exposes only complete pre/post state.
52. Validate one-shot recovery at both lifecycle phases. Choose legal u64 inputs
    with `RH(P)==R==WF`; initial one-byte R/WF reductions return only the matching
    `ParticipantRecoveryHandshakeTooLarge`. With one parked row, downward R/WF
    returns only `SdkParkingCapacityIncompatible`, preserving rows and writing
    nothing. Under the one SDK-wide domain race each of admission,
    Reserved/RetryAuthorized→InFlight plus write authorization, response deletion,
    expiry deletion, and authority invalidation against phase latch+validation+commit
    in both orders: row action first is wholly reflected in the latched phase;
    validation first completes/refuses before the row action. No order sends a row
    or partial handshake under a rejected configuration, resurrects/deletes a
    snapshotted row, or changes the selected phase outcome.
53. Replay a committed credential-attach token with byte-identical body,
    altered secret, and matching-secret changed generation/marker option; assert
    stable result, `StaleAuthority`, and `AttemptTokenBodyConflict` respectively.
    For a detach cell use exactly its encodable non-secret arms: byte-identical body
    returns stable `DetachCommitted`; same generation/token with a changed detach
    operation field returns `AttemptTokenBodyConflict`; generation mismatch returns
    `StaleAuthority`. No detach altered-secret arm exists. For a committed Leave
    tombstone retain byte-identical, matching-secret body conflict, and altered-secret
    arms. Verify verifier precedence, fixed charge/lifetime, codec normalization,
    and no duplicate commit.
54. Under one causal major create two binding terminals, including P0's terminal,
    and markers for both participants including P0. Assert exact tuple sequence:
    all `(major,BindingTerminal,index)` in index order, then every
    `(major,CompactionMarker,index)` in index order; no collision occurs for P0.
    Crash with all candidates durable and restart: reconstruction is byte-identical
    and drains the same sequence. Attempt a duplicate full tuple and classify the
    snapshot as corrupt rather than overwrite a candidate.
55. With `I=3,Qe=2`, retain the existing arm where the leaver's terminal is last:
    drain earlier terminals/markers, then the legal two-record core and exit markers.
    Add the permutation `P-own terminal major=10`, unrelated terminal major=11`,
    and P's X-major Left `=12`. Because 11 lies strictly between, never compose
    `{10,12}`: commit P's terminal at 10 separately, drain all major-10 markers,
    commit/drain 11 and its markers, then commit one-record Left at 12. Crash before
    and after P's separate terminal and before/after Left; each prefix is valid and
    exact replay's `prior_terminal_delivery_seq` names the major-10 record. Both
    arms are Q-bounded and converge to the same ordered log/tombstone.
56. Test-seed the crashed post-AttachBound state and startup result (Bm entries):

    | Quantity | Pre-recovery equality | Post-recovery equality |
    |---|---|---|
    | H/F/o; floor | `h=MAX-6,F=h-1,o=h`; mandatory `cap_floor=h` | `H'=h+1,F'=h,o=h`; `cap_floor=h` |
    | L/E/T/M; cursor | `1/1/1/0`; P0=h-2; no credit | `1/1/0/1`; same cursor; P0 credit |
    | A/X/order | `1/1`, high MAX-4; A/RO/RA/X own MAX-3/MAX-2/MAX-1/MAX | `0/1`, high MAX-3; RO/RA/X retain MAX-2/MAX-1/MAX |
    | Debt/edge | Q; `PhysicalCompaction { from_floor:h-1, through_seq:h }`; full quartet/milestones; K_remaining K | Q; partially satisfied `PhysicalCompaction { from_floor:h, through_seq:h }` (fate cannot invalidate storage); quartet retained for its dead-epoch successor; same K_remaining |
    | Budget values | T=`MAX-5`; `TerminalProduct(T)`=`MAX-4`; RS=`MAX-3`; RT=`MAX-2`; `TerminalProduct(RT)`=`MAX-1`; E=`MAX` | M=`MAX-4`; RS/RT/L×RT/E retain MAX-3/MAX-2/MAX-1/MAX |
    | Caps/S/B | `I=1,cap=5; S_actual=S=2,C=0,B=3` | `S_actual=2,S=3,C=1,B=3`; fit `3+2=5` |
    | Identity/candidates | P0 index0 epoch `(40,7;gen12)`/K12; no candidate | detached; terminal h+1; marker `(MAX-3,CompactionMarker,0)` seq MAX-4 |

    Pre canonical equality is `6=1+1+0+1+1+1+1+0`; post is
    `5=1+0+1+1+0+1+1+0`. Order is 4 then 3. Startup spends T/A, appends one
    terminal, transfers the T-product marker into M, and preserves PCP. PCP
    completion later consumes its stored selection milestone and selects DCR from
    the same quartet; it never re-endows one. Crash yields one stable recovery.

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
| SDK restart/renegotiation with parked rows present | `SdkParkingCapacityIncompatible` | scope; offending dimension (`RowSchemaBytes`, `CheckedProduct`, `RowBytesBound`, `SdkBytesBound`, `RecoverableSlots`, `ConversationRows`, `ConversationBytes`, `SdkConversations`, `SdkRows`, `SdkBytes`, `RequestBytes`, `RowBytes`, `RecoverableInterestSlots`, `RecoveryHandshakeRequestBytes`, or `RecoveryHandshakeWireFrameBytes`); exact operands; conversation id/park order when applicable; actual/limit; recovery fields and exact u128 encoded bytes | Parked-phase only; preserve every row and interest; send no row and no partial handshake; require correction. |
| Startup/first negotiation configuration shape | `ParticipantParkingConfigurationInvalid` | dimension (`RowSchemaBytes`, `CheckedProduct`, `RowBytesBound`, `SdkBytesBound`, or `RecoverableSlots`) and exact factors/product/actual/required/limit operands appropriate to it | Initial-phase only; configuration-shape precedence before RH size; refuse before traffic, rows, interests, or handshake bytes. |
| Startup/first negotiation, or restart/renegotiation with no parked row | `ParticipantRecoveryHandshakeTooLarge` | max entries P, framing bytes `RF+RC(P)`, entry bytes RE, exact u128 `RH(P)`, R, WF, dimension (`RequestBytes` or `WireFrameBytes`) | Initial-phase only; refuse participant mode/configuration before traffic; no rows are created/discarded and no chunk protocol runs. |
| Participant retention startup validation | `ParticipantRetentionCapacityInvalid` | entry/byte dimension, configured cap, checked `2Q+I×marker` required value or overflow discriminant | Refuse participant mode before traffic; no conversation state exists. |
| Participant connection-incarnation mint | `ConnectionIncarnationExhausted` | exhausted component (`ServerIncarnation` or `ConnectionOrdinal`), current u64 value, attempted server incarnation when applicable | Refuse new participant connections/startup before first participant use; preserve all durable identities and never wrap/reuse. |
| Server startup binding recovery | `BindingRecoveryCommitted` | participant/conversation, recovered binding epoch, `UncleanServerRestart` cause/prior server incarnation, assigned major, finalization (`Appended { delivery_seq }` or `Pending { admission_order }`), repayment edge | Internal durable success; exact restart replay is idempotent, receipt status is UnboundReceipt, and no wire retry/poll is created. |
| Any exact token-keyed receipt/cell/Leave tombstone with applicable authority verified but changed canonical non-secret body | `AttemptTokenBodyConflict` | token, operation, conversation id, optional presented participant/generation/marker sequence, conflict (`Generation`, `MarkerDeliverySequence`, `CanonicalBody`) | “Applicable” means matching secret proof for attach/Leave and same presented generation for non-secret detach; detach generation mismatch is instead `StaleAuthority`. Verifier-before-result/tombstone refusal discloses no stored body, secret, receipt, current slot, or `LeaveCommitted`, and mutates nothing. |
| SDK-local untokenized response loss | `RecordAdmissionUnknown` | conversation id, presented participant id/generation, operation identity `OrdinaryRecordAdmission`, park order | Atomically delete the row/release last interest when applicable; terminal ambiguity and never resend automatically. |
| First participant operation or reconnect refusal entry for a conversation on this connection | `ConnectionConversationCapacityExceeded` | conversation id, signed connection-conversation limit | No participant mutation, interest arming, or readiness promise; use a connection with a free negotiated slot. |
| Enrollment or credential attach binding attempt | `ConnectionConversationBindingOccupied` | conversation id and `presented_participant_id: Option<ParticipantId>` (`None` for enrollment); no occupying identity fields | Terminal for this attempt; no mint/receipt/rotation/replay/binding/record; same-participant rotation remains eligible. |
| Optional operation requiring an unreserved `transaction_order` major | `ConversationOrderExhausted` | conversation id, `counter: TransactionOrder`, current high/next value when any, `order_remaining`, `reserved_claims`, operation scope, required/resulting claims | Terminal for this attempt with no wrap/rebase/alias or mutation. Accepted terminal/exit obligations consume `A`/`X`; all marker candidates share their causal major, and already ordered candidates drain without another major. |
| Credential attach, receipt/status replay, detach, Leave, normal ack, marker ack, or ordinary admission | `ParticipantUnknown` | presented conversation id and participant id; presented generation, token, and requested boundary when those fields exist in that operation | Terminal semantic outcome for this attempt; connection remains open and no participant/durable state is created. |
| New detach with **no Pending cell**, Leave while a different live binding epoch exists, normal ack, marker ack, or ordinary admission after exact-token lookup misses | `NoBinding` | presented conversation id, participant id, generation | Terminal; no other binding disclosed and no state change. Detached exact-credential Leave is eligible; any Pending detach cell is classified at step 0b first. |
| Any listed participant-id operation with a live generation mismatch, or any secret-bearing operation with equal generation and wrong secret | `StaleAuthority` | presented participant id/generation and current generation; operation-specific token/boundary when present; no secret-derived detail | Terminal under invalid authority; equal generation fields classify secret mismatch without a new oracle. No state changes. |
| Any listed participant-id operation whose presented id has a tombstone | operation-specific `Retired` | presented participant id/generation, retired generation, and operation-specific token/boundary | Terminal tombstone result; no secret, binding, live cursor, or state change. |
| Any closure-checked admission | `MarkerClosureCapacityExceeded` | conversation id, entry/byte dimension, checked required, configured limit, marker capacity-credit count, anchor count, entry/byte `ClosureDebt`, repayment-edge tag, edge sequence/order-position claim counts, exact edge K_remaining occupancy claim, and K headroom | No floor, membership, credit, participant, candidate, sequence, edge, or debt mutation; terminal for every unaccepted request, with no SDK park or automatic retry. Already accepted server candidates and claimed terminal paths remain event-driven. Marker acceptance itself never has this outcome. |
| Enrollment attach | `EnrollBound` | enrollment token, participant id, generation `1`, secret, receipt/provenance deadlines, exact binding epoch | Success; persist before exposing `Bound`; replay same token only after unknown response. |
| Enrollment attach | `EnrollUnboundReceipt` | same credential fields and origin binding epoch | Persist, then fresh-token credential attach; never expose binding. |
| Enrollment attach | `EnrollmentKnown` | enrollment token, participant id, current generation | Committed identity, no secret/binding; use a valid current-or-newer credential or enter `CredentialRecoveryLost`; never remint. |
| Enrollment attach | `ReceiptExpired` | token, participant id, result/current generations, `Deadline` or `Superseded` | Exact only inside the enrollment provenance window; use a valid current-or-newer credential or enter `CredentialRecoveryLost`. |
| Enrollment attach | `Retired` | token, participant id, retired generation | Terminal; preserve identity for operator record, no attach. |
| Enrollment attach | `ReceiptCapacityExceeded` | token, `scope`, `limit` | No SDK auto-retry; surface typed admission refusal. |
| Enrollment attach | `IdentityCapacityExceeded` | token, `scope`, `limit` | Terminal for new enrollment; no participant was minted. |
| Enrollment attach | `ObserverBackpressure` | token, observer progress, backpressure epoch | Persist `AwaitingObserverProgress`; retry once after matching `ObserverProgressed` or reconnect status. |
| Enrollment attach | `ConversationSequenceExhausted` | token plus the canonical resulting `SequenceBudget` payload | Terminal for this mint; reserve check includes the flat `Left`, binding terminal, and cursor-0 marker fixed point before identity or sequence mutation. |
| Credential attach | `AttachBound` | attach token, participant id, new generation/secret, deadlines, exact binding epoch, persisted cursor, optional accepted marker sequence | Success; generation-ordered persist, then expose binding/replay. Optional marker field is present only after atomic fenced recovery; no ack parking state exists. |
| Credential attach | `AttachUnboundReceipt` | same credential fields and origin binding epoch | Persist, then fresh-token attach; no replay/ack authority. |
| Credential attach | `ReceiptExpired` / `StaleOrUnknownReceipt` / `Retired` | attach token and fields defined above | Never retry same request; preserve newer credential or enter the specified terminal state. |
| Credential attach | `StaleAuthority` | token, presented/current generations | Terminal for this request; preserve current durable credential. |
| Credential attach | `GenerationExhausted` | token, participant id, current generation `u64::MAX` | Terminal for attach with no commit/wrap; preserve current secret and enter `CapabilityGenerationExhausted`, from which only exact-token status or exact-current terminal Leave (bound or detached) is allowed. |
| Credential attach with `accept_marker_delivery_seq` | `MarkerNotDelivered` / `MarkerMismatch` | token, participant id/generation, presented marker seq, retained/delivered marker seq when non-secret, reason | Terminal for this attach attempt; no marker accept, cursor, floor, rotation, record, or binding mutation. |
| Credential attach | `ReceiptCapacityExceeded` / `ObserverBackpressure` / `ConversationSequenceExhausted` | token plus capacity/epoch fields; exhaustion adds the canonical resulting `SequenceBudget` payload and required record cost | Capacity is surfaced; backpressure enters a shared epoch; reserve exhaustion precedes rotation, binding, floor, and marker changes. Detached attach may consume closure Q but never sequence/order claims it did not reserve. |
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
| Detached `LeaveRequest` with exact current secret, at any generation | `LeaveCommitted` | leave token, participant id, presented/retired generation, no ended binding epoch, optional prior-terminal delivery seq, `left_delivery_seq` | Terminal success only: positionally compose the prior terminal only with no intervening tuple, otherwise drain/link it separately; release marker/membership/cursor, tombstone, and never bind or grant replay/ack authority. |
| `LeaveRequest` | `StaleAuthority` / `Retired` | leave token, participant id, presented/current or retired generation | Terminal; equal presented/current `StaleAuthority` means wrong secret without further detail. `Retired` returns no secret, binding, or new record. |
| `LeaveRequest` | `ObserverBackpressure` | leave token, generation, refusal epoch/progress, whether a prior terminal cell exists | Preserve valid authority/request only in the bounded SDK row and retry once after progress. Leave creates no server pending cell, whether or not an earlier binding terminal exists. |
| `MarkerAck { conversation_id, participant_id, capability_generation, marker_delivery_seq }` | `MarkerAckCommitted` / `AckNoOp` | presented participant id/generation, requested marker seq, matched persisted cursor | Success only after tuple+binding match; record abandonment or idempotent confirmation. |
| `MarkerAck` | `MarkerNotDelivered` / `MarkerMismatch` / `StaleAuthority` | presented participant id/generation, marker/requested/current sequences, reason | Terminal for this ack; cursor holds. |
| `MarkerAck` | `Retired` | presented participant id/generation, requested marker sequence, retired generation | Tombstone lookup uses the presented id; no secret, binding, or live/current cursor is present. |
| `RecordAdmission { conversation_id, participant_id, capability_generation, payload }` | `RecordCommitted` | verified/derived sender participant id, assigned delivery seq | Success only after tuple+binding match; after response loss enter SDK `RecordAdmissionUnknown` and never resend automatically. |
| Ordinary record admission | `RecordTooLarge` / `ConversationSequenceExhausted` / `MarkerClosureCapacityExceeded` | measured cap, the canonical resulting `SequenceBudget` payload, or checked closure required/limit/credits/anchors/debt edge | Terminal for this record; refusal consumes no sequence and preserves floor, identity, every exit/terminal/marker claim, credit, and debt edge. |
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
connections: assert no cross-stream order, atomically retire P's exact old SDK
dispatch slot before exposing Q's `AttachBound`, reject later old-slot delivery,
and keep the physical reader serving unrelated conversations until connection fate.
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
liveness detector. The receive brief must be implementable from explicit R12
facts, except where §7 names a genuine gate.

## 7. Named sockets and decision register

R14 distinguishes decisions made by the draft from genuine unknowns. A
**decided-by-draft** row has one candidate answer and may change only by explicit
key refusal/revision. A **genuinely-open** row names unavailable platform
evidence, an implementation linearization mechanism, a layer-above policy, or
an owner migration decision. Open rows are not permission for implementations
to choose incompatible semantics silently.

| Socket | Status | R14 answer or reason openness is genuine | Closure/refutation evidence |
|---|---|---|---|
| `«ATTACH-TRANSACTION»` | **decided-by-draft** | Attach, detach, and Leave use mandatory write-ahead tokens and one serialized participant state. The identity slot's detach cell is `Empty`, token-bearing `Pending` without a sequence, or `Committed` with the exact binding epoch/sequence; it is the sole stable-echo source until next attach/Leave. | All send/commit/response windows, same/different token while Pending, crash around Pending→Committed, echo after binding removal, stale replay after replacement death, detach loss across attach/Leave, replacement recovery, and terminal receipt tests. |
| `«RECEIPT-LIFETIME»` | **decided-by-draft** | Signed receipt TTL/count and non-secret provenance TTL/count caps bound both bodies and classifiers. Cleanup uses admitted deadlines; TTL is the maximum supported recovery outage and expiry may enter `CredentialRecoveryLost`. | Delayed generation, crash order, exact/unknown before/after provenance, cap exhaustion, composed outage, and no-sweep tests. |
| `«RETIRED-IDENTITY-BOUND»` | **decided-by-draft** | Enrollment reserves signed server/conversation identity slots; each slot owns the enrollment-token mapping for the live+tombstone lifetime, and Leave converts it to a permanent tombstone. Cap exhaustion refuses before mint, bounding churn without weakening `EnrollmentKnown`, `Retired`, or duplicate `LeaveCommitted`. | Post-GC enrollment T versus fresh U, boundary churn, no-ghost replay, lost Leave response, and pre-mint capacity refusal. |
| `«PARTICIPANT-ID-ORIGIN»` | **decided-by-draft** | Server mints participant id, generation 1, and initial secret inside tokenized enrollment; later attach proves current generation/secret and atomically increments/rotates before cursor access. External ids never authorize a cursor. | Authorization-before-cursor, collision, enrollment replay, generation ordering, rotation loss, and reconnect tests. |
| `«MEMBERSHIP-BOUNDARY»` | **decided-by-draft** | Membership begins at mint with cursor `0`, survives detach, entitles full history and offline commits, and ends only with authoritative tokenized Leave that retires id, receipts, and soft retention claim. | Late join, offline replay, Leave authority/idempotency/races, and floor-transition tests. |
| `«LEAVE-AUTHORITY»` | **decided-by-draft** | Exact current generation/secret authorizes tokenized terminal Leave bound or detached. A pending own terminal composes with Left only if no unrelated tuple lies between their majors; otherwise terminal/intermediates drain separately and one-record Left links the terminal sequence. Detached Leave grants no cursor/binding authority; v1 has no operator Leave. | Authority refusals, duplicate stable result, attach/death races, both positional permutations and crash boundaries, full debt, and generation maximum. |
| `«ACK-SHAPE»` | **decided-by-draft** | Normal `ParticipantAck { conversation_id, participant_id, capability_generation, through_seq }` and `MarkerAck { conversation_id, participant_id, capability_generation, marker_delivery_seq }` carry presented authority. At one serialized point the server looks up the presented id, matches generation plus the exact binding epoch, then validates continuity or named abandonment. Post-Leave uses the presented id's tombstone and never a live cursor. | P-ack-after-Q-bind ambiguity, same-id rotation, unknown/unbound/stale/Retired codec vectors, normal gap/regression, marker-delivery refusal, and proof abandoned payload is not marked delivered. |
| `«COMPACTION-EXIT»` | **decided-by-draft** | A pending marker shares the durable server-candidate admission order with finalizations; at append it recomputes `abandoned_after..abandoned_through` and the physical-floor snapshot. Acking abandons through that pre-marker watermark and advances to marker sequence. | Both marker/finalization orders, append-time fields, retained-suffix choice, marker loss/redelivery, later independent floor episode, concurrent-live, and post-ack flow tests. |
| `«RETENTION-UNITS»` | **decided-by-draft** | Caps start at `2Q+I×marker`: debt borrows only Q; K=Q is componentwise transferable recovery occupancy. Recovery charge moves from K_remaining into B; fit uses exact post-transfer K_remaining. One quartet and a consumable event/selection milestone bitmap back the complete successor plan; anchored DCR forbids ordinary attach. | Empty equality with one-record enrollment; exact S/B/K_remaining transfers; marker ack/compaction; fenced DCR/Leave; equality refusal and farther-max quartet; crash-atomic successor tests. |
| `«MULTI-CONVERSATION-MUX»` | **decided-by-draft** | Yes. One connection carries many conversations, demuxed by `conversation_id`; participant `stream_id = 0` and has no semantic role. | Cross-conversation interleaving and independent-cursor tests. |
| `«MULTI-BINDING-PER-CONVERSATION»` | **decided-by-draft (excluded in v1)** | At most one participant binds each `(connection_incarnation, conversation_id)`. A different-id enrollment/attach gets `ConnectionConversationBindingOccupied` containing only its presented optional id; same-id rotation is allowed. | Empty-slot race, enrollment with no presented id, Q-after-P refusal without P disclosure, same-P rotation, and conversation-only delivery codec. |
| `«LIFECYCLE-VERDICT-RECIPIENTS»` | **decided-by-draft** | Every member is entitled to every lifecycle/compaction record in total order, including while detached, unless it explicitly accepts a named abandonment after compaction broke continuity. | Three-party lifecycle/compaction races, offline replay, and explicit-abandonment tests. |
| `«LIFECYCLE-OBSERVER-DELIVERY»` | **decided-by-draft** | The log is sole completed lifecycle history and observer progress hard. Signed per-conversation and SDK-wide conversation/row/full-byte caps plus request/row maxima bound parking; durable first-row interest slots make all Awaiting conversations armable. `Reserved` restart, checked u64 order, cohort marking, authority loss, epoch monotonicity, and all-dimension renegotiation are explicit. Acks never park. | Independent row/byte/global cap hits, interest-slot recovery, Reserved crash, per-row downward incompatibility, near-max order, epoch races, credential loss, and no orphan/ack/poll. |
| `«SUPERSESSION-FENCE»` | **decided-by-draft** | Every credential-bearing successful attach checked-increments generation, rotates the secret, and creates a distinct immutable binding epoch even on one physical connection. Final outbound construction drops old-epoch work behind the response barrier; stale proof is a no-record refusal. | Two-holder and same-connection rotation races, single handoff pair, exact-epoch receipt recovery, queued-work barrier, and stale-proof zero-record tests. |
| `«RESUME-COMMENT-SERVER-MISMATCH»` | **genuinely-open** | Owner must choose separate legacy subscription recovery versus version/deprecate/delete it for participant attach. Outline governs over the earlier tear's preference to decide now; R12 nevertheless makes “comment-only fix” insufficient. | Owner ruling covering types, cursor owner, starting convention, persistence, release boundary, and removal/compatibility tests. |
| `«MEMBERSHIP-EVENT-SOURCE»` | **genuinely-open implementation dependency** | External behavior is fixed: membership deltas must be pushed and shutdown must wake the source. The selected membership backend's non-poll event API is not yet chosen. | Backend event subscription/callback with ordered delta and explicit shutdown tests; delete `PollLoop`, `poll_once` cadence, and sleep. |
| `«HEALTH-ACCEPT-SHUTDOWN-WAKE»` | **genuinely-open implementation dependency** | External behavior is fixed: health accept blocks/uses readiness and explicit shutdown interrupts it. The concrete cross-platform wake mechanism is not selected. | Per-target accept/shutdown race tests with no WouldBlock sleep or shutdown-flag sampling loop. |
| `«SHUTDOWN-DRAIN-NOTIFICATION»` | **genuinely-open implementation dependency** | External behavior is fixed: connection exit updates a completion primitive raced against one admitted deadline; force-close uses the same notification. Concrete supervisor API is not selected. | Exit/drain notification API, crash/force-close/deadline races, and deletion of reap/count/sleep loops. |
| `«CHANNEL-REPLY-EVENT-RACE»` | **genuinely-open implementation dependency** | External behavior is fixed: a command wait parks on the reply, pushed process exit, and one admitted command deadline. The scheduler-to-waiter exit-notification primitive is not selected. | Reply/exit/deadline races in every order; delete `LIVENESS_POLL`, repeated `recv_timeout`, and process-table sampling. |
| `«SDK-PUSH-READER-SHUTDOWN-WAKE»` | **genuinely-open implementation dependency** | External behavior is fixed: the push reader blocks on/readiness-waits for socket input and explicit local shutdown interrupts that wait. The portable wake primitive is not selected. | Silent-socket data/EOF/error/shutdown races and join tests; delete `READER_POLL_TIMEOUT`, timeout-as-`None`, and stop-flag rechecks. |
| `«SDK-SUBSCRIPTION-READER-SHUTDOWN-WAKE»` | **genuinely-open implementation dependency** | External behavior is fixed: the subscription reader blocks on/readiness-waits for socket input and explicit local shutdown interrupts that wait. The portable wake primitive is not selected. | Silent-socket data/EOF/error/shutdown races and join tests; delete `READER_POLL_TIMEOUT`, timeout-as-`None`, and stop-flag rechecks. |
| `«DURABILITY-BRIDGE-WAKE»` | **genuinely-open implementation dependency** | External behavior is fixed: asserted synchronous backends receive one poll and loud Pending error, or a real executor parks and honors the waker. Backend integration selects which shape. | Ready/one-Pending/real-wake cases; delete `MAX_POLLS`, `NoopWaker`, repeated `poll`, and `yield_now`. |
| `«PUSH-REPLY-AWAITER-EVENT-RACE»` | **genuinely-open implementation dependency** | External behavior is fixed: the SDK call site waits once on reply, connection fate, and one admitted deadline. A one-shot internal quantum may remain only as mechanism, never a blessed caller re-arm loop. | All race orders and cancellation/drop; delete indefinite `receive` reinvocation contract and repeated short-poll tests. |
| `«SDK-SUBSCRIPTION-SETUP-DEADLINE-RACE»` | **genuinely-open implementation dependency** | External behavior is fixed: handshake/Subscribe setup races socket input with one total deadline. Portable readiness/deadline composition is implementation-owned. | Input/deadline/EOF/error races; delete 100ms `read_one_frame` re-arm and `Instant::now` loop sampling. |
| `«SDK-RECONNECT-TIMER-LOOP»` | **genuinely-open implementation dependency** | Reconnect is armed by a transport/connection-fate event or explicit manual request; failure waits for another event and never timer-retries. Concrete platform event wiring is implementation-owned. | All event/failure/manual races; delete backoff loop, default-infinite attempts, sleep, and setTimeout. |
| `«SDK-RECONNECT-DELAY-CONTRACT»` | **genuinely-open implementation dependency** | Rust/Gleam delay computation is not itself a wait, but a returned delay is valid only for the one attempt armed by a fresh transport-fate event. Repeated Reconnecting→reconnect or timer re-arm without another event is illegal. Concrete event/API wiring is implementation-owned. | Fresh-event/attempt/delay and manual-connect races in both SDKs; remove the public repeated-delay re-arm contract while preserving pure bounded calculation if useful. |
| `«DEDUP-EXPIRY-EVENT-SOURCE»` | **genuinely-open implementation dependency** | Dedup expiry is keyed by admitted deadline or mutation notification, never periodic full-store scan. Deadline-queue/storage integration is implementation-owned. | Expiry/mutation/crash races; delete sweep interval and timer-driven `sweep_once` scan. |
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

Extract every guillemet-delimited socket name and require each result to appear
in this register or be one of the two SDK gate names explained here. For decided rows, search for
one normative answer in §§2-6. For genuinely-open rows, require the named owner
or evidence and refuse implementation where the gap is load-bearing. Search all
timers, wakes, loops, callbacks, and tasks—including all thirteen §1 families and
startup recovery—and prove each is driven by admitted work, reply/data readiness, kernel
connection fate, explicit shutdown/process exit, one admitted deadline, or an
existing domain event rather than change detection.

### R14 define-what-you-name and coherence audits

The drafter ran the standing name audit over this complete artifact. Extraction
between Unicode U+00AB/U+00BB yields 38 unique guillemet names: 36 exact register
rows plus exactly the two §6 SDK gate names permitted above; there is no
unregistered socket. The typed-name audit requires every declaration-shaped use
to resolve to its normative field/phase/outcome table. The r14 delta resolves
u64/u128 RF/RC/RE/WF/RH and lifecycle phase in R-A4; B/K_remaining, the marker-
backing enum, canonical SequenceBudget, RS/RT/RO/RA, successor event/selection
milestones, edge tags, and decreasing measures in R-A2/R-C2/R-C4; the non-secret
detach verifier in R-C0; and every reachable outcome/dimension in R-D1.

Shared concepts have one definition and citations elsewhere:

| Shared concept | Sole normative definition |
|---|---|
| Binding epoch | R-A1 `BindingEpoch`; R-C0/R-C1/R-C5/startup recovery cite it. |
| Reserved counter capacity | R-A2's accepted-obligation claim principle; R-C2 and R-C4 apply it. |
| Marker capacity credit | R-C4 from planning through physical compaction; cases 45/48 cite it. |

The exhaustive-outcome pass searched every `returns`, `return`, `outcome`, and
new discriminant. `ParticipantParkingConfigurationInvalid` has five initial-phase
triggers and case-25 arms; parked equivalents use explicit dimensions. Reconnect
delay has the fresh-fate trigger and case 50. Every trigger has an outcome and no
new outcome lacks a trigger/case. Detach has no unencodable secret arm.

Acceptance references remain contiguous 1–56. Every test-seeded boundary is
enumerated here: cases **21, 26, 31, 43, 44, 47, 51, and 56**. Each names every
quantity read by its transition. Re-derived cases 21/25/26/31/37/43/44/45/47/48/
49/51/56 were arithmetically re-run against the displayed pre/post formulas; every
floor-moving step states cap_floor. The row-by-row invalidator, guillemet-socket,
and define-what-you-name audits find no dangling tag, outcome, case, section,
quantity, backing class, quartet, or superseded minimum. Idle cost is closed, and
the 21-count LAW-1 sweep plus case 50 covers lexical and structural waits.
---

**Gate posture:** DESIGN DRAFT R14. All earlier-round decisions remain in force.
R14 closes E1–E11: transferable recovery occupancy; one consumed quartet;
product-backed markers; exhaustive parking configuration outcomes; one class-
selected cap-floor envelope; SDK-wide row serialization; non-secret detach;
Rust/Gleam reconnect-delay retirement; canonical sequence payloads; complete,
recomputed fixtures; and no partial handshake wording. The socket register
separates decisions from real implementation dependencies.
Reviewer key plus Hermes Crumpet's liminal domain-owner key are still required;
until both turn, this document is not ratified and grants no implementation
authority.
