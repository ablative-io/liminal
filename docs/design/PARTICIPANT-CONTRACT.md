# Participant-domain wire/server contract — design draft R1

**Status: DRAFT — decides nothing.** This document parks at the two-key gate:
reviewer-of-record plus the liminal domain-owner pass (Hermes Crumpet). I draft;
I do not decide. Every recommendation below is a proposal with an explicit
refutation target.

## 0. Provenance, authority, and laws

### 0.1 Provenance

This draft is pinned to liminal commit
`ce8814daa748373d8ffc66b3ff1664f1697a5f4e` (the result of `git rev-parse HEAD`
in the assigned checkout). Every repository citation in this document was
opened and re-verified at that commit. A bare `path:line` below therefore means
that path and line at that named commit; no citation is inherited from the
outline.

Origin: SDK-receive dispatch blocked-upstream, norn session `256a81a0`, envelope
`claude-dev-sdk-receive.8rss9K`. That worker reached the transport-versus-
participant-domain gap, stopped at the brief's valve, and produced an
assertion-by-assertion evidence table. That session is the **RESUME VEHICLE**
once this contract lands.

Corrected claim, with Cally's pen and Waffles' tear both owned: the park-flip
chain built the server side of symmetric **transport**, not a participant-domain
contract. At the pinned commit that transport parks on readiness, has wakeable
bounded inboxes, and owns correlated push-reply slots
(`crates/liminal-server/src/server/connection/process.rs:193-212`,
`crates/liminal-server/src/config/types.rs:226-236`,
`crates/liminal-server/src/server/connection/supervisor.rs:224-240`). Those are
real 0.2.4 surfaces; the three crates identify themselves as 0.2.4
(`crates/liminal/Cargo.toml:3`, `crates/liminal-sdk/Cargo.toml:3`,
`crates/liminal-server/Cargo.toml:3`). They do not by themselves specify
participant identity, participant cursor ownership, replay, or participant
lifecycle verdicts (`crates/liminal/src/protocol/frame.rs:381-444`,
`crates/liminal-server/src/server/connection/notifier.rs:46-52`,
`crates/liminal-sdk/src/remote/tcp/mod.rs:190-207`).

### 0.2 Binding laws

**LAW 1 — NO-POLLING (Tom, 2026-07-13 01:16Z, design constant).** No
application-layer poll loops anywhere in the product. Crash detection stays
event-driven: linked-EXIT in-VM, connection-fate cross-process. The heartbeat
option for no-FIN liveness is dead. The no-FIN bound is designed **inside**
event-driven machinery, not around it. If a design has a timer whose job is
"check whether something changed," it is wrong: redesign it to be **TOLD**.
This law binds §§2-5, especially §3.

**LAW 2 — BELIEVED STATE IS NOT CITABLE STATE (tear-side law).** Every sentence
asserting the state of the liminal codebase must cite a verified file and line
at the named commit above, or must be a named socket in §7. This law binds the
whole document.

**LAW 3 — TWO-KEY GATE.** Nothing here is decided until the reviewer key and
Hermes Crumpet's liminal domain-owner key both pass it. “Recommend” always means
“candidate for those keys,” never “implementation authority.”

**Silence attack / provenance acceptance.** A reviewer refutes this section by
finding any repository-state sentence without a pinned citation, any citation
that does not support its sentence at the pinned commit, or any wording that
claims a decision before both keys turn.

## 1. The verified gap

The audit claims were re-verified rather than copied:

| Claim | Verified evidence at `ce8814d…` | Contract consequence |
|---|---|---|
| Unregistration carries no cause. | `ConnectionNotifier::on_worker_unregistered(&self, pid: u64)` carries only `pid` (`crates/liminal-server/src/server/connection/notifier.rs:46-52`). Both ordinary `finish` and crash removal call the same cause-free notifier (`crates/liminal-server/src/server/connection/supervisor.rs:1550-1554`, `crates/liminal-server/src/server/connection/supervisor.rs:1593-1596`, `crates/liminal-server/src/server/connection/supervisor.rs:1623-1631`). | Clean leave and connection failure collapse at the application seam; §2 proposes typed cause plumbing. |
| In-VM participant liveness is deliberate event doctrine, not a missing heartbeat. | The conversation resource says participant crash arrives through a trapped linked-EXIT notifier “never by polling, sleeping, or a heartbeat” (`crates/liminal-server/src/server/connection/conversation.rs:3-7`), and its waiter parks on that notifier until the EXIT handler wakes it (`crates/liminal-server/src/server/connection/conversation.rs:43-49`). | §3 must stay inside linked-EXIT/readiness/connection-fate events. |
| TCP transport resume is refused. | `TcpRemoteTransport::resume` returns `SdkError::Protocol` (`crates/liminal-sdk/src/remote/tcp/mod.rs:190-207`). Its comment says there is no resume frame and that this transport lacks the mapping needed to re-drive Subscribe (`crates/liminal-sdk/src/remote/tcp/mod.rs:195-200`). | Participant resume needs the server and wire contract in §§4-5 before an SDK API can honestly implement it. |
| The existing correlated push has no participant envelope. | `Frame::Push` is exactly `{ flags, stream_id, correlation_id, payload }`; its payload is opaque application bytes (`crates/liminal/src/protocol/frame.rs:381-394`). It contains no conversation id, sender participant id, delivery sequence, or lifecycle verdict. | §5 wraps opaque bytes in a participant-domain envelope without repurposing Push. |
| A nearby delivery sequence is narrower than this contract. | `Frame::Deliver` already carries `delivery_seq`, but documents it as per-subscription and as an anchor for future ack/resume (`crates/liminal/src/protocol/frame.rs:429-444`); unsubscribe removes the counter so a re-subscribe restarts at 1 (`crates/liminal-server/src/server/connection/apply.rs:432-443`). | R-C2 proposes a distinct per-conversation sequence that survives connection/subscription incarnations. |
| Existing caps have a signed §5 shape. | `ServerConfig` embeds defaulted `ServicesConfig` and `LimitsConfig` (`crates/liminal-server/src/config/types.rs:32-44`); `LimitsConfig` defines named per-scope hard caps with signed defaults (`crates/liminal-server/src/config/types.rs:193-203`), rejects zero values by field name (`crates/liminal-server/src/config/types.rs:257-300`), and supplies explicit defaults (`crates/liminal-server/src/config/types.rs:304-316`). | The no-FIN bound and history retention must extend this pattern, not create an unrelated configuration species. |

**Prominent verification discrepancy — `«RESUME-COMMENT-SERVER-MISMATCH»`.** The
resume comment says a re-issued Subscribe causes replay from a durable log
(`crates/liminal-sdk/src/remote/tcp/mod.rs:195-198`), but the server Subscribe
path constructs a fresh subscription and returns its id without accepting a
cursor (`crates/liminal-server/src/server/connection/apply.rs:349-421`), while
re-subscribe resets delivery sequence to 1
(`crates/liminal-server/src/server/connection/apply.rs:432-443`). The comment and the
server shape do not establish the same contract. This draft does not paper over
the mismatch; §7 parks it by name.

A second verified boundary matters for evolution: the server negotiates only
protocol 1.0 (`crates/liminal-server/src/server/connection/apply.rs:21`), while
the codec preserves unknown discriminants as `Frame::Unknown`
(`crates/liminal/src/protocol/codec/known.rs:44-53`). Preservation is not consent
to send a participant frame to a 0.2.4 client; R-D2 therefore requires an
explicit negotiated gate.

**Silence attack / gap acceptance.** Refute this section by identifying an
existing frame that jointly carries participant identity, conversation identity,
a durable per-participant cursor, replay position, and lifecycle verdict, or by
showing server Subscribe consumes a resume cursor. A nearby field with narrower
scope does not close the gap.

## 2. Section (a) — typed close cause through finalization

### Proposed contract

**R-A1 — Typed cause.** Introduce `CloseCause` (name subject to domain-owner
review) and carry it through every connection/participant finalization path to
the notifier. The minimum proposed taxonomy is:

- `CleanDeregister`: an explicit participant detach or clean protocol
  `Disconnect`; the frame dispatcher already classifies `Disconnect` as Close
  (`crates/liminal-server/src/server/connection/apply.rs:36-52`) and the process
  then calls `finish` with a normal exit (`crates/liminal-server/src/server/connection/process.rs:280-289`).
- `ConnectionLost`: transport EOF/FIN without participant detach, or a kernel
  read/write failure. EOF and read error are already distinct match arms
  (`crates/liminal-server/src/server/connection/process.rs:238-269`), and fatal
  write failure is already reported by `OutboundWriter::drain`
  (`crates/liminal-server/src/server/connection/outbound.rs:174-193`,
  `crates/liminal-server/src/server/connection/outbound.rs:219-235`).
- `ProcessKilled`: a trapped linked-EXIT or a locally known supervisor
  termination. Conversation actors already expose the trapped EXIT event
  (`crates/liminal-server/src/server/connection/conversation.rs:173-186`), but
  the external connection-process reap path cannot retrieve beamr's private exit
  reason (`crates/liminal-server/src/server/connection/supervisor.rs:1643-1658`);
  `«EXTERNAL-EXIT-REASON»` therefore blocks a complete mapping.
- `ProtocolError`: decode or protocol-state refusal that terminates the
  connection. Decode errors are returned distinctly before frame application
  (`crates/liminal-server/src/server/connection/process.rs:695-710`) and the
  caller marks that route as an error
  (`crates/liminal-server/src/server/connection/process.rs:291-296`).

The enum may carry typed detail, but no catch-all may erase one of those four
classes. EOF is not proof of a clean participant deregistration; only an
explicit domain detach/clean protocol close earns `CleanDeregister`.

**R-A2 — Plumbing, not detection.** Add no detector, sample, sweep, or liveness
timer. Each cause is assigned where an existing FIN/EOF, linked-EXIT,
read/write error, protocol error, or explicit close event is received, then
travels unchanged through removal and notifier delivery
(`crates/liminal-server/src/server/connection/process.rs:238-296`,
`crates/liminal-server/src/server/connection/conversation.rs:173-186`,
`crates/liminal-server/src/server/connection/supervisor.rs:1550-1596`). This is
event-driven end to end and is bound by LAW 1.

**R-A3 — Compatibility position.** Two shapes are available: (1) widen
`on_worker_unregistered(pid)` to `on_worker_unregistered(pid, cause)`, forcing
every implementation to choose deliberately; or (2) add a parallel cause-aware
method with a default implementation that calls the old method. **Recommendation:
widen the trait method in the next version.** A compile failure inventories every
implementer, whereas a default can silently discard the very distinction this
section exists to create. Refutation target: demonstrate a supported external
implementer population whose source break cannot be version-gated, or a
cause-preserving parallel method whose default cannot collapse information.

### Silence-attacking acceptance frame

A test registers equivalent workers, has one deregister explicitly, kills the
other with SIGKILL, and asserts different observed causes. Every finalization
route (explicit detach/Disconnect, EOF, read error, write error, protocol error,
linked EXIT, local force-close, external termination, and shutdown) gets a
cause assertion. A reviewer refutes R-A1 by finding any route that reaches the
notifier as `Unknown`, no cause, or the wrong clean/failure class.

## 3. Section (b) — no-FIN liveness inside event-driven crash detection

### Constraint first

**NO-POLLING:** no application-layer poll loops anywhere in the product. Crash
detection stays event-driven — linked-EXIT in-VM, connection-fate
cross-process. The heartbeat option for no-FIN liveness is dead. The no-FIN
bound is designed inside event-driven machinery, not around it. A timer whose
job is “check whether something changed” is wrong; the application must be
told.

A cable pull, hard power loss, or peer stopped forever can produce no immediate
application event. The proposed bound therefore comes from kernel connection-
fate machinery, not an application observer.

### Proposed contract

**R-B1 — Kernel connection fate.** Enable `SO_KEEPALIVE` on each participant TCP
socket and configure per-socket idle, probe interval, and probe count. The
kernel owns probe scheduling; userspace waits on the existing socket-readiness
path (`crates/liminal-server/src/server/connection/process.rs:193-212`) and is
told by EOF/HUP/read error/write error
(`crates/liminal-server/src/server/connection/process.rs:238-269`). This qualifies as Tom's
“connection-fate cross-process,” not application polling. Failure to apply a
required option is a startup/accept refusal, not a silent unbounded fallback.

**R-B2 — Named signed parameters.** Propose three `[limits]` fields:
`participant_tcp_keepalive_idle_ms`,
`participant_tcp_keepalive_interval_ms`, and
`participant_tcp_keepalive_probe_count`. Each is nonzero, has a certifying-pair-
signed default, is validated by field name, and is applied as a socket option.
That mirrors the verified hard-cap/default/refusal pattern
(`crates/liminal-server/src/config/types.rs:193-203`,
`crates/liminal-server/src/config/types.rs:257-316`). Exact defaults
and platform option mapping remain `«NO-FIN-KERNEL-BOUND»` and
`«KEEPALIVE-PORTABILITY»`; this draft refuses to manufacture numbers.

**R-B3 — Honest bound.** The advertised no-FIN detection bound is the worst-case
bound delivered by the configured kernel options on a supported platform,
including platform granularity and retry semantics. A fully silent peer is
undetectable until the kernel bound expires. The contract promises no tighter
bound. If a platform cannot expose or verify the signed bound, participant mode
is refused there rather than silently claiming it.

**R-B4 — Write-path fate.** Any ordinary send remains an immediate additional
connection-fate event: the outbound writer turns a zero write or unrecoverable
kernel error into a fatal result (`crates/liminal-server/src/server/connection/outbound.rs:174-193`,
`crates/liminal-server/src/server/connection/outbound.rs:219-235`), and the connection process tears down on that result
(`crates/liminal-server/src/server/connection/process.rs:169-182`). A send is
not generated merely to test liveness.

**R-B5 — Cause integration.** Kernel expiry, EOF/HUP, and read/write errors flow
to R-A1 as `ConnectionLost`; linked-EXIT flows as `ProcessKilled`. No secondary
liveness score or timeout races the authoritative event.

### Explicit non-goals

- No application-layer heartbeat frames or heartbeat sweep loop.
- No sweep/scan loops.
- No wall-clock liveness scoring.

### Silence-attacking acceptance frame

With the three signed socket parameters set to test values, a SIGSTOP-forever
or no-FIN network fault must produce `ConnectionLost` no earlier than the
platform's defensible lower edge and no later than its documented worst-case
kernel bound plus scheduler/test tolerance. The same test audits thread/timer
inventory: there is no application timer, periodic task, scan, synthetic send,
or wake whose job is “check whether something changed.” A reviewer refutes this
section by finding any tighter promise than the named kernel bound, any silent
unsupported-platform fallback, or any application timer/poll/sweep performing
change detection.

## 4. Section (c) — participant registration and server-backed cursor/replay

### Proposed contract

**R-C1 — Identity at attach.** A version-gated participant attach/join frame
carries `participant_id` and `conversation_id`. The server atomically registers
the `(participant, conversation) ↔ connection incarnation` binding before
accepting deliveries. Detach and teardown remove that incarnation through
R-A1, preserving its typed cause. Who mints the id remains
`«PARTICIPANT-ID-ORIGIN»`.

**R-C2 — One per-conversation order.** The server is the sole writer of a
strictly increasing, gap-free `delivery_seq` for each conversation. Sequence is
assigned before retention/admission and defines one total order across senders
and lifecycle verdicts in that conversation. It is not the existing
per-subscription counter, whose scope and restart behavior are narrower
(`crates/liminal/src/protocol/frame.rs:429-444`,
`crates/liminal-server/src/server/connection/apply.rs:432-443`). Sequence
exhaustion is a typed terminal refusal, never wraparound.

**R-C3 — Server-backed cursor.** The server stores one cumulative cursor per
`(conversation_id, participant_id)`. Cursor advances only after a participant
acknowledges contiguous delivery through sequence N; an ack with a gap or a
regression is refused. On reconnect, successful attach resumes from `cursor +
1`, not from connection-local state.

Two ack shapes are available: piggyback the cumulative cursor on the next
participant-to-server frame, or send an explicit ack frame. **Recommendation:
use an explicit cumulative `ParticipantAck { conversation_id, through_seq }`.**
It advances the durable cursor independently of whether the participant has
outbound application traffic and is itself an event, not a timer. The server
persists the cursor before confirming ack acceptance. Refutation target: prove
that every participant is guaranteed a timely outbound frame on which a
piggyback cannot be lost or delayed, while preserving a bounded replay window.
The gate still owns the choice under `«ACK-SHAPE»`.

If delivery reached the SDK but its ack did not reach the server, replay may
repeat that sequence on the transport. The proposed SDK sequence guard must
suppress it before application delivery; “zero duplication” means zero
duplicate application delivery, not a false promise that TCP/reconnect never
retransmits.

**R-C4 — Bounded retention and honest compaction.** Retain the ordered
conversation log behind a nonzero §5-style signed cap. **Recommendation: sign
both `max_retained_conversation_bytes` and
`max_retained_conversation_entries`, and compact when either cap is reached.**
Bytes bound aggregate memory/storage while entries bound tiny-message metadata;
refutation target: provide a single unit that bounds both failure modes without
an implicit unlimited dimension. The final choice remains
`«RETENTION-UNITS»`. These fields extend `LimitsConfig`'s verified named-default,
nonzero-validation pattern (`crates/liminal-server/src/config/types.rs:193-203`,
`crates/liminal-server/src/config/types.rs:239-316`).

When a participant cursor precedes the retention floor, attach/replay opens with
an in-band `HistoryCompacted { requested_after, retention_floor }` verdict. It
must not silently jump the cursor, fabricate continuity, or emit later messages
as though the gap did not exist. Recovery policy after that verdict belongs
above this wire/server contract.

**R-C5 — Replay then live is one event-driven stream.** At attach, the server
establishes a sequencer watermark and subscribes the connection to later
sequence events without a race; it emits retained `(cursor, watermark]` in
order, then hands off exactly once to live events `> watermark`. Live events
arriving during replay are queued under the same retention/admission bounds,
not discovered by a scan. The live half uses the existing READY event
vocabulary, in which a marker causes one full slice
(`crates/liminal-server/src/server/connection/process.rs:648-658`) and a quiet
connection parks after an armed final probe
(`crates/liminal-server/src/server/connection/process.rs:193-212`). The atomic
watermark/binding mechanism remains `«REPLAY-LIVE-CUTOVER»`; no prose silently
pretends the interleaving is already implemented.

### Silence-attacking acceptance frame

1. Kill a participant after it receives N but before its ack status is known;
   reconnect it and prove application observation is exactly sequences
   `1..M`, with no loss and no duplicate after the sequence guard.
2. Race a new live send against replay's final retained record; prove every
   sequence appears exactly once and in order across the handoff.
3. Put the stored cursor below the retention floor; prove the first replay
   outcome is `HistoryCompacted`, never a later delivery with a silent gap.
4. Hold the system otherwise silent; replay advances only because attach,
   storage completion, ack, and live-delivery events fire—never because a timer
   checks for work.

A reviewer refutes this section by producing any reconnect/cutover interleaving
that drops or duplicates an application-visible sequence, any cursor advance
without an acknowledged contiguous delivery, or any compaction gap without the
verdict.

## 5. Section (d) — participant envelope on the wire

### Proposed contract

**R-D1 — Envelope.** Every participant delivery carries a versioned envelope
with `conversation_id`, `sender_participant_id`, the server-assigned
`delivery_seq` from R-C2, a lifecycle-verdict slot, and opaque `payload`. The
verdict vocabulary includes `HistoryCompacted` and, where the affected
participant/conversation can still be addressed, R-A1 close causes. The exact
recipient/fan-out rule for a departed participant remains
`«LIFECYCLE-VERDICT-RECIPIENTS»`.

**R-D2 — Wire evolution.** Two options exist: extend `Frame::Push` under a
versioned layout, or add participant frame types beside Push. **Recommendation:
add version-gated participant attach/delivery/ack/verdict frame types and leave
Push byte-for-byte untouched.** Push is a correlated request/reply transport
whose verified fields are only flags, stream, correlation, and opaque payload
(`crates/liminal/src/protocol/frame.rs:381-405`); participant replay has a
different lifetime and ordering key. Refutation target: demonstrate a Push
extension that an unmodified 0.2.4 loader cannot misparse and that cannot
confuse push correlation with participant delivery sequence.

The server must not emit the new frames until handshake negotiation explicitly
selects their protocol version/capability. The pinned server supports protocol
1.0 (`crates/liminal-server/src/server/connection/apply.rs:21`), and the codec's
ability to retain an unknown frame as `Frame::Unknown` is only parsing behavior
(`crates/liminal/src/protocol/codec/known.rs:44-53`), not a participant-feature
agreement. A 0.2.4 connection therefore must continue to receive only its
existing layouts.

**R-D3 — Correlation remains correlation.** `correlation_id` remains the Push ↔
PushReply key; PushReply echoes it so the server resolves the one-shot slot
(`crates/liminal/src/protocol/frame.rs:381-405`,
`crates/liminal-server/src/server/connection/supervisor.rs:224-233`). The
participant envelope neither reuses delivery sequence as correlation nor
changes R1(vi)'s push-reply shape. A participant application reply, if later
specified, needs an explicit relationship rather than overloading either key.

**R-D4 — Payload opacity.** Payload remains uninterpreted application bytes.
The envelope wraps those bytes; it does not schema-tize, inspect, or derive
participant facts from them. This preserves the verified opacity of Push's
payload while moving participant facts into typed wire fields
(`crates/liminal/src/protocol/frame.rs:381-394`).

### Silence-attacking acceptance frame

From captured wire bytes alone, an SDK `receive()` must attribute every
participant delivery to exactly `(conversation_id, sender_participant_id,
delivery_seq)`, distinguish payload from lifecycle verdict, and observe
`HistoryCompacted`/close verdicts in band. Mixed-version tests prove a 0.2.4
connection never receives the new discriminants and that Push/PushReply codec
vectors remain byte-identical. A reviewer refutes this section by finding any
required receive fact available only from an out-of-band map, inferred payload
schema, connection-local counter, or reused correlation id.

## 6. Interactions and non-goals

### Interactions

**R-I1 — Resume becomes buildable, not designed at the SDK surface.** The
transport's resume method is a typed refusal because it has no resume frame or
retained re-subscribe mapping (`crates/liminal-sdk/src/remote/tcp/mod.rs:190-207`).
R-C1/R-C3 plus R-D1/R-D2 provide the attach, cursor, replay, and verdict facts
on which a later SDK implementation can be built. This document does not choose
that SDK method's types or ergonomics.

**R-I2 — Cally/F-3a gate.** The checked-in receive brief assigns the SDK receive
lane to Cally and parks merge at a reviewer-plus-Hermes two-key gate
(`docs/design/SDK-PARTICIPANT-RECEIVE.md:3-10`,
`docs/design/SDK-PARTICIPANT-RECEIVE.md:35-41`). Its receive contract
requires conversation, sender, payload, and sequence
(`docs/design/SDK-PARTICIPANT-RECEIVE.md:96-110`) and its resume
contract requires reattach from a server cursor with a typed compaction outcome
(`docs/design/SDK-PARTICIPANT-RECEIVE.md:128-136`). Consequently, the version that carries that receive path must not
claim those guarantees before this wire/server contract passes and lands.
However, the outline's literal `«LIMINAL-SDK-VERSION»` name is not present in
that checked-in note; `«LIMINAL-SDK-VERSION-GATE-NAME»` records the discrepancy
in §7.

**R-I3 — Resume vehicle.** After both keys pass and the contract lands, norn
session `256a81a0` / envelope `claude-dev-sdk-receive.8rss9K` is the designated
resume vehicle. Until then it remains blocked upstream.

### Non-goals

- No implementation.
- No schedule or release promise.
- No SDK API/ergonomics design.
- No application payload schema.
- No application heartbeat, sweep, scan, or wall-clock liveness score.
- Wire and server semantics only.

### Silence-attacking acceptance frame

A reviewer refutes this section by finding an SDK API commitment, schedule,
implementation claim, or hidden liveness mechanism in this document; or by
showing that R-C/R-D still leave the receive brief dependent on an unstated
server fact. Gate-name drift must remain explicit until its owner names it.

## 7. Named sockets and open questions

No socket below is silently resolved by recommendation. The two-key gate must
close it explicitly or leave it open in the next revision.

| Socket | Open question / required owner decision | Refutation or closure evidence |
|---|---|---|
| `«PARTICIPANT-ID-ORIGIN»` | Who mints participant ids: server, SDK, or the aion layer above? What prevents collision and unauthorized reuse? | Signed ownership and authentication rule plus reconnect tests. |
| `«ACK-SHAPE»` | Explicit cumulative ack (recommended) or piggyback? Is ack acceptance itself confirmed? | Wire state machine and loss/race tests proving cursor monotonicity. |
| `«RETENTION-UNITS»` | Bytes, entries, or both (both recommended), and what signed defaults? | Aggregate bound calculation and typed config/refusal tests. |
| `«MULTI-CONVERSATION-MUX»` | May one connection carry many conversations, and how do per-conversation seq/cursor streams interact with `stream_id`? | Defined demux/ordering rules and cross-conversation interleaving tests. |
| `«RESUME-COMMENT-SERVER-MISMATCH»` | The SDK comment says re-Subscribe triggers durable replay (`crates/liminal-sdk/src/remote/tcp/mod.rs:195-198`), but Subscribe has no cursor (`crates/liminal-server/src/server/connection/apply.rs:349-421`) and re-subscribe resets sequence (`crates/liminal-server/src/server/connection/apply.rs:432-443`). Which statement is corrected? | Owner ruling and a test that demonstrates the surviving behavior. |
| `«EXTERNAL-EXIT-REASON»` | How does R-A1 learn `ProcessKilled` when external reaping cannot access beamr's private tombstone reason (`crates/liminal-server/src/server/connection/supervisor.rs:1643-1658`)? | Event payload/API or termination-intent plumbing; no scan-based substitute. |
| `«NO-FIN-KERNEL-BOUND»` | What signed idle/interval/count defaults and exact worst-case formula are certified? | Platform documentation plus fault-injection bound tests. |
| `«KEEPALIVE-PORTABILITY»` | Which OS socket options realize the same contract, and which platforms refuse participant mode? | Per-platform option readback and bounded no-FIN tests; no silent fallback. |
| `«REPLAY-LIVE-CUTOVER»` | What atomic sequencer/storage/binding operation fixes the replay watermark while admitting later live events? | Linearization point and adversarial reconnect/send interleaving test. |
| `«LIFECYCLE-VERDICT-RECIPIENTS»` | Which attached participants receive join/leave/death verdicts, and where are those verdicts ordered relative to deliveries? | One per-conversation ordering rule and race tests. |
| `«LIMINAL-SDK-VERSION-GATE-NAME»` | The outline names `«LIMINAL-SDK-VERSION»`; the checked-in Cally brief instead contains the filled `«LIMINAL-BASE-VERSION: 0.2.4»` sequencing gate (`docs/design/SDK-PARTICIPANT-RECEIVE.md:12-33`). Which artifact owns the SDK-version gate name? | Cally/domain-owner correction; do not cite a placeholder that is absent. |

**LAW 2 closes the escape hatch:** an unresolved dependency is either one of
these grep-able names or it is not allowed to support a codebase-state claim.
**LAW 1 closes the other escape hatch:** none may be “solved” by a timer, poll,
sweep, scan, heartbeat, or synthetic write whose job is to ask whether state
changed.

### Silence-attacking acceptance frame

Search the document for `«` and require a named owner decision or explicit
carry-forward for every result. Search every proposed timer/wake/task and prove
it is driven by admitted work, a kernel connection-fate event, or an existing
domain event—not change detection. A reviewer refutes closure by finding a
socket answered only in prose, an uncited checkout assumption, an unsigned
bound, or a polling mechanism hidden behind a different name.

---

**Gate posture:** DRAFT. This parks here. Reviewer key plus Hermes Crumpet's
liminal domain-owner key are required; until both turn, every recommendation is
a refutation target and nothing in this document is decided.
