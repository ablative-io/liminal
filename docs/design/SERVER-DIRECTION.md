# Liminal Server — direction & phase-1 build plan

*2026-07-07, Hermes Crumpet. Product scoping for a standout liminal-server,
from a three-scout + adversarial-judge panel (incumbent-pain,
primitive-native, agent-native lenses; judge verified every buildability
claim against source). Companion to `liminal-ledger.md`; v1 build specs in
§4 govern the H-wave implementation.*

## 1. Thesis

**Liminal is the bus where the runtime supervises your consumers.** Worker
death is a process-link EXIT on the same scheduler tick — not a heartbeat
timeout — so consumer groups never rebalance. A slow consumer becomes a
synchronous `Defer` in the producer's publish return — not a lag dashboard.
A crashed handler resumes at its last committed step from an event-sourced,
content-addressed log — not a redelivery loop through a dead-letter queue.
Incumbents cannot retrofit any of these: each requires the broker and the
workers to share a VM, and the log to be state, not bytes.

## 2. The six capabilities, ranked (defensibility × nearness)

1. **Rebalance-free crash-linked consumer groups.** Failover is a process
   link, physically unavailable to a network broker. Activates the built
   dispatch layer (`CrashPolicy::RouteToNext`, link-before-forward,
   ADR-007); needs the delivery pump + the A3 routing wire. Honesty caveat:
   v1 "delivered" = no-EXIT-within-window; positive ack is the A1 v2 credit
   mode.
2. **Producer-actionable backpressure on the wire.** No incumbent protocol
   has a producer return channel — lag is a metric, not a signal. Fully
   designed (A1-DEFER-SEMANTICS.md); brief A1-1 is unblocked the day the
   beamr 0.12.1 pin lands. Nearest to done.
3. **Resume-not-redeliver durable processing.** Offset-log brokers hold no
   per-consumer processing state; redelivery always re-executes. Activates
   the event-sourced `StepCompleted`/`ResumeFrom` conversation log
   (recovery-only today; ADR-006). Structurally uncopyable.
4. **Fork-at-hash replay & forensics (scoped).** Offset logs fork at O(n);
   haematite prolly trees fork at O(1). Scoped to *fork-for-replay and
   forensics* — live traffic-shadowing and divergent-log merge are
   explicitly out (no defined semantics; fork is a snapshot). Most
   uncopyable storage property, most greenfield plumbing; phase 3.
5. **Publish-time schema with non-disconnecting additive evolution.**
   Shipped (ledger A2). Copyable in principle (Pulsar does broker-side
   validation) but in the channel actor, not a sidecar — and it composes
   with 3.
6. **Embedded zero-hop same-code bus.** ADR-010; aion proves the seam. An
   architecture moat that costs the server almost nothing — positioning
   fact more than build item.

## 3. Explicitly not chasing (parity traps & killed claims)

- Kafka-API compatibility; throughput benchmark wars; "no ZooKeeper"
  marketing (Redpanda/NATS are already single-binary).
- Auth/ACL sophistication beyond table stakes (incumbents do this best; we
  need the table stakes — §4.H4 — nothing more yet).
- **Uploadable user code in the broker** — killed on verification: routing
  "modules" are native Rust closures, bytecode is hashed for dedup and
  never executed (`routing/function/loader.rs:5,132`); the Gleam execution
  tier plus beamr's capability sandbox is multi-quarter greenfield, not a
  missing wire.
- **Cryptographic delivery proofs** — storage attestation ≠ delivery proof;
  no client-verifiable machinery exists.
- Live traffic-shadowing forks; cross-node pressure accounting (A1 §8
  defers it deliberately).

## 4. Phase 1 — "messages arrive" (the pump gate)

Nothing in §2 is demoable until a remote subscriber can receive a message.
Phase-1 items, dependency-ordered; H-numbers extend the ledger:

### H1. Delivery pump + G4 outbound writer (M) — the keystone
The server→client delivery path, with the G4 fix designed in rather than
patched after (a pump built on today's `write_frame` ghosts every
connection carrying a >~64KB frame).

**Outbound writer (fixes ledger G4):** per-connection outbound byte buffer.
All server-originated frames are enqueued (bounded, default 4 MiB) and
drained cooperatively in the connection slice loop with partial-write
tracking (`write()` loop, not `write_all`); `WouldBlock` mid-frame leaves
the residue queued for the next slice. Unrecoverable write error or buffer
overflow ⇒ tear the connection down (a desynced stream must never survive).
Existing direct `write_frame` call sites route through the writer.

**Deliver frame (additive, discriminant 0x19):** `Frame::Deliver { stream_id
(the subscription's stream), flags, payload: [delivery_seq u64 BE][
MessageEnvelope bytes] }`. `delivery_seq` is a per-subscription monotonic
counter starting at 1 — carried from day one so the future ack/resume
protocol (A1 v2 credit) has an anchor. Reuses `MessageEnvelope`
serialization; unknown-frame forward-compat rules apply to old clients, but
Deliver is only ever sent on subscriptions the client itself opened.

**Server drain:** `SubscriptionResource` (services.rs:34) gains
`fn try_next(&mut self) -> Option<Envelope>` implemented over the wrapped
`SubscriptionHandle::try_next`; the connection process services its stored
subscriptions each slice after socket/control work, encoding up to a
per-slice budget (default 32 frames) into the outbound writer. No new
wakeup plumbing: the connection process already runs every slice.

**SDK receive:** a `SubscriptionStream` on the TCP transport following the
`PushClient` pattern (dedicated connection whose reader thread routes
Deliver frames into an mpsc by stream id; `recv_timeout` surface). v1: one
subscription per dedicated connection — multiplexing arrives with the v2
credit mode.

**Tests:** e2e subscribe→publish→remote receipt (order + envelope
fidelity); **large-payload delivery ≥96KB** proving G4 dead (the exact
Vesper repro, inverted); slow-reader does not wedge the connection process
slice; teardown on filled outbound buffer; existing push/e2e suites stay
green. *Coordination: ping Vesper before merge (G4 commitment).*

### H2 (= ledger G1, pulled forward). Durable-restart recovery (S)
Failing restart-then-publish test first (single-connection — the G0
starvation cannot touch it), then wire `recover_durable_channel` into
server channel construction so `expected_seq` derives from the store.
*Coordination: ping Vesper before merge.*

### H3. `/metrics` exposition + first server metrics (S)
Serve Prometheus text on the existing health endpoint (`GET /metrics`,
`metrics::export::render` over the global registry); record
`liminal_connections_active` (gauge), `liminal_publishes_total`,
`liminal_deliveries_total` (counters) at the connection/services layer.
Table-stakes hygiene so the first external demo isn't disqualified.

### H4. Auth-token interpretation (S)
`[auth] token = "..."` (optional) in server config; when set, the Connect
handshake's hitherto-unread `auth_token` must match (constant-time
comparison) or the connection gets `ConnectError` (existing reason-code
machinery) and closes. Absent config ⇒ open, as today. Not an ACL system —
the table stakes only.

### Post-0.12.1 tail (already specced elsewhere)
beamr 0.12.1 pin bump (G0) the day it publishes → A1-1 bounded inboxes →
A1-2/3/4 → A3 routing wire (activates capability 1).

**Phase-1 exit test:** a remote subscriber receives a transcript-sized
(>96KB) message stream, under 20+ concurrent connections (post-pin), with
credit-gated flow (post-A1-1).

## 5. Phase 2 — activate the moats
A1 complete (capability 2 headline-ready); A3 routing + consumer-group
dispatch wire (capability 1 headline-ready); resume-not-redeliver surfaced
as a server feature (durable conversations wired to live actors — the
DUR-005/006 machinery's first production caller); v2 explicit-credit acks
(honest "delivered", multiplexed subscriptions).

## 6. Phase 3 — the storage moat & reach
Fork-at-hash replay/forensics API (joint design with Apollo — cursor,
dedup-namespace, and subscriber-state semantics under fork need a doc);
WebSocket transport for the TS SDK (ledger B3, joint framing doc with
beamr's browser transport); embedded-mode backends (B1, with frame).
