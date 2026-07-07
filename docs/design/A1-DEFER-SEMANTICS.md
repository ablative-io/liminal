# A1 Defer Semantics — design (prerequisite doc for backpressure wiring)

*2026-07-07, Hermes Crumpet. Prerequisite design doc for ledger item A1 per
`liminal-assets-pack.md` §3: who buffers, retry/escalation rules, interaction
with durable channels and dedup. Produced by a three-design judge panel
(consumer-credit 124/150, durability-first 115, producer-retry 109; three
adversarial judges: crash-safety, protocol coherence, implementability).
The consumer-credit design won and is the spine of this doc; the deltas in
§0 were grafted from the panel. Designed against the G1-recovered restart
contract (`expected_seq` derives from the store) — the G1 fix lands before
any A1 code.*

## 0. Panel synthesis deltas (what was grafted and why)

1. **Derived counters, not independently mutated ones** (from
   durability-first, demanded by all three judges): `pressure_signal()`
   reads counts derived from the authoritative queue state under the inbox
   lock, rather than paired `record_*` mutations. Eliminates the
   `InFlightUnderflow`/`BufferUnderflow` drift class by construction.
2. **Durability-scoped dedup namespaces** (closes the panel's sharpest
   crash hole): an ephemeral channel's keyed publish must not complete a
   receipt that outlives the message. See §5.
3. **Pre-append hard watermark for durable channels** (from
   durability-first's admission-Reject idea, adapted): durable channels
   Defer-not-Reject as a semantic truth *after* append, but the log must
   not grow unbounded on a producer that ignores delay hints. A coarse
   channel-aggregate check *before* persist yields an honest Reject
   (nothing was appended yet). See §4.
4. **Host-side auto-catch-up for lagging durable subscribers** (from
   durability-first, relocated off the actor slice per the implementability
   judge): a connected-but-slow durable subscriber must converge without a
   manual re-subscribe. See §4.
5. **`channel/types.rs` is already over the 500-LOC budget (551)** — brief 1
   creates `channel/admission.rs` rather than growing it (from
   producer-retry).
6. **Three producer-retry tests adopted verbatim** (§10): concurrent-publisher
   hard-cap, dead-subscriber-releases-capacity, zero-appends-while-deferred.
7. **Server-side `StreamPressure` drain-gating** (from producer-retry) is
   recorded as the TCP consumer-credit thread-through, explicitly dependent
   on the not-yet-existing server→client subscription delivery pump. See §6.

## 1. Summary & core stance

**The bus buffers; producers are throttled, never custodians.** Every
subscriber gets a bounded, bus-owned delivery buffer (the existing
`SubscriberInbox`, today unbounded, becomes the bounded credit ledger). A
publish is admitted or shed at publish time, synchronously, per subscriber:

- **Accept** while the subscriber's in-flight window has credit;
- **Defer** when the window is exhausted but the buffer band has room — the
  message is still admitted and enqueued; Defer is purely a pacing signal to
  the producer;
- **Reject** when the buffer band is full — the message is shed for that
  subscriber (ephemeral channels; durable channels differ, §4).

Consumers return credit by consuming: popping the inbox (`try_next`) is the
v1 credit event; an explicit post-processing ack is specified as the v2
refinement (§2). There is no bus-side retry machinery and no producer-side
buffering: a Deferred message's "redelivery" is simply the consumer's next
pop, because the bus already holds it. This makes ADR-004's decided text
("defer: buffered, will deliver when capacity frees") mechanically true and
reuses the complete, unwired decision model in `pressure/capacity.rs`.

## 2. Decision attachment

**Where the decision lives.** A new `BoundedInbox` in
`channel/subscription.rs`: the queue and the capacity accounting live under
**one mutex**, so decide+record+push is atomic against a concurrent pop, and
concurrent publishers serialize on the same lock (closing the publish-time
TOCTOU the panel flagged). Counts are **derived** (graft §0.1): the in-flight
band is `min(queue.len(), max_in_flight)`, the buffered band is
`queue.len().saturating_sub(max_in_flight)`, and `pressure_signal()` becomes
a pure function of `queue.len()` against `(max_in_flight,
max_buffer_depth)`. `CapacityTracker` keeps its decision rule; its mutation
API is not used on the hot path. Occupancy invariant:
`queue.len() == in_flight_band + buffered_band` — true by construction.

Actor-model fit: payloads still ride the host-side shared queue (never beamr
mailboxes), the decision runs inside `apply_publish` on the actor's single
command slice **after the predicate** (no false backpressure from
non-matching subscribers), and the publish path stays synchronous on the
caller thread up to fan-out.

**Capacity declaration.** `ChannelHandle::subscribe_with_capacity(
ConsumerCapacity)` is added (additive); plain `subscribe()` gets defaults
(`max_in_flight = 128`, `max_buffer_depth = 1024`) so **every** inbox is
bounded — no opt-out. The `Subscribe` frame already carries `max_in_flight`;
`max_buffer_depth` stays bus policy (per-channel server config), matching
`StreamPressure`'s caller-supplied `buffer_capacity` design.

**Credit (v1):** consumed = the consumer took the message out of the bus's
custody (`try_next` pop). **v2 (specified, not shipped in A1):** an
explicit-credit subscription mode with a per-message delivery tag acked via
the embedded `AckToken` / the existing inbound `Frame::Accept` (0x10, today
ignored by `process.rs` — that is the wiring slot). v2 is blocked on the TCP
subscription delivery pump, which does not exist yet.

**Pressure state is never persisted.** Counters describe exactly the
envelopes the bus currently holds in memory. After a restart (G1-recovered
contract) inboxes are empty, counts are zero, durable consumers converge via
replay. Nothing to recover, nothing to drift.

## 3. The Defer lifecycle

**Emission** is synchronous in the publish round trip. Per-subscriber
outcomes aggregate to one producer-visible signal:

| Matching-subscriber outcomes | Producer signal |
|---|---|
| all Accept (or zero matching subscribers) | **Accept** |
| any Defer, or mixed Accept/Reject | **Defer** (worst delay hint) |
| all Reject | **Reject** |

The rule keeps `Reject ⇒ delivered to nobody` exact, which §5 depends on.

**Who holds the message:** the bus (bounded buffer and/or durable log).
**Redelivery trigger:** none needed — queue order, consumer's next pop. No
timers, no re-fan-out, no per-retry appends (Apollo's batching constraint is
satisfied trivially: one append per publish, ever).
**Escalation to Reject:** structural, never time-based — the buffer band
filling flips the next decision; a resuming consumer de-escalates the same
way.

**Producer contract (incl. `Defer { delay }`):**
1. Defer means *admitted; you are consuming buffer; pace yourself*. `delay`
   is an advisory hint: `base + (max − base) × buffer_fill_fraction`
   (defaults 25ms/250ms, per-channel config). The bus never blocks a
   producer and never lies; ignoring hints leads to Reject (ephemeral) or
   growing delays and eventually watermark-Reject (durable, §4).
2. The producer MUST NOT re-publish a Deferred message **with a fresh
   idempotency key** — that bypasses the receipt and double-delivers
   alongside the still-buffered original. Re-publishing **with the same key
   is safe and is suppressed** while the bus owns the message (§5), and is
   the blessed at-least-once pattern for must-execute producers (§7): a
   same-incarnation retry is a no-op; a post-crash retry on an ephemeral
   channel re-claims and re-delivers exactly because the incarnation-scoped
   receipt died with the message.
3. Reject means *shed, delivered to nobody*: the producer owns the retry
   decision, with its own backoff (aion #197 territory, §7).

## 4. Durable channels & replay

CN7 stands: durable persist happens before fan-out, on the caller thread,
one immediate durable commit per publish (~17–20ms WAL flush on macOS per
haematite guidance; batch-append remains the API for bulk producers).

**Rule: after append, durable channels Defer, never Reject.** Reject means
"shed", and an appended message is not shed — it sits at its sequence
position and every replay consumer sees it exactly once regardless of what
the live buffers did. Emitting Reject after a successful append would be a
lie. Live-buffer overflow on a durable channel sheds only the *live push*,
marks the subscriber `lagging`, and replay covers the gap.

**Pre-append hard watermark (graft §0.3).** To bound log growth against a
hint-ignoring producer, publish on a durable channel first checks a cheap
channel-aggregate occupancy watermark (host-readable, no predicate
evaluation needed): if aggregate live-buffer fill ≥ the configured hard
watermark (`pressure.durable_reject_watermark`, default 100% of every
matching-capable subscriber's total bound), the publish is **Rejected before
anything is appended** — honest, because nothing was persisted. This check
is deliberately coarse (it may reject a message whose target subscriber is
fast); the imprecision is documented and the precise per-cursor lag bound is
the v2 once live consumer cursors exist (C2/DUR-003 wiring). The WAL flush
(~17–20ms per publish) is a natural producer rate ceiling **on macOS only**
(F_FULLFSYNC); on Linux PLP NVMe the same commit is ~100× cheaper, so the
hard watermark carries the entire bound in production — do not tune the
default against macOS pacing (Apollo review, 2026-07-07).

**Append failure is a third outcome, distinct from Reject.** The append
itself can fail (I/O error, or `SequenceConflict` if anything else ever
writes the stream): nothing was persisted, so the mapping is
`release_claim` + a producer-visible *error* (`PublishError`/`SdkError`),
never a pressure signal — the producer's deliberate retry can re-claim
exactly as after a Reject.

**TTL rule: durable channel streams never append with TTL.** Liminal does
not use `append_with_ttl` on channel partitions today and this design keeps
that invariant, so a shed range can never *expire* out from under a lagging
subscriber's replay — replay via blessed reads always finds the full range.
(If a TTL'd durable class is ever introduced, expiry during a lag window
must be specified as intentional disappearance, not loss.)

**Auto-catch-up (graft §0.4).** A `lagging` durable subscriber converges
without manual re-subscribe: when its inbox drains below a low watermark
(default `max_in_flight / 2`), the *host side* (subscription handle path,
never the actor command slice — replay reads must not stall the actor)
replays the shed range from the durable log via the blessed
`replay_from`/EventStore read paths, refilling through the same bounded
admission. In-order delivery is preserved per subscriber because live pushes
stay shed while `lagging` is set; the flag clears when replay reaches the
log head, evaluated **loop-until-caught-up**: the head can move under
concurrent appends, so refill re-reads the head each batch and loops — the
chase is bounded because the pre-append watermark throttles producers while
any subscriber lags. This is the one place A1 wires the (today orphaned)
replay module into live delivery — scoped to a refill helper, not a
delivery-model rewrite.

**Replay/cursor interaction.** Replayed messages enter through the same
bounded inbox admission as live pushes; replay batches size themselves to
the free buffer band, so replay can never blow the bound it exists to serve.

## 5. Dedup interaction

Server order today (C1-hardened, dd82fb5): `claim_or_get` → deliver →
`complete_receipt` on success / `release_claim` on failure. A1 slots in:

- **Defer ⇒ `complete_receipt`.** Admitted and bus-held; the key is done. A
  producer retrying the same key hits `DedupDecision::Completed` — no
  duplicate delivery, no InFlight leak.
- **Reject ⇒ `release_claim`.** Aggregation makes producer-visible Reject
  mean delivered-to-zero, so the correct dedup state is "as if never
  published": tombstone so a deliberate retry can re-claim. `release_claim`
  never clobbers a receipt (at-most-once guard preserved). Mixed
  accept/reject reports Defer ⇒ complete — consistent with "at least one
  delivery happened".
- Durable channels only produce pre-append Reject (nothing persisted,
  release is exact) or Defer (complete). Every outcome maps to exactly one
  of complete/release — same shape as the C1 contract.

**Durability-scoped dedup namespaces (graft §0.2).** The panel's sharpest
crash hole: ephemeral + keyed publish → Defer (buffered in memory) →
`complete_receipt` (persisted) → crash before the consumer pops. The message
dies with the process but the receipt survives and suppresses the producer's
retry — silent permanent loss of a message the producer was told was
admitted. Fix: the dedup namespace for **ephemeral** channels is scoped to
the server process incarnation (namespace carries a per-boot incarnation
id), so message and receipt share a lifetime and a post-crash retry
re-claims and re-delivers (at-least-once, which is exactly ephemeral's
contract). **Durable** channels keep the persisted namespace — there the
message also survived, so the surviving receipt is truthful. This changes no
dedup API; it changes only the namespace string the server composes.

**Incarnation garbage (Apollo review):** a per-boot namespace strands the
previous incarnation's receipt keyspace forever if left alone (engine-side
tombstone GC is deferred). Ephemeral-namespace dedup records are therefore
written with the engine's native TTL (`append_with_ttl`, bound to the
channel's `dedup_ttl`) so stranded incarnations age out; the existing
`DedupSweeper` remains the belt-and-braces path once scheduled. Durable
namespaces are unaffected.

## 6. Wire & SDK mapping

The SDK `PressureResponse` vocabulary (`Accept`/`Defer{delay}`/
`Reject{reason}`) is frozen; the conformance harness is the spec. Nothing
below changes its shape.

**Embedded mode.** `PublishOutcome` gains a `signal: PressureSignal` field;
`ChannelDelivery` gains a `pressure()` accessor (additive). The SDK's no-op
embedded backend keeps returning `Accept` (honest test seam until B1).

**TCP mode — layered, strictly additive:**
1. **Flag bit (every client, zero risk):** `PublishAck` gains
   `PUBLISH_DEFERRED_FLAG = 0x02` (0x01 is `PUBLISH_DELIVERED_FLAG`). Old
   SDKs ignore unknown bits; meaning unchanged.
2. **Full frames (version-gated):** protocol minor bump. On connections
   negotiating ≥ vNext, a deferred publish is answered with the **existing**
   `Frame::Defer` (0x11, payload shape untouched) and a shed publish with
   `Frame::Reject` (0x12) instead of `PublishAck`/`PublishError`. Old
   connections always get `PublishAck` (+ flag) — they never see a frame
   their `publish_response` treats as unexpected (the panel verified
   ungated Defer frames hard-error every deployed SDK at
   `remote/tcp/mod.rs:241`; the gate is mandatory). The `delay` hint rides
   Defer via `DEFER_RETRY_AFTER_MS_FLAG = 0x01` declaring a trailing `u32`
   millis — only ever sent on ≥ vNext (old decoders reject trailing bytes;
   the gate covers it).
3. SDK `publish_response`/`publish_delivery_response` extend their match:
   `Defer → PressureResponse::Defer{delay}`, `Reject →
   PressureResponse::Reject{reason}`. All three SDK codecs move together in
   one PR (the TS SDK compiles the Rust codec).

**Server-side drain-gating (graft §0.7, recorded not shipped):** when the
server→client subscription delivery pump exists, `Subscribe.max_in_flight`
gates the drain loop through the existing `StreamPressure` state machine
(`record_accept` + the today-ignored inbound `Frame::Accept` are the hooks),
back-propagating a stalled TCP consumer into channel Defer end-to-end. A1
does not build the pump.

**G4 honesty note.** Defer/Reject publish responses are server-originated
frames on the same non-blocking `write_frame` path that truncates large
frames (ledger G4). They are <100 bytes — far below the ~64KB danger
boundary — so not practically exposed, but the dependency is recorded: A1's
server briefs must not introduce any large server-originated frame before
G4's outbound-buffer fix lands.

## 7. Aion & frame fit

What an aion worker sees on a deferred publish
(`RemoteChannelHandle::publish_with_idempotency_key`):
`Ok(DeliveryAck { pressure: Defer{delay}, accepted: false })`.

Joint contract for Vesper's #197 (revised per Vesper review, 2026-07-07):
- **Defer is a dispatch-rate signal, not a re-enqueue trigger** — apply
  `delay` before the next dispatch to that channel/queue; slow down instead
  of queueing blind.
- **Completion policy is chosen by message class, because v1 emits no later
  delivered/settled signal for a deferred message** (that is the v2
  explicit-credit territory, §2/§6):
  - *Fire-and-forget publishes* (events, telemetry): complete on
    `is_admitted()` (`Accept | Defer`) — a new `DeliveryAck` accessor,
    distinct from `is_accepted()` ("a subscriber genuinely received it").
  - *Must-execute dispatches on ephemeral channels*: **complete on Accept
    only.** On Defer, hold the outbox row open, apply `delay`, and retry
    **with the same idempotency key**: a same-incarnation retry is
    suppressed by the receipt (the bus still owns the message — no
    double-delivery); a post-crash retry re-claims and re-delivers because
    the incarnation-scoped receipt died with the message (§5). At-least-once
    with zero double-delivery, derived entirely from §5's semantics.
  - *Must-execute dispatches on durable channels*: complete on
    `is_admitted()` — a Deferred message is already in the log and survives
    crashes, so holding the row open buys nothing.
- **Reject is the retry trigger.** Delivered-to-nobody and claim-released
  (§5); backoff and retry with the same key cannot double-deliver.
- **REQUIRED consumer change (aion): per-logical-message idempotency keys.**
  Aion's outbox currently mints per-attempt keys (a deliberate pre-C1
  honest-ack discipline). Under A1 that is dangerous on the Defer path: a
  fresh-key retry of a deferred message bypasses the receipt and
  double-delivers alongside the still-buffered original. Within one logical
  message, Defer-path retries MUST reuse the key; Reject-path retries may
  also reuse it (claim released, re-claim works). Vesper aligns aion's key
  minting to per-logical-message as part of #197 — recorded here so the
  change is not silent.
- No new frames needed for aion; the vNext responses plus the flag bit cover
  new and old aion SDK pins.

This section seeds the joint design note ledger synergy #1 asks for — review
with Vesper before brief 2 lands (standing coordination commitment). Frame
F-3a's acceptance criterion is met by test 1 in §10 (Defer asserted under
real slow-subscriber load).

## 8. Cluster stance

**v1 pressure is local-node-only, documented.** The producer's signal
reflects the local node's subscribers. The cluster observer ignores pressure
on the send side; on the receiving node, remote envelopes land in
`SubscriberProcess::accept_remote_frame`, which pushes through the same
bounded admission — overflow **drops the remote frame** and increments an
observable shed counter, with durable receivers converging via replay like
any lagging subscriber. Justification: cross-node credit needs a return
channel over beamr distribution (a joint design with beamr, not a
liminal-side patch); the cross-node queue-full path is admittedly untested
(assets pack §2) and G0 makes clustered load tests unreliable until the
0.12.1 pin; and `Term::try_pid` range-skips already silently drop cross-node
deliveries (ledger D) — pretending remote pressure is accounted while
deliveries can vanish would be dishonest. The hard rule v1 does enforce: the
remote leg cannot cause unbounded local growth, because the bound lives in
the inbox the remote frame lands in.

## 9. Buffers inventory

| Buffer | Location | Bound | Overflow behaviour | Signal |
|---|---|---|---|---|
| Per-subscriber `BoundedInbox` | host-side, one per registration | `max_in_flight + max_buffer_depth` (128+1024 default) | Ephemeral: shed (Reject). Durable: shed live push, mark `lagging`, auto-catch-up replays | Accept/Defer/Reject at publish |
| Channel actor command queue | `ChannelActorCore` | ≤ concurrent callers (each blocks on a rendezvous reply); hard cap `MAX_PENDING_COMMANDS = 4096` as defense | enqueue error → `DeliveryFailed` → `PublishError` | error, not pressure |
| Channel actor beamr mailbox | beamr | atoms only, paired 1:1 with command queue | n/a | n/a |
| Subscriber beamr mailbox | beamr | local: wakeups/EXITs only; remote frames drained each wakeup into the bounded inbox | drain-and-shed at inbox | remote shed counter |
| Durable channel log | haematite | pre-append hard watermark (§4) + disk/compaction (C2); WAL flush is a rate ceiling on macOS only; watermark carries the bound on Linux | pre-append Reject | Defer w/ max hint, then Reject |
| Dedup InFlight claims | haematite streams | one per in-flight key; every outcome completes or releases; TTL sweep | tombstone | n/a |
| Server connection read buffer | `process.rs` | existing frame-size limits, unchanged | decode error | n/a |
| `PressureMonitor` maps | channel-level enforcer | #channels × #consumers; entry removed on unsubscribe/EXIT (new cleanup, brief 4) | n/a | n/a |

No queue anywhere grows without bound when a consumer stalls.

## 10. Test plan — asserted, not assumed

Conformance impact, explicitly: **`backpressure.publish_variants` is
unchanged** (vocabulary pinned; this design emits exactly it). Brief 3 adds
`backpressure.publish_response_frames` (vNext Defer/Reject + flag bit) and
codec vectors for the flag-gated `retry_after` field — scenarios +
all three harnesses in the same PR, per standing order #3.

Load/integration proofs (decision-function unit tests already exist in
`pressure/`):

1. `pressure_defers_under_real_slow_subscriber_load` — headline / frame
   F-3a. Real scheduler, ephemeral channel, capacity (4, 16), subscriber
   stops draining. Publishes 1–4 Accept; 5–20 Defer with non-decreasing
   delay hints; 21 Reject. Exact boundaries, real actor fan-out. *Gated on
   the beamr 0.12.1 pin (G0).*
2. `pressure_recovers_when_subscriber_resumes_draining`.
3. `durable_channel_defers_under_stall_and_log_is_complete` — every response
   Defer (below watermark), log ordered-complete, fresh subscriber replays
   all (blessed read paths).
4. `durable_watermark_rejects_before_append` — hint-ignoring producer hits
   the hard watermark: Reject returned, **store append count unchanged**
   (counting-store decorator), claim released.
5. `durable_lagging_subscriber_auto_catches_up` — overflow the live buffer,
   drain below the low watermark, assert the shed range arrives in order
   via host-side refill with no re-subscribe, exactly once.
6. `deferred_publish_with_idempotency_key_retried_delivers_once`.
7. `rejected_publish_releases_claim_and_retry_delivers`.
8. `ephemeral_keyed_receipt_dies_with_process_incarnation` — restart; a
   retried key on the ephemeral channel re-claims and re-delivers (the §5
   crash-hole regression test).
9. `fanout_aggregation_mixed_outcomes` — (full, free) subscribers ⇒ Defer +
   delivered to the free one; all-full ⇒ Reject and `delivered_count == 0`.
10. `inbox_hard_cap_never_exceeded_under_concurrent_publishers` (from
    producer-retry) — N threads flood one stalled subscriber; queue length
    never exceeds the bound at any observation point.
11. `dead_subscriber_releases_pressure_capacity` (from producer-retry) —
    kill-9 the subscriber process; EXIT prunes fan-out AND its inbox no
    longer counts toward any pressure decision.
12. `deferred_durable_publish_appends_exactly_once` (from producer-retry) —
    counting-store decorator: zero additional appends across
    Defer-and-drain.
13. `restart_resets_pressure_counters_against_recovered_contract` —
    *sequenced after G1; ping Vesper before landing.*
14. `tcp_publish_defer_maps_to_sdk_defer` /
    `legacy_client_receives_publish_ack_with_deferred_flag` — the vNext gate
    and old-client path.

## 11. Rejected alternatives

- **Producer-holds-and-retries (Defer = "not admitted, retry").** Moves
  buffering into every producer and all three SDKs, contradicts ADR-004's
  decided text, turns dedup hostile (retry becomes *required*, claims span
  retries), and on durable channels forces duplicate appends or a
  persisted-but-unadmitted limbo. The panel's protocol judge additionally
  verified its ungated Defer-frame publish response breaks every deployed
  SDK. Rejected 3-0 by aggregate ranking.
- **Full log-as-buffer with live per-subscriber cursors (durability-first
  as written).** Best durable semantics on paper, but live cursors do not
  exist — it is a push→pull delivery rewrite far beyond A1, and its `Refill`
  actor command puts blocking store reads inside the actor's command slice.
  Its best ideas (derived counters, watermark admission, auto-catch-up —
  relocated host-side) are grafted instead.
- **Prepare/reserve/commit two-phase durable admission.** Two actor wakeups
  per publish + a reservation table with restart-generation guards, to buy a
  post-append Reject that the pre-append watermark makes unnecessary.
- **Blocking publish.** Hides pressure instead of signaling it, and is
  deadlock-shaped on a one-command-per-wakeup actor.
- **Channel-level single tracker.** Misreports the moment consumers differ;
  discards the per-consumer model the monitor/enforcer is built around.
- **Credit grants over beamr mailboxes.** Violates the payload/mailbox
  discipline.
- **Full windowed wire flow-control now.** Right long-term shape; pointless
  before a TCP delivery pump exists. Sketched as v2 (§2, §6).

## 12. Implementation sketch (brief split)

- **Brief A1-1 — bus-side credit core (lands first, fully unblocked once
  0.12.1 is pinned).** `channel/subscription.rs` (`BoundedInbox`, derived
  counts, credit-on-pop, `subscribe_with_capacity`), **new
  `channel/admission.rs`** (decision + aggregation; keeps `types.rs` — at
  551 LOC — from growing), `channel/actor/mod.rs` + `actor/queue.rs`
  (decision in fan-out, `PublishOutcome.signal`, command-queue cap),
  `pressure/mod.rs` re-exports. Tests 1, 2, 9, 10, 11.
- **Brief A1-2 — durable & dedup semantics.** Durable Defer-not-Reject +
  pre-append watermark + host-side auto-catch-up (`channel/types.rs`,
  refill helper by the subscription handle), durability-scoped dedup
  namespaces + complete-on-Defer/release-on-Reject
  (`liminal-server services.rs`). Tests 3–8, 12, 13. *After the G1 fix;
  review §5/§4 haematite interactions with Apollo and §7 with Vesper before
  landing.*
- **Brief A1-3 — wire & SDK.** `protocol/frame.rs` flags, flag-gated Defer
  trailing millis in the codec, version bump + negotiation gate in
  `process.rs`, SDK mapping, conformance scenarios + all three harnesses in
  one PR. Test 14.
- **Brief A1-4 — policy & observability (parallel to 2/3).** Post-publish
  `ConsumerPressureMetrics` → channel-level `PressureEnforcer` (host-side,
  off the per-message path); `PolicyEvent`s ride the `aion.observability.v1`
  drain-tap pattern (standing order #6); `SlowProducer.reduction_factor`
  scales the delay hint; monitor-entry cleanup on unsubscribe/EXIT; the
  finalized #197 joint note with Vesper.

Order: A1-1 → A1-2 → A1-3, with A1-4 parallel after A1-1. A1-1 alone closes
the "unbounded subscriber inbox" hole and delivers frame F-3a's
slow-subscriber proof.
