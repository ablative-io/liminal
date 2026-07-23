# F-0c Unit 2 — server-initiated ServerPush producer build brief

Base: `liminal` main at `2bf71c4` (F-0c Unit 1 landed).

Round: r5 re-review closure fold. Lineage: r1 draft, r2 seat fold, r3 fold of
the completed pre-review under the seat's rulings, r4 ratifier's crash-cut
check, r5 closure of the re-review's surviving finding. All repository anchors
in this revision were read against the bytes at `2bf71c4`; no anchor is
inherited on trust from a pre-landing draft.

## Goal

Every committed participant record that has a landed Unit 1 production fact is
materialized into a durable `ServerPush::ParticipantDelivery` outbox after —
never before or with — its producing commit, offered in conversation sequence
order to the exact production-time recipient set, replayed after reattach until
cumulatively acknowledged, and published through both real socket transports
without treating a socket handoff as receipt.

This unit owns the ServerPush producer, recipient fan-out, delivery scheduler,
and socket publication path that Unit 1 explicitly left behind
(`docs/design/F0C-UNIT1-CLAIM-FRONTIER.md:639-651`). It does not reopen any D3
ruling.

## Acceptance lens

**G1 — Sent is not receipt.** A successful encode, outbound enqueue, socket
write, or SDK `Push` return proves only local transport progress. It MUST NOT
advance or reclaim a participant delivery. Only a durably committed cumulative
`ParticipantAck.through_seq` advances that participant's acknowledgement
frontier. The SDK already demonstrates the analogous distinction for requests:
`send_operation` installs correlation and returns `Sent` after the write, while
only receive applies a server value (`crates/liminal-sdk/src/remote/participant.rs:357-398,401-469`).

**G2 — terminal answers remain correlatable.** Push production is secondary to
the producing request's terminal `ServerValue`. No new producer, outbox, wake,
queue, or pressure path may replace, suppress, or silently drop that answer.
The existing participant dispatch has distinct encoded response and fatal arms
(`crates/liminal-server/src/server/participant/dispatch.rs:200-225,227-261`);
Unit 2 must preserve the exact Unit 1 request/response correlation path.

The frame two-request snapshot rule is a non-contradiction boundary, not a
surface this unit changes. The cited object is not reachable from this worktree;
the seat verified and vendored these exact bytes from
`frame@c6e3158:examples/frame-demo/PROTOCOL.md:69-79, verified by the seat 2026-07-18`:

    A `requestSnapshot` answer serves the newest available snapshot cut not yet served — for the liminal-backed feed this is the snapshot cache, so an answer may be the current generation's baseline, at or behind the frame that forced the resync. In that case the receiver asks exactly once more, and that answer must resolve to a strictly newer cut that covers or stales the trigger; the publisher's periodic generation baseline (every `SNAPSHOT_PERIOD`, ≈5 s at current cadence) bounds the wait. A second consecutive uncovering answer is a feed protocol violation and the receiver fails terminally and visibly.

Push replay, duplicate delivery, reattach, and acknowledgement do not create an
exception.

## 1. Pinned facts and exact missing boundary

### 1.1 Wire direction, body, and codec are already complete

`ParticipantFrame` classifies `ClientRequest` as client-to-server and both
`ServerValue` and `ServerPush` as server-to-client
(`crates/liminal-protocol/src/wire/codec.rs:133-142`). The receiver-direction
gate accepts push discriminants only for `ReceiverDirection::Client`; the server
direction accepts only client discriminants (`wire/codec.rs:383-399`). This unit
adds no wire tag or direction exception.

`ServerPush` is the exhaustive two-variant enum at the pin:
`ObserverProgressed { conversation_id, refused_epoch, observer_progress }` and
`ParticipantDelivery(ParticipantDelivery)` (`wire/push.rs:145-169`). A participant
delivery is exactly conversation id, conversation-owned `delivery_seq`, and one
of the six tagged record bodies — `OrdinaryRecord`, `Attached`, `Detached`,
`Died`, `Left`, or `HistoryCompacted` (`wire/push.rs:64-143`). Encoding writes
conversation, sequence, kind, then the exact kind body; decoding performs the
inverse and rejects trailing/non-canonical bytes (`wire/codec.rs:593-699`).

**Byte-checked server universal.** An exact `ServerPush` search over
`crates/liminal-server` at `2bf71c4` returns ONE occurrence: the inbound-invalid
arm at `crates/liminal-server/src/server/participant/transport.rs:144`. The
server has `encode_server_value` only (`transport.rs:154-176`); there is no
server push encoder, producer, queue, pump, or publisher. The task's suggested
`:144` anchor did not move in the landed bytes and needs no correction.

### 1.2 Unit 1 durable facts Unit 2 consumes

The participant operation log is schema v2, reads version before the complete
row, and returns typed `OperationLogError::SchemaVersion(version)` for every
non-v2 row (`crates/liminal-server/src/server/participant/production/log.rs:25-46,90-108`).
Append at the optimistic head followed by `flush` is its publication barrier
(`production/log.rs:111-136`). The exhaustive v2 operation set is eight kinds:
`Genesis`, `Enrolled`, `Attached`, `Detached`, `ZeroDebtAck`, `MarkerDrained`,
`RecordAdmission`, and `Left` (`production/log.rs:150-222`). This is an
exhaustive claim about that enum at the pin, not about future schemas.

The delivery-bearing facts are sufficient but not yet an outbox:

- enrollment persists participant id, binding epoch, `attached_order`, and
  `attached_seq` (`production/log.rs:369-381`); publication installs the bound
  slot only after the aggregate barrier succeeds
  (`production/ops_enroll.rs:350-407`);
- attach persists its `attached_seq` and, on supersession, the immediately prior
  terminal sequence (`production/log.rs:420-438`). The allocator gives both one
  transaction major in terminal-then-attached sequence order
  (`production/ops_attach.rs:125-170`), and the barrier precedes binding/frontier
  installation (`ops_attach.rs:243-267`);
- detach persists terminal order/sequence and canonical event
  (`production/log.rs:183-197`); its shared commit installs the frontier and
  detached binding only after the barrier (`production/ops_session.rs:238-258`);
- marker drain persists the exact canonical marker bytes, its keyed charge, the
  complete resulting charge list, and successor audit
  (`production/log.rs:241-248`). The marker row is appended/flushed before the
  owner is installed and the unchanged admission retries
  (`production/ops_frontier.rs:143-184`);
- RecordAdmission persists the exact token-bearing request and payload plus
  transaction order, `delivery_seq`, canonical charge, capacity effect,
  resulting charges, and closure-accounting audit
  (`production/log.rs:250-296`). Its handler decomposes the sealed commit once,
  appends the v2 row, then installs the owner/counters and returns
  `RecordCommitted` (`production/ops_frontier.rs:186-233`);
- Leave persists request/verifier, receiving epoch, left order/sequence, ended
  binding epoch, and prior terminal sequence (`production/log.rs:298-342`); its
  live arm appends before installing the frontier/tombstone and advancing the
  log head (`production/ops_leave.rs:180-198`).

The marker seam is deliberately linear. `MarkerDrainCommit` owns frontiers,
closure accounting, keyed retained charges, successor, and the validated marker
record; consuming it consumes the record token and returns the complete owner
plus successor (`crates/liminal-protocol/src/lifecycle/operations/marker_drain.rs:45-124`).
The server's cold replay re-derives and byte-checks the marker and successor
(`production/ops_frontier.rs:235-285`) and re-runs RecordAdmission through the
same selector while checking every stored audit (`production/ops_frontier.rs:287-385`).
These are the pending marker/delivery facts; Unit 2 SHALL consume them rather
than inventing a second lifecycle interpretation.

### 1.3 Frontier, closure, and acknowledgement owners

`ConversationAuthority` is the sole live conversation owner. It holds the
move-only `LiveFrontierOwner`, live and retired identities, the next order and
conversation delivery sequence, log head, and observer progress
(`production/state.rs:119-146`). It takes/installs the frontier as one value and
allocates monotonically paired positions (`production/state.rs:234-290`).
`LiveFrontierOwner` itself inseparably owns `ClaimFrontiers`,
`ClosureAccounting`, keyed retained charges, and the signed retained-row limit
(`crates/liminal-protocol/src/lifecycle/operations/live_frontier.rs:36-149`).
Unit 2 SHALL add one linear outbox owner beside this authority; it SHALL NOT put
delivery truth in a connection-local clone.

`ParticipantAck` is cumulative at the landed bytes. The selector rejects a
regression, returns no-op at the current cursor, rejects a boundary above
contiguously available delivery, and otherwise advances to `through_seq`,
consumes that participant's facts through the same boundary, and recomputes the
floor from the post-ack minimum cursor and other protocol-owned facts
(`crates/liminal-protocol/src/lifecycle/cursor_facts.rs:487-579`). The live
frontier transition copies the committed request boundary into the exact
participant rank (`lifecycle/operations/live_frontier.rs:690-724`). The server
persists the ack before applying it to the slot and installing the frontier
(`production/ops_acks.rs:133-180`). At the pin MarkerAck is intentionally
unreachable because the server supplies no delivered-marker witness
(`production/ops_acks.rs:185-244`); Unit 2 must replace that factual-empty seam
with outbox testimony, not bypass the selector.

### 1.4 Existing delivery precedent is not the participant path

The subscription pump drains core subscription inboxes, assigns a sequence per
subscription, builds generic `Frame::Deliver`, and holds one frame when the
current outbound buffer lacks room (`crates/liminal-server/src/server/connection/delivery.rs:83-199`).
Its sequence is connection-local and keyed by subscription id
(`server/connection/state.rs:43-59`), and its overflow policy can shed a
subscription with `SubscribeError` (`connection/delivery.rs:103-149`).

Unit 2 SHALL reuse only these mechanics:

1. the transport-neutral `DeliverySink` capacity/room/enqueue seam
   (`connection/delivery.rs:47-81`);
2. slice fairness and encode-before-room-check shape;
3. held-head-first ordering; and
4. the TCP and WebSocket invocation pattern (`connection/process.rs:120-181` and
   `connection/websocket/process.rs:188-203`).

It SHALL build a separate participant push pump. `ServerPush` uses the protocol
participant frame on generic stream zero, not `Frame::Deliver`; its
`delivery_seq` is allocated durably per conversation, not per subscription; its
payload comes from the participant outbox, not a core envelope; and D3 forbids
the subscription path's cancellation/shedding response. Reusing
`service_subscriptions` or `delivery_seqs` would violate all four distinctions.

### 1.5 A conforming SDK already receives Push

`RemoteParticipantInbound::Push` carries the exact `ServerPush` and transport
provenance without response correlation (`crates/liminal-sdk/src/remote/participant.rs:206-232`).
`receive` accepts `ParticipantFrame::ServerPush` in the client direction and
returns that arm; a client request received from the server remains an invalid
direction (`remote/participant.rs:401-419`). An exact search under
`crates/liminal-sdk/src/remote` found no automatic ParticipantAck on this path;
applications already receive the sequence and explicitly issue cumulative
acks. Unit 2 does not turn decode into receipt and does not auto-ack before the
consumer observes the value.

## 2. Ruled D3 contract — normative, not reopened

1. **D3-1 durable outbox after commit.** A producing operation first crosses its
   existing append/flush barrier. Only after that succeeds may one durable
   outbox source-batch row be appended and flushed. No outbox row shares the
   producing transaction, and no push is eligible for a socket before both
   barriers. An uncommitted operation is therefore unobservable as push.
2. **D3-2 recipient snapshot.** The recipient set is the sorted set of permanent
   participant ids whose binding is live in the committed poststate, minus the
   producing sender. It is computed once under the same conversation lock and
   persisted in each projected record in the source batch. Later attach/detach
   does not rewrite it. A system-authored marker has no participant sender, so
   no member is subtracted; its target can receive and acknowledge the marker.
3. **D3-3 ordering.** The durable participant `delivery_seq` is the only order
   key. Every recipient observes one conversation in strictly increasing
   sequence; a superseding attach's stored terminal is the first record and its
   attached record is the second record of one source batch. Cross-conversation
   order is intentionally unspecified.
4. **D3-4 retention.** A projected record remains live while any persisted
   recipient obligation has `ack_through < delivery_seq`; its enclosing source
   batch remains as append-only audit. A committed
   `ParticipantAck.through_seq` discharges all that participant's obligations
   through the boundary. The payload becomes logically reclaimable only when
   every recipient obligation is discharged. Socket offer/write changes none of
   these facts. **SEAT RULING — FLAGGED FOR WAFFLES/TOM RATIFICATION:** a
   committed `Leave` also discharges all outstanding recipient obligations of
   the leaving identity at the Leave commit. This is the seat's explicit D3-4
   interpretation, not a silent contract change: Leave validates identity and
   an empty immutable prefix but never catch-up
   (`claim_frontier.rs:3495-3553`), then permanently removes the identity
   (`claim_frontier.rs:2844-2853`); ids are never reused. Permanent retirement
   makes the D3-6 replay purpose impossible forever, while append-only audit rows
   remain. The discharge is an atomic effect of the Leave source batch in the
   Unit 2 extension stream. Section 9 records both this ruled bound and the
   rejected conservative bound for ratification.
5. **D3-5 pump holdback.** Current outbound pressure holds back that recipient's
   exact next frame. It never cancels a delivery, sheds participant interest,
   emits a synthetic terminal answer, or tears down a connection merely because
   current room is insufficient. Other connections and recipients continue.
6. **D3-6 reattach replay.** A committed attach registers the new binding and
   wakes delivery from the participant's durable `ack_through + 1`. Volatile
   offered progress from an old binding is discarded. Every still-live
   obligation is offered again, in order; duplicates before ack are legal.
7. **D3-7 loud migration.** Every persisted outbox row carries one exact schema
   version. Unsupported, missing, mixed, or drifted rows fail startup/first
   touch with a typed schema-version/corruption error before an owner or push is
   published. There is no serde default, alias, second decoder, or silent
   reinterpretation.

## 3. Durable outbox and crash cut

### 3.1 Separate post-commit stream

Add one versioned Unit 2 per-conversation extension stream, also called the
outbox stream, owned by the production handler and keyed separately from the v2
participant operation stream. The first schema is `schema_version = 1`; it does
not rename, bump, or reinterpret the Unit 1 v2 stream. Its exhaustive row kinds
are `Produced`, `AckAdvanced`, and `MarkerAckCommitted`.

`Produced` is a SOURCE BATCH with this shape:

- `source_log_sequence` and `source_kind` identify exactly one committed v2
  source;
- `ordered_records` is a nonempty list of one or two projected records;
- each projected record carries its own conversation `delivery_seq`, exact
  typed `ParticipantRecord` body, sorted duplicate-free production-time
  recipient snapshot, optional sender participant id proving exclusion, and
  canonical encoded-byte charge used by the signed derived bounds.

A superseding attach batch contains exactly two records, terminal then attached,
in that order. Every other producing source contains exactly one. Source
uniqueness and uncertain-append idempotency apply to the byte-identical whole
batch, including record count and order; a batch differing in any record,
recipient, sender, charge, or byte is typed corruption. This is one post-commit
append even for the two-record supersession mapping.

`AckAdvanced` identifies its committed v2 ack source and carries participant id
and the exact durably committed `through_seq`. `MarkerAckCommitted` is the only
frontier-affecting extension row. It carries the exact canonical `MarkerAck`
request, receiving binding epoch, offered-marker delivery witness (marker
`delivery_seq` and delivered binding epoch), and complete post-transition audit,
plus ordering metadata `{ base_log_head, extension_sequence }`.
`base_log_head` is recorded under the conversation lock at commit;
`extension_sequence` must equal the physically assigned extension sequence, which
orders ties at one base boundary.

Cold restore stream-merges the two streams. It applies all extension rows at a
recorded base boundary in physical extension-sequence order, then applies the
next v2 base row. A base-backed `Produced` or `AckAdvanced` boundary is exactly
`source_log_sequence + 1`; a marker-ack boundary comes from its stored
`base_log_head`. Nonmonotone, impossible, malformed, unknown, or mixed-version
boundaries refuse loudly before authority publication. Marker replay re-runs the
same authoritative marker-ack selector with the stored witness, byte-checks
every audit field, and only then applies its frontier transition. Among
extension variants, ONLY explicitly typed `MarkerAckCommitted` may feed
frontier state; `Produced` and `AckAdvanced` never do.

Extension restore folds delivery rows into one move-only `ConversationOutbox`:
records keyed by sequence, per-recipient ack frontiers, per-recipient next-live
obligation, and the next optimistic extension sequence. Constructors remain
private; restore validates whole-source uniqueness, record/sequence agreement,
recipient sorting, sender exclusion, monotone ack, charge bytes, and Leave
discharge. The v2 participant stream remains the primary lifecycle source; the
extension stream adds only the explicitly ruled marker-ack transition.

### 3.2 Ordered two-barrier protocol

Every delivery-bearing live arm SHALL perform this order under the existing
per-conversation lock:

1. select and consume the protocol operation;
2. append/flush the existing v2 operation row;
3. install its committed poststate sufficiently to take the D3 recipient
   snapshot, without releasing the conversation lock;
4. build the complete typed source batch from the sealed/stored commit facts;
5. append/flush one outbox `Produced` source-batch row; then
6. publish the request's existing terminal response and wake eligible live
   recipient connections.

For `Left`, step 5 also durably and atomically records the ruled discharge of all
older obligations owned by the permanently retired identity; installation of
that discharge follows the extension flush and precedes response publication.

A push can therefore never race ahead of its answer's durable operation. The
answer and push remain different values: the answer is correlated to the
request; the push is fan-out work.

There is an unavoidable crash window between barriers. Startup and every cold
first touch SHALL restore the v2 base and extension stream through the ordered
merge above, then reconcile every committed delivery-bearing source in v2 log
order. A missing projection is materialized as one complete source-batch
append/flush before the handler is published; an exact existing whole batch is
accepted; a conflicting batch fails loudly. Ack reconciliation follows the same
rule from committed `ZeroDebtAck` rows. Reconciliation emits no socket or ack
work until it completes.

If barrier 1 commits and barrier 2 fails live, the operation is committed but
push projection is pending repair. The handler returns a typed internal fault;
it does not fabricate a refusal or erase the terminal answer authority. The
connection fate makes the request outcome uncertain, and exact-token retry/cold
repair recovers the Unit 1 terminal result. Tests must not claim rollback of a
successfully appended operation.

## 4. Complete production mapping

Add one exhaustive conversion beside the v2 replay fold, not ad-hoc conversions
inside socket code:

| v2 committed source | ordered records in its `Produced` source batch | sender used by D3-2 |
|---|---|---|
| `Genesis` | none | none |
| `Enrolled` | `Attached { affected_participant_id, binding_epoch }` at `attached_seq` | newly enrolled participant |
| `Attached`, ordinary | `Attached` at `attached_seq` | attaching participant |
| `Attached`, superseding | `Detached { old epoch, Superseded }` at `superseded_terminal_seq`, then `Attached` at `attached_seq` | attaching participant for both |
| `Detached` | `Detached { affected participant, receiving epoch, CleanDeregister }` at `terminal_seq` | detaching participant |
| `ZeroDebtAck` | none; append `AckAdvanced` after its v2 commit | none |
| `MarkerDrained` | `HistoryCompacted` from the protocol-selected marker target/range at the marker sequence | none (system authored) |
| `RecordAdmission` | `OrdinaryRecord { sender_participant_id, payload }` at stored `delivery_seq` | request participant |
| `Left` | `Left { affected_participant_id, ended_binding_epoch }` at `left_delivery_seq`; the batch also discharges the left identity's older obligations | leaving participant |

`MarkerAckCommitted` (extension stream) maps to no push record. The exact wire
field in `Attached` is `binding_epoch` (`wire/push.rs:74-80`).

The mapping is exhaustive for `StoredOperation` at `2bf71c4`. The wire's
`Died` body remains codec-covered, but no v2 `StoredOperation` at the pin
represents a Died commit; Unit 2 SHALL NOT synthesize one from socket teardown.
Connection-loss lifecycle production requires a protocol-owned committed Died
shape and is outside this unit, not a missing arm in the table.

Marker conversion needs a protocol-owned consuming/borrowing projection that
returns the exact `HistoryCompacted` fields from the validated marker record
before its token is consumed. Debug-format bytes at
`production/ops_frontier.rs:388-397` are an audit encoding, not a wire-body
parser. Extend the marker seam so the server receives a typed delivery
projection coupled to `MarkerDrainCommit`; do not parse `Debug`, expose raw
candidate construction, or duplicate marker provenance rules.

## 5. Recipient registry and reattach ownership

Add a server-wide participant publication registry installed in
`InstalledParticipantService`, which already couples the handler, store, and
frame limit (`server/participant/dispatch.rs:150-198`). A registration is keyed
by durable `ConnectionIncarnation` and owns a bounded/coalescing readiness
handle into exactly one TCP or WebSocket connection process. The connection
process owns its inbox, per-conversation offered cursors, and held heads; the
registry holds only a weak/non-owning publication handle so teardown cannot be
prevented.

A successful enrollment/attach poststate binds participant id to a connection
incarnation already carried by `BindingEpoch`. Recipient snapshots store
participant ids, while dispatch resolves the participant's current bound epoch
to the registry at offer time. This is why detach causes holdback and reattach
causes replay rather than rewriting durable recipients.

Registration and deregistration are event-driven at connection spawn/teardown.
Push readiness coalesces by conversation; it never queues an unbounded copy of
every payload. The existing single READY vocabulary is a non-blocking wake
(`server/connection/wake.rs:27-75`), and TCP's final probe prevents an
empty-to-nonempty race before parking (`connection/process.rs:195-220`). Unit 2
adds participant-inbox work to both final probes and both cleanup paths. There
is no sweep, timer, sleep, or polling loop.

## 6. Participant delivery pump and socket publication

Add `encode_server_push(ServerPush) -> Frame` beside
`encode_server_value`, using the shared participant codec and generic stream
zero. Codec tests must prove byte identity with direct
`ParticipantFrame::ServerPush` encoding. The pump runs after inbound/control
response work and before outbound drain so G2 answers already selected in the
slice are enqueued before unrelated pushes.

For each ready recipient, the pump:

1. resumes a held encoded head first;
2. otherwise asks the outbox for the least eligible sequence greater than the
   connection's volatile offered cursor and greater than durable ack;
3. encodes exactly one `ServerPush::ParticipantDelivery` and computes its size;
4. if it fits an empty sink but not current free room, retains that exact frame
   and returns holdback without changing offered or ack progress;
5. after successful enqueue only, advances volatile offered progress and
   continues within the signed slice budget; and
6. on writable readiness, drains and resumes.

A frame larger than an empty sink is not treated as pressure. Configuration and
canonical-size validation must make it unreachable for every persisted record;
encountering it is typed schema/config corruption and fails loudly. Ordinary
current-room pressure never calls the sink's overflow path, never releases the
binding, and never returns a teardown instruction.

Round-robin scheduling is by live connection then conversation; no conversation
may consume a second item while another ready conversation on that connection
has not received its first in the slice. Ordering is per conversation only.
`ObserverProgressed` and participant-delivery pushes consume the same signed
slice counter; observer wakes never receive an additional budget. The numeric
slice budget is gated by `WALL-CONFIG-SIGNOFF` below.

## 7. Ack retention, marker testimony, and replay

Startup/first-touch outbox reconciliation completes before any ack is handled.
After `ParticipantAck` crosses the existing v2 append/flush barrier, append and
flush `AckAdvanced` before answering `AckCommitted`, then install the same
boundary in the outbox owner. Refusal, no-op, regression, gap, append failure,
or an ack for a non-recipient does not reclaim an obligation. Reclaim is logical
in the append-only store: acknowledged rows cease to occupy the live outbox and
capacity projection; historical source/ack rows remain replayable audit bytes
because the landed `DurableStore` exposes independently keyed append/read/flush
streams but no truncate/delete contract
(`crates/liminal/src/durability/store.rs:20-57`). This implements "become
reclaimable" without pretending physical deletion exists.

**Q5 CLOSED — DURABLE-OBLIGATION PREFIX.**
`contiguously_available_through` is the recipient's reconciled
DURABLE-OBLIGATION prefix, never the conversation-global `next_seq - 1` now
supplied at `production/ops_acks.rs:31-40` and never a volatile-offered cursor.
The `ParticipantAck` itself is the receipt testimony. Volatile-offered is
strictly worse: restart erases it, and an ack arriving on a new binding before
replay would otherwise refuse truthful testimony until a duplicate offer. That
is incompatible with the producing barrier's ground truth that "nothing is
published until these bytes are durable" (`production/log.rs:113-114`). G1
still holds: no encode, enqueue, write, or volatile cursor supplies receipt.

The existing scalar selector is insufficient: it accepts any `through_seq <=
min(bound, H')` and does not prove that the endpoint is an obligation
(`cursor_facts.rs:535-579`). The build SHALL extend the authoritative protocol
selector with sealed per-recipient availability testimony. The requested
endpoint MUST exist in that recipient's committed obligation index. Internal
sequence gaps caused by sender exclusion or absence from an older snapshot are
skipped as non-obligations, but a request ENDING on a sequence with no committed
obligation is `AckGap`. The build also updates the stale "offered to that exact
epoch" contract documentation at `cursor_facts.rs:487-497` to describe durable
per-recipient obligation testimony. Server code may not reimplement this
endpoint rule.

The D3-4 Leave ruling is applied at this same owner boundary: after the Leave
source batch flushes, all older obligations of the permanently retired identity
are discharged together, while source and audit rows remain. This interpretation
remains visibly flagged for Waffles/Tom ratification; without it, section 9's
rejected identity-cohort bound is the honest one.

For MarkerAck, the outbox supplies the exact marker obligation, delivered
binding epoch, and delivery witness required to construct `MarkerProofState`.
Only a marker frame successfully enqueued on that exact binding creates volatile
"offered" testimony; a socket write is still not receipt. The existing total
`apply_marker_ack` selector remains authoritative. A commit appends and flushes
one `MarkerAckCommitted` row to the Unit 2 extension stream, then installs the
selector's frontier transition. The row carries the ruled Q3 body and the
`{ base_log_head, extension_sequence }` merge metadata defined in section 3.1.
Cold replay presents the stored witness to the same selector and checks the
complete audit before installation.

`MarkerAckCommitted` is not push-producing, so durable-outbox-after-commit is not
implicated: its one extension append/flush IS the marker-ack commit barrier. The
participant base log remains literal v2. Independently keyed durable streams and
the separately versioned observer stream are landed precedent
(`crates/liminal/src/durability/store.rs:20-57`;
`production/observer.rs:19-55`). The r2 v3 necessity claim and ratification are
reversed: absence of a MarkerAck variant in the v2 base enum proves only that the
row needs a typed durable home, not that the base enum must change. The signature
pack carries the Unit 2 extension schema and no participant-schema diff.

## 8. ObserverProgressed boundary

This unit's durable D3 outbox is for `ParticipantDelivery`. The other wire
variant, `ObserverProgressed`, is a refusal-arm wake, not a recipient record and
has no `ParticipantAck` sequence. The landed authority initializes
`observer_progress` to zero (`production/state.rs:216-231`), production never
assigns live progress, `decide_progress_advance` requires its caller to present
the value (`observer_recovery.rs:247-281`), and only restore currently folds an
`Advance` (`production/observer.rs:144-178`). Unit 2 SHALL close both the
committed-source projection hole and the source-commit-to-Advance crash window.

Every source that can advance hard-observer progress is exhaustive here: a
committed normal `ParticipantAck` (the v2 `ZeroDebtAck` arm and any shell
`NonzeroDebtAck` source), committed `MarkerAckCommitted`, and each committed
binding-ending fate — a superseding `Attached` terminal, `Detached`, `Left`, and
a protocol-owned `BindingFate`/`Died` source. The protocol transition owning each
source SHALL surrender a sealed, exact
`{ conversation_id, new_observer_progress }` projection. The server neither
invents a maximum/formula nor infers progress from record delivery. The shell's
nonzero-debt ack and binding-fate source census is explicit at
`operation_event.rs:493-570`.

For each advancing source, the source append/flush barrier completes first.
While the observer owner remains exclusively serialized, production presents
that exact projection to `decide_progress_advance`, appends/flushes
`ObserverRow::Advance`, commits the protocol transaction, and only then may
publish the fired `ObserverProgressed`. Startup and cold first touch replay every
participant source, reconcile any missing `Advance`, and restore the observer
aggregate before publishing conversation authority or admitting an observer
handshake. An already equal-or-greater exact durable Advance satisfies
reconciliation; disagreement or a nonmonotone source refuses loudly. Thus a
crash after a participant source barrier but before its Advance is repaired
before an observer can observe stale authority.

**Q2 CLOSED — observer v1 stands; no durable arm target.** A persisted old
connection incarnation would name no surviving socket. The existing handshake
deterministically reports `progressed` whenever current progress exceeds the
refused epoch (`observer_recovery.rs:691-707`), so recovery needs a live target,
not a durable dead one. Under the observer-owner mutex, an accepted recovery
batch appends/flushes `ObserverRow::Arms`, then installs or replaces the volatile
`(conversation_id, refused_epoch) -> weak live connection` association before
unlocking or answering the handshake. A dead connection makes the weak handle
missing; it is never replaced by a default or broadcast target.

The advance path shares that same non-cancellable owner critical section. After
`ObserverRow::Advance` flushes, it commits the protocol transaction and transfers
the exact fired payload to the associated live inbox before unlock. The payload
rides `encode_server_push`, READY, and the shared signed push-slice budget, but
not ParticipantAck fan-out. A dead or missing handle drops only that targeted
wake; no other connection receives it, no socket handoff becomes receipt, and no
semantic terminal response is displaced.

The deciding crash cuts are:

- before `Arms` flush, there is no arm;
- after `Arms` flush but before volatile registration, a process crash destroys
  the only target socket, and reattach either re-arms or observes progressed;
- after `Advance` flush but before socket handoff, a process crash leaves durable
  progress, so the next handshake observes progressed; and
- reattach versus advance serializes under the observer owner: advance-first
  yields a progressed response, while reattach-first installs the one live
  target that receives the fired payload.

No observer schema change is required or permitted for targeting; the observer
stream remains v1.

## 9. Derived values requiring signoff

No number in this table is normative until Tom and Annabel sign it. Build code
must use the signed value/formula, with checked arithmetic and accumulated
configuration validation. A blank, omitted, overflowed, or substituted value
blocks dispatch.

| named placeholder | recommended value | derivation | worst-case cost |
|---|---:|---|---|
| `UNIT2_PUSH_SLICE_BUDGET` | `32` | Reuse, after signoff, the existing subscription fairness scale `DELIVERY_SLICE_BUDGET = 32` (`connection/delivery.rs:37-40`) so adding participant work does not create a larger scheduler slice. Participant-delivery and observer pushes debit this same counter. | 32 total participant-frame encodes/enqueues per connection slice across both `ParticipantDelivery` and `ObserverProgressed`, in addition to separately budgeted subscription work. |
| `UNIT2_OUTBOX_RESTORE_BATCH_ROWS` | `64` | Reuse, after signoff, the participant log's durable read-page scale `READ_BATCH_SIZE = 64` (`production/log.rs:25-30`). | One batch holds 64 encoded extension/outbox rows plus decode state; total restore remains streaming. |
| `UNIT2_MAX_LIVE_RECIPIENT_OBLIGATIONS` | `checked(max_retained_record_rows × identity_slots)` | Every live retained participant record has at most every permanent conversation identity as a production-time recipient; both factors are already required signed participant inputs (`config/types.rs:475-483,509-516`). Leave atomically discharges the retired identity's cohort. Reject arithmetic overflow. | Per conversation, at most the signed product of recipient index entries; payload is stored once per projected record, not once per recipient. |
| `UNIT2_MAX_LIVE_OUTBOX_PAYLOAD_BYTES` | `checked(retained_capacity_bytes + fixed_outbox_overhead(max_retained_record_rows, identity_slots))` | Payload-bearing rows already count against signed retained bytes; the ratification-flagged Leave discharge prevents retired identities from stranding successive full payload cohorts. Add only canonical recipient/index framing measured by the new encoder; a checked fixture determines the exact fixed term before signoff. | Per conversation, the signed retained payload cap plus the measured fixed metadata term; no hidden per-recipient payload copies. |
| `UNIT2_MAX_HELD_HEADS_PER_CONNECTION` | `max_semantic_conversations_per_connection` | One held participant head per tracked conversation; the existing signed connection map already bounds that cardinality (`dispatch.rs:23-69`). | One encoded frame per tracked conversation, each additionally proven `<= wire_frame_limit`; no second unbounded connection queue. |

**REJECTED conservative branch, preserved for Tom's eyes:** without the ruled
D3-4 discharge, fill → retire-unacked → reclaim → refill can strand one full
cohort per permanent identity because Leave requires no catch-up and ids are
never reused. The honest bound would be
`checked(identity_slots × retained_capacity_bytes + metadata_for(max_retained_record_rows × identity_slots))`.
The seat instead rules discharge-on-Leave, yielding the table's
`checked(retained_capacity_bytes + measured fixed metadata term)` shape. Waffles
and Tom must ratify that interpretation and therefore the smaller signed shape;
until then it remains a visible ratification gate, not a hidden assumption.

The canonical encoder tests SHALL print the measured fixed outbox overhead and
maximum encoded push for Tom/Annabel. If either invalidates a recommendation,
the build STOPS with exact bytes for a signoff amendment; it never silently
bumps a value.

## Acceptance tests

No criterion is satisfied by inspecting a send result, an unflushed row,
volatile offered progress, a hand-constructed push, or a sleep-based eventual
assertion. Every asynchronous test uses deterministic append/flush gates,
readiness markers, or socket readability.

### Layer 0 — protocol and codec

1. **`server_push_direction_and_codec_round_trip_all_record_kinds`**: round-trip
   both push variants and all six `ParticipantRecord` kinds through the client
   receiver; server receiver rejects their discriminants. Assert exact
   conversation/sequence/body bytes.
2. **`marker_commit_projects_typed_history_compacted_without_debug_parse`**:
   drain each marker provenance/target shape and consume the new typed projection;
   prove it matches the retained record and that no public raw constructor or
   `Debug` parser exists.
3. **`participant_ack_only_advances_receipt_frontier`**: enqueue/write testimony
   leaves ack/frontier/outbox unchanged; an exact committed cumulative ack
   advances through all eligible obligations; regression/gap/no-op do not.
4. **`participant_outbox_owner_is_move_only`**: compile-fail/privacy tests prove
   outbox, frontier owner, recipient obligations, producer commit, held head,
   and replay reconciliation owner cannot be cloned, copied, or assembled from
   independent conversation parts.

### Layer 1 — producer mapping and two-barrier durability

5. **`every_v2_source_maps_to_one_exact_source_batch_or_none`**: table-test all
   eight v2 kinds — Genesis, Enrolled, Attached, Detached, ZeroDebtAck,
   MarkerDrained, RecordAdmission, and Left — plus extension MarkerAck. Assert
   Genesis/acks produce no `Produced`, every delivery source produces exactly one
   batch, supersession alone has two ordered records (terminal then attached),
   every other batch has one, and MarkerAck produces no push batch. Check each
   record's own sequence, typed body, and recipient snapshot.
6. **`recipient_snapshot_is_postcommit_bound_and_resumable_detached_minus_sender`**
   (trued 2026-07-23 to the B1 ruled contract, superseding the prior Bound-only
   rule): with sender, two bound peers, one resumable-detached member, and one
   cleanly-departed (retired) identity, commit each participant-authored source;
   persist the two bound peers AND the resumable-detached member, sorted and
   duplicate-free — the departed identity stays absent (clean Leave is the sole
   slot-removal path, so map presence is the discriminator). Later detach/attach
   does not rewrite the batch. A system marker includes its live target.
7. **`outbox_row_is_impossible_before_producing_flush`**: gate operation append
   and flush independently. Before producing flush there is no outbox append,
   wake, encoded push, or response. After it, gate the outbox append/flush; no
   push is eligible before outbox flush.
8. **`postcommit_outbox_failure_is_repaired_not_rolled_back`**: inject barrier-2
   append and flush failures. The v2 operation remains durable, no push is
   published, and cold first touch derives one byte-identical missing source
   batch before authority publication. Exact retry receives the correlated Unit
   1 terminal answer.
9. **`uncertain_duplicate_source_batch_is_idempotent_only_by_exact_bytes`**:
   replay one exact whole batch twice and get one logical batch; alter record
   count/order, any record's recipient/body/sequence, sender, or charge and
   receive typed corruption.

### Layer 2 — retention, replay, and extension restore

10. **`ack_through_reclaims_only_that_recipient_prefix`**: fan one payload to A,
    B, and C. A's ack through N releases only A's obligations through N; the
    payload remains live until B and C cover it. Ack N+K cumulatively releases
    all eligible earlier rows without per-row acks.
11. **`socket_offer_and_write_never_reclaim`**: encode, enqueue, partial-write,
    complete-write, peer-close, and reconnect without ack; every durable
    obligation remains and is replayed. This is the named G1 oracle.
12. **`reattach_replays_unacked_in_order_after_acked_frontier`** [r5 — corrected
    to conform to D3-2]: ack through N, then commit N+1..N+3 while the
    recipient is STILL BOUND and therefore included in each persisted recipient
    snapshot, detach before offer/ack, reattach, and observe exactly those live
    recipient obligations in ascending sequence; reconnect again before ack and
    observe duplicates in the same order. Complementary half: records committed
    AFTER the detach create no obligation for the detached identity (production
    binding occupancy recognizes only `BindingState::Bound`,
    `production/state.rs:293-309`) and do not appear after reattach.
13. **`marker_ack_requires_exact_offered_binding_testimony`**: before offer,
    wrong marker, wrong binding epoch, and stale generation remain typed
    refusals. Exact offered marker on the current binding commits one
    `MarkerAckCommitted` extension row, replays through the same selector and
    stored audit, and advances the protocol owner without producing a push.
14. **Extension/base replay trio:**
    - **`literal_v2_base_streams_continue_to_restore`**: empty and literal v2
      participant streams restore unchanged; no participant schema diff exists.
    - **`malformed_unknown_and_mixed_unit2_extension_rows_refuse_before_publication`**:
      malformed, unknown-kind, missing-version, unsupported-version, and mixed
      Unit 2 extension rows refuse before authority, outbox, or push publication.
    - **`marker_ack_and_base_row_interleavings_replay_exactly_and_totally`**:
      place marker commits before, between, and tied at base boundaries; physical
      extension sequence resolves ties, every audit is checked, and each merged
      replay exactly matches live frontier/outbox state.
15. **`extension_schema_version_refuses_before_projection`**: literal missing,
    old, unknown, and mixed schema fixtures for the extension/outbox stream fail
    before projection. No serde default fixture may pass.

### Layer 3 — scheduler, pressure, and transport parity

16. **`slow_recipient_holds_only_its_head`**: fill A's current outbound room,
    leave B writable, and publish two conversations. A retains its exact head;
    B progresses; no cancel, subscription shed, disconnect, response
    fabrication, or connection teardown occurs.
17. **`held_head_precedes_later_sequence_after_writable_ready`**: force holdback
    of N, add N+1, fire writable readiness, and observe N then N+1. Duplicate
    READY markers do not duplicate a volatile offer within one binding.
18. **`push_slice_budget_and_round_robin_are_exact`**: mix participant deliveries
    and observer pushes beyond the signed budget; assert their shared exact
    per-slice cap and one-per-ready-conversation first pass. Observer work gets no
    second allowance; readiness deterministically schedules the remainder.
19. **`tcp_and_websocket_publish_identical_participant_bytes`**: run the same
    outbox through both `DeliverySink` implementations and compare complete
    participant bytes and SDK-decoded values. Neither uses `Frame::Deliver` nor
    a nonzero generic stream.
20. **`oversize_is_config_corruption_not_pressure_policy`**: signed maximum
    variants fit an empty TCP and WebSocket sink. A deliberately corrupt durable
    oversize fixture fails typed before registration; a merely full current
    buffer holds back without teardown.
21. **`parked_connection_wakes_on_outbox_and_no_polling_occurs`**: park TCP and
    WebSocket processes, commit one eligible outbox row, and assert one coalesced
    READY wake, final-probe safety, publication, then stable slice counters while
    idle. No sleeps or repeated probes.

### Layer 4 — real socket, cold reopen, SDK, and G2

22. **`serverpush_sent_is_not_receipt_real_socket`**: over loopback, enroll
    sender plus two recipients, commit an ordinary record, receive exact
    `RemoteParticipantInbound::Push` on peers only, close one peer without ack,
    reattach it, and receive the same sequence again. Only its subsequent
    `ParticipantAck` prevents a second replay.
23. **`terminal_answer_precedes_independent_push_work`**: the sender receives its
    exact-token `RecordCommitted` and releases the SDK write-ahead slot even
    while a peer is held back. A fresh token commits on the same connection.
    No push enters response correlation and no producer fault becomes a silent
    answer loss. This is the named G2 oracle.
24. **`cold_reopen_reconciles_and_replays_all_record_shapes`**: commit each
    mapped record shape, stop/join/drop every client, supervisor, service, and
    store owner, reopen the same disk, reattach normally, and receive each
    unacknowledged recipient obligation in order. Pair wire observations with
    decoded v2 base and extension/outbox rows.
25. **`observer_progressed_fires_after_source_and_advance_flushes`**: arm recovery
    on a real connection and gate the participant source and observer Advance
    append/flush barriers independently. Source failure creates neither Advance
    nor push. Source flush followed by Advance append/flush failure publishes no
    push; stop/reopen then reconciles the missing Advance before authority or
    handshake publication. Advance flush followed by a pre-handoff process cut
    makes the reattached handshake report progressed. The live path delivers the
    exact fired payload once; a dead arm owner is never broadcast, and
    advance-first/reattach-first outcomes follow the observer-mutex ordering.
26. **`restart_between_delivery_and_ack_accepts`**: offer a committed obligation,
    restart and cold-reconcile the outbox before any duplicate offer, then submit
    the truthful cumulative ack. Durable-obligation testimony accepts and
    commits it although volatile-offered state is gone.
27. **`ack_after_reattach_before_replay_accepts_after_reconciliation`**: offer an
    obligation on epoch E, reattach at E+1, gate replay before the first offer on
    E+1, and submit the ack. Completed first-touch reconciliation supplies the
    durable obligation and the current binding authorizes acceptance.
28. **`endpoint_with_no_committed_obligation_refuses`**: construct internal
    non-obligation gaps and prove an ack may skip them only when it ends on a
    committed recipient obligation; ending on a sender-excluded or
    non-recipient sequence is exactly `AckGap`.
29. **`leave_discharges_the_left_identitys_obligations_and_bounds_live_payload`**:
    fill obligations to B; let B leave unacked; assert B's obligations discharge
    at the Leave extension commit, live payload returns under the signed bound,
    and append-only source/audit rows remain. Repeat fill → leave-unacked →
    reclaim → refill across permanent identities and assert the measured maximum
    never exceeds the ratified
    `checked(retained_capacity_bytes + measured fixed metadata term)` shape.
30. **`leave_discharge_replays_deterministically_across_the_commit_boundary`**
    [r4, ratifier's check]: the discharge crash cut, same species as tests
    26-28, pointed at the D3-4 Leave ruling. Cut (a): Leave's v2 row and its
    extension source batch both flush, obligations discharge, then crash before
    any further operation; cold restore replays the discharge deterministically
    from the Left row plus extension stream, the live payload projection
    recomputes under the signed bound, and source/audit rows are intact. Cut
    (b): crash BETWEEN the v2 Left flush and the extension append; cold first
    touch reconciles the missing Leave source batch per section 3.2 and the
    discharge holds with byte-identical results to cut (a). In both cuts a
    second restore is idempotent — asserted concretely: the extension stream's
    row count and head are unchanged by the second restore (whole-source
    uniqueness accepts the exact existing batch rather than appending another)
    and no discharged obligation reappears.

### Regressions and full gates

Keep the existing fatal-invariant regressions; do not invert them to fabricate a
value. The current production encode/dispatch harness is
`production/tests.rs:68-140`, the real participant socket harness is
`production/e2e_tests.rs:113-229`, and the cold-reopen harness is
`production/e2e_cold_tests.rs:45-184`. Extend those harnesses rather than replacing
them with direct state calls.

All commands must exit zero on the integrated build:

```text
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo check -p liminal-protocol --target wasm32-unknown-unknown --no-default-features
cargo check -p liminal-sdk --target wasm32-unknown-unknown --no-default-features
```

The protocol marker projection/codec touch makes both wasm/no-default legs
mandatory.

## Walls / laws

- **WALL-YG-560:** never merge, rebase, cherry-pick, or pull. Build and review
  from the named pin using ordinary commits only.
- **WALL-NO-PUBLISH:** no publish and no tag; publication remains lead-gated.
- **WALL-DURABLE-AFTER-COMMIT:** no outbox source batch before or inside the
  producing operation barrier; no push eligibility before the extension flush.
- **WALL-RECIPIENT-SNAPSHOT:** persist live bound recipients minus sender exactly
  once per projected record under the conversation lock; never recompute the
  historical set at send.
- **WALL-ACK-IS-RECEIPT:** offered/enqueued/written is never receipt. Only the
  committed ParticipantAck frontier proves receipt and reclaims by ack; the
  ratification-flagged permanent-Leave rule separately discharges the retired
  identity's forever-unreplayable obligations.
- **WALL-HOLDBACK:** current outbound pressure holds a recipient's head. No
  overflow cancellation, participant shed, synthetic answer, or pressure-driven
  connection teardown.
- **WALL-ORDER:** one recipient never observes a conversation out of
  `delivery_seq` order; no cross-conversation ordering promise is added.
- **WALL-REATTACH-REPLAY:** a new binding starts from durable ack, not an old
  volatile offered cursor.
- **WALL-MOVE-ONLY:** `ConversationOutbox`, producer/reconciliation commits,
  recipient-obligation owners, held participant heads, marker delivery
  projection, and every new linear wrapper are non-`Clone`, non-`Copy`, moved in
  once and out once. Existing copyable ids, epochs, wire values, and snapshots
  are not falsely claimed linear.
- **WALL-TYPED-REFUSAL:** protocol-legal ack/marker/capacity outcomes remain
  sealed typed responses. Schema/config/invariant faults fail loudly without a
  fabricated value.
- **WALL-LOUD-MIGRATION:** Unit 2 extension/outbox schema mismatches and any
  actual observer schema change reject old, unknown, malformed, or mixed rows
  deterministically before publication. No default, alias, compatibility
  decoder, or alternate stream key hides incompatible extension/observer rows;
  the participant base stream remains v2.
- **WALL-NO-PANIC:** no production `unwrap`, `expect`, or `panic`; no lint
  suppression, ignored test, silent fallback, sleep-based proof, polling loop,
  or periodically scanned producer. Pump and replay are request/readiness/event
  driven.
- **WALL-CONFIG-SIGNOFF:** Tom and Annabel sign every row/formula in the derived
  table before build dispatch. No recommendation is a default.
- **WALL-NO-DEFERRALS:** every normative item in this brief lands or the build
  stops with exact contradictory bytes/input. No TODO, compatibility shim, or
  narrowed acceptance claim substitutes for it.
- **WALL-PRE-REVIEW:** r3 folds the completed independent review of every anchor,
  universal claim, and D3/G1/G2 crash cut against `2bf71c4`. Any later build
  drift is rechecked and recorded here before dispatch.

## Out of scope

- Any change to the frame two-request snapshot semantics.
- Any participant base-log schema change, converter, dual decoder, or silent
  migration; the participant stream remains literal v2. A physical compactor is
  also outside the landed `DurableStore`; typed extension refusal and logical
  reclamation are in scope.
- Subscription `Frame::Deliver` semantics, subscription credit/ack/resume,
  schema negotiation, or its shed policy. Participant push only reuses the
  transport-neutral sink/readiness mechanics.
- Automatic SDK acknowledgement, application payload interpretation, exactly-once
  delivery, or a claim that duplicate replay before ack is an error.
- Inventing a `Died` commit from transport loss. The wire variant remains
  supported; a future protocol-owned Died operation is a separate lifecycle
  unit because no such committed source exists at `2bf71c4`.
- Dashboards, operator UI, manual outbox editing, publishing, or version tags.

## Open questions for the fold

Only questions 1 and 4 remain open, both for signatures. Questions 2, 3, and 5
are RULED and recorded here so build dispatch has one status ledger.

1. **Config signatures — OPEN:** Tom and Annabel must sign or veto every
   recommended value/formula and its stated worst-case cost in section 9.
2. **Observer targeting — RULED (pre-review + seat):** observer v1 stands and no
   durable arm target is added. A persisted connection incarnation cannot name a
   surviving socket; the live weak association and serialized crash cuts in
   section 8 are sufficient, and progressed handshakes recover durable Advance.
3. **MarkerAck body/home — RULED, home updated by r3:** the r2 body stands.
   `MarkerAckCommitted` persists the same census discipline as the landed
   `RecordAdmission` v2 row (`production/log.rs:250-296`): exact canonical
   request, receiving binding epoch, offered-marker delivery witness, and
   complete post-transition audit, with no derived-only fields. R3 places it in
   the schema-v1 Unit 2 extension stream with `{ base_log_head,
   extension_sequence }`; replay re-runs the authoritative selector and checks
   every audit (`production/ops_frontier.rs:287-385`). It does not alter the v2
   participant base enum.
4. **Canonical outbox encoding — OPEN:** Tom and Annabel sign the measured fixed
   metadata term after the build prototype reports exact bytes for every record
   kind and maximum recipient vector. No guessed byte constant may enter the
   implementation. Section 9's checked-assertion rider prints measured bytes and
   STOPS on any invalidated recommendation.
5. **Ack availability basis — RULED (pre-review + seat concur):** the basis is
   the reconciled durable-obligation prefix, with sealed endpoint-membership
   testimony as section 7 specifies. Burden allocation is recorded: the
   volatile-offered candidate had to preserve truthful receipt testimony across
   restart and reattach-before-replay, and failed because its only fact is erased;
   the durable candidate is grounded by the participant source barrier, while
   the ParticipantAck itself supplies receipt testimony and a non-obligation
   endpoint still returns `AckGap`.

**D3-4 ratification flag (not an open semantic branch):** the seat rules that
permanent Leave discharges the left identity's outstanding obligations. Waffles
and Tom must ratify that interpretation and the corresponding section 9 bound;
the rejected conservative formula remains printed there for the decision.

## Revision record

| revision | date | author | record |
|---|---|---|---|
| r1 | 2026-07-18 | implementation specialist | First pinned Unit 2 build brief: ruled D3 durable post-commit outbox, recipient snapshot, ordered readiness-driven holdback pump, ParticipantAck retention/reclaim, reattach replay, marker testimony, transport/SDK integration, loud v3 migration, layered acceptance, signed derived values, and explicit walls/out-of-scope. |
| r2 | 2026-07-18 | seat fold (Hermes Crumpet) | Fold pass on r1 with spot anchors re-verified at `2bf71c4` (ServerPush enum, MarkerAck factual-empty seam, eight-kind v2 census, signed config fields, `DELIVERY_SLICE_BUDGET`). Ruled Q3: `MarkerAckCommitted` row body fixed to the RecordAdmission census discipline. Ratified the participant v2→v3 loud migration as a D3-7 consequence (flagged for Tom/Annabel signoff). Opened Q5: `contiguously_available_through` basis (volatile-offered vs durable-obligation prefix) routed to pre-review with the seat's lean recorded; section 7 amended to carry both candidates. |
| r3 | 2026-07-18 | implementation specialist, pre-review fold under seat rulings | Folded all five major findings and the note. Replaced one-record Produced with source batches; reversed r2's v3 ratification after the adversarial independent-stream check and placed the ruled MarkerAck body in the Unit 2 extension stream; closed Q2 on observer-v1 live targeting and Q5 on durable-obligation testimony; recorded the discharge-on-Leave seat ruling and flagged it for Waffles/Tom ratification with both bounds visible; vendored the seat-verified frame excerpt. |
| r4 | 2026-07-18 | seat fold (Hermes Crumpet) | Added test 30, the ratifier's check on the discharge-on-Leave ruling (Waffles, on ratifying at his seat): the Leave commit-boundary crash cut in both forms — crash after both flushes, and crash between the v2 Left flush and the extension append with reconciliation repair — proving the discharge replays deterministically from the Left row, idempotent across a second restore. |
| r5 | 2026-07-18 | seat fold (Hermes Crumpet) | Re-review closure. The resumed lens confirmed five of six majors and the note closed, and caught the sixth surviving verbatim — a seat dispatch omission in the r3 amendment list, disclosed as such: test 12 still committed records to a detached recipient, contradicting D3-2. Corrected: records commit while bound and snapshot-included, detach precedes offer/ack, plus the complementary no-obligation-after-detach half. Also folded the lens's two mechanical items: header round label advanced, and test 30's idempotency made concrete (unchanged extension row count/head on second restore). |
