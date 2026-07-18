# F-0c Unit 1 — claim-frontier acquisition build brief

Base: `liminal` main at `8d2bfd3`.

Ruling provenance: Waffles, `#stack-devs`, 2026-07-18 04:31Z, message
`ad33b32b`, recorded for Tom's veto. D1 is explicit RecordAdmission wire
correlation now; D2 remains required deployment input; Unit 1 folds and builds
before Unit 2.

All repository anchors in this revision were read against the bytes at
`8d2bfd3`. Frame anchors name their own immutable frame commits.

## Goal

An authorized RecordAdmission commits durably and the requesting client
receives a correlatable terminal answer — no fully-authorized request path may
end in silent connection close.

## Acceptance lens

The normative observations are quoted verbatim from
`frame@86f6a7f:docs/briefs/F-0c-FINDINGS.md:142-157`.

> **Finding G1 — `Sent` is not receipt.** First observed by this run: after
> the server fail-closed A's connection, a follow-up
> `record_operation → send_operation` on the SAME dead connection reported
> `RemoteParticipantSendOutcome::Sent`. A TCP write against a peer-closed
> socket succeeds locally, so `Sent` is transport-write testimony only —
> NEVER evidence the server received the operation. Death is observable only
> on the next read. Pinned in test 3.

> **Finding G2 — no terminal answer, one write-ahead slot.** The server's
> fail-closed path sends nothing, so the client crate's outstanding
> write-ahead correlation for the admission is never answered; the aggregate
> holds its (cardinality ONE) expected-operation slot until a transport-loss
> fate clears it through the SDK's recovery surface. Whoever lands the
> frontier unit must make the terminal answer to an admission a value the
> client crate can correlate, or every failed admission strands the slot
> until a connection fate.

G1 means no send return value is an acceptance oracle. Receipt is proved only
when the SDK applies the exact terminal answer. G2 means the answer must clear
the one write-ahead expected-operation slot without waiting for a connection
fate.

The frame two-request snapshot rule remains binding but is not exercised by
this unit: a first uncovering snapshot permits exactly one further request; a
second consecutive uncovering answer is a terminal, visible protocol violation
(`frame@c6e3158:examples/frame-demo/PROTOCOL.md:69-79`). Nothing in frontier
restore, replay, or response correlation may weaken or restate that rule.

## Scope slices

Slices land in this order. A commit may combine adjacent slices when the
move-only types make separation dishonest, but no later slice may be enabled
before its durability and replay prerequisites are green.

### 1. Protocol — acquire and maintain live frontier ownership

**Choice: protocol-owned live transitions, not server-maintained restore
facts.** `ClaimFrontiers` is the non-`Clone` executable authority over validated
identity ranks, retained rows, and coupled sequence/order frontiers
(`crates/liminal-protocol/src/lifecycle/claim_frontier.rs:1302-1313`). Today its
public consuming surface is: initial enrollment and full restore
(`claim_frontier.rs:1877-1943`); ordinary-record projection, which consumes the
whole authority (`claim_frontier.rs:2118-2136`); Leave preparation, which
consumes the same authority (`claim_frontier.rs:2326-2404`); the public marker
drain `drain_next_marker(ClaimFrontiers, ClosureState) -> MarkerDrainCommit`
(`lifecycle/operations/marker_drain.rs:98-116`, re-exported at
`operations/mod.rs:33-35`); and the total `apply_record_admission` wrapper,
whose prestate owns the authority (re-exported at `operations/mod.rs:51-55`).
The five additions below are the MISSING lifecycle-transition families beside
that existing surface — the list above is the complete existing consuming
surface, not a claim that nothing else touches `ClaimFrontiers`. [r2]

The build SHALL add protocol-owned consuming transition surfaces for subsequent
enrollment, credential attach, detach, participant acknowledgement, and marker
acknowledgement. Each surface accepts one typed transition input coupling: the
existing opaque typed operation commit; the one current
frontier/closure/retention owner; and — for every transition that mints a
newly retained durable row — the exact server-computed canonical keyed
charge(s) for that row. The charge is a required separate input by protocol
design, not a convenience: payload bytes and storage framing belong to the
server durability schema (`ordinary_record_projection.rs:32-56` — 
`RetainedRecordCharge::new` requires an externally computed `ResourceVector`),
the row-producing commits carry no charge (`EnrollmentCommit` at
`lifecycle/enrollment.rs:292-302`; `AttachCommit` at `lifecycle/attach.rs:100-115`;
`CommittedDetachTransition` at `lifecycle/detach.rs:271-279`), and the existing
initial-frontier API already takes `attached_charge` separately
(`claim_frontier.rs:1888-1898`). Commit-plus-owner is therefore NOT the
complete input set; describing it as such was an r1 defect. [r2] Each surface
returns either that complete unchanged owner or one complete poststate owner.
It SHALL NOT accept caller-selected raw claims, row lists, cursor positions,
or ledgers; the keyed charge input names its exact row key and is validated
one-for-one, never a substitute for protocol-derived facts.

This choice keeps lifecycle rules in `liminal-protocol`. Making the server
manufacture `ClaimFrontiersRestore` after every live event would duplicate the
same provenance and cross-counter rules that restore validates, and would let
independently produced facts approach an executable authority boundary.
`FrontierParticipant` couples each permanent participant/cursor to an exact
bound or detached `BindingEpoch` (`claim_frontier.rs:73-122`), while the public
cold capsule validates participant history, frontiers, and closure together and
forbids component splicing (`crates/liminal-protocol/src/lifecycle/storage.rs:87-173,
809-852`). Live transitions SHALL preserve that discipline:

- the current authority is moved in once and moved out once;
- conversation id, participant identity, binding epoch, cursor, retained row,
  sequence owner, order owner, closure accounting, and retained charge cannot
  be supplied as separable substitutes;
- a refusal returns the exact unchanged owner; a commit returns the exact
  poststate; a fault returns no fabricated replacement;
- initial enrollment remains derived from the admitted operation, not a raw
  restore. Its existing atomic wrapper already owns operation, frontier,
  closure accounting, and retained `Attached` charge
  (`claim_frontier.rs:1315-1372`);
- cold replay of the same operation history must produce a state equal to live
  transitions, and mixed-history/mixed-conversation components must fail.

`ClaimFrontiersRestore` remains the cold-storage shape. It includes the owning
conversation, active identities, identity cap, retained floor and records,
active and historical marker facts, both frontier restores, and recovery marker
selector (`claim_frontier.rs:1134-1173`). It is not the server's per-operation
mutation API. This closes the exact missing-live-transition and missing-durable-
facts boundary recorded at
`docs/design/LP-GAP-CLOSURE-GOAL.md:151-166`.

### 2. Protocol — D1 explicit RecordAdmission correlation

D1 is ruled. Add this concrete wire form:

1. Define `RecordAdmissionAttemptToken` as the same opaque fixed-width
   `[u8; 16]` newtype pattern used by `EnrollmentToken`,
   `AttachAttemptToken`, `DetachAttemptToken`, and `LeaveAttemptToken`
   (`crates/liminal-protocol/src/wire/primitives.rs:106-152`). Export it from
   `wire/mod.rs` beside those token types (`wire/mod.rs:39-47`).
2. Add
   `pub record_admission_attempt_token: RecordAdmissionAttemptToken` to
   `wire/request.rs::RecordAdmission`, after `capability_generation` and before
   `payload`. The current four-field body is at `wire/request.rs:89-100`; the
   fixed token precedes the only length-delimited field. The client chooses the
   token for this request attempt.
3. Add the same public field, in the same position after
   `capability_generation`, to `RecordAdmissionEnvelope`. The payload remains
   deliberately absent. Today this common response envelope has only
   conversation, participant, and generation
   (`crates/liminal-protocol/src/wire/envelope.rs:82-91`).
4. Every semantic terminal RecordAdmission answer SHALL echo the request's
   exact token through that common envelope. This covers the exhaustive sealed
   `RecordAdmissionResponse` set: connection capacity, order exhaustion,
   participant unknown, no binding, stale authority, retired, marker-closure
   capacity, committed, too large, sequence exhaustion, and observer
   backpressure
   (`wire/authority/records.rs:14-164`). Transport rejection before a
   RecordAdmission body is decoded is not a semantic RecordAdmission answer.
5. In request encoding, write the fixed 16 bytes after generation and before
   the existing payload length/body; in request decoding, read them in that
   position. The exact sites are `wire/codec.rs:462-506` and
   `wire/codec.rs:523-588`. In response encoding/decoding, add the token to
   `put_record_admission` and `take_record_admission`; every nested
   RecordAdmission-capable envelope routes through those shared helpers —
   the helpers sit at `wire/server_codec.rs:383-492,1260-1348` and their
   nested/specialized call sites are `:1375-1377` (participant reference),
   `:1402-1404` (binding required), `:1424-1426` (order allocating),
   `:1445-1447` (closure checked), `:1463-1465` (sequence allocating),
   `:2042-2045` (stale authority), and `:2294+` (observer backpressure);
   pre-review independently searched for a bypass and found none. [r2] Update
   all request/envelope constructors, including protocol lookup
   (`lifecycle/lookup.rs:1033-1053`), the total selector, production envelope
   construction, codec fixtures, and golden lengths. There is no optional
   token and no payload/hash echo.
   **Legacy-byte honesty [r2]:** the staged in-place layout is NOT
   byte-disjoint from the r1-claimed universal rejection. The old request
   suffix is `u32 payload_len || payload` (`wire/codec.rs:501-506`) and the
   token macro accepts every `[u8;16]` (`wire/primitives.rs:106-130`), so an
   old request whose 16-byte payload ends in four zero bytes decodes under the
   new schema as a valid token plus empty payload. No legacy decoder, dual
   schema, or compatibility shim is supported — but the brief does NOT claim
   every tokenless body fails decode, and no acceptance criterion may rest on
   that false universal. This is acceptable because the surface is
   staged-unpublished: no deployed peer has ever emitted the old body.
6. Change the client key from
   `Record(conversation, participant, generation, local_authorization)` to
   `Record(conversation, participant, generation,
   RecordAdmissionAttemptToken)`. Enrollment/attach/detach/leave already key
   correlation with their request tokens
   (`crates/liminal-protocol/src/client/correlation.rs:10-73`). Remove the
   RecordAdmission hard-false in `matches_request`; extract the wire token from
   every nested `RecordAdmissionEnvelope` instead of the current synthetic zero
   (`client/correlation.rs:91-101,365-480`). ALSO in scope [r2]:
   `response_key` currently rewrites every extracted `RequestKey::Record(..., _)`
   to `ambiguous_authorization` (`client/correlation.rs:159-245`, rewrite at
   `:236-242`) — delete that Record-specific rewrite (the sealed local
   authority is independently enforced by `decide_correlated_inbound` before
   wire matching), while retaining ObserverRecovery's local-authorization
   handling unchanged. The process-local sealed
   `ClientResponseCorrelation` remains a required authority; the wire token is
   an additional necessary equality, not a replacement for that authority.
7. Replace the unconditional RecordAdmission `AmbiguousResponse` branch at
   `client/inbound.rs:214-223` with the ruled flip: an exact token match follows
   the normal apply path and clears `aggregate.expected`; a RecordAdmission
   response with the same request class but no matching token is refused as
   `AmbiguousResponse`, preserving the aggregate, value, and local correlation.
   Unsealed inbound continues to fail the existing response-authority gate.

This form is chosen over a server operation id or transport-provenance-only
exception because the existing enrollment/attach/detach/leave contract already
uses client-chosen, fixed 16-byte request tokens as terminal response identity.
Putting the new token in `RecordAdmissionEnvelope` makes every legal terminal
variant carry the same identity through one codec seam, while the opaque
payload remains secret from response envelopes. The local sealed authority and
the echoed wire token prove different facts; acceptance requires both.

**Resume impact is part of D1.** `decode_expected` already delegates nested
request bytes to the shared wire decoder
(`crates/liminal-protocol/src/client/resume_decode.rs:102-129`), so its canonical
fixtures must absorb the new field. RecordAdmission is no longer tokenless:
remove it from EVERY production tokenless classification, of which there are
FIVE, not two [r2]: the `TokenlessAfterCrash` abandonment classification in
restore (`client/resume.rs:129-203`, exact match `:178-183`); the
lost-testimony coupling gate (`client/resume.rs:371-380` — without this edit
the FIRST restore of an issued RecordAdmission mints
`IssuedOperationCorrelation` testimony at `:195-204`, and the SECOND restore
of that persisted state rejects it as `LostAuthorityTestimonyMismatch`); the
pending-abandonment conflict match (`client/resume.rs:398-404`); the permitted
abandonment decoder (`client/resume_decode.rs:262-307`, exact match
`:279-295`); and the record-gate abandonment refusal
(`client/barrier.rs:475-485`). Only ObserverRecovery remains in each class.
An issued restored RecordAdmission keeps its expected operation and receives
the existing lost-correlation testimony; this unit does not remint an
already-issued send. No dual decoder or compatibility shim is added; the
legacy-byte honesty note in item 5 governs what old bytes may still decode
as. [r2]

This is absorbed by the staged, unpublished `0.2.0` protocol surface. Keep the
existing request/response discriminants and replace their staged body schema in
place: no published-version dance, optional legacy body, parallel tag, publish,
or tag. Publish remains lead-gated. Tom SHALL see the wire diff and codec tests
before any publish action.

### 3. Server — own executable state and rebuild it on cold replay

Extend `ConversationAuthority` with one move-only executable owner containing
`ClaimFrontiers`, current closure accounting/state, keyed retained charges, and
any retained-row cap facts needed to serialize the next snapshot. The current
authority owns shell, live slots, token indices, next order/sequence/participant,
log head, and observer progress, but none of frontier/closure/retained-charge
state (`crates/liminal-server/src/server/participant/production/state.rs:119-140`).
The existing `observer_progress` remains the hard-observer fact; do not add a
second counter.

The owner is absent only before an initial enrollment has durably established
it. Initial enrollment acquires it through the protocol's atomic initial
frontier result. Every subsequent enrollment/attach/detach/ack transition moves
it through Slice 1. RecordAdmission and Leave temporarily take the owner,
return it unchanged on typed refusal, or install the exact committed poststate.
A protocol/durability fault follows the existing rule: discard the possibly
part-consumed in-memory authority and cold-replay durable truth.

Cold replay SHALL reconstruct executable state by replaying the protocol-owned
live transitions, not by having server code assemble raw claim unions. The
handler already replays every registered conversation at startup and cold
replays on first touch (`production/handler.rs:57-116,138-195`). Startup fails
loudly if a durable conversation cannot restore; no partially restored owner is
published.

### 4. Server — one durable transaction and a loud schema migration

The current log has a same-stream `schema_version: u8` field and rejects any
value other than `SCHEMA_VERSION`; appending one JSON entry and flushing is the
publication barrier (`crates/liminal-server/src/server/participant/production/log.rs:24-45,
89-140`). Its operation enum has exactly Genesis, Enrolled, Attached, Detached,
and ZeroDebtAck (`production/log.rs:143-200`).

**Migration choice: VERSION LOUDLY.** Bump the participant production log schema
from v1 to v2 while retaining the existing stream key. Existing logs at the old
five-kind schema MUST fail replay with
`OperationLogError::SchemaVersion(1)` before any authority is published. Do not
change the stream prefix, treat missing new fields as defaults, alias the old
shape, or reinterpret v1 bytes as v2. A mixed stream fails at its first v1 row.
There is no automatic migration tool in this unit; old v1 logs are rejected
loudly and visibly.

The v2 schema SHALL add durable rows for:

- every frontier-affecting lifecycle transition needed by Slice 1;
- a marker-drain poststate when RecordAdmission selects `DrainFirst`;
- one committed RecordAdmission containing the payload-bearing record, exact
  token-bearing outcome identity, resulting frontier/ledger/floor/closure/
  retained-charge/accounting state, and connection-capacity effect;
- the authorized Leave commit and its exact retired/tombstone replay facts.

On successful admission, consume
`RecordAdmissionCommit::into_persistence_parts()` exactly once. Its fields are
the outcome, durable record, connection-capacity commit, order and sequence
allocations, observer/closure permits, complete frontiers, floor, retained
charge, baseline, accounting, required-capacity plan, caller row/charge,
retained charges, and marker candidates
(`crates/liminal-protocol/src/lifecycle/operations/record_admission.rs:297-350,
401-439`). Serialize one v2 stored operation from those authorities, append it
at the optimistic head, and flush it as ONE atomic transaction. Only after the
flush succeeds may the server install the poststate, advance tracking/log head,
and return `RecordCommitted`. No frontier is cloned, no response precedes the
flush, and no second append carries part of the same commit.

`RecordAdmissionDecision` is handled exhaustively
(`record_admission.rs:490-500`):

- `Respond` restores the unchanged owner and emits its typed value;
- `DrainFirst` durably applies the protocol-owned mandatory marker drain, then
  retries the same unchanged request under the same conversation lock;
- `Commit` performs the one append/flush transaction above;
- `Fault` returns a semantic error, fabricates no value, and leaves durable
  state authoritative.

The marker drain today is an opaque atomic frontier/closure/successor commit
whose consuming decomposition returns ONLY
`(ClaimFrontiers, ClosureState, StoredEdge)`
(`lifecycle/operations/marker_drain.rs:50-89`, entry `:98-140`). **That
decomposition is INSUFFICIENT for the same-lock retry as it stands [r2]:** the
retry's `RecordAdmissionPrestate` requires complete `ClosureAccounting` and
keyed `retained_charges` (`record_admission.rs:43-56,63-90`),
`ClosureAccounting` has private fields with read-only accessors
(`closure_accounting.rs:150-207`), and no existing producer applies a
marker-drain accounting/charge transition — rebuilding it server-side through
`try_new` would be exactly the raw-fact reassembly Slice 1 forbids. The build
SHALL therefore extend or compose the marker-drain seam in `liminal-protocol`
so that draining consumes the current `ClosureAccounting`, the current keyed
retained charges, and the exact server-computed canonical marker-row charge,
and returns the COMPLETE updated frontier/closure-accounting/retention owner
that the retry consumes directly. `DrainFirst` retries only with that
protocol-produced complete owner. Unit 1 persists the resulting pending marker
work; Unit 2 owns socket push production.

Change the RecordAdmission and Leave closures to pass `appender`, not the
current `_appender` (`production/handler.rs:408-426`). Add a migration test that
writes a literal v1 entry under the existing stream key and proves startup or
first touch reports schema version 1 without publishing an empty/new authority.

### 5. Server — required deployment configuration and real consumers

`RecordAdmissionPrestate` consumes this exact set of facts
(`crates/liminal-protocol/src/lifecycle/operations/record_admission.rs:37-90`):

| prestate fact | owner/source in the build |
|---|---|
| request | decoded wire request, including the D1 token |
| presented identity and current binding | `ConversationAuthority` live slot |
| receiving binding epoch | connection incarnation plus presented generation |
| connection tracking and capacity | existing per-connection state and `ParticipantConfig` |
| closure accounting | durable executable conversation owner |
| maximum ordinary-record charge | new required deployment configuration |
| `ClaimFrontiers` | durable executable conversation owner |
| keyed retained charges | canonical durable rows/log poststate |
| observer progress | existing durable hard-observer progress |
| ordinary projection limits | new required deployment configuration |

The server SHALL add the following flat, required `[participant]` keys. There
is deliberately no value/default column: every value is a required input owned
by Tom/Annabel.

| required key | protocol destination | type and unit | honest admissible bound |
|---|---|---|---|
| `max_ordinary_record_entries` | `max_ordinary_record_charge.entries` | `u64`, encoded durable entries per ordinary record | `1..=u64::MAX`; must admit at least the one caller row |
| `max_ordinary_record_bytes` | `max_ordinary_record_charge.bytes` | `u64`, encoded durable bytes per ordinary record | `1..=u64::MAX`; validated against canonical encoded charge |
| `max_generated_marker_entries` | `projection_limits.marker_max.entries` | `u64`, encoded durable entries per generated marker | `1..=u64::MAX`; must cover the canonical marker row |
| `max_generated_marker_bytes` | `projection_limits.marker_max.bytes` | `u64`, encoded durable bytes per generated marker | `1..=u64::MAX`; must cover the canonical marker row |
| `mandatory_transaction_bound_entries` | `projection_limits.mandatory_bound.entries` | `u64`, retained entries in mandatory envelope `Q` | `1..=u64::MAX` [corrected r4 — the r1 `0..=` claim was false at the bytes: initial enrollment requires the one-entry Attached charge to fit inside `Q` (`enrollment_closure.rs:604-621`), so zero cannot initialize a conversation] |
| `mandatory_transaction_bound_bytes` | `projection_limits.mandatory_bound.bytes` | `u64`, retained bytes in mandatory envelope `Q` | must admit the canonical Attached row bytes (`enrollment_closure.rs:616-621`) [corrected r4] |
| `full_recovery_claim_entries` | `projection_limits.full_recovery_claim.entries` | `u64`, transferable recovery occupancy `K`, entries | MUST EQUAL `Q.entries` — current construction rejects `mandatory_bound != recovery_claim` (`enrollment_closure.rs:528-530`) [corrected r4] |
| `full_recovery_claim_bytes` | `projection_limits.full_recovery_claim.bytes` | `u64`, transferable recovery occupancy `K`, bytes | MUST EQUAL `Q.bytes` (same constraint) [corrected r4] |
| `retained_capacity_entries` | `ClosureAccounting.configured_cap.entries` | `u64`, total retained durable entries | computed initial/live baseline `..=u64::MAX`; startup rejects a cap below baseline |
| `retained_capacity_bytes` | `ClosureAccounting.configured_cap.bytes` | `u64`, total retained encoded bytes | computed initial/live baseline `..=u64::MAX`; startup rejects a cap below baseline |
| `max_retained_record_rows` | `ClaimFrontiersRestore.retained_record_limit` | `u64`, retained causal-row count | current required retained rows `..=u64::MAX`; no truncation to fit |
| `closure_episode_churn_limit` | `ClosureAccounting.episode_churn_limit` | `u64`, closure churn cycles per episode | `2..=u32::MAX` [corrected r4 — the r1 `1..=u64::MAX` bound was false at the bytes: initial enrollment enforces `2..=MAX_CHURN_LIMIT` with `MAX_CHURN_LIMIT = u32::MAX` (`enrollment_closure.rs:26-27,519-524`)] |

`ResourceVector` is exactly two `u64` components, entries and encoded bytes
(`crates/liminal-protocol/src/algebra/types.rs:3-24`). The three projection
vectors are exactly marker maximum, mandatory bound, and full recovery claim
(`lifecycle/operations/ordinary_record_projection.rs:78-118`). Closure
accounting rejects zero churn limits, use above the signed churn limit, and a
baseline above either configured capacity component
(`lifecycle/closure_accounting.rs:44-111`). The retained-row count is a separate
cold-restore bound (`lifecycle/claim_frontier.rs:1134-1173`); it is not inferred
from bytes.

All keys are serde-required, have no defaults, and receive accumulated semantic
validation. This follows the present `ParticipantConfig` contract: required
fields, `deny_unknown_fields`, no assumed defaults, and no inert keys
(`crates/liminal-server/src/config/types.rs:422-499`). The server computes
actual row charges from canonical durable bytes; it never substitutes a
configured maximum for an actual charge.

**Dispatch gate:** the build CANNOT dispatch this config slice, and therefore
cannot dispatch the integrated Unit 1 build, until Tom and Annabel sign every
key above and its validation bounds. A blank, omitted, zero-by-accident, or
invented value is not permission to proceed.

### 6. Server — replace the authorized fall-throughs; Leave rides

For RecordAdmission, replace only the fully authorized fall-through at
`crates/liminal-server/src/server/participant/production/ops_frontier.rs:151-173`
with construction of the complete prestate and the total selector
`apply_record_admission` (`crates/liminal-protocol/src/lifecycle/operations/record_admission.rs:543-710`).
The existing lookup and stage-6 typed responses remain crate-selected. The
record's exact durable encoded charge is computed before size admission from
the v2 canonical row representation. Every terminal `Respond` and `Commit`
emits the D1 token-bearing envelope.

**Leave call: Leave rides in Unit 1.** This is not a one-line sibling fix. The
sized surface is: the authorized bound/detached invariant in
`ops_frontier.rs:97-114`; the unused appender in `production/handler.rs:408-415`;
the existing consuming settled/pending frontier preparation APIs
(`claim_frontier.rs:2326-2404`); the consuming `commit_leave` and
`commit_pending_leave` paths
(`crates/liminal-protocol/src/lifecycle/membership.rs:823-923`); the five-kind
log that lacks Leave; and server state that currently contains only live slots
(`production/state.rs:70-140`). The incremental work is one v2 Leave durable
row, exact retired/tombstone replay state, appender plumbing, and bound,
detached, retry, and cold-reopen tests. It rides because frontier ownership and
the schema transaction are the dominant shared work, and preserving this
second authorized invariant would knowingly retain the same silent-close class
after the acquisition exists.

Authorized bound and detached Leave SHALL run stage 6, consume the acquired
frontier through the applicable settled/pending preparation and protocol
commit, append/flush one v2 transaction, then return the existing token-correlated
`LeaveCommitted`. The exact committed Leave token/body replay and conflict
classification are in scope because durable retry correctness requires them.
General retired-participant response routing for other operations is not.

### 7. SDK/client — apply the answer and release the slot

The SDK continues to install its write-ahead correlation only after the exact
transport write succeeds (`crates/liminal-sdk/src/remote/participant.rs:357-397`).
`Sent` does not mutate the expected operation into receipt. On receive, an exact
local correlation plus exact D1 wire token applies the `ServerValue`, persists
the resulting aggregate, and leaves `state.correlation` empty; a refusal
restores both aggregate and correlation unchanged
(`remote/participant.rs:401-468`).

Acceptance requires an exact-token terminal RecordAdmission answer to release
the cardinality-one slot. Before that answer, a second write-ahead operation is
refused as already outstanding. After it, a second RecordAdmission with a fresh
token is recordable, sendable, and answerable on the same connection. A
mismatched token never releases the slot.

## Acceptance tests

No criterion is satisfied by inspecting a send outcome, an in-memory state
before flush, or a hand-constructed server value.

### D1 flip test — protocol/client, committed positive and negative halves

Add one named test module covering all of the following:

1. An issued RecordAdmission and a terminal answer with the same conversation,
   participant, generation, and `RecordAdmissionAttemptToken` are accepted by
   `decide_correlated_inbound`; `aggregate.expected` becomes `None`.
2. With the same local response authority and same non-token identity, an
   answer carrying a different RecordAdmission token is refused as
   `AmbiguousResponse`; the expected operation and local correlation remain.
   This negative half is a committed regression test, not a temporary old-wire
   fixture.
3. Calling unsealed `decide_inbound` for a RecordAdmission answer remains
   refused for missing response authority.
4. Each of the eleven legal `RecordAdmissionResponse` outcomes round-trips
   through the client-direction server-value codec with the exact token.
5. The named checked-in legacy fixtures (the r1 canonical RecordAdmission
   request/response bodies) fail canonical decode under the staged schema.
   [narrowed r2] This is a fixture assertion, not a universal: per the Slice 2
   legacy-byte honesty note, old arbitrary-payload bytes are not guaranteed
   byte-disjoint from the staged layout, and no criterion rests on that.
6. LPCR expected-operation round-trip retains the token. Restore no longer
   emits `TokenlessAfterCrash` abandonment for RecordAdmission, while
   ObserverRecovery still does.
7. Two-cycle resume soak [r2]: restore an issued RecordAdmission with lost
   testimony, persist the resulting state, restore AGAIN — the second restore
   succeeds with the expected operation and testimony intact (this is the
   exact `LostAuthorityTestimonyMismatch` path the five-site removal exists
   to fix). The barrier half runs with a restored ObserverRecovery abandonment
   PRESENT [r3 — "absent" was a coverage defect: with no abandonment stored,
   the pre-r2 gate at `barrier.rs:475-485` already admits and the test would
   never exercise the fifth removal site]: while that abandonment remains
   stored, a fresh RecordAdmission is admitted, and a separate assertion keeps
   a fresh ObserverRecovery refused by that same stored abandonment.

The positive half flips the deliberate refusal at
`crates/liminal-protocol/src/client/inbound.rs:214-223`; the negative half
proves that only the ruled correlated form was admitted.

### Layer 1 — selector and production lookup matrix

Extend the existing production binding matrix at
`crates/liminal-server/src/server/participant/production/tests_binding.rs:474-550`.
Preserve unknown, stale, and no-binding rows; assert their echoed D1 tokens.
Add authorized-at-capacity refusal and authorized commit rows. Add protocol
selector cases for too-large, order exhaustion, sequence exhaustion, observer
backpressure, marker-closure refusal, `DrainFirst`, and commit. For every
`Respond`, assert unchanged frontier/closure/retained ownership; for every
commit, assert the exact poststate owner.

Add transition-history tests for initial enrollment, subsequent enrollment,
attach, detach, participant ack, marker ack, two ordinary records, and Leave.
For each history, live transition state and cold restore state are equal.
Compile-fail or privacy tests prove a frontier, closure state, retained charges,
and participant history cannot be recombined from different owners.

Leave joins this layer: authorized bound and authorized detached rows return
`LeaveCommitted`; exact-token replay returns the same terminal result; token
body conflict remains typed; stage-6 capacity refuses before commit.

### Layer 2 — production dispatch, wire, durability, and migration

Use the real encode → `dispatch_generic_frame` → decode harness at
`crates/liminal-server/src/server/participant/production/tests.rs:68-135`.
The following are required:

- authorized RecordAdmission encodes and decodes `RecordCommitted` with the
  exact token and assigned delivery sequence, while the one atomically stored
  durable record retains the exact request payload;
- connection capacity, record size, observer, closure, order, and sequence
  outcomes encode their exact typed refusals with the token;
- every answered path is `ParticipantDispatch::Respond`, never fatal/close;
- a commit is not returned until the append and flush complete;
- injected append or flush failure publishes neither response nor in-memory
  poststate, and cold replay sees no partial record;
- one committed row advances frontier, closure, retained charges, sequence,
  order, and log head atomically;
- `DrainFirst` persists its whole marker poststate before retrying and does not
  publish a participant push;
- a literal same-stream v1 five-kind row fails with
  `SchemaVersion(1)`; empty streams and all-v2 streams restore; a mixed stream
  fails at its first old row;
- authorized bound/detached Leave appends one transaction and answers
  `LeaveCommitted`; cold replay preserves exact-token retry.

### Layer 3 — real socket, cold reopen, and SDK slot release

Build from the loopback socket, participant codec, and production service setup
at
`crates/liminal-server/src/server/participant/production/e2e_tests.rs:113-229`.
**Harness note [r2]:** the current socket fixture constructs store, handler,
service, supervisor, and connection inline (`e2e_tests.rs:195-211`) and only
drops the client and shuts the supervisor at the end (`:367-369`) — it has NO
cold-reopen capability. Step 6 below therefore requires extending the socket
harness with a start/stop/join/drop/reopen helper that deterministically stops
and drops the first client, connection handle, supervisor, services, and every
store `Arc` owner before the second disk open, borrowing the scoped
disk-reopen pattern already proven in the non-socket harness
(`production/tests.rs:143-210` — first scoped open `:151-153`, scope close
`:205`, same-directory second open and rebuilt handler `:207-210`) [r3]. Replace the current records-blocked leg
(`e2e_tests.rs:264-282`) with an authorized request. The test SHALL:

1. enroll and bind over a real socket;
2. record and send an authorized RecordAdmission with token A;
3. assert `Sent` only means the write completed and the write-ahead slot remains
   occupied;
4. receive `RecordCommitted` by content with token A and the assigned sequence;
5. prove the connection survives by committing token B on that same socket;
6. stop the server, reopen the same disk store, rebuild the production handler,
   reconnect/attach through normal protocol operations, and commit token C;
7. prove token C's delivery sequence follows the first two commits, and pair
   that wire assertion with Layer 2's durable-order assertion, so replay rebuilt
   frontier and retained state rather than starting fresh;
8. run authorized bound and detached Leave on real sockets, receive
   `LeaveCommitted`, and prove the connection remains usable through the
   applicable terminal/replay path.

The SDK leg uses `RemoteParticipantHandle`, not direct aggregate calls. A test
whose name contains `sent_is_not_receipt` SHALL show that the second
write-ahead operation cannot be recorded after `send_operation` reports
`Sent`, then becomes recordable only after `receive` applies the exact-token
answer. The follow-up operation succeeds on the same connection. This is the
G1/G2 acceptance oracle.

### Regressions and full gates

Keep semantic invariants fatal without fabricating a protocol value. The
existing dispatch regression is
`crates/liminal-server/src/server/participant/dispatch_tests.rs:185-209`; the
connection-process regression is
`crates/liminal-server/src/server/connection/process_tests.rs:1296-1320`.
Neither is inverted or deleted. Add separate production-authorized tests for
the now-answered path.

All of these commands exit zero on the integrated commit:

```text
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo check -p liminal-protocol --target wasm32-unknown-unknown --no-default-features
cargo check -p liminal-sdk --target wasm32-unknown-unknown --no-default-features
```

The wire touch makes both wasm/no-default legs mandatory.

## Walls / laws

- **WALL-YG-560:** never merge, rebase, cherry-pick, or pull into the build
  branch. Build from the named base and use review/fold commits only.
- **WALL-NO-PUBLISH:** no publish and no tag. `0.2.0`/`0.3.0` stay staged; the
  D1 wire change is absorbed before publication. Tom sees it first; publish is
  lead-gated.
- **WALL-D1-REQUIRED:** no tokenless, optional-token, payload-echo,
  transport-provenance-only, or dual-schema RecordAdmission correlation.
- **WALL-D1-FLIP:** exact request-token echo plus sealed local authority may
  apply; mismatched/uncorrelated answers remain refused and retain the slot.
- **WALL-ATOMIC-RECORD:** all `RecordAdmissionPersistenceParts` cross one
  append/flush boundary. No response or poststate is published before it.
  Honesty note [r2]: the durable store exposes append and flush as separate
  calls, so a flush error leaves the commit outcome UNCERTAIN — acceptance
  tests assert that no partial row or in-memory poststate is observable after
  an injected failure, and must NOT assume a successfully appended full row
  was rolled back.
- **WALL-LOG-V2:** v1 five-kind logs VERSION LOUDLY and fail; no silent
  reinterpretation, new stream key, serde default, or compatibility fallback.
- **WALL-MOVE-ONLY [narrowed r2]:** no `Clone`/`Copy` on the executable linear
  authorities: `ClaimFrontiers`, `ValidatedMarkerCandidate`/`ValidatedMarkerRecord`,
  `MarkerDrainCommit`, `RecordAdmissionCommit`/`RecordAdmissionPersistenceParts`,
  `LeaveCommit`, the initial-enrollment frontier commit, and EVERY owner,
  transition-input, and persistence wrapper this unit adds. Existing copyable
  value snapshots (`StoredEdge`, `ClosureState`, `ClosureAccounting` —
  `edge.rs:662-683`, `closure_accounting.rs:44-60`) and existing low-level
  operation commits (`EnrollmentCommit`, `AttachCommit`, `ParticipantAckCommit`,
  `MarkerAckCommit`, `CommittedDetachTransition`) already derive `Clone`/`Copy`
  at base and are exempt; the r1 universal form was false at the pin. No
  raw-fact server transition path.
- **WALL-CONFIG-SIGNOFF:** no invented defaults or placeholder values. Tom and
  Annabel sign every D2 field before the config/integrated build dispatch.
- **WALL-TYPED-REFUSAL:** all legal capacity/size/observer/closure/exhaustion
  outcomes use sealed typed responses. Internal invariant faults close without
  fabricated values.
- **WALL-NO-PANIC:** no production `unwrap`, `expect`, or `panic`; no lint
  suppression, ignored test, sleep-based proof, polling loop, or silent
  fallback.
- **WALL-UNIT2:** persist pending marker/delivery facts needed by atomic state,
  but do not build the ServerPush socket producer in Unit 1.
- **WALL-NO-DEFERRALS:** every item in this brief lands or the build stops with
  the exact contradictory bytes/required input. Scope is not silently narrowed.
- **WALL-PRE-REVIEW:** after fold and before build dispatch, an independent Sol
  review checks this brief against `8d2bfd3`: every criterion implementable at
  its anchors and every universal claim held. Amendments land in this file,
  never only in scratchpads or envelopes.

## Out of scope

- The ServerPush producer, recipient fan-out, delivery scheduler, and socket
  publication path are Unit 2. Unit 1 may persist work that Unit 2 will consume.
- General retired-participant response routing is out of scope. The minimal
  retired/tombstone state required for an authorized Leave commit and exact
  Leave retry is in scope; do not use that necessity to activate unrelated
  retired rows.
- Participant configuration persistence UI/tooling, deployment editors,
  observability dashboards, and console surfaces.
- A v1-to-v2 participant-log converter. Unit 1 rejects v1 loudly.
- Any frame snapshot semantic change; the frame two-request rule is only a
  non-contradiction boundary here.
- Publishing, version tags, or a second wire-version compatibility track.

## Required-input holes

### D2 deployment values — blocking

The twelve required `[participant]` keys in Slice 5 have types, units, and
bounds but intentionally contain no values in this brief. Required owners:

| decision | Tom | Annabel | dispatch effect |
|---|---|---|---|
| ordinary-record entry/byte maximum | sign | sign | blocks config and integrated build |
| generated-marker entry/byte maximum | sign | sign | blocks config and integrated build |
| mandatory-envelope entry/byte bound | sign | sign | blocks config and integrated build |
| full-recovery entry/byte claim | sign | sign | blocks config and integrated build |
| retained entry/byte capacity | sign | sign | blocks config and integrated build |
| retained causal-row cap | sign | sign | blocks config and integrated build |
| closure episode churn limit | sign | sign | blocks config and integrated build |

Sign-off must state the exact numeric value for every key and confirm each
unit. Until signed, the build CANNOT dispatch its config slice and
CANNOT claim Unit 1 accepted. There are no fallback values.

#### Derived value proposal [r4] — awaiting Tom's confirm-or-veto

Signature chain as ruled (Waffles 05:30Z under Tom's delegation; Annabel
deferred her half to Tom in-session ~05:35Z): Tom's direction — "no arbitrary
limits, no arbitrary defaults, so long as things make sense, give users a good
experience" — authorizes DERIVATION from real quantities, not invention. The
quantities below come from the byte-grounded extraction (Sol research session
455999d1, envelope `claude-research-f0c-d2.mx8rrN`, run against the code pin;
its two Slice 5 bound contradictions are byte-confirmed and corrected above).

| key | proposed value | derivation (one line) |
|---|---:|---|
| `max_ordinary_record_entries` | 1 | Exact protocol constant, not a choice: the ordinary fixed point rejects any encoded caller charge whose `entries != 1` (`ordinary_record_projection.rs:897-937`). |
| `max_ordinary_record_bytes` | 131,072 | 2× the deployment-precedent complete-frame limit 65,536 (payload ≤ frame−44, `wire/codec.rs:501-506`): every payload the transport can carry admits with 2× headroom for v2 framing; ≪ the 4 MiB outbound sink, so deliveries always fit. The transport refuses first — a user can never feel this cap. |
| `max_generated_marker_entries` | 1 | Exact: one drain mints exactly one retained marker row, and every retained keyed charge must have `entries == 1` (`claim_frontier.rs:2778-2817`; `ordinary_record_projection.rs:909-929`). |
| `max_generated_marker_bytes` | 4,096 | Marker rows are payload-free fixed-field rows (ids, epochs, provenance — ~10 scalar fields, `claim_frontier.rs:222-296`); 4 KiB dwarfs any canonical encoding of that shape. Checked-assertion rider below makes this a verified claim, not a guess. |
| `mandatory_transaction_bound_entries` | 4 | Must admit the one-entry Attached row (`enrollment_closure.rs:604-615`); every committed fixture models Q as 2 entries; doubled for free generosity. |
| `mandatory_transaction_bound_bytes` | 16,384 | 4× the marker envelope: Attached/successor rows are id/epoch-scale (≪ 4 KiB); rider-checked against the real v2 encoder. |
| `full_recovery_claim_entries` | 4 | Equality with Q is construction-required (`enrollment_closure.rs:528-530`); K covers the RS+RT recovery pair (`admission/sequence.rs:5-24`) with 2× margin. |
| `full_recovery_claim_bytes` | 16,384 | Equals Q bytes (same constraint); recovery rows are id/epoch-scale. |
| `retained_capacity_entries` | 2,048 | Row-cap-driven: 1,024 retained rows (below) + `identity_slots`×1 marker reserves + Q + K entries ≈ 1,036, doubled. Baseline validation floor holds: fresh B0 = I×marker_max = (4, 16,384) ≪ cap. |
| `retained_capacity_bytes` | 16,777,216 | 16 MiB per conversation: ≥128 maximum-size retained records or thousands of typical ones; normal floors advance with acks so retained charge stays far below it. **This is the cost-bearing number** — worst case per fully saturated connection = 32 conversations × 16 MiB = 512 MiB durable retention; flagged for Tom's eyes per the idle-cost law, four-signature form. |
| `max_retained_record_rows` | 1,024 | Matches the house's existing signed retention scales (`max_live_attach_receipts_server` 1,024, `max_retired_identity_slots_server` 1,024); restore rejects-not-truncates (`claim_frontier.rs:3758-3775`), and ack-driven floor advancement bounds spans in normal operation (fixture scale: 32). |
| `closure_episode_churn_limit` | 1,024 | Orders of magnitude above any plausible binding-change cycles per episode (existing per-participant scales: 8 receipts, 64 provenance); inside the corrected `2..=u32::MAX` bound. |

**Checked-assertion rider [r4] (converts NOT-DERIVABLE encodings into verified
claims):** the canonical v2 byte sizes of marker, Attached, and recovery rows
do not exist at the pin — the v2 encoder is this build's own deliverable. The
build therefore SHALL add committed tests asserting the canonical v2 encoding
of (a) every marker provenance/target variant fits `max_generated_marker_bytes`,
(b) the Attached row fits `mandatory_transaction_bound_bytes`, and (c) each
RS/RT recovery row fits `full_recovery_claim_bytes`. A failing assertion is a
STOP (value renegotiation with the exact size), never a silent bump. Same form
as the house's pinned-number cost papers.

**Two derivation questions returned upward, not guessed (per Tom's
direction):** none blocking — the two candidates (operation-to-churn-cycle
mapping; Q/K composition formula) are absent at the bytes but the proposed
values bound them from existing scales generously; if Tom wants them derived
from a live formula instead, that formula is Unit 1 build output and the
values can be revisited at his 0.2.x publish gate with real encoder numbers
in hand.

### Pre-publish wire review — blocking publication, not implementation

Tom must review the D1 field placement, fixed width, exhaustive response echo,
codec goldens, flip test, and resume changes before any publish. No publication
is part of this unit.

## Revision record

| revision | date | author | record |
|---|---|---|---|
| r1 | 2026-07-18 | Hermes Crumpet fold; Sol draft | Initial ruled build brief for F-0c Unit 1: live frontier transitions, explicit RecordAdmission token correlation, loud v2 log migration, signed D2 inputs, and Leave riding the shared acquisition. |
| r4 | 2026-07-18 | Hermes Crumpet fold (derivation + amendment text); Sol research 455999d1 quantities, both bound contradictions byte-confirmed at the fold seat | D2 derivation round under Tom's direction (relayed by Waffles 05:30Z; Annabel deferred her half to Tom in-session): two Slice 5 bound corrections (Q/K cannot be zero and must be equal per `enrollment_closure.rs:528-530,604-621`; churn bound is `2..=u32::MAX` per `:26-27,519-524`); twelve proposed values with one-line derivations from real quantities; checked-assertion rider converting the absent v2 encoder sizes into build-verified claims; the 16 MiB retained-capacity cost number flagged for Tom's eyes in four-signature form. Research checkout note: worker ran at dc3df6d (Slice 5 byte-identical through 44d6b34). |
| r3 | 2026-07-18 | Hermes Crumpet fold | Confirmation round (same Sol session vs dc3df6d) verdict READY with two non-blocking comments, both folded: acceptance item 7's barrier half corrected from abandonment-absent to abandonment-PRESENT (absent never exercised the fifth removal site); cold-reopen precedent citation expanded to `production/tests.rs:143-210` with the second-open lines named. All eight r1 findings confirmed CLOSED at the bytes; no new false universal found. |
| r2 | 2026-07-18 | Hermes Crumpet fold (amendment text); Sol pre-review 752f8977 findings, all byte-confirmed at the fold seat | Pre-review NOT_READY closed by amendment: (M1) legacy-byte universal rejection withdrawn — staged layout is not byte-disjoint, fixture-narrowed test 5 plus honesty note; (M2) resume tokenless removal widened from two to five production sites plus two-cycle soak test; (M3) Slice 1 transition inputs gain the required server-computed canonical keyed row charges — commit-plus-owner was not the complete input set; (M4) marker-drain seam must be extended/composed to return the complete accounting/retention owner for the same-lock retry; (M5) WALL-MOVE-ONLY narrowed to the explicit linear-authority list, base-copyable types exempted. Minors: nested codec call-site list expanded; correlation.rs:159-245 ambiguous_authorization rewrite deletion added to D1 sites; Layer 3 harness start/stop/join/drop/reopen extension stated; WALL-ATOMIC flush-uncertainty honesty note. |
