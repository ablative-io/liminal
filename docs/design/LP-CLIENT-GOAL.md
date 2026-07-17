# liminal-protocol client unit — goal session

You are adding the client unit to `crates/liminal-protocol`. This Tom-authorized unit is on the critical path to the
frame prototype: browser-as-liminal-participant. Phase C round 2 (`feature/lp-sdk-r2` at `d814e1c`) halted on four
verified TERM-5 stops. At pin `2e5a731b5f009b0cac2b8c28b90f9b1245372732`, the crate owns server aggregate
machinery (`lifecycle/aggregate_commit.rs:1-12`), consumed through the production server's sealed barrier
(`crates/liminal-server/src/server/participant/production/barrier.rs:127-149`), but its SDK detach authority has only
construction, inspection, and attach supersession (`outcome/local.rs:151-192`), and its reconnect surface has outcomes
but no producer (`outcome/local.rs:204-232`). It has no public client aggregate, complete client persistence record, or
reconnect producer. The SDK cannot finish without locally re-deriving rules, which the LP arc forbids. This brief closes
those gaps IN THE CRATE.

Treat this brief, the pinned repository, and `~/.norn/delegations/claude-dev-lpsdk-r2.h4Zh3Q` as FROZEN, READ-ONLY
sources. The record's four `remaining_work` stops and suggested crate shapes are mandated API starting points; its
`session_notes` control interpretation. Follow the existing non-`Clone`, private-state shell
(`lifecycle/conversation.rs:38-55`) and sealed effects (`lifecycle/aggregate_commit.rs:51-128`): callers delegate
rather than compare fields.

## Ground rules

- Work ONLY in `.worktrees/lp-client`, branch `feature/lp-client`, from the pin. Never touch another branch/worktree.
- Your FIRST commit is this brief, unmodified. Then modify only `crates/liminal-protocol`; commit and push each unit
  (`feat(protocol): ...` / `test(protocol): ...`).
- Set `CARGO_TARGET_DIR` to repository `target/`. Match workspace formatting/lints. No `unsafe`; no
  `unwrap`/`expect`/`panic` outside `#[cfg(test)]`; public items have doc-comments.
- At each checkpoint run the roadmap protocol build, `clippy --all-targets -- -D warnings`, and tests; final gates also
  cover the workspace. Record genuine exit codes.
- Add no prose document, lint suppression, ignored test, fallback, or caller-owned rule. Gates are compiler, clippy,
  and tests.
- Red/fail-first evidence (r2, 2026-07-18): every claimed red test is committed in the tree, or its output and diff
  are attached to durable evidence. A declaration citing uncommitted tests is void.

## Scope

This unit is TRANSPORT-AGNOSTIC: TCP today, WebSocket/wasm next. No mandated API contains a socket, stream, timer,
runtime, thread, or transport handle; transport fates arrive as typed events. Preserve `no_std + alloc`
(`src/lib.rs:1-10`) and gate `--no-default-features`.

TCP/SDK wiring is OUT: `RemoteTransport` participant entrypoints, reader wiring, real `TcpStream` attempts, and SDK
loopback tests are a separate bounded leg AFTER this unit lands. The claim-frontier unit is also OUT: different
consumers, different brief; reduced-B1 context is `docs/design/LP-GAP-CLOSURE-GOAL.md:145-181`. Do not change
server-side `lifecycle/aggregate_commit.rs` decisions, `lifecycle/storage.rs`, production server code, or wire-register
semantics. Capability advertising stays ON; do not revisit it.

The lost-authority testimony amendment (piece 4 and every r2-marked rule; r2, 2026-07-18) is closed by ONE
implementation pass against this amended text — explicitly NOT a round 5 of the old scope; round-by-round defect
chasing against adjacent public doors is exactly what this re-scope retires. The do-not-regress floor proven across
rounds 1-4 carries forward verbatim as the unchanged bar: M4 preservation, permit one-use, B1/B2/M6/M8/M10, codec
round-trips, refusal purity on every changed arm, zero timers, `cfg(test)`-only wire authority construction, server
untouched, and the `no_std + alloc` build and lint gates.

## Four mandated pieces

**1 — sealed client correlation and detach replay.** `ClientRequest` is exhaustive (`wire/request.rs:118-153`), while
`ServerValue` is independently exhaustive and `originating_request` yields only a discriminant
(`wire/response.rs:1705-1780`, `:1841-1944`). Add non-`Clone`, private-field `ClientParticipantAggregate` and sealed,
non-`Clone` `ExpectedParticipantOperation`. Mint the latter only through a client durability barrier with the ruled
commit-seal → persist → release order (Tom's round-3 ruling, 2026-07-17, superseding this brief's earlier
decide → persist → commit sketch; precedent `lifecycle/aggregate_commit.rs:51-128`, consumed at
`crates/liminal-server/src/server/participant/production/barrier.rs:127-149`): `record_operation` returns a pending
decision holding the expected operation and successor aggregate as private, unreachable state; `commit()` seals it
into a committed record that exposes canonical `ClientResumeRecord` bytes while still withholding all authority; the
caller durably persists those bytes; only then does `into_parts()` release the successor aggregate and one-use
operation. `abort()` before commit returns the unchanged aggregate and request. Speculative pre-commit state is never
encodable — the sealed committed record is the only byte source, so no pending-state bytes can exist to be promoted.
The crate owns no storage and cannot observe the write — the barrier's guarantee is reachability: nothing executable
escapes before the seal, and nothing releases before the caller's persist step. Minimum public seam:

```rust
pub fn record_operation(
    aggregate: ClientParticipantAggregate,
    request: ClientRequest,
) -> ClientOperationRecordDecision;
pub fn decide_inbound(
    aggregate: ClientParticipantAggregate,
    value: ServerValue,
) -> ClientInboundDecision;
```

The aggregate owns the outstanding-operation state and ALL match selection: `decide_inbound` takes no expected-
operation parameter, because caller pre-selection of which operation a response correlates against is itself the
correlation decision. Cardinality is a crate-enforced rule: at most ONE outstanding write-ahead operation at a time;
`record_operation` while one is expected returns a typed refusal carrying the unchanged aggregate and the refused
request — no queue, no silent replacement. Re-recording the retained detach envelope is admitted only when the
retained replay status is compatible with a fresh first send (r2, 2026-07-18): a replay that is superseded,
Leave-superseded, or terminal refuses the same-envelope re-record with a typed reason instead of reviving
expected-detach authority over an inactive replay, so a correlation-consuming success can never be re-recorded into an
expected-Detach-plus-inactive-replay state. Continuous acknowledgements are not write-ahead operations and never occupy
the slot; their outcomes, like server-initiated values with no originating request, route through their own crate
rules. `decide_inbound` performs request identity, operation, conversation, participant, token, generation, and
binding correlation inside the crate and matches every `ServerValue` arm. Applied effects return resulting aggregate
and typed value; refusals return the unchanged aggregate — still holding its expected authority — and the refused
value. Distinguish at least already-dead, foreign-response, and delayed-response refusals. No boolean/string/generic
error, dropped packet, or caller relabeling of a decoded value as applied is permitted.

Add `SdkDetachReplayAggregate`, retaining the exact private `DetachEnvelope` (`wire/envelope.rs:30-41`), and subsume
standalone `SdkDetachReplayAuthority`: no public `supersede` escape may bypass the aggregate. Closed status vocabulary:

```rust
pub enum DetachReplayStatus {
    Parked,
    InFlight,
    Superseded,
    LeaveSuperseded,
    Terminal(DetachReplayTerminal),
}
pub enum DetachReplayTerminal {
    DetachCommitted(DetachCommitted),
    DetachInProgress(DetachInProgress),
    TerminalizedDetachCell(TerminalizedDetachCell),
}
```

If an initial no-detach state is needed, name it; `Option` may mean only that state and never erase a terminal arm. The
aggregate owns these consuming decisions; refine names only without weakening their shape:

```rust
record_detach(...) -> RecordDetachDecision
transport_attempt_started(...) -> DetachTransportAttemptDecision
transport_fate(...) -> DetachTransportFateDecision
apply_attach(...) -> ApplyAttachDecision
apply_leave_durable(...) -> ApplyLeaveDecision
apply_detach_outcome(...) -> ApplyDetachOutcomeDecision
```

Every arm returns a defined continuable result or unchanged state plus refused input. Every public fate or recovery
entry point consumes either a live one-use correlation or a serialized lost-authority testimony (piece 4; r2,
2026-07-18); a fate that consumes neither is unrepresentable in the API, because the fate value itself is not publicly
constructible. `transport_attempt_started` alone produces `Parked -> InFlight`; only typed fate returns it to
`Parked`. A refused non-matching `AttachBound` PRESERVES
`InFlight`: M4 rules that attach is not transport fate and proves nothing about an outstanding send. No refusal strands
authority. Matching attach supersession, durable Leave supersession, and terminalization by `DetachCommitted`,
`DetachInProgress`, or `TerminalizedDetachCell` are aggregate decisions, never caller comparisons. Validate their typed
fields (`wire/response.rs:317-354`, `:1194-1279`, `:1431-1535`) against the stored request. As with
`LP-EXTRACTION-GOAL.md` Fix 1, caller re-derivation must be a compile error by construction.

**2 — client-owned resume record.** Add private-field `ClientResumeRecord` with ITS OWN codec. It carries every client
fact: exact expected request/correlation state, client binding state, full replay state (`Parked`/`InFlight`/
`Superseded`/`LeaveSuperseded`/each `Terminal` payload distinctly), and reconnect permit state. No projection collapses
a terminal state into `None`. The record also carries the lost-authority testimony and the pending tokenless
abandonment (piece 4; r2, 2026-07-18): encoding an aggregate holding either pending atom is refused with a typed
error or emits bytes that retain the atom — no encode path may silently drop a pending testimony, and
encode-without-take loses nothing.

```rust
impl ClientResumeRecord {
    pub fn encode_canonical(&self) -> Vec<u8>;
    pub fn decode_canonical(
        input: &[u8],
    ) -> Result<Self, ClientResumeRecordDecodeError>;
    pub fn restore(self) -> Result<ClientParticipantAggregate, ClientResumeRestoreError>;
}
```

Use a stable v1 envelope like the server codec: network byte order, exact length, and typed truncation/magic/version/
tag/length and restore-invariant errors. Decode is inert; only validated `restore` mints executable aggregate/permit
authority, following public cold-restore discipline (`lifecycle/storage.rs:87-119`) without modifying that surface.

NEVER fabricate or reuse server conversation events. `ConversationEventBody` stays `pub(super)`
(`lifecycle/conversation.rs:266-274`); `ConversationEvent` stays opaque and unfabricatable (`:276-295`); its canonical
methods stay the server event codec (`:310-338`). This is load-bearing: a client fact is not a committed server operation.

**3 — reconnect producer.** Add non-`Clone` `ReconnectAggregate`. Fresh-event producers return sealed, ONE-USE,
non-`Clone` `ReconnectAttemptPermit`; minimum transport-fate path:

```rust
record_transport_fate(...) -> ReconnectPermitDecision
redeem_attempt(...) -> ReconnectAttemptDecision
record_attempt_fate(...) -> ReconnectAttemptFateDecision
```

Also add typed producers for a proved online transition and explicit caller action. Established-connection transport
fate, proved online transition, and explicit caller action are the only fresh event classes; each authorizes one attempt.
`redeem_attempt` consumes the permit into a sealed in-progress attempt before the binding opens a REAL connection;
reporting typed success/failure consumes it back through the aggregate. No transport handle crosses the API. Failure
parks typed and returns no timer/retry effect; elapsed time, backoff, wakeup, and internal loops mint nothing. Process
loss of an issued permit or in-progress attempt is recorded only by consuming the serialized lost-authority testimony
minted at restore (piece 4; r2, 2026-07-18); reconnect process-fate values are not publicly constructible.

The existing `ReconnectState`, `ReconnectRequiredEvent`, and `ReconnectDelayResult` (`outcome/local.rs:204-232`) gain
their producer and required exhaustive event arms. A second request or stale/double redemption returns typed no-permit
with unchanged aggregate; moved-permit reuse also fails to compile. Move case 50 to, or prove it against, the crate
implementation: local `ReconnectHarness` is labeled “SDK/product mechanics” (`tests/acceptance_45_51.rs:1967-1972`),
holds one optional permit (`:1981-2005`), and proves consume-once/no-mutation on a second call (`:2007-2057`). This brief
supersedes its timer interpretation: a fresh event permits one real attempt, never a timer arm.

**4 — lost-authority testimony and durable abandonment (r2, 2026-07-18).** A crash or restore that destroys a live
process-local authority — an issued operation's response correlation, an in-flight detach transport attempt, an
issued reconnect permit or in-progress attempt — must leave a first-class record of that destruction in the protocol
state. Add a serialized, take-once lost-authority testimony atom: the crate mints it exactly when a validated restore
accepts a state whose live authority did not survive the process; it is persisted in the aggregate encoding so
encode/decode round-trips it losslessly; it is consumed exactly once, by the one recovery path that resolves the
loss; and it is never creatable from the public API. State this as a type-surface property: the atom has no public
constructor, and no caller-suppliable flag or value gates any authority transition. CRITICALLY, no encode path may
silently drop a pending testimony — serializing while a testimony is pending is either refused with a typed error or
produces bytes that carry the testimony. Losing the atom on the way to disk is the same defect the atom exists to
close.

Both round-4 doors close BY CONSTRUCTION, as structural properties of the type surface, never as behavioral promises.
(a) Every public fate/recovery entry point consumes either a live one-use correlation or a serialized testimony; a
fate consuming neither must be unrepresentable in the API — the fate value itself must not be publicly constructible.
The round-4 exemplar is `record_issued_expected_operation_fate(aggregate, fate)` (`client/barrier.rs:269`): its
`IssuedExpectedOperationFate::ProcessLost` is a public unit variant any caller can mint, so an issued operation can be
terminalized while its live correlation still exists. That shape is forbidden wherever it appears, including the
reconnect process-fate producers. (b) The same-envelope re-record arm (`client/barrier.rs:502`) requires
replay-status compatibility as ruled in piece 1, so a correlation-consuming success cannot be re-recorded into an
expected-Detach-plus-inactive-replay state; the audit table's "never accepted" rows must be unreachable by
construction — reachable-in-live-code but refused-only-at-restore is exactly the door this piece removes.

Tokenless operation classes (`RecordAdmission`, `ObserverRecovery`) keep the existing typed-ABANDONED-on-restore rule
unchanged, and the abandonment marker becomes durable: it is serialized in the aggregate, survives
encode-without-take, and is consumed exactly once. A restart between restore and take may not lose the abandonment,
and a second take may not observe it.

## Phase 1 — client core (checkpoint 1)

1. Add the four public pieces, private codecs/effects, and re-exports; expose no raw state parts.
2. Test replay lifecycle: record/start/fate/park/replay, matching attach, durable Leave, all three terminal outcomes,
   and refused non-matching attach preserving exact `InFlight`.
3. Round-trip ALL resume facts, including each binding, expected operation, permit state, replay arm, terminal
   payload, lost-authority testimony, and pending abandonment distinctly through decode and validated restore.
4. Prove one-use: first redemption starts one attempt; double/stale redemption refuses without mutation; failure parks;
   no timer path exists; cloning/reusing a moved permit fails to compile.
5. Drive already-dead, foreign, delayed/older, and exact response correlation. Refusals retain aggregate, expected
   authority, and value; only exact responses apply. Prove the cardinality rule: `record_operation` while one
   operation is outstanding refuses without mutation, and a pending decision releases nothing before `commit()`.

**Checkpoint 1 declaration:** on one clean commit run roadmap protocol gates
(`docs/design/LP-ROADMAP.md:50-67`), no-default-features build, and relevant workspace gates. Push; state commit, module
inventory, per-suite counts, and exit codes. PAUSE for bounded review before phase 2; at most two rounds, then escalate
(`LP-ROADMAP.md:50-53`).

## Phase 2 — laws and final proof

6. Property-test crash-replay determinism: every decision replayed from recorded pre-state with identical input yields
   identical typed effect and state, following `lifecycle/aggregate_commit_tests.rs:194-227`.
7. Property-test permit single-use under interleaved fresh events, redemptions, fates, stale permits, restores, and
   inbound refusals. Started attempts never exceed fresh authorizations; failure creates no authorization or timer.
8. Prove the conservation property against the crate's own testimony (r2, 2026-07-18): the property ATTEMPTS every
   action at every step and asserts the crate's typed refusal for illegal interleavings — pruning an avoided
   interleaving out of the schedule is not exploration. The operation/transition alphabet includes at least the
   `ProcessLost` interleaving, sequential same-envelope re-record, and a token-bearing non-detach operation class
   alongside detach and the tokenless classes. The explored-space count is derived from the run and drift-checked,
   but is never offered as correctness evidence. The testimony the property observes is the CRATE's serialized atom,
   never a harness-side counter — the harness-side lost counter is the defect, not the instrument.
9. Extend the module-doc audit table (r2, 2026-07-18): rows for the restored-loss testimony atom and the
   pending-abandonment atom; both coupling-refusal directions listed; every row is either reachable-and-tested or
   refused-by-construction with the refusing type named. A row that is unreachable only because callers are polite
   is a defect.
10. Add doc-comments citing this brief for every mandated deviation: M4 preservation, client/server-event separation,
    lossless terminal persistence, and event-only reconnect. Run all gates on final bytes, commit, push, declare, stop.

## Declaration specification

State final commit and clean status; module/file inventory; phase and total test counts by suite; and every build/clippy/
test command with genuine exit code and concise output. List every interpretation from `ReconnectHarness` with exact
`tests/acceptance_45_51.rs:<line>` and every API-shape deviation from this brief with its compile/type reason. Explicitly
confirm untouched server aggregate decisions, storage, wire semantics, capability advertising, SDK/TCP wiring, and
claim-frontier unit. List every claimed red test with the commit that contains it, or attach its output and diff to
durable evidence — a declaration citing uncommitted tests is void (r2, 2026-07-18) — and confirm the rounds 1-4
do-not-regress floor from the Scope section item by item.

## What happens after (not your scope)

After review, a separate SDK leg binds real TCP; WebSocket/wasm then consumes the same unit for the browser frame
prototype. You do neither here.

## Amendments

**2026-07-18 (r2, post-round-4 structural re-scope) — first-class lost-authority testimony atom.** Authority:
Waffles's seat under Tom's standing keep-it-moving authority, recorded for veto. Rounds 1-4 on `feature/lp-client`
(tip `40244d6`) each fixed the previous round's exact authority-conservation defects, and each time the class
re-emerged through an adjacent public door. The reviewer's structural diagnosis, ratified at the tear seat: the crate
has no first-class "lost authority" testimony. Crashes destroy live authority — transport attempts, in-flight
correlations — but nothing in the protocol state records that destruction, so the conservation property had to fake a
harness-side lost counter (`client/authority_property_tests.rs`, the `lost` field with pruning `Ok(None)` arms), and
public fate/recovery doors could mint or duplicate authority invisibly. The two round-4 doors on record:
`record_issued_expected_operation_fate(aggregate, fate)` consumes neither a live correlation nor any testimony and
its `ProcessLost` fate is publicly constructible (`client/barrier.rs:269`), and the same-envelope admission arm
re-records a retained detach envelope with no replay-status check (`client/barrier.rs:502`), reviving expected-detach
authority over an inactive replay.

This amendment lands as decided text in place; this entry is the audit record. It adds mandated piece 4
(lost-authority testimony and durable abandonment), the same-envelope replay-status-compatibility rule and the
fate-consumes-correlation-or-testimony rule in piece 1, the testimony and abandonment resume facts and the
no-silent-drop encode rule in piece 2, the reconnect process-fate consumption rule in piece 3, Phase 1 items 1 and 3
updated for the fourth piece and the new atoms, Phase 2 items 8-9 (attempt-and-assert-refusal conservation law;
audit-table obligations), the red/fail-first evidence ground rule, the declaration-specification evidence and floor
obligations, and the one-pass r2 scope paragraph carrying the rounds 1-4 do-not-regress floor verbatim. No prior
requirement is weakened, reinterpreted, or removed; where earlier body text conflicted, it was rewritten to the
amended rule. Revision lineage, numbered retroactively for this record: r1 — brief torn at `17143cd`; r1.1 — tear
amendments at `5740cf8`; r1.2 — round-3 barrier amendment at `4661fc6`; r2 — this amendment.
