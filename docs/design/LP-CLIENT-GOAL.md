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

## Scope

This unit is TRANSPORT-AGNOSTIC: TCP today, WebSocket/wasm next. No mandated API contains a socket, stream, timer,
runtime, thread, or transport handle; transport fates arrive as typed events. Preserve `no_std + alloc`
(`src/lib.rs:1-10`) and gate `--no-default-features`.

TCP/SDK wiring is OUT: `RemoteTransport` participant entrypoints, reader wiring, real `TcpStream` attempts, and SDK
loopback tests are a separate bounded leg AFTER this unit lands. The claim-frontier unit is also OUT: different
consumers, different brief; reduced-B1 context is `docs/design/LP-GAP-CLOSURE-GOAL.md:145-181`. Do not change
server-side `lifecycle/aggregate_commit.rs` decisions, `lifecycle/storage.rs`, production server code, or wire-register
semantics. Capability advertising stays ON; do not revisit it.

## Three mandated pieces

**1 — sealed client correlation and detach replay.** `ClientRequest` is exhaustive (`wire/request.rs:118-153`), while
`ServerValue` is independently exhaustive and `originating_request` yields only a discriminant
(`wire/response.rs:1705-1780`, `:1841-1944`). Add non-`Clone`, private-field `ClientParticipantAggregate` and sealed,
non-`Clone` `ExpectedParticipantOperation`. Mint the latter only through a client durability barrier with the crate's
decide → durable persist → commit/abort shape (`lifecycle/aggregate_commit.rs:51-128`, consumed at
`crates/liminal-server/src/server/participant/production/barrier.rs:127-149`): `record_operation` returns a pending
decision holding the expected operation and successor aggregate as private, unreachable state; the caller durably
persists the encoded `ClientResumeRecord` carrying that pending operation, then `commit()` releases both; `abort()`
returns the unchanged aggregate and request. The crate owns no storage and cannot observe the write — the barrier's
guarantee is reachability: nothing executable escapes while the decision is speculative. Minimum public seam:

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
request — no queue, no silent replacement. Continuous acknowledgements are not write-ahead operations and never occupy
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

Every arm returns a defined continuable result or unchanged state plus refused input. `transport_attempt_started` alone
produces `Parked -> InFlight`; only typed fate returns it to `Parked`. A refused non-matching `AttachBound` PRESERVES
`InFlight`: M4 rules that attach is not transport fate and proves nothing about an outstanding send. No refusal strands
authority. Matching attach supersession, durable Leave supersession, and terminalization by `DetachCommitted`,
`DetachInProgress`, or `TerminalizedDetachCell` are aggregate decisions, never caller comparisons. Validate their typed
fields (`wire/response.rs:317-354`, `:1194-1279`, `:1431-1535`) against the stored request. As with
`LP-EXTRACTION-GOAL.md` Fix 1, caller re-derivation must be a compile error by construction.

**2 — client-owned resume record.** Add private-field `ClientResumeRecord` with ITS OWN codec. It carries every client
fact: exact expected request/correlation state, client binding state, full replay state (`Parked`/`InFlight`/
`Superseded`/`LeaveSuperseded`/each `Terminal` payload distinctly), and reconnect permit state. No projection collapses
a terminal state into `None`.

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
parks typed and returns no timer/retry effect; elapsed time, backoff, wakeup, and internal loops mint nothing.

The existing `ReconnectState`, `ReconnectRequiredEvent`, and `ReconnectDelayResult` (`outcome/local.rs:204-232`) gain
their producer and required exhaustive event arms. A second request or stale/double redemption returns typed no-permit
with unchanged aggregate; moved-permit reuse also fails to compile. Move case 50 to, or prove it against, the crate
implementation: local `ReconnectHarness` is labeled “SDK/product mechanics” (`tests/acceptance_45_51.rs:1967-1972`),
holds one optional permit (`:1981-2005`), and proves consume-once/no-mutation on a second call (`:2007-2057`). This brief
supersedes its timer interpretation: a fresh event permits one real attempt, never a timer arm.

## Phase 1 — client core (checkpoint 1)

1. Add the three public pieces, private codecs/effects, and re-exports; expose no raw state parts.
2. Test replay lifecycle: record/start/fate/park/replay, matching attach, durable Leave, all three terminal outcomes,
   and refused non-matching attach preserving exact `InFlight`.
3. Round-trip ALL resume facts, including each binding, expected operation, permit state, replay arm, and terminal
   payload distinctly through decode and validated restore.
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
8. Add doc-comments citing this brief for every mandated deviation: M4 preservation, client/server-event separation,
   lossless terminal persistence, and event-only reconnect. Run all gates on final bytes, commit, push, declare, stop.

## Declaration specification

State final commit and clean status; module/file inventory; phase and total test counts by suite; and every build/clippy/
test command with genuine exit code and concise output. List every interpretation from `ReconnectHarness` with exact
`tests/acceptance_45_51.rs:<line>` and every API-shape deviation from this brief with its compile/type reason. Explicitly
confirm untouched server aggregate decisions, storage, wire semantics, capability advertising, SDK/TCP wiring, and
claim-frontier unit.

## What happens after (not your scope)

After review, a separate SDK leg binds real TCP; WebSocket/wasm then consumes the same unit for the browser frame
prototype. You do neither here.
