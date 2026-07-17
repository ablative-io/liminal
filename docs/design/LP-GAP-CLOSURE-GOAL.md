# liminal-protocol gap closure + participant activation — goal session (phase B2)

You are closing the six crate API gaps reported by the phase B server-binding
session, then ACTIVATING production participant capability in
`liminal-server`. Phase B landed the binding infrastructure sealed under
`cfg(test)` precisely because these gaps made honest production construction
impossible. This session ends with participant lifecycle LIVE: capability
advertised, real handlers installed, and the blocker scenarios passing as
production-path tests — no seals, no dormant infrastructure.

Read first: `docs/design/LP-EXTRACTION-GOAL.md` (the crate's brief — its two
mandated design fixes remain law), `docs/design/LP-SERVER-BINDING-GOAL.md`
(the crate/server boundary rule: the crate owns rules, the server owns
bindings), and `docs/design/LP-ROADMAP.md` (review protocol). The frozen
semantic authority is `docs/design/PARTICIPANT-CONTRACT.md` @
`55856ae3c53206f9c662e6815650dfc67a89ce85` (in the participant-contract
worktree; read-only).

## Ground rules

Identical to LP-EXTRACTION-GOAL.md's ground rules, substituting: worktree
`.worktrees/lp-gap-closure`, branch `feature/lp-gap-closure` (branched from
current `main` after phase B merges). First commit: this brief file. You may
modify `crates/liminal-protocol`, `crates/liminal-server`, and workspace
manifests. `CARGO_TARGET_DIR` = the repo's own `target/`. Gates at every
commit: `cargo fmt --all -- --check`, build, clippy `--all-targets --
-D warnings`, tests. No new dependencies. No new prose documents. No
deferrals: every item below lands in this session or is reported as blocked
with the exact obstacle — never silently narrowed.

## Part A — the six crate gaps, in dependency order

**A1 (foundation) — aggregate event shell + public cold restore.** The shell
(`lifecycle/conversation.rs`) knows only `GenesisValidated`; complete
participant restoration (`lifecycle/storage.rs`:
`ParticipantConversationRestore`, `ParticipantLifecycleRestore::restore`,
`ParticipantConversationState`) is `cfg(test)`. Build the event-body taxonomy
for the six lifecycle operations (attach, detach, leave, crash/fate,
nonzero-debt marker ack, enrollment) with canonical encode/decode, and make
the whole-conversation cold restore public — preserving the anti-splicing
invariant documented at storage.rs:87-94 (components must not be
independently combinable) and the history validation the production path
already does (restored participants, never
`ValidatedConversationHistory::empty()`). Invariants: contiguous ordinal
monotonicity, one-shot-per-event preconditions, the `ConversationCommit`
durability barrier.

**A2 (small, parallel with A1) — record-admission persistence payload
public.** `RecordAdmissionPersistenceParts` and
`RecordAdmissionCommit::into_persistence_parts`
(lifecycle/record_admission.rs:306, :398) go from `pub(in crate::lifecycle)`
to public, preserving move-not-clone semantics (the doc at :393 is the law:
no cloning or dropping of any frontier, accounting, row, or marker
authority). Remove the dead-code allows; add a consuming test.

**A3 (depends on A1, consumes A2) — total aggregate commits for the six
operations.** The per-operation typed commits already exist and are public
(`commit_attach`, `commit_detach`, `commit_leave`/`commit_pending_leave` —
now returning `LeaveCommit` with frontiers — `apply_recovered_binding_fate`,
`apply_nonzero_participant_ack`, `commit_enrollment`). Wire each through the
shell: an aggregate-level commit that consumes the typed commit result, mints
the corresponding event body, and advances the shell under the durability
barrier. Atomicity law: detach = terminal append + floor transition + cell
replacement + binding release as ONE event; attach terminalizes
`Committed→Terminalized` atomically (Fix 1); the nonzero-debt ack carries
Fix 2's per-participant cursor-fact accounting.

**A4 (depends on A1) — observer-recovery atomic transaction.** The selection
(`apply_observer_recovery`) exists; the atomicity lives only in prose
(observer_recovery.rs:36-40, :82-84). Build the transactional surface where
the progress read and arm installation are one owned unit against the
aggregate, so a crash cannot leave a partially-armed request. Property tests
over interleavings and crash replay.

**A5 (independent) — request-bound response authority.** No type currently
prevents a handler from answering a request with an unrelated `ServerValue`.
Add the request-indexed response-authority surface (per-request-kind wrapper
or sealed mapping from each `ClientRequest` variant to its legal
`ServerValue` subset) so an unrelated response is a compile error by
construction — the same discipline as the detach cell. Transcribe the legal
request→response matrix from the frozen contract's R-D1 register (the
participant-contract worktree has the document; the matrix is the
~5,500-5,800 region). Every arm cross-checked against the register; the
matrix test cites the register rows.

**A6 (investigate FIRST, build only if mandated) — incarnation-reference
inventory.** `DurableIncarnationReferences` exists publicly
(lifecycle/incarnation.rs:207) but accepts an arbitrary pre-built slice; the
question is whether a completeness witness (bindings + receipts + work items
+ recovery rows, per the doc-comment at :213-215) is a crate rule or a
server binding. Read the frozen contract around the cited lines (~484-504
per incarnation.rs:3) and REPORT the answer with quotes in your declaration
BEFORE building anything. If the contract mandates a proof-carrying
inventory, build it small; if it is server-owned enumeration, say so and
stop — do not over-build server storage concerns into the crate.

## Part B — activation (after A1-A5 are green)

**B1 — production construction unsealed.** `InstalledParticipantService::new`
loses its `cfg(test)` gate; `LiminalConnectionServices` gains the production
constructor wiring a real semantic handler backed by the Part A aggregate
commits and restore. The handler implements the eight request variants
through the protocol seam exactly as the test handlers do — but there must
be exactly ONE production handler, in the server, calling crate transitions
only (the crate-owns-rules audit will be re-run on your final commit).

**B2 — capability advertised, config-driven WF.** The participant capability
bit flows from the real service presence (apply.rs:374-390 already selects
on `Some`); `configured_wf` originates from real server configuration (not a
constant, not a test harness value — if the server config surface lacks the
field, add it with NO default: absence is a startup error, discussed values
only, per the no-assumed-defaults rule).

**B3 — the blocker scenarios go live.** The two phase-B repository tests
(terminalized detach cold-reopen with old epoch; two-participant same-suffix
cursors with the regression refusal) are re-expressed as PRODUCTION-PATH
tests: through the live dispatch seam (real `InstalledParticipantService`,
real wire frames, real durable store on disk), cold restart included. The
repository-layer tests remain; these are additional. Plus one full E2E:
enroll → attach → records → acks → detach → replay old token →
`TerminalizedDetachCell` with old epoch, over a real socket against the
running server, wire-encoded end to end.

**B4 — LAW-1 and boundary re-audit hold.** No polling anywhere in what you
touch; no rule duplication; no hand-built outcomes; fail-closed on every new
arm. Your declaration states each explicitly.

## Amendments

**2026-07-17 (A5 lane, review fix round) — A5 decision-arm migration file
scope.** The A5 lane mandate as dispatched scoped file changes to
`lifecycle/record_admission.rs` and `wire/` only. Migrating the operation
decision arms onto the request-bound response authorities is
compile-required across the selectors and operations that mint responses,
so the A5 diff also touches `lifecycle/admission/capacity.rs`,
`lifecycle/observer_recovery.rs`, `lifecycle/operations/enrollment_operation.rs`,
`lifecycle/operations/marker_ack.rs`, `lifecycle/operations/participant_ack.rs`,
`lifecycle/operations/nonzero_participant_ack.rs`, their test files, and the
`lifecycle` mod re-exports. The review round examined these edits and found
them correct and a necessary consequence of A5; this amendment records that
the decision-arm migration across those lifecycle files is IN-MANDATE for
the A5 lane. The explicit DO-NOT files (`lifecycle/conversation.rs`,
`lifecycle/storage.rs`, `liminal-server`) remain untouched and out of scope.

**2026-07-17 (activation fix round) — reduced B1 surface: RecordAdmission and
authorized Leave fail closed pending the live claim-frontier acquisition.**
The B1 mandate as written ("the handler implements the eight request variants
through the protocol seam") is NOT fully closable inside this lane, and the
prior declaration's claim that all eight variants dispatch through crate
transitions was wrong for RecordAdmission and the authorized Leave arms. The
exact obstacle, verified against the crate surface: (a) `commit_leave`/
`commit_pending_leave` consume a `PreparedLeaveAuthority` and
`apply_record_admission` consumes a `RecordAdmissionPrestate`, both of which
require a validated `ClaimFrontiers` value; (b) `ClaimFrontiers` is
constructible only from `from_initial_enrollment` (participant zero, at
enrollment) or `restore` (complete per-participant sequence/order claim
state), and the crate exposes NO frontier transitions for the attach, detach,
subsequent-enrollment, or ack operations this binding already commits — so a
live frontier value cannot be maintained across a conversation's history and
a restore-based acquisition requires durable claim/retained-record facts the
transition-input log does not carry (this is the A1 whole-conversation
LIVE-restore capsule, a separate protocol-crate unit); (c) the record/leave
closure parameters (`max_ordinary_record_charge`, projection limits, retained
caps) are deployment-owner configuration values that per this document's own
config note land only together with their real consumers — inventing them
here would violate the no-assumed-defaults rule.

What lands in this round instead: both arms classify every frozen pre-commit
stage through crate selectors — the stage 2-5 lookup rows via
`classify_record_admission_binding` / `lookup_leave` and the stage-6
connection-conversation capacity gate — and fail closed with a typed
diagnostic ONLY on a fully authorized commit; production-path tests pin the
typed refusal rows, and the E2E's records step exercises this reduced typed
surface over the wire. Residual consequences, all open until the frontier
unit lands: a fully AUTHORIZED RecordAdmission or Leave still fails the
connection closed while `PARTICIPANT_CAPABILITY_BIT` is advertised — whether
advertising with this reduced surface is acceptable is an ESCALATED decision
for the session owner, not settled here; the mandated committed-records E2E
step stays blocked; the server-scope identity-capacity counter
(`max_retired_identity_slots`, server scope) has no configured limit yet, so
only the conversation scope refuses.

**2026-07-17 (activation fix round) — connection-incarnation unseal
reconciliation.** `allocate_connection_incarnation`'s seal comment demanded a
"complete durable reference inventory" before production use; the durable
inventory was never wired and is NOT needed for uniqueness. The recorded
proof (now also at the call site in `supervisor.rs`): startup strictly
increments the server incarnation and fsyncs it before any listener is ready;
allocations are mutex-serialized, propose ordinals strictly above the durable
`last_examined_connection_ordinal`, and fsync their event before publication;
durable references can only name previously published pairs, and the shared
store's flush barrier orders the allocator event before any conversation-log
entry referencing it. Published pairs are therefore unique against ALL
durable references by allocator-log monotonicity alone; the live-connection
reference set is bounded defense in depth against a rolled-back allocator
stream.

**2026-07-17 (activation review fix round 2) — R-C0 stage-8 receipt/identity
capacity family CLOSED; enrollment receipt supersession CLOSED.** The review
round found the stage-8 receipt/provenance capacity family silently absent
(the crate's verified seven-scope/five-scope selectors bypassed, no receipt
caps in `ParticipantConfig`, `ReceiptCapacityExceeded` rows unreachable,
expired fingerprints retained forever) and enrollment receipt supersession
unimplemented. Both are now closed on the production path:

1. `ParticipantConfig` gains the five receipt/provenance cap fields AND
   `max_retired_identity_slots_server`, all REQUIRED with no defaults,
   validated in the contract's frozen nine-field order. Closing the
   previously recorded server-scope identity gap is a compile-required
   consequence of routing enrollment through the crate's seven-scope
   `select_enrollment_capacity` (its first scope IS identity Server); the
   earlier amendment's open-gap note is superseded.
2. Both arms run the frozen stage-8 position: enrollment through
   `select_enrollment_capacity`, credential attach through
   `select_credential_attach_capacity`, over per-conversation/per-participant
   occupancies computed from the conversation authority and server-scope
   occupancies from a shared in-memory ledger with atomic check-and-reserve
   (rolled back on any pre-publication failure). A scope whose configured
   limit was lowered beneath retained occupancy refuses with true numbers at
   counter construction, in the same fixed scope order.
3. Cleanup is request-time checks only, never a sweep: retained provenance
   fingerprints prune at their own deadlines on the next request touching
   them, and the classification of an unmatched token derives from the R-C0
   completeness rule (rotation-from-presented-generation witness in window →
   `StaleAuthority` no-commit proof; otherwise `StaleOrUnknownReceipt`), so
   pruning never regresses an exact old token to a false no-commit claim.
   The contract's "admitted durable deadline events" half of the cleanup
   sentence stays open with the delivery machinery it belongs to: no timer
   or deadline-event infrastructure exists in this binding (LAW-1), and the
   request-time checks alone bound retained state through the caps.
4. Server-scope exactness across restarts comes from a durable conversation
   registry (`liminal:participant-conversation-registry`): one row appended
   and flushed immediately BEFORE each conversation's genesis append, read
   at handler construction to replay every durable conversation into the
   ledger. A store-prefix scan was rejected: haematite's scan visits only
   shards already materialised in this process, so it is silently empty on a
   freshly reopened database.
5. A committed credential attach now ends the enrollment receipt body with
   the exact terminal reason (`Superseded` inside the receipt's live window,
   `Deadline` after it), derived from the committing operation's admitted
   clock so cold replay reproduces the identical record; the invalidated
   generation-1 secret payload is never re-served after rotation.

## Declaration

Commit hash; per-gap closure evidence (file:line of the new public
surfaces); the A6 ruling report with contract quotes; test counts; gate
outputs; the B3 E2E result; any gap you could not close with the exact
obstacle. External review (Sol lens + the lead's gates) runs on your
declared commit; findings return as fix items; maximum two rounds.
