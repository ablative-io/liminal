# Board #14 — the attach path's silent refusals

Status: ANALYSIS IN PROGRESS. Written at `0c3bacc`. Every citation below carries
its rev because a `file:line` without one is ambiguous between seats by
construction.

## The count was wrong, and the correction matters

The banked claim was "17 Fatal sinks in the attach path, TEN in the fenced
verifier". Re-measured at `0c3bacc` over the seven attach-path files with the
predicate `StateError::invariant|StateError::fatal`:

| file | sites |
|---|---|
| `ops_attach.rs` | 4 |
| `ops_attach_lookup.rs` | 10 |
| `ops_attach_verify.rs` | 10 |
| `ops_attach_capacity.rs` | 1 |
| `ops_attach_finalizer.rs` | 3 |
| `fenced_attach_codec.rs` | 0 |
| `fenced_attach_terminal.rs` | 0 |
| **total** | **28** |

The "TEN in the fenced verifier" half survives exactly (`ops_attach_verify.rs`).
The 17 does not reproduce under this predicate at this rev. Recorded as a
correction rather than a discrepancy to chase: the earlier number was taken at a
different rev and its predicate was never written down, so the two numbers are
not comparable and the older one cannot be defended. **A count is not a
measurement until it names what it counted.**

## The funnel is ONE sink, not 28

All 28 sites converge before they reach the client:

```
StateError::invariant  (28 sites, attach path)
  -> ParticipantSemanticError::Internal { message }
       doc: "Diagnostic text for server logs; never placed on the participant wire"
  -> dispatch.rs:826   ParticipantDispatch::Fatal(Semantic(error))
  -> apply.rs:208-211  tracing::warn!(%error, ...); FrameAction::Close
  -> bare close, NO FRAME
```

Consequence for the fix: **the wire half of #14 is not 28 edits.** It is one
decision at one place, plus a classification of which sites are entitled to
reach it.

## Why R4 stopped, and why the stop was right

R4 (`silent-close-attach-refusal` = `557eed7`, red committed, green deliberately
not written) could not invent the mapping. The reason is structural, not a lapse:

`liminal-protocol/src/wire/authority/credential_attach.rs` @ `0c3bacc` exposes
18 constructors and says so in its own doc — *"Constructors exist only for the
outcomes the frozen R-D1 register admits for credential attach; every other
pairing is a compile error by construction."* The pairing matrix is
**transcribed, not invented**, from `docs/design/PARTICIPANT-CONTRACT.md`.

**There is no constructor for "refused, terminal, internal."** Not by oversight:
`PARTICIPANT-CONTRACT.md` @ `0c3bacc` line 5845 states that *"SDK-local,
startup/configuration, accepted-socket, and internal-recovery outcomes in R-D1
are deliberately absent from the wire registry."* Verified at the bytes:
`ParticipantStateCorrupt` appears nowhere under `crates/liminal-protocol/src/wire/`,
while `StaleAuthority` — a positive control run through the same predicate over
the same directory — is present at `wire/response.rs:1744`.

So a whole class of outcomes is *designed* to end in silence, and the register
row for it (line 5714) states the remedy in its own words: *"fail the
conversation closed, preserve durable bytes, and create no wire retry/poll."*

⚠ **The transcription pin is stale.** `wire/authority/mod.rs` cites the contract
at `55856ae3c53206f9c662e6815650dfc67a89ce85`, whose blob is `433f80ba`; HEAD's
blob is `692e85cf`, +97/-13. The register table has moved off the cited line
numbers (5624-5689). The citation names a rev, which is what makes this
detectable at all — but it no longer resolves to the rows it claims. Separate
small item; it does not change the analysis above, which was read at HEAD.

## Therefore #14 is a CLASSIFICATION problem, not a plumbing problem

The defect is not "the server fails to speak." It is that **two populations share
one exit**:

- **Class A — genuinely internal.** State corruption, decode failure, broken
  invariants over durable bytes. The contract says these fail closed with no
  wire value. Silence here is compliant, and inventing a wire answer for them
  would contradict the register.
- **Class B — ordinary refusals wearing a `StateError::invariant` coat.** A
  well-formed, correctly-authorized request that the server declines for a
  reason the client could act on. These are entitled to a frame and do not get
  one.

The red test's case is Class B and proves the class is non-empty: a credential
attach with the CURRENT generation and CORRECT secret, against a binding sitting
in `PendingFinalization`, passes `lookup_credential_attach` as `AuthorizedFresh`
and then dies at `ops_attach.rs:215` on a bare `StateError::invariant`. Nothing
about that is corrupt state. The client is simply not told.

**The work of #14 is to partition the 28 and say why for each one.** Class B
sites need either an existing register row they were always entitled to, or a
named gap in the register — which is a contract question, not a code question,
and would go to the contract's owner rather than being invented locally.

## The discriminator, and it is testable

Most of the 28 are **impossible-pairing guards**. The protocol crate returns a
union type covering several operations, and the server asserts the variant
belongs to *this* operation: *"leave conflict row observed in the
credential-attach lookup"*, *"enrollment provenance row observed in the
credential-attach lookup"*, *"authorized attach routed through the refusal
mapper"*, *"attach marker proof classified under a foreign operation envelope"*.
`ops_attach_lookup.rs` @ `0c3bacc` states the intent in its own doc: *"The crate
selects the exact typed refusal; ... observing one is a loud invariant failure —
never a silently hand-built outcome."* That is correct design. If one of those
fires, the process really has lost internal consistency and has nothing truthful
to say to a client.

⇒ **A guard is Class A only if its impossibility premise is TRUE.** That is the
whole discriminator, and it is a reachability question — measurable, not a
matter of opinion.

**And the one premise anybody actually tested was false.** `ops_attach.rs:215` @
`0c3bacc` reads:

```rust
BindingState::PendingFinalization(_) => Err(StateError::invariant(
    "pending finalization observed in a binding that commits detaches immediately",
)),
```

The message asserts the state cannot arise. The red test at `557eed7` reaches it
with a well-formed, correctly-authorized, current-generation attach — and board
#23 reaches it a second way, through a connection dropped under retention
pressure. **The comment is not describing an invariant; it is describing an
assumption that no longer holds.** The verb is the contract and the comment is
not.

⇒ So #14 is not "28 sinks must learn to speak". It is **one proven-false
impossibility claim hiding inside a population of correct guards**, and an open
question of how many of the other 27 premises are equally stale. That question
is answered by reachability analysis per site, not by mapping them all to frames.

## Sub-populations found, with their live/dead status

- **`ops_attach.rs` (4).** `:215` is Class B, reachable, proven. `:266`, `:295`,
  `:471` are Class A: the two `{error:?}` sites flatten a typed
  `AttachCommitError` into a string, but all six of its variants are
  internal-consistency failures ("cell/history mismatch", "canonical receipt
  invariant rejected after verification") and the Debug text is preserved **in
  the log**, so nothing client-actionable is lost.
- **`ops_attach_lookup.rs` (10).** All impossible-pairing or
  missing-stored-receipt guards. Class A on their face — each premise still
  wants its own reachability check before that is banked.
- **`ops_attach_verify.rs` (10).** Split by match arm on `(binding, mode)`:
  the `Ordinary` and `Superseding` arms are LIVE; the `Fenced { .. }` arms are
  currently unreachable in production — board #13's constructor census found
  ZERO production constructors of `StoredAttachModeV3::Fenced`. ⚠ They become
  live the moment a fenced repair energises that path, which is the sense in
  which the banked "TEN in the fenced verifier the repair ENERGISES" was right.
- **`ops_attach_capacity.rs` (1), `ops_attach_finalizer.rs` (3).** Not yet
  classified. Named here so the census says which refs it saw.

## Reachability sweep — first tranche (`ops_attach_lookup.rs`, 4 of 10 settled)

Method: constructor census over the COMPLETE producer set, then the server's own
call site. Each result is a measurement, not a reading of the prose.

**PROVEN CLASS A — the premise is true, silence is contract-compliant:**

1. *"leave conflict row observed in the credential-attach lookup"*. The complete
   producer set is `lookup_credential_attach` (`lookup.rs:339-420`) plus its one
   delegate `lookup_live_attach_receipt` (`:422-468`). Across both, the ONLY
   `AttemptTokenBodyConflict` construction is `::CredentialAttach`. A Leave row
   cannot arrive. Unreachable.
2. *"enrollment provenance row observed in the credential-attach lookup"*. Same
   producer set; the ONLY `ReceiptExpired` construction is `::CredentialAttach`.
   Unreachable.
3. *"retired identity observed in a binding that mints no tombstones"*. I
   suspected this one — it is a claim about SERVER STATE, structurally identical
   to the claim already proven false at `ops_attach.rs:215`. **It survived.**
   Both identity inputs to the lookup are hardcoded on the attach path:
   `ops_attach.rs:82` passes `PresentedIdentity::Live(&slot.member)`, and
   `ops_attach_lookup.rs:45` binds `ResolvedIdentity::Live(&self.member)`. The
   server mints a `Retired` identity in exactly one place, `ops_leave.rs:164`,
   which is the leave path and not this one. Unreachable; premise TRUE.
4. *"authorized attach routed through the refusal mapper"*. Caller-discipline
   guard: reachable only if `ops_attach.rs:87`'s `AuthorizedFresh` check and the
   mapper disagree, which is one function calling another six lines away. Class A.

## Reachability sweep — tranche 2: `ops_attach_lookup.rs` is CLOSED, 10 of 10 Class A

The remaining six settled, all by construction rather than by inspection.

**The four in `marker_bearing_attach_refusal`.** This function IS live and its
trigger is client-controlled — `ops_attach.rs:117` enters it whenever
`request.accept_marker_delivery_seq.is_some()`, a field the client supplies. So
these are not husk guards. They are still all unreachable:

5. *"marker-bearing attach classification without a presented marker"*.
   `MarkerProofInput::credential_attach` (`marker_proof.rs:29`) returns `None`
   **exactly when** `accept_marker_delivery_seq` is `None`, and the caller only
   enters when it `is_some()`. **The caller's guard and the constructor's `None`
   condition are the same predicate on the same field.**
6. *"attach marker proof classified as a marker-ack no-op"*. `AckNoOp` is
   reachable only at `marker_proof.rs:241-245`, which requires
   `input.is_marker_ack()`. The input here is `MarkerProofInput::CredentialAttach`.
7. *"attach marker proof classified under a foreign operation envelope"*. Every
   `MarkerMismatch`/`MarkerNotDelivered` in the selector is built with
   `input.into_wire_request()` — it echoes the input it was given, and the input
   is `CredentialAttach`. (Premise is testable and tested:
   `marker_proof_tests.rs:103` pins that the envelope is preserved.)
8. *"marker proof permitted although no marker was ever delivered"*. The server
   constructs `MarkerProofState::new(cursor, false, None, proof_epoch, None)`,
   so `expected_marker_delivery_seq` is `None`, and `marker_proof.rs:248` returns
   `MarkerMismatch::NoMarkerExpected` **before any path to `Permit` exists**.

**The two receipt-replay guards.** `CredentialAttachTokenPhase::LiveReceipt` is
constructed only inside the `Some(attach)` arm of `attach_token_phase`
(`ops_attach_lookup.rs:47-54`), `Bound`/`UnboundReceipt` are produced only from
the LiveReceipt phase, and the mapper then reads `slot.attach` on the same
borrow within the same request. `slot.attach` is `Some` by construction wherever
those arms run.

⇒ **`ops_attach_lookup.rs`: 10 of 10 Class A. The file is closed.** Ten guards,
zero silences that a client was entitled to hear about.

### Two things this tranche found that are not #14

**It narrows board #12.** `#12` tracks `accepted_marker_at_cursor` hardcoded
`false` at three sites, one of them this file's `MarkerProofState::new`. **At
this site the hardcoded `false` is INERT**: the only branch that reads it
(`marker_proof.rs:241`) also requires `input.is_marker_ack()`, which is false by
construction on the attach path. Whatever #12 is elsewhere, it cannot change an
outcome here. One site down, on evidence.

**A functional limitation, honestly stated.** Because
`expected_marker_delivery_seq` is hardcoded `None`, a client that presents
`accept_marker_delivery_seq` **always** receives a `MarkerMismatch` and can never
attach — only two of the selector's five outcomes are reachable at all. That is
the "no participant-record delivery pump yet" boundary the call site documents.
⚠ It is NOT a #14 item: the client gets a real frame on the reliable path. It is
a capability gap, and it belongs to whoever owns the delivery pump.

⚠ **An instrument note, because it nearly produced a clean wrong answer.** Two of
my own measurements of the same population disagreed — one predicate returned
zero `Retired` constructions, another found one at `ops_leave.rs:164`. The count
was wrong: `PresentedIdentity::Retired` does not match the turbofish form
`PresentedIdentity::<Digest, Digest, Digest>::Retired` that the code actually
uses. **When two machine readings disagree, measure; do not pick.** A zero from
the narrower predicate would have been reported as "the server never mints
tombstones at all", which is false.

## A conformance gap found in passing — NOT a #14 silence, its own row

`CredentialAttachResponse::retired()` exists as a register-admitted outcome
(rows 5648/5659/5667) and has **ZERO production callers** — the only reference is
`wire/authority_tests.rs:257`. Positive control through the same predicate:
`stale_authority` has a real caller at `ops_attach_lookup.rs:208`.

Because both identity inputs are hardcoded `Live`, the server cannot produce the
`Retired` outcome the register admits for credential attach. An attach naming a
participant that has left appears to fall to `ops_attach.rs:74`'s
`participant_unknown` arm instead — collapsing "never existed" and "existed and
is retired", which the register deliberately distinguishes.

⚠ Stated as a CANDIDATE, not a finding: I have not established what leave does to
the slot (whether it is removed or retained with a retired identity state), and
that determines which arm actually runs. It is **not** a #14 item either way —
the client gets a frame, just possibly the wrong one. Separate row.

## Reachability sweep — tranche 3: capacity and finalizer, and a COUPLING that constrains the fix

**`ops_attach_capacity.rs:184` — Class A, type-system gap.** `let [lrs, lrp, ps,
pc, pp]: [CapacityCounter; 5] = counters.try_into()`. `ordered` is a fixed
five-element array literal (the frozen five-scope order), the loop pushes exactly
one counter per scope, and every non-`Valid` branch returns early. `counters.len()`
is 5 by construction at that line; Rust simply cannot prove it statically.
Unreachable.

**`ops_attach_finalizer.rs:68/:88/:115` — Class A on production data, but
CONDITIONALLY, and the condition is the Class B site itself.** All three sit
behind `let BindingState::PendingFinalization(pending) = binding else { return
Ok(None) }`, so all three need a binding in `PendingFinalization`.

⇒ ⛔ **THE COUPLING. `allocate_attach_mode` — the `:215` gate — has exactly ONE
caller, `ops_attach.rs:153`, and it is on the LIVE path only. `replay_attached`
(`:222`) goes straight to `commit_attach_entry` with `CommitMode::Replay` and
BYPASSES THAT GATE ENTIRELY.**

So today these three are unreachable on production data only because the live
path refuses at `:215` to ever *create* the stored state that would reach them.
That refusal is the Class B defect — the one site #14 exists to fix.

⇒ **This constrains #14's remedy, and it is the most useful thing in this
tranche.** A fix at `:215` that merely *admits* a `PendingFinalization` attach
would let that attach be COMMITTED AND STORED; on the next cold replay it arrives
at `select_fenced_finalizer` through a path `:215` does not guard, and meets
three bare-close `StateError::invariant` sinks. **A repair at `:215` that admits
the state energises three downstream silences on a path its own gate never
covers.** This is the same "the repair ENERGISES" shape already flagged for the
fenced verifier, now with a second instance and a named mechanism.

⚠ It is also a live instance of the banked correction that cold replay
re-executes attaches, **so the live and replay paths are not disjoint** — a live
guard is not a replay guard.

**⇒ ACCEPTANCE CONSTRAINT for whoever fixes `:215`:** answering the client with a
frame is necessary and NOT sufficient. The fix must either (a) keep refusing to
commit the state, changing only how the refusal is *delivered*, or (b) if it
admits the state, carry the replay path with it and account for all three
finalizer guards. **Option (a) is the smaller, safer change and is what the red
test at `557eed7` actually asks for** — it asserts `Respond`, not admission.

## Honest limits of this pass

- Class A status is asserted from the *shape* of each guard, not yet from a
  reachability proof per site. One such premise has already been proven false;
  that is reason to check the rest, not to assume them.
- `ops_attach_capacity.rs` and `ops_attach_finalizer.rs` (4 sites) are
  unclassified.
- The last hop below `FrameAction` — the actual socket write, flush, and drain —
  is established by reading, not by assertion, exactly as R4's test doc says.

⛔ The red branch `silent-close-attach-refusal` (`557eed7`, 596/3) is
LOAD-BEARING and stays red. Nobody fixes it, tidies it, or lands it.

## Why #37 waits on this

#37 (retain provenance only for delivery-observed receipts) needs a
discriminator: was this receipt actually delivered? A Class B refusal that exits
as a bare close is precisely a receipt whose delivery was never observed and
never can be. Until the attach path can distinguish "told the client" from
"closed on the client", #37 has nothing to key on. **They are the same event
from two sides.**

### ⚠ AMENDED 2026-08-12 — the paragraph above is wrong about its own lane

The section above stayed as written because it is the reasoning the amendment
replaces, and a struck premise is worth more in place than deleted.

**The funnel does not unblock #37, and could not have.** The one
`PresentedRefusal` raise site is `ops_attach::allocate_attach_mode`'s
`PendingFinalization` arm, called at `ops_attach.rs:146`; `attach_commit` — the
only path to the retention site `install_attach_receipt` (`ops_attach.rs:415`,
insert at `:433`) — is called at `ops_attach.rs:157`. **A refusal returns eleven
lines before any receipt is minted, so the population the funnel made
wire-visible and the population that retains provenance are disjoint.** #14 gave
a frame to a path that never retained anything.

Worse, the discriminator the paragraph asks for still does not exist. Nothing on
a receipt records delivery (`AttachReceiptState`, `state.rs:48-67`), the attach
path is documented as having no delivery facts at all (`ops_attach.rs:116-119`;
`ops_attach_lookup.rs:164-168`, *"a capability gap owned by the delivery pump"*),
and the one layer with a delivery notion says explicitly that it is not one —
`supervisor.rs:379-381`, *"`Ok` promises ADMISSION, not delivery"*. It is also
volatile and absent from `StoredAttachAllocation` (`log_v3.rs:141-150`), so
keying retention on it would make cold replay rebuild a different provenance set
than the live path produced — the very "a live guard is not a replay guard"
hazard recorded earlier in this document.

**THE RULED DEFINITION (Tom's seat via Waffles, 2026-08-12, binding).** "Delivery
was observed" means **the client demonstrably possessed the secret the receipt
minted**. Two rejected alternatives, recorded so the choice is not re-litigated
by drift:

- *Observer progress reaching the receipt's delivery seq* — REJECTED. It
  measures the observer record stream: a third party saw the record, not that
  the client was told.
- *"Told the client" vs "closed on the client"* — the sentence above, and
  unimplementable without the witness build. Building that witness inside a leak
  fix is a smuggle; it queues as its own lane (durable schema version plus the
  delivery pump that owns the gap by name) and owns any future upgrade of
  "observed" to told-the-client.

**Why the definition needs no new plumbing.** Only enrollment and credential
attach are secret-bearing, a fresh attempt token is verified against
`slot.attach_secret` (`ops_attach_lookup.rs:45-46`) and only `AuthorizedFresh`
reaches a commit (`ops_attach.rs:90-92`), and every committed attach supersedes
the receipt that minted the secret it presented. So proof of possession is
structural, and already durable:

| receipt | possession proven ⟺ |
|---|---|
| enrollment | `slot.enrollment_receipt_ended.is_some()` |
| attach receipt N | it has been retired into `slot.attach_provenance` |
| the CURRENT receipt, and an unended enrollment receipt | never yet — UNPROVEN |

Every input is rebuilt by cold replay, so retention stays a pure function of
durable bytes.

**What changed:** the unproven pair stops OCCUPYING a stage-8 provenance slot
(`occupancy.rs`, `ops_enroll_capacity.rs`, `ops_attach_capacity.rs`). An
enrolment that crashes before ever attaching now leaves no provenance residue.
Exactly one provenance entry is still created per committed attach — it just
belongs to the predecessor rather than to the receipt being minted — so the
crate's "admit one" selector algebra and the frozen R-D1 scope order are
untouched. This is not the #39 caps lane: no cap number, limit, or scope order
moves.

**What did NOT change: classification.** Occupancy and classification read
different state. Every fingerprint still classifies through its own window in
`ops_attach_lookup`; only what consumes a signed slot moved. Pinned by
`tests_37_observed_provenance::r_c0_token_phase_classification_is_unchanged_by_observed_provenance_retention`,
red-proved against a build whose `install_attach_receipt` retains nothing.
⛔ That pin's first draft PASSED against that mutation and was vacuous: both
arms presented generation 2, whose successor is witnessed by the CURRENT
receipt, which no retention change removes. The arms now present `GEN_ONE`,
whose successor is witnessed only by the retained record. **A neutrality pin
must be exercised on a generation whose only witness is the thing being
changed.**

**⚠ NAMED REMAINDER — what definition (3) does not reach.** Measured against the
lane's outcome gate ("an afternoon of run-crash-fix-run cycles consumes nothing
durable"), a crash cycle still consumes:

1. **One identity slot, permanently.** `capacity_contribution`'s
   `identity: self.next_participant` (`occupancy.rs`) plus the retirement
   tombstone reservation. Identity is monotonic by design and this lane does not
   touch it. **This is the dominant residue of a run-crash-fix-run afternoon and
   definition (3) does nothing for it.**
2. **The live receipt body**, until `attach_receipt_ttl_ms`. Unproven possession
   frees the provenance tail (`receipt_provenance_ttl_ms` minus
   `attach_receipt_ttl_ms`), not the receipt window itself.
3. **The durable operation-log rows** of the enrolment. Retention accounting is
   derived; the log is not compacted here.

Per the ruling: if a measurement shows crash cycles still consuming durable
state through a path (3) does not reach, that measurement returns the definition
to Waffles's desk. Item 1 is that path, and it is named here rather than
discovered later.

## Build outcome — the funnel landed, and the sweep's own classification moved

Written on branch `attach-funnel-presentation`, base `edeabeb`, code at
`313912c`. Every citation below is at `313912c` unless it names another rev.

**The wire half is built and it is one place.**
`production/presented_refusal.rs` carries a Class B refusal out to
`handler_semantic::conversation_operation_with_impact`, the single wrapper
every semantic arm routes through, where it rejoins the ordinary response path.
`ParticipantSemanticError`'s "never invent a lifecycle response" property is
preserved by construction rather than by convention: `PresentedRefusal`'s only
constructors take a `CredentialAttachResponse` or a `DetachResponse`, and there
is none from a bare `ServerValue`.

⛔ **The precondition the carrier imposes, because it is not obvious and it is
the thing a future sink will get wrong.** A `PresentedRefusal` becomes an `Ok`
at the arm boundary, so the conversation owner is RETAINED — where every other
`StateError` is an `Err` there and `with_conversation_reconciliation` answers an
`Err` by dropping the possibly part-consumed owner and cold-replaying durable
truth. A `PresentedRefusal` may therefore only be raised where the transition
has consumed no authority: no `take_frontier`, no `take_shell`, no
`slots.remove_entry`, no append. A sink that ignores this buys its frame with
corruption.

**`ops_attach.rs:270` (`allocate_attach_mode`'s `PendingFinalization` arm) is
closed.** It answers `ObserverBackpressure`, and the row is derived rather than
chosen: `PendingFinalization` is minted only where
`binding_terminal.rs`'s `Pending` arm requires
`hard_observer_progress < key.delivery_seq` — its type calls itself the
"observer-blocked pending terminal admission" — and `PARTICIPANT-CONTRACT.md`
line 1527 requires that such a slot is settled when "progress wake appends
exactly one correctly ordered record", while register row 5954 pairs the detach
that CREATES the state with `ObserverBackpressure`. So the blocked resource is
hard-observer progress and the admitted credential-attach row (5943) already
carries the matching retry discipline. The refusal, the predicate and the
(empty) durable effect are unchanged; only delivery moved, per this document's
own acceptance constraint (a).

⚠ **A CORRECTION to the tranche-1 classification above, and it matters more
than the site it fixes.** This document banked `ops_attach.rs`'s `:471` (now
`:531`) as Class A on the premise that the `{error:?}` sites "flatten a typed
`AttachCommitError`" whose six variants are all internal-consistency failures.
That is true of `:355` (`protocol attach transition failed`). It is **not** true
of `:531`: that site flattens a **`LiveFrontierError`**, a different type, and
its `Precedence` variant is documented as *"a mandatory immutable/recovery
transition has precedence"* — lane occupancy, which this repository elsewhere
calls *"a designed structural boundary ... not corruption"*
(`dispatch.rs`'s `BindingTerminalAdmissionRefused` doc). `ops_session.rs:241`
is the identical site on the detach arm. **Two sites were classified by the
type they were assumed to carry rather than the type they carry.** The lesson is
the one already in this file's instrument note: a classification is not a
measurement until it names what it read.

**Those two sites are NOT fixed here, and the reason is a contract question.**
`Precedence` has three distinct clearing conditions and they do not share a
retry story:

1. an immutable **binding-terminal** candidate — observer-blocked, cleared by
   the progress wake, and `ObserverBackpressure` would be correct;
2. an immutable **marker** candidate awaiting its drain — cleared by the next
   record admission's `DrainFirst` (`ops_frontier.rs`) or by boot drain, i.e.
   by *another participant's write*; and
3. an armed **fenced-recovery block** — cleared by the fenced attach that
   consumes it.

The frozen R-D1 register admits no row for (2) or (3), and its own text closes
the question against improvising one: *"The cross-cutting and operation rows
together are exhaustive; no generic 'proof/admission refusal' exists."*
Answering (2) with `ObserverBackpressure` would tell a client to wait for an
`ObserverProgressed` that nothing has promised to send; answering it with
`MarkerClosureCapacityExceeded { scope: RecoveryFence }` would borrow a row
whose trigger the register defines as something else entirely. **This needs
contract surface, not a guess.**

Discriminating only sub-case (1) is *also* not available as a cheap partial. The
refusal is raised inside `attach_commit` after `slots.remove_entry`,
`take_frontier`, and `prepare_selected_fenced_finalizer` have all run, so it
violates the carrier's precondition; `LiveFrontierFailure::into_parts` returns
the owner, but the in-memory finalizer state is not restored by it. Moving the
predicate to a pre-flight is worse rather than cheaper: it would refuse before
`verify_attach_mode`'s ten guards, so a genuinely corrupt state that happened to
arrive while the lane was occupied would be answered "retry later" instead of
failing loudly — the fix that closes a different failure, and in the direction
that hides corruption.

**Board #12's last site is closed in the same lane.**
`ops_attach_lookup::attach_marker_proof_state` now derives
`accepted_marker_at_cursor` from `ConversationAuthority::marker_record_accepted_at_cursor`,
the same census the live marker-ack site has used since `f8753fb`. This
document banked the site as INERT on the argument that the only branch reading
the flag also requires `input.is_marker_ack()`; the argument was right, and it
is now a test that measures the selector with the flag both ways and carries a
marker-ack positive control through the same comparison. The site no longer
depends on the argument being remembered.

**Standing count, and it names its population.** Of the **28** counted over the
seven attach-path files: one was Class B and is answered (`ops_attach.rs`'s
`allocate_attach_mode` arm), one is misclassified and blocked on contract
surface (`ops_attach.rs`'s frontier-transition site), and **26 remain Class A**
on the tranches above. `ops_session.rs:241` is the detach twin of the
misclassified site and was never inside the 28 — the original census covered
the attach path only, so the detach arm has had no sweep at all and its
`StateError::invariant` sites are unclassified.

## The detach arm's first two classifications, measured at `902f514`

The detach arm carries **eight** `StateError::invariant` sites
(`ops_session.rs` `:96 :101 :190 :205 :214 :223 :241 :256`). Two of them sit at
the LOOKUP stage, ahead of `allocate_position` (`:117`) and `detach_commit`
(`:125`), where every sibling arm already returns
`Ok(ArmOutcome::respond(..))` — so unlike `:241` they satisfy the carrier's
consumed-no-authority precondition and are the arm's only cheap candidates.
Both are classified here rather than left to the phrase "unclassified".

**`:96` — `Retired` — CLASS A, and now measured rather than assumed.** Its
message asserts "retired identity observed in a binding that mints no
tombstones". That is the same *form* of claim as the one the funnel proved
false at `allocate_attach_mode`, so it was checked at the bytes rather than
read: `ConversationAuthority::retired` is constructed empty
(`state.rs:398`) and **has no insertion site anywhere in `liminal-server`** —
the only `retired.insert` in the crate belongs to `outbox.rs`'s unrelated
`BTreeSet`. The map is therefore permanently empty, no lookup can resolve a
tombstone, and the message is accurate. ⚠ The same single measurement settles
the identically-worded `ops_attach_lookup.rs:312` and `ops_enroll.rs:554`, so
tranche 1's Class A call at `:312` is confirmed by measurement rather than by
its own construction argument.

**`:101` — `PendingReplayRequired` — CLASS B, an exact register row exists, and
it is still NOT built here.** This is the detach arm's true twin of the sink
the funnel answered, and every part of the case is positive:

- **It is reachable, by this lane's own fixture.** The register mints
  `PendingFinalization` and `detach_replay::Pending` in ONE transition (the
  "first accepted while append is blocked" row), so the pending detach cell is
  born with the binding state whose reachability `tests_14_attach_presentation`
  already proves. The message "pending detach cell observed in a binding that
  commits detaches immediately" is false for the same reason its attach twin's
  was.
- **The row is exact, not borrowed.** The register's
  "`DetachRequest` exact-token replay while cell is Pending" row is
  `ObserverBackpressure`, and `DetachLookupResult`'s own doc names this variant
  "an exact pending token requires the equality/drain/rewrite transition".
- **The protocol already implements the whole transition.**
  `PendingReplayRequest::apply` (`detach.rs:690+`) covers all three arms.

**Why it is nonetheless out of scope for the funnel, stated as a boundary and
not a deferral.** Only the EQUALITY arm
(`observer_progress == cell.refused_epoch()`, `PendingDrainDecision::NotAttempted`)
is pure presentation — it returns the cell unchanged. The other two arms are
record mutations: `StillBlocked` rewrites `refused_epoch` to the newer
progress, and `Committed` completes the detach and appends. #14's constraint is
that a refusal which commits nothing must still commit nothing, and this lane
changes no record mutations; driving a drain decision so the server can choose
between those arms is a server capability that does not exist yet, not a
delivery change. Building only the equality arm and leaving the others as bare
closes would answer the common retry and still strand the client precisely when
progress HAS moved — the fix that closes a different failure.

**⇒ The detach arm's standing count.** Of eight sites: one Class A by
measurement (`:96`), one Class B with an exact row and a named blocker
(`:101`), one misclassified `LiveFrontierError` twin (`:241`), and **five
unclassified** (`:190 :205 :214 :223 :256`).
