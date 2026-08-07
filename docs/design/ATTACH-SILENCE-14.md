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

**STILL OPEN in this file (6):** two × *"attach receipt replay without a stored
receipt"*, and the four inside `marker_bearing_attach_refusal`.

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
