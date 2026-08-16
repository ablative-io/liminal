# #195 diagnosis — killed-mid-attach + receipt deadline = permanent identity orphan

Lane task #78 (board); Waffles's board #195. DIAGNOSED at main `93d8cc7`
(every liminal cite @93d8cc7; manifold cites @f6f29c3, read-only). Specimen:
Waffles msg 0a70d0b0 (08-13), manifold day file block edfd6fa. Field shape:
TERM'd seat's resume record holds an issued in-flight CredentialAttachRequest;
fresh process cannot re-attach ("participant 5 gen 2" fossil, refused);
restart-heals REFUTED; A7 operator re-issue is the only cure (#74 field arc).

## The mechanism — three correct behaviors composing into an orphan

1. **Attach commit rotates the credential.** `stored_attach_allocation`
   mints a fresh secret (`ops_attach.rs:815`, `facts::mint_secret_bytes()`)
   and the commit installs it as the slot's current secret
   (`ops_attach.rs:509`); the generation advances to the successor (doc
   `:794`). The `AttachBound` response is the SOLE carrier of the rotated
   credential (`wire/response.rs:996-1006`, `attach_secret` field). A client
   killed between issue and response never learns the new credential.

2. **Receipt windows are fixed at commit and never re-open**
   (`ops_attach_lookup.rs:29-31`, `Slot::attach_token_phase`). Inside
   `receipt_expires_at`, a re-presented SAME token takes the `LiveReceipt`
   phase, verified against the receipt's own committed PRESENTED secret — the
   invalidated OLD secret, deliberately (contract row 4, `:50
   verifier_bytes = attach.verifier`) — and the lookup's `Bound` /
   `UnboundReceipt` arms REPLAY THE STORED OUTCOME including the rotated
   credential (`credential_attach_refusal` `:268-287`). Past the window:
   `ReceiptExpired` (typed, carries result/current generations, `:288-306`),
   then `StaleOrUnknownReceipt` (claims no commit proof, `:103-104`). The
   committed outcome becomes permanently unanswerable — by design (#37/#39
   occupancy caps bound secret-bearing receipt bodies).

3. **The SDK terminalizes the lost op but never spends the window.**
   `RemoteParticipantHandle::resolve_lost_operation_authority`
   (`liminal-sdk/remote/participant/recovery.rs:191-226`) consumes the
   restore-minted testimony and hands back the exact request AS DATA;
   `recover_expected_operation` (`:158-184`) covers only the UNISSUED case
   (`NotAvailable{already_issued: true}` for this shape). No SDK surface
   re-presents the retained envelope; nothing tells the embedder that
   re-presentation is the healing act or that a deadline is running.

**The healing act is lawful and client-reachable TODAY, wire-unchanged:**
re-present the exact retained envelope (same `attach_attempt_token`, same
generation, same old secret). `ClientBindingState::accepts_request` admits
the re-record (`client.rs:183-192` — matches the retained credential);
A2/#47 token dedup makes it at-most-once. Server answers, exhaustively:
- committed + in receipt window → outcome replay → client applies
  `AttachBound` → **healed with the rotated credential**;
- never committed → `AuthorizedFresh` → commits now → healed;
- committed + window expired → typed `ReceiptExpired` → honest terminal:
  re-issue required (A7);
- past provenance → `StaleOrUnknownReceipt` → same honest terminal.

Field consequence: manifold "terminalizes correctly (no re-send)" — the
correct-looking flow — so the receipt window expires unspent on every
killed-mid-attach, and the orphan becomes permanent exactly when the deadline
passes. The defect is a MISSING RECOVERY DRIVER, not any single wrong line.

## Fix shape (liminal-sdk, red-first; zero wire delta expected)

1. A composed SDK recovery driver for the issued-CredentialAttach testimony:
   resolve → re-record the exact retained envelope → send → apply the
   answer. Success arms heal in place; `ReceiptExpired` /
   `StaleOrUnknownReceipt` surface a NEW TYPED TERMINAL STATE
   ("re-issue required") the embedder can act on (A7 hand-off) instead of a
   generic refusal.
2. The driver fires at restore time, immediately — the A5 build-lane flag
   binds here: condition-2 retry discipline must not EXTEND the orphan
   window, so the probe is first-act, not behind backoff.
3. Detach ops keep their existing replay machinery (untouched); tokenless
   ops keep typed abandonment (untouched).
4. Red test = the natural embedder flow at base: commit attach via kill-shape
   (issued, response never consumed), fresh restore, resolve, attempt
   re-attach with retained credential + fresh token → refused
   (StaleAuthority) → THE ORPHAN, asserted at the transport. Fix-side pins:
   all four server-answer arms above + restart parity + the A5 no-extension
   pin.

## #104 cousin verdict: RHYME, NOT SHARED ROOT

#104 (refused enrolment deposits a half-built mailbox dir): the deposit sites
are manifold's — `mailbox/lock.rs:68` and `mailbox/resume_store.rs:159`
(@f6f29c3) `create_dir_all` BEFORE any operation commits, with no
failure-path disposal. #195's durable residue (resume.lpcr holding an issued
op) is CORRECT persistence (round-3 order: commit-seal → persist → release);
the defect is the missing consumer of it. Fixing either does nothing for the
other — two fixes, two repos, no double-fix hazard. They COMPOSE: the #195
driver's typed "re-issue required" terminal is the state at which manifold
can dispose/re-enroll cleanly — his #104 fix should consume that state
rather than invent its own probe.

## Honest residue

- "Broker closes pre-frame" in the specimen is NOT explained by these bytes:
  the refusal paths here answer typed responses (`ArmOutcome::respond`).
  The close may be manifold's broker layer or another seam — flagged to
  Waffles with the diagnosis; it does not change the liminal fix, but the
  field cross-check should watch for it.
- The receipt TTL length (config-owned since #39) bounds the healable
  death duration; a seat dead longer than the TTL is honestly terminal into
  re-issue. That boundary is policy, stated not hidden.

## Carried constraints

Red-first; fresh branch off landed main; baseline 2088/2/3 over 56; F8 pair
by name; battery exit captured directly; logs in-tree under
`gate-logs/p195-orphan/`; no protocol-crate WIRE delta (client-aggregate
additive API is fine; refusal enums are breaking per the A5 decoder
measurement). STOP clauses: any needed wire change, any pin conflict, any
measurement contradicting the mechanism above — flag to seat, never silent.
