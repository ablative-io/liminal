# Park-Flip / D1 Activation — one seam, one plan

**Status:** SIGNED 2026-07-12 (Waffles the Terrible reviewer-of-record;
build leg Vesper Lynd; pair asks (a) timer shape and (b) re-register
clause folded in this revision). Build dispatch gated on the beamr 0.13.1
artifact resolving — NOTHING merges before the pinned artifact exists. Closes the idle-connection busy-spin incident
(docs/stack-review/AION-HOST-RESOURCE-INCIDENT-2026-07-11.md) by activating
the D1-prep consumer machinery (b3c8a38, landed dark) against the beamr
readiness service (beamr main 1f98f73, composition commit 6, pair-certified).
**Base:** liminal main a028711.
**Ground truth:** norn scout claude-scout-parkflip (envelope
`~/.norn/delegations/claude-scout-parkflip.cWin0z`), every site cited below
verified in that pass at a028711.

## 0. The seam verdict (the question this plan was asked to settle)

**Park-flip and D1 activation are ONE change, not two.** The busy-spin is a
single live return path — `ConnectionProcess::handle_slice` unconditionally
returns `NativeOutcome::Continue` after its drains
(crates/liminal-server/src/server/connection/process.rs:107-170; the
WouldBlock→requeue mapping at :198-210). The D1-prep commit landed every
consumer-side prerequisite (outbound tri-state, bounded wakeable
subscription inboxes, nonblocking pending-reply correlation, the single
READY atom/waker, typed limits, teardown prep, slice counters) and
deliberately stopped at that return: its own commit message says "NO
parking", gated on commit 6. Activating D1 *means* replacing that
`Continue` with a registered, armed, probe-guarded `Wait` — there is no
intermediate activation that doesn't flip the park, and no park flip that
doesn't activate the machinery.

**Coverage cut (pre-agreed conditional, now proven):** this plan takes the
park-flip + connection-scheduler D1 activation. The SDK receive path
(crates/liminal-sdk/src/remote/tcp/connection.rs — its own WouldBlock
handling at :397) is UNTOUCHED by this change and stays with Cally Ray.
Shared surfaces to coordinate, not divide: root Cargo.toml/Cargo.lock, and
crates/liminal-server/tests/sdk_tcp_e2e.rs if both lanes add cases.

## 1. The dependency-identity trap (do this FIRST, verify mechanically)

Liminal resolves `beamr = "0.13.0"` from crates.io — and beamr main at
1f98f73 is ALSO versioned 0.13.0. The semver string cannot distinguish the
readiness-bearing code from the locked API-less artifact: a manifest that
merely names the feature would keep resolving the old checksum and the
build would fail (or worse, a future lockfile refresh would flip meaning
silently).

**Plan:** beamr publishes **0.13.1** from 1f98f73 (Tom's ops action — same
decision class as the pending liminal 0.2.4; the publish chain this fix
completes is beamr 0.13.1 → liminal 0.2.4 → aion pin bump). Liminal then
pins `beamr = { version = "0.13.1", features = ["readiness"] }` — the
explicit feature request even though readiness sits in beamr's defaults, so
the dependency is legible in the manifest, not implied by someone else's
default set. **Acceptance:** `cargo tree -p liminal-server -e features`
prints beamr with feature `readiness`; a build against 0.13.0 must FAIL to
compile the new code (the API absence is the guard). Interim development
may use a `[patch.crates-io]` path override, marked not-for-commit, exactly
as the failover re-proofs did.

## 2. Composition (beamr doc §8, first-consumer recorded)

`SupervisorInner::new` composes readiness **Owned on the CONNECTION
scheduler only**; channel/conversation schedulers stay Disabled (they own
no fds). Build failure is **fatal-loud at server startup** — a server that
cannot park connections IS the incident, refused at birth (doc §8's
recorded composition; `SchedulerServices::…owned_readiness()`). The D2
census pins gain the `beamr-readiness-poll` inventory line assertion.

## 3. Registration lifecycle (the host-reachable token rule)

Registration happens in-slice (fd + Interest + pid + READY marker); the
token must ALSO be host-reachable: beamr's ACK'd deregistration is
host-side (`Scheduler::readiness_deregister`), and a token held only
inside `ConnectionProcess` state cannot be deregistered after an external
kill/reap. **Plan:** the supervisor's connection record carries the token — set
ONCE at first registration, dropped at deregistration. There is no
live-connection re-register case (pair ask (b): the "updated on
re-register" clause is STRUCK as decoration): one connection = one
process = one fd for its whole life, re-arming is `rearm` on the same
token never a fresh registration, and every path that would invalidate
the registration (EOF, close, kill, reap, ServiceFailed→fatal-loud) ends
the connection. Deregistration
funnels through the EXISTING single removal/finalization path — covering
spawn rollback, EOF, protocol close, error close, force close, reap,
external termination, and shutdown sweep. The ACK'd dereg completes BEFORE
the last fd handle drops, so kernel fd reuse cannot race a stale
registration (beamr's generation wall is the second net, not the plan).

## 4. The slice contract (the incident doc's ordering wall, now enforceable)

`handle_slice` is refactored to compute a work/interest decision instead of
returning unconditional `Continue`:

1. Drain, bounded: mailbox (whole-mailbox drain per READY burst,
   preserved), socket reads to WouldBlock or budget, subscription inbox,
   pending-reply completions/expiries, controls, outbound flush.
2. Budget exhausted with known in-memory work remaining → `Continue`
   (runnable; no arming needed — we KNOW there's work).
3. Otherwise arm/rearm: READABLE always; WRITABLE **only** when the
   outbound drain returned `WouldBlockWithResidue` (arming writable on a
   drained socket is permanently-ready and rebuilds the busy loop with the
   readiness service as the spinner — the exact failure this plan exists
   to kill). `Drained` parks read-only; `Progress` stays runnable.
4. **Final nonblocking probe of EVERY consumer-owned source** (socket,
   inbox, reply queue + deadline, controls, output residue) AFTER arming —
   the C1/C4 pre-Wait race barrier the incident contract pins
   (AION-HOST-RESOURCE-INCIDENT-2026-07-11.md:70-83). Anything found →
   `Continue` (the registration stays armed; a spurious wake is tolerated
   by contract).
5. Nothing found → `NativeOutcome::Wait`.

Registering once at spawn is INSUFFICIENT — beamr readiness is one-shot;
every parked slice re-arms (step 3) after draining. Duplicate READY
markers are idempotent by the whole-drain discipline.

## 5. The missing wake: pending-reply deadlines

Scout finding: pending_reply.rs:32-39,234-235 and process.rs:277-282
explicitly require a deadline-driven READY event, and no timer source
exists — parking without it strands timeout frames under zero traffic.
**Plan:** admitting a pending-reply entry arms a deadline timer whose
expiry fires the SAME `ReadyWaker` (one wake vocabulary, no second
channel); completion/close cancels it; the woken slice reuses the existing
`expire_due`. This rides liminal's existing timer facilities — it is a
consumer-side wake source, NOT a claim on beamr's readiness service (beamr
doc §9 explicitly keeps timer expiry off the service).

**Shape (pair ask (a), stated): per-pending-entry timers** — one armed at
admission, cancelled on that entry's completion/close, so cancellation
rides the entry's existing completion path instead of a per-connection
min-deadline state machine that must re-arm whenever an earlier deadline
is admitted. **Bound:** the pending-reply typed limit caps live timers at
(pending-reply cap × live connections); every armed timer belongs to
admitted active work — an idle parked connection holds ZERO timers, so
this is active-work cost, never idle cost. Expiry batch-drains via
`expire_due`, so co-expiring timers still produce one slice of work.

## 6. Test inversion + acceptance matrix

- `process_wake_tests.rs:59-70` currently PINS the busy-spin (idle slices
  keep increasing — the deliberate dark-land assertion). It flips to its
  mirror: an idle parked connection's slice count is FLAT across a soak.
- Acceptance matrix (each an explicit test, per the incident contract):
  readable→Pong→repark; blocked-write→WRITABLE rearm→exact flush;
  subscription publish wake; reply-availability wake; timeout-ONLY wake
  (zero traffic); controls/push delivered to a parked process; duplicate
  marker idempotence; EOF/HUP → dereg through the single removal path;
  external kill + fd reuse (host-side dereg proves the token rule);
  pre-Wait race barrier (data arriving between arm and Wait is found by
  the final probe); connection churn > worker count.
- The G4 >96KB pin and the failover re-proof suites re-run on the
  activated build (owed re-verification, carried from #237).

## 7. Sequencing and gates

1. beamr 0.13.1 publish (Tom) — plan proceeds on a not-for-commit path
   override until then; NOTHING merges before the pinned artifact resolves.
2. One reviewed unit: dependency uptake + composition + token lifecycle +
   slice contract + deadline timer + test inversion. It is one semantic
   change (§0); splitting it manufactures a broken intermediate.
3. Gates: house battery (fmt/clippy -D/test workspace) + the §6 matrix +
   idle-quiescence soak (flat slice count AND the beamr inventory line
   showing the one parked poll thread) + sdk_tcp_e2e untouched-lane check.
4. Reviewer-of-record: Waffles. Cally gets the boundary note (§0 cut) with
   this plan attached before the build dispatches.

## 8. Explicitly out of scope

- crates/liminal-sdk/src/remote/tcp/connection.rs — Cally's lane, untouched.
- beamr changes of any kind (the service is consumed as certified; if
  activation finds a beamr defect it goes back through the pair, not into
  this diff).
- Shared readiness composition (doc §8: no Shared needed for liminal v1).
- liminal 0.2.4 / aion pin-bump publishes — sequenced ops actions after
  this lands, Tom's call.
