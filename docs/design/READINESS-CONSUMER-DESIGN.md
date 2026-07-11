# Liminal readiness-consumer workstream — design

*2026-07-11, Hermes Crumpet. The liminal half of the aion host resource
incident fix. Normative dependencies: beamr `docs/READINESS-CONTRACT-SPEC.md`
(branch readiness-contract-draft @ 3fe7d47 — clauses C1–C4, consumer
requirements R1–R8, churn gates T1–T7) and
`docs/stack-review/AION-HOST-RESOURCE-INCIDENT-2026-07-11.md`. Evidence base:
Sol scout session 02468176 (envelope retained), verified against liminal main
@ 218a378 / 0.2.3. Status: DRAFT — routes through the review chain (norn
passes → Vesper Lynd review → independent certification by Vesper Lynd +
Waffles the Terrible) before any code. No code exists yet; the liminal
consumer merges only after beamr's §2.5 pinning suite is green on main.*

## 0. Principle and scope

**Sleeping must cost nothing, everywhere, forever.** This workstream removes
liminal's permanent-runnable invariant and every adjacent resident cost the
incident and its scout pass exposed. Five deliverables:

- **D1** — connection parking: the readiness-consumer redesign of the
  connection process (the critical fix).
- **D2** — capability-scoped worker front door (the high fix).
- **D3** — ephemeral store lifecycle ownership (the moderate fix).
- **D4** — conversation/finalization bookkeeping repair (scout-found leak
  class, same resource axis).
- **D5** — listener/health polling (evidence-gated follow-up after D1).

Lens v1.1 ships with this workstream in `liminal-assets-pack.md` (canonical
source of truth) and §7 answers it for everything resident this design
touches.

## 1. D1 — Connection parking

### 1.1 Slice shape

Implements contract §5.1 exactly (bounded-drain-then-Wait; the
budget-vs-`WouldBlock` split is normative and decided by the outbound
tri-state). Connection processes park via plain `NativeOutcome::Wait` only —
never a gated suspension (C2 scope) — asserted by test.

### 1.2 Structural changes, mapped to contract requirements

1. **Outbound tri-state (R2, shape-invariant).** `OutboundWriter::drain`
   returns `Drained` (empty), `Progress` (budget/partial progress, socket
   still writable), or `WouldBlockWithResidue`. Writable interest is armed
   iff `WouldBlockWithResidue`; existing 4 MiB bound and byte-exactness
   guarantees unchanged, tests extended.
2. **Subscription inbox notifier (R3, shape-invariant).** The shared inbox
   (`channel/subscription.rs`) gains a notifier slot installed at subscribe
   time: on empty→non-empty it calls `enqueue_atom_message(conn_pid,
   READY)` — cheap, non-blocking, safe on the publishing actor's slice.
   Both legs fire it: local `SubscriberRegistration::deliver` and remote
   `SubscriberProcess::accept_remote_frame`. Install-before-final-recheck
   ordering closes the publish-vs-park race consumer-side (C1 closes it
   VM-side). The `delivery.rs:1-9` every-slice assumption is deleted.
3. **Pending-reply continuation (R1(vi)).** The in-slice 5 s reply drain for
   reply-requested `ConversationMessage` frames is removed. The connection
   records a pending-reply continuation; `deliver_participant_reply` fires
   the connection marker; the next slice writes the correlated reply frame
   (or a timeout error frame when the deadline passes — deadline checked on
   wake and via a beamr timer, not by blocking). Removes the last
   bounded-blocking wait from the slice path and the four-concurrent-replies
   scheduler wedge.
4. **Single idempotent marker (R6).** One `READY` atom per connection; any
   marker triggers one full slice servicing all sources. Marker coalescing
   and duplicates are structurally harmless.
5. **Registration lifecycle (R4/R5).** Registration at spawn, keyed
   `(pid, generation)`. **One idempotent finalization path** — a single
   function used by every teardown caller: EOF, `ProcessStatus::Close`,
   outbound overflow/write-error, `mark_crashed`, `ForceClose`,
   `reap_crashed` (dereg must not require the dead process to run — the
   supervisor/reaper calls finalization), spawn rollback, and scheduler
   shutdown. Finalization: deregister readiness; purge queued controls for
   the pid; cancel all pending push slots; fire worker-unregistration once;
   release every subscription (removing inbox notifiers before drop);
   close/terminate every conversation actor AND participant (D4); remove
   runtime registry entries. A final sweep at shutdown catches force-close
   misses (closes the scout's "records merely logged" gap).
6. **Quiescence instrumentation (R7).** Test-only per-connection slice
   counter; parked counters must not advance without an event. Permanent
   rule-1 assertion.
7. **Shutdown semantics for parked connections** (scout Q2 decision):
   `NotifyShutdown` broadcasts to ALL live connections, not just subscribed
   ones — the control atom already wakes parked processes (C1); woken
   connections write `Disconnect`, drain best-effort, finalize.
   ForceClose-after-deadline backstop unchanged.

### 1.3 Shape dependency

D1 consumes the readiness service per the joint contract: shape (b) primary
(beamr-owned service; liminal holds tokens and obeys R4/R5), shape (a)
fallback (liminal owns the reactor thread; same slice shape, same
requirements; reactor thread inventoried through liminal's lens answers).
Nothing in §1.2 changes between shapes.

## 2. D2 — Capability-scoped worker front door

A new construction path — `WorkerFrontDoorServices` (name TBD) implementing
`ConnectionServices` — for aion-style deployments that need registration,
correlated push/reply, and notifier-consumed reserved publishes ONLY:

- Constructs: the connection supervisor. **Nothing else.** No haematite
  database (no 47-thread scheduler, no temp dir), no channel supervisor (no
  30-thread scheduler), no conversation supervisor (ditto), no dedup cache.
- Ordinary publish/subscribe/conversation frames are **explicitly rejected**
  with a typed error frame (not silently accepted, not routed to absent
  services). The reserved observability tap and worker registration work
  exactly as today.
- Full-service mode is unchanged; scheduler-count reduction *within* a
  retained scheduler (dirty pools, DistSender) is the beamr composition
  lane's territory — this deliverable removes whole unused schedulers and
  cites Artemis's lane for what remains (scout Q1 decision: both).
- Selection is explicit config (`[services] profile = "worker-front-door" |
  "full"`), validated; not inferred.

## 3. D3 — Ephemeral store lifecycle

Ownership-graph finding (from the July 7 exchange with Apollo Biscuit,
confirmed by the scout): the store is `Arc`-shared into channel handles, so a
guard owned by `LiminalConnectionServices` cannot guarantee the
database-drops-first rule on every path. Design: **the cleanup owner lives
inside the shared store wrapper itself** — the struct holding the
`EventStore`/`Database` gains `_ephemeral_dir: Option<TempDir>` declared
AFTER the store field. Rust's declaration-order drop means: last `Arc` clone
drops → database closes (shard actors join, `writer.lock` releases on fd
close per haematite's contract) → guard removes the directory. This covers
normal drop, abrupt-but-orderly teardown, and — because the guard is created
before `Database::open` — partial startup failure. Repeated start/stop cycles
each own distinct directories. Persistent-path stores are untouched.
`ReadOnlyDatabase` observers are never handed an ephemeral dir (per Apollo's
deletion-safety note). If certification wants deterministic removal at
shutdown rather than at last-drop, Apollo's offered `close_and_remove()`
lands and finalization calls it; the guard remains the backstop.

## 4. D4 — Conversation & finalization bookkeeping repair

Scout-found leak class (independent of readiness, same resource axis):

- Conversation close terminates the participant and calls
  `ParticipantRuntime::deregister` (today it only stops the actor —
  participants stay parked and registered forever).
- Connection teardown closes all open conversations via the §1.2(5)
  finalization path (today abrupt drop leaks both actor and participant).
- Actor/participant runtime registries gain dead-key removal, called from
  finalization and actor exit paths; a churn test pins bounded registry size.
- Push-slot cancellation on reply timeout (today the slot leaks until
  connection close).

## 5. Operational caps (scout Q4 — rule-2 items)

Config gains explicit bounds, each with a pinning test and a
certifying-pair-signed number: `max_connections` (listener refuses beyond),
`max_subscriptions_per_connection`, `max_conversations_per_connection`,
`max_pending_pushes_per_connection`. Durable disk quota is deferred to the
haematite GC lane (Apollo's workstream) with a pointer, not duplicated here.
Defaults proposed at review time; unlimited-by-silence is no longer a legal
state.

## 6. D5 — Listener/health polling (evidence-gated)

The listener accept loop wakes ~100/s and runs an O(live connections) reap
scan per iteration; health mirrors the wake rate. After D1 lands, if the
T1 delta methodology shows these remain material on the incident host:
register the listener fd with the same readiness service (accept becomes
readiness-driven), move reaping to event-driven (readiness service dead-pid
path) or amortized cadence, and give the health endpoint a blocking accept
with a shutdown wake. Gated on measurement, not assumption — the incident
fix must not balloon; but the bound is recorded now: listener idle cost has
a pinned ceiling in §7 either way.

## 7. Idle/resource-cost lens answers (v1.1)

- **Parked connection (D1):** Q1 — zero CPU, zero slices (R7 counter
  static); memory = buffers + registration entry, bounded by §5 caps.
  Q2 — aggregate bounded by `max_connections`. Q3 — T1 delta + T2 matrix +
  R7 counters; they fail on any regression to permanent-runnable. Q4 — the
  4 MiB outbound bound (existing, signed via this doc's certification); no
  other by-design idle cost remains.
- **Readiness consumption:** shape (b): zero liminal threads (the service's
  thread is beamr-inventoried); shape (a): exactly one reactor thread,
  inventoried, with its own soak assertion. Q3 — T4/T5 churn gates.
- **Worker front door (D2):** Q1 — connection supervisor only: 4 normal
  workers + beamr per-scheduler residents (floor owned by composition
  lane). Q2 — one scheduler per server instance, no hidden N. Q3 — the
  front-door gate: constructing it creates no channel/conversation/haematite
  scheduler, no store, no temp dir (thread + fs assertions). Q4 — none.
- **Ephemeral store (D3):** Q1 — while live: haematite's documented cost;
  disk bounded by what's written. Q2 — one dir per store instance, removed
  at last drop; leak test asserts zero residue across repeated start/stop.
  Q3 — the D3 lifecycle tests. Q4 — none (removal is unconditional).
- **Conversation machinery (D4):** Q1 — parked actors/participants are
  memory-only (verified: they park correctly today; the defect is
  lifecycle, not CPU). Q2 — bounded by §5 caps + registry-removal churn
  test. Q3 — the D4 churn test fails on any registry growth without bound.
  Q4 — none.
- **Listener (D6/D5):** Q1 today — ~100 wakes/s + O(connections) scan,
  recorded as the pinned ceiling pending D5 measurement. Q2 — exactly one.
  Q3 — T1's baseline measurement captures it. Q4 — if D5 is deferred, the
  ceiling + this doc's sign-off IS the rule-2 record.

## 8. How the original shipped (rule 3)

Three self-aware steps, none ignorant. `ff8d863` replaced a 10 ms sleep with
permanent requeue — a correct fix for "a sleeping connection blocks one of
four workers" that traded a bounded local defect for an unbounded global
one, and said so in a comment. `bb81724` (H1) built the delivery pump ON the
busy loop and cited it as a feature ("no wakeup plumbing needed — the
connection already runs every slice"). The ledger recorded "busy-polls by
design." I wrote the latter two. Missing controls, nearest first: no
idle-cost negative assertion existed (rule 1 — R7/T1 are that gate now); the
review battery that caught six real bugs the same week had no resource-cost
lens (rule 5 — the lens in `liminal-assets-pack.md` is that gate now);
"by design" was accepted as authorization with no bound, test, or sign-off
(rule 2 — the certifying pair is that gate now). Documentation of a defect
is not authorization for it; we documented this one twice and called it
design both times.

## 9. Acceptance gates

Contract §6 T1–T7 in full (T1 as the certified delta assertion with recorded
methodology), plus: D2 front-door construction gate (thread/fs assertions +
explicit rejection of unsupported frames); D3 lifecycle gate (removal on
normal drop, startup rollback, and a repeated start/stop cycle — zero
residue); D4 churn gate (no parked participant after close; registries
bounded; push slots cancelled on timeout); §5 cap-refusal tests; C2-scope
assertion (connections never park gated). All are permanent rule-1
assertions. Nothing re-enables under launchd on the incident host until T1
passes at the certified delta.

## 10. Sequencing

1. This doc + the joint contract through the review chain (norn passes, then
   Vesper Lynd, then independent certification).
2. beamr lands §2.5 contract pinning suite; shape decision certified.
3. D2/D3/D4 can begin on focused branches immediately after doc approval
   (shape-independent); D1 after §2.5 is green on beamr main.
4. D1 → T1 soak → certification → launchd re-enable decision (Tom briefed).
5. D5 measured, then built or formally deferred with its ceiling signed.
