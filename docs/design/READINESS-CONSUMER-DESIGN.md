# Liminal readiness-consumer workstream — design

*2026-07-11, Hermes Crumpet. The liminal half of the aion host resource
incident fix. Normative dependencies: beamr `docs/READINESS-CONTRACT-SPEC.md`
(branch readiness-contract-draft @ 4916161 — clauses C1–C4, consumer
requirements R1–R8, churn gates T1–T7) and
`docs/stack-review/AION-HOST-RESOURCE-INCIDENT-2026-07-11.md`. Evidence base:
Sol scout session 02468176 (envelope retained), verified against liminal main
@ 218a378 / 0.2.3. Sol architecture review session
44a6c465-8fb9-4f14-844d-06b09a6ae19b returned `not_ready` with five major
findings against the first revision; all five verified against source and
folded here (reply seam + pending-reply state machine, shape-(a) ownership
carve-out, D3 exclusive-ownership constructor, lens-answer repairs, stale
contract pin). Status: **COMPLETE APPROVAL by the certifying pair,
2026-07-11** — Vesper Lynd (three advisories + §5 defaults request) and
Waffles the Terrible (seventh wake source: reply-deadline expiry; §1.2(7)
shutdown-completion clarification), all consolidated fold items folded,
followed by the pair's item-7/denomination rulings (connection-scoped
4 MiB inbox byte budget, serialized-bytes-as-admitted) and the
tombstone-lifecycle ruling (TTL deleted after the author's
residual-window finding — both halves independently chose
scope-over-time; "rule 2 signs costs, not residual wrongness"). Per §10
sequencing: D2/D3/D4 unblocked for focused branches; D1 waits on beamr
§2.5 pinning suite green on main. No code before that gate. No code exists yet; the liminal
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
   The slot captures the **connection scheduler's** enqueue handle at
   install time — the channel actor fires from its own scheduler's slice,
   and an enqueue routed to the firing caller's ambient scheduler is a
   silently lost wake (Vesper advisory 3). Both legs fire it: local
   `SubscriberRegistration::deliver` and remote
   `SubscriberProcess::accept_remote_frame`. Install-before-final-recheck
   ordering closes the publish-vs-park race consumer-side (C1 closes it
   VM-side). The `delivery.rs:1-9` every-slice assumption is deleted.
3. **Pending-reply continuation (R1(vi)).** The in-slice 5 s reply drain for
   reply-requested `ConversationMessage` frames is removed. Two library-side
   pieces are part of D1, not implementation detail:

   **(a) Reply-availability seam.** The conversation core today exposes only
   a blocking `receive_timeout`; there is no notifier API, so R1(vi) has no
   seam to fire through. D1 adds a reply-availability notifier registration
   on the conversation core (mirror of the subscription-inbox notifier):
   installed **permanently at conversation open** — normative, not a
   choice; per-message install under pipelining would need refcounted
   notifiers with per-operation removal, and the pending-reply table
   already does the per-operation correlation, so the notifier's lifecycle
   is simply the conversation's (Vesper advisory 1). It fires on the reply
   queue's empty→non-empty transition and on terminal actor error, and is
   included in the slice's final post-arm probe (C4) so a reply that lands
   between drain and park is never lost. Because the conversation core runs
   on another scheduler's slice, the notifier slot captures the
   **connection scheduler's** enqueue handle at install, never the firing
   caller's ambient one — a misrouted enqueue is a silently lost wake
   (Vesper advisory 3; the VM-side twin is the composition spec's
   Shared-delivery gate). The notifier is removed at conversation close;
   markers arriving after close or carrying a stale connection generation
   are discarded (R5). A deterministic race test injects a reply between
   notifier install and the final probe, and between the probe and `Wait`,
   including a cross-scheduler-publish case — not only an eventual
   end-to-end test.

   **(b) Pending-reply state machine.** Removing the blocking drain removes
   the serialization it accidentally provided, so pending replies need real
   bookkeeping: one bounded table per connection, keyed by a monotonic
   internal operation id, each entry carrying conversation id, stream id,
   correlation id, deadline, timer reference, and terminal state. Matching
   is FIFO within a conversation. On completion the entry's timer is
   cancelled; on timeout the entry becomes a tombstone (a late reply is
   discarded, not mis-correlated to a newer request) and **the timer's
   expiry itself delivers the connection's READY marker** — reply-deadline
   expiry is a wake source in its own right (R1(vi) extended, Waffles'
   finding: nothing else guarantees a wake, so without this clause a
   parked connection whose pending reply expires under zero other traffic
   never wakes, and the client waits indefinitely for a timeout error the
   connection believes it handled). The timeout error frame is then
   written in the woken slice. **Tombstone lifecycle (pair ruling,
   2026-07-11 — replaces the earlier TTL design):** only two events
   genuinely disambiguate a tombstone — its late reply arriving (consume:
   reply discarded, tombstone freed) or its conversation ending (close
   sweep). Those are the ONLY reclamation triggers; there is no time-based
   expiry. The TTL was retired after the author's residual-window finding:
   any time-based reclamation, however guarded, eventually races an actor
   that is slow rather than dead — and slow-not-dead is precisely what
   produced the timeout — letting a very-late reply FIFO-match a younger
   entry admitted after reclamation. The ruling's principle, recorded for
   reuse (Waffles): **rule 2 signs costs, not residual wrongness** — a
   nonzero probability of delivering the wrong reply as the right one is
   semantic corruption, not a cost, and no signature attaches to it at any
   probability. Bounding is by scope instead of time: tombstones count
   against a per-conversation sub-cap ONLY
   (`max_pending_replies_per_conversation`, §5); pending entries count
   against both the sub-cap and the connection table. A conversation
   accumulating tombstones therefore wedges ITSELF — new reply-requested
   admissions on it draw the existing typed cap refusal until the
   conversation closes. The self-wedge is the honest semantic, not a
   degraded mode: a conversation holding a tombstone whose reply may still
   arrive IS ambiguous, and the refusal confines the ambiguity to the
   party that created it — sibling conversations and the connection's
   table are untouched. Tombstone memory is exact, with no latency
   assumptions: sub-cap × `max_conversations_per_connection` fixed-size
   entries, inside the already-signed tables budget. The connection finalizer (§1.2(5))
   cancels every entry, notifier, and timer **before** conversation actors
   are closed. The table is capped by
   `max_pending_conversation_replies_per_connection` (§5 — distinct from
   server-push slots). Tests: multiple pipelined reply-requested frames on
   one and on several conversations; timeout followed by a late reply and a
   new request on the same conversation; capacity recovery via late-reply
   consume and via conversation close; a wedged conversation (sub-cap full
   of tombstones) refusing new reply-requested admissions with the typed
   error while sibling conversations proceed unaffected; **the slow-actor
   sequence end-to-end: a tombstone-only conversation, a new admission
   (refused at the sub-cap, or admitted under it), then the very late
   reply — asserted discarded or consumed, never matched to the younger
   entry**; **a reply that times out while the connection is parked with
   zero other traffic — the timeout error frame still reaches the client
   promptly** (the seventh-wake-source gate); crash/close with entries
   pending; reply arriving after finalization. Removes the last bounded-blocking wait
   from the slice path and the four-concurrent-replies scheduler wedge.
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
   the pid; cancel all pending push slots; cancel every pending-reply
   entry, reply notifier, and timer (§1.2(3b)); fire worker-unregistration
   once; release every subscription (removing inbox notifiers before drop);
   close/terminate every conversation actor AND participant (D4); remove
   runtime registry entries. A final sweep at shutdown catches force-close
   misses (closes the scout's "records merely logged" gap). This
   consumer-side deregistration and the readiness service's own
   dead-scheduler sweep (composition spec advisory 2) are **deliberate
   redundancy, not duplication**: neither side depends on the other's
   diligence — the consumer deregisters per connection on every termination
   path, and the service sweeps whatever a wedged consumer leaves behind.
   The contract records the same statement at R4/T5.
6. **Quiescence instrumentation (R7).** Test-only per-connection slice
   counter; parked counters must not advance without an event. Permanent
   rule-1 assertion.
7. **Shutdown semantics for parked connections** (scout Q2 decision):
   `NotifyShutdown` broadcasts to ALL live connections, not just subscribed
   ones — the control atom already wakes parked processes (C1); woken
   connections write `Disconnect`, drain best-effort, finalize.
   ForceClose-after-deadline backstop unchanged. To be explicit against
   contract T5 (which forbids shutdown depending on parked processes
   running a final slice): the broadcast wake is a **courtesy**, the
   ForceClose deadline is the **guarantee** — shutdown COMPLETION never
   depends on any woken slice actually running; the supervisor/reaper-owned
   teardown of T5 completes regardless. The two documents describe the same
   design, not a contradiction (Waffles' reading note).

### 1.3 Shape dependency

D1 consumes the readiness service per the joint contract: shape (b) primary
(beamr-owned service; liminal holds tokens and obeys R4/R5), shape (a)
fallback (liminal owns the reactor thread; same slice shape, same
requirements; reactor thread inventoried through liminal's lens answers).

The slice shape (§1.2(1)–(4), (6), (7)) is shape-invariant. The R4/R5
registration mechanics are **not**: under shape (a), if the connection
process owns its socket, process death (`reap_crashed`, external kill) can
drop the last fd handle before the reactor deregisters — and a reused fd
number could then deliver another connection's events against a stale
registration. Shape (a) therefore keeps fd/stream lifetime in the
`(pid, generation)` registration record itself (reactor/supervisor-owned
close guard): the reaper deregisters and receives the acknowledgement
before the record — and with it the fd — is released. If that guard proves
impractical, a beamr process-exit hook becomes an explicit shape-(a)
prerequisite, not an assumed given. An externally-killed-pid test proves
deregister-ACK precedes fd close and reuse. Under shape (b) this ordering
is the service's obligation (contract §3.3); the test still runs as the
consumer-side pin (T5).

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
database-drops-first rule on every path. And a guard placed merely *beside*
an `Arc<EventStore>` field is not enough either: today's construction is
nested — `Arc<HaematiteStore>` wrapping a **caller-supplied**
`Arc<EventStore>` (`HaematiteStore::new` is public,
`durability/store.rs:71`) — so the inner store can outlive any wrapper that
holds a clone of it, and field declaration order proves nothing across that
boundary. Design: **the guard lives with exclusive ownership of the
database.** A private ephemeral constructor takes the
`Database`/`EventStore` **by value**, creates the inner `Arc` internally,
and returns a non-`Clone` guarded wrapper that never exposes the inner
`Arc` (no getter, no caller-supplied clones possible). Drop order inside
that single owner — store field first, `_ephemeral_dir: Option<TempDir>`
declared after it — then genuinely means: last handle drops → database
closes (shard actors join, `writer.lock` releases on fd close per
haematite's contract) → guard removes the directory. The existing public
`HaematiteStore::new` remains for persistent/unguarded stores only. This
covers normal drop, abrupt-but-orderly teardown, and — because the guard is
created before `Database::open` — partial startup failure *on the liminal
side*. Haematite's failed-startup semantics are now cited from source
(Apollo Biscuit, 2026-07-11, contract doc-comment
`haematite/src/db/startup.rs:67-98`, v0.4.1): a failed `create`/`open`
leaves the writer lock **free** (released on the error return) and the
directory either intact-as-found or — only when that `create` call itself
created it — wholly removed under the still-held lock; a pre-existing
directory is never removed. Under this design the guard's directory
pre-exists `create`, so haematite never competes with the guard for
removal: the `TempDir` is the sole owner of directory lifetime on every
failure path. The never-delete-pre-existing half is pinned in haematite
today; the removes-what-it-created half lands as a permanent rule-1
injected-failure assertion in Apollo's doc-4 (recovery/boot hardening)
plan. Certification of this design does not wait on that pin (Vesper's
ordering ruling: a source-verified doc-comment contract with one half
already pinned is enough for a design to stand on), but the pin must be
green before any consumer code relying on the removes-what-it-created half
merges — the same contract-test-before-consumer-merge sequencing as
readiness §2.5. Liminal's own §9 gate additionally injects open failure at
this layer and asserts zero residue via the guard, independent of
haematite's internal cleanup.
Lifecycle tests run with channel/store handle clones alive at teardown,
with startup failure injected, and across the final last-handle drop.
Repeated start/stop cycles each own distinct directories. Persistent-path
stores are untouched.
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
`max_pending_pushes_per_connection`,
`max_pending_conversation_replies_per_connection` (§1.2(3b) table), and
`max_subscription_inbox_depth` — the shared inbox is an unbounded
`VecDeque` today (`channel/subscription.rs:33`); it gains a per-inbox depth
cap, and on overflow the subscription is released with an explicit error
frame to the subscriber, mirroring the outbound 4 MiB overflow policy (a
slow consumer sheds its own subscription; it cannot grow server memory
without bound). Durable disk quota is deferred to the haematite GC lane
(Apollo's workstream) with a pointer, not duplicated here.

Proposed defaults (certifying pair signs or adjusts; each refusal is a
typed error, each cap a config key):

- `max_connections` = **256** — liminal is a worker-bus, not an internet
  broker: an order of magnitude above any observed fleet (the incident
  host ran 11), while keeping every worst-case product below meaningful.
- `max_subscriptions_per_connection` = **32** — observed workers hold a
  handful of taps (observability, liveness, capability); 32 is generous
  headroom without letting one connection fan out unboundedly.
- `max_conversations_per_connection` = **32** — same shape as
  subscriptions: correlated request/reply surfaces per worker are few.
- `max_pending_pushes_per_connection` = **32** — in-flight correlated
  pushes are latency-bound and timeout-guarded; 32 outstanding per worker
  exceeds any observed dispatch depth.
- `max_pending_conversation_replies_per_connection` = **32** — symmetric
  with pushes; entries are small and timers cancel on completion, so the
  cap exists for admission control, not memory.
- `max_pending_replies_per_conversation` = **8** — the sub-cap that
  confines tombstone ambiguity to its own conversation (pair ruling):
  pending entries count against both this and the connection table;
  tombstones against this alone, so tombstone memory is exactly ≤ 8 × 32
  fixed-size entries per connection and an ambiguous conversation can
  never starve its siblings.
- `max_connection_inbox_bytes` = **4 MiB** — one shared inbox-byte budget
  per connection, spent across ALL that connection's subscription inboxes,
  deliberately mirroring the outbound 4 MiB bound (a connection gets 4 MiB
  of buffering in each direction, full stop). **Denomination ruled by the
  pair (both halves, 2026-07-11):** count caps bound a variable the design
  doesn't control (envelope size), and per-inbox byte caps re-create the
  same disease one level down — connection-scoped bytes is the only
  denomination whose signed product is exact. **Accounting unit, named so
  the signed number cannot drift (Waffles' refinement, adopted):**
  serialized envelope bytes as admitted — charged at enqueue, released at
  dequeue; not capacity, not payload-only, not an estimate. Overflow sheds
  the offending subscription with a typed error frame, as above.
- Per-inbox **256-envelope count** retained as a secondary fairness trip
  only — it stops one subscription starving its siblings inside the shared
  budget; it is no longer load-bearing for the signed bound.

Worst-case aggregate (lens Q2): 256 connections × (4 MiB in + 4 MiB out +
the small tables) ≈ **2 GiB, exact and envelope-size-independent** — no
nominal envelope size doing load-bearing work. The certifying pair signs
the defaults against this product, not against the per-cap numbers in
isolation. Unlimited-by-silence is no longer a legal state.

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

- **Parked connection (D1):** Q1 — zero *marginal* CPU and zero slices (R7
  counter static): a parked connection adds nothing above the VM's idle
  floor, but that floor itself is a real, signed cost owned by the
  composition lane (the 5 ms tick bound) — "costs nothing" without that
  qualifier would let a signed bound masquerade as zero, which is the
  distinction T1's delta methodology exists to keep honest; memory =
  buffers + registration entry + pending-reply table +
  subscription inboxes, every component bounded by a named §5 cap (the
  connection-scoped inbox byte budget and reply table included — no
  unbounded queue survives this doc);
  pending-reply timers are bounded by the same table cap and cancelled on
  completion/finalization. Q2 — aggregate bounded by `max_connections` ×
  the per-connection caps. Q3 — T1 delta + T2 matrix + R7 counters; they
  fail on any regression to permanent-runnable. Q4 — the 4 MiB outbound
  bound (existing, signed via this doc's certification); no other by-design
  idle cost remains.
- **Readiness consumption:** shape (b): zero liminal threads (the service's
  thread is beamr-inventoried); shape (a): exactly one reactor thread,
  inventoried, with its own soak assertion. Q3 — T4/T5 churn gates.
- **Worker front door (D2):** Q1 — connection supervisor only: 4 normal
  workers + beamr per-scheduler residents (floor owned by composition
  lane). Q2 — one scheduler per server instance, no hidden N. Q3 — the
  front-door gate: constructing it creates no channel/conversation/haematite
  scheduler, no store, no temp dir (thread + fs assertions). Q4 — the
  four-thread connection scheduler itself is the accepted by-design
  resident cost: bound = fixed worker count pinned by the thread assertion,
  sign-off via this doc's certification; the per-scheduler idle floor is
  the composition lane's Q4, cited not duplicated.
- **Ephemeral store (D3):** Q1 — while live: haematite's documented cost;
  disk growth has **no liminal-side quota** — that is a by-design gap,
  recorded here as a Q4 item, not waved through: either an ephemeral-store
  byte quota lands with D3, or the unbounded (write-amplified) disk cost is
  explicitly signed by the certifying pair with the haematite GC lane
  pointer as the remediation owner. Q2 — one dir per store instance,
  removed at last drop; leak test asserts zero residue across repeated
  start/stop. Q3 — the D3 lifecycle tests. Q4 — removal is unconditional;
  the disk-quota record above is the open Q4 item.
- **Conversation machinery (D4):** Q1 — parked actors/participants are
  memory-only (verified: they park correctly today; the defect is
  lifecycle, not CPU). The implementation adds one **exit watcher** per
  live conversation (supervision-integrity observer, added at review to
  close the bare-actor-exit leak): also a parked, memory-only native
  process — each live conversation carries three parked processes (actor,
  participant, watcher), all torn down by the same finalization. The
  watcher parks only via the arm→drain→final-liveness-probe slice (the
  contract C1/C4 discipline applied to process observation — the
  first shipped instance of the pattern D1 will use for connections).
  Q2 — bounded by §5 caps + registry-removal churn test; the watcher adds
  a constant factor (3 not 2 processes per conversation), no new axis.
  Q3 — the D4 churn test fails on any registry growth without bound.
  Q4 — none.
- **Listener (D5):** Q1 today — ~100 wakes/s + O(connections) scan,
  recorded as the pinned ceiling pending D5 measurement. Q2 — exactly one.
  Q3 — a dedicated test-only listener wake/scan counter with a hard numeric
  ceiling (or an OS-visible soak of the listener alone), its methodology
  recorded to the same standard as T1 (environment, duration, measurement
  source — reproducible, not anecdotal). T1 cannot serve
  here: it is a *delta* over a baseline that contains the listener on both
  sides, so it is structurally blind to listener cost — the listener needs
  its own assertion that fails independently of T1. Q4 — if D5 is deferred,
  the ceiling + certifying-pair sign-off IS the rule-2 record.

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
methodology), plus: D1 reply gates (deterministic reply-vs-park race
injection per §1.2(3a); multi-flight/timeout/late-reply/reply-after-
finalization state-machine tests per §1.2(3b)); the externally-killed-pid
dereg-ACK-before-close test (§1.3 — consumer-side pin under both shapes);
D2 front-door construction gate (thread/fs assertions + explicit rejection
of unsupported frames); D3 lifecycle gate (removal on normal drop, startup
rollback including injected haematite-open failure, repeated start/stop,
and teardown with store-handle clones still alive — zero residue); D4 churn
gate (no parked participant after close; registries bounded; push slots
cancelled on timeout); §5 cap-refusal tests including inbox-overflow shed;
listener wake-counter ceiling (§7, independent of T1); C2-scope assertion
(connections never park gated). All are permanent rule-1
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
