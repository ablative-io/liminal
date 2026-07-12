# Liminal ops-console + MCP surface — the liminal half of the seam

**Status:** DRAFT — the one-pager this doc drafts against is now **TORN AND
SETTLED** (Apollo Biscuit's tear folded at `design/mcp-seam-one-pager` @
`7b768eb`, amendments T1–T6); this revision is the post-tear sync. Routes to
the certifying pair after the domain owner's read. Every section that rests
on a one-pager conclusion carries a **[SEAM-DEP]** tag; the tags now mark
sections *synced to the settled one-pager @ 7b768eb* — if the contract ever
moves again, grep the tag and only those sections change.

**Base:** liminal branch `design/mcp-seam-one-pager` @ `7b768eb` (this doc
cites that branch's settled `docs/design/MCP-SEAM-ONE-PAGER.md`; it is NOT
written off main).
**Ground truth:** the liminal console scout (norn session b4fad82a, envelope
`~/.norn/delegations/claude-scout-console.GMHHYy`, observed at liminal main
`3c3aa10`). The scout's file:line anchors are cited throughout. The tree has
moved since `3c3aa10` (this branch descends from main `a028711`; the
push-reply deadline fix has since **merged to main @ `68379e8`** — see §4.5);
every anchor a design decision rests
on was re-derived on this branch and the re-derived line is the one cited.
Where a cited number is load-bearing it was opened and read, not trusted from
the scout.

**Companion inputs:** the joint MCP-seam one-pager (`MCP-SEAM-ONE-PAGER.md`,
this branch) — the seam contract this doc implements the liminal half of;
`PARK-FLIP-D1-ACTIVATION.md` (this branch) — the incident-closure design this
console must never regress, especially its §4 slice contract and §5
resident-cost discipline; Cally Ray's F-0c §R1 client-protocol contract
(frame `6a750c8`, `docs/briefs/F-0c-liminal-conversation-spike.md`) — the
consumer surface the conversations view meets only at its field list.

---

## 0. Product framing — the first window into liminal-as-product

Tom's directive shapes this doc from birth, so it is stated before the
engineering: liminal is being uplifted from a message broker into a product,
"as capable as aion, NATS-class." The north star is an agent that registers
its **mailbox** with liminal, its **workflows** with aion, its **dispatch**
with norn — MCP on all three. This console/MCP surface is the **first window
into liminal-as-product**: the first place an operator or an agent sees
liminal as a thing that describes itself, speaks a typed contract, and
refuses honestly.

Two consequences bind everything below:

1. **The MCP seam is the durable artifact; the console UI is interim
   chrome.** Consoles are eventually rewritten in Frame (one-pager §out of
   scope). So this doc is weighted **contract-heavy** (tool/resource schemas,
   typed refusals, versioning posture) and **chrome-light** (no UI design
   beyond the interim console's needs). When a sentence here could describe
   either the wire contract or a rendered panel, it describes the contract.

2. **Product honesty is a wire property, not a slogan.** liminal-as-product
   speaks MCP itself, not through a chaperone (one-pager §(i)). An
   unreachable surface means *this process* is unreachable, and a refused
   call names its class and remedy. The incident that this design descends
   from was invisible for five days (§10); the product's first window must be
   the surface that would have made it visible on day one.

The one-pager settled that the seam is **per-service, mounted per-host-
process** (§(i)): liminal-server defines liminal's contract and mounts it (and,
if the embedder opts in, haematite's, namespaced). This doc designs liminal's
contract and its mount. It does not design the Frame console, the stack
gateway, or any other service's seam (§14).

---

## 1. What this doc delivers, and the seam it fills — [SEAM-DEP: one-pager §(i)]

The one-pager is a contract skeleton across two repos; this is the liminal
implementation half. Concretely, this doc commits to:

- **OpsState** (§2–§3): the read-optimized host-side state object that makes
  every liminal view answerable without waking a beamr process or taking a
  hot-path lock. This is the central design gap the scout named ("the
  principal design gap is a read-optimized OpsState", scout summary) and the
  centerpiece here.
- **The six v1 views** (§4), each field carried to full per-field discipline:
  owner / update-event / read-primitive / lock-behavior / staleness /
  cardinality / redaction / idle-cost.
- **The DO-NOT-EXPOSE list** (§5): named negative design decisions, each a
  verified wake- or contention-hazard, each with its safe alternative.
- **The R7 slice-gauge promotion** (§6): the per-connection AtomicU64 that
  turns the incident's invisible cost into a first-class instrument.
- **Auth** (§7 — [SEAM-DEP §(ii)]), **the idle clause and acceptance gates**
  (§8 — [SEAM-DEP §(iv)]), **the four-question lens** (§9), **the
  observability-gap retrospective** (§10).
- **The MCP surface concretely** (§11 — [SEAM-DEP §(iii)]): named read-only
  resources and tools, JSON shapes at sketch level, the typed-refusal
  taxonomy, and versioning posture.

**Transport binding, restated from the settled one-pager §(i):** the
liminal MCP surface rides the **EXISTING health listener** process-side (same
bind-address exposure model, no new listener thread in v1), OR a sibling
listener in the same process if the certifying pair rules that the health
listener's serial handler and 2 s read timeout disqualify sharing
(`health/endpoint.rs:122,146` — one worker, `thread::sleep(10ms)` on
WouldBlock, `set_read_timeout(2s)`). **v1 chooses reuse-no-new-thread** —
**domain-owner determination (b), recorded:** v1 stays on the shared health
worker with the bounded-response + scrape-work budget (§8); the
sibling-listener fallback fires only if the pair rules the head-of-line risk
disqualifying, and is a rule-2 item from birth (§8) because it adds a
resident thread.

**T1 (tear amendment, folded): the contract is transport-identical over
stdio and HTTP.** The tear confirmed a real stdio host in the seam (the
`haem` stats CLI class: zero listener, zero idle cost, auth = OS
process-spawn rights). **liminal-server's v1 surface is HTTP** — it is a
process with an existing listener, and nothing here proposes a liminal stdio
host — but the CONTRACT (§11) must not assume HTTP: same resources, same
refusals, same versioning over either transport. Concretely, no §11 resource
or refusal may encode an HTTP-ism (status codes, headers, URLs-as-semantics);
HTTP is the mount, not the meaning. This whole section is [SEAM-DEP §(i)],
synced @ 7b768eb.

---

## 2. OpsState — the centerpiece

### 2.1 The problem OpsState exists to solve

Every attractive-looking read path into liminal's live state is either a
scheduler wake or a hot-path lock. The scout's map is unambiguous:
`ChannelRegistry::list` enqueues to every channel actor and blocks
(`channel/registry.rs:89-100` → `subscriber_count` → `enqueue_atom_message`);
`ConnectionRuntime.records` is a `Mutex<HashMap>` that **every connection
slice already locks** via `is_registered` (`process.rs:130` →
`supervisor.rs` records lock); the R7 slice counter is a `cfg(test)`
`Mutex<HashMap>` (`supervisor.rs:671`); subscription depth, pending replies,
tombstones, outbound bytes, and deadline state live *only* inside
`ConnectionProcessState`, serviced on the slice (`state.rs`, `process.rs`);
`ConversationActor::state()` routes through `handle.query_state` — an actor
command (`conversation/actor.rs`). None of these can back a console read
without regressing the incident (§5 enumerates each as a named refusal).

The only surfaces that are already safe are the ones built for the health
listener: `SharedReadinessState` (four `AtomicBool`, `SeqCst` snapshot,
lock-free — `health/checks.rs`), the process-global metrics registry (three
families, `Relaxed` collectors, read-lock only on structural registration —
`metrics.rs:15-17`, `metrics/registry.rs`), and beamr's `service_inventory()`
(host-side metadata copy, no actor query — `supervisor_tests.rs:21-34`).

OpsState generalizes exactly that shape to every field the six views need. It
is the design's answer to the scout's central finding.

### 2.2 Composition — the concrete type, not three adjectives

`OpsState` is an `Arc<OpsState>` whose members are **exactly** these, and
**nothing that reads through a scheduler or a hot lock**:

1. **Immutable redacted config** (`OpsConfig`, plain owned struct). A cloned,
   redacted snapshot of the validated `ServerConfig`, frozen at construction:
   profile, all eight limits (`config/types.rs:203-236`), configured channel
   names, persistence mode/flag, `auth_enabled` (boolean only), listen and
   health addresses, `cluster_configured`. Redaction is applied at clone time
   (§7.3): no auth token bytes, no cluster cookie, no raw fds, no filesystem
   paths beyond what config already names publicly. Immutable ⇒ read is a
   pointer deref, zero lock, exact, zero idle cost.

2. **`phase: AtomicU8` — the explicit lifecycle atomic.** Not derived from
   readiness flags (which are independent booleans with no total order,
   `health/checks.rs:139-200`): OpsState owns its own phase with exact
   transitions, all written by `run()`:
   `Starting` (at construction) → `Running` (set immediately after
   `readiness.set_listener_bound(true)` at `runtime.rs:87` AND the §2.3
   inventory seal — both must hold) → `Draining` (set when
   `shutdown_handle.wait()` returns at `runtime.rs:102`, before cluster/
   connection teardown). No other transition exists; the writer is the runtime
   thread only. Every §11 response carries it (§3.1).

3. **Lifecycle-maintained atomics.** A flat block of `AtomicU64`/`AtomicUsize`
   counters and gauges written through the §2.5 write-side API — each write
   rides an existing lifecycle event. Reads are `Relaxed`/`Acquire` loads —
   lock-free, bounded-stale by one event, one cache line each.

4. **Arc-swapped copy-on-write snapshots** (`ArcSwap<ConnectionsSnapshot>`),
   updated ONLY through the §2.6 single-writer protocol. The snapshot is a
   plain immutable structure keyed by pid; a reader does one atomic load of
   the current `Arc` and walks it. **Global rebuild events — connection
   lifecycle ONLY:** admission publication, auth, worker registration,
   readiness registration, and every close/reap route. Subscription
   transitions do NOT rebuild the global snapshot (Sol r2: subscribe/
   unsubscribe are processed ON the connection's slice via `service_socket`,
   `process.rs:120-141` → `apply.rs:349-447`, at request rate with no
   lifetime churn bound — a global CoW per transition would let one
   authenticated client force full-population copies at frame rate and
   monopolize the §2.6 writer authority). They ride the per-connection
   sub-list slot instead (§2.5(b)).

   The field tables rely on one refinement (**the shared-live-atomic
   pattern** — §4.1, §4.3, §6): a snapshot **entry** embeds `Arc`-shared live
   atomics from the connection's §2.5 observation bundle. The counters
   **survive snapshot swaps** (only the entry structure is rebuilt; the
   `Arc`s are carried), the hot path keeps its existing write handle, and a
   scrape reads them lock-free through the arcswap — per-connection values
   that change at slice or delivery rate are readable **without** rebuilding
   snapshots at that rate and **without** touching any map mutex.

5. **`inventory: OnceLock<InventoryHandles>` — sealed once, late** (§2.3).
   `InventoryHandles` holds **`Weak`** scheduler handles: connection (always;
   `Weak<Scheduler>` downgraded from `ConnectionSupervisor::scheduler()`,
   `supervisor.rs:152-156`), channel + conversation (full profile only,
   captured pre-coercion). **Weak, not strong, per handle — decided:**
   OpsState is owned by the `'static` health worker and must never extend a
   scheduler's life past `run()`'s teardown; a strong `Arc<Scheduler>` in the
   health worker would do exactly that (the original "captured by reference"
   wording was a contradiction — a reference cannot move into the `'static`
   worker, and a strong Arc extends ownership; this replaces it). **Upgrade-
   failure semantics:** a failed `Weak::upgrade()` during draining renders the
   inventory view's scheduler block as typed-absent with `phase: draining` —
   an expected lifecycle fact, not an error. The `ClusterHandle`/membership
   note follows the same rule: if cluster facts are ever exposed (out of v1,
   §13), they ride a `Weak` + published snapshot, never a strong retention.

6. **Reused readers, not duplicated state:** `SharedReadinessState` (clone)
   and the process-global metrics registry reader. These stay their own
   surfaces (§12); OpsState holds read handles only.

These back the §3 read-primitive table — the **only** read primitives the
console is permitted (one-pager §(iii): "all of it rides three read
primitives and nothing else" — config/atomics/arcswap are the OpsState class;
metrics and inventory are the reused pair). No further primitive is
admissible; §5 is the fence that keeps it so.

### 2.3 Where OpsState is assembled — the corrected ordering

The health listener is started **early and populated late**. Verified on this
branch (and corrected from this doc's earlier revision, which mis-stated the
erasure point):

```
runtime.rs:28   config = load_config(...)                       // config known
runtime.rs:33   metrics::init()                                 // registry live
runtime.rs:35   readiness = SharedReadinessState::new(...)      // shared handle
runtime.rs:36   start_health_server(addr, readiness.clone())    // SERVING begins
runtime.rs:56   match profile { Full => {
runtime.rs:58       services = Arc::new(LiminalConnectionServices::from_config)  // concrete
runtime.rs:61       with_services_and_auth(services, ..)         // takes Arc<dyn …> — ERASURE HERE
                       └─ SupervisorInner::new (supervisor.rs:461-499)
                          constructs the CONNECTION SCHEDULER *inside*, AFTER erasure
runtime.rs:86-87   readiness.set_config_loaded/​set_listener_bound(true)
```

**The factual correction, load-bearing:** `with_services_and_auth` accepts the
already-erased `Arc<dyn ConnectionServices>` (`supervisor.rs:111-119`), and
the connection scheduler does not exist until `SupervisorInner::new`
constructs it (`supervisor.rs:471-484`) — *after* the erasure. So "capture
everything before erasure" is impossible for the connection scheduler and the
assembly is a three-step seal, in this order:

1. **Construct `Arc<OpsState>`** (config shell + `phase=Starting` + zeroed
   atomics + empty snapshot + empty `OnceLock`) right after `load_config`;
   hand `ops.clone()` to `start_health_server` at line 36 (its signature grows
   one parameter) and the §2.5 writer handles to service construction.
2. **Capture channel + conversation scheduler handles PRE-coercion** from the
   concrete `LiminalConnectionServices` (`services.rs:397-418` — the only
   window; after coercion they are unreachable and bolting queries onto the
   trait object risks wake-unsafe implementations). Downgrade to `Weak`.
3. **Construct the supervisor, THEN capture its scheduler** via the existing
   `ConnectionSupervisor::scheduler()` accessor (`supervisor.rs:152-156`),
   downgrade to `Weak`, and **SEAL**: `inventory.set(InventoryHandles{…})` —
   exactly once, before `phase` flips to `Running`. A scrape that arrives
   before the seal sees `phase: starting` and an inventory view that is
   typed-absent ("not yet sealed"), never a partial handle set.

**The boot window is honest, not hidden:** between line 36 and the seal,
gauges read zero, the snapshot is empty, `phase` says `starting` — precisely
as `/ready` reads 503 until :86. A consumer can never mistake a boot zero for
steady state because `phase` travels in every response (§3.1).

**Named transition tests** (each fail-first): (t1) scrape before seal →
`starting` + typed-absent inventory, no partial handles; (t2) scrape
immediately after `Running` flips → sealed inventory renders; (t3) scrape
during `Draining` with schedulers torn down → `Weak` upgrade fails → typed-
absent-with-phase, no panic, no stall; (t4) `phase` transitions are monotonic
Starting→Running→Draining under concurrent scrapes (no observable regression
to an earlier phase).

The worker-front-door profile (`runtime.rs:76-82`) builds only the connection
supervisor; OpsState's channel/conversation/store/cluster slots are then
**absent-by-profile, not zero** (scout D2 finding). §4's tables carry a
profile column for exactly this distinction.

### 2.4 What OpsState is NOT

It is not a new listener thread (§1, §8). It is not a cache that a background
thread refreshes (that would be a resident wake — rule-2). It is not a
transactional snapshot: each atomic is independently current, so a scrape is
consistent *per field*, not across fields — the console labels a scrape
timestamp and never claims an all-fields instant (scout risk: "Metric snapshot
consistency is per-atomic, not transactional"). And it never extends a
domain object's lifetime: every retained scheduler handle is `Weak` (§2.2.5);
the only strong `Arc`s OpsState holds are its own config, atomics, snapshot
generations, and observation-bundle counters (plain atomics with no Drop
side-effects, harmless to outlive their connection until the next rebuild
drops the entry).

### 2.5 The write side — ownership defined, not implied

Reading is `OpsReadState`'s job (§3.2); **writing is a separate API** with
named owners for every gauge. Sol's review established that several gauges
have no viable writer path without this: `admissions` is a non-shareable
`AtomicU64` embedded in `ConnectionRuntime` (`supervisor.rs:687-692`), the
inbox budget is **lazy** — `ConnectionProcessState.inbox_budget` is `None`
until first subscribe (`state.rs:47-52`; created at `apply.rs:383-395`) — and
channel/conversation lifecycle events fire in the lower `liminal` crate,
which must not know about a server-owned OpsState. The write side is
therefore three shapes:

**(a) The per-connection OBSERVATION BUNDLE.** One
`Arc<ConnectionObservation>` created **at admission** (in
`try_reserve_admission`'s success path, before process construction),
containing the connection's live members: the §6 R7 slice counter,
`subscriptions_active`, `conversations_active`, `pending_pushes`,
`pending_replies`/`tombstones`/`armed_timers` (per-connection homes of the
§4.4 gauges), the §4.5 per-conversation-worst pair, an
**initially-empty budget slot** (`OnceLock<Arc<ConnectionInboxBudget>>`),
and the **per-connection subscription sub-list slot** (§2.5(b) — the home of
the per-subscription observation objects; the bundle holds the slot, the
inboxes own the objects). Clones go three ways: one into the connection
process state (the slice-path
writer — no map lookup on any hot path), one into the host-side connection
record (host-side writers: push slots, teardown sweeps), one embedded in the
snapshot entry (the reader). Every rollback/close route resolves the bundle
it already holds — admission rollback decrements what it incremented and
unpublishes the entry; the single removal/finalization path (park-flip §3)
is where close-side decrements and entry removal happen, covering EOF,
protocol close, error close, force close, reap, external kill, and shutdown
sweep. **One-write-per-transition tests** are named per gauge in §9.

**Constructor migration, explicit (Sol r2 finding 1 — the injection is only
mechanically viable if the production constructor carries it):** production
runtime calls `ConnectionSupervisor::with_services_and_auth(services,
auth_token)` (`runtime.rs:58-61`), which today passes
`LimitsConfig::default()` to `SupervisorInner::new`
(`supervisor.rs:111-119`); only `from_config_via` threads `config.limits`
(`:65-79`). Left as-is, the shared admissions Arc would render the
CONFIGURED cap from OpsConfig while the CAS enforces the DEFAULT 256 — the
caps resource and the CAS authority disagreeing on every non-default
deployment. **The migration:** the production constructor grows to
`with_services_auth_and_ops(services, auth_token, limits: LimitsConfig,
ops: OpsWriteHandles)` where `OpsWriteHandles` carries the injected
admissions `Arc<AtomicU64>`, the observation-bundle factory, and the §2.5(c)
injected gauges — threaded intact through `SupervisorInner::new` to
`ConnectionRuntime::new`. Test/non-config constructors explicitly take
defaults (default limits + a fresh throwaway `OpsWriteHandles`), stated in
their doc comments as test-only defaults, not silently. **Pin:** a
non-default `max_connections` test (e.g. 7) proving the rendered caps-view
limit, the CAS refusal threshold (8th admission refused), and the
`admissions_current` load are all the SAME injected state — one value, three
observers.

**(b) The lazy-budget + subscription-publication shape — REVISED (r2
composition of findings 2+4; supersedes the "subscribe joins the rebuild
set" decision, which was wrong twice: subscribe/unsubscribe run ON the
connection slice via `service_socket` — calling them "never on a slice" was
internally false — and their rate is bounded only by frame rate, not by the
occupancy cap, so a global CoW per transition was a writer-side DoS).**

- **Per-subscription observation object, created WITH the inbox:**
  `SubscriptionObservation { depth: Arc<AtomicUsize>, overflowed:
  Arc<AtomicBool> }`, constructed by `SubscriptionInbox` itself so the
  sticky overflow flag becomes the SHARED atomic (today it is an embedded
  non-shareable `AtomicBool`, `subscription.rs:179-187` — the field's owner
  changes from embedded to `Arc`, its `store(true, Release)` sites
  unchanged in meaning). Injected through the **existing `InboxInstall`
  seam** (`subscription.rs:127`, installed at subscribe, `apply.rs:401`) —
  the seam that already carries budget/cap/notifier, so no new construction
  path exists.
- **The aggregate `overflow_flags_total`:** an injected `Arc<AtomicU64>`
  riding the same `InboxInstall`; the inbox increments it on the **first
  false→true transition only** — `overflowed.swap(true, AcqRel)` returning
  `false` gates the increment — so repeated refused admits
  (`subscription.rs:283-311` stores on every refusal) count once per shed,
  not once per drop. Semantics now defined, not implied.
- **Publication — the per-connection sub-list, NOT the global snapshot:**
  the bundle holds `sub_list: ArcSwap<SubscriptionList>` (≤ 32 entries of
  `{ subscription_id, channel: Arc<str>, observation }`). Subscription
  transitions (subscribe, unsubscribe, **automatic shed** —
  `process.rs:377-398`, which removes a subscription with NO unsubscribe
  frame and must publish like one) rebuild ONLY this list: a ≤ 32-entry
  copy, swapped on the bundle's own slot. The global `ConnectionsSnapshot`
  is untouched; the §2.6 global writer mutex is NOT acquired. **Write
  authority:** the connection's own slice is the only subscription-
  transition writer (a connection's slices are serialized by beamr), and
  the close path clears the slot through the single removal path — a
  per-connection cold mutex in the bundle serializes those two, contended
  at most once per connection life.
- **Honest slice-path pricing:** subscribe/unsubscribe/shed IS slice-path
  work, and nothing structurally bounds a client's churn RATE (the
  occupancy cap bounds the map length, `apply.rs:349-377`, not the
  transition count — a client may subscribe/unsubscribe at frame rate
  indefinitely). The per-event cost is therefore stated as a bound, not
  assumed infrequent: one ≤ 32-entry list copy + one `ArcSwap` swap +
  (first subscribe only) one `OnceLock` fill, all connection-local. A
  churning client burns ITS OWN slice budget (the §4 slice contract already
  meters that) and contends with nobody: not the global writer, not other
  connections, not readers. **Named storm test:** a malicious subscriber
  churning subscribe/unsubscribe at frame rate while the server accepts and
  closes other connections — accept/close latency and other connections'
  publication latency stay bounded (flat), and the churner's own slice
  counter shows the cost landing where it belongs.
- **The budget slot:** first subscribe fills the bundle's `OnceLock` with
  the budget `Arc` it just created (`apply.rs:383-395` keeps its lazy
  `get_or_insert_with`, byte-identical wire semantics); the slot is
  lock-free readable from the entry — no rebuild of anything is needed to
  publish it. Before first subscribe the view renders `inbox_bytes_used:
  null` with `subscriptions_active: 0` — a typed never-subscribed fact.

**(c) Dependency-neutral writer handles for the lower crate.** Channel
subscriber gauges, conversation registration gauges, and the aggregate
overflow counter (b) move at events owned by `crates/liminal`
(registry/actor/inbox code), which cannot name a server type. They receive
**plain injected atomics** at construction: the server pre-registers
per-channel `Arc<AtomicU64>` subscriber gauges (keyed by the immutable
configured-channel list) and hands them to channel construction; the
conversation supervisor's registration/deregistration sites take two
injected `Arc<AtomicU64>`s (actors, participants); the inbox takes the
overflow aggregate through `InboxInstall`. No trait, no callback
into the server, no reverse dependency — an `Arc<AtomicU64>` is
dependency-neutral by construction. Every deregistration/cleanup path that
the D4 churn discipline already covers decrements what registration
incremented; the §9 transition tests assert balance.

### 2.6 The snapshot writer protocol — one authority, cold by construction

The snapshot has MULTIPLE natural writers — the accept thread
(reserve/spawn/register, `supervisor.rs:502-560`), connection scheduler
workers (worker registration `:979-995`, readiness registration
`:1197-1218`), and the converging close/crash/reap routes
(`:1148-1266,1345-1382`). A naive load-copy-store on the `ArcSwap` loses
updates under that concurrency (a lost close leaves a ghost connection; a
lost accept omits a live one), and serializing on the `records` mutex is
forbidden — every slice locks it via `is_registered`
(`process.rs:120-131` → `supervisor.rs:1165-1170,1269-1273`), which would
re-import §5.2.

**Decision: a dedicated snapshot-writer mutex is the single update
authority.** `ops_snapshot_writer: Mutex<()>` (or `Mutex<SnapshotBuilder>`),
with these invariants:

- **Every GLOBAL mutation** (publish entry, update posture, remove entry)
  acquires the writer mutex, clones the current snapshot, applies a
  **keyed idempotent transformation** (by pid; apply-to-missing is a no-op,
  re-apply is harmless), and swaps. No unserialized load-copy-store exists.
  Subscription sub-lists and the budget slot are NOT global mutations — they
  ride the per-connection authority and the bundle `OnceLock` (§2.5(b)) and
  never touch this mutex.
- **"Cold" is a precise claim, not a vibe:** the writer mutex is acquired
  ONLY at connection-lifecycle events (admission, auth, worker/readiness
  registration, close/reap) — never on the per-slice service path, never
  per-delivery, never at subscription transitions (§2.5(b)), and **never by
  readers** (a scrape does one `ArcSwap` load; it cannot contend this mutex
  by construction). A slice-path writer touches it at most once per
  registration event in its connection's LIFE, not per slice.
- **Ordering, per route:** admission — reserve CAS → construct bundle →
  insert record → publish snapshot entry (under writer mutex) → return from
  spawn; nothing external can address the connection before its entry
  exists. Admission rollback — remove entry if present (idempotent), release
  reservation. Worker/readiness updates — keyed posture update; entry
  already gone (concurrent close) ⇒ no-op. Close/reap — all routes funnel
  through the single removal path, which removes the record, then the
  snapshot entry, then drops the bundle clone; entry removal is the LAST
  publication, so a scrape never sees an entry whose record is still
  half-torn.
- **Named collision tests:** (c1) concurrent accept×N + close×N storm —
  snapshot converges to exactly the live record set at quiescence, no ghost
  and no missing entry; (c2) worker/readiness registration racing close —
  no resurrection of a removed entry; (c3) readers scraping continuously
  during (c1)/(c2) never block (bounded scrape latency while the writer
  mutex is held-and-released under storm) and never observe a torn entry.

---

## 3. The read primitives, named

For the field tables, "read-primitive" takes one of exactly these values, in
increasing cost, none of which wakes a scheduler or takes a slice-hot lock:

| Primitive | Backing | Cost | Staleness |
|---|---|---|---|
| `config` | Immutable redacted `ServerConfig` clone in OpsState | pointer deref | exact (frozen at boot) |
| `atomic` | `AtomicU64`/`AtomicUsize` in OpsState, written at a lifecycle event | one load | bounded-stale by one event |
| `arcswap` | `ArcSwap<Snapshot>` in OpsState, rebuilt on infrequent per-connection events | one `Arc` load + immutable walk | bounded-stale to last such event |
| `metrics` | Process-global registry snapshot (`metrics_route.rs`) | RwLock **read** guard, copy | bounded-stale, structural lock only |
| `readiness` | `SharedReadinessState::snapshot` (`health/checks.rs`) | four `SeqCst` loads | bounded-stale, lock-free |
| `inventory` | beamr `service_inventory()`/`worker_names()`/`service_policies()` on a retained scheduler handle | metadata Vec copy | bounded-stale, no actor query |

`config` is exact; every other primitive is **bounded-stale** and the field
tables say so explicitly (§8's honesty rule: staleness is stated, never
implied). An `arcswap` entry may embed shared live atomics (§2.2's
shared-live-atomic pattern); reading such a field is one `Arc` load plus one
`Relaxed` atomic load — still lock-free, still this primitive. No primitive
outside this table is admissible; introducing one is a §5 violation.

### 3.1 The common envelope — its own field table, same discipline

Every §11 response carries an envelope; its fields obey the same per-field
discipline as view fields (they were previously asserted without rows — Sol
minor, fixed):

| Field | Owner | Update event | Read-prim | Lock | Staleness | Cardinality | Redaction | Idle-cost |
|---|---|---|---|---|---|---|---|---|
| `phase` | OpsState `AtomicU8` (§2.2.2); writer = runtime thread only; transitions Starting→Running (listener bound + inventory sealed) → Draining (shutdown initiated) | the two named transitions | `atomic` | lock-free | bounded-stale by one transition | 1 | none | 0 |
| `scraped_at` | **render-time work, NOT state** — `SystemTime::now()` read while building the response, formatted RFC 3339 UTC. Explicitly outside the §3 primitive table: it is per-request work on the serving thread, not a stored fact. Clock failure ⇒ field is `null`, response still serves. | per response render | — (render work) | none | exact at render | 1 per response | none | 0 (no scrape ⇒ no clock read) |
| `as_of` (per observer-backed field; v1: unused) | the observer's own replay/refresh timestamp, carried in-response per T3 (§5.4) | observer refresh | rides the field | rides the field | stated by the field itself | per observer-backed field (v1: 0) | none | rides the field |
| `contract`, `major`, `additive_rev` | **compile-time contract metadata** — `const` in the §11.0 contract module, NOT ServerConfig (it describes the code's contract, not the operator's config; the earlier "immutable config" attribution was wrong and is corrected here) | — (frozen at build) | `config`-class (const deref) | none | exact | 1 | none | 0 |

### 3.2 `OpsReadState` — the structural read boundary

View builders and endpoint handlers receive **`OpsReadState`**, a read-only
boundary type that exposes ONLY: the `OpsConfig` deref, the atomic loads, a
**scoped** snapshot accessor — `with_snapshot(|s| …)`, so a snapshot `Arc`
cannot escape a view build (the §9 generation invariant is structural, not
convention) — the readiness snapshot, the metrics renderer, and
the sealed inventory's `Weak` upgrades. It holds **no** `ConnectionRuntime`,
no supervisor, no process state, no actor/channel/conversation handles, no
store — a view builder **cannot** reach a hot mutex because the types it can
name do not contain one. This is enforced structurally (module visibility:
the write side of §2.5/§2.6 lives in a module the view code cannot import)
and pinned by the §8 no-hot-lock barrier test. The boundary is the design's
answer to "the no-hot-lock property must be enforced by the abstraction, not
by review vigilance."

---

## 4. The six v1 views — full per-field discipline

These are the six views the one-pager §(iii) names: **connections, channels,
subscriptions, conversations, caps/pressure, inventory**. [SEAM-DEP §(iii)]:
the view SET is fixed by the one-pager; if the tear adds or drops a view, a
table here is added or struck. Every field carries the full line the one-pager
demands: **owner / update-event / read-primitive / lock-behavior / staleness /
cardinality / redaction / idle-cost**. Idle-cost is stated for the *steady
idle* server (no traffic, connections parked): a field's idle-cost is the cost
its maintenance imposes when nothing is happening. For every OpsState field
that is written only at a lifecycle event, idle-cost is **zero** — the whole
point of the design. Where a field's idle-cost is non-zero it is called out and
routed to §8/§9 as a rule-2 item.

Legend for lock-behavior: **lock-free** (atomics only); **read-lock** (a
structural read guard never taken by a hot path); **none** (immutable deref).

### 4.1 connections

| Field | Owner | Update event | Read-prim | Lock | Staleness | Cardinality | Redaction | Idle-cost |
|---|---|---|---|---|---|---|---|---|
| `active` | `liminal_connections_active` gauge | accept/close | `metrics` | read-lock | bounded-stale | 1 | none | 0 |
| `admissions_current` | **the CAS'd CURRENT admitted+reserved count.** Writer path defined (Sol finding — the existing `admissions` AtomicU64 is embedded non-shareably in `ConnectionRuntime`, `supervisor.rs:687-692`): OpsState owns an `Arc<AtomicU64>` and `ConnectionRuntime::new` takes it injected at construction as ITS admission counter — `try_reserve_admission` (`supervisor.rs:741-765`) CASes the shared atomic directly. One atomic, two names, no mirror, no new write. Threaded via the §2.5(a) constructor migration WITH `config.limits` (the production constructor today defaults limits, `supervisor.rs:111-119` — carry both or the caps view and the CAS disagree); pinned by the §2.5(a) non-default-cap test. Decrements at teardown/rollback; NOT a monotonic total. | admission CAS / teardown decrement (existing writes on the shared atomic) | `atomic` | lock-free | bounded-stale | 1 | none | 0 |
| `admissions_started_total` | NEW monotonic OpsState counter, incremented once at admission-success (a new write at a connection-lifecycle event, not a promotion of the CAS count) | admission success | `atomic` | lock-free | bounded-stale | 1 | none | 0 |
| `reserved_in_flight` | derived `admissions_current.saturating_sub(active)`: current admitted+reserved minus connections that reached a live record — the reserved-before-record window the scout flagged. The two terms are **independently sampled** (§2.4 disclaims cross-field atomicity): a cross-event sample can transiently exceed truth or hit zero via saturation — **`saturating_sub`, never unsigned wrap**, and the caveat rides the field's description in the contract. | — | `atomic`+`metrics` | **read-lock** (the `active` term rides the metrics registry snapshot) | bounded-stale, cross-sampled | 1 | none | 0 |
| `peer` (per conn) | `ConnectionsSnapshot` arcswap | accept/auth/close | `arcswap` | lock-free | last conn event | ≤ `max_connections` (256) | peer addr shown; **fd redacted** | 0 |
| `worker_posture` (per conn) | `ConnectionsSnapshot` | worker register | `arcswap` | lock-free | last conn event | ≤ 256 | registration summarized, no raw fd | 0 |
| `readiness_posture` (per conn) | `ConnectionsSnapshot` | readiness register/dereg | `arcswap` | lock-free | last conn event | ≤ 256 | token **never** shown | 0 |
| `slice_serviced` (per conn) | R7 `Arc<AtomicU64>` shared into the snapshot entry (§6, the §2.2 shared-live-atomic pattern — never read through the records map) | serviced slice | `arcswap` (entry-embedded atomic) | lock-free | bounded-stale | ≤ 256 | none | 0 (parked ⇒ no write) |

**Never** the `records` mutex (§5.2). Per-connection detail rides the arcswap
snapshot, rebuilt only on the §2.2.4 event set (connection lifecycle +
subscribe/unsubscribe, under the §2.6 writer mutex) — never
on a slice.

### 4.2 channels

| Field | Owner | Update event | Read-prim | Lock | Staleness | Cardinality | Redaction | Idle-cost |
|---|---|---|---|---|---|---|---|---|
| `name`, `configured` metadata | immutable config channel list (`config/types.rs`) | — (frozen) | `config` | none | exact | # configured channels | none | 0 |
| `subscriber_count` (per channel) | injected per-channel `Arc<AtomicU64>` (§2.5(c)), pre-registered from the immutable configured-channel list | subscribe/unsubscribe | `atomic` | lock-free | bounded-stale | ≤ `max_configured_channels` (1,024 — the §9 config cap) | none | 0 |
| `publishes_total` | `liminal_publishes_total` | publish accepted | `metrics` | read-lock | bounded-stale | 1 (process-global) | none | 0 |
| `deliveries_total` | `liminal_deliveries_total` | delivery | `metrics` | read-lock | bounded-stale | 1 (process-global) | none | 0 |

**Never** `ChannelRegistry::list` (§5.1). Subscriber gauges are per-configured-
channel atoms keyed by the immutable config names (bounded cardinality,
pre-registered — no dynamic label creation on a hot path, scout cardinality
risk). Publishes/deliveries stay process-global in v1 (the granularity gap the
scout names); per-channel split is a v2 instrument with its own signed atomic
cost, not smuggled in here. **Absent-by-profile** in worker-front-door.

### 4.3 subscriptions

| Field | Owner | Update event | Read-prim | Lock | Staleness | Cardinality | Redaction | Idle-cost |
|---|---|---|---|---|---|---|---|---|
| `inbox_bytes_used` (per conn) | the EXISTING `ConnectionInboxBudget.used` AtomicUsize (`subscription.rs:47-118`), `Arc`-shared into the snapshot entry (§2.2 pattern) — **not mirrored; NO new write exists.** One-line code change implied: promote the `cfg(test)` `used()` accessor to a host-reachable handle captured at budget construction. | admit/drain (existing writes, unchanged) | `arcswap` (entry-embedded atomic) | lock-free | bounded-stale | ≤ 256 | none | 0 |
| `inbox_bytes_cap` | immutable `max_connection_inbox_bytes` (4 MiB) | — | `config` | none | exact | 1 | none | 0 |
| `overflow_flags_total` | injected `Arc<AtomicU64>` riding `InboxInstall` (§2.5(b)/(c)); incremented ONLY on the first false→true overflow transition — `overflowed.swap(true, AcqRel) == false` gates it, so the repeated `store(true)` refusal sites (`subscription.rs:283-311`) count once per shed, not once per dropped frame. Semantics: counts SHEDS, not refused admits. | first overflow per subscription | `atomic` | lock-free | bounded-stale | 1 | none | 0 |
| `subscriptions_active` (per conn) | per-connection AtomicU64 in the §2.5 observation bundle, embedded in the snapshot entry (the keyed read path — a bare "OpsState atomic" has no per-connection key; corrected per Sol) | subscribe/unsubscribe/shed | `arcswap` (entry-embedded atomic) | lock-free | bounded-stale | ≤ 256 | none | 0 |
| `depth` (per subscription) | the `SubscriptionObservation.depth` `Arc<AtomicUsize>` created WITH the inbox (§2.5(b)). **Write placement is mandated, not optional — INSIDE the inbox lock** (outside-the-lock is racy: close can drain between a queue op and a delayed depth update, stranding nonzero depth on a closed inbox): after successful push, increment BEFORE unlock (`subscription.rs:277-326`); after successful pop, decrement BEFORE unlock; on close, store zero (or subtract the drained count) WHILE HOLDING the lock (`:354-385`); a refused admit writes nothing. Published via the per-connection sub-list (§2.5(b)). Readers never take the mutex. | admit/pop/drain/close | `arcswap` (sub-list-embedded atomic) | lock-free read; **write is inside the inbox hot mutex** | bounded-stale | ≤ 256 × 32 = 8,192 | none | 0 |
| `overflowed` (per subscription) | the `SubscriptionObservation.overflowed` `Arc<AtomicBool>` — the inbox's sticky flag itself, its owner changed from embedded to `Arc` (`subscription.rs:179-187`; set sites unchanged). Sticky-true survives until the shed's unsubscribe publication removes the sub-list entry, so a scrape between overflow and shed sees `overflowed: true` on a still-listed subscription — the honest window. **Automatic shed ordering:** the shed path (`process.rs:377-398`) removes the subscription with no Unsubscribe frame; it publishes the sub-list removal exactly as an explicit unsubscribe does — no path removes an inbox without publishing. | overflow set (sticky); entry removed at shed/unsubscribe publication | `arcswap` (sub-list-embedded atomic) | lock-free | bounded-stale | ≤ 8,192 | none | 0 |

**Depth is a settled-seam REQUIREMENT, not an option** (one-pager §(iii):
"inbox budget occupancy, overflow flags, **depth gauges** (event-maintained
host atomics)"). An earlier revision deferred it; Ruling A reversed that:
implement the settled seam, do not amend it. The shape chosen is
**per-subscription** (not per-connection aggregate) because the governing cap
— `max_subscription_inbox_depth` = 256 — is per-subscription: an aggregate
cannot be compared against the cap it exists to observe.

**Re-priced honestly (Sol r2 — "one Relaxed write beside the lock" was an
under-claim):** because the write is mandated INSIDE the lock, the cost is
**one atomic RMW added to the publisher/delivery critical section while the
inbox hot mutex is held**, plus reader-side cache-line traffic that can
contend with a lock-holding writer (a scrape loading `depth` invalidates the
line the writer is about to touch). Zero while parked, but the critical-
section widening is the number the pair signs — stated as such in §8, not
hidden inside "beside". **Named pins:** (depth-t1) concurrent admit/pop/close
tests asserting `depth == queue.len()` at every lock-protected checkpoint,
reaching zero at teardown without underflow; (depth-t2) cost pin — the depth
write is present on the admit/pop paths and absent from the parked path
(scrape moves no parked depth gauge, the §6-class sibling).

**Never** the inbox queue lock for READS: `has_pending`/`len` share the mutex
the admit/pop delivery hot paths take (`subscription.rs:350-395`, §5) — the
depth atomic exists precisely so no reader ever wants that mutex; the WRITE
lives inside it by correctness necessity, and is priced accordingly.

Write-rate honesty for this view's NEW atomics: `subscriptions_active` and
the sub-list copies move at subscription-transition rate (slice-path,
rate-unbounded — priced in §2.5(b) with the storm test);
`overflow_flags_total` moves once per shed. `depth` moves **per delivery**
(inside-lock RMW, priced above). `inbox_bytes_used` moves per delivery, but
that write **already exists** (the budget's own accounting); this design
adds no write to it.

### 4.4 conversations — [SEAM-DEP: F-0c §R1 boundary, §4.7 below]

| Field | Owner | Update event | Read-prim | Lock | Staleness | Cardinality | Redaction | Idle-cost |
|---|---|---|---|---|---|---|---|---|
| `actors_registered` | injected global `Arc<AtomicU64>` (§2.5(c)) written by the conversation supervisor's register/dereg sites (the D4-count sites, `conversation/actor.rs:124-150`, whose `Mutex<HashMap>` is never read) | actor register/dereg | `atomic` | lock-free | bounded-stale | 1 | none | 0 |
| `participants_registered` | injected global `Arc<AtomicU64>` (§2.5(c)) | participant register/dereg | `atomic` | lock-free | bounded-stale | 1 | none | 0 |
| `pending_replies` (per conn) + `pending_replies_total` | per-connection AtomicU64 in the §2.5 observation bundle, written by the pending-reply table's transition sites the process already executes (`pending_reply.rs:139-308`: admit / complete / cancel / expire); **total = render-time sum over snapshot entries** (≤ 256 loads, bounded render work — one write per transition, no global duplicate) | admit/complete/cancel/expire | `arcswap` (entry-embedded; total summed at render) | lock-free | bounded-stale | ≤ 256 (+1 derived) | none | 0 |
| `tombstones` (per conn) + `tombstones_total` | same bundle shape; written at tombstone insert / reap-sweep | tombstone insert/reap | `arcswap` (summed at render) | lock-free | bounded-stale | ≤ 256 (+1 derived) | none | 0 |
| `armed_timers` (per conn) + `armed_timers_total` | same bundle shape; written at timer arm/cancel (park-flip §5 deadline timers) | timer arm/cancel | `arcswap` (summed at render) | lock-free | bounded-stale | ≤ 256 (+1 derived) | none | 0 |
| `conversations_active` (per conn) | bundle AtomicU64, written where the process enforces `max_conversations_per_connection` (conversation join/leave on the connection) | conversation join/leave | `arcswap` (entry-embedded) | lock-free | bounded-stale | ≤ 256 | none | 0 |

**Never** the conversation registry mutex (`count` locks the maps actor
dispatch/lifecycle share, §5.5), **never** `ConversationActor::state()` (an
actor command → wake-unsafe, §5.6). All are event-maintained host atomics
updated at the same lifecycle transitions the process/supervisor already
executes, homed per §2.5 (bundle for per-connection, injected globals for the
supervisor pair). **Write-rate honesty:** the pending/tombstone/timer/
conversation gauges move at REQUEST-lifecycle rate — each write is a NEW
**active-work cost** (one `Relaxed` atomic write per transition), zero at
idle, priced and grouped with the R7 rule-2 discussion in §8, **not**
implied-infrequent; `actors_registered`/`participants_registered` move at the
slower conversation-lifecycle rate. Totals derived by render-sum cost ≤ 256
atomic loads per scrape — render work, not state. The
`armed_timers` gauge is the direct observability of the park-flip §5 deadline
machinery — an idle parked connection holds ZERO timers by that design, so this
gauge reads 0 at idle and is itself the proof the deadline timers are
active-work-only.

### 4.5 caps/pressure

The §5 `LimitsConfig` matrix (`config/types.rs:203-236`) rendered as
**config-vs-occupancy pairs** — the cap from `config`, the occupancy from the
atomic the relevant view already maintains:

| Cap (config) | Default | Occupancy source (granularity matches the cap's scope) | Read-prim |
|---|---|---|---|
| `max_connections` | 256 | connections.`admissions_current` — **the CAS-enforced value** (`try_reserve_admission`, `supervisor.rs:741-765`, refuses against admitted+reserved, so `active` alone can read below-cap while admissions already refuse; Sol finding, fixed). `active` and `reserved_in_flight` are exposed alongside, separately. | `atomic` |
| `max_subscriptions_per_connection` | 32 | subscriptions.`subscriptions_active` (per-connection, bundle) | `arcswap` |
| `max_conversations_per_connection` | 32 | conversations.`conversations_active` (per-connection, bundle — §4.4) | `arcswap` |
| `max_pending_pushes_per_connection` | 32 | `pending_pushes` (per-connection, bundle — full row below) | `arcswap` |
| `max_pending_conversation_replies_per_connection` | 32 | conversations.`pending_replies` (per-connection, bundle — §4.4) | `arcswap` |
| `max_pending_replies_per_conversation` | 8 | `max_pending_plus_tombstones_per_conversation` (per-connection WORST-conversation value, bundle — full row below). **Scope-matching, per Sol r2:** the earlier `saturated_conversations` pairing was semantically invalid — a count of saturated conversations (e.g. 9) compared against limit 8 reads as false over-cap pressure; the caps row needs a value in the CAP'S unit (pending+tombstones of the worst conversation), which this is. | `arcswap` |
| `max_connection_inbox_bytes` | 4 MiB | subscriptions.`inbox_bytes_used` (per-connection, shared budget atomic — §4.3) | `arcswap` |
| `max_subscription_inbox_depth` | 256 | subscriptions.`depth` (per-subscription — §4.3) + overflow counter | `arcswap` + `atomic` |

**Granularity, resolved (supersedes the earlier deferral):** an earlier
revision paired two per-connection caps with process-global gauges and
deferred the fix. The §2.5 observation bundle dissolves the deferral's
reason: per-connection occupancy is the SAME single write per transition
(targeting a bundle atomic instead of a global), needs NO request-rate
snapshot rebuilds (the Arcs are entry-embedded), and its per-event cost is
already in the §8 rule-2 set being signed. Every per-connection cap is
therefore paired with a per-connection gauge in v1; view-level totals are
render-sums (§4.4). In the JSON (§11.1), per-connection occupancies appear
in the caps document as `{ cap, limit, worst: { id, occupancy }, over_80pct:
n }`-shaped summaries plus per-connection detail in the connections view —
sketch-level, frozen by the contract module.

**The new bundle gauges, full rows:**

| Field | Owner | Update event | Read-prim | Lock | Staleness | Cardinality | Redaction | Idle-cost |
|---|---|---|---|---|---|---|---|---|
| `pending_pushes` (per conn) | bundle AtomicU64. Transitions, enumerated from `supervisor.rs:1002-1091` + the merged deadline surface: +1 `register_push`; −1 reply-resolved; −1 timeout/`Expired` disposition (`supervisor.rs:657` on main); −k close-sweep (all of a pid's slots dropped at close — decrement per removed slot, balanced). Host-side writers resolve the bundle through the current snapshot (lock-free `ArcSwap` load) or the record's bundle clone — never a new map. | push register/resolve/expire/close-sweep | `arcswap` (entry-embedded) | lock-free | bounded-stale | ≤ 256 | none | 0 |
| `max_pending_plus_tombstones_per_conversation` (per conn, worst) | TWO bundle atomics: worst VALUE (AtomicU64) + worst CONVERSATION ID (AtomicU64). Recomputed by the process-owned table — a bounded scan (≤ `max_conversations_per_connection` = 32 conversations) — after each **cardinality-changing** transition, then both stored. **The cardinality-changing set, corrected per Sol from `pending_reply.rs:139-308`:** admit (`:139-198`), cancel (`:200-210`), reply removal (`:212-240`), conversation sweep (`:284-299`), cancel-all (`:301-308`). **Expiry is NOT in the set** — it converts Pending→Tombstone (`:243-267`) without changing pending+tombstone cardinality, so expire neither raises nor lowers this value. **Redaction considered:** the conversation id is an identifier, not content — exposed by default (it names WHERE the pressure is, which is the row's operational point) and listed in §7.3's identifier class; the cross-sample caveat (value and id are two atomics, §2.4) rides the field. | admit/cancel/reply-removal/sweep/cancel-all (cardinality changes only) | `arcswap` (entry-embedded pair) | lock-free | bounded-stale; value/id cross-sampled | ≤ 256 × 2 | id = identifier class (§7.3) | 0 |
| `saturated_conversations` (per conn) | bundle AtomicU64 — **a separately-named PRESSURE count, deliberately NOT in the caps table** (it answers "how many conversations are pinned at cap", not "how close is the worst one"). Maintained on the same corrected cardinality-changing set as the row above: +1 when a conversation's pending+tombstones reaches cap via admit; −1 when a cardinality-reducing transition (cancel / reply removal / sweep / cancel-all) drops it below; **expiry does neither**. | the corrected cardinality-changing set | `arcswap` (entry-embedded) | lock-free | bounded-stale | ≤ 256 | none | 0 |

All are push/request-rate **active-work writes** (the worst-pair recompute
adds a ≤ 32-entry scan at request rate — bounded, stated, in the signed
set); each gets boundary-crossing tests (§9) for the corrected transition
set explicitly: **admit, pending-reply removal, late-reply tombstone reap,
cancel, conversation close, connection close** — plus the expiry
NON-transition (an expiry storm moves neither gauge, the fail-first pin for
the corrected semantics). Close-sweep balance explicit: a connection dying
with k outstanding pushes and s saturated conversations zeroes all gauges
through the single removal path.

Plus **refusal counters by typed-refusal class** — one atomic per class,
incremented where the refusal is emitted:

- `refused:unauthenticated`, `refused:read-only`, `refused:not-mounted`,
  `refused:requires-exclusive-store` (the one-pager §(iii) four-class
  MCP-surface taxonomy, §11.3 — the fourth class is reserved liminal-side,
  §11.3).
- Wire-protocol refusal counters: auth failures, cap-exceeded rejections by
  cap class, backpressure `Reject`/`Defer` counts.
- **Push-reply expiry counter — now a merged surface.** The push-reply
  deadline fix **merged to main @ `68379e8`** (ledger G7):
  `ConnectionSupervisor::push_to_connection_with_deadline`
  (`supervisor.rs:295` on main) and `ServerError::PushReplyExpired`
  (`error.rs:78` on main; an expired slot resolves to it at
  `supervisor.rs:657`). The caps/pressure taxonomy carries
  `push_reply_expired_total` as a first-class pressure counter, incremented
  where the `Expired` disposition resolves — a typed expiry, not an untyped
  drop.

All lock-free atomics; idle-cost 0 (occupancy gauges are written at the same
events the caps govern; refusal counters only move when a refusal is emitted,
which is by definition not idle).

### 4.6 inventory

| Field | Owner | Update event | Read-prim | Lock | Staleness | Cardinality | Redaction | Idle-cost |
|---|---|---|---|---|---|---|---|---|
| beamr `service_inventory()` lines | retained connection (always) + channel/conversation (full) scheduler handles | — (metadata copy per scrape) | `inventory` | metadata copy, no actor query | bounded-stale | # ancillary services | fd_classes shown as class, not raw fd | 0 (no scrape ⇒ no copy) |
| `worker_names()` | same handles | — | `inventory` | same | bounded-stale | # workers | thread names only | 0 |
| `service_policies()` | same handles | — | `inventory` | same | bounded-stale | # policies | none | 0 |
| scheduler census | OpsState captured handles (§2.3) | — | `config`+`inventory` | none | exact set, stale counts | connection always; channel/conv full-only | none | 0 |

These are the same lines the D2 census pins assert (`supervisor_tests.rs:21-34`
pins `readiness configured=1, actual=1, thread_names=[beamr-readiness-poll]`).
`service_inventory()` is host-side and verified to schedule no actor (scout no-
wake: SAFE). The scrape allocates a Vec **only when called**, so idle-cost is
0. Shared service instances must be deduplicated by instance id (scout risk).
`worker_names()` must be rendered alongside `service_inventory()` or normal
scheduler workers are under-reported (scout risk).

### 4.7 The conversations view vs the F-0c client contract — met only at the field list

The conversations view (§4.4) is the **operator** surface. Cally Ray's F-0c §R1
is the **client-protocol** surface (frame `6a750c8`). The one-pager fences them
explicitly: "this seam is the *operator* surface; they meet only at the
conversations view's field list." Stated as the boundary:

| F-0c §R1 client-contract assertion | Operator conversations view |
|---|---|
| OPEN/CREATE a conversation from the SDK | **not exposed** — creation is a client act, not an operator read |
| receive unsolicited messages; reply | **not exposed** — message content/flow is never on the operator surface |
| observe participant lifecycle (join/leave/death) | `participants_registered` **count only** — no identity, no lifecycle event stream |
| message ORDERING | **not exposed** — ordering is a client-protocol guarantee, invisible to the operator gauge |
| backpressure (Accept/Defer/Reject) | caps/pressure **refusal counters** (§4.5) — aggregate counts, not per-message decisions |
| resume after restart | **not exposed** — a cursor/replay concern owned by the SDK/participant, not OpsState |

The two surfaces share exactly one fact: *a conversation exists, has N
participants, has M pending replies*. The operator view reports the
**cardinality and health** of conversations; the client view reports their
**message semantics**. This doc's conversations view stops at the field list
above and asserts no client-protocol semantics. [SEAM-DEP: if the F-0c §R1 list
is re-cut (it was re-cut once already, per that brief), only this table changes.]

---

## 5. DO-NOT-EXPOSE — named negative design decisions

Each is a verified wake- or contention-hazard; each is a decision, not an
omission; each names its safe alternative (house rule: refusals name their
remedy). These are the fence that keeps §3's primitive list closed.

### 5.1 `ChannelRegistry::list` — WAKE-UNSAFE
`registry.rs:89-100` clones handles under the registry mutex then calls
`handle.subscriber_count()` per channel, which delegates to `list_subscribers`
→ creates a reply channel → `enqueue_atom_message(ListSubscribers)` → **blocks
on the actor reply** (`channel/types.rs`, `channel/actor/mod.rs`). One console
refresh would wake **every** channel actor — the exact birth-constraint
violation (park-flip §4). **Safe alternative:** immutable configured-channel
metadata (`config`) + per-channel lifecycle-maintained subscriber atomics
(§4.2).

### 5.2 `ConnectionRuntime.records` mutex scrape — CONTENTION-UNSAFE
`records` is a `Mutex<HashMap>` that **every connection slice already locks**:
`process.rs:130` calls `is_registered(pid)` → `records.lock().contains(...)` on
the slice hot path. A console that locked `records` per scrape would delay live
slices — a read-side contention regression on the exact path the incident is
about. **Safe alternative:** the `ConnectionsSnapshot` arcswap rebuilt on
infrequent connection events (§2.2, §4.1); `admissions` promoted to a
`Relaxed` accessor.

### 5.3 The `cfg(test)` R7 `Mutex<HashMap>` promoted unchanged — CONTENTION-UNSAFE
`slice_counts: Mutex<HashMap<u64,u64>>` (`supervisor.rs:671`) is `cfg(test)`;
every test-build slice locks and increments it. Promoting it **as-is** to
production would add a mutex op to every slice and let a console read contend
the very path it observes. **Safe alternative:** the §6 per-connection
AtomicU64 with `Relaxed` — one cache-line write per serviced active slice, zero
while parked.

### 5.4 `DurableStore` scan/read/flush probes — WAKE/LOCK/IO-UNSAFE
`DurableStore` exposes only `append/read_from/cas/read_value/scan/flush`
(`durability/store.rs:20-58`); each delegates to haematite's `EventStore`,
which **synchronously waits on owning shard actors**. There is no stats/status
accessor, and haematite 0.4.1 has no general `DatabaseStats` API (scout dep-
inspection). Probing the store for console health would do actor/IO work and
contend the durability writer path. **Safe alternative:** report store **mode/
path-configured from the construction branch** (`config`, `services.rs:830-888`
selects `Some(path)`→persistent vs `None`→ephemeral) plus, if wanted, explicit
operation/last-error **counters instrumented at the call sites** — never a
store probe. **Domain-owner determination (d), recorded: store status =
construction-facts only, confirmed.**

**The settled split (T4, no longer a deferral):** the tear settled the
durability line's two halves. **liminal-side: construction facts** (mode/
path-configured from the construction branch, as above). **haematite-side:
the T4 views, Apollo's half of the settled contract** — store identity
(data_dir AS CONFIGURED, format_version, shard_count — config echo), shard
gauges (materialised count, per-shard committed seq, embedder-maintained),
branch/snapshot metadata counts (names redacted by default), and the last
`VacuumReport` **as a document** (the seam serves the artifact, never runs
the tool). No content reads, no scan, no checkout. When liminal-server opts
in to mounting haematite's contract (namespaced, one-pager §(i)), it mounts
those T4 views as specified there — this doc adds nothing to them.

**T3 shape inheritance (folded):** every liminal v1 field is
event-maintained or config-frozen, so **no liminal v1 field is
observer-backed** — but the durability line inherits T3's shape for any
future observer-backed field (haematite's `ReadOnlyDatabase` read is
WAL-replay-per-call, LEDGER A2): such a field carries its **per-call cost as
a §9 lens line PLUS an `as_of` staleness field in the response itself**. A
view that hides its polling cost is the incident wearing a dashboard
(one-pager §(iv), Apollo's wording). §11.1's response shape reserves the
`as_of` slot for exactly this.

### 5.5 Conversation registry `count` scrapes — CONTENTION-UNSAFE
`registered_actor_count`/`registered_participant_count`
(`conversation/actor.rs:124-150`) lock the same `Mutex<HashMap>` maps that
actor dispatch and lifecycle use. **Safe alternative:** lifecycle-maintained
atomic gauges (§4.4).

### 5.6 `ConversationActor::state()` queries — WAKE-UNSAFE
`state()` routes through `handle.query_state` (`conversation/actor.rs:174-180`)
— an actor command that wakes the conversation actor. **Safe alternative:**
event-maintained host atomics; `has_pending_reply` is a direct core query but
individual actors are not held in a host snapshot, so it is not console-
reachable and is not exposed.

### 5.7 The catch-all
No console read may use `enqueue_atom_message`, an actor command, process-table
execution, a connection control message, or any lock a write/flush/sweep/slice
hot path takes (one-pager §(iv), park-flip §4). Any field not answerable from
§3's six primitives is **not in v1** and its exposure is a new design act with
its own idle-cost sign-off — never a bolt-on.

---

## 6. R7 promotion — the per-connection slice gauge

**Decision:** replace the `cfg(test)` `Mutex<HashMap>` (`supervisor.rs:671`)
with a **per-connection `AtomicU64`, `Relaxed` ordering**. The active slice
path bumps it with one `fetch_add(1, Relaxed)` — **one cache-line write per
serviced slice, zero writes while parked** (a parked connection returns
`NativeOutcome::Wait` and runs no slice, `process.rs:211`). Reads are
`Relaxed` loads, lock-free.

**The read path is load-bearing, so its shape is specified, not implied.** If
the counter lived only inside the `records` map, READING it would take the
slice-hot records mutex — §5.2's own violation, re-imported through the back
door. The shape that avoids it (§2.2's shared-live-atomic pattern):

- the counter is an **`Arc<AtomicU64>` created at admission** — the first
  member of the §2.5 observation bundle, and the pattern the bundle's other
  counters follow;
- **one clone rides with the connection process/record** for the slice-path
  `fetch_add` (the writer's handle — no map lookup on the hot path);
- **one clone is embedded in the immutable `ConnectionsSnapshot` entry**, so
  a scrape reads it lock-free through the arcswap — never through `records`;
- **snapshot rebuilds carry the SAME `Arc`**: the counter survives snapshot
  swaps; only the entry `Vec` is rebuilt. A rebuild never resets or copies
  the count, and no rebuild is triggered by counting.

This is the difference between the design working and §5.2 regressing.

- **Cardinality:** one atomic per live connection, ≤ `max_connections` (256).
  Not a per-pid labelled metric (registry cardinality/cache risk, scout) and
  not the mutex map (§5.3). This is the explicit cardinality decision the scout
  asked for — **domain-owner determination (a), recorded: per-connection
  AtomicU64 cardinality CONFIRMED; the increment-cost number stays the
  pair's to sign.**
- **Signed active-slice cost:** the added steady cost is exactly one `Relaxed`
  atomic increment on the slice path that already ran. At idle there is no
  slice, so the added idle cost is **zero**. Because it touches the hot path at
  all, the number is a **rule-2 item**: the increment's presence is bounded
  (one `fetch_add` per serviced slice, never on the park path) and the bound is
  signed by the certifying pair at their desk before merge (§8, one-pager
  §(iii): "R7 slice counters ride only via their signed promotion … signed at
  the pair's desk").
- **Permanent regression (the pin):** *a console scrape does not advance a
  parked connection's slice counter.* This is the sibling of the park-flip
  quiescence pin (`supervisor_tests.rs:38-59` today records a parked slice
  count, sleeps, asserts it does not advance, then proves Ping→one Pong→repark).
  The new test parks a connection, records the R7 counter, calls **every**
  console/MCP endpoint repeatedly, and asserts the parked counter is unchanged
  — plus asserts no actor command/control/mailbox API was invoked on the scrape
  path. A contract documented but never pinned is a pin that can't fail; this is
  the pin, and it is written fail-first (able to fail against a live regression)
  exactly like the park-flip inversion (`process_wake_tests.rs`).

This gauge is also the retrospective's answer (§10): had it existed, the
incident's four pinned cores would have shown as slice growth attributed to
idle connections on day one.

---

## 7. Auth — a separate operator credential, not H4 — [SEAM-DEP: one-pager §(ii)]

### 7.1 The console/MCP surface does NOT inherit H4
H4 authenticates **wire-protocol clients**: a frame-level token gate on the
Connect frame, checked constant-time before version negotiation, with
application frames default-denied until handshake (`apply.rs:23-51,228-317`;
`config/types.rs:23-31` — token absent ⇒ open). That is a *different audience*
from operators/agents reading the service. And mechanically, the health
listener **cannot** inherit H4: its request parser reads only the request line
and ignores headers (`endpoint.rs:184`), and it has no access to the H4 token.
So the MCP surface needs its own auth decision (scout: "An HTTP console route
on the health listener does not inherit H4 … it needs a separate design
decision").

### 7.2 The contract (one-pager §(ii))
1. **Separate credential, same discipline.** MCP access carries its **own
   bearer token** (`[console] auth_token` class, key exact at design-doc
   level), configured independently of the H4 client token. One credential
   class per audience: rotating operator access never touches client auth and
   vice versa.
2. **Bind-address control is exposure management, not authentication.** The
   health listener today has *no* auth — exposure is entirely the configured
   bind address (`config/types.rs:14`). Bind scoping stays as defence-in-depth,
   but it is never *the* auth; a bound-but-unauthenticated MCP surface is a
   misconfiguration the design refuses to normalize.
3. **Unauthenticated is a typed refusal, not a silent 404.** A missing/wrong
   credential returns `refused:unauthenticated` (§11.3), distinguishable from
   `refused:not-mounted`. An agent must be able to tell "wrong credential" from
   "surface not here."
4. **Stdio transport: auth N/A by construction (T1, folded).** Where a seam
   host's transport is stdio (the `haem`-class local host), the bearer-token
   clause is **N/A by construction and says so** — auth is the OS's
   process-spawn boundary; nobody bolts a token onto a pipe. liminal-server's
   v1 mount is HTTP, so the bearer clause applies here in full — but the
   CONTRACT's auth section is worded per-transport, not assumed-HTTP (§1 T1),
   so a future stdio host of this same contract inherits the N/A clause
   rather than a vestigial token check.

[SEAM-DEP §(ii)]: synced to the settled one-pager @ 7b768eb (tear folded T1
into §(ii)(1)); if the credential model ever moves again (e.g. mTLS or an
authenticated proxy over bearer), §7.2 and the §11.3 refusal wiring change;
nothing else does.

### 7.3 Redaction defaults
Redacted from **every** MCP response by default, widened only by an explicit
named operator config act (never a default): **raw fds** (rendered as
fd-classes only, §4.6), **auth token bytes** (only `auth_enabled: bool`
escapes), **cluster cookies**, and **filesystem paths** beyond what config
already names publicly (persistence shown as mode + configured flag, §5.4). The
redaction is applied at OpsState **construction** (§2.2), so an un-redacted
value is never resident in the read object — redaction is structural, not a
render-time filter that could be bypassed by a new endpoint.

---

## 8. The idle clause and acceptance gates — [SEAM-DEP: one-pager §(iv)]

The one-pager's shared idle clause, liminal's half restated as this doc's
acceptance gates (no console may resurrect the busy-spin):

**G-NOWAKE (the no-wake gate).** Serving any MCP/console read **never wakes a
beamr process, scheduler, or mailbox** — no actor command, no
`enqueue_atom_message`, no process-table execution. Reads ride event-maintained
host state (§2–§3); writers pay at their existing lifecycle events. **Pinned
by** the §6 permanent regression (a console scrape does not advance a parked
connection's slice counter) **plus** a boundary assertion that no
actor/control/mailbox API is invoked on any scrape path (scout next-step).
**Honest scope limit (Sol finding, folded): this gate pins WAKE-safety only.**
A read-only scrape can lock the `records` mutex without waking anything — the
parked slice counter stays flat while the exact §5.2 contention regression
ships. The R7 pin does not and cannot detect lock contention; claiming
otherwise left half the safety property uncertified. Hence the second gate:

**G-NOHOTLOCK (the no-hot-lock gate).** Serving any MCP/console read **never
acquires a lock that a slice, delivery, actor-dispatch, or write path
acquires** — `records`, inbox queue mutexes, conversation registries, push
slots, or any successor. Pinned TWO ways, independent of G-NOWAKE:

1. **Structurally:** view builders receive only `OpsReadState` (§3.2), a type
   that cannot name `ConnectionRuntime`, process state, actor handles, or
   queue maps — enforced by module visibility and an API-surface test that
   fails if the boundary type ever grows a reachable hot-lock owner.
2. **Behaviorally — the barrier test:** a deterministic test acquires and
   HOLDS each named hot mutex (`records`, a subscription inbox lock, the
   conversation registries, `push_replies`) while repeatedly scraping every
   endpoint; every scrape must complete within its §11.6 latency budget
   while the lock is held. A scrape that ever wants one of those locks
   deadlocks/times out the test — fail-first by construction. (Lock-order
   instrumentation in debug builds is an acceptable strengthening, not a
   substitute.)

**The honest caveat, stated not hidden (one-pager §(iv)).** The serving
**thread** is not free. The health worker **already wakes every 10 ms polling
accept** (`endpoint.rs:122`, `thread::sleep(10ms)` on WouldBlock), and an
HTTP/MCP request executes on that worker. That is a **pre-existing,
separately-signed idle cost of the transport, not of the seam** — it predates
this design and was signed when the health listener shipped. This design adds
**no new resident thread** in v1 (§1: reuse-no-new-thread). The only new
steady cost this design introduces is:

- the §6 R7 `Relaxed` increment on the *active* slice path (zero at idle) —
  a **rule-2 item**: bound stated (one `fetch_add` per serviced slice), pinning
  test named (§6 regression), certifying-pair sign-off on the number required
  before merge.
- **the active-work write set — priced with R7, enumerated COMPLETELY, not
  implied-infrequent.** Every NEW cost this design adds on or adjacent to
  hot paths, each with its honest shape:
  (i) per-delivery: subscription `depth` — **one atomic RMW INSIDE the inbox
  hot mutex** (§4.3's mandated ordering; a critical-section widening plus
  reader cache-line contention with a lock-holding writer, NOT a free write
  beside the lock);
  (ii) request-rate: per-connection `pending_replies`, `tombstones`,
  `armed_timers`, `conversations_active`, `saturated_conversations`, **plus
  the worst-pair recompute's ≤ 32-entry table scan** (§4.4, §4.5);
  (iii) push-rate: `pending_pushes` register/resolve/expire (§4.5);
  (iv) subscription-transition rate (slice-path, rate-UNBOUNDED by
  occupancy caps): the ≤ 32-entry sub-list copy + swap (§2.5(b)),
  connection-local, pinned by the §2.5(b) storm test;
  (v) refusal counters (only move on refusals).
  Zero at idle, every one. Bounds as stated per item; pinned
  by the §9 one-write-per-transition + balanced-teardown tests and the
  §6-class scrape-moves-nothing-parked regression; signed at the pair's desk
  **in one sitting with the R7 number** — one sign-off covering the complete
  set. A future gauge that adds a hot-adjacent write joins this list or
  doesn't ship.
- **costs this design does NOT add, stated so they aren't double-counted:**
  `inbox_bytes_used` is the budget's EXISTING per-delivery atomic,
  `Arc`-shared into the snapshot (§4.3, no new write); `admissions_current`
  is the existing CAS atomic injected at construction (§4.1, no new write).
- the genuinely infrequent writers: `admissions_started_total` and
  conversation registration gauges (connection/conversation lifecycle),
  overflow counter (once per shed), and **global snapshot rebuilds under the
  §2.6 writer mutex** — connection-lifecycle rate ONLY (subscription
  transitions ride the per-connection sub-list, item (iv) above, NOT the
  global rebuild — the r2 correction), each rebuild one CoW allocation
  ∝ live connections, never per-delivery, never per-slice. (Per-channel
  subscriber atomics move at subscription-transition rate and belong to
  item (iv)'s rate class — one `Relaxed` write per transition, listed there
  in spirit, named here so nothing hides.) Zero-idle-cost by
  construction. No background refresh thread
  exists (§2.4); if one were ever added it would be a rule-2 item from birth.

**The sibling-listener fallback is a rule-2 item from birth (§1).** If the pair
rules the shared serial handler's head-of-line risk (a client can occupy the
sole worker for its 2 s read timeout, degrading `/health` and `/ready` —
`endpoint.rs:146`, scout risk) disqualifies sharing, the sibling listener adds
a resident thread whose idle cost (its own accept-poll cadence) must be bounded,
pinned, and signed. v1 avoids this by reusing the existing worker and by
**addressing the serial 2 s head-of-line behavior before adding expensive
endpoints** (scout next-step) — with the NUMERIC budgets and head-of-line
pins now set in §11.6, not left as adjectives. **Domain-owner determination
(b), recorded:** exactly this posture is confirmed — shared worker +
bounded-response + scrape-work budget; sibling listener only on the pair's
disqualifying ruling. (For contrast, the seam's stdio-host transport — T1,
not liminal-server's mount — carries **zero** idle cost by construction; the
10 ms accept-poll is a cost of *this host's* HTTP transport, not of the
contract.)

House rule enforced here: **every acceptance claim names its (future) pinning
test.** G-NOWAKE names the §6 regression + the boundary assertion. No claim in
this section is asserted without a named pin, because a contract documented but
never pinned is a pin that can't fail.

---

## 9. The four-question idle/resource lens — answered for the design as a whole

Campaign standing rule 5, answered for the console/MCP surface as one unit:

**Q1 — What is this design's idle cost, and its ceiling?** At steady idle (no
traffic, all connections parked): the console/MCP surface adds **zero** new
wakes and **zero** new resident threads (§8). Every OpsState field is written
only at a lifecycle event, so an idle server writes none of them. The reads are
pull-only (a scrape arrives or it doesn't). The non-zero costs are all
active-work: the §6 R7 increment fires **only on a serviced slice** — of
which a parked connection runs none — and the §4.4 request-rate gauge writes
fire only on request-lifecycle events, of which an idle server has none
(Q4(c)). **The resident-memory ceiling, as a formula.** Two rounds of
corrections are folded here: configured channels are an unbounded `Vec` in
`ServerConfig` (`config/types.rs:17-18`), NOT governed by any of the eight
limits — **this design adds `max_configured_channels = 1024`**, validated at
config load, refuse-at-load beyond. And a COUNT cap does not bound BYTES
(Sol r2): `ChannelDef.name` is an unconstrained `String` (validation checks
only empty/duplicate, `config/validation.rs:138-158`), so **byte ceilings
are added as validated config rules**: channel name ≤ 64 B; `[console]
auth_token` ≤ 128 B; total rendered redacted-config text ≤ 64 KiB (checked
once at load against the actual redacted rendering — the config either fits
its ceiling or the server refuses to start). **Channel-name representation,
decided:** interned once in OpsConfig as `Arc<str>` and SHARED — sub-list
records and snapshot entries clone the `Arc`, never the string, so name
bytes appear exactly once in the formula regardless of generations.

```
R = |OpsConfig|                                    ≤ 64 KiB (validated byte ceiling above)
  + N_ch × (8 B gauge + handle + Arc<str> hdr)     N_ch ≤ 1,024 → ≤ ~96 KiB (name bytes in |OpsConfig|)
  + N_conn × |bundle|                              N_conn ≤ 256
      |bundle| ≈ 11 atomics × 8 B + sub-list slot + budget slot
                + per-conn mutex + Arc overhead    ≈ ~200 B → ≤ ~50 KiB
  + N_sub × |observation object|                   N_sub ≤ 8,192; one-time (owned by inboxes,
      2 Arcs (depth + overflowed) ≈ 64 B           not per-generation) → ≤ ~512 KiB
  + N_conn × G_sub × |sub-list|                    per-connection sub-list generations
      |sub-list| = ≤32 × |record|;                 G_sub ≤ 2 (current + one reader-held);
      |record| = id + Arc<str> clone + 2 Arc ptrs
                 + vec overhead ≈ 64 B             → ≤32 × 64 B × 2 × 256 ≈ ≤ ~1 MiB
  + G × |global snapshot|                          |entry| = peer (≤64 B) + posture strings
      (≤256 B, truncated at publication) + bundle Arc + sub-list slot ptr
      ≈ ≤ 512 B → G × ≤ 128 KiB
  + InventoryHandles (3 Weak) + phase + flat counters (constant, < 1 KiB)
```

**The generation multiplier G, stated as what the mechanism guarantees, not
as a global constant (Sol r2 minor):** `G = 2 +
max_concurrent_snapshot_readers`. The invariant that keeps readers at 1 in
v1 is made structural: `OpsReadState` exposes snapshot access ONLY as a
scoped `with_snapshot(|s| …)` — the `Arc` cannot escape a view build — and
both v1 adapters are explicitly serialized (the single health worker; the
stdio adapter is single-request by construction). So v1 has G ≤ 3 as a
CONSEQUENCE, not an assumption; any future host with concurrent readers
re-derives G from its own reader cap, which the formula is parameterized
for. **Pin:** a generation-retention test under writer churn (writer swaps
in a loop while a scoped reader renders; assert old generations are freed —
live-generation count never exceeds the formula's G).

Worst-case resident ≈ ~2 MiB at full config and full caps; **every term is
parameterized by a validated cap or byte ceiling** — nothing scales with
traffic or deployment config outside a named ceiling. Transient per-scrape:
one response buffer ≤ the §11.6 response budget + the inventory Vec copy.
**Pin:** an allocation test at maximal validated configuration (256 conns,
8,192 subs, 1,024 max-length channel names) asserting measured resident
bytes within the formula's ceiling.

**Q2 — What is the aggregate ceiling across all views?** Bounded by the eight
`LimitsConfig` caps **plus the new `max_configured_channels` config cap**
(the honest fix to the earlier eight-limits claim, which was false for the
channels view): connections ≤ 256, per-connection subs/convs/pushes ≤ 32,
subscriptions ≤ 8,192, inbox ≤ 4 MiB, channels ≤ 1,024. Every gauge is a
fixed-width atomic or a snapshot bounded by those caps; refusal counters are
a fixed class set (§4.5). The metrics registry read-lock is the one
shared-lock touch — a **structural read** guard never taken by the recording
hot path (`metrics/registry.rs:140-157`); its ceiling is the registered
family count (three in v1).

**Q3 — Which test asserts quiescence, and would it catch THIS diff's cost?**
Answered per property, without over-claiming (Sol finding — the earlier
answer claimed the R7 pin catches a records-mutex scrape; it does not, a
lock acquisition wakes nothing):
- **Wake-safety:** the park-flip quiescence pin (`supervisor_tests.rs:38-59`)
  + the §6 scrape regression. Catches any console read wired through an
  actor command, mailbox, or process-table execution.
- **Hot-lock safety:** NOT caught by the above — caught by the §8
  G-NOHOTLOCK pair (the `OpsReadState` structural boundary + the
  barrier test that scrapes while each hot mutex is held).
- **Write-cost correctness:** NOT caught by quiescence soaks (they exercise
  the idle path) — caught by the §9 one-write-per-transition and
  balanced-teardown tests per gauge, plus the pair-signed bound on the §8
  active-work set. Stated, not papered over.

**Q4 — By-design costs → bound + test + sign-off.** The by-design costs are
enumerated in §8: (a) the transport thread's 10 ms accept-poll — pre-existing,
separately signed, not this design's to re-sign; (b) the §6 R7 active-slice
increment — bound stated, §6 test named, pair sign-off required; (c) the
complete §8 active-work write set (per-delivery depth, request-rate
pending/tombstone/timer/conversation/saturation, push-rate slots) — one
`Relaxed` write per transition, zero at idle, one-write-per-transition +
balanced-teardown tests per gauge, signed in the same sitting as (b); (d) the
snapshot-rebuild set including subscribe/unsubscribe — one CoW allocation per
control-plane event, bounded by entry-size bounds and generation cap (Q1);
(e) any sibling listener, if the pair mandates one — rule-2 from birth. No
by-design cost is left unbounded or unsigned. No silent tradeoffs.

---

## 10. How the original shipped — the observability gap this design closes

*(Campaign standing rule: two honest paragraphs on what the original lacked.)*

liminal shipped its production network observability as **exactly three metric
families** — `liminal_connections_active`, `liminal_publishes_total`,
`liminal_deliveries_total` (`metrics.rs:15-17`; the scout confirmed nothing
else is registered by server init) — behind **one unauthenticated health
listener** serving `/health`, `/ready`, `/metrics` (`endpoint.rs:14-16,184`;
no token, header, or TLS check, `apply`/`endpoint` parse only the request
line). Richer instruments *existed in the code* — `ChannelMetrics`
(message_rate, subscriber_count, queue_depth, delivery_latency) and
`ConversationMetrics` (active, completion, duration, error) — but their
constructors are reachable only from tests; **the running server registers
none of them** (scout: "the extra families are NOT OBSERVABLE in the running
server today"). The control that was missing was a **per-connection cost
attribution**: nothing exported let an operator ask "which connections are
consuming CPU while idle?"

That gap had a five-day price. The aion host-resource incident
(`AION-HOST-RESOURCE-INCIDENT-2026-07-11.md`, closed by park-flip) was a
**four-core busy-spin driven by idle connections** that unconditionally
returned `NativeOutcome::Continue` after their drains — and it was **invisible
to the shipped observability for five days**, because `liminal_connections_active`
counts *connections*, not the *slices* they burn: a parked-looking connection
and a spinning one read identically on the shipped surface. The gate in THIS
design that would have surfaced it on day one is the **§6 R7-class
per-connection slice gauge**: with `slice_serviced` exported per connection
(§4.1), four connections advancing their slice counters at full tilt while
carrying no traffic would have shown as **CPU attributed to idle connections**
the first time anyone scraped — the incident would have been a dashboard line,
not a five-day mystery. That is the concrete reason this gauge is the
centerpiece instrument and not an optional extra.

---

## 11. The MCP surface, concretely — [SEAM-DEP: one-pager §(iii)]

v1 is **read-only as a typed-refusal contract, not an absence** (one-pager
§(iii)): the seam declares the mutation classes it does not serve and refuses
them by name.

### 11.0 The contract module — transport-neutral, defined ONCE

Per Ruling C, the decisions a builder needs are made here, not invented
during coding. The contract lives in one module (`ops-contract` class,
compile-time metadata per §3.1) that BOTH adapters (§11.5) call — no adapter
owns any semantics:

- **Frozen request/result/refusal structs.** `OpsRequest` (resource URI or
  tool name + validated args), `OpsResult` (the §11.1 documents / §11.2 tool
  results, envelope per §3.1), and **`OpsRefusal` — defined ONCE and reused
  everywhere**: `{ class: <one of the four §11.3 names>, message, remedy,
  gate: <doc/version reference, when class = read-only>, migration: <path,
  when serving an old major> }`. Resources, tools, refusal counters, and
  old-major migration responses all carry THIS struct — there is no second
  refusal shape anywhere.
- **Discovery tables.** A resource table (the six URIs + `ops/contract`) and
  a tool table (§11.2), served through MCP's standard list operations from
  compile-time constants — discovery never reads runtime state except
  `phase`-driven mount facts.
- **The mutation-class registry.** An explicit compile-time registry of
  known-refused verb classes — `connection.close`, `channel.publish`,
  `cap.set`, `conversation.terminate`, plus haematite's vacuum/sweep when
  mounted — each with its refusal class (`read-only` or
  `requires-exclusive-store`), gate doc, and remedy. **Classification rule,
  explicit:** a request naming a registry entry gets its typed refusal; a
  request naming a tool in NEITHER the tool table NOR the registry is an
  **MCP protocol-level unknown-tool error, not a refusal** — the taxonomy
  never absorbs typos.
- **Normal-miss results** (§11.2): data-level misses are typed results,
  never refusals, never protocol errors.

JSON shapes below remain at sketch level (field lists); the frozen schema is
this module's structs, versioned per §11.4.

### 11.1 Resources (read-only, one per view)
MCP resources, one per §4 view, each a JSON document sourced entirely from
OpsState/the reused primitives:

- `liminal://ops/contract` — `{ contract, major, additive_rev }` — **read
  FIRST** (§11.4).
- `liminal://ops/connections` — `{ phase, active, admissions_current, admissions_started_total, reserved_in_flight, connections: [{ id, peer, worker_posture, readiness_posture, slice_serviced, subscriptions_active, conversations_active, pending_replies, pending_pushes }] }`
- `liminal://ops/channels` — `{ phase, channels: [{ name, subscriber_count }], publishes_total, deliveries_total }`
- `liminal://ops/subscriptions{?cursor,limit}` — **a standard MCP resource
  TEMPLATE, not a fixed URI** (Sol r2: 8,192 possible entries against a
  1,024-item budget means pagination must be a contract operation, not a
  dangling `next` field; and deferring it past the v1 freeze would make
  completing it a major-version act under §11.4). Shape: `{ phase,
  per_connection: [{ id, subscriptions_active, inbox_bytes_used,
  inbox_bytes_cap, subscriptions: [{ subscription_id, channel, depth,
  overflowed }] }], overflow_flags_total, page: { truncated, total, next }
  }`. **Cursor contract:** opaque base64 of the last-seen `(connection_id,
  subscription_id)` pair; **stable global ordering** = connection id
  ascending, then subscription id ascending; `limit` clamped to the item
  budget. **Stale-cursor semantics:** a cursor whose connection/subscription
  has since closed resumes at the next id in order (well-defined under the
  stable ordering — no error); a syntactically invalid cursor returns a
  normal typed result `{ cursor_invalid: true, items: [] }` (a data-level
  outcome, not a refusal, per §11.3). Transport-neutral: the same
  `OpsRequest` args over stdio and HTTP. **Pin:** full traversal of a
  populated 8,192-entry view — no duplicates, no omissions of entries that
  live through the whole traversal, body/item budgets honored on EVERY page.
- `liminal://ops/conversations` — `{ phase, actors_registered, participants_registered, pending_replies_total, tombstones_total, armed_timers_total }`
- `liminal://ops/caps` — `{ phase, caps: [{ name, limit, worst: { id, occupancy }, over_80pct }], refusals: { unauthenticated, read_only, not_mounted, requires_exclusive_store, auth_failures, cap_exceeded: {..}, backpressure: { reject, defer }, push_reply_expired } }` — **all four taxonomy counters present; `requires_exclusive_store` serves as `0` while the class is reserved (§11.3)** — the schema does not shrink because a counter is quiet.
- `liminal://ops/inventory` — `{ phase, profile, services: [{ service, mode, instance, configured, actual, thread_names, fd_classes }], schedulers: {..} }`

Every document carries `phase` (`starting` | `running` | `draining`, §2.3) and
a `scraped_at` timestamp; the console **never** claims an all-fields atomic
instant (§2.4, scout risk). The response shape also reserves a per-field
`as_of` staleness slot (T3, §5.4): unused by liminal's v1 event-maintained
fields, **mandatory** for any future observer-backed field, whose response
must carry its own as-of alongside the §9 lens line for its per-call cost.
Per §1 T1, none of these shapes encode an HTTP-ism — they are
transport-identical documents.

### 11.2 Tools (read-only in v1)
Tools are the *queryable* face of the same state (a resource is the whole view;
a tool is a parameterized read):

- `ops.get_connection(id)` → the single-connection subset of `connections`.
  An unknown id is a **normal typed tool result** — `{ found: false, id }` —
  NOT a refusal: a connection that closed a second ago is a data-level miss,
  and a consumer switching on refusal classes must never see
  `refused:not-mounted` for it. The four refusal classes are
  access/capability outcomes, never data-existence outcomes (§11.3).
- `ops.get_caps()` → the caps/pressure matrix (same as the resource; offered as
  a tool for agents that poll pressure).
- `ops.get_inventory()` → the inventory view.
- `ops.health()` / `ops.ready()` → thin typed wrappers over the existing
  `/health` and `/ready` (reuse, not duplicate — §2.1).

**No mutating tool ships in v1.** Every mutation verb an agent might try
(`connection.close`, `channel.publish`, `cap.set`, `conversation.terminate`)
is declared as a **known-refused class**, so an attempt returns a typed refusal
naming the class, the version gate, and the remedy (§11.3) — not a silent 404.

### 11.3 The typed-refusal taxonomy (one-pager §(iii), four classes per T2)
**Four** distinct outcomes, **never conflated** (the tear added the fourth):

| Refusal | Meaning | Remedy named in the refusal |
|---|---|---|
| `refused:unauthenticated` | missing/wrong operator bearer token (§7) | "present a valid `[console] auth_token` bearer" |
| `refused:read-only` | a mutation verb that is design-gated out of v1 | "not in v1; mutation class `X` is design-gated on `<doc>`" (v2+) |
| `refused:not-mounted` | the surface/service is not mounted in this process (e.g. a full-only view under worker-front-door, §2.3) | "view `Y` is absent-by-profile / mount the embedding service" |
| `refused:requires-exclusive-store` | an offline-inspector capability asked of a live host (T2: the `vacuum_stats` class takes the A4 writer lock by design; offline tools never mount on a live host's seam) | **names the offline runbook** — "run `<offline tool>` against a stopped store per `<runbook>`" |

**`refused:requires-exclusive-store` is reserved/unused liminal-side in v1 —
stated honestly rather than inventing a liminal use.** The class exists in
haematite's half (Apollo's T2 wall: without it, someone eventually wires
`haem stats` behind an MCP tool on liminal-server and the console acquires
the writer lock against a live database — the storage twin of the busy-spin
resurrection). liminal's own v1 views have no exclusive-store capability to
refuse; liminal carries the class because the taxonomy is seam-wide
(consumers switch on four names, not per-service subsets), and it becomes
live on this host exactly when liminal-server opts in to mounting haematite's
contract (§5.4).

**The invariant (corrected per Sol — the earlier "seam/mount/credential-level
only" wording was wrong for two of its own classes):** refusals are
**contract access/capability outcomes, never row/data-existence outcomes**.
`refused:unauthenticated` and `refused:not-mounted` are access outcomes;
`refused:read-only` is a mutation-**capability** outcome and
`refused:requires-exclusive-store` an execution-mode-**capability** outcome —
none of the four ever answers "does this row exist?". An unknown connection
id, an empty view, or an absent optional value is a **normal typed tool
result/document** (`{ found: false }`, an empty list, a null field) —
distinct from the refusal taxonomy by construction. And per §11.0, an
unregistered verb is an MCP protocol error, not a refusal. Every refusal is
the ONE `OpsRefusal` struct (§11.0), everywhere it appears.

A consumer can build against v1 and know **exactly which wall it hit** — wrong
credential vs design-gated vs not-here vs needs-the-store-offline — which is
the product-honesty property §0 requires. Each refusal also increments its
§4.5 counter, so refusal rates are themselves observable.

### 11.4 Versioning — aligned to the one-pager §Versioning (T5), not restated differently
The seam's version discipline is settled contract text; this section quotes
and applies it rather than paraphrasing it into drift:

> The contract version is a **resource the consumer reads FIRST**. Additive =
> new views/fields. Breaking = any removal, rename, redaction-widening
> reversal, or refusal-class change; a breaking change bumps the major, and
> the old contract's refusals name the migration.
> — one-pager §Versioning (T5) @ 7b768eb

Applied to liminal's half:

- **Version-as-resource, read first:** `liminal://ops/contract` returns
  `{ contract: "liminal-ops", major: 1, additive_rev: N }`; a consumer reads
  it before any view. It is served from compile-time contract metadata
  (§3.1 — not ServerConfig; exact, zero idle-cost).
- **Additive** (no bump): new views, new fields — **exactly the settled
  list, nothing appended** (Ruling B: an earlier revision added "new read
  tools" to this class; the settled text does not authorize it, and under
  the current contract **a new tool is NOT additive** — it takes the
  major-version treatment). A consumer must ignore unknown fields (§11.1
  shapes are open-world within a major). *Open question, recorded for the
  joint page, not enacted here:* a future one-pager amendment could define a
  tools-additive class (a strictly-new read tool arguably cannot break a
  consumer that never calls it) — that is Apollo's-half/joint territory and
  stays an open question until the one-pager says so.
- **Breaking** (major bump): any removal, rename, **redaction-widening
  reversal** (un-exposing something previously widened), or refusal-class
  change — the four refusal names of §11.3 are contract, and changing them is
  a break by the settled definition. Plus, per the ruling above: new tools.
- **Refusals name the migration:** after a major bump, the OLD contract's
  refusals name the migration path — the `OpsRefusal.migration` field
  (§11.0), the same struct as every other refusal — a v1 consumer hitting a
  v2 host is told where to go, not left to a 404.
- **Mutation is a major-version act:** v2 introduces mutating tools behind
  their own design gate (§14); until then `refused:read-only` names v2 as the
  remedy.

This discipline is the part other seats' seams (aion/norn) inherit verbatim
(T6, §14), so it is deliberately the one-pager's text, not this doc's.

### 11.5 The two adapters, the [console] config, and the auth mechanics

**The adapters.** Both are thin hosts of the §11.0 contract module; tests
prove **identical semantic payloads over both** (the T1 transport-identical
pin):

- **stdio adapter** — the contract's other mount (T1): standard MCP JSON-RPC
  2.0 over stdio, stateless per the read-only surface. Auth N/A by
  construction (§7.2(4)). liminal-server does not SHIP a stdio mount in v1
  (it is a process with a listener); the adapter exists in the contract
  crate so the contract is provably transport-neutral and a future local
  inspector binary mounts it unchanged.
- **HTTP adapter, on the existing health worker** — one new route:
  `POST /mcp`, carrying a single MCP JSON-RPC 2.0 message per request,
  response in the body; no SSE/streaming, no sessions in v1 (a read-only
  request/response surface needs neither; adding either later is a design
  act). `GET /health`, `/ready`, `/metrics` are byte-identical to today.
- **The error-vs-refusal mapping, exact:** malformed HTTP or JSON-RPC →
  protocol-level error (HTTP 400 / JSON-RPC parse-invalid-request error),
  never a refusal; unknown tool (neither table nor registry, §11.0) →
  JSON-RPC method-level error; **application refusals (`OpsRefusal`) are
  ALWAYS successful JSON-RPC responses carrying the refusal as the typed
  result** — a refusal is a contract answer, not a transport failure, and it
  must survive any HTTP status-code folklore. HTTP status is 200 for every
  well-formed request that reaches the contract, including refusals; 401 is
  NOT used for `refused:unauthenticated` (the refusal payload is the
  contract; a bare 401 would be an HTTP-ism encoding the auth outcome —
  T1 violation).

**The `[console]` config section, under `deny_unknown_fields`.**
`ServerConfig` is `deny_unknown_fields` (`config/types.rs:9`), so a NEW
optional `console: Option<ConsoleConfig>` field is **additive**: existing
config files (no `[console]` section) deserialize unchanged. Semantics:

- `[console]` **absent** → the MCP surface is NOT mounted. `POST /mcp`
  returns the `refused:not-mounted` `OpsRefusal` (the surface that answers
  refusals is compiled in; the seam is what's unmounted — this is exactly
  the not-mounted class's job). Health routes unchanged.
- `[console]` **present** → `auth_token` is REQUIRED by validation:
  **mounted-without-token = REFUSE STARTUP, fail-loud** (domain-owner call,
  consistent with the refuse-at-birth posture of park-flip §2 — a server
  that would serve operator state without a credential is a misconfiguration
  refused at birth, not warned about). No default-open mode exists for this
  surface, unlike H4's absent-token-open wire posture — different audience,
  different default, stated deliberately.

**Auth mechanics, bounded and exact:** `POST /mcp` (and only it — the
protected-route scope is exactly the MCP route; §7.2's liveness-probe
exemption for `/health`/`/ready`/`/metrics` is deliberate and unchanged)
requires `Authorization: Bearer <token>`. The health parser today reads only
the request line (`endpoint.rs:173-215`); the adapter extends it to parse
headers **for POST /mcp only**, with hard bounds: request head ≤ 8 KiB, body
≤ 64 KiB (both NUMERIC, both enforced before any allocation proportional to
claimed size; over-bound → protocol-level 413/400, not a refusal). Token
comparison is `constant_time_eq` — the same discipline H4 already uses
(`apply.rs:258-317`). Missing/malformed/wrong bearer →
`refused:unauthenticated` `OpsRefusal` (well-formed HTTP, wrong credential —
a contract answer). Token bytes live only in the adapter's auth check, never
in OpsState (§7.3).

**Socket deadlines — absolute, not per-read (Sol r2 finding: the earlier
"at most the existing 2 s read timeout" claim was false for a multi-read
request).** Today's handler does ONE bounded read under the 2 s socket
timeout (`endpoint.rs:136-162`); reading an 8 KiB head + 64 KiB body takes
REPEATED reads, and a per-read socket timeout lets a trickling peer feed one
byte per ~2 s window and occupy the serial worker indefinitely. The response
path is worse: `write_all`/`flush` (`endpoint.rs:162-170`) has NO deadline
at all, so a client that sends a valid request and never reads the ≤ 256 KiB
response blocks the worker unboundedly. Mandated:

- **One monotonic whole-REQUEST deadline: 2 s** from accept to
  fully-parsed request. Every blocking read sets its socket timeout to
  `remaining = deadline − now` (clamped > 0); `remaining ≤ 0` → close, 408.
  Covers header termination (head not CRLF-terminated within budget →
  400/431), `Content-Length` validation (required for `POST /mcp`, must be
  ≤ 64 KiB, mismatch/short body → 400 at deadline), partial reads, and
  early EOF (→ close, no response owed).
- **One monotonic whole-RESPONSE deadline: 2 s** from render-complete to
  last byte written. The response path becomes a bounded-write loop
  (nonblocking or per-write `remaining` timeout — implementation's pick,
  same bound): `remaining ≤ 0` with bytes unsent → drop the connection.
  A response is never retried and never buffered past its deadline.
- Worst-case worker occupancy per request is therefore **request deadline +
  render budget + response deadline ≈ 4.05 s, a hard ceiling** — stated as
  the number h1–h4 pin against, and honestly ~2× today's single-read worst
  case, which is why the pins below are gates, not decoration.

### 11.6 Numeric budgets and the head-of-line pins

Determination (b) promised bounded-response + scrape-work budgets; Sol's
round demanded the numbers and their enforcement. Set here, enforced in the
contract module's render path (transport-neutral), pinned:

| Budget | Number | Enforcement |
|---|---|---|
| Response body | ≤ 256 KiB | render truncates deterministically (below) |
| List items per response | ≤ 1,024 entries | covers connections (≤256) and subscriptions (≤8,192 → paginated) with headroom |
| Scrape work | one snapshot load + ≤ (N_conn + N_sub + N_ch + inventory) atomic loads + one metrics snapshot + render — no loops beyond cap-bounded cardinality | structural: `OpsReadState` exposes nothing unbounded |
| Scrape latency (the head-of-line number) | a maximal scrape completes ≤ 50 ms on the reference config (256 conns, 8,192 subs, 1,024 channels); CI pin uses a generous multiplier for machine variance | the head-of-line pin below |
| Request head / body | 8 KiB / 64 KiB (§11.5) | pre-allocation bound |

**Deterministic truncation/pagination rule:** lists exceeding the item
budget truncate at the stable sort order (connection id, then subscription
id) with `{ truncated: true, total: N, next: <cursor> }`, and the cursor is
REDEEMABLE — the `{?cursor,limit}` resource template (§11.1) is the contract
operation that accepts it; truncation without a follow-up operation is not
pagination. Same rule on both transports; a truncated response is
well-formed and says so, never silently partial. **Byte-budget enforcement
happens AT serialization, not by item count alone (Sol r2):** the renderer
tracks serialized bytes and closes the page at whichever budget trips first
(items or 256 KiB). **Oversized-single-item policy:** an item that alone
exceeds the remaining page (pathological strings should be impossible under
the §9 byte ceilings, but the policy exists anyway) serializes as a
truncation stub `{ id, item_truncated: true }` — the page stays valid JSON
and under budget; a partial item is never emitted.

**Head-of-line pins (on `/health` and `/ready`, the routes that must not
starve):** (h1) with a maximal-config scrape in flight, a following
`/health` request answers within (scrape budget + ε) — pinned against the
50 ms number; (h2) a stalled-then-dead client on `POST /mcp` costs at most
the whole-request deadline; (h3) **byte-trickle:** a client feeding one byte
per read-timeout window is cut at the 2 s absolute request deadline — the
pin proves the deadline is absolute, not per-read; (h4) **never-reads-
response:** a valid MCP client that stops reading its ≤ 256 KiB response is
cut at the 2 s response deadline. Under all four, `/health` and `/ready`
answer within the stated ceiling: **worst-case delay ≤ one in-flight
request's remaining hard ceiling (≈ 4.05 s) + own service time** — the
ceiling the pins enforce and the pair signs. **Stated plainly: if the single
serial worker cannot meet that `/health`/`/ready` ceiling under h1–h4, the
sibling listener (already rule-2-specified in §8) is TAKEN — that is the
decision rule, not a revisit.** Honest accounting: today's worst case was
~2 s (single read, no write deadline but responses were tiny); the MCP
route's larger I/O makes the deadline machinery load-bearing where it used
to be latent.

[SEAM-DEP §(iii)]: synced to the settled view set, four-class taxonomy, and
§Versioning @ 7b768eb; if the contract moves again, §11.0–§11.6 are the
rework surface.

---

## 12. Reuse, not duplication

The design **reuses** three shipped surfaces rather than re-implementing their
facts (scout next-step): `/metrics` (the process-global registry snapshot —
the console's channel/publish/delivery counters ARE these families, not
copies), `/ready` (the `SharedReadinessState` four-flag snapshot — G2
established rides it lock-free), and `/health`. OpsState **adds** only the
facts none of those carry (per-connection detail, occupancy gauges, inventory,
R7). The MCP `ops.health`/`ops.ready` tools (§11.2) are typed wrappers over the
existing routes, so there is one source of truth per fact.

---

## 13. Open risks carried forward (tear settled; pair still to rule)

Stated so the pair sees them, not buried:

- **The literal-no-wake ambiguity (scout risk #1).** Any HTTP/MCP request runs
  *a* server thread; the defensible incident-safe reading is "never wake a
  beamr process/scheduler/mailbox," which this design meets, while the health
  worker's own 10 ms poll is a pre-existing signed cost (§8). If the pair
  demands *literal* zero-OS-thread-wake, only an out-of-process shared-memory/
  file export satisfies it — a different design, flagged not chosen.
- **Head-of-line on the shared worker (scout risk #6).** The serial 2 s
  read-timeout handler means an expensive MCP call can delay `/health`/`/ready`.
  v1 mitigates with a bounded response and scrape-work budget (§8 —
  domain-owner determination (b) confirms this posture); the sibling listener
  (rule-2) remains the fallback **the pair rules on** — that ruling is the
  open item here.
- **The named numbers are proposals until signed.** This revision sets
  concrete numbers where the review demanded them — `max_configured_channels`
  = 1,024 (§9); byte ceilings: channel name ≤ 64 B, console token ≤ 128 B,
  rendered config ≤ 64 KiB (§9); response ≤ 256 KiB / items ≤ 1,024 /
  scrape ≤ 50 ms / head 8 KiB / body 64 KiB (§11.6); request/response socket
  deadlines 2 s + 2 s with the ≈ 4.05 s worker-occupancy ceiling (§11.5);
  snapshot entry-string bounds (§9 Q1) — each
  with its enforcement rule and pin. The NUMBERS are the certifying pair's to
  confirm or move; the STRUCTURE (every budget numeric, enforced, pinned)
  is this design's commitment either way.
- **Cluster peer snapshot staleness (scout risk #11).** Peer names are stale by
  up to the 250 ms membership poll and are mutex-backed; if ever exposed, they
  ride a copy-on-write name snapshot updated by the existing poll thread,
  labelled as status-not-realtime. **Domain-owner determination (c), recorded:
  cluster peer names are OUT of v1, confirmed.** The note stays as the
  standing future-candidate record: any later pull-in is an additive act
  against §11.4 with the CoW-snapshot read primitive already designed here.

---

## 14. Explicitly out of scope (fence confirmed by the tear, T6)

The one-pager's out-of-scope fence was named so the tear could confirm it —
and the tear confirmed it (T6, settled @ 7b768eb). This doc's fence is the
same fence:

- **Mutations of any kind** — v2+, design-gated; v1 refuses them by class
  (§11.2–§11.3); haematite's vacuum/sweep sit in the mutation-class registry
  from birth (settled §out-of-scope). The mutation design doc does not exist
  yet; `refused:read-only` names it as the remedy without pretending it does.
- **The Frame console rewrite** — a consumer of this seam, not this seam
  (settled §out-of-scope, §0). This doc is chrome-light precisely because the
  chrome is disposable and the contract is not.
- **The stack gateway** — a thin MCP *client* re-exporting mounted seams, a
  Frame/console concern (one-pager §(i)). Not designed here.
- **Other services' seams** — haematite's half is now settled contract text
  (the T2 read-mode split, T3 no-writer-lock wording, T4 views — cited in
  §5.4 and §11.3, designed on Apollo's side); aion/norn MCP surfaces are
  their own seats, same shape expected, **and per T6 they inherit the
  §Versioning discipline verbatim, so four seams don't invent four version
  disciplines** (§11.4).
- **Per-channel publish/delivery split and deeper haematite internals** —
  named deferrals (§4.2, §5.4), each a future instrument with its own signed
  idle-cost, never a v1 bolt-on. (Exact subscription depth WAS on this list
  in an earlier revision; Ruling A pulled it into v1 — §4.3 — and the fence
  no longer claims otherwise.)
- **The push-reply deadline fix** — merged to main @ `68379e8` (ledger G7);
  this doc consumes its `PushReplyExpired` surface in the §4.5 counter
  taxonomy and adds nothing to it.

---

## Appendix A — anchor verification note

The scout observed at main `3c3aa10`; this branch descends from `a028711`. The
load-bearing anchors were re-derived on this branch and read, not trusted:
`runtime.rs:28-116` (assembly order, health-listener-before-erasure — §2.3);
`process.rs:120-211` (`handle_slice`, `is_registered` at :130, `Wait` at :211 —
§5.2, §6); `supervisor.rs:671` (`slice_counts: Mutex<HashMap>` `cfg(test)` —
§5.3, §6); `health/endpoint.rs:14-16,122,146,184` (routes, 10 ms poll, 2 s
timeout, request-line-only parse — §7, §8); `channel/registry.rs:89-100`
(`list`→`subscriber_count` — §5.1); `metrics.rs:15-17` (three families — §10);
`config/types.rs:14,203-236` (health addr, eight limits — §4.5). The §4.5
push-reply anchors (`push_to_connection_with_deadline` at `supervisor.rs:295`,
`ServerError::PushReplyExpired` at `error.rs:78`, expiry resolution at
`supervisor.rs:657`) were verified on **origin/main @ `68379e8`** — that
surface is merged to main but not on this design branch's base. The Sol-round
corrections were re-verified on this branch before folding:
`supervisor.rs:111-119` (`with_services_and_auth` takes the erased
`Arc<dyn ConnectionServices>`), `supervisor.rs:461-499` (`SupervisorInner::new`
constructs the connection scheduler inside, after erasure),
`supervisor.rs:152-156` (`scheduler()` accessor), `state.rs:47-52` +
`apply.rs:383-395` (lazy inbox budget, created on first subscribe),
`config/types.rs:9,17-18` (`deny_unknown_fields`; `channels: Vec<ChannelDef>`
unbounded). The Sol round-2 anchors were likewise re-verified before folding:
`supervisor.rs:65-79,111-119` (only `from_config_via` threads `config.limits`;
the production constructor defaults them), `apply.rs:349-447`
(subscribe/unsubscribe processed on the connection slice; occupancy cap
bounds map length, not transition rate), `process.rs:377-398` (automatic
overflow shed, no Unsubscribe frame), `subscription.rs:127,179-187,277-385`
(`InboxInstall` seam; embedded sticky `overflowed: AtomicBool` with repeated
`store(true)` refusal sites; admit/pop/close lock scopes),
`pending_reply.rs:243-267` (expiry converts Pending→Tombstone, cardinality
unchanged), `endpoint.rs:136-170` (single bounded read; `write_all`/`flush`
with no write deadline), `config/validation.rs:138-158` (channel-name
validation checks empty/duplicate only). Anchors cited
but not individually re-opened (taken from the scout at its stated lines, and
flagged as such) are the deeper conversation/subscription/durability internals
in §4.3–§4.4 and §5.4–§5.6; a builder implementing those views re-verifies each
at build time, because a design-doc anchor is a starting coordinate, not a pin.
