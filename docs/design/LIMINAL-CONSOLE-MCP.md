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

### 2.2 Composition — three ingredients, one object

`OpsState` is an `Arc<OpsState>` holding three kinds of member, and **nothing
that reads through a scheduler or a hot lock**:

1. **Immutable redacted config.** A cloned, redacted snapshot of the
   validated `ServerConfig`, frozen at construction: profile, all eight
   limits (`config/types.rs:203-236`), configured channel names, persistence
   mode/flag, `auth_enabled` (boolean only), listen and health addresses,
   `cluster_configured`. Redaction is applied at clone time (§7.3): no auth
   token bytes, no cluster cookie, no raw fds, no filesystem paths beyond what
   config already names publicly. Immutable ⇒ read is a pointer deref, zero
   lock, exact, zero idle cost.

2. **Lifecycle-maintained atomics.** A flat block of `AtomicU64`/`AtomicUsize`
   counters and gauges, each **written only at an existing lifecycle event**
   the writer already executes (accept/auth/register/readiness/close, publish,
   subscribe/unsubscribe, conversation register/deregister, timer arm/cancel).
   Reads are `Relaxed`/`Acquire` loads — lock-free, bounded-stale by one event,
   one cache line each. These carry the occupancy gauges the process-owned
   maps cannot safely expose (§4).

3. **Arc-swapped copy-on-write snapshots.** For per-connection *detail* (peer,
   worker posture, readiness posture) that is too structured for a scalar
   atomic, OpsState holds an `ArcSwap<ConnectionsSnapshot>` (and siblings).
   The snapshot is a plain immutable `Vec`/map, rebuilt and swapped **on
   infrequent per-connection lifecycle events only** — accept, auth, worker
   register, readiness register, close — never on the slice hot path. A reader
   does one atomic load of the current `Arc` and walks an immutable structure;
   a writer pays a copy-on-write allocation at an event it already handles.
   This is the scout's explicit recommendation ("publish an Arc-swapped/
   copy-on-write snapshot on infrequent events … rather than reading
   ConnectionRuntime.records").

All three read primitives — plus the two the console reuses rather than
duplicates (the /metrics registry snapshot and `SharedReadinessState`) — are
the **only** read primitives the console is permitted (one-pager §(iii): "all
of it rides three read primitives and nothing else"). No fourth primitive is
introduced; §5 is the fence that keeps it so.

### 2.3 Where OpsState is assembled — the ordering constraint

This is load-bearing and was verified by reading `runtime.rs` on this branch.
The health listener is started **early and populated late**:

```
runtime.rs:28   config = load_config(...)                      // config known
runtime.rs:33   metrics::init()                                // registry live
runtime.rs:35   readiness = SharedReadinessState::new(...)     // shared handle
runtime.rs:36   start_health_server(addr, readiness.clone())   // SERVING begins
runtime.rs:56   match profile { Full => {
runtime.rs:58       services = Arc::new(LiminalConnectionServices::from_config)  // concrete
runtime.rs:61       ConnectionSupervisor::with_services_and_auth(services, ..)   // ERASURE
runtime.rs:67       readiness.set_cluster_configured(...)       // late population
runtime.rs:86-87   readiness.set_config_loaded/​set_listener_bound(true)
```

`SharedReadinessState` is the existing proof that the health worker can hold a
shared handle from line 36 and have lifecycle events mutate it afterward
(`set_config_loaded` at :86 fires long after the worker is serving). **OpsState
rides the identical seam:**

- **Construct the immutable-config shell + zeroed atomics right after
  `load_config` (≈ line 28–34)**, so its config half is exact from the first
  scrape. Hand `ops.clone()` to `start_health_server` at line 36 (its
  signature grows one `Arc<OpsState>` parameter — the only runtime change to
  the listener's start path).

- **Capture the safe scheduler handles BEFORE the erasure at line 58/61.** The
  concrete `LiminalConnectionServices` still exposes the channel cluster,
  conversation supervisor, and store *before* it is coerced into
  `Arc<dyn ConnectionServices>` (scout: `services.rs:397-419`, "concrete
  full-service access … before trait erasure"; the coercion happens as
  `services` moves into `with_services_and_auth`). After erasure those handles
  are gone and bolting actor queries onto the trait object would risk
  wake-unsafe implementations (scout risk). So the connection scheduler handle
  (always), and the channel/conversation scheduler handles (full profile
  only), are moved into OpsState's inventory slot at this point. The task's
  phrase "assembled BEFORE the Arc<dyn ConnectionServices> type erasure" is
  exactly this capture.

- **The boot-window is honest, not hidden.** Between line 36 (worker serving)
  and full assembly, the atomic gauges read zero and the connection snapshot
  is empty — precisely as `/ready` reads 503 until :86. A scrape during boot
  therefore returns `profile` and `limits` (exact, config-derived) with gauges
  marked `starting` (§4 staleness column, §11 JSON `phase` field), never a
  false "0 connections, healthy". This mirrors the readiness state machine and
  is stated so no consumer reads a boot zero as steady state.

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
consistency is per-atomic, not transactional"). It never retains a handle
beyond `run()`'s ownership (ClusterHandle/membership reachability is captured
by reference into OpsState without extending its lifetime — scout next-step).

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
implied). No primitive outside this table is admissible; introducing one is a
§5 violation.

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
| `admissions_total` | new OpsState atomic (promotes `admissions` AtomicU64, `supervisor.rs:654`, which today has no accessor) | admission CAS | `atomic` | lock-free | bounded-stale | 1 | none | 0 |
| `reserved_in_flight` | derived `admissions − active` | — | `atomic`+`metrics` | lock-free | bounded-stale | 1 | none | 0 |
| `peer` (per conn) | `ConnectionsSnapshot` arcswap | accept/auth/close | `arcswap` | lock-free | last conn event | ≤ `max_connections` (256) | peer addr shown; **fd redacted** | 0 |
| `worker_posture` (per conn) | `ConnectionsSnapshot` | worker register | `arcswap` | lock-free | last conn event | ≤ 256 | registration summarized, no raw fd | 0 |
| `readiness_posture` (per conn) | `ConnectionsSnapshot` | readiness register/dereg | `arcswap` | lock-free | last conn event | ≤ 256 | token **never** shown | 0 |
| `slice_serviced` (per conn) | R7 AtomicU64 (§6) | serviced slice | `atomic` | lock-free | bounded-stale | ≤ 256 | none | 0 (parked ⇒ no write) |

**Never** the `records` mutex (§5.2). Per-connection detail rides the arcswap
snapshot, rebuilt only on accept/auth/worker-register/readiness/close — never
on a slice.

### 4.2 channels

| Field | Owner | Update event | Read-prim | Lock | Staleness | Cardinality | Redaction | Idle-cost |
|---|---|---|---|---|---|---|---|---|
| `name`, `configured` metadata | immutable config channel list (`config/types.rs`) | — (frozen) | `config` | none | exact | # configured channels | none | 0 |
| `subscriber_count` (per channel) | new lifecycle-maintained atomic per configured channel | subscribe/unsubscribe | `atomic` | lock-free | bounded-stale | # configured channels (pre-registered) | none | 0 |
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
| `inbox_bytes_used` (per conn) | new OpsState atomic mirroring `ConnectionInboxBudget.used` (`subscription.rs:47-118`, today `cfg(test)`, host-unreachable) | admit/drain | `atomic` | lock-free | bounded-stale | ≤ 256 | none | 0 |
| `inbox_bytes_cap` | immutable `max_connection_inbox_bytes` (4 MiB) | — | `config` | none | exact | 1 | none | 0 |
| `overflow_flags_total` | new atomic counter, incremented when a subscription sets its sticky `overflowed` (`subscription.rs:349-396`) | overflow set | `atomic` | lock-free | bounded-stale | 1 | none | 0 |
| `subscriptions_active` (per conn) | new lifecycle atomic (process-owned map length is host-unreachable) | subscribe/unsubscribe | `atomic` | lock-free | bounded-stale | ≤ 256 | none | 0 |

**Never** the inbox queue lock: `has_pending`/`len` share the same mutex the
admit/pop delivery hot paths take (`subscription.rs`, §5). Depth is **not**
exposed exactly in v1 — only the event-maintained byte-budget occupancy and
overflow counter, which are the safe instruments (scout: depth is
contention-unsafe; byte budget is safe once promoted to a host-reachable
atomic). Exact per-subscription depth is deferred with its reason stated.

### 4.4 conversations — [SEAM-DEP: F-0c §R1 boundary, §4.7 below]

| Field | Owner | Update event | Read-prim | Lock | Staleness | Cardinality | Redaction | Idle-cost |
|---|---|---|---|---|---|---|---|---|
| `actors_registered` | new lifecycle atomic (mirrors D4 `registered_actor_count`, `conversation/actor.rs:124-150`, whose registry is `Mutex<HashMap>`) | actor register/dereg | `atomic` | lock-free | bounded-stale | 1 | none | 0 |
| `participants_registered` | new lifecycle atomic (mirrors `registered_participant_count`) | participant register/dereg | `atomic` | lock-free | bounded-stale | 1 | none | 0 |
| `pending_replies_total` | new OpsState atomic (table is process-owned, `pending_reply.rs`; `len` is `cfg(test)`) | admit/complete/expire | `atomic` | lock-free | bounded-stale | 1 | none | 0 |
| `tombstones_total` | new OpsState atomic | tombstone insert/reap | `atomic` | lock-free | bounded-stale | 1 | none | 0 |
| `armed_timers` | new OpsState atomic (park-flip §5 deadline timers) | timer arm/cancel | `atomic` | lock-free | bounded-stale | 1 | none | 0 |

**Never** the conversation registry mutex (`count` locks the maps actor
dispatch/lifecycle share, §5.5), **never** `ConversationActor::state()` (an
actor command → wake-unsafe, §5.6). All five are event-maintained host atomics
updated at the same lifecycle transitions the actors already execute. The
`armed_timers` gauge is the direct observability of the park-flip §5 deadline
machinery — an idle parked connection holds ZERO timers by that design, so this
gauge reads 0 at idle and is itself the proof the deadline timers are
active-work-only.

### 4.5 caps/pressure

The §5 `LimitsConfig` matrix (`config/types.rs:203-236`) rendered as
**config-vs-occupancy pairs** — the cap from `config`, the occupancy from the
atomic the relevant view already maintains:

| Cap (config) | Default | Occupancy source | Read-prim |
|---|---|---|---|
| `max_connections` | 256 | connections.`active` | `metrics` |
| `max_subscriptions_per_connection` | 32 | subscriptions.`subscriptions_active` | `atomic` |
| `max_conversations_per_connection` | 32 | conversations gauges | `atomic` |
| `max_pending_pushes_per_connection` | 32 | new push-slot atomic (map is shared hot mutex, `cfg(test)` accessor only — §5) | `atomic` |
| `max_pending_conversation_replies_per_connection` | 32 | conversations.`pending_replies_total` | `atomic` |
| `max_pending_replies_per_conversation` | 8 | per-conversation saturation counter (event-maintained) | `atomic` |
| `max_connection_inbox_bytes` | 4 MiB | subscriptions.`inbox_bytes_used` | `atomic` |
| `max_subscription_inbox_depth` | 256 | overflow counter (depth itself contention-unsafe) | `atomic` |

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
with a **per-connection `AtomicU64`, `Relaxed` ordering**, living on the
connection record (or its OpsState mirror). The active slice path bumps it with
one `fetch_add(1, Relaxed)` — **one cache-line write per serviced slice, zero
writes while parked** (a parked connection returns `NativeOutcome::Wait` and
runs no slice, `process.rs:211`). Reads are `Relaxed` loads, lock-free.

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
actor/control/mailbox API is invoked on any scrape path (scout next-step: "park
a connection, record R7, call every console endpoint repeatedly, and assert no
slice advance; separately assert no actor command/control queue/mailbox APIs
are invoked").

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
- the per-channel subscriber atomics, per-connection inbox-byte atomics,
  conversation gauges, and arcswap rebuilds — **all written only at existing
  lifecycle events**, so each is zero-idle-cost by construction. No background
  refresh thread exists (§2.4); if one were ever added it would be a rule-2
  item from birth.

**The sibling-listener fallback is a rule-2 item from birth (§1).** If the pair
rules the shared serial handler's head-of-line risk (a client can occupy the
sole worker for its 2 s read timeout, degrading `/health` and `/ready` —
`endpoint.rs:146`, scout risk) disqualifies sharing, the sibling listener adds
a resident thread whose idle cost (its own accept-poll cadence) must be bounded,
pinned, and signed. v1 avoids this by reusing the existing worker and by
**addressing the serial 2 s head-of-line behavior before adding expensive
endpoints** (scout next-step) — e.g. a bounded response size and a scrape-work
budget so an MCP call cannot starve liveness. **Domain-owner determination
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
pull-only (a scrape arrives or it doesn't). The one non-zero steady cost is the
§6 R7 increment, which fires **only on a serviced slice** — of which a parked
connection runs none. **Ceiling:** the memory ceiling is OpsState's resident
size — immutable config (constant) + atomics bounded by `max_connections` × a
handful of `AtomicU64` (256 × O(10) × 8 B ≈ single-digit KiB) + the current
`ConnectionsSnapshot` arc (≤ 256 entries). No unbounded growth: cardinality is
capped by the §5 limits.

**Q2 — What is the aggregate ceiling across all views?** Bounded by the eight
`LimitsConfig` caps (§4.5): connections ≤ 256, per-connection subs/convs/pushes
≤ 32, inbox ≤ 4 MiB. Every gauge is a fixed-width atomic or a snapshot bounded
by those caps; the refusal counters are a fixed small set of classes (§4.5). No
view has cardinality that grows with traffic rather than with the capped live
population. The metrics registry read-lock is the one shared-lock touch, and it
is a **structural read** guard never taken by the recording hot path
(`metrics/registry.rs:140-157`) — its ceiling is the registered family count
(three in v1).

**Q3 — Which test asserts quiescence, and would it catch THIS diff's cost?**
The park-flip quiescence pin (`supervisor_tests.rs:38-59`: parked slice count
flat across a soak) plus the §6 sibling regression (a scrape leaves the parked
counter unchanged). **Would it catch this diff's cost?** Yes for the wake class:
if any console read were wired through an actor command or the `records` mutex,
the parked-counter-unchanged assertion (with the no-actor-API boundary check)
fails. It would **not** catch a mis-signed *active*-slice cost (the R7
increment on a busy connection) — that is why the increment is a rule-2 number
signed at the pair's desk (§8), not left to a quiescence soak that only
exercises the idle path. This gap is stated, not papered over.

**Q4 — By-design costs → bound + test + sign-off.** The by-design costs are
enumerated in §8: (a) the transport thread's 10 ms accept-poll — pre-existing,
separately signed, not this design's to re-sign; (b) the §6 R7 active-slice
increment — bound stated, §6 test named, pair sign-off required; (c) any
sibling listener, if the pair mandates one — rule-2 from birth. No by-design
cost is left unbounded or unsigned. No silent tradeoffs.

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
them by name. JSON shapes are at sketch level (field lists, not a frozen
schema); the frozen schema is a §11.4 versioning act.

### 11.1 Resources (read-only, one per view)
MCP resources, one per §4 view, each a JSON document sourced entirely from
OpsState/the reused primitives:

- `liminal://ops/connections` — `{ phase, active, admissions_total, reserved_in_flight, connections: [{ id, peer, worker_posture, readiness_posture, slice_serviced }] }`
- `liminal://ops/channels` — `{ phase, channels: [{ name, subscriber_count }], publishes_total, deliveries_total }`
- `liminal://ops/subscriptions` — `{ phase, per_connection: [{ id, subscriptions_active, inbox_bytes_used, inbox_bytes_cap }], overflow_flags_total }`
- `liminal://ops/conversations` — `{ phase, actors_registered, participants_registered, pending_replies_total, tombstones_total, armed_timers }`
- `liminal://ops/caps` — `{ phase, caps: [{ name, limit, occupancy }], refusals: { unauthenticated, read_only, not_mounted, auth_failures, cap_exceeded: {..}, backpressure: { reject, defer }, push_reply_expired } }`
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

- `ops.get_connection(id)` → the single-connection subset of `connections`, or
  `refused:not-mounted`-class `not_found` if the id is unknown.
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
  it before any view. It is itself served from immutable OpsState config
  (read-prim `config`, exact, zero idle-cost).
- **Additive** (no bump): new views, new fields, new read tools. A consumer
  must ignore unknown fields (§11.1 shapes are open-world within a major).
- **Breaking** (major bump): any removal, rename, **redaction-widening
  reversal** (un-exposing something previously widened), or refusal-class
  change — the four refusal names of §11.3 are contract, and changing them is
  a break by the settled definition.
- **Refusals name the migration:** after a major bump, the OLD contract's
  refusals name the migration path — a v1 consumer hitting a v2 host is told
  where to go, not left to a 404.
- **Mutation is a major-version act:** v2 introduces mutating tools behind
  their own design gate (§14); until then `refused:read-only` names v2 as the
  remedy.

This discipline is the part other seats' seams (aion/norn) inherit verbatim
(T6, §14), so it is deliberately the one-pager's text, not this doc's.

[SEAM-DEP §(iii)]: synced to the settled view set, four-class taxonomy, and
§Versioning @ 7b768eb; if the contract moves again, §11.1–§11.4 are the
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
- **Per-channel publish/delivery split, exact subscription depth, deeper
  haematite internals** — named deferrals (§4.2, §4.3, §5.4), each a future
  instrument with its own signed idle-cost, never a v1 bolt-on.
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
surface is merged to main but not on this design branch's base. Anchors cited
but not individually re-opened (taken from the scout at its stated lines, and
flagged as such) are the deeper conversation/subscription/durability internals
in §4.3–§4.4 and §5.4–§5.6; a builder implementing those views re-verifies each
at build time, because a design-doc anchor is a starting coordinate, not a pin.
