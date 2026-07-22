# W4 — LAW-1 polling retirement (server-internal readiness/notification wave)

**Revision r2 — design-first brief of record, 2026-07-22 (lens r1 fold: 3 MAJOR + 5 minor + coordinator finding)**

This brief rules the first buildable wave of the LAW-1 polling-retirement
program named as lane **W4** in the wiring ledger. It is a docs-only lane: it
specifies the build, its scope partition, its replacement designs, and its
acceptance oracles; it does not claim any replacement is implemented. It
re-pins the design/scoping skeleton `docs/design/LAW1-POLLING-RETIREMENT.md`
(codebase pin `ce8814d`, now stale) to current main and re-derives every
codebase-state sentence at the new pin under LAW-2 — nothing is inherited.

## 0. Authority, pin, and binding lane law

The byte pin for every ground fact in this brief is liminal **`829b3c3`**
(`829b3c30a9f27bab8aa31cbe21470e687c59937d`), verified `git status --porcelain`
clean at authoring. At that commit the wiring ledger identifies itself as
**r1.9, 2026-07-20** (`docs/design/WIRING-LEDGER.md:1-6`), and its two binding
rules are:

1. **Wire-with-oracle:** a lane is complete only with a production caller and a
   named behaviour oracle (`docs/design/WIRING-LEDGER.md:16-21`).
2. **No row, no dormancy:** a dormant seam requires a ledger row carrying its
   named consumer, trigger, owner, and oracle floor
   (`docs/design/WIRING-LEDGER.md:22-25`).

### 0.1 Board-binding authority — Tom's NO-POLLING ruling

Quoted verbatim from the LAW-1 skeleton header
(`docs/design/LAW1-POLLING-RETIREMENT.md:9-12`):

> **Board-binding authority.** Tom's NO-POLLING ruling is the design constant:
> "real-time is a design constant. No application-layer poll loops anywhere in
> the product... if a design has a timer whose job is 'check whether something
> changed,' it's wrong — redesign it to be TOLD."

### 0.2 LAW-1 and LAW-2 (verbatim from the skeleton)

`docs/design/LAW1-POLLING-RETIREMENT.md:14-22`:

> **LAW-1 — NO-POLLING.** Participant-contract draft R11
> (`origin/design/participant-contract` at `1e6aa99`,
> `docs/design/PARTICIPANT-CONTRACT.md`) makes these mechanisms non-conforming
> when their job is to discover changed state: timer, poll, sweep, scan,
> heartbeat, listener backoff, periodic reap, read-timeout wake, stop-flag
> sampling, and a synthetic write or probe. LAW-1 binds every replacement shape,
> every acceptance frame, every dependency choice, and the growth/sweep sections
> below. A one-shot admitted domain deadline may win an event race; it may not be
> turned into periodic sampling.

`docs/design/LAW1-POLLING-RETIREMENT.md:24-30`:

> **LAW-2 — BELIEVED STATE IS NOT CITABLE STATE.** Every codebase-state sentence
> in this document is backed by a file:line range opened and re-verified at
> `ce8814d`, or by a grep-able genuinely-open socket. An unresolved dependency
> that is neither is not evidence. LAW-2 binds this whole document, including
> candidate mechanisms and future rows.

This brief re-binds LAW-2 to pin `829b3c3`: every file:line below was opened or
grep-verified at `829b3c3`, and every drift from the skeleton's `ce8814d`
anchors is recorded in §1 rather than carried silently.

### 0.3 The house idle-cost lens (per replacement, mandatory)

Quoted from `docs/design/LAW1-POLLING-RETIREMENT.md:32-38`. Each replacement
SHALL answer: **(1)** idle cost and a pinned ceiling; **(2)** the aggregate
ceiling across instances; **(3)** the test that asserts quiescence; and **(4)**
every by-design idle cost's bound, pinning test, and certifying-pair sign-off.
"Event-driven" without those four answers is not a readiness claim.

### 0.4 The controlling ledger row (quoted byte-for-byte)

Source: `docs/design/WIRING-LEDGER.md:212-219`.

> ### W4 — LAW-1 polling retirement
> - **What sits open:** the polling seams LAW-1 retires, board item since
>   Hermes's catch (see `docs/design/LAW1-POLLING-RETIREMENT.md`).
> - **Named consumer:** the event-driven replacements the LAW-1 design names.
> - **Trigger:** next liminal maintenance window after the wiring lanes W1/W2
>   open (sequencing at Hermes's seat).
> - **Oracle floor:** per LAW-1 doc — absence proofs (no polling observed under
>   the doc's named workloads), not just presence of the new path.

The oracle floor is therefore **ABSENCE PROOFS** — no polling observed under
the doc's named workloads — not merely the presence of the new event path. §5
is built to that floor: every replacement family carries a structural-grep
absence oracle **and** a runtime quiescence counter, plus an idle-honesty
both-sides fixture that must not pass by hiding a timer.

## 1. Re-pin — drift ledger since `ce8814d`

LAW-2 forbids inheriting the skeleton's anchors. Main has advanced across the
landed W1b fate machinery and the W2 obligation-debt dispatch arm. Every
citation this brief carries was re-verified at `829b3c3`; the drift is:

| skeleton anchor (`ce8814d`) | state at `829b3c3` | disposition |
|---|---|---|
| Unconditional `notify_ready` after every successful semantic return | **Retired by W2.** Notification is now impact-driven: `InstalledParticipantService::notify_impact` iterates `impact.target_union()` at `crates/liminal-server/src/server/participant/dispatch.rs:582-588`. The `notify_impact` calls at `dispatch.rs:625,676` are **unconditional**; the gating lives inside `target_union` itself — `DispatchImpact::Unchanged` returns an empty `BTreeSet` at `crates/liminal-server/src/server/participant/dispatch_impact.rs:97-104` (empty-set arm at `:99`), so an `Unchanged` impact fires no READY. A grep for `notify_ready` in that module returns nothing. | Reuse this TOLD vocabulary; do not reintroduce an unconditional tell. |
| Main listener loop `listener.rs:125-155` | Loop body `crates/liminal-server/src/server/listener.rs:130-155`; constants unchanged at `:11-12`; nonblocking at `:100`; reap+accept+`WouldBlock` sleep at `:131-142`; transient 50 ms sleep at `:147`. | Re-pinned. W4-NOW leg 1. |
| — (absent at `ce8814d`) | **NEW family:** the sibling WebSocket accept worker `crates/liminal-server/src/server/connection/websocket/listener.rs`, landed with the W2 WebSocket transport. Constants `:26-28`, nonblocking `:67`, `accept_loop` reap+accept+`WouldBlock` 10 ms sleep + 50 ms transient sleep at `:150-177`. It self-documents "Mirrors the main TCP [`ServerListener`]…ownership shape exactly" at the module doc `:3-4`, and its constants are commented "matching the main TCP listener's accept loop" at `:25`. | New at `829b3c3`. W4-NOW leg 1 (same shape, same wave). |
| Health serve loop `endpoint.rs:104-133` | `crates/liminal-server/src/health/endpoint.rs:104-134`; startup nonblocking+spawn at `:83-101`; `WouldBlock` 10 ms sleep at `:121-122`. | Re-pinned. W4-NOW leg 2. |
| Shutdown drain loop `shutdown.rs:185-226`; settle `:229-245`; constants `:14-16` | Constants unchanged at `crates/liminal-server/src/server/shutdown.rs:14-16`; `drain_connections` reap/count/sleep loop drifted to `:196-238` (sleep `:236`); `wait_after_force_close` drifted to `:240-256` (sleep `:255`). `run_shutdown_sequence` now also takes an optional `WebSocketListener` and stop-accepts both transports at `:169-194`. A TOLD `Condvar`-based `ShutdownHandle::wait` already exists at `:49-62,86-107`. | Re-pinned. W4-NOW leg 3; the existing `Condvar` is a reuse candidate for the drain-completion primitive. |
| Cluster membership `membership.rs:40-44,112-128,198-223,225-230` | `POLL_INTERVAL` at `crates/liminal-server/src/cluster/membership.rs:44`; `poll_once` at `:115`; `PollLoop` field at `:149`; `run_poll_loop` reap/sleep at `:225-228`; `start` at `:362`. | Re-pinned. **Scoped out** (§3): external beamr `«MEMBERSHIP-EVENT-SOURCE»`. |
| Channel command-reply `wait.rs:18-24,73-95` | `LIVENESS_POLL` 10 ms at `crates/liminal/src/channel/actor/wait.rs:24`; `poll_reply` `loop { recv_timeout(LIVENESS_POLL); on timeout → process_table().get(pid) + deadline }` at `:76-96`. | Re-pinned. **Scoped out** (§3): external beamr `«CHANNEL-REPLY-EVENT-RACE»`. |
| SDK push reader `push_client.rs:49-52,378-385,459-510,543-579` | `READER_POLL_TIMEOUT` 100 ms at `crates/liminal-sdk/src/remote/tcp/push_client.rs:52`; installed `:382`; timeout→`Ok(None)` at `:510`; `FillOutcome::TimedOut` classifier `:544-574`. | Re-pinned. **Scoped out** (§3): shared SDK-reader lane. |
| SDK subscription reader `subscription.rs:43-49,218-241,316-387,419-456` | `READER_POLL_TIMEOUT` 100 ms at `crates/liminal-sdk/src/remote/tcp/subscription.rs:47`; installed `:230`; timeout→`Ok(None)` at `:382`; classifier `:404-451`. | Re-pinned. **Scoped out** (§3): shared SDK-reader lane. |
| §C `PushReplyAwaiter` re-arm `supervisor.rs:533-636` | The push-reply quantum now lives at `crates/liminal-server/src/server/connection/supervisor.rs:745,790,844` (`recv_timeout`/`try_recv`). | Re-pinned. **Growth candidate** (§2.2). |
| §1.5 readiness precedent | The shared readiness reactor is still present: its one-thread inventory is pinned in `crates/liminal-server/src/server/connection/supervisor_tests.rs` (thread-name assertion) and the parked-connection quiescence oracle is `crates/liminal-server/src/server/participant/publication.rs:408` (`parked_connection_wakes_on_outbox_and_no_polling_occurs`). | Re-pinned as the reuse precedent for leg 1. Exact wake-rate pin is a build obligation (§4.1). |
| W2 source-absence exemplar | `dispatch_source_has_no_timer_sweep_or_periodic_probe` lives in `crates/liminal-server/src/server/participant/production/tests_w2_leg1_census.rs`; the census forbid-list `["sleep(", "interval(", "timer(", "sweep(", "register("]` is at `:106`. | The exemplar this brief's §5 absence oracles follow. |

## 2. Family census (mechanical sweep at `829b3c3`)

The sweep ran the skeleton's §D pattern set (`sleep(`, poll constants,
`recv_timeout`/`try_recv`, poll-words, `WouldBlock`/`TimedOut`/`set_read_timeout`,
wait/deadline primitives, `interval`/`tick`/`timeout` calls, and the
underscore-tolerant identifier grep) over `crates sdks`. Root enumeration:
`git ls-files '*.rs' | grep -v '^crates/'` returns only three tracked wasm-src
Rust paths under `sdks/liminal-ts/wasm-src/`; no non-`crates/` product-Rust
family exists outside the SDK wasm codec. Every confirmed family below carries a
byte-verified loop; every candidate carries grep evidence plus the D.2 pairing
question it still owes.

**Class exclusions stated explicitly (skeleton §D.1: silence about a class is a
defect).** The `crates/liminal-sdk/examples/` tree is excluded from the product
claim: the only `sleep` there is `crates/liminal-sdk/examples/demo_feed_publisher/main.rs:90`
(`thread::sleep(TICK_INTERVAL)`), which is demo publish-pacing in an example
binary, not shipped product. The `#[cfg(test)]` sleeps surfaced by the sweep
(e.g. `listener.rs:284`, `execute.rs:407,431,452`, the `*_tests.rs` files) are
test scaffolding, not production loops. The TypeScript/Gleam SDK roots remain
under the skeleton's own §C/§D closure and are not re-swept here.

### 2.1 Confirmed polling families (byte-verified loop at `829b3c3`)

| # | family | evidence (loop + cadence) | scope (§3) |
|---:|---|---|---|
| F1 | Main TCP listener accept loop | `listener.rs:130-155`: `while !shutdown.load` reaps, `accept()`, sleeps `ACCEPT_IDLE_BACKOFF=10ms` on `WouldBlock` (`:141-142`), `TRANSIENT_ERROR_BACKOFF=50ms` on EMFILE/ENFILE (`:147`); constants `:11-12`; nonblocking `:100`. ~100 accept attempts/s idle. | **W4-NOW leg 1** |
| F2 | WebSocket server listener accept loop **(new at `829b3c3`)** | `websocket/listener.rs:150-177`: same shape — `handshakes.reap_finished()` (`:156`), `accept()`, `WouldBlock` 10 ms sleep (`:161-162`), transient 50 ms sleep (`:165-167`); constants `:26-28`; nonblocking `:67`. | **W4-NOW leg 1** |
| F3 | Health accept loop | `health/endpoint.rs:104-134`: `while !shutdown.load`, `accept()`, `WouldBlock` 10 ms sleep (`:121-122`); startup nonblocking+spawn `:83-101`. ~100 accept attempts/s idle. | **W4-NOW leg 2** |
| F4 | Shutdown drain loop | `shutdown.rs:196-238`: `loop` reaps (`:203`), counts active (`:211`), samples deadline, `thread::sleep(remaining.min(FORCE_CLOSE_POLL_INTERVAL=10ms))` (`:236`); constants `:14-16`. ~100 reap/count wakes/s while draining. | **W4-NOW leg 3** |
| F5 | Force-close settle loop | `shutdown.rs:240-256`: `while Instant::now() < deadline` reaps (`:243`), counts (`:244`), `thread::sleep(FORCE_CLOSE_POLL_INTERVAL)` (`:255`). Up to ~50 settle waits over the 500 ms window. | **W4-NOW leg 3** |
| F6 | Cluster membership `PollLoop` | `membership.rs:225-228`: `run_poll_loop` `while !stop.load` applies `poll_once()` delta then `sleep(POLL_INTERVAL=250ms)`; `poll_once` snapshot/diff `:115`; constant `:44`. 4 snapshot/diff wakes/s idle. | **ledgered-elsewhere** (external beamr ask) |
| F7 | Channel command-reply liveness | `channel/actor/wait.rs:76-96`: `poll_reply` `loop { recv_timeout(LIVENESS_POLL=10ms); on Timeout → process_table().get(pid) + deadline check }`; constant `:24`; 5 s `COMMAND_TIMEOUT` `:19`. ~100 process-table lookups/s per outstanding command. | **ledgered-elsewhere** (external beamr ask) |
| F8 | SDK TCP push reader | `push_client.rs:459-510`: `run_reader` installs `READER_POLL_TIMEOUT=100ms` read timeout (`:382`), converts timeout→`Ok(None)` (`:510`) to re-check the stop flag; constant `:52`; classifier `:544-574`. ~10 read attempts/s on a silent socket. | **ledgered-elsewhere** (shared SDK-reader lane) |
| F9 | SDK TCP subscription reader | `subscription.rs:316-387`: same 100 ms read-timeout/stop-flag shape; installed `:230`, timeout→`Ok(None)` `:382`, constant `:47`, classifier `:404-451`. | **ledgered-elsewhere** (shared SDK-reader lane) |

### 2.2 Growth / candidate rows (grep-surfaced; owe a D.2 structural pairing or a lens decision)

Per LAW-2 and the house NO-DEFERRALS rule these are recorded, never dropped.
Each becomes a numbered lens question in §7; none is silently assumed clean or
dirty.

| # | candidate | evidence at `829b3c3` | required question (→ §7) |
|---:|---|---|---|
| C1 | WebSocket SDK background reader **(new at `829b3c3`)** | **Absence evidence.** The background reader `run_reader` (`websocket/subscription.rs:399-419`) contains **no** `set_read_timeout` call, so it arms no read window; a `SocketRead::TimedOut` merely re-enters the blocking `read_event` via `continue` (`:417`), self-documented "not a poll: no interval" (`:414-416`). The window is explicitly disarmed once in setup at `websocket/subscription.rs:283` (`set_read_timeout(None)`). The `set_read_timeout` primitive is defined at `std_socket.rs:155` (doc: "`None` blocks indefinitely") and passes through at `:158`; the only `Some(window)` arming anywhere is the connect handshake `Some(IO_TIMEOUT)` at `std_socket.rs:93`. So the reader blocks with no interval. Materially different from F8/F9. | Lens Q5 — conforming blocking reader, or a family? Must carry the four idle-cost answers **and** a shutdown-wake proof. |
| C2 | WebSocket keepalive Ping timer **(new at `829b3c3`)** | `websocket/process.rs:78-91,640-659`: `KeepaliveSchedule` from config `ping_interval_ms`; W2 §5.1 already discloses it as a transport slice and pins that slice/Ping counters GROW while debt counters stay flat. | Lens Q6 — by-design transport idle cost (SENDS liveness, does not poll for change); confirm bound + pin + sign-off, and that it stays outside W4 change-detection scope. |
| C3 | `PushReplyAwaiter` recv re-arm | `supervisor.rs:745,790,844` (`recv_timeout`/`try_recv`); the skeleton's §C `UNVERIFIED-UNTIL-SWEPT` row, re-pinned. | Lens Q9 — confirmed out-of-W4-NOW; tracked to its own SDK-successor disposition. |
| C4 | Durability bridge `block_on` (`MAX_POLLS`, NoopWaker scan) | `durability/bridge.rs:52,87-93`: bounded 8-poll loop against a **NoopWaker** (a waker that can never be woken); skeleton §C row, unchanged. This is the NoopWaker-scan mechanism — distinct from the real-waker runtime parks C5/C6 and never to be conflated with them (skeleton §C row 3). | Lens Q9 — synchronous-contract assertion vs bounded scan; out-of-W4-NOW. |
| C5 | Cluster **startup** tokio `block_on` (real-waker park) **(skeleton §C row-3 ordered site)** | `cluster/membership.rs:320` (`runtime.block_on(scheduler.start_distribution_listener(...))`) and `:328` (`runtime.block_on(discovery::connect_seeds(...))`): a real tokio runtime parks the startup thread on an async I/O op until the reactor wakes it on completion. No application-layer interval/poll; not a NoopWaker scan. | Lens Q11 — classify as a real-waker runtime park (event-driven blocking bridge), distinct from C4, pending confirmation the awaited distribution futures hide no poll (dependency boundary, §D.5). |
| C6 | Cluster **per-write** tokio `block_on` (real-waker park) **(skeleton §C row-3 ordered site)** | `cluster/sync.rs:277-290`: on a multithread runtime `tokio::task::block_in_place(\|\| handle.block_on(write))` (`:282`), else a `Builder::new_current_thread()` runtime `block_on(write)` (`:286-290`), where `write` is `connection.write_raw(&frame).await` (`:275`). Real-waker park on a socket write, per outbound write, not a change-detection loop. | Lens Q11 — same classification as C5; the two `block_on` sites the skeleton §C row 3 ordered separately classified. |
| C7 | Dedup sweeper | `durability/dedup/sweep.rs:76-105` (`DedupSweeper`, `scan`+sweep). Grep finds **no production timer arming it** — only re-exports in `dedup.rs`/`durability/mod.rs`; not scheduled into a periodic loop at this pin. | Lens Q8 — confirm on-demand-only; a future periodic caller re-enters it as a family. |
| C8 | Single-shot admitted-deadline `recv_timeout` waits | `conversation/actor/sync.rs:33`; `conversation/actor.rs:469` (5 s); `routing/table.rs:437` (1 s); `routing/dispatch.rs:316` (`HANDOFF_CONFIRMATION_WINDOW`); `connection/conversation.rs:214,278` (`try_recv`/`recv_timeout` on `exit_rx`). Each appears to be a one-shot admitted wait budget, not a change-detection loop. | Lens Q7 — D.2 pairing must confirm each is a terminal admitted-deadline wait, not a re-check loop. |
| C9 | TypeScript SDK reconnection loop | Skeleton §C `OPEN` row (`sdks/liminal-ts/src/connection.ts`), outside the `crates sdks` Rust roots; unchanged at this pin. | Out-of-scope here (TS root); tracked by the skeleton's own §C/§D closure. |

**Examined and excluded — not a candidate.** The cluster resolver `NoopWaker`
`block_on` that an earlier draft carried as a production candidate is **test-only**:
it lives entirely inside `#[cfg(test)] mod tests` at `cluster/discovery.rs:180-182`
(`NoopWake` at `:189`, helper `resolve_now` at `:195`, `#[test]` fns at
`:222,240`). Production `ClusterResolver::resolve` at `discovery.rs:66` returns a
`ResolveFuture` driven by the tokio runtime, not a NoopWaker scan. It is recorded
here so its examination is not a silent drop, but it carries no lens question
because it is not production code.

Census discipline: names F1–F9 and C1–C9 are unique. Any future sweep hit
appends a new row; it is never folded into a similar one.

## 3. Scope ruling

W4-NOW is the **server-internal readiness/notification wave** — the families
whose replacement is liminal-local (no external beamr/Artemis ask blocks the
build) and whose acceptance shape is shared: replace a server-owned
sample/sleep loop with readiness or completion notification plus explicit
shutdown/deadline events. This matches the skeleton's §B grouping proposal,
extended by the new WebSocket listener (F2), which arrived after `ce8814d` with
the identical accept-loop shape and is therefore folded into leg 1 rather than
opened as a separate lane.

| partition | families | why |
|---|---|---|
| **W4-NOW** (this brief) | F1, F2 (leg 1); F3 (leg 2); F4, F5 (leg 3) | Liminal-local; server-owned sample/sleep loops; shared arm-before-observe / teardown / silence-counter acceptance. Buildable now. |
| **Ledgered-elsewhere** | F6 (cluster membership) | Blocked on external `«MEMBERSHIP-EVENT-SOURCE»` — an ordered join/leave API on beamr distribution's `ConnectionManager`; native-only (distribution is net-gated). Its own successor brief (skeleton §B). |
| **Ledgered-elsewhere** | F7 (channel command-reply) | Blocked on external `«CHANNEL-REPLY-EVENT-RACE»` — a scheduler-to-waiter process-exit notification (beamr/Artemis lane). Separate scheduler-seam brief. |
| **Ledgered-elsewhere** | F8, F9 (SDK TCP readers), and C1 (SDK WS reader) | Liminal-local but client-side, sharing **one** portable socket-wake mechanism and a different race taxonomy from the server loops. Own shared SDK-reader brief (skeleton §B). Kept out of W4-NOW so the server wave is not gated on the SDK wake decision. |
| **Out-of-scope-with-why** | C2 (WS keepalive), C3–C8 (growth), C9 (TS) | C2 is a by-design transport idle cost already disclosed by W2, not change-detection. C3–C8 are unresolved growth candidates owed a D.2 pairing, a synchronous-contract classification, or a real-waker-park confirmation; C5/C6 are the two tokio `block_on` sites the skeleton §C row 3 ordered separately classified (event-driven runtime parks, not the NoopWaker scan C4). C9 is outside the Rust roots. All carry a §7 lens question so none is a silent gap. |

W4-NOW is a buildable wave of **three legs** (leg count ≤ 3, per the W2 shape).
Each leg separates **FIXED behaviour** from **OPEN mechanism** per the
skeleton's own convention.

## 4. W4-NOW replacement designs

**Board-binding law: redesign it to be TOLD.** No leg may discover state by a
timer, poll, sweep, scan, periodic reap, `WouldBlock`/timeout wake, or stop-flag
sample. The landed TOLD vocabularies this wave REUSES — it does not invent
parallel ones — are: **(a)** the shared beamr-readiness reactor already consumed
by parked connections (inventory pinned in `supervisor_tests.rs`; quiescence
oracle `publication.rs:408`); **(b)** the W1b connection-fate exit delivery —
the `ConnectionFateWorkItem` struct is defined at
`crates/liminal-server/src/server/participant/dispatch.rs:115`, and its routing
funnel `handle_connection_fate` is at `dispatch.rs:618-628`; completion appends
at
`crates/liminal-server/src/server/participant/production/binding_fate_completion.rs`
— which already delivers a **registered** connection's exit rather than finding
it by a reap scan (the §4.1 handshake-stage carve-out bounds what (b) does and
does not cover); and **(c)** the existing `Condvar`-based
`ShutdownHandle` (`shutdown.rs:49-62,86-107`) as the completion-notification
shape.

### 4.1 Leg 1 — listener accept retirement (F1 + F2)

**FIXED.** Both the main TCP listener and the sibling WebSocket listener wait
for acceptability **without a backoff loop**; explicit shutdown interrupts that
wait; and a connection-process exit is **delivered** to supervisor cleanup, not
found by the per-iteration `reap_crashed_connections()` (F1) /
`handshakes.reap_finished()` (F2) scan. An admitted connection is still handed
to the supervisor (F1 `spawn_connection`, `listener.rs:137`) or handshake
supervisor (F2 `handshakes.begin`, `websocket/listener.rs:159`); a
resource-exhaustion policy must fail, shed, or await a genuine resource event —
never sleep-and-retry the EMFILE/ENFILE path (`listener.rs:145-147`,
`websocket/listener.rs:165-167`).

**Handshake-stage carve-out (coordinator finding, verified at bytes).** F2 has
**two** distinct exits, and the W1b funnel (b) covers only one of them. A
**completed** upgrade is spawned into the shared `ConnectionSupervisor`
(`websocket/supervisor.rs:194,212`) and becomes an ordinary supervised
connection — its later exit rides the W1b `ConnectionFateWorkItem` delivery
exactly like a TCP connection's. But a **handshake-stage** worker that never
registers — a refused or failed upgrade (`websocket/supervisor.rs:196-201`), or
a pre-upgrade shutdown interrupt — ends without ever entering the shared
supervisor; its thread-completion is tracked today by the join-scan
`HandshakeSupervisor::reap_finished` (`websocket/supervisor.rs:112`), called each
accept iteration at `websocket/listener.rs:156`. **W1b does not cover
handshake-stage exits.** The module doc confirms the boundary: "A COMPLETED
upgrade is then spawned into the SHARED `ConnectionSupervisor`"
(`websocket/supervisor.rs:7-10`). Therefore leg 1 replaces `reap_finished` with
its **own** TOLD handshake-worker completion delivery — a worker signals its own
thread-end into a completion primitive; still no reap loop — which is **not**
literally the W1b funnel. Only post-upgrade connection exits reuse (b).

**OPEN mechanism (candidates, none selected).** Socket
`«MAIN-LISTENER-READINESS-SHUTDOWN-EXIT»` and its WebSocket sibling
`«WS-LISTENER-READINESS-SHUTDOWN-EXIT»`: expose host-owned listener-fd readiness
from the existing shared reactor, **or** a portable blocking-accept plus an
explicit cross-platform interrupt. The **registered** connection-exit half
REUSES the W1b fate exit delivery (b/c) rather than a new reap path; the
**handshake-worker** completion half needs its own TOLD delivery per the
carve-out above, whose concrete completion primitive is also OPEN.

**Idle-cost lens.** (1) Zero application-thread wakes and zero repeated
accept/reap calls on a silent listener; pin zero. (2) For `L_tcp + L_ws`
listeners the aggregate application ceiling is `0`; if the shared reactor is
chosen, adding both listener fds must add **zero** reactor threads. (3)
Quiescence tests `silent_main_listener_has_zero_application_wakes` and
`silent_websocket_listener_has_zero_application_wakes` (counters, not timing).
(4) The shared readiness reactor is by-design infrastructure: the existing
one-thread **inventory** pin (`supervisor_tests.rs`) is not a wake-RATE pin —
leg 1 must add a wake-count pin and obtain certifying-pair sign-off (Lens Q10).

### 4.2 Leg 2 — health accept retirement (F3)

**FIXED.** The health worker blocks or readiness-waits for acceptability and
explicit shutdown interrupts it. Existing health/readiness/metrics request
semantics (`endpoint.rs:136-160+`) are unchanged. **OPEN:** socket
`«HEALTH-ACCEPT-SHUTDOWN-WAKE»`, a liminal-local portable accept-interrupt /
readiness choice (no external readiness asserted). Cross-reference: the
console/OpsState design rides the health listener in its v1 shape; this leg
changes only the accept wait, not routes, auth, or handlers.

**Idle-cost lens.** (1) Zero application wakes / repeated accepts on a silent
health listener; pin zero. (2) Across `H` health listeners the ceiling is `0`.
(3) `silent_health_listener_has_zero_application_wakes` (accept-attempt + wake
counters). (4) Any selector thread / wake socket added is by-design cost: bound,
pin, sign-off.

### 4.3 Leg 3 — shutdown drain + force-close settle retirement (F4 + F5)

**FIXED.** Each connection exit updates a completion primitive; graceful
completion races against **one** admitted shutdown deadline; the deadline may
win but may never sample completion. Force-close reuses the **same** exit
notification rather than a second settle poll. The per-iteration
`reap_crashed_connections()` + `active_connection_count()` scans (`shutdown.rs:203,211,243,244`)
are deleted. **OPEN:** the concrete supervisor completion API/primitive — a
`Condvar` with a generation (the `ShutdownHandle` shape (c) is the reuse
candidate), an event-count/latch, or a one-shot channel.

**Wake-vocabulary reuse — crash/restart composition.** A connection's exit is
already delivered by W1b as a `ConnectionFateWorkItem` (b); leg 3's
drain-completion decrement consumes that SAME exit delivery in the single
connection-removal funnel, so a Died/Detached/crash exit and an orderly close
both decrement the drain generation through one path — no parallel exit source,
no reap scan. Progress logs move from periodic ticks to logs on actual
count-transition events and the one deadline.

**Idle-cost lens.** (1) Zero periodic wakes during a quiet drain; admitted
ceiling is one wake per delivered connection exit plus at most one deadline wake
per shutdown sequence. (2) Across `S` server instances, pre-deadline silence
costs zero wakes; the sequence ceiling is `Σ delivered exits + at most S`
deadline wakes, never `100 × S`/s. (3) `quiet_drain_wakes_only_for_exit_or_single_deadline`
(wake counter + lost-wake barriers). (4) The one deadline is by-design: pin
exactly one arming and at most one delivery per sequence, no helper tick,
certifying-pair sign-off.

## 5. Acceptance oracle census

The build is not accepted unless every row exists under its exact name. Every
name derives from a §4 design row and appears exactly once. Rows are of four
kinds required by the ledger floor: **absence proofs** (structural grep +
runtime quiescence counter under a named workload), **replacement-correctness**,
**crash-cut** (a wake can be lost), and **idle-honesty both-sides** (unrelated
counters GROW while retired-family counters stay FLAT — the fixture must not
pass by hiding the timer). No sleep-based, log-only, or mock test that bypasses
the production accept/drain path satisfies a row.

| # | exact oracle | leg / kind | required observation |
|---:|---|---|---|
| 1 | `silent_main_listener_has_zero_application_wakes` | 1 / absence | On a quiet TCP listener, zero application-thread wakes, zero repeated `accept`, zero reap calls after the wait is armed. |
| 2 | `silent_websocket_listener_has_zero_application_wakes` | 1 / absence | Same for the WebSocket accept worker: zero wakes / accepts / handshake-reap calls after arming. |
| 3 | `main_listener_source_has_no_accept_backoff_or_reap_poll` | 1 / absence-grep | No hit for `ACCEPT_IDLE_BACKOFF\|TRANSIENT_ERROR_BACKOFF\|WouldBlock.*sleep\|reap_crashed_connections` in the replacement accept path. |
| 4 | `websocket_listener_source_has_no_accept_backoff_or_handshake_reap_poll` | 1 / absence-grep | No hit for the same constants nor `reap_finished` in a per-iteration sleep in the WebSocket accept path. |
| 5 | `listener_shutdown_interrupts_accept_wait_without_backoff` | 1 / correctness | Race shutdown before, during, and after arming, on both listeners; the wait returns promptly with no sleep and no lost accept. |
| 6 | `registered_connection_exit_reaches_supervisor_cleanup_by_delivery_not_reap` | 1 / correctness | A **registered** connection-process exit (TCP, or a post-upgrade WebSocket connection) reaches supervisor cleanup via the W1b fate delivery, with no per-iteration `reap_crashed_connections` scan. |
| 7 | `listener_idle_grows_unrelated_reactor_slices_while_accept_counters_stay_flat` | 1 / idle-honesty | Under an unrelated live workload, reactor/transport slice counters GROW while accept-attempt and reap counters stay FLAT — proving the test cannot pass by disabling the reactor. |
| 8 | `silent_health_listener_has_zero_application_wakes` | 2 / absence | Quiet health listener: zero accept attempts and zero application wakes after arming, route behaviour unchanged. |
| 9 | `health_accept_source_has_no_wouldblock_sleep_poll` | 2 / absence-grep | No hit for `set_nonblocking\(true\)`-driven `WouldBlock` + `sleep` in the health accept path. |
| 10 | `health_shutdown_interrupts_accept_wait` | 2 / correctness | Race shutdown before wait, between arm and wait, concurrent with accept readiness, and after accept returns; no descriptor or worker leak. |
| 11 | `health_idle_grows_unrelated_counters_while_accept_stays_flat` | 2 / idle-honesty | An unrelated served request increments its counters while the health accept-attempt counter stays FLAT during silence. |
| 12 | `quiet_drain_wakes_only_for_exit_or_single_deadline` | 3 / absence | Hold active connections silent; the drain waiter does not wake until a delivered exit or the single admitted deadline. |
| 13 | `drain_source_has_no_reap_count_sleep_loop` | 3 / absence-grep | No hit for `DRAIN_PROGRESS_INTERVAL\|FORCE_CLOSE_SETTLE_TIMEOUT\|FORCE_CLOSE_POLL_INTERVAL\|reap_crashed_connections` in the drain/settle implementation. |
| 14 | `force_close_settle_uses_exit_notification_not_second_poll` | 3 / correctness | Force-close continues on the same connection-exit notification; no second settle poll loop exists. |
| 15 | `drain_completes_on_last_exit_delivered_by_w1b_fate` | 3 / correctness | A Died/Detached/crash exit and an orderly close both decrement drain completion through the one W1b exit funnel, not a reap scan. |
| 16 | `drain_deadline_fires_at_most_once_per_sequence` | 3 / by-design pin | Exactly one deadline arming and at most one delivery per shutdown sequence; no helper tick. |
| 17 | `drain_idle_grows_unrelated_slices_while_drain_counters_stay_flat` | 3 / idle-honesty | During a quiet drain, unrelated scheduler slices GROW while drain reap/count wake counters stay FLAT. |
| 18 | `drain_exit_between_predicate_and_park_is_not_lost` | 3 / crash-cut | An exit delivered between the completion-predicate observation and the park is not lost (arm-before-observe barrier). |
| 19 | `last_drain_exit_simultaneous_with_deadline_resolves_one_winner` | 3 / crash-cut | The last exit arriving at the same barrier as the deadline resolves to exactly one winner; completion is neither double-counted nor dropped. |
| 20 | `accepted_socket_racing_shutdown_is_supervised_or_shed_never_slept` | 1 / crash-cut | A socket accepted while shutdown fires is either supervised or explicitly shed, never left to a sleep-retry; no connection slips past the shutdown broadcast. |
| 21 | `handshake_worker_completion_delivered_not_reap_scanned` | 1 / correctness | A handshake-stage worker that never registers (refused/failed upgrade, or pre-upgrade shutdown) signals its own completion; no `HandshakeSupervisor::reap_finished` join-scan runs per accept iteration. This is the F2 exit W1b does not cover. |

## 6. Scope walls

| inside W4-NOW | expressly outside W4-NOW |
|---|---|
| Retiring the five server-owned sample/sleep loops F1–F5; TOLD readiness/completion + explicit shutdown/deadline wakes; reuse of the beamr-readiness reactor, W1b fate exit delivery, and the `ShutdownHandle` `Condvar`; the 21 oracles above. | Inventing any new reactor, second exit path, or parallel wake vocabulary. Reuse (a)/(b)/(c) or leave the mechanism OPEN under a named socket. |
| The listener/health accept wait and the drain/settle completion wait. | Health/readiness/metrics route semantics, auth, or the console/OpsState design that rides the health listener. |
| Composition of drain completion with W1b connection-fate exits. | Reopening W1b fate classification, schema, source order, finalizer ownership, or `ParticipantServiceFatal`. |
| Server-side loops only. | Cluster membership (F6), channel command-reply (F7), and the SDK readers (F8/F9/C1) — each its own ledgered lane (§3). W4-NOW neither claims nor blocks their replacements. |
| Nothing beyond source edits that are forward-only. | Dual old/new runtime modes, fallback migration, tags, or a dormant compatibility branch (YG-560 forward-only). |

## 7. Questions FOR THE LENS

These are review obligations, not deferred design choices (NO DEFERRALS is
house law). A **no** with contradicting bytes blocks build dispatch and returns
the brief for revision; it does not license an implementation guess.

1. Is the W4-NOW boundary correct — the server-internal readiness group
   (F1+F2 listeners, F3 health, F4+F5 drain/settle) as the buildable wave, with
   F6 membership, F7 channel-reply, and F8/F9/C1 SDK readers held to their own
   ledgered lanes?
2. Is the new WebSocket server listener (F2) correctly folded into leg 1 —
   same accept-loop shape as F1, same wave — rather than opened as a separate
   lane?
3. Wake mechanism (OPEN): should leg 1 reuse the shared beamr-readiness reactor
   for the two listener fds, or select a portable blocking-accept interrupt?
   Neither is chosen here; the lens rules whether either candidate is admissible
   before build.
4. Does reusing the W1b `ConnectionFateWorkItem` exit delivery cover the leg-1
   **registered**-connection reap (F1, and post-upgrade WebSocket connections)
   and the leg-3 drain-completion decrement through one funnel — while the F2
   **handshake-stage** `reap_finished` (`websocket/supervisor.rs:112`) is
   replaced by its own TOLD handshake-worker completion delivery (oracle 21),
   since W1b does not cover pre-registration handshake exits
   (`websocket/supervisor.rs:7-10,194,212`)?
5. Is the WebSocket SDK background reader (C1) — blocking, arming no read window
   (no `set_read_timeout` in `run_reader` at `websocket/subscription.rs:399-419`),
   self-described "not a poll: no interval" (`:414-417`) — genuinely conforming,
   and if so does it carry the four idle-cost answers AND a shutdown-wake proof?
   If not, it is a family and re-enters the SDK lane.
6. Is the WebSocket keepalive Ping timer (C2) a by-design transport idle cost
   (it SENDS liveness, does not poll for change) already bounded/pinned/signed
   under W2 §5.1, and does it stay outside W4 change-detection scope?
7. Do the single-shot admitted-deadline `recv_timeout` waits (C8:
   `conversation/actor/sync.rs:33`, `conversation/actor.rs:469`,
   `routing/table.rs:437`, `routing/dispatch.rs:316`,
   `connection/conversation.rs:214,278`) each pass D.2 structural pairing as a
   terminal admitted-wait budget rather than a change-detection re-check loop?
8. Does the dedup sweeper (C7, `durability/dedup/sweep.rs:76-105`) have no
   production timer arming it at `829b3c3` — confirming on-demand-only — with a
   standing note that any future periodic caller re-enters it as a family?
9. Are the growth carryovers — `PushReplyAwaiter` re-arm (C3,
   `supervisor.rs:745-790`) and the durability bridge `MAX_POLLS` NoopWaker scan
   (C4, `bridge.rs:52,87-93`) — confirmed out of W4-NOW and tracked to their own
   dispositions? (The skeleton's `discovery.rs` NoopWaker is **not** a production
   candidate: it is entirely `#[cfg(test)]` — `mod tests` at `discovery.rs:180-182`,
   `NoopWake` at `:189`, helper `resolve_now` at `:195`; production `resolve` at
   `:66` returns a runtime-driven future, not a NoopWaker scan.)
10. Does the shared readiness reactor receive a wake-RATE pin (not merely the
    existing one-thread inventory pin in `supervisor_tests.rs`) plus
    certifying-pair sign-off before leg 1 is accepted?
11. Are the two tokio-runtime `block_on` sites the skeleton §C row 3 ordered
    separately classified — cluster startup (C5, `membership.rs:320,328`: bind +
    seed-connect) and cluster per-write (C6, `sync.rs:277-290`: `block_in_place`
    / current-thread runtime around `write_raw(...).await`) — correctly
    classified as **real-waker runtime parks** (the tokio reactor wakes the
    thread on I/O completion), a different mechanism from the NoopWaker scan (C4)
    that must not be conflated with it, and not LAW-1 change-detection families —
    pending confirmation that the awaited distribution futures hide no poll loop
    of their own (dependency boundary per skeleton §D.5)?

## 8. Revision record

| revision | date | byte/ledger pin | record |
|---|---|---|---|
| r1 | 2026-07-22 | liminal `829b3c30a9f27bab8aa31cbe21470e687c59937d`; `WIRING-LEDGER.md` r1.9, 2026-07-20 | Initial design-first brief of record for lane W4. Re-pins the `ce8814d` skeleton to `829b3c3` with a full drift ledger (§1): W2 retired the unconditional `notify_ready` for `notify_impact`/`target_union`; the WebSocket transport added a new server listener family (F2) and SDK reader/keepalive candidates (C1/C2); shutdown/membership/reader anchors moved. Mechanical family census (§2): nine confirmed byte-verified polling families (F1–F9) plus eight growth/candidate rows (C1–C8), unique-named. Scope ruling (§3): W4-NOW = server-internal readiness wave (F1–F5) in three legs; F6/F7/F8/F9/C1 ledgered to their own lanes; C2–C8 out-of-scope-with-why. Replacement designs (§4) reuse the landed beamr-readiness reactor, W1b fate exit delivery, and the `ShutdownHandle` `Condvar` — no parallel wake vocabulary — each with FIXED/OPEN separation and the four-part idle-cost lens. Twenty-oracle absence-proof census (§5): structural grep + runtime quiescence + crash-cut + idle-honesty both-sides. Scope walls (§6) and ten numbered lens questions (§7). Ready for the lens rounds. |
| r2 | 2026-07-22 | same liminal/ledger pin | Folds lens r1 (**3 MAJOR + 5 minor**) plus the **coordinator (Fable-seat) finding**, each re-verified at `829b3c3` bytes. **MAJOR-1 — phantom production candidate:** the cluster resolver `NoopWaker` `block_on` is entirely `#[cfg(test)]` (`discovery.rs:180-182,189,195`); production `resolve` at `:66` returns a runtime-driven future. The candidate row is deleted and re-recorded as "examined and excluded — test-only" in §2.2; Q9 rewritten accordingly. **MAJOR-2 — dropped skeleton-ordered sites:** the two tokio-runtime `block_on` sites the skeleton §C row 3 ordered separately classified are added as census rows C5 (`membership.rs:320,328`, startup) and C6 (`sync.rs:277-290`, per-write), classified as real-waker runtime parks (distinct from the NoopWaker scan C4) and gated by new lens question Q11. **MAJOR-3 — C1 cited nonexistent bytes:** the "no armed read window" claim is rewritten to ABSENCE form — `run_reader` (`websocket/subscription.rs:399-419`) makes no `set_read_timeout` call; the primitive is defined at `std_socket.rs:155`/passed at `:158`, and the only `Some(window)` arming is `std_socket.rs:93`. **minor-a** listener doc attribution corrected to `:3-4` ("Mirrors") + `:25` ("matching"); **minor-b** the `notify_impact` gloss re-anchored — calls at `dispatch.rs:625,676` are unconditional, gating is `target_union`'s empty-set arm at `dispatch_impact.rs:97-104` (`:99`); **minor-c** `ConnectionFateWorkItem` routing re-cited to `handle_connection_fate` at `dispatch.rs:618-628` (struct def `:115`); **minor-d** the `examples/` class exclusion stated explicitly (`demo_feed_publisher/main.rs:90` demo pacing) plus the `#[cfg(test)]` sleeps; **minor-e** full path for `binding_fate_completion.rs`. **Coordinator finding (design amendment):** verified at bytes that `HandshakeSupervisor::reap_finished` (`websocket/supervisor.rs:112`) reaps handshake-stage workers that never register, which W1b's `ConnectionFateWorkItem` does not cover — §4.1 gains an explicit handshake-stage carve-out (own TOLD completion delivery, not the W1b funnel), oracle 6 is scoped to **registered** connections, new oracle 21 `handshake_worker_completion_delivered_not_reap_scanned` is added (census now 21), and Q4 amended. Census now F1–F9 + C1–C9; eleven lens questions. |
