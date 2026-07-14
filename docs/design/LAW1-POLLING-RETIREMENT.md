# LAW-1 application polling retirement — post-key-turn brief skeleton

> **STATUS: DESIGN/SCOPING — PAPER ONLY.** This document grants no
> implementation authority. It is **OPEN-ENDED by design**: the r12 exhaustive
> sweep appends families mechanically; this document is not complete merely
> because the seven R11 blockers are represented. **Codebase pin:** liminal
> main at `ce8814d`.

**Board-binding authority.** Tom's NO-POLLING ruling is the design constant:
“real-time is a design constant. No application-layer poll loops anywhere in
the product... if a design has a timer whose job is ‘check whether something
changed,’ it's wrong — redesign it to be TOLD.”

**LAW-1 — NO-POLLING.** Participant-contract draft R11
(`origin/design/participant-contract` at `1e6aa99`,
`docs/design/PARTICIPANT-CONTRACT.md`) makes these mechanisms non-conforming
when their job is to discover changed state: timer, poll, sweep, scan,
heartbeat, listener backoff, periodic reap, read-timeout wake, stop-flag
sampling, and a synthetic write or probe. LAW-1 binds every replacement shape,
every acceptance frame, every dependency choice, and the growth/sweep sections
below. A one-shot admitted domain deadline may win an event race; it may not be
turned into periodic sampling.

**LAW-2 — BELIEVED STATE IS NOT CITABLE STATE.** Every codebase-state sentence
in this document is backed by a file:line range opened and re-verified at
`ce8814d`, or by a grep-able genuinely-open socket. An unresolved dependency
that is neither is not evidence. LAW-2 binds this whole document, including
candidate mechanisms and future rows. The R11 document above and the
console/MCP branch named below are external design inputs, not claims that
those files exist in the `ce8814d` tree.

**House brief standard.** A dispatch brief produced from this skeleton must use
numbered requirements, current file:line evidence or a named socket,
silence-attacking acceptance, and no invented readiness. Each replacement must
answer the house idle-cost lens: (1) idle cost and a pinned ceiling, (2) the
aggregate ceiling across instances, (3) the test that asserts quiescence, and
(4) every by-design idle cost's bound, pinning test, and certifying-pair
sign-off. “Event-driven” without those answers is not a readiness claim.

**Citation audit.** The seven approximate source ranges supplied to this scope
have not drifted at `ce8814d`. The exact anchors below sharpen them where the
range contains several operations—for example, the push reader's outer
stop-flag loop is at
`crates/liminal-sdk/src/remote/tcp/push_client.rs:464-495`, while the actual
timeout-to-`None` conversion is at
`crates/liminal-sdk/src/remote/tcp/push_client.rs:497-510`. No source citation
below is inherited from R11.

## 1. Listener accept loop

**1.1 Requirement — loop identity and evidence.** Retire the main wire listener
poll family. It defines 10 ms idle and 50 ms transient-error backoffs at
`crates/liminal-server/src/server/listener.rs:11-12`, makes the listener
nonblocking at `crates/liminal-server/src/server/listener.rs:99-103`, and then
repeatedly samples shutdown, reaps crashed processes, calls `accept`, and
sleeps on `WouldBlock` or resource exhaustion at
`crates/liminal-server/src/server/listener.rs:125-155`.

**1.2 Requirement — polled state, cadence, and present idle cost.** The loop
asks three independent questions: whether shutdown changed, whether a
connection process disappeared, and whether the listener became acceptable.
On a quiet listener, `ACCEPT_IDLE_BACKOFF = 10 ms` derives a nominal 100 wake
cycles and 100 `accept` attempts per second per listener. Each quiet cycle has
one socket `accept` call, one shutdown atomic load, and one supervisor reap
call; the exact kernel-syscall total per wake, including the standard library's
sleep implementation, is platform-dependent and requires measurement. A
transient `EMFILE`/`ENFILE` result selects the 50 ms backoff, nominally 20
retries per second; that is an error-path retry cadence, not quiescence.

**1.3 Requirement — contract socket.** R11 names **no socket** for this blocker.
That is an explicit LAW-2 gap. The post-key-turn brief must register the
proposed genuinely-open dependency
**`«MAIN-LISTENER-READINESS-SHUTDOWN-EXIT»`**: listener readiness, explicit
shutdown wake, and process-exit delivery must replace all three sampled
questions. The name is a brief proposal, not a selected API.

**1.4 Requirement — FIXED behavior versus OPEN mechanism.** **FIXED:** the main
listener waits for acceptability without a backoff loop, shutdown explicitly
interrupts that wait, and connection exit is delivered rather than found by a
periodic reap. An admitted connection is still handed to the supervisor; a
resource-exhaustion policy must fail, shed, or await a genuine resource event,
never sleep and retry. **OPEN:** the concrete ownership and cross-platform wake
mechanism, the listener-facing readiness API, and the process-exit delivery
API. Candidate mechanisms may be evaluated; none is selected here.

**1.5 Requirement — candidate event-driven replacement shape.** The natural
candidate is the **same beamr readiness service** already consumed by parked
connections, not a second reactor. The implemented precedent registers or
rearms a connection fd through the scheduler readiness facility at
`crates/liminal-server/src/server/connection/process.rs:423-461`, arms before a
final probe, and returns `NativeOutcome::Wait` when no work remains at
`crates/liminal-server/src/server/connection/process.rs:193-213`. Its inventory
test pins one shared `beamr-readiness-poll` thread at
`crates/liminal-server/src/server/connection/supervisor_tests.rs:23-35`, and
its silence check pins a parked connection's slice count flat at
`crates/liminal-server/src/server/connection/supervisor_tests.rs:47-60`—zero
connection slices and zero connection-process wakes while idle. Extending that
service to a host-owned listener fd, or representing the listener as a beamr
process, are candidates under the socket; blocking `accept` plus an explicit
portable interrupt is another candidate. The brief must separately route
process-exit events into supervisor cleanup. These are candidates, not a
mechanism decision.

### 1.6 Silence-attacking acceptance frame

Delete `ACCEPT_IDLE_BACKOFF` and `TRANSIENT_ERROR_BACKOFF` at
`crates/liminal-server/src/server/listener.rs:11-12`, the nonblocking accept
poll shape at `crates/liminal-server/src/server/listener.rs:99-103`, and the
shutdown/reap/accept/sleep loop at
`crates/liminal-server/src/server/listener.rs:125-155`. Race accept readiness
before, during, and after arming against shutdown; race shutdown against an
accepted-but-not-yet-supervised socket; deliver a connection-process exit while
the listener is silent; cover interrupted accept, bursts, and resource
exhaustion without a timer. A source check must return no listener-family hit
for `ACCEPT_IDLE_BACKOFF|TRANSIENT_ERROR_BACKOFF|WouldBlock.*sleep|reap_crashed_connections`
in the replacement accept path. A silence soak must prove no accept attempts,
reap calls, application slices, or application-thread wakes after the wait is
armed.

**1.7 Requirement — idle-cost lens.** (1) Target idle cost is zero
application-thread wakes and zero repeated accept/reap calls on a silent
listener; pin that zero. (2) For `L` listeners the aggregate application idle
ceiling is `0 × L = 0`; if the existing shared beamr service is chosen, adding
the listener must add zero reactor threads. (3) The brief must add the named
quiescence test `silent_main_listener_has_zero_application_wakes`, with counters
that can fail rather than a timing-only assertion. (4) A shared readiness
reactor is by-design infrastructure: the existing one-thread inventory pin is
not a wake-rate pin. The brief must bound and measure its idle wake behavior,
add a wake-count pinning test, and obtain certifying-pair sign-off. Any other
helper thread or wake source receives the same treatment.

## 2. Cluster membership `PollLoop`

**2.1 Requirement — loop identity and evidence.** Retire the cluster membership
poll family. `POLL_INTERVAL` is 250 ms at
`crates/liminal-server/src/cluster/membership.rs:40-44`; `poll_once` snapshots
`connected_nodes`, diffs it against the tracked set, and replaces that set at
`crates/liminal-server/src/cluster/membership.rs:112-128`; `PollLoop` owns an
atomic stop flag and thread at
`crates/liminal-server/src/cluster/membership.rs:198-223`; and
`run_poll_loop` samples the flag, applies a snapshot delta, and sleeps at
`crates/liminal-server/src/cluster/membership.rs:225-230`.

**2.2 Requirement — polled state, cadence, and present idle cost.** The loop
asks whether beamr's connected-node table changed and whether local shutdown
was requested. A stable cluster still runs one snapshot/diff every 250 ms:
nominally four wake cycles per second per `PollLoop`, each with one
`connected_nodes` snapshot, one tracked-set mutex acquisition/diff, and one
stop atomic load. No direct I/O syscall appears in this source path; the exact
kernel-syscall count per wake, including sleep and beamr internals, is not
derivable from this file and must be measured rather than invented.

**2.3 Requirement — contract socket.** The binding socket is
**`«MEMBERSHIP-EVENT-SOURCE»`**, a genuinely-open implementation dependency.
LAW-2 permits the missing backend event API to remain open only under this
name; LAW-1 forbids filling it with another snapshot cadence.

**2.4 Requirement — FIXED behavior versus OPEN mechanism.** **FIXED:** ordered
membership join/leave deltas are pushed to the consumer, and shutdown
explicitly wakes and terminates that consumption. Initial-state handoff must
not lose a membership transition between snapshot and subscription.
**OPEN:** the membership event API's shape. At the pin the backend **is** beamr
distribution: `crates/liminal-server/src/cluster/membership.rs:32` imports
`beamr::distribution::connection::ConnectionManager`, and `poll_once` samples
its `connected_nodes()`. The family is also narrower than a full membership
feed: the `POLL_INTERVAL` doc-comment at
`crates/liminal-server/src/cluster/membership.rs:40-44` records that node-down
handling does **not** depend on the cadence — beamr's own hook drives the pg
purge synchronously on the drop — so the poll drives only membership logging
and R5 peer-join backfill. The ask is therefore an ordered **join**/leave event
API extending an existing synchronous drop-hook precedent, which makes the
initial-state handoff clause above the load-bearing FIXED part. A
`ConnectionManager` subscription, callback, or ordered event stream are
candidates only; replacing the backend instead would be its own ruling.

**2.5 Requirement — candidate event-driven replacement shape.** A candidate
source atomically establishes an initial membership view plus a continuation
cursor/subscription, then pushes ordered deltas over a blocking channel or
scheduler mailbox. The consumer blocks on that source and an explicit shutdown
wake; `ClusterSync` receives the same join/leave facts without diffing periodic
snapshots. The source must define duplicate, coalescing, overflow, disconnect,
and resubscription behavior. No candidate becomes selected until the external
backend choice closes `«MEMBERSHIP-EVENT-SOURCE»`.

### 2.6 Silence-attacking acceptance frame

Delete `POLL_INTERVAL` at
`crates/liminal-server/src/cluster/membership.rs:40-44`, `poll_once`'s
change-detection role at
`crates/liminal-server/src/cluster/membership.rs:112-128`, `PollLoop` at
`crates/liminal-server/src/cluster/membership.rs:198-223`, and the stop/load/
sleep loop at `crates/liminal-server/src/cluster/membership.rs:225-230`. Race a
join or leave across initial snapshot/subscription handoff; rapid join/leave/
rejoin; duplicate and coalesced backend events; backend close/error; and
shutdown before, with, and after a delta. A source check must return no
membership-family hit for `PollLoop|POLL_INTERVAL|poll_once|run_poll_loop`.
With a stable backend and no shutdown, a silence soak must show zero consumer
wakes and zero table snapshots after subscription.

**2.7 Requirement — idle-cost lens.** (1) Target idle cost is zero membership
consumer wakes, snapshots, diffs, and locks while no backend event or shutdown
exists; pin zero. (2) Across `M` cluster instances the consumer-side idle
ceiling is `0 × M = 0`. (3) The brief must add
`stable_membership_source_has_zero_consumer_wakes`, backed by source and
consumer counters. (4) If the selected backend maintains a by-design watcher,
thread, keepalive, or other idle activity, its numeric bound and aggregate
formula must come from backend evidence, be pinned by a test, and receive
certifying-pair sign-off. A backend heartbeat used to discover membership is
not eligible for that exception under LAW-1.

## 3. Health accept loop

**3.1 Requirement — loop identity and evidence.** Retire the health listener
poll family. Startup makes the health listener nonblocking and spawns one
worker at `crates/liminal-server/src/health/endpoint.rs:83-101`; `serve` samples
an atomic shutdown flag, calls `accept`, and sleeps 10 ms on `WouldBlock` at
`crates/liminal-server/src/health/endpoint.rs:104-133`.

**3.2 Requirement — polled state, cadence, and present idle cost.** The loop
asks whether the health listener became acceptable and whether shutdown
changed. A quiet endpoint derives a nominal 100 wake cycles and 100 `accept`
attempts per second per health listener from the 10 ms sleep. Each quiet cycle
has one socket `accept` call and one shutdown atomic load; the exact total
kernel syscalls per wake, including sleep, is platform-dependent and requires
measurement.

**3.3 Requirement — contract socket.** The binding genuinely-open socket is
**`«HEALTH-ACCEPT-SHUTDOWN-WAKE»`**. It owns the cross-platform mechanism that
interrupts a blocking/readiness accept on explicit shutdown.

**3.4 Requirement — FIXED behavior versus OPEN mechanism.** **FIXED:** health
accept blocks or readiness-waits, and explicit shutdown interrupts it. Existing
health/readiness/metrics request semantics are outside this retirement brief.
**OPEN:** the concrete cross-platform wake primitive and whether the worker
uses a blocking listener or a readiness facility. The console/OpsState design
on `design/console-mcp-design` at `0b63300`
(`docs/design/LIMINAL-CONSOLE-MCP.md` on that branch) explicitly rides the
existing health listener in its v1 shape. This section cross-references that
dependency; it does not redesign the console, OpsState, routes, auth, or
handler concurrency.

**3.5 Requirement — candidate event-driven replacement shape.** Candidates are
a blocking accept whose owned listener can be explicitly interrupted, or a
blocking readiness wait over the listener plus a dedicated shutdown event.
The worker drains all accept-ready connections according to a bounded rule and
returns to one wait. Closing a cloned descriptor, a platform-neutral wake
socket, or a shared readiness service may be evaluated for portability; none
is selected by R11 or this skeleton.

### 3.6 Silence-attacking acceptance frame

Delete the nonblocking accept configuration at
`crates/liminal-server/src/health/endpoint.rs:83-87` and the shutdown/load/
`WouldBlock`/sleep loop at
`crates/liminal-server/src/health/endpoint.rs:104-133`. Race shutdown before
wait, between arm and wait, concurrent with accept readiness, and after accept
returns; race accepted-client ownership with shutdown; cover interrupted
accept and verify no descriptor or worker leak. A source check must return no
health-accept-family hit for `set_nonblocking\(true\)|WouldBlock|sleep` in the
accept path. A silent-listener soak must prove zero accept attempts and zero
application-thread wakes after arming, while route behavior remains unchanged.

**3.7 Requirement — idle-cost lens.** (1) Target idle cost is zero
application-thread wakes and zero repeated accepts on a silent health
listener; pin zero. (2) Across `H` health listeners the application idle
ceiling is `0 × H = 0`. (3) The brief must add
`silent_health_listener_has_zero_application_wakes`, instrumenting accept
attempts and wake delivery. (4) Any selected selector thread, wake socket, or
shared reactor is by-design cost: state its bound and aggregate ceiling, pin it
with a test, and require certifying-pair sign-off. Merely proving prompt
shutdown does not certify quiescence.

## 4. Shutdown drain and force-close settle

**4.1 Requirement — loop identity and evidence.** Retire both shutdown polling
loops. The current constants admit 100 ms progress logging, a 500 ms
force-close settle window, and a 10 ms polling interval at
`crates/liminal-server/src/server/shutdown.rs:14-16`. `drain_connections`
repeatedly reaps, counts active connections, samples the deadline, and sleeps
at `crates/liminal-server/src/server/shutdown.rs:185-226`.
`wait_after_force_close` repeats reap/count/sleep until its separate deadline
at `crates/liminal-server/src/server/shutdown.rs:229-245`.

**4.2 Requirement — polled state, cadence, and present idle cost.** These loops
ask whether the active connection count reached zero and whether time passed.
While a drain has quiet active connections, the 10 ms interval derives a
nominal 100 wake cycles per second: each drain cycle makes one reap call and one
active-count call; each settle cycle does the same. The settle window can
therefore schedule up to roughly 50 interval waits before its final count,
subject to scheduler delay. The source has no direct I/O syscall in either
loop; exact clock/sleep kernel syscalls per wake are platform-dependent and
must be measured. These costs exist only while shutdown is active, but LAW-1
does not exempt short-lived polling.

**4.3 Requirement — contract socket.** The binding genuinely-open socket is
**`«SHUTDOWN-DRAIN-NOTIFICATION»`**. It owns the supervisor API by which every
connection removal updates drain completion and wakes a waiter without a reap
or count scan.

**4.4 Requirement — FIXED behavior versus OPEN mechanism.** **FIXED:** each
connection exit updates a completion primitive; graceful completion races
against **one admitted shutdown deadline**; the deadline may win but may never
sample completion. Force-close uses the same exit notification rather than a
second settle poll. **OPEN:** the concrete supervisor completion API and
primitive. A condition variable with a generation, event-count/latch, one-shot
channel, or scheduler-delivered completion are candidates, not decisions.

**4.5 Requirement — candidate event-driven replacement shape.** A candidate
supervisor creates a drain generation/guard before stop-accepting can lose an
exit, decrements it in the single connection-removal funnel, and completes a
one-shot when the count reaches zero. The shutdown coordinator races that
one-shot against one admitted deadline event. On deadline it force-closes the
remaining generation and continues on the same completion signal; periodic
progress logs are replaced by logs on actual count transitions and the one
deadline. The existing public surface only exposes reap and count at
`crates/liminal-server/src/server/connection/supervisor.rs:158-174`, so the
notification API remains genuinely open.

### 4.6 Silence-attacking acceptance frame

Delete `DRAIN_PROGRESS_INTERVAL`, `FORCE_CLOSE_SETTLE_TIMEOUT`, and
`FORCE_CLOSE_POLL_INTERVAL` at
`crates/liminal-server/src/server/shutdown.rs:14-16`; delete the reap/count/
clock/sleep drain loop at
`crates/liminal-server/src/server/shutdown.rs:185-226` and the repeated
post-force-close loop at
`crates/liminal-server/src/server/shutdown.rs:229-245`. Test zero connections,
last exit before registration, exit between predicate observation and park,
last exit simultaneous with the deadline, crash exit, force-close exit,
multiple exits, and repeated shutdown. A source check must return no shutdown-
family hit for `DRAIN_PROGRESS_INTERVAL|FORCE_CLOSE_SETTLE_TIMEOUT|FORCE_CLOSE_POLL_INTERVAL|reap_crashed_connections`
in the drain/settle implementation. Hold active connections silent and prove
the waiter does not wake until an exit or the single deadline.

**4.7 Requirement — idle-cost lens.** (1) Target cost during a quiet drain is
zero periodic wakes; the admitted ceiling is one wake per delivered connection
exit plus at most one deadline wake for the shutdown sequence. (2) Across `S`
concurrent server instances, silence before any deadline costs zero wakes;
across complete sequences the ceiling is the sum of delivered exits plus at
most `S` admitted deadline wakes, never `100 × S` per second. (3) The brief must
add `quiet_drain_wakes_only_for_exit_or_single_deadline`, including a wake
counter and lost-wake barriers. (4) The one deadline is by-design cost: pin
exactly one arming and at most one delivery per sequence, with no helper tick,
and obtain certifying-pair sign-off. Any primitive with a maintenance thread
must disclose, bound, test, and receive sign-off for that cost as well.

## 5. Channel command-reply liveness wait

**5.1 Requirement — loop identity and evidence.** Retire the channel actor
command-reply liveness poll family. `COMMAND_TIMEOUT` is five seconds and
`LIVENESS_POLL` is 10 ms at
`crates/liminal/src/channel/actor/wait.rs:18-24`; `poll_reply` loops on
`recv_timeout(LIVENESS_POLL)`, then queries the scheduler process table and
samples its deadline at `crates/liminal/src/channel/actor/wait.rs:73-95`.

**5.2 Requirement — polled state, cadence, and present idle cost.** The loop
asks whether a reply arrived, whether the actor disappeared from the process
table, and whether the command deadline passed. A live but silent actor causes
nominally 100 timeout wakes per second per outstanding command, for up to the
five-second command budget. Each timeout wake performs one process-table lookup
and one monotonic-clock sample after the channel wait; the exact kernel-syscall
count of `recv_timeout` and clock access is standard-library/platform dependent
and must be measured.

**5.3 Requirement — contract socket.** The binding genuinely-open socket is
**`«CHANNEL-REPLY-EVENT-RACE»`**. Its load-bearing dependency is a
scheduler-to-waiter process-exit notification primitive. That is a beamr API
ask owned by the beamr/Artemis lane, not a liminal process-table query to hide
behind another timer.

**5.4 Requirement — FIXED behavior versus OPEN mechanism.** **FIXED:** one
command wait parks until the first of reply, pushed target-process exit, or one
admitted command deadline. Channel disconnect remains a terminal event. No
repeated liveness query remains. **OPEN:** beamr's exit-subscription API and the
liminal primitive that composes reply, exit, disconnect, and deadline. A
beamr monitor/linked exit delivered to a host waiter, scheduler event stream,
or a single combined one-shot are candidates only.

**5.5 Requirement — candidate event-driven replacement shape.** A candidate
registers an exit interest before or atomically with command publication,
composes reply and exit into a one-winner completion, and arms exactly one
command deadline. The waiting thread blocks on that completion. Registration
must report an already-dead pid immediately and must close the subscribe/
publication race without querying the process table on a cadence. This shape
is blocked on the beamr/Artemis API choice; it is not selected here.

### 5.6 Silence-attacking acceptance frame

Delete `LIVENESS_POLL` at
`crates/liminal/src/channel/actor/wait.rs:21-24` and the `poll_reply`
`recv_timeout`/process-table/deadline-sampling loop at
`crates/liminal/src/channel/actor/wait.rs:73-95`. Race reply, process exit,
reply-channel disconnect, and deadline in every order, including reply/exit
and reply/deadline at the same barrier; test target exit before interest
registration and publication failure after registration. A source check must
return no channel-wait-family hit for
`LIVENESS_POLL|poll_reply|recv_timeout|process_table\(\)\.get` in the
replacement. A silent wait before its deadline must show no wake, process-table
lookup, or clock sampling.

**5.7 Requirement — idle-cost lens.** (1) Target idle cost is zero periodic
wakes and zero liveness queries; each command admits at most one deadline event
and one terminal completion winner. (2) Across `W` outstanding command waits,
pre-deadline silence costs zero wakes; if all expire, the aggregate admitted
deadline ceiling is `W`, not `100 × W` wakes per second. (3) The brief must add
`channel_reply_wait_wakes_only_for_reply_exit_or_deadline`, instrumenting
subscriptions, wake delivery, and process-table access. (4) The one-shot
deadline and any beamr exit-dispatch service are by-design costs. Their per-wait
and shared aggregate bounds must be stated, pinned by tests, and signed by the
certifying pair; a hidden scheduler scan cannot qualify as event delivery.

## 6. SDK push reader

**6.1 Requirement — loop identity and evidence.** Retire the SDK push-reader
shutdown poll family. `READER_POLL_TIMEOUT` is 100 ms at
`crates/liminal-sdk/src/remote/tcp/push_client.rs:49-52`; the socket installs it
as a read timeout specifically to observe a stop flag at
`crates/liminal-sdk/src/remote/tcp/push_client.rs:378-385`; `run_reader` samples
the flag and treats `None` as a recheck opportunity at
`crates/liminal-sdk/src/remote/tcp/push_client.rs:459-495`; and `next_frame`
turns a timed-out read into `Ok(None)` at
`crates/liminal-sdk/src/remote/tcp/push_client.rs:497-510`. The lower read
helper classifies `WouldBlock`/`TimedOut` as non-fatal timeout at
`crates/liminal-sdk/src/remote/tcp/push_client.rs:543-579`.

**6.2 Requirement — polled state, cadence, and present idle cost.** The reader
asks whether socket data arrived and whether local drop set the stop flag. An
indefinitely silent socket times out every 100 ms: nominally ten wake cycles and
ten socket-read attempts per second per `PushClient` reader. Each cycle has at
least one `read` call and one stop atomic load; the exact kernel work of socket
timeout setup/expiry and thread scheduling is platform-dependent and requires
measurement.

**6.3 Requirement — contract socket.** The binding genuinely-open socket is
**`«SDK-PUSH-READER-SHUTDOWN-WAKE»`**. It owns a portable explicit local wake
that interrupts socket waiting. Socket silence is not a clock.

**6.4 Requirement — FIXED behavior versus OPEN mechanism.** **FIXED:** the
reader blocks or readiness-waits for socket input and races that input against
an explicit local shutdown wake. Data, EOF, fatal I/O/decode error, receiver
drop, and shutdown remain events; no timeout is converted into “nothing
changed.” **OPEN:** the portable wake primitive and winner/cleanup mechanics.
Socket shutdown via an owned clone, a selector over the socket and an explicit
wake descriptor, or a portable runtime cancellation primitive are candidates
only.

**6.5 Requirement — candidate event-driven replacement shape.** One candidate
reader owns a blocking/readiness input wait plus a single-use local shutdown
registration; `Drop` signals shutdown, the wait returns, and join completes
without waiting for socket silence to expire. The push and subscription
readers must share **one** portable wake design and one race taxonomy; this is a
single SDK-reader brief candidate, not two independently selected primitives.
The wake is allowed to signal the admitted shutdown event; it may not be a
synthetic probe asking whether the socket or flag changed.

### 6.6 Silence-attacking acceptance frame

Delete `READER_POLL_TIMEOUT` at
`crates/liminal-sdk/src/remote/tcp/push_client.rs:49-52`, the read-timeout setup
at `crates/liminal-sdk/src/remote/tcp/push_client.rs:378-385`, the stop flag and
join-by-timeout assumption at
`crates/liminal-sdk/src/remote/tcp/push_client.rs:356-364`, and the stop/load/
timeout-as-`None` shape at
`crates/liminal-sdk/src/remote/tcp/push_client.rs:459-510`. Race local shutdown
with readable bytes, a partial frame, complete frame, EOF, fatal read error,
decode error, and inbound-receiver drop; prove prompt join on an indefinitely
silent peer and deterministic ownership of a frame that races shutdown. A
source check must return no push-reader-family hit for
`READER_POLL_TIMEOUT|set_read_timeout|Ok\(None\)|stop\.load` in the background
reader path. A silence soak must show zero reader wakes and zero read retries
before explicit shutdown.

**6.7 Requirement — idle-cost lens.** (1) Target idle cost is zero reader-thread
wakes and zero repeated reads while the peer and local owner are silent; pin
zero. (2) Across `P` push readers the application idle ceiling is
`0 × P = 0`. (3) The shared SDK brief must add
`silent_push_reader_wakes_only_for_data_fate_or_shutdown`, with read-attempt,
wake, and join counters. (4) A selector, wake descriptor, runtime driver, or
helper thread is by-design cost: the shared design must state its per-process
and per-reader bound, pin the aggregate in tests, and obtain certifying-pair
sign-off. A per-reader maintenance tick is forbidden, not merely expensive.

## 7. SDK subscription reader

**7.1 Requirement — loop identity and evidence.** Retire the SDK subscription-
reader shutdown poll family. `READER_POLL_TIMEOUT` is the same 100 ms cadence at
`crates/liminal-sdk/src/remote/tcp/subscription.rs:43-49`; the socket installs
it at `crates/liminal-sdk/src/remote/tcp/subscription.rs:218-241`; `run_reader`
samples the stop flag and treats timeout as a loop recheck at
`crates/liminal-sdk/src/remote/tcp/subscription.rs:316-367`; and `next_frame`
turns timeout into `Ok(None)` at
`crates/liminal-sdk/src/remote/tcp/subscription.rs:369-387`. The read helper's
`WouldBlock`/`TimedOut` conversion is at
`crates/liminal-sdk/src/remote/tcp/subscription.rs:419-456`.

**7.2 Requirement — polled state, cadence, and present idle cost.** The reader
asks whether delivery data arrived and whether local drop set the stop flag.
An indefinitely silent subscription times out every 100 ms: nominally ten wake
cycles and ten socket-read attempts per second per reader. Each cycle has at
least one `read` call and one stop atomic load; exact kernel work per timeout
wake is platform-dependent and requires measurement. The same timeout is also
used by synchronous setup; that distinct clock-sampling candidate is recorded
in §C rather than silently folded into this reader row.

**7.3 Requirement — contract socket.** The binding genuinely-open socket is
**`«SDK-SUBSCRIPTION-READER-SHUTDOWN-WAKE»`**. It owns the same portable local
shutdown-wake decision as `«SDK-PUSH-READER-SHUTDOWN-WAKE»`; two independently
invented mechanisms do not close either socket.

**7.4 Requirement — FIXED behavior versus OPEN mechanism.** **FIXED:** the
reader blocks or readiness-waits for socket input and races it against explicit
local shutdown. Delivery, disconnect frame, EOF, fatal I/O/decode error,
receiver drop, and shutdown are events; timeout is not an event. **OPEN:** the
one portable wake primitive shared with the push reader and the exact
winner/cleanup policy. The candidates listed in §6.4 remain candidates.

**7.5 Requirement — candidate event-driven replacement shape.** The candidate
shape is the same shared SDK-reader component as §6.5, parameterized by frame
routing rather than duplicated cancellation machinery. It must preserve the
setup residue handed into `run_reader` while replacing only the idle wait and
shutdown path. A single post-key-turn SDK-reader brief evaluates and selects
the portable primitive for both sockets; this paper does not.

### 7.6 Silence-attacking acceptance frame

Delete `READER_POLL_TIMEOUT` at
`crates/liminal-sdk/src/remote/tcp/subscription.rs:43-49`, its reader-facing
read-timeout use at
`crates/liminal-sdk/src/remote/tcp/subscription.rs:229-233`, the stop flag and
join-by-timeout assumption at
`crates/liminal-sdk/src/remote/tcp/subscription.rs:197-215`, and the stop/load/
timeout-as-`None` reader shape at
`crates/liminal-sdk/src/remote/tcp/subscription.rs:316-387`. Race local shutdown
with setup residue, readable bytes, partial and complete delivery frames,
`Disconnect`, EOF, fatal read error, decode error, and receiver drop; prove
prompt join on an indefinitely silent peer. A source check must return no
subscription-reader-family hit for
`READER_POLL_TIMEOUT|Ok\(None\)|stop\.load` in the background reader path. A
silence soak must show zero reader wakes and zero read retries before explicit
shutdown. Setup deadline handling remains an explicit §C sweep row, never a
reason to retain the reader poll.

**7.7 Requirement — idle-cost lens.** (1) Target idle cost is zero reader-thread
wakes and zero repeated reads while peer and owner are silent; pin zero. (2)
Across `U` subscription readers the application idle ceiling is
`0 × U = 0`; combined with push readers, the shared reader design's ceiling is
still `0 × (P + U) = 0`. (3) The shared SDK brief must add
`silent_subscription_reader_wakes_only_for_data_fate_or_shutdown` and a mixed
push/subscription aggregate quiescence test. (4) The shared primitive's
selector, descriptor, driver, and helper-thread costs are one disclosed
by-design budget, with a numeric bound, aggregate pinning test, and
certifying-pair sign-off. Neither reader may hide its own maintenance wake.

## A. Dependency map

LAW-2 binds every row: an unselected external mechanism remains a named socket,
not a readiness claim. “Local” means the production edit can live in liminal;
it does not mean reviewed, selected, or ready.

| Retirement | Liminal-local paper-to-code surface | External ask or choice | Owner lane / state |
|---|---|---|---|
| Main listener | Listener ownership, shutdown wiring, supervisor cleanup, and acceptance instrumentation | `«MAIN-LISTENER-READINESS-SHUTDOWN-EXIT»`: possibly expose host-owned fd registration/wake delivery from beamr's existing readiness service; if that candidate is rejected, select and prove a portable blocking-accept interrupt | beamr/Artemis for any readiness API; otherwise mechanism remains OPEN in the server brief |
| Health accept | Health worker wait, explicit shutdown signal, and cross-platform race tests | `«HEALTH-ACCEPT-SHUTDOWN-WAKE»`: portable accept interrupt/readiness choice | liminal-local choice; genuinely open, no external readiness asserted |
| Shutdown drain/settle | Add a supervisor completion-generation API and wire every connection-removal path | `«SHUTDOWN-DRAIN-NOTIFICATION»`: concrete primitive choice | liminal-local; genuinely open |
| Cluster membership | Consume ordered deltas and explicit shutdown, remove snapshot cadence | `«MEMBERSHIP-EVENT-SOURCE»`: an ordered join/leave event API with atomic initial view on beamr distribution's `ConnectionManager` — the backend at `ce8814d` (`membership.rs:32`, `poll_once` over `connected_nodes()` at `membership.rs:112-128`); a backend change instead would be its own ruling | beamr/Artemis; **native-only surface** (distribution is net-gated and compiled out of wasm); genuinely open |
| Channel command reply | Compose reply, disconnect, process exit, and one deadline | `«CHANNEL-REPLY-EVENT-RACE»`: scheduler-to-host-waiter process-exit notification | beamr/Artemis; API-gated and genuinely open |
| SDK push + subscription readers | One shared portable cancellation/wake component and both frame-reader integrations | `«SDK-PUSH-READER-SHUTDOWN-WAKE»` plus `«SDK-SUBSCRIPTION-READER-SHUTDOWN-WAKE»`: select one portable primitive | liminal SDK brief; genuinely open |

The named beamr asks are **three**: (1) a scheduler-to-waiter exit notification
for the channel seam; (2) an ordered membership join/leave event API on
distribution's `ConnectionManager` — a **native-only** surface, since
distribution is net-gated and compiled out of wasm — unless a backend change is
separately ruled; and (3) possibly a host/listener readiness surface if the
server brief selects the existing-service candidate. The remaining work is
liminal-local only after each named mechanism is selected and certified; this
map makes no readiness or sequence claim.

## B. Brief grouping proposal for post-key-turn server briefs

| Proposed brief | Included retirements | Grouping reason and common acceptance shape |
|---|---|---|
| Server-internal readiness/notification brief | Main listener, health accept, shutdown drain/settle | All replace a server-owned sample/sleep loop with readiness or completion notification plus explicit shutdown/deadline events. They share arm-before-observe lost-wake barriers, teardown ownership, silence counters, and deletion of listener/reap/count sleeps. The listener's optional beamr ask remains a dependency, not a reason to claim the group ready. |
| Cluster membership event-source brief | Membership `PollLoop` | Its mechanism and correctness hinge on the selected backend's atomic initial view and ordered delta API. Its acceptance is join/leave ordering and source shutdown, not fd acceptance. |
| Scheduler-seam brief | Channel command-reply liveness | Reply/exit/deadline composition needs the beamr/Artemis exit-notification ask and races at command publication. It must not be hidden inside the server-internal brief. |
| Shared SDK-reader brief | Push reader and subscription reader | Both have the same 100 ms read-timeout/stop-flag shape and require one portable explicit wake, the same data/fate/shutdown race matrix, mixed-instance aggregate quiescence, and one by-design idle budget. |

Grouping follows shared mechanism and shared acceptance, not calendar order.
Only the dependencies in §A constrain a brief. This proposal makes no claim
that any owner, API, backend, or certifying pair is ready.

## C. Growth section — candidate families pending the r12 exhaustive sweep

**Every row in this table is `UNVERIFIED-UNTIL-SWEPT`.** The evidence shown was
opened at `ce8814d`, but neither family boundaries nor exhaustiveness are
certified until r12. LAW-1 and LAW-2 bind these rows exactly as they bind the
seven blockers. Each r12 finding appends a new row here; it is never folded into
prose or dismissed because a nearby row looks similar.

| Status | Candidate family | Candidate evidence at `ce8814d` | Required r12 question / expected shape, not a decision |
|---|---|---|---|
| **UNVERIFIED-UNTIL-SWEPT** | `PushReplyAwaiter` receive re-arm — the push-reply quantum contract restored by liminal ledger gate G7 (“a push's reply deadline belongs to the push, not to the caller's poll”) | The public contract says `timeout` is a wait quantum and explicitly blesses indefinite reinvocation at `crates/liminal-server/src/server/connection/supervisor.rs:533-570`; its no-deadline path performs one `recv_timeout` at `crates/liminal-server/src/server/connection/supervisor.rs:573-588`, while the deadlined path loops over reply observation, `Instant::now`, quantum exhaustion, and `recv_timeout` at `crates/liminal-server/src/server/connection/supervisor.rs:590-636`. The pinned test deliberately executes five short 10 ms polls and re-arms for the eventual reply at `crates/liminal-server/src/server/connection/supervisor_tests.rs:549-590`. | Examiner finding B4 must receive a named requirement. **Expected outcome at this pin: CONCESSION** — an SDK call-site successor owning the race of reply, connection fate, and one admitted deadline, with the quantum surface intact underneath as the wait primitive, no longer asking callers to re-invoke short waits. The alternative argue branch (one `receive` quantum serves the caller's explicit wait budget rather than change detection) required evidence that production callers do not install an indefinite check loop; at `ce8814d` that evidence is absent and its negation is present — the blessing test's own doc-comment protects “an aion re-arm loop” as the production calling pattern at `crates/liminal-server/src/server/connection/supervisor_tests.rs:550-558`. If the r12 sweep or a later pin removes that caller pattern, the argue branch reopens. The contract and tests may not bless application polling by name while relying on an unproved caller distinction. |
| **UNVERIFIED-UNTIL-SWEPT** | Subscription setup clock-sampling | One 100 ms timeout is shared by reader and synchronous setup at `crates/liminal-sdk/src/remote/tcp/subscription.rs:43-49` and installed at `crates/liminal-sdk/src/remote/tcp/subscription.rs:229-233`. `read_one_frame` creates one deadline from `SETUP_TIMEOUT` (5 s, `crates/liminal-sdk/src/remote/tcp/subscription.rs:47-49`) but repeatedly converts socket timeout into an `Instant::now() >= deadline` check at `crates/liminal-sdk/src/remote/tcp/subscription.rs:389-416`. | Decide whether the admitted setup deadline excuses only the single deadline event, not the 100 ms sampling implementation. Expected candidate shape is a blocking/readiness control-frame read raced directly with one setup deadline; no periodic clock observation. Confirm every setup entry point and partial-frame behavior before promoting this row. |
| **UNVERIFIED-UNTIL-SWEPT** | Durability bridge `block_on` | The bridge documents a bounded `MAX_POLLS = 8` pending-future loop at `crates/liminal/src/durability/bridge.rs:31-52`; implementation polls and `yield_now`s up to eight times against a **no-op waker that can never be woken** at `crates/liminal/src/durability/bridge.rs:83-99`; its negative test feeds an always-pending future at `crates/liminal/src/durability/bridge.rs:114-123`. Verified production callers at `ce8814d`: durable publish, flush, and recovery at `crates/liminal/src/channel/types.rs:359`, `crates/liminal/src/channel/types.rs:506`, and `crates/liminal/src/channel/types.rs:578`; dedup claim/release and one further site at `crates/liminal-server/src/server/connection/services.rs:515`, `crates/liminal-server/src/server/connection/services.rs:533`, and `crates/liminal-server/src/server/connection/services.rs:951`. The `block_on` use in `crates/liminal/src/durability/store.rs:449-488` is inside a `#[cfg(test)]`-style test module, not production. | Determine whether this is a synchronous-backend contract assertion or a bounded application scan asking whether a future changed. Honest outcomes: prove one poll is the complete synchronous contract and fail immediately on `Pending`, or concede a real waker-driven executor/bridge. A smaller `MAX_POLLS`, another yield, or a sleep is not a retirement. The six production call sites above are the family's current boundary evidence; the r12 sweep must also **separately classify** the distinct tokio-runtime `block_on` sites at `crates/liminal-server/src/cluster/membership.rs:320-328` (startup) and `crates/liminal-server/src/cluster/sync.rs:282-290` (per-write, including a runtime-construction fallback) — a real-waker runtime park is a different mechanism from the NoopWaker scan and must not be conflated with it, in either direction. |

## D. Repeatable sweep protocol

**D.1 Run from the pinned tree and retain the raw result.** The r12 sweep runs
at a named commit over **every tracked Rust source in the repository** — the
sweep root is itself a LAW-2 claim, not an assumption. At `ce8814d`,
`git ls-files '*.rs' | grep -v '^crates/'` returns exactly one path:
`sdks/liminal-ts/wasm-src/src/lib.rs`, the TypeScript SDK's wasm shim — product
source, in scope. The sweep roots are therefore `crates sdks`. **Re-run that
enumeration at the sweep commit before the greps run; any newly appearing root
is classified and added, never silently omitted.** Tests are included because
they can bless a banned calling pattern. These searches intentionally
over-match; semantic review narrows them, never an exclusion added merely to
make the output small.

```text
git ls-files '*.rs' | grep -v '^crates/'   # root enumeration — classify every hit
rg -n --glob '*.rs' 'sleep\s*\(' crates sdks
rg -n --glob '*.rs' '\b(recv_timeout|try_recv)\s*\(' crates sdks
rg -n --glob '*.rs' '(?i)\b(poll|polling|poll_interval|interval|sweep|scan|heartbeat|backoff|reap)\b' crates sdks
rg -n --glob '*.rs' '\b(POLL[A-Z0-9_]*|[A-Z0-9_]*POLL[A-Z0-9_]*|Interval)\b' crates sdks
rg -n --glob '*.rs' 'Instant::now\s*\(' crates sdks
rg -n --glob '*.rs' 'WouldBlock|TimedOut|set_read_timeout|Ok\(None\)' crates sdks
rg -n --glob '*.rs' '(stop|shutdown).{0,80}\.load\(|\.load\([^)]*\).{0,80}(stop|shutdown)' crates sdks
rg -n --glob '*.rs' '(synthetic|probe|wake).{0,80}(write|send)|(write|send).{0,80}(synthetic|probe|wake)' crates sdks
rg -n --glob '*.rs' '\b(wait_timeout|wait_timeout_ms|park_timeout|sleep_until|sleep_till|deadline)\b' crates sdks
rg -n --glob '*.rs' '\b(interval|tick|timeout)\s*\(' crates sdks
rg -n --glob '*.rs' 'SystemTime::now\s*\(|\.elapsed\s*\(\)' crates sdks
```

The last three lines exist because wake-by-time primitives are their own
escape class: a condvar loop — `while !done { cv.wait_timeout(guard, dur) }` —
or a `thread::park_timeout` loop produces **no** `sleep`, `recv_timeout`,
poll-word, `Instant::now`, `WouldBlock`, or (with the flag under the mutex)
atomic-load hit. A primitive class absent from the pattern set is a blind
spot, not over-match headroom.

**D.2 Pair textual hits structurally.** For every `Instant::now`, atomic load,
`WouldBlock`, `TimedOut`, and `Ok(None)` hit, inspect the containing function and
its callers. Search the same function for `loop`, `while`, `for`, re-entry by a
caller, and timer/rearm registration. In particular, pair `WouldBlock` with any
sleep/backoff path, timeout-as-`None` with its caller's loop, and stop/shutdown
loads with socket-read loops. A split across helper functions is one family,
not a reason for two greps to miss it. Also inspect generated task/thread names
and external runtime adapters; application polling does not become legal when
wrapped.

**D.3 Turn every plausible hit into a row.** Append one §C row containing: (1)
family name and semantic job; (2) exact current file:line evidence; (3) present
cadence, wake/syscall cost, and aggregate formula or an explicit measurement
obligation; (4) existing socket or a proposed grep-able brief/socket name;
(5) FIXED behavior separated from OPEN mechanism candidates; (6) a
**Silence-attacking acceptance frame** with deletion targets, event races,
source grep, and a quiescence test; and (7) all four idle-cost answers. Even an
argued-legal caller deadline remains a row until review records why its timer
serves an admitted wait budget rather than change detection.

**D.4 Closure rule.** This document is never “done” by editorial declaration.
It closes only when the repeatable sweep results are attached to a named commit
and return **zero unretired families**: every textual hit is either deleted or
belongs to a reviewed event/deadline primitive whose non-change-detection job,
idle ceiling, tests, and certifying-pair sign-off are recorded. Any new r12 hit
reopens the document and appends a row mechanically.

## E. How the original shipped

These loops shipped under a review culture with substantial correctness
lenses—shutdown completion, prompt liveness errors, bounded waits, protocol
outcomes, and resource cleanup—but without a binding no-polling law and without
an idle-cost lens applied to every background loop. A short sleep looked like a
responsiveness bound, a read timeout looked like cancellability, and repeated
checks looked bounded in isolation. The missing control was a mandatory proof
of quiescence and aggregate idle cost: nobody had to answer what wakes on
silence, how that multiplies across instances, or which test pins the ceiling.

The gate now stopping recurrence is explicit: LAW-1 rejects every timer, poll,
sweep, scan, heartbeat, backoff, periodic reap, read-timeout wake, stop-flag
sample, or synthetic probe whose job is change detection; the house idle-cost
core forces the four answers—per-instance ceiling, aggregate ceiling,
quiescence test, and signed bound for by-design cost; and §D makes the search
repeatable and open-ended. A correctness argument without those controls can
no longer certify a loop, and a believed dependency without a LAW-2 citation
or genuinely-open socket cannot carry it across the gate.
