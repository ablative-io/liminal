# Silent-failure inventory — Leg E (`fix/silent-failures`)

Baseline for every cite: liminal @ `c921827`. Derived by a four-way sweep of all five
workspace members (server core / server connection+participant / protocol / sdk+wasm),
every row verified at the bytes. Granularity: one row = one code location from the
sweep records; families list their members explicitly. Sweep near-miss ledgers
(examined-and-excluded sites with reasons) ship with the battery evidence.

Scope ruling (lane owner): Leg E FIXES only the original-class set below; family (a)
is PROMOTED to its own lane unfixed; family (b) is already fixed on Leg C; family (c)
and the whole protocol crate are untouched (0.3.2 unchangedness is load-bearing for
the pending tag). Everything else is inventoried here for row-opening.

## Exact counts

- FIXED in this leg (original class, red-first where derivable): **32 sites**
- Family (a) poisoned-lock masking: **27 sites** (24 open + 3 addressed-in-leg with
  logging; one verified full implementation exists in branch history at
  `3899279`+`06e0f83`, reverted at `5a87b69` per the scope ruling — cherry-pickable)
- Family (b) SDK fabricated-success transport: **5 sites + 1 consequent** — FIXED
  PENDING LANDING on Leg C (`liminal-sdk/src/remote/protocol.rs` `not_connected`,
  Leg C bytes :120-148, verified by the lane owner)
- Family (c) protocol deferred-invalidate codec: **3 named shapes + 23 family
  members** — untouched
- Protocol crate, other rows (untouched): **14 named + 2 family members**
- SDK remaining open rows: **32**
- Server remaining rows: **20 documented-silence comments landed in-leg** +
  **17 accepted/plain rows** (existing comments own most; 3 plain rows flagged)
- Excluded as not-silent-failures: PoisonError::into_inner recover-and-continue
  idiom family (uses the data; distinct from family (a)'s read-poison-as-absence),
  EINTR retries, and the sweeps' near-miss ledgers (~60 sites with correct handling)

## 1. FIXED in this leg (32)

Batch 1 (12): env.rs:126 typo'd LIMINAL_* key warn [MANDATORY]; apply.rs:151-162
stray/unknown inbound frame-kind warn, no-teardown kept [MANDATORY];
services_schema.rs:30-41 declared-but-unloaded schema warn; metrics.rs:34-44 init
no-registry warn; metrics.rs:83-92 registration-failure warn; shutdown.rs:55-57 +
58-63 poisoned-wait error-log (behaviour kept); shutdown.rs:104-108 poisoned-notify
error-log + notify-anyway (guard recovered, ordering preserved); shutdown.rs:133-135
signal-worker join debug→warn; membership.rs:465-471 consumer spawn-failure error;
membership.rs:475-480 consumer-panic warn at stop; sync.rs:272-276 cross-node
distribution write-failure warn.

Batch 2 (20): conversation.rs:208-217 + 275-281 Disconnected-is-a-crash-observation
(both legs; poison halves NOT touched — family (a)); conversation.rs:231-236
unserviceable state query reads failed, warned; supervisor.rs:969 reclaim-reactor
exit names its reason; supervisor.rs:1240-1251+1275 truthful cause on refused
control push (incl. caller-visible text: "process is not live" → "the control was
not published"); listener.rs:92-94, health/endpoint.rs:109-111,
ws/listener.rs:160-162 self-connect interrupt failure warns (named shutdown-deadlock
condition); listener.rs:198-199, health/endpoint.rs:219-221, ws/listener.rs:197
reserve-fd acquisition warns; listener.rs:264-267 reserve re-establishment warn;
health/endpoint.rs:234-246 admitted-slot clone-failure warn; apply.rs:218 + :393
partial-activation drift warns before Close; apply.rs:238 rejection-encode-failure
warn; publication.rs:114 + :386 undelivered READY-wake warns; membership.rs:262-267
peer_names renders `<atom N>` instead of dropping; handler.rs:409-411 repair failure
composed into the reported state error.

Red discipline: true observed reds for env, apply (both split tests), schema,
metrics-register, shutdown-poison (both halves incl. parked-waiter wake), membership
join-panic, sync write (loopback link, killed write half), conversation (3 reds +
control), reclaim-reactor + control-push cause, partial-activation, publication
(both legs), peer_names, handler composition (retracted-and-replaced pair — see
branch history at d06cbb7 for the honest retraction). Honest no-reds, stated in
commit messages: metrics-init branch, signal-worker join level, membership
spawn-failure, all six lifecycle warns (OS-level fault injection disproportionate),
rejection-encode (unconstructible by the wire types).

## 2. Family (a) — poisoned-lock masking (27; PROMOTED, unfixed)

Global fact for every severity read: no custom panic hook and no catch_unwind exist
in production. The poisoning panic prints ONCE to raw process stderr via the default
hook — typically uncollected by tracing pipelines — and nothing links that print to
later reads. What each mask hides is not the panic but the conversion of poisoned
state into a benign answer, at unbounded later times. "Unobservable?" below answers:
is the MASKED CONSEQUENCE observable by any other signal — no means this site is the
only witness and it lies.

Open (24):
- supervisor.rs:2968-2969 active_count→0 on poison. Unobservable: YES; shutdown
  drain reports complete under live connections. SEVERITY: HIGHEST.
- supervisor.rs:2992-2997 remove→None on poison. Unobservable: YES; record, live
  socket fd, gauge decrement and admission slot leak permanently; reclaim_terminated
  early-returns before its own warn. SEVERITY: HIGHEST.
- supervisor.rs:1662-1667 ready_waker→None on poison. Unobservable: YES; every wake
  install degrades server-wide to "no wake" (root of three downstream sites).
  SEVERITY: HIGH.
- supervisor.rs:2935 pop_control→None. Unobservable: YES; queued Push/ForceClose/
  NotifyShutdown never drained; push awaiters time out. SEVERITY: HIGH.
- supervisor.rs:2954-2957 remove_control→false→"published". Unobservable: YES —
  documented deliberate misreport with no operator signal. SEVERITY: HIGH.
- supervisor.rs:1765-1773 flush-wake pid list→empty. Unobservable: YES; shutdown
  flush barrier "woke everyone" having woken no one; accepted publishes lost at
  shutdown. SEVERITY: HIGH.
- ws/supervisor.rs:177-187 stop() skips socket interruption. Unobservable: PARTIAL
  (shutdown hang is observable, unattributed). SEVERITY: HIGH.
- ws/supervisor.rs:204-216 JoinHandle dropped on poison. Unobservable: YES (panicked
  worker's own error! never fires). SEVERITY: MED.
- supervisor.rs:2862-2866 contains→false. Unobservable: PARTIAL (pushes fail with a
  wrong "disconnected" reason). SEVERITY: MED.
- supervisor.rs:2941-2945 has_control→false. Unobservable: YES (connection parks
  with undrained control). SEVERITY: MED.
- supervisor.rs:2772-2776 timers never cancelled. Unobservable: YES (unbounded timer
  leak firing at dead pids). SEVERITY: MED.
- supervisor.rs:1746-1749 quiesced→false. Unobservable: PARTIAL (shutdown burns full
  deadline — fail-safe direction). SEVERITY: LOW-MED.
- ws/supervisor.rs:225-238 complete_worker lost. Unobservable: YES. SEVERITY: MED.
- ws/supervisor.rs:242-245 completion never counted, notify_all never runs.
  Unobservable: YES (waiters sleep forever). SEVERITY: MED.
- ws/supervisor.rs:252-263 drain joins nothing. Unobservable: YES. SEVERITY: MED.
- ws/supervisor.rs:372-376 inflight entry leaks. Unobservable: YES. SEVERITY: LOW-MED.
- conversation.rs:208-217 / :220-226 / :275-281 poison halves — crash observation
  lost or unreplayable. Unobservable: YES (crash reads as healthy; the in-leg
  Disconnected fix covers the sender-gone path but not poison). SEVERITY: HIGH (3
  sites). NOTE: a verified recover_lock implementation for these exists in history
  (9524e9b's reverted half).
- publication.rs:315-319 deregister skipped. Unobservable: YES (leak + blocks a
  later legitimate registration via DuplicateRegistration). SEVERITY: MED.
- process.rs:412-418 poison folded into "capability not configured", Ok(()).
  Unobservable: YES (participant connection silently mute for life). SEVERITY: HIGH.
- apply.rs:485-489 + :617-621 poison-caused None waker → no notifier installed.
  Unobservable: YES (parked connection never woken for delivery/replies).
  SEVERITY: HIGH (2 sites).
- sdk tcp/push_client.rs:270-276 poisoned writer lock skips half-close →
  RST path drops unfanned publishes. Unobservable: YES. SEVERITY: MED.

Addressed-in-leg (3): shutdown.rs:55-57, :58-63 (poison logged, behaviour kept),
:104-108 (poison logged AND notify recovered — the only site where masking was
also functionally repaired, pre-ruling, batch 1).

Cherry-pick note for the promoted lane: `3899279`+`06e0f83` (supervisor+ws
recovery, recover_lock pub(super)+context+logging) and the reverted half of
`9524e9b` (conversation.rs) are one unit — conversation.rs imports the reworked
recover_lock. Revert commit `5a87b69` documents the coupling.

## 3. Family (b) — SDK fabricated success (5+1; FIXED PENDING LANDING, Leg C)

remote/protocol.rs:95-105 publish synthesizes Accept; :127-137 subscribe Ok-no-op;
:139-149 send_conversation Ok-no-op; :170-180 resume Ok-no-op; remote.rs:127
default-install reachability. All return not_connected(...) on Leg C (:120-148,
verified at the lane owner's bytes). Consequent: handles.rs:182-185 instantly-
terminating "successful" subscribe stream resolves for the default transport
(SdkSubscription::error path). NOT open; do not double-fix.

## 4. Family (c) — protocol deferred-invalidate codec (3 shapes + 23 members; UNTOUCHED)

Design verified closed at the bytes: decode_server_value_body is the sole entry and
always reaches finish() (server_codec.rs:69-91); substituted values cannot escape a
completed decode. Named shapes: take_string invalid-UTF-8→"" (server_codec.rs:
350-362); take_generation zero→Generation(1) — A FORGED VALID CAPABILITY VALUE —
(server_codec.rs:302-310 + codec.rs:949-956); EpochAhead missing-current→0
(server_codec.rs:2529-2533). Family members (23 further invalidate-and-substitute
sites): server_codec.rs 1615, 1655, 1678, 1693, 1716, 1729, 1811, 1887, 1907, 1957,
1966, 2136, 2140, 2145, 2148, 2151, 2186, 2268, 2318, 2462, 2469, 2522, 2531.
FLAG: the forged-Generation shape deserves a design look when this crate next opens.

## 5. Protocol crate — other rows (14 + 2 members; UNTOUCHED, crate frozen at 0.3.2)

client/inbound.rs:264-269 invariant break reported as Applied (only success-shaped
failure path in the crate); client/inbound.rs:299-327 24-variant exhaustive no-op
arm over server refusals (compile-error-on-drift is the guard; value reaches caller
via into_parts); lifecycle/claim_frontier.rs:1967 sequence-exhaustion→cursor None
(guard at :1957-1959 errors one allocation later; final-allocation case unreported);
claim_frontier.rs:6879/6883 ordinal underflow clamps to 0 inside error payloads;
claim_frontier.rs:6892/6896 + observer_recovery.rs:630-632 + ordinary_record_
projection.rs:1289/1293 (2 family members) saturating narrowing helpers (fail-closed
but fabricate reported counts); cursor_facts.rs:648-651 facts dropped for absent
participants (departed vs bug indistinguishable); cursor_facts.rs:661-664 + :888
consumed-fact list discarded (informational Vec, not a Result); client/barrier.rs:
163-165 issued-flag skip with no else on a one-use send authority; barrier.rs:
139-140 saturating_sub on authorization at abort; client/replay.rs:324-330 map_or(0)
+ no-else on the same invariant (authority-0 effect would present as Started);
replay.rs:381-384 un-issue skip still reports Parked; closure_accounting.rs:479-482
out-of-domain debt→u64::MAX on the wire (constructor refuses upstream). Also noted
(not counted): ~55-site map_err(|_| flat-variant) detail-discard category — loud but
cause-erasing; storage restore reports one variant for eleven distinct checks.

## 6. SDK remaining open rows (32)

exchange.rs:161, :204, :332 unsolicited deliveries dropped during round trips
(inconsistent with subscription.rs:377-379 which retains — FLAG); exchange.rs:291
close skipped on driver refusal; tcp/connection.rs:152-163 + :358-362 pooled-
subscribe deliveries discarded (comments own it, incl. buffer-overflow teardown);
tcp/subscription.rs:341-346 reader-death error identity lost; :366-369 unknown
frames swallowed; :199-219 Drop discards teardown writes + reader PANIC (join.ok);
ws/subscription.rs:473 DriverOutput::Refused swallowed — STRONGEST UNANNOTATED SITE,
every sibling consumer errors (FLAG); :436-440 + :460-462 decode detail dropped at
close; :459 unknown frames (TCP parity); :469 LossRecordOutcome::Refused discarded —
reconnect permit may never mint, client disconnected forever (FLAG); :479-483
close_link refusal skip; :316-325 Drop discards shutdown + reader panic;
connection.rs:303-306 close-refusal skip THEN false CloseCompleted record (FLAG);
push_client.rs:693-696 fatal reader errors incl. an invariant break authored loud
then swallowed (FLAG); :689-692 unexpected frames ("for this spike"); :535-537 Drop
reader panic; :262-266 teardown pushes discarded (documented); flush.rs:295-300
Disconnected folded into unresolved (documented T1); std_socket.rs:182-199 close
failures recorded to an unread field (documented); adapter.rs:135-142 signal dropped
on "impossible" borrow conflict; :152-155 whole queue stranded, uncommented twin;
handles.rs:302 request() overwrites unconsumed reply — correlation mispairing as
success (FLAG); :344-346 + :496-498 lifecycle() always-empty streams; embedded.rs:
201-207 + :213-219 null backends accept-and-discard (Default-installed); :314
embedded subscribe empty stream; wasm frame_bridge.rs:54 + :55 unhandled variants →
reason_code 0 = "no error" to JS (FLAG — TS-facing contract).

## 7. Server remaining rows (20 commented in-leg + 17 accepted/plain)

Documented-silence comments landed in-leg (20 sites, commit 3aacdb7): sync.rs
write_frame no-connection drop; process.rs:323/:458/:472/:722 close-path drains +
:1013 unwrap_or(&[]); ws/process.rs:552/:898 drains + :584-588/:908-912 close-flush
pairs; services_cluster.rs:95-103 clock-fault constant-fold owned honestly;
supervisor.rs:1109 peer_addr triage cost; apply.rs:532-535 wire-silent unsubscribe;
runtime.rs:132-147 error precedence; outbound.rs:192 non-Binary branch;
websocket.rs:347 unwrap_or(&[]); dispatch.rs:316-321/:331-338 no-op trait defaults;
incarnation_stream.rs:730-732 generation-0 indistinguishability.

Accepted with pre-existing comments (12): membership.rs:169-173 INV-ALTERNATION
absorb (module no-logging law); services.rs:622-625 dedup epoch-0 anchor;
services.rs:1032 (+participant saturation twins) count saturation; supervisor.rs:
2578 reply-slot send; supervisor.rs:1063-1070 detached reclaim JoinHandle (LAW-1;
panic loud on stderr); conversation.rs:329 close().ok() on Failed; main.rs:41-42
RUST_LOG fallback (doc'd; pre-subscriber); main.rs:44-54 try_init; metrics_route.rs:
18-19 200-empty (now loud at cause via metrics warns); endpoint.rs:94-101 socket
shutdown discard; endpoint.rs:253-260 per-request debug level; endpoint.rs:137-142
(+listener twin) drop-path debug.

Plain rows, no fix and no comment this leg (5): env.rs:60-63 non-UTF-8 key skip
(unreachable; asymmetric with env_string); health/endpoint.rs:262 (+listener.rs:215)
EINTR spin invisibility under signal storm (correct retry, no counter);
fate_occurrence.rs:169 bare catch-all Ok(()) in durable-row dispatch (hardening =
explicit variant enumeration, deferred); supervisor.rs:2465-adjacent none;
listener.rs peer half — reserved for row-opening as the lane owner sees fit.

## 8. Battery bindings honored by this run

Pre-stated denominator in the run header, compared post-run. JSON-stream/Summary
reconciliation asserted: started == ok + failed + ignored, and JSON ignored ==
Summary skipped; disagreement is RED. Model disclosure: batch 1 implemented by a
default-inherit subagent (Fable), batches 2-3 by the pinned opus5-implementer type,
per the estate model rule as available at dispatch time.
