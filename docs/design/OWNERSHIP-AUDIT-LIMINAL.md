# Ownership-Leverage Audit — liminal

**Directive provenance.** Tom, via Waffles (DM `74cd95b6`): "We've written our
entire own stack from the ground up… where are we using that to our advantage
and where are we not… We're going for generational change, not just our own
version of something." Two questions: (Q1) where does whole-stack ownership
already let liminal do what library-gluers cannot; (Q2) where is liminal
copying a convention some library or std forced on it — missed leverage, at
the file/design-decision level. Changing existing rulings is explicitly in
scope for Q2 findings.

**Method.** Two read-only sweeps at main `54ac0ee` (one over the
protocol-native replay/cursor/durable surfaces, one an external-dependency
convention inventory), folded with this-era receipts already verified at the
bytes. Every claim carries a `file:line` coordinate from one of those sweeps.
Honesty note: coordinates were confirmed at `54ac0ee`; anything cited from
another repo (frame, haematite, beamr) names that repo and is a this-era
receipt, not re-verified in this pass.

---

## Q1 — Where whole-stack ownership already pays

### 1.1 Consumption and replay are wire vocabulary, not client convention

The protocol crate makes consumer progress a first-class citizen of the wire,
which no library-gluer can do because they don't own the wire:

- Cursor advance is a numbered opcode: `ParticipantAck` (`0x0004`) carries
  "greatest continuously available sequence being acknowledged"
  (`liminal-protocol/src/wire/request.rs:50,:57-58`); marker consumption is a
  second, distinct opcode (`MarkerAck`, `:78`).
- The attach receipt IS the replay-point handshake: `AttachBound` returns the
  persisted participant cursor on the wire
  (`liminal-protocol/src/wire/response.rs:927,:940-943`), and a fenced
  recovery attach's accepted marker "is also the resulting persisted cursor
  by construction" (`:993`). Enrollment is definitionally cursor-zero —
  `EnrollBound::persisted_cursor` is a `const fn` returning 0 (`:653-656`);
  a wrong value is unrepresentable.
- Cursor monotonicity is enforced in the wire type constructors: `AckGap`
  refuses `through_seq > current_cursor` (`response.rs:1366,:1377`),
  `AckRegression` refuses `< current_cursor` (`:1410,:1422`). An
  out-of-order ack cannot be *encoded*. Kafka-family clients validate this
  server-side at best; here the type system does it before bytes exist.
- Retention loss is an in-band sequenced fact: `HistoryCompacted` travels in
  the same ordered delivery stream as application records, carrying
  `abandoned_after`/`abandoned_through`/`physical_floor_at_decision`
  (`liminal-protocol/src/wire/push.rs:64,:107`). Consumers see the gap as
  data, never as a silent hole.

### 1.2 Unforgeable authority — capabilities enforced across crate seams

Because protocol, server, and SDK are co-owned, invariants are enforced with
move-only types and `compile_fail` doctests rather than documentation:

- `RecipientAckObligations` "cannot be assembled from independent public
  fields" (`lifecycle/cursor_facts.rs:53,:35-49`, compile_fail doctest) —
  delivery obligations are an unforgeable capability.
- `MarkerDeliveryProjection`: "storage cannot assemble a delivery from
  unrelated raw marker fields" (`lifecycle/operations/marker_drain.rs:47-73`,
  two compile_fail doctests including "Debug output is not a parser").
- `ObserverProgressProjection`: consuming code "cannot construct one from a
  guessed maximum" (`lifecycle/observer_recovery.rs:10-26`).
- The lifecycle event vocabulary states "no path exists from a decoded or
  constructed payload to a typed lifecycle state" — deserialization is not
  authority (`lifecycle/operation_event.rs:1-25`).
- Server-side, the same discipline: `HeldParticipantHead` is "move-only by
  construction… only the owning connection process can resume it"
  (`liminal-server/src/server/connection/participant_delivery.rs:30`), and 12
  trybuild compile-fail cases pin non-Clone proof tokens and one-shot
  consumption (`liminal-server/tests/trybuild/`).

A gluer whose storage layer is a foreign library cannot make "storage can't
forge a delivery" a compile-time fact.

### 1.3 LAW-1 held end-to-end: event-driven with zero timers, provably

- Reconnect is permit-based and timer-free by construction:
  `ReconnectFreshEvent` is the closed set of the only three events that may
  mint a permit, none a timer (`liminal-protocol/src/client/reconnect.rs:29`);
  "the retained name ReconnectDelayResult carries an event, never a delay"
  (`client.rs:22-23`); SDK transports repeat the stance verbatim ("no
  automatic retry and no timer", `liminal-sdk/src/remote/websocket.rs:201`).
- Subscriber liveness is structural, not sampled: the channel actor LINKS to
  the beamr subscription process and the EXIT signal removes the dead
  subscriber — "there is NO weak-Arc polling"
  (`liminal/src/channel/subscription.rs:1-11`). This works because the
  scheduler is ours.
- The participant handler: "everything is event-driven… no timer, sweep, or
  polling loop exists" (`liminal-server/src/server/participant/production/handler.rs:1-12`);
  cold replay's empty page "is end-of-stream, never a timer or polling
  signal" (`participant/conversation_stream.rs:1-6`).
- Where a timer facility is genuinely absent, the absence is surfaced as a
  typed error, not papered over ("connection scheduler has no timer facility
  for reply deadlines", `connection/process.rs:647`).
- The discipline has an enforcement arm: LAW-1's oracle floor is ABSENCE
  PROOFS with a per-file sweep ledger
  (`docs/design/W4-LAW1-POLLING-RETIREMENT.md:104-107`,
  `docs/design/LAW1-POLLING-RETIREMENT.md`).

No one gluing an event bus out of tokio + a broker client can *rule* "no
timers anywhere" — their dependencies arm timers internally. We can, and
mostly do (residuals in Q2.4).

### 1.4 The owned core is dependency-free all the way down

- `liminal-protocol` has ZERO runtime dependencies and is `#![no_std]`
  (`liminal-protocol/src/lib.rs:1,7`); its only dev-dep is proptest.
- The TS SDK has zero runtime deps (`sdks/liminal-ts/package.json`).
- The wasm bridge takes no crates at all — it inlines the protocol module by
  path (`sdks/liminal-ts/wasm-src/src/lib.rs:1-2`).

The protocol is ours from the wire bytes to the browser with nothing rented
in between. That is the structural precondition for everything in 1.1–1.3.

### 1.5 Consumer-callsite-driven design — possible only with named consumers

SDK-011 was specified against frame's exact callsites (frame's
`handle/attach.rs:145-146,:202-204`, `handle/leave.rs:124`), with binding
constraints invisible from the producer side. F8 moves the protocol
0.3.2→0.4.0 with the zero-consumer break verified by sibling control —
possible only because the consumer set is closed and enumerable. The estate
re-pins atomically (Minerva's 11-crate re-pin at `85426e1` executed same-day
in 0.5.1). Anonymous-consumer projects cannot do any of this.

### 1.6 Convention refusal at rented boundaries

Where we do rent a library, its defaults are overruled at the seam:
tungstenite's 64 MiB `max_message_size` "is not a legal state here" — both
ends pin to the liminal frame bound
(`liminal-sdk/src/remote/websocket/std_socket.rs:48-54`,
`liminal-server/src/server/connection/websocket.rs:111-117`); `wss://` gets a
typed refusal with TLS ruled to a named fronting proxy (`std_socket.rs:56-60`);
the server drives tungstenite's *raw* handshake pieces specifically to omit
`Sec-WebSocket-Extensions` (`websocket.rs:33-46`). And test evidence is built
to the same bar: socket2 exists as a dev-dep solely to make
WouldBlock-with-residue certain by construction
(`liminal-server/Cargo.toml:40-43`).

### 1.7 Cross-crate co-design at owner speed

The reclamation design reuses haematite's own failed-create
delete-under-lock discipline (haematite `db.rs:130-132`), and the
`delete_if_unlocked` API split — mechanism tests locks, caller owns the
orphan judgment — was co-designed with haematite's owner inside an hour
(Apollo's #67). F8's ruled recovery guarantee (a fixed binary RESTORES a
poisoned store, because boot repair re-runs the same measurement) is only
rulable when store, protocol, and server are one estate.

---

## Q2 — Where liminal copies a convention it doesn't have to

Ranked by live cost. Each item: the copied convention, its coordinates, and
the ownership answer.

### 2.1 The stringly seam — frame branches on our error TEXT (#1)

frame classifies liminal failures by string-matching `SdkError` display text
(frame-conv `src/seam.rs:38-52`: "timed out" / "os error 35" / "Resource
temporarily unavailable" → QuantumElapsed). Both sides are ours; the typed
seam (ASK-2) is designed and unbuilt. The cost is already live and
compounding: two dispatch briefs this era carry frozen-string binding
constraints, and SDK-011B §3.5's sharpest edge — an expired-deadline io-error
Display putting "timed out" into a setup failure that frame's seam then
classifies benign — exists *only* because of this seam. A library-gluer MUST
string-match a foreign error; we do it by omission. Sibling instances of the
same habit at seams we own: `binding_fate.rs:373`'s
`.map_err(|_| OwnerTransition)` hides five causes (F8 names carrying the
inner error as a requirement); haematite `db/helpers.rs:23`'s catch-all
destroys the tree-error type (#31/#56, re-pin trigger = BOTH fixed);
jsonschema compile errors flattened to `String` via `to_string()`
(`liminal/src/channel/schema.rs:150-160`). One habit, four coordinates:
typed information is destroyed at boundaries we control.

### 2.2 Half the delivery surface is not protocol-native — while the other half is

The repo contains two disjoint delivery paths, stated as deliberate
(`participant_delivery.rs:1-5`):

- **Participant conversations**: durable ack frontier, marker acks, replay
  from the persisted cursor on rebind (`ParticipantOfferedProgress` — "a
  different binding epoch discards this progress and restarts from the
  durable recipient acknowledgement frontier",
  `liminal-server/src/server/participant/publication.rs:22-28`). Fully
  protocol-native, the 1.1 showcase.
- **Channel subscriptions** (`Frame::Deliver`): per-subscription
  `delivery_seq` restarts at 1 on re-subscribe
  (`connection/apply.rs:526-531`), no durable cursor, credit "advisory"
  (`liminal-sdk/src/remote/tcp/subscription.rs:50-53`), and **resume is
  unimplemented on both wire transports** — the SDK returns a typed
  refusal, "re-subscribe to trigger server replay"
  (`remote/tcp/mod.rs:269-280`, `remote/websocket.rs:322-331`). The SDK's
  `ResumeRequest`/`SubscriptionRecovery` types exist with no wire frame
  behind them (`liminal-sdk/src/connection/recovery.rs:33,:53`); a
  `WireFrame::Resume` exists only in the SDK's internal enum
  (`remote/protocol.rs:398`), with no `Resume` in the core `Frame`.

This is the shape of a broker-client convention: a subscription is a socket,
and the consumer's position is the consumer's problem. The ownership answer
is already in-repo, one path over — the durability substrate even has the
literal primitive (`recover_cursor_with_replay`,
`liminal/src/durability/recovery.rs:87`). The v1/v2-credit boundary
(`frame.rs:437-448` calls `delivery_seq` "the anchor the future ack/resume
(A1 v2 credit) protocol builds on") is honest, but the missed leverage is
that we are treating protocol-native consumption as a *second phase* when it
is the thing we uniquely can do on day one.

### 2.3 Deadline and cancellation are socket-option arithmetic, not protocol

`SETUP_TIMEOUT` exists because std::net offers only a per-read timeout, not a
per-exchange deadline (`liminal-sdk/src/remote.rs:44-64` — and what it
replaced "was never chosen": a 100 ms reader poll composing "by accident,
into a 100 ms-per-read fatal deadline", `:59-62`). The whole
arm/disarm choreography (`websocket/subscription.rs:216→:292`,
`tcp/subscription.rs:239,:246→:147`) plus platform-ambiguity handling
(`WouldBlock|TimedOut` treated as one outcome, `std_socket.rs:234`) is us
programming *around* the rented model. Cancellation likewise:
`try_clone_stream` exists purely so an owner can `shutdown()` a socket to
unblock a reader (`std_socket.rs:133-144`) — the wakeup primitive is socket
teardown because the model has no cancel. SDK-011's two legs are patches to
this convention. The ownership answer: deadlines and cancellation as
*protocol* concepts — a server-cooperative setup budget and a cancel frame —
instead of every client independently doing socket-option arithmetic. Both
SDK-011 briefs stay correct as interim mechanism; this names where the
mechanism should eventually live.

### 2.4 Two self-declared LAW-1 residuals whose fixes are parked in our own stack

- The channel delivery pump still rides the every-slice busy loop, with the
  R3 notifier already installed and the park-flip commit named as the
  deletion (`liminal-server/src/server/connection/delivery.rs:9-16` — the
  doc comment itself calls the every-slice assumption "the permanent-runnable
  cost being removed").
- `wait.rs` polls our own scheduler's process table at 10 ms (5 hits, all in
  `liminal/src/channel/actor/wait.rs`) because beamr lacks `watch_exit` —
  and beamr is OURS; the API ships in 0.17.0 (F7 gated on its publish).

Neither is news; both are ledgered. The audit point is the pattern: when the
blocker is inside our own estate, "waiting for upstream" is a convention we
imported from a world where upstream was a stranger.

### 2.5 Production stores in system temp under tempfile's discard-errors Drop

The entire reclamation saga (task #3, design certified at `54ac0ee`) is the
bill for inheriting a test-fixture convention into production:
`open_ephemeral` mints stores in system temp via `tempfile`
(`liminal/src/durability/store.rs:407,:444-446`), whose `TempDir::drop`
discards `remove_dir_all`'s Result (verified at tempfile-3.27.0
`src/dir/mod.rs`) — so ~470 MiB of `durable=false` payloads sat orphaned in
an unowned population, and no observed exit path cleans. The ownership
answer is already designed: an explicit `EphemeralRoot` the server owns,
boot-sweep reclamation with fd-liveness, and a clean path that calls
`TempDir::close()` and logs the error (task #5). Copied convention: "temp
dirs are the OS's problem." Ours: an owned population with a named owner.

### 2.6 Auth is a byte-blob that defaults open

`connect_with_auth(&[])` means open access — an empty token "behaves
identically"; `SubscriptionStream` hardcodes an empty token, making
token-gated buses unusable via the SDK subscribe path (task #2, verified at
the bytes this era). Auth presence is a caller convention, exactly as it
would be if we were wrapping someone else's client. Owning the wire means
auth can be a protocol *state* (an attach that names its credential class,
with "open" an explicit variant), not a maybe-empty field.

### 2.7 Semver fear pointed inward

The protocol crate sat frozen at 0.3.2 across eras; `non_exhaustive`-at-birth
was adopted only this week; `SdkError` still isn't `non_exhaustive` (the
breaking split is documented in SDK-011 scoping). The estate re-pins
atomically (1.5), so internal breaking changes are cheap — we have been
pricing them like crates.io-stranger changes. Real strangers do exist (the
crates are public), but the enumerable-consumer verification F8 used shows
the honest price of a break, and it is low. F8's 0.3.2→0.4.0 is the first
correction of this reflex.

### 2.8 Pins held as prose where we could hold them as tests

`store.rs:398-401` pins guard-dir behaviour to "haematite 0.4.1
`db/startup.rs`" while the lock holds 0.7.0 — an instruction, not a control
(contrast the writer.lock name, which got a conformance test in the
reclamation design). Same family: trybuild snapshots pin type-level
invariants to rustc's diagnostic *wording* (12 cases,
`liminal-server/tests/trybuild/`) — the same disease as 2.1, aimed at rustc;
churn-prone but at least failing loud. Candidate: conformance-pin the
guard-dir behaviour the way writer.lock is pinned.

### 2.9 Smaller instances, named for completeness

- **Uncounted pre-upgrade handshake window**: the ws listener's pre-upgrade
  window is "UNCOUNTED and UNDEADLINED… out of contract for untrusted
  networks" (`connection/websocket.rs:21-31`, restated at
  `config/types.rs:204-206`) — ledgered post-demo hardening; a wire library
  that owned its handshake the way we own frames would deadline it.
- **Two observability systems**: our own `liminal::tracing`
  (`liminal/src/tracing.rs:1-16`) coexists with the external `tracing` crate
  as "sink of last resort" (123 macro sites); neither is ruled canonical.
- **tokio accommodated rather than owned**: the cluster layer builds a
  2-worker multi-thread runtime because current-thread deadlocks bring-up
  (`cluster/membership.rs:568-573`) plus a runtime-agnostic probe bridge
  (`cluster/sync.rs:264-292`) — scoped and documented, but it is the one
  place an unowned scheduler lives inside our server while the rest of the
  estate runs on beamr.
- **async-trait boxed futures** on `DurableStore`
  (`liminal/src/durability/store.rs:22,:92,:353`) — a macro convention where
  an owned trait could choose its dispatch shape.

---

## Ranked shortlist — what to do first

1. **Build ASK-2, the typed seam** (2.1). Cheapest item on the list, highest
   live cost, and every era it waits mints new frozen-string constraints.
   Retires SDK-011B §3.5's trap class and the text-pin apparatus with it.
2. **Rule that channel-subscription consumption becomes protocol-native**
   (2.2) — cursor + resume for `Frame::Deliver` subscriptions, folding the
   orphaned SDK resume types into a real wire frame. Rides the v2-credit
   design that `frame.rs:437-448` already anchors; F8's 0.4.0 cut proves the
   protocol can move.
3. **Land the parked LAW-1 deletions** (2.4): the park-flip commit (notifier
   already installed) and, on beamr 0.17.0's publish, the `wait.rs`
   poll retirement (F7). Both are deletions of conventions whose
   replacements we already built.
4. **Execute the ephemeral-ownership design** (2.5): EphemeralRoot + boot
   sweep + task #5's close-and-log — already certified, awaiting the named
   GO chain.
5. **Make auth a protocol state** (2.6), fixing task #2 in passing.
6. **Adopt the inward semver price** (2.7): `non_exhaustive` the SDK error
   surface at the next natural break; keep using F8-style sibling-control
   verification as the standard cost-of-break proof.
7. **Longer arc: deadline/cancel as protocol concepts** (2.3) — after
   SDK-011's interim legs land, design the server-cooperative version.

**Rulings this would touch** (per the directive, changing them is welcome):
the v1/v2-credit phasing (item 2 pulls consumption forward), SDK-011's
mechanism seat (item 7 relocates it), and the protocol version-freeze reflex
(item 6 formalizes its retirement, already begun by F8).
