# SDK-011 — dispatch brief (build + battery)

> ## ⛔ READ THIS DOCUMENT AT HEAD, NEVER BY AN EARLIER COMMIT
>
> This brief exists at more than one commit and **the earlier versions lead to
> building forbidden shapes.** `742fa58` predates §1.1.1 (the return-type
> constraint — its absence leads to breaking frame at three sites) and was cited
> at least once in the dispatch chain in that stale form. Versions before the
> errata below contain **§1.3's false site list.** §1.1.1 and §1.3 are
> load-bearing. Cite this file by the commit you READ, and read it at HEAD.

**From:** Hermes Crumpet (liminal seat — designs and specs; cannot compile).
**To:** an executor seat that compiles.
**Scope basis:** `docs/design/SDK-011-SCOPING.md` at this commit. Read it first;
this brief is the build order, that document is the reasoning and the refusals.
**Ruled by:** Cally (stack lead), on Athena's measurements at frame venue
`origin/fix-wave/republish-readiness @ 3def5a1`.
**Amended:** 2026-07-31 after Artemis's five errata (all verified independently
at this seat) and Hermes's ruling on the `RemoteConfig` leg (§1.4).

---

## 1. WHAT TO BUILD

**A settable per-connection setup deadline, additive, shipping as `liminal-sdk`
0.5.2.** The ratified 5 s stays the default; nothing existing changes shape.

> ### ⚠️ SCOPE NARROWED BY RULING (2026-07-31): THE PUSHCLIENT FAMILY ONLY.
> The `RemoteConfig` leg is SPLIT OUT — see §1.4 for the ruling and the
> measured reason. §1.2's "both families" requirement is superseded: it assumed
> both families shared a threadable setup mechanism, and they do not.

### 1.1 New surface

```rust
// NEW TYPE — born #[non_exhaustive] (estate rule: free at birth, major forever after)
#[non_exhaustive]
pub struct PendingConnect { /* config + deadline */ }

impl RemoteConfig {
    /// Additive. Carries an explicit setup deadline into the connect step.
    pub fn with_setup_deadline(self, deadline: Duration) -> PendingConnect;
}

impl PendingConnect {
    // ⛔ EVERY ONE OF THESE RETURNS RemoteConfig. SEE 1.1.1 — NOT NEGOTIABLE.
    pub fn connect_tcp(self) -> Result<RemoteConfig, SdkError>;
    pub fn connect_tcp_with_auth(self, auth_token: &[u8]) -> Result<RemoteConfig, SdkError>;
    pub fn connect_websocket(self) -> Result<RemoteConfig, SdkError>;
    pub fn connect_websocket_with_auth(self, auth_token: &[u8]) -> Result<RemoteConfig, SdkError>;
}
```

### 1.1.1 ⛔ THE RETURN TYPE IS `RemoteConfig`. RETURNING THE NEW TYPE BREAKS FRAME.

**Athena's constraint, measured at frame's callsites against the published SDK.
This would have surfaced as a failed build on the one executor handoff we have.**

**`RemoteConfig` is not a builder discarded at connect — it is the handle's
constructor argument and it carries the live transport:**

- `remote.rs:145` — `connect_tcp(mut self) -> Result<Self, SdkError>` mutates
  `self.transport` and **returns Self**.
- `participant.rs:267` — `RemoteParticipantHandle::new(config: &RemoteConfig, ..)`
- `participant.rs:276` — `::restore(config: &RemoteConfig, ..)`

Frame consumes it exactly that way at **three** sites — `attach.rs:145-146`
(`new`), `attach.rs:202-204` (`restore`), `leave.rs:124` — and frame's own
`attach::connect` returns `Result<RemoteConfig, AttachError>`, handing the
**connected** config onward by reference.

⇒ **if `connect_*` returned `PendingConnect`, frame stops compiling at all three
handle-construction sites.** That is a **type in a signature**, not an
incidental construction style — **a real break, and it does NOT fall to the
caution we applied to `attach.rs:33`.** Returning `RemoteConfig` leaves frame
untouched and makes the intended chain a one-liner at `attach.rs:32-49`:

```rust
RemoteConfig::new(..)?.with_setup_deadline(deadline).connect_tcp()?
```

**On the name:** Athena flagged (correctly) that a type called `RemoteConnect`
whose methods return `RemoteConfig` reads as a surprise. **`PendingConnect` is
chosen so the return type is the obvious one** — a *pending connect*, when
performed, yields the connected config; it was never a thing you were meant to
keep. **The name is adjustable by the implementer; the return type is not.**

### 1.2 ⚠️ THE SECOND FAMILY — DO NOT SKIP IT

Frame's three callsites split **two ways**. `RemoteConfig` serves only one of
them. **`PushClient` is the family on the boot-storm path** —
`frame-host/src/announcer.rs:211` and `frame-view/src/publish/transport.rs:37`
call `PushClient::connect` / `connect_with_auth`.

**The announcer is the entire motivating case for a settable deadline (16-wide
contended boot). If only the `RemoteConfig` family gains one, SDK-011 misses the
case it exists for.** So `PushClient` needs additive equivalents:

```rust
impl PushClient {
    pub fn connect_with_deadline(address: &str, deadline: Duration) -> Result<Self, SdkError>;
    pub fn connect_with_auth_and_deadline(address: &str, auth_token: &[u8], deadline: Duration)
        -> Result<Self, SdkError>;
}
```

If a cleaner additive shape presents itself in the code, take it — **the
requirement is that both families can set a deadline, not these exact names.**

### 1.3 Internal threading — CORRECTED (Artemis errata 1–3, verified here)

`SETUP_TIMEOUT` (`crates/liminal-sdk/src/remote.rs:64`, `pub(crate) const`, 5 s)
is consumed at **FIVE production sites plus ONE test assertion**
(`websocket/subscription.rs:564` is `assert_eq!`, not a consumer) — the original
"six sites" was the count a builder works from, and it was wrong. Thread the
caller's deadline to the five; **keep the constant as the default** so every
existing path is byte-identical in behaviour.

> ⛔ **AND NONE OF THOSE SITES IS ON THE `RemoteConfig` CONNECT PATH.** The
> original §1.3 instructed threading `SETUP_TIMEOUT` to satisfy §1.2's
> both-families requirement — **an internal contradiction: built exactly as
> written, the `RemoteConfig` half would have accepted a deadline, stored it,
> and threaded it into code `connect_tcp` never executes.** A knob stored and
> never read — the precise defect §3's second property was written to catch,
> specified into the brief by its own author.
>
> Measured (Artemis, Cally, and this seat independently, all at `92e65ce`):
> `RemoteConfig::connect_tcp` → `tcp/connection.rs:56`, `connect_websocket` →
> `websocket/std_socket.rs:82`; both paths' only bound is **`IO_TIMEOUT`** — a
> separate, privately-declared 5 s **PER-IO, STEADY-STATE** bound
> (`tcp/connection.rs:31`, `std_socket.rs:26`), restored to the constant after
> every conversation drain (`connection.rs:344`, *"Always restore the
> steady-state timeout, even on error"*). **No wall-clock deadline, no setup
> window, no end-of-setup disarm anywhere on that path** — while the
> `PushClient` family has all three. A family with a setup phase ends it
> explicitly; **the missing disarm is the structural signature that
> `RemoteConfig` has no setup phase to bound.**

### 1.4 RULING (Hermes, 2026-07-31): THE `RemoteConfig` LEG IS SPLIT OUT

**Option (b): this brief ships the `PushClient` leg alone; the `RemoteConfig`
leg gets its own brief and its own red-first design pass.** Because:

1. **There is nothing to thread.** A setup deadline on that family means
   **BUILDING a wall-clock bound over the handshake** with steady state left at
   `IO_TIMEOUT` — a new mechanism, not a parameter. A different size of job
   than this brief priced, riding under a brief that believed it was cheap.
2. **The motivating crash is fixed by the `PushClient` leg on its own.** The
   announcer (boot storm) is on `PushClient`; `RemoteConfig`'s consumer
   (frame-conv attach) is user-driven and uncontended (Athena's (c)).
3. **The defect on the `RemoteConfig` path is real and deserves its own name:**
   today's "5 s" there is **5 s PER READ over N sequential handshake I/Os —
   unbounded in total.** That is the boot-storm failure mode itself, and the
   mechanism cannot express a setup deadline at ANY value. Fixing it as a
   rider would bury it.

**Artemis's law, adopted into this document: AN ASSIGNMENT'S TIMING IS NOT ITS
SCOPE.** When the thing assigned is a property of a long-lived object, where it
is set says nothing about how long it governs — **ask what reads it and what
resets it.** `IO_TIMEOUT` is assigned at connect because that is when the socket
exists; it governs steady state forever after.

---

## 2. ⛔ BINDING CONSTRAINTS — VIOLATING ANY OF THESE IS A CONSUMER-VISIBLE BUG

1. **DO NOT MODIFY ANY EXISTING ERROR MESSAGE STRING.** Frame string-matches
   liminal's error **text** (`frame-conv/src/seam.rs:38-52`, venue), branching on
   `"timed out"`, `"os error 35"`, `"Resource temporarily unavailable"`.
   **A reword shipped as a patch is a silent behaviour change in a consumer with
   no semver protection and no failure mode available to any check.**
2. **NO NEW MESSAGE MAY CONTAIN `"timed out"`** unless it denotes a genuine
   benign elapse on a path reaching `RemoteParticipantError::Transport`.
3. **The deadline changes WHEN a timeout fires, never WHAT IT SAYS.**
4. **DO NOT let a reconnect run inside a receive path** — task #40, a
   load-bearing invariant. Breaking it silently redirects setup errors into
   frame's classifier **with an empty frame diff.**
5. **DO NOT touch `ConnectionPoolConfig::timeout_millis`.** Refused with
   measurement (§3.3.1): callers pass 10 ms and 1 ms into it, and frame declares
   that slot as ASK-2's **receive quantum**. Wiring it would set a 10 ms connect
   deadline in a boot storm. It is a separate, logged finding.
6. **DO NOT add a `setup_deadline` field to `RemoteConfig` or any field to
   `ConnectionPoolConfig`** — **but the original stated reason was HALF FALSE
   (Artemis erratum 4, verified here).** `RemoteConfig` has four public **and
   TWO private** fields (`transport` at `remote.rs:102`; `websocket` under
   `#[cfg(feature = "std")]` at `:106-107`), so downstream struct literals are
   **already impossible** and an added field would be MINOR, not major. The
   all-public/zero-private claim is true only of `ConnectionPoolConfig`
   (`connection/pool.rs:16`). **The prohibition stands on the ground that was
   never semver: a setup deadline is meaningful only DURING connect, and
   `RemoteConfig` outlives connect** — it is the handle's constructor argument,
   held after the transport is live; a deadline field would sit there
   permanently describing a phase that has ended. That is why the pending type
   is right, untouched by who can construct the struct.
   **Builder carry-forward:** a test needing a `RemoteConfig` cannot build one
   by literal — go through `RemoteConfig::new` and the connect path.
7. **No typed `SdkError` variant.** Frame cannot consume one today (all three
   callsites collapse the error immediately and none branches). That work rides
   the next major with `#[non_exhaustive]`.

---

## 3. HOW TO VERIFY — THE PART THAT MAKES IT REAL

- **Red-first.** Each named unit gets a failing test before the fix.
- **A test that proves the DEFAULT is unchanged** — an existing path still gets
  5 s. This is the regression that matters most: the whole promise is
  "additive".
- **A test that proves a supplied deadline is actually honoured** at both
  families, not merely accepted. *A knob that is stored and not read is the exact
  defect logged one struct over.*
- **Battery with the pinned denominator** via `scripts/baseline-compare.py`.
  **Suite-count tell before trusting green.**
- **`scripts/pin-registry-gate.py`** must pass — note `Cargo.lock` still pins
  **yanked `spin 0.9.8`** (task #39). If the gate REDs on that, it is a known
  finding, not your regression; report it, do not silently `cargo update`.
- **Teed full logs.** ⛔ no `2>/dev/null`, no `|| true`, no `-q` on anything
  whose output is evidence.

---

## 4. PROCESS

- **Fresh branch off landed `main`.** ⛔ YG-560: no merge/rebase/cherry-pick/pull
  **into** the feature branch.
- **Default target dir only** (shared target dirs race; suite-count drift is the
  tell).
- **Conflict rule: STOP > silent workaround.** If anything here contradicts what
  the code actually does, **stop and report** — this brief was written by a seat
  that could not compile it, so the code is the authority and I expect to be
  wrong somewhere.
- **Review floor: ≥1 named Sol/Fable reviewer.**
- **No version bump, no tag, no publish.** ⛔ Publishing needs Tom's own word.
  This lands on `main` as unreleased work; the 0.5.2 cut is a separate decision.
