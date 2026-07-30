# SDK-011 — dispatch brief (build + battery)

**From:** Hermes Crumpet (liminal seat — designs and specs; cannot compile).
**To:** an executor seat that compiles.
**Scope basis:** `docs/design/SDK-011-SCOPING.md` at this commit. Read it first;
this brief is the build order, that document is the reasoning and the refusals.
**Ruled by:** Cally (stack lead), on Athena's measurements at frame venue
`origin/fix-wave/republish-readiness @ 3def5a1`.

---

## 1. WHAT TO BUILD

**A settable per-connection setup deadline, additive, shipping as `liminal-sdk`
0.5.2.** The ratified 5 s stays the default; nothing existing changes shape.

### 1.1 New surface

```rust
// NEW TYPE — born #[non_exhaustive] (estate rule: free at birth, major forever after)
#[non_exhaustive]
pub struct RemoteConnect { /* config + deadline */ }

impl RemoteConfig {
    /// Additive. Carries an explicit setup deadline into the connect step.
    pub fn with_setup_deadline(self, deadline: Duration) -> RemoteConnect;
}

impl RemoteConnect {
    pub fn connect_tcp(self) -> Result<RemoteConfig, SdkError>;
    pub fn connect_tcp_with_auth(self, auth_token: &[u8]) -> Result<RemoteConfig, SdkError>;
    pub fn connect_websocket(self) -> Result<RemoteConfig, SdkError>;
    pub fn connect_websocket_with_auth(self, auth_token: &[u8]) -> Result<RemoteConfig, SdkError>;
}
```

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

### 1.3 Internal threading

`SETUP_TIMEOUT` (`crates/liminal-sdk/src/remote.rs:64`, `pub(crate) const`, 5 s)
is consumed at six sites across `tcp/subscription.rs`, `tcp/push_client.rs`,
`websocket/subscription.rs`. Thread the caller's deadline to those sites;
**keep the constant as the default** so every existing path is byte-identical in
behaviour.

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
6. **DO NOT add a field to `RemoteConfig` or `ConnectionPoolConfig`** — all-public
   fields, zero private, so any added field is **major**.
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
