# SDK-011(b) — a setup deadline for the `RemoteConfig` leg: mechanism brief

**Status: DESIGN DRAFT for review — not a dispatch.** No executor is named, the
freeze doctrine applies to any build, and nothing here authorises one.
**Author:** Hermes Crumpet (liminal seat — designs and specs; cannot compile).
**Provenance:** split out of SDK-011 by ruling (b) at `c824f44`
(`docs/design/SDK-011-DISPATCH.md` §1.4). The PushClient leg is built
separately; this document is the *other* leg — the one that had **no existing
phase bound to make settable**, and therefore needs a mechanism designed
before anyone types code.

> ⚠️ **READ THIS AT HEAD.** Any commit hash citing this file names the tree it
> read, not the current ruling. Earlier SDK-011 documents contain superseded
> statements preserved as history; this file supersedes none of them and
> depends on `SDK-011-SCOPING.md` §3–§4 for reasoning not repeated here.

---

## 1. WHY THIS LEG IS A NEW MECHANISM, NOT A KNOB

The PushClient leg made an **existing** bound settable: `SETUP_TIMEOUT`
(`crates/liminal-sdk/src/remote.rs:64`) already delimited a setup phase, and
SDK-011 threaded a caller value to its consumption sites.

The `RemoteConfig` connect path has no such phase bound. Measured at these
coordinates (all verified at current main, `f16cfee` lineage):

- **The TCP establish step is unbounded by us.** Both transports call plain
  `TcpStream::connect` — `remote/tcp/connection.rs:57` and
  `remote/websocket/std_socket.rs:84`. No `connect_timeout`, so the budget is
  the OS default (macOS: on the order of a minute).
- **The handshake is bounded per-IO, not in total.** `IO_TIMEOUT`
  (`remote/tcp/connection.rs:31`, 5 s) is armed on the stream at `:66`/`:71`
  and governs **each** read and write. A server that trickles one byte every
  4.9 s keeps every individual read under the limit while extending the
  `Connect → ConnectAck` exchange indefinitely. **Five seconds per read is not
  a setup deadline; it is a cadence ceiling with no total.**
- **`IO_TIMEOUT` is steady-state, permanently.** It is restored after every
  scoped narrowing (`receive_with_timeout`, restore at `:344`, comment: "Always
  restore the steady-state timeout, even on error") and never disarmed. There
  is no moment at which the connection transitions out of a "setup" timeout
  regime, because no such regime exists.

So a deadline here is a **wall-clock budget that does not currently exist**:
armed at entry to `connect_*`, measured across the whole setup sequence,
disarmed on completion by restoring the steady state. That is a mechanism with
its own failure modes, which is why ruling (b) refused to let it ride the
PushClient dispatch.

## 2. THE SURFACE (ruled in SDK-011-DISPATCH §1.1 — unchanged, restated)

```rust
#[non_exhaustive]                       // estate rule: free at birth
pub struct PendingConnect { /* config + deadline */ }

impl RemoteConfig {
    pub fn with_setup_deadline(self, deadline: Duration) -> PendingConnect;
}

impl PendingConnect {
    // ⛔ ALL FOUR RETURN RemoteConfig — frame constructs handles from it at
    // three sites (attach.rs:145-146, attach.rs:202-204, leave.rs:124).
    pub fn connect_tcp(self) -> Result<RemoteConfig, SdkError>;
    pub fn connect_tcp_with_auth(self, auth_token: &[u8]) -> Result<RemoteConfig, SdkError>;
    pub fn connect_websocket(self) -> Result<RemoteConfig, SdkError>;
    pub fn connect_websocket_with_auth(self, auth_token: &[u8]) -> Result<RemoteConfig, SdkError>;
}
```

Names adjustable by the implementer; return type is not. This brief specifies
what the four methods **do** with the deadline.

## 3. MECHANISM SPECIFICATION

### 3.1 Semantics: one budget over the whole sequence

The deadline is a **single total wall-clock budget** covering everything
between entry to `connect_*` and a ready transport: address resolution
hand-off, TCP establish, WebSocket upgrade where applicable, and the
`Connect → ConnectAck` protocol handshake. Implementation shape: capture
`end = Instant::now() + deadline` at entry; every bounded step computes
`remaining = end.saturating_duration_since(now)`; `remaining == 0` before any
step is itself expiry — **fail fast, do not attempt the step with a zero
timeout** (a zero passed to `set_read_timeout` is an `Err` on some platforms,
not an instant elapse — do not lean on it).

### 3.2 TCP establish: `connect_timeout`, per candidate, after resolution

`TcpStream::connect_timeout` requires a resolved `SocketAddr`, so the
mechanism resolves first, then attempts candidates each under `remaining`.

**HONESTY SECTION (Tom's no-silent-tradeoffs rule): DNS resolution itself is
outside the budget.** `std` offers no timeout on `getaddrinfo`; resolution
blocks for however long the resolver takes, before our clock can bound
anything it does. The deadline therefore genuinely bounds *connect + upgrade +
handshake* and does **not** bound *resolution*. This limit goes in the
doc-comment of `with_setup_deadline` in plain words. The alternatives —
a resolver thread we abandon on timeout (leaks a thread per slow resolve), or
a third-party resolver dependency — are both worse than an honest sentence,
and either can be revisited on its own merits later.

### 3.3 Handshake IO: narrow to `remaining`, recomputed per IO

Before each setup-phase read/write, set the stream timeout to `remaining`
(recomputed each time, monotonically shrinking), so the per-IO bound and the
total bound are the same object during setup. On ANY exit from the setup
sequence — success or failure — restore `IO_TIMEOUT` on **both** read and
write before returning (the invariant `:344` already models, and the LAW-1
comment at `remote.rs:55-57` states: a deadline that outlives its exchange is
just a slower cadence).

### 3.4 The default path is byte-identical

`RemoteConfig::connect_tcp` (and the other three) **without**
`with_setup_deadline` keep exactly today's behaviour: plain
`TcpStream::connect`, `IO_TIMEOUT` per-IO, no total. The budget exists only
through `PendingConnect`. No constant changes value; no existing call
re-routes through new arithmetic it can observe.

### 3.5 ⛔ THE ERROR-TEXT TRAP, PRE-NAMED — DO NOT INTERPOLATE THE IO ERROR

This is the leg's sharpest edge and the most likely silent defect. Frame
string-matches liminal error **text** (`frame-conv/src/seam.rs:38-52`):
`"timed out"`, `"os error 35"`, `"Resource temporarily unavailable"` all
classify as **QuantumElapsed — a benign elapse**. A setup-deadline expiry is a
**setup failure**, the opposite classification.

The existing connect arms format the OS error straight into the description —
`format!("failed to connect to {address}: {source}")` at
`remote/tcp/connection.rs:57-59`. An expired `connect_timeout` returns an
`io::Error` whose Display **contains "timed out"** on the platforms we ship
on. ⇒ **naively interpolating `{source}` for a deadline expiry manufactures
the exact misclassification SDK-011 exists to prevent, with no compile error
and no test failure unless a test asserts the text.**

Therefore, binding:

1. Deadline expiry at ANY step is detected structurally (`ErrorKind::TimedOut`
   / `WouldBlock` from a step we armed, or `remaining == 0`) and reported with
   **our own fixed wording that never embeds the io error's Display**.
   Proposed: `setup deadline of {N} ms exceeded before ConnectAck` — carries
   the budget and the phase, contains none of frame's three matched
   substrings. Wording adjustable ONLY under the constraint that it must not
   contain `"timed out"`, `"os error 35"`, or `"Resource temporarily
   unavailable"` — and a unit test pins that property (§5).
2. Non-deadline connect failures (refused, unreachable, handshake rejection)
   keep today's formats **unchanged, byte for byte** — those strings are
   frozen (SDK-011-DISPATCH §2.1).

### 3.6 Carried constraints (unchanged from SDK-011-DISPATCH §2)

No existing error string modified · no reconnect inside a receive path (task
#40 tripwire) · `ConnectionPoolConfig::timeout_millis` untouched (ASK-2's
receive quantum) · no field added to `RemoteConfig` or `ConnectionPoolConfig`
(major) · no typed `SdkError` variant (rides the next major).

## 4. HOW TO VERIFY — the part that makes it real

- **Red-first** for each named unit.
- **Default-unchanged:** the plain `connect_*` path still shows OS-default
  establish + 5 s per-IO. The whole promise is "additive".
- **Deadline honoured, both transports:** a listener that `accept()`s and then
  sends nothing must fail in ~deadline wall-clock — not at 5 s × N reads. This
  is the slow-loris case §1 proves is unbounded today; it is the test that
  distinguishes a stored knob from a working one.
- **Already-expired budget fails fast** without arming a zero timeout.
- **Steady-state restore:** after a deadline-bounded connect **succeeds**, read
  and write timeouts both equal `IO_TIMEOUT`; after one **fails**, no stream
  survives to carry a narrowed timeout into steady state.
- **Text-pinning test:** the deadline-expiry message contains none of frame's
  three matched substrings (§3.5). This test is load-bearing, not cosmetic.
- **Battery with the pinned denominator** (`scripts/baseline-compare.py`),
  suite-count tell before trusting green, teed full logs, no silencing
  redirections on evidence.

## 5. WHAT THIS BRIEF IS NOT

Not a dispatch: no executor, no branch, no GO. Any build happens post-freeze
under the says-before/named-GO doctrine, on a fresh branch off landed main,
with the review floor (≥1 named Sol/Fable). The PushClient leg neither waits
for this leg nor rides with it. Ships, when it ships, as part of a
`liminal-sdk` 0.5.x additive release — version decision is separate and is
not made here.
