# In-process transport (SDK loopback) — design + build brief

Status: BUILD BRIEF (was: sketch r1). Ruled at ablative/docs
`design/face-substrate/DESIGN-DRAFT-r2.md` §12 item 2 (rev `86a458a`), which
names this document's sketch revision (`design-50-loopback-sketch @ 28f6d85`)
as the ruling's artifact. BUILD GO given 2026-08-08 (Waffles, relaying Tom's
read of the hardened draft), with four brief-time rulings folded into §9
below.

**The guarantee, in the hardened draft's own language (r2 §5, binding on
every doc comment this build writes):** the loopback secures the **RECORD
PATH** — it carries the exact framed wire image through the same preflight,
participant gate, and token compare, so no append reaches the record except
through the same door. The mount itself is **TRUSTED-CODE**: a co-resident
extension reaches the host's heap, descriptors, and store handle without ever
calling the loopback, so the record vouches for a trusted-code mount only as
far as the host process is trusted. **The word "isolated" is banned from this
build.** Every append admitted through the loopback carries a mount
attestation from the admitting door (see §10 for what that means at liminal's
layer). Code unchanged across all three mounts (in-process / local process /
remote).

All `file:line` citations below are at liminal `e63b3e9` (main).

## 1. The one decision everything follows from: BYTES, not frames

The loopback is a **byte-level** transport: an in-memory bounded duplex byte
queue that carries the exact framed wire image, with encode and decode running
on both sides, unchanged. Not a frame-object channel, not a request-object
channel.

Why this is forced by the hard line: the server's inbound path is
`preflight_generic_bytes` (frame-limit refusal read from the raw header,
`server/participant/transport.rs:30`) → `liminal::protocol::decode` →
auth gate → `apply_frame` (`server/connection/apply.rs:29`), and the
participant gate **re-materializes the 10-byte wire header** and runs
`gate_inbound` on the byte image (`transport.rs:113-155`). A frame-level
loopback would skip the preflight, the codec, and the gate's byte
re-materialization — three places where a divergence class lives (a frame that
encodes differently IS a different frame). Byte-level makes "indistinguishable
on the record" structural rather than asserted, and it makes the discriminating
test (§6) meaningful: byte-identical in, byte-identical out, byte-identical
record.

Consequence accepted openly: the loopback pays encode+decode. What it removes
is the syscall, the copy through kernel buffers, the fd lifecycle, and the
network round trip. That is the round-trip cost Tom's extensions-as-members
concern names; the codec cost is small, symmetric across mounts, and is the
price of the no-side-door guarantee.

## 2. Where it sits — both sides are third siblings, not new seams

The codebase has already grown a second transport once, and the shape held:
`WebSocketConnectionProcess` (`server/connection/websocket/process.rs:104`)
shares `apply_frame`, `ConnectionRuntime`, `ConnectionProcessState`, the
subscription/publication servicing, and the pending-reply table, replacing only
the read/write halves. The loopback is the third member of that family, on both
ends:

**Server side** — `server/connection/loopback/` (inside the `connection`
module family, exactly as `websocket/` is, because `spawn_transport_connection`
(`supervisor.rs:517`) is `pub(super)` on purpose: module privacy is what makes
the runtime, admission counter, incarnation authority, and registry unreachable
from outside — that privacy wall IS the no-side-door enforcement, and the
loopback lives inside it rather than widening it):

- `LoopbackConnectionProcess`: sibling of `ConnectionProcess`
  (`process.rs:45`); reads from the inbound ring of a `LoopbackDuplex` instead
  of `read_available(&mut TcpStream, ..)` (`process.rs:1008`); everything from
  `process_buffer` (`process.rs:908`) down is shared, not copied.
- A `spawn_loopback_connection` on the supervisor that mirrors
  `spawn_connection` (`supervisor.rs:1087-1156`) minus the socket steps:
  **same `try_reserve_admission`** (`:1099` — in-process connections consume
  the same admission slots; capacity has no side door either), **same
  `allocate_connection_incarnation`** (`:1118` — a real durable incarnation, so
  participant binding, resume, and fate records work identically), same
  registry record — with `fd_guard: None` (the guard exists to keep an fd
  alive for readiness deregistration, `supervisor.rs:3021`; a loopback has
  nothing to guard).

**Client side** — `remote/loopback/` in `liminal-sdk`, sibling of `tcp/` and
`websocket/`: a `LoopbackRemoteTransport` implementing the existing
crate-private `RemoteTransport` trait (`remote/protocol.rs:28`) and
`ParticipantRemoteTransport` (`remote/protocol/participant.rs:17`), plus
`RemoteConfig::connect_loopback(&EmbeddedServer)` mirroring `connect_tcp`
(`remote.rs:145`). The trait stays crate-private and the `transport` field
stays private — a consumer cannot hand-roll a transport that skips anything,
which is the client half of no-side-door.

`EmbeddedServer` is the one genuinely new public object: a handle owning a
`ConnectionSupervisor` built via the existing embedding constructor
`with_services` / `with_services_and_auth` (`supervisor.rs:110`, `:129` — the
in-tree `SocketFixture` at `production/e2e_socket_fixture.rs:333-383` already
proves the full production stack constructs and runs with no listener). Its
only connection-granting surface is `connect_loopback`; it exposes no state,
no handler, no store. How `EmbeddedServer` relates to the existing
`SdkConfig::Embedded` arm (`remote/config.rs:8`) is a brief-time question —
they may unify, but nothing in this sketch depends on it.

## 3. What "internal channel" means concretely

`LoopbackDuplex`: two **bounded** SPSC byte rings (client→server,
server→client), synchronous (the SDK is deliberately sync and tokio-free on
the connection path — `remote/tcp/mod.rs:8-15`; the server's connection path
likewise), each end exposing:

- `write(&[u8]) -> io::Result<usize>` — partial writes when the ring is
  nearly full, and a `WouldBlock`-equivalent when full. **Bounded is
  load-bearing**: a socket gives backpressure through kernel buffers and
  `WouldBlock`; an unbounded queue would give the loopback mount a semantics
  no other mount has (infinite buffering) and an unbounded idle-memory class.
  The `OutboundWriter` budget/partial-write logic (`outbound.rs:194`) then
  behaves identically over the ring.
- `readable_bytes() -> usize` non-consuming — the `final_probe` equivalent
  (`process.rs:660-675` peeks the socket before parking; the `else` branch at
  `:673` shows the shape is already pluggable).
- close semantics: dropping either end makes the peer's reads return EOF and
  writes return `BrokenPipe` — mapping to the same close/fate paths a socket
  hangup drives.

**Wake — the NO-POLLING answer.** The loopback has no fd, and the beamr
readiness facility is `RawFd`-only (`process.rs:616`). The design does NOT arm
readiness and does NOT return `Continue` to spin (the busy loop this codebase
deliberately retired — `process.rs:333-345`). Instead the writer wakes the
reader: each `write` into a previously-empty ring enqueues the peer
connection's READY atom via the existing `ReadyWaker` vocabulary
(`server/connection/wake.rs`) — the exact mechanism that already wakes parked
connections for participant publications, subscription inboxes, and reply
deadlines. The transport is TOLD, never polls. Client-side blocking reads use
a condvar with `recv_timeout` mirroring the socket timeouts (`remote.rs:64`,
`tcp/connection.rs:31`).

## 4. Admission without a listener

The listener's only jobs are `accept()` and `spawn_connection(stream)`
(`listener.rs:191-227`). `connect_loopback` replaces exactly that pair: create
the duplex, call `spawn_loopback_connection` with the server half, hand the
client half to the transport. Everything after is the identical admission
path, unskipped:

1. Admission slot reserved (`try_reserve_admission`) — or refused, exactly as
   a socket connect is refused at capacity.
2. Client sends `Frame::Connect { auth_token }` as bytes; server runs
   `connect_once` → `connect_response` (`apply.rs:167`, `:346-406`):
   constant-time token compare, version negotiation, participant capability
   bit iff a participant service and durable incarnation exist. **An embedded
   caller with the wrong token is refused on its own loopback** — admission is
   admission.
3. Enrollment → CredentialAttach → acks → RecordAdmission all flow as gated
   participant frames through `dispatch_generic_frame` (`dispatch.rs:806`).

Nothing on this path reads socket facts: identity is purely protocol-level
(tokens/secrets in frames; `peer_addr` is diagnostics-only). The loopback
record carries `peer_addr: None` — visible in diagnostics as the mount's
honest description, invisible to semantics.

## 5. Scope fence for v1

- **In:** the full participant contract over the loopback — connect,
  admission, enrollment, attach/detach, acks, record admission, receive,
  resume/reconnect (`ParticipantRemoteTransport` complete).
- **Named out, with reason:** `PushClient` (`tcp/push_client.rs:544`) and
  `SubscriptionStream` (`tcp/subscription.rs:226`) bypass the transport trait
  and open their own sockets today. Generalizing them is real work that is
  orthogonal to the participant contract the ruling names; they stay TCP-only
  in v1 and are the first follow-on. (Task #2's hardcoded-auth defect lives in
  `SubscriptionStream` — same neighborhood, same follow-on.)
- **Not attempted:** any cross-process isolation story. An in-process
  participant that panics takes the process with it; memory isolation is the
  process's own. That is inherent to the mount, not a defect of it — stated
  so nobody reads the loopback as a sandbox.

## 6. Test story

**The discriminating test (the one Waffles named, and the build's acceptance
gate):** one participant implementation, driven twice — through the loopback
and through a real socket (the `SocketFixture` / `SdkSocketFixture` pattern,
`e2e_socket_fixture.rs:227`, `:260`) — asserting **byte-identical record
outcomes**: the durable op-log rows and canonical shell event bytes the two
runs produce, and the response frames byte-for-byte. Precedent already
in-tree: `ws_and_tcp_connect_responses_are_byte_identical`
(`tests/ws_transport_e2e.rs:367`) does exactly this discrimination for the
WS/TCP pair; the loopback joins that parity family as the third column.

Supporting pins, per estate law:

- **Idle-cost pin** (no-silent-tradeoffs): a parked loopback connection
  schedules zero wakeups and holds only its bounded rings — proven
  keepalive-honest (unrelated counters grow while the loopback's stay flat).
- **Backpressure pin:** a full ring produces the same observable semantics as
  a full socket buffer (partial write / would-block / no desync), mirroring
  `oversize_frame_survives_wouldblock_boundary_and_no_desync`
  (`tests/subscription_e2e.rs:527`).
- **No-side-door pin (structural):** the loopback modules export no type that
  reaches state, and the discriminating test's record-equality is the
  behavioral half of the same guarantee.
- **Admission pin:** wrong token over loopback is refused byte-identically to
  wrong token over TCP.
- **Close/fate pin:** dropping the client half drives the same fate path as a
  socket hangup, leaving the same record.

## 7. Honesty section — what this design trades

- **Timing semantics shift.** A zero-latency transport races differently.
  One known site: `CONVERSATION_DRAIN_TIMEOUT` treats 250ms of silence as
  drain-complete (`tcp/connection.rs:313-330`) — over a loopback, "silence"
  arrives instantly and means something stronger. Carried into the brief as a
  named review item, not silently absorbed.
- **The outbound sink should be abstracted, not copied.** `OutboundWriter`
  is nailed to `&mut TcpStream` (`outbound.rs:194`) and the WS transport
  already had to grow a parallel `WebSocketOutbound` — a third copy is the
  default outcome unless the sink becomes `dyn Write` at this build. The
  brief should include that refactor rather than accept copy three.
- **The fd-guard registry slot goes `Option`-shaped** for loopback records;
  teardown paths that assume a guard need the audit, not an assumption.
- **Two constructions must not drift:** `spawn_connection` and
  `spawn_loopback_connection` share admission/incarnation/registry steps by
  construction (shared helper), not by parallel maintenance.

## 8. Build order (binding)

1. Outbound sink abstraction (`dyn Write`) + `final_probe` pluggability —
   pure refactor, gate must hold at baseline.
2. `LoopbackDuplex` with bounded rings, wake-on-write, close semantics + its
   own unit pins (backpressure, EOF, wake).
3. Server side: `spawn_loopback_connection` + `LoopbackConnectionProcess` +
   the `MountKind` fact (§10).
4. Client side: `LoopbackRemoteTransport` + `EmbeddedServer` +
   `RemoteConfig::connect_loopback`.
5. The discriminating parity test + the six supporting pins (§6 five + §10's
   mount-fact pin).
6. Follow-ons named, not built: push/subscription generalization (inherits
   task #2's hardcoded-auth defect explicitly), loopback drain-timeout
   tightening (§9.1), TS/Gleam SDK trait mirrors (r2 §12.2: Rust first).

## 9. Brief-time rulings (2026-08-08, binding on the build)

1. **Drain-timeout semantics: v1 INHERITS socket semantics unchanged.** The
   stronger meaning of loopback silence is documented (§7), not exploited —
   no code path may treat loopback silence differently from socket silence
   in v1. Tightening is a named follow-on.
2. **`OutboundWriter` sink abstraction is IN scope. Copy three is refused** —
   a diff showing a third parallel writer means the build is wrong.
3. **Scope fence ratified as drawn (§5):** full participant contract IN;
   `PushClient` / `SubscriptionStream` OUT of v1, their generalization the
   first follow-on, task #2's auth defect inherited by that follow-on
   explicitly.
4. **`SdkConfig::Embedded` shape: this build's call, one constraint —
   `EmbeddedServer`'s only granting surface stays `connect_loopback`, under
   whatever config shape is picked.**

## 10. The mount attestation at liminal's layer

r2 §5: "every append admitted through the loopback carries a mount
attestation from the admitting door." The record being attested is the
CONSUMER's (manifold's estate record); the admitting door for those appends
is the consumer's participant machinery riding on liminal. Liminal's
obligation is therefore to supply the **unforgeable mount fact**, not to
stamp its own rows:

- The connection registry record and the participant handler context carry a
  `MountKind` (`Tcp` / `WebSocket` / `Loopback`), set by the server at spawn
  from its own knowledge of which door admitted the connection. **No client
  input influences it** — it is not negotiated, not carried in any frame,
  and not readable from anything the client sends.
- The consumer's door reads `MountKind` from the handler context and stamps
  its append. Liminal's own durable op-log rows carry NO mount field — which
  is what keeps §6's discriminating test exact: liminal record outcomes stay
  byte-identical across mounts, and the mount fact lives only in the context
  surface where the consumer's door reads it.
- **Sixth supporting pin:** the handler context reports `Loopback` for a
  loopback-admitted connection and `Tcp` for a socket-admitted one, and a
  client presenting any frame content cannot move it.

If the consume side needs the fact somewhere other than the handler context,
that is a declaration-time conversation with Waffles, not a silent widening.
