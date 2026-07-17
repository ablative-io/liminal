# liminal browser/WebSocket transport — goal session (folded r1.1)

**Status:** folded r1.1 — pre-dispatch complete (2026-07-17). The tear rulings
and boundary-read dispositions are integrated into the requirement bodies.
Dispatch remains gated on the client unit landing (see "Required client-unit
input").

**History:** 2026-07-17 — folded r1.1 integrates the decided pre-dispatch read.

**Liminal evidence pin:** `origin/main` at
`2e5a731b5f009b0cac2b8c28b90f9b1245372732`.

**Required client-unit input:** `feature/lp-client` at
`4118aa11d3e5cc1149e160d445f8bb2ed14fe840`. Implementation MUST NOT begin from
a base that does not contain that commit or its reviewed successor.

**Governing frame input:** frame `docs/component-contract` at
`3a07ce0653dbce657e19d011a389ae96979550b2`, especially
`docs/planning/DECISIONS-2026-07-12.md:22-41` and
`docs/design/COMPONENT-CONTRACT.md:68-74,407-416,451-465,610-615`.

## Goal and governing decisions

Add a real WebSocket route through liminal so a browser can be a first-class
liminal participant. The route carries the canonical liminal wire protocol; it
does not translate that protocol into a browser protocol. Native server and SDK
work prove the transport boundary first. A separately dispatched wasm leg then
binds the same transport-neutral driver to `web-sys::WebSocket` and the same
conversation surface.

Three inputs are binding, not suggestions:

1. **D3 veto — PARTICIPANT, not gateway.** Tom's 2026-07-17 veto says the
   browser attaches to the stack network, owns a mailbox/conversation as a
   first-class liminal participant, and uses liminal-sdk over WebSocket behind
   the same conversation handle; native-first then wasm is sequencing, never a
   reduction of scope (frame
   `docs/planning/DECISIONS-2026-07-12.md:27-41`). Therefore TypeScript MUST NOT
   reimplement the conversation protocol, correlate liminal requests, apply
   reconnect rules, or translate browser JSON into liminal operations. The
   frame component contract repeats that the thin TypeScript shell has no
   independent authorization logic and “does not reimplement liminal’s
   conversation protocol” (`COMPONENT-CONTRACT.md:453-465`).
2. **Component-contract ruling 9 — server authority.** The browser binding is a
   server-issued, instance-scoped opaque credential, delivered in the live
   manifest over an authenticated participant connection, redeemed to establish
   the subscription, freshly checked on every reconnect, and revocable on the
   server. It is never a prop or a client-selected conversation address
   (`COMPONENT-CONTRACT.md:68-74,407-416,610-615`). Section R4 pins the concrete
   additive liminal wire types and bytes that ruling 9 deliberately left to this
   brief.
3. **The client unit owns transport-fate rules.** At `4118aa1`,
   `EstablishedConnectionTransportFate`, `ReconnectFreshEvent`,
   `ReconnectAggregate`, and `record_transport_fate` are closed typed machinery
   (`crates/liminal-protocol/src/client/reconnect.rs:4-43,61-90,181-188`);
   `redeem_attempt` must run before a real open and `record_attempt_fate`
   consumes typed `Connected`/`Failed` afterwards (`:406-488,490-558`). Detach
   replay likewise accepts a typed fate at
   `crates/liminal-protocol/src/client/replay.rs:336-368`. The transport work
   WIRES socket facts into `ClientParticipantAggregate`/`ReconnectAggregate`; it
   never re-derives permit, retry, replay, correlation, or timer rules.

## Verified evidence at the pins

These findings were re-read or re-run; scout wording was not accepted on trust.

- **The SDK is synchronous and std-driven today.** `liminal-sdk` declares no
  tokio dependency (`crates/liminal-sdk/Cargo.toml:11-23`). Its TCP module says
  the API is synchronous and uses blocking `std::net::TcpStream` without an
  async runtime (`src/remote/tcp/mod.rs:8-15`); subscription and push paths use
  `std::sync::mpsc` and reader threads
  (`src/remote/tcp/subscription.rs:27-30,152-158` and
  `src/remote/tcp/push_client.rs:30-34,200-219`).
- **The narrow native second-transport seam already exists.** The complete
  internal `RemoteTransport` trait is at
  `crates/liminal-sdk/src/remote/protocol.rs:26-84` and
  `RemoteConversationHandle` stores it as `Arc<dyn RemoteTransport>` at
  `src/remote/handles.rs:257-279`. `RemoteConfig` installs the TCP implementation
  behind that object at `src/remote.rs:100-137`. This is the native seam to
  prove, not permission to put browser policy inside the trait.
- **One canonical wire frame is self-delimiting.** Every frame has a ten-byte
  header; bytes 6..10 are `payload_length`
  (`crates/liminal/src/protocol/frame.rs:8-10,198-213`). The codec reads type,
  flags, stream ID, and length, computes `10 + payload_length`, and returns the
  consumed size (`protocol/codec.rs:82-134`). `PayloadReader`/`PayloadWriter` use
  big-endian integers (`protocol/codec/payload.rs:86-94,209-222`). Therefore one
  encoded liminal frame maps exactly to one logical WebSocket binary message,
  with the real ten-byte liminal header retained.
- **Protocol application is separated, socket ownership is not.**
  `apply_frame(...) -> FrameAction` is the decoded-frame seam
  (`crates/liminal-server/src/server/connection/apply.rs:29-34` and
  `state.rs:81-95`). The TCP listener hands `TcpStream` directly to its
  supervisor (`server/listener.rs:125-140`), while supervisor, process, read
  loop, and outbound writer remain concretely `TcpStream`-typed
  (`connection/supervisor.rs:166,797`, `connection/process.rs:2,44,768-806`,
  `connection/outbound.rs:26,196`). A WebSocket acceptor is consequently a
  sibling listener/process owner that reuses semantic application, not a
  retrofit of the TCP byte-stream path.
- **The no-default wasm floor is real, with one correction to the scout.** On
  the pin, both
  `cargo check -p liminal-protocol --target wasm32-unknown-unknown
  --no-default-features` and the corresponding `liminal-sdk` command exit 0.
  `liminal-sdk` makes `liminal` optional under `std`
  (`crates/liminal-sdk/Cargo.toml:11-20`). A default-feature wasm check exits
  101 in that optional `liminal -> beamr` graph. The rerun reported both
  `zstd-sys` and `region -> cranelift-jit` as target-incompatible branches;
  `cargo tree -p liminal-sdk --target wasm32-unknown-unknown -i zstd-sys` and
  `-i region` place both below `liminal`. The scout's narrower “only at
  zstd-sys” claim is therefore NOT repeated. R3 protects the demonstrated
  no-default floor; it does not pretend the default graph already builds.

## Non-negotiable transport contract

- A **WebSocket message** below means one reassembled logical message, not one
  RFC 6455 fragment. Exactly one binary message contains exactly one canonical
  liminal frame: header plus the declared body, no stripping, batching, prefix,
  base64, JSON, or trailing frame.
- Text messages, empty messages, malformed headers, truncated bodies, declared
  lengths beyond the active liminal frame bound, and valid-frame-plus-trailing-
  bytes are typed protocol failures and close the connection under the same
  terminal supervision discipline as malformed TCP input. They are never
  forwarded partially to `apply_frame`.
- RFC 6455 Ping/Pong and Close are transport control. They are not converted to
  liminal `Frame::Ping`, `Frame::Pong`, or `Frame::Disconnect`. Conversely,
  liminal control frames remain ordinary canonical binary-message payloads.
- WebSocket fragmentation may be accepted only through library reassembly.
  Compression extensions are OFF in this unit, so canonical liminal bytes are
  not exposed to a second compression/bomb limit.
- Normal close, peer loss, protocol error, I/O error, and server shutdown each
  reach the same connection cleanup and typed client-fate paths used by TCP.
  WebSocket close codes may enrich diagnostics; they do not select aggregate
  transitions.
- There is no polling. Native readers block on socket input and signal events;
  browser readers are callback-driven. No interval, short-timeout retry loop,
  `try_recv` sweep, JavaScript timer, or application re-arm loop is a source of
  truth (LAW-1 heritage; `docs/design/LAW1-POLLING-RETIREMENT.md`).

## R1 — sibling server WebSocket acceptor

### R1.1 — first HTTP surface, deliberately tiny

Add a separately configured WebSocket listen address and route. Use one
workspace-pinned direct **`tungstenite`** dependency with only its synchronous
handshake/protocol requirements enabled in `Cargo.toml`, `Cargo.lock`, and
`crates/liminal-server/Cargo.toml`. This is the honest minimum: liminal needs an
RFC 6455 implementation and a correct HTTP/1.1 Upgrade parser, while
`tungstenite` supplies both over `std::net::TcpStream` without an async runtime
or web framework. Do not add tokio, `tokio-tungstenite`, hyper, axum, warp, a
router, middleware stack, general request handlers, cookies, or an HTTP service
abstraction. The server's first HTTP surface accepts only the configured
WebSocket Upgrade path; every ordinary HTTP request receives a small fixed
non-success response and closes.

The decided TLS/origin contract (tear Q1) is raw `ws://` behind a named
TLS-terminating proxy that owns public `wss://` and certificates; liminal grows
no TLS stack. Origin validation nonetheless belongs to this acceptor. Server
configuration provides an explicit allowed-origin allow-list that is checked on
every Origin-bearing upgrade. There is no default list: absent or empty origin
configuration fails closed for browser-origin upgrades with a typed refusal,
while a native client that sends no `Origin` header may upgrade.

**Files:** `Cargo.toml`, `Cargo.lock`, `crates/liminal-server/Cargo.toml`;
`crates/liminal-server/src/config/{types,env,file,validation}.rs`;
`crates/liminal-server/src/server/{runtime,shutdown}.rs`; new
`crates/liminal-server/src/server/connection/websocket.rs` and
`connection/websocket/{listener,supervisor,process,outbound}.rs`.

**Acceptance:**

1. Configuration has an explicit opt-in WebSocket address and one exact path;
   absent configuration starts no HTTP/WS listener and preserves current
   startup bytes and behavior. Address/path validation, bind errors, logs,
   readiness, stop-accepting, drain, and forced shutdown are covered.
2. **F6:** a listed-Origin upgrade on the configured path passes; an unlisted
   Origin receives a typed refusal; empty or absent origin configuration refuses
   Origin-bearing upgrades only; and a native client sending no Origin passes.
   The checkpoint-3 browser harness sends an Origin and its test configuration
   lists that Origin. Wrong method/path/version, malformed or oversized headers,
   and plain HTTP remain bounded and rejected without entering supervision.
3. **F1:** extension offers, including browsers' unavoidable
   `permessage-deflate` offer, are declined and never negotiated. The upgrade
   response has no `Sec-WebSocket-Extensions` header, and checkpoint 3 asserts
   both the absent response header and `ws.extensions === ""`. Malformed
   upgrades remain rejected. Subprotocols are separate: the liminal browser
   client offers none, and any offered subprotocol is rejected.
4. Dependency inspection proves there is no async runtime or framework added.
   Upgrade tests use raw HTTP bytes as well as a real WebSocket client; they do
   not test an implementation echo.

### R1.2 — one binary message, one canonical frame

The WS process receives a complete binary message, checks its declared liminal
size before allocation where the library permits, requires
`decode(bytes)` to consume `bytes.len()`, and only then calls semantic
application. Every outbound `FrameAction::Respond` or server-pushed frame is
canonically encoded once and sent as one binary message. There is no WS-specific
codec.

**Files:** new `connection/websocket/{process,outbound}.rs`; canonical codec
remains `crates/liminal/src/protocol/{frame,codec}.rs`.

**Acceptance:** byte fixtures for every existing frame category are identical
before WS send and after WS receive; lengths around 9/10 bytes, declared-body
boundaries, trailing bytes, two concatenated frames, text, fragmented binary,
close, and control Ping/Pong are pinned. A cross-transport test sends the same
`Frame`, captures TCP bytes and WS binary payload bytes, and asserts equality.
No production line in `crates/liminal/src/protocol/{frame,codec}.rs` changes.

### R1.3 — same application seam and supervision, sibling ownership

The WebSocket listener owns its own accept worker, connection registry, socket
reader/writer, and shutdown handle. Its per-connection process constructs the
same `ConnectionProcessState`, invokes the same `apply_frame`, handles every
`FrameAction` exhaustively, uses the same `ConnectionServices`, delivery pump,
pending-reply, participant ingress, incarnation allocation, cleanup, and close-
cause semantics as TCP. Do not make the current TCP listener accept HTTP, wrap
`TcpStream` in a transport enum, or generalize its read/write hot path.

**Q-A (keepalive) — FINAL:** a server-side WebSocket Ping schedule is authorized
only as transport liveness, as a precise LAW-1 carve-out. Liveness pings never
mint application events, never re-arm application state, and never serve as a
source of truth; failure detection remains the socket's typed terminal events.
The interval is an explicit named configuration value, and absent configuration
means pings are disabled, with proxy-idle-disconnect churn accepted and
documented. The bound is one ping per interval per connection; the idle cost is
interval × connection-count and is stated and tested.

**Files:** `crates/liminal-server/src/server/connection.rs` only for new module
registration/re-export; new `connection/websocket*.rs`; runtime ownership in
`server/runtime.rs` and `server/shutdown.rs`. Existing
`server/listener.rs`, `connection/{supervisor,process,outbound}.rs` are reference
implementations and behavioral controls, not refactor targets.

**Acceptance:** the same authenticated connect, participant capability,
subscribe/deliver, publish/backpressure, conversation request/reply, push/reply,
malformed-input cleanup, crash reap, graceful drain, and forced-close scenarios
pass through both transports. Connection/subscription/conversation resources are
removed exactly once for every WS terminal fate. Existing TCP test binaries and
fixtures pass unchanged, and `git diff` shows no production edit to
`server/listener.rs` or `connection/{supervisor,process,outbound}.rs`.

## R2 — native SDK WebSocket transport

### R2.1 — prove `RemoteTransport` with a real second implementation

Add `WebSocketRemoteTransport` for std targets and explicit
`RemoteConfig::connect_websocket` / `connect_websocket_with_auth` entrypoints.
Use blocking `tungstenite`, matching the SDK's current synchronous model. The
transport uses the same canonical `liminal::protocol::encode/decode` and
implements every `RemoteTransport` method; it does not call TCP transport
methods, translate through JSON, or duplicate protocol correlation rules.
`ws://` is required for the native loopback proof. Public `wss://` is owned by
the named TLS-terminating proxy; liminal does not terminate TLS.

**F2:** use `tungstenite` accept and client configuration variants whose
`max_message_size` and `max_frame_size` are both pinned to the active liminal
frame bound derived from the named product limit. This library reassembly bound
is the pre-allocation authority on both native ends. An oversize-declared message
fails at the pinned bound, never after allocation of the library's 64 MiB
default buffer.

**Files:** `Cargo.toml`, `Cargo.lock`, `crates/liminal-sdk/Cargo.toml`;
`crates/liminal-sdk/src/remote.rs`, `remote/protocol.rs`, and new
`remote/websocket.rs`, `remote/websocket/{core,std_socket,connection,subscription}.rs`;
new integration tests under `crates/liminal-sdk/tests/` and
`crates/liminal-server/tests/`.

**Acceptance:** one `RemoteConversationHandle` and channel surface run the same
publish, delivered-ack, subscribe/delivery, send, request/reply, resume,
auth-success, auth-refusal, server-close, malformed-message, and reconnect
fixtures once with TCP and once with WS. The handle still owns
`Arc<dyn RemoteTransport>` on native targets and tests prove two concrete
implementations traverse it. TCP snapshots and behavior remain unchanged.

### R2.2 — typed fates enter the client unit; policy never exits it

Socket open is performed only after a one-use permit has become
`ReconnectInProgressAttempt`. Established connection loss is passed as
`EstablishedConnectionTransportFate::Lost` to `record_transport_fate`; open
completion is passed as `ReconnectAttemptFate::{Connected,Failed}` to
`record_attempt_fate`; detach-send loss is passed to the existing typed detach
replay fate. Aggregate decisions, not the WS adapter, decide whether an attempt,
replay, park, refusal, or terminal result exists. A WS close code, I/O string,
timeout, or JavaScript event is diagnostic data only, never a fresh reconnect
authorization.

**Q-B (send pressure):** the client unit is the v1 outbound-pressure authority.
Its at-most-one outstanding write-ahead operation rule plus request-response
correlation structurally bounds request-class outbound traffic, and v1 browser
traffic is user-action-scale. Observing `bufferedAmount` is FORBIDDEN: it has no
event and would require polling. Any future bulk-outbound component class must
arrive with its own typed flow-control brief rather than bolt onto this transport.

**Files:** the SDK binding/wiring modules introduced by the reviewed client-unit
integration; authoritative APIs remain
`crates/liminal-protocol/src/client/{reconnect,replay}.rs` from `4118aa1`.

**Acceptance:** tests inject every socket fate at permit-before-open,
open-in-progress, established, detach-in-flight, already-parked, and stale-
attempt points. They assert aggregate-returned typed decisions, one real open
per fresh authorization, unchanged state on refusal, failure returning Parked,
and no timer/retry effect. Searches and review find no SDK copy of aggregate
matching logic and no transport-owned automatic reconnect loop.

## R3 — swappable socket seam and the separate wasm leg

### R3.1 — transport-neutral, event-driven driver now

Do not model the shared seam as blocking `Read + Write`, `TcpStream`,
`tungstenite::WebSocket`, an async-runtime trait, or `web_sys` callbacks. Put a
`no_std + alloc` WebSocket liminal driver in `remote/websocket/core.rs` whose
inputs are closed socket events (`Opened`, one complete `Binary` message,
`Closed`, `Failed`) and whose outputs are closed commands (`Open`, `SendBinary`,
`Close`) plus typed transport fates. It owns canonical-frame validation and
in-flight wire correlation state. The std adapter drives commands with blocking
`tungstenite`; the later web adapter drives the same commands from browser
callbacks. Neither adapter owns reconnect or participant decisions.

**F3:** terminal-event handling is post-terminal-tolerant and follows
first-terminal-mints-fate. Abnormal browser loss always produces `error` THEN
`close`; clean close produces `close` alone; and a commanded `Close` echoes as a
later close event. The first terminal mints exactly one typed fate. Every later
terminal is a typed no-op inside the driver and never reaches
`record_transport_fate`.

**Files:** `crates/liminal-sdk/src/remote/websocket/core.rs` and target-specific
siblings; feature/cfg wiring in `crates/liminal-sdk/{Cargo.toml,src/remote.rs}`.

**Acceptance:** deterministic driver tests run without a socket and prove the
same command/event trace for std and a fake browser adapter; no platform type is
present in the core module; binary messages retain canonical bytes; and no
`poll`, interval, sleep, timeout-rearm, or runtime executor is required to make
progress. Trace tests pin F3's abnormal `error`-then-`close`, clean-close-only,
and commanded-Close-then-later-close cases and prove exactly one recorded fate.
Existing no-default wasm checks remain green after the native leg.

### R3.2 — wasm implementation is a separate bounded delivery leg

The native checkpoint does not claim browser completion. After native review,
a separate commit series adds `wasm-bindgen`/`web-sys` target-only dependencies
and `WebSysWebSocketSocket`. **F4:** the adapter sets
`binaryType = "arraybuffer"` at construction, before subscribing to `open`,
`message`, `close`, and `error`; it accepts only `ArrayBuffer`/byte binary data,
feeds each callback once into the shared driver, and drains emitted commands
without timer polling. Blob's asynchronous default is not supported.

The decided wasm ownership shape (tear Q4) keeps the JavaScript socket outside
the transport-neutral handle. Closed events and commands cross a channel seam,
and the wasm leg remains single-threaded and event-driven; no public
`ConversationHandle` trait-bound changes in this unit. **F5:** the exported
browser conversation surface MIRRORS the `ConversationHandle` method set with
equal semantics; it does not implement the `Send + Sync` trait, following the
beamr-wasm `WasmVm` precedent. If the channel seam proves insufficient, work
STOPS with evidence; a blocking shim, timer pump, or TypeScript protocol
reimplementation is forbidden.

The browser-visible API is the liminal-sdk conversation surface compiled to
wasm, not a TypeScript protocol client. TypeScript may instantiate the wasm
package, pass the server-issued binding credential, attach callbacks, and render
decoded application data only. This bounded leg ends at the Rust/wasm socket
adapter and a browser-run transport harness. It does NOT build frame's browser
shell, rendering, manifest machinery, production bundler, or npm publication
path.

**Files:** target-specific manifest sections in `Cargo.toml` and
`crates/liminal-sdk/Cargo.toml`; new
`crates/liminal-sdk/src/remote/websocket/web_socket.rs`; a new narrowly named
wasm transport test target/harness. Production browser packaging remains with
its repository owner; protocol logic may not move there.

**Acceptance:**

1. `liminal-protocol` and `liminal-sdk --no-default-features` compile for
   `wasm32-unknown-unknown`; the WS core is included in that compile.
2. A browser-run test opens the real sibling server acceptor, redeems a binding,
   subscribes, receives one conversation value, observes a forced transport
   loss, and reconnects only through the client aggregate's typed permit path.
3. A compile/dependency audit proves no `std::net`, tungstenite, thread, native
   mpsc, tokio, or TypeScript wire codec in the wasm artifact. A test fails on
   text messages and proves fragmented binary is delivered once after browser
   reassembly.
4. The exported browser handle preserves liminal `ConversationHandle`
   semantics through F5's mirror-not-implementor shape. It does not implement
   the native `Send + Sync` trait
   (`crates/liminal-sdk/src/conversation.rs:80-119`), and no trait-bound change,
   polling, or second browser protocol is introduced.

## R4 — concrete browser binding credential on the liminal wire

### R4.1 — frozen additive types and tags

Pin the ruling-9 credential as these protocol-v1 types:

- `BrowserBindingCredential`: an opaque **32-byte** bearer capability. Its Rust
  field is private; canonical encoding is exactly those 32 bytes. The client may
  retain, compare, and re-present it but gets no conversation, participant,
  instance, feed, expiry, or authorization fields from it. Byte construction
  never confers validity: only a live server authority record does.
- `BrowserBindingRedemptionToken`: a **16-byte**, client-generated,
  single-attempt correlation token, following existing attempt-token width at
  `wire/primitives.rs:106-157`. It is not authority.
- `BrowserBindingSubscriptionId`: a connection-incarnation-scoped `u64` naming
  an active redeemed subscription. It is not reusable as a credential.
- `ClientDiscriminant::RedeemBrowserBinding = 0x0009` with
  `RedeemBrowserBinding { credential, redemption_token }`.
- `ServerDiscriminant::BrowserBindingRedeemed = 0x0125` with
  `{ redemption_token, subscription_id }`.
- `ServerDiscriminant::BrowserBindingUnavailable = 0x0126` with
  `{ redemption_token }`. Unknown, expired, revoked, wrong-participant,
  wrong-instance, and failed fresh-capability checks deliberately share this one
  non-oracular wire refusal.
- `PushDiscriminant::BrowserBindingRevoked = 0x0202` with
  `{ subscription_id }`, emitted when a live redemption is withdrawn. Loss of
  the transport needs no push; it is already typed transport fate.

The request consumes no client-selected conversation address. Server-side
resolution of the credential selects exactly one instance binding and its
conversation subscription. Success installs that subscription on this
connection; reconnect must send a new redemption request, receive a new
connection-scoped subscription ID, and pass a fresh authority check. Revocation
removes the active subscription before publishing the push/close consequence;
a racing consuming act is refused server-side. The credential itself is never
echoed in any response, push, log, metric label, or error.

The decided credential-durability contract (tear Q2) is issuer-epoch rotation.
The credential carries an issuer epoch, and server restart rotates the epoch to
revoke every outstanding browser-binding credential at once. Browsers recover
only through normal reconnect plus a fresh-live-manifest path. Restart
revocation is explicit acceptance behavior, not an accident of in-memory
lifetime; a later durable authority store must preserve these wire bytes.

The decided connection cardinality (tear Q3) is one WebSocket connection per
browser surface, multiplexing many concurrently redeemed instance credentials
as multiple active `subscription_id`s. Per-connection instance-binding capacity
is an explicit named configuration value with a typed refusal at the bound and
no assumed default. Revoking one instance credential terminates only that
subscription; revoking the participant terminates the connection.

**Files:** `crates/liminal-protocol/src/wire/{primitives,tags,request,response,push,codec,server_codec,mod}.rs`
and their existing focused test modules; SDK client correlation/resume handling
from the `4118aa1` client unit; server authority/service dispatch under
`crates/liminal-server/src/server/connection/` without changing participant
aggregate decisions.

**Acceptance:**

1. Registry tests pin all old tag/value pairs byte-for-byte and append exactly
   `0x0009`, `0x0125`, `0x0126`, and `0x0202`; unknown tags still refuse as
   before. Existing message encodings and meanings do not change.
2. Canonical fixtures pin exact field order and lengths; round trips reject
   truncation, trailing bytes, wrong direction, wrong body/tag, and oversized
   frames. Existing client correlation is extended exhaustively so the
   redemption token, not caller preselection, correlates the two responses.
3. An authority test issues credentials for two participants and two component
   instances and proves no cross-participant/cross-instance redemption,
   substitution, address choice, or oracle distinction. Two random credentials
   cannot alias; credentials never appear in diagnostics.
4. A reconnect test proves that a credential valid on connection A is checked
   again on B, B gets a new subscription ID, and revocation between A and B
   yields `BrowserBindingUnavailable`. Server restart rotates the issuer epoch,
   invalidates every previously issued credential, and requires reconnect plus
   fresh-live-manifest issuance. A live-revocation test removes delivery
   authority and emits exactly one `BrowserBindingRevoked` for the active ID.
5. Credential issuance/redemption uses a CSPRNG and constant-time secret
   comparison or a server-stored digest/MAC design; no raw credential is stored
   in logs or observability. Expiry/revocation/participant authority is checked
   at the consuming act, not cached in the client.
6. `ClientParticipantAggregate`, reconnect, and detach replay own all resulting
   decisions. Server aggregate transition laws, claim-frontier arithmetic, and
   capability advertising are untouched.
7. One browser-surface connection concurrently redeems multiple instance
   credentials up to the named capacity. The next redemption receives the typed
   bound refusal. Instance revocation removes only its subscription, while
   participant revocation closes the connection.

## Boundaries and byte-identity discipline

- NO behavior or production-code change to the TCP listener, TCP supervisor,
  TCP process/read loop, or TCP outbound path. Shared semantic modules may be
  called by the new sibling; the old path is not generalized to make that easy.
- NO server participant aggregate decision change. The credential authority
  provider and active-redemption table are an additive server service boundary,
  not new participant lifecycle law.
- NO polling anywhere (LAW-1). No hidden timer retries, browser interval,
  application check loop, or “eventually” test.
- Capability advertising remains exactly as it is. The WebSocket route negotiates
  the same liminal capabilities through the same canonical `Connect`; no
  `websocket-v1` liminal capability is invented.
- Claim-frontier, closure accounting, admission, and storage algebra are out and
  untouched.
- No TypeScript protocol implementation, JSON gateway, REST endpoint, SSE path,
  long polling, fallback transport, second auth policy, or generic HTTP server.
- No permessage-deflate in this unit. No TLS policy disguised as protocol bytes.
- No existing wire tag, field, error meaning, or codec fixture may change. New
  fields/types/tags are additive only.

## Checkpoints and full liminal house bar

**Checkpoint 1 — R1 + R2 native proof.** One clean commit must show sibling
server ownership, native SDK selection, canonical byte identity, cross-transport
behavioral parity, and typed client-fate wiring. Run crate-focused tests plus the
full bar; push and pause for one bounded review.

**Checkpoint 2 — R4 authority/wire proof.** On a clean successor commit, pin
registry bytes, authority isolation, revocation, reconnect re-check, and client
correlation. Re-run the full bar and adversarially review non-oracular refusals
and old-byte identity.

**Checkpoint 3 — R3 wasm leg.** Separately commit the `web-sys` adapter and real
browser-run test. Native gates and wasm gates must pass together. Native-first
is not declaration of goal completion.

Every checkpoint uses repository `target/` and runs each command separately,
recording its genuine exit status:

```bash
export CARGO_TARGET_DIR=/Users/tom/Developer/ablative/liminal/target
cargo fmt --all -- --check
cargo build --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo doc --workspace --no-deps
cargo check -p liminal-protocol --target wasm32-unknown-unknown --no-default-features
cargo check -p liminal-sdk --target wasm32-unknown-unknown --no-default-features
```

Also run focused liminal-protocol registry/codec tests, liminal-server WS tests,
liminal-sdk TCP/WS parity tests, and the browser runner at the checkpoint that
owns them. Tests must use bounded event observation sourced from named product
limits; sleeps and polling cannot establish acceptance. The reviewer reruns all
commands on the declared final bytes with a clean worktree.

## Dispatch (only after Waffles's tear is folded)

- Work ONLY in a new worktree `.worktrees/lp-ws-transport`, branch
  `feature/lp-ws-transport`. Never edit or switch the main checkout or another
  worktree.
- Branch from the recorded integration commit that contains the approved form
  of this brief and client unit `4118aa1` (or its reviewed descendant). Record
  that full SHA in the first declaration; do not silently substitute
  `origin/main` if it lacks either input.
- The FIRST implementation commit is the approved brief, unmodified. Then keep
  R1/R2 native, R4, and R3 wasm as reviewable checkpoint commits. Do not fold the
  draft artifact into code as though it had already survived tear.
- Always set
  `CARGO_TARGET_DIR=/Users/tom/Developer/ablative/liminal/target`; never create a
  worktree-local `target/` or alternate dependency cache.
- Modify only the file families named by a requirement. No lint suppression,
  ignored test, unsafe, silent fallback, caller-owned rule, or opportunistic TCP
  refactor. Public items have docs; workspace lint policy remains in force.
- Root `Cargo.toml`/`Cargo.lock` are single-owner coordination files. Commit and
  push each checkpoint; declare commit, clean status, file inventory, test
  counts, every command/exit code, and every deviation with its compile/type
  reason. At most two review rounds per checkpoint, then escalate for a scope
  ruling rather than opening an exam loop.

## Pre-tear questions (historical prompt)

These prompts are retained as drafting history. Their decided contracts are now
stated directly in the R-sections above; they are not open implementation
choices.

1. **TLS and origin ownership.** The repository pins no public TLS terminator,
   certificate loader, allowed-origin policy, or `wss://` ownership for this
   first HTTP surface. Should liminal terminate TLS and validate `Origin`
   directly, or is the supported deployment contract raw `ws://` behind a
   named TLS/origin-enforcing proxy? Browser mixed-content rules mean this must
   be ruled before the wasm checkpoint; it does not alter canonical WS payload
   bytes.
2. **Credential authority durability.** Ruling 9 requires issuance, fresh checks,
   and revocation, but neither the liminal pin nor the frame contract assigns
   storage ownership or restart survival for the credential record. Must a
   server restart preserve still-valid credentials and revocations, or revoke
   all outstanding credentials by rotating an issuer epoch? The choice must be
   explicit and tested; reconnect freshness cannot be satisfied by accidental
   in-memory lifetime.
3. **Connection cardinality for instance bindings.** The frame contract permits
   multiple component instances, while the inspected liminal participant state
   exposes one participant session plus multiple ordinary subscriptions and
   conversations. Must one browser participant connection redeem many instance
   credentials concurrently, or is one WS connection per surface binding the
   v1 rule? R4's bytes support multiple active `subscription_id`s, but runtime
   limits, revocation fan-out, and the end-to-end fixture need the authoring
   seat's cardinality ruling.
4. **`Send + Sync` on the wasm conversation surface.** The present public
   `ConversationHandle` requires `Send + Sync`, while `web_sys::WebSocket` is a
   browser event source and cannot be stored behind the current native
   `Arc<dyn RemoteTransport>` shape. R3 proposes keeping the JS socket outside
   the transport-neutral handle and feeding closed events/commands. Tear must
   confirm that ownership shape or rule a target-specific trait-bound change;
   a blocking shim, timer pump, and separate TypeScript protocol are forbidden
   answers.

## Retained tear and boundary-read audit record

## Tear rulings (2026-07-17)

1. **TLS and origin (Q1).** v1's supported deployment contract is raw `ws://`
   behind a named TLS-terminating proxy that owns `wss://` and certificates —
   liminal grows NO TLS stack, keeping R1.1's first HTTP surface tiny. Origin
   is nonetheless validated IN liminal's acceptor: an explicit allowed-origin
   allow-list in server config, checked on every upgrade. There is no default
   list — absent or empty origin configuration REFUSES browser-origin upgrades
   with a typed error (fail closed; a deployment must state its origins to
   serve browsers). Documented as the deployment contract in the brief's R1
   acceptance.
2. **Credential durability (Q2).** v1 rules issuer-epoch rotation: the binding
   credential carries an issuer epoch; server restart rotates the epoch,
   revoking every outstanding browser-binding credential at once. Browsers
   re-establish through the normal reconnect + fresh-live-manifest path, which
   the component contract already obligates them to support. Explicit and
   tested — restart-revocation is an acceptance case, not an accident of
   in-memory lifetime. Durable credential records can arrive later without
   wire change precisely because the epoch is in the credential.
3. **Connection cardinality (Q3).** One WebSocket connection per browser
   surface, multiplexing MANY concurrently redeemed instance credentials —
   R4's multiple active `subscription_id`s are the intended shape.
   Per-connection instance-binding capacity is an explicit named config value
   with a typed refusal at the bound (no assumed default). Revocation fan-out:
   revoking one instance credential terminates that subscription only;
   revoking the participant terminates the connection.
4. **Wasm ownership shape (Q4).** The draft's proposal is CONFIRMED: the JS
   socket stays outside the transport-neutral handle; events and commands
   cross a channel seam; the wasm leg is a single-threaded, event-driven
   driver. No trait-bound change to the public `ConversationHandle` in this
   unit. If the wasm leg finds the channel seam insufficient, it STOPs and
   returns with evidence — the blocking shim, timer pump, and TypeScript
   protocol reimplementation remain forbidden answers.

## Boundary-read dispositions (Artemis Peach, 2026-07-17 — all ruled by the
## tear seat; the pre-dispatch fold integrates F-items into the body text)

- **F1 ACCEPTED (blocking-grade text fix).** R1.1's "unsupported
  extension/subprotocol rejected" would reject every real browser (all three
  engines offer `permessage-deflate` unremovably). Corrected contract:
  extension OFFERS are DECLINED, never negotiated — the upgrade response
  carries no `Sec-WebSocket-Extensions` header, pinned in the checkpoint-3
  browser harness by asserting the absent header AND `ws.extensions === ""`
  (which makes compression-off browser-observable). Malformed upgrades remain
  rejected; subprotocols pinned separately (the liminal browser client offers
  none).
- **F2 ACCEPTED.** The WS library's reassembly bound is the real
  pre-allocation limit: require `accept`/`client` config variants with
  `max_message_size`/`max_frame_size` pinned to the liminal frame bound from
  the named product limit. Oversize-declared messages must fail at the pinned
  bound, not after a 64 MiB default buffer.
- **F3 ACCEPTED.** Browser terminal-event mapping pinned in R3.1's trace
  tests: abnormal loss fires `error` THEN `close` (two terminals, always);
  clean close fires `close` alone; a commanded Close echoes as a later close
  event. The driver is post-terminal-tolerant: the FIRST terminal mints the
  one typed fate; subsequent terminals are typed no-ops at the driver and
  never reach `record_transport_fate`.
- **F4 ACCEPTED.** The wasm adapter sets `binaryType = "arraybuffer"` at
  construction — the "blob" default demands async reads, which tempt exactly
  the deferred machinery this brief forbids.
- **F5 ACCEPTED (clarifying sentence).** The browser conversation surface
  MIRRORS the `ConversationHandle` method set with equal semantics; it does
  not implement the `Send + Sync` trait. Ruling 4's "no trait-bound change"
  and R3.2's "preserves semantics" are both satisfied by the mirror shape —
  the beamr-wasm `WasmVm` precedent. A STOP on this apparent contradiction is
  therefore unfounded.
- **F6 ACCEPTED (fold hygiene).** Ruling 1's origin cases enumerated into
  R1.1 acceptance: listed-Origin upgrade passes; unlisted-Origin refuses
  typed; empty/absent origin config refuses Origin-BEARING upgrades only
  (native clients sending no Origin header pass); the checkpoint-3 harness
  sends an Origin, so its test config must list it.
- **Q-A RULED (keepalive) — FINAL under Tom's keep-it-moving authority
  (2026-07-17; the provisional flag was cleared by Tom's direction that
  technically-correct dispositions are ruled at the tear seat).** A
  server-side WS Ping schedule is authorized as TRANSPORT LIVENESS, with the
  LAW-1 carve-out stated precisely: liveness pings never mint application
  events, never re-arm application state, and never serve as a source of
  truth — failure detection remains the socket's typed terminal events. The
  interval is an explicit named config value; absent config = pings disabled
  with proxy-idle-disconnect churn documented as the accepted consequence.
  Bound: one ping per interval per connection; the idle cost is
  interval × connection-count, stated and tested. Per the idle-cost doctrine
  this carve-out is flagged to Tom and reversible by one line before
  dispatch.
- **Q-B RULED (send pressure).** The outbound-pressure authority for v1 is
  the client unit itself: the at-most-one outstanding write-ahead operation
  rule plus request-response correlation structurally bound request-class
  outbound, and v1 browser traffic is user-action-scale. Observing
  `bufferedAmount` — which has no event and therefore requires polling — is
  FORBIDDEN. If a future component class needs bulk outbound streaming, it
  arrives with its own typed flow-control brief; it does not bolt onto this
  transport.
