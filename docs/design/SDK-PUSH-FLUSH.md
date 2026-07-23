# SDK-PUSH-FLUSH — an explicit flush surface for `PushClient`

**Revision r2 — torn 2026-07-23 by the tear seat (Waffles): r1 APPROVED with two
fold conditions (T1, T2) and four §6 rulings folded in. Docs-only lane: this
specifies the 0.4.0 build and its acceptance shape; it does not claim the surface
is implemented. Every codebase-state sentence carries a file:line pin re-verified
at the authoring pin. Decisions D1-D5 are now torn-approved; the T1/T2 decisions
and rulings (1)-(4) are recorded in §7.**

**r1 — design-first, 2026-07-23. Original design-first draft; every decision
proposed-pending-tear.**

## 0. Authority and pin

The byte pin for every ground fact below is liminal **`14032ca`**
(`14032cae8fd3cf726cddcec8766291862722b53c`), the `release: liminal-server 0.3.3
+ liminal-sdk 0.3.3` commit. Both crates are at `0.3.3`
(`crates/liminal-sdk/Cargo.toml:3`, `crates/liminal-server/Cargo.toml:3`). The
0.3.3 drop-drain machinery this design stands on is live at
`crates/liminal-sdk/src/remote/tcp/push_client.rs`: `DROP_DRAIN_BUDGET` (`:62`),
`drain_pending_acks` (`:400-455`), and the sole-owner graceful half-close
(`:404,:434`). The ruling frames that machinery as **the degenerate case of
flush** — the ack plumbing `flush()` is built on. This document does not re-open
it; it names the surface `flush()` adds above it.

## 1. Problem statement — two boarded findings, one mechanism

Both findings share a root cause: on the push connection, **publish is
fire-and-forget and the caller is never told the server's verdict.** The 0.3.3
drop-drain fixed the *loss* leg but not the *learning* leg.

**Finding (i): burst-then-drop silently loses / hides outcomes.** `PushWriter::publish`
writes one `Frame::Publish` and returns as soon as the bytes hit the socket
(`push_client.rs:330-342`) — no wait, no verdict. Before 0.3.3, closing on a
burst RST-truncated unread publishes; the drop-drain (`push_client.rs:368-387`)
now half-closes gracefully so accepted publishes survive teardown. But a caller
still has **no way to learn** whether any given publish was accepted: the
background reader `run_reader` surfaces only `Frame::Push` frames and explicitly
**discards every other inbound frame**, including `PublishAck`/`PublishError`
(`push_client.rs:558-583`, the `Ok(Some(_) | None) => {}` arm at `:579`), and the
drop-drain reads those same ack bytes only to throw them away (`:411-454`). The
outcome is on the wire and is dropped on the floor.

**Finding (ii): schema-0 validation rejections are invisible.** A publish on a
schema-bearing channel is validated server-side: `apply_publish` calls
`schema.validate(payload)` and maps a mismatch to `LiminalError::SchemaMismatch`
(`crates/liminal/src/channel/actor/mod.rs:332,443-447`), which
`LiminalConnectionServices::publish` surfaces as an `Err`
(`crates/liminal-server/src/server/connection/services.rs:955-1008`). The
connection handler turns that `Err` into a real `Frame::PublishError` on the wire
(`crates/liminal-server/src/server/connection/apply.rs:408-436`, error arm
`:429-434`). So the server **already tells the truth** — but the SDK reader
discards it (above), so a fire-and-forget publisher never sees the rejection.
Two independent reproductions are on record: a consumer diagnostic, and a build
worker's wrong-reason-red where non-JSON payloads were silently rejected at
publish.

**One mechanism closes both.** With the ack-reader always live post-0.3.3, the
0.4.0 SDK stops discarding `PublishAck`/`PublishError`, correlates them to
outstanding publishes, and `flush()` is where the collected verdicts return to
the caller.

## 2. `flush()` semantics

```rust
pub fn flush(&self) -> Result<FlushOutcome, SdkError>;
```

- **"Currently-accepted"** = every publish whose `Frame::Publish` bytes were
  written to the socket **before** this `flush()` call (via `PushClient::publish`
  or a `PushWriter::publish` on the same socket, `push_client.rs:300-302,330-342`).
  Publishes written concurrently *after* the call began are not promised.
- **What it awaits:** the server's per-publish response frame for each of those —
  a `Frame::PublishAck` (`crates/liminal/src/protocol/frame.rs:315-319`) or a
  `Frame::PublishError` (`:321-326`) — captured by the always-live reader rather
  than discarded.
- **What it returns:** a typed `FlushOutcome`. The outer `Result::Err(SdkError)`
  is reserved for a failure of the flush *mechanism itself* (poisoned writer lock
  → `SdkError::Connection`, exactly as `reply`/`publish` already map it,
  `push_client.rs:268-270`; and the ruling-(1) response-count mismatch below).

```rust
pub struct FlushOutcome {
    /// Per-publish rejections observed among the flushed publishes, in wire order.
    failures: Vec<PublishRejection>,
    /// Flushed publishes still unresolved when the budget expired — neither acked
    /// nor rejected. A NORMAL outcome the caller MUST inspect, not an error: a
    /// nonzero `unresolved` means those publishes' fate is connection-indeterminate.
    unresolved: usize,
    /// Whether this flush also half-closed the socket (sole owner) or only
    /// collected verdicts because a live `PushWriter` clone shares it (§3).
    mode: FlushMode,
}

/// A flush is proven-accepted for every flushed publish ONLY when
/// `failures.is_empty() && unresolved == 0`. Any other shape is the caller's to
/// inspect — this is the T1 anti-silent-fallback invariant.
pub enum FlushMode {
    /// Sole owner: acks drained AND the write half was FIN'd.
    FlushedAndHalfClosed,
    /// A live `PushWriter` clone shares the socket: verdicts collected, no FIN.
    VerdictOnly,
}

pub struct PublishRejection {
    /// The server's numeric reason. Today always SERVER_ERROR_CODE (0xFFFF).
    reason_code: u16,
    /// The server's human-readable detail (carries the schema-mismatch text).
    message: Option<String>,
}
```

**T1 (torn condition — anti-silent-fallback).** `failures` alone cannot represent
budget expiry: three unresolved publishes would be byte-indistinguishable from a
clean all-accepted flush — a silent fallback wearing a struct. `unresolved` makes
the distinction explicit, and the invariant is that **`failures.is_empty() &&
unresolved == 0` is the ONLY shape that reads as proven-accepted.** Expiry with
unresolved publishes is a **normal outcome the caller must inspect**, never an
`Err`.

Rationale for `PublishRejection` against the existing vocabulary: `PublishError`
carries only `{ flags, stream_id, reason_code, message }` (`frame.rs:321-326`),
and the server sets `reason_code` to a **blanket** `SERVER_ERROR_CODE = 0xFFFF`
for *every* publish failure (`apply.rs:26,432`) — a schema mismatch is **not
wire-distinguishable** from any other publish error today; only the `message`
string differs. Per ruling (4), `flush()` returns the raw `reason_code` + `message`
**verbatim** rather than fabricating an `SdkError::TypeValidation`
(`crates/liminal-sdk/src/error.rs:31-36`) it cannot prove.

- **Bounding / LAW-1.** `flush()` is bounded exactly like the drop-drain: a
  single wall-clock budget in the spirit of `DROP_DRAIN_BUDGET`
  (`push_client.rs:62`), no unbounded wait. It **blocks on a channel receive with
  a deadline** (the collected-response channel), never a poll loop, timer, or
  stop-flag sample — LAW-1 conformant. On budget expiry it returns with the
  still-unresolved publishes counted in `FlushOutcome.unresolved` (above) rather
  than hanging or silently reporting success.

**T2 (torn condition — concurrent flush).** Two threads calling `flush()` while
FIFO responses arrive on the one stream could split the response sequence between
them — misattribution by interleaving, the exact hazard D4 manages. **Ruled:
`flush()` serializes internally.** A second concurrent `flush()` waits on the same
flush guard; when it proceeds it covers only its own write-boundary (the publishes
written before *it* was called, minus those the first flush already resolved). No
two flushes ever consume from the response sequence at once, so FIFO attribution
stays single-reader. The guard is a mutex-style wait, not a poll (LAW-1).
- **What `flush()` does NOT promise.** A `PublishAck` proves **server
  acceptance**, not delivery to any subscriber. `apply_publish` counts local
  subscribers but delivery is best-effort and uncounted in the ack
  (`channel/actor/mod.rs:340-355`; the `PUBLISH_DELIVERED_FLAG` bit,
  `frame.rs:47`, reports only whether *some* subscriber received it, and is not
  part of the flush contract). **`flush()` proves the server accepted the
  publish; it never proves fan-out reached any subscriber.**

## 3. Close semantics — graceful by default

- **`close()` = flush-then-close.** 0.4.0 adds an explicit
  `close(self) -> Result<FlushOutcome, SdkError>` that runs `flush()` and then the
  graceful half-close, so the caller learns the verdict of every in-flight publish
  *before* the socket goes away — which `Drop` structurally cannot do.
- **Relationship to `Drop`.** `Drop` stays **best-effort and bounded**
  exactly as today (`push_client.rs:368-387`, drain `:400-455`): it drains acks so
  teardown emits a clean FIN, but it returns nothing and surfaces no failures.
  Explicit `close()` is the surface for callers who need the outcome; `Drop`
  remains the safety net for callers who drop without it. No behavior of `Drop`
  changes.
- **Shared-socket (`PushWriter` clone) case — ruling (2).** When a live
  `PushWriter` clone still shares the `Arc<Mutex<TcpStream>>`
  (`Arc::strong_count > 1`, `push_client.rs:404`), a write-half `shutdown` is
  unsafe — it would break the clone's publishes — so today's `Drop` degrades to a
  bounded receive-drain (`:413-425`). `flush()` and `close()` over a live clone
  are **PERMITTED as verdict-only**: they drain and return verdicts for what was
  written but MUST NOT half-close. The asymmetry is a fine decision; **silent
  degradation is not** — so the mode is **disclosed** in `FlushOutcome.mode`
  (`FlushedAndHalfClosed` on sole ownership vs `VerdictOnly` over a live clone,
  §2). A caller that needs the FIN guarantee inspects `mode` and drops the clones
  first (§7, «SHARED-SOCKET-VERDICT-ONLY»).

## 4. Schema-0 surfacing — the current wire truth, and what 0.4.0 needs

**The server already sends the rejection frame.** Traced at bytes: `Frame::Publish`
on a non-reserved channel → `publish_response` → `services.publish` returns `Err`
on schema mismatch → `Frame::PublishError { reason_code: 0xFFFF, message }`
(`apply.rs:62-85,408-436`). **Rejections do NOT produce "no frame"** for ordinary
channels — the frame exists in the 0.3.x wire already. The invisibility is
**purely SDK-side**: the reader discards it (`push_client.rs:579`).

Therefore **0.4.0 requires no server change to produce the rejection.** The build
is SDK-local: capture `PublishAck`/`PublishError` in the reader and correlate.

**One genuine gap the bytes expose — ruling (1): FIFO ACCEPTED for 0.4.0.** There
is **no correlation id** on the publish/ack path. `PublishAck` carries
`{ flags, stream_id, message_id }` and `PublishError` carries
`{ flags, stream_id, reason_code, message }` (`frame.rs:315-326`); neither echoes
anything tying a response back to a specific `Frame::Publish`. Push-client
publishes all ride one stream id (`APPLICATION_STREAM_ID = 1`,
`push_client.rs:70`). `flush()` therefore pairs responses to publishes **by FIFO
order** — a single connection slice processes and answers frames in order — and
reports failures *in wire order* without binding a failure to an exact payload
beyond that order. The correlation token stays **OUT** of 0.4.0 (deferred lane
below).

**Build obligation carried by ruling (1).** FIFO's real hazard is **silent
misattribution** if the elicits-response classification is ever wrong (§ below).
The build therefore carries: (a) an **interleaved observability-plus-ordinary
pairing pin** — a fixture that interleaves reserved-channel (no-response) and
ordinary (response-eliciting) publishes and asserts `flush()` pairs each response
to the correct ordinary publish; and (b) a **response-count check at flush time**:
if the number of collected responses does not match the number of
response-eliciting publishes in the flushed window, `flush()` returns a **typed
MECHANISM error** (`Err(SdkError)`) — **fail loudly, never mispair.** A count
mismatch is not a per-publish failure; it is a broken invariant.

**The reserved observability channel is unackable by design and excluded.** A
publish the server routes to its notifier hook returns `FrameAction::NoResponse`
(`apply.rs:74-76`) — no ack ever. Those publishes break any naive FIFO count, so
the SDK MUST know which publishes elicit a response (this is exactly the
classification the pairing pin protects). `flush()` covers only response-eliciting
publishes; observability-channel publishes are explicitly out of the flush
contract (§7, «OBSERVABILITY-UNACKED»).

**Ruled deferred server lane (rulings (1) + (3), NOT in 0.4.0).** A single
wire-additive server change — one review — would add both a client-supplied
correlation token echoed on `PublishAck`/`PublishError` (precise per-publish
attribution) and a distinct `reason_code` for schema mismatch (splitting the
blanket `0xFFFF`, ruling (3): **YES in principle, not in 0.4.0**). The lane's
**opening condition is a real consumer demonstrating programmatic need** — not
before. Until then the minimal 0.4.0 surface ships on FIFO ordering + raw
reason/message. This is a torn deferral with a named opening condition, not an
open question.

## 5. Semver and compatibility

| axis | disposition |
|---|---|
| liminal-sdk | **0.4.0.** New API (`flush`, `close`, `FlushOutcome`) is additive; the behavior change is `close`-by-default being flush-then-close (not silent). Minor bump. |
| liminal-server | **No change required for 0.4.0.** `PublishAck`/`PublishError` and the schema-rejection frame already exist at 0.3.x (`apply.rs:408-436`); the minimal surface adds nothing server-side. The correlation-token + typed-reason change of §4 is a **ruled deferred lane** (one additive server bump, one review) that opens only when a real consumer demonstrates programmatic need. |
| wire vs a 0.3.x server | **Fully compatible.** `flush()` reads frames a 0.3.x server *already emits*; no new frame type, no protocol-version negotiation change (client still advertises `1.0`/`1.0`, `push_client.rs:45-47`). Against an old server, flush behaves identically — the frames were always there; only the SDK stopped discarding them. |

## 6. Honesty section (house convention)

**Deliberately not solved.**
- **Delivery guarantees.** `flush()` proves server *acceptance*, never fan-out to
  a subscriber (§2). No subscriber-receipt surface is added.
- **Multi-connection flush.** `flush()` is scoped to one `PushClient`'s socket. A
  caller holding several clients flushes each; there is no cross-connection
  barrier.
- **Pipelined-request correlation.** With no correlation id on the wire (§4),
  per-publish attribution is FIFO-order only. A precise publish→verdict binding is
  explicitly deferred to the optional server change.

**Idle cost.** `flush()` adds **no background work when unused**: the surface is a
caller-driven blocking receive; when nobody calls `flush()`/`close()`, the reader
does exactly what it does today (the 0.3.3 100 ms stop-flag read-timeout,
unchanged and separately owned by the W4 SDK-reader lane). No timer, thread, or
periodic wake is added — LAW-1 clean, and the quiescence claim is that the retired
family's counters (none) stay flat while the reader's existing counters are
unchanged.

**Recorded rulings (r1's open questions, now torn — no longer questions).**
1. **FIFO-order failure reporting is accepted** for the minimal 0.4.0; the
   correlation-token server change is a ruled deferred lane (§4), not in 0.4.0.
2. **`flush()`/`close()` over a live-clone shared socket is permitted** as
   verdict-only, with the mode disclosed in `FlushOutcome.mode` (§3, ruling (2)).
3. **The blanket `reason_code = 0xFFFF` (`apply.rs:432`) will be split** — yes in
   principle, not in 0.4.0; it rides the same deferred server lane as the
   correlation token (§4, ruling (3)).
4. **`FlushOutcome.failures` carries the raw `{reason_code, message}` verbatim**
   with no `SdkError` mapping; fabricating `TypeValidation` from a message string
   is a lie wearing a type. Mapping arrives only if the server lane makes reason
   codes provable (ruling (4)).

## 7. Decision register

D1-D5 are **torn-approved** (r1 design decisions, approved at the cited bytes).
T1-T2 are the **torn fold conditions**; R1-R4 are the **four §6 rulings** — all
TORN, not proposed.

| # | «SOCKET-NAME» | decision | status |
|---|---|---|---|
| D1 | «FLUSH-IS-DEGENERATE-DRAIN» | `flush()` is built directly on the 0.3.3 drop-drain ack plumbing (`push_client.rs:400-455`); the drain is flush's degenerate, verdict-discarding case. No parallel ack path is invented. | torn-approved |
| D2 | «PUBLISH-STAYS-FIRE-AND-FORGET» | `publish` keeps its non-blocking hot path unchanged (`push_client.rs:330-342`); no new blocking is added on publish. Learning outcomes happens only at an explicit `flush()`/`close()`. | torn-approved |
| D3 | «CLOSE-GRACEFUL-BY-DEFAULT» | `close()` = flush-then-graceful-half-close, surfacing failures `Drop` cannot; `Drop` stays best-effort-bounded and silent (`push_client.rs:368-387`). | torn-approved |
| D4 | «FIFO-VERDICT-NO-CORRELATION» | With no wire correlation id (`frame.rs:315-326`), `flush()` pairs responses by FIFO order and returns failures in wire order. | torn-approved |
| D5 | «OBSERVABILITY-UNACKED» | Reserved-observability-channel publishes return no frame by design (`apply.rs:74-76`) and are excluded from the flush contract; `flush()` covers only response-eliciting publishes. | torn-approved |
| T1 | «FLUSH-UNRESOLVED-DISCLOSURE» | `FlushOutcome` carries `unresolved: usize`; `failures.is_empty() && unresolved == 0` is the ONLY proven-accepted shape. Budget expiry with unresolved publishes is a normal caller-inspected outcome, never an `Err` — no silent fallback wearing a struct (§2). | torn |
| T2 | «CONCURRENT-FLUSH-SERIAL» | `flush()` serializes internally: a second concurrent flush waits on the flush guard, then covers only its own write-boundary, so the FIFO response sequence is never split between two readers (§2). Guard is a wait, not a poll. | torn |
| R1 | «FIFO-MINIMAL-CORRELATION-DEFERRED» | Ruling (1): FIFO accepted for 0.4.0; correlation token deferred to a named server lane (§4). Build obligation: interleaved observability-plus-ordinary pairing pin, and a response-count mismatch at flush time returns a typed MECHANISM `Err` — fail loudly, never mispair. | torn |
| R2 | «SHARED-SOCKET-VERDICT-ONLY» | Ruling (2): `flush()`/`close()` over a live `PushWriter` clone is permitted as verdict-only (no FIN), with the mode disclosed in `FlushOutcome.mode` (§3). Silent degradation forbidden. | torn |
| R3 | «REASON-CODE-SPLIT-DEFERRED» | Ruling (3): splitting the blanket `0xFFFF` (`apply.rs:432`) into a typed schema `reason_code` is agreed in principle but NOT in 0.4.0; it rides the same deferred server lane as R1, opening only on a real consumer's programmatic need (§4). | torn |
| R4 | «RAW-REASON-NO-MAPPING» | Ruling (4): `PublishRejection` carries the raw `{reason_code, message}` verbatim; no mapping to `SdkError::TypeValidation` — fabricating a type from a message string is a lie wearing a type (§2). Mapping arrives only when the R3 lane makes reason codes provable. | torn |
