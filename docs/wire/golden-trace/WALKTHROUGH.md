# The participant wire, one canonical session, byte by byte

This document walks a real capture of the liminal **participant wire**, taken at
the socket seam, frame by frame, and maps every byte to the code that encodes or
decodes it.

It exists because there is no independent second implementation of this wire.
`liminal-ts` speaks the legacy channel protocol and delegates encoding to the
WASM codec, so it re-uses these exact bytes rather than reproducing them. Until
somebody writes a foreign implementation, the specification has never been read
back by a stranger — and a specification that has never been read back is a
claim, not a contract. This capture is the evidence a stranger can check their
work against.

Every code citation below carries the revision it was read at, both directions,
per repository law. All citations here are at **`339e81a`**.

---

## 1. What is in this directory

| path | what it is |
|------|------------|
| `admitter.c2s.bin` | every byte connection A sent, verbatim, in order |
| `admitter.s2c.bin` | every byte connection A received, verbatim, in order |
| `observer.c2s.bin` | every byte connection B sent |
| `observer.s2c.bin` | every byte connection B received |
| `frames.jsonl` | one JSON row per frame: direction, stream offset, length, outer type, discriminant, hex, and the **run-variable byte ranges** |
| `session.hex` | annotated hexdump with direction markers and a `~`/`.` mask row under every byte |
| `decompose.py` | an **independent** field-by-field reader (see §7) |
| `DIGESTS.txt` | `(rev, path, sha256)` for every artifact here |
| `WALKTHROUGH.md` | this document |

The generator is
`crates/liminal-server/tests/golden_trace_participant_wire.rs@339e81a`. It is
committed with the capture on purpose: a frozen capture with no generator is a
mystery, not evidence.

**Regenerate:**

```sh
LIMINAL_GOLDEN_TRACE_OUT=docs/wire/golden-trace \
  cargo test -p liminal-server --test golden_trace_participant_wire
```

**Verify without regenerating** — the same test, run with no environment
variable, re-runs the whole session against a fresh server and checks it against
what is committed here:

```sh
cargo test -p liminal-server --test golden_trace_participant_wire
python3 docs/wire/golden-trace/decompose.py     # exits non-zero on any mismatch
```

---

## 2. Why the capture is at a socket

Wire bytes exist only on a socket transport. liminal's in-process loopback mount
carries the identical framed image through the identical preflight and
`apply_frame` seam — that is proven byte-for-byte in
`crates/liminal-server/tests/loopback_parity_e2e.rs@339e81a` — but it never
serialises to a file descriptor, so there is no seam at which to hold a byte and
say *this is what crosses the network*.

The harness therefore binds a real `ServerListener` on `127.0.0.1:0` and drives
it from a raw `TcpStream` that the test itself owns. Recording happens at the
`write_all` and `read` calls: the capture is what crossed the file descriptor.
Nothing here is a decoded value re-serialised to stand in for what crossed.

---

## 3. The transport stack

A participant connection is **not** a separate port or protocol. It is the
ordinary liminal connection, carrying participant frames as one reserved generic
frame type.

### 3.1 The handshake comes first

Before any participant frame, the client performs the legacy
`Connect`/`ConnectAck` exchange and must confirm the server advertises the
participant capability bit.

```
C->S  0100000000000000000c000100000001000000000000     Connect
S->C  020000000000000000080001000000000001             ConnectAck
```

The `ConnectAck`'s trailing `00000001` is `capabilities`, and bit 0 is
`PARTICIPANT_CAPABILITY_BIT`
(`crates/liminal-server/src/server/participant/transport.rs:15@339e81a`). A
server that has no complete participant service installed will not set it, and
will reject participant frames at the transport gate. **A foreign client must
check this bit, not assume it.**

Generic framing itself is
`crates/liminal/src/protocol/frame.rs:9@339e81a` (`HEADER_LEN = 10`) with
`encode`/`decode` at `crates/liminal/src/protocol/codec.rs:52@339e81a` and
`:94@339e81a`.

### 3.2 Every participant frame

Participant traffic rides as generic frame type `0x1A`
(`PARTICIPANT_FRAME_TYPE`,
`crates/liminal-protocol/src/wire/codec.rs:14@339e81a`). The participant codec
owns the byte layout **end to end**, including the generic header — see
`encode` at `crates/liminal-protocol/src/wire/codec.rs:300-327@339e81a`, which
writes the outer header itself.

Fixed 16-byte prefix on every participant frame:

| offset | width | field | value |
|--------|-------|-------|-------|
| 0 | 1 | `frame_type` | always `0x1A` |
| 1 | 1 | `flags` | always `0x00`; the decoder refuses anything else |
| 2 | 4 | `stream_id` | always `0x00000000`; the decoder refuses anything else |
| 6 | 4 | `payload_length` | u32 BE. Complete frame = `10 + payload_length` |
| 10 | 2 | `version.major` | `0x0001` |
| 12 | 2 | `version.minor` | `0x0000` |
| 14 | 2 | `discriminant` | selects the body |

That is `GENERIC_HEADER_LEN = 10` plus `PARTICIPANT_PREFIX_LEN = 6`
(`codec.rs:17@339e81a`, `codec.rs:20@339e81a`).

Three decoder rules a foreign implementation must match, all at
`crates/liminal-protocol/src/wire/codec.rs:340-400@339e81a`:

- `flags != 0` or `stream_id != 0` or `payload_len < 6` → framing error.
- Input **shorter** than the declared complete frame → incomplete, keep reading.
- Input **longer** than the declared complete frame → `CanonicalEncoding` error.
  The exact-frame API refuses trailing bytes. Slice to exactly
  `10 + payload_length` before decoding.

All integers are **big-endian**. Variable-length byte fields are `u32` BE length
prefix followed by the bytes (`Sink::put_bytes`, `codec.rs:831-838@339e81a`).
Optional values are a **one-byte presence tag** (`0x00` absent, `0x01` present)
followed by the value only when present (`put_option_u64`,
`codec.rs:781@339e81a`; `Encoder::put_option_u64`,
`crates/liminal-protocol/src/wire/server_codec.rs:142-150@339e81a`).

There is no request-correlation id anywhere in the frame. **The only
discriminator between a reply and an unsolicited push is the discriminant
itself.** A client must demultiplex on the frame variant; see §6.

### 3.3 Discriminant registries

From `crates/liminal-protocol/src/wire/tags.rs@339e81a`:

| range | registry | in this capture |
|-------|----------|-----------------|
| `0x0001`–`0x0008` | `ClientDiscriminant` (`tags.rs:56`) | `0x0001` Enrollment, `0x0002` CredentialAttach, `0x0003` Detach, `0x0004` ParticipantAck, `0x0007` RecordAdmission |
| `0x0100`–`0x0124` | `ServerDiscriminant` (`tags.rs:78`) | `0x010A` EnrollBound, `0x0111` AttachBound, `0x0117` DetachCommitted, `0x0119` AckCommitted, `0x011F` RecordCommitted |
| `0x0200`–`0x0201` | `PushDiscriminant` (`tags.rs:158`) | `0x0201` ParticipantDelivery |
| `0x0000`–`0x0005` | `RecordKind` (`tags.rs:168`) | `0x0000` OrdinaryRecord, `0x0001` Attached, `0x0002` Detached |

The client and server registries share the same 16-bit field but never the same
values, so a decoder selects its registry by **which end it is**
(`ReceiverDirection`, `codec.rs:36-42@339e81a`), not by inspecting the number.

### 3.4 The trap: `originating_request`

**This is the detail a foreign implementation will get wrong first.**

A server value carries a leading `u16` naming the `ClientDiscriminant` that
caused it, *before* its own body, on every row EXCEPT the origin-free set:
`0x0100` (transport-rejected) and the observer-recovery block
`0x0121..=0x0124`. The rule is stated as that complement (`carries_origin`,
`crates/liminal-protocol/src/wire/server_codec.rs`, breaking-window-a5-a4
lane) because the A5/A4 settlement rows (`0x0125`, `0x0126`) sit above the
origin-free block, so no contiguous window expresses the shape. At the
capture's own revision the rule was the equivalent window `0x0101..=0x0120`
— every tag in this capture is `<= 0x0124`, where the two forms agree:

`crates/liminal-protocol/src/wire/server_codec.rs:49-54@339e81a` (superseded
form, current at the capture's revision):

```rust
if (0x0101..=0x0120).contains(&discriminant.wire_value()) {
    let originating_request = value.originating_request().ok_or(CodecError::InvalidValue)?;
    encoder.put_u16(originating_request.wire_value());
}
```

with the mirror on decode at `server_codec.rs:76-86@339e81a`, which additionally
validates the pairing (`origin_is_valid`) and rejects an implausible
request/response combination.

Every server value in this capture carries the origin prefix, so every one of
them begins its body with two extra bytes:

| response | discriminant | leading `originating_request` |
|----------|--------------|-------------------------------|
| `EnrollBound` | `0x010A` | `0x0001` EnrollmentRequest |
| `AttachBound` | `0x0111` | `0x0002` CredentialAttachRequest |
| `DetachCommitted` | `0x0117` | `0x0003` DetachRequest |
| `AckCommitted` | `0x0119` | `0x0004` ParticipantAck |
| `RecordCommitted` | `0x011F` | `0x0007` RecordAdmission |

Pushes (`0x0201`) have **no** such field: their body starts at offset 16.

---

## 4. The session

Two connections, because a delivery needs somewhere to go.

| step | connection | frame |
|------|------------|-------|
| 1 | admitter | `Connect` → `ConnectAck` |
| 2 | observer | `Connect` → `ConnectAck` |
| 3 | admitter | `EnrollmentRequest` → `EnrollBound` |
| 4 | observer | `EnrollmentRequest` → `EnrollBound` |
| 5 | admitter | `CredentialAttachRequest` → `AttachBound` |
| 6 | admitter | `RecordAdmission` → `RecordCommitted` |
| 7 | observer | drains `ParticipantDelivery` pushes |
| 8 | observer | `ParticipantAck` → `AckCommitted` |
| 9 | admitter | `DetachRequest` → `DetachCommitted` |

The conversation's record stream, as this session produces it:

| seq | record | delivered to |
|-----|--------|--------------|
| 1 | `Attached` pid 0 gen 1 — the admitter enrolls | nobody (no observer yet) |
| 2 | `Attached` pid 1 gen 1 — the observer enrolls | admitter |
| 3 | `Detached` pid 0 gen 1, cause `Superseded` | observer |
| 4 | `Attached` pid 0 gen 2 — the credential attach | observer |
| 5 | `OrdinaryRecord` from pid 0 | observer |

Two facts here are **not** guessable from the request list, and are the reason a
capture beats a prose spec:

1. **A credential attach on a live binding supersedes it rather than failing.**
   The admitter enrolls (binding at generation 1) and then immediately presents
   its credential. That does not error: the server retires the old binding —
   emitting a `Detached` record carrying `cause: Superseded` at seq 3 — and
   installs a new one at the **rotated** generation 2, emitting `Attached` at
   seq 4. It also mints a **fresh** attach secret. Every subsequent request must
   present generation 2; presenting 1 is stale authority.

2. **A record is never delivered to the participant that admitted it.** Seq 5
   reaches the observer and never the admitter. A lone participant therefore
   holds no delivery obligation at all — which is why a single-connection trace
   would contain no `ParticipantDelivery` frame, and why this capture needs two
   connections.

---

## 5. Identity is minted, never declared

The whole shape of the wire follows from this.

`EnrollmentRequest` carries **exactly two fields** — the conversation and a
client-minted enrollment token
(`crates/liminal-protocol/src/wire/request.rs:11-16@339e81a`). It does not, and
cannot, state who the client is:

```
1a 00 00000000 0000001e 0001 0000 0001    prefix, discriminant 0x0001
0000000000000069                          conversation_id
69696969696969696969696969696969          enrollment_token (client-minted)
```

The 40-byte enrollment request is answered with a 156-byte `EnrollBound` that
**hands back** the identity:

- `participant_id` at offset 42 — minted by the server.
- `attach_secret` at offset 59, 32 bytes — minted by the server from
  `/dev/urandom` (`production/facts.rs:42@339e81a`, `mint_secret_bytes`).
- `origin_binding_epoch` at offset 91 — server incarnation, connection ordinal,
  capability generation.
- `receipt_expires_at` / `provenance_expires_at` at offsets 124 and 140, u128 BE
  epoch milliseconds (`production/facts.rs:57@339e81a`, `now_unix_millis`).

Every later request quotes the minted `participant_id`, `capability_generation`
and (for attach) `attach_secret` back. A client that invents any of them is
refused.

### The one field a client must get right on its own

`RecordAdmission.record_admission_attempt_token` is the client's half of
admission idempotence. The server deduplicates on the triple *(token, payload
fingerprint, verified participant)*.

Mint it **once per record**, persist it beside the staged bytes, and re-present
that exact token after a lost answer. Deriving it per *presentation* from
anything that can change between attempts silently defeats dedup — and that is
not hypothetical. The doc comment at
`crates/liminal-protocol/src/wire/request.rs:91-132@339e81a` records the field
incident: a client derived the token per presentation using the current
capability generation as an input, a recovery attach rotated the generation 4→6
between two presentations, and the same bytes arrived under two different tokens
and committed twice.

The server deliberately does not close this from its side, because two
intent-distinct sends of an identical body must remain two commits.

---

## 6. Responses are ordered; pushes are a schedule

The request/response spine is deterministic: one `ServerValue` per
`ClientRequest`, in order.

Pushes are not. A `ParticipantDelivery` is not a reply — its position in the
inbound stream, and how many times an unacknowledged obligation is re-offered,
are decided by the server's publication pump.

**This was measured, not assumed.** Five consecutive runs of this identical
scenario at `339e81a` put the admitter's first delivery *before* `AttachBound`
in two runs and *after* it in three. The committed
`admitter.s2c.bin` also contains the **same delivery twice** (seq 2, at stream
offsets 174 and 404) because the admitter never acknowledged it: delivery is
at-least-once.

Consequences for a foreign implementation:

- Demultiplex on the discriminant. Read frames in a loop; stash pushes; stop
  when the `ServerValue` arrives. There is no correlation id to match on.
- Be idempotent over `delivery_seq`. The same sequence can arrive more than
  once.
- Do not assume a push arrives at all before a given response.

The harness holds pushes to the weaker standard this permits: every push
observed must be one of the committed push **images**, but the count and
position are free.

---

## 7. Which bytes are structural and which are run-variable

A foreign implementer needs to know exactly which bytes to expect verbatim and
which to compute or read back. In this capture there are only **two kinds** of
run-variable bytes:

| field | width | why it moves | where it appears |
|-------|-------|--------------|------------------|
| `attach_secret` | 32 | 32 bytes of `/dev/urandom` per enrollment and per credential rotation (`production/facts.rs:42@339e81a`). A predictable attach secret must never be issued, so this is nondeterministic **on purpose**. | every `EnrollBound` / `AttachBound`, and echoed in the `CredentialAttachRequest` that quotes it back |
| `receipt_expires_at`, `provenance_expires_at` | 16 each | wall-clock reads plus the configured TTLs (`production/facts.rs:57@339e81a`). Two runs a few milliseconds apart stamp different deadlines. | every `EnrollBound` / `AttachBound` |

A useful detail for a reader of the hex: these deadlines are `u128` big-endian
holding epoch **milliseconds**, so their top ten bytes are zero for any
plausible date. Only the low six bytes move.

Everything else in this capture is structural and must be reproduced byte for
byte.

### How that claim is enforced rather than asserted

`frames.jsonl` records the run-variable ranges per frame. The harness locates
them by searching each frame for the exact 32-byte secret and 16-byte deadline
the server produced *that run* — the substitution technique
`tests/loopback_parity_e2e.rs@339e81a` uses for the same purpose — and then:

1. masks the fresh frames and the committed frames at those recorded ranges, and
2. requires the two masked images to be **byte-identical**.

So the split above is a measured claim. If any byte outside a declared variable
range moved, the test goes red. That discriminator was proven both ways at
`339e81a`: flipping a byte *inside* a declared range leaves the test green
(correct — it is masked), and flipping a structural byte in the same frame turns
it red with `a request/response byte outside every declared run-variable range
changed`.

`participant_id` deserves a note. It is **minted**, so a foreign implementer
must always read it out of `EnrollBound` and never predict it. In this capture
it happens to be `0` and `1`, because identities are handed out from slot 0 in a
fresh store and the session enrolls exactly two. Treat the values in these bytes
as an artifact of a fresh store, not as a contract.

### The independent reader

`decompose.py` in this directory is deliberately *not* a wrapper around the Rust
codec. It is a second reading of these bytes written from the wire contract and
the encoder source alone, in another language, sharing no code. That makes it
the first foreign implementation of this wire's read path, and its agreement
with the capture is evidence rather than tautology.

It walks each frame field by field and requires every frame to be consumed to
**exactly zero** remaining bytes; a misunderstood field cannot hide, because the
frame's own declared `payload_length` has to close over the fields the reader
names. At `339e81a` all **17** participant frames decompose exactly. Its
discriminating power was checked the other way too: dropping the record
payload's length prefix from `0x12` to `0x11` makes it exit non-zero with
`UNCONSUMED 1 bytes`.

The field tables in §9 are that reader's output — machine-produced from the
committed bytes, not transcribed.

---

## 8. What this capture does NOT cover

Named explicitly so nobody mistakes one happy session for full coverage.

**Not exercised at all:**

- **Every refusal and error frame.** `ServerValue` has 36 variants
  (`crates/liminal-protocol/src/wire/response.rs:1728-1801@339e81a`); this
  capture contains **five**. Nothing here shows `StaleAuthority`,
  `ParticipantUnknown`, `NoBinding`, `Retired`, `AttemptTokenBodyConflict`,
  `RecordTooLarge`, `ReceiptExpired`, any capacity-exceeded variant, or
  `ParticipantTransportRejected` — and note that `0x0100` and `0x0121`+ fall
  *outside* the `originating_request` range, so their body layout differs from
  every response captured here.
- **`ParticipantTransportRejected` and the inbound gate**
  (`codec.rs:223-270@339e81a`): oversize frames, unauthenticated frames, and
  pre-capability limits.
- **Leave** (`0x0005`) and **MarkerAck** (`0x0006`) requests, and their
  responses `LeaveCommitted` / `MarkerAckCommitted`.
- **`ObserverRecoveryHandshake`** (`0x0008`) and `ObserverProgressed` (`0x0200`)
  — the entire reconnect-recovery path.
- **Resume and replay.** Nothing here reconnects, resumes from a persisted
  cursor, or replays an attempt token after a lost answer. The idempotence
  contract described in §5 is *documented* here and *not exercised*.
- **Record kinds `Died`, `Left`, `HistoryCompacted`** (`0x0003`–`0x0005`), and
  the `Died` variable-length `UncleanServerRestart` tail.
- **Markers**, fenced recovery attach, and
  `accept_marker_delivery_seq` in its present form — every optional in this
  capture is absent (`0x00`), so the **present** branch of every optional field
  is untested here.
- **Non-empty `auth_token`.** The handshake captured here is unauthenticated.

**Different transports:**

- **WebSocket.** The participant wire also runs over WS
  (`server/connection/websocket.rs`), where these same frames are carried as
  binary messages. Not captured.
- **The in-process loopback mount.** Deliberately out of scope — see §2.

**Scale and shape:**

- One conversation, two participants, one 18-byte record, no fragmentation, no
  frame anywhere near `wire_frame_limit`, no backpressure, no multi-conversation
  connection.

---

## 9. Appendix: generated field tables

Produced by `decompose.py` from the committed bytes. `RUN-VARIABLE` marks a
field overlapping a declared variable range in `frames.jsonl`; everything marked
`structural` must be reproduced verbatim.

```text
### admitter C->S #1  ClientRequest::EnrollmentRequest  len=40
    0..1    structural   frame_type                                             1a   # 0x1A: the generic type reserved for participant traffic
    1..2    structural   flags                                                  00   # MUST be 0x00; the decoder refuses otherwise
    2..6    structural   stream_id                                              00000000   # MUST be 0x00000000 on participant traffic
    6..10   structural   payload_length                                         0000001e   # u32 BE; complete frame = 10 + this
   10..12   structural   participant version.major                              0001   # MUST be 0x0001
   12..14   structural   participant version.minor                              0000   # MUST be 0x0000
   14..16   structural   discriminant                                           0001
   16..24   structural   conversation_id                                        0000000000000069
   24..40   structural   enrollment_token                                       69696969696969696969696969696969   # client-minted; identity is NOT declared here

### admitter S->C #1  ServerValue::EnrollBound  len=156
    0..1    structural   frame_type                                             1a   # 0x1A: the generic type reserved for participant traffic
    1..2    structural   flags                                                  00   # MUST be 0x00; the decoder refuses otherwise
    2..6    structural   stream_id                                              00000000   # MUST be 0x00000000 on participant traffic
    6..10   structural   payload_length                                         00000092   # u32 BE; complete frame = 10 + this
   10..12   structural   participant version.major                              0001   # MUST be 0x0001
   12..14   structural   participant version.minor                              0000   # MUST be 0x0000
   14..16   structural   discriminant                                           010a
   16..18   structural   originating_request                                    0001   # 0x0001 = ClientDiscriminant::EnrollmentRequest -- present ONLY for 0x0101..=0x0120
   18..26   structural   conversation_id                                        0000000000000069
   26..42   structural   token                                                  69696969696969696969696969696969   # the request's own attempt token, echoed
   42..50   structural   participant_id                                         0000000000000000   # MINTED BY THE SERVER
   50..51   structural   request_generation: presence tag (0x00 absent / 0x01 present) 00
   51..59   structural   capability_generation                                  0000000000000001   # the generation now in force
   59..91   RUN-VARIABLE attach_secret                                          731f2162dbc52575e55fd647318e0877153306bf3a3a0e2204f7141116fb64df   # MINTED: 32 bytes of entropy
   91..99   structural   origin_binding_epoch.connection_incarnation.server_incarnation 0000000000000001
   99..107  structural   origin_binding_epoch.connection_incarnation.connection_ordinal 0000000000000000
  107..115  structural   origin_binding_epoch.capability_generation             0000000000000001
  115..123  structural   persisted_cursor                                       0000000000000000
  123..124  structural   accepted_marker_delivery_seq: presence tag (0x00 absent / 0x01 present) 00
  124..140  RUN-VARIABLE receipt_expires_at                                     00000000000000000000019ff6f37d6a   # u128 BE, epoch milliseconds
  140..156  RUN-VARIABLE provenance_expires_at                                  00000000000000000000019ff6fbbaca   # u128 BE, epoch milliseconds

### admitter C->S #2  ClientRequest::CredentialAttachRequest  len=89
    0..1    structural   frame_type                                             1a   # 0x1A: the generic type reserved for participant traffic
    1..2    structural   flags                                                  00   # MUST be 0x00; the decoder refuses otherwise
    2..6    structural   stream_id                                              00000000   # MUST be 0x00000000 on participant traffic
    6..10   structural   payload_length                                         0000004f   # u32 BE; complete frame = 10 + this
   10..12   structural   participant version.major                              0001   # MUST be 0x0001
   12..14   structural   participant version.minor                              0000   # MUST be 0x0000
   14..16   structural   discriminant                                           0002
   16..24   structural   conversation_id                                        0000000000000069
   24..32   structural   participant_id                                         0000000000000000   # server-minted; quoted back from EnrollBound
   32..40   structural   capability_generation                                  0000000000000001
   40..72   RUN-VARIABLE attach_secret                                          731f2162dbc52575e55fd647318e0877153306bf3a3a0e2204f7141116fb64df   # server-minted; quoted back from EnrollBound
   72..88   structural   attach_attempt_token                                   6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a   # client-minted
   88..89   structural   accept_marker_delivery_seq: presence tag (0x00 absent / 0x01 present) 00

### admitter S->C #2  ServerPush::ParticipantDelivery  len=66
    0..1    structural   frame_type                                             1a   # 0x1A: the generic type reserved for participant traffic
    1..2    structural   flags                                                  00   # MUST be 0x00; the decoder refuses otherwise
    2..6    structural   stream_id                                              00000000   # MUST be 0x00000000 on participant traffic
    6..10   structural   payload_length                                         00000038   # u32 BE; complete frame = 10 + this
   10..12   structural   participant version.major                              0001   # MUST be 0x0001
   12..14   structural   participant version.minor                              0000   # MUST be 0x0000
   14..16   structural   discriminant                                           0201
   16..24   structural   conversation_id                                        0000000000000069
   24..32   structural   delivery_seq                                           0000000000000002
   32..34   structural   record_kind                                            0001   # 0x0001 = RecordKind::Attached
   34..42   structural   affected_participant_id                                0000000000000001
   42..50   structural   binding_epoch.connection_incarnation.server_incarnation 0000000000000001
   50..58   structural   binding_epoch.connection_incarnation.connection_ordinal 0000000000000001
   58..66   structural   binding_epoch.capability_generation                    0000000000000001

### admitter S->C #3  ServerValue::AttachBound  len=164
    0..1    structural   frame_type                                             1a   # 0x1A: the generic type reserved for participant traffic
    1..2    structural   flags                                                  00   # MUST be 0x00; the decoder refuses otherwise
    2..6    structural   stream_id                                              00000000   # MUST be 0x00000000 on participant traffic
    6..10   structural   payload_length                                         0000009a   # u32 BE; complete frame = 10 + this
   10..12   structural   participant version.major                              0001   # MUST be 0x0001
   12..14   structural   participant version.minor                              0000   # MUST be 0x0000
   14..16   structural   discriminant                                           0111
   16..18   structural   originating_request                                    0002   # 0x0002 = ClientDiscriminant::CredentialAttachRequest -- present ONLY for 0x0101..=0x0120
   18..26   structural   conversation_id                                        0000000000000069
   26..42   structural   token                                                  6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a   # the request's own attempt token, echoed
   42..50   structural   participant_id                                         0000000000000000   # MINTED BY THE SERVER
   50..51   structural   request_generation: presence tag (0x00 absent / 0x01 present) 01
   51..59   structural   request_generation: value                              0000000000000001
   59..67   structural   capability_generation                                  0000000000000002   # the generation now in force
   67..99   RUN-VARIABLE attach_secret                                          faeedfb30e242b28c5511080022bc5ad3bed832f61fedcfe1e39523b35d887bc   # MINTED: 32 bytes of entropy
   99..107  structural   origin_binding_epoch.connection_incarnation.server_incarnation 0000000000000001
  107..115  structural   origin_binding_epoch.connection_incarnation.connection_ordinal 0000000000000000
  115..123  structural   origin_binding_epoch.capability_generation             0000000000000002
  123..131  structural   persisted_cursor                                       0000000000000000
  131..132  structural   accepted_marker_delivery_seq: presence tag (0x00 absent / 0x01 present) 00
  132..148  RUN-VARIABLE receipt_expires_at                                     00000000000000000000019ff6f37e5a   # u128 BE, epoch milliseconds
  148..164  RUN-VARIABLE provenance_expires_at                                  00000000000000000000019ff6fbbbba   # u128 BE, epoch milliseconds

### admitter C->S #3  ClientRequest::RecordAdmission  len=78
    0..1    structural   frame_type                                             1a   # 0x1A: the generic type reserved for participant traffic
    1..2    structural   flags                                                  00   # MUST be 0x00; the decoder refuses otherwise
    2..6    structural   stream_id                                              00000000   # MUST be 0x00000000 on participant traffic
    6..10   structural   payload_length                                         00000044   # u32 BE; complete frame = 10 + this
   10..12   structural   participant version.major                              0001   # MUST be 0x0001
   12..14   structural   participant version.minor                              0000   # MUST be 0x0000
   14..16   structural   discriminant                                           0007
   16..24   structural   conversation_id                                        0000000000000069
   24..32   structural   participant_id                                         0000000000000000
   32..40   structural   capability_generation                                  0000000000000002
   40..56   structural   record_admission_attempt_token                         6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b   # client-minted ONCE PER RECORD
   56..60   structural   payload: length prefix                                 00000012   # u32 BE
   60..78   structural   payload: bytes                                         676f6c64656e2d74726163652d70302d3639   # opaque; never echoed in a response

### admitter S->C #4  ServerPush::ParticipantDelivery  len=66
    0..1    structural   frame_type                                             1a   # 0x1A: the generic type reserved for participant traffic
    1..2    structural   flags                                                  00   # MUST be 0x00; the decoder refuses otherwise
    2..6    structural   stream_id                                              00000000   # MUST be 0x00000000 on participant traffic
    6..10   structural   payload_length                                         00000038   # u32 BE; complete frame = 10 + this
   10..12   structural   participant version.major                              0001   # MUST be 0x0001
   12..14   structural   participant version.minor                              0000   # MUST be 0x0000
   14..16   structural   discriminant                                           0201
   16..24   structural   conversation_id                                        0000000000000069
   24..32   structural   delivery_seq                                           0000000000000002
   32..34   structural   record_kind                                            0001   # 0x0001 = RecordKind::Attached
   34..42   structural   affected_participant_id                                0000000000000001
   42..50   structural   binding_epoch.connection_incarnation.server_incarnation 0000000000000001
   50..58   structural   binding_epoch.connection_incarnation.connection_ordinal 0000000000000001
   58..66   structural   binding_epoch.capability_generation                    0000000000000001

### admitter S->C #5  ServerValue::RecordCommitted  len=74
    0..1    structural   frame_type                                             1a   # 0x1A: the generic type reserved for participant traffic
    1..2    structural   flags                                                  00   # MUST be 0x00; the decoder refuses otherwise
    2..6    structural   stream_id                                              00000000   # MUST be 0x00000000 on participant traffic
    6..10   structural   payload_length                                         00000040   # u32 BE; complete frame = 10 + this
   10..12   structural   participant version.major                              0001   # MUST be 0x0001
   12..14   structural   participant version.minor                              0000   # MUST be 0x0000
   14..16   structural   discriminant                                           011f
   16..18   structural   originating_request                                    0007   # 0x0007 = ClientDiscriminant::RecordAdmission -- present ONLY for 0x0101..=0x0120
   18..26   structural   conversation_id                                        0000000000000069
   26..34   structural   participant_id                                         0000000000000000
   34..42   structural   capability_generation                                  0000000000000002
   42..58   structural   record_admission_attempt_token                         6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b   # echoed; the dedup key's first term
   58..66   structural   sender_participant_id                                  0000000000000000
   66..74   structural   delivery_seq                                           0000000000000005   # the sequence the record committed at

### admitter C->S #4  ClientRequest::DetachRequest  len=56
    0..1    structural   frame_type                                             1a   # 0x1A: the generic type reserved for participant traffic
    1..2    structural   flags                                                  00   # MUST be 0x00; the decoder refuses otherwise
    2..6    structural   stream_id                                              00000000   # MUST be 0x00000000 on participant traffic
    6..10   structural   payload_length                                         0000002e   # u32 BE; complete frame = 10 + this
   10..12   structural   participant version.major                              0001   # MUST be 0x0001
   12..14   structural   participant version.minor                              0000   # MUST be 0x0000
   14..16   structural   discriminant                                           0003
   16..24   structural   conversation_id                                        0000000000000069
   24..32   structural   participant_id                                         0000000000000000
   32..40   structural   capability_generation                                  0000000000000002
   40..56   structural   detach_attempt_token                                   6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c   # client-minted

### admitter S->C #6  ServerValue::DetachCommitted  len=90
    0..1    structural   frame_type                                             1a   # 0x1A: the generic type reserved for participant traffic
    1..2    structural   flags                                                  00   # MUST be 0x00; the decoder refuses otherwise
    2..6    structural   stream_id                                              00000000   # MUST be 0x00000000 on participant traffic
    6..10   structural   payload_length                                         00000050   # u32 BE; complete frame = 10 + this
   10..12   structural   participant version.major                              0001   # MUST be 0x0001
   12..14   structural   participant version.minor                              0000   # MUST be 0x0000
   14..16   structural   discriminant                                           0117
   16..18   structural   originating_request                                    0003   # 0x0003 = ClientDiscriminant::DetachRequest -- present ONLY for 0x0101..=0x0120
   18..26   structural   conversation_id                                        0000000000000069
   26..34   structural   participant_id                                         0000000000000000
   34..42   structural   capability_generation                                  0000000000000002
   42..58   structural   detach_attempt_token                                   6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c   # echoed
   58..66   structural   committed_binding_epoch.connection_incarnation.server_incarnation 0000000000000001
   66..74   structural   committed_binding_epoch.connection_incarnation.connection_ordinal 0000000000000000
   74..82   structural   committed_binding_epoch.capability_generation          0000000000000002
   82..90   structural   detached_delivery_seq                                  0000000000000006   # the sequence the Detached record landed at

### observer C->S #1  ClientRequest::EnrollmentRequest  len=40
    0..1    structural   frame_type                                             1a   # 0x1A: the generic type reserved for participant traffic
    1..2    structural   flags                                                  00   # MUST be 0x00; the decoder refuses otherwise
    2..6    structural   stream_id                                              00000000   # MUST be 0x00000000 on participant traffic
    6..10   structural   payload_length                                         0000001e   # u32 BE; complete frame = 10 + this
   10..12   structural   participant version.major                              0001   # MUST be 0x0001
   12..14   structural   participant version.minor                              0000   # MUST be 0x0000
   14..16   structural   discriminant                                           0001
   16..24   structural   conversation_id                                        0000000000000069
   24..40   structural   enrollment_token                                       6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e   # client-minted; identity is NOT declared here

### observer S->C #1  ServerValue::EnrollBound  len=156
    0..1    structural   frame_type                                             1a   # 0x1A: the generic type reserved for participant traffic
    1..2    structural   flags                                                  00   # MUST be 0x00; the decoder refuses otherwise
    2..6    structural   stream_id                                              00000000   # MUST be 0x00000000 on participant traffic
    6..10   structural   payload_length                                         00000092   # u32 BE; complete frame = 10 + this
   10..12   structural   participant version.major                              0001   # MUST be 0x0001
   12..14   structural   participant version.minor                              0000   # MUST be 0x0000
   14..16   structural   discriminant                                           010a
   16..18   structural   originating_request                                    0001   # 0x0001 = ClientDiscriminant::EnrollmentRequest -- present ONLY for 0x0101..=0x0120
   18..26   structural   conversation_id                                        0000000000000069
   26..42   structural   token                                                  6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e   # the request's own attempt token, echoed
   42..50   structural   participant_id                                         0000000000000001   # MINTED BY THE SERVER
   50..51   structural   request_generation: presence tag (0x00 absent / 0x01 present) 00
   51..59   structural   capability_generation                                  0000000000000001   # the generation now in force
   59..91   RUN-VARIABLE attach_secret                                          459fec8447f666c4c7695dc9009ae972203f345316c77f41063266265811d40c   # MINTED: 32 bytes of entropy
   91..99   structural   origin_binding_epoch.connection_incarnation.server_incarnation 0000000000000001
   99..107  structural   origin_binding_epoch.connection_incarnation.connection_ordinal 0000000000000001
  107..115  structural   origin_binding_epoch.capability_generation             0000000000000001
  115..123  structural   persisted_cursor                                       0000000000000000
  123..124  structural   accepted_marker_delivery_seq: presence tag (0x00 absent / 0x01 present) 00
  124..140  RUN-VARIABLE receipt_expires_at                                     00000000000000000000019ff6f37e24   # u128 BE, epoch milliseconds
  140..156  RUN-VARIABLE provenance_expires_at                                  00000000000000000000019ff6fbbb84   # u128 BE, epoch milliseconds

### observer S->C #2  ServerPush::ParticipantDelivery  len=68
    0..1    structural   frame_type                                             1a   # 0x1A: the generic type reserved for participant traffic
    1..2    structural   flags                                                  00   # MUST be 0x00; the decoder refuses otherwise
    2..6    structural   stream_id                                              00000000   # MUST be 0x00000000 on participant traffic
    6..10   structural   payload_length                                         0000003a   # u32 BE; complete frame = 10 + this
   10..12   structural   participant version.major                              0001   # MUST be 0x0001
   12..14   structural   participant version.minor                              0000   # MUST be 0x0000
   14..16   structural   discriminant                                           0201
   16..24   structural   conversation_id                                        0000000000000069
   24..32   structural   delivery_seq                                           0000000000000003
   32..34   structural   record_kind                                            0002   # 0x0002 = RecordKind::Detached
   34..42   structural   affected_participant_id                                0000000000000000
   42..50   structural   binding_epoch.connection_incarnation.server_incarnation 0000000000000001
   50..58   structural   binding_epoch.connection_incarnation.connection_ordinal 0000000000000000
   58..66   structural   binding_epoch.capability_generation                    0000000000000001
   66..68   structural   close_cause tag                                        0005   # 0x0005 = Superseded

### observer S->C #3  ServerPush::ParticipantDelivery  len=66
    0..1    structural   frame_type                                             1a   # 0x1A: the generic type reserved for participant traffic
    1..2    structural   flags                                                  00   # MUST be 0x00; the decoder refuses otherwise
    2..6    structural   stream_id                                              00000000   # MUST be 0x00000000 on participant traffic
    6..10   structural   payload_length                                         00000038   # u32 BE; complete frame = 10 + this
   10..12   structural   participant version.major                              0001   # MUST be 0x0001
   12..14   structural   participant version.minor                              0000   # MUST be 0x0000
   14..16   structural   discriminant                                           0201
   16..24   structural   conversation_id                                        0000000000000069
   24..32   structural   delivery_seq                                           0000000000000004
   32..34   structural   record_kind                                            0001   # 0x0001 = RecordKind::Attached
   34..42   structural   affected_participant_id                                0000000000000000
   42..50   structural   binding_epoch.connection_incarnation.server_incarnation 0000000000000001
   50..58   structural   binding_epoch.connection_incarnation.connection_ordinal 0000000000000000
   58..66   structural   binding_epoch.capability_generation                    0000000000000002

### observer S->C #4  ServerPush::ParticipantDelivery  len=64
    0..1    structural   frame_type                                             1a   # 0x1A: the generic type reserved for participant traffic
    1..2    structural   flags                                                  00   # MUST be 0x00; the decoder refuses otherwise
    2..6    structural   stream_id                                              00000000   # MUST be 0x00000000 on participant traffic
    6..10   structural   payload_length                                         00000036   # u32 BE; complete frame = 10 + this
   10..12   structural   participant version.major                              0001   # MUST be 0x0001
   12..14   structural   participant version.minor                              0000   # MUST be 0x0000
   14..16   structural   discriminant                                           0201
   16..24   structural   conversation_id                                        0000000000000069
   24..32   structural   delivery_seq                                           0000000000000005
   32..34   structural   record_kind                                            0000   # 0x0000 = RecordKind::OrdinaryRecord
   34..42   structural   sender_participant_id                                  0000000000000000
   42..46   structural   payload: length prefix                                 00000012   # u32 BE
   46..64   structural   payload: bytes                                         676f6c64656e2d74726163652d70302d3639

### observer C->S #2  ClientRequest::ParticipantAck  len=48
    0..1    structural   frame_type                                             1a   # 0x1A: the generic type reserved for participant traffic
    1..2    structural   flags                                                  00   # MUST be 0x00; the decoder refuses otherwise
    2..6    structural   stream_id                                              00000000   # MUST be 0x00000000 on participant traffic
    6..10   structural   payload_length                                         00000026   # u32 BE; complete frame = 10 + this
   10..12   structural   participant version.major                              0001   # MUST be 0x0001
   12..14   structural   participant version.minor                              0000   # MUST be 0x0000
   14..16   structural   discriminant                                           0004
   16..24   structural   conversation_id                                        0000000000000069
   24..32   structural   participant_id                                         0000000000000001
   32..40   structural   capability_generation                                  0000000000000001
   40..48   structural   through_seq                                            0000000000000005   # cumulative: everything at or below this is acknowledged

### observer S->C #5  ServerValue::AckCommitted  len=58
    0..1    structural   frame_type                                             1a   # 0x1A: the generic type reserved for participant traffic
    1..2    structural   flags                                                  00   # MUST be 0x00; the decoder refuses otherwise
    2..6    structural   stream_id                                              00000000   # MUST be 0x00000000 on participant traffic
    6..10   structural   payload_length                                         00000030   # u32 BE; complete frame = 10 + this
   10..12   structural   participant version.major                              0001   # MUST be 0x0001
   12..14   structural   participant version.minor                              0000   # MUST be 0x0000
   14..16   structural   discriminant                                           0119
   16..18   structural   originating_request                                    0004   # 0x0004 = ClientDiscriminant::ParticipantAck -- present ONLY for 0x0101..=0x0120
   18..26   structural   conversation_id                                        0000000000000069
   26..34   structural   participant_id                                         0000000000000001
   34..42   structural   capability_generation                                  0000000000000001
   42..50   structural   through_seq                                            0000000000000005   # echoed
   50..58   structural   current_cursor                                         0000000000000005   # where the server now holds this participant

17 participant frames decomposed to exactly zero remaining bytes.
```
