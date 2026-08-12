# Participant-wire golden trace

A byte-exact capture of one canonical participant session, taken at the socket
seam of a real `ServerListener` on `127.0.0.1:0`, kept as evidence for anyone
writing a foreign implementation of the participant wire.

**Start with [`WALKTHROUGH.md`](WALKTHROUGH.md).** It maps every byte to the
code that encodes it, names which byte ranges are run-variable, and — in §8 —
states plainly what this capture does *not* cover.

## Contents

| path | what it is |
|------|------------|
| `WALKTHROUGH.md` | the annotated frame-by-frame document; read this first |
| `admitter.c2s.bin` `admitter.s2c.bin` | raw bytes, both directions, connection A |
| `observer.c2s.bin` `observer.s2c.bin` | raw bytes, both directions, connection B |
| `frames.jsonl` | per-frame index: direction, offset, length, discriminant, hex, run-variable ranges |
| `session.hex` | annotated hexdump with direction markers and a per-byte structural/variable mask |
| `decompose.py` | an independent field-by-field reader, sharing no code with the Rust codec |
| `DIGESTS.txt` | `(rev, path, sha256)` for every artifact here |

The generator is
`crates/liminal-server/tests/golden_trace_participant_wire.rs`, committed
alongside: a frozen capture with no generator is a mystery, not evidence.

## Check it

The capture is not decoration — it is pinned. This re-runs the whole session
against a fresh server and compares it, masked at the recorded run-variable
ranges, against what is committed here:

```sh
cargo test -p liminal-server --test golden_trace_participant_wire
python3 docs/wire/golden-trace/decompose.py
sha256sum -c <(awk '{print $3"  "$2}' DIGESTS.txt)   # or shasum -a 256 on macOS
```

## Regenerate it

```sh
LIMINAL_GOLDEN_TRACE_OUT=docs/wire/golden-trace \
  cargo test -p liminal-server --test golden_trace_participant_wire
```

Regenerating rewrites the evidence, so re-run `decompose.py` afterwards and
refresh `DIGESTS.txt`. Do not regenerate to make a red test green: the test goes
red precisely when a byte outside a declared run-variable range moved, and that
is a finding, not a nuisance.

## The one-paragraph version

Participant traffic rides inside the ordinary liminal connection as generic
frame type `0x1A`, after a legacy `Connect`/`ConnectAck` handshake in which the
server must advertise the participant capability bit. Every participant frame
carries a fixed 16-byte prefix (10-byte generic header, then version `1.0` and a
16-bit discriminant), all integers big-endian, byte blobs `u32`-length-prefixed,
optionals a one-byte presence tag. Identity is **minted**: enrollment sends only
`{conversation_id, enrollment_token}` and the server hands back the
`participant_id`, an `attach_secret`, and a capability generation that every
later request must quote. There is no correlation id, so a client demultiplexes
responses from unsolicited pushes on the discriminant alone.
