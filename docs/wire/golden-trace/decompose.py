#!/usr/bin/env python3
"""Field-by-field decomposition of the committed participant-wire golden trace.

WHAT THIS IS, AND WHY IT IS PYTHON

This is deliberately NOT a wrapper around the Rust codec. It is a second,
independent reading of the same bytes, written from the wire contract and the
encoder source alone, in a different language, with no shared code. That makes
it the first foreign implementation of the participant wire's read path -- which
is precisely the thing the repository did not have and the reason the golden
trace exists.

Its agreement with the capture is therefore evidence, not tautology. It reads
every captured frame, walks it field by field, and requires each frame to be
consumed to EXACTLY zero remaining bytes. A field this reader gets wrong shows
up immediately as a leftover or a short read, because the frame's own declared
payload_length has to close over the fields it names.

It also generates the field tables in WALKTHROUGH.md, so those tables are
machine-produced from the committed bytes rather than transcribed by hand.

USAGE

    python3 docs/wire/golden-trace/decompose.py [capture_dir]

Exits non-zero if any frame fails to decompose exactly. That is the check --
run it after regenerating the capture.

SCOPE

Covers exactly the frame kinds the canonical session contains. An unknown
discriminant is reported as unconsumed rather than skipped, so widening the
session without widening this reader fails loudly instead of silently passing.
"""

import json
import sys
from pathlib import Path

# Registries, transcribed from crates/liminal-protocol/src/wire/tags.rs @ 339e81a.
CLIENT = {
    0x0001: "EnrollmentRequest",
    0x0002: "CredentialAttachRequest",
    0x0003: "DetachRequest",
    0x0004: "ParticipantAck",
    0x0005: "LeaveRequest",
    0x0006: "MarkerAck",
    0x0007: "RecordAdmission",
    0x0008: "ObserverRecoveryHandshake",
}
SERVER = {
    0x010A: "EnrollBound",
    0x0111: "AttachBound",
    0x0117: "DetachCommitted",
    0x0119: "AckCommitted",
    0x011F: "RecordCommitted",
}
PUSH = {0x0200: "ObserverProgressed", 0x0201: "ParticipantDelivery"}
RECORD_KIND = {
    0x0000: "OrdinaryRecord",
    0x0001: "Attached",
    0x0002: "Detached",
    0x0003: "Died",
    0x0004: "Left",
    0x0005: "HistoryCompacted",
}
CLOSE_CAUSE = {
    0x0000: "CleanDeregister",
    0x0001: "ConnectionLost",
    0x0002: "ProcessKilled",
    0x0003: "ProtocolError",
    0x0005: "Superseded",
}

# Server values in this INCLUSIVE range carry a leading u16 naming the client
# discriminant that caused them, before their own body. Outside it they do not.
# crates/liminal-protocol/src/wire/server_codec.rs:49-54 @ 339e81a.
ORIGIN_RANGE = range(0x0101, 0x0121)


class Reader:
    """A cursor that records every field it takes, so the walk is auditable."""

    def __init__(self, raw):
        self.raw = raw
        self.at = 0
        self.rows = []

    def take(self, n, name, note=""):
        if self.at + n > len(self.raw):
            raise ValueError(f"short read taking {n} bytes for {name!r} at {self.at}")
        chunk = self.raw[self.at : self.at + n]
        self.rows.append((self.at, n, name, chunk.hex(), note))
        self.at += n
        return chunk

    def uint(self, n, name, note=""):
        return int.from_bytes(self.take(n, name, note), "big")

    def option(self, name, width, note=""):
        """A one-byte presence tag, then the value only when it is 1."""
        if self.uint(1, f"{name}: presence tag (0x00 absent / 0x01 present)", note):
            return self.uint(width, f"{name}: value")
        return None


def generic_header(r):
    r.take(1, "frame_type", "0x1A: the generic type reserved for participant traffic")
    r.take(1, "flags", "MUST be 0x00; the decoder refuses otherwise")
    r.take(4, "stream_id", "MUST be 0x00000000 on participant traffic")
    payload = r.uint(4, "payload_length", "u32 BE; complete frame = 10 + this")
    major = r.uint(2, "participant version.major", "MUST be 0x0001")
    minor = r.uint(2, "participant version.minor", "MUST be 0x0000")
    if (major, minor) != (1, 0):
        raise ValueError(f"unsupported participant version {major}.{minor}")
    if payload + 10 != len(r.raw):
        raise ValueError(f"payload_length {payload} does not close a {len(r.raw)}-byte frame")
    return r.uint(2, "discriminant", "")


def binding_epoch(r, prefix):
    r.uint(8, f"{prefix}.connection_incarnation.server_incarnation")
    r.uint(8, f"{prefix}.connection_incarnation.connection_ordinal")
    r.uint(8, f"{prefix}.capability_generation")


def client_request(r, tag):
    """crates/liminal-protocol/src/wire/codec.rs:462-522 @ 339e81a."""
    if tag == 0x0001:
        r.uint(8, "conversation_id")
        r.take(16, "enrollment_token", "client-minted; identity is NOT declared here")
    elif tag == 0x0002:
        r.uint(8, "conversation_id")
        r.uint(8, "participant_id", "server-minted; quoted back from EnrollBound")
        r.uint(8, "capability_generation")
        r.take(32, "attach_secret", "server-minted; quoted back from EnrollBound")
        r.take(16, "attach_attempt_token", "client-minted")
        r.option("accept_marker_delivery_seq", 8)
    elif tag == 0x0003:
        r.uint(8, "conversation_id")
        r.uint(8, "participant_id")
        r.uint(8, "capability_generation")
        r.take(16, "detach_attempt_token", "client-minted")
    elif tag == 0x0004:
        r.uint(8, "conversation_id")
        r.uint(8, "participant_id")
        r.uint(8, "capability_generation")
        r.uint(8, "through_seq", "cumulative: everything at or below this is acknowledged")
    elif tag == 0x0007:
        r.uint(8, "conversation_id")
        r.uint(8, "participant_id")
        r.uint(8, "capability_generation")
        r.take(16, "record_admission_attempt_token", "client-minted ONCE PER RECORD")
        size = r.uint(4, "payload: length prefix", "u32 BE")
        r.take(size, "payload: bytes", "opaque; never echoed in a response")
    else:
        raise ValueError(f"unhandled client discriminant 0x{tag:04X}")


def server_value(r, tag):
    """crates/liminal-protocol/src/wire/server_codec.rs:749-885 @ 339e81a."""
    if tag in ORIGIN_RANGE:
        raw = r.uint(2, "originating_request", "")
        head = r.rows[-1]
        r.rows[-1] = (
            head[0],
            head[1],
            head[2],
            head[3],
            f"0x{raw:04X} = ClientDiscriminant::{CLIENT.get(raw, '??')} "
            "-- present ONLY for 0x0101..=0x0120",
        )
    if tag in (0x010A, 0x0111):
        r.uint(8, "conversation_id")
        r.take(16, "token", "the request's own attempt token, echoed")
        r.uint(8, "participant_id", "MINTED BY THE SERVER")
        r.option("request_generation", 8)
        r.uint(8, "capability_generation", "the generation now in force")
        r.take(32, "attach_secret", "MINTED: 32 bytes of entropy")
        binding_epoch(r, "origin_binding_epoch")
        r.uint(8, "persisted_cursor")
        r.option("accepted_marker_delivery_seq", 8)
        r.uint(16, "receipt_expires_at", "u128 BE, epoch milliseconds")
        r.uint(16, "provenance_expires_at", "u128 BE, epoch milliseconds")
    elif tag == 0x0117:
        r.uint(8, "conversation_id")
        r.uint(8, "participant_id")
        r.uint(8, "capability_generation")
        r.take(16, "detach_attempt_token", "echoed")
        binding_epoch(r, "committed_binding_epoch")
        r.uint(8, "detached_delivery_seq", "the sequence the Detached record landed at")
    elif tag == 0x0119:
        r.uint(8, "conversation_id")
        r.uint(8, "participant_id")
        r.uint(8, "capability_generation")
        r.uint(8, "through_seq", "echoed")
        r.uint(8, "current_cursor", "where the server now holds this participant")
    elif tag == 0x011F:
        r.uint(8, "conversation_id")
        r.uint(8, "participant_id")
        r.uint(8, "capability_generation")
        r.take(16, "record_admission_attempt_token", "echoed; the dedup key's first term")
        r.uint(8, "sender_participant_id")
        r.uint(8, "delivery_seq", "the sequence the record committed at")
    else:
        raise ValueError(f"unhandled server discriminant 0x{tag:04X}")


def server_push(r, tag):
    """crates/liminal-protocol/src/wire/codec.rs:593-674 @ 339e81a."""
    if tag != 0x0201:
        raise ValueError(f"unhandled push discriminant 0x{tag:04X}")
    r.uint(8, "conversation_id")
    r.uint(8, "delivery_seq")
    kind = r.uint(2, "record_kind", f"see RecordKind")
    r.rows[-1] = (
        r.rows[-1][0],
        2,
        "record_kind",
        r.rows[-1][3],
        f"0x{kind:04X} = RecordKind::{RECORD_KIND.get(kind, '??')}",
    )
    if kind == 0x0000:
        r.uint(8, "sender_participant_id")
        size = r.uint(4, "payload: length prefix", "u32 BE")
        r.take(size, "payload: bytes")
    elif kind == 0x0001:
        r.uint(8, "affected_participant_id")
        binding_epoch(r, "binding_epoch")
    elif kind == 0x0002:
        r.uint(8, "affected_participant_id")
        binding_epoch(r, "binding_epoch")
        cause = r.uint(2, "close_cause tag", "")
        r.rows[-1] = (
            r.rows[-1][0],
            2,
            "close_cause tag",
            r.rows[-1][3],
            f"0x{cause:04X} = {CLOSE_CAUSE.get(cause, '??')}",
        )
    else:
        raise ValueError(f"unhandled record kind 0x{kind:04X}")


def label(direction, tag):
    if direction == "C->S":
        return f"ClientRequest::{CLIENT.get(tag, '??')}"
    if tag in PUSH:
        return f"ServerPush::{PUSH[tag]}"
    return f"ServerValue::{SERVER.get(tag, '??')}"


def main(argv):
    here = Path(argv[1]) if len(argv) > 1 else Path(__file__).resolve().parent
    index = here / "frames.jsonl"
    if not index.exists():
        print(f"no frame index at {index}", file=sys.stderr)
        return 2

    failures = 0
    decomposed = 0
    for line in index.read_text().splitlines():
        if not line.strip():
            continue
        frame = json.loads(line)
        raw = bytes.fromhex(frame["hex"])
        if not raw or raw[0] != 0x1A:
            continue  # the legacy Connect/ConnectAck handshake; not participant framing

        variable = set()
        for span in frame["run_variable_ranges"]:
            variable.update(range(span["offset"], span["offset"] + span["len"]))

        reader = Reader(raw)
        print(
            f"\n### {frame['connection']} {frame['direction']} #{frame['ordinal']}  "
            f"{frame['label']}  len={frame['len']}"
        )
        try:
            tag = generic_header(reader)
            if frame["direction"] == "C->S":
                client_request(reader, tag)
            elif tag in PUSH:
                server_push(reader, tag)
            else:
                server_value(reader, tag)
            expected = label(frame["direction"], tag)
            if expected != frame["label"]:
                raise ValueError(f"label disagrees: index says {frame['label']}, bytes say {expected}")
        except ValueError as error:
            print(f"  !! {error}")
            failures += 1
            continue

        for at, width, name, chunk, note in reader.rows:
            kind = "RUN-VARIABLE" if any(i in variable for i in range(at, at + width)) else "structural"
            suffix = f"   # {note}" if note else ""
            print(f"  {at:3}..{at + width:<4} {kind:<12} {name:<54} {chunk}{suffix}")

        if reader.at != len(raw):
            print(f"  !! UNCONSUMED {len(raw) - reader.at} bytes: {raw[reader.at:].hex()}")
            failures += 1
        else:
            decomposed += 1

    print(f"\n{decomposed} participant frames decomposed to exactly zero remaining bytes.")
    if failures:
        print(f"{failures} FRAMES FAILED", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
