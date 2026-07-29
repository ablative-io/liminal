---
name: handshake-lane-seed-brief-2026-07-27
description: "Vesper's LIMINAL-HANDSHAKE-PROTOCOL seed brief VERBATIM (re-sent 2026-07-28 after routing loss into Cally's thread) — the WHY/WHAT-IT-MEANS for rescue branch 8c6679bb; lane not started"
metadata: 
  node_type: memory
  type: project
  originSessionId: 5b70322e-e7a9-451c-91ca-a3dfa7b05bd9
  modified: 2026-07-28T04:32:59.695Z
---

# LIMINAL-HANDSHAKE-PROTOCOL seed brief (Vesper Lynd, written 2026-07-27, re-sent to me 2026-07-28)

Provenance: original went into Cally's thread — routing loss, not session loss. Re-sent verbatim on my request (DM msg dd55f520). The rescue diff (`origin/rescue/handshake-protocol-snapshot` @ 8c6679bb, verified at my hands 2026-07-28) is the WHAT; this is the WHY. Related: [[emitter-design-round-2026-07-24]] (lane status lives there).

**VERBATIM TEXT OF THE BRIEF:**

The semantic statement for the lane, read from the rescue branch diff (origin/main...rescue/handshake-protocol-snapshot) at the bytes. Anchored to the rescued files, not reconstructed.

**What changes on the wire: exactly ONE frame — `WorkerRegister`.** A trailing field is appended after `identity`: a u32 descriptor count, then per descriptor three length-prefixed strings (`name`, `input_schema_json`, `output_schema_json`). That's the entire wire-format delta. `WorkerRegisterAck`, and every other frame, are untouched. `frame.rs` adds the `WorkerActivityDescriptor` struct and the `activities` field on `WorkerRegistration` ("empty identifies a pre-contract worker" — the doc states the compatibility intent).

**Compatibility, both directions:**
- **Old worker → new server: SAFE by construction.** The new decoder reads through `identity`, then checks `is_finished()` (the new `PayloadReader::is_finished` in payload.rs); an old frame ends exactly there, so `activities` decodes as empty — the pre-contract census. This is deliberate one-way tolerance, and it's what lets every existing worker keep registering.
- **New worker → old server: BREAKS, loudly.** The new encoder writes the u32 count UNCONDITIONALLY — even a zero census appends four bytes — and the old decoder's `finish()` refuses leftover payload bytes as a typed `ProtocolError`. So EVERY registration from a new-codec worker fails the stream against a ≤0.4.1 server. Not silent, but fatal to the connection.
- **Therefore the deployment-order constraint is: servers before workers**, and the version bump must say so. Under liminal's pre-1.0 discipline a one-direction wire break is 0.5.0 — Cally's guess is what the bytes support — but that's your call to make with these facts.

**A design point your review should decide, not inherit:** the change ships with NO `ProtocolVersion` negotiation touch — compatibility rides entirely on trailing-bytes sniffing (`is_finished`). That works, but it's implicit. Your review should decide deliberately between keeping the sniff (with a pin that an old-shaped frame decodes to the empty census) or gating on a protocol-version bump. Either is defensible; inheriting the sniff without deciding is not.

**One hunk is NOT protocol work and should probably be DROPPED, not cherry-picked: the `subscription.rs` change.** It replaces `spawn_native_trap_exit` (the 0.16.1 beamr API whose whole point is trap_exit set pre-runnable, by construction, no post-spawn race) with `spawn_native` + a host-side `set_trap_exit` afterwards — the exact shape the 0.16.1 API was built to close, per the comment the hunk deletes. My read: this is vendoring collateral — the vendored crate was made to build against a published beamr that lacked the newer API — not an intended design change. Keep `spawn_native_trap_exit` and take only the protocol files; if you believe the host-side ordering is genuinely race-free, that's a separate ruling to make with beamr's owner (Artemis), not a rider on a registration-frame lane.

**Test surface in the rescue:** codec tests_support gains the `activities` fixture field; one codec test addition. The lane should add the two pins the semantics above imply: old-shaped frame → empty census (forward tolerance), and a round-trip with a non-empty census.

**Aion-side consumption, for sequencing:** the handshake branch reads `registration.activities` at `liminal_transport.rs:1343` and validates via the shared `contracts.rs`; once liminal publishes, the branch drops the `[patch.crates-io]` + workspace exclude + the two example path-deps, bumps the pins, and re-gates in a fresh worktree per the ruling. Nothing else aion-side changes.

That's the whole intent, stated once. The rescue diff is the WHAT; this is the WHY and the WHAT-IT-MEANS.

**[END VERBATIM]**

My seat's notes (not part of the brief): lane NOT started — estate wind-down + emitter hold stand; starts only on Tom's/Waffles' word. When it starts, the brief's own demands on me: (1) rule sniff-vs-version-negotiation deliberately with a pin either way; (2) DROP the subscription.rs hunk unless separately ruled with Artemis; (3) the two named codec pins; (4) 0.5.0-shaped bump + servers-before-workers deployment note; (5) publish stays lead-gated as always.
