//! The in-process (loopback) transport (design `docs/design/IN-PROCESS-TRANSPORT.md`).
//!
//! **What the loopback secures is the RECORD PATH** (hardened draft r2 §5). It
//! carries the exact framed wire image, encoded and decoded on both sides
//! unchanged, so an in-process caller reaches the record through the same
//! preflight, the same participant gate, the same constant-time token compare,
//! and the same `apply_frame` seam a socket caller reaches it through. No
//! append arrives except through that door, and the door does not know which
//! mount knocked.
//!
//! **What the loopback does NOT secure is the mount.** An in-process mount is
//! TRUSTED CODE: a co-resident extension reaches the host's heap, its
//! descriptors, and its store handle without ever calling this transport, so
//! the record vouches for a trusted-code mount only as far as the host process
//! itself is trusted. Every append admitted here carries a mount attestation
//! from the admitting door precisely because the mount fact is what the
//! consumer must weigh; the transport is not a sandbox and must never be read
//! as one.
//!
//! This module is the third member of the transport family that
//! [`super::websocket`] joined first: it replaces the read and write halves
//! and nothing else. It lives INSIDE the `connection` module rather than
//! beside it because `spawn_transport_connection` is `pub(super)` on purpose —
//! that privacy wall is what keeps the runtime, the admission counter, the
//! incarnation authority, and the registry unreachable from outside, and the
//! loopback lives within the wall rather than widening it.
//!
//! Build order (§8) stages this module. Step 2 — this commit — is the byte
//! duplex alone: two bounded rings, wake-on-write, close semantics, and its
//! unit pins. The connection process, the supervisor spawn path, the mount
//! fact, and the SDK-side transport are steps 3 and 4 and are not here.

#[path = "loopback/duplex.rs"]
mod duplex;

#[cfg(test)]
#[path = "loopback/duplex_tests.rs"]
mod duplex_tests;

// `LoopbackServerEnd` is not re-exported: it is named through
// [`LoopbackDuplex::bounded`]'s return type by the step-3 sites, which live in
// this module's own subtree, and it needs no path of its own.
pub use duplex::{LoopbackClientEnd, LoopbackDuplex};
