//! Stable refusal reason codes, and the band map that governs who mints them.
//!
//! A refusal that travels back to a client carries a `u16` reason code beside
//! its human-readable message. The code is the part a program may branch on;
//! the message is the part a person reads. More than one crate in this
//! workspace mints such codes, and a client that hardcodes a number cannot see
//! which crate minted it — so the numeric space is partitioned into bands, and
//! the partition is recorded here, beside the codes it governs.
//!
//! # The band map
//!
//! | Band | Owner | Currently minted in |
//! | --- | --- | --- |
//! | `0x0000`–`0x00FF` | protocol layer — parse, negotiation, auth | `ProtocolError`'s associated consts in the `liminal` crate, `crates/liminal/src/protocol/error.rs:56-72` (`0x0001`–`0x0009` in use) |
//! | `0x0100`–`0x01FF` | server layer — channel-roster refusals | this module ([`CHANNEL_NOT_REGISTERED_CODE`], [`CHANNEL_QUIESCED_CODE`]; the rest of the band is reserved for further roster refusals) |
//! | `0xFFFF` | the server's undifferentiated error | `SERVER_ERROR_CODE` in `liminal-server`, minted privately in three modules: `src/server/connection/apply.rs:26`, `src/server/connection/pending_reply.rs:51`, `src/server/connection/delivery.rs:45` |
//!
//! The map is the fence. Two crates minting `u16` reason codes with no shared
//! registry collide eventually, and the collision is silent: a client reads a
//! number that means one thing to the crate that sent it and another to the
//! crate that documented it. A new code is minted by taking the next free value
//! inside its layer's band and recording it in the row above; a band that has
//! no row here has no owner and may not be minted into.
//!
//! Channel-roster refusals sit outside the protocol band deliberately. A name
//! that is not on the roster is an application-layer statement about server
//! state, not a parse, negotiation, or authentication failure; filing it under
//! the protocol band would make that band mean nothing.
//!
//! The codes live in this crate rather than beside the protocol-layer consts
//! they neighbour numerically, because `liminal-protocol` is the crate a wire
//! client actually depends on: `liminal-sdk` requires it unconditionally and
//! takes `liminal` only as an optional dependency, so a code minted in
//! `liminal` — or in `liminal-server` — is a code every SDK client would
//! otherwise hardcode as a bare literal.
//!
//! # Not the participant tag registry
//!
//! [`crate::wire::ServerDiscriminant`] also occupies `0x0100..=0x0124`, and
//! [`crate::wire::ClientDiscriminant`] also occupies `0x0001..=0x0008`. Those
//! are a different `u16` altogether: the discriminant field selecting a
//! participant value inside a participant frame (see [`crate::outcome`]), not a
//! refusal's reason code. The two registries reuse numbers without colliding
//! because they never share a field. Nothing in the band map above governs,
//! reserves, or constrains that registry, and nothing in that registry
//! reserves a reason code.

#[cfg(test)]
mod band_tests;

/// The named channel is not on the server's roster.
///
/// Reserved in the server-layer band (`0x0100`–`0x01FF`). The code is reserved
/// for the admission decision itself: a failure raised *after* a channel was
/// admitted is not a statement about the roster and carries the
/// undifferentiated `0xFFFF` instead.
pub const CHANNEL_NOT_REGISTERED_CODE: u16 = 0x0101;

/// The named channel is on the roster but quiesced; the reason it was quiesced
/// travels in the refusal's message, not in this code.
///
/// Reserved in the server-layer band (`0x0100`–`0x01FF`). It is a distinct code
/// rather than a reuse of [`CHANNEL_NOT_REGISTERED_CODE`] because the two say
/// opposite things about the roster, and rather than a fallback to `0xFFFF`
/// because a quiesced channel would then be indistinguishable from an internal
/// fault.
pub const CHANNEL_QUIESCED_CODE: u16 = 0x0102;
