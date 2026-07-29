//! Frame-type and header constants for the SDK's own wire encoder.
//!
//! RETAINED BUT UNCALLED IN PRODUCTION — see the note on `WireFrame` in the
//! parent module. Every constant here fed
//! `ProtocolRemoteTransport`, which framed a message and discarded the bytes.
//! That transport refuses now, so nothing outside this module's tests reads
//! them. The whole module is dead together or alive together, so the allow sits
//! once at module scope instead of on each constant.
#![allow(dead_code, reason = "orphaned by the unconnected-transport refusal")]

pub(super) const WIRE_HEADER_LEN: usize = 10;
pub(super) const FRAME_TYPE_SUBSCRIBE: u8 = 0x05;
pub(super) const FRAME_TYPE_PUBLISH: u8 = 0x09;
pub(super) const FRAME_TYPE_CONVERSATION_MESSAGE: u8 = 0x0D;
pub(super) const FRAME_TYPE_RESUME: u8 = 0x06;
pub(super) const FRAME_TYPE_ACCEPT: u8 = 0x10;
pub(super) const FRAME_TYPE_DEFER: u8 = 0x11;
pub(super) const FRAME_TYPE_REJECT: u8 = 0x12;
pub(super) const APPLICATION_STREAM_ID: u32 = 1;
