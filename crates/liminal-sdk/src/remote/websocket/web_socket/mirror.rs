//! Platform-neutral browser-fact mirror for the wasm adapter (R3.2, F5).
//!
//! This module is the adapter's entire conversion layer: it maps the four
//! browser socket callbacks (`open`, `message`, `close`, `error`) onto the
//! driver's closed [`SocketEvent`] set, and maps emitted [`SocketCommand`]s
//! onto the closed [`BrowserSocketAction`] set the `web-sys` shim executes.
//!
//! F5 mirror-not-implementor: every function here is pure and stateless. The
//! mirror never validates frames, never correlates exchanges, never decides a
//! fate, and never retries — those judgments belong exclusively to the
//! transport-neutral driver core. Because no platform type appears here, the
//! whole layer compiles natively and is pinned by the deterministic
//! `ws_browser_mirror_trace` suite without a browser.

use alloc::vec::Vec;

use super::super::core::{SocketCommand, SocketEvent, SocketFailure};

/// Classified payload of one browser `message` event, as the `web-sys` shim
/// observes it after F4 pinned `binaryType = "arraybuffer"` at construction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BrowserMessageData {
    /// One complete binary message delivered as an `ArrayBuffer` — the only
    /// data shape the transport accepts (F4). The browser has already
    /// reassembled fragmented frames into this single complete message.
    ArrayBuffer(Vec<u8>),
    /// A text (string) message. The liminal wire contract admits only binary.
    Text,
    /// A `Blob` — the browser's asynchronous binary default, which F4 rules
    /// out by pinning `binaryType` before any message can arrive. Observing
    /// one means the transport contract broke; its contents are never read.
    Blob,
    /// Message data of any other JavaScript type.
    Unrecognized,
}

/// Converts one classified browser `message` event into the driver's event.
///
/// Text surfaces as the core's typed
/// [`SocketFailure::UnsupportedTextMessage`]; `Blob` and unrecognized data
/// are typed transport failures (F4). The mirror performs no frame
/// validation: canonical-frame judgment on `ArrayBuffer` bytes is the core's
/// alone (F5).
#[must_use]
pub fn message_event(data: BrowserMessageData) -> SocketEvent {
    match data {
        BrowserMessageData::ArrayBuffer(bytes) => SocketEvent::Binary(bytes),
        BrowserMessageData::Text => SocketEvent::Failed(SocketFailure::UnsupportedTextMessage),
        BrowserMessageData::Blob | BrowserMessageData::Unrecognized => {
            SocketEvent::Failed(SocketFailure::Transport)
        }
    }
}

/// Converts the browser `open` event, checking the F1 extension posture.
///
/// `negotiated_extensions` is the socket's `extensions` attribute at `open`
/// time. The landed acceptor declines every extension offer
/// (decline-never-negotiate), so extension-free operation is guaranteed by
/// the server's F1 posture — the browser cannot be configured to withhold
/// its offers. A non-empty negotiated string therefore proves the peer is
/// not honoring the liminal transport contract: the mirror reports a typed
/// transport failure and the driver mints the fate and closes the socket.
#[must_use]
pub const fn open_event(negotiated_extensions: &str) -> SocketEvent {
    if negotiated_extensions.is_empty() {
        SocketEvent::Opened
    } else {
        SocketEvent::Failed(SocketFailure::Transport)
    }
}

/// Converts the browser `close` event with close-code fidelity.
///
/// A clean close (`wasClean == true`) is the orderly close fact,
/// [`SocketEvent::Closed`]. An abnormal close (`wasClean == false`) is a
/// loss fact, not a clean peer close: it converts to a typed transport
/// failure so a lone abnormal close can never misrepresent itself as
/// orderly. In F3's browser shape abnormal loss fires `error` first, so this
/// abnormal conversion normally lands post-terminal and stays a typed no-op.
/// The numeric close code and reason string carry no additional decision
/// weight; the shim retains them as diagnostics only.
#[must_use]
pub const fn close_event(was_clean: bool) -> SocketEvent {
    if was_clean {
        SocketEvent::Closed
    } else {
        SocketEvent::Failed(SocketFailure::Transport)
    }
}

/// Converts the browser `error` event.
///
/// Browser error events are opaque by specification — they deliberately
/// carry no failure detail. The conversion therefore states only the typed
/// class, [`SocketFailure::Transport`], and invents nothing beyond it.
#[must_use]
pub const fn error_event() -> SocketEvent {
    SocketEvent::Failed(SocketFailure::Transport)
}

/// Closed browser socket actions the `web-sys` shim executes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BrowserSocketAction {
    /// Send one complete binary message (`WebSocket.send`).
    SendBinary(Vec<u8>),
    /// Close the socket (`WebSocket.close`).
    Close,
}

/// Typed refusal for a driver command with no runtime browser action.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BrowserCommandRefusal {
    /// A browser socket begins connecting when it is constructed, so the
    /// driver's single `Open` command is executed by construction itself; a
    /// runtime `Open` mapping would be a second connect and is refused.
    #[error("the browser socket opens at construction; Open is not a runtime action")]
    OpenIsConstruction,
}

/// Maps one driver command onto the browser action that executes it.
///
/// # Errors
///
/// Returns [`BrowserCommandRefusal::OpenIsConstruction`] for
/// [`SocketCommand::Open`]: construction of the browser socket is the one
/// execution of that command.
pub fn action_for_command(
    command: SocketCommand,
) -> Result<BrowserSocketAction, BrowserCommandRefusal> {
    match command {
        SocketCommand::Open => Err(BrowserCommandRefusal::OpenIsConstruction),
        SocketCommand::SendBinary(bytes) => Ok(BrowserSocketAction::SendBinary(bytes)),
        SocketCommand::Close => Ok(BrowserSocketAction::Close),
    }
}
