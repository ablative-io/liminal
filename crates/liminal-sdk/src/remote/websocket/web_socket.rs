//! Browser (wasm) WebSocket adapter for the transport-neutral driver
//! (LP-WS-TRANSPORT R3.2, folded F4/F5).
//!
//! Two layers:
//!
//! - [`mirror`] — the platform-neutral conversion layer (F5): pure functions
//!   mapping browser socket facts onto the driver's closed [`SocketEvent`]
//!   set and driver commands onto closed browser actions. No platform type
//!   appears, so it compiles natively and is pinned by the deterministic
//!   `ws_browser_mirror_trace` suite without a browser.
//! - [`adapter`] — the `web-sys` shim (wasm32 with the `browser` feature):
//!   owns the JavaScript socket, pins `binaryType = "arraybuffer"` at
//!   construction (F4), feeds each callback once into the shared driver, and
//!   executes emitted commands. Event-driven only: no timer, interval,
//!   executor, or `bufferedAmount` polling.
//!
//! All protocol, framing, correlation, and fate logic stays in
//! [`core`](super::core); both layers are mirrors, not implementors.
//!
//! [`SocketEvent`]: super::core::SocketEvent

pub mod mirror;

#[cfg(all(target_arch = "wasm32", feature = "browser"))]
pub mod adapter;

pub use mirror::{
    BrowserCommandRefusal, BrowserMessageData, BrowserSocketAction, action_for_command,
    close_event, error_event, message_event, open_event,
};

#[cfg(all(target_arch = "wasm32", feature = "browser"))]
pub use adapter::{
    AdapterFault, AdapterSignal, AdapterSignalSink, BrowserSocketError, WebSysWebSocketSocket,
};
