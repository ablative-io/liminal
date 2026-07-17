//! Blocking `tungstenite` adapter for the transport-neutral driver (R2.1).
//!
//! This module is mechanical: it opens the socket the driver commanded,
//! executes [`SocketCommand`]s, and reports observed socket facts as closed
//! [`SocketEvent`]s. It makes no protocol, correlation, reconnect, or replay
//! decisions — those belong to the driver and the client-unit binding.

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::time::Duration;

use std::net::TcpStream;

use tungstenite::client::IntoClientRequest;
use tungstenite::handshake::HandshakeError;
use tungstenite::http::Uri;
use tungstenite::protocol::{WebSocket, WebSocketConfig};

use crate::SdkError;

use super::core::{SocketEvent, SocketFailure};

/// Maximum time spent waiting on a single socket read or write, mirroring the
/// TCP transport's steady-state I/O bound.
pub(super) const IO_TIMEOUT: Duration = Duration::from_secs(5);

/// Default TCP port for the `ws://` scheme (RFC 6455 §3).
const WS_DEFAULT_PORT: u16 = 80;

/// Outcome of one bounded read attempt.
#[derive(Debug)]
pub(super) enum SocketRead {
    /// A closed socket event was observed.
    Event(SocketEvent),
    /// The armed read window elapsed with no complete message.
    TimedOut,
}

/// Builds the pinned client-side protocol configuration (F2).
///
/// `max_message_size` and `max_frame_size` are both the liminal frame bound,
/// so an oversize-declared server message fails at the pinned bound from its
/// declared length — never after allocating the library's 64 MiB default
/// buffer. `max_write_buffer_size` is the bound plus one write-chunk of
/// headroom: the transport hands the library one message at a time and
/// flushes it, so the internal buffer never holds more than one message.
fn pinned_client_config(message_bound: usize) -> WebSocketConfig {
    let base = WebSocketConfig::default();
    let write_headroom = base.write_buffer_size.saturating_mul(2);
    base.max_message_size(Some(message_bound))
        .max_frame_size(Some(message_bound))
        .max_write_buffer_size(message_bound.saturating_add(write_headroom))
}

/// One blocking client WebSocket over a plain `TcpStream`.
///
/// `ws://` only: the supported deployment contract keeps public `wss://` with
/// the named TLS-terminating proxy, so this transport refuses every other
/// scheme with a typed error rather than growing a TLS stack.
pub(super) struct WsSocket {
    socket: WebSocket<TcpStream>,
    /// Human-readable detail for the most recent failure event, used to
    /// enrich typed [`SdkError`] descriptions (diagnostics only).
    last_failure_detail: Option<String>,
}

impl WsSocket {
    /// Connects the TCP socket and completes the RFC 6455 client handshake
    /// with the pinned protocol configuration.
    ///
    /// The client offers no subprotocol and no extension; `tungstenite` does
    /// not offer `permessage-deflate` without its compression feature, so
    /// canonical liminal bytes are never exposed to a second compression
    /// limit.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when the URL is not a usable `ws://`
    /// address, the TCP connection cannot be established, socket options
    /// cannot be applied, or the WebSocket handshake is refused.
    pub(super) fn connect(url: &str, message_bound: usize) -> Result<Self, SdkError> {
        let authority = ws_authority(url)?;
        let stream = TcpStream::connect(&authority).map_err(|source| SdkError::Connection {
            description: format!("failed to connect websocket to {authority}: {source}"),
        })?;
        stream
            .set_nodelay(true)
            .map_err(|source| SdkError::Connection {
                description: format!("failed to disable Nagle for {authority}: {source}"),
            })?;
        stream
            .set_read_timeout(Some(IO_TIMEOUT))
            .map_err(|source| SdkError::Connection {
                description: format!("failed to set websocket read timeout: {source}"),
            })?;
        stream
            .set_write_timeout(Some(IO_TIMEOUT))
            .map_err(|source| SdkError::Connection {
                description: format!("failed to set websocket write timeout: {source}"),
            })?;

        let request = url
            .into_client_request()
            .map_err(|source| SdkError::Connection {
                description: format!("invalid websocket request for {url}: {source}"),
            })?;
        let config = pinned_client_config(message_bound);
        let mut attempt = tungstenite::client::client_with_config(request, stream, Some(config));
        loop {
            match attempt {
                Ok((socket, _response)) => {
                    return Ok(Self {
                        socket,
                        last_failure_detail: None,
                    });
                }
                Err(HandshakeError::Interrupted(mid)) => attempt = mid.handshake(),
                Err(HandshakeError::Failure(error)) => {
                    return Err(SdkError::Connection {
                        description: format!("websocket handshake with {url} failed: {error}"),
                    });
                }
            }
        }
    }

    /// Detail recorded with the most recent failure event, if any.
    pub(super) fn last_failure_detail(&self) -> Option<&str> {
        self.last_failure_detail.as_deref()
    }

    /// Clones the underlying TCP stream handle.
    ///
    /// The clone shares the one socket; it exists so an owner can shut the
    /// socket down (unblocking a reader blocked in [`read_event`]) without
    /// polling a stop flag. It must never be used for reads or writes.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when the OS refuses the handle clone.
    ///
    /// [`read_event`]: Self::read_event
    pub(super) fn try_clone_stream(&self) -> Result<TcpStream, SdkError> {
        self.socket
            .get_ref()
            .try_clone()
            .map_err(|source| SdkError::Connection {
                description: format!("failed to clone websocket stream handle: {source}"),
            })
    }

    /// Arms the read window for subsequent [`read_event`](Self::read_event)
    /// calls. `None` blocks indefinitely.
    pub(super) fn set_read_timeout(&self, timeout: Option<Duration>) -> Result<(), SdkError> {
        self.socket
            .get_ref()
            .set_read_timeout(timeout)
            .map_err(|source| SdkError::Connection {
                description: format!("failed to arm websocket read timeout: {source}"),
            })
    }

    /// Sends one complete binary message.
    ///
    /// # Errors
    ///
    /// Returns the closed [`SocketFailure`] class of the write failure; the
    /// human-readable detail is retained for diagnostics.
    pub(super) fn send_binary(&mut self, bytes: Vec<u8>) -> Result<(), SocketFailure> {
        match self.socket.send(tungstenite::Message::Binary(bytes.into())) {
            Ok(()) => Ok(()),
            Err(error) => Err(self.record_failure(&error)),
        }
    }

    /// Executes the driver's close command: best-effort close handshake.
    ///
    /// Failures are recorded as diagnostics only — the driver has already
    /// minted (or will mint) the typed terminal, and a dead socket cannot be
    /// closed twice.
    pub(super) fn execute_close(&mut self) {
        match self.socket.close(None) {
            Ok(())
            | Err(tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed) => {}
            Err(error) => {
                self.last_failure_detail = Some(format!("websocket close failed: {error}"));
            }
        }
        if let Err(error) = self.socket.flush() {
            match error {
                tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed => {}
                other => {
                    self.last_failure_detail =
                        Some(format!("websocket close flush failed: {other}"));
                }
            }
        }
    }

    /// Blocks for the next socket fact within the armed read window.
    ///
    /// RFC 6455 Ping/Pong are transport control: the library answers them and
    /// this adapter continues reading — they are never surfaced as events and
    /// never converted to liminal frames.
    pub(super) fn read_event(&mut self) -> SocketRead {
        loop {
            match self.socket.read() {
                Ok(tungstenite::Message::Binary(bytes)) => {
                    return SocketRead::Event(SocketEvent::Binary(bytes.to_vec()));
                }
                Ok(tungstenite::Message::Text(text)) => {
                    self.last_failure_detail = Some(format!(
                        "peer sent a text message ({} bytes) on the binary-only liminal route",
                        text.len()
                    ));
                    return SocketRead::Event(SocketEvent::Failed(
                        SocketFailure::UnsupportedTextMessage,
                    ));
                }
                Ok(tungstenite::Message::Ping(_) | tungstenite::Message::Pong(_)) => {}
                Ok(tungstenite::Message::Close(_))
                | Err(tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed) => {
                    return SocketRead::Event(SocketEvent::Closed);
                }
                Ok(tungstenite::Message::Frame(_)) => {
                    self.last_failure_detail =
                        Some("websocket library surfaced a raw frame".to_string());
                    return SocketRead::Event(SocketEvent::Failed(SocketFailure::Transport));
                }
                Err(tungstenite::Error::Io(error))
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    return SocketRead::TimedOut;
                }
                Err(error) => {
                    return SocketRead::Event(SocketEvent::Failed(self.record_failure(&error)));
                }
            }
        }
    }

    /// Maps a library error to its closed failure class, retaining detail.
    fn record_failure(&mut self, error: &tungstenite::Error) -> SocketFailure {
        let failure = match error {
            tungstenite::Error::Capacity(_) => SocketFailure::MessageBeyondBound,
            _ => SocketFailure::Transport,
        };
        self.last_failure_detail = Some(format!("websocket transport failure: {error}"));
        failure
    }
}

impl core::fmt::Debug for WsSocket {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.debug_struct("WsSocket").finish_non_exhaustive()
    }
}

/// Extracts `host:port` from a validated `ws://` URL.
///
/// # Errors
///
/// Returns [`SdkError::Connection`] for a malformed URL, a missing authority,
/// or any scheme other than `ws` — including `wss`, which is deliberately
/// refused because TLS termination is owned by the named proxy.
fn ws_authority(url: &str) -> Result<String, SdkError> {
    let uri: Uri = url.parse().map_err(|source| SdkError::Connection {
        description: format!("invalid websocket url {url}: {source}"),
    })?;
    match uri.scheme_str() {
        Some("ws") => {}
        Some("wss") => {
            return Err(SdkError::Connection {
                description: format!(
                    "wss is not terminated by liminal; the named TLS-terminating proxy owns \
                     wss and forwards ws to the server (url: {url})"
                ),
            });
        }
        Some(other) => {
            return Err(SdkError::Connection {
                description: format!("websocket transport requires a ws:// url, got {other}://"),
            });
        }
        None => {
            return Err(SdkError::Connection {
                description: format!("websocket transport requires a ws:// url, got {url}"),
            });
        }
    }
    let Some(host) = uri.host() else {
        return Err(SdkError::Connection {
            description: format!("websocket url {url} has no host"),
        });
    };
    let port = uri.port_u16().unwrap_or(WS_DEFAULT_PORT);
    Ok(format!("{host}:{port}"))
}
