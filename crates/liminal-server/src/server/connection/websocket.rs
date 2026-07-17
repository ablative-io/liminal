//! Sibling WebSocket transport acceptor (LP-WS-TRANSPORT R1).
//!
//! The WebSocket route carries the CANONICAL liminal wire protocol: exactly one
//! reassembled binary message contains exactly one canonical liminal frame —
//! the real ten-byte header plus the declared body, with no stripping,
//! batching, prefixing, base64, JSON, or trailing bytes. It is a sibling
//! listener/process owner that reuses the semantic application seam
//! ([`super::apply::apply_frame`]), the shared [`super::supervisor::ConnectionRuntime`]
//! (one §5 `max_connections` admission bound, one incarnation authority, one
//! registry for controls/pushes/reap/drain), the delivery pump, and the
//! pending-reply table. The TCP listener, supervisor spawn path, process read
//! loop, and outbound byte path are untouched reference implementations.
//!
//! Deployment contract (tear ruling Q1): raw `ws://` behind a named
//! TLS-terminating proxy that owns public `wss://` and certificates — liminal
//! grows no TLS stack. Origin validation nonetheless lives HERE (F6): the
//! configured allow-list is checked on every Origin-bearing upgrade and there
//! is no default list, so absent/empty configuration fails closed for browsers
//! while native no-`Origin` clients may upgrade.
//!
//! Deployment contract, pre-upgrade window (domain-owner ruling, 2026-07-18,
//! same shape as the Q1 TLS ruling): the named fronting proxy ALSO owns
//! pre-upgrade read-timeout, handshake-concurrency, and rate bounds. This
//! listener's pre-upgrade window is UNCOUNTED and UNDEADLINED: a socket that
//! has been accepted but has not completed its upgrade is outside the shared
//! §5 `max_connections` bound and its handshake read has no deadline — only
//! its SIZE is bounded, by [`MAX_UPGRADE_REQUEST_BYTES`]. An unproxied
//! deployment is therefore out of contract for untrusted networks. The
//! ledgered follow-up (post-demo hardening, option (a)) is a named handshake
//! read-deadline config value plus an in-flight handshake cap derived from the
//! configured §5 `max_connections` value — never an invented constant.
//!
//! Extension posture (F1): extension offers — including the `permessage-deflate`
//! offer every browser engine sends unremovably — are DECLINED, never
//! negotiated. The upgrade response is built by this module and carries no
//! `Sec-WebSocket-Extensions` header, so canonical liminal bytes are never
//! exposed to a second compression/bomb limit. Subprotocols are separate: the
//! liminal browser client offers none, and any offered subprotocol is refused.

use std::io::{Read, Write};
use std::net::TcpStream;

use tungstenite::handshake::machine::TryParse;
use tungstenite::handshake::server::{Request, create_response, write_response};
use tungstenite::http::{HeaderValue, Response as HttpResponse, StatusCode};
use tungstenite::protocol::WebSocketConfig as TungsteniteConfig;

use liminal::protocol::{Frame, ProtocolError, decode};
use liminal_protocol::wire::FRAME_MAX;

use crate::ServerError;

#[path = "websocket/listener.rs"]
mod listener;
#[path = "websocket/outbound.rs"]
mod outbound;
#[path = "websocket/process.rs"]
mod process;
#[path = "websocket/supervisor.rs"]
mod supervisor;

#[cfg(test)]
#[path = "websocket/handshake_tests.rs"]
mod handshake_tests;

pub use listener::WebSocketListener;

/// Upper bound on one HTTP upgrade request head (request line + headers), in
/// bytes. R1.1 requires malformed or oversized headers to "remain bounded and
/// rejected without entering supervision"; this constant IS that bound. The
/// value matches the connection read-buffer granularity (8 KiB) — a legitimate
/// browser upgrade request is a few hundred bytes, so this is an order of
/// magnitude of headroom, not a tuning knob.
pub(super) const MAX_UPGRADE_REQUEST_BYTES: usize = 8192;

/// The F2 reassembly bound: the active liminal frame bound, derived from the
/// protocol's named product limit [`FRAME_MAX`] (ten-byte header plus the
/// generic `u32` payload ceiling). Both `max_message_size` and `max_frame_size`
/// are pinned to this exact value, so an oversize-declared WebSocket message
/// fails at the pinned bound from its declared length — never after allocation
/// of the library's 64 MiB default buffer, and never at a WebSocket-invented
/// limit tighter than what the same frame would be allowed over TCP.
///
/// Participant frames carry their own tighter negotiated/pre-capability limit,
/// enforced by the SAME shared preflight gate the TCP path runs
/// ([`crate::server::participant::preflight_generic_bytes`]) — transport parity,
/// not a second policy.
///
/// # Errors
/// Returns a typed startup error when the build target's `usize` cannot
/// represent the bound (a 32-bit target). Refusing to start is the only honest
/// option: silently clamping would change which canonical frames the transport
/// admits.
pub(super) fn liminal_ws_message_bound() -> Result<usize, ServerError> {
    usize::try_from(FRAME_MAX).map_err(|_| ServerError::ListenerAccept {
        message: format!(
            "websocket acceptor cannot start: this target's usize cannot represent the \
             liminal frame bound of {FRAME_MAX} bytes"
        ),
    })
}

/// Builds the pinned tungstenite protocol configuration (F2).
///
/// `max_message_size` and `max_frame_size` are both the liminal frame bound.
/// `max_write_buffer_size` is the same bound plus one write-chunk of headroom:
/// the outbound queue hands tungstenite ONE message at a time and flushes it
/// before the next, so the library's internal buffer never needs to hold more
/// than one in-flight message — the default unbounded value would be a silent
/// unbounded buffer, which is not a legal state here.
pub(super) fn pinned_protocol_config(message_bound: usize) -> TungsteniteConfig {
    let base = TungsteniteConfig::default();
    let write_headroom = base.write_buffer_size.saturating_mul(2);
    base.max_message_size(Some(message_bound))
        .max_frame_size(Some(message_bound))
        .max_write_buffer_size(message_bound.saturating_add(write_headroom))
}

/// Validated per-acceptor settings shared by the handshake workers and every
/// spawned WebSocket connection process.
#[derive(Debug)]
pub(super) struct AcceptorSettings {
    /// The single exact upgrade path.
    pub(super) path: String,
    /// The explicit origin allow-list (F6). Empty fails closed for every
    /// Origin-bearing upgrade.
    pub(super) allowed_origins: Vec<String>,
    /// Q-A keepalive interval. `None` means pings are disabled.
    pub(super) ping_interval: Option<std::time::Duration>,
    /// The F2 message bound this acceptor pinned at bind time.
    pub(super) message_bound: usize,
}

/// One typed reason an HTTP request on the WebSocket port was refused.
///
/// Every refusal is LOUD: the requester receives the small fixed non-success
/// response carried by [`Self::status`] and the connection closes. Nothing on
/// this enum enters connection supervision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum UpgradeRefusal {
    /// The request head exceeded [`MAX_UPGRADE_REQUEST_BYTES`].
    OversizedRequestHead {
        /// Bytes received before the bound tripped.
        received: usize,
    },
    /// The request head could not be parsed as HTTP/1.1.
    MalformedRequest {
        /// Parser diagnostic.
        detail: String,
    },
    /// Bytes followed the request head before the upgrade completed.
    JunkAfterRequest,
    /// A parseable HTTP request that is not a well-formed RFC 6455 upgrade
    /// (wrong method, wrong version, missing/invalid upgrade headers or key) —
    /// including every ordinary HTTP request.
    NotAWebSocketUpgrade {
        /// Validator diagnostic.
        detail: String,
    },
    /// The request path is not the configured upgrade path (or carried a query
    /// or fragment; the configured path is a single exact path).
    WrongPath {
        /// The path the client requested.
        requested: String,
    },
    /// More than one `Origin` header was present.
    DuplicateOriginHeader,
    /// The `Origin` header value was not valid visible ASCII.
    MalformedOriginHeader,
    /// F6: the request bore an `Origin` that is not on the configured
    /// allow-list — including EVERY Origin-bearing request when the list is
    /// absent or empty (fail closed, no default list).
    OriginNotAllowed {
        /// The refused origin value.
        origin: String,
    },
    /// The client offered a subprotocol; the liminal route negotiates none.
    SubprotocolOffered,
}

impl UpgradeRefusal {
    /// The fixed non-success status this refusal answers with.
    pub(super) const fn status(&self) -> StatusCode {
        match self {
            Self::OversizedRequestHead { .. } => StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
            Self::MalformedRequest { .. }
            | Self::JunkAfterRequest
            | Self::NotAWebSocketUpgrade { .. }
            | Self::DuplicateOriginHeader
            | Self::MalformedOriginHeader
            | Self::SubprotocolOffered => StatusCode::BAD_REQUEST,
            Self::WrongPath { .. } => StatusCode::NOT_FOUND,
            Self::OriginNotAllowed { .. } => StatusCode::FORBIDDEN,
        }
    }
}

impl std::fmt::Display for UpgradeRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OversizedRequestHead { received } => write!(
                formatter,
                "upgrade request head exceeded the {MAX_UPGRADE_REQUEST_BYTES}-byte bound \
                 ({received} bytes received)"
            ),
            Self::MalformedRequest { detail } => {
                write!(formatter, "malformed HTTP request: {detail}")
            }
            Self::JunkAfterRequest => {
                write!(
                    formatter,
                    "bytes followed the request head before the upgrade"
                )
            }
            Self::NotAWebSocketUpgrade { detail } => {
                write!(formatter, "not a well-formed WebSocket upgrade: {detail}")
            }
            Self::WrongPath { requested } => {
                write!(
                    formatter,
                    "request path '{requested}' is not the upgrade path"
                )
            }
            Self::DuplicateOriginHeader => {
                write!(formatter, "multiple Origin headers present")
            }
            Self::MalformedOriginHeader => {
                write!(formatter, "Origin header is not valid visible ASCII")
            }
            Self::OriginNotAllowed { origin } => {
                write!(
                    formatter,
                    "origin '{origin}' is not on the configured allow-list"
                )
            }
            Self::SubprotocolOffered => {
                write!(formatter, "subprotocols are not negotiated on this route")
            }
        }
    }
}

/// The outcome of driving one accepted socket through the upgrade handshake.
#[derive(Debug)]
pub(super) enum HandshakeOutcome {
    /// The 101 response was written; the stream is ready to be wrapped.
    Upgraded,
    /// A typed refusal was written and the connection was closed.
    Refused(UpgradeRefusal),
    /// The socket failed (peer loss, interrupt via shutdown) before an outcome.
    SocketError(std::io::Error),
}

/// Reads one bounded HTTP request head from `stream` and either completes the
/// RFC 6455 upgrade (writing the 101 response, F1: with NO
/// `Sec-WebSocket-Extensions` and NO `Sec-WebSocket-Protocol` header) or writes
/// the refusal's small fixed non-success response and closes.
///
/// The parse and upgrade validation are tungstenite's
/// ([`Request::try_parse`] / [`create_response`]) — the same HTTP/1.1 Upgrade
/// parser its own accept path uses — driven here so that EVERY refusal path,
/// including tungstenite-internal protocol failures its `accept` would drop the
/// stream on, still answers with the mandated fixed response.
pub(super) fn perform_upgrade(
    stream: &mut TcpStream,
    settings: &AcceptorSettings,
) -> HandshakeOutcome {
    let request = match read_upgrade_request(stream) {
        Ok(Ok(request)) => request,
        Ok(Err(refusal)) => return refuse(stream, refusal),
        Err(error) => return HandshakeOutcome::SocketError(error),
    };
    match validate_upgrade_request(&request, settings) {
        Ok(()) => {}
        Err(refusal) => return refuse(stream, refusal),
    }
    // tungstenite's response builder is the RFC 6455 authority for the accept
    // key and upgrade headers; it adds exactly Connection/Upgrade/
    // Sec-WebSocket-Accept — no extensions header exists to strip (F1) and no
    // subprotocol is echoed.
    let response = match create_response(&request) {
        Ok(response) => response,
        Err(error) => {
            return refuse(
                stream,
                UpgradeRefusal::NotAWebSocketUpgrade {
                    detail: error.to_string(),
                },
            );
        }
    };
    // Serialize the response into one buffer and write it in a single call so
    // the 101 reaches the peer as one segment rather than a header-per-write
    // trickle.
    let mut serialized: Vec<u8> = Vec::new();
    if let Err(error) = write_response(&mut serialized, &response) {
        return HandshakeOutcome::SocketError(std::io::Error::other(error.to_string()));
    }
    if let Err(error) = stream.write_all(&serialized).and_then(|()| stream.flush()) {
        return HandshakeOutcome::SocketError(error);
    }
    HandshakeOutcome::Upgraded
}

/// Reads the request head with the [`MAX_UPGRADE_REQUEST_BYTES`] bound.
///
/// Outer `Err` is a socket failure; inner `Err` is a typed refusal to answer.
fn read_upgrade_request(
    stream: &mut TcpStream,
) -> Result<Result<Request, UpgradeRefusal>, std::io::Error> {
    let mut head: Vec<u8> = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        match Request::try_parse(&head) {
            Ok(Some((parsed_length, request))) => {
                // A WebSocket client sends nothing after its request head until
                // the 101 arrives; early bytes are refused exactly as
                // tungstenite's own `JunkAfterRequest` discipline refuses them.
                if parsed_length != head.len() {
                    return Ok(Err(UpgradeRefusal::JunkAfterRequest));
                }
                return Ok(Ok(request));
            }
            Ok(None) => {}
            Err(error) => {
                return Ok(Err(UpgradeRefusal::MalformedRequest {
                    detail: error.to_string(),
                }));
            }
        }
        if head.len() >= MAX_UPGRADE_REQUEST_BYTES {
            return Ok(Err(UpgradeRefusal::OversizedRequestHead {
                received: head.len(),
            }));
        }
        let read = match stream.read(&mut chunk) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "peer closed before completing the upgrade request head",
                ));
            }
            Ok(read) => read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        head.extend_from_slice(chunk.get(..read).unwrap_or(&[]));
        if head.len() > MAX_UPGRADE_REQUEST_BYTES {
            return Ok(Err(UpgradeRefusal::OversizedRequestHead {
                received: head.len(),
            }));
        }
    }
}

/// Applies the acceptor's route policy: exact path, F6 origin cases, and the
/// no-subprotocol rule. Pure so the F6 case enumeration is unit-testable
/// without sockets.
pub(super) fn validate_upgrade_request(
    request: &Request,
    settings: &AcceptorSettings,
) -> Result<(), UpgradeRefusal> {
    if request.uri().path() != settings.path || request.uri().query().is_some() {
        return Err(UpgradeRefusal::WrongPath {
            requested: request.uri().to_string(),
        });
    }

    // F6 origin cases: (1) no Origin header — a native client — passes
    // regardless of configuration; (2) a listed Origin passes; (3) an unlisted
    // Origin refuses typed; (4) an absent/empty allow-list refuses EVERY
    // Origin-bearing upgrade (fail closed, no default list).
    let mut origins = request.headers().get_all("Origin").iter();
    let origin = origins.next();
    if origins.next().is_some() {
        return Err(UpgradeRefusal::DuplicateOriginHeader);
    }
    if let Some(origin) = origin {
        let Ok(origin) = origin.to_str() else {
            return Err(UpgradeRefusal::MalformedOriginHeader);
        };
        if !settings
            .allowed_origins
            .iter()
            .any(|allowed| allowed == origin)
        {
            return Err(UpgradeRefusal::OriginNotAllowed {
                origin: origin.to_owned(),
            });
        }
    }

    if request.headers().get("Sec-WebSocket-Protocol").is_some() {
        return Err(UpgradeRefusal::SubprotocolOffered);
    }

    Ok(())
}

/// Writes `refusal`'s small fixed non-success response and closes the socket.
///
/// The response is deliberately tiny and constant-shaped: status line,
/// `Connection: close`, `Content-Length: 0`, blank line. Write failures are
/// absorbed — the refusal stands whether or not the peer read it — but the
/// refusal itself is always logged with its typed reason.
fn refuse(stream: &mut TcpStream, refusal: UpgradeRefusal) -> HandshakeOutcome {
    let response = HttpResponse::builder()
        .status(refusal.status())
        .header("Connection", HeaderValue::from_static("close"))
        .header("Content-Length", HeaderValue::from_static("0"))
        .body(());
    match response {
        Ok(response) => {
            let mut serialized: Vec<u8> = Vec::new();
            if let Err(error) = write_response(&mut serialized, &response) {
                tracing::debug!(%error, "websocket refusal response could not be serialized");
            } else if let Err(error) = stream.write_all(&serialized).and_then(|()| stream.flush()) {
                tracing::debug!(%error, "websocket refusal response could not be written");
            }
        }
        Err(error) => {
            tracing::debug!(%error, "websocket refusal response could not be constructed");
        }
    }
    if let Err(error) = stream.shutdown(std::net::Shutdown::Both) {
        tracing::debug!(%error, "websocket refusal socket shutdown failed");
    }
    HandshakeOutcome::Refused(refusal)
}

/// One typed reason a complete inbound binary message violated the
/// one-message-one-canonical-frame transport contract. Every variant closes the
/// connection under the same terminal supervision discipline as malformed TCP
/// input; none is ever partially forwarded to `apply_frame`.
#[derive(Debug)]
pub(super) enum WsInboundViolation {
    /// A text message arrived; the canonical route is binary-only.
    TextMessage,
    /// The message did not decode as one canonical liminal frame (empty
    /// messages and truncated bodies land here: the message is complete, so an
    /// incomplete frame is terminal rather than "wait for more bytes").
    MalformedFrame(ProtocolError),
    /// The message decoded one canonical frame but carried trailing bytes
    /// (e.g. two concatenated frames).
    TrailingBytes {
        /// Bytes the canonical frame consumed.
        consumed: usize,
        /// Total message length.
        length: usize,
    },
}

impl std::fmt::Display for WsInboundViolation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TextMessage => write!(
                formatter,
                "text message on the canonical binary-only websocket route"
            ),
            Self::MalformedFrame(error) => write!(
                formatter,
                "binary message is not one complete canonical liminal frame: {error}"
            ),
            Self::TrailingBytes { consumed, length } => write!(
                formatter,
                "binary message carried trailing bytes: frame consumed {consumed} of {length}"
            ),
        }
    }
}

/// Decodes one complete binary message as EXACTLY one canonical liminal frame.
///
/// `decode` must consume `bytes.len()`: a complete message that decodes short
/// is trailing bytes; one that needs more bytes is a truncated body. Both are
/// typed violations, mirroring R1.2's non-negotiable contract.
///
/// # Errors
/// Returns the typed [`WsInboundViolation`] for every contract breach.
pub(super) fn decode_ws_binary(bytes: &[u8]) -> Result<Frame, WsInboundViolation> {
    let (frame, consumed) = decode(bytes).map_err(WsInboundViolation::MalformedFrame)?;
    if consumed != bytes.len() {
        return Err(WsInboundViolation::TrailingBytes {
            consumed,
            length: bytes.len(),
        });
    }
    Ok(frame)
}
