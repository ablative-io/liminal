//! What a refused connection is TOLD, and what the record says about it.
//!
//! A connection can be refused after its transport is already established: the
//! WebSocket route has written and flushed its HTTP 101 before admission is
//! attempted at all, and the TCP route has an accepted socket in hand. Until
//! P0 #56 both routes answered that refusal by dropping the socket, so a
//! browser saw close code 1006 with zero frames in either direction — a
//! transport fault, indistinguishable from a network partition or a crashed
//! server, for what was in fact a deliberate, typed, server-side decision.
//!
//! This module is the one place that decides three things together, so they
//! cannot drift apart: the class name that appears in the metric label, the
//! reason string the client is told, and the WebSocket close code. All three
//! derive from one enum with a bounded number of variants, which is also what
//! makes the metric label safe to record — a peer address or an error message
//! would be unbounded scrape cardinality.

use std::io::Write as _;
use std::net::TcpStream;
use std::time::Duration;

use liminal::protocol::{Frame, encode, encoded_len};
use tungstenite::Message;
use tungstenite::protocol::WebSocket;
use tungstenite::protocol::frame::coding::CloseCode;

use crate::ServerError;
use crate::server::connection::incarnation::{
    AMBIGUOUS_DURABLE_WRITE_PHASE, AUTHORITY_SURRENDERED_PHASE,
};

/// How long a refusal write may block before the socket is abandoned.
///
/// A refusal is best-effort by nature: the peer may already be gone, and a
/// server that is refusing connections must not acquire a new way to block on
/// one. The frame is tens of bytes and fits in any healthy send buffer, so this
/// only bites when the peer has stopped reading.
const REFUSAL_WRITE_TIMEOUT: Duration = Duration::from_millis(250);

/// Undifferentiated server error, the protocol's `0xFFFF`.
///
/// Admission refusals share it deliberately. The reason-code space belongs to
/// `liminal-protocol` and this lane is not the place to mint new points in it;
/// the discrimination a client needs is carried by the reason STRING and, on
/// the WebSocket route, by the close code, both of which are per-class.
const SERVER_ERROR_CODE: u16 = 0xFFFF;

/// Bytes available to a Close frame's reason string.
///
/// RFC 6455 caps a control frame's payload at 125 bytes; the close code takes
/// the first two. A longer reason does not truncate — the send fails.
pub(in crate::server) const MAX_CLOSE_REASON_BYTES: usize = 123;

/// Why one connection was refused, in bounded classes.
///
/// Ordering note: classification reads the `ServerError` it is given and never
/// consults the authority, so it is a pure function of the refusal that already
/// happened. Adding a variant here is the only way to add a metric label value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AdmissionRefusal {
    /// The incarnation authority is holding after an ambiguous durable write.
    AdmissionHeld,
    /// The incarnation authority surrendered the stream to another process.
    AuthoritySurrendered,
    /// The configured `max_connections` bound is reached.
    ConnectionsSaturated,
    /// The participant service has latched a fatal.
    ParticipantServiceFatal,
    /// Connection-ordinal or server-incarnation space is exhausted.
    IncarnationExhausted,
    /// Durable allocation failed for some other typed reason.
    AllocationFailed,
    /// Everything after allocation: process spawn, registration, pid collision.
    SpawnFailed,
}

impl AdmissionRefusal {
    /// Classifies a refusal from the error the admission path produced.
    pub(in crate::server) fn classify(error: &ServerError) -> Self {
        match error {
            ServerError::ConnectionLimitReached { .. } => Self::ConnectionsSaturated,
            ServerError::ParticipantServiceFatal { .. } => Self::ParticipantServiceFatal,
            ServerError::ConnectionIncarnationExhausted { .. }
            | ServerError::ServerIncarnationExhausted => Self::IncarnationExhausted,
            // The phase strings are the CONSTANTS the authority constructs
            // with, imported rather than retyped, so the classifier and the
            // construction site cannot drift into disagreeing.
            ServerError::ParticipantIncarnation { phase, .. }
                if *phase == AMBIGUOUS_DURABLE_WRITE_PHASE =>
            {
                Self::AdmissionHeld
            }
            ServerError::ParticipantIncarnation { phase, .. }
                if *phase == AUTHORITY_SURRENDERED_PHASE =>
            {
                Self::AuthoritySurrendered
            }
            ServerError::ParticipantIncarnation { .. } => Self::AllocationFailed,
            _ => Self::SpawnFailed,
        }
    }

    /// Every class's metric label, indexed by [`Self::slot`].
    ///
    /// Fixed strings only. Never a peer address, never an error message: each
    /// value becomes a permanent time series on an operator's scrape target,
    /// and cardinality here is bounded by this array's length by construction.
    pub(crate) const LABELS: [&'static str; 7] = [
        "admission_held",
        "authority_surrendered",
        "connections_saturated",
        "participant_service_fatal",
        "incarnation_exhausted",
        "allocation_failed",
        "spawn_failed",
    ];

    /// This class's index into [`Self::LABELS`] and the pre-registered handles.
    pub(crate) const fn slot(self) -> usize {
        match self {
            Self::AdmissionHeld => 0,
            Self::AuthoritySurrendered => 1,
            Self::ConnectionsSaturated => 2,
            Self::ParticipantServiceFatal => 3,
            Self::IncarnationExhausted => 4,
            Self::AllocationFailed => 5,
            Self::SpawnFailed => 6,
        }
    }

    /// The bounded metric label value for this class.
    pub(crate) const fn label(self) -> &'static str {
        Self::LABELS[self.slot()]
    }

    /// The reason string the refused client is told.
    ///
    /// Deliberately names the class in words a human reading a browser console
    /// can act on, because that console is where this ends up.
    pub(in crate::server) const fn reason(self) -> &'static str {
        match self {
            Self::AdmissionHeld => {
                "admission held: the server's durable connection-incarnation write had an \
                 ambiguous result and admission is refused until it re-reads its store"
            }
            Self::AuthoritySurrendered => {
                "admission surrendered: another server process owns this durable \
                 connection-incarnation stream"
            }
            Self::ConnectionsSaturated => {
                "admission refused: the server is at its configured max_connections bound"
            }
            Self::ParticipantServiceFatal => {
                "admission refused: the server's participant service has latched a fatal"
            }
            Self::IncarnationExhausted => {
                "admission refused: the server's connection-incarnation space is exhausted"
            }
            Self::AllocationFailed => {
                "admission refused: the server could not durably allocate a connection incarnation"
            }
            Self::SpawnFailed => {
                "admission refused: the server could not start a connection process"
            }
        }
    }

    /// The short reason carried by the WebSocket Close frame.
    ///
    /// Separate from [`Self::reason`] because a Close frame is a CONTROL frame:
    /// RFC 6455 caps its whole payload at 125 bytes, two of which are the close
    /// code, leaving 123 for this string. Exceeding it is not a truncation — the
    /// send fails outright with `ControlFrameTooBig`, and a failed Close is a
    /// bare drop wearing this lane's clothes. [`MAX_CLOSE_REASON_BYTES`] and the
    /// pin over it exist so a future edit cannot reintroduce that silently.
    pub(in crate::server) const fn close_reason(self) -> &'static str {
        match self {
            Self::AdmissionHeld => "admission_held: durable incarnation write was ambiguous",
            Self::AuthoritySurrendered => {
                "authority_surrendered: another process owns the incarnation stream"
            }
            Self::ConnectionsSaturated => {
                "connections_saturated: the max_connections bound is reached"
            }
            Self::ParticipantServiceFatal => {
                "participant_service_fatal: the participant service has latched"
            }
            Self::IncarnationExhausted => {
                "incarnation_exhausted: connection-incarnation space is spent"
            }
            Self::AllocationFailed => "allocation_failed: durable incarnation allocation failed",
            Self::SpawnFailed => "spawn_failed: the connection process did not start",
        }
    }

    /// The WebSocket close code for this class.
    ///
    /// 4000-4999 is the application-private range, so these cannot collide with
    /// a protocol-defined code. They exist so a browser client — which cannot
    /// read a liminal frame if it closed before parsing one — still learns the
    /// class from `CloseEvent.code` alone.
    pub(in crate::server) const fn close_code(self) -> u16 {
        match self {
            Self::AdmissionHeld => 4001,
            Self::AuthoritySurrendered => 4002,
            Self::ConnectionsSaturated => 4003,
            Self::ParticipantServiceFatal => 4004,
            Self::IncarnationExhausted => 4005,
            Self::AllocationFailed => 4006,
            Self::SpawnFailed => 4007,
        }
    }

    /// The liminal `ConnectError` frame carrying this refusal.
    ///
    /// Sent unsolicited: the client has not yet had the chance to send
    /// `Connect`, because the connection process that would have read it was
    /// never spawned. The in-tree TS client surfaces `CONNECT_ERROR` as
    /// `CONNECT_REJECTED` regardless of phase
    /// (`sdks/liminal-ts/src/feed-websocket.ts:230-232`), so the refusal
    /// arrives as a typed rejection rather than a transport fault.
    pub(in crate::server) fn connect_error_frame(self) -> Frame {
        Frame::ConnectError {
            flags: 0,
            reason_code: SERVER_ERROR_CODE,
            message: Some(self.reason().to_owned()),
        }
    }

    /// The canonical liminal bytes of this refusal's `ConnectError`.
    pub(in crate::server) fn connect_error_bytes(self) -> Option<Vec<u8>> {
        let frame = self.connect_error_frame();
        let needed = encoded_len(&frame).ok()?;
        let mut bytes = vec![0_u8; needed];
        let written = encode(&frame, &mut bytes).ok()?;
        bytes.truncate(written);
        Some(bytes)
    }
}

/// Trims a close reason to the control-frame budget on a char boundary.
///
/// Belt to the pin's braces. `every_close_reason_fits_in_a_control_frame`
/// asserts no reason needs trimming; this guarantees that if one ever does, the
/// failure mode is a SHORTER reason rather than a `ControlFrameTooBig` that
/// aborts the Close and hands the client the bare drop this lane exists to
/// remove. A budget overrun must degrade the message, never the mechanism.
fn clamp_close_reason(reason: &str) -> &str {
    if reason.len() <= MAX_CLOSE_REASON_BYTES {
        return reason;
    }
    let boundary = reason
        .char_indices()
        .map(|(index, _)| index)
        .take_while(|index| *index <= MAX_CLOSE_REASON_BYTES)
        .last()
        .unwrap_or(0);
    reason.get(..boundary).unwrap_or("")
}

/// Tells a refused WebSocket client why, then closes.
///
/// Two things go out, and both matter. The `ConnectError` is what a liminal
/// client parses into a typed rejection; the Close frame's code and reason are
/// what a client that never got as far as parsing a liminal frame still sees on
/// `CloseEvent`. Sending only the first would leave a bare TCP FIN behind it;
/// sending only the second would give a liminal client nothing to type.
///
/// The FLUSH is the whole point of this function existing rather than an
/// enqueue at the call site. `WebSocket::send` writes and flushes, and the
/// explicit `flush` after `close` pushes the Close frame out too — an enqueued
/// frame that the socket drop discards is byte-for-byte indistinguishable on
/// the wire from the bare drop this replaces.
///
/// Best-effort by contract: every step's error is returned for the caller to
/// log, never propagated as a new failure. The connection was already refused.
pub(in crate::server) fn send_websocket_refusal(
    socket: &mut WebSocket<TcpStream>,
    refusal: AdmissionRefusal,
) -> Result<(), tungstenite::Error> {
    // The upgraded socket was put in non-blocking mode for the connection
    // process that is now never going to exist. A refusal write has to be able
    // to complete here and now, so it goes back to blocking under a timeout.
    let stream = socket.get_ref();
    stream.set_nonblocking(false).map_err(tungstenite::Error::Io)?;
    stream
        .set_write_timeout(Some(REFUSAL_WRITE_TIMEOUT))
        .map_err(tungstenite::Error::Io)?;

    if let Some(bytes) = refusal.connect_error_bytes() {
        socket.send(Message::Binary(bytes.into()))?;
    }
    socket.close(Some(tungstenite::protocol::CloseFrame {
        code: CloseCode::Library(refusal.close_code()),
        reason: clamp_close_reason(refusal.close_reason()).into(),
    }))?;
    socket.flush()
}

/// Tells a refused raw-TCP client why, then shuts the socket down.
///
/// The TCP route carries canonical liminal frames with no transport framing of
/// its own, so the refusal is the encoded `ConnectError` bytes and nothing
/// else — there is no Close frame to pair it with, and the shutdown is the
/// close. `flush` is explicit for the same reason as on the WebSocket route.
pub(in crate::server) fn send_tcp_refusal(
    stream: &mut TcpStream,
    refusal: AdmissionRefusal,
) -> std::io::Result<()> {
    let Some(bytes) = refusal.connect_error_bytes() else {
        return Ok(());
    };
    stream.set_nonblocking(false)?;
    stream.set_write_timeout(Some(REFUSAL_WRITE_TIMEOUT))?;
    stream.write_all(&bytes)?;
    stream.flush()?;
    stream.shutdown(std::net::Shutdown::Both)
}
