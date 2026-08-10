//! Stream ownership and frame I/O, shared by every byte-level SDK transport.
//!
//! [`Connection`] wraps one blocking byte stream, buffers partial reads until a
//! whole frame decodes (mirroring the server's `process_buffer` loop), and tracks
//! which conversations have been opened so a message never re-opens a conversation
//! or leaves an undrained error frame on the shared connection.
//!
//! # Why this is generic, and over exactly what
//!
//! This layer used to live inside `tcp/` and be nailed to [`TcpStream`]. The
//! in-process (loopback) transport carries the identical framed wire image over
//! an in-memory duplex rather than a socket
//! (`docs/design/IN-PROCESS-TRANSPORT.md` §1), so it needs this exact
//! handshake, this exact partial-frame buffering, this exact `Deliver` demux,
//! and this exact conversation-drain logic. A parallel copy of them is the
//! failure mode that design names and refuses (§7, §9 ruling 2): a second
//! `fill_buffer` is a second place for a desync to appear and only one of them
//! would ever get the fix.
//!
//! So [`Connection`] became generic over [`FrameStream`] — a trait covering
//! EXACTLY the four things this file asked a `TcpStream` for and nothing more:
//! a bounded read, a whole-buffer write, a flush, and a settable read deadline.
//! It is not an abstraction over sockets; it is the shadow this file already
//! cast. The socket path is unchanged: each `self.stream.…` site is the same
//! call it was, reached through a trait method whose `TcpStream` implementation
//! is the original expression.

use alloc::collections::BTreeSet;
use alloc::format;
use alloc::string::ToString;
use alloc::vec;
use alloc::vec::Vec;
use core::time::Duration;

use std::io;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Instant;

use liminal::protocol::{
    CONVERSATION_REPLY_REQUESTED_FLAG, Frame, FrameType, MessageEnvelope, ProtocolError,
    ProtocolVersion, decode, encode, encoded_len,
};

use super::tcp::participant;
use crate::SdkError;

/// Minimum protocol version this client advertises during the handshake.
const CLIENT_MIN_VERSION: ProtocolVersion = ProtocolVersion::new(1, 0);
/// Maximum protocol version this client advertises during the handshake.
const CLIENT_MAX_VERSION: ProtocolVersion = ProtocolVersion::new(1, 0);
/// Maximum time spent waiting on a single stream read or write.
///
/// This bounds ONE read window, not one response. A window that closes with no
/// bytes is a wait, not a failure — see [`RESPONSE_DEADLINE`], which is what
/// actually ends a wait.
pub(in crate::remote) const IO_TIMEOUT: Duration = Duration::from_secs(5);
/// Total wall-clock budget for one server response, spanning as many closed
/// read windows as it takes.
///
/// Derivation: server admission is O(N) in conversation history at a measured
/// 1.153 ms/record (2026-08-10), so a single [`IO_TIMEOUT`] window reaches only
/// ~4,300 records — inside real session sizes, which is how a slow-but-answering
/// server came to be read as a dead one. 60 s reaches ~52,000 records at that
/// rate while still ending the wait on a genuinely silent peer within a minute.
/// It is the bound; [`IO_TIMEOUT`] is only the polling grain beneath it.
pub(in crate::remote) const RESPONSE_DEADLINE: Duration = Duration::from_secs(60);
/// Brief window used to detect an error reply for an otherwise-silent
/// conversation send. The server replies synchronously on the connection thread,
/// so this only needs to cover that one round of processing; on success the
/// server stays silent and this read times out cleanly with nothing buffered.
const CONVERSATION_DRAIN_TIMEOUT: Duration = Duration::from_millis(250);
/// Read chunk size used when draining the socket into the frame buffer.
const READ_CHUNK_BYTES: usize = 4096;
/// Upper bound on a single response frame, guarding against runaway buffering.
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
/// Application stream id used for conversation frames.
const APPLICATION_STREAM_ID: u32 = 1;

/// The exact stream surface [`Connection`] uses: four operations, no more.
///
/// Deliberately NOT `std::io::Read + Write`. Those traits carry a great deal
/// this layer never asks for, and — decisively — they carry no way to say
/// "bound the next read by this window", which is the one socket option
/// [`Connection::receive_with_timeout`] genuinely needs. Naming the four
/// operations directly is what makes a second implementation obviously
/// complete rather than plausibly complete.
pub(in crate::remote) trait FrameStream {
    /// Reads into `buf`, bounded by the deadline last set by
    /// [`set_read_deadline`](Self::set_read_deadline).
    ///
    /// `Ok(0)` means end of file. A window that closes with no bytes must
    /// report `WouldBlock` or `TimedOut`; the two are read identically by
    /// [`Connection::fill_buffer_once`], mirroring the socket contract
    /// where the platform picks between them.
    ///
    /// # Errors
    /// Any transport read failure.
    fn read_bytes(&mut self, buf: &mut [u8]) -> io::Result<usize>;

    /// Writes all of `bytes`, waiting out backpressure under this transport's
    /// write deadline exactly as a blocking socket's `write_all` does.
    ///
    /// # Errors
    /// Any transport write failure, including a deadline that closes with
    /// bytes still unwritten.
    fn write_all_bytes(&mut self, bytes: &[u8]) -> io::Result<()>;

    /// Pushes anything this transport buffers behind the write.
    ///
    /// # Errors
    /// Any transport flush failure.
    fn flush_bytes(&mut self) -> io::Result<()>;

    /// Bounds subsequent [`read_bytes`](Self::read_bytes) calls by `timeout`.
    ///
    /// # Errors
    /// Any failure to install the deadline.
    fn set_read_deadline(&mut self, timeout: Duration) -> io::Result<()>;
}

impl FrameStream for TcpStream {
    fn read_bytes(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        Read::read(self, buf)
    }

    fn write_all_bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
        Write::write_all(self, bytes)
    }

    fn flush_bytes(&mut self) -> io::Result<()> {
        Write::flush(self)
    }

    fn set_read_deadline(&mut self, timeout: Duration) -> io::Result<()> {
        self.set_read_timeout(Some(timeout))
    }
}

/// Owns the stream and the partial-frame read buffer for one server connection.
pub(in crate::remote) struct Connection<S> {
    stream: S,
    buffer: Vec<u8>,
    /// Conversation ids already opened on this connection, so a message does not
    /// re-send `ConversationOpen` (which would leave the server with a duplicate).
    open_conversations: BTreeSet<u64>,
}

impl Connection<TcpStream> {
    /// Connects and completes the handshake carrying `auth_token`, for a server
    /// gated by an `[auth]` section. An empty slice selects open access.
    pub(in crate::remote) fn connect_with_auth(
        address: &str,
        auth_token: &[u8],
    ) -> Result<Self, SdkError> {
        let stream = TcpStream::connect(address).map_err(|source| SdkError::Connection {
            description: format!("failed to connect to {address}: {source}"),
        })?;
        stream
            .set_nodelay(true)
            .map_err(|source| SdkError::Connection {
                description: format!("failed to disable Nagle for {address}: {source}"),
            })?;
        stream
            .set_read_timeout(Some(IO_TIMEOUT))
            .map_err(|source| SdkError::Connection {
                description: format!("failed to set read timeout for {address}: {source}"),
            })?;
        stream
            .set_write_timeout(Some(IO_TIMEOUT))
            .map_err(|source| SdkError::Connection {
                description: format!("failed to set write timeout for {address}: {source}"),
            })?;

        Self::established(stream, auth_token)
    }
}

impl<S: FrameStream> Connection<S> {
    /// Takes an already-open byte stream and drives the protocol handshake
    /// (`Connect` -> `ConnectAck`) over it.
    ///
    /// This is where every mount converges: the stream differs, the handshake
    /// does not. A returned connection has been accepted by the server's
    /// `connect_response` — same version negotiation, same constant-time token
    /// compare — whatever carried the bytes.
    pub(in crate::remote) fn established(stream: S, auth_token: &[u8]) -> Result<Self, SdkError> {
        let mut connection = Self {
            stream,
            buffer: Vec::new(),
            open_conversations: BTreeSet::new(),
        };
        connection.handshake(auth_token)?;
        Ok(connection)
    }

    /// Sends a request frame and blocks for the matching response frame.
    pub(in crate::remote) fn round_trip(&mut self, request: &Frame) -> Result<Frame, SdkError> {
        self.send(request)?;
        self.receive()
    }

    /// Writes one canonical participant request on this established connection.
    pub(in crate::remote) fn send_participant(
        &mut self,
        request: &liminal_protocol::wire::ClientRequest,
    ) -> Result<(), SdkError> {
        self.send(&participant::request_frame(request)?)
    }

    /// Reads and direction-decodes one canonical participant response.
    pub(in crate::remote) fn receive_participant(
        &mut self,
    ) -> Result<liminal_protocol::wire::ParticipantFrame, SdkError> {
        participant::response_frame(self.receive()?)
    }

    fn handshake(&mut self, auth_token: &[u8]) -> Result<(), SdkError> {
        let connect = Frame::Connect {
            flags: 0,
            min_version: CLIENT_MIN_VERSION,
            max_version: CLIENT_MAX_VERSION,
            auth_token: auth_token.to_vec(),
        };
        self.send(&connect)?;
        match self.receive()? {
            Frame::ConnectAck { .. } => Ok(()),
            Frame::ConnectError {
                reason_code,
                message,
                ..
            } => Err(SdkError::Connection {
                description: format!(
                    "server rejected connection (reason {reason_code}): {}",
                    message.unwrap_or_else(|| "no detail".to_string())
                ),
            }),
            other => Err(unexpected_frame("ConnectAck", &other)),
        }
    }

    fn send(&mut self, frame: &Frame) -> Result<(), SdkError> {
        let len = encoded_len(frame).map_err(|error| protocol_error(&error))?;
        let mut bytes = vec![0_u8; len];
        let written = encode(frame, &mut bytes).map_err(|error| protocol_error(&error))?;
        let encoded = bytes.get(..written).ok_or_else(|| SdkError::Protocol {
            description: "wire encoder reported an invalid byte count".to_string(),
        })?;
        self.stream
            .write_all_bytes(encoded)
            .map_err(|source| SdkError::Connection {
                description: format!("failed to write frame to server: {source}"),
            })?;
        self.stream
            .flush_bytes()
            .map_err(|source| SdkError::Connection {
                description: format!("failed to flush frame to server: {source}"),
            })
    }

    fn receive(&mut self) -> Result<Frame, SdkError> {
        self.receive_within(RESPONSE_DEADLINE)
    }

    /// Reads one frame, spending at most `budget` in total across however many
    /// read windows it takes. The budget runs from entry, so a stream of
    /// unsolicited `Deliver` frames cannot extend it indefinitely.
    fn receive_within(&mut self, budget: Duration) -> Result<Frame, SdkError> {
        let started = Instant::now();
        loop {
            match decode(&self.buffer) {
                Ok((frame, consumed)) => {
                    self.buffer.drain(..consumed);
                    if matches!(frame, Frame::Deliver { .. }) {
                        // An unsolicited server `Deliver` on a request/response
                        // connection: in v1, channel deliveries are surfaced only via
                        // the dedicated `SubscriptionStream`, so drain and ignore this
                        // frame here to keep round-trip framing in sync. A pooled
                        // `subscribe` registers a real server-side subscriber for the
                        // delivery-ack signal, so the server pumps a `Deliver` here for
                        // every message on the channel; this drain consumes and discards
                        // them on each round trip (see the teardown caveat on
                        // `TcpRemoteTransport::subscribe`).
                        continue;
                    }
                    return Ok(frame);
                }
                Err(
                    ProtocolError::IncompleteHeader { .. } | ProtocolError::TruncatedPayload { .. },
                ) => self.fill_buffer(started, budget)?,
                Err(error) => return Err(protocol_error(&error)),
            }
        }
    }

    /// Reads until at least one byte lands, ending only when `budget` from
    /// `started` is spent.
    ///
    /// A closed read window is a wait, not a failure. The socket carries
    /// `SO_RCVTIMEO = IO_TIMEOUT`, so one window closing means only that the
    /// reply is slower than 5 s — which server admission, being O(N) in
    /// conversation history, routinely is. Ending the connection there abandons
    /// a socket whose answer may still be in flight; that is the 2026-08-10
    /// outage's client-side mechanism. Only [`RESPONSE_DEADLINE`] ends the wait,
    /// and it says so in its own words rather than surfacing the raw `EAGAIN`
    /// the window reports.
    ///
    /// This is the same shape the push and subscription setup readers already
    /// carry (`tcp/push_client.rs`, `tcp/subscription.rs`), and it is what makes
    /// this path and [`fill_buffer_once`](Self::fill_buffer_once) one read path
    /// under two policies rather than two read paths — the asymmetry that let
    /// only one of them absorb a closed window.
    fn fill_buffer(&mut self, started: Instant, budget: Duration) -> Result<(), SdkError> {
        loop {
            match self.fill_buffer_once()? {
                FillOutcome::Read => return Ok(()),
                FillOutcome::TimedOut => {
                    let elapsed = started.elapsed();
                    if elapsed >= budget {
                        return Err(SdkError::Connection {
                            description: format!(
                                "timed out after {:.3}s waiting for a server response \
                                 (deadline {:.3}s): no complete frame arrived",
                                elapsed.as_secs_f64(),
                                budget.as_secs_f64()
                            ),
                        });
                    }
                }
            }
        }
    }

    /// Sends a conversation message, opening the conversation first if needed, and
    /// surfaces any server `ConversationError` instead of dropping it.
    ///
    /// The wire protocol is asymmetric for conversations: the server stays silent
    /// on success and only replies with a `ConversationError` frame on failure.
    /// After sending, this drains a single error reply (if any) under a brief
    /// timeout so a rejection is reported as an [`SdkError`] and never left
    /// undrained on the shared connection (which would desync the next call).
    pub(super) fn send_conversation_message(
        &mut self,
        conversation_id: u64,
        subject: &str,
        envelope: MessageEnvelope,
    ) -> Result<(), SdkError> {
        self.ensure_conversation_open(conversation_id, subject)?;

        let message = Frame::ConversationMessage {
            flags: 0,
            stream_id: APPLICATION_STREAM_ID,
            conversation_id,
            envelope,
        };
        self.send(&message)?;
        self.drain_conversation_error(conversation_id)
    }

    /// Sends a conversation request that asks for a correlated reply and blocks
    /// for that reply over the socket.
    ///
    /// Opens the conversation on first use, sends the `ConversationMessage` with
    /// the reply-requested flag set, then reads the server's correlated response.
    /// A `ConversationMessage` carrying the same `conversation_id` is the reply and
    /// its payload bytes are returned; a `ConversationError` for the conversation
    /// is surfaced as an [`SdkError`]. Any other frame is a protocol violation.
    ///
    /// Correlation in this synchronous, one-request-per-socket model is positional
    /// plus `conversation_id`: the reply is the next frame the server writes after
    /// receiving this request, and its `conversation_id` must match the request's.
    pub(super) fn conversation_request_reply(
        &mut self,
        conversation_id: u64,
        subject: &str,
        envelope: MessageEnvelope,
    ) -> Result<Vec<u8>, SdkError> {
        self.ensure_conversation_open(conversation_id, subject)?;

        let message = Frame::ConversationMessage {
            flags: CONVERSATION_REPLY_REQUESTED_FLAG,
            stream_id: APPLICATION_STREAM_ID,
            conversation_id,
            envelope,
        };
        self.send(&message)?;
        self.receive_conversation_reply(conversation_id)
    }

    /// Reads the correlated reply frame for `conversation_id`, mapping a matching
    /// `ConversationMessage` to its payload and a `ConversationError` to an error.
    fn receive_conversation_reply(&mut self, conversation_id: u64) -> Result<Vec<u8>, SdkError> {
        match self.receive()? {
            Frame::ConversationMessage {
                conversation_id: replied,
                envelope,
                ..
            } if replied == conversation_id => Ok(envelope.payload),
            Frame::ConversationError {
                conversation_id: replied,
                reason_code,
                message,
                ..
            } => Err(SdkError::Conversation {
                conversation_id: replied.to_string(),
                description: format!(
                    "server rejected conversation {conversation_id} (reason {reason_code}): {}",
                    message.unwrap_or_else(|| "no detail".to_string())
                ),
            }),
            other => Err(unexpected_frame(
                "ConversationMessage reply or ConversationError",
                &other,
            )),
        }
    }

    /// Opens the conversation on first use, surfacing any open failure, and records
    /// it as open only after the server accepts the `ConversationOpen`.
    fn ensure_conversation_open(
        &mut self,
        conversation_id: u64,
        subject: &str,
    ) -> Result<(), SdkError> {
        if self.open_conversations.contains(&conversation_id) {
            return Ok(());
        }
        let open = Frame::ConversationOpen {
            flags: 0,
            stream_id: APPLICATION_STREAM_ID,
            conversation_id,
            subject: subject.to_string(),
        };
        self.send(&open)?;
        // Surface an open failure before recording the conversation as open.
        self.drain_conversation_error(conversation_id)?;
        self.open_conversations.insert(conversation_id);
        Ok(())
    }

    /// Reads a single pending response under a brief timeout. A `ConversationError`
    /// is surfaced as an [`SdkError::Conversation`]; silence (timeout) is success.
    fn drain_conversation_error(&mut self, conversation_id: u64) -> Result<(), SdkError> {
        match self.receive_with_timeout(CONVERSATION_DRAIN_TIMEOUT)? {
            None => Ok(()),
            Some(Frame::ConversationError {
                conversation_id: replied,
                reason_code,
                message,
                ..
            }) => Err(SdkError::Conversation {
                conversation_id: replied.to_string(),
                description: format!(
                    "server rejected conversation {conversation_id} (reason {reason_code}): {}",
                    message.unwrap_or_else(|| "no detail".to_string())
                ),
            }),
            Some(other) => Err(unexpected_frame("ConversationError or no reply", &other)),
        }
    }

    /// Attempts to read one frame within `timeout`. Returns `Ok(None)` when no
    /// bytes arrive in the window, leaving the buffer untouched (no stale frame).
    fn receive_with_timeout(&mut self, timeout: Duration) -> Result<Option<Frame>, SdkError> {
        self.stream
            .set_read_deadline(timeout)
            .map_err(|source| SdkError::Connection {
                description: format!("failed to set conversation drain timeout: {source}"),
            })?;
        let result = self.try_receive_once();
        // Always restore the steady-state timeout, even on error.
        let restore =
            self.stream
                .set_read_deadline(IO_TIMEOUT)
                .map_err(|source| SdkError::Connection {
                    description: format!("failed to restore read timeout: {source}"),
                });
        let frame = result?;
        restore?;
        Ok(frame)
    }

    fn try_receive_once(&mut self) -> Result<Option<Frame>, SdkError> {
        loop {
            match decode(&self.buffer) {
                Ok((frame, consumed)) => {
                    self.buffer.drain(..consumed);
                    if matches!(frame, Frame::Deliver { .. }) {
                        // Skip unsolicited server deliveries (see `receive`): they are
                        // not the correlated reply this drain is looking for.
                        continue;
                    }
                    return Ok(Some(frame));
                }
                Err(
                    ProtocolError::IncompleteHeader { .. } | ProtocolError::TruncatedPayload { .. },
                ) => match self.fill_buffer_once()? {
                    FillOutcome::Read => {}
                    FillOutcome::TimedOut => return Ok(None),
                },
                Err(error) => return Err(protocol_error(&error)),
            }
        }
    }

    /// One read attempt: appends whatever arrives, and reports a closed read
    /// window as [`FillOutcome::TimedOut`] rather than throwing it.
    ///
    /// The only read primitive on this connection. Both callers reach the
    /// socket through it and differ only in what they do with a closed window:
    /// [`fill_buffer`](Self::fill_buffer) weighs it against
    /// [`RESPONSE_DEADLINE`] and keeps waiting, while
    /// [`try_receive_once`](Self::try_receive_once) reads it as the silence that
    /// means a conversation send was accepted. Neither treats it as an I/O
    /// failure; a genuine one still lands on the fatal arm below.
    fn fill_buffer_once(&mut self) -> Result<FillOutcome, SdkError> {
        if self.buffer.len() > MAX_RESPONSE_BYTES {
            return Err(SdkError::Protocol {
                description: format!(
                    "server response exceeded {MAX_RESPONSE_BYTES} bytes without a complete frame"
                ),
            });
        }
        let mut chunk = [0_u8; READ_CHUNK_BYTES];
        match self.stream.read_bytes(&mut chunk) {
            Ok(0) => Err(SdkError::Connection {
                description: "server closed the connection before a full frame arrived".to_string(),
            }),
            Ok(read) => {
                let Some(received) = chunk.get(..read) else {
                    return Err(SdkError::Protocol {
                        description: "socket read reported more bytes than the read buffer holds"
                            .to_string(),
                    });
                };
                self.buffer.extend_from_slice(received);
                Ok(FillOutcome::Read)
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                Ok(FillOutcome::TimedOut)
            }
            Err(error) => Err(SdkError::Connection {
                description: format!("failed to read frame from server: {error}"),
            }),
        }
    }
}

/// Outcome of a single non-fatal socket read attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FillOutcome {
    /// Bytes were appended to the buffer.
    Read,
    /// The read timed out with no bytes available.
    TimedOut,
}

#[cfg(test)]
#[path = "framing_tests.rs"]
mod tests;

/// Maps a low-level wire codec error into the SDK error taxonomy.
pub(in crate::remote) fn protocol_error(error: &ProtocolError) -> SdkError {
    SdkError::Protocol {
        description: format!("wire codec error: {error}"),
    }
}

/// Builds a protocol error describing an unexpected response frame.
pub(in crate::remote) fn unexpected_frame(expected: &str, actual: &Frame) -> SdkError {
    SdkError::Protocol {
        description: format!(
            "expected {expected} frame, received {:?}",
            FrameType::from(u8::from(actual.frame_type()))
        ),
    }
}
