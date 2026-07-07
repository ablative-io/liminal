//! Socket ownership and frame I/O for the TCP transport.
//!
//! [`Connection`] wraps one blocking [`TcpStream`], buffers partial reads until a
//! whole frame decodes (mirroring the server's `process_buffer` loop), and tracks
//! which conversations have been opened so a message never re-opens a conversation
//! or leaves an undrained error frame on the shared socket.

use alloc::collections::BTreeSet;
use alloc::format;
use alloc::string::ToString;
use alloc::vec;
use alloc::vec::Vec;
use core::time::Duration;

use std::io::{Read, Write};
use std::net::TcpStream;

use liminal::protocol::{
    CONVERSATION_REPLY_REQUESTED_FLAG, Frame, FrameType, MessageEnvelope, ProtocolError,
    ProtocolVersion, decode, encode, encoded_len,
};

use crate::SdkError;

/// Minimum protocol version this client advertises during the handshake.
const CLIENT_MIN_VERSION: ProtocolVersion = ProtocolVersion::new(1, 0);
/// Maximum protocol version this client advertises during the handshake.
const CLIENT_MAX_VERSION: ProtocolVersion = ProtocolVersion::new(1, 0);
/// Maximum time spent waiting on a single socket read or write.
const IO_TIMEOUT: Duration = Duration::from_secs(5);
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

/// Owns the socket and the partial-frame read buffer for one server connection.
pub(super) struct Connection {
    stream: TcpStream,
    buffer: Vec<u8>,
    /// Conversation ids already opened on this connection, so a message does not
    /// re-send `ConversationOpen` (which would leave the server with a duplicate).
    open_conversations: BTreeSet<u64>,
}

impl Connection {
    pub(super) fn connect(address: &str) -> Result<Self, SdkError> {
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

        let mut connection = Self {
            stream,
            buffer: Vec::new(),
            open_conversations: BTreeSet::new(),
        };
        connection.handshake()?;
        Ok(connection)
    }

    /// Sends a request frame and blocks for the matching response frame.
    pub(super) fn round_trip(&mut self, request: &Frame) -> Result<Frame, SdkError> {
        self.send(request)?;
        self.receive()
    }

    fn handshake(&mut self) -> Result<(), SdkError> {
        let connect = Frame::Connect {
            flags: 0,
            min_version: CLIENT_MIN_VERSION,
            max_version: CLIENT_MAX_VERSION,
            auth_token: Vec::new(),
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
            .write_all(encoded)
            .map_err(|source| SdkError::Connection {
                description: format!("failed to write frame to server: {source}"),
            })?;
        self.stream.flush().map_err(|source| SdkError::Connection {
            description: format!("failed to flush frame to server: {source}"),
        })
    }

    fn receive(&mut self) -> Result<Frame, SdkError> {
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
                ) => self.fill_buffer()?,
                Err(error) => return Err(protocol_error(&error)),
            }
        }
    }

    fn fill_buffer(&mut self) -> Result<(), SdkError> {
        if self.buffer.len() > MAX_RESPONSE_BYTES {
            return Err(SdkError::Protocol {
                description: format!(
                    "server response exceeded {MAX_RESPONSE_BYTES} bytes without a complete frame"
                ),
            });
        }
        let mut chunk = [0_u8; READ_CHUNK_BYTES];
        let read = self
            .stream
            .read(&mut chunk)
            .map_err(|source| SdkError::Connection {
                description: format!("failed to read frame from server: {source}"),
            })?;
        if read == 0 {
            return Err(SdkError::Connection {
                description: "server closed the connection before a full frame arrived".to_string(),
            });
        }
        let Some(received) = chunk.get(..read) else {
            return Err(SdkError::Protocol {
                description: "socket read reported more bytes than the read buffer holds"
                    .to_string(),
            });
        };
        self.buffer.extend_from_slice(received);
        Ok(())
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
            .set_read_timeout(Some(timeout))
            .map_err(|source| SdkError::Connection {
                description: format!("failed to set conversation drain timeout: {source}"),
            })?;
        let result = self.try_receive_once();
        // Always restore the steady-state timeout, even on error.
        let restore = self
            .stream
            .set_read_timeout(Some(IO_TIMEOUT))
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
                ) => match self.fill_buffer_nonfatal()? {
                    FillOutcome::Read => {}
                    FillOutcome::TimedOut => return Ok(None),
                },
                Err(error) => return Err(protocol_error(&error)),
            }
        }
    }

    /// Like [`fill_buffer`](Self::fill_buffer) but treats a read timeout as a
    /// non-fatal [`FillOutcome::TimedOut`] rather than an error, so an absent
    /// (silent-success) reply can be distinguished from a real I/O failure.
    fn fill_buffer_nonfatal(&mut self) -> Result<FillOutcome, SdkError> {
        if self.buffer.len() > MAX_RESPONSE_BYTES {
            return Err(SdkError::Protocol {
                description: format!(
                    "server response exceeded {MAX_RESPONSE_BYTES} bytes without a complete frame"
                ),
            });
        }
        let mut chunk = [0_u8; READ_CHUNK_BYTES];
        match self.stream.read(&mut chunk) {
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

/// Maps a low-level wire codec error into the SDK error taxonomy.
pub(super) fn protocol_error(error: &ProtocolError) -> SdkError {
    SdkError::Protocol {
        description: format!("wire codec error: {error}"),
    }
}

/// Builds a protocol error describing an unexpected response frame.
pub(super) fn unexpected_frame(expected: &str, actual: &Frame) -> SdkError {
    SdkError::Protocol {
        description: format!(
            "expected {expected} frame, received {:?}",
            FrameType::from(u8::from(actual.frame_type()))
        ),
    }
}
