//! Client-side background reader for server-initiated pushes.
//!
//! Every other SDK transport call is request/response: the client writes a frame
//! and reads exactly one reply to its own request ([`Connection::round_trip`]). A
//! server PUSH inverts that — the server writes a [`Frame::Push`] on the client's
//! existing connection at a time of the server's choosing, with no outstanding
//! client request to read it. [`PushClient`] is the piece that consumes those
//! inbound frames: it owns a connection whose socket is drained by a dedicated
//! background reader thread, surfaces each pushed frame on a channel, and lets the
//! caller send back a correlated [`Frame::PushReply`] on the same socket.
//!
//! # Read/write split
//!
//! A push connection is read concurrently (the background thread blocks on the
//! socket) and written concurrently (the caller replies). `TcpStream` is cloned so
//! the reader thread owns one handle and the writer holds the other behind a
//! `Mutex`; the two handles share the same underlying socket, so a reply written
//! by the caller travels the connection the server is pushing on. This keeps the
//! request/reply [`Connection`] (which couples a single read to a single write)
//! completely untouched — the push path is additive, not a rewrite.

use alloc::format;
use alloc::string::ToString;
use alloc::sync::Arc;
use alloc::vec;
use alloc::vec::Vec;
use core::time::Duration;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::thread::JoinHandle;

use liminal::protocol::{Frame, ProtocolError, ProtocolVersion, decode, encode, encoded_len};

use crate::SdkError;

/// Minimum protocol version this client advertises during the handshake.
const CLIENT_MIN_VERSION: ProtocolVersion = ProtocolVersion::new(1, 0);
/// Maximum protocol version this client advertises during the handshake.
const CLIENT_MAX_VERSION: ProtocolVersion = ProtocolVersion::new(1, 0);
/// Bound on a single socket write.
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
/// Poll cadence the reader thread uses so it can observe the stop flag promptly
/// between reads while still blocking efficiently on the socket the rest of the
/// time.
const READER_POLL_TIMEOUT: Duration = Duration::from_millis(100);
/// Read chunk size used when draining the socket into the frame buffer.
const READ_CHUNK_BYTES: usize = 4096;
/// Upper bound on a single buffered frame, guarding against runaway buffering.
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
/// Application stream id used for the client's push reply frames.
const APPLICATION_STREAM_ID: u32 = 1;

/// A frame the server pushed to this client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushedFrame {
    /// Correlation id the server assigned; echo it on the reply.
    correlation_id: u64,
    /// Opaque payload bytes the server pushed.
    payload: Vec<u8>,
}

impl PushedFrame {
    /// Correlation id to echo back on the reply so the server matches it.
    #[must_use]
    pub const fn correlation_id(&self) -> u64 {
        self.correlation_id
    }

    /// Opaque payload bytes the server pushed.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Consumes the frame, returning the owned payload bytes.
    #[must_use]
    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }
}

/// A connected client that consumes server pushes and sends correlated replies.
///
/// Construct with [`PushClient::connect`]; the background reader starts
/// immediately and runs until the client is dropped. Pull pushed frames with
/// [`PushClient::recv_timeout`] and answer them with [`PushClient::reply`].
#[derive(Debug)]
pub struct PushClient {
    /// Write half of the shared socket, guarded so the caller's reply does not
    /// interleave bytes with any other writer.
    writer: Arc<Mutex<TcpStream>>,
    /// Inbound pushed frames surfaced by the background reader.
    inbound: Receiver<PushedFrame>,
    /// Signals the reader thread to stop; set on drop.
    stop: Arc<AtomicBool>,
    /// Background reader handle, joined on drop.
    reader: Option<JoinHandle<()>>,
}

impl PushClient {
    /// Connects to `address`, performs the protocol handshake, and starts the
    /// background reader that drains inbound server pushes.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when the TCP connection or socket
    /// configuration fails, and [`SdkError::Protocol`] when the handshake is
    /// rejected or the socket cannot be cloned for the reader thread.
    pub fn connect(address: &str) -> Result<Self, SdkError> {
        let mut stream = TcpStream::connect(address).map_err(|source| SdkError::Connection {
            description: format!("failed to connect push client to {address}: {source}"),
        })?;
        stream
            .set_nodelay(true)
            .map_err(|source| SdkError::Connection {
                description: format!("failed to disable Nagle for {address}: {source}"),
            })?;
        // A bounded read timeout lets the reader thread wake to check the stop flag
        // even when the server is silent; without it the thread would block forever
        // on a quiet connection and never observe drop.
        stream
            .set_read_timeout(Some(READER_POLL_TIMEOUT))
            .map_err(|source| SdkError::Connection {
                description: format!("failed to set push read timeout for {address}: {source}"),
            })?;
        stream
            .set_write_timeout(Some(WRITE_TIMEOUT))
            .map_err(|source| SdkError::Connection {
                description: format!("failed to set push write timeout for {address}: {source}"),
            })?;

        handshake(&mut stream)?;

        // Clone the socket so the reader thread owns one handle and the writer
        // holds the other; both refer to the same underlying connection.
        let read_stream = stream.try_clone().map_err(|source| SdkError::Protocol {
            description: format!("failed to clone push socket for reader thread: {source}"),
        })?;

        let stop = Arc::new(AtomicBool::new(false));
        let (sender, inbound) = channel();
        let reader_stop = Arc::clone(&stop);
        let reader = std::thread::Builder::new()
            .name("liminal-push-reader".to_string())
            .spawn(move || run_reader(read_stream, &sender, &reader_stop))
            .map_err(|source| SdkError::Protocol {
                description: format!("failed to start push reader thread: {source}"),
            })?;

        Ok(Self {
            writer: Arc::new(Mutex::new(stream)),
            inbound,
            stop,
            reader: Some(reader),
        })
    }

    /// Blocks up to `timeout` for the next pushed frame from the server.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when no push arrives within `timeout` or
    /// the background reader has stopped (e.g. the server closed the connection).
    pub fn recv_timeout(&self, timeout: Duration) -> Result<PushedFrame, SdkError> {
        self.inbound.recv_timeout(timeout).map_err(|error| {
            let detail = match error {
                RecvTimeoutError::Timeout => "no server push arrived within the timeout",
                RecvTimeoutError::Disconnected => {
                    "the push reader stopped before a server push arrived"
                }
            };
            SdkError::Connection {
                description: format!("push receive failed: {detail}"),
            }
        })
    }

    /// Sends a correlated reply to a pushed frame, echoing its correlation id so
    /// the server matches the reply back to the originating push.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Protocol`] when the reply frame cannot be encoded and
    /// [`SdkError::Connection`] when it cannot be written to the socket or the
    /// writer lock is poisoned.
    pub fn reply(&self, correlation_id: u64, payload: Vec<u8>) -> Result<(), SdkError> {
        let frame = Frame::new_push_reply(APPLICATION_STREAM_ID, correlation_id, payload)
            .map_err(|error| protocol_error(&error))?;
        let mut writer = self.writer.lock().map_err(|error| SdkError::Connection {
            description: format!("push writer lock poisoned: {error}"),
        })?;
        write_frame(&mut writer, &frame)
    }
}

impl Drop for PushClient {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(reader) = self.reader.take() {
            // The reader wakes within READER_POLL_TIMEOUT to observe the stop flag,
            // so this join does not hang on a quiet connection.
            reader.join().ok();
        }
    }
}

/// Drives the client handshake (`Connect` -> `ConnectAck`) on a fresh socket.
fn handshake(stream: &mut TcpStream) -> Result<(), SdkError> {
    let connect = Frame::Connect {
        flags: 0,
        min_version: CLIENT_MIN_VERSION,
        max_version: CLIENT_MAX_VERSION,
        auth_token: Vec::new(),
    };
    write_frame(stream, &connect)?;
    let mut buffer = Vec::new();
    match read_one_frame(stream, &mut buffer)? {
        Frame::ConnectAck { .. } => Ok(()),
        Frame::ConnectError {
            reason_code,
            message,
            ..
        } => Err(SdkError::Connection {
            description: format!(
                "server rejected push connection (reason {reason_code}): {}",
                message.unwrap_or_else(|| "no detail".to_string())
            ),
        }),
        other => Err(SdkError::Protocol {
            description: format!(
                "expected ConnectAck during push handshake, received {:?}",
                other.frame_type()
            ),
        }),
    }
}

/// Background loop: drains the socket, surfacing each `Push` frame on `sender`.
///
/// Returns (ending the thread) when the stop flag is set, the connection closes,
/// or a fatal decode/IO error occurs. A read timeout is non-fatal: it just lets
/// the loop re-check the stop flag.
fn run_reader(mut stream: TcpStream, sender: &Sender<PushedFrame>, stop: &AtomicBool) {
    let mut buffer = Vec::new();
    while !stop.load(Ordering::SeqCst) {
        match next_frame(&mut stream, &mut buffer) {
            Ok(Some(Frame::Push {
                correlation_id,
                payload,
                ..
            })) => {
                if sender
                    .send(PushedFrame {
                        correlation_id,
                        payload,
                    })
                    .is_err()
                {
                    // The receiver was dropped; nothing will consume further
                    // pushes, so stop reading.
                    return;
                }
            }
            // `Some(_)`: any non-Push frame on a push connection is unexpected for
            // this spike — ignore it rather than tearing the reader down so a stray
            // frame cannot silently drop subsequent pushes. `None`: a read timeout
            // with no complete frame. Both just loop to re-check the stop flag.
            Ok(Some(_) | None) => {}
            // Connection closed or a fatal read/decode error: end the thread. The
            // dropped `sender` surfaces as a `Disconnected` on the receiver side.
            Err(_) => return,
        }
    }
}

/// Reads until one complete frame decodes, treating a read timeout as
/// `Ok(None)` so the caller can re-check the stop flag without ending the loop.
fn next_frame(stream: &mut TcpStream, buffer: &mut Vec<u8>) -> Result<Option<Frame>, SdkError> {
    loop {
        match decode(buffer) {
            Ok((frame, consumed)) => {
                buffer.drain(..consumed);
                return Ok(Some(frame));
            }
            Err(
                ProtocolError::IncompleteHeader { .. } | ProtocolError::TruncatedPayload { .. },
            ) => match fill_buffer(stream, buffer)? {
                FillOutcome::Read => {}
                FillOutcome::TimedOut => return Ok(None),
            },
            Err(error) => return Err(protocol_error(&error)),
        }
    }
}

/// Reads one complete frame, blocking (no timeout tolerance) — used only for the
/// synchronous handshake reply before the background reader starts.
fn read_one_frame(stream: &mut TcpStream, buffer: &mut Vec<u8>) -> Result<Frame, SdkError> {
    loop {
        match decode(buffer) {
            Ok((frame, consumed)) => {
                buffer.drain(..consumed);
                return Ok(frame);
            }
            Err(
                ProtocolError::IncompleteHeader { .. } | ProtocolError::TruncatedPayload { .. },
            ) => match fill_buffer(stream, buffer)? {
                FillOutcome::Read => {}
                FillOutcome::TimedOut => {
                    return Err(SdkError::Connection {
                        description: "push handshake timed out waiting for ConnectAck".to_string(),
                    });
                }
            },
            Err(error) => return Err(protocol_error(&error)),
        }
    }
}

/// Appends one socket read into `buffer`, mapping a read timeout to a non-fatal
/// [`FillOutcome::TimedOut`] so the reader can poll the stop flag.
fn fill_buffer(stream: &mut TcpStream, buffer: &mut Vec<u8>) -> Result<FillOutcome, SdkError> {
    if buffer.len() > MAX_FRAME_BYTES {
        return Err(SdkError::Protocol {
            description: format!(
                "push frame exceeded {MAX_FRAME_BYTES} bytes without a complete frame"
            ),
        });
    }
    let mut chunk = [0_u8; READ_CHUNK_BYTES];
    match stream.read(&mut chunk) {
        Ok(0) => Err(SdkError::Connection {
            description: "server closed the push connection".to_string(),
        }),
        Ok(read) => {
            let Some(received) = chunk.get(..read) else {
                return Err(SdkError::Protocol {
                    description: "push socket read reported more bytes than the buffer holds"
                        .to_string(),
                });
            };
            buffer.extend_from_slice(received);
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
            description: format!("failed to read from push connection: {error}"),
        }),
    }
}

/// Outcome of one non-fatal socket read attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FillOutcome {
    Read,
    TimedOut,
}

/// Encodes and writes one frame to the socket, flushing it.
fn write_frame(stream: &mut TcpStream, frame: &Frame) -> Result<(), SdkError> {
    let len = encoded_len(frame).map_err(|error| protocol_error(&error))?;
    let mut bytes = vec![0_u8; len];
    let written = encode(frame, &mut bytes).map_err(|error| protocol_error(&error))?;
    let encoded = bytes.get(..written).ok_or_else(|| SdkError::Protocol {
        description: "push wire encoder reported an invalid byte count".to_string(),
    })?;
    stream
        .write_all(encoded)
        .map_err(|source| SdkError::Connection {
            description: format!("failed to write push frame: {source}"),
        })?;
    stream.flush().map_err(|source| SdkError::Connection {
        description: format!("failed to flush push frame: {source}"),
    })
}

/// Maps a wire codec error into the SDK error taxonomy.
fn protocol_error(error: &ProtocolError) -> SdkError {
    SdkError::Protocol {
        description: format!("push wire codec error: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use liminal::protocol::FrameType;

    #[test]
    fn pushed_frame_exposes_correlation_and_payload() {
        let frame = PushedFrame {
            correlation_id: 7,
            payload: vec![1, 2, 3],
        };
        assert_eq!(frame.correlation_id(), 7);
        assert_eq!(frame.payload(), &[1, 2, 3]);
        assert_eq!(frame.into_payload(), vec![1, 2, 3]);
    }

    #[test]
    fn reply_frame_round_trips_through_codec() -> Result<(), SdkError> {
        let frame = Frame::new_push_reply(APPLICATION_STREAM_ID, 9, vec![4, 5])
            .map_err(|error| protocol_error(&error))?;
        let len = encoded_len(&frame).map_err(|error| protocol_error(&error))?;
        let mut bytes = vec![0_u8; len];
        let written = encode(&frame, &mut bytes).map_err(|error| protocol_error(&error))?;
        let (decoded, consumed) =
            decode(&bytes[..written]).map_err(|error| protocol_error(&error))?;
        assert_eq!(consumed, written);
        assert_eq!(decoded.frame_type(), FrameType::PushReply);
        Ok(())
    }
}
