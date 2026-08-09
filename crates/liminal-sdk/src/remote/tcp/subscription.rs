//! Client-side subscription stream: the receive half of the delivery pump.
//!
//! Where [`PushClient`](super::push_client::PushClient) consumes server-initiated
//! *pushes*, a [`SubscriptionStream`] consumes server-initiated *deliveries*: the
//! server writes a [`Frame::Deliver`] on the subscription's stream every time a
//! message is published to the subscribed channel. This client owns a dedicated
//! connection whose socket is drained by a background reader thread that routes
//! each `Deliver` into an mpsc queue the caller pulls with
//! [`SubscriptionStream::recv_timeout`].
//!
//! # v1 shape
//!
//! One subscription per dedicated connection. Multiplexing several subscriptions
//! over one connection arrives with the v2 credit mode (which also adds explicit
//! per-delivery acks); until then a `SubscriptionStream` is a single channel
//! subscription bound to its own socket, mirroring the one-connection-per-role
//! shape the `PushClient` already uses.

use alloc::format;
use alloc::string::ToString;
use alloc::vec;
use alloc::vec::Vec;
use core::time::Duration;

use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::thread::JoinHandle;
use std::time::Instant;

use liminal::protocol::{
    Frame, ProtocolError, ProtocolVersion, SchemaId, decode, encode, encoded_len,
};

use crate::SdkError;
use crate::remote::SETUP_TIMEOUT;

/// Minimum protocol version this client advertises during the handshake.
const CLIENT_MIN_VERSION: ProtocolVersion = ProtocolVersion::new(1, 0);
/// Maximum protocol version this client advertises during the handshake.
const CLIENT_MAX_VERSION: ProtocolVersion = ProtocolVersion::new(1, 0);
/// Bound on a single socket write.
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
/// Read chunk size used when draining the socket into the frame buffer.
const READ_CHUNK_BYTES: usize = 4096;
/// Upper bound on a single buffered frame, guarding against runaway buffering.
const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;
/// The single application stream this subscription's deliveries ride on. One
/// subscription per connection in v1, so a fixed stream id is sufficient.
const SUBSCRIPTION_STREAM_ID: u32 = 1;
/// In-flight window advertised on subscribe. The v1 server does not gate delivery
/// on credit, so this is advisory; a generous value avoids any future pacing
/// surprise while the credit mode is still v2 work.
const SUBSCRIBE_MAX_IN_FLIGHT: u32 = 1024;

/// A message the server delivered on this subscription.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliveredMessage {
    delivery_seq: u64,
    schema_id: SchemaId,
    payload: Vec<u8>,
}

impl DeliveredMessage {
    /// The per-subscription monotonic delivery sequence (starts at 1). The anchor
    /// the future ack/resume protocol will acknowledge against.
    #[must_use]
    pub const fn delivery_seq(&self) -> u64 {
        self.delivery_seq
    }

    /// The schema id the server selected for this subscription's stream.
    #[must_use]
    pub const fn schema_id(&self) -> SchemaId {
        self.schema_id
    }

    /// The delivered payload bytes.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Consumes the message, returning the owned payload bytes.
    #[must_use]
    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }
}

/// A connected subscription whose background reader surfaces delivered messages.
///
/// Construct with [`SubscriptionStream::open`]; the background reader starts
/// immediately and runs until the stream is dropped. Pull delivered messages with
/// [`SubscriptionStream::recv_timeout`].
#[derive(Debug)]
pub struct SubscriptionStream {
    /// Write half, used only by setup and the best-effort teardown on drop.
    writer: TcpStream,
    /// Server-assigned subscription id, echoed on `Unsubscribe` at teardown.
    subscription_id: u64,
    /// Delivered messages surfaced by the background reader, or the one typed
    /// terminal the server sent instead. A `SubscribeError` arriving mid-stream
    /// is the ONLY explanation the consumer will ever get for deliveries
    /// stopping, so it rides the same queue as the deliveries rather than being
    /// dropped in the reader (P0 #55).
    inbound: Receiver<Result<DeliveredMessage, SdkError>>,
    /// Background reader handle, joined on drop.
    reader: Option<JoinHandle<()>>,
}

impl SubscriptionStream {
    /// Connects to `address`, performs the handshake, subscribes to `channel`, and
    /// starts the background reader that drains delivered messages.
    ///
    /// `accepted_schemas` is the client's schema-compatibility list; pass an empty
    /// vector to let the server select the channel's configured schema (the
    /// server's negotiation contract).
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when the TCP connection or socket
    /// configuration fails, and [`SdkError::Protocol`] when the handshake or
    /// subscribe is rejected, or the socket cannot be cloned for the reader thread.
    pub fn open(
        address: &str,
        channel: &str,
        accepted_schemas: Vec<SchemaId>,
    ) -> Result<Self, SdkError> {
        Self::open_with_auth(address, channel, accepted_schemas, &[])
    }

    /// Connects, handshakes carrying `auth_token`, subscribes to `channel`, and
    /// starts the background reader.
    ///
    /// A subscription owns a dedicated connection (the v1 shape), so it presents
    /// its own credential in its own `Connect` frame; the token a
    /// request/response transport was built with lives on that transport's
    /// socket and cannot travel here. Additive to [`open`]: an empty token is
    /// exactly the open-access handshake `open` performs, so an ungated server
    /// sees byte-identical bytes either way.
    ///
    /// The server compares the token during the handshake and answers a
    /// mismatch with `ConnectError` before closing, which surfaces here as
    /// [`SdkError::Connection`].
    ///
    /// `accepted_schemas` is the client's schema-compatibility list; pass an
    /// empty vector to let the server select the channel's configured schema.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when the TCP connection or socket
    /// configuration fails or the token is rejected, and [`SdkError::Protocol`]
    /// when the subscribe is rejected, or the socket cannot be cloned for the
    /// reader thread.
    ///
    /// [`open`]: Self::open
    pub fn open_with_auth(
        address: &str,
        channel: &str,
        accepted_schemas: Vec<SchemaId>,
        auth_token: &[u8],
    ) -> Result<Self, SdkError> {
        let mut stream = connect_socket(address)?;
        // A single buffer threads through the whole synchronous setup so any bytes
        // the setup reads past the control-frame reply are preserved. The server
        // may coalesce a `SubscribeAck` with the first `Deliver` frames into one TCP
        // segment (the delivery pump runs in the same slice that acks the
        // subscribe), and a socket read pulls up to `READ_CHUNK_BYTES` at once — so
        // this buffer can hold whole (or partial) `Deliver` frames after the ack.
        // Handing that residue to the reader thread is what keeps those deliveries
        // from being dropped and, worse, from desyncing a reader that would
        // otherwise start mid-frame on a fresh empty buffer.
        let mut buffer = Vec::new();
        handshake(&mut stream, &mut buffer, auth_token)?;
        let subscription_id = subscribe(&mut stream, &mut buffer, channel, accepted_schemas)?;

        // The control exchange is over, so its deadline comes off: the reader
        // blocks on socket input with no read window at all. Teardown shuts the
        // socket down, which surfaces as a typed terminal — the socket signals,
        // nothing sweeps. A window left armed here would be a wake cadence in
        // steady state, which is the defect this retires, whatever period it
        // carried.
        stream
            .set_read_timeout(None)
            .map_err(|source| SdkError::Connection {
                description: format!("failed to clear the subscription read deadline: {source}"),
            })?;
        let read_stream = stream.try_clone().map_err(|source| SdkError::Protocol {
            description: format!("failed to clone subscription socket for reader thread: {source}"),
        })?;
        let (sender, inbound) = mpsc::channel();
        let reader = std::thread::Builder::new()
            .name("liminal-subscription-reader".to_string())
            .spawn(move || run_reader(read_stream, buffer, &sender))
            .map_err(|source| SdkError::Protocol {
                description: format!("failed to start subscription reader thread: {source}"),
            })?;

        Ok(Self {
            writer: stream,
            subscription_id,
            inbound,
            reader: Some(reader),
        })
    }

    /// Blocks up to `timeout` for the next delivered message from the server.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when no message arrives within `timeout`
    /// or the background reader has stopped (e.g. the server closed the stream).
    pub fn recv_timeout(&self, timeout: Duration) -> Result<DeliveredMessage, SdkError> {
        match self.inbound.recv_timeout(timeout) {
            Ok(delivery) => delivery,
            Err(error) => {
                let detail = match error {
                    RecvTimeoutError::Timeout => "no delivery arrived within the timeout",
                    RecvTimeoutError::Disconnected => {
                        "the subscription reader stopped before a delivery arrived"
                    }
                };
                Err(SdkError::Connection {
                    description: format!("subscription receive failed: {detail}"),
                })
            }
        }
    }

    /// The server-assigned id for this subscription.
    #[must_use]
    pub const fn subscription_id(&self) -> u64 {
        self.subscription_id
    }
}

impl Drop for SubscriptionStream {
    fn drop(&mut self) {
        // Best-effort clean teardown: tell the server to drop the subscription and
        // close the connection. Failures are ignored — the connection close alone
        // frees the server-side subscription when its subscriber process exits.
        let unsubscribe = Frame::Unsubscribe {
            flags: 0,
            stream_id: SUBSCRIPTION_STREAM_ID,
            subscription_id: self.subscription_id,
        };
        let _ = write_frame(&mut self.writer, &unsubscribe);
        let _ = write_frame(&mut self.writer, &Frame::Disconnect { flags: 0 });
        // Then TELL the reader. It blocks on socket input with no read window, so
        // nothing but the socket can end its wait — a stop flag it never wakes to
        // sample would be a lie about how it stops. Shutting the socket down
        // surfaces a typed terminal to the blocked reader, exactly as the
        // WebSocket sibling does, and the shutdown of the write half flushes the
        // frames just written before its FIN. The join is therefore bounded by the
        // shutdown, not by a peer's goodwill.
        let _ = self.writer.shutdown(Shutdown::Both);
        if let Some(reader) = self.reader.take() {
            reader.join().ok();
        }
    }
}

/// Opens and configures the subscription socket (Nagle off, bounded read/write
/// timeouts) before any framing.
fn connect_socket(address: &str) -> Result<TcpStream, SdkError> {
    let stream = TcpStream::connect(address).map_err(|source| SdkError::Connection {
        description: format!("failed to connect subscription client to {address}: {source}"),
    })?;
    stream
        .set_nodelay(true)
        .map_err(|source| SdkError::Connection {
            description: format!("failed to disable Nagle for {address}: {source}"),
        })?;
    // The named deadline for a synchronous control-frame reply, and nothing
    // else: it covers the `Connect`/`ConnectAck` and `Subscribe`/`SubscribeAck`
    // exchanges that run on the calling thread, and `open` takes it back off
    // before the background reader ever sees the socket.
    stream
        .set_read_timeout(Some(SETUP_TIMEOUT))
        .map_err(|source| SdkError::Connection {
            description: format!(
                "failed to set the subscription setup deadline for {address}: {source}"
            ),
        })?;
    stream
        .set_write_timeout(Some(WRITE_TIMEOUT))
        .map_err(|source| SdkError::Connection {
            description: format!(
                "failed to set subscription write timeout for {address}: {source}"
            ),
        })?;
    Ok(stream)
}

/// Drives the client handshake (`Connect` -> `ConnectAck`) on a fresh socket,
/// presenting `auth_token` (empty for an open, non-auth server).
///
/// `buffer` carries any residue read past the reply forward to the next setup step
/// (and ultimately the reader thread) rather than discarding it.
fn handshake(
    stream: &mut TcpStream,
    buffer: &mut Vec<u8>,
    auth_token: &[u8],
) -> Result<(), SdkError> {
    let connect = Frame::Connect {
        flags: 0,
        min_version: CLIENT_MIN_VERSION,
        max_version: CLIENT_MAX_VERSION,
        auth_token: auth_token.to_vec(),
    };
    write_frame(stream, &connect)?;
    match read_one_frame(stream, buffer)? {
        Frame::ConnectAck { .. } => Ok(()),
        Frame::ConnectError {
            reason_code,
            message,
            ..
        } => Err(SdkError::Connection {
            description: format!(
                "server rejected subscription connection (reason {reason_code}): {}",
                message.unwrap_or_else(|| "no detail".to_string())
            ),
        }),
        other => Err(SdkError::Protocol {
            description: format!(
                "expected ConnectAck during subscription handshake, received {:?}",
                other.frame_type()
            ),
        }),
    }
}

/// Drives the synchronous subscribe round trip (`Subscribe` -> `SubscribeAck`) on
/// a handshaken socket, returning the server-assigned subscription id.
fn subscribe(
    stream: &mut TcpStream,
    buffer: &mut Vec<u8>,
    channel: &str,
    accepted_schemas: Vec<SchemaId>,
) -> Result<u64, SdkError> {
    let frame = Frame::Subscribe {
        flags: 0,
        stream_id: SUBSCRIPTION_STREAM_ID,
        channel: channel.to_string(),
        accepted_schemas,
        max_in_flight: SUBSCRIBE_MAX_IN_FLIGHT,
    };
    write_frame(stream, &frame)?;
    match read_one_frame(stream, buffer)? {
        Frame::SubscribeAck {
            subscription_id, ..
        } => Ok(subscription_id),
        Frame::SubscribeError {
            reason_code,
            message,
            ..
        } => Err(SdkError::Protocol {
            description: format!(
                "server rejected subscribe (reason {reason_code}): {}",
                message.unwrap_or_else(|| "no detail".to_string())
            ),
        }),
        other => Err(SdkError::Protocol {
            description: format!(
                "expected SubscribeAck during subscribe, received {:?}",
                other.frame_type()
            ),
        }),
    }
}

/// Background loop: drains the socket, surfacing each `Deliver` frame's message on
/// `sender`.
///
/// The socket carries no read window here, so the loop blocks until the server
/// sends or the connection ends: nothing wakes it on a timer and nothing sweeps.
/// It returns (ending the thread) when the connection closes — including the
/// `shutdown` teardown performs — when a `Disconnect` arrives, when the consumer
/// has gone away, or on a fatal decode/IO error.
///
/// `buffer` is seeded with the setup residue (see [`SubscriptionStream::open`]): any
/// `Deliver` bytes the synchronous subscribe read past the `SubscribeAck` are
/// already here, so the loop decodes them first — before its next socket read —
/// instead of losing them and starting mid-stream.
fn run_reader(
    mut stream: TcpStream,
    mut buffer: Vec<u8>,
    sender: &Sender<Result<DeliveredMessage, SdkError>>,
) {
    loop {
        // Connection closed or a fatal read/decode error: end the thread. The
        // dropped `sender` surfaces as a `Disconnected` on the receiver side.
        let Ok(frame) = next_frame(&mut stream, &mut buffer) else {
            return;
        };
        match frame {
            Frame::Deliver {
                delivery_seq,
                envelope,
                ..
            } => {
                let message = DeliveredMessage {
                    delivery_seq,
                    schema_id: envelope.schema_id,
                    payload: envelope.payload,
                };
                if sender.send(Ok(message)).is_err() {
                    // The receiver was dropped; nothing will consume further
                    // deliveries, so stop reading.
                    return;
                }
            }
            // A server `Disconnect` ends the subscription cleanly.
            Frame::Disconnect { .. } => return,
            // A `SubscribeError` arriving AFTER setup is the server ending this
            // subscription -- the overflow shed sends exactly this and then
            // releases the subscription at the channel actor, so no further
            // delivery can ever arrive. It is surfaced to the consumer and ends
            // the reader (P0 #55).
            //
            // This is the one exception to the stray-frame rule below, and the
            // distinction is deliveries: ignoring a stray frame protects the
            // deliveries still to come, and here there are none. Dropping this
            // frame is what left a shed subscriber unable to tell "the server
            // dropped me" from "nothing was published".
            Frame::SubscribeError {
                reason_code,
                message,
                ..
            } => {
                let _sent = sender.send(Err(subscription_ended(reason_code, message)));
                return;
            }
            // Any other frame on a subscription connection is unexpected; ignore it
            // rather than tearing the reader down so a stray frame cannot silently
            // drop subsequent deliveries.
            _ => {}
        }
    }
}

/// Builds the typed terminal for a `SubscribeError` the server sent mid-stream.
///
/// The server's own detail is carried VERBATIM: it is the only text that says
/// which limit ended the subscription, and a client that paraphrased it would
/// leave an operator correlating a client log against a server log by guesswork.
fn subscription_ended(reason_code: u16, message: Option<alloc::string::String>) -> SdkError {
    SdkError::Protocol {
        description: format!(
            "server ended the subscription (reason {reason_code}): {}",
            message.unwrap_or_else(|| "no detail".to_string())
        ),
    }
}

/// Reads until one complete frame decodes on the windowless steady-state socket.
///
/// There is no read window to expire here, so a [`FillOutcome::TimedOut`] would
/// mean one was re-armed behind the reader's back. That is reported as the
/// invariant break it is, rather than swallowed into a spin — a reader that
/// looped on it would be a busy-wait, which is worse than the cadence this
/// retired.
fn next_frame(stream: &mut TcpStream, buffer: &mut Vec<u8>) -> Result<Frame, SdkError> {
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
                        description: "the subscription reader's steady-state socket reported a \
                                      read deadline it should not carry"
                            .to_string(),
                    });
                }
            },
            Err(error) => return Err(protocol_error(&error)),
        }
    }
}

/// Reads one complete control-frame reply under the named [`SETUP_TIMEOUT`]
/// deadline — used for the synchronous handshake and subscribe replies, on the
/// calling thread, before the background reader starts.
///
/// A socket read window elapsing is NOT the end: the reply may simply be slow, or
/// arriving in pieces. Only the total deadline for this reply ends the wait.
fn read_one_frame(stream: &mut TcpStream, buffer: &mut Vec<u8>) -> Result<Frame, SdkError> {
    let deadline = Instant::now() + SETUP_TIMEOUT;
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
                    if Instant::now() >= deadline {
                        return Err(SdkError::Connection {
                            description:
                                "subscription connection timed out waiting for a control-frame reply"
                                    .to_string(),
                        });
                    }
                }
            },
            Err(error) => return Err(protocol_error(&error)),
        }
    }
}

/// Appends one socket read into `buffer`, mapping a read timeout to a non-fatal
/// [`FillOutcome::TimedOut`] so the setup reader can weigh it against its
/// deadline.
fn fill_buffer(stream: &mut TcpStream, buffer: &mut Vec<u8>) -> Result<FillOutcome, SdkError> {
    if buffer.len() > MAX_FRAME_BYTES {
        return Err(SdkError::Protocol {
            description: format!(
                "subscription frame exceeded {MAX_FRAME_BYTES} bytes without a complete frame"
            ),
        });
    }
    let mut chunk = [0_u8; READ_CHUNK_BYTES];
    match stream.read(&mut chunk) {
        Ok(0) => Err(SdkError::Connection {
            description: "server closed the subscription connection".to_string(),
        }),
        Ok(read) => {
            let Some(received) = chunk.get(..read) else {
                return Err(SdkError::Protocol {
                    description:
                        "subscription socket read reported more bytes than the buffer holds"
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
            description: format!("failed to read from subscription connection: {error}"),
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
        description: "subscription wire encoder reported an invalid byte count".to_string(),
    })?;
    stream
        .write_all(encoded)
        .map_err(|source| SdkError::Connection {
            description: format!("failed to write subscription frame: {source}"),
        })?;
    stream.flush().map_err(|source| SdkError::Connection {
        description: format!("failed to flush subscription frame: {source}"),
    })
}

/// Maps a wire codec error into the SDK error taxonomy.
fn protocol_error(error: &ProtocolError) -> SdkError {
    SdkError::Protocol {
        description: format!("subscription wire codec error: {error}"),
    }
}

#[cfg(test)]
#[path = "subscription_tests.rs"]
mod tests;
