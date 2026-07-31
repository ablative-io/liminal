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

mod pending_connect;

pub use pending_connect::PendingPushConnect;

use alloc::format;
use alloc::string::ToString;
use alloc::sync::Arc;
use alloc::vec;
use alloc::vec::Vec;
use core::time::Duration;

use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::Mutex;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::thread::JoinHandle;
use std::time::Instant;

use liminal::protocol::{
    CausalContext, Frame, MessageEnvelope, ProtocolError, ProtocolVersion, SchemaId,
    WorkerRegisterOutcome, WorkerRegistration, decode, encode, encoded_len,
};

use super::flush::{
    FLUSH_BUDGET, FlushLedger, FlushMode, FlushOutcome, PublishRejection, PublishVerdict,
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
/// Total wall-clock budget for the drop-time graceful close, so the teardown
/// never hangs on a peer that never sends its FIN even though the common path
/// reaches EOF within a few milliseconds of the write-half `shutdown`.
const DROP_DRAIN_BUDGET: Duration = Duration::from_secs(5);
/// Upper bound on a single buffered frame, guarding against runaway buffering.
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
/// Application stream id used for the client's push reply frames.
const APPLICATION_STREAM_ID: u32 = 1;

/// The reserved channel a worker publishes agent-observability events to over its
/// existing push connection.
///
/// It is NOT a general pub/sub channel: the server routes a publish on this exact
/// channel name straight to its `ConnectionNotifier` observability hook (bypassing
/// the channel-fan-out cluster), so a worker never needs a second connection to
/// stream a transcript. The name is a wire contract shared by the worker publisher
/// and the server's demux, so it is pinned here as the single source of truth.
pub const OBSERVABILITY_CHANNEL: &str = "aion.observability.v1";

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
    ///
    /// Also the reader's own liveness signal: the reader owns the sending half,
    /// so when it ends, this receiver reports `Disconnected`. Teardown waits on
    /// that rather than on a flag the blocked reader could never sample.
    inbound: Receiver<PushedFrame>,
    /// Background reader handle, joined on drop.
    reader: Option<JoinHandle<()>>,
    /// Publish/verdict accounting behind [`PushClient::flush`] and
    /// [`PushClient::close`]; shared with every [`PushWriter`] clone.
    ledger: Arc<FlushLedger>,
}

impl PushClient {
    /// Prepares a push-client connection whose synchronous setup replies use
    /// `deadline` instead of the default five-second setup duration.
    ///
    /// This does not open a socket. Configure optional authentication or worker
    /// registration on the returned value, then call
    /// [`PendingPushConnect::connect`]. See [`PendingPushConnect`] for the exact
    /// per-read and per-control-exchange bounds.
    #[must_use]
    pub const fn with_setup_deadline(address: &str, deadline: Duration) -> PendingPushConnect<'_> {
        PendingPushConnect::new(address, deadline)
    }

    /// Connects to `address`, performs the protocol handshake, and starts the
    /// background reader that drains inbound server pushes.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when the TCP connection or socket
    /// configuration fails, and [`SdkError::Protocol`] when the handshake is
    /// rejected or the socket cannot be cloned for the reader thread.
    pub fn connect(address: &str) -> Result<Self, SdkError> {
        // Open access: an empty token is byte-identical to the pre-auth handshake.
        Self::connect_with_auth(address, &[])
    }

    /// Connects and handshakes carrying `auth_token`, then starts the background
    /// reader, for a server gated by an `[auth]` section. Additive to [`connect`];
    /// an empty token is equivalent to it.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when the TCP connection or socket
    /// configuration fails or the server rejects the token, and
    /// [`SdkError::Protocol`] when the handshake is otherwise rejected or the socket
    /// cannot be cloned for the reader thread.
    ///
    /// [`connect`]: Self::connect
    pub fn connect_with_auth(address: &str, auth_token: &[u8]) -> Result<Self, SdkError> {
        Self::connect_configured(address, auth_token, None, SETUP_TIMEOUT)
    }

    /// Connects, performs the handshake, then synchronously registers this client
    /// as a worker before starting the background reader.
    ///
    /// This mirrors the synchronous `Connect`/`ConnectAck` pattern: the
    /// `WorkerRegister` frame is written and its [`Frame::WorkerRegisterAck`] read
    /// on the calling thread, BEFORE the Push-only background reader is spawned, so
    /// the ack is never swallowed by the reader. A connect-variant (rather than a
    /// `register()` method on a connected client) is the cleanest fit: `connect`
    /// spawns the reader as its last step, so registration must be threaded into
    /// the connect sequence to land before that spawn; a post-connect method would
    /// race the already-running reader for the ack frame.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when the TCP connection or socket
    /// configuration fails, and [`SdkError::Protocol`] when the handshake is
    /// rejected, the server rejects the registration (the rejection reason is
    /// carried in the error), or the socket cannot be cloned for the reader thread.
    pub fn connect_with_registration(
        address: &str,
        registration: WorkerRegistration,
    ) -> Result<Self, SdkError> {
        Self::connect_with_registration_and_auth(address, registration, &[])
    }

    /// Connects, handshakes carrying `auth_token`, registers the worker, then starts
    /// the reader — the auth-gated variant of [`connect_with_registration`]. Additive;
    /// an empty token is equivalent to it.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when the TCP connection or socket
    /// configuration fails or the server rejects the token, and
    /// [`SdkError::Protocol`] when the handshake is otherwise rejected, the server
    /// rejects the registration (the reason is carried in the error), or the socket
    /// cannot be cloned for the reader thread.
    ///
    /// [`connect_with_registration`]: Self::connect_with_registration
    pub fn connect_with_registration_and_auth(
        address: &str,
        registration: WorkerRegistration,
        auth_token: &[u8],
    ) -> Result<Self, SdkError> {
        Self::connect_configured(address, auth_token, Some(registration), SETUP_TIMEOUT)
    }

    fn connect_configured(
        address: &str,
        auth_token: &[u8],
        registration: Option<WorkerRegistration>,
        setup_deadline: Duration,
    ) -> Result<Self, SdkError> {
        let mut stream = connect_socket(address, setup_deadline)?;
        handshake(&mut stream, auth_token, setup_deadline)?;
        if let Some(registration) = registration {
            register(&mut stream, registration, setup_deadline)?;
        }
        Self::start_reader(stream)
    }

    /// Spawns the Push-only background reader over a handshaken (and, for a worker,
    /// already-registered) stream and returns the running client.
    fn start_reader(stream: TcpStream) -> Result<Self, SdkError> {
        // The control exchange is over, so its deadline comes off: the reader
        // blocks on socket input with no read window at all. Teardown ends that
        // wait by shutting the socket down, which surfaces as a typed terminal —
        // the socket signals, nothing sweeps. A window left armed here would be a
        // wake cadence in steady state, which is the defect this retires,
        // whatever period it carried.
        stream
            .set_read_timeout(None)
            .map_err(|source| SdkError::Connection {
                description: format!("failed to clear the push read deadline: {source}"),
            })?;
        // Clone the socket so the reader thread owns one handle and the writer
        // holds the other; both refer to the same underlying connection.
        let read_stream = stream.try_clone().map_err(|source| SdkError::Protocol {
            description: format!("failed to clone push socket for reader thread: {source}"),
        })?;

        let (sender, inbound) = channel();
        let (ledger, verdicts) = FlushLedger::new();
        let ledger = Arc::new(ledger);
        let reader_ledger = Arc::clone(&ledger);
        let reader = std::thread::Builder::new()
            .name("liminal-push-reader".to_string())
            .spawn(move || {
                run_reader(read_stream, &sender, &verdicts, &reader_ledger);
            })
            .map_err(|source| SdkError::Protocol {
                description: format!("failed to start push reader thread: {source}"),
            })?;

        Ok(Self {
            writer: Arc::new(Mutex::new(stream)),
            inbound,
            reader: Some(reader),
            ledger,
        })
    }

    /// Blocks until the background reader ends or `budget` elapses, reporting
    /// whether it ended inside the budget.
    ///
    /// The reader owns the sending half of `inbound`, so its exit drops that half
    /// and surfaces here as `Disconnected` — the reader telling teardown it is
    /// done, with no flag to sample and no cadence to wake on. Pushes that arrive
    /// meanwhile are discarded: the client is going away.
    fn await_reader_exit(&self, budget: Duration) -> bool {
        let deadline = Instant::now() + budget;
        loop {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            match self.inbound.recv_timeout(deadline.duration_since(now)) {
                Ok(_) => {}
                Err(RecvTimeoutError::Disconnected) => return true,
                Err(RecvTimeoutError::Timeout) => return false,
            }
        }
    }

    /// Shuts the shared socket down in `how`, ignoring a poisoned lock (the
    /// socket still closes when the last handle drops).
    fn shutdown_socket(&self, how: Shutdown) {
        if let Ok(stream) = self.writer.lock() {
            let _ = stream.shutdown(how);
        }
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

    /// A cheap, cloneable handle to this push connection's write half, for
    /// background tasks that publish out-of-band frames on the same socket without
    /// owning the full client (which cannot be cloned — it holds the reader thread
    /// join handle).
    ///
    /// The returned [`PushWriter`] shares the client's `Arc<Mutex<TcpStream>>`, so a
    /// frame it writes travels the SAME connection the server pushes on. It is the
    /// worker's observability-drain leg: a drain task holds one and publishes each
    /// [`OBSERVABILITY_CHANNEL`] event live while the client keeps serving pushes.
    #[must_use]
    pub fn writer_handle(&self) -> PushWriter {
        PushWriter {
            writer: Arc::clone(&self.writer),
            ledger: Arc::clone(&self.ledger),
        }
    }

    /// Publish `payload` to `channel` over this connection (out-of-band from the
    /// push/reply round trip).
    ///
    /// Convenience shorthand for `self.writer_handle().publish(channel, payload)`.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Protocol`] when the publish frame cannot be encoded and
    /// [`SdkError::Connection`] when it cannot be written to the socket or the
    /// writer lock is poisoned.
    pub fn publish(&self, channel: &str, payload: Vec<u8>) -> Result<(), SdkError> {
        self.writer_handle().publish(channel, payload)
    }

    /// Awaits the server's verdict for every response-eliciting publish
    /// written to this connection before the call, bounded by a single
    /// wall-clock budget (5 s, in the spirit of [`DROP_DRAIN_BUDGET`]) — a
    /// deadline'd blocking channel receive, never a poll loop.
    ///
    /// Publishes to the reserved [`OBSERVABILITY_CHANNEL`] elicit no server
    /// response by design and are excluded from the flush contract. Responses
    /// are paired to publishes by FIFO wire order (there is no correlation id
    /// on the wire), and rejections are returned verbatim in
    /// [`FlushOutcome::failures`].
    ///
    /// `failures.is_empty() && unresolved == 0` is the ONLY proven-accepted
    /// shape. **Budget expiry with unresolved publishes is a NORMAL outcome
    /// the caller must inspect ([`FlushOutcome::unresolved`]), never an
    /// `Err`.** A `flush()` never half-closes the socket — the client stays
    /// fully usable — so its [`FlushOutcome::mode`] is always
    /// [`FlushMode::VerdictOnly`]; [`FlushMode::FlushedAndHalfClosed`] can
    /// only be produced by [`PushClient::close`]. Concurrent flushes
    /// serialize: a second flush waits on the flush guard, then covers only
    /// its own write-boundary. A [`Frame::PublishAck`] proves server
    /// acceptance, never delivery to any subscriber.
    ///
    /// # Errors
    ///
    /// The outer `Err` is reserved for failures of the flush mechanism
    /// itself: [`SdkError::Connection`] when the flush guard is poisoned, and
    /// [`SdkError::Protocol`] when more publish responses arrived than
    /// response-eliciting publishes were written (a broken pairing invariant
    /// — the flush fails loudly rather than ever mispairing a verdict).
    pub fn flush(&self) -> Result<FlushOutcome, SdkError> {
        let (failures, unresolved) = self.ledger.drain(FLUSH_BUDGET)?;
        Ok(FlushOutcome::new(
            failures,
            unresolved,
            FlushMode::VerdictOnly,
        ))
    }

    /// Flush-then-graceful-close: runs [`PushClient::flush`], then tears the
    /// connection down the way `Drop` does — so the caller learns the verdict
    /// of every in-flight publish BEFORE the socket goes away, which `Drop`
    /// structurally cannot report.
    ///
    /// As sole owner of the socket the teardown half-closes gracefully (FIN,
    /// then drain to the server's FIN) and the outcome's mode is
    /// [`FlushMode::FlushedAndHalfClosed`]. When a live [`PushWriter`] clone
    /// still shares the socket a write-half shutdown would break the clone's
    /// publishes, so close collects verdicts only — no FIN — and discloses
    /// the degradation as [`FlushMode::VerdictOnly`]; a caller that needs the
    /// FIN guarantee drops the clones first.
    ///
    /// # Errors
    ///
    /// Exactly [`PushClient::flush`]'s mechanism errors; the teardown itself
    /// is best-effort and silent, as on `Drop`.
    pub fn close(self) -> Result<FlushOutcome, SdkError> {
        let (failures, unresolved) = self.ledger.drain(FLUSH_BUDGET)?;
        // Sole owner iff no live `PushWriter` clone shares the write half (the
        // reader thread holds a raw cloned stream, not this `Arc`).
        let mode = if Arc::strong_count(&self.writer) == 1 {
            FlushMode::FlushedAndHalfClosed
        } else {
            FlushMode::VerdictOnly
        };
        // `Drop` performs the graceful teardown this mode discloses: stop and
        // join the reader, then drain pending acks — with a write-half FIN as
        // sole owner, or a bounded best-effort drain (no FIN) over a shared
        // socket.
        drop(self);
        Ok(FlushOutcome::new(failures, unresolved, mode))
    }
}

/// A cheap clone of a [`PushClient`]'s write half.
///
/// It writes `Frame::Publish` frames on the SAME socket the client receives pushes
/// on, so a background drain task can stream observability events upstream without a
/// second connection. Cloning is an `Arc` bump; the underlying socket and its write
/// lock are shared with the originating [`PushClient`].
#[derive(Clone, Debug)]
pub struct PushWriter {
    writer: Arc<Mutex<TcpStream>>,
    /// Shared flush accounting: publishes this clone writes are counted so the
    /// originating client's [`PushClient::flush`] covers them too.
    ledger: Arc<FlushLedger>,
}

impl PushWriter {
    /// Publish `payload` to `channel` on the shared connection.
    ///
    /// Writes a single `Frame::Publish` carrying the opaque bytes verbatim (schema
    /// id zero, an independent causal context — the server routes the reserved
    /// observability channel straight to its notifier hook, so no schema negotiation
    /// or ordering context is required). The write takes the shared writer lock, so
    /// it never interleaves bytes with a concurrent push reply.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Protocol`] when the publish frame cannot be encoded and
    /// [`SdkError::Connection`] when it cannot be written to the socket or the writer
    /// lock is poisoned.
    pub fn publish(&self, channel: &str, payload: Vec<u8>) -> Result<(), SdkError> {
        let envelope = MessageEnvelope::new(
            SchemaId::new([0_u8; SchemaId::WIRE_LEN]),
            CausalContext::independent(),
            payload,
        );
        let frame = Frame::new_publish(APPLICATION_STREAM_ID, channel, envelope)
            .map_err(|error| protocol_error(&error))?;
        let mut writer = self.writer.lock().map_err(|error| SdkError::Connection {
            description: format!("push writer lock poisoned: {error}"),
        })?;
        write_frame(&mut writer, &frame)?;
        // Count the publish for the flush contract while still holding the
        // writer lock, so the count follows wire order. Publishes to the
        // reserved observability channel elicit no server response by design
        // and stay OUT of the flush contract.
        if channel != OBSERVABILITY_CHANNEL {
            self.ledger.record_written();
        }
        Ok(())
    }

    /// Send a correlated reply to a server push on the shared connection, echoing the
    /// push's `correlation_id` so the server matches the reply to its push.
    ///
    /// Identical wire effect to [`PushClient::reply`], but issued from a cheap
    /// [`PushWriter`] clone so a BACKGROUND task (e.g. a long-running agent dispatch)
    /// can answer its own push after it completes, without holding the full client or
    /// blocking the serve loop. Shares the writer lock, so it never interleaves bytes
    /// with a concurrent publish or reply.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Protocol`] when the reply frame cannot be encoded and
    /// [`SdkError::Connection`] when it cannot be written to the socket or the writer
    /// lock is poisoned.
    pub fn reply(&self, correlation_id: u64, payload: Vec<u8>) -> Result<(), SdkError> {
        let frame = Frame::new_push_reply(APPLICATION_STREAM_ID, correlation_id, payload)
            .map_err(|error| protocol_error(&error))?;
        let mut writer = self.writer.lock().map_err(|error| SdkError::Connection {
            description: format!("push writer lock poisoned: {error}"),
        })?;
        write_frame(&mut writer, &frame)
    }
}

/// Graceful, TOLD teardown.
///
/// The reader blocks on socket input with no read window, so nothing but the
/// socket itself can end its wait — a stop flag it never wakes to sample would
/// be a lie about how it stops. The half-close IS that tell, and it is the same
/// act that keeps the close graceful: shutting the write half sends a FIN, so
/// the server reads and fans out every publish frame still buffered before it,
/// acks each one, then closes. The reader consumes those acks into the flush
/// ledger — it is now the drainer — and exits on the server's own FIN.
///
/// Reading to EOF is what keeps the final close a FIN rather than a RST: closing
/// a socket whose receive buffer still holds unread bytes resets the connection,
/// and on a reset the server's kernel discards the publish frames it has not yet
/// read, so those fire-and-forget publishes never fan out. That guarantee is
/// unchanged; only the reader, rather than a separate drain loop, now performs
/// it. Its wall-clock bound has moved with it, from a per-read deadline plus a
/// read cap to a single [`DROP_DRAIN_BUDGET`] wait on the reader's own exit — so
/// a peer that never sends its FIN still cannot wedge drop.
///
/// The half-close is taken only when this `PushClient` is the sole owner of the
/// socket. With a live [`PushWriter`] clone still publishing, a write-half
/// shutdown would break the clone's writes, so only the read half is shut: that
/// ends the reader's wait and leaves the clone writing. Nothing reads the
/// clone's later acks after this point — the degradation already disclosed as
/// [`FlushMode::VerdictOnly`], and the reason a caller who needs verdicts calls
/// [`PushClient::close`] before dropping.
impl Drop for PushClient {
    fn drop(&mut self) {
        // Sole owner iff no live `PushWriter` clone shares the write half; the
        // reader thread holds a raw cloned stream, not this `Arc`.
        let sole_owner = Arc::strong_count(&self.writer) == 1;
        if sole_owner {
            self.shutdown_socket(Shutdown::Write);
            if !self.await_reader_exit(DROP_DRAIN_BUDGET) {
                // The peer never closed inside the budget. End the reader's wait
                // at the socket so drop stays bounded rather than hanging on a
                // FIN that is not coming.
                self.shutdown_socket(Shutdown::Both);
            }
        } else {
            self.shutdown_socket(Shutdown::Read);
        }
        if let Some(reader) = self.reader.take() {
            reader.join().ok();
        }
    }
}

/// Opens and configures the push-client socket (Nagle off, the caller-selected
/// maximum wait for one setup read, a bounded write timeout) before any framing.
fn connect_socket(address: &str, setup_deadline: Duration) -> Result<TcpStream, SdkError> {
    let stream = TcpStream::connect(address).map_err(|source| SdkError::Connection {
        description: format!("failed to connect push client to {address}: {source}"),
    })?;
    stream
        .set_nodelay(true)
        .map_err(|source| SdkError::Connection {
            description: format!("failed to disable Nagle for {address}: {source}"),
        })?;
    // The named deadline for a synchronous control-frame reply, and nothing
    // else: it covers the `Connect`/`ConnectAck` and
    // `WorkerRegister`/`WorkerRegisterAck` exchanges that run on the CALLING
    // thread, and `start_reader` takes it back off before the background reader
    // ever sees the socket.
    stream
        .set_read_timeout(Some(setup_deadline))
        .map_err(|source| SdkError::Connection {
            description: format!("failed to set the push setup deadline for {address}: {source}"),
        })?;
    stream
        .set_write_timeout(Some(WRITE_TIMEOUT))
        .map_err(|source| SdkError::Connection {
            description: format!("failed to set push write timeout for {address}: {source}"),
        })?;
    Ok(stream)
}

/// Drives the synchronous worker-registration round trip
/// (`WorkerRegister` -> `WorkerRegisterAck`) on a handshaken socket, before the
/// background reader is spawned.
///
/// A `Rejected` ack maps to a typed [`SdkError::Protocol`] carrying the server's
/// reason; any non-ack reply is a protocol error.
fn register(
    stream: &mut TcpStream,
    registration: WorkerRegistration,
    setup_deadline: Duration,
) -> Result<(), SdkError> {
    let frame = Frame::WorkerRegister {
        flags: 0,
        registration,
    };
    write_frame(stream, &frame)?;
    let mut buffer = Vec::new();
    match read_one_frame(stream, &mut buffer, setup_deadline)? {
        Frame::WorkerRegisterAck {
            outcome: WorkerRegisterOutcome::Accepted,
            ..
        } => Ok(()),
        Frame::WorkerRegisterAck {
            outcome: WorkerRegisterOutcome::Rejected { reason },
            ..
        } => Err(SdkError::Protocol {
            description: format!("server rejected worker registration: {reason}"),
        }),
        other => Err(SdkError::Protocol {
            description: format!(
                "expected WorkerRegisterAck during registration, received {:?}",
                other.frame_type()
            ),
        }),
    }
}

/// Drives the client handshake (`Connect` -> `ConnectAck`) on a fresh socket,
/// carrying `auth_token` (empty for an open, non-auth server).
fn handshake(
    stream: &mut TcpStream,
    auth_token: &[u8],
    setup_deadline: Duration,
) -> Result<(), SdkError> {
    let connect = Frame::Connect {
        flags: 0,
        min_version: CLIENT_MIN_VERSION,
        max_version: CLIENT_MAX_VERSION,
        auth_token: auth_token.to_vec(),
    };
    write_frame(stream, &connect)?;
    let mut buffer = Vec::new();
    match read_one_frame(stream, &mut buffer, setup_deadline)? {
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

/// Background loop: drains the socket, surfacing each `Push` frame on `sender`
/// and each publish verdict (`PublishAck`/`PublishError`) on `verdicts` in wire
/// order for the flush contract.
///
/// The socket carries no read window here, so the loop blocks until the server
/// sends or the connection ends: nothing wakes it on a timer and nothing sweeps.
/// It returns (ending the thread) when the connection closes — including the
/// `shutdown` teardown performs — when a consumer has gone away, or on a fatal
/// decode/IO error.
fn run_reader(
    mut stream: TcpStream,
    sender: &Sender<PushedFrame>,
    verdicts: &Sender<PublishVerdict>,
    ledger: &FlushLedger,
) {
    let mut buffer = Vec::new();
    loop {
        match next_frame(&mut stream, &mut buffer) {
            Ok(Frame::Push {
                correlation_id,
                payload,
                ..
            }) => {
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
            // The server's per-publish verdicts: captured and forwarded in wire
            // order (never discarded — they are what `flush()` awaits).
            Ok(Frame::PublishAck { .. }) => {
                if verdicts.send(PublishVerdict::Accepted).is_err() {
                    return;
                }
                ledger.record_arrival();
            }
            Ok(Frame::PublishError {
                reason_code,
                message,
                ..
            }) => {
                let rejection = PublishRejection::new(reason_code, message);
                if verdicts.send(PublishVerdict::Rejected(rejection)).is_err() {
                    return;
                }
                ledger.record_arrival();
            }
            // Any other frame on a push connection is unexpected for this spike —
            // ignore it rather than tearing the reader down so a stray frame
            // cannot silently drop subsequent pushes.
            Ok(_) => {}
            // Connection closed or a fatal read/decode error: end the thread. The
            // dropped `sender` surfaces as a `Disconnected` on the receiver side,
            // which is also how teardown learns the reader is done.
            Err(_) => return,
        }
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
                        description: "the push reader's steady-state socket reported a read \
                                      deadline it should not carry"
                            .to_string(),
                    });
                }
            },
            Err(error) => return Err(protocol_error(&error)),
        }
    }
}

/// Reads one complete control-frame reply under the caller-selected wall-clock
/// deadline — used for the synchronous handshake and worker-registration replies,
/// on the calling thread, before the background reader starts.
///
/// A socket read window elapsing is NOT the end: the reply may simply be slow, or
/// arriving in pieces. Only the total deadline for this reply ends the wait. The
/// shape this replaces died on the FIRST elapsed window, which — composed with a
/// 100 ms reader poll cadence armed before the handshake — made connect fatal to
/// any peer slower than 100 ms. Nobody chose that.
fn read_one_frame(
    stream: &mut TcpStream,
    buffer: &mut Vec<u8>,
    setup_deadline: Duration,
) -> Result<Frame, SdkError> {
    let deadline = Instant::now() + setup_deadline;
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
                                "push connection timed out waiting for a control-frame reply"
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
#[path = "push_client_tests.rs"]
mod tests;
