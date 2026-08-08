//! The bounded byte duplex the loopback transport carries frames over (§3).
//!
//! Two SPSC byte rings — client-to-server and server-to-client — each a
//! `VecDeque<u8>` behind a `Mutex` with a `Condvar` for the one blocking
//! reader. Correctness over cleverness is the deliberate choice: the cost
//! baseline this replaces is a syscall, so a lock-free ring would buy nothing
//! measurable and would cost the ability to read the close semantics off the
//! page.
//!
//! **Bounded is load-bearing.** A socket applies backpressure through kernel
//! buffers and `WouldBlock`; an unbounded queue would give the loopback mount a
//! semantics no other mount has (infinite buffering) and an unbounded
//! idle-memory class. A full ring answers `WouldBlock`, a nearly-full ring
//! accepts a partial write, and [`super::super::outbound::OutboundWriter`]'s
//! budget and partial-write logic then behaves over a ring exactly as it
//! behaves over a socket. Neither end's `write` ever blocks.
//!
//! **The reader is TOLD, never polls.** The loopback has no descriptor, so the
//! beamr readiness facility cannot arm it and the retired busy loop is not
//! coming back. Instead the writer wakes the reader: a write that takes the
//! client-to-server ring from empty to non-empty invokes the server end's
//! registered waker, as does the client end's drop, so a parked connection
//! learns about bytes and about hangups by the same mechanism. Writes into a
//! ring that already has bytes wake nobody — the reader has already been told.
//! The client end needs no waker: its blocking read parks on the condvar, and
//! that condvar IS its wake.
//!
//! **Idle cost is zero.** A duplex owns no thread, arms no timer, and
//! schedules nothing. Two idle rings are two empty `VecDeque`s and their
//! synchronisation primitives; nothing runs until a byte is written or an end
//! is dropped.

use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use crate::ServerError;

use super::super::process::InboundPending;

/// The smallest ring a duplex will hand out, in bytes.
///
/// A zero-capacity ring can never accept a byte, so its writer would see
/// `WouldBlock` forever while its reader waits for bytes that cannot arrive: a
/// ring that can make no progress is not a legal state. The floor is applied
/// rather than refused so that [`LoopbackDuplex::bounded`] stays infallible.
const MIN_RING_CAPACITY_BYTES: usize = 1;

/// Locks a mutex, adopting the guarded value through a poisoned lock.
///
/// A poisoned ring is a ring whose writer panicked mid-`extend`; the bytes
/// behind it are still a well-formed `VecDeque` and the close flags are still
/// booleans, so refusing to read them would turn one participant's panic into
/// a wedged transport. This mirrors the supervisor's `recover_lock` discipline.
fn recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The mutable half of one ring: the bytes in flight and the two end-of-life
/// flags that give the ring its close semantics.
#[derive(Debug)]
struct RingState {
    /// Bytes written but not yet read, in order.
    bytes: VecDeque<u8>,
    /// The end that WRITES into this ring has been dropped. Once `bytes` is
    /// drained the reader is at end of file.
    writer_gone: bool,
    /// The end that READS from this ring has been dropped. Further writes are
    /// `BrokenPipe` immediately, drained or not — the bytes have nowhere to go.
    reader_gone: bool,
}

/// One bounded direction of the duplex.
#[derive(Debug)]
struct Ring {
    /// The hard byte bound. Never grows.
    capacity: usize,
    /// The bytes and the close flags.
    state: Mutex<RingState>,
    /// Signalled on every change a blocked reader could care about: bytes
    /// arriving, and the writer end going away.
    changed: Condvar,
}

impl Ring {
    const fn new(capacity: usize) -> Self {
        Self {
            capacity,
            state: Mutex::new(RingState {
                bytes: VecDeque::new(),
                writer_gone: false,
                reader_gone: false,
            }),
            changed: Condvar::new(),
        }
    }

    /// Bytes readable right now, consuming none of them.
    fn readable_bytes(&self) -> usize {
        recover(&self.state).bytes.len()
    }

    /// Whether a read would answer immediately — with bytes, or with the end
    /// of file the writer's drop left behind.
    fn read_would_answer(&self) -> bool {
        let state = recover(&self.state);
        !state.bytes.is_empty() || state.writer_gone
    }

    /// Writes what fits, never blocking.
    ///
    /// Returns the accepted byte count and whether this write took the ring
    /// from empty to non-empty — the one edge a waker fires on.
    ///
    /// # Errors
    /// `BrokenPipe` when the reading end has been dropped; `WouldBlock` when
    /// the ring is full. Never `Ok(0)` for a non-empty `buf` on a live ring:
    /// a zero-byte answer is what [`super::super::outbound::OutboundWriter`]
    /// reads as a lost peer, so the honest full-ring answer is `WouldBlock`.
    fn write(&self, buf: &[u8]) -> io::Result<(usize, bool)> {
        let mut state = recover(&self.state);
        if state.reader_gone {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "loopback peer end was dropped",
            ));
        }
        if buf.is_empty() {
            return Ok((0, false));
        }
        let free = self.capacity.saturating_sub(state.bytes.len());
        if free == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "loopback ring is full",
            ));
        }
        let accepted = free.min(buf.len());
        let was_empty = state.bytes.is_empty();
        state.bytes.extend(buf.get(..accepted).unwrap_or(buf));
        drop(state);
        self.changed.notify_all();
        Ok((accepted, was_empty))
    }

    /// Reads what is there, never blocking.
    ///
    /// # Errors
    /// `WouldBlock` when the ring is empty and its writer is still alive. An
    /// empty ring whose writer is gone is `Ok(0)` — end of file, exactly as a
    /// hung-up socket reads.
    fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        // The guard is confined to this block so the lock is never held while
        // an error value is being built.
        let (taken, writer_gone) = {
            let mut state = recover(&self.state);
            (take_from(&mut state.bytes, buf), state.writer_gone)
        };
        if taken > 0 || buf.is_empty() || writer_gone {
            return Ok(taken);
        }
        Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "loopback ring is empty",
        ))
    }

    /// Reads what is there, parking on the condvar until bytes arrive, the
    /// writer goes away, or `timeout` elapses.
    ///
    /// `None` parks without a deadline. A `timeout` so large that the deadline
    /// is not representable as an `Instant` is treated as `None`, which is the
    /// honest reading of a deadline beyond the end of time.
    ///
    /// # Errors
    /// `TimedOut` when the window closes with no bytes and a live writer.
    fn read_until(&self, buf: &mut [u8], timeout: Option<Duration>) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let deadline = timeout.and_then(|window| Instant::now().checked_add(window));
        let mut state = recover(&self.state);
        loop {
            let taken = take_from(&mut state.bytes, buf);
            if taken > 0 {
                return Ok(taken);
            }
            if state.writer_gone {
                return Ok(0);
            }
            let Some(deadline) = deadline else {
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner);
                continue;
            };
            // Re-derived every pass so a spurious wake cannot extend the window.
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "loopback read deadline expired",
                ));
            };
            let (next, _) = self
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(PoisonError::into_inner);
            state = next;
        }
    }

    /// Marks the writing end gone: the reader drains, then sees end of file.
    fn close_writer(&self) {
        recover(&self.state).writer_gone = true;
        self.changed.notify_all();
    }

    /// Marks the reading end gone: the writer sees `BrokenPipe`.
    fn close_reader(&self) {
        recover(&self.state).reader_gone = true;
        self.changed.notify_all();
    }
}

/// Moves up to `buf.len()` bytes out of `bytes`, in order, returning how many.
fn take_from(bytes: &mut VecDeque<u8>, buf: &mut [u8]) -> usize {
    let taken = bytes.len().min(buf.len());
    for (slot, byte) in buf.iter_mut().zip(bytes.drain(..taken)) {
        *slot = byte;
    }
    taken
}

/// The server end's registered wake callback (§3's no-polling answer).
///
/// Held behind an `Arc` shared with the client end so the client's writes and
/// the client's drop can fire it, and behind a `Mutex` so it can be registered
/// after the duplex is built — the callback names a connection that does not
/// exist until the process it belongs to has been spawned.
#[derive(Default)]
struct WakerSlot {
    /// `None` until a reader registers. An unregistered slot is not an error:
    /// a duplex with no parked reader has nothing to tell.
    callback: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl WakerSlot {
    fn set(&self, callback: Arc<dyn Fn() + Send + Sync>) {
        *recover(&self.callback) = Some(callback);
    }

    /// Invokes the callback with NO lock held — neither the ring's nor the
    /// slot's. A waker that writes back into the duplex, or that re-registers
    /// itself, must not be able to deadlock the end that woke it.
    fn fire(&self) {
        let callback = recover(&self.callback).as_ref().map(Arc::clone);
        if let Some(callback) = callback {
            callback();
        }
    }
}

impl std::fmt::Debug for WakerSlot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WakerSlot")
            .field("registered", &recover(&self.callback).is_some())
            .finish()
    }
}

/// The constructor for a loopback duplex.
///
/// Deliberately uninhabited: it names the constructor and owns its
/// documentation, and there is no duplex value to hold — a duplex IS its two
/// ends, and each end is owned by the side that uses it.
#[derive(Debug)]
pub enum LoopbackDuplex {}

impl LoopbackDuplex {
    /// Builds a duplex with `capacity_per_ring` bytes in each direction and
    /// returns its two ends.
    ///
    /// The capacity is per ring, not shared: a full client-to-server ring does
    /// not starve the server-to-client direction, which is the property that
    /// lets a server drain a reply while its inbound side is backed up. The
    /// capacity is floored at [`MIN_RING_CAPACITY_BYTES`] so a caller's zero
    /// cannot build a duplex that can never move a byte.
    #[must_use]
    pub fn bounded(capacity_per_ring: usize) -> (LoopbackClientEnd, LoopbackServerEnd) {
        let capacity = capacity_per_ring.max(MIN_RING_CAPACITY_BYTES);
        let to_server = Arc::new(Ring::new(capacity));
        let to_client = Arc::new(Ring::new(capacity));
        let waker = Arc::new(WakerSlot::default());
        let client = LoopbackClientEnd {
            to_server: Arc::clone(&to_server),
            from_server: Arc::clone(&to_client),
            server_waker: Arc::clone(&waker),
        };
        let server = LoopbackServerEnd {
            from_client: to_server,
            to_client,
            waker,
        };
        (client, server)
    }
}

/// The client half of a loopback duplex: the end an embedded caller holds.
///
/// Reads BLOCK (with an optional deadline), mirroring the SDK's socket
/// connection, whose reads are bounded by `set_read_timeout` and whose caller
/// reads both `WouldBlock` and `TimedOut` as the same "no bytes in the window"
/// answer. Writes never block: a full ring answers `WouldBlock`, exactly as a
/// full socket send buffer does.
#[derive(Debug)]
pub struct LoopbackClientEnd {
    /// Bytes this end writes; the server end reads them.
    to_server: Arc<Ring>,
    /// Bytes this end reads; the server end wrote them.
    from_server: Arc<Ring>,
    /// The server's wake callback, fired on the inbound write edge and on this
    /// end's drop.
    server_waker: Arc<WakerSlot>,
}

impl LoopbackClientEnd {
    /// Bytes already waiting for this end, consuming none of them.
    #[must_use]
    pub fn readable_bytes(&self) -> usize {
        self.from_server.readable_bytes()
    }

    /// Reads into `buf`, parking until bytes arrive, the server end is
    /// dropped, or `timeout` elapses. `None` parks without a deadline.
    ///
    /// `Ok(0)` is end of file: the server end is gone and its ring is drained.
    ///
    /// # Errors
    /// `TimedOut` when the window closes with no bytes and a live server end.
    /// The SDK's socket path reads `WouldBlock` and `TimedOut` identically, so
    /// either satisfies its contract; `TimedOut` is the one that names what
    /// happened.
    pub fn read_timeout(&mut self, buf: &mut [u8], timeout: Option<Duration>) -> io::Result<usize> {
        self.from_server.read_until(buf, timeout)
    }
}

impl Read for LoopbackClientEnd {
    /// Blocks without a deadline. Callers that need one use
    /// [`LoopbackClientEnd::read_timeout`].
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.from_server.read_until(buf, None)
    }
}

impl Write for LoopbackClientEnd {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let (written, opened_the_ring) = self.to_server.write(buf)?;
        if opened_the_ring {
            self.server_waker.fire();
        }
        Ok(written)
    }

    /// Nothing buffers behind the ring, so a flush has nothing to push.
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for LoopbackClientEnd {
    fn drop(&mut self) {
        self.to_server.close_writer();
        self.from_server.close_reader();
        // A parked server learns about the hangup the same way it learns about
        // bytes: it is told. Without this a connection whose client vanished
        // would sit parked until some unrelated wake happened past it.
        self.server_waker.fire();
    }
}

/// The server half of a loopback duplex: the end a connection process holds.
///
/// Both halves are non-blocking, which is what makes this end a drop-in for
/// the non-blocking `TcpStream` a socket connection owns: reads answer
/// `WouldBlock` rather than parking, and the end implements
/// [`InboundPending`] so the pre-park probe can ask it the one transport
/// question that probe asks. As an [`io::Write`] it is usable directly as the
/// `&mut dyn Write` sink [`super::super::outbound::OutboundWriter::drain`]
/// takes.
#[derive(Debug)]
pub struct LoopbackServerEnd {
    /// Bytes this end reads; the client end wrote them.
    from_client: Arc<Ring>,
    /// Bytes this end writes; the client end reads them.
    to_client: Arc<Ring>,
    /// This end's wake callback, fired by the client's writes and drop.
    waker: Arc<WakerSlot>,
}

impl LoopbackServerEnd {
    /// Registers the callback the client end fires when this end's inbound
    /// ring goes from empty to non-empty, and when the client end is dropped.
    ///
    /// Registration is separate from construction because the callback names
    /// the connection process this end belongs to, and that process does not
    /// exist until after the duplex has been built and handed to it. Setting a
    /// second waker replaces the first; a duplex whose waker is never set
    /// simply tells nobody.
    pub fn set_waker(&self, waker: Box<dyn Fn() + Send + Sync>) {
        self.waker.set(Arc::from(waker));
    }

    /// Bytes already waiting for this end, consuming none of them.
    #[must_use]
    pub fn readable_bytes(&self) -> usize {
        self.from_client.readable_bytes()
    }
}

impl Read for LoopbackServerEnd {
    /// Never blocks: an empty ring with a live client answers `WouldBlock`, a
    /// drained ring with a dropped client answers `Ok(0)`.
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.from_client.read(buf)
    }
}

impl Write for LoopbackServerEnd {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // No waker on this direction: the client's blocking read parks on the
        // ring's own condvar, which `Ring::write` has already signalled.
        let (written, _) = self.to_client.write(buf)?;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl InboundPending for LoopbackServerEnd {
    /// Answers from the ring's own bytes, and never fails.
    ///
    /// A hung-up client reports PENDING, not idle — the same answer a socket
    /// gives, whose `peek` on a closed connection returns `Ok(0)` and lands on
    /// the probe's `Ok(_) => Ok(true)` arm. The pending work in that case is
    /// the end of file itself: a connection that parked on a dead peer would
    /// never learn it was dead.
    fn inbound_pending(&self) -> Result<bool, ServerError> {
        Ok(self.from_client.read_would_answer())
    }
}

impl Drop for LoopbackServerEnd {
    fn drop(&mut self) {
        self.to_client.close_writer();
        self.from_client.close_reader();
    }
}
