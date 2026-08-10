//! The loopback byte stream: a [`LoopbackClientEnd`] wearing the shared
//! framing layer's [`FrameStream`] contract.
//!
//! This file is the whole of what differs between the in-process mount and the
//! socket mount on the client side. The handshake, the partial-frame buffer,
//! the `Deliver` demux, the conversation drain, and every frame this SDK sends
//! or reads are [`super::super::framing::Connection`]'s, unchanged and shared.
//! What is here is four method bodies mapping the duplex's answers onto the
//! socket's, and the mapping is a mapping — not a reinterpretation.

use core::time::Duration;

use std::io;

use liminal_server::server::connection::LoopbackClientEnd;

use super::super::framing::{FrameStream, IO_TIMEOUT};

/// One end of a loopback duplex, carrying the read deadline the socket would
/// have kept inside itself.
///
/// A `TcpStream` stores its read timeout in the kernel, so `set_read_timeout`
/// is a syscall and every later `read` honours it. A duplex end has no such
/// hidden place, so the deadline lives here and is passed into each read. That
/// is the same fact stored in a different drawer; nothing about when a read
/// gives up changes.
#[derive(Debug)]
pub(super) struct LoopbackStream {
    end: LoopbackClientEnd,
    /// The window the next read is bounded by, mirroring the socket's
    /// `SO_RCVTIMEO`. Initialised to the steady-state
    /// [`IO_TIMEOUT`](super::super::framing::IO_TIMEOUT) the socket path sets
    /// at connect, and moved by the framing layer for the conversation drain.
    read_deadline: Duration,
}

impl LoopbackStream {
    /// Wraps a freshly granted client end at the steady-state read deadline.
    pub(super) const fn new(end: LoopbackClientEnd) -> Self {
        Self {
            end,
            read_deadline: IO_TIMEOUT,
        }
    }
}

impl FrameStream for LoopbackStream {
    /// Reads under the current deadline.
    ///
    /// The duplex answers a closed window with `TimedOut` and the socket
    /// answers it with `WouldBlock` or `TimedOut` depending on the platform.
    /// The framing layer's `fill_buffer_once` already treats those two
    /// kinds identically — it has to, because the socket itself is not
    /// consistent about which it gives — so `TimedOut` lands on the same arm a
    /// silent socket lands on. `Ok(0)` is end of file on both.
    fn read_bytes(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.end.read_timeout(buf, Some(self.read_deadline))
    }

    /// Writes every byte, waiting out a full ring under the same
    /// [`IO_TIMEOUT`] the socket path installs as its write timeout.
    ///
    /// The blocking half of the duplex is deliberate. A socket's `write_all`
    /// waits while the kernel send buffer drains and fails only when the write
    /// deadline closes; the duplex's non-blocking `write` answers `WouldBlock`
    /// the instant its ring is full, which `io::Write::write_all` reads as a
    /// hard failure. Using the non-blocking half here would give the in-process
    /// mount a backpressure semantics no other mount has, which is exactly the
    /// divergence class the design forbids (§9 ruling 1).
    ///
    /// A short accept is a partial write, not a failure — the same partial the
    /// socket path absorbs — so the loop advances rather than erroring.
    fn write_all_bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
        let mut remaining = bytes;
        while !remaining.is_empty() {
            let written = self.end.write_timeout(remaining, Some(IO_TIMEOUT))?;
            if written == 0 {
                // A live ring with space never accepts zero, and a zero here
                // would spin this loop forever, so it is reported as the lost
                // peer it can only be.
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "loopback ring accepted no bytes",
                ));
            }
            let Some(rest) = remaining.get(written..) else {
                return Err(io::Error::other(
                    "loopback write reported more bytes than were offered",
                ));
            };
            remaining = rest;
        }
        Ok(())
    }

    /// Nothing buffers behind the ring, so a flush has nothing to push. The
    /// socket's `flush` is likewise a no-op on `TcpStream`.
    fn flush_bytes(&mut self) -> io::Result<()> {
        Ok(())
    }

    /// Moves the read window. Infallible here, where the socket's equivalent is
    /// a syscall that can fail; the framing layer handles the `Result` either
    /// way.
    fn set_read_deadline(&mut self, timeout: Duration) -> io::Result<()> {
        self.read_deadline = timeout;
        Ok(())
    }
}
