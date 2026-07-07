//! Per-connection outbound byte buffer with cooperative, partial-write draining.
//!
//! Every server-originated frame (acks, errors, `Push`, `Disconnect`, `Pong`,
//! `Deliver`, ...) is encoded into this bounded buffer and drained on the
//! connection's scheduler slice. This replaces the previous direct `write_all`
//! on the connection's NON-BLOCKING socket (ledger G4): `write_all` on a
//! non-blocking stream fails the instant the kernel send buffer fills, which a
//! frame larger than the socket buffer (~64 KiB) hits mid-write, corrupting or
//! dropping the stream. Here a [`std::io::Write::write`] loop tracks partial
//! progress and a `WouldBlock` mid-frame simply leaves the residue queued for the
//! next slice — the frame streams out across as many slices as the peer's read
//! rate requires, so an arbitrarily large frame is delivered intact.
//!
//! The buffer is bounded (default [`DEFAULT_OUTBOUND_CAPACITY`]): an enqueue that
//! would exceed the cap, or an unrecoverable write error, is a fatal condition —
//! a desynced stream must never survive, so the caller tears the connection down.

use std::collections::VecDeque;
use std::io::Write;
use std::net::TcpStream;

use liminal::protocol::{Frame, ProtocolError, encode, encoded_len};

/// Default cap on a single connection's outbound byte buffer (4 MiB).
pub(super) const DEFAULT_OUTBOUND_CAPACITY: usize = 4 * 1024 * 1024;

/// A fatal condition on the outbound path: either the bounded buffer overflowed
/// or the socket reported an unrecoverable write error. Both tear the connection
/// down; a partially written, desynced stream is never allowed to survive.
#[derive(Debug)]
pub(super) enum OutboundError {
    /// Enqueuing the frame would push the buffer past its capacity.
    Overflow {
        /// Bytes already queued and not yet drained.
        queued: usize,
        /// Bytes the rejected frame would have added.
        needed: usize,
        /// The buffer's fixed capacity.
        capacity: usize,
    },
    /// The frame could not be encoded into wire bytes.
    Encode(ProtocolError),
    /// The socket reported an unrecoverable write error while draining.
    Write(std::io::Error),
}

impl std::fmt::Display for OutboundError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Overflow {
                queued,
                needed,
                capacity,
            } => write!(
                formatter,
                "outbound buffer overflow: {queued} queued + {needed} needed exceeds \
                 capacity {capacity}"
            ),
            Self::Encode(error) => write!(formatter, "outbound frame encode failed: {error}"),
            Self::Write(error) => write!(formatter, "outbound socket write failed: {error}"),
        }
    }
}

impl std::error::Error for OutboundError {}

/// A bounded, per-connection outbound byte queue drained with partial-write
/// tracking on the connection's scheduler slice.
#[derive(Debug)]
pub(super) struct OutboundWriter {
    buffer: VecDeque<u8>,
    capacity: usize,
}

impl OutboundWriter {
    /// Creates an outbound writer with the default 4 MiB capacity.
    pub(super) const fn new() -> Self {
        Self::with_capacity(DEFAULT_OUTBOUND_CAPACITY)
    }

    /// Creates an outbound writer with an explicit capacity (used by tests to
    /// force overflow with a small cap).
    pub(super) const fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: VecDeque::new(),
            capacity,
        }
    }

    /// Encodes `frame` into the buffer, rejecting it when the buffer would exceed
    /// its capacity.
    ///
    /// # Errors
    /// Returns [`OutboundError::Overflow`] when the frame does not fit within the
    /// remaining capacity and [`OutboundError::Encode`] when the frame cannot be
    /// encoded. Either is fatal for the connection.
    pub(super) fn enqueue_frame(&mut self, frame: &Frame) -> Result<(), OutboundError> {
        let needed = encoded_len(frame).map_err(OutboundError::Encode)?;
        let queued = self.buffer.len();
        let projected = queued.checked_add(needed).ok_or(OutboundError::Overflow {
            queued,
            needed,
            capacity: self.capacity,
        })?;
        if projected > self.capacity {
            return Err(OutboundError::Overflow {
                queued,
                needed,
                capacity: self.capacity,
            });
        }
        let mut bytes = vec![0_u8; needed];
        let written = encode(frame, &mut bytes).map_err(OutboundError::Encode)?;
        // `written` never exceeds `needed` (encode fills the sized buffer), so
        // truncating to the reported count is a safe no-op in the normal case and
        // a defensive guard against a short write otherwise.
        bytes.truncate(written);
        self.buffer.extend(bytes);
        Ok(())
    }

    /// Drains as many queued bytes to `stream` as it will accept without blocking.
    ///
    /// Writes proceed from the front of the queue with a `write()` loop that
    /// tracks partial progress; a `WouldBlock` (the non-blocking socket's send
    /// buffer is full) returns `Ok(())` with the residue left queued for the next
    /// slice. `Interrupted` retries. Any other error — or a zero-length write,
    /// which means the peer is gone — is fatal.
    ///
    /// # Errors
    /// Returns [`OutboundError::Write`] on an unrecoverable socket write error.
    pub(super) fn drain(&mut self, stream: &mut TcpStream) -> Result<(), OutboundError> {
        while !self.buffer.is_empty() {
            // `as_slices().0` is the front contiguous run; a wrapped queue drains in
            // two passes across loop iterations, so no reallocation is forced.
            let front = self.buffer.as_slices().0;
            match stream.write(front) {
                Ok(0) => {
                    return Err(OutboundError::Write(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "connection peer accepted zero bytes",
                    )));
                }
                Ok(written) => {
                    self.buffer.drain(..written);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(OutboundError::Write(error)),
            }
        }
        Ok(())
    }

    /// Number of bytes currently queued and not yet drained.
    #[cfg(test)]
    pub(super) fn queued_len(&self) -> usize {
        self.buffer.len()
    }

    /// Removes and returns all queued bytes, so a test can decode what would have
    /// been written without going through a socket.
    #[cfg(test)]
    pub(super) fn take_bytes(&mut self) -> Vec<u8> {
        self.buffer.drain(..).collect()
    }
}

#[cfg(test)]
#[path = "outbound_tests.rs"]
mod tests;
