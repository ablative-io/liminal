//! Per-connection outbound queue for the WebSocket sibling (R1.2/R1.3).
//!
//! EVERY server-originated liminal frame is canonically encoded ONCE and queued
//! as one discrete message, because the transport contract maps exactly one
//! canonical frame onto exactly one binary WebSocket message — the TCP path's
//! byte-stream [`OutboundWriter`](super::super::outbound::OutboundWriter) cannot
//! be reused verbatim, but its discipline is mirrored precisely: the queue is
//! bounded by the SAME 4 MiB capacity, an enqueue that would exceed it (or an
//! encode fault) is fatal and tears the connection down, and draining tracks
//! partial progress so a message larger than the socket send buffer streams out
//! across as many slices as the peer's read rate requires.
//!
//! Draining hands tungstenite ONE message at a time and flushes it fully before
//! the next, so the library's internal write buffer never holds more than one
//! in-flight message (plus transport-control pong/close echoes) — the queue
//! here remains the single bounded accounting authority.

use std::collections::VecDeque;
use std::net::TcpStream;

use tungstenite::Message;
use tungstenite::protocol::WebSocket;

use liminal::protocol::{Frame, encode, encoded_len};

use super::super::outbound::{DEFAULT_OUTBOUND_CAPACITY, DrainOutcome, OutboundError};

#[cfg(test)]
#[path = "outbound_tests.rs"]
mod tests;

/// A bounded queue of canonically encoded frames, each drained as one binary
/// WebSocket message.
#[derive(Debug)]
pub(in super::super) struct WebSocketOutbound {
    /// Encoded canonical frames awaiting transmission, one message each.
    queue: VecDeque<Vec<u8>>,
    /// Bytes across `queue` plus the in-flight message, counted against
    /// `capacity`.
    queued_bytes: usize,
    /// The fixed capacity in bytes (the spec-inherent single-frame bound).
    capacity: usize,
    /// Length of the message currently handed to tungstenite but not yet fully
    /// flushed. Counted in `queued_bytes` until the flush completes, so the
    /// bound stays honest across partial writes.
    in_flight: Option<usize>,
    /// Whether tungstenite queued transport-control bytes (automatic pong, a
    /// close echo) that still need a flush even when the frame queue is empty.
    transport_flush_pending: bool,
}

impl WebSocketOutbound {
    /// Creates an outbound queue with the shared default 4 MiB capacity.
    pub(in super::super) const fn new() -> Self {
        Self::with_capacity(DEFAULT_OUTBOUND_CAPACITY)
    }

    /// Creates an outbound queue with an explicit capacity (tests force
    /// overflow with a small cap, exactly like the TCP writer's tests).
    pub(in super::super) const fn with_capacity(capacity: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            queued_bytes: 0,
            capacity,
            in_flight: None,
            transport_flush_pending: false,
        }
    }

    /// Encodes `frame` once, canonically, into the queue as one message.
    ///
    /// # Errors
    /// Returns [`OutboundError::Overflow`] when the frame does not fit within
    /// the remaining capacity and [`OutboundError::Encode`] when the frame
    /// cannot be encoded. Either is fatal for the connection.
    pub(in super::super) fn enqueue_frame(&mut self, frame: &Frame) -> Result<(), OutboundError> {
        let needed = encoded_len(frame).map_err(OutboundError::Encode)?;
        let projected = self
            .queued_bytes
            .checked_add(needed)
            .ok_or(OutboundError::Overflow {
                queued: self.queued_bytes,
                needed,
                capacity: self.capacity,
            })?;
        if projected > self.capacity {
            return Err(OutboundError::Overflow {
                queued: self.queued_bytes,
                needed,
                capacity: self.capacity,
            });
        }
        let mut bytes = vec![0_u8; needed];
        let written = encode(frame, &mut bytes).map_err(OutboundError::Encode)?;
        bytes.truncate(written);
        self.queued_bytes = self.queued_bytes.saturating_add(bytes.len());
        self.queue.push_back(bytes);
        Ok(())
    }

    /// The queue's fixed capacity in bytes.
    pub(in super::super) const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Whether `needed` more bytes fit within the remaining capacity.
    pub(in super::super) fn has_room(&self, needed: usize) -> bool {
        self.queued_bytes
            .checked_add(needed)
            .is_some_and(|projected| projected <= self.capacity)
    }

    /// Records that tungstenite queued transport-control bytes (an automatic
    /// pong reply, a close echo, a keepalive ping) that require a later flush.
    pub(in super::super) const fn note_transport_write_pending(&mut self) {
        self.transport_flush_pending = true;
    }

    /// Drains queued messages to `socket`, reporting the shared
    /// [`DrainOutcome`] tri-state with the TCP writer's exact semantics: a
    /// `WouldBlock` with residue still queued (in this queue OR inside
    /// tungstenite) returns [`DrainOutcome::WouldBlockWithResidue`]; an
    /// exhausted `budget` (bytes fully flushed this drain) with residue returns
    /// [`DrainOutcome::Progress`]; an emptied queue returns
    /// [`DrainOutcome::Drained`].
    ///
    /// # Errors
    /// Returns [`OutboundError::Write`] on an unrecoverable transport write
    /// error.
    pub(in super::super) fn drain(
        &mut self,
        socket: &mut WebSocket<TcpStream>,
        budget: Option<usize>,
    ) -> Result<DrainOutcome, OutboundError> {
        let mut written_total: usize = 0;
        loop {
            // Complete the in-flight message (and any transport-control bytes)
            // before handing tungstenite another message: one message at a
            // time keeps the library's internal buffer structurally bounded.
            if self.in_flight.is_some() || self.transport_flush_pending {
                match socket.flush() {
                    Ok(()) => {
                        if let Some(flushed) = self.in_flight.take() {
                            self.queued_bytes = self.queued_bytes.saturating_sub(flushed);
                            written_total = written_total.saturating_add(flushed);
                        }
                        self.transport_flush_pending = false;
                    }
                    Err(error) => return self.map_drain_error(error),
                }
            }
            if self.queue.is_empty() {
                return Ok(DrainOutcome::Drained);
            }
            if let Some(limit) = budget {
                if written_total >= limit {
                    return Ok(DrainOutcome::Progress);
                }
            }
            let Some(message) = self.queue.pop_front() else {
                return Ok(DrainOutcome::Drained);
            };
            self.in_flight = Some(message.len());
            match socket.write(Message::Binary(message.into())) {
                // Queued inside tungstenite; the next loop iteration flushes it.
                Ok(()) => {}
                Err(error) => return self.map_drain_error(error),
            }
        }
    }

    /// Maps a tungstenite transport error to the shared drain result. An I/O
    /// `WouldBlock` leaves residue queued (here or inside tungstenite) for the
    /// next slice; everything else is the fatal typed write error.
    fn map_drain_error(
        &mut self,
        error: tungstenite::Error,
    ) -> Result<DrainOutcome, OutboundError> {
        match error {
            tungstenite::Error::Io(io_error)
                if io_error.kind() == std::io::ErrorKind::WouldBlock
                    || io_error.kind() == std::io::ErrorKind::Interrupted =>
            {
                Ok(DrainOutcome::WouldBlockWithResidue)
            }
            tungstenite::Error::Io(io_error) => Err(OutboundError::Write(io_error)),
            tungstenite::Error::WriteBufferFull(message) => {
                // The message was NOT queued inside tungstenite. Requeue it at
                // the front so ordering is preserved; structurally this can
                // only occur if transport-control bytes are wedged, which the
                // flush-first loop retries next slice.
                if let Message::Binary(bytes) = *message {
                    self.in_flight = None;
                    self.queue.push_front(bytes.to_vec());
                }
                Ok(DrainOutcome::WouldBlockWithResidue)
            }
            other => Err(OutboundError::Write(std::io::Error::other(
                other.to_string(),
            ))),
        }
    }

    /// Number of bytes currently queued (including in-flight) and not yet
    /// flushed.
    #[cfg(test)]
    pub(in super::super) const fn queued_len(&self) -> usize {
        self.queued_bytes
    }

    /// Removes and returns all queued messages, so a test can inspect the
    /// exact canonical bytes each message would carry.
    #[cfg(test)]
    pub(in super::super) fn take_messages(&mut self) -> Vec<Vec<u8>> {
        let messages: Vec<Vec<u8>> = self.queue.drain(..).collect();
        let drained: usize = messages.iter().map(Vec::len).sum();
        self.queued_bytes = self.queued_bytes.saturating_sub(drained);
        messages
    }
}

impl super::super::delivery::DeliverySink for WebSocketOutbound {
    fn capacity(&self) -> usize {
        Self::capacity(self)
    }

    fn has_room(&self, needed: usize) -> bool {
        Self::has_room(self, needed)
    }

    fn enqueue_frame(&mut self, frame: &Frame) -> Result<(), OutboundError> {
        Self::enqueue_frame(self, frame)
    }
}
