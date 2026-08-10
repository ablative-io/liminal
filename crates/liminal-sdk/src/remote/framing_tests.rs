use super::*;
use std::net::TcpListener;

/// `EAGAIN` as this platform numbers it — 35 on macOS and the BSDs, 11 on Linux.
/// A socket carrying `SO_RCVTIMEO` reports a closed window with it, and
/// `io::Error` maps either number onto [`io::ErrorKind::WouldBlock`], so a fake
/// stream built on it exercises the same arm the kernel does.
#[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
const EAGAIN: i32 = 35;
#[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "freebsd")))]
const EAGAIN: i32 = 11;

/// A stall longer than [`IO_TIMEOUT`], so a real socket's receive window closes
/// at least once before the reply lands. Kept just past the window: this test
/// spends it twice in wall-clock time.
const STALL_PAST_THE_WINDOW: Duration = Duration::from_millis(5_500);

/// Simulated read-window duration for the in-memory stalling stream. Long enough
/// that a bounded budget is spent in a handful of ticks rather than a hot spin,
/// short enough to keep the pin sub-second.
const FAKE_WINDOW: Duration = Duration::from_millis(20);

/// A [`FrameStream`] that reports a closed receive window `stalls` times before
/// handing over `ready`, exactly as `SO_RCVTIMEO` does on a slow-but-answering
/// peer. Writes are swallowed; the handshake's `Connect` has no reader here.
struct StallingStream {
    /// Read windows still to close with `EAGAIN` before any byte is produced.
    stalls: usize,
    /// Bytes handed over once the stalls are spent, drained front-first.
    ready: Vec<u8>,
    /// Set once `stalls` reaches zero with `ready` empty: an endless stall.
    endless: bool,
}

impl StallingStream {
    /// A stream that closes its window `stalls` times, then delivers `ready`.
    fn answering(stalls: usize, ready: Vec<u8>) -> Self {
        Self {
            stalls,
            ready,
            endless: false,
        }
    }
}

impl FrameStream for StallingStream {
    fn read_bytes(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.endless || self.stalls > 0 {
            self.stalls = self.stalls.saturating_sub(1);
            std::thread::sleep(FAKE_WINDOW);
            return Err(io::Error::from_raw_os_error(EAGAIN));
        }
        let take = self.ready.len().min(buf.len());
        let Some(target) = buf.get_mut(..take) else {
            return Ok(0);
        };
        target.copy_from_slice(self.ready.drain(..take).as_slice());
        Ok(take)
    }

    fn write_all_bytes(&mut self, _bytes: &[u8]) -> io::Result<()> {
        Ok(())
    }

    fn flush_bytes(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn set_read_deadline(&mut self, _timeout: Duration) -> io::Result<()> {
        Ok(())
    }
}

/// The `ConnectAck` the fixtures answer a handshake with.
const fn connect_ack() -> Frame {
    Frame::ConnectAck {
        flags: 0,
        selected_version: CLIENT_MAX_VERSION,
        capabilities: 0,
    }
}

/// Encodes `frame` to its exact wire bytes.
fn frame_bytes(frame: &Frame) -> Result<Vec<u8>, SdkError> {
    let len = encoded_len(frame).map_err(|error| protocol_error(&error))?;
    let mut bytes = vec![0_u8; len];
    let written = encode(frame, &mut bytes).map_err(|error| protocol_error(&error))?;
    bytes.truncate(written);
    Ok(bytes)
}

/// Binds a loopback listener and returns it with its dialable address.
fn bind_fake_server() -> Result<(TcpListener, String), SdkError> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|source| SdkError::Connection {
        description: format!("failed to bind fake server: {source}"),
    })?;
    let address = listener
        .local_addr()
        .map_err(|source| SdkError::Connection {
            description: format!("failed to read fake server address: {source}"),
        })?
        .to_string();
    Ok((listener, address))
}

/// Blocks reading `socket` until one complete frame decodes, discarding it.
fn read_and_discard_one(socket: &mut TcpStream, buffer: &mut Vec<u8>) -> Result<(), SdkError> {
    loop {
        match decode(buffer) {
            Ok((_, consumed)) => {
                buffer.drain(..consumed);
                return Ok(());
            }
            Err(
                ProtocolError::IncompleteHeader { .. } | ProtocolError::TruncatedPayload { .. },
            ) => {
                let mut chunk = [0_u8; 512];
                let read = socket
                    .read(&mut chunk)
                    .map_err(|source| SdkError::Connection {
                        description: format!("fake server read failed: {source}"),
                    })?;
                if read == 0 {
                    return Err(SdkError::Connection {
                        description: "fake server: client closed before a full frame".to_string(),
                    });
                }
                buffer.extend_from_slice(chunk.get(..read).unwrap_or(&[]));
            }
            Err(error) => return Err(protocol_error(&error)),
        }
    }
}

/// RED PIN (p0-61, direction (a)) — a closed receive window is not a lost peer.
///
/// `fill_buffer` maps every `read_bytes` error to a fatal `SdkError::Connection`
/// through one `?`. A socket carrying `SO_RCVTIMEO = IO_TIMEOUT` answers a
/// closed window with `EAGAIN`, so a server reply slower than that window is
/// read as a dead connection and the socket is abandoned — with the answer
/// possibly still in flight. That is the 2026-08-10 outage's client-side
/// mechanism, and server admission being O(N) in conversation history puts real
/// sessions past the window.
///
/// Against the pre-fix client this fails with `SdkError::Connection {
/// description: "failed to read frame from server: Resource temporarily
/// unavailable (os error 35)" }`.
#[test]
fn a_closed_receive_window_is_a_wait_not_a_lost_connection() -> Result<(), SdkError> {
    let stream = StallingStream::answering(3, frame_bytes(&connect_ack())?);

    let connection = Connection::established(stream, &[])?;
    drop(connection);
    Ok(())
}

/// RED PIN (p0-61, direction (b)) — the field shape over a real socket.
///
/// A real `TcpStream` under the real `IO_TIMEOUT`, against a server that is slow
/// twice: once before writing a byte (the pre-frame stall) and once with a frame
/// half-written (the mid-frame stall). Both are the same defect and only the
/// second can strand a partially-read frame in the buffer.
#[test]
fn a_real_socket_survives_a_reply_slower_than_its_receive_window() -> Result<(), SdkError> {
    let (listener, address) = bind_fake_server()?;
    let ack = frame_bytes(&connect_ack())?;
    let split = ack.len() / 2;
    let server = std::thread::spawn(move || -> Result<(), SdkError> {
        let (mut socket, _peer) = listener.accept().map_err(|source| SdkError::Connection {
            description: format!("fake server accept failed: {source}"),
        })?;
        let mut buffer = Vec::new();

        // Pre-frame stall: the handshake reply arrives after the window closes.
        read_and_discard_one(&mut socket, &mut buffer)?;
        std::thread::sleep(STALL_PAST_THE_WINDOW);
        socket
            .write_all(&ack)
            .map_err(|source| SdkError::Connection {
                description: format!("fake server write failed: {source}"),
            })?;

        // Mid-frame stall: the second reply is torn across a closed window.
        read_and_discard_one(&mut socket, &mut buffer)?;
        socket
            .write_all(ack.get(..split).unwrap_or(&[]))
            .map_err(|source| SdkError::Connection {
                description: format!("fake server head write failed: {source}"),
            })?;
        socket.flush().map_err(|source| SdkError::Connection {
            description: format!("fake server flush failed: {source}"),
        })?;
        std::thread::sleep(STALL_PAST_THE_WINDOW);
        socket
            .write_all(ack.get(split..).unwrap_or(&[]))
            .map_err(|source| SdkError::Connection {
                description: format!("fake server tail write failed: {source}"),
            })?;

        let mut scratch = [0_u8; 512];
        while socket.read(&mut scratch).unwrap_or(0) > 0 {}
        Ok(())
    });

    let mut connection = Connection::connect_with_auth(&address, &[])?;
    let replied = connection.round_trip(&connect_ack())?;
    assert!(
        matches!(replied, Frame::ConnectAck { .. }),
        "expected the mid-frame-stalled reply to complete, received {replied:?}"
    );
    drop(connection);
    server.join().ok();
    Ok(())
}
