use super::*;
use std::net::TcpListener;
use std::time::Instant;

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

    /// A stream whose window closes forever: the peer that never answers.
    const fn silent() -> Self {
        Self {
            stalls: 0,
            ready: Vec::new(),
            endless: true,
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

/// A fixture-level failure, carried in the error type these tests already
/// return so a broken fixture is never mistaken for a broken client.
fn fixture_failure(detail: &str) -> SdkError {
    SdkError::Protocol {
        description: detail.to_string(),
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

/// RED PIN (p0-61, direction (c)) — the bound, and what it is allowed to say.
///
/// Absorbing a closed window must not trade a fatal error for an unbounded
/// hang: a peer that never answers still ends the wait, and the error names the
/// timeout and how long it waited. It must never surface the raw
/// `EAGAIN`/`Resource temporarily unavailable` text, which reads as a broken
/// socket and sent the outage's diagnosis in the wrong direction.
#[test]
fn an_expired_response_deadline_names_the_timeout_and_never_the_errno() -> Result<(), SdkError> {
    const BUDGET: Duration = Duration::from_millis(120);

    let mut connection = Connection {
        stream: StallingStream::silent(),
        buffer: Vec::new(),
        open_conversations: BTreeSet::new(),
    };

    let started = Instant::now();
    let outcome = connection.receive_within(BUDGET);
    let waited = started.elapsed();

    let error = match outcome {
        Ok(frame) => {
            return Err(fixture_failure(&format!(
                "a silent peer must not be waited on forever; a frame decoded: {frame:?}"
            )));
        }
        Err(error) => error,
    };
    assert!(
        waited >= BUDGET,
        "the deadline ended the wait early: waited {waited:?} against a {BUDGET:?} budget"
    );
    let SdkError::Connection { description } = &error else {
        return Err(fixture_failure(&format!(
            "expected a Connection error naming the timeout, received {error:?}"
        )));
    };
    assert!(
        description.contains("timed out") && description.contains("waiting for a server response"),
        "the deadline error does not name a timeout: {description}"
    );
    assert!(
        !description.contains("os error")
            && !description
                .to_lowercase()
                .contains("temporarily unavailable"),
        "the deadline error leaked the raw receive-window errno: {description}"
    );
    Ok(())
}

/// The response deadline is a chosen bound, not the socket's receive window: the
/// two must not be the same number, or absorbing a closed window would buy
/// nothing. Pins the ordering the fix rests on rather than the literal value.
#[test]
fn the_response_deadline_outlives_a_single_receive_window() {
    assert!(
        RESPONSE_DEADLINE > IO_TIMEOUT,
        "the response deadline ({RESPONSE_DEADLINE:?}) must span more than one \
         receive window ({IO_TIMEOUT:?})"
    );
}

/// A stall past one real receive window but well inside a pump caller's own
/// budget: the reply is slow, not absent.
const STALL_PAST_A_PUMP_WINDOW: Duration = Duration::from_secs(8);

/// RED PIN (p0-63, direction (a)) — silence is an outcome, not a failure.
///
/// The pump door must answer a quiet connection with `Ok(None)` inside the
/// budget its caller named. Before this lane the only participant read was
/// `receive_participant`, which waits out [`RESPONSE_DEADLINE`] = 60 s because
/// its caller is owed a reply — correct there, and the reason a drain loop on an
/// idle connection blew a 30 s boot gate in the field.
///
/// The bound asserted is deliberately far below `RESPONSE_DEADLINE` rather than
/// merely below it: a pump read that quietly inherited the reply-owed deadline
/// would still be "under 60 s" on any implementation that returned at all.
#[test]
fn a_quiet_pump_read_reports_silence_within_the_callers_budget() -> Result<(), SdkError> {
    const BUDGET: Duration = Duration::from_millis(120);

    let mut connection = Connection {
        stream: StallingStream::silent(),
        buffer: Vec::new(),
        open_conversations: BTreeSet::new(),
    };

    let started = Instant::now();
    let outcome = connection.receive_optional_within(BUDGET)?;
    let waited = started.elapsed();

    if let Some(frame) = outcome {
        return Err(fixture_failure(&format!(
            "a silent peer produced a frame: {frame:?}"
        )));
    }
    assert!(
        waited >= BUDGET,
        "the pump read reported silence before its budget was spent: waited {waited:?} \
         against a {BUDGET:?} budget"
    );
    assert!(
        waited < RESPONSE_DEADLINE / 10,
        "the pump read waited {waited:?} for a {BUDGET:?} budget — it is inheriting the \
         reply-owed response deadline ({RESPONSE_DEADLINE:?}) instead of its own bound"
    );
    Ok(())
}

/// A zero budget is a poll of what already decoded, and must never reach the
/// socket: `setsockopt` refuses a zero read deadline with `EINVAL`, so an
/// implementation that passed the budget straight down would turn the cheapest
/// possible pump call into a transport error.
#[test]
fn a_zero_budget_polls_without_arming_a_read() -> Result<(), SdkError> {
    let (listener, address) = bind_fake_server()?;
    let ack = frame_bytes(&connect_ack())?;
    let server = std::thread::spawn(move || -> Result<(), SdkError> {
        let (mut socket, _peer) = listener.accept().map_err(|source| SdkError::Connection {
            description: format!("fake server accept failed: {source}"),
        })?;
        let mut buffer = Vec::new();
        // The handshake.
        read_and_discard_one(&mut socket, &mut buffer)?;
        socket
            .write_all(&ack)
            .map_err(|source| SdkError::Connection {
                description: format!("fake server handshake write failed: {source}"),
            })?;
        // The post-poll request, proving the connection still works.
        read_and_discard_one(&mut socket, &mut buffer)?;
        socket
            .write_all(&ack)
            .map_err(|source| SdkError::Connection {
                description: format!("fake server second write failed: {source}"),
            })?;
        let mut scratch = [0_u8; 512];
        while socket.read(&mut scratch).unwrap_or(0) > 0 {}
        Ok(())
    });

    // A real socket, so a zero deadline would reach a real `setsockopt`.
    let mut connection = Connection::connect_with_auth(&address, &[])?;
    let polled = connection.receive_optional_within(Duration::ZERO)?;
    assert!(
        polled.is_none(),
        "a zero-budget poll of an empty buffer produced a frame: {polled:?}"
    );

    // The connection is still usable afterwards: the poll neither armed a
    // window it failed to restore nor consumed anything.
    connection.send(&connect_ack())?;
    let replied = connection.receive_optional_within(Duration::from_secs(10))?;
    assert!(
        matches!(replied, Some(Frame::ConnectAck { .. })),
        "the connection did not survive a zero-budget poll: {replied:?}"
    );
    drop(connection);
    server.join().ok();
    Ok(())
}

/// BOTH-WAYS PROOF (p0-63, direction (c)) — the pump window is a GRAIN, the
/// caller's budget is the BOUND.
///
/// The failure this forecloses is the mirror of the one the lane fixes: making
/// a bounded read return on the first closed receive window would hand every
/// caller a 5 s ceiling, and a caller using this door to await a correlated
/// answer would then get the 2026-08-10 outage back through the new API.
///
/// A real socket under the real [`IO_TIMEOUT`], against a server that answers
/// after [`STALL_PAST_A_PUMP_WINDOW`] — longer than one receive window and
/// longer than [`PARTICIPANT_PUMP_WINDOW`], but well inside the 30 s budget the
/// caller names. The reply must arrive.
#[test]
fn a_pump_read_spends_the_callers_budget_not_one_receive_window() -> Result<(), SdkError> {
    const CALLER_BUDGET: Duration = Duration::from_secs(30);

    assert!(
        STALL_PAST_A_PUMP_WINDOW > IO_TIMEOUT
            && STALL_PAST_A_PUMP_WINDOW > crate::PARTICIPANT_PUMP_WINDOW,
        "the fixture no longer crosses a closed window, so it would pass against a read \
         that gave up on the first one"
    );

    let (listener, address) = bind_fake_server()?;
    let ack = frame_bytes(&connect_ack())?;
    let server = std::thread::spawn(move || -> Result<(), SdkError> {
        let (mut socket, _peer) = listener.accept().map_err(|source| SdkError::Connection {
            description: format!("fake server accept failed: {source}"),
        })?;
        let mut buffer = Vec::new();
        // The handshake, answered promptly.
        read_and_discard_one(&mut socket, &mut buffer)?;
        socket
            .write_all(&ack)
            .map_err(|source| SdkError::Connection {
                description: format!("fake server handshake write failed: {source}"),
            })?;
        // The slow reply: several closed receive windows, then the frame.
        read_and_discard_one(&mut socket, &mut buffer)?;
        std::thread::sleep(STALL_PAST_A_PUMP_WINDOW);
        socket
            .write_all(&ack)
            .map_err(|source| SdkError::Connection {
                description: format!("fake server slow write failed: {source}"),
            })?;
        let mut scratch = [0_u8; 512];
        while socket.read(&mut scratch).unwrap_or(0) > 0 {}
        Ok(())
    });

    let mut connection = Connection::connect_with_auth(&address, &[])?;
    connection.send(&connect_ack())?;
    let started = Instant::now();
    let replied = connection.receive_optional_within(CALLER_BUDGET)?;
    let waited = started.elapsed();

    assert!(
        matches!(replied, Some(Frame::ConnectAck { .. })),
        "a reply slower than one receive window was reported as silence after {waited:?}: \
         {replied:?} — the bounded read is ending on the window instead of the budget"
    );
    assert!(
        waited >= IO_TIMEOUT,
        "the fixture did not actually cross a closed receive window: waited {waited:?}"
    );
    drop(connection);
    server.join().ok();
    Ok(())
}

/// The pump window must sit strictly below the reply-owed deadline, or the two
/// doors would be one door and the lane would have changed nothing. Pins the
/// ordering the split rests on rather than either literal value.
#[test]
fn the_pump_window_is_shorter_than_the_reply_owed_deadline() {
    assert!(
        crate::PARTICIPANT_PUMP_WINDOW < RESPONSE_DEADLINE,
        "the pump window ({:?}) must be shorter than the reply-owed response deadline \
         ({RESPONSE_DEADLINE:?})",
        crate::PARTICIPANT_PUMP_WINDOW
    );
}
