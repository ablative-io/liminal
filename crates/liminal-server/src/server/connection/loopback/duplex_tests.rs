//! §3 duplex pins: backpressure, close semantics, the wake edge, the client's
//! bounded blocking read, the pre-park probe's answer, and a cross-thread
//! round-trip larger than the ring.
//!
//! These are transport-level pins, deliberately socket-free and process-free.
//! The record-path parity they exist to protect — byte-identical frames,
//! byte-identical record outcomes across mounts — is the discriminating test of
//! build step 5, and cannot be written until the connection process and the SDK
//! transport land in steps 3 and 4.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::io::{ErrorKind, Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use liminal::protocol::{CausalContext, Frame, MessageEnvelope, SchemaId, encode, encoded_len};

use super::super::process::InboundPending;
use super::duplex::LoopbackServerEnd;
use super::{LoopbackClientEnd, LoopbackDuplex};

/// A waker that counts its invocations, so a pin can assert an EDGE rather than
/// a mere "something fired".
fn counting_waker(server: &LoopbackServerEnd) -> Arc<AtomicUsize> {
    let count = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&count);
    server.set_waker(Box::new(move || {
        counter.fetch_add(1, Ordering::SeqCst);
    }));
    count
}

/// One canonical frame, so the drain-sink pin can compare the ring's bytes
/// against the frame's exact wire image.
fn deliver_frame(payload: Vec<u8>) -> Frame {
    Frame::Deliver {
        flags: 0,
        stream_id: 1,
        delivery_seq: 1,
        envelope: MessageEnvelope::new(
            SchemaId::new([7; SchemaId::WIRE_LEN]),
            CausalContext::independent(),
            payload,
        ),
    }
}

/// Drains everything readable on a non-blocking end into a vector.
fn drain_nonblocking(reader: &mut impl Read) -> std::io::Result<Vec<u8>> {
    let mut seen = Vec::new();
    let mut chunk = [0_u8; 64];
    loop {
        match reader.read(&mut chunk) {
            Ok(0) => return Ok(seen),
            Ok(read) => seen.extend_from_slice(chunk.get(..read).unwrap_or(&[])),
            Err(error) if error.kind() == ErrorKind::WouldBlock => return Ok(seen),
            Err(error) => return Err(error),
        }
    }
}

#[test]
fn a_full_ring_answers_would_block_and_never_zero() {
    let (mut client, server) = LoopbackDuplex::bounded(4);
    assert_eq!(client.write(&[1, 2, 3, 4]).expect("first write fits"), 4);

    let error = client
        .write(&[5])
        .expect_err("a full ring must refuse, not accept zero");
    assert_eq!(error.kind(), ErrorKind::WouldBlock);
    assert_eq!(server.readable_bytes(), 4);
}

#[test]
fn a_nearly_full_ring_accepts_a_partial_write_without_desync() {
    let (mut client, mut server) = LoopbackDuplex::bounded(8);
    let payload: Vec<u8> = (0..20_u8).collect();

    // Three passes, each bounded by what the ring can hold, with the reader
    // draining between them: the byte SEQUENCE across the boundary is what a
    // partial write must not disturb.
    let mut sent = 0_usize;
    let mut received = Vec::new();
    while sent < payload.len() {
        let written = client
            .write(payload.get(sent..).unwrap_or(&[]))
            .expect("a ring with room accepts a partial write");
        assert!(written > 0, "a live ring never accepts zero bytes");
        assert!(written <= 8, "a bounded ring never accepts past its bound");
        sent += written;
        received.extend_from_slice(&drain_nonblocking(&mut server).expect("reads succeed"));
    }

    assert_eq!(received, payload);
}

#[test]
fn reads_drain_to_end_of_file_after_the_peer_end_is_dropped() {
    let (mut client, mut server) = LoopbackDuplex::bounded(16);
    assert_eq!(client.write(&[7, 8, 9]).expect("write fits"), 3);
    drop(client);

    // The bytes already in flight survive the drop; only a DRAINED ring is EOF.
    let mut chunk = [0_u8; 8];
    assert_eq!(server.read(&mut chunk).expect("queued bytes still read"), 3);
    assert_eq!(chunk.get(..3), Some([7, 8, 9].as_slice()));
    assert_eq!(server.read(&mut chunk).expect("drained ring is EOF"), 0);
    assert_eq!(server.read(&mut chunk).expect("EOF is stable"), 0);
}

#[test]
fn a_write_to_a_dropped_peer_reports_broken_pipe() {
    let (client, mut server) = LoopbackDuplex::bounded(16);
    drop(client);

    let error = server
        .write(&[1])
        .expect_err("a dropped reader breaks the pipe");
    assert_eq!(error.kind(), ErrorKind::BrokenPipe);

    let (mut client, server) = LoopbackDuplex::bounded(16);
    drop(server);
    let error = client
        .write(&[1])
        .expect_err("the mirror direction breaks too");
    assert_eq!(error.kind(), ErrorKind::BrokenPipe);
}

#[test]
fn the_waker_fires_once_per_empty_to_nonempty_transition() {
    let (mut client, mut server) = LoopbackDuplex::bounded(32);
    let wakes = counting_waker(&server);

    assert_eq!(client.write(&[1, 2]).expect("first write fits"), 2);
    assert_eq!(
        wakes.load(Ordering::SeqCst),
        1,
        "empty to non-empty is one wake"
    );

    assert_eq!(client.write(&[3, 4]).expect("second write fits"), 2);
    assert_eq!(client.write(&[5, 6]).expect("third write fits"), 2);
    assert_eq!(
        wakes.load(Ordering::SeqCst),
        1,
        "a reader already told is not told again"
    );

    assert_eq!(
        drain_nonblocking(&mut server).expect("reads succeed"),
        vec![1, 2, 3, 4, 5, 6]
    );
    assert_eq!(client.write(&[7]).expect("write after drain fits"), 1);
    assert_eq!(
        wakes.load(Ordering::SeqCst),
        2,
        "the ring emptied, so the next write is a new edge"
    );
}

#[test]
fn the_waker_fires_when_the_client_end_is_dropped() {
    let (client, server) = LoopbackDuplex::bounded(32);
    let wakes = counting_waker(&server);

    drop(client);

    assert_eq!(
        wakes.load(Ordering::SeqCst),
        1,
        "a parked server must be told about a hangup"
    );
}

#[test]
fn a_client_read_timeout_expires_without_bytes() {
    let (mut client, _server) = LoopbackDuplex::bounded(16);
    let mut chunk = [0_u8; 8];

    let started = Instant::now();
    let error = client
        .read_timeout(&mut chunk, Some(Duration::from_millis(40)))
        .expect_err("a silent window expires");
    assert_eq!(error.kind(), ErrorKind::TimedOut);
    assert!(
        started.elapsed() >= Duration::from_millis(40),
        "the deadline is waited out, not short-circuited"
    );
}

#[test]
fn a_client_read_timeout_returns_bytes_that_arrive_inside_the_window() {
    let (mut client, mut server) = LoopbackDuplex::bounded(16);
    let writer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(10));
        assert_eq!(server.write(&[42, 43]).expect("write fits"), 2);
        server
    });

    let mut chunk = [0_u8; 8];
    let read = client
        .read_timeout(&mut chunk, Some(Duration::from_secs(5)))
        .expect("bytes arrive inside the window");

    assert_eq!(read, 2);
    assert_eq!(chunk.get(..2), Some([42, 43].as_slice()));
    drop(writer.join().expect("the writer thread finishes"));
}

#[test]
fn inbound_pending_answers_bytes_silence_and_hangup() {
    let (mut client, mut server) = LoopbackDuplex::bounded(16);
    assert!(
        !server.inbound_pending().expect("the probe never errors"),
        "an empty ring with a live client is silence"
    );

    assert_eq!(client.write(&[1, 2]).expect("write fits"), 2);
    assert!(
        server.inbound_pending().expect("the probe never errors"),
        "queued bytes are pending"
    );

    assert_eq!(
        drain_nonblocking(&mut server).expect("reads succeed"),
        vec![1, 2]
    );
    assert!(
        !server.inbound_pending().expect("the probe never errors"),
        "a drained ring with a live client is silence again"
    );

    drop(client);
    assert!(
        server.inbound_pending().expect("the probe never errors"),
        "a hangup is pending work, exactly as a peeked-closed socket is"
    );
}

#[test]
fn the_server_end_serves_as_the_outbound_drain_sink() {
    use super::super::outbound::{DrainOutcome, OutboundWriter};

    // The whole point of step 1's `&mut dyn Write` sink: the server end drops
    // into `OutboundWriter::drain` with no third parallel writer, and the bytes
    // that come out the client side are the frame's exact wire image.
    let frame = deliver_frame(vec![9, 8, 7, 6]);
    let mut expected = vec![0_u8; encoded_len(&frame).expect("the frame sizes")];
    let written = encode(&frame, &mut expected).expect("the frame encodes");
    assert_eq!(written, expected.len());

    let (mut client, mut server) = LoopbackDuplex::bounded(4096);
    let mut outbound = OutboundWriter::new();
    outbound.enqueue_frame(&frame).expect("the frame fits");

    let sink: &mut dyn Write = &mut server;
    let outcome = outbound
        .drain(sink, None)
        .expect("the ring accepts the drain");

    assert!(matches!(outcome, DrainOutcome::Drained));
    let mut seen = Vec::new();
    let mut chunk = [0_u8; 64];
    while seen.len() < expected.len() {
        let read = client
            .read_timeout(&mut chunk, Some(Duration::from_secs(5)))
            .expect("the drained bytes arrive");
        assert!(read > 0, "the sink must not have closed mid-frame");
        seen.extend_from_slice(chunk.get(..read).unwrap_or(&[]));
    }
    assert_eq!(seen, expected);
}

#[test]
fn a_zero_capacity_request_is_floored_so_the_ring_can_progress() {
    let (mut client, mut server) = LoopbackDuplex::bounded(0);

    assert_eq!(
        client
            .write(&[5, 6, 7])
            .expect("the floored ring accepts a byte"),
        1
    );
    assert_eq!(
        drain_nonblocking(&mut server).expect("reads succeed"),
        vec![5]
    );
}

#[test]
fn either_drop_order_leaves_no_panic_and_no_deadlock() {
    let (client, server) = LoopbackDuplex::bounded(8);
    drop(client);
    drop(server);

    let (client, server) = LoopbackDuplex::bounded(8);
    drop(server);
    drop(client);

    // A registered waker on an end whose peer is already gone must still be
    // safe to fire, and must not be fired by its own end's drop.
    let (client, server) = LoopbackDuplex::bounded(8);
    let wakes = counting_waker(&server);
    drop(server);
    assert_eq!(
        wakes.load(Ordering::SeqCst),
        0,
        "a server drop wakes nobody"
    );
    drop(client);
    assert_eq!(
        wakes.load(Ordering::SeqCst),
        1,
        "the client drop still fires, harmlessly, into a gone server"
    );
}

#[test]
fn a_sequence_larger_than_the_ring_round_trips_across_two_threads() {
    const RING: usize = 64;
    let payload: Vec<u8> = (0..4096_u32)
        .map(|index| u8::try_from(index % 251).unwrap_or_default())
        .collect();

    let (client, mut server) = LoopbackDuplex::bounded(RING);
    let echo = std::thread::spawn(move || echo_until_hangup(client));

    let mut sent = 0_usize;
    let mut echoed: Vec<u8> = Vec::new();
    let mut chunk = [0_u8; 128];
    let deadline = Instant::now() + Duration::from_secs(30);
    while sent < payload.len() || echoed.len() < payload.len() {
        assert!(Instant::now() < deadline, "the round-trip stalled");
        if sent < payload.len() {
            match server.write(payload.get(sent..).unwrap_or(&[])) {
                Ok(written) => sent += written,
                Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                Err(error) => panic!("forward write failed: {error}"),
            }
        }
        match server.read(&mut chunk) {
            Ok(0) => panic!("the echo end hung up early"),
            Ok(read) => echoed.extend_from_slice(chunk.get(..read).unwrap_or(&[])),
            Err(error) if error.kind() == ErrorKind::WouldBlock => std::thread::yield_now(),
            Err(error) => panic!("return read failed: {error}"),
        }
    }
    drop(server);

    assert_eq!(
        echo.join().expect("the echo thread finishes"),
        payload.len()
    );
    assert_eq!(echoed, payload, "the byte sequence survived the round-trip");
}

/// Blocking-reads from `client` and writes every byte straight back, until the
/// server end hangs up. Returns how many bytes it echoed.
fn echo_until_hangup(mut client: LoopbackClientEnd) -> usize {
    let mut echoed = 0_usize;
    let mut chunk = [0_u8; 96];
    loop {
        let read = match client.read_timeout(&mut chunk, Some(Duration::from_secs(30))) {
            Ok(0) => return echoed,
            Ok(read) => read,
            Err(error) => panic!("echo read failed: {error}"),
        };
        let bytes = chunk.get(..read).unwrap_or(&[]).to_vec();
        let mut offset = 0_usize;
        while offset < bytes.len() {
            match client.write(bytes.get(offset..).unwrap_or(&[])) {
                Ok(written) => offset += written,
                Err(error) if error.kind() == ErrorKind::WouldBlock => std::thread::yield_now(),
                Err(error) => panic!("echo write failed: {error}"),
            }
        }
        echoed += read;
    }
}

/// The write-side twin of the blocking read: a full ring must PARK the writer
/// and let it through when the reader frees space, because that is what a
/// socket's `write_all` does under a write timeout. A writer that could only
/// see an instant `WouldBlock` would give the in-process mount a backpressure
/// semantics no other mount has.
#[test]
fn a_blocked_client_write_resumes_when_the_reader_frees_space() {
    const RING: usize = 8;
    let (mut client, mut server) = LoopbackDuplex::bounded(RING);
    // Fill the ring so the next write has nowhere to go.
    assert_eq!(
        client
            .write_timeout(&[1_u8; RING], Some(Duration::from_secs(5)))
            .expect("the empty ring accepts a full buffer"),
        RING
    );

    let started = Instant::now();
    let writer = std::thread::spawn(move || {
        let written = client
            .write_timeout(&[2_u8; 4], Some(Duration::from_secs(30)))
            .expect("the blocked write completes once space appears");
        // The end is returned so the ring stays open for the assertions below.
        (client, written)
    });

    // Give the writer a moment to actually park rather than racing past.
    std::thread::sleep(Duration::from_millis(50));
    let mut drained = [0_u8; RING];
    assert_eq!(
        server.read(&mut drained).expect("the full ring reads"),
        RING,
        "the reader took the bytes that were blocking the writer"
    );

    let (client, written) = writer.join().expect("the writer thread finishes");
    assert!(written > 0, "the unblocked write accepted nothing");
    assert!(
        started.elapsed() < Duration::from_secs(30),
        "the write was never unblocked"
    );
    let mut second = [0_u8; 4];
    let read = server.read(&mut second).expect("the resumed bytes read");
    assert_eq!(
        second.get(..read),
        Some(&[2_u8; 4][..written]),
        "the bytes that got through are the ones that were offered"
    );
    drop(client);
}

/// A blocked write is bounded, not eternal: a window that closes with the ring
/// still full is `TimedOut`, the same shape a socket write timeout produces.
#[test]
fn a_blocked_client_write_gives_up_when_its_window_closes() {
    const RING: usize = 4;
    let (mut client, _server) = LoopbackDuplex::bounded(RING);
    assert_eq!(
        client
            .write_timeout(&[9_u8; RING], Some(Duration::from_secs(5)))
            .expect("the empty ring accepts a full buffer"),
        RING
    );

    let started = Instant::now();
    let error = client
        .write_timeout(&[9_u8; 1], Some(Duration::from_millis(120)))
        .expect_err("a full ring with no reader cannot accept more");
    assert_eq!(error.kind(), ErrorKind::TimedOut);
    assert!(
        started.elapsed() >= Duration::from_millis(100),
        "the write gave up before its window closed: {:?}",
        started.elapsed()
    );
}

/// A writer parked on a full ring learns its reader vanished, rather than
/// waiting out its whole window against an end that will never drain.
#[test]
fn a_blocked_client_write_learns_its_reader_vanished() {
    const RING: usize = 4;
    let (mut client, server) = LoopbackDuplex::bounded(RING);
    assert_eq!(
        client
            .write_timeout(&[3_u8; RING], Some(Duration::from_secs(5)))
            .expect("the empty ring accepts a full buffer"),
        RING
    );

    let writer = std::thread::spawn(move || {
        client
            .write_timeout(&[3_u8; 1], Some(Duration::from_secs(30)))
            .expect_err("a write into a ring whose reader is gone cannot succeed")
            .kind()
    });
    std::thread::sleep(Duration::from_millis(50));
    drop(server);

    assert_eq!(
        writer.join().expect("the writer thread finishes"),
        ErrorKind::BrokenPipe
    );
}
