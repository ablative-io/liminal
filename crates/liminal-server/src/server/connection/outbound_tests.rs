use std::io::Read;
use std::net::{TcpListener, TcpStream};

use liminal::protocol::{CausalContext, Frame, MessageEnvelope, SchemaId, decode};

use super::{OutboundError, OutboundWriter};

fn envelope(payload: Vec<u8>) -> MessageEnvelope {
    MessageEnvelope::new(
        SchemaId::new([7; SchemaId::WIRE_LEN]),
        CausalContext::independent(),
        payload,
    )
}

fn deliver_frame(payload: Vec<u8>) -> Frame {
    Frame::Deliver {
        flags: 0,
        stream_id: 1,
        delivery_seq: 1,
        envelope: envelope(payload),
    }
}

/// A connected loopback socket pair: the writer half is non-blocking (matching a
/// server connection stream), the reader half blocks so a test can pull bytes.
fn socket_pair() -> Result<(TcpStream, TcpStream), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let writer = TcpStream::connect(address)?;
    let (reader, _) = listener.accept()?;
    writer.set_nonblocking(true)?;
    Ok((writer, reader))
}

#[test]
fn enqueue_then_drain_delivers_the_frame() -> Result<(), Box<dyn std::error::Error>> {
    let (mut writer_socket, mut reader_socket) = socket_pair()?;
    let mut outbound = OutboundWriter::new();
    let frame = deliver_frame(vec![1, 2, 3, 4]);
    outbound.enqueue_frame(&frame)?;
    outbound.drain(&mut writer_socket)?;
    assert_eq!(
        outbound.queued_len(),
        0,
        "a small frame drains fully in one slice"
    );

    let mut buffer = vec![0_u8; 4096];
    let read = reader_socket.read(&mut buffer)?;
    let (decoded, _) = decode(&buffer[..read])?;
    assert_eq!(decoded, frame);
    Ok(())
}

#[test]
fn enqueue_beyond_capacity_reports_overflow() {
    // A 64-byte cap cannot hold a Deliver frame carrying a 4 KiB payload.
    let mut outbound = OutboundWriter::with_capacity(64);
    let result = outbound.enqueue_frame(&deliver_frame(vec![0_u8; 4096]));
    assert!(matches!(result, Err(OutboundError::Overflow { .. })));
    assert_eq!(
        outbound.queued_len(),
        0,
        "a rejected frame must not partially enqueue"
    );
}

#[test]
fn drain_of_empty_buffer_is_ok() -> Result<(), Box<dyn std::error::Error>> {
    let (mut writer_socket, _reader) = socket_pair()?;
    let mut outbound = OutboundWriter::new();
    outbound.drain(&mut writer_socket)?;
    assert_eq!(outbound.queued_len(), 0);
    Ok(())
}

/// A frame far larger than a typical kernel send buffer must survive a
/// `WouldBlock` mid-drain: bytes not accepted this slice stay queued and flush on
/// later slices as the reader consumes. This is the inverted G4 regression at the
/// unit level — `write_all` would have errored the instant the send buffer filled.
#[test]
fn large_frame_survives_partial_writes() -> Result<(), Box<dyn std::error::Error>> {
    let (mut writer_socket, mut reader_socket) = socket_pair()?;
    reader_socket.set_nonblocking(true)?;
    let mut outbound = OutboundWriter::new();
    let payload: Vec<u8> = (0..300_000_usize)
        .map(|index| u8::try_from(index % 251).unwrap_or(0))
        .collect();
    let frame = deliver_frame(payload.clone());
    outbound.enqueue_frame(&frame)?;

    let mut received = Vec::new();
    let mut scratch = vec![0_u8; 65536];
    // Alternate draining (writer side) with reading (reader side) until the whole
    // frame has been transferred. A single drain would leave residue queued once
    // the send buffer fills; the loop proves the residue flushes across slices.
    for _ in 0..10_000 {
        outbound.drain(&mut writer_socket)?;
        match reader_socket.read(&mut scratch) {
            Ok(0) => break,
            Ok(read) => received.extend_from_slice(&scratch[..read]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error.into()),
        }
        if outbound.queued_len() == 0 && received.len() >= payload.len() {
            break;
        }
    }

    let (decoded, _) = decode(&received)?;
    assert_eq!(
        decoded, frame,
        "the large frame is delivered intact across slices"
    );
    Ok(())
}
