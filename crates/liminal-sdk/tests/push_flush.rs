//! Push-client flush-surface pins against a scripted wire-level fake server.
//!
//! These pins exercise the 0.4.0 `flush()`/`close()` contract of
//! `docs/design/SDK-PUSH-FLUSH.md` (r2, torn) where the SERVER's behavior must
//! be controlled frame-by-frame — selective rejection, withheld responses,
//! unsolicited responses, and FIN observation — which a real `liminal-server`
//! cannot be scripted to do:
//!
//! * R1(a)/D4/D5 — FIFO verdict pairing with interleaved reserved-channel
//!   (no-response) publishes excluded from the contract;
//! * R1(b) — a response-count mismatch is a typed MECHANISM `Err`, fail-loud;
//! * T1 — budget expiry with unresolved publishes is a NORMAL outcome, never
//!   an `Err`, and its shape is distinguishable from proven-accepted;
//! * R2/D3 — `close()` half-closes (FIN observed) as sole owner and degrades
//!   to disclosed verdict-only (socket demonstrably still usable) over a live
//!   `PushWriter` clone.
//!
//! The schema-0 end-to-end rejection pin runs against a REAL server in
//! `crates/liminal-server/tests/push_flush_e2e.rs`.
#![cfg(feature = "std")]

use std::error::Error;
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread::JoinHandle;
use std::time::Duration;

use liminal::protocol::{Frame, ProtocolError, ProtocolVersion, decode, encode, encoded_len};
use liminal_sdk::SdkError;
use liminal_sdk::remote::{FlushMode, OBSERVABILITY_CHANNEL, PushClient};

/// The ordinary (response-eliciting) channel the pins publish on.
const CHANNEL: &str = "app.events";
/// Bound on every test-side receive so a wedged pin fails, never hangs.
const DEADLINE: Duration = Duration::from_secs(5);
/// The blanket reason code the server stamps on every publish failure.
const SERVER_ERROR_CODE: u16 = 0xFFFF;
/// Stream id mirrored back on scripted responses.
const STREAM_ID: u32 = 1;

/// What the fake server observed, surfaced to the test thread.
#[derive(Debug, PartialEq, Eq)]
enum ServerEvent {
    /// A publish frame arrived, with its channel and payload bytes.
    Publish { channel: String, payload: Vec<u8> },
    /// The client's write half FIN'd: the read side saw a clean EOF.
    Eof,
}

/// Per-publish script: given (publish ordinal, channel, payload), the frames
/// the fake server writes back. An empty vec models a withheld / by-design
/// absent response.
type Responder = Box<dyn FnMut(u64, &str, &[u8]) -> Vec<Frame> + Send>;

/// A single-connection scripted server speaking the real wire codec.
struct FakeServer {
    addr: String,
    events: Receiver<ServerEvent>,
    handle: Option<JoinHandle<()>>,
}

impl FakeServer {
    fn spawn(responder: Responder) -> Result<Self, Box<dyn Error>> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?.to_string();
        let (events_tx, events) = channel();
        let mut responder = responder;
        let handle = std::thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                serve_connection(stream, &mut responder, &events_tx);
            }
        });
        Ok(Self {
            addr,
            events,
            handle: Some(handle),
        })
    }

    /// Blocks (bounded) for the next observed event.
    fn recv_event(&self) -> Result<ServerEvent, Box<dyn Error>> {
        Ok(self.events.recv_timeout(DEADLINE)?)
    }
}

impl Drop for FakeServer {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.join().ok();
        }
    }
}

/// Reads frames off the socket, answers the handshake, records publishes, and
/// plays the responder's scripted frames. Ends on EOF (reported as an event),
/// a disconnect frame, or a socket/codec error.
fn serve_connection(
    mut stream: TcpStream,
    responder: &mut Responder,
    events: &Sender<ServerEvent>,
) {
    let mut buffer: Vec<u8> = Vec::new();
    let mut publish_ordinal: u64 = 0;
    loop {
        match decode(&buffer) {
            Ok((frame, consumed)) => {
                buffer.drain(..consumed);
                match frame {
                    Frame::Connect { .. } => {
                        let ack = Frame::ConnectAck {
                            flags: 0,
                            selected_version: ProtocolVersion::new(1, 0),
                            capabilities: 0,
                        };
                        if write_frame(&mut stream, &ack).is_err() {
                            return;
                        }
                    }
                    Frame::Publish {
                        channel, envelope, ..
                    } => {
                        if events
                            .send(ServerEvent::Publish {
                                channel: channel.clone(),
                                payload: envelope.payload.clone(),
                            })
                            .is_err()
                        {
                            return;
                        }
                        for response in responder(publish_ordinal, &channel, &envelope.payload) {
                            if write_frame(&mut stream, &response).is_err() {
                                return;
                            }
                        }
                        publish_ordinal += 1;
                    }
                    Frame::Disconnect { .. } => return,
                    _ => {}
                }
            }
            Err(
                ProtocolError::IncompleteHeader { .. } | ProtocolError::TruncatedPayload { .. },
            ) => {
                let mut chunk = [0_u8; 4096];
                match std::io::Read::read(&mut stream, &mut chunk) {
                    Ok(0) => {
                        events.send(ServerEvent::Eof).ok();
                        return;
                    }
                    Ok(read) => {
                        if let Some(received) = chunk.get(..read) {
                            buffer.extend_from_slice(received);
                        } else {
                            return;
                        }
                    }
                    Err(_) => return,
                }
            }
            Err(_) => return,
        }
    }
}

/// Encodes and writes one frame.
fn write_frame(stream: &mut TcpStream, frame: &Frame) -> Result<(), Box<dyn Error>> {
    let len = encoded_len(frame)?;
    let mut bytes = vec![0_u8; len];
    let written = encode(frame, &mut bytes)?;
    let encoded = bytes.get(..written).ok_or("encoder byte count invalid")?;
    std::io::Write::write_all(stream, encoded)?;
    Ok(())
}

/// An ack for publish `ordinal`.
const fn ack(ordinal: u64) -> Frame {
    Frame::PublishAck {
        flags: 0,
        stream_id: STREAM_ID,
        message_id: ordinal,
    }
}

/// A rejection quoting the payload it rejects, so a pin can PROVE which
/// publish a verdict was paired to (the FIFO-attribution witness).
fn rejection_quoting(payload: &[u8]) -> Frame {
    Frame::PublishError {
        flags: 0,
        stream_id: STREAM_ID,
        reason_code: SERVER_ERROR_CODE,
        message: Some(format!("rejected:{}", String::from_utf8_lossy(payload))),
    }
}

/// A responder that acks ordinary publishes, rejects those whose payload
/// contains `reject` (quoting the payload), and stays silent on the reserved
/// observability channel.
fn selective_responder() -> Responder {
    Box::new(|ordinal, channel, payload| {
        if channel == OBSERVABILITY_CHANNEL {
            Vec::new()
        } else if payload.windows(6).any(|window| window == b"reject") {
            vec![rejection_quoting(payload)]
        } else {
            vec![ack(ordinal)]
        }
    })
}

/// R1(a)/D4/D5 pin — interleaved observability-plus-ordinary FIFO pairing.
///
/// Ordinary (response-eliciting) and reserved-channel (no-response) publishes
/// interleave; the middle ordinary publish is rejected with a message quoting
/// its payload. `flush()` must pair the one rejection to exactly that publish
/// (proved by the quoted payload), exclude the observability publishes from
/// the contract (they'd otherwise break the count and hang the flush), and a
/// SECOND flush must come back clean — the client stays fully usable, so a
/// plain flush's mode is `VerdictOnly` by construction.
#[test]
fn flush_pairs_verdicts_fifo_and_excludes_observability() -> Result<(), Box<dyn Error>> {
    let server = FakeServer::spawn(selective_responder())?;
    let client = PushClient::connect(&server.addr)?;

    client.publish(CHANNEL, b"accept-0".to_vec())?;
    client.publish(OBSERVABILITY_CHANNEL, b"obs-0".to_vec())?;
    client.publish(CHANNEL, b"reject-1".to_vec())?;
    client.publish(OBSERVABILITY_CHANNEL, b"obs-1".to_vec())?;
    client.publish(CHANNEL, b"accept-2".to_vec())?;

    let outcome = client.flush()?;
    assert_eq!(outcome.unresolved(), 0, "all verdicts arrive inside budget");
    assert_eq!(outcome.mode(), FlushMode::VerdictOnly);
    assert!(!outcome.is_proven_accepted());
    let failures = outcome.failures();
    assert_eq!(failures.len(), 1, "exactly one publish was rejected");
    assert_eq!(failures[0].reason_code(), SERVER_ERROR_CODE);
    assert_eq!(
        failures[0].message(),
        Some("rejected:reject-1"),
        "the rejection paired to the SECOND ordinary publish, in FIFO order"
    );

    // The flushed window is resolved; a repeat flush covers nothing and is the
    // proven-accepted shape — and the client remained usable after flushing.
    let repeat = client.flush()?;
    assert!(repeat.is_proven_accepted());
    assert_eq!(repeat.mode(), FlushMode::VerdictOnly);
    Ok(())
}

/// R1(b) pin — response-count mismatch is a typed MECHANISM error.
///
/// The server (mis)acks a reserved-channel publish the SDK rightly classifies
/// as non-response-eliciting, then sends a Push as a TOLD sync point proving
/// the reader consumed the unsolicited ack. The next `flush()` must fail
/// loudly with a typed mechanism `Err` — never pair the stray verdict.
#[test]
fn unsolicited_publish_response_is_a_typed_mechanism_error() -> Result<(), Box<dyn Error>> {
    let responder: Responder = Box::new(|_, channel, _| {
        if channel == OBSERVABILITY_CHANNEL {
            vec![
                ack(0),
                Frame::Push {
                    flags: 0,
                    stream_id: STREAM_ID,
                    correlation_id: 7,
                    payload: b"sync".to_vec(),
                },
            ]
        } else {
            Vec::new()
        }
    });
    let server = FakeServer::spawn(responder)?;
    let client = PushClient::connect(&server.addr)?;

    client.publish(OBSERVABILITY_CHANNEL, b"obs".to_vec())?;
    // The push arrives AFTER the unsolicited ack on the same stream, so once
    // it is received the reader has definitely captured the stray verdict.
    let push = client.recv_timeout(DEADLINE)?;
    assert_eq!(push.payload(), b"sync");

    match client.flush() {
        Err(SdkError::Protocol { description }) => {
            assert!(
                description.contains("response-count mismatch"),
                "mechanism error names the broken invariant: {description}"
            );
        }
        other => return Err(format!("expected a typed mechanism error, got {other:?}").into()),
    }
    Ok(())
}

/// T1 pin — budget expiry is a NORMAL, inspectable outcome, never an `Err`.
///
/// The server withholds every verdict. `flush()` must return `Ok` with every
/// flushed publish counted in `unresolved` and no failures — a shape
/// byte-distinguishable from proven-accepted (which requires BOTH empty
/// failures AND zero unresolved).
#[test]
fn budget_expiry_reports_unresolved_not_an_error() -> Result<(), Box<dyn Error>> {
    let responder: Responder = Box::new(|_, _, _| Vec::new());
    let server = FakeServer::spawn(responder)?;
    let client = PushClient::connect(&server.addr)?;

    client.publish(CHANNEL, b"withheld-0".to_vec())?;
    client.publish(CHANNEL, b"withheld-1".to_vec())?;

    let outcome = client.flush()?;
    assert!(outcome.failures().is_empty());
    assert_eq!(outcome.unresolved(), 2);
    assert!(
        !outcome.is_proven_accepted(),
        "unresolved publishes must never read as proven-accepted"
    );
    Ok(())
}

/// R2/D3 pin (sole owner) — `close()` flushes then half-closes gracefully.
///
/// With no live `PushWriter` clone, `close()` returns the verdicts, reports
/// `FlushedAndHalfClosed`, and the server observes the client's FIN (a clean
/// EOF on its read side) — the graceful teardown, disclosed.
#[test]
fn close_sole_owner_half_closes_and_discloses_mode() -> Result<(), Box<dyn Error>> {
    let server = FakeServer::spawn(selective_responder())?;
    let client = PushClient::connect(&server.addr)?;

    client.publish(CHANNEL, b"accept-a".to_vec())?;
    client.publish(CHANNEL, b"accept-b".to_vec())?;

    let outcome = client.close()?;
    assert!(outcome.is_proven_accepted());
    assert_eq!(outcome.mode(), FlushMode::FlushedAndHalfClosed);

    // Drain the two observed publishes, then the FIN witness.
    assert!(matches!(server.recv_event()?, ServerEvent::Publish { .. }));
    assert!(matches!(server.recv_event()?, ServerEvent::Publish { .. }));
    assert_eq!(server.recv_event()?, ServerEvent::Eof);
    Ok(())
}

/// R2 pin (shared socket) — `close()` over a live clone is verdict-only,
/// disclosed, and leaves the socket usable.
///
/// A live `PushWriter` clone shares the socket, so `close()` must NOT FIN; it
/// still returns the verdicts and discloses the degradation as `VerdictOnly`.
/// The clone then publishes again and the server RECEIVES that publish — the
/// no-FIN proof; a half-closed write half could never carry it.
#[test]
fn close_with_live_clone_is_verdict_only_and_keeps_socket_open() -> Result<(), Box<dyn Error>> {
    let server = FakeServer::spawn(selective_responder())?;
    let client = PushClient::connect(&server.addr)?;
    let writer = client.writer_handle();

    client.publish(CHANNEL, b"accept-pre".to_vec())?;
    let outcome = client.close()?;
    assert!(outcome.is_proven_accepted());
    assert_eq!(
        outcome.mode(),
        FlushMode::VerdictOnly,
        "a live clone forbids the FIN; the degradation is disclosed, not silent"
    );

    assert!(matches!(server.recv_event()?, ServerEvent::Publish { .. }));
    writer.publish(CHANNEL, b"after-close".to_vec())?;
    match server.recv_event()? {
        ServerEvent::Publish { channel, payload } => {
            assert_eq!(channel, CHANNEL);
            assert_eq!(payload, b"after-close".to_vec());
        }
        other @ ServerEvent::Eof => {
            return Err(format!("expected the clone's post-close publish, got {other:?}").into());
        }
    }
    drop(writer);
    Ok(())
}
