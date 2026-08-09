//! P0 #56 pin 3: a refused connection is TOLD, and the telling reaches the wire.
//!
//! These read from a real client socket on a real TCP connection, not from a
//! buffer. That is the whole design of the pin: this codebase has an
//! enqueue-then-flush outbound shape, and a frame that is enqueued but never
//! flushed is byte-for-byte indistinguishable on the wire from the bare socket
//! drop this lane replaces. Asserting on a queue would green either way.
//! Asserting on bytes the kernel actually delivered to the peer cannot.

use std::io::Read as _;
use std::net::{SocketAddr, TcpListener, TcpStream};

use liminal::protocol::{Frame, decode};
use tungstenite::Message;
use tungstenite::protocol::{Role, WebSocket};

use super::refusal::{
    AdmissionRefusal, MAX_CLOSE_REASON_BYTES, send_tcp_refusal, send_websocket_refusal,
};
use crate::ServerError;

/// Every refusal class, so the distinctness and budget pins cannot silently
/// stop covering a variant that someone adds later.
const ALL_CLASSES: [AdmissionRefusal; 7] = [
    AdmissionRefusal::AdmissionHeld,
    AdmissionRefusal::AuthoritySurrendered,
    AdmissionRefusal::ConnectionsSaturated,
    AdmissionRefusal::ParticipantServiceFatal,
    AdmissionRefusal::IncarnationExhausted,
    AdmissionRefusal::AllocationFailed,
    AdmissionRefusal::SpawnFailed,
];

fn tcp_pair() -> Result<(TcpStream, TcpStream), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address: SocketAddr = listener.local_addr()?;
    let client = TcpStream::connect(address)?;
    let (server, _) = listener.accept()?;
    Ok((client, server))
}

#[test]
fn a_refused_websocket_client_reads_a_typed_frame_and_a_close_with_reason()
-> Result<(), Box<dyn std::error::Error>> {
    let (client, server) = tcp_pair()?;
    // The refusal path expects the non-blocking mode `spawn_upgraded` puts an
    // upgraded socket into, so the fixture reproduces it rather than starting
    // from a convenient state.
    server.set_nonblocking(true)?;
    let mut server_socket = WebSocket::from_raw_socket(server, Role::Server, None);
    let mut client_socket = WebSocket::from_raw_socket(client, Role::Client, None);

    send_websocket_refusal(&mut server_socket, AdmissionRefusal::AdmissionHeld)?;
    // Drop the server end entirely. Anything the client can still read after
    // this crossed the kernel boundary before the drop — which is exactly the
    // flush claim under test.
    drop(server_socket);

    let first = client_socket.read()?;
    let Message::Binary(bytes) = first else {
        return Err(format!("expected a binary liminal frame, got {first:?}").into());
    };
    let (frame, consumed) = decode(&bytes)?;
    assert_eq!(consumed, bytes.len(), "the refusal is exactly one frame");
    let Frame::ConnectError {
        reason_code,
        message,
        ..
    } = frame
    else {
        return Err(format!("expected ConnectError, got {frame:?}").into());
    };
    assert_eq!(reason_code, 0xFFFF);
    let message = message.ok_or("ConnectError carried no message")?;
    assert!(
        message.contains("admission held"),
        "the reason must name the refusal class, got: {message}"
    );

    let second = client_socket.read()?;
    let Message::Close(Some(close)) = second else {
        return Err(format!("expected a Close frame with a reason, got {second:?}").into());
    };
    assert_eq!(
        u16::from(close.code),
        AdmissionRefusal::AdmissionHeld.close_code()
    );
    assert!(
        close.reason.as_str().contains("admission_held"),
        "the close reason must name the refusal class, got: {}",
        close.reason
    );
    Ok(())
}

/// A Close frame is a control frame: RFC 6455 caps its payload at 125 bytes,
/// two of which are the code. Over that, `send` fails with `ControlFrameTooBig`
/// and the client gets a bare drop again — the exact defect this lane closes,
/// reintroduced by a long sentence. This pin is the guard on that edit.
#[test]
fn every_close_reason_fits_in_a_control_frame() {
    for class in ALL_CLASSES {
        let reason = class.close_reason();
        assert!(
            reason.len() <= MAX_CLOSE_REASON_BYTES,
            "{} close reason is {} bytes, over the {MAX_CLOSE_REASON_BYTES}-byte control-frame \
             budget; a Close that large FAILS TO SEND and degrades to a bare drop",
            class.label(),
            reason.len()
        );
    }
}

#[test]
fn each_refusal_class_is_distinguishable_on_the_wire() {
    // The three classes the brief calls out by name, plus the surrendered
    // authority, must not collapse into one another: an operator reading a
    // browser console has to be able to tell "the server is full" from "the
    // server cannot allocate" from "the participant service died".
    let classes = ALL_CLASSES;
    let mut codes: Vec<u16> = classes.iter().map(|class| class.close_code()).collect();
    let mut labels: Vec<&str> = classes.iter().map(|class| class.label()).collect();
    let mut reasons: Vec<&str> = classes.iter().map(|class| class.reason()).collect();
    let class_count = classes.len();

    codes.sort_unstable();
    codes.dedup();
    labels.sort_unstable();
    labels.dedup();
    reasons.sort_unstable();
    reasons.dedup();

    assert_eq!(codes.len(), class_count, "close codes must be distinct");
    assert_eq!(labels.len(), class_count, "metric labels must be distinct");
    assert_eq!(reasons.len(), class_count, "wire reasons must be distinct");

    // Close codes stay inside the application-private range, so they can never
    // be confused with a protocol-defined code such as 1006.
    for class in classes {
        let code = class.close_code();
        assert!(
            (4000..5000).contains(&code),
            "{} used {code}, outside the application range",
            class.label()
        );
    }
}

#[test]
fn the_saturation_refusal_classifies_from_its_own_server_error() {
    // Classification reads the error the admission path actually produced, so
    // the pin builds the error the way the supervisor builds it rather than
    // asserting against a hand-picked variant.
    assert_eq!(
        AdmissionRefusal::classify(&ServerError::ConnectionLimitReached { limit: 256 }),
        AdmissionRefusal::ConnectionsSaturated
    );
    assert_eq!(
        AdmissionRefusal::classify(&ServerError::ConnectionIncarnationExhausted {
            attempted_server_incarnation: 9,
        }),
        AdmissionRefusal::IncarnationExhausted
    );
    assert_eq!(
        AdmissionRefusal::classify(&ServerError::ParticipantIncarnation {
            phase: super::incarnation::AMBIGUOUS_DURABLE_WRITE_PHASE,
            message: String::new(),
        }),
        AdmissionRefusal::AdmissionHeld
    );
    assert_eq!(
        AdmissionRefusal::classify(&ServerError::ParticipantIncarnation {
            phase: super::incarnation::AUTHORITY_SURRENDERED_PHASE,
            message: String::new(),
        }),
        AdmissionRefusal::AuthoritySurrendered
    );
    assert_eq!(
        AdmissionRefusal::classify(&ServerError::ConnectionPidCollision { pid: 1 }),
        AdmissionRefusal::SpawnFailed
    );
}

#[test]
fn a_refused_tcp_client_reads_a_typed_frame_before_the_close()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut client, mut server) = tcp_pair()?;
    // Parity with the live path: `spawn_connection` sets the accepted stream
    // non-blocking before the allocation that refuses it.
    server.set_nonblocking(true)?;

    send_tcp_refusal(&mut server, AdmissionRefusal::ConnectionsSaturated)?;
    drop(server);

    // Read to EOF. The TCP route has no transport framing of its own, so the
    // refusal is the canonical liminal bytes and then the FIN.
    let mut received = Vec::new();
    client.read_to_end(&mut received)?;
    assert!(
        !received.is_empty(),
        "a refused TCP client must receive the refusal, not a bare FIN"
    );

    let (frame, consumed) = decode(&received)?;
    assert_eq!(consumed, received.len(), "the refusal is exactly one frame");
    let Frame::ConnectError { message, .. } = frame else {
        return Err(format!("expected ConnectError, got {frame:?}").into());
    };
    let message = message.ok_or("ConnectError carried no message")?;
    assert!(
        message.contains("max_connections"),
        "the reason must name the refusal class, got: {message}"
    );
    Ok(())
}
