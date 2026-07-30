use super::*;
use liminal::protocol::FrameType;
use std::net::TcpListener;

/// The named setup deadline SDK-010 installs: 5 s, the estate's already-ratified
/// constant generalized (`websocket/std_socket.rs` `IO_TIMEOUT` and the TCP
/// subscription reader's own `SETUP_TIMEOUT` both already read 5 s), never a new
/// default.
const NAMED_SETUP_DEADLINE: Duration = Duration::from_secs(5);

/// A control-frame reply that is slow but well-behaved: comfortably past the
/// retired 100 ms `READER_POLL_TIMEOUT` cadence and far inside the named 5 s
/// setup deadline. This delay is the fixture's INTENDED slow reply — the thing
/// under test — not a proof device standing in for a real signal.
const SLOW_BUT_ANSWERED: Duration = Duration::from_millis(250);

/// A caller-selected deadline short enough to distinguish it from the default
/// while leaving ample room for a loopback connect and one control-frame write.
const CUSTOM_SETUP_DEADLINE: Duration = Duration::from_millis(100);

/// Blocks reading `socket` until one complete frame decodes, discarding it. Used
/// by the fake servers below to consume the client's `Connect` frame.
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
                        description: format!("fake push server read failed: {source}"),
                    })?;
                if read == 0 {
                    return Err(SdkError::Connection {
                        description: "fake push server: client closed before a full frame"
                            .to_string(),
                    });
                }
                buffer.extend_from_slice(chunk.get(..read).unwrap_or(&[]));
            }
            Err(error) => return Err(protocol_error(&error)),
        }
    }
}

/// Binds a loopback listener and returns it with its dialable address.
fn bind_fake_server() -> Result<(TcpListener, String), SdkError> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|source| SdkError::Connection {
        description: format!("failed to bind fake push server: {source}"),
    })?;
    let address = listener
        .local_addr()
        .map_err(|source| SdkError::Connection {
            description: format!("failed to read fake push server address: {source}"),
        })?
        .to_string();
    Ok((listener, address))
}

/// The `ConnectAck` this fake server answers a handshake with.
const fn connect_ack() -> Frame {
    Frame::ConnectAck {
        flags: 0,
        selected_version: CLIENT_MAX_VERSION,
        capabilities: 0,
    }
}

/// RED PIN (SDK-010 R2, direction (a)) — the accidental fatality, pinned.
///
/// `connect_socket` arms `READER_POLL_TIMEOUT` (100 ms) on the socket BEFORE the
/// handshake, under a comment that speaks only about the background reader
/// thread. But `handshake` runs on the CALLING thread through `read_one_frame`,
/// whose `FillOutcome::TimedOut` arm is fatal on the FIRST timeout. The
/// composition is a 100 ms-per-read FATAL deadline on connect. **Nobody chose
/// it** — and that is the whole justification for replacing it.
///
/// Here a real server answers the handshake in 250 ms: slow, but answering, and
/// far inside any deadline anyone would choose. Against the pre-change client
/// this fails with `SdkError::Connection { description: "push connection timed
/// out waiting for a control-frame reply" }`.
#[test]
fn connect_survives_a_control_reply_slower_than_the_retired_poll_cadence() -> Result<(), SdkError> {
    let (listener, address) = bind_fake_server()?;
    let server = std::thread::spawn(move || -> Result<(), SdkError> {
        let (mut socket, _peer) = listener.accept().map_err(|source| SdkError::Connection {
            description: format!("fake push server accept failed: {source}"),
        })?;
        let mut buffer = Vec::new();
        read_and_discard_one(&mut socket, &mut buffer)?;
        std::thread::sleep(SLOW_BUT_ANSWERED);
        write_frame(&mut socket, &connect_ack())?;
        // Read to the client's teardown so the socket outlives the assertions.
        let mut scratch = [0_u8; 512];
        while socket.read(&mut scratch).unwrap_or(0) > 0 {}
        Ok(())
    });

    let client = PushClient::connect(&address)?;
    drop(client);
    server.join().ok();
    Ok(())
}

/// RED PIN (SDK-010 R2, direction (b)) — the disarm, pinned behaviorally.
///
/// Once the control exchange is over the reader must block on socket input with
/// NO window: LAW-1 says the socket tells it, nothing sweeps. `read_timeout()`
/// reads the live `SO_RCVTIMEO` off the very kernel socket the reader thread
/// blocks on (the writer handle is a `try_clone` of it), so this is a behavioral
/// observation, not a source claim.
///
/// The assertion is `None`, not "longer than before": a leaked setup deadline
/// would report `Some(5s)` and fail exactly as the retired `Some(100ms)` cadence
/// does. Trading a 100 ms cadence for a 5 s one is the same defect at a
/// different period.
#[test]
fn the_push_reader_carries_no_read_window_in_steady_state() -> Result<(), SdkError> {
    let (listener, address) = bind_fake_server()?;
    let server = std::thread::spawn(move || -> Result<(), SdkError> {
        let (mut socket, _peer) = listener.accept().map_err(|source| SdkError::Connection {
            description: format!("fake push server accept failed: {source}"),
        })?;
        let mut buffer = Vec::new();
        read_and_discard_one(&mut socket, &mut buffer)?;
        write_frame(&mut socket, &connect_ack())?;
        let mut scratch = [0_u8; 512];
        while socket.read(&mut scratch).unwrap_or(0) > 0 {}
        Ok(())
    });

    let client = PushClient::connect(&address)?;
    let observed = client
        .writer
        .lock()
        .map_err(|error| SdkError::Connection {
            description: format!("push writer lock poisoned: {error}"),
        })?
        .read_timeout()
        .map_err(|source| SdkError::Connection {
            description: format!("failed to read the push socket read timeout: {source}"),
        })?;
    assert_eq!(
        observed, None,
        "the push reader must block with no read window once the control exchange \
         is over; a Some(_) here is a cadence, whatever its period"
    );
    drop(client);
    server.join().ok();
    Ok(())
}

/// SDK-011 — a caller-selected setup deadline reaches both the per-read socket
/// window and the wall-clock deadline for the control-frame reply.
///
/// If either site keeps the five-second default, this silent peer holds the call
/// until that default and the upper-bound assertion fails. The lower bound keeps
/// the test honest in the other direction: accepting the value must not turn the
/// deadline into an immediate refusal.
#[test]
fn a_supplied_setup_deadline_bounds_the_control_frame_reply() -> Result<(), SdkError> {
    let (listener, address) = bind_fake_server()?;
    let server = std::thread::spawn(move || -> Result<(), SdkError> {
        let (mut socket, _peer) = listener.accept().map_err(|source| SdkError::Connection {
            description: format!("fake push server accept failed: {source}"),
        })?;
        let mut buffer = Vec::new();
        read_and_discard_one(&mut socket, &mut buffer)?;
        // Never answer. Hold the socket until the refused client closes it, so
        // the selected deadline — not an EOF — is what ends the client's wait.
        let mut scratch = [0_u8; 512];
        while socket.read(&mut scratch).unwrap_or(0) > 0 {}
        Ok(())
    });

    let started = Instant::now();
    let outcome =
        PushClient::with_setup_deadline(&address, CUSTOM_SETUP_DEADLINE).connect();
    let elapsed = started.elapsed();
    server.join().ok();

    assert!(
        matches!(outcome, Err(SdkError::Connection { .. })),
        "a silent peer must be refused with a typed connection error, got {outcome:?}"
    );
    assert!(
        elapsed >= CUSTOM_SETUP_DEADLINE,
        "the refusal must not precede the caller's {CUSTOM_SETUP_DEADLINE:?} setup \
         deadline; it arrived in {elapsed:?}"
    );
    assert!(
        elapsed < NAMED_SETUP_DEADLINE,
        "the caller's {CUSTOM_SETUP_DEADLINE:?} setup deadline must replace the \
         {NAMED_SETUP_DEADLINE:?} default at both deadline sites; refusal took {elapsed:?}"
    );
    Ok(())
}

/// SDK-011 — the caller-selected setup deadline is phase-local. Once the
/// handshake succeeds, the background reader's live socket must carry no read
/// timeout at all.
#[test]
fn a_supplied_setup_deadline_is_disarmed_before_the_reader_starts() -> Result<(), SdkError> {
    let (listener, address) = bind_fake_server()?;
    let server = std::thread::spawn(move || -> Result<(), SdkError> {
        let (mut socket, _peer) = listener.accept().map_err(|source| SdkError::Connection {
            description: format!("fake push server accept failed: {source}"),
        })?;
        let mut buffer = Vec::new();
        read_and_discard_one(&mut socket, &mut buffer)?;
        write_frame(&mut socket, &connect_ack())?;
        let mut scratch = [0_u8; 512];
        while socket.read(&mut scratch).unwrap_or(0) > 0 {}
        Ok(())
    });

    let client = PushClient::with_setup_deadline(&address, CUSTOM_SETUP_DEADLINE).connect()?;
    let observed = client
        .writer
        .lock()
        .map_err(|error| SdkError::Connection {
            description: format!("push writer lock poisoned: {error}"),
        })?
        .read_timeout()
        .map_err(|source| SdkError::Connection {
            description: format!("failed to read the push socket read timeout: {source}"),
        })?;
    assert_eq!(observed, None, "a setup deadline must not survive setup");
    drop(client);
    server.join().ok();
    Ok(())
}

/// RED PIN (SDK-010 R2) — the setup deadline is genuinely ARMED, pinned.
///
/// The disarm above would be trivially satisfiable by never arming anything, so
/// the other side is pinned too: a peer that accepts the connection, consumes the
/// `Connect`, and then never answers is REFUSED — and refused at the named 5 s
/// deadline, not at the retired 100 ms cadence. Against the pre-change client the
/// refusal arrives in roughly 100 ms and this fails on the elapsed floor.
#[test]
fn a_peer_that_never_answers_the_handshake_is_refused_at_the_named_deadline() -> Result<(), SdkError>
{
    let (listener, address) = bind_fake_server()?;
    let server = std::thread::spawn(move || -> Result<(), SdkError> {
        let (mut socket, _peer) = listener.accept().map_err(|source| SdkError::Connection {
            description: format!("fake push server accept failed: {source}"),
        })?;
        let mut buffer = Vec::new();
        read_and_discard_one(&mut socket, &mut buffer)?;
        // Never answer. Hold the socket until the refused client closes it, so
        // the deadline — not an EOF — is what ends the client's wait.
        let mut scratch = [0_u8; 512];
        while socket.read(&mut scratch).unwrap_or(0) > 0 {}
        Ok(())
    });

    let started = Instant::now();
    let outcome = PushClient::connect(&address);
    let elapsed = started.elapsed();
    server.join().ok();

    assert!(
        matches!(outcome, Err(SdkError::Connection { .. })),
        "a silent peer must be refused with a typed connection error, got {outcome:?}"
    );
    assert!(
        elapsed >= NAMED_SETUP_DEADLINE,
        "the refusal must arrive at the named {NAMED_SETUP_DEADLINE:?} setup \
         deadline, not at a poll cadence nobody chose; it arrived in {elapsed:?}"
    );
    Ok(())
}

/// TOMBSTONE (SDK-010 R5) — the retired reader poll family must not reappear in
/// this reader's production source. Modelled on
/// `membership_source_has_no_retired_poll_family`
/// (`crates/liminal-server/src/cluster/membership.rs`).
#[test]
fn push_client_source_has_no_retired_reader_poll_family() {
    const SOURCE: &str = include_str!("push_client.rs");
    let production = SOURCE.split("#[cfg(test)]").next().unwrap_or(SOURCE);
    for forbidden in [
        "READER_POLL_TIMEOUT",
        "AtomicBool",
        "stop.load",
        "stop.store",
        "re-check the stop flag",
        "poll the stop flag",
    ] {
        assert!(
            !production.contains(forbidden),
            "retired push-reader poll-family source `{forbidden}` reappeared"
        );
    }
}

#[test]
fn pushed_frame_exposes_correlation_and_payload() {
    let frame = PushedFrame {
        correlation_id: 7,
        payload: vec![1, 2, 3],
    };
    assert_eq!(frame.correlation_id(), 7);
    assert_eq!(frame.payload(), &[1, 2, 3]);
    assert_eq!(frame.into_payload(), vec![1, 2, 3]);
}

#[test]
fn publish_frame_round_trips_through_codec() -> Result<(), SdkError> {
    // The observability publish frame the drain leg writes: a Publish on the
    // reserved channel carrying opaque payload bytes verbatim.
    let envelope = MessageEnvelope::new(
        SchemaId::new([0_u8; SchemaId::WIRE_LEN]),
        CausalContext::independent(),
        vec![9, 9, 9],
    );
    let frame = Frame::new_publish(APPLICATION_STREAM_ID, OBSERVABILITY_CHANNEL, envelope)
        .map_err(|error| protocol_error(&error))?;
    let len = encoded_len(&frame).map_err(|error| protocol_error(&error))?;
    let mut bytes = vec![0_u8; len];
    let written = encode(&frame, &mut bytes).map_err(|error| protocol_error(&error))?;
    let (decoded, consumed) = decode(&bytes[..written]).map_err(|error| protocol_error(&error))?;
    assert_eq!(consumed, written);
    assert_eq!(decoded.frame_type(), FrameType::Publish);
    let Frame::Publish {
        channel, envelope, ..
    } = decoded
    else {
        return Err(SdkError::Protocol {
            description: "expected a Publish frame".to_string(),
        });
    };
    assert_eq!(channel, OBSERVABILITY_CHANNEL);
    assert_eq!(envelope.payload, vec![9, 9, 9]);
    Ok(())
}

#[test]
fn reply_frame_round_trips_through_codec() -> Result<(), SdkError> {
    let frame = Frame::new_push_reply(APPLICATION_STREAM_ID, 9, vec![4, 5])
        .map_err(|error| protocol_error(&error))?;
    let len = encoded_len(&frame).map_err(|error| protocol_error(&error))?;
    let mut bytes = vec![0_u8; len];
    let written = encode(&frame, &mut bytes).map_err(|error| protocol_error(&error))?;
    let (decoded, consumed) = decode(&bytes[..written]).map_err(|error| protocol_error(&error))?;
    assert_eq!(consumed, written);
    assert_eq!(decoded.frame_type(), FrameType::PushReply);
    Ok(())
}
