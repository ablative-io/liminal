//! R3.1 deterministic driver-trace tests (LP-WS-TRANSPORT, folded r1.1).
//!
//! These tests run the transport-neutral WebSocket liminal driver without any
//! socket, pinning the closed event/command contract, canonical-frame
//! validation, in-flight wire correlation, and the F3 terminal-event
//! discipline (first terminal mints exactly one typed fate; every later
//! terminal is a typed no-op).

use liminal_sdk::remote::websocket::{
    CommandRefusal, DriverOutput, DriverPhase, DriverStep, FrameCorrelation, FrameViolation,
    PostTerminalEvent, ResponseExpectation, SocketCommand, SocketEvent, SocketFailure,
    TransportTerminal, WebSocketFrameDriver,
};

type TestResult<T = ()> = Result<T, String>;

/// Generic liminal frame header length (ten bytes).
const HEADER_LEN: usize = 10;
/// Wire discriminant of the server `Deliver` frame.
const FRAME_TYPE_DELIVER: u8 = 0x19;
/// Wire discriminant of the `PublishAck` frame (an ordinary response class).
const FRAME_TYPE_PUBLISH_ACK: u8 = 0x0A;

/// Builds one canonical liminal frame image: ten-byte header plus payload.
fn frame(frame_type: u8, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(HEADER_LEN + payload.len());
    bytes.push(frame_type);
    bytes.push(0);
    bytes.extend_from_slice(&1_u32.to_be_bytes());
    let len = u32::try_from(payload.len()).unwrap_or(u32::MAX);
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

/// Drives a fresh driver to the established phase.
fn established_driver() -> TestResult<WebSocketFrameDriver> {
    let mut driver = WebSocketFrameDriver::new();
    let command = driver
        .command_open()
        .map_err(|refusal| format!("fresh driver must accept open: {refusal:?}"))?;
    if command != SocketCommand::Open {
        return Err(format!("open must emit the Open command, got {command:?}"));
    }
    let step = driver.handle_event(SocketEvent::Opened);
    if step.output != DriverOutput::Opened {
        return Err(format!("Opened event must report Opened, got {step:?}"));
    }
    if driver.phase() != DriverPhase::Established {
        return Err(format!(
            "driver must be established after Opened, got {:?}",
            driver.phase()
        ));
    }
    Ok(driver)
}

#[test]
fn open_command_then_opened_reaches_established() -> TestResult {
    let driver = established_driver()?;
    assert!(!driver.has_outstanding_exchange());
    Ok(())
}

#[test]
fn open_is_single_use_and_refused_after_first_command() -> TestResult {
    let mut driver = WebSocketFrameDriver::new();
    driver
        .command_open()
        .map_err(|refusal| format!("first open must be accepted: {refusal:?}"))?;
    let Err(refusal) = driver.command_open() else {
        return Err("second open command must be refused".to_string());
    };
    assert!(matches!(refusal, CommandRefusal::InvalidPhase { .. }));
    Ok(())
}

#[test]
fn opened_without_open_command_is_typed_refusal() {
    let mut driver = WebSocketFrameDriver::new();
    let step = driver.handle_event(SocketEvent::Opened);
    assert!(
        matches!(step.output, DriverOutput::Refused(_)),
        "Opened before any open command must be a typed refusal, got {step:?}"
    );
    assert_eq!(step.command, None);
}

#[test]
fn correlated_response_resolves_single_outstanding_exchange() -> TestResult {
    let mut driver = established_driver()?;
    let request = frame(0x09, b"payload");
    let command = driver
        .command_send(request.clone(), ResponseExpectation::Correlated)
        .map_err(|refusal| format!("established send must be accepted: {refusal:?}"))?;
    if command != SocketCommand::SendBinary(request) {
        return Err(format!("send must emit SendBinary, got {command:?}"));
    }
    assert!(driver.has_outstanding_exchange());

    let response = frame(FRAME_TYPE_PUBLISH_ACK, b"ack");
    let step = driver.handle_event(SocketEvent::Binary(response.clone()));
    assert_eq!(
        step.output,
        DriverOutput::Frame {
            bytes: response,
            correlation: FrameCorrelation::CorrelatedResponse,
        }
    );
    assert!(!driver.has_outstanding_exchange());
    Ok(())
}

#[test]
fn at_most_one_outstanding_correlated_exchange() -> TestResult {
    // Q-B: the at-most-one outstanding write-ahead rule is structural in the
    // driver, not an adapter convention.
    let mut driver = established_driver()?;
    driver
        .command_send(frame(0x09, b"first"), ResponseExpectation::Correlated)
        .map_err(|refusal| format!("first correlated send must be accepted: {refusal:?}"))?;
    let Err(refusal) = driver.command_send(frame(0x09, b"second"), ResponseExpectation::Correlated)
    else {
        return Err("second correlated send must be refused while one is outstanding".to_string());
    };
    assert!(matches!(refusal, CommandRefusal::ExchangeOutstanding));
    Ok(())
}

#[test]
fn deliver_frames_stay_unsolicited_even_mid_exchange() -> TestResult {
    let mut driver = established_driver()?;
    driver
        .command_send(frame(0x09, b"request"), ResponseExpectation::Correlated)
        .map_err(|refusal| format!("correlated send must be accepted: {refusal:?}"))?;

    let delivery = frame(FRAME_TYPE_DELIVER, b"delivered");
    let step = driver.handle_event(SocketEvent::Binary(delivery.clone()));
    assert_eq!(
        step.output,
        DriverOutput::Frame {
            bytes: delivery,
            correlation: FrameCorrelation::UnsolicitedDelivery,
        }
    );
    // The delivery must not consume the outstanding exchange.
    assert!(driver.has_outstanding_exchange());
    Ok(())
}

#[test]
fn non_deliver_frame_without_exchange_is_unsolicited_frame() -> TestResult {
    let mut driver = established_driver()?;
    let unexpected = frame(FRAME_TYPE_PUBLISH_ACK, b"stray");
    let step = driver.handle_event(SocketEvent::Binary(unexpected.clone()));
    assert_eq!(
        step.output,
        DriverOutput::Frame {
            bytes: unexpected,
            correlation: FrameCorrelation::UnsolicitedFrame,
        }
    );
    Ok(())
}

#[test]
fn empty_message_is_typed_violation_and_closes() -> TestResult {
    let mut driver = established_driver()?;
    let step = driver.handle_event(SocketEvent::Binary(Vec::new()));
    assert_eq!(
        step.output,
        DriverOutput::Terminal(TransportTerminal::ProtocolViolation(
            FrameViolation::EmptyMessage
        ))
    );
    assert_eq!(step.command, Some(SocketCommand::Close));
    assert_eq!(driver.phase(), DriverPhase::Terminated);
    Ok(())
}

#[test]
fn truncated_header_is_typed_violation() -> TestResult {
    let mut driver = established_driver()?;
    let step = driver.handle_event(SocketEvent::Binary(vec![0x09; HEADER_LEN - 1]));
    assert!(
        matches!(
            step.output,
            DriverOutput::Terminal(TransportTerminal::ProtocolViolation(
                FrameViolation::TruncatedHeader { .. }
            ))
        ),
        "nine bytes must be a truncated header, got {step:?}"
    );
    assert_eq!(step.command, Some(SocketCommand::Close));
    Ok(())
}

#[test]
fn truncated_body_is_typed_violation() -> TestResult {
    let mut driver = established_driver()?;
    let mut bytes = frame(0x09, b"whole-body");
    bytes.truncate(HEADER_LEN + 3);
    let step = driver.handle_event(SocketEvent::Binary(bytes));
    assert!(
        matches!(
            step.output,
            DriverOutput::Terminal(TransportTerminal::ProtocolViolation(
                FrameViolation::TruncatedBody { .. }
            ))
        ),
        "short body must be a truncated-body violation, got {step:?}"
    );
    Ok(())
}

#[test]
fn trailing_bytes_and_concatenated_frames_are_typed_violations() -> TestResult {
    let mut driver = established_driver()?;
    let mut trailing = frame(0x09, b"body");
    trailing.push(0xFF);
    let step = driver.handle_event(SocketEvent::Binary(trailing));
    assert!(
        matches!(
            step.output,
            DriverOutput::Terminal(TransportTerminal::ProtocolViolation(
                FrameViolation::TrailingBytes { .. }
            ))
        ),
        "trailing byte must be a trailing-bytes violation, got {step:?}"
    );

    // Two concatenated canonical frames in one message are equally refused.
    let mut driver = established_driver()?;
    let mut two = frame(0x09, b"one");
    two.extend_from_slice(&frame(0x09, b"two"));
    let step = driver.handle_event(SocketEvent::Binary(two));
    assert!(
        matches!(
            step.output,
            DriverOutput::Terminal(TransportTerminal::ProtocolViolation(
                FrameViolation::TrailingBytes { .. }
            ))
        ),
        "two concatenated frames must be a trailing-bytes violation, got {step:?}"
    );
    Ok(())
}

#[test]
fn declared_length_beyond_frame_bound_is_typed_violation() -> TestResult {
    // A header declaring the u32 payload maximum pushes the complete frame past
    // the liminal frame bound; the driver must refuse from the declared length
    // alone (the body is absent).
    let mut driver = established_driver()?;
    let mut bytes = Vec::with_capacity(HEADER_LEN);
    bytes.push(0x09);
    bytes.push(0);
    bytes.extend_from_slice(&1_u32.to_be_bytes());
    bytes.extend_from_slice(&u32::MAX.to_be_bytes());
    let step = driver.handle_event(SocketEvent::Binary(bytes));
    assert!(
        matches!(
            step.output,
            DriverOutput::Terminal(TransportTerminal::ProtocolViolation(
                FrameViolation::TruncatedBody { .. }
            )) | DriverOutput::Terminal(TransportTerminal::ProtocolViolation(
                FrameViolation::DeclaredBeyondBound { .. }
            ))
        ),
        "oversize declaration without body must be refused, got {step:?}"
    );
    Ok(())
}

#[test]
fn f3_abnormal_loss_error_then_close_mints_exactly_one_fate() -> TestResult {
    // F3: abnormal browser loss always produces `error` THEN `close`. The first
    // terminal mints the one typed fate; the echoed close is a typed no-op.
    let mut driver = established_driver()?;
    let step = driver.handle_event(SocketEvent::Failed(SocketFailure::Transport));
    assert_eq!(
        step.output,
        DriverOutput::Terminal(TransportTerminal::SocketFailed(SocketFailure::Transport))
    );
    assert_eq!(step.command, Some(SocketCommand::Close));

    let echo = driver.handle_event(SocketEvent::Closed);
    assert_eq!(
        echo.output,
        DriverOutput::PostTerminalIgnored(PostTerminalEvent::Closed)
    );
    assert_eq!(echo.command, None);
    assert_eq!(driver.phase(), DriverPhase::Terminated);
    Ok(())
}

#[test]
fn f3_clean_close_mints_peer_closed_alone() -> TestResult {
    let mut driver = established_driver()?;
    let step = driver.handle_event(SocketEvent::Closed);
    assert_eq!(
        step.output,
        DriverOutput::Terminal(TransportTerminal::PeerClosed)
    );

    // Nothing follows a clean close; a duplicate close stays a typed no-op.
    let duplicate = driver.handle_event(SocketEvent::Closed);
    assert_eq!(
        duplicate.output,
        DriverOutput::PostTerminalIgnored(PostTerminalEvent::Closed)
    );
    Ok(())
}

#[test]
fn f3_commanded_close_echo_mints_close_completed_once() -> TestResult {
    let mut driver = established_driver()?;
    let command = driver
        .command_close()
        .map_err(|refusal| format!("established close must be accepted: {refusal:?}"))?;
    if command != SocketCommand::Close {
        return Err(format!("close must emit the Close command, got {command:?}"));
    }
    assert_eq!(driver.phase(), DriverPhase::Closing);

    let echo = driver.handle_event(SocketEvent::Closed);
    assert_eq!(
        echo.output,
        DriverOutput::Terminal(TransportTerminal::CloseCompleted)
    );

    let after = driver.handle_event(SocketEvent::Closed);
    assert_eq!(
        after.output,
        DriverOutput::PostTerminalIgnored(PostTerminalEvent::Closed)
    );
    Ok(())
}

#[test]
fn every_post_terminal_event_is_a_typed_no_op() -> TestResult {
    let mut driver = established_driver()?;
    let first = driver.handle_event(SocketEvent::Failed(SocketFailure::Transport));
    assert!(matches!(first.output, DriverOutput::Terminal(_)));

    let events: [(SocketEvent, PostTerminalEvent); 4] = [
        (SocketEvent::Closed, PostTerminalEvent::Closed),
        (
            SocketEvent::Failed(SocketFailure::Transport),
            PostTerminalEvent::Failed,
        ),
        (SocketEvent::Opened, PostTerminalEvent::Opened),
        (
            SocketEvent::Binary(frame(0x09, b"late")),
            PostTerminalEvent::Binary,
        ),
    ];
    for (event, expected) in events {
        let step = driver.handle_event(event);
        assert_eq!(step.output, DriverOutput::PostTerminalIgnored(expected));
        assert_eq!(step.command, None);
    }
    Ok(())
}

#[test]
fn text_message_socket_failure_is_typed_terminal() -> TestResult {
    // The transport contract types a text message as a protocol failure that
    // closes the connection; the adapter reports the observed socket fact and
    // the driver mints the single typed fate.
    let mut driver = established_driver()?;
    let step = driver.handle_event(SocketEvent::Failed(SocketFailure::UnsupportedTextMessage));
    assert_eq!(
        step.output,
        DriverOutput::Terminal(TransportTerminal::SocketFailed(
            SocketFailure::UnsupportedTextMessage
        ))
    );
    assert_eq!(step.command, Some(SocketCommand::Close));
    Ok(())
}

#[test]
fn send_refused_before_open_and_after_terminal() -> TestResult {
    let mut driver = WebSocketFrameDriver::new();
    let Err(refusal) = driver.command_send(frame(0x09, b"early"), ResponseExpectation::None) else {
        return Err("send before open must be refused".to_string());
    };
    assert!(matches!(refusal, CommandRefusal::InvalidPhase { .. }));

    let mut driver = established_driver()?;
    let step = driver.handle_event(SocketEvent::Closed);
    assert!(matches!(step.output, DriverOutput::Terminal(_)));
    let Err(refusal) = driver.command_send(frame(0x09, b"late"), ResponseExpectation::None) else {
        return Err("send after terminal must be refused".to_string());
    };
    assert!(matches!(refusal, CommandRefusal::InvalidPhase { .. }));
    Ok(())
}

/// One closed adapter-visible trace entry: the driver step produced by an event.
fn run_trace(events: Vec<SocketEvent>) -> TestResult<Vec<DriverStep>> {
    let mut driver = WebSocketFrameDriver::new();
    driver
        .command_open()
        .map_err(|refusal| format!("trace driver must open: {refusal:?}"))?;
    Ok(events
        .into_iter()
        .map(|event| driver.handle_event(event))
        .collect())
}

#[test]
fn std_and_fake_browser_adapters_share_one_trace() -> TestResult {
    // The same closed event sequence — delivered once in the shape a blocking
    // std reader observes it and once in the shape browser callbacks deliver it
    // (F3's abnormal loss: error THEN close) — must produce byte-identical
    // command/output traces, because the driver is the only decision-maker.
    let delivery = frame(FRAME_TYPE_DELIVER, b"value");
    let std_events = vec![
        SocketEvent::Opened,
        SocketEvent::Binary(delivery.clone()),
        SocketEvent::Failed(SocketFailure::Transport),
        SocketEvent::Closed,
    ];
    let browser_events = vec![
        SocketEvent::Opened,
        SocketEvent::Binary(delivery),
        SocketEvent::Failed(SocketFailure::Transport),
        SocketEvent::Closed,
    ];
    let std_trace = run_trace(std_events)?;
    let browser_trace = run_trace(browser_events)?;
    assert_eq!(std_trace, browser_trace);

    let terminals = std_trace
        .iter()
        .filter(|step| matches!(step.output, DriverOutput::Terminal(_)))
        .count();
    assert_eq!(terminals, 1, "exactly one typed fate per driver lifetime");
    Ok(())
}
