//! R3.2 deterministic browser-mirror trace tests (LP-WS-TRANSPORT, folded r1.1).
//!
//! These tests run the browser adapter's platform-neutral conversion layer —
//! the F5 mirror — against the shared transport-neutral driver, without a
//! browser and without a socket. The mirror converts observed browser socket
//! facts (`open`, `message`, `close`, `error` callbacks) into the driver's
//! closed [`SocketEvent`] set and maps emitted [`SocketCommand`]s onto closed
//! browser socket actions. All protocol, framing, correlation, and fate logic
//! stays in the core driver: the mirror decides nothing.
//!
//! Pinned here:
//! - F4: only `ArrayBuffer` binary data is accepted; a text message surfaces
//!   as the core's typed `UnsupportedTextMessage` failure; `Blob` (the
//!   asynchronous browser default) and every other data shape is a typed
//!   transport failure.
//! - F1: the browser socket must come up extension-free; a negotiated
//!   extensions string on `open` is a typed failure, never a renegotiation.
//! - F3: the browser's abnormal `error`-then-`close` shape mints exactly one
//!   typed fate; the close echo stays a typed no-op.
//! - F5: the mirror defers canonical-frame validation to the core and has no
//!   decision of its own.

use liminal_sdk::remote::websocket::web_socket::{
    BrowserCommandRefusal, BrowserMessageData, BrowserSocketAction, action_for_command,
    close_event, error_event, message_event, open_event,
};
use liminal_sdk::remote::websocket::{
    DriverOutput, DriverPhase, DriverStep, FrameCorrelation, FrameViolation, PostTerminalEvent,
    SocketCommand, SocketEvent, SocketFailure, TransportTerminal, WebSocketFrameDriver,
};

type TestResult<T = ()> = Result<T, String>;

/// Generic liminal frame header length (ten bytes).
const HEADER_LEN: usize = 10;
/// Wire discriminant of the server `Deliver` frame.
const FRAME_TYPE_DELIVER: u8 = 0x19;
/// Wire discriminant of the `Publish` frame (an ordinary request class).
const FRAME_TYPE_PUBLISH: u8 = 0x09;

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

/// Drives a fresh driver through the mirror's extension-free `open` shape.
fn established_driver() -> TestResult<WebSocketFrameDriver> {
    let mut driver = WebSocketFrameDriver::new();
    let command = driver
        .command_open()
        .map_err(|refusal| format!("fresh driver must accept open: {refusal:?}"))?;
    if command != SocketCommand::Open {
        return Err(format!("open must emit the Open command, got {command:?}"));
    }
    let step = driver.handle_event(open_event(""));
    if step.output != DriverOutput::Opened {
        return Err(format!(
            "extension-free browser open must report Opened, got {step:?}"
        ));
    }
    Ok(driver)
}

#[test]
fn array_buffer_message_mirrors_to_binary_with_canonical_bytes() -> TestResult {
    let bytes = frame(FRAME_TYPE_DELIVER, b"delivered-value");
    let event = message_event(BrowserMessageData::ArrayBuffer(bytes.clone()));
    assert_eq!(event, SocketEvent::Binary(bytes.clone()));

    let mut driver = established_driver()?;
    let step = driver.handle_event(event);
    assert_eq!(
        step.output,
        DriverOutput::Frame {
            bytes,
            correlation: FrameCorrelation::UnsolicitedDelivery,
        }
    );
    Ok(())
}

#[test]
fn text_message_mirrors_to_the_cores_unsupported_text_failure() -> TestResult {
    // F4: the wire contract admits only binary; the mirror reports the
    // observed text fact and the CORE owns the typed failure class and fate.
    let event = message_event(BrowserMessageData::Text);
    assert_eq!(
        event,
        SocketEvent::Failed(SocketFailure::UnsupportedTextMessage)
    );

    let mut driver = established_driver()?;
    let step = driver.handle_event(event);
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
fn blob_message_is_refused_as_typed_transport_failure() -> TestResult {
    // F4: `binaryType = "arraybuffer"` is set before any subscription, so a
    // `Blob` can only mean the transport contract broke; the asynchronous
    // Blob default is not supported and never read.
    let event = message_event(BrowserMessageData::Blob);
    assert_eq!(event, SocketEvent::Failed(SocketFailure::Transport));

    let mut driver = established_driver()?;
    let step = driver.handle_event(event);
    assert_eq!(
        step.output,
        DriverOutput::Terminal(TransportTerminal::SocketFailed(SocketFailure::Transport))
    );
    assert_eq!(step.command, Some(SocketCommand::Close));
    Ok(())
}

#[test]
fn unrecognized_message_data_is_refused_as_typed_transport_failure() {
    let event = message_event(BrowserMessageData::Unrecognized);
    assert_eq!(event, SocketEvent::Failed(SocketFailure::Transport));
}

#[test]
fn extension_free_open_mirrors_to_opened() {
    assert_eq!(open_event(""), SocketEvent::Opened);
}

#[test]
fn negotiated_extension_open_is_typed_failure_and_closes() -> TestResult {
    // F1 decline-never-negotiate, observed at the browser socket: the landed
    // acceptor declines every extension offer, so a non-empty negotiated
    // extensions string proves the peer is not honoring the liminal transport
    // contract. The mirror reports the fact; the driver mints the fate and
    // actively closes the socket.
    let event = open_event("permessage-deflate");
    assert_eq!(event, SocketEvent::Failed(SocketFailure::Transport));

    let mut driver = WebSocketFrameDriver::new();
    driver
        .command_open()
        .map_err(|refusal| format!("fresh driver must accept open: {refusal:?}"))?;
    let step = driver.handle_event(event);
    assert_eq!(
        step.output,
        DriverOutput::Terminal(TransportTerminal::SocketFailed(SocketFailure::Transport))
    );
    assert_eq!(step.command, Some(SocketCommand::Close));
    assert_eq!(driver.phase(), DriverPhase::Terminated);
    Ok(())
}

#[test]
fn clean_close_mirrors_to_closed_peer_fate() -> TestResult {
    // A clean browser close (`wasClean == true`) is the peer's orderly close.
    let event = close_event(true);
    assert_eq!(event, SocketEvent::Closed);

    let mut driver = established_driver()?;
    let step = driver.handle_event(event);
    assert_eq!(
        step.output,
        DriverOutput::Terminal(TransportTerminal::PeerClosed)
    );
    Ok(())
}

#[test]
fn abnormal_close_without_error_is_typed_transport_failure() -> TestResult {
    // Close-fidelity: a `wasClean == false` close is an abnormal loss, not an
    // orderly peer close. F3's browser shape fires `error` first — making
    // this conversion a post-terminal no-op — but a lone abnormal close must
    // not misrepresent itself as clean.
    let event = close_event(false);
    assert_eq!(event, SocketEvent::Failed(SocketFailure::Transport));

    let mut driver = established_driver()?;
    let step = driver.handle_event(event);
    assert_eq!(
        step.output,
        DriverOutput::Terminal(TransportTerminal::SocketFailed(SocketFailure::Transport))
    );
    Ok(())
}

#[test]
fn f3_browser_error_then_close_echo_stays_single_fate() -> TestResult {
    // F3: abnormal browser loss always produces `error` THEN `close`. The
    // opaque error event mints the one typed fate (with no invented detail —
    // the class is Transport, nothing more); the close echo is a typed no-op.
    let mut driver = established_driver()?;
    let step = driver.handle_event(error_event());
    assert_eq!(
        step.output,
        DriverOutput::Terminal(TransportTerminal::SocketFailed(SocketFailure::Transport))
    );
    assert_eq!(step.command, Some(SocketCommand::Close));

    let echo = driver.handle_event(close_event(false));
    assert_eq!(
        echo.output,
        DriverOutput::PostTerminalIgnored(PostTerminalEvent::Failed)
    );
    assert_eq!(echo.command, None);
    assert_eq!(driver.phase(), DriverPhase::Terminated);
    Ok(())
}

#[test]
fn commanded_close_echo_completes_through_mirror() -> TestResult {
    let mut driver = established_driver()?;
    let command = driver
        .command_close()
        .map_err(|refusal| format!("established close must be accepted: {refusal:?}"))?;
    let action = action_for_command(command)
        .map_err(|refusal| format!("close command must map to an action: {refusal:?}"))?;
    assert_eq!(action, BrowserSocketAction::Close);

    let echo = driver.handle_event(close_event(true));
    assert_eq!(
        echo.output,
        DriverOutput::Terminal(TransportTerminal::CloseCompleted)
    );
    Ok(())
}

#[test]
fn mirror_defers_frame_validation_to_core() -> TestResult {
    // F5 mirror-not-implementor: the mirror converts a garbage ArrayBuffer to
    // a Binary event untouched — canonical-frame judgment belongs to the core.
    let garbage = vec![0xFF_u8; 3];
    let event = message_event(BrowserMessageData::ArrayBuffer(garbage.clone()));
    assert_eq!(event, SocketEvent::Binary(garbage));

    let mut driver = established_driver()?;
    let step = driver.handle_event(event);
    assert!(
        matches!(
            step.output,
            DriverOutput::Terminal(TransportTerminal::ProtocolViolation(
                FrameViolation::TruncatedHeader { .. }
            ))
        ),
        "the core, not the mirror, must refuse the malformed frame, got {step:?}"
    );
    Ok(())
}

#[test]
fn open_command_is_construction_not_a_runtime_action() -> TestResult {
    // In a browser the socket begins connecting at construction: the Open
    // command is executed by constructing the socket, so mapping it to a
    // runtime action is a typed refusal, never a second connect.
    let refused = action_for_command(SocketCommand::Open);
    assert_eq!(refused, Err(BrowserCommandRefusal::OpenIsConstruction));

    let bytes = frame(FRAME_TYPE_PUBLISH, b"payload");
    let send = action_for_command(SocketCommand::SendBinary(bytes.clone()))
        .map_err(|refusal| format!("send must map to an action: {refusal:?}"))?;
    assert_eq!(send, BrowserSocketAction::SendBinary(bytes));

    let close = action_for_command(SocketCommand::Close)
        .map_err(|refusal| format!("close must map to an action: {refusal:?}"))?;
    assert_eq!(close, BrowserSocketAction::Close);
    Ok(())
}

/// Runs one event sequence through a fresh opened driver, collecting steps.
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
fn browser_mirror_and_std_shapes_share_the_decision_trace() -> TestResult {
    // The same socket life — open, one delivery, one correlated response,
    // abnormal loss — expressed once as the std adapter's direct events and
    // once through the real browser mirror, must make identical decisions:
    // identical outputs up to and including the single typed fate. After the
    // fate, every step in both shapes is a typed no-op (the no-op KINDS may
    // differ: close-fidelity converts the browser's abnormal close echo as a
    // failure fact, which the terminated driver ignores just the same).
    let delivery = frame(FRAME_TYPE_DELIVER, b"value");

    let std_events = vec![
        SocketEvent::Opened,
        SocketEvent::Binary(delivery.clone()),
        SocketEvent::Failed(SocketFailure::Transport),
        SocketEvent::Closed,
    ];
    let browser_events = vec![
        open_event(""),
        message_event(BrowserMessageData::ArrayBuffer(delivery)),
        error_event(),
        close_event(false),
    ];

    let std_trace = run_trace(std_events)?;
    let browser_trace = run_trace(browser_events)?;
    assert_eq!(std_trace.len(), browser_trace.len());

    let terminal_at = |trace: &[DriverStep]| -> TestResult<usize> {
        trace
            .iter()
            .position(|step| matches!(step.output, DriverOutput::Terminal(_)))
            .ok_or_else(|| "trace must contain the one typed fate".to_string())
    };
    let std_terminal = terminal_at(&std_trace)?;
    let browser_terminal = terminal_at(&browser_trace)?;
    assert_eq!(std_terminal, browser_terminal);
    assert_eq!(
        std_trace[..=std_terminal],
        browser_trace[..=browser_terminal],
        "decisions through the fate must be identical across adapter shapes"
    );

    for trace in [&std_trace, &browser_trace] {
        let terminals = trace
            .iter()
            .filter(|step| matches!(step.output, DriverOutput::Terminal(_)))
            .count();
        assert_eq!(terminals, 1, "exactly one typed fate per driver lifetime");
        for step in &trace[std_terminal + 1..] {
            assert!(
                matches!(step.output, DriverOutput::PostTerminalIgnored(_)),
                "every post-fate step must be a typed no-op, got {step:?}"
            );
            assert_eq!(step.command, None);
        }
    }
    Ok(())
}
