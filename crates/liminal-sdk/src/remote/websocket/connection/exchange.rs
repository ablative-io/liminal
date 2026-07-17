//! Link-level canonical-frame exchange machinery for [`WsConnection`].
//!
//! Every function here operates on one established [`WsLink`]: it feeds
//! socket facts through the transport-neutral driver, executes emitted
//! commands, and reports failures as typed [`ExchangeError`]s. The owning
//! connection decides what a loss means (routing the typed terminal into the
//! client unit); nothing here owns reconnect, retry, or replay policy.
//!
//! [`WsConnection`]: super::WsConnection

use alloc::boxed::Box;
use alloc::collections::BTreeSet;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::time::Duration;

use liminal::protocol::{Frame, FrameType, ProtocolVersion, decode};

use crate::SdkError;

use super::super::core::{
    DriverOutput, FrameCorrelation, FrameViolation, ResponseExpectation, SocketCommand,
    SocketEvent, SocketFailure, TransportTerminal, WebSocketFrameDriver,
};
use super::super::encode_frame;
use super::super::std_socket::{IO_TIMEOUT, SocketRead, WsSocket};

/// Minimum protocol version this client advertises during the handshake.
const CLIENT_MIN_VERSION: ProtocolVersion = ProtocolVersion::new(1, 0);
/// Maximum protocol version this client advertises during the handshake.
const CLIENT_MAX_VERSION: ProtocolVersion = ProtocolVersion::new(1, 0);
/// Brief window used to detect an error reply for an otherwise-silent
/// conversation send, mirroring the TCP transport's drain contract: the server
/// replies synchronously on the connection thread, so on success it stays
/// silent and this single bounded read times out cleanly.
const CONVERSATION_DRAIN_TIMEOUT: Duration = Duration::from_millis(250);
/// Application stream id used for conversation frames.
pub(super) const APPLICATION_STREAM_ID: u32 = 1;

/// One established WebSocket link: socket, driver, and per-connection
/// conversation-open state (server conversation state is connection-scoped,
/// so the set resets with every reconnect).
pub(super) struct WsLink {
    pub(super) socket: WsSocket,
    pub(super) driver: WebSocketFrameDriver,
    pub(super) open_conversations: BTreeSet<u64>,
}

/// A failure inside one link operation.
pub(super) enum ExchangeError {
    /// The link is dead: the typed terminal must reach the client unit and
    /// the link must be dropped.
    Loss(Box<LinkLoss>),
    /// A local, non-fatal failure; the link remains usable.
    Local(SdkError),
}

/// The typed terminal and diagnostic detail of one link loss.
pub(super) struct LinkLoss {
    pub(super) terminal: TransportTerminal,
    pub(super) description: String,
}

impl ExchangeError {
    fn loss(terminal: TransportTerminal, description: String) -> Self {
        Self::Loss(Box::new(LinkLoss {
            terminal,
            description,
        }))
    }
}

/// Drives the liminal handshake (`Connect` -> `ConnectAck`) over a fresh link.
pub(super) fn handshake(link: &mut WsLink, auth_token: &[u8]) -> Result<(), ExchangeError> {
    let connect = Frame::Connect {
        flags: 0,
        min_version: CLIENT_MIN_VERSION,
        max_version: CLIENT_MAX_VERSION,
        auth_token: auth_token.to_vec(),
    };
    let bytes = encode_frame(&connect).map_err(ExchangeError::Local)?;
    match exchange_correlated(link, bytes)? {
        Frame::ConnectAck { .. } => Ok(()),
        Frame::ConnectError {
            reason_code,
            message,
            ..
        } => Err(ExchangeError::Local(SdkError::Connection {
            description: format!(
                "server rejected connection (reason {reason_code}): {}",
                message.unwrap_or_else(|| "no detail".to_string())
            ),
        })),
        other => Err(ExchangeError::Local(unexpected_response(
            "ConnectAck",
            &other,
        ))),
    }
}

/// Sends one fire-and-forget frame through the driver.
pub(super) fn send_frame(link: &mut WsLink, frame: &Frame) -> Result<(), ExchangeError> {
    let bytes = encode_frame(frame).map_err(ExchangeError::Local)?;
    dispatch_send(link, bytes, ResponseExpectation::None)
}

/// Sends one correlated request and blocks for its response frame.
pub(super) fn exchange_correlated(
    link: &mut WsLink,
    bytes: Vec<u8>,
) -> Result<Frame, ExchangeError> {
    dispatch_send(link, bytes, ResponseExpectation::Correlated)?;
    receive_correlated(link)
}

/// Emits one send command from the driver and executes it on the socket.
fn dispatch_send(
    link: &mut WsLink,
    bytes: Vec<u8>,
    expectation: ResponseExpectation,
) -> Result<(), ExchangeError> {
    let command = link
        .driver
        .command_send(bytes, expectation)
        .map_err(|refusal| {
            ExchangeError::Local(SdkError::Protocol {
                description: format!("websocket driver refused the send: {refusal:?}"),
            })
        })?;
    let SocketCommand::SendBinary(payload) = command else {
        return Err(ExchangeError::Local(SdkError::Protocol {
            description: "websocket driver emitted a non-send command for a send".to_string(),
        }));
    };
    if let Err(failure) = link.socket.send_binary(payload) {
        let step = link.driver.handle_event(SocketEvent::Failed(failure));
        if step.command == Some(SocketCommand::Close) {
            link.socket.execute_close();
        }
        let detail = link
            .socket
            .last_failure_detail()
            .unwrap_or("websocket send failed")
            .to_string();
        return Err(ExchangeError::loss(
            TransportTerminal::SocketFailed(failure),
            detail,
        ));
    }
    Ok(())
}

/// Blocks for the frame that resolves the outstanding correlated exchange,
/// draining unsolicited deliveries exactly like the TCP transport does.
fn receive_correlated(link: &mut WsLink) -> Result<Frame, ExchangeError> {
    loop {
        match read_step(link, None)? {
            LinkRead::Frame { bytes, correlation } => match correlation {
                FrameCorrelation::CorrelatedResponse => return decode_response(link, &bytes),
                FrameCorrelation::UnsolicitedDelivery => {}
                FrameCorrelation::UnsolicitedFrame => {
                    return Err(ExchangeError::Local(SdkError::Protocol {
                        description:
                            "websocket driver classified a response as unsolicited while an \
                             exchange was outstanding"
                                .to_string(),
                    }));
                }
            },
            LinkRead::TimedOut => {
                // The correlation window is broken: the positional contract
                // cannot recover, so the link terminates typed (the TCP
                // transport's timeout equally poisons its byte stream).
                let step = link
                    .driver
                    .handle_event(SocketEvent::Failed(SocketFailure::Transport));
                if step.command == Some(SocketCommand::Close) {
                    link.socket.execute_close();
                }
                return Err(ExchangeError::loss(
                    TransportTerminal::SocketFailed(SocketFailure::Transport),
                    "timed out waiting for the correlated websocket response".to_string(),
                ));
            }
        }
    }
}

/// Blocks for the next non-delivery frame with no exchange outstanding: the
/// participant receive path. Unsolicited deliveries are drained exactly like
/// the TCP transport's receive loop; a read-window expiry is a typed error
/// with the link retained (TCP parity: a participant receive timeout does not
/// poison the message-framed connection).
pub(super) fn receive_participant_frame(link: &mut WsLink) -> Result<Frame, ExchangeError> {
    loop {
        match read_step(link, None)? {
            LinkRead::TimedOut => {
                return Err(ExchangeError::Local(SdkError::Connection {
                    description: "timed out waiting for a participant websocket frame".to_string(),
                }));
            }
            LinkRead::Frame { bytes, correlation } => match correlation {
                FrameCorrelation::UnsolicitedDelivery => {}
                FrameCorrelation::CorrelatedResponse | FrameCorrelation::UnsolicitedFrame => {
                    return decode_response(link, &bytes);
                }
            },
        }
    }
}

/// One validated read outcome surfaced to the exchange loops.
enum LinkRead {
    /// A validated canonical frame with its correlation class.
    Frame {
        /// Exact canonical frame bytes.
        bytes: Vec<u8>,
        /// Correlation class assigned by the driver.
        correlation: FrameCorrelation,
    },
    /// The armed read window elapsed (only when a window was armed).
    TimedOut,
}

/// Reads one socket fact, feeds it through the driver, executes any emitted
/// command, and maps terminals to typed losses.
fn read_step(link: &mut WsLink, window: Option<Duration>) -> Result<LinkRead, ExchangeError> {
    if let Some(window) = window {
        link.socket
            .set_read_timeout(Some(window))
            .map_err(ExchangeError::Local)?;
    }
    let read = link.socket.read_event();
    if window.is_some() {
        link.socket
            .set_read_timeout(Some(IO_TIMEOUT))
            .map_err(ExchangeError::Local)?;
    }
    let event = match read {
        SocketRead::TimedOut => return Ok(LinkRead::TimedOut),
        SocketRead::Event(event) => event,
    };
    let step = link.driver.handle_event(event);
    if step.command == Some(SocketCommand::Close) {
        link.socket.execute_close();
    }
    match step.output {
        DriverOutput::Frame { bytes, correlation } => Ok(LinkRead::Frame { bytes, correlation }),
        DriverOutput::Terminal(terminal) => {
            let detail = link
                .socket
                .last_failure_detail()
                .map_or_else(|| terminal_description(&terminal), ToString::to_string);
            Err(ExchangeError::loss(terminal, detail))
        }
        DriverOutput::Opened | DriverOutput::PostTerminalIgnored(_) | DriverOutput::Refused(_) => {
            Err(ExchangeError::Local(SdkError::Protocol {
                description: format!(
                    "websocket driver produced an unexpected read output: {:?}",
                    step.output
                ),
            }))
        }
    }
}

/// Decodes one driver-validated canonical frame, requiring the decoder to
/// consume exactly the message bytes (one message, one frame).
pub(super) fn decode_response(link: &mut WsLink, bytes: &[u8]) -> Result<Frame, ExchangeError> {
    match decode(bytes) {
        Ok((frame, consumed)) if consumed == bytes.len() => Ok(frame),
        Ok((_, consumed)) => Err(undecodable_body(
            link,
            &format!(
                "canonical decode consumed {consumed} of {} websocket message bytes",
                bytes.len()
            ),
        )),
        Err(error) => Err(undecodable_body(
            link,
            &format!("canonical decode failed: {error}"),
        )),
    }
}

/// Terminates the link for a semantically undecodable body: the framing was
/// valid but the canonical codec refused the bytes, which is malformed input
/// and closes the connection like every other malformed message.
fn undecodable_body(link: &mut WsLink, detail: &str) -> ExchangeError {
    if link.driver.command_close().is_ok() {
        link.socket.execute_close();
    }
    ExchangeError::loss(
        TransportTerminal::ProtocolViolation(FrameViolation::UndecodableBody),
        format!("websocket message failed canonical decode: {detail}"),
    )
}

/// Opens the conversation on first use, surfacing any open failure before
/// recording the conversation as open.
pub(super) fn ensure_conversation_open(
    link: &mut WsLink,
    conversation_id: u64,
    subject: &str,
) -> Result<(), ExchangeError> {
    if link.open_conversations.contains(&conversation_id) {
        return Ok(());
    }
    let open = Frame::ConversationOpen {
        flags: 0,
        stream_id: APPLICATION_STREAM_ID,
        conversation_id,
        subject: subject.to_string(),
    };
    send_frame(link, &open)?;
    drain_conversation_error(link, conversation_id)?;
    link.open_conversations.insert(conversation_id);
    Ok(())
}

/// Reads a single pending reply under the brief drain window. A
/// `ConversationError` is surfaced typed; silence (timeout) is success.
pub(super) fn drain_conversation_error(
    link: &mut WsLink,
    conversation_id: u64,
) -> Result<(), ExchangeError> {
    loop {
        match read_step(link, Some(CONVERSATION_DRAIN_TIMEOUT))? {
            LinkRead::TimedOut => return Ok(()),
            LinkRead::Frame { bytes, correlation } => match correlation {
                FrameCorrelation::UnsolicitedDelivery => {}
                FrameCorrelation::CorrelatedResponse | FrameCorrelation::UnsolicitedFrame => {
                    let frame = decode_response(link, &bytes)?;
                    match frame {
                        Frame::ConversationError {
                            conversation_id: replied,
                            reason_code,
                            message,
                            ..
                        } => {
                            return Err(ExchangeError::Local(SdkError::Conversation {
                                conversation_id: replied.to_string(),
                                description: format!(
                                    "server rejected conversation {conversation_id} \
                                     (reason {reason_code}): {}",
                                    message.unwrap_or_else(|| "no detail".to_string())
                                ),
                            }));
                        }
                        other => {
                            return Err(ExchangeError::Local(unexpected_response(
                                "ConversationError or no reply",
                                &other,
                            )));
                        }
                    }
                }
            },
        }
    }
}

/// Builds a protocol error describing an unexpected response frame.
pub(in crate::remote::websocket) fn unexpected_response(
    expected: &str,
    actual: &Frame,
) -> SdkError {
    SdkError::Protocol {
        description: format!(
            "expected {expected} frame, received {:?}",
            FrameType::from(u8::from(actual.frame_type()))
        ),
    }
}

/// Human-readable description of a typed terminal (diagnostics only).
fn terminal_description(terminal: &TransportTerminal) -> String {
    match terminal {
        TransportTerminal::PeerClosed => "server closed the websocket connection".to_string(),
        TransportTerminal::CloseCompleted => "websocket close completed".to_string(),
        TransportTerminal::SocketFailed(failure) => {
            format!("websocket socket failed: {failure:?}")
        }
        TransportTerminal::ProtocolViolation(violation) => {
            format!("websocket message violated the frame contract: {violation:?}")
        }
    }
}

/// Maps a typed terminal to the SDK error taxonomy: protocol violations and
/// bound/content failures are protocol errors; socket losses are connection
/// errors. The mapping affects only the reported error class — the aggregate
/// transition was already decided by the client unit.
pub(super) fn loss_error(terminal: &TransportTerminal, description: &str) -> SdkError {
    match terminal {
        TransportTerminal::ProtocolViolation(_)
        | TransportTerminal::SocketFailed(
            SocketFailure::UnsupportedTextMessage | SocketFailure::MessageBeyondBound,
        ) => SdkError::Protocol {
            description: description.to_string(),
        },
        TransportTerminal::PeerClosed
        | TransportTerminal::CloseCompleted
        | TransportTerminal::SocketFailed(SocketFailure::Transport) => SdkError::Connection {
            description: description.to_string(),
        },
    }
}
