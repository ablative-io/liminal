//! Socket ownership and canonical-frame exchange for the WebSocket transport.
//!
//! [`WsConnection`] mirrors the TCP transport's connection semantics — one
//! synchronous request/response exchange at a time, conversation open/error
//! draining, positional-plus-id correlation — while every socket fact travels
//! through the transport-neutral driver and every open/loss/fate decision is
//! returned by the client unit via [`WebSocketAuthorityBinding`] (R2.2). One
//! encoded liminal frame maps to exactly one binary WebSocket message; there
//! is no WS-specific codec.

use alloc::boxed::Box;
use alloc::collections::BTreeSet;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::time::Duration;

use liminal::protocol::{
    CONVERSATION_REPLY_REQUESTED_FLAG, Frame, FrameType, MessageEnvelope, ProtocolVersion, decode,
};

use crate::SdkError;

use super::binding::{
    AttemptFateOutcome, LossRecordOutcome, OpenRequestDecision, WebSocketAuthorityBinding,
};
use super::core::{
    DriverOutput, FrameCorrelation, FrameViolation, ResponseExpectation, SocketCommand,
    SocketEvent, SocketFailure, TransportTerminal, WebSocketFrameDriver,
};
use super::encode_frame;
use super::std_socket::{IO_TIMEOUT, SocketRead, WsSocket};

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
const APPLICATION_STREAM_ID: u32 = 1;

/// One established WebSocket link: socket, driver, and per-connection
/// conversation-open state (server conversation state is connection-scoped,
/// so the set resets with every reconnect).
struct WsLink {
    socket: WsSocket,
    driver: WebSocketFrameDriver,
    open_conversations: BTreeSet<u64>,
}

/// A failure inside one link operation.
enum ExchangeError {
    /// The link is dead: the typed terminal must reach the client unit and
    /// the link must be dropped.
    Loss(Box<LinkLoss>),
    /// A local, non-fatal failure; the link remains usable.
    Local(SdkError),
}

/// The typed terminal and diagnostic detail of one link loss.
struct LinkLoss {
    terminal: TransportTerminal,
    description: String,
}

impl ExchangeError {
    fn loss(terminal: TransportTerminal, description: String) -> Self {
        Self::Loss(Box::new(LinkLoss {
            terminal,
            description,
        }))
    }
}

/// Owns the WebSocket link lifecycle for one remote transport.
pub(super) struct WsConnection {
    url: String,
    auth_token: Vec<u8>,
    message_bound: usize,
    binding: WebSocketAuthorityBinding,
    link: Option<WsLink>,
    /// Diagnostic description of the most recent loss, enriching the typed
    /// errors returned while no link exists.
    last_loss: Option<String>,
}

impl WsConnection {
    /// Opens the authorized connection: permit, real socket open, liminal
    /// handshake, typed `Connected` fate.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when authority refuses the open, the
    /// socket cannot be opened, or the handshake is rejected.
    pub(super) fn connect(
        url: &str,
        auth_token: &[u8],
        message_bound: usize,
    ) -> Result<Self, SdkError> {
        let mut connection = Self {
            url: url.to_string(),
            auth_token: auth_token.to_vec(),
            message_bound,
            binding: WebSocketAuthorityBinding::new(),
            link: None,
            last_loss: None,
        };
        connection.open_authorized()?;
        Ok(connection)
    }

    /// Reports the client unit's reconnect state.
    pub(super) const fn reconnect_state(&self) -> liminal_protocol::outcome::ReconnectState {
        self.binding.reconnect_state()
    }

    /// Performs one authorized reconnect open (R2.2: the retained loss permit
    /// or a fresh explicit caller action authorizes exactly one real open).
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when a link is still established,
    /// authority refuses the open, or the open/handshake fails (which parks
    /// the aggregate without retry authority).
    pub(super) fn reconnect(&mut self) -> Result<(), SdkError> {
        self.open_authorized()
    }

    fn open_authorized(&mut self) -> Result<(), SdkError> {
        if self.link.is_some() {
            return Err(SdkError::Connection {
                description: "websocket transport is already connected".to_string(),
            });
        }
        match self.binding.request_open() {
            OpenRequestDecision::Authorized { .. } => {}
            OpenRequestDecision::Refused(refusal) => {
                return Err(SdkError::Connection {
                    description: format!(
                        "client authority refused the websocket open: {refusal:?}"
                    ),
                });
            }
        }
        match self.perform_open() {
            Ok(link) => {
                self.link = Some(link);
                match self.binding.connection_established() {
                    AttemptFateOutcome::Recorded { .. } => {
                        self.last_loss = None;
                        Ok(())
                    }
                    AttemptFateOutcome::Refused(refusal) => {
                        // The aggregate refused the Connected fate: the open
                        // is not authorized to stand, so tear it down typed.
                        if let Some(mut link) = self.link.take() {
                            link.socket.execute_close();
                        }
                        Err(SdkError::Protocol {
                            description: format!(
                                "client authority refused the Connected fate: {refusal:?}"
                            ),
                        })
                    }
                }
            }
            Err(error) => match self.binding.open_failed() {
                AttemptFateOutcome::Recorded { .. } => Err(error),
                AttemptFateOutcome::Refused(refusal) => Err(SdkError::Protocol {
                    description: format!(
                        "websocket open failed ({error}) and the client authority refused \
                             the Failed fate: {refusal:?}"
                    ),
                }),
            },
        }
    }

    /// Executes the driver's one open command and the liminal handshake.
    fn perform_open(&self) -> Result<WsLink, SdkError> {
        let mut driver = WebSocketFrameDriver::new();
        let command = driver
            .command_open()
            .map_err(|refusal| SdkError::Protocol {
                description: format!("websocket driver refused its first open: {refusal:?}"),
            })?;
        if command != SocketCommand::Open {
            return Err(SdkError::Protocol {
                description: "websocket driver emitted a non-open first command".to_string(),
            });
        }
        let socket = WsSocket::connect(&self.url, self.message_bound)?;
        let step = driver.handle_event(SocketEvent::Opened);
        if step.output != DriverOutput::Opened {
            return Err(SdkError::Protocol {
                description: format!("websocket driver refused the opened socket: {step:?}"),
            });
        }
        let mut link = WsLink {
            socket,
            driver,
            open_conversations: BTreeSet::new(),
        };
        match handshake(&mut link, &self.auth_token) {
            Ok(()) => Ok(link),
            Err(ExchangeError::Local(error)) => {
                link.socket.execute_close();
                Err(error)
            }
            Err(ExchangeError::Loss(loss)) => {
                link.socket.execute_close();
                Err(loss_error(&loss.terminal, &loss.description))
            }
        }
    }

    /// Sends a request frame and blocks for the correlated response frame.
    pub(super) fn round_trip(&mut self, request: &Frame) -> Result<Frame, SdkError> {
        let bytes = encode_frame(request)?;
        self.with_link(|link| exchange_correlated(link, bytes))
    }

    /// Sends a conversation message, opening the conversation first if
    /// needed, and surfaces any server `ConversationError` instead of
    /// dropping it (the server is silent on success).
    pub(super) fn send_conversation_message(
        &mut self,
        conversation_id: u64,
        subject: &str,
        envelope: MessageEnvelope,
    ) -> Result<(), SdkError> {
        let subject = subject.to_string();
        self.with_link(move |link| {
            ensure_conversation_open(link, conversation_id, &subject)?;
            let message = Frame::ConversationMessage {
                flags: 0,
                stream_id: APPLICATION_STREAM_ID,
                conversation_id,
                envelope,
            };
            send_frame(link, &message)?;
            drain_conversation_error(link, conversation_id)
        })
    }

    /// Sends a conversation request with the reply-requested flag and blocks
    /// for the correlated reply payload.
    pub(super) fn conversation_request_reply(
        &mut self,
        conversation_id: u64,
        subject: &str,
        envelope: MessageEnvelope,
    ) -> Result<Vec<u8>, SdkError> {
        let subject = subject.to_string();
        self.with_link(move |link| {
            ensure_conversation_open(link, conversation_id, &subject)?;
            let message = Frame::ConversationMessage {
                flags: CONVERSATION_REPLY_REQUESTED_FLAG,
                stream_id: APPLICATION_STREAM_ID,
                conversation_id,
                envelope,
            };
            let bytes = encode_frame(&message).map_err(ExchangeError::Local)?;
            let reply = exchange_correlated(link, bytes)?;
            match reply {
                Frame::ConversationMessage {
                    conversation_id: replied,
                    envelope,
                    ..
                } if replied == conversation_id => Ok(envelope.payload),
                Frame::ConversationError {
                    conversation_id: replied,
                    reason_code,
                    message,
                    ..
                } => Err(ExchangeError::Local(SdkError::Conversation {
                    conversation_id: replied.to_string(),
                    description: format!(
                        "server rejected conversation {conversation_id} (reason {reason_code}): {}",
                        message.unwrap_or_else(|| "no detail".to_string())
                    ),
                })),
                other => Err(ExchangeError::Local(unexpected_response(
                    "ConversationMessage reply or ConversationError",
                    &other,
                ))),
            }
        })
    }

    /// Runs one link operation, routing a loss through the client unit and
    /// dropping the dead link.
    fn with_link<T>(
        &mut self,
        operation: impl FnOnce(&mut WsLink) -> Result<T, ExchangeError>,
    ) -> Result<T, SdkError> {
        let outcome = match self.link.as_mut() {
            Some(link) => operation(link),
            None => {
                return Err(self.lost_error());
            }
        };
        match outcome {
            Ok(value) => Ok(value),
            Err(ExchangeError::Local(error)) => Err(error),
            Err(ExchangeError::Loss(loss)) => Err(self.record_loss(&loss)),
        }
    }

    /// Records one established-connection loss: the typed terminal enters the
    /// client unit, the dead link is dropped, and the typed error carries the
    /// diagnostics. Terminal variants never select aggregate transitions.
    fn record_loss(&mut self, loss: &LinkLoss) -> SdkError {
        self.link = None;
        let outcome = self.binding.established_terminal(&loss.terminal);
        let description = match outcome {
            LossRecordOutcome::PermitRetained => loss.description.clone(),
            LossRecordOutcome::Refused(refusal) => format!(
                "{} (loss fate refused by client authority: {refusal:?})",
                loss.description
            ),
        };
        self.last_loss = Some(description.clone());
        loss_error(&loss.terminal, &description)
    }

    fn lost_error(&self) -> SdkError {
        let context = self
            .last_loss
            .as_deref()
            .unwrap_or("no websocket connection is established");
        SdkError::Connection {
            description: format!("websocket connection unavailable: {context}; reconnect required"),
        }
    }
}

impl core::fmt::Debug for WsConnection {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("WsConnection")
            .field("connected", &self.link.is_some())
            .finish_non_exhaustive()
    }
}

/// Drives the liminal handshake (`Connect` -> `ConnectAck`) over a fresh link.
fn handshake(link: &mut WsLink, auth_token: &[u8]) -> Result<(), ExchangeError> {
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
fn send_frame(link: &mut WsLink, frame: &Frame) -> Result<(), ExchangeError> {
    let bytes = encode_frame(frame).map_err(ExchangeError::Local)?;
    dispatch_send(link, bytes, ResponseExpectation::None)
}

/// Sends one correlated request and blocks for its response frame.
fn exchange_correlated(link: &mut WsLink, bytes: Vec<u8>) -> Result<Frame, ExchangeError> {
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
fn decode_response(link: &mut WsLink, bytes: &[u8]) -> Result<Frame, ExchangeError> {
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
fn ensure_conversation_open(
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
fn drain_conversation_error(link: &mut WsLink, conversation_id: u64) -> Result<(), ExchangeError> {
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
pub(super) fn unexpected_response(expected: &str, actual: &Frame) -> SdkError {
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
fn loss_error(terminal: &TransportTerminal, description: &str) -> SdkError {
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
