//! Client-side WebSocket subscription stream: the receive half of the
//! delivery pump, sibling to the TCP [`SubscriptionStream`].
//!
//! One subscription per dedicated connection (the v1 shape). The socket open
//! is authorized through the client unit exactly like the request/response
//! transport (R2.2), the background reader blocks on socket input with no
//! timer or polling loop, and teardown shuts the socket down so the blocked
//! reader exits on the socket's own typed terminal event (LAW-1: the socket
//! signals; nothing sweeps).
//!
//! [`SubscriptionStream`]: crate::SubscriptionStream

use alloc::format;
use alloc::string::ToString;
use alloc::sync::Arc;
use alloc::vec::Vec;

use core::time::Duration;

use std::net::TcpStream;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::thread::JoinHandle;

use liminal::protocol::{Frame, SchemaId, decode};
use liminal_protocol::outcome::ReconnectState;
use spin::Mutex;

use crate::SdkError;

use super::binding::{AttemptFateOutcome, OpenRequestDecision, WebSocketAuthorityBinding};
use super::connection_error;
use super::core::{
    DriverOutput, FrameCorrelation, ResponseExpectation, SocketCommand, SocketEvent,
    WebSocketFrameDriver,
};
use super::liminal_ws_message_bound;
use super::std_socket::{SocketRead, WsSocket};

/// Minimum protocol version this client advertises during the handshake.
const CLIENT_MIN_VERSION: liminal::protocol::ProtocolVersion =
    liminal::protocol::ProtocolVersion::new(1, 0);
/// Maximum protocol version this client advertises during the handshake.
const CLIENT_MAX_VERSION: liminal::protocol::ProtocolVersion =
    liminal::protocol::ProtocolVersion::new(1, 0);
/// The single application stream this subscription's deliveries ride on.
const SUBSCRIPTION_STREAM_ID: u32 = 1;
/// In-flight window advertised on subscribe (advisory in v1; TCP parity).
const SUBSCRIBE_MAX_IN_FLIGHT: u32 = 1024;

/// A message the server delivered on this WebSocket subscription.
///
/// Mirrors the TCP [`DeliveredMessage`](crate::DeliveredMessage) surface; the
/// TCP type keeps its fields private, so the WebSocket sibling carries its own
/// identical shape.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WebSocketDeliveredMessage {
    delivery_seq: u64,
    schema_id: SchemaId,
    payload: Vec<u8>,
}

impl WebSocketDeliveredMessage {
    /// The per-subscription monotonic delivery sequence (starts at 1).
    #[must_use]
    pub const fn delivery_seq(&self) -> u64 {
        self.delivery_seq
    }

    /// The schema id the server selected for this subscription's stream.
    #[must_use]
    pub const fn schema_id(&self) -> SchemaId {
        self.schema_id
    }

    /// The delivered payload bytes.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Consumes the message, returning the owned payload bytes.
    #[must_use]
    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }
}

/// A connected WebSocket subscription whose background reader surfaces
/// delivered messages.
#[derive(Debug)]
pub struct WebSocketSubscriptionStream {
    /// Shutdown handle for the one socket; used only on drop.
    shutdown: TcpStream,
    subscription_id: u64,
    inbound: Receiver<WebSocketDeliveredMessage>,
    binding: Arc<Mutex<WebSocketAuthorityBinding>>,
    reader: Option<JoinHandle<()>>,
}

impl WebSocketSubscriptionStream {
    /// Connects to the `ws://` address, performs the liminal handshake,
    /// subscribes to `channel`, and starts the background reader.
    ///
    /// Deliveries the server coalesces with the `SubscribeAck` are retained
    /// and surfaced first, never dropped.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when the client unit refuses the
    /// open or the socket cannot be opened, and [`SdkError::Protocol`] when
    /// the handshake or subscribe is rejected.
    pub fn open(
        address: &str,
        channel: &str,
        accepted_schemas: Vec<SchemaId>,
    ) -> Result<Self, SdkError> {
        let message_bound = liminal_ws_message_bound()?;
        let mut binding = WebSocketAuthorityBinding::new();
        match binding.request_open() {
            OpenRequestDecision::Authorized { .. } => {}
            OpenRequestDecision::Refused(refusal) => {
                return Err(connection_error(&format!(
                    "client authority refused the subscription open: {refusal:?}"
                )));
            }
        }
        match Self::open_link(address, channel, accepted_schemas, message_bound) {
            Ok((socket, driver, subscription_id, pending)) => {
                match binding.connection_established() {
                    AttemptFateOutcome::Recorded { .. } => {}
                    AttemptFateOutcome::Refused(refusal) => {
                        return Err(SdkError::Protocol {
                            description: format!(
                                "client authority refused the Connected fate for the \
                                 subscription open: {refusal:?}"
                            ),
                        });
                    }
                }
                Self::start(socket, driver, binding, subscription_id, pending)
            }
            Err(error) => match binding.open_failed() {
                AttemptFateOutcome::Recorded { .. } => Err(error),
                AttemptFateOutcome::Refused(refusal) => Err(SdkError::Protocol {
                    description: format!(
                        "subscription open failed ({error}) and the client authority \
                             refused the Failed fate: {refusal:?}"
                    ),
                }),
            },
        }
    }

    /// Blocks up to `timeout` for the next delivered message.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when no message arrives within
    /// `timeout` or the background reader has stopped.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<WebSocketDeliveredMessage, SdkError> {
        self.inbound.recv_timeout(timeout).map_err(|error| {
            let detail = match error {
                RecvTimeoutError::Timeout => "no delivery arrived within the timeout",
                RecvTimeoutError::Disconnected => {
                    "the subscription reader stopped before a delivery arrived"
                }
            };
            connection_error(&format!("websocket subscription receive failed: {detail}"))
        })
    }

    /// The server-assigned id for this subscription.
    #[must_use]
    pub const fn subscription_id(&self) -> u64 {
        self.subscription_id
    }

    /// The client unit's reconnect state for this subscription's connection.
    #[must_use]
    pub fn reconnect_state(&self) -> ReconnectState {
        self.binding.lock().reconnect_state()
    }

    /// Performs the socket open, handshake, and subscribe exchange.
    fn open_link(
        address: &str,
        channel: &str,
        accepted_schemas: Vec<SchemaId>,
        message_bound: usize,
    ) -> Result<
        (
            WsSocket,
            WebSocketFrameDriver,
            u64,
            Vec<WebSocketDeliveredMessage>,
        ),
        SdkError,
    > {
        let mut driver = WebSocketFrameDriver::new();
        let command = driver
            .command_open()
            .map_err(|refusal| SdkError::Protocol {
                description: format!("subscription driver refused its first open: {refusal:?}"),
            })?;
        if command != SocketCommand::Open {
            return Err(SdkError::Protocol {
                description: "subscription driver emitted a non-open first command".to_string(),
            });
        }
        let mut socket = WsSocket::connect(address, message_bound)?;
        let step = driver.handle_event(SocketEvent::Opened);
        if step.output != DriverOutput::Opened {
            return Err(SdkError::Protocol {
                description: format!("subscription driver refused the opened socket: {step:?}"),
            });
        }

        let mut pending = Vec::new();
        let connect = Frame::Connect {
            flags: 0,
            min_version: CLIENT_MIN_VERSION,
            max_version: CLIENT_MAX_VERSION,
            auth_token: Vec::new(),
        };
        match setup_exchange(&mut socket, &mut driver, &connect, &mut pending)? {
            Frame::ConnectAck { .. } => {}
            Frame::ConnectError {
                reason_code,
                message,
                ..
            } => {
                return Err(connection_error(&format!(
                    "server rejected subscription connection (reason {reason_code}): {}",
                    message.unwrap_or_else(|| "no detail".to_string())
                )));
            }
            other => {
                return Err(unexpected_setup_frame("ConnectAck", &other));
            }
        }

        let subscribe = Frame::Subscribe {
            flags: 0,
            stream_id: SUBSCRIPTION_STREAM_ID,
            channel: channel.to_string(),
            accepted_schemas,
            max_in_flight: SUBSCRIBE_MAX_IN_FLIGHT,
        };
        let subscription_id =
            match setup_exchange(&mut socket, &mut driver, &subscribe, &mut pending)? {
                Frame::SubscribeAck {
                    subscription_id, ..
                } => subscription_id,
                Frame::SubscribeError {
                    reason_code,
                    message,
                    ..
                } => {
                    return Err(SdkError::Protocol {
                        description: format!(
                            "server rejected subscribe (reason {reason_code}): {}",
                            message.unwrap_or_else(|| "no detail".to_string())
                        ),
                    });
                }
                other => {
                    return Err(unexpected_setup_frame("SubscribeAck", &other));
                }
            };
        Ok((socket, driver, subscription_id, pending))
    }

    /// Starts the background reader over the established link.
    fn start(
        socket: WsSocket,
        driver: WebSocketFrameDriver,
        binding: WebSocketAuthorityBinding,
        subscription_id: u64,
        pending: Vec<WebSocketDeliveredMessage>,
    ) -> Result<Self, SdkError> {
        // The reader blocks on socket input with no read window: teardown
        // shuts the socket down, which surfaces as a typed terminal event.
        socket.set_read_timeout(None)?;
        let shutdown = socket.try_clone_stream()?;
        let binding = Arc::new(Mutex::new(binding));
        let reader_binding = Arc::clone(&binding);
        let (sender, inbound) = mpsc::channel();
        let reader = std::thread::Builder::new()
            .name("liminal-ws-subscription-reader".to_string())
            .spawn(move || run_reader(socket, driver, &reader_binding, pending, &sender))
            .map_err(|source| SdkError::Protocol {
                description: format!(
                    "failed to start websocket subscription reader thread: {source}"
                ),
            })?;
        Ok(Self {
            shutdown,
            subscription_id,
            inbound,
            binding,
            reader: Some(reader),
        })
    }
}

impl Drop for WebSocketSubscriptionStream {
    fn drop(&mut self) {
        // Best-effort teardown: shutting the shared socket down surfaces a
        // typed terminal to the blocked reader, which records it and exits.
        // The server frees the subscription on the connection's terminal fate
        // (R1.3), so no unsubscribe write is required from this half.
        self.shutdown.shutdown(std::net::Shutdown::Both).ok();
        if let Some(reader) = self.reader.take() {
            reader.join().ok();
        }
    }
}

/// Sends one setup request and blocks for its correlated control reply,
/// retaining (never dropping) any deliveries that arrive first.
fn setup_exchange(
    socket: &mut WsSocket,
    driver: &mut WebSocketFrameDriver,
    request: &Frame,
    pending: &mut Vec<WebSocketDeliveredMessage>,
) -> Result<Frame, SdkError> {
    let bytes = super::encode_frame(request)?;
    let command = driver
        .command_send(bytes, ResponseExpectation::Correlated)
        .map_err(|refusal| SdkError::Protocol {
            description: format!("subscription driver refused the setup send: {refusal:?}"),
        })?;
    let SocketCommand::SendBinary(payload) = command else {
        return Err(SdkError::Protocol {
            description: "subscription driver emitted a non-send command for a send".to_string(),
        });
    };
    if let Err(failure) = socket.send_binary(payload) {
        let step = driver.handle_event(SocketEvent::Failed(failure));
        if step.command == Some(SocketCommand::Close) {
            socket.execute_close();
        }
        return Err(connection_error(&format!(
            "failed to send subscription setup frame: {}",
            socket
                .last_failure_detail()
                .unwrap_or("websocket send failed")
        )));
    }
    loop {
        let event = match socket.read_event() {
            SocketRead::TimedOut => {
                return Err(connection_error(
                    "subscription connection timed out waiting for a control-frame reply",
                ));
            }
            SocketRead::Event(event) => event,
        };
        let step = driver.handle_event(event);
        if step.command == Some(SocketCommand::Close) {
            socket.execute_close();
        }
        match step.output {
            DriverOutput::Frame { bytes, correlation } => {
                let frame = decode_message(&bytes)?;
                match correlation {
                    FrameCorrelation::UnsolicitedDelivery => {
                        if let Some(message) = delivered_message(frame) {
                            pending.push(message);
                        }
                    }
                    FrameCorrelation::CorrelatedResponse | FrameCorrelation::UnsolicitedFrame => {
                        return Ok(frame);
                    }
                }
            }
            DriverOutput::Terminal(terminal) => {
                return Err(connection_error(&format!(
                    "subscription connection terminated during setup: {terminal:?}"
                )));
            }
            DriverOutput::Opened
            | DriverOutput::PostTerminalIgnored(_)
            | DriverOutput::Refused(_) => {
                return Err(SdkError::Protocol {
                    description: format!(
                        "subscription driver produced an unexpected setup output: {:?}",
                        step.output
                    ),
                });
            }
        }
    }
}

/// Background loop: feeds every socket fact through the driver and surfaces
/// each delivery. Ends on the link's typed terminal fate (recorded into the
/// client unit) or when the receiver is dropped.
fn run_reader(
    mut socket: WsSocket,
    mut driver: WebSocketFrameDriver,
    binding: &Mutex<WebSocketAuthorityBinding>,
    pending: Vec<WebSocketDeliveredMessage>,
    sender: &Sender<WebSocketDeliveredMessage>,
) {
    for message in pending {
        if sender.send(message).is_err() {
            close_link(&mut socket, &mut driver);
            return;
        }
    }
    loop {
        let event = match socket.read_event() {
            // No read window is armed on the reader socket; a timeout here
            // means the OS returned early, and re-entering the blocking read
            // is the only correct continuation (not a poll: no interval).
            SocketRead::TimedOut => continue,
            SocketRead::Event(event) => event,
        };
        let step = driver.handle_event(event);
        if step.command == Some(SocketCommand::Close) {
            socket.execute_close();
        }
        match step.output {
            DriverOutput::Frame { bytes, correlation } => match correlation {
                FrameCorrelation::UnsolicitedDelivery => {
                    let Ok(frame) = decode_message(&bytes) else {
                        // Malformed input closes the connection.
                        close_link(&mut socket, &mut driver);
                        continue;
                    };
                    if let Some(message) = delivered_message(frame) {
                        if sender.send(message).is_err() {
                            close_link(&mut socket, &mut driver);
                            return;
                        }
                    }
                }
                FrameCorrelation::CorrelatedResponse | FrameCorrelation::UnsolicitedFrame => {
                    match decode_message(&bytes) {
                        Ok(Frame::Disconnect { .. }) => {
                            // A server Disconnect ends the subscription
                            // cleanly; commanding close lets the echoed close
                            // event mint the one typed terminal below.
                            close_link(&mut socket, &mut driver);
                        }
                        // Any other frame on a subscription connection is
                        // unexpected; it is ignored (TCP parity) so a stray
                        // frame cannot silently drop subsequent deliveries.
                        Ok(_) => {}
                        Err(_) => {
                            close_link(&mut socket, &mut driver);
                        }
                    }
                }
            },
            DriverOutput::Terminal(terminal) => {
                // The one typed fate of this link enters the client unit;
                // the dropped sender signals consumers that the pump ended.
                let _outcome = binding.lock().established_terminal(&terminal);
                return;
            }
            DriverOutput::PostTerminalIgnored(_) => return,
            DriverOutput::Opened | DriverOutput::Refused(_) => {}
        }
    }
}

/// Commands a close (when still legal) and executes it on the socket.
fn close_link(socket: &mut WsSocket, driver: &mut WebSocketFrameDriver) {
    if driver.command_close().is_ok() {
        socket.execute_close();
    }
}

/// Decodes one driver-validated message into its canonical frame.
fn decode_message(bytes: &[u8]) -> Result<Frame, SdkError> {
    match decode(bytes) {
        Ok((frame, consumed)) if consumed == bytes.len() => Ok(frame),
        Ok((_, consumed)) => Err(SdkError::Protocol {
            description: format!(
                "subscription decode consumed {consumed} of {} message bytes",
                bytes.len()
            ),
        }),
        Err(error) => Err(SdkError::Protocol {
            description: format!("subscription wire codec error: {error}"),
        }),
    }
}

/// Maps a `Deliver` frame to its delivered message; other frames map to none.
fn delivered_message(frame: Frame) -> Option<WebSocketDeliveredMessage> {
    match frame {
        Frame::Deliver {
            delivery_seq,
            envelope,
            ..
        } => Some(WebSocketDeliveredMessage {
            delivery_seq,
            schema_id: envelope.schema_id,
            payload: envelope.payload,
        }),
        _ => None,
    }
}

/// Builds a protocol error describing an unexpected setup response frame.
fn unexpected_setup_frame(expected: &str, actual: &Frame) -> SdkError {
    SdkError::Protocol {
        description: format!(
            "expected {expected} during subscription setup, received {:?}",
            actual.frame_type()
        ),
    }
}
