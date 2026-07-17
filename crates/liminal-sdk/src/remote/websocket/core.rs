//! Transport-neutral, event-driven WebSocket liminal driver (R3.1).
//!
//! The driver's inputs are the closed socket events an adapter observes
//! ([`SocketEvent`]) and its outputs are the closed commands an adapter
//! executes ([`SocketCommand`]) plus typed transport fates. It owns
//! canonical-frame validation (exactly one self-delimiting liminal frame per
//! binary message, bounded by the named product limit) and the in-flight wire
//! correlation state (at most one outstanding correlated exchange, the Q-B
//! rule). It is `no_std + alloc` and contains no platform type: the blocking
//! std adapter and the later browser adapter drive the same commands, so
//! neither owns reconnect, retry, replay, or correlation policy.
//!
//! F3 terminal discipline: the FIRST terminal event mints exactly one typed
//! [`TransportTerminal`]; every later terminal (the browser's `close` echo
//! after `error`, a duplicate close, a late message) is a typed no-op
//! ([`DriverOutput::PostTerminalIgnored`]) and never reaches the client unit.

use alloc::vec::Vec;

use liminal_protocol::wire::{FRAME_MAX, GENERIC_HEADER_LEN};

/// Wire discriminant of the server `Deliver` frame in the canonical registry.
///
/// The driver is `no_std` and cannot depend on the std-bound `liminal` crate
/// that owns `FrameType`; the parity suite pins this byte against
/// `FrameType::Deliver` so drift is impossible without a red test.
const FRAME_TYPE_DELIVER: u8 = 0x19;

/// Byte offset of the big-endian `u32` payload length in the generic header.
const PAYLOAD_LENGTH_OFFSET: usize = 6;

/// Closed socket-failure classes an adapter may report.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SocketFailure {
    /// The socket failed at the transport layer (I/O or WebSocket protocol).
    Transport,
    /// The peer sent a text message; the wire contract admits only binary.
    UnsupportedTextMessage,
    /// The peer declared a message beyond the pinned reassembly bound (F2).
    MessageBeyondBound,
}

/// Closed socket events an adapter feeds into the driver.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SocketEvent {
    /// The socket completed its transport-level open.
    Opened,
    /// One complete, reassembled binary message.
    Binary(Vec<u8>),
    /// The socket closed cleanly.
    Closed,
    /// The socket failed with a typed failure class.
    Failed(SocketFailure),
}

/// Closed commands the driver emits for an adapter to execute.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SocketCommand {
    /// Open the transport-level socket.
    Open,
    /// Send one complete binary message.
    SendBinary(Vec<u8>),
    /// Close the transport-level socket.
    Close,
}

/// Typed canonical-frame violations the driver detects on inbound messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameViolation {
    /// The binary message carried no bytes at all.
    EmptyMessage,
    /// The message ended before the ten-byte generic header completed.
    TruncatedHeader {
        /// Bytes actually present.
        length: usize,
    },
    /// The declared complete frame exceeds the active liminal frame bound.
    DeclaredBeyondBound {
        /// Declared complete-frame size in bytes.
        declared_total: u64,
        /// The active bound the declaration exceeded.
        bound: u64,
    },
    /// The message ended before the declared body completed.
    TruncatedBody {
        /// Declared complete-frame size in bytes.
        declared_total: u64,
        /// Bytes actually present.
        actual: u64,
    },
    /// The message carried bytes past the declared frame (including a second
    /// concatenated frame).
    TrailingBytes {
        /// Declared complete-frame size in bytes.
        declared_total: u64,
        /// Bytes actually present.
        actual: u64,
    },
    /// The framing was valid but the canonical codec refused the body.
    ///
    /// This variant is minted by the codec-owning adapter layer, never by the
    /// `no_std` driver (which validates the self-delimiting header shape and
    /// deliberately does not decode bodies).
    UndecodableBody,
}

/// The one typed terminal fate a driver lifetime mints (F3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportTerminal {
    /// The peer closed the connection cleanly without a local close command.
    PeerClosed,
    /// A locally commanded close completed with the echoed close event.
    CloseCompleted,
    /// The socket failed with a typed failure class.
    SocketFailed(SocketFailure),
    /// An inbound message violated the canonical-frame contract.
    ProtocolViolation(FrameViolation),
}

/// Driver lifecycle phase, exposed read-only for adapters and tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DriverPhase {
    /// No open has been commanded.
    Idle,
    /// The open command was emitted and no `Opened` event has arrived.
    Opening,
    /// The socket is established.
    Established,
    /// A close was commanded and its echoed close event is pending.
    Closing,
    /// A terminal fate was minted; every further event is a typed no-op.
    Terminated,
}

/// Correlation class of one validated inbound frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameCorrelation {
    /// The frame resolves the single outstanding correlated exchange.
    CorrelatedResponse,
    /// A server `Deliver` frame, never correlated to an exchange.
    UnsolicitedDelivery,
    /// A non-`Deliver` frame that arrived with no exchange outstanding.
    UnsolicitedFrame,
}

/// Kind of event observed after the terminal fate was already minted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PostTerminalEvent {
    /// A late `Opened` event.
    Opened,
    /// A late binary message.
    Binary,
    /// A late close event (the F3 `error`-then-`close` echo).
    Closed,
    /// A late failure event.
    Failed,
}

/// Typed refusal for an event that is illegal in the current phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventRefusal {
    /// `Opened` arrived while no open command was outstanding.
    OpenedWithoutOpenCommand,
    /// A binary message arrived before the socket was established.
    BinaryBeforeEstablished,
}

/// Whether an outbound send expects a correlated response frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResponseExpectation {
    /// Fire-and-forget: no response frame is correlated to this send.
    None,
    /// The next non-`Deliver` inbound frame resolves this exchange.
    Correlated,
}

/// Typed refusal for a command that is illegal in the current driver state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandRefusal {
    /// The command is not legal in the current phase.
    InvalidPhase {
        /// The refusing phase.
        phase: DriverPhase,
    },
    /// A correlated exchange is already outstanding (the Q-B rule).
    ExchangeOutstanding,
}

/// One decision output of the driver.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DriverOutput {
    /// The socket proved open; the binding may record `Connected`.
    Opened,
    /// One validated canonical frame with its correlation class.
    Frame {
        /// The exact canonical frame bytes (header plus declared body).
        bytes: Vec<u8>,
        /// Correlation class of this frame.
        correlation: FrameCorrelation,
    },
    /// The one typed terminal fate of this driver lifetime.
    Terminal(TransportTerminal),
    /// A typed post-terminal no-op (F3); never reaches the client unit.
    PostTerminalIgnored(PostTerminalEvent),
    /// A typed refusal of an event illegal in the current phase.
    Refused(EventRefusal),
}

/// One driver step: the decision output plus at most one emitted command.
#[derive(Clone, Debug, PartialEq, Eq)]
#[must_use]
pub struct DriverStep {
    /// The decision output for the handled event.
    pub output: DriverOutput,
    /// A command the adapter must execute, when one was emitted.
    pub command: Option<SocketCommand>,
}

impl DriverStep {
    const fn output(output: DriverOutput) -> Self {
        Self {
            output,
            command: None,
        }
    }

    const fn with_command(output: DriverOutput, command: SocketCommand) -> Self {
        Self {
            output,
            command: Some(command),
        }
    }
}

/// Transport-neutral WebSocket liminal driver.
///
/// One driver owns one socket lifetime: it opens at most once and mints at
/// most one terminal fate. Reconnecting means a fresh driver under a fresh
/// aggregate-issued authorization — the driver itself can never re-open.
#[derive(Debug)]
pub struct WebSocketFrameDriver {
    phase: DriverPhase,
    exchange_outstanding: bool,
}

impl WebSocketFrameDriver {
    /// Creates an idle driver bound to the canonical liminal frame bound.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            phase: DriverPhase::Idle,
            exchange_outstanding: false,
        }
    }

    /// Current lifecycle phase.
    #[must_use]
    pub const fn phase(&self) -> DriverPhase {
        self.phase
    }

    /// Reports whether one correlated exchange is outstanding.
    #[must_use]
    pub const fn has_outstanding_exchange(&self) -> bool {
        self.exchange_outstanding
    }

    /// Emits the single open command of this driver lifetime.
    ///
    /// # Errors
    ///
    /// Returns [`CommandRefusal::InvalidPhase`] unless the driver is idle.
    pub const fn command_open(&mut self) -> Result<SocketCommand, CommandRefusal> {
        match self.phase {
            DriverPhase::Idle => {
                self.phase = DriverPhase::Opening;
                Ok(SocketCommand::Open)
            }
            DriverPhase::Opening
            | DriverPhase::Established
            | DriverPhase::Closing
            | DriverPhase::Terminated => Err(CommandRefusal::InvalidPhase { phase: self.phase }),
        }
    }

    /// Emits a send command for one canonical frame image.
    ///
    /// # Errors
    ///
    /// Returns [`CommandRefusal::InvalidPhase`] unless the socket is
    /// established, and [`CommandRefusal::ExchangeOutstanding`] when a
    /// correlated exchange is already in flight (the Q-B rule).
    pub fn command_send(
        &mut self,
        bytes: Vec<u8>,
        expectation: ResponseExpectation,
    ) -> Result<SocketCommand, CommandRefusal> {
        if self.phase != DriverPhase::Established {
            return Err(CommandRefusal::InvalidPhase { phase: self.phase });
        }
        match expectation {
            ResponseExpectation::Correlated => {
                if self.exchange_outstanding {
                    return Err(CommandRefusal::ExchangeOutstanding);
                }
                self.exchange_outstanding = true;
            }
            ResponseExpectation::None => {}
        }
        Ok(SocketCommand::SendBinary(bytes))
    }

    /// Emits the close command, moving the driver into the closing phase.
    ///
    /// The commanded close is not itself terminal: the echoed close event
    /// mints [`TransportTerminal::CloseCompleted`] (F3).
    ///
    /// # Errors
    ///
    /// Returns [`CommandRefusal::InvalidPhase`] unless the socket is opening
    /// or established.
    pub const fn command_close(&mut self) -> Result<SocketCommand, CommandRefusal> {
        match self.phase {
            DriverPhase::Opening | DriverPhase::Established => {
                self.phase = DriverPhase::Closing;
                Ok(SocketCommand::Close)
            }
            DriverPhase::Idle | DriverPhase::Closing | DriverPhase::Terminated => {
                Err(CommandRefusal::InvalidPhase { phase: self.phase })
            }
        }
    }

    /// Handles one closed socket event and returns the driver's decision.
    pub fn handle_event(&mut self, event: SocketEvent) -> DriverStep {
        if self.phase == DriverPhase::Terminated {
            let kind = match event {
                SocketEvent::Opened => PostTerminalEvent::Opened,
                SocketEvent::Binary(_) => PostTerminalEvent::Binary,
                SocketEvent::Closed => PostTerminalEvent::Closed,
                SocketEvent::Failed(_) => PostTerminalEvent::Failed,
            };
            return DriverStep::output(DriverOutput::PostTerminalIgnored(kind));
        }
        match event {
            SocketEvent::Opened => self.handle_opened(),
            SocketEvent::Binary(bytes) => self.handle_binary(bytes),
            SocketEvent::Closed => self.handle_closed(),
            SocketEvent::Failed(failure) => self.handle_failed(failure),
        }
    }

    const fn handle_opened(&mut self) -> DriverStep {
        match self.phase {
            DriverPhase::Opening => {
                self.phase = DriverPhase::Established;
                DriverStep::output(DriverOutput::Opened)
            }
            DriverPhase::Idle | DriverPhase::Established | DriverPhase::Closing => {
                DriverStep::output(DriverOutput::Refused(
                    EventRefusal::OpenedWithoutOpenCommand,
                ))
            }
            DriverPhase::Terminated => unreachable_terminated(),
        }
    }

    fn handle_binary(&mut self, bytes: Vec<u8>) -> DriverStep {
        match self.phase {
            DriverPhase::Established | DriverPhase::Closing => match validate_frame(&bytes) {
                Ok(()) => {
                    let correlation = self.classify_frame(&bytes);
                    DriverStep::output(DriverOutput::Frame { bytes, correlation })
                }
                Err(violation) => self.mint_terminal(
                    TransportTerminal::ProtocolViolation(violation),
                    // A violation from the established phase must actively
                    // close the socket; after a commanded close it is already
                    // closing, so no second close command is emitted.
                    self.phase == DriverPhase::Established,
                ),
            },
            DriverPhase::Idle | DriverPhase::Opening => {
                DriverStep::output(DriverOutput::Refused(EventRefusal::BinaryBeforeEstablished))
            }
            DriverPhase::Terminated => unreachable_terminated(),
        }
    }

    const fn handle_closed(&mut self) -> DriverStep {
        let terminal = match self.phase {
            DriverPhase::Closing => TransportTerminal::CloseCompleted,
            DriverPhase::Idle | DriverPhase::Opening | DriverPhase::Established => {
                TransportTerminal::PeerClosed
            }
            DriverPhase::Terminated => return unreachable_terminated(),
        };
        self.mint_terminal(terminal, false)
    }

    const fn handle_failed(&mut self, failure: SocketFailure) -> DriverStep {
        let emit_close = !matches!(self.phase, DriverPhase::Closing);
        self.mint_terminal(TransportTerminal::SocketFailed(failure), emit_close)
    }

    /// Mints the single terminal fate and optionally commands a socket close.
    const fn mint_terminal(&mut self, terminal: TransportTerminal, emit_close: bool) -> DriverStep {
        self.phase = DriverPhase::Terminated;
        self.exchange_outstanding = false;
        if emit_close {
            DriverStep::with_command(DriverOutput::Terminal(terminal), SocketCommand::Close)
        } else {
            DriverStep::output(DriverOutput::Terminal(terminal))
        }
    }

    fn classify_frame(&mut self, bytes: &[u8]) -> FrameCorrelation {
        if bytes.first().copied() == Some(FRAME_TYPE_DELIVER) {
            return FrameCorrelation::UnsolicitedDelivery;
        }
        if self.exchange_outstanding {
            self.exchange_outstanding = false;
            return FrameCorrelation::CorrelatedResponse;
        }
        FrameCorrelation::UnsolicitedFrame
    }
}

impl Default for WebSocketFrameDriver {
    fn default() -> Self {
        Self::new()
    }
}

/// The terminated phase is filtered before per-event handling; reaching a
/// per-event handler terminated would be a driver defect, and reporting it as
/// a typed post-terminal no-op keeps the closed output contract intact.
const fn unreachable_terminated() -> DriverStep {
    DriverStep::output(DriverOutput::PostTerminalIgnored(PostTerminalEvent::Failed))
}

/// Validates that `bytes` hold exactly one canonical liminal frame.
///
/// The generic frame is self-delimiting: a ten-byte header whose bytes
/// `6..10` carry the big-endian `u32` payload length, so one complete frame
/// occupies exactly `10 + payload_length` bytes. The complete size is bounded
/// by the named product limit [`FRAME_MAX`].
fn validate_frame(bytes: &[u8]) -> Result<(), FrameViolation> {
    if bytes.is_empty() {
        return Err(FrameViolation::EmptyMessage);
    }
    if bytes.len() < GENERIC_HEADER_LEN {
        return Err(FrameViolation::TruncatedHeader {
            length: bytes.len(),
        });
    }
    let Some(length_bytes) = bytes.get(PAYLOAD_LENGTH_OFFSET..GENERIC_HEADER_LEN) else {
        return Err(FrameViolation::TruncatedHeader {
            length: bytes.len(),
        });
    };
    let Ok(length_array) = <[u8; 4]>::try_from(length_bytes) else {
        return Err(FrameViolation::TruncatedHeader {
            length: bytes.len(),
        });
    };
    let declared_payload = u64::from(u32::from_be_bytes(length_array));
    let Ok(header_len) = u64::try_from(GENERIC_HEADER_LEN) else {
        // The ten-byte header length always fits `u64`; a target where it did
        // not could not validate any frame, so the message is refused typed.
        return Err(FrameViolation::TruncatedHeader {
            length: bytes.len(),
        });
    };
    let declared_total = declared_payload.saturating_add(header_len);
    if declared_total > FRAME_MAX {
        return Err(FrameViolation::DeclaredBeyondBound {
            declared_total,
            bound: FRAME_MAX,
        });
    }
    // A message longer than `u64::MAX` bytes cannot exist on any real target;
    // saturating keeps the comparison honest (it exceeds every declaration).
    let actual = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if actual < declared_total {
        return Err(FrameViolation::TruncatedBody {
            declared_total,
            actual,
        });
    }
    if actual > declared_total {
        return Err(FrameViolation::TrailingBytes {
            declared_total,
            actual,
        });
    }
    Ok(())
}
