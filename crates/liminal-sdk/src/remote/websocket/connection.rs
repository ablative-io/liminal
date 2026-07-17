//! Socket ownership and link lifecycle for the WebSocket transport.
//!
//! [`WsConnection`] mirrors the TCP transport's connection semantics — one
//! synchronous request/response exchange at a time, conversation open/error
//! draining, positional-plus-id correlation — while every socket fact travels
//! through the transport-neutral driver and every open/loss/fate decision is
//! returned by the client unit via [`WebSocketAuthorityBinding`] (R2.2). One
//! encoded liminal frame maps to exactly one binary WebSocket message; there
//! is no WS-specific codec. The per-link frame machinery lives in
//! [`exchange`].

mod exchange;

pub(super) use exchange::unexpected_response;

use alloc::collections::BTreeSet;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use liminal::protocol::{CONVERSATION_REPLY_REQUESTED_FLAG, Frame, MessageEnvelope};

use liminal_protocol::wire::ParticipantFrame;

use crate::SdkError;
use crate::remote::participant::ParticipantResponseProvenance;

use self::exchange::{
    APPLICATION_STREAM_ID, ExchangeError, LinkLoss, WsLink, drain_conversation_error,
    ensure_conversation_open, exchange_correlated, handshake, loss_error,
    receive_participant_frame, send_frame,
};
use super::binding::{
    AttemptFateOutcome, LossRecordOutcome, OpenRequestDecision, WebSocketAuthorityBinding,
};
use super::core::{DriverOutput, SocketCommand, SocketEvent, WebSocketFrameDriver};
use super::encode_frame;
use super::std_socket::WsSocket;

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
    /// Participant provenance for the currently established socket, counted
    /// exactly like the TCP transport's connection slot.
    provenance: ParticipantResponseProvenance,
    next_attempt_id: u64,
    next_connection_id: u64,
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
            provenance: ParticipantResponseProvenance::new(1, 1),
            next_attempt_id: 2,
            next_connection_id: 2,
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

    /// Writes one canonical participant request on the established link and
    /// returns the provenance of the socket that carried it (TCP parity).
    pub(super) fn send_participant(
        &mut self,
        request: &liminal_protocol::wire::ClientRequest,
    ) -> Result<ParticipantResponseProvenance, SdkError> {
        let frame = super::participant::request_frame(request)?;
        self.with_link(|link| send_frame(link, &frame))?;
        Ok(self.provenance)
    }

    /// Blocks for the next canonical participant frame, draining unsolicited
    /// deliveries, and returns it with the delivering socket's provenance.
    pub(super) fn receive_participant(
        &mut self,
    ) -> Result<(ParticipantFrame, ParticipantResponseProvenance), SdkError> {
        let frame = self.with_link(receive_participant_frame)?;
        let participant = super::participant::response_frame(frame)?;
        Ok((participant, self.provenance))
    }

    /// Replaces the socket for the participant machinery (TCP parity): the
    /// attempt identity is consumed before the dial, the connection identity
    /// only on success, and the fresh provenance names both.
    ///
    /// A still-established link is first closed locally and its typed
    /// terminal recorded, so the replacement open still traverses the client
    /// unit's one-permit-per-open law; the TCP transport instead drops its
    /// old socket silently, which the WebSocket unit deliberately does not.
    pub(super) fn reconnect_participant(
        &mut self,
    ) -> Result<ParticipantResponseProvenance, SdkError> {
        let attempt_id = self.next_attempt_id;
        self.next_attempt_id =
            self.next_attempt_id
                .checked_add(1)
                .ok_or_else(|| SdkError::Connection {
                    description: "participant transport attempt identity exhausted".to_string(),
                })?;
        if let Some(mut link) = self.link.take() {
            if link.driver.command_close().is_ok() {
                link.socket.execute_close();
            }
            let outcome = self
                .binding
                .established_terminal(&super::core::TransportTerminal::CloseCompleted);
            if let super::binding::LossRecordOutcome::Refused(refusal) = outcome {
                return Err(SdkError::Protocol {
                    description: format!(
                        "client authority refused the replacement-close fate: {refusal:?}"
                    ),
                });
            }
        }
        self.open_authorized()?;
        let connection_id = self.next_connection_id;
        self.next_connection_id =
            self.next_connection_id
                .checked_add(1)
                .ok_or_else(|| SdkError::Connection {
                    description: "participant transport connection identity exhausted".to_string(),
                })?;
        let provenance = ParticipantResponseProvenance::new(connection_id, attempt_id);
        self.provenance = provenance;
        Ok(provenance)
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
