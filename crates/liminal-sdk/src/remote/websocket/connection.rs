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

use crate::SdkError;

use self::exchange::{
    APPLICATION_STREAM_ID, ExchangeError, LinkLoss, WsLink, drain_conversation_error,
    ensure_conversation_open, exchange_correlated, handshake, loss_error, send_frame,
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
