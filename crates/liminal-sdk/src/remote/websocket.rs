//! WebSocket transport for the remote SDK (LP-WS-TRANSPORT R2/R3, client legs).
//!
//! Layering, bottom to top:
//!
//! - [`core`] — the transport-neutral, `no_std + alloc`, event-driven liminal
//!   driver (R3.1). Closed socket events in, closed commands out; owns
//!   canonical-frame validation and in-flight wire correlation.
//! - [`binding`] — the R2.2 conduit that passes socket facts into the landed
//!   client unit (`liminal-protocol`) as typed fates and returns
//!   aggregate-made decisions; no reconnect, retry, replay, or timer policy
//!   lives outside the aggregate.
//! - The blocking std adapter, connection, and subscription stream (R2.1),
//!   which drive the same driver commands with synchronous `tungstenite`,
//!   matching the SDK's synchronous model. The later wasm leg binds the same
//!   driver to browser callbacks; it is a separately dispatched delivery leg.
//!
//! The transport carries the canonical liminal wire protocol: one encoded
//! frame is exactly one binary WebSocket message, encoded and decoded by
//! `liminal::protocol` — there is no WS-specific codec and no protocol
//! translation.

pub mod binding;
pub mod core;

#[cfg(feature = "std")]
mod connection;
#[cfg(feature = "std")]
mod participant;
#[cfg(feature = "std")]
mod std_socket;
#[cfg(feature = "std")]
mod subscription;

pub use binding::{
    AttemptFateOutcome, AttemptFateRefusal, DetachLossOutcome, LossRecordOutcome,
    LossRecordRefusal, OpenRequestDecision, OpenRequestRefusal, WebSocketAuthorityBinding,
};
pub use core::{
    CommandRefusal, DriverOutput, DriverPhase, DriverStep, EventRefusal, FrameCorrelation,
    FrameViolation, PostTerminalEvent, ResponseExpectation, SocketCommand, SocketEvent,
    SocketFailure, TransportTerminal, WebSocketFrameDriver,
};
#[cfg(feature = "std")]
pub use subscription::{WebSocketDeliveredMessage, WebSocketSubscriptionStream};

use alloc::format;

use liminal_protocol::wire::FRAME_MAX;

use crate::SdkError;

/// The F2 reassembly bound: the active liminal frame bound, derived from the
/// protocol's named product limit [`FRAME_MAX`] (ten-byte header plus the
/// generic `u32` payload ceiling).
///
/// Both `max_message_size` and `max_frame_size` of the client WebSocket are
/// pinned to this exact value, so an oversize-declared message fails at the
/// pinned bound from its declared length — never after allocation of the
/// library's 64 MiB default buffer, and never at a WebSocket-invented limit
/// tighter than what the same frame would be allowed over TCP.
///
/// # Errors
///
/// Returns [`SdkError::Protocol`] when the build target's `usize` cannot
/// represent the bound (a 32-bit target). Refusing to connect is the only
/// honest option: silently clamping would change which canonical frames the
/// transport admits.
pub fn liminal_ws_message_bound() -> Result<usize, SdkError> {
    usize::try_from(FRAME_MAX).map_err(|_| SdkError::Protocol {
        description: format!(
            "websocket transport cannot start: this target's usize cannot represent the \
             liminal frame bound of {FRAME_MAX} bytes"
        ),
    })
}

/// Builds a connection error with the given description.
#[cfg(feature = "std")]
pub(crate) fn connection_error(description: &str) -> SdkError {
    use alloc::string::ToString;
    SdkError::Connection {
        description: description.to_string(),
    }
}

/// Encodes one canonical frame into its exact byte image.
#[cfg(feature = "std")]
fn encode_frame(frame: &liminal::protocol::Frame) -> Result<alloc::vec::Vec<u8>, SdkError> {
    use liminal::protocol::{encode, encoded_len};
    let len = encoded_len(frame).map_err(|error| SdkError::Protocol {
        description: format!("wire codec error: {error}"),
    })?;
    let mut bytes = alloc::vec![0_u8; len];
    let written = encode(frame, &mut bytes).map_err(|error| SdkError::Protocol {
        description: format!("wire codec error: {error}"),
    })?;
    if written != bytes.len() {
        return Err(SdkError::Protocol {
            description: "wire encoder reported an invalid byte count".to_string(),
        });
    }
    Ok(bytes)
}

#[cfg(feature = "std")]
mod transport {
    //! The [`RemoteTransport`] implementation over one [`WsConnection`].

    use alloc::format;
    use alloc::string::ToString;
    use alloc::sync::Arc;
    use alloc::vec::Vec;
    use core::fmt;

    use liminal::protocol::{
        CausalContext, Frame, MessageEnvelope, PUBLISH_DELIVERED_FLAG,
        PUBLISH_IDEMPOTENCY_KEY_FLAG, SchemaId,
    };
    use liminal_protocol::outcome::ReconnectState;
    use spin::Mutex;

    use crate::remote::ServerAddress;
    use crate::remote::participant::ParticipantResponseProvenance;
    use crate::remote::protocol::{
        ParticipantRemoteTransport, ParticipantTransportFrame, RemoteTransport,
        WireConversationRequest, WirePublishRequest, WireResumeRequest, WireSubscribeRequest,
    };
    use crate::{DeliveryAck, PressureResponse, SdkError};

    use super::connection::WsConnection;
    use super::liminal_ws_message_bound;

    /// Application stream id used for non-subscription application frames.
    const APPLICATION_STREAM_ID: u32 = 1;
    /// In-flight credit advertised on subscribe; one keeps strict pacing.
    const DEFAULT_MAX_IN_FLIGHT: u32 = 1;
    /// Schema id used for payloads whose schema is not carried on the wire.
    const SCHEMALESS_SCHEMA: &[u8] = &[];

    /// Real WebSocket transport that exchanges canonical wire frames with a
    /// liminal server over the sibling WebSocket acceptor.
    ///
    /// The frame construction and response mapping deliberately mirror the
    /// TCP transport line for line; the cross-transport byte-identity and
    /// behavioral parity tests pin the two implementations together so they
    /// cannot drift apart silently.
    pub struct WebSocketRemoteTransport {
        connection: Arc<Mutex<WsConnection>>,
    }

    impl fmt::Debug for WebSocketRemoteTransport {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter
                .debug_struct("WebSocketRemoteTransport")
                .finish_non_exhaustive()
        }
    }

    impl WebSocketRemoteTransport {
        /// Connects to the `ws://` server address, completes the WebSocket
        /// upgrade and liminal handshake, and returns a ready transport.
        ///
        /// # Errors
        ///
        /// Returns [`SdkError::Connection`] when the address is not a usable
        /// `ws://` URL, the client unit refuses the open, the socket cannot
        /// be established, or the handshake is rejected, and
        /// [`SdkError::Protocol`] when frames cannot be encoded or decoded.
        pub fn connect(server_address: &ServerAddress) -> Result<Self, SdkError> {
            Self::connect_with_auth(server_address, &[])
        }

        /// Connects and handshakes carrying `auth_token`, for a server gated
        /// by an `[auth]` section. Additive to [`connect`]; an empty token is
        /// equivalent to it.
        ///
        /// # Errors
        ///
        /// Returns [`SdkError::Connection`] when the connection cannot be
        /// established or the token is rejected, and [`SdkError::Protocol`]
        /// when the handshake frames cannot be encoded or sent.
        ///
        /// [`connect`]: Self::connect
        pub fn connect_with_auth(
            server_address: &ServerAddress,
            auth_token: &[u8],
        ) -> Result<Self, SdkError> {
            let bound = liminal_ws_message_bound()?;
            let connection = WsConnection::connect(server_address.as_str(), auth_token, bound)?;
            Ok(Self {
                connection: Arc::new(Mutex::new(connection)),
            })
        }

        /// Performs one authorized reconnect open through the client unit's
        /// typed permit path (R2.2): a permit retained from the established
        /// loss — or a fresh explicit caller action — authorizes exactly one
        /// real open. There is no automatic retry and no timer.
        ///
        /// # Errors
        ///
        /// Returns [`SdkError::Connection`] when the transport is still
        /// connected, the client unit refuses the open, or the open fails
        /// (parking the aggregate without retry authority).
        pub fn reconnect(&self) -> Result<(), SdkError> {
            self.connection.lock().reconnect()
        }

        /// Reports the client unit's reconnect state for this transport.
        #[must_use]
        pub fn reconnect_state(&self) -> ReconnectState {
            self.connection.lock().reconnect_state()
        }

        fn round_trip(&self, request: &Frame) -> Result<Frame, SdkError> {
            let mut connection = self.connection.lock();
            connection.round_trip(request)
        }
    }

    impl ParticipantRemoteTransport for WebSocketRemoteTransport {
        fn send_participant(
            &self,
            _server_address: &ServerAddress,
            request: &liminal_protocol::wire::ClientRequest,
        ) -> Result<ParticipantResponseProvenance, SdkError> {
            self.connection.lock().send_participant(request)
        }

        fn receive_participant(
            &self,
            _server_address: &ServerAddress,
        ) -> Result<ParticipantTransportFrame, SdkError> {
            let (frame, provenance) = self.connection.lock().receive_participant()?;
            Ok(ParticipantTransportFrame { frame, provenance })
        }

        fn reconnect_participant(
            &self,
            _server_address: &ServerAddress,
        ) -> Result<ParticipantResponseProvenance, SdkError> {
            self.connection.lock().reconnect_participant()
        }
    }

    impl RemoteTransport for WebSocketRemoteTransport {
        fn publish(
            &self,
            _server_address: &ServerAddress,
            request: &WirePublishRequest,
        ) -> Result<PressureResponse, SdkError> {
            let frame = build_publish_frame(request);
            let response = self.round_trip(&frame)?;
            publish_response(response)
        }

        fn publish_with_delivery(
            &self,
            _server_address: &ServerAddress,
            request: &WirePublishRequest,
        ) -> Result<DeliveryAck, SdkError> {
            let frame = build_publish_frame(request);
            let response = self.round_trip(&frame)?;
            publish_delivery_response(response)
        }

        /// Subscribes over the shared request/response connection, with the
        /// same v1 pooled-subscribe caveat as the TCP transport: channel
        /// deliveries are consumed through a dedicated
        /// [`WebSocketSubscriptionStream`](super::WebSocketSubscriptionStream),
        /// and the pooled subscribe serves as the delivery-ack signal.
        fn subscribe(
            &self,
            _server_address: &ServerAddress,
            request: &WireSubscribeRequest,
        ) -> Result<(), SdkError> {
            let frame = Frame::Subscribe {
                flags: 0,
                stream_id: request.stream_id(),
                channel: request.channel().to_string(),
                // An empty accepted-schema list lets the server select the
                // channel's configured schema (the negotiation contract).
                accepted_schemas: Vec::new(),
                max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            };
            let response = self.round_trip(&frame)?;
            subscribe_response(response)
        }

        fn send_conversation(
            &self,
            _server_address: &ServerAddress,
            request: &WireConversationRequest,
        ) -> Result<(), SdkError> {
            let conversation_label = request.conversation_id().as_str();
            let conversation_id = conversation_wire_id(conversation_label);
            let envelope = build_envelope(SCHEMALESS_SCHEMA, request.payload());
            let mut connection = self.connection.lock();
            connection.send_conversation_message(conversation_id, conversation_label, envelope)
        }

        fn request_reply_conversation(
            &self,
            _server_address: &ServerAddress,
            request: &WireConversationRequest,
        ) -> Result<Vec<u8>, SdkError> {
            let conversation_label = request.conversation_id().as_str();
            let conversation_id = conversation_wire_id(conversation_label);
            let envelope = build_envelope(SCHEMALESS_SCHEMA, request.payload());
            let mut connection = self.connection.lock();
            connection.conversation_request_reply(conversation_id, conversation_label, envelope)
        }

        fn resume(
            &self,
            _server_address: &ServerAddress,
            request: &WireResumeRequest,
        ) -> Result<(), SdkError> {
            // The wire protocol has no resume frame (the server replays a
            // subscription only when the SDK re-issues its Subscribe), so
            // this transport surfaces the same typed refusal as TCP instead
            // of reporting success while dropping the resume intent.
            let _ = (request.subscription_id(), request.resume_from_sequence());
            Err(SdkError::Protocol {
                description:
                    "resume is not yet supported over the WebSocket transport; re-subscribe to \
                     trigger server replay"
                        .to_string(),
            })
        }
    }

    fn build_envelope(schema_bytes: &[u8], payload: &[u8]) -> MessageEnvelope {
        MessageEnvelope::new(
            schema_id_from_bytes(schema_bytes),
            CausalContext::independent(),
            payload.to_vec(),
        )
    }

    /// Derives a stable 32-byte schema id from arbitrary schema bytes via
    /// FNV-1a (byte-identical to the TCP transport's derivation, pinned by
    /// the cross-transport byte-identity test).
    fn schema_id_from_bytes(schema_bytes: &[u8]) -> SchemaId {
        let mut id = [0_u8; SchemaId::WIRE_LEN];
        let mut hash = fnv1a(schema_bytes).to_be_bytes();
        for (index, slot) in id.iter_mut().enumerate() {
            *slot = hash[index % hash.len()];
            if index % hash.len() == hash.len() - 1 {
                hash = fnv1a(&hash).to_be_bytes();
            }
        }
        SchemaId::new(id)
    }

    fn conversation_wire_id(conversation_id: &str) -> u64 {
        fnv1a(conversation_id.as_bytes())
    }

    /// FNV-1a 64-bit hash, used only for deterministic wire-id derivation.
    fn fnv1a(bytes: &[u8]) -> u64 {
        const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
        const PRIME: u64 = 0x0000_0100_0000_01b3;
        let mut hash = OFFSET_BASIS;
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }
        hash
    }

    /// Builds the wire `Publish` frame, attaching the idempotency key (and
    /// its flag) only when the request carries one, keeping a no-key publish
    /// byte-identical to the TCP transport's layout.
    fn build_publish_frame(request: &WirePublishRequest) -> Frame {
        let envelope = build_envelope(request.schema().schema.as_ref(), request.payload());
        let flags = match request.idempotency_key() {
            Some(_) => PUBLISH_IDEMPOTENCY_KEY_FLAG,
            None => 0,
        };
        Frame::Publish {
            flags,
            stream_id: APPLICATION_STREAM_ID,
            channel: request.channel().to_string(),
            envelope,
            idempotency_key: request.idempotency_key().map(ToString::to_string),
        }
    }

    fn publish_response(frame: Frame) -> Result<PressureResponse, SdkError> {
        match frame {
            Frame::PublishAck { .. } => Ok(PressureResponse::Accept),
            Frame::PublishError {
                reason_code,
                message,
                ..
            } => Err(SdkError::Backpressure {
                reason: format!(
                    "server rejected publish (reason {reason_code}): {}",
                    message.unwrap_or_else(|| "no detail".to_string())
                ),
            }),
            other => Err(super::connection::unexpected_response("PublishAck", &other)),
        }
    }

    /// Maps a publish ack into a genuine delivery ack via the
    /// `PUBLISH_DELIVERED_FLAG` bit, exactly like the TCP transport.
    fn publish_delivery_response(frame: Frame) -> Result<DeliveryAck, SdkError> {
        match frame {
            Frame::PublishAck { flags, .. } => {
                let accepted = flags & PUBLISH_DELIVERED_FLAG != 0;
                Ok(DeliveryAck::new(PressureResponse::Accept, accepted))
            }
            Frame::PublishError {
                reason_code,
                message,
                ..
            } => Err(SdkError::Backpressure {
                reason: format!(
                    "server rejected publish (reason {reason_code}): {}",
                    message.unwrap_or_else(|| "no detail".to_string())
                ),
            }),
            other => Err(super::connection::unexpected_response("PublishAck", &other)),
        }
    }

    fn subscribe_response(frame: Frame) -> Result<(), SdkError> {
        match frame {
            Frame::SubscribeAck { .. } => Ok(()),
            Frame::SubscribeError {
                reason_code,
                message,
                ..
            } => Err(SdkError::Protocol {
                description: format!(
                    "server rejected subscribe (reason {reason_code}): {}",
                    message.unwrap_or_else(|| "no detail".to_string())
                ),
            }),
            other => Err(super::connection::unexpected_response(
                "SubscribeAck",
                &other,
            )),
        }
    }
}

#[cfg(feature = "std")]
pub use transport::WebSocketRemoteTransport;
