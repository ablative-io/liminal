//! Real TCP transport for the remote SDK.
//!
//! Unlike [`ProtocolRemoteTransport`](super::protocol::ProtocolRemoteTransport),
//! which only exercises the SDK's framing in-process, this transport opens a real
//! `TcpStream` to a running `liminal-server`, performs the protocol handshake, and
//! exchanges canonical wire frames over the socket.
//!
//! # Blocking model
//!
//! The SDK API surface is synchronous: [`RemoteTransport`] methods return plain
//! `Result` values, and the rest of the SDK (connection pool, lifecycle) is
//! driven by ordinary blocking calls. This transport therefore uses
//! `std::net::TcpStream` in blocking mode with explicit read/write timeouts; it
//! does not introduce an async runtime. Each transport call holds a short-lived
//! connection lock for the duration of one request/response exchange.

mod connection;
mod participant;
mod push_client;
mod subscription;

pub use push_client::{OBSERVABILITY_CHANNEL, PushClient, PushWriter, PushedFrame};
pub use subscription::{DeliveredMessage, SubscriptionStream};

use alloc::format;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::fmt;

use liminal::protocol::{
    CausalContext, Frame, MessageEnvelope, PUBLISH_DELIVERED_FLAG, PUBLISH_IDEMPOTENCY_KEY_FLAG,
    SchemaId,
};
use spin::Mutex;

use crate::{DeliveryAck, PressureResponse, SdkError};

use self::connection::{Connection, unexpected_frame};
use super::ServerAddress;
use super::participant::ParticipantResponseProvenance;
use super::protocol::{
    ParticipantRemoteTransport, ParticipantTransportFrame, RemoteTransport,
    WireConversationRequest, WirePublishRequest, WireResumeRequest, WireSubscribeRequest,
};

/// Application stream id used for non-subscription application frames.
const APPLICATION_STREAM_ID: u32 = 1;
/// In-flight credit advertised on subscribe; one keeps strict pacing.
const DEFAULT_MAX_IN_FLIGHT: u32 = 1;
/// Schema id used for payloads whose schema is not carried on the wire.
const SCHEMALESS_SCHEMA: &[u8] = &[];

struct ConnectionSlot {
    connection: Connection,
    provenance: ParticipantResponseProvenance,
    next_attempt_id: u64,
    next_connection_id: u64,
}

/// Real TCP transport that exchanges canonical wire frames with a liminal server.
pub struct TcpRemoteTransport {
    connection: Arc<Mutex<ConnectionSlot>>,
    address: String,
    auth_token: Vec<u8>,
}

impl fmt::Debug for TcpRemoteTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TcpRemoteTransport")
            .finish_non_exhaustive()
    }
}

impl TcpRemoteTransport {
    /// Connects to `server_address`, completes the handshake, and returns a ready transport.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when the TCP connection cannot be
    /// established, and [`SdkError::Protocol`] when the handshake frames cannot be
    /// encoded, sent, or are rejected by the server.
    pub fn connect(server_address: &ServerAddress) -> Result<Self, SdkError> {
        Self::connect_with_auth(server_address, &[])
    }

    /// Connects and handshakes carrying `auth_token`, for a server gated by an
    /// `[auth]` section. Additive to [`connect`]; an empty token is equivalent to it.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when the TCP connection cannot be
    /// established or the server rejects the token (a `ConnectError` closes the
    /// socket), and [`SdkError::Protocol`] when the handshake frames cannot be
    /// encoded or sent.
    ///
    /// [`connect`]: Self::connect
    pub fn connect_with_auth(
        server_address: &ServerAddress,
        auth_token: &[u8],
    ) -> Result<Self, SdkError> {
        let address = server_address.as_str().to_string();
        let connection = Connection::connect_with_auth(&address, auth_token)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(ConnectionSlot {
                connection,
                provenance: ParticipantResponseProvenance::new(1, 1),
                next_attempt_id: 2,
                next_connection_id: 2,
            })),
            address,
            auth_token: auth_token.to_vec(),
        })
    }

    fn round_trip(&self, request: &Frame) -> Result<Frame, SdkError> {
        self.connection.lock().connection.round_trip(request)
    }
}

impl ParticipantRemoteTransport for TcpRemoteTransport {
    fn send_participant(
        &self,
        _server_address: &ServerAddress,
        request: &liminal_protocol::wire::ClientRequest,
    ) -> Result<ParticipantResponseProvenance, SdkError> {
        let mut slot = self.connection.lock();
        slot.connection.send_participant(request)?;
        Ok(slot.provenance)
    }

    fn receive_participant(
        &self,
        _server_address: &ServerAddress,
    ) -> Result<ParticipantTransportFrame, SdkError> {
        let mut slot = self.connection.lock();
        let frame = slot.connection.receive_participant()?;
        Ok(ParticipantTransportFrame {
            frame,
            provenance: slot.provenance,
        })
    }

    fn reconnect_participant(
        &self,
        _server_address: &ServerAddress,
    ) -> Result<ParticipantResponseProvenance, SdkError> {
        let mut slot = self.connection.lock();
        let attempt_id = slot.next_attempt_id;
        slot.next_attempt_id =
            slot.next_attempt_id
                .checked_add(1)
                .ok_or_else(|| SdkError::Connection {
                    description: "participant transport attempt identity exhausted".to_string(),
                })?;
        let connection = Connection::connect_with_auth(&self.address, &self.auth_token)?;
        let connection_id = slot.next_connection_id;
        slot.next_connection_id =
            slot.next_connection_id
                .checked_add(1)
                .ok_or_else(|| SdkError::Connection {
                    description: "participant transport connection identity exhausted".to_string(),
                })?;
        let provenance = ParticipantResponseProvenance::new(connection_id, attempt_id);
        slot.connection = connection;
        slot.provenance = provenance;
        Ok(provenance)
    }
}

impl RemoteTransport for TcpRemoteTransport {
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

    /// Subscribes over the shared request/response connection.
    ///
    /// # v1 caveat — pooled subscribe registers a delivering subscriber
    ///
    /// This registers a *real* server-side subscriber on the shared pool
    /// connection, which is what lets a subsequent keyed publish observe a genuine
    /// delivery ack ([`PUBLISH_DELIVERED_FLAG`](liminal::protocol::PUBLISH_DELIVERED_FLAG)).
    /// The server then pumps a `Deliver` frame for every message on the channel onto
    /// this connection. Because the connection only reads (and discards) those
    /// frames during a round trip, an application that subscribes for the ack signal
    /// and then goes idle on a busy channel lets the server's bounded outbound buffer
    /// (default 4 MiB) fill; on overflow the server tears the connection down, and
    /// every later request on this transport then fails through no fault of the
    /// caller. An actively-used transport is self-limiting (each round trip drains
    /// the backlog), but a subscribe-then-idle client on a hot channel is at risk.
    ///
    /// v1 guidance: consume channel deliveries through a dedicated
    /// [`SubscriptionStream`] (its own connection with a background reader), and use
    /// the pooled subscribe only as the delivery-ack signal alongside regular
    /// traffic. The v2 credit mode removes this by gating and multiplexing delivery.
    fn subscribe(
        &self,
        _server_address: &ServerAddress,
        request: &WireSubscribeRequest,
    ) -> Result<(), SdkError> {
        let frame = Frame::Subscribe {
            flags: 0,
            stream_id: request.stream_id(),
            channel: request.channel().to_string(),
            // An empty accepted-schema list lets the server select the channel's
            // configured schema, mirroring the server's negotiation contract.
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
        self.connection.lock().connection.send_conversation_message(
            conversation_id,
            conversation_label,
            envelope,
        )
    }

    fn request_reply_conversation(
        &self,
        _server_address: &ServerAddress,
        request: &WireConversationRequest,
    ) -> Result<Vec<u8>, SdkError> {
        let conversation_label = request.conversation_id().as_str();
        let conversation_id = conversation_wire_id(conversation_label);
        let envelope = build_envelope(SCHEMALESS_SCHEMA, request.payload());
        self.connection
            .lock()
            .connection
            .conversation_request_reply(conversation_id, conversation_label, envelope)
    }

    fn resume(
        &self,
        _server_address: &ServerAddress,
        request: &WireResumeRequest,
    ) -> Result<(), SdkError> {
        // The wire protocol has no resume frame: the server replays a subscription
        // from its durable log only when the SDK re-issues the Subscribe for that
        // stream on reconnect. This transport does not retain the channel/stream
        // mapping needed to re-drive that Subscribe here, so it cannot honour the
        // resume over the socket. Returning a clear error keeps the contract honest
        // rather than reporting success while dropping the user's resume intent.
        let _ = (request.subscription_id(), request.resume_from_sequence());
        Err(SdkError::Protocol {
            description:
                "resume is not yet supported over the TCP transport; re-subscribe to trigger \
                 server replay"
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

/// Derives a stable 32-byte schema id from arbitrary schema bytes via FNV-1a.
///
/// The server selects the channel's configured schema on subscribe and stores the
/// published envelope verbatim, so this id only needs to be deterministic, not a
/// negotiated value.
fn schema_id_from_bytes(schema_bytes: &[u8]) -> SchemaId {
    let mut id = [0_u8; SchemaId::WIRE_LEN];
    let mut hash = fnv1a(schema_bytes).to_be_bytes();
    // Spread the 8-byte digest across the 32-byte id deterministically.
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

/// Builds the wire `Publish` frame, attaching the idempotency key (and its flag)
/// only when the request carries one so a no-key publish stays byte-identical to
/// the pre-13-L1 layout.
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
        other => Err(unexpected_frame("PublishAck", &other)),
    }
}

/// Maps a publish ack into a genuine delivery ack: the `PUBLISH_DELIVERED_FLAG`
/// bit on the ack reports whether a subscriber actually received the message.
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
        other => Err(unexpected_frame("PublishAck", &other)),
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
        other => Err(unexpected_frame("SubscribeAck", &other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_ids_are_deterministic_and_distinct() {
        assert_eq!(schema_id_from_bytes(b"a"), schema_id_from_bytes(b"a"));
        assert_ne!(schema_id_from_bytes(b"a"), schema_id_from_bytes(b"b"));
    }

    #[test]
    fn conversation_ids_are_stable() {
        assert_eq!(conversation_wire_id("chat"), conversation_wire_id("chat"));
        assert_ne!(conversation_wire_id("chat"), conversation_wire_id("other"));
    }

    #[test]
    fn publish_ack_maps_to_accept() -> Result<(), SdkError> {
        let frame = Frame::PublishAck {
            flags: 0,
            stream_id: 1,
            message_id: 7,
        };
        assert_eq!(publish_response(frame)?, PressureResponse::Accept);
        Ok(())
    }

    #[test]
    fn publish_error_maps_to_backpressure() {
        let frame = Frame::PublishError {
            flags: 0,
            stream_id: 1,
            reason_code: 9,
            message: Some("nope".to_string()),
        };
        assert!(matches!(
            publish_response(frame),
            Err(SdkError::Backpressure { .. })
        ));
    }
}
