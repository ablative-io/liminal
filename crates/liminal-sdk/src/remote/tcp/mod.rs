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

use alloc::format;
use alloc::string::ToString;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::fmt;

use liminal::protocol::{CausalContext, Frame, MessageEnvelope, SchemaId};
use spin::Mutex;

use crate::{PressureResponse, SdkError};

use self::connection::{Connection, unexpected_frame};
use super::ServerAddress;
use super::protocol::{
    RemoteTransport, WireConversationRequest, WirePublishRequest, WireResumeRequest,
    WireSubscribeRequest,
};

/// Application stream id used for non-subscription application frames.
const APPLICATION_STREAM_ID: u32 = 1;
/// In-flight credit advertised on subscribe; one keeps strict pacing.
const DEFAULT_MAX_IN_FLIGHT: u32 = 1;
/// Schema id used for payloads whose schema is not carried on the wire.
const SCHEMALESS_SCHEMA: &[u8] = &[];

/// Real TCP transport that exchanges canonical wire frames with a liminal server.
pub struct TcpRemoteTransport {
    connection: Arc<Mutex<Connection>>,
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
        let connection = Connection::connect(server_address.as_str())?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    fn round_trip(&self, request: &Frame) -> Result<Frame, SdkError> {
        let mut connection = self.connection.lock();
        connection.round_trip(request)
    }
}

impl RemoteTransport for TcpRemoteTransport {
    fn publish(
        &self,
        _server_address: &ServerAddress,
        request: &WirePublishRequest,
    ) -> Result<PressureResponse, SdkError> {
        let envelope = build_envelope(request.schema().schema.as_ref(), request.payload());
        let frame = Frame::Publish {
            flags: 0,
            stream_id: APPLICATION_STREAM_ID,
            channel: request.channel().to_string(),
            envelope,
        };
        let response = self.round_trip(&frame)?;
        publish_response(response)
    }

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
