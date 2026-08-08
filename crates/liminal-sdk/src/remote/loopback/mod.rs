//! The in-process (loopback) transport: the client half of
//! `docs/design/IN-PROCESS-TRANSPORT.md`.
//!
//! **What this transport secures is the RECORD PATH** (hardened
//! face-substrate draft r2 §5). It carries the exact framed wire image an
//! ordinary socket client carries — encoded by the same codec, handshaken by
//! the same `Connect`/`ConnectAck`, admitted through the same slot pool, token
//! compare, frame preflight and participant gate — so an in-process caller
//! reaches the record through the same door and no append arrives except
//! through it.
//!
//! **What this transport does NOT secure is the mount.** A co-resident caller
//! is TRUSTED CODE: it holds the host process's heap, descriptors and store
//! handle whether or not it ever calls this transport. The record therefore
//! vouches for a co-resident mount only as far as the host process itself is
//! trusted. This is not a sandbox and must never be read as one; every append
//! admitted here carries the mount fact the admitting server door stamped
//! (design §10) precisely because a consumer must weigh the mount.
//!
//! **The no-side-door guarantee has a client half, and it is the type system.**
//! [`RemoteTransport`] and [`ParticipantRemoteTransport`] stay crate-private
//! and `RemoteConfig::transport` stays a private field, so an embedder cannot
//! hand-roll a transport that skips the handshake, the gate, or the codec. The
//! only way in is [`RemoteConfig::connect_loopback`], and the only way it gets
//! a byte stream is [`EmbeddedServer::connect_loopback`], which is the server's
//! whole grant.
//!
//! **Scope (§5, ratified §9 ruling 3):** the full participant contract — connect,
//! admission, enrollment, attach/detach, acks, record admission, receive, and
//! reconnect — plus the generic publish/subscribe/conversation surface the
//! shared framing layer already carries. `PushClient` and `SubscriptionStream`
//! open their own sockets today, bypassing the transport trait entirely; they
//! stay TCP-only in v1 and are the first follow-on.

#[path = "stream.rs"]
mod stream;

use alloc::format;
use alloc::string::ToString;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::fmt;

use liminal::protocol::{
    CausalContext, Frame, MessageEnvelope, PUBLISH_DELIVERED_FLAG, PUBLISH_IDEMPOTENCY_KEY_FLAG,
    SchemaId,
};
use liminal_server::server::embedded::EmbeddedServer;
use spin::Mutex;

use crate::{DeliveryAck, PressureResponse, SdkError};

use self::stream::LoopbackStream;
use super::ServerAddress;
use super::framing::{Connection, unexpected_frame};
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

/// The established connection plus the response provenance the participant
/// layer stamps on everything read from it. Mirrors the TCP transport's slot
/// exactly, because reconnect identity is a protocol fact and not a socket one.
struct ConnectionSlot {
    connection: Connection<LoopbackStream>,
    provenance: ParticipantResponseProvenance,
    next_attempt_id: u64,
    next_connection_id: u64,
}

/// An in-process transport bound to one [`EmbeddedServer`].
///
/// The server handle is held behind an `Arc` for one reason:
/// [`ParticipantRemoteTransport::reconnect_participant`] must be able to open a
/// SECOND connection later, and a `RemoteConfig` carries its transport as
/// `Arc<dyn RemoteTransport>` with no lifetime to borrow against. The `Arc` is
/// not a second granting surface — the grant is still
/// [`EmbeddedServer::connect_loopback`] and nothing else — it is what keeps the
/// server alive for as long as a connection to it exists, which is the same
/// order a socket server and its accepted sockets already have.
pub(super) struct LoopbackRemoteTransport {
    server: Arc<EmbeddedServer>,
    connection: Arc<Mutex<ConnectionSlot>>,
    auth_token: Vec<u8>,
}

impl fmt::Debug for LoopbackRemoteTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LoopbackRemoteTransport")
            .finish_non_exhaustive()
    }
}

impl LoopbackRemoteTransport {
    /// Takes one loopback connection from `server` and completes the handshake
    /// carrying `auth_token`. An empty slice selects open access.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when the server refuses to admit the
    /// connection (at `max_connections` this is the same refusal a socket
    /// connect receives) or rejects the token, and [`SdkError::Protocol`] when
    /// the handshake frames cannot be encoded or the server answers with
    /// something other than `ConnectAck`.
    pub(super) fn connect_with_auth(
        server: Arc<EmbeddedServer>,
        auth_token: &[u8],
    ) -> Result<Self, SdkError> {
        let connection = open_connection(&server, auth_token)?;
        Ok(Self {
            server,
            connection: Arc::new(Mutex::new(ConnectionSlot {
                connection,
                provenance: ParticipantResponseProvenance::new(1, 1),
                next_attempt_id: 2,
                next_connection_id: 2,
            })),
            auth_token: auth_token.to_vec(),
        })
    }

    fn round_trip(&self, request: &Frame) -> Result<Frame, SdkError> {
        self.connection.lock().connection.round_trip(request)
    }
}

/// Asks the server for a connection and handshakes over it.
///
/// The admission refusal is mapped into [`SdkError::Connection`] rather than
/// propagated as a server error, because from the client's side an embedded
/// server at capacity and a listener at capacity are the same event: the
/// connect did not happen.
fn open_connection(
    server: &EmbeddedServer,
    auth_token: &[u8],
) -> Result<Connection<LoopbackStream>, SdkError> {
    let end = server
        .connect_loopback()
        .map_err(|source| SdkError::Connection {
            description: format!("failed to open an in-process connection: {source}"),
        })?;
    Connection::established(LoopbackStream::new(end), auth_token)
}

impl ParticipantRemoteTransport for LoopbackRemoteTransport {
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

    /// Opens a fresh loopback connection and advances the transport identity,
    /// step for step with the socket transport's reconnect.
    ///
    /// Replacing `slot.connection` drops the previous connection's client end,
    /// which is the end-of-file the server reads as a hangup — so the old
    /// connection is torn down and its admission slot released by the same path
    /// a dropped socket drives.
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
        let connection = open_connection(&self.server, &self.auth_token)?;
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

impl RemoteTransport for LoopbackRemoteTransport {
    fn publish(
        &self,
        _server_address: &ServerAddress,
        request: &WirePublishRequest,
    ) -> Result<PressureResponse, SdkError> {
        let frame = build_publish_frame(request);
        publish_response(self.round_trip(&frame)?)
    }

    fn publish_with_delivery(
        &self,
        _server_address: &ServerAddress,
        request: &WirePublishRequest,
    ) -> Result<DeliveryAck, SdkError> {
        let frame = build_publish_frame(request);
        publish_delivery_response(self.round_trip(&frame)?)
    }

    /// Subscribes over the shared request/response connection.
    ///
    /// The pooled-subscribe caveat the socket transport documents applies here
    /// unchanged: this registers a real server-side subscriber, so the server
    /// pumps a `Deliver` for every message on the channel onto this connection
    /// and a subscribe-then-idle client on a hot channel can overflow the
    /// server's bounded outbound buffer. The ring is bounded exactly so that
    /// this behaves the way the socket does rather than buffering without end.
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
        subscribe_response(self.round_trip(&frame)?)
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

    /// Refused for the same reason the socket transport refuses it: the wire
    /// protocol has no resume frame, and this transport does not retain the
    /// channel/stream mapping needed to re-drive the `Subscribe` that triggers
    /// server replay. Reporting the refusal keeps the contract honest instead
    /// of dropping the caller's resume intent under a success.
    fn resume(
        &self,
        _server_address: &ServerAddress,
        request: &WireResumeRequest,
    ) -> Result<(), SdkError> {
        let _ = (request.subscription_id(), request.resume_from_sequence());
        Err(SdkError::Protocol {
            description:
                "resume is not yet supported over the in-process transport; re-subscribe to \
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

/// Derives a stable 32-byte schema id from arbitrary schema bytes via FNV-1a.
///
/// Byte-for-byte the socket transport's derivation, and it has to be: the
/// design's discriminating property is that the two mounts put the SAME bytes
/// on the record, and a differently-derived schema id would be a different
/// envelope.
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

/// Builds the wire `Publish` frame, attaching the idempotency key (and its
/// flag) only when the request carries one so a no-key publish stays
/// byte-identical to the socket mount's layout.
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

fn publish_delivery_response(frame: Frame) -> Result<DeliveryAck, SdkError> {
    match frame {
        Frame::PublishAck { flags, .. } => Ok(DeliveryAck::new(
            PressureResponse::Accept,
            flags & PUBLISH_DELIVERED_FLAG != 0,
        )),
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
