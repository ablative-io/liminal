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
//! does not introduce an async runtime. Each transport call performs one
//! request/response round trip under a short-lived connection lock.

use alloc::format;
use alloc::string::ToString;
use alloc::sync::Arc;
use alloc::vec;
use alloc::vec::Vec;
use core::fmt;
use core::time::Duration;

use std::io::{Read, Write};
use std::net::TcpStream;

use liminal::protocol::{
    CausalContext, Frame, FrameType, MessageEnvelope, ProtocolError, ProtocolVersion, SchemaId,
    decode, encode, encoded_len,
};
use spin::Mutex;

use crate::{PressureResponse, SdkError};

use super::ServerAddress;
use super::protocol::{
    RemoteTransport, WireConversationRequest, WirePublishRequest, WireResumeRequest,
    WireSubscribeRequest,
};

/// Minimum protocol version this client advertises during the handshake.
const CLIENT_MIN_VERSION: ProtocolVersion = ProtocolVersion::new(1, 0);
/// Maximum protocol version this client advertises during the handshake.
const CLIENT_MAX_VERSION: ProtocolVersion = ProtocolVersion::new(1, 0);
/// Maximum time spent waiting on a single socket read or write.
const IO_TIMEOUT: Duration = Duration::from_secs(5);
/// Read chunk size used when draining the socket into the frame buffer.
const READ_CHUNK_BYTES: usize = 4096;
/// Upper bound on a single response frame, guarding against runaway buffering.
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

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
        connection.send(request)?;
        connection.receive()
    }

    fn fire(&self, request: &Frame) -> Result<(), SdkError> {
        let mut connection = self.connection.lock();
        connection.send(request)
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
        let conversation_id = conversation_wire_id(request.conversation_id().as_str());
        let envelope = build_envelope(SCHEMALESS_SCHEMA, request.payload());
        let frame = Frame::ConversationMessage {
            flags: 0,
            stream_id: APPLICATION_STREAM_ID,
            conversation_id,
            envelope,
        };
        // Conversation messages are fire-and-forget on the success path: the
        // server only replies with a ConversationError frame on failure.
        self.fire(&frame)
    }

    fn resume(
        &self,
        _server_address: &ServerAddress,
        request: &WireResumeRequest,
    ) -> Result<(), SdkError> {
        // The wire protocol expresses resume as re-subscription bookkeeping; the
        // SDK tracks the resume sequence locally and the server replays from its
        // durable log. There is no distinct resume frame, so a resume is a no-op
        // over the socket beyond recording intent, which the caller already did.
        let _ = (request.subscription_id(), request.resume_from_sequence());
        Ok(())
    }
}

/// Application stream id used for non-subscription application frames.
const APPLICATION_STREAM_ID: u32 = 1;
/// In-flight credit advertised on subscribe; one keeps strict pacing.
const DEFAULT_MAX_IN_FLIGHT: u32 = 1;
/// Schema id used for payloads whose schema is not carried on the wire.
const SCHEMALESS_SCHEMA: &[u8] = &[];

/// Owns the socket and the partial-frame read buffer for one server connection.
struct Connection {
    stream: TcpStream,
    buffer: Vec<u8>,
}

impl Connection {
    fn connect(address: &str) -> Result<Self, SdkError> {
        let stream = TcpStream::connect(address).map_err(|source| SdkError::Connection {
            description: format!("failed to connect to {address}: {source}"),
        })?;
        stream
            .set_nodelay(true)
            .map_err(|source| SdkError::Connection {
                description: format!("failed to disable Nagle for {address}: {source}"),
            })?;
        stream
            .set_read_timeout(Some(IO_TIMEOUT))
            .map_err(|source| SdkError::Connection {
                description: format!("failed to set read timeout for {address}: {source}"),
            })?;
        stream
            .set_write_timeout(Some(IO_TIMEOUT))
            .map_err(|source| SdkError::Connection {
                description: format!("failed to set write timeout for {address}: {source}"),
            })?;

        let mut connection = Self {
            stream,
            buffer: Vec::new(),
        };
        connection.handshake()?;
        Ok(connection)
    }

    fn handshake(&mut self) -> Result<(), SdkError> {
        let connect = Frame::Connect {
            flags: 0,
            min_version: CLIENT_MIN_VERSION,
            max_version: CLIENT_MAX_VERSION,
            auth_token: Vec::new(),
        };
        self.send(&connect)?;
        match self.receive()? {
            Frame::ConnectAck { .. } => Ok(()),
            Frame::ConnectError {
                reason_code,
                message,
                ..
            } => Err(SdkError::Connection {
                description: format!(
                    "server rejected connection (reason {reason_code}): {}",
                    message.unwrap_or_else(|| "no detail".to_string())
                ),
            }),
            other => Err(unexpected_frame("ConnectAck", &other)),
        }
    }

    fn send(&mut self, frame: &Frame) -> Result<(), SdkError> {
        let len = encoded_len(frame).map_err(|error| protocol_error(&error))?;
        let mut bytes = vec![0_u8; len];
        let written = encode(frame, &mut bytes).map_err(|error| protocol_error(&error))?;
        let encoded = bytes.get(..written).ok_or_else(|| SdkError::Protocol {
            description: "wire encoder reported an invalid byte count".to_string(),
        })?;
        self.stream
            .write_all(encoded)
            .map_err(|source| SdkError::Connection {
                description: format!("failed to write frame to server: {source}"),
            })?;
        self.stream.flush().map_err(|source| SdkError::Connection {
            description: format!("failed to flush frame to server: {source}"),
        })
    }

    fn receive(&mut self) -> Result<Frame, SdkError> {
        loop {
            match decode(&self.buffer) {
                Ok((frame, consumed)) => {
                    self.buffer.drain(..consumed);
                    return Ok(frame);
                }
                Err(
                    ProtocolError::IncompleteHeader { .. } | ProtocolError::TruncatedPayload { .. },
                ) => self.fill_buffer()?,
                Err(error) => return Err(protocol_error(&error)),
            }
        }
    }

    fn fill_buffer(&mut self) -> Result<(), SdkError> {
        if self.buffer.len() > MAX_RESPONSE_BYTES {
            return Err(SdkError::Protocol {
                description: format!(
                    "server response exceeded {MAX_RESPONSE_BYTES} bytes without a complete frame"
                ),
            });
        }
        let mut chunk = [0_u8; READ_CHUNK_BYTES];
        let read = self
            .stream
            .read(&mut chunk)
            .map_err(|source| SdkError::Connection {
                description: format!("failed to read frame from server: {source}"),
            })?;
        if read == 0 {
            return Err(SdkError::Connection {
                description: "server closed the connection before a full frame arrived".to_string(),
            });
        }
        let Some(received) = chunk.get(..read) else {
            return Err(SdkError::Protocol {
                description: "socket read reported more bytes than the read buffer holds"
                    .to_string(),
            });
        };
        self.buffer.extend_from_slice(received);
        Ok(())
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

fn protocol_error(error: &ProtocolError) -> SdkError {
    SdkError::Protocol {
        description: format!("wire codec error: {error}"),
    }
}

fn unexpected_frame(expected: &str, actual: &Frame) -> SdkError {
    SdkError::Protocol {
        description: format!(
            "expected {expected} frame, received {:?}",
            FrameType::from(u8::from(actual.frame_type()))
        ),
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
