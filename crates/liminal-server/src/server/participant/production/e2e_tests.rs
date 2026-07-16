//! Full participant E2E over a real socket against a running server.
//!
//! Enroll → attach lifecycle → acks → detach → replay of the old detach
//! token, asserting the terminalized cell carries the OLD committed epoch —
//! every request and response wire-encoded end to end through the production
//! connection supervisor and the installed production semantic handler.

use std::error::Error;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use liminal::protocol::{
    Frame, MessageEnvelope, ProtocolError, ProtocolVersion, SchemaId, decode as decode_generic,
    encode as encode_generic, encoded_len as generic_encoded_len,
};
use liminal_protocol::wire::{
    AttachAttemptToken, ClientRequest, CredentialAttachRequest, DetachAttemptToken, DetachRequest,
    DetachStaleAuthority, EnrollmentRequest, EnrollmentToken, Generation, PARTICIPANT_FRAME_TYPE,
    ParticipantAck, ParticipantFrame, ReceiverDirection, ServerValue, StaleAuthority,
    decode as decode_participant, encode as encode_participant,
    encoded_len as participant_encoded_len,
};

use crate::ServerError;
use crate::server::connection::{
    ConnectionConversation, ConnectionServices, ConnectionSubscription, ConnectionSupervisor,
    PublishOutcome,
};
use crate::server::participant::{InstalledParticipantService, PARTICIPANT_CAPABILITY_BIT};

use super::ProductionParticipantHandler;
use super::tests::{open_disk_store_for_tests, test_participant_config};

/// Connection services carrying ONLY the production participant service.
#[derive(Debug)]
struct ParticipantOnlyServices {
    participant_service: InstalledParticipantService,
}

impl ParticipantOnlyServices {
    fn unsupported(operation: &str) -> ServerError {
        ServerError::ListenerAccept {
            message: format!("participant production e2e fixture does not support {operation}"),
        }
    }
}

impl ConnectionServices for ParticipantOnlyServices {
    fn participant_service(&self) -> Option<InstalledParticipantService> {
        Some(self.participant_service.clone())
    }

    fn publish(
        &self,
        _channel: &str,
        _envelope: &MessageEnvelope,
        _idempotency_key: Option<&str>,
    ) -> Result<PublishOutcome, ServerError> {
        Err(Self::unsupported("publish"))
    }

    fn subscribe(
        &self,
        _channel: &str,
        _accepted_schemas: &[SchemaId],
        _install: Option<liminal::channel::InboxInstall>,
    ) -> Result<ConnectionSubscription, ServerError> {
        Err(Self::unsupported("subscribe"))
    }

    fn unsubscribe(&self, _subscription: ConnectionSubscription) -> Result<(), ServerError> {
        Err(Self::unsupported("unsubscribe"))
    }

    fn open_conversation(
        &self,
        _conversation_id: u64,
        _subject: &str,
    ) -> Result<ConnectionConversation, ServerError> {
        Err(Self::unsupported("conversation open"))
    }

    fn conversation_message(
        &self,
        _conversation: &ConnectionConversation,
        _envelope: &MessageEnvelope,
    ) -> Result<(), ServerError> {
        Err(Self::unsupported("conversation message"))
    }

    fn close_conversation(&self, _conversation: ConnectionConversation) -> Result<(), ServerError> {
        Err(Self::unsupported("conversation close"))
    }

    fn flush_durable_state(&self) -> Result<(), ServerError> {
        Ok(())
    }

    fn supports_channel_operations(&self) -> bool {
        false
    }
}

fn tcp_pair() -> Result<(TcpStream, TcpStream), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address: SocketAddr = listener.local_addr()?;
    let client = TcpStream::connect(address)?;
    let (server, _) = listener.accept()?;
    Ok((client, server))
}

fn encode_frame(frame: &Frame) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut bytes = vec![0; generic_encoded_len(frame)?];
    let written = encode_generic(frame, &mut bytes)?;
    bytes.truncate(written);
    Ok(bytes)
}

fn encode_request(request: ClientRequest) -> Result<Vec<u8>, Box<dyn Error>> {
    let frame = ParticipantFrame::ClientRequest(request);
    let mut bytes = vec![0; participant_encoded_len(&frame).map_err(|error| format!("{error:?}"))?];
    let written = encode_participant(&frame, &mut bytes).map_err(|error| format!("{error:?}"))?;
    bytes.truncate(written);
    Ok(bytes)
}

fn read_frame(socket: &mut TcpStream, buffer: &mut Vec<u8>) -> Result<Frame, Box<dyn Error>> {
    loop {
        match decode_generic(buffer) {
            Ok((frame, consumed)) => {
                buffer.drain(..consumed);
                return Ok(frame);
            }
            Err(
                ProtocolError::IncompleteHeader { .. } | ProtocolError::TruncatedPayload { .. },
            ) => {
                let mut chunk = [0_u8; 512];
                let read = socket.read(&mut chunk)?;
                if read == 0 {
                    return Err("connection closed before a complete frame arrived".into());
                }
                buffer.extend_from_slice(chunk.get(..read).unwrap_or(&[]));
            }
            Err(error) => return Err(Box::new(error)),
        }
    }
}

/// Sends one participant request over the live socket and decodes the
/// wire-encoded semantic response.
fn roundtrip(
    client: &mut TcpStream,
    inbound: &mut Vec<u8>,
    request: ClientRequest,
) -> Result<ServerValue, Box<dyn Error>> {
    client.write_all(&encode_request(request)?)?;
    let frame = read_frame(client, inbound)?;
    assert!(
        matches!(
            frame,
            Frame::Unknown {
                type_id: PARTICIPANT_FRAME_TYPE,
                ..
            }
        ),
        "expected a participant frame, got {frame:?}"
    );
    // Re-encode the preserved generic frame back into its exact wire bytes:
    // the participant codec owns the byte layout end to end.
    let bytes = encode_frame(&frame)?;
    let decoded = decode_participant(&bytes, ReceiverDirection::Client)
        .map_err(|error| format!("{error:?}"))?;
    let ParticipantFrame::ServerValue(value) = decoded else {
        return Err("participant response did not decode as a server value".into());
    };
    Ok(value)
}

const CONVERSATION: u64 = 401;

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "the E2E narrates one complete lifecycle in wire order"
)]
fn full_lifecycle_e2e_over_real_socket_replays_old_epoch() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let store = open_disk_store_for_tests(&data_dir)?;
    let config = test_participant_config();
    let participant_service = InstalledParticipantService::new(
        Arc::new(ProductionParticipantHandler::new(
            Arc::clone(&store),
            config,
        )),
        store,
        config.wire_frame_limit,
    )
    .map_err(|error| format!("{error:?}"))?;
    let services: Arc<dyn ConnectionServices> = Arc::new(ParticipantOnlyServices {
        participant_service,
    });
    let supervisor = ConnectionSupervisor::with_services(services)?;
    let (mut client, server) = tcp_pair()?;
    client.set_read_timeout(Some(Duration::from_secs(10)))?;
    client.set_write_timeout(Some(Duration::from_secs(10)))?;
    let _handle = supervisor.spawn_connection(server)?;

    // Real handshake: the participant capability bit is advertised because
    // the REAL production service is installed.
    client.write_all(&encode_frame(&Frame::Connect {
        flags: 0,
        min_version: ProtocolVersion::new(1, 0),
        max_version: ProtocolVersion::new(1, 0),
        auth_token: Vec::new(),
    })?)?;
    let mut inbound = Vec::new();
    let ack = read_frame(&mut client, &mut inbound)?;
    assert!(
        matches!(
            ack,
            Frame::ConnectAck { capabilities, .. } if capabilities == PARTICIPANT_CAPABILITY_BIT
        ),
        "participant capability was not advertised: {ack:?}"
    );

    // Enroll.
    let enrolled = roundtrip(
        &mut client,
        &mut inbound,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new([9; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(receipt) = enrolled else {
        return Err(format!("enrollment did not bind: {enrolled:?}").into());
    };
    let old_epoch = receipt.origin_binding_epoch();
    let secret = receipt.attach_secret();
    let participant = receipt.participant_id();
    assert_eq!(old_epoch.capability_generation, Generation::ONE);

    // Acknowledge the retained lifecycle record (a real committed cursor).
    let acked = roundtrip(
        &mut client,
        &mut inbound,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: CONVERSATION,
            participant_id: participant,
            capability_generation: Generation::ONE,
            through_seq: 1,
        }),
    )?;
    assert!(
        matches!(acked, ServerValue::AckCommitted(_)),
        "ack did not commit: {acked:?}"
    );

    // Detach (the OLD epoch is committed into the cell here).
    let detach_token = DetachAttemptToken::new([8; 16]);
    let detached = roundtrip(
        &mut client,
        &mut inbound,
        ClientRequest::Detach(DetachRequest {
            conversation_id: CONVERSATION,
            participant_id: participant,
            capability_generation: Generation::ONE,
            detach_attempt_token: detach_token,
        }),
    )?;
    assert!(
        matches!(detached, ServerValue::DetachCommitted(_)),
        "detach did not commit: {detached:?}"
    );

    // Attach again over the same live connection: Fix 1 terminalizes the
    // committed cell atomically with the credential rotation.
    let attached = roundtrip(
        &mut client,
        &mut inbound,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: CONVERSATION,
            participant_id: participant,
            capability_generation: Generation::ONE,
            attach_secret: secret,
            attach_attempt_token: AttachAttemptToken::new([10; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    assert!(
        matches!(attached, ServerValue::AttachBound(_)),
        "attach did not bind: {attached:?}"
    );

    // Replay the OLD detach token: the terminalized cell must answer with
    // the OLD committed epoch, wire-encoded end to end.
    let replayed = roundtrip(
        &mut client,
        &mut inbound,
        ClientRequest::Detach(DetachRequest {
            conversation_id: CONVERSATION,
            participant_id: participant,
            capability_generation: Generation::ONE,
            detach_attempt_token: detach_token,
        }),
    )?;
    let ServerValue::StaleAuthority(StaleAuthority::Detach(
        DetachStaleAuthority::TerminalizedDetachCell(cell),
    )) = replayed
    else {
        return Err(
            format!("old detach token did not replay the terminalized cell: {replayed:?}").into(),
        );
    };
    assert_eq!(
        cell.committed_binding_epoch(),
        old_epoch,
        "the terminalized cell must carry the OLD committed epoch"
    );
    assert_eq!(cell.detach_attempt_token(), detach_token);

    drop(client);
    supervisor.shutdown();
    Ok(())
}
