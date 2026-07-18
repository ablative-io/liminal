//! Full participant E2E over a real socket against a running server.
//!
//! Enroll → ack → committed records → detach →
//! attach → replay of the old detach token, asserting the terminalized cell
//! carries the OLD committed epoch — every request and response wire-encoded
//! end to end through the production connection supervisor and the installed
//! production semantic handler.

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
    ParticipantAck, ParticipantFrame, ParticipantRecord, ReceiverDirection, RecordAdmission,
    RecordAdmissionAttemptToken, ServerPush, ServerValue, StaleAuthority,
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

#[path = "e2e_socket_fixture.rs"]
mod socket_fixture;
pub(super) use socket_fixture::SocketFixture;

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

#[test]
fn ack_after_reattach_before_replay_accepts_after_reconciliation() -> Result<(), Box<dyn Error>> {
    const CONVERSATION: u64 = 527;

    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let mut server = SocketFixture::start_with_replay_gate(&data_dir)?;
    let mut sender_socket = server.spawn_peer()?;

    let enrolled = server.request(ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: CONVERSATION,
        enrollment_token: EnrollmentToken::new([0x27; 16]),
    }))?;
    let ServerValue::EnrollBound(recipient) = enrolled else {
        return Err(format!("recipient enrollment did not bind: {enrolled:?}").into());
    };
    let sender_enrolled = sender_socket.request(ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: CONVERSATION,
        enrollment_token: EnrollmentToken::new([0xA7; 16]),
    }))?;
    let ServerValue::EnrollBound(sender) = sender_enrolled else {
        return Err(format!("sender enrollment did not bind: {sender_enrolled:?}").into());
    };
    let ServerPush::ParticipantDelivery(offered_on_e) = server.read_push()? else {
        return Err("epoch-E offer was not a participant delivery".into());
    };
    assert_eq!(offered_on_e.conversation_id, CONVERSATION);
    assert_eq!(offered_on_e.delivery_seq, 2);
    assert_eq!(
        offered_on_e.record,
        ParticipantRecord::Attached {
            affected_participant_id: sender.participant_id(),
            binding_epoch: sender.origin_binding_epoch(),
        }
    );
    let recipient_id = recipient.participant_id();
    let obligation_seq = offered_on_e.delivery_seq;
    let reconciled = server.participant_owner_facts(CONVERSATION, recipient_id)?;
    assert_eq!(reconciled.frontier_cursor, 0);
    assert_eq!(reconciled.outbox_ack_through, 0);
    assert_eq!(reconciled.next_live_obligation, Some(obligation_seq));

    server.block_publication_replay()?;
    let mut reattached_socket = server.spawn_peer()?;
    let attached =
        reattached_socket.request(ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: CONVERSATION,
            participant_id: recipient_id,
            capability_generation: Generation::ONE,
            attach_secret: recipient.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([0xB7; 16]),
            accept_marker_delivery_seq: None,
        }))?;
    let ServerValue::AttachBound(reattached) = attached else {
        return Err(format!("recipient reattach did not bind E+1: {attached:?}").into());
    };
    assert_eq!(
        reattached.capability_generation(),
        Generation::new(2).ok_or("generation two is nonzero")?
    );
    assert_ne!(
        reattached.origin_binding_epoch(),
        recipient.origin_binding_epoch()
    );
    assert!(
        server.blocked_publication_scans()? > 0,
        "the replay gate did not intercept the first E+1 publication selection"
    );

    let before_ack = server.participant_owner_facts(CONVERSATION, recipient_id)?;
    assert_eq!(before_ack.frontier_cursor, 0);
    assert_eq!(before_ack.outbox_ack_through, 0);
    assert_eq!(before_ack.next_live_obligation, Some(obligation_seq));
    let truthful_ack = ParticipantAck {
        conversation_id: CONVERSATION,
        participant_id: recipient_id,
        capability_generation: reattached.capability_generation(),
        through_seq: obligation_seq,
    };
    let outcome = reattached_socket.request(ClientRequest::ParticipantAck(truthful_ack))?;
    let ServerValue::AckCommitted(committed) = outcome else {
        return Err(format!("pre-replay reconciled ack was refused: {outcome:?}").into());
    };
    assert_eq!(committed.request().conversation_id, CONVERSATION);
    assert_eq!(committed.request().participant_id, recipient_id);
    assert_eq!(committed.request().through_seq, obligation_seq);

    let after_ack = server.participant_owner_facts(CONVERSATION, recipient_id)?;
    assert_eq!(after_ack.frontier_cursor, obligation_seq);
    assert_eq!(after_ack.outbox_ack_through, obligation_seq);
    assert_eq!(after_ack.next_live_obligation, None);
    assert_eq!(
        after_ack.live_record_count + 1,
        before_ack.live_record_count
    );
    assert!(after_ack.charged_bytes < before_ack.charged_bytes);
    drop(reattached_socket);
    drop(sender_socket);
    server.stop();
    Ok(())
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
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), config)?;
    let participant_service =
        InstalledParticipantService::new(Arc::new(handler), store, config.wire_frame_limit)
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

    // An authorized payload-bearing record commits over the same live socket
    // and echoes its exact D1 token without closing the connection.
    let record_token = RecordAdmissionAttemptToken::new([0xA7; 16]);
    let record = roundtrip(
        &mut client,
        &mut inbound,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: CONVERSATION,
            participant_id: participant,
            capability_generation: Generation::ONE,
            record_admission_attempt_token: record_token,
            payload: vec![1, 2, 3],
        }),
    )?;
    let ServerValue::RecordCommitted(record) = record else {
        return Err(format!("authorized socket record did not commit: {record:?}").into());
    };
    assert_eq!(
        record.request().record_admission_attempt_token,
        record_token
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
    let ServerValue::AttachBound(bound) = attached else {
        return Err(format!("attach did not bind: {attached:?}").into());
    };
    // The rotation's checked-increment law, wire-encoded end to end: the new
    // epoch carries generation 2 on the SAME connection incarnation, echoes
    // the presented generation separately, and rotates the secret.
    assert_eq!(
        bound.origin_binding_epoch().capability_generation,
        Generation::new(2).ok_or("generation two is nonzero")?,
        "the new binding epoch must carry the minted successor generation"
    );
    assert_eq!(
        bound.origin_binding_epoch().connection_incarnation,
        old_epoch.connection_incarnation,
        "the new epoch names the same live connection incarnation"
    );
    assert_eq!(bound.request_generation(), Generation::ONE);
    assert_ne!(
        bound.attach_secret(),
        secret,
        "the rotation must invalidate the enrollment secret"
    );
    assert_eq!(bound.participant_id(), participant);
    assert_eq!(bound.conversation_id(), CONVERSATION);

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
