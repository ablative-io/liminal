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
use std::sync::mpsc::{Receiver, RecvTimeoutError};
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
use super::tests::{
    dispatch as production_dispatch, open_disk_store_for_tests, test_participant_config,
};

#[path = "e2e_socket_fixture.rs"]
mod socket_fixture;
pub(super) use socket_fixture::{OutboxOwnerFacts, SdkSocketFixture, SocketFixture, SocketPeer};

#[path = "e2e_sdk_tests.rs"]
mod e2e_sdk_tests;

#[path = "tests_endpoint_ack.rs"]
mod tests_endpoint_ack;

#[path = "tests_w2_leg3_idle.rs"]
mod tests_w2_leg3_idle;

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

/// Lowercase hex of a byte slice, for the amplifier's raw-frame dump.
fn to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        out.push(char::from_digit(u32::from(byte >> 4), 16).unwrap_or('?'));
        out.push(char::from_digit(u32::from(byte & 0x0f), 16).unwrap_or('?'));
    }
    out
}

/// Names the decoded participant frame variant for the amplifier dump.
fn participant_variant_name(frame: &ParticipantFrame) -> &'static str {
    match frame {
        ParticipantFrame::ClientRequest(_) => "ClientRequest",
        ParticipantFrame::ServerValue(_) => "ServerValue",
        ParticipantFrame::ServerPush(_) => "ServerPush",
    }
}

/// Amplifier-only instrumented sibling of [`roundtrip`]. It performs the exact
/// same wire steps but, when the frame read back decodes to a variant OTHER than
/// `ServerValue` (the response-displacement failure this lane is chasing), it
/// dumps the raw frame bytes as hex, the decoded variant name, the decoded
/// frame Debug, and the residual buffer length before returning the same error
/// text the real harness produces. This never weakens the landed `roundtrip`'s
/// assertions and is only reachable through the `#[ignore]` amplifier.
fn roundtrip_capture(
    client: &mut TcpStream,
    inbound: &mut Vec<u8>,
    request: ClientRequest,
    label: &str,
) -> Result<ServerValue, Box<dyn Error>> {
    client.write_all(&encode_request(request)?)?;
    let frame = read_frame(client, inbound)?;
    let Frame::Unknown {
        type_id: PARTICIPANT_FRAME_TYPE,
        ..
    } = frame
    else {
        return Err(format!("{label}: expected a participant frame, got {frame:?}").into());
    };
    let bytes = encode_frame(&frame)?;
    let decoded = decode_participant(&bytes, ReceiverDirection::Client)
        .map_err(|error| format!("{label}: {error:?}"))?;
    match decoded {
        ParticipantFrame::ServerValue(value) => Ok(value),
        other => {
            let variant = participant_variant_name(&other);
            eprintln!(
                "[AMP-CAPTURE] {label}: FRAME THAT DISPLACED THE RESPONSE variant={variant} \
                 frame_len={} residue_bytes={} hex={}",
                bytes.len(),
                inbound.len(),
                to_hex(&bytes)
            );
            eprintln!("[AMP-CAPTURE] {label}: decoded_frame_debug={other:?}");
            Err(format!(
                "participant response did not decode as a server value (got {variant}, \
                 frame_len={}, residue_bytes={})",
                bytes.len(),
                inbound.len()
            )
            .into())
        }
    }
}

fn await_genuine_park(
    server: &SocketFixture,
    pid: u64,
    marker: &Receiver<u64>,
) -> Result<u64, Box<dyn Error>> {
    marker
        .recv_timeout(Duration::from_secs(2))
        .map_err(|error| format!("process {pid} did not report its final-probe park: {error}"))?;
    let parked_at = server
        .observe_settled_park(pid)
        .recv_timeout(Duration::from_secs(2))
        .map_err(|error| format!("process {pid} did not settle after its park: {error}"))?;
    assert_eq!(server.slice_count(pid), parked_at);
    Ok(parked_at)
}

fn assert_idle_slice_count_is_stable(server: &SocketFixture, pid: u64, parked_at: u64) {
    let unexpected_slice = server.observe_next_slice(pid);
    assert!(
        matches!(
            unexpected_slice.recv_timeout(Duration::from_millis(100)),
            Err(RecvTimeoutError::Timeout)
        ),
        "parked process {pid} serviced a slice without a readiness event"
    );
    assert_eq!(
        server.slice_count(pid),
        parked_at,
        "parked process {pid} polled while idle"
    );
}

#[test]
fn parked_tcp_and_websocket_processes_wake_on_outbox_without_polling() -> Result<(), Box<dyn Error>>
{
    const TCP_CONVERSATION: u64 = 0x21_01;
    const WS_CONVERSATION: u64 = 0x21_02;

    // TCP: an enrollment request gives the real process a deterministic event
    // after the park marker is installed. The marker is emitted only when the
    // production final probe returns false and the native process selects Wait.
    let tcp_home = tempfile::tempdir()?;
    let mut tcp_server = SocketFixture::start(&tcp_home.path().join("tcp"))?;
    let tcp_pid = tcp_server.pid();
    let initial_tcp_park = tcp_server.observe_next_park(tcp_pid);
    let tcp_recipient = tcp_server.request(ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: TCP_CONVERSATION,
        enrollment_token: EnrollmentToken::new([0x21; 16]),
    }))?;
    let ServerValue::EnrollBound(tcp_recipient) = tcp_recipient else {
        return Err(format!("TCP recipient enrollment did not bind: {tcp_recipient:?}").into());
    };
    let parked_at = await_genuine_park(&tcp_server, tcp_pid, &initial_tcp_park)?;
    assert_idle_slice_count_is_stable(&tcp_server, tcp_pid, parked_at);

    let tcp_wake_park = tcp_server.observe_next_park(tcp_pid);
    let mut tcp_sender_socket = tcp_server.spawn_peer()?;
    let tcp_sender = tcp_sender_socket.request(ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: TCP_CONVERSATION,
        enrollment_token: EnrollmentToken::new([0x22; 16]),
    }))?;
    let ServerValue::EnrollBound(tcp_sender) = tcp_sender else {
        return Err(format!("TCP sender enrollment did not bind: {tcp_sender:?}").into());
    };
    let tcp_push = tcp_server.read_push()?;
    assert_eq!(
        tcp_push,
        ServerPush::ParticipantDelivery(liminal_protocol::wire::ParticipantDelivery {
            conversation_id: TCP_CONVERSATION,
            delivery_seq: 2,
            record: ParticipantRecord::Attached {
                affected_participant_id: tcp_sender.participant_id(),
                binding_epoch: tcp_sender.origin_binding_epoch(),
            },
        })
    );
    assert_ne!(tcp_recipient.participant_id(), tcp_sender.participant_id());
    let reparks_at = await_genuine_park(&tcp_server, tcp_pid, &tcp_wake_park)?;
    assert!(reparks_at > parked_at);
    assert_idle_slice_count_is_stable(&tcp_server, tcp_pid, reparks_at);
    drop(tcp_sender_socket);
    tcp_server.stop();

    // WebSocket: the sibling listener installs the same production participant
    // service and registry into an actual WebSocket connection process. Its own
    // final probe must park, wake from the eligible source-batch commit, publish
    // one binary participant frame, and repark without an idle slice.
    let ws_home = tempfile::tempdir()?;
    let mut ws_server = SocketFixture::start(&ws_home.path().join("websocket"))?;
    let mut ws_endpoint = ws_server.spawn_websocket_peer()?;
    let ws_pid = ws_endpoint.peer.pid();
    let initial_ws_park = ws_server.observe_next_park(ws_pid);
    let ws_recipient = ws_endpoint
        .peer
        .request(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: WS_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x23; 16]),
        }))?;
    let ServerValue::EnrollBound(ws_recipient) = ws_recipient else {
        return Err(
            format!("WebSocket recipient enrollment did not bind: {ws_recipient:?}").into(),
        );
    };
    let ws_parked_at = await_genuine_park(&ws_server, ws_pid, &initial_ws_park)?;
    assert_idle_slice_count_is_stable(&ws_server, ws_pid, ws_parked_at);

    let ws_wake_park = ws_server.observe_next_park(ws_pid);
    let ws_sender = ws_server.request(ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: WS_CONVERSATION,
        enrollment_token: EnrollmentToken::new([0x24; 16]),
    }))?;
    let ServerValue::EnrollBound(ws_sender) = ws_sender else {
        return Err(format!("WebSocket sender enrollment did not bind: {ws_sender:?}").into());
    };
    let ws_push = ws_endpoint.peer.read_push()?;
    assert_eq!(
        ws_push,
        ServerPush::ParticipantDelivery(liminal_protocol::wire::ParticipantDelivery {
            conversation_id: WS_CONVERSATION,
            delivery_seq: 2,
            record: ParticipantRecord::Attached {
                affected_participant_id: ws_sender.participant_id(),
                binding_epoch: ws_sender.origin_binding_epoch(),
            },
        })
    );
    assert_ne!(ws_recipient.participant_id(), ws_sender.participant_id());
    let ws_reparks_at = await_genuine_park(&ws_server, ws_pid, &ws_wake_park)?;
    assert!(ws_reparks_at > ws_parked_at);
    assert_idle_slice_count_is_stable(&ws_server, ws_pid, ws_reparks_at);
    ws_endpoint.stop()?;
    ws_server.stop();
    Ok(())
}

#[test]
pub(super) fn ack_after_reattach_before_replay_accepts_after_reconciliation()
-> Result<(), Box<dyn Error>> {
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
    let handler = Arc::new(ProductionParticipantHandler::new(
        Arc::clone(&store),
        config,
    )?);
    let participant_service = InstalledParticipantService::new(
        Arc::clone(&handler) as Arc<_>,
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

    // A peer enrollment creates sequence 2 as a real recipient obligation for
    // participant zero; sequence 1 is its sender-excluded internal endpoint.
    let peer = production_dispatch(
        &handler,
        liminal_protocol::wire::ConnectionIncarnation::new(0x401, 2),
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x41; 16]),
        }),
    )?;
    assert!(matches!(peer, ServerValue::EnrollBound(_)));

    // Acknowledge the real durable peer-enrollment obligation before offer.
    let acked = roundtrip(
        &mut client,
        &mut inbound,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: CONVERSATION,
            participant_id: participant,
            capability_generation: Generation::ONE,
            through_seq: 2,
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

/// One amplifier iteration. Returns `Ok(true)` when the sender's response
/// roundtrip was displaced by an interleaved push (the captured failure),
/// `Ok(false)` when the response arrived cleanly.
fn amplify_interleave_once(iteration: u32, peer_count: u32) -> Result<bool, Box<dyn Error>> {
    const AMP_CONVERSATION: u64 = 0x5150;
    // The conversation identity capacity is 4 (sender + 3 peers), so hold the
    // peer roster at three: enough to accrue several held `Attached` pushes.
    let peer_count = peer_count.min(3);

    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    // Gate CLOSED: every enrollment's `Attached` delivery obligation for the
    // sender accrues undelivered, exactly as in the leave regression's gated
    // enrollment phase.
    let mut sender_fixture = SocketFixture::start_replay_gated(&data_dir)?;
    let enrolled = sender_fixture.request(ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: AMP_CONVERSATION,
        enrollment_token: EnrollmentToken::new([0x10; 16]),
    }))?;
    let ServerValue::EnrollBound(sender) = enrolled else {
        return Err(format!("amp {iteration}: sender did not enroll: {enrolled:?}").into());
    };

    // Peers enroll behind the closed gate; each creates a held `Attached`
    // obligation owed to the sender's connection.
    let mut peers = Vec::with_capacity(peer_count as usize);
    let mut peer_ids = Vec::with_capacity(peer_count as usize);
    for index in 0..peer_count {
        let mut peer = sender_fixture.spawn_peer()?;
        let token = u8::try_from(0x40 + (index & 0x1f)).unwrap_or(0x40);
        let bound = peer.request(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: AMP_CONVERSATION,
            enrollment_token: EnrollmentToken::new([token; 16]),
        }))?;
        let ServerValue::EnrollBound(peer_bound) = bound else {
            return Err(format!("amp {iteration}: peer {index} did not enroll: {bound:?}").into());
        };
        peer_ids.push(peer_bound.participant_id());
        peers.push(peer);
    }

    // Open the gate, then fire a fresh readiness edge on the SENDER's connection
    // by having the last-enrolled peer (which owes no held pushes of its own)
    // commit a record. The resulting `OrdinaryRecord` obligation for the sender
    // marks the sender's connection ready, waking a publication slice that can
    // flush the sender's held `Attached` pushes ahead of the response to the
    // sender's own request — the leave regression's `open_publication_replay`
    // followed by an immediate roundtrip.
    sender_fixture.open_publication_replay()?;
    if let (Some(waker_peer), Some(waker_id)) = (peers.last_mut(), peer_ids.last()) {
        let woke = waker_peer.request_capture(
            ClientRequest::RecordAdmission(RecordAdmission {
                conversation_id: AMP_CONVERSATION,
                participant_id: *waker_id,
                capability_generation: Generation::ONE,
                record_admission_attempt_token: RecordAdmissionAttemptToken::new([0xB2; 16]),
                payload: vec![0xD4],
            }),
            &format!("amp#{iteration}-waker"),
        );
        // A displaced waker roundtrip is itself an interleave; either way the
        // sender's connection has been marked ready.
        if let Err(error) = woke {
            let text = error.to_string();
            if !text.contains("did not decode as a server value") {
                return Err(format!("amp {iteration}: waker record failed: {text}").into());
            }
        }
    }

    // The sender immediately drives a RecordAdmission roundtrip. `request_capture`
    // reads ONE frame and dumps raw bytes if it is not a `ServerValue`.
    let label = format!("amp#{iteration}");
    let outcome = sender_fixture.request_capture(
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: AMP_CONVERSATION,
            participant_id: sender.participant_id(),
            capability_generation: Generation::ONE,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([0xA2; 16]),
            payload: vec![0xD3],
        }),
        &label,
    );

    let interleaved = match outcome {
        Ok(ServerValue::RecordCommitted(_)) => false,
        Ok(other) => {
            return Err(format!("amp {iteration}: unexpected clean value: {other:?}").into());
        }
        Err(error) => {
            let text = error.to_string();
            if text.contains("did not decode as a server value") {
                eprintln!("[AMP-CAPTURE] {label}: CAPTURED interleave -> {text}");
                true
            } else {
                return Err(format!("amp {iteration}: unexpected roundtrip error: {text}").into());
            }
        }
    };

    drop(peers);
    sender_fixture.stop();
    Ok(interleaved)
}

/// Interleave amplifier — IGNORED so it never joins the normal battery.
///
/// Reproduces the contention-dependent failure of
/// `leave_after_detach_reattach_supersession_discharges_unacked_obligation_and_reopens`:
/// on one participant connection an unsolicited `ServerPush`
/// (`ParticipantDelivery`) can reach the wire AHEAD of the semantic response to
/// a client request, because a publication `READY` wake can schedule a
/// connection slice that flushes a held obligation before the request's bytes
/// are read (see the single-FIFO-writer, per-slice ordering in
/// `server/connection/process.rs`). The harness `roundtrip` reads exactly ONE
/// frame and requires `ServerValue`, so it observes the push and reports
/// "participant response did not decode as a server value".
///
/// Self-contained: spawns `std::thread` CPU burners so no external load is
/// needed. Tune via env vars `AMP_ITERS` (default 400), `AMP_PEERS` (default 6),
/// `AMP_BURNERS` (default 8).
#[test]
#[ignore = "amplifier: run explicitly to reproduce the response/push interleave"]
fn amplify_leave_regression_response_push_interleave() -> Result<(), Box<dyn Error>> {
    fn env_u32(key: &str, default: u32) -> u32 {
        std::env::var(key)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    }

    let iterations = env_u32("AMP_ITERS", 400);
    let peer_count = env_u32("AMP_PEERS", 6);
    let burner_count = env_u32("AMP_BURNERS", 8);

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut burners = Vec::with_capacity(burner_count as usize);
    for _ in 0..burner_count {
        let stop = Arc::clone(&stop);
        burners.push(std::thread::spawn(move || {
            let mut accumulator = 0_u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                for step in 0..8192_u64 {
                    accumulator = accumulator
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(step);
                }
                std::hint::black_box(accumulator);
            }
        }));
    }

    eprintln!("[AMP] starting: iters={iterations} peers={peer_count} burners={burner_count}");
    let mut failures = 0_u32;
    let mut first_failure_iter: Option<u32> = None;
    let mut run_error: Option<String> = None;
    for iteration in 0..iterations {
        match amplify_interleave_once(iteration, peer_count) {
            Ok(true) => {
                failures = failures.saturating_add(1);
                if first_failure_iter.is_none() {
                    first_failure_iter = Some(iteration);
                }
            }
            Ok(false) => {}
            Err(error) => {
                run_error = Some(error.to_string());
                break;
            }
        }
        if iteration % 25 == 0 {
            eprintln!("[AMP] progress iter={iteration} failures_so_far={failures}");
        }
    }

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for burner in burners {
        let _ = burner.join();
    }

    eprintln!(
        "[AMP] RESULT: {failures} interleave failures / {iterations} iterations \
         under {burner_count} burners, {peer_count} peers; first_failure_iter={first_failure_iter:?}"
    );
    if let Some(error) = run_error {
        return Err(format!("amplifier aborted on a non-interleave error: {error}").into());
    }
    // Fail-first gate: while an unsolicited `ServerPush` can displace the
    // response that `roundtrip` reads back, this is non-zero. It goes green only
    // once the harness demultiplexes pushes from responses (the recommended
    // fix), never by changing production emission.
    assert_eq!(
        failures, 0,
        "response/push interleave reproduced: an unsolicited ServerPush displaced \
         the semantic response on {failures} of {iterations} roundtrips"
    );
    Ok(())
}
