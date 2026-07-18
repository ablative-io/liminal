//! SDK-backed real-socket acceptance oracles for `ServerPush` delivery and response
//! correlation. These clients enter through `RemoteParticipantHandle`; no test
//! constructs an inbound push or applies participant state directly.

use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use liminal_protocol::wire::{
    AttachAttemptToken, ClientRequest, CredentialAttachRequest, EnrollBound, EnrollmentRequest,
    EnrollmentToken, Generation, ParticipantAck, ParticipantDelivery, ParticipantRecord,
    RecordAdmission, RecordAdmissionAttemptToken, ServerPush, ServerValue,
};
use liminal_sdk::{
    ConnectionPoolConfig, ParticipantResumeStore, RemoteConfig, RemoteOperationRecordOutcome,
    RemoteParticipantError, RemoteParticipantHandle, RemoteParticipantInbound,
    RemoteParticipantSendOutcome, SdkError,
};

use super::SdkSocketFixture;

const G1_CONVERSATION: u64 = 0x22_01;
const G2_CONVERSATION: u64 = 0x23_01;

#[derive(Debug, Default)]
struct MemoryResumeStore {
    canonical: Vec<u8>,
}

impl ParticipantResumeStore for MemoryResumeStore {
    fn persist(&mut self, canonical_lpcr: &[u8]) -> Result<(), SdkError> {
        self.canonical.clear();
        self.canonical.extend_from_slice(canonical_lpcr);
        Ok(())
    }
}

type SdkParticipant = RemoteParticipantHandle<MemoryResumeStore>;

fn connect_participant(
    address: SocketAddr,
    conversation_id: u64,
) -> Result<SdkParticipant, Box<dyn Error>> {
    let config = RemoteConfig::new(
        address.to_string(),
        "participant-acceptance",
        conversation_id.to_string(),
        ConnectionPoolConfig::new(1, 1, 8),
    )?
    .connect_tcp()?;
    Ok(RemoteParticipantHandle::new(
        &config,
        MemoryResumeStore::default(),
    )?)
}

fn send_operation(
    participant: &SdkParticipant,
    request: ClientRequest,
) -> Result<(), Box<dyn Error>> {
    let operation = match participant.record_operation(request)? {
        RemoteOperationRecordOutcome::Recorded(operation)
        | RemoteOperationRecordOutcome::Continuous(operation) => operation,
        RemoteOperationRecordOutcome::Refused { request, reason } => {
            return Err(format!("SDK refused outbound request {request:?}: {reason:?}").into());
        }
    };
    match participant.send_operation(operation)? {
        RemoteParticipantSendOutcome::Sent { .. } => Ok(()),
        RemoteParticipantSendOutcome::TransportLost { error, .. } => {
            Err(format!("SDK transport lost while sending participant operation: {error}").into())
        }
    }
}

fn exchange(
    participant: &SdkParticipant,
    request: ClientRequest,
) -> Result<RemoteParticipantInbound, Box<dyn Error>> {
    send_operation(participant, request)?;
    participant.receive().map_err(Into::into)
}

fn expect_applied(inbound: RemoteParticipantInbound) -> Result<ServerValue, Box<dyn Error>> {
    match inbound {
        RemoteParticipantInbound::Applied { value, .. } => Ok(value),
        other => Err(format!("expected SDK-applied server value, got {other:?}").into()),
    }
}

fn expect_push(participant: &SdkParticipant) -> Result<(ServerPush, u64, u64), Box<dyn Error>> {
    match participant.receive()? {
        RemoteParticipantInbound::Push { value, provenance } => {
            Ok((value, provenance.connection_id(), provenance.attempt_id()))
        }
        other => Err(format!("expected exact SDK Push inbound, got {other:?}").into()),
    }
}

fn acknowledge(
    participant: &SdkParticipant,
    participant_id: u64,
    generation: Generation,
    through_seq: u64,
) -> Result<(), Box<dyn Error>> {
    let value = expect_applied(exchange(
        participant,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: G1_CONVERSATION,
            participant_id,
            capability_generation: generation,
            through_seq,
        }),
    )?)?;
    let ServerValue::AckCommitted(committed) = value else {
        return Err(format!("participant ack did not commit: {value:?}").into());
    };
    assert_eq!(committed.request().through_seq, through_seq);
    Ok(())
}

fn assert_no_sdk_push(participant: &SdkParticipant) -> Result<(), Box<dyn Error>> {
    match participant.receive() {
        Err(RemoteParticipantError::Transport(_)) => Ok(()),
        Ok(RemoteParticipantInbound::Push { value, .. }) => Err(format!(
            "sender or acknowledged reattach received unexpected push: {value:?}"
        )
        .into()),
        Ok(other) => Err(format!("unexpected SDK inbound while proving no push: {other:?}").into()),
        Err(error) => Err(format!("unexpected SDK receive failure: {error}").into()),
    }
}

fn record_write_ahead(
    participant: &SdkParticipant,
    request: ClientRequest,
) -> Result<liminal_sdk::RemoteParticipantOperation, Box<dyn Error>> {
    match participant.record_operation(request)? {
        RemoteOperationRecordOutcome::Recorded(operation) => Ok(operation),
        RemoteOperationRecordOutcome::Continuous(_) => {
            Err("record admission bypassed the SDK write-ahead slot".into())
        }
        RemoteOperationRecordOutcome::Refused { request, reason } => {
            Err(format!("SDK write-ahead refused {request:?}: {reason:?}").into())
        }
    }
}

fn expect_sent(
    participant: &SdkParticipant,
    operation: liminal_sdk::RemoteParticipantOperation,
) -> Result<(), Box<dyn Error>> {
    match participant.send_operation(operation)? {
        RemoteParticipantSendOutcome::Sent { .. } => Ok(()),
        RemoteParticipantSendOutcome::TransportLost { error, .. } => {
            Err(format!("SDK transport lost before the operation was sent: {error}").into())
        }
    }
}

struct G1Participants {
    sender: SdkParticipant,
    sender_bound: EnrollBound,
    peer_one: SdkParticipant,
    peer_one_bound: EnrollBound,
    peer_two: SdkParticipant,
}

fn enroll_g1_participants(address: SocketAddr) -> Result<G1Participants, Box<dyn Error>> {
    let sender = connect_participant(address, G1_CONVERSATION)?;
    let sender_enrolled = expect_applied(exchange(
        &sender,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: G1_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x31; 16]),
        }),
    )?)?;
    let ServerValue::EnrollBound(sender_bound) = sender_enrolled else {
        return Err(format!("sender enrollment did not bind: {sender_enrolled:?}").into());
    };

    let peer_one = connect_participant(address, G1_CONVERSATION)?;
    let peer_one_enrolled = expect_applied(exchange(
        &peer_one,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: G1_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x32; 16]),
        }),
    )?)?;
    let ServerValue::EnrollBound(peer_one_bound) = peer_one_enrolled else {
        return Err(format!("first peer enrollment did not bind: {peer_one_enrolled:?}").into());
    };
    let (sender_saw_peer_one, _, _) = expect_push(&sender)?;
    assert!(matches!(
        sender_saw_peer_one,
        ServerPush::ParticipantDelivery(ParticipantDelivery {
            conversation_id: G1_CONVERSATION,
            delivery_seq: 2,
            record: ParticipantRecord::Attached { affected_participant_id, .. },
        }) if affected_participant_id == peer_one_bound.participant_id()
    ));

    let peer_two = connect_participant(address, G1_CONVERSATION)?;
    let peer_two_enrolled = expect_applied(exchange(
        &peer_two,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: G1_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x33; 16]),
        }),
    )?)?;
    let ServerValue::EnrollBound(peer_two_bound) = peer_two_enrolled else {
        return Err(format!("second peer enrollment did not bind: {peer_two_enrolled:?}").into());
    };
    let (sender_saw_peer_two, _, _) = expect_push(&sender)?;
    let (peer_one_saw_peer_two, _, _) = expect_push(&peer_one)?;
    let expected = ServerPush::ParticipantDelivery(ParticipantDelivery {
        conversation_id: G1_CONVERSATION,
        delivery_seq: 3,
        record: ParticipantRecord::Attached {
            affected_participant_id: peer_two_bound.participant_id(),
            binding_epoch: peer_two_bound.origin_binding_epoch(),
        },
    });
    assert_eq!(sender_saw_peer_two, expected);
    assert_eq!(peer_one_saw_peer_two, expected);
    Ok(G1Participants {
        sender,
        sender_bound,
        peer_one,
        peer_one_bound,
        peer_two,
    })
}

#[test]
fn serverpush_sent_is_not_receipt_real_socket() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let server = SdkSocketFixture::start(&home.path().join("g1"))?;
    let address = server.address()?;
    let G1Participants {
        sender,
        sender_bound,
        peer_one,
        peer_one_bound,
        peer_two,
    } = enroll_g1_participants(address)?;

    // Clear only the pre-record obligations. The ordinary record below is never
    // acknowledged before peer one's first close, so socket handoff cannot be
    // mistaken for receipt.
    acknowledge(&sender, sender_bound.participant_id(), Generation::ONE, 3)?;
    acknowledge(
        &peer_one,
        peer_one_bound.participant_id(),
        Generation::ONE,
        3,
    )?;

    let record_token = RecordAdmissionAttemptToken::new([0xA4; 16]);
    let sentinel_payload = vec![0x00, 0xFF, 0x47, 0x00, 0xA5];
    let committed = expect_applied(exchange(
        &sender,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: G1_CONVERSATION,
            participant_id: sender_bound.participant_id(),
            capability_generation: Generation::ONE,
            record_admission_attempt_token: record_token,
            payload: sentinel_payload.clone(),
        }),
    )?)?;
    let ServerValue::RecordCommitted(committed) = committed else {
        return Err(format!("ordinary record did not commit: {committed:?}").into());
    };
    assert_eq!(
        committed.request().record_admission_attempt_token,
        record_token
    );
    let sequence = committed.delivery_seq();
    let expected_push = ServerPush::ParticipantDelivery(ParticipantDelivery {
        conversation_id: G1_CONVERSATION,
        delivery_seq: sequence,
        record: ParticipantRecord::OrdinaryRecord {
            sender_participant_id: sender_bound.participant_id(),
            payload: sentinel_payload,
        },
    });
    let (peer_one_push, first_connection, first_attempt) = expect_push(&peer_one)?;
    let (peer_two_push, _, _) = expect_push(&peer_two)?;
    assert_eq!(peer_one_push, expected_push);
    assert_eq!(peer_two_push, expected_push);
    assert_no_sdk_push(&sender)?;

    // Closing after a successful SDK Push return supplies no receipt. A fresh
    // socket and credential attach must replay the same durable sequence.
    drop(peer_one);
    let peer_one_reattached = connect_participant(address, G1_CONVERSATION)?;
    let attached = expect_applied(exchange(
        &peer_one_reattached,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: G1_CONVERSATION,
            participant_id: peer_one_bound.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: peer_one_bound.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([0x34; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?)?;
    let ServerValue::AttachBound(attached) = attached else {
        return Err(format!("first peer reattach did not bind: {attached:?}").into());
    };
    let (replayed, replay_connection, replay_attempt) = expect_push(&peer_one_reattached)?;
    assert_eq!(replayed, expected_push);
    assert_eq!(replay_connection, first_connection);
    assert_eq!(replay_attempt, first_attempt);

    // Only the committed cumulative ParticipantAck changes the replay result.
    acknowledge(
        &peer_one_reattached,
        peer_one_bound.participant_id(),
        attached.capability_generation(),
        sequence,
    )?;
    let rotated_secret = attached.attach_secret();
    let generation = attached.capability_generation();
    drop(peer_one_reattached);

    let peer_one_second_reattach = connect_participant(address, G1_CONVERSATION)?;
    let attached_again = expect_applied(exchange(
        &peer_one_second_reattach,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: G1_CONVERSATION,
            participant_id: peer_one_bound.participant_id(),
            capability_generation: generation,
            attach_secret: rotated_secret,
            attach_attempt_token: AttachAttemptToken::new([0x35; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?)?;
    assert!(matches!(attached_again, ServerValue::AttachBound(_)));
    assert_no_sdk_push(&peer_one_second_reattach)?;

    drop(peer_one_second_reattach);
    drop(peer_two);
    drop(sender);
    server.stop()?;
    Ok(())
}

struct G2Pair {
    sender: SdkParticipant,
    peer: Arc<SdkParticipant>,
    sender_id: u64,
    peer_bound: EnrollBound,
}

fn enroll_g2_pair(server: &SdkSocketFixture) -> Result<G2Pair, Box<dyn Error>> {
    let address = server.address()?;
    let sender = connect_participant(address, G2_CONVERSATION)?;
    let sender_value = expect_applied(exchange(
        &sender,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: G2_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x41; 16]),
        }),
    )?)?;
    let ServerValue::EnrollBound(sender_bound) = sender_value else {
        return Err(format!("G2 sender enrollment did not bind: {sender_value:?}").into());
    };

    let peer = Arc::new(connect_participant(address, G2_CONVERSATION)?);
    let peer_value = expect_applied(exchange(
        &peer,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: G2_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x42; 16]),
        }),
    )?)?;
    let ServerValue::EnrollBound(peer_bound) = peer_value else {
        return Err(format!("G2 peer enrollment did not bind: {peer_value:?}").into());
    };
    let (peer_attach, _, _) = expect_push(&sender)?;
    assert!(matches!(
        peer_attach,
        ServerPush::ParticipantDelivery(ParticipantDelivery {
            conversation_id: G2_CONVERSATION,
            delivery_seq: 2,
            record: ParticipantRecord::Attached { affected_participant_id, .. },
        }) if affected_participant_id == peer_bound.participant_id()
    ));

    let sender_ack = expect_applied(exchange(
        &sender,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: G2_CONVERSATION,
            participant_id: sender_bound.participant_id(),
            capability_generation: Generation::ONE,
            through_seq: 2,
        }),
    )?)?;
    assert!(matches!(sender_ack, ServerValue::AckCommitted(_)));
    Ok(G2Pair {
        sender,
        peer,
        sender_id: sender_bound.participant_id(),
        peer_bound,
    })
}

fn assert_g2_fault_arm_is_typed() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let server = SdkSocketFixture::start_gated(&home.path().join("g2-fault"))?;
    let G2Pair {
        sender,
        peer,
        sender_id,
        peer_bound: _,
    } = enroll_g2_pair(&server)?;
    server.fail_next_outbox_append()?;

    let fault_token = RecordAdmissionAttemptToken::new([0xF3; 16]);
    let operation = record_write_ahead(
        &sender,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: G2_CONVERSATION,
            participant_id: sender_id,
            capability_generation: Generation::ONE,
            record_admission_attempt_token: fault_token,
            payload: vec![0xFA, 0x00, 0x17],
        }),
    )?;
    expect_sent(&sender, operation)?;
    assert!(
        matches!(sender.receive(), Err(RemoteParticipantError::Transport(_))),
        "the producer fault arm must terminate with a typed SDK transport outcome, not hang or fabricate a semantic value"
    );

    drop(peer);
    drop(sender);
    server.stop()?;
    Ok(())
}

struct HeldG2 {
    sender: SdkParticipant,
    sender_id: u64,
    held_peer: SdkParticipant,
    held_pid: u64,
    priming_seq: u64,
    priming_payload: Vec<u8>,
}

fn engage_g2_holdback(server: &SdkSocketFixture, pair: G2Pair) -> Result<HeldG2, Box<dyn Error>> {
    let G2Pair {
        sender,
        peer,
        sender_id,
        peer_bound,
    } = pair;
    drop(peer);
    let priming_payload = vec![0xA5; 400];
    let priming = expect_applied(exchange(
        &sender,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: G2_CONVERSATION,
            participant_id: sender_id,
            capability_generation: Generation::ONE,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([0x50; 16]),
            payload: priming_payload.clone(),
        }),
    )?)?;
    let ServerValue::RecordCommitted(priming) = priming else {
        return Err(format!("G2 priming record did not commit: {priming:?}").into());
    };
    server.queue_next_outbound_capacity(480);
    let before = server.active_connection_pids();
    let held_peer = connect_participant(server.address()?, G2_CONVERSATION)?;
    let held_pid = server
        .active_connection_pids()
        .into_iter()
        .find(|pid| !before.contains(pid))
        .ok_or("G2 reattached peer process was not registered")?;
    let holdback = server.install_participant_holdback_pause(held_pid);
    send_operation(
        &held_peer,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: G2_CONVERSATION,
            participant_id: peer_bound.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: peer_bound.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([0x43; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    holdback
        .recv_timeout(Duration::from_secs(2))
        .map_err(|error| format!("G2 peer never engaged holdback: {error}"))?;
    let _ = expect_push(&sender)?;
    let _ = expect_push(&sender)?;
    Ok(HeldG2 {
        sender,
        sender_id,
        held_peer,
        held_pid,
        priming_seq: priming.delivery_seq(),
        priming_payload,
    })
}

struct HeldDeliveries {
    priming_seq: u64,
    priming_payload: Vec<u8>,
    first_seq: u64,
    first_payload: Vec<u8>,
    second_seq: u64,
    second_payload: Vec<u8>,
}

fn resume_and_assert_held_deliveries(
    server: &SdkSocketFixture,
    held_peer: &SdkParticipant,
    held_pid: u64,
    sender_id: u64,
    deliveries: HeldDeliveries,
) -> Result<(), Box<dyn Error>> {
    assert!(server.resume_process(held_pid));
    let attached = expect_applied(held_peer.receive()?)?;
    assert!(matches!(attached, ServerValue::AttachBound(_)));
    let expected = [
        (deliveries.priming_seq, deliveries.priming_payload),
        (deliveries.first_seq, deliveries.first_payload),
        (deliveries.second_seq, deliveries.second_payload),
    ];
    for (delivery_seq, payload) in expected {
        let (push, _, _) = expect_push(held_peer)?;
        assert_eq!(
            push,
            ServerPush::ParticipantDelivery(ParticipantDelivery {
                conversation_id: G2_CONVERSATION,
                delivery_seq,
                record: ParticipantRecord::OrdinaryRecord {
                    sender_participant_id: sender_id,
                    payload
                },
            })
        );
    }
    Ok(())
}

#[test]
fn terminal_answer_precedes_independent_push_work() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let server = SdkSocketFixture::start_gated(&home.path().join("g2"))?;
    let HeldG2 {
        sender,
        sender_id,
        held_peer,
        held_pid,
        priming_seq,
        priming_payload,
    } = engage_g2_holdback(&server, enroll_g2_pair(&server)?)?;

    let first_token = RecordAdmissionAttemptToken::new([0x51; 16]);
    let first_payload = vec![0x00, 0xFF, 0x51, 0x00];
    let first_operation = record_write_ahead(
        &sender,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: G2_CONVERSATION,
            participant_id: sender_id,
            capability_generation: Generation::ONE,
            record_admission_attempt_token: first_token,
            payload: first_payload.clone(),
        }),
    )?;
    expect_sent(&sender, first_operation)?;
    let first_inbound = sender.receive()?;
    let RemoteParticipantInbound::Applied {
        value: ServerValue::RecordCommitted(first_committed),
        ..
    } = first_inbound
    else {
        return Err(format!(
            "record N did not deliver its correlated terminal answer while peer holdback was engaged: {first_inbound:?}"
        )
        .into());
    };
    assert_eq!(
        first_committed.request().record_admission_attempt_token,
        first_token
    );

    // Receiving the exact-token terminal answer releases the SDK's one
    // write-ahead slot. A fresh token must therefore be recordable, sent, and
    // committed on this same connection while the unrelated peer remains held.
    let second_token = RecordAdmissionAttemptToken::new([0x52; 16]);
    let second_payload = vec![0x00, 0xFF, 0x52, 0x00];
    let second_operation = record_write_ahead(
        &sender,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: G2_CONVERSATION,
            participant_id: sender_id,
            capability_generation: Generation::ONE,
            record_admission_attempt_token: second_token,
            payload: second_payload.clone(),
        }),
    )?;
    expect_sent(&sender, second_operation)?;
    let second_inbound = sender.receive()?;
    let RemoteParticipantInbound::Applied {
        value: ServerValue::RecordCommitted(second_committed),
        ..
    } = second_inbound
    else {
        return Err(format!(
            "fresh token response was displaced by independent push work: {second_inbound:?}"
        )
        .into());
    };
    assert_eq!(
        second_committed.request().record_admission_attempt_token,
        second_token
    );
    assert_eq!(
        second_committed.delivery_seq(),
        first_committed.delivery_seq() + 1
    );

    resume_and_assert_held_deliveries(
        &server,
        &held_peer,
        held_pid,
        sender_id,
        HeldDeliveries {
            priming_seq,
            priming_payload,
            first_seq: first_committed.delivery_seq(),
            first_payload,
            second_seq: second_committed.delivery_seq(),
            second_payload,
        },
    )?;

    drop(held_peer);
    drop(sender);
    server.stop()?;

    // A post-commit producer fault is not allowed to become an unbounded silent
    // wait or a fabricated protocol value. The real socket closes and the SDK
    // reports its closed typed transport arm.
    assert_g2_fault_arm_is_typed()?;
    Ok(())
}
