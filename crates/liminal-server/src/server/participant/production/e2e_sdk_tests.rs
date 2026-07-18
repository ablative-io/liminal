//! SDK-backed real-socket acceptance oracles for ServerPush delivery and response
//! correlation. These clients enter through `RemoteParticipantHandle`; no test
//! constructs an inbound push or applies participant state directly.

use std::error::Error;
use std::net::SocketAddr;

use liminal_protocol::wire::{
    AttachAttemptToken, ClientRequest, CredentialAttachRequest, EnrollmentRequest, EnrollmentToken,
    Generation, ParticipantAck, ParticipantDelivery, ParticipantRecord, RecordAdmission,
    RecordAdmissionAttemptToken, ServerPush, ServerValue,
};
use liminal_sdk::{
    ConnectionPoolConfig, ParticipantResumeStore, RemoteConfig, RemoteOperationRecordOutcome,
    RemoteParticipantError, RemoteParticipantHandle, RemoteParticipantInbound,
    RemoteParticipantSendOutcome, SdkError,
};

use super::SdkSocketFixture;

const G1_CONVERSATION: u64 = 0x22_01;

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

#[test]
fn serverpush_sent_is_not_receipt_real_socket() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let server = SdkSocketFixture::start(&home.path().join("g1"))?;
    let address = server.address()?;

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
    let expected_peer_two_attach = ServerPush::ParticipantDelivery(ParticipantDelivery {
        conversation_id: G1_CONVERSATION,
        delivery_seq: 3,
        record: ParticipantRecord::Attached {
            affected_participant_id: peer_two_bound.participant_id(),
            binding_epoch: peer_two_bound.origin_binding_epoch(),
        },
    });
    assert_eq!(sender_saw_peer_two, expected_peer_two_attach);
    assert_eq!(peer_one_saw_peer_two, expected_peer_two_attach);

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
