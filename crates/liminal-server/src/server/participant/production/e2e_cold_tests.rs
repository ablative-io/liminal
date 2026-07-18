//! Real-socket record and Leave acceptance across a same-directory cold reopen.

use std::error::Error;
use std::path::Path;

use liminal_protocol::wire::{
    AttachAttemptToken, AttachSecret, ClientRequest, CredentialAttachRequest, DetachAttemptToken,
    DetachRequest, EnrollmentRequest, EnrollmentToken, Generation, LeaveAttemptToken, LeaveRequest,
    ParticipantAck, ParticipantRecord, RecordAdmission, RecordAdmissionAttemptToken,
    RecordCommitted, ServerPush, ServerValue,
};

use super::e2e_tests::SocketFixture;

struct ColdSocketState {
    participant_id: u64,
    attach_secret: AttachSecret,
    last_record_seq: u64,
}

fn committed_record(
    socket: &mut SocketFixture,
    conversation_id: u64,
    participant_id: u64,
    generation: Generation,
    token: u8,
) -> Result<RecordCommitted, Box<dyn Error>> {
    let value = socket.request(ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id,
        participant_id,
        capability_generation: generation,
        record_admission_attempt_token: RecordAdmissionAttemptToken::new([token; 16]),
        payload: vec![token, 1, 2, 3],
    }))?;
    let ServerValue::RecordCommitted(committed) = value else {
        return Err(format!("record {token:#x} did not commit: {value:?}").into());
    };
    if committed.request().record_admission_attempt_token
        != RecordAdmissionAttemptToken::new([token; 16])
    {
        return Err("record response did not echo its exact token".into());
    }
    Ok(committed)
}

fn write_two_records_then_stop(data_dir: &Path) -> Result<ColdSocketState, Box<dyn Error>> {
    let mut socket = SocketFixture::start(data_dir)?;
    let enrolled = socket.request(ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: 501,
        enrollment_token: EnrollmentToken::new([0x51; 16]),
    }))?;
    let ServerValue::EnrollBound(receipt) = enrolled else {
        return Err(format!("cold-reopen enrollment did not bind: {enrolled:?}").into());
    };
    let first = committed_record(
        &mut socket,
        501,
        receipt.participant_id(),
        Generation::ONE,
        0x52,
    )?;
    let second = committed_record(
        &mut socket,
        501,
        receipt.participant_id(),
        Generation::ONE,
        0x53,
    )?;
    if second.delivery_seq() != first.delivery_seq().saturating_add(1) {
        return Err("second real-socket record was not the next sequence".into());
    }
    let state = ColdSocketState {
        participant_id: receipt.participant_id(),
        attach_secret: receipt.attach_secret(),
        last_record_seq: second.delivery_seq(),
    };
    socket.stop();
    Ok(state)
}

fn commit_bound_leave_after_reopen(
    socket: &mut SocketFixture,
    state: &ColdSocketState,
) -> Result<(), Box<dyn Error>> {
    let attached = socket.request(ClientRequest::CredentialAttach(CredentialAttachRequest {
        conversation_id: 501,
        participant_id: state.participant_id,
        capability_generation: Generation::ONE,
        attach_secret: state.attach_secret,
        attach_attempt_token: AttachAttemptToken::new([0x54; 16]),
        accept_marker_delivery_seq: None,
    }))?;
    let ServerValue::AttachBound(bound) = attached else {
        return Err(format!("cold-reopen attach did not bind: {attached:?}").into());
    };
    let third = committed_record(
        socket,
        501,
        state.participant_id,
        bound.capability_generation(),
        0x55,
    )?;
    let expected_third_seq = state
        .last_record_seq
        .checked_add(3)
        .ok_or("cold-reopen sequence fixture overflowed")?;
    if third.delivery_seq() != expected_third_seq {
        return Err(format!(
            "cold-reopen record sequence was {}, expected exact next post-attach sequence {expected_third_seq}",
            third.delivery_seq()
        )
        .into());
    }
    let leave = LeaveRequest {
        conversation_id: 501,
        participant_id: state.participant_id,
        capability_generation: bound.capability_generation(),
        attach_secret: bound.attach_secret(),
        leave_attempt_token: LeaveAttemptToken::new([0x56; 16]),
    };
    let committed = socket.request(ClientRequest::Leave(leave.clone()))?;
    let ServerValue::LeaveCommitted(committed) = committed else {
        return Err(format!("cold-reopen bound Leave did not commit: {committed:?}").into());
    };
    let replayed = socket.request(ClientRequest::Leave(leave))?;
    if replayed != ServerValue::LeaveCommitted(committed) {
        return Err("bound Leave exact-token socket replay drifted".into());
    }
    Ok(())
}

fn commit_detached_leave_on_same_socket(socket: &mut SocketFixture) -> Result<(), Box<dyn Error>> {
    let enrolled = socket.request(ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: 502,
        enrollment_token: EnrollmentToken::new([0x57; 16]),
    }))?;
    let ServerValue::EnrollBound(receipt) = enrolled else {
        return Err(format!("detached-Leave enrollment did not bind: {enrolled:?}").into());
    };
    let detached = socket.request(ClientRequest::Detach(DetachRequest {
        conversation_id: 502,
        participant_id: receipt.participant_id(),
        capability_generation: Generation::ONE,
        detach_attempt_token: DetachAttemptToken::new([0x58; 16]),
    }))?;
    if !matches!(detached, ServerValue::DetachCommitted(_)) {
        return Err(format!("socket detach did not commit: {detached:?}").into());
    }
    let leave = LeaveRequest {
        conversation_id: 502,
        participant_id: receipt.participant_id(),
        capability_generation: Generation::ONE,
        attach_secret: receipt.attach_secret(),
        leave_attempt_token: LeaveAttemptToken::new([0x59; 16]),
    };
    let committed = socket.request(ClientRequest::Leave(leave.clone()))?;
    let ServerValue::LeaveCommitted(committed) = committed else {
        return Err(format!("socket detached Leave did not commit: {committed:?}").into());
    };
    if committed.ended_binding_epoch().is_some()
        || committed.prior_terminal_delivery_seq().is_none()
    {
        return Err("detached Leave returned the wrong binding fate".into());
    }
    let replayed = socket.request(ClientRequest::Leave(leave))?;
    if replayed != ServerValue::LeaveCommitted(committed) {
        return Err("detached Leave exact-token socket replay drifted".into());
    }
    Ok(())
}

#[test]
fn restart_between_delivery_and_ack_accepts() -> Result<(), Box<dyn Error>> {
    const CONVERSATION: u64 = 526;
    const PAYLOAD: &[u8] = &[0x26, 0x00, 0xFF, 0xA5];

    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let mut first_server = SocketFixture::start(&data_dir)?;
    let mut sender_socket = first_server.spawn_peer()?;
    let enrolled = first_server.request(ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: CONVERSATION,
        enrollment_token: EnrollmentToken::new([0x26; 16]),
    }))?;
    let ServerValue::EnrollBound(recipient) = enrolled else {
        return Err(format!("recipient enrollment did not bind: {enrolled:?}").into());
    };
    let sender_enrolled = sender_socket.request(ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: CONVERSATION,
        enrollment_token: EnrollmentToken::new([0xA6; 16]),
    }))?;
    let ServerValue::EnrollBound(sender) = sender_enrolled else {
        return Err(format!("sender enrollment did not bind: {sender_enrolled:?}").into());
    };
    let ServerPush::ParticipantDelivery(attached_delivery) = first_server.read_push()? else {
        return Err("the recipient's initial obligation was not a participant delivery".into());
    };
    assert_eq!(attached_delivery.conversation_id, CONVERSATION);
    assert_eq!(attached_delivery.delivery_seq, 2);

    let initial_ack = ParticipantAck {
        conversation_id: CONVERSATION,
        participant_id: recipient.participant_id(),
        capability_generation: Generation::ONE,
        through_seq: attached_delivery.delivery_seq,
    };
    assert!(matches!(
        first_server.request(ClientRequest::ParticipantAck(initial_ack))?,
        ServerValue::AckCommitted(_)
    ));

    let committed = sender_socket.request(ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: CONVERSATION,
        participant_id: sender.participant_id(),
        capability_generation: Generation::ONE,
        record_admission_attempt_token: RecordAdmissionAttemptToken::new([0xC6; 16]),
        payload: PAYLOAD.to_vec(),
    }))?;
    let ServerValue::RecordCommitted(committed) = committed else {
        return Err(format!("sentinel record did not commit: {committed:?}").into());
    };
    let delivered_seq = committed.delivery_seq();
    let ServerPush::ParticipantDelivery(delivered) = first_server.read_push()? else {
        return Err("the committed obligation was not delivered as participant push".into());
    };
    assert_eq!(delivered.delivery_seq, delivered_seq);
    assert_eq!(
        delivered.record,
        ParticipantRecord::OrdinaryRecord {
            sender_participant_id: sender.participant_id(),
            payload: PAYLOAD.to_vec(),
        }
    );
    let offered_facts =
        first_server.participant_owner_facts(CONVERSATION, recipient.participant_id())?;
    assert_eq!(offered_facts.frontier_cursor, 2);
    assert_eq!(offered_facts.outbox_ack_through, 2);
    assert_eq!(offered_facts.next_live_obligation, Some(delivered_seq));
    assert_eq!(offered_facts.live_record_count, 1);
    assert!(offered_facts.charged_bytes > 0);
    let recipient_id = recipient.participant_id();
    let attach_secret = recipient.attach_secret();

    // `stop` drops the client, synchronously shuts down and joins the
    // supervisor, then drops the connection, handler, service, and disk-store
    // owners before this same directory is reopened.
    drop(sender_socket);
    first_server.stop();

    let mut reopened = SocketFixture::start_replay_gated(&data_dir)?;
    let attached = reopened.request(ClientRequest::CredentialAttach(CredentialAttachRequest {
        conversation_id: CONVERSATION,
        participant_id: recipient_id,
        capability_generation: Generation::ONE,
        attach_secret,
        attach_attempt_token: AttachAttemptToken::new([0xD6; 16]),
        accept_marker_delivery_seq: None,
    }))?;
    let ServerValue::AttachBound(attached) = attached else {
        return Err(format!("post-restart recipient attach did not bind: {attached:?}").into());
    };
    assert!(
        reopened.blocked_publication_scans()? > 0,
        "the deterministic gate did not intercept replay before a duplicate offer"
    );

    let before_ack = reopened.participant_owner_facts(CONVERSATION, recipient_id)?;
    assert_eq!(before_ack.frontier_cursor, 2);
    assert_eq!(before_ack.outbox_ack_through, 2);
    assert_eq!(before_ack.next_live_obligation, Some(delivered_seq));
    let truthful_ack = ParticipantAck {
        conversation_id: CONVERSATION,
        participant_id: recipient_id,
        capability_generation: attached.capability_generation(),
        through_seq: delivered_seq,
    };
    let outcome = reopened.request(ClientRequest::ParticipantAck(truthful_ack))?;
    let ServerValue::AckCommitted(committed_ack) = outcome else {
        return Err(format!("reconciled durable obligation ack was refused: {outcome:?}").into());
    };
    assert_eq!(committed_ack.request().conversation_id, CONVERSATION);
    assert_eq!(committed_ack.request().participant_id, recipient_id);
    assert_eq!(
        committed_ack.request().capability_generation,
        attached.capability_generation()
    );
    assert_eq!(committed_ack.request().through_seq, delivered_seq);

    let after_ack = reopened.participant_owner_facts(CONVERSATION, recipient_id)?;
    assert_eq!(after_ack.frontier_cursor, delivered_seq);
    assert_eq!(after_ack.outbox_ack_through, delivered_seq);
    assert_eq!(after_ack.next_live_obligation, None);
    assert_eq!(
        after_ack.live_record_count + 1,
        before_ack.live_record_count
    );
    assert!(after_ack.charged_bytes < before_ack.charged_bytes);
    reopened.stop();
    Ok(())
}

#[test]
fn records_and_leave_survive_real_socket_cold_reopen() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let state = write_two_records_then_stop(&data_dir)?;

    // Every first-server socket, process, service, handler, and store owner was
    // synchronously stopped and dropped before opening this same directory.
    let mut reopened = SocketFixture::start(&data_dir)?;
    commit_bound_leave_after_reopen(&mut reopened, &state)?;
    commit_detached_leave_on_same_socket(&mut reopened)?;
    reopened.stop();
    Ok(())
}
