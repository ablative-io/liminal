//! Real-socket regression for Leave behind supersession-created marker work.

use std::error::Error;
use std::path::Path;

use liminal_protocol::wire::{
    AttachAttemptToken, AttachBound, ClientRequest, CredentialAttachRequest, DetachAttemptToken,
    DetachRequest, EnrollBound, EnrollmentRequest, EnrollmentToken, Generation, LeaveAttemptToken,
    LeaveRequest, ParticipantId, ParticipantRecord, RecordAdmission, RecordAdmissionAttemptToken,
    RecordCommitted, ServerPush, ServerValue,
};

use super::e2e_tests::{SocketFixture, SocketPeer};
use super::tests_marker_ack_fixture::marker_fixture_config;

const CONVERSATION: u64 = 527;

fn enroll_three(
    primary: &mut SocketFixture,
    peer: &mut SocketPeer,
    leaver: &mut SocketPeer,
) -> Result<(EnrollBound, EnrollBound, EnrollBound), Box<dyn Error>> {
    let alpha = primary.request(ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: CONVERSATION,
        enrollment_token: EnrollmentToken::new([0xA1; 16]),
    }))?;
    let ServerValue::EnrollBound(alpha) = alpha else {
        return Err(format!("participant A did not enroll: {alpha:?}").into());
    };
    let bravo = peer.request(ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: CONVERSATION,
        enrollment_token: EnrollmentToken::new([0xB1; 16]),
    }))?;
    let ServerValue::EnrollBound(bravo) = bravo else {
        return Err(format!("participant B did not enroll: {bravo:?}").into());
    };
    let charlie = leaver.request(ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: CONVERSATION,
        enrollment_token: EnrollmentToken::new([0xC1; 16]),
    }))?;
    let ServerValue::EnrollBound(charlie) = charlie else {
        return Err(format!("participant C did not enroll: {charlie:?}").into());
    };
    Ok((alpha, bravo, charlie))
}

fn rotate_leaver(
    primary: &SocketFixture,
    original: &mut SocketPeer,
    leaver: &EnrollBound,
) -> Result<(SocketPeer, AttachBound), Box<dyn Error>> {
    let detached = original.request(ClientRequest::Detach(DetachRequest {
        conversation_id: CONVERSATION,
        participant_id: leaver.participant_id(),
        capability_generation: Generation::ONE,
        detach_attempt_token: DetachAttemptToken::new([0xC2; 16]),
    }))?;
    assert!(matches!(detached, ServerValue::DetachCommitted(_)));

    let mut reconnect = primary.spawn_peer()?;
    let ordinary = reconnect.request(ClientRequest::CredentialAttach(CredentialAttachRequest {
        conversation_id: CONVERSATION,
        participant_id: leaver.participant_id(),
        capability_generation: Generation::ONE,
        attach_secret: leaver.attach_secret(),
        attach_attempt_token: AttachAttemptToken::new([0xC3; 16]),
        accept_marker_delivery_seq: None,
    }))?;
    let ServerValue::AttachBound(ordinary) = ordinary else {
        return Err(format!("participant C ordinary reattach failed: {ordinary:?}").into());
    };

    let mut replacement = primary.spawn_peer()?;
    let superseding =
        replacement.request(ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: CONVERSATION,
            participant_id: leaver.participant_id(),
            capability_generation: ordinary.capability_generation(),
            attach_secret: ordinary.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([0xC4; 16]),
            accept_marker_delivery_seq: None,
        }))?;
    let ServerValue::AttachBound(superseding) = superseding else {
        return Err(format!("participant C superseding attach failed: {superseding:?}").into());
    };
    drop(reconnect);
    Ok((replacement, superseding))
}

fn commit_and_deliver_record(
    primary: &mut SocketFixture,
    recipient: &mut SocketPeer,
    sender: ParticipantId,
) -> Result<RecordCommitted, Box<dyn Error>> {
    let outcome = primary.request(ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: CONVERSATION,
        participant_id: sender,
        capability_generation: Generation::ONE,
        record_admission_attempt_token: RecordAdmissionAttemptToken::new([0xA2; 16]),
        payload: vec![0xD3],
    }))?;
    let ServerValue::RecordCommitted(committed) = outcome else {
        return Err(format!("participant A's ordinary record did not commit: {outcome:?}").into());
    };

    primary.open_publication_replay()?;
    let mut wake_peer = primary.spawn_peer()?;
    let wake = wake_peer.request(ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: CONVERSATION,
        participant_id: u64::MAX,
        capability_generation: Generation::ONE,
        record_admission_attempt_token: RecordAdmissionAttemptToken::new([0xD1; 16]),
        payload: Vec::new(),
    }))?;
    assert!(matches!(wake, ServerValue::ParticipantUnknown(_)));
    let ServerPush::ParticipantDelivery(delivery) = recipient.read_push()? else {
        return Err("participant C did not receive the ordinary record".into());
    };
    assert_eq!(delivery.delivery_seq, committed.delivery_seq());
    assert!(matches!(
        delivery.record,
        ParticipantRecord::OrdinaryRecord { sender_participant_id, ref payload }
            if sender_participant_id == sender && payload == &[0xD3]
    ));
    Ok(committed)
}

fn reopened_fixture(data_dir: &Path) -> Result<SocketFixture, Box<dyn Error>> {
    SocketFixture::start_replay_gated_with_config(data_dir, marker_fixture_config())
}

#[test]
fn leave_after_detach_reattach_supersession_discharges_unacked_obligation_and_reopens()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let mut primary = reopened_fixture(&data_dir)?;
    let mut peer = primary.spawn_peer()?;
    let mut original = primary.spawn_peer()?;
    let (sender, observer, leaver) = enroll_three(&mut primary, &mut peer, &mut original)?;
    let (mut replacement, binding) = rotate_leaver(&primary, &mut original, &leaver)?;
    let record =
        commit_and_deliver_record(&mut primary, &mut replacement, sender.participant_id())?;

    let before = primary.outbox_owner_facts(CONVERSATION, leaver.participant_id())?;
    assert_eq!(before.next_live_obligation, Some(record.delivery_seq()));
    assert_eq!(primary.immutable_candidate_counts(CONVERSATION)?, (3, 1));
    let leave = LeaveRequest {
        conversation_id: CONVERSATION,
        participant_id: leaver.participant_id(),
        capability_generation: binding.capability_generation(),
        attach_secret: binding.attach_secret(),
        leave_attempt_token: LeaveAttemptToken::new([0xC5; 16]),
    };
    let outcome = replacement.request(ClientRequest::Leave(leave.clone()))?;
    let ServerValue::LeaveCommitted(committed) = outcome else {
        return Err(format!("participant C Leave did not commit: {outcome:?}").into());
    };
    let after = primary.outbox_owner_facts(CONVERSATION, leaver.participant_id())?;
    assert_eq!(after.next_live_obligation, None);
    let ids = [
        sender.participant_id(),
        observer.participant_id(),
        leaver.participant_id(),
    ];
    let durable = [
        primary.outbox_owner_facts(CONVERSATION, ids[0])?,
        primary.outbox_owner_facts(CONVERSATION, ids[1])?,
        after,
    ];

    drop(peer);
    drop(original);
    drop(replacement);
    primary.stop();
    let mut reopened = reopened_fixture(&data_dir)?;
    for (participant_id, expected) in ids.into_iter().zip(durable) {
        assert_eq!(
            reopened.outbox_owner_facts(CONVERSATION, participant_id)?,
            expected
        );
    }
    assert_eq!(
        reopened.request(ClientRequest::Leave(leave))?,
        ServerValue::LeaveCommitted(committed)
    );
    reopened.stop();
    Ok(())
}
