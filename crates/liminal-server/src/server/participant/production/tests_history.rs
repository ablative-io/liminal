use std::error::Error;
use std::path::Path;

use liminal_protocol::wire::{
    AttachAttemptToken, ClientRequest, ConnectionIncarnation, CredentialAttachRequest,
    DetachAttemptToken, DetachRequest, EnrollBound, EnrollmentRequest, EnrollmentToken, Generation,
    LeaveAttemptToken, LeaveRequest, MarkerAck, ParticipantAck, RecordAdmission,
    RecordAdmissionAttemptToken, ServerValue,
};

use super::ProductionParticipantHandler;
use super::tests::{dispatch, open_disk_store_for_tests, test_participant_config};

fn authority_snapshot(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
) -> Result<String, Box<dyn Error>> {
    let cell = handler.cell(conversation_id)?;
    let owner = cell
        .lock()
        .map_err(|_| "conversation authority lock was poisoned")?;
    let authority = owner
        .as_ref()
        .ok_or("conversation authority must be restored")?;
    let snapshot = format!("{authority:#?}");
    drop(owner);
    Ok(snapshot)
}

fn cold_reopen_matches_live(
    handler: ProductionParticipantHandler,
    data_dir: &Path,
    conversation_id: u64,
    history_step: &str,
) -> Result<ProductionParticipantHandler, Box<dyn Error>> {
    let live = authority_snapshot(&handler, conversation_id)?;
    drop(handler);
    let store = open_disk_store_for_tests(data_dir)?;
    let restored = ProductionParticipantHandler::new(store, test_participant_config())?;
    let cold = authority_snapshot(&restored, conversation_id)?;
    assert_eq!(live, cold, "live/cold state diverged after {history_step}");
    Ok(restored)
}

fn require_enrolled(value: ServerValue) -> Result<EnrollBound, Box<dyn Error>> {
    let ServerValue::EnrollBound(receipt) = value else {
        return Err(format!("history enrollment did not bind: {value:?}").into());
    };
    Ok(receipt)
}

fn walk_enrollment_history(
    data_dir: &Path,
    conversation_id: u64,
    first_connection: ConnectionIncarnation,
    second_connection: ConnectionIncarnation,
) -> Result<(ProductionParticipantHandler, EnrollBound, EnrollBound), Box<dyn Error>> {
    let store = open_disk_store_for_tests(data_dir)?;
    let mut handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    let first = require_enrolled(dispatch(
        &handler,
        first_connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0xD0; 16]),
        }),
    )?)?;
    handler = cold_reopen_matches_live(handler, data_dir, conversation_id, "initial enrollment")?;

    let second = require_enrolled(dispatch(
        &handler,
        second_connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0xD1; 16]),
        }),
    )?)?;
    handler =
        cold_reopen_matches_live(handler, data_dir, conversation_id, "subsequent enrollment")?;
    Ok((handler, first, second))
}

fn walk_attach_detach_and_ack_history(
    mut handler: ProductionParticipantHandler,
    data_dir: &Path,
    conversation_id: u64,
    first_connection: ConnectionIncarnation,
    second_connection: ConnectionIncarnation,
    first: &EnrollBound,
    second: &EnrollBound,
) -> Result<ProductionParticipantHandler, Box<dyn Error>> {
    let attached = dispatch(
        &handler,
        first_connection,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id,
            participant_id: first.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: first.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([0xD2; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    let ServerValue::AttachBound(attached) = attached else {
        return Err(format!("history attach did not bind: {attached:?}").into());
    };
    assert_eq!(attached.capability_generation().get(), 2);
    handler = cold_reopen_matches_live(handler, data_dir, conversation_id, "attach")?;

    let detached = dispatch(
        &handler,
        first_connection,
        ClientRequest::Detach(DetachRequest {
            conversation_id,
            participant_id: first.participant_id(),
            capability_generation: attached.capability_generation(),
            detach_attempt_token: DetachAttemptToken::new([0xD3; 16]),
        }),
    )?;
    assert!(matches!(detached, ServerValue::DetachCommitted(_)));
    handler = cold_reopen_matches_live(handler, data_dir, conversation_id, "detach")?;

    let acknowledged = dispatch(
        &handler,
        second_connection,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id,
            participant_id: second.participant_id(),
            capability_generation: Generation::ONE,
            through_seq: 5,
        }),
    )?;
    assert!(matches!(acknowledged, ServerValue::AckCommitted(_)));
    handler = cold_reopen_matches_live(handler, data_dir, conversation_id, "participant ack")?;

    let marker_ack = dispatch(
        &handler,
        second_connection,
        ClientRequest::MarkerAck(MarkerAck {
            conversation_id,
            participant_id: second.participant_id(),
            capability_generation: Generation::ONE,
            marker_delivery_seq: 5,
        }),
    )?;
    assert!(matches!(
        marker_ack,
        ServerValue::MarkerMismatch(_) | ServerValue::MarkerNotDelivered(_)
    ));
    cold_reopen_matches_live(handler, data_dir, conversation_id, "marker ack")
}

fn walk_record_and_leave_history(
    mut handler: ProductionParticipantHandler,
    data_dir: &Path,
    conversation_id: u64,
    connection: ConnectionIncarnation,
    participant: &EnrollBound,
) -> Result<(), Box<dyn Error>> {
    for (index, token) in [0xD4_u8, 0xD5].into_iter().enumerate() {
        let recorded = dispatch(
            &handler,
            connection,
            ClientRequest::RecordAdmission(RecordAdmission {
                conversation_id,
                participant_id: participant.participant_id(),
                capability_generation: Generation::ONE,
                record_admission_attempt_token: RecordAdmissionAttemptToken::new([token; 16]),
                payload: vec![token],
            }),
        )?;
        assert!(matches!(recorded, ServerValue::RecordCommitted(_)));
        let step = format!("ordinary record {}", index + 1);
        handler = cold_reopen_matches_live(handler, data_dir, conversation_id, &step)?;
    }

    let left = dispatch(
        &handler,
        connection,
        ClientRequest::Leave(LeaveRequest {
            conversation_id,
            participant_id: participant.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: participant.attach_secret(),
            leave_attempt_token: LeaveAttemptToken::new([0xD6; 16]),
        }),
    )?;
    assert!(matches!(left, ServerValue::LeaveCommitted(_)));
    let _ = cold_reopen_matches_live(handler, data_dir, conversation_id, "Leave")?;
    Ok(())
}

#[test]
fn every_transition_history_matches_live_state_after_cold_restore() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let conversation_id = 906;
    let first_connection = ConnectionIncarnation::new(88, 1);
    let second_connection = ConnectionIncarnation::new(88, 2);
    let (handler, first, second) = walk_enrollment_history(
        &data_dir,
        conversation_id,
        first_connection,
        second_connection,
    )?;
    let handler = walk_attach_detach_and_ack_history(
        handler,
        &data_dir,
        conversation_id,
        first_connection,
        second_connection,
        &first,
        &second,
    )?;
    walk_record_and_leave_history(
        handler,
        &data_dir,
        conversation_id,
        second_connection,
        &second,
    )
}
