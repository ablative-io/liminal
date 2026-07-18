use std::error::Error;

use liminal_protocol::algebra::ResourceDimension;
use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, EnrollmentRequest, EnrollmentToken, Generation,
    ParticipantAck, RecordAdmission, RecordAdmissionAttemptToken, ServerValue,
};

use super::ProductionParticipantHandler;
use super::tests::{dispatch, open_disk_store_for_tests, test_participant_config};

#[test]
fn authorized_record_at_static_capacity_refuses_with_exact_token() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(87, 1);
    let conversation_id = 905;
    let store = open_disk_store_for_tests(&data_dir)?;
    let mut config = test_participant_config();
    config.max_ordinary_record_bytes = 1;
    let handler = ProductionParticipantHandler::new(store, config)?;

    let enrolled = dispatch(
        &handler,
        incarnation,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([122; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(receipt) = enrolled else {
        return Err(format!("capacity fixture enrollment did not bind: {enrolled:?}").into());
    };
    let token = RecordAdmissionAttemptToken::new([0xA5; 16]);
    let refused = dispatch(
        &handler,
        incarnation,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id,
            participant_id: receipt.participant_id(),
            capability_generation: Generation::ONE,
            record_admission_attempt_token: token,
            payload: vec![8],
        }),
    )?;
    let ServerValue::RecordTooLarge(too_large) = refused else {
        return Err(format!("authorized at-capacity record must be typed: {refused:?}").into());
    };
    assert_eq!(too_large.request.record_admission_attempt_token, token);
    assert_eq!(too_large.dimension, ResourceDimension::Bytes);

    let acknowledged = dispatch(
        &handler,
        incarnation,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id,
            participant_id: receipt.participant_id(),
            capability_generation: Generation::ONE,
            through_seq: 1,
        }),
    )?;
    assert!(matches!(acknowledged, ServerValue::AckCommitted(_)));
    Ok(())
}
