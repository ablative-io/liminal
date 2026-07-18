//! Durable production Leave acceptance coverage.

use std::error::Error;

use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, DetachAttemptToken, DetachRequest, EnrollmentRequest,
    EnrollmentToken, Generation, LeaveAttemptToken, LeaveRequest, ServerValue,
};

use super::ProductionParticipantHandler;
use super::tests::{
    dispatch, dispatch_tracked, open_disk_store_for_tests, test_participant_config,
};

fn generation(value: u64) -> Result<Generation, Box<dyn Error>> {
    Generation::new(value).ok_or_else(|| "zero generation in Leave test fixture".into())
}

#[test]
fn bound_leave_commits_retries_conflicts_and_survives_cold_reopen() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(91, 1);
    let conversation_id = 1_901;
    let request;
    let committed;

    {
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
        let enrolled = dispatch(
            &handler,
            incarnation,
            ClientRequest::Enrollment(EnrollmentRequest {
                conversation_id,
                enrollment_token: EnrollmentToken::new([0xD1; 16]),
            }),
        )?;
        let ServerValue::EnrollBound(receipt) = enrolled else {
            return Err(format!("enrollment did not bind: {enrolled:?}").into());
        };
        request = LeaveRequest {
            conversation_id,
            participant_id: receipt.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: receipt.attach_secret(),
            leave_attempt_token: LeaveAttemptToken::new([0xD2; 16]),
        };

        let first = dispatch(&handler, incarnation, ClientRequest::Leave(request.clone()))?;
        let ServerValue::LeaveCommitted(first_commit) = first else {
            return Err(format!("bound Leave did not commit: {first:?}").into());
        };
        assert_eq!(
            first_commit.leave_attempt_token(),
            request.leave_attempt_token
        );
        assert_eq!(first_commit.participant_id(), request.participant_id);
        assert_eq!(
            first_commit
                .ended_binding_epoch()
                .map(|epoch| epoch.connection_incarnation),
            Some(incarnation)
        );
        committed = first_commit;

        let retry = dispatch(&handler, incarnation, ClientRequest::Leave(request.clone()))?;
        assert_eq!(retry, ServerValue::LeaveCommitted(committed.clone()));

        let conflict = dispatch(
            &handler,
            incarnation,
            ClientRequest::Leave(LeaveRequest {
                capability_generation: generation(2)?,
                ..request
            }),
        )?;
        assert!(matches!(conflict, ServerValue::AttemptTokenBodyConflict(_)));

        let retired = dispatch(
            &handler,
            incarnation,
            ClientRequest::Leave(LeaveRequest {
                leave_attempt_token: LeaveAttemptToken::new([0xD3; 16]),
                ..request
            }),
        )?;
        assert!(matches!(retired, ServerValue::Retired(_)));
    }

    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    let cold_retry = dispatch(&handler, incarnation, ClientRequest::Leave(request))?;
    assert_eq!(cold_retry, ServerValue::LeaveCommitted(committed));
    Ok(())
}

#[test]
fn detached_leave_commits_prior_terminal_and_retries_after_cold_reopen()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(92, 1);
    let conversation_id = 1_902;
    let request;
    let committed;

    {
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
        let enrolled = dispatch(
            &handler,
            incarnation,
            ClientRequest::Enrollment(EnrollmentRequest {
                conversation_id,
                enrollment_token: EnrollmentToken::new([0xD4; 16]),
            }),
        )?;
        let ServerValue::EnrollBound(receipt) = enrolled else {
            return Err(format!("enrollment did not bind: {enrolled:?}").into());
        };
        let participant_id = receipt.participant_id();
        let detached = dispatch(
            &handler,
            incarnation,
            ClientRequest::Detach(DetachRequest {
                conversation_id,
                participant_id,
                capability_generation: Generation::ONE,
                detach_attempt_token: DetachAttemptToken::new([0xD5; 16]),
            }),
        )?;
        let ServerValue::DetachCommitted(detach_commit) = detached else {
            return Err(format!("detach did not commit: {detached:?}").into());
        };
        request = LeaveRequest {
            conversation_id,
            participant_id,
            capability_generation: Generation::ONE,
            attach_secret: receipt.attach_secret(),
            leave_attempt_token: LeaveAttemptToken::new([0xD6; 16]),
        };

        let left = dispatch(&handler, incarnation, ClientRequest::Leave(request.clone()))?;
        let ServerValue::LeaveCommitted(left_commit) = left else {
            return Err(format!("detached Leave did not commit: {left:?}").into());
        };
        assert_eq!(left_commit.ended_binding_epoch(), None);
        assert_eq!(
            left_commit.prior_terminal_delivery_seq(),
            Some(detach_commit.detached_delivery_seq())
        );
        assert!(left_commit.left_delivery_seq() > detach_commit.detached_delivery_seq());
        committed = left_commit;
    }

    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    let cold_retry = dispatch(&handler, incarnation, ClientRequest::Leave(request))?;
    assert_eq!(cold_retry, ServerValue::LeaveCommitted(committed));
    Ok(())
}

#[test]
fn authorized_leave_refuses_stage_six_capacity_before_commit() -> Result<(), Box<dyn Error>> {
    use crate::server::participant::ParticipantConnectionConversations;

    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(93, 1);
    let mut config = test_participant_config();
    config.max_semantic_conversations_per_connection = 1;
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(store, config)?;

    let target = dispatch(
        &handler,
        incarnation,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: 1_903,
            enrollment_token: EnrollmentToken::new([0xD7; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(target) = target else {
        return Err(format!("target enrollment did not bind: {target:?}").into());
    };
    let request = LeaveRequest {
        conversation_id: 1_903,
        participant_id: target.participant_id(),
        capability_generation: Generation::ONE,
        attach_secret: target.attach_secret(),
        leave_attempt_token: LeaveAttemptToken::new([0xD8; 16]),
    };

    let mut conversations = ParticipantConnectionConversations::default();
    let blocker = dispatch_tracked(
        &handler,
        incarnation,
        &mut conversations,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: 1_904,
            enrollment_token: EnrollmentToken::new([0xD9; 16]),
        }),
    )?;
    assert!(matches!(blocker, ServerValue::EnrollBound(_)));

    let refused = dispatch_tracked(
        &handler,
        incarnation,
        &mut conversations,
        ClientRequest::Leave(request.clone()),
    )?;
    assert!(
        matches!(
            refused,
            ServerValue::ConnectionConversationCapacityExceeded(_)
        ),
        "authorized Leave did not refuse at stage 6: {refused:?}"
    );

    let retry = dispatch(&handler, incarnation, ClientRequest::Leave(request))?;
    assert!(
        matches!(retry, ServerValue::LeaveCommitted(_)),
        "capacity refusal mutated durable Leave state: {retry:?}"
    );
    Ok(())
}
