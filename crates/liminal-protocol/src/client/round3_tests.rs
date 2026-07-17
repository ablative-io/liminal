use super::*;
use crate::wire::{
    AttachAttemptToken, AttachSecret, BindingEpoch, ClientRequest, ConnectionIncarnation,
    CredentialAttachRequest, DetachAttemptToken, DetachEnvelope, DetachRequest, Generation,
    LeaveAttemptToken, LeaveRequest,
};

type TestResult<T = ()> = Result<T, &'static str>;

fn generation(value: u64) -> TestResult<Generation> {
    Generation::new(value).ok_or("generation must be nonzero")
}

fn epoch(value: u64) -> TestResult<BindingEpoch> {
    Ok(BindingEpoch::new(
        ConnectionIncarnation::new(81, 82),
        generation(value)?,
    ))
}

fn bound() -> TestResult<ClientParticipantAggregate> {
    let mut aggregate = ClientParticipantAggregate::new();
    aggregate.binding = ClientBindingState::Bound {
        conversation_id: 91,
        participant_id: 92,
        generation: generation(7)?,
        attach_secret: AttachSecret::new([93; 32]),
        binding_epoch: epoch(7)?,
    };
    Ok(aggregate)
}

fn detach(token: u8) -> TestResult<ClientRequest> {
    Ok(ClientRequest::Detach(DetachRequest {
        conversation_id: 91,
        participant_id: 92,
        capability_generation: generation(7)?,
        detach_attempt_token: DetachAttemptToken::new([token; 16]),
    }))
}

#[test]
fn active_replay_without_matching_expected_detach_refuses_restore() -> TestResult {
    let mut aggregate = bound()?;
    aggregate.detach_replay.state = replay::DetachReplayState::Recorded {
        request: DetachEnvelope {
            conversation_id: 91,
            participant_id: 92,
            capability_generation: generation(7)?,
            detach_attempt_token: DetachAttemptToken::new([94; 16]),
        },
        status: DetachReplayStatus::Parked,
    };
    let record = aggregate
        .resume_record()
        .map_err(|_| "uncoupled fixture must encode inertly")?;
    assert_eq!(
        record.restore(),
        Err(ClientResumeRestoreError::ActiveReplayExpectedDetachMismatch)
    );
    Ok(())
}

#[test]
fn refused_detach_admission_preserves_authorization_counter() -> TestResult {
    let mut aggregate = bound()?;
    aggregate.next_operation_authorization = 17;
    aggregate.detach_replay.state = replay::DetachReplayState::Recorded {
        request: DetachEnvelope {
            conversation_id: 91,
            participant_id: 92,
            capability_generation: generation(7)?,
            detach_attempt_token: DetachAttemptToken::new([95; 16]),
        },
        status: DetachReplayStatus::Parked,
    };
    let ClientOperationRecordDecision::Refused(refusal) = record_operation(aggregate, detach(96)?)
    else {
        return Err("different active detach must refuse");
    };
    assert_eq!(
        refusal.reason(),
        ClientOperationRecordRefusalReason::DetachReplayOutstanding
    );
    let (aggregate, _) = refusal.into_parts();
    assert_eq!(aggregate.next_operation_authorization, 17);
    Ok(())
}

#[test]
fn same_identity_wrong_secret_attach_and_leave_refuse() -> TestResult {
    let attach = ClientRequest::CredentialAttach(CredentialAttachRequest {
        conversation_id: 91,
        participant_id: 92,
        capability_generation: generation(7)?,
        attach_secret: AttachSecret::new([97; 32]),
        attach_attempt_token: AttachAttemptToken::new([98; 16]),
        accept_marker_delivery_seq: None,
    });
    let ClientOperationRecordDecision::Refused(refusal) = record_operation(bound()?, attach) else {
        return Err("wrong-secret attach must refuse");
    };
    assert_eq!(
        refusal.reason(),
        ClientOperationRecordRefusalReason::BindingMismatch
    );

    let leave = ClientRequest::Leave(LeaveRequest {
        conversation_id: 91,
        participant_id: 92,
        capability_generation: generation(7)?,
        attach_secret: AttachSecret::new([99; 32]),
        leave_attempt_token: LeaveAttemptToken::new([100; 16]),
    });
    let ClientOperationRecordDecision::Refused(refusal) = record_operation(bound()?, leave) else {
        return Err("wrong-secret leave must refuse");
    };
    assert_eq!(
        refusal.reason(),
        ClientOperationRecordRefusalReason::BindingMismatch
    );
    Ok(())
}
