use alloc::vec;

use super::*;
use crate::wire::{
    AttachSecret, BindingEpoch, ClientRequest, ConnectionIncarnation, DetachAttemptToken,
    DetachEnvelope, DetachRequest, EnrollBound, EnrollmentRequest, EnrollmentToken, Generation,
    ObserverProgressStatus, ObserverRecoveryAccepted, ObserverRecoveryHandshake, ObserverRefusal,
    ParticipantReferenceEnvelope, RecordAdmission, RecordAdmissionEnvelope, RecordCommitted,
    Retired, ServerValue,
};

type TestResult<T = ()> = Result<T, &'static str>;

fn generation(value: u64) -> TestResult<Generation> {
    Generation::new(value).ok_or("generation must be nonzero")
}

fn epoch(value: u64) -> TestResult<BindingEpoch> {
    Ok(BindingEpoch::new(
        ConnectionIncarnation::new(51, 52),
        generation(value)?,
    ))
}

fn bound(value: u64) -> TestResult<ClientParticipantAggregate> {
    let mut aggregate = ClientParticipantAggregate::new();
    aggregate.binding = ClientBindingState::Bound {
        conversation_id: 41,
        participant_id: 42,
        generation: generation(value)?,
        attach_secret: AttachSecret::new([43; 32]),
        binding_epoch: epoch(value)?,
    };
    Ok(aggregate)
}

fn detach(value: u64, token: u8) -> TestResult<ClientRequest> {
    Ok(ClientRequest::Detach(DetachRequest {
        conversation_id: 41,
        participant_id: 42,
        capability_generation: generation(value)?,
        detach_attempt_token: DetachAttemptToken::new([token; 16]),
    }))
}

fn committed_operation(
    aggregate: ClientParticipantAggregate,
    request: ClientRequest,
) -> TestResult<ClientOperationCommit> {
    let ClientOperationRecordDecision::Pending(pending) = record_operation(aggregate, request)
    else {
        return Err("operation must enter pending barrier");
    };
    Ok(pending.commit())
}

#[test]
fn b1_resume_preserves_issued_permit_testimony() -> TestResult {
    let ReconnectPermitDecision::Permitted { aggregate, .. } = record_explicit_reconnect(
        ClientParticipantAggregate::new(),
        ExplicitReconnectAction::ReconnectNow,
    ) else {
        return Err("fresh action must issue permit");
    };
    let restored = aggregate
        .resume_record()
        .map_err(|_| "issued permit must encode")?
        .restore()
        .map_err(|_| "issued permit must restore")?;
    let RecoveredReconnectPermitDecision::NotAvailable { aggregate, .. } =
        recover_reconnect_permit(restored)
    else {
        return Err("issued testimony must not re-mint permit");
    };
    assert!(matches!(
        record_issued_permit_fate(aggregate, IssuedReconnectPermitFate::ProcessLost),
        ReconnectRestoreExitDecision::Recorded { .. }
    ));
    Ok(())
}

#[test]
fn b2_restored_attempt_has_typed_process_loss_exit() -> TestResult {
    let ReconnectPermitDecision::Permitted {
        aggregate, permit, ..
    } = record_explicit_reconnect(
        ClientParticipantAggregate::new(),
        ExplicitReconnectAction::ReconnectNow,
    )
    else {
        return Err("fresh action must issue permit");
    };
    let ReconnectAttemptDecision::Started { aggregate, .. } = redeem_attempt(aggregate, permit)
    else {
        return Err("permit must start attempt");
    };
    let restored = aggregate
        .resume_record()
        .map_err(|_| "attempt must encode")?
        .restore()
        .map_err(|_| "attempt must restore")?;
    let ReconnectRestoreExitDecision::Recorded { aggregate, .. } =
        record_interrupted_attempt_fate(restored, InterruptedReconnectAttemptFate::ProcessLost)
    else {
        return Err("restored attempt must have typed exit");
    };
    assert!(matches!(
        record_transport_fate(aggregate, EstablishedConnectionTransportFate::Lost),
        ReconnectPermitDecision::Permitted { .. }
    ));
    Ok(())
}

#[test]
fn b3_commit_seal_record_restores_before_authority_release() -> TestResult {
    let request = ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: 41,
        enrollment_token: EnrollmentToken::new([44; 16]),
    });
    let ClientOperationRecordDecision::Pending(pending) =
        record_operation(ClientParticipantAggregate::new(), request)
    else {
        return Err("enrollment must become pending");
    };
    let commit = pending.commit();
    let restored = commit
        .resume_record()
        .map_err(|_| "committed record must encode")?
        .restore()
        .map_err(|_| "committed record must restore")?;
    assert!(matches!(
        recover_expected_operation(restored),
        RecoveredExpectedOperationDecision::Recovered { .. }
    ));
    Ok(())
}

#[test]
fn b4_cold_restored_expected_operation_is_released_once() -> TestResult {
    let request = ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: 41,
        enrollment_token: EnrollmentToken::new([45; 16]),
    });
    let restored = committed_operation(ClientParticipantAggregate::new(), request.clone())?
        .resume_record()
        .map_err(|_| "commit must encode")?
        .restore()
        .map_err(|_| "commit must restore")?;
    let RecoveredExpectedOperationDecision::Recovered {
        aggregate,
        operation,
    } = recover_expected_operation(restored)
    else {
        return Err("cold operation must recover");
    };
    assert_eq!(operation.request(), &request);
    assert!(matches!(
        recover_expected_operation(aggregate),
        RecoveredExpectedOperationDecision::NotAvailable {
            already_issued: true,
            ..
        }
    ));
    Ok(())
}

#[test]
fn b4_restored_issued_operation_has_typed_process_loss_exit() -> TestResult {
    let request = ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: 41,
        enrollment_token: EnrollmentToken::new([46; 16]),
    });
    let commit = committed_operation(ClientParticipantAggregate::new(), request)?;
    let (aggregate, operation) = commit.into_parts();
    let (_request, _correlation) = operation.into_request();
    let restored = aggregate
        .resume_record()
        .map_err(|_| "issued aggregate must encode")?
        .restore()
        .map_err(|_| "issued aggregate must restore")?;
    let IssuedExpectedOperationFateDecision::Recorded { aggregate, .. } =
        record_issued_expected_operation_fate(restored, IssuedExpectedOperationFate::ProcessLost)
    else {
        return Err("restored issued operation must have process-loss exit");
    };
    assert!(!aggregate.has_expected_operation());
    assert!(matches!(
        record_issued_expected_operation_fate(aggregate, IssuedExpectedOperationFate::ProcessLost,),
        IssuedExpectedOperationFateDecision::Refused { .. }
    ));
    Ok(())
}

#[test]
fn b5_detach_has_exactly_one_initial_send_authority() -> TestResult {
    let commit = committed_operation(bound(7)?, detach(7, 46)?)?;
    let cold = commit
        .resume_record()
        .map_err(|_| "detach commit must encode")?
        .restore()
        .map_err(|_| "detach commit must restore")?;
    let RecoveredExpectedOperationDecision::Recovered { aggregate, .. } =
        recover_expected_operation(cold)
    else {
        return Err("cold detach must recover one send authority");
    };
    assert!(matches!(
        transport_attempt_started(aggregate),
        DetachTransportAttemptDecision::Refused(_)
    ));

    let commit = committed_operation(bound(7)?, detach(7, 47)?)?;
    let (aggregate, _) = commit.into_parts();
    assert!(matches!(
        transport_attempt_started(aggregate),
        DetachTransportAttemptDecision::Refused(_)
    ));

    let inverse = committed_operation(bound(7)?, detach(7, 48)?)?
        .resume_record()
        .map_err(|_| "inverse detach commit must encode")?
        .restore()
        .map_err(|_| "inverse detach commit must restore")?;
    let DetachTransportAttemptDecision::Started { aggregate, .. } =
        transport_attempt_started(inverse)
    else {
        return Err("inverse order must release exactly one replay send");
    };
    assert!(matches!(
        recover_expected_operation(aggregate),
        RecoveredExpectedOperationDecision::NotAvailable {
            already_issued: true,
            ..
        }
    ));
    Ok(())
}

#[test]
fn m6_binding_state_gates_outbound_and_inbound() -> TestResult {
    let enrollment = ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: 41,
        enrollment_token: EnrollmentToken::new([48; 16]),
    });
    let ClientOperationRecordDecision::Refused(refusal) =
        record_operation(bound(7)?, enrollment.clone())
    else {
        return Err("bound enrollment must be refused");
    };
    assert_eq!(
        refusal.reason(),
        ClientOperationRecordRefusalReason::BindingMismatch
    );

    let mut aggregate = bound(7)?;
    aggregate.expected = Some(ExpectedOperationState {
        request: enrollment,
        issued: true,
        authorization: 1,
    });
    aggregate.next_operation_authorization = 1;
    let response = EnrollBound::new(
        41,
        EnrollmentToken::new([48; 16]),
        42,
        AttachSecret::new([49; 32]),
        epoch(1)?,
        100,
        200,
    )
    .ok_or("enrollment fixture must use generation one")?;
    assert!(matches!(
        decide_inbound(aggregate, ServerValue::EnrollBound(response)),
        ClientInboundDecision::Refused(_)
    ));

    let mut left = bound(7)?;
    left.binding = ClientBindingState::Left {
        conversation_id: 41,
        participant_id: 42,
        generation: generation(8)?,
    };
    let ClientOperationRecordDecision::Refused(refusal) = record_operation(left, detach(8, 50)?)
    else {
        return Err("post-left request must be refused");
    };
    assert_eq!(
        refusal.reason(),
        ClientOperationRecordRefusalReason::AlreadyDead
    );
    Ok(())
}

#[test]
fn m7_actual_older_record_response_is_always_ambiguous() -> TestResult {
    let expected = ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: 41,
        participant_id: 42,
        capability_generation: generation(7)?,
        payload: vec![2],
    });
    let (aggregate, operation) = committed_operation(bound(7)?, expected)?.into_parts();
    let (_, correlation) = operation.into_request();
    let older_response = ServerValue::RecordCommitted(RecordCommitted::new(
        RecordAdmissionEnvelope {
            conversation_id: 41,
            participant_id: 42,
            capability_generation: generation(7)?,
        },
        8,
    ));
    let ClientInboundDecision::Refused(refusal) = decide_inbound(aggregate, older_response.clone())
    else {
        return Err("older body-omitting response must never apply");
    };
    assert_eq!(
        refusal.reason(),
        ClientInboundRefusalReason::AmbiguousResponse
    );
    let (aggregate, _) = refusal.into_parts();
    let ClientCorrelatedInboundDecision::Refused(refusal) =
        decide_correlated_inbound(aggregate, older_response, correlation)
    else {
        return Err("caller-paired correlation must not prove response provenance");
    };
    assert_eq!(
        refusal.reason(),
        ClientInboundRefusalReason::AmbiguousResponse
    );
    Ok(())
}

#[test]
fn m7_actual_older_observer_response_cannot_apply() -> TestResult {
    let expected = ClientRequest::ObserverRecovery(ObserverRecoveryHandshake {
        observer_refusals: vec![ObserverRefusal {
            conversation_id: 71,
            refused_epoch: 72,
        }],
    });
    let (aggregate, _) =
        committed_operation(ClientParticipantAggregate::new(), expected)?.into_parts();
    let older = ServerValue::ObserverRecoveryAccepted(ObserverRecoveryAccepted {
        statuses: vec![ObserverProgressStatus {
            conversation_id: 70,
            refused_epoch: 71,
            current_observer_progress: 73,
            armed: true,
            progressed: false,
        }],
    });
    let ClientInboundDecision::Refused(refusal) = decide_inbound(aggregate, older) else {
        return Err("older echoed observer list must not apply");
    };
    assert_eq!(
        refusal.reason(),
        ClientInboundRefusalReason::DelayedResponse
    );
    let (aggregate, _) = refusal.into_parts();
    let matching = ServerValue::ObserverRecoveryAccepted(ObserverRecoveryAccepted {
        statuses: vec![ObserverProgressStatus {
            conversation_id: 71,
            refused_epoch: 72,
            current_observer_progress: 73,
            armed: true,
            progressed: false,
        }],
    });
    assert!(matches!(
        decide_inbound(aggregate, matching),
        ClientInboundDecision::Applied(_)
    ));
    Ok(())
}

#[test]
fn m8_detach_and_resume_preserve_attach_secret() -> TestResult {
    let secret = AttachSecret::new([61; 32]);
    let mut aggregate = bound(7)?;
    if let ClientBindingState::Bound { attach_secret, .. } = &mut aggregate.binding {
        *attach_secret = secret;
    }
    let request = detach(7, 62)?;
    let (aggregate, _) = committed_operation(aggregate, request)?.into_parts();
    let committed =
        crate::wire::DetachCommitted::new(41, 42, DetachAttemptToken::new([62; 16]), epoch(7)?, 0);
    let ClientInboundDecision::Applied(applied) =
        decide_inbound(aggregate, ServerValue::DetachCommitted(committed))
    else {
        return Err("exact detach commit must apply");
    };
    let (aggregate, _) = applied.into_parts();
    let restored = aggregate
        .resume_record()
        .map_err(|_| "detached binding must encode")?
        .restore()
        .map_err(|_| "detached binding must restore")?;
    assert!(matches!(
        restored.binding,
        ClientBindingState::Detached { attach_secret, .. } if attach_secret == secret
    ));
    Ok(())
}

#[test]
fn m9_terminal_and_superseded_replay_yield_to_new_generation_detach() -> TestResult {
    for status in [
        DetachReplayStatus::Superseded,
        DetachReplayStatus::Terminal(DetachReplayTerminal::DetachCommitted(
            crate::wire::DetachCommitted::new(
                41,
                42,
                DetachAttemptToken::new([63; 16]),
                epoch(7)?,
                0,
            ),
        )),
    ] {
        let mut aggregate = bound(8)?;
        aggregate.detach_replay.state = replay::DetachReplayState::Recorded {
            request: DetachEnvelope {
                conversation_id: 41,
                participant_id: 42,
                capability_generation: generation(7)?,
                detach_attempt_token: DetachAttemptToken::new([63; 16]),
            },
            status,
        };
        assert!(matches!(
            record_operation(aggregate, detach(8, 64)?),
            ClientOperationRecordDecision::Pending(_)
        ));
    }
    Ok(())
}

#[test]
fn m10_retired_terminalizes_binding_and_future_operations() -> TestResult {
    let request = detach(7, 65)?;
    let (aggregate, _) = committed_operation(bound(7)?, request)?.into_parts();
    let retired = Retired::Participant {
        request: ParticipantReferenceEnvelope::Detach(DetachEnvelope {
            conversation_id: 41,
            participant_id: 42,
            capability_generation: generation(7)?,
            detach_attempt_token: DetachAttemptToken::new([65; 16]),
        }),
        retired_generation: generation(8)?,
    };
    let ClientInboundDecision::Applied(applied) =
        decide_inbound(aggregate, ServerValue::Retired(retired))
    else {
        return Err("correlated retirement must apply");
    };
    let (aggregate, _) = applied.into_parts();
    assert_eq!(aggregate.binding_status(), ClientBindingStatus::Left);
    let ClientOperationRecordDecision::Refused(refusal) =
        record_operation(aggregate, detach(8, 66)?)
    else {
        return Err("retired participant must refuse future operation");
    };
    assert_eq!(
        refusal.reason(),
        ClientOperationRecordRefusalReason::AlreadyDead
    );
    Ok(())
}
