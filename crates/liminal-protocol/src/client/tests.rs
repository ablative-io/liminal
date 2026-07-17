use super::*;
use crate::wire::{
    AttachAttemptToken, AttachSecret, BindingEpoch, BindingStateView, ClientRequest,
    ConnectionIncarnation, DetachAttemptToken, DetachEnvelope, DetachInProgress, EnrollmentRequest,
    EnrollmentResponse, EnrollmentToken, Generation, LeaveAttemptToken, LeaveCommitted,
    TerminalizedDetachCell,
};

type TestResult<T = ()> = Result<T, &'static str>;

fn generation(value: u64) -> TestResult<Generation> {
    Generation::new(value).ok_or("test generation must be nonzero")
}

fn epoch(generation_value: u64) -> TestResult<BindingEpoch> {
    Ok(BindingEpoch::new(
        ConnectionIncarnation::new(11, 12),
        generation(generation_value)?,
    ))
}

fn detach() -> TestResult<DetachEnvelope> {
    Ok(DetachEnvelope {
        conversation_id: 21,
        participant_id: 22,
        capability_generation: generation(7)?,
        detach_attempt_token: DetachAttemptToken::new([23; 16]),
    })
}

fn attach(conversation_id: u64, participant_id: u64) -> TestResult<crate::wire::AttachBound> {
    crate::wire::AttachBound::ordinary(
        conversation_id,
        AttachAttemptToken::new([24; 16]),
        participant_id,
        generation(7)?,
        AttachSecret::new([25; 32]),
        epoch(8)?,
        0,
        100,
        200,
    )
    .ok_or("test attach must have an exact successor generation")
}

fn record_replay() -> TestResult<ClientParticipantAggregate> {
    let RecordDetachDecision::Recorded(applied) =
        record_detach(ClientParticipantAggregate::new(), detach()?)
    else {
        return Err("fresh aggregate must record detach");
    };
    Ok(applied.into_aggregate())
}

fn start_replay(
    aggregate: ClientParticipantAggregate,
) -> TestResult<(ClientParticipantAggregate, DetachTransportAttempt)> {
    let DetachTransportAttemptDecision::Started { aggregate, attempt } =
        transport_attempt_started(aggregate)
    else {
        return Err("parked detach must start");
    };
    Ok((aggregate, attempt))
}

#[test]
fn operation_barrier_enforces_cardinality_and_exact_correlation() -> TestResult {
    let token = EnrollmentToken::new([1; 16]);
    let request = ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: 2,
        enrollment_token: token,
    });
    let ClientOperationRecordDecision::Pending(pending) =
        record_operation(ClientParticipantAggregate::new(), request.clone())
    else {
        return Err("first write-ahead operation must be pending");
    };
    let (aggregate, operation) = pending.commit().into_parts();
    assert_eq!(operation.request(), &request);

    let refused_request = ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: 3,
        enrollment_token: EnrollmentToken::new([3; 16]),
    });
    let ClientOperationRecordDecision::Refused(refusal) =
        record_operation(aggregate, refused_request.clone())
    else {
        return Err("second write-ahead operation must be refused");
    };
    assert_eq!(
        refusal.reason(),
        ClientOperationRecordRefusalReason::OutstandingOperation
    );
    let (aggregate, returned) = refusal.into_parts();
    assert_eq!(returned, refused_request);
    assert!(aggregate.has_expected_operation());

    let foreign = EnrollmentResponse::connection_conversation_capacity_exceeded(
        crate::wire::EnrollmentEnvelope {
            conversation_id: 9,
            enrollment_token: token,
        },
        10,
    )
    .into_server_value();
    let ClientInboundDecision::Refused(refusal) = decide_inbound(aggregate, foreign) else {
        return Err("foreign response cannot apply");
    };
    assert_eq!(
        refusal.reason(),
        ClientInboundRefusalReason::ForeignResponse
    );
    let (aggregate, returned_foreign) = refusal.into_parts();
    assert!(matches!(
        returned_foreign,
        ServerValue::ConnectionConversationCapacityExceeded(_)
    ));
    assert!(aggregate.has_expected_operation());

    let delayed = EnrollmentResponse::connection_conversation_capacity_exceeded(
        crate::wire::EnrollmentEnvelope {
            conversation_id: 2,
            enrollment_token: EnrollmentToken::new([4; 16]),
        },
        10,
    )
    .into_server_value();
    let ClientInboundDecision::Refused(refusal) = decide_inbound(aggregate, delayed) else {
        return Err("older token cannot apply");
    };
    assert_eq!(
        refusal.reason(),
        ClientInboundRefusalReason::DelayedResponse
    );
    let (aggregate, _) = refusal.into_parts();
    assert!(aggregate.has_expected_operation());

    let exact = EnrollmentResponse::connection_conversation_capacity_exceeded(
        crate::wire::EnrollmentEnvelope {
            conversation_id: 2,
            enrollment_token: token,
        },
        10,
    )
    .into_server_value();
    let ClientInboundDecision::Applied(applied) = decide_inbound(aggregate, exact) else {
        return Err("exact response must apply");
    };
    let (aggregate, _) = applied.into_parts();
    assert!(!aggregate.has_expected_operation());

    let delayed_after_apply = EnrollmentResponse::connection_conversation_capacity_exceeded(
        crate::wire::EnrollmentEnvelope {
            conversation_id: 2,
            enrollment_token: token,
        },
        10,
    )
    .into_server_value();
    let ClientInboundDecision::Refused(refusal) = decide_inbound(aggregate, delayed_after_apply)
    else {
        return Err("response without expectation cannot apply");
    };
    assert_eq!(
        refusal.reason(),
        ClientInboundRefusalReason::DelayedResponse
    );
    Ok(())
}

#[test]
fn abort_and_continuous_ack_release_no_speculative_slot() -> TestResult {
    let request = ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: 5,
        enrollment_token: EnrollmentToken::new([6; 16]),
    });
    let ClientOperationRecordDecision::Pending(pending) =
        record_operation(ClientParticipantAggregate::new(), request.clone())
    else {
        return Err("operation must be pending");
    };
    let (aggregate, returned) = pending.abort();
    assert_eq!(returned, request);
    assert!(!aggregate.has_expected_operation());

    let ack = ClientRequest::ParticipantAck(crate::wire::ParticipantAck {
        conversation_id: 5,
        participant_id: 6,
        capability_generation: Generation::ONE,
        through_seq: 7,
    });
    let ClientOperationRecordDecision::Continuous(continuous) =
        record_operation(aggregate, ack.clone())
    else {
        return Err("continuous ack must bypass write-ahead slot");
    };
    let (aggregate, operation) = continuous.into_parts();
    assert!(!aggregate.has_expected_operation());
    assert_eq!(operation.into_request(), ack);
    Ok(())
}

#[test]
fn replay_start_fate_replay_and_nonmatching_attach_preserve_authority() -> TestResult {
    let aggregate = record_replay()?;
    assert!(matches!(
        aggregate.detach_replay().status(),
        Some(DetachReplayStatus::Parked)
    ));
    let (aggregate, attempt) = start_replay(aggregate)?;
    assert_eq!(attempt.request(), &detach()?);

    let ApplyAttachDecision::Refused(refusal) = apply_attach(aggregate, attach(99, 22)?) else {
        return Err("foreign attach cannot supersede");
    };
    let (aggregate, _) = refusal.into_parts();
    assert!(matches!(
        aggregate.detach_replay().status(),
        Some(DetachReplayStatus::InFlight)
    ));

    let DetachTransportFateDecision::Parked(applied) =
        transport_fate(aggregate, DetachTransportFate::ResponseUnavailable)
    else {
        return Err("typed fate must park in-flight replay");
    };
    let (aggregate, _) = start_replay(applied.into_aggregate())?;
    let ApplyAttachDecision::Superseded(applied) = apply_attach(aggregate, attach(21, 22)?) else {
        return Err("matching attach must supersede");
    };
    assert!(matches!(
        applied.into_aggregate().detach_replay().status(),
        Some(DetachReplayStatus::Superseded)
    ));
    Ok(())
}

#[test]
fn replay_durable_leave_and_all_terminal_payloads_are_distinct() -> TestResult {
    let leave = LeaveCommitted::new(
        21,
        LeaveAttemptToken::new([31; 16]),
        22,
        generation(7)?,
        None,
        Some(1),
        2,
    )
    .ok_or("test leave ordering must be valid")?;
    let ApplyLeaveDecision::Superseded(applied) = apply_leave_durable(record_replay()?, leave)
    else {
        return Err("matching leave must supersede");
    };
    assert!(matches!(
        applied.into_aggregate().detach_replay().status(),
        Some(DetachReplayStatus::LeaveSuperseded)
    ));

    let request = detach()?;
    let committed =
        crate::wire::DetachCommitted::new(21, 22, request.detach_attempt_token, epoch(7)?, 9);
    let ApplyDetachOutcomeDecision::Terminal(applied) = apply_detach_outcome(
        record_replay()?,
        DetachReplayOutcome::DetachCommitted(committed),
    ) else {
        return Err("exact commit must terminalize");
    };
    assert!(matches!(
        applied.into_aggregate().detach_replay().status(),
        Some(DetachReplayStatus::Terminal(
            DetachReplayTerminal::DetachCommitted(_)
        ))
    ));

    let in_progress = DetachInProgress {
        conversation_id: 21,
        participant_id: 22,
        presented_token: request.detach_attempt_token,
        presented_generation: generation(7)?,
        committed_binding_epoch: epoch(7)?,
    };
    let ApplyDetachOutcomeDecision::Terminal(applied) = apply_detach_outcome(
        record_replay()?,
        DetachReplayOutcome::DetachInProgress(in_progress),
    ) else {
        return Err("exact pending cell must terminalize");
    };
    assert!(matches!(
        applied.into_aggregate().detach_replay().status(),
        Some(DetachReplayStatus::Terminal(
            DetachReplayTerminal::DetachInProgress(_)
        ))
    ));

    let cell = TerminalizedDetachCell::for_client_test(
        21,
        22,
        generation(7)?,
        request.detach_attempt_token,
        generation(8)?,
        epoch(7)?,
        BindingStateView::Detached,
    );
    let ApplyDetachOutcomeDecision::Terminal(applied) = apply_detach_outcome(
        record_replay()?,
        DetachReplayOutcome::TerminalizedDetachCell(cell),
    ) else {
        return Err("exact terminalized cell must apply");
    };
    assert!(matches!(
        applied.into_aggregate().detach_replay().status(),
        Some(DetachReplayStatus::Terminal(
            DetachReplayTerminal::TerminalizedDetachCell(_)
        ))
    ));
    Ok(())
}
