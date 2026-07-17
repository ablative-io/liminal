//! Fail-first evidence and closure tests for the `LP-CLIENT-GOAL` r2
//! lost-authority testimony amendment (2026-07-18).
//!
//! The first four tests were authored RED against round-4 tip `40244d6` and
//! their pre-fix failures are recorded in the commit that introduced them:
//!
//! - [`same_envelope_re_record_over_terminal_replay_refuses`] — round-4 door
//!   (b): the retained-envelope admission arm revived expected-detach authority
//!   over an inactive replay.
//! - [`tokenless_abandonment_survives_encode_without_take`] — the typed
//!   `TokenlessAfterCrash` abandonment was process-local and silently dropped
//!   by encode-without-take.
//! - [`restored_issued_permit_rejects_prior_process_permit`] — a cold-restored
//!   issued permit could still be redeemed by the dead process's permit value
//!   while the loss went unrecorded.
//! - [`restored_issued_operation_rejects_prior_process_correlation`] — a
//!   cold-restored issued operation could still be resolved by the dead
//!   process's correlation while the loss went unrecorded.

use super::*;
use crate::wire::{
    AttachSecret, BindingEpoch, ClientRequest, ConnectionIncarnation, DetachAttemptToken,
    DetachRequest, EnrollmentRequest, EnrollmentResponse, EnrollmentToken, Generation,
    RecordAdmission,
};
use alloc::vec;

type TestResult<T = ()> = Result<T, &'static str>;

fn generation(value: u64) -> TestResult<Generation> {
    Generation::new(value).ok_or("generation must be nonzero")
}

fn epoch(value: u64) -> TestResult<BindingEpoch> {
    Ok(BindingEpoch::new(
        ConnectionIncarnation::new(161, 162),
        generation(value)?,
    ))
}

fn bound() -> TestResult<ClientParticipantAggregate> {
    let mut aggregate = ClientParticipantAggregate::new();
    aggregate.binding = ClientBindingState::Bound {
        conversation_id: 161,
        participant_id: 162,
        generation: generation(7)?,
        attach_secret: AttachSecret::new([163; 32]),
        binding_epoch: epoch(7)?,
    };
    Ok(aggregate)
}

fn detach_request() -> TestResult<ClientRequest> {
    Ok(ClientRequest::Detach(DetachRequest {
        conversation_id: 161,
        participant_id: 162,
        capability_generation: generation(7)?,
        detach_attempt_token: DetachAttemptToken::new([164; 16]),
    }))
}

#[test]
fn same_envelope_re_record_over_terminal_replay_refuses() -> TestResult {
    let ClientOperationRecordDecision::Pending(pending) =
        record_operation(bound()?, detach_request()?)
    else {
        return Err("bound detach must enter the durability barrier");
    };
    let (aggregate, operation) = pending.commit().into_parts();
    let (_, correlation) = operation.into_request();
    let committed = crate::wire::DetachCommitted::new(
        161,
        162,
        DetachAttemptToken::new([164; 16]),
        epoch(7)?,
        0,
    );
    let ApplyDetachOutcomeDecision::Terminal(applied) = apply_detach_outcome(
        aggregate,
        DetachReplayOutcome::DetachCommitted(committed),
        correlation,
    ) else {
        return Err("exact detach outcome must terminalize replay");
    };
    let ClientOperationRecordDecision::Refused(refusal) =
        record_operation(applied.into_aggregate(), detach_request()?)
    else {
        return Err("same-envelope re-record over terminal replay must refuse");
    };
    let (aggregate, _) = refusal.into_parts();
    assert!(!aggregate.has_expected_operation());
    Ok(())
}

#[test]
fn tokenless_abandonment_survives_encode_without_take() -> TestResult {
    let mut aggregate = bound()?;
    let request = ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: 161,
        participant_id: 162,
        capability_generation: generation(7)?,
        payload: vec![9],
    });
    aggregate.expected = Some(ExpectedOperationState {
        request: request.clone(),
        issued: true,
        authorization: 1,
    });
    aggregate.next_operation_authorization = 1;
    let restored = aggregate
        .resume_record()
        .map_err(|_| "tokenless fixture must encode")?
        .restore()
        .map_err(|_| "tokenless fixture must restore")?;
    let mut second = restored
        .resume_record()
        .map_err(|_| "pending abandonment must encode")?
        .restore()
        .map_err(|_| "re-encoded abandonment must restore")?;
    let abandonment = second
        .take_restored_operation_abandonment()
        .ok_or("encode-without-take must not lose the abandonment")?;
    assert_eq!(abandonment.into_request(), request);
    Ok(())
}

#[test]
fn restored_issued_permit_rejects_prior_process_permit() -> TestResult {
    let ReconnectPermitDecision::Permitted {
        aggregate, permit, ..
    } = record_explicit_reconnect(
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
    assert!(matches!(
        redeem_attempt(restored, permit),
        ReconnectAttemptDecision::Refused { .. }
    ));
    Ok(())
}

#[test]
fn restored_issued_operation_rejects_prior_process_correlation() -> TestResult {
    let token = EnrollmentToken::new([21; 16]);
    let request = ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: 2,
        enrollment_token: token,
    });
    let ClientOperationRecordDecision::Pending(pending) =
        record_operation(ClientParticipantAggregate::new(), request)
    else {
        return Err("enrollment must enter the durability barrier");
    };
    let (aggregate, operation) = pending.commit().into_parts();
    let (_, correlation) = operation.into_request();
    let restored = aggregate
        .resume_record()
        .map_err(|_| "issued operation must encode")?
        .restore()
        .map_err(|_| "issued operation must restore")?;
    let exact = EnrollmentResponse::connection_conversation_capacity_exceeded(
        crate::wire::EnrollmentEnvelope {
            conversation_id: 2,
            enrollment_token: token,
        },
        10,
    )
    .into_server_value();
    assert!(matches!(
        decide_correlated_inbound(restored, exact, correlation),
        ClientCorrelatedInboundDecision::Refused(_)
    ));
    Ok(())
}
