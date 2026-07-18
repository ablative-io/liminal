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
};
use alloc::vec;

type TestResult<T = ()> = Result<T, &'static str>;

fn generation(value: u64) -> TestResult<Generation> {
    Generation::new(value).ok_or("generation must be nonzero")
}

fn tokenless_request() -> ClientRequest {
    ClientRequest::ObserverRecovery(crate::wire::ObserverRecoveryHandshake {
        observer_refusals: vec![],
    })
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
    let request = tokenless_request();
    aggregate.expected = Some(ExpectedOperationState {
        request: request.clone(),
        issued: true,
        authorization: 1,
        lost: None,
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
    )
    else {
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

fn detach_envelope() -> TestResult<crate::wire::DetachEnvelope> {
    Ok(crate::wire::DetachEnvelope {
        conversation_id: 161,
        participant_id: 162,
        capability_generation: generation(7)?,
        detach_attempt_token: DetachAttemptToken::new([164; 16]),
    })
}

fn round_trip(aggregate: &ClientParticipantAggregate) -> TestResult<ClientParticipantAggregate> {
    let bytes = aggregate
        .resume_record()
        .map_err(|_| "aggregate must encode")?
        .encode_canonical();
    ClientResumeRecord::decode_canonical(&bytes)
        .map_err(|_| "canonical record must decode")?
        .restore()
        .map_err(|_| "canonical record must restore")
}

fn restored_issued(request: ClientRequest) -> TestResult<ClientParticipantAggregate> {
    let start = if matches!(request, ClientRequest::Enrollment(_)) {
        ClientParticipantAggregate::new()
    } else {
        bound()?
    };
    let ClientOperationRecordDecision::Pending(pending) = record_operation(start, request) else {
        return Err("token-bearing request must enter the durability barrier");
    };
    let (aggregate, operation) = pending.commit().into_parts();
    let (_, _correlation) = operation.into_request();
    round_trip(&aggregate)
}

#[test]
fn testimony_round_trips_distinctly_for_every_kind() -> TestResult {
    let restored = restored_issued(detach_request()?)?;
    assert_eq!(
        restored
            .lost_operation_testimony()
            .map(LostAuthorityTestimony::kind),
        Some(LostAuthorityKind::DetachTransportAttempt)
    );
    let again = round_trip(&restored)?;
    assert_eq!(
        again
            .lost_operation_testimony()
            .map(LostAuthorityTestimony::kind),
        Some(LostAuthorityKind::DetachTransportAttempt),
        "encode-without-take must carry the detach testimony exactly once"
    );

    let attach = ClientRequest::CredentialAttach(crate::wire::CredentialAttachRequest {
        conversation_id: 161,
        participant_id: 162,
        capability_generation: generation(7)?,
        attach_secret: AttachSecret::new([163; 32]),
        attach_attempt_token: crate::wire::AttachAttemptToken::new([165; 16]),
        accept_marker_delivery_seq: None,
    });
    let restored = restored_issued(attach)?;
    assert_eq!(
        restored
            .lost_operation_testimony()
            .map(LostAuthorityTestimony::kind),
        Some(LostAuthorityKind::IssuedOperationCorrelation)
    );
    let again = round_trip(&restored)?;
    assert_eq!(
        again
            .lost_operation_testimony()
            .map(LostAuthorityTestimony::kind),
        Some(LostAuthorityKind::IssuedOperationCorrelation),
        "encode-without-take must carry the correlation testimony exactly once"
    );

    let ReconnectPermitDecision::Permitted { aggregate, .. } = record_explicit_reconnect(
        ClientParticipantAggregate::new(),
        ExplicitReconnectAction::ReconnectNow,
    ) else {
        return Err("fresh action must issue permit");
    };
    let restored = round_trip(&aggregate)?;
    assert_eq!(
        restored
            .lost_reconnect_testimony()
            .map(LostAuthorityTestimony::kind),
        Some(LostAuthorityKind::ReconnectPermit)
    );
    let again = round_trip(&restored)?;
    assert_eq!(
        again
            .lost_reconnect_testimony()
            .map(LostAuthorityTestimony::kind),
        Some(LostAuthorityKind::ReconnectPermit)
    );

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
    let restored = round_trip(&aggregate)?;
    assert_eq!(
        restored
            .lost_reconnect_testimony()
            .map(LostAuthorityTestimony::kind),
        Some(LostAuthorityKind::ReconnectAttempt)
    );
    let again = round_trip(&restored)?;
    assert_eq!(
        again
            .lost_reconnect_testimony()
            .map(LostAuthorityTestimony::kind),
        Some(LostAuthorityKind::ReconnectAttempt)
    );
    Ok(())
}

#[test]
fn detach_loss_resolves_to_parked_and_reissues_exactly_once() -> TestResult {
    let restored = restored_issued(detach_request()?)?;
    let LostOperationAuthorityDecision::DetachParked {
        aggregate,
        request,
        testimony,
    } = resolve_lost_operation_authority(restored)
    else {
        return Err("lost detach attempt must park through its testimony");
    };
    assert_eq!(testimony.kind(), LostAuthorityKind::DetachTransportAttempt);
    assert_eq!(request, detach_request()?);
    assert!(matches!(
        aggregate.detach_replay().status(),
        Some(DetachReplayStatus::Parked)
    ));
    let LostOperationAuthorityDecision::Refused {
        aggregate,
        reason: LostAuthorityResolutionRefusalReason::NoPendingTestimony,
    } = resolve_lost_operation_authority(aggregate)
    else {
        return Err("second resolution must refuse without a pending testimony");
    };
    let DetachTransportAttemptDecision::Started { aggregate, .. } =
        transport_attempt_started(aggregate)
    else {
        return Err("parked exact detach must release one replacement send");
    };
    assert!(matches!(
        transport_attempt_started(aggregate),
        DetachTransportAttemptDecision::Refused(_)
    ));
    Ok(())
}

#[test]
fn restore_refuses_testimony_and_abandonment_coupling_mismatches() -> TestResult {
    let mut unissued = bound()?;
    unissued.expected = Some(ExpectedOperationState {
        request: ClientRequest::CredentialAttach(crate::wire::CredentialAttachRequest {
            conversation_id: 161,
            participant_id: 162,
            capability_generation: generation(7)?,
            attach_secret: AttachSecret::new([163; 32]),
            attach_attempt_token: crate::wire::AttachAttemptToken::new([166; 16]),
            accept_marker_delivery_seq: None,
        }),
        issued: false,
        authorization: 1,
        lost: Some(LostAuthorityTestimony::mint(
            LostAuthorityKind::IssuedOperationCorrelation,
        )),
    });
    unissued.next_operation_authorization = 1;
    assert_eq!(
        unissued
            .resume_record()
            .map_err(|_| "mismatch fixture must encode")?
            .restore(),
        Err(ClientResumeRestoreError::LostAuthorityTestimonyMismatch)
    );

    let mut wrong_kind = bound()?;
    wrong_kind.expected = Some(ExpectedOperationState {
        request: detach_request()?,
        issued: true,
        authorization: 1,
        lost: Some(LostAuthorityTestimony::mint(
            LostAuthorityKind::IssuedOperationCorrelation,
        )),
    });
    wrong_kind.next_operation_authorization = 1;
    wrong_kind.detach_replay.state = super::replay::DetachReplayState::Recorded {
        request: detach_envelope()?,
        status: DetachReplayStatus::InFlight,
    };
    assert_eq!(
        wrong_kind
            .resume_record()
            .map_err(|_| "wrong-kind fixture must encode")?
            .restore(),
        Err(ClientResumeRestoreError::LostAuthorityTestimonyMismatch)
    );

    let mut parked = ClientParticipantAggregate::new();
    parked.reconnect.lost = Some(LostAuthorityTestimony::mint(
        LostAuthorityKind::ReconnectPermit,
    ));
    assert_eq!(
        parked
            .resume_record()
            .map_err(|_| "parked fixture must encode")?
            .restore(),
        Err(ClientResumeRestoreError::LostAuthorityTestimonyMismatch)
    );

    let ReconnectPermitDecision::Permitted { mut aggregate, .. } = record_explicit_reconnect(
        ClientParticipantAggregate::new(),
        ExplicitReconnectAction::ReconnectNow,
    ) else {
        return Err("fresh action must issue permit");
    };
    aggregate.reconnect.lost = Some(LostAuthorityTestimony::mint(
        LostAuthorityKind::ReconnectAttempt,
    ));
    assert_eq!(
        aggregate
            .resume_record()
            .map_err(|_| "wrong reconnect kind must encode")?
            .restore(),
        Err(ClientResumeRestoreError::LostAuthorityTestimonyMismatch)
    );

    Ok(())
}

#[test]
fn restore_refuses_abandonment_beside_tokenless_expected() -> TestResult {
    let mut conflicted = bound()?;
    conflicted.restored_abandonment = Some(RestoredExpectedOperationAbandonment {
        request: tokenless_request(),
        reason: RestoredExpectedOperationAbandonmentReason::TokenlessAfterCrash,
        was_issued: true,
    });
    conflicted.expected = Some(ExpectedOperationState {
        request: tokenless_request(),
        issued: false,
        authorization: 1,
        lost: None,
    });
    conflicted.next_operation_authorization = 1;
    assert_eq!(
        conflicted
            .resume_record()
            .map_err(|_| "conflict fixture must encode")?
            .restore(),
        Err(ClientResumeRestoreError::PendingAbandonmentConflict)
    );
    Ok(())
}

#[test]
fn pending_operation_testimony_gates_every_correlation_path() -> TestResult {
    let restored = restored_issued(ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: 161,
        enrollment_token: EnrollmentToken::new([167; 16]),
    }))?;
    let exact = EnrollmentResponse::connection_conversation_capacity_exceeded(
        crate::wire::EnrollmentEnvelope {
            conversation_id: 161,
            enrollment_token: EnrollmentToken::new([167; 16]),
        },
        10,
    )
    .into_server_value();
    let correlation = ClientResponseCorrelation { authorization: 1 };
    let ClientCorrelatedInboundDecision::Refused(refusal) =
        decide_correlated_inbound(restored, exact, correlation)
    else {
        return Err("pending testimony must refuse correlated inbound");
    };
    assert_eq!(
        refusal.reason(),
        ClientInboundRefusalReason::LostAuthorityPending
    );
    let (aggregate, _, correlation) = refusal.into_parts();
    let ExpectedOperationFateDecision::Refused { reason, .. } = record_expected_operation_fate(
        aggregate,
        correlation,
        ExpectedOperationTransportFate::ResponseUnavailable,
    ) else {
        return Err("pending testimony must refuse transport fate");
    };
    assert_eq!(
        reason,
        ExpectedOperationFateRefusalReason::LostAuthorityPending
    );

    let restored = restored_issued(detach_request()?)?;
    let correlation = ClientResponseCorrelation { authorization: 1 };
    let DetachTransportFateDecision::Refused(refusal) = transport_fate(
        restored,
        correlation,
        DetachTransportFate::ResponseUnavailable,
    ) else {
        return Err("pending testimony must refuse detach transport fate");
    };
    assert_eq!(
        refusal.reason(),
        DetachReplayRefusalReason::LostAuthorityPending
    );
    let (aggregate, (correlation, _)) = refusal.into_parts();
    let committed = crate::wire::DetachCommitted::new(
        161,
        162,
        DetachAttemptToken::new([164; 16]),
        epoch(7)?,
        0,
    );
    let ApplyDetachOutcomeDecision::Refused(refusal) = apply_detach_outcome(
        aggregate,
        DetachReplayOutcome::DetachCommitted(committed),
        correlation,
    ) else {
        return Err("pending testimony must refuse detach outcome");
    };
    assert_eq!(
        refusal.reason(),
        DetachReplayRefusalReason::LostAuthorityPending
    );
    Ok(())
}

#[test]
fn pending_reconnect_testimony_gates_redemption_and_attempt_fate() -> TestResult {
    let ReconnectPermitDecision::Permitted {
        aggregate, permit, ..
    } = record_explicit_reconnect(
        ClientParticipantAggregate::new(),
        ExplicitReconnectAction::ReconnectNow,
    )
    else {
        return Err("fresh action must issue permit");
    };
    let restored = round_trip(&aggregate)?;
    let ReconnectAttemptDecision::Refused { reason, .. } = redeem_attempt(restored, permit) else {
        return Err("pending testimony must refuse redemption");
    };
    assert_eq!(reason, ReconnectAttemptRefusalReason::LostAuthorityPending);

    let ReconnectPermitDecision::Permitted {
        aggregate, permit, ..
    } = record_explicit_reconnect(
        ClientParticipantAggregate::new(),
        ExplicitReconnectAction::ReconnectNow,
    )
    else {
        return Err("fresh action must issue permit");
    };
    let ReconnectAttemptDecision::Started { aggregate, attempt } =
        redeem_attempt(aggregate, permit)
    else {
        return Err("permit must start attempt");
    };
    let restored = round_trip(&aggregate)?;
    let ReconnectAttemptFateDecision::Refused { reason, .. } =
        record_attempt_fate(restored, attempt, ReconnectAttemptFate::Connected)
    else {
        return Err("pending testimony must refuse attempt fate");
    };
    assert_eq!(
        reason,
        ReconnectAttemptFateRefusalReason::LostAuthorityPending
    );
    Ok(())
}

#[test]
fn pending_abandonment_gates_tokenless_admission_only() -> TestResult {
    let mut aggregate = bound()?;
    let request = tokenless_request();
    aggregate.expected = Some(ExpectedOperationState {
        request: request.clone(),
        issued: true,
        authorization: 1,
        lost: None,
    });
    aggregate.next_operation_authorization = 1;
    let restored = round_trip(&aggregate)?;
    assert!(
        restored
            .restored_operation_abandonment()
            .is_some_and(RestoredExpectedOperationAbandonment::was_issued)
    );
    let ClientOperationRecordDecision::Refused(refusal) = record_operation(restored, request)
    else {
        return Err("tokenless re-record must wait for the pending abandonment");
    };
    assert_eq!(
        refusal.reason(),
        ClientOperationRecordRefusalReason::AbandonmentPending
    );
    let (aggregate, _) = refusal.into_parts();
    assert!(
        aggregate.restored_operation_abandonment().is_some(),
        "refusal must retain the pending abandonment"
    );
    assert!(matches!(
        record_operation(aggregate, detach_request()?),
        ClientOperationRecordDecision::Pending(_)
    ));
    Ok(())
}

#[test]
fn unissued_tokenless_abandonment_take_releases_nothing() -> TestResult {
    let ClientOperationRecordDecision::Pending(pending) =
        record_operation(bound()?, tokenless_request())
    else {
        return Err("record admission must enter the durability barrier");
    };
    let commit = pending.commit();
    let mut restored = commit
        .resume_record()
        .map_err(|_| "commit must encode")?
        .restore()
        .map_err(|_| "commit must restore")?;
    let abandonment = restored
        .take_restored_operation_abandonment()
        .ok_or("unissued tokenless restore must abandon")?;
    assert!(!abandonment.was_issued());
    assert!(restored.take_restored_operation_abandonment().is_none());
    assert!(matches!(
        recover_expected_operation(restored),
        RecoveredExpectedOperationDecision::NotAvailable {
            already_issued: false,
            ..
        }
    ));
    Ok(())
}

#[test]
fn same_envelope_re_record_refuses_every_inactive_status_and_admits_parked() -> TestResult {
    let inactive = [
        DetachReplayStatus::Superseded,
        DetachReplayStatus::LeaveSuperseded,
        DetachReplayStatus::Terminal(DetachReplayTerminal::DetachCommitted(
            crate::wire::DetachCommitted::new(
                161,
                162,
                DetachAttemptToken::new([164; 16]),
                epoch(7)?,
                0,
            ),
        )),
    ];
    for status in inactive {
        let mut aggregate = bound()?;
        aggregate.detach_replay.state = super::replay::DetachReplayState::Recorded {
            request: detach_envelope()?,
            status,
        };
        let ClientOperationRecordDecision::Refused(refusal) =
            record_operation(aggregate, detach_request()?)
        else {
            return Err("inactive same-envelope re-record must refuse");
        };
        assert_eq!(
            refusal.reason(),
            ClientOperationRecordRefusalReason::DetachReplayIncompatible
        );
    }

    let mut parked = bound()?;
    parked.detach_replay.state = super::replay::DetachReplayState::Recorded {
        request: detach_envelope()?,
        status: DetachReplayStatus::Parked,
    };
    assert!(matches!(
        record_operation(parked, detach_request()?),
        ClientOperationRecordDecision::Pending(_)
    ));
    Ok(())
}
