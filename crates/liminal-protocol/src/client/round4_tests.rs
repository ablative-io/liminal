use super::*;
use crate::wire::{
    AttachAttemptToken, AttachSecret, BindingEpoch, ClientRequest, ConnectionIncarnation,
    CredentialAttachRequest, DetachAttemptToken, DetachEnvelope, DetachRequest, Generation,
    RecordAdmission, ServerValue,
};
use alloc::vec;

type TestResult<T = ()> = Result<T, &'static str>;

fn generation(value: u64) -> TestResult<Generation> {
    Generation::new(value).ok_or("generation must be nonzero")
}

fn epoch(value: u64) -> TestResult<BindingEpoch> {
    Ok(BindingEpoch::new(
        ConnectionIncarnation::new(131, 132),
        generation(value)?,
    ))
}

fn bound() -> TestResult<ClientParticipantAggregate> {
    let mut aggregate = ClientParticipantAggregate::new();
    aggregate.binding = ClientBindingState::Bound {
        conversation_id: 141,
        participant_id: 142,
        generation: generation(7)?,
        attach_secret: AttachSecret::new([143; 32]),
        binding_epoch: epoch(7)?,
    };
    Ok(aggregate)
}

fn detach_request(token: u8) -> TestResult<ClientRequest> {
    Ok(ClientRequest::Detach(DetachRequest {
        conversation_id: 141,
        participant_id: 142,
        capability_generation: generation(7)?,
        detach_attempt_token: DetachAttemptToken::new([token; 16]),
    }))
}

fn detach_envelope(token: u8) -> TestResult<DetachEnvelope> {
    Ok(DetachEnvelope {
        conversation_id: 141,
        participant_id: 142,
        capability_generation: generation(7)?,
        detach_attempt_token: DetachAttemptToken::new([token; 16]),
    })
}

fn expected_detach(token: u8, issued: bool) -> TestResult<ExpectedOperationState> {
    Ok(ExpectedOperationState {
        request: detach_request(token)?,
        issued,
        authorization: 1,
    })
}

#[test]
fn expected_detach_requires_active_matching_replay_full_matrix() -> TestResult {
    let inactive = [
        None,
        Some(DetachReplayStatus::Superseded),
        Some(DetachReplayStatus::LeaveSuperseded),
        Some(DetachReplayStatus::Terminal(
            DetachReplayTerminal::DetachCommitted(crate::wire::DetachCommitted::new(
                141,
                142,
                DetachAttemptToken::new([144; 16]),
                epoch(7)?,
                0,
            )),
        )),
    ];
    for status in inactive {
        let mut aggregate = bound()?;
        aggregate.expected = Some(expected_detach(144, false)?);
        aggregate.next_operation_authorization = 1;
        aggregate.detach_replay.state = match status {
            None => replay::DetachReplayState::Empty,
            Some(status) => replay::DetachReplayState::Recorded {
                request: detach_envelope(144)?,
                status,
            },
        };
        assert_eq!(
            aggregate
                .resume_record()
                .map_err(|_| "converse fixture must encode")?
                .restore(),
            Err(ClientResumeRestoreError::ExpectedDetachActiveReplayMismatch)
        );
    }
    Ok(())
}

#[test]
fn active_replay_requires_exact_expected_detach_full_matrix() -> TestResult {
    for status in [DetachReplayStatus::Parked, DetachReplayStatus::InFlight] {
        for expected in [
            None,
            Some(ExpectedOperationState {
                request: ClientRequest::RecordAdmission(RecordAdmission {
                    conversation_id: 141,
                    participant_id: 142,
                    capability_generation: generation(7)?,
                    payload: vec![1],
                }),
                issued: matches!(status, DetachReplayStatus::InFlight),
                authorization: 1,
            }),
            Some(expected_detach(
                146,
                matches!(status, DetachReplayStatus::InFlight),
            )?),
        ] {
            let mut aggregate = bound()?;
            aggregate.expected = expected;
            aggregate.next_operation_authorization = 1;
            aggregate.detach_replay.state = replay::DetachReplayState::Recorded {
                request: detach_envelope(145)?,
                status: status.clone(),
            };
            assert_eq!(
                aggregate
                    .resume_record()
                    .map_err(|_| "coupling fixture must encode")?
                    .restore(),
                Err(ClientResumeRestoreError::ActiveReplayExpectedDetachMismatch)
            );
        }
    }
    Ok(())
}

#[test]
fn in_flight_refusal_preserves_authorization_counter() -> TestResult {
    let mut aggregate = bound()?;
    aggregate.next_operation_authorization = 17;
    aggregate.detach_replay.state = replay::DetachReplayState::Recorded {
        request: detach_envelope(147)?,
        status: DetachReplayStatus::InFlight,
    };
    let ClientOperationRecordDecision::Refused(refusal) =
        record_operation(aggregate, detach_request(148)?)
    else {
        return Err("in-flight replay must refuse admission");
    };
    assert_eq!(refusal.into_parts().0.next_operation_authorization, 17);
    Ok(())
}

#[test]
fn inbound_wrong_secret_refuses_with_authority_retained() -> TestResult {
    let mut aggregate = bound()?;
    aggregate.expected = Some(ExpectedOperationState {
        request: ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: 141,
            participant_id: 142,
            capability_generation: generation(7)?,
            attach_secret: AttachSecret::new([149; 32]),
            attach_attempt_token: AttachAttemptToken::new([150; 16]),
            accept_marker_delivery_seq: None,
        }),
        issued: true,
        authorization: 1,
    });
    aggregate.next_operation_authorization = 1;
    let response = crate::wire::AttachBound::ordinary(
        141,
        AttachAttemptToken::new([150; 16]),
        142,
        generation(7)?,
        AttachSecret::new([151; 32]),
        epoch(8)?,
        0,
        0,
        0,
    )
    .ok_or("attach response must have successor generation")?;
    let correlation = ClientResponseCorrelation { authorization: 1 };
    let ClientCorrelatedInboundDecision::Refused(refusal) =
        decide_correlated_inbound(aggregate, ServerValue::AttachBound(response), correlation)
    else {
        return Err("wrong-secret expected attach must refuse inbound");
    };
    assert_eq!(
        refusal.reason(),
        ClientInboundRefusalReason::ForeignResponse
    );
    let (aggregate, _, _) = refusal.into_parts();
    assert!(aggregate.has_expected_operation());
    Ok(())
}

#[test]
fn tokenless_restore_is_typed_abandoned_and_never_released() -> TestResult {
    for issued in [false, true] {
        let mut aggregate = bound()?;
        let request = ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: 141,
            participant_id: 142,
            capability_generation: generation(7)?,
            payload: vec![2],
        });
        aggregate.expected = Some(ExpectedOperationState {
            request: request.clone(),
            issued,
            authorization: 1,
        });
        aggregate.next_operation_authorization = 1;
        let mut restored = aggregate
            .resume_record()
            .map_err(|_| "tokenless fixture must encode")?
            .restore()
            .map_err(|_| "tokenless fixture must restore")?;
        let abandonment = restored
            .take_restored_operation_abandonment()
            .ok_or("restore must expose abandonment")?;
        assert_eq!(abandonment.into_request(), request);
        assert!(matches!(
            recover_expected_operation(restored),
            RecoveredExpectedOperationDecision::NotAvailable {
                already_issued: false,
                ..
            }
        ));
    }
    Ok(())
}
