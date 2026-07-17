use alloc::{vec, vec::Vec};

use super::*;
use crate::wire::{
    AttachAttemptToken, AttachSecret, BindingEpoch, BindingStateView, ClientRequest,
    ConnectionIncarnation, CredentialAttachRequest, DetachAttemptToken, DetachEnvelope,
    DetachInProgress, DetachRequest, EnrollmentRequest, EnrollmentToken, Generation,
    LeaveAttemptToken, LeaveRequest, MarkerAck, ObserverRecoveryHandshake, ParticipantAck,
    RecordAdmission, TerminalizedDetachCell,
};

type TestResult<T = ()> = Result<T, &'static str>;

fn generation(value: u64) -> TestResult<Generation> {
    Generation::new(value).ok_or("test generation must be nonzero")
}

fn epoch(value: u64) -> TestResult<BindingEpoch> {
    Ok(BindingEpoch::new(
        ConnectionIncarnation::new(4, 5),
        generation(value)?,
    ))
}

fn request() -> TestResult<DetachEnvelope> {
    Ok(DetachEnvelope {
        conversation_id: 10,
        participant_id: 11,
        capability_generation: generation(7)?,
        detach_attempt_token: DetachAttemptToken::new([12; 16]),
    })
}

fn round_trip(aggregate: &ClientParticipantAggregate) -> TestResult<ClientParticipantAggregate> {
    let record = aggregate
        .resume_record()
        .map_err(|_| "live aggregate must encode")?;
    let bytes = record.encode_canonical();
    assert_eq!(bytes, record.encode_canonical());
    ClientResumeRecord::decode_canonical(&bytes)
        .map_err(|_| "canonical record must decode")?
        .restore()
        .map_err(|_| "canonical facts must restore")
}

fn aggregate_with_replay(status: DetachReplayStatus) -> TestResult<ClientParticipantAggregate> {
    let mut aggregate = ClientParticipantAggregate::new();
    aggregate.detach_replay.state = super::replay::DetachReplayState::Recorded {
        request: request()?,
        status,
    };
    Ok(aggregate)
}

#[test]
fn resume_round_trips_every_binding_and_expected_operation() -> TestResult {
    let binding_states = vec![
        ClientBindingState::Unbound,
        ClientBindingState::Bound {
            conversation_id: 1,
            participant_id: 2,
            generation: generation(3)?,
            attach_secret: AttachSecret::new([4; 32]),
            binding_epoch: epoch(3)?,
        },
        ClientBindingState::Detached {
            conversation_id: 1,
            participant_id: 2,
            generation: generation(3)?,
        },
        ClientBindingState::Left {
            conversation_id: 1,
            participant_id: 2,
            generation: generation(3)?,
        },
    ];
    for binding in binding_states {
        let mut aggregate = ClientParticipantAggregate::new();
        aggregate.binding = binding.clone();
        let restored = round_trip(&aggregate)?;
        assert_eq!(restored.binding, binding);
    }

    let generation = generation(2)?;
    let operations = vec![
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: 1,
            enrollment_token: EnrollmentToken::new([1; 16]),
        }),
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: 1,
            participant_id: 2,
            capability_generation: generation,
            attach_secret: AttachSecret::new([2; 32]),
            attach_attempt_token: AttachAttemptToken::new([3; 16]),
            accept_marker_delivery_seq: Some(4),
        }),
        ClientRequest::Detach(DetachRequest {
            conversation_id: 1,
            participant_id: 2,
            capability_generation: generation,
            detach_attempt_token: DetachAttemptToken::new([4; 16]),
        }),
        ClientRequest::Leave(LeaveRequest {
            conversation_id: 1,
            participant_id: 2,
            capability_generation: generation,
            attach_secret: AttachSecret::new([5; 32]),
            leave_attempt_token: LeaveAttemptToken::new([6; 16]),
        }),
        ClientRequest::MarkerAck(MarkerAck {
            conversation_id: 1,
            participant_id: 2,
            capability_generation: generation,
            marker_delivery_seq: 7,
        }),
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: 1,
            participant_id: 2,
            capability_generation: generation,
            payload: vec![8, 9],
        }),
        ClientRequest::ObserverRecovery(ObserverRecoveryHandshake {
            observer_refusals: Vec::new(),
        }),
    ];
    for operation in operations {
        let ClientOperationRecordDecision::Pending(pending) =
            record_operation(ClientParticipantAggregate::new(), operation.clone())
        else {
            return Err("write-ahead operation must become pending");
        };
        let bytes = pending
            .encode_resume_record()
            .map_err(|_| "pending successor must encode")?;
        let restored = ClientResumeRecord::decode_canonical(&bytes)
            .map_err(|_| "pending record must decode")?
            .restore()
            .map_err(|_| "pending record must restore")?;
        assert_eq!(restored.expected.as_ref(), Some(&operation));
        let (aborted, returned) = pending.abort();
        assert!(!aborted.has_expected_operation());
        assert_eq!(returned, operation);
    }

    let ack = ClientRequest::ParticipantAck(ParticipantAck {
        conversation_id: 1,
        participant_id: 2,
        capability_generation: generation,
        through_seq: 9,
    });
    assert!(matches!(
        record_operation(ClientParticipantAggregate::new(), ack),
        ClientOperationRecordDecision::Continuous(_)
    ));
    Ok(())
}

#[test]
fn resume_round_trips_every_replay_status_and_terminal_payload() -> TestResult {
    let simple = [
        DetachReplayStatus::Parked,
        DetachReplayStatus::InFlight,
        DetachReplayStatus::Superseded,
        DetachReplayStatus::LeaveSuperseded,
    ];
    for status in simple {
        let aggregate = aggregate_with_replay(status)?;
        let restored = round_trip(&aggregate)?;
        assert_eq!(
            restored.detach_replay.status(),
            aggregate.detach_replay.status()
        );
        assert_eq!(
            restored.detach_replay.request(),
            aggregate.detach_replay.request()
        );
    }

    let request = request()?;
    let committed =
        crate::wire::DetachCommitted::new(10, 11, request.detach_attempt_token, epoch(7)?, 20);
    let committed = aggregate_with_replay(DetachReplayStatus::Terminal(
        DetachReplayTerminal::DetachCommitted(committed),
    ))?;
    assert!(matches!(
        round_trip(&committed)?.detach_replay.status(),
        Some(DetachReplayStatus::Terminal(
            DetachReplayTerminal::DetachCommitted(_)
        ))
    ));

    let in_progress = DetachInProgress {
        conversation_id: 10,
        participant_id: 11,
        presented_token: request.detach_attempt_token,
        presented_generation: generation(7)?,
        committed_binding_epoch: epoch(7)?,
    };
    let in_progress = aggregate_with_replay(DetachReplayStatus::Terminal(
        DetachReplayTerminal::DetachInProgress(in_progress),
    ))?;
    assert!(matches!(
        round_trip(&in_progress)?.detach_replay.status(),
        Some(DetachReplayStatus::Terminal(
            DetachReplayTerminal::DetachInProgress(_)
        ))
    ));

    let cell = TerminalizedDetachCell::for_client_test(
        10,
        11,
        generation(7)?,
        request.detach_attempt_token,
        generation(8)?,
        epoch(7)?,
        BindingStateView::Detached,
    );
    let cell = aggregate_with_replay(DetachReplayStatus::Terminal(
        DetachReplayTerminal::TerminalizedDetachCell(cell),
    ))?;
    assert!(matches!(
        round_trip(&cell)?.detach_replay.status(),
        Some(DetachReplayStatus::Terminal(
            DetachReplayTerminal::TerminalizedDetachCell(_)
        ))
    ));
    Ok(())
}

#[test]
fn restored_permit_is_released_once_and_failure_parks_without_timer() -> TestResult {
    let ReconnectPermitDecision::Permitted {
        aggregate, permit, ..
    } = record_transport_fate(
        ClientParticipantAggregate::new(),
        EstablishedConnectionTransportFate::Lost,
    )
    else {
        return Err("fresh fate must mint permit");
    };
    let ReconnectPermitDecision::Permitted {
        aggregate: conflicting_aggregate,
        permit: _,
        ..
    } = record_explicit_reconnect(
        ClientParticipantAggregate::new(),
        ExplicitReconnectAction::ReconnectNow,
    )
    else {
        return Err("explicit action must mint permit");
    };
    let ReconnectAttemptDecision::Refused {
        permit,
        reason: ReconnectAttemptRefusalReason::StalePermit,
        ..
    } = redeem_attempt(conflicting_aggregate, permit)
    else {
        return Err("permit for another fresh event must refuse stale");
    };
    let record = aggregate
        .resume_record()
        .map_err(|_| "permit state must encode")?;
    let restored = ClientResumeRecord::decode_canonical(&record.encode_canonical())
        .map_err(|_| "permit record must decode")?
        .restore()
        .map_err(|_| "permit record must restore")?;
    let RecoveredReconnectPermitDecision::Recovered {
        aggregate,
        permit: restored_permit,
    } = recover_reconnect_permit(restored)
    else {
        return Err("restored permit must release once");
    };
    let RecoveredReconnectPermitDecision::NotAvailable { aggregate, .. } =
        recover_reconnect_permit(aggregate)
    else {
        return Err("second restored release must refuse");
    };
    let ReconnectAttemptDecision::Started { aggregate, attempt } =
        redeem_attempt(aggregate, restored_permit)
    else {
        return Err("restored permit must redeem");
    };
    let ReconnectAttemptFateDecision::Recorded(aggregate) =
        record_attempt_fate(aggregate, attempt, ReconnectAttemptFate::Failed)
    else {
        return Err("failure must record");
    };
    assert_eq!(
        aggregate.reconnect().state(),
        crate::outcome::ReconnectState::Parked
    );

    let stale = redeem_attempt(aggregate, permit);
    assert!(matches!(
        stale,
        ReconnectAttemptDecision::Refused {
            reason: ReconnectAttemptRefusalReason::NoPermit,
            ..
        }
    ));
    Ok(())
}

#[test]
fn resume_round_trips_all_reconnect_machine_states() -> TestResult {
    let event = ReconnectFreshEvent::OnlineTransition(ProvedOnlineTransition::ProvedOnline);
    let states = [
        super::reconnect::ReconnectMachineState::Parked,
        super::reconnect::ReconnectMachineState::Online,
        super::reconnect::ReconnectMachineState::Attempt {
            authorization: 1,
            event,
        },
    ];
    for state in states {
        let mut aggregate = ClientParticipantAggregate::new();
        aggregate.reconnect.state = state.clone();
        aggregate.reconnect.next_authorization = u64::from(matches!(
            state,
            super::reconnect::ReconnectMachineState::Attempt { .. }
        ));
        let restored = round_trip(&aggregate)?;
        assert_eq!(restored.reconnect.state, state);
    }
    Ok(())
}

#[test]
fn codec_reports_envelope_errors_and_restore_invariants() -> TestResult {
    assert!(matches!(
        ClientResumeRecord::decode_canonical(&[]),
        Err(ClientResumeRecordDecodeError::Truncated { .. })
    ));
    let record = ClientParticipantAggregate::new()
        .resume_record()
        .map_err(|_| "fresh record must encode")?;
    let mut bytes = record.encode_canonical();
    bytes[0] = b'X';
    assert!(matches!(
        ClientResumeRecord::decode_canonical(&bytes),
        Err(ClientResumeRecordDecodeError::InvalidMagic { .. })
    ));
    let mut bytes = record.encode_canonical();
    bytes[5] = 2;
    assert!(matches!(
        ClientResumeRecord::decode_canonical(&bytes),
        Err(ClientResumeRecordDecodeError::UnsupportedVersion { .. })
    ));
    let mut bytes = record.encode_canonical();
    bytes[13] = bytes[13].saturating_add(1);
    assert!(matches!(
        ClientResumeRecord::decode_canonical(&bytes),
        Err(ClientResumeRecordDecodeError::LengthMismatch { .. })
    ));
    let mut bytes = record.encode_canonical();
    bytes[14] = 99;
    assert!(matches!(
        ClientResumeRecord::decode_canonical(&bytes),
        Err(ClientResumeRecordDecodeError::InvalidTag {
            section: ClientResumeRecordSection::Binding,
            ..
        })
    ));

    let mut aggregate = ClientParticipantAggregate::new();
    aggregate.binding = ClientBindingState::Bound {
        conversation_id: 1,
        participant_id: 2,
        generation: generation(3)?,
        attach_secret: AttachSecret::new([4; 32]),
        binding_epoch: epoch(3)?,
    };
    let record = aggregate
        .resume_record()
        .map_err(|_| "bound record must encode")?;
    let mut bytes = record.encode_canonical();
    let epoch_generation_offset = 14 + 1 + 8 + 8 + 8 + 32 + 8 + 8;
    bytes[epoch_generation_offset + 7] = 4;
    let decoded = ClientResumeRecord::decode_canonical(&bytes)
        .map_err(|_| "cross-invariant corruption stays inert at decode")?;
    assert_eq!(
        decoded.restore(),
        Err(ClientResumeRestoreError::BindingGenerationMismatch)
    );
    Ok(())
}

#[test]
fn already_dead_refuses_inbound_without_losing_value() -> TestResult {
    let mut aggregate = ClientParticipantAggregate::new();
    aggregate.binding = ClientBindingState::Left {
        conversation_id: 1,
        participant_id: 2,
        generation: generation(3)?,
    };
    let value = crate::wire::ParticipantTransportRejected {
        reason: crate::wire::TransportRejectionReason::AuthenticationFailed,
    };
    let ClientInboundDecision::Refused(refusal) = decide_inbound(
        aggregate,
        crate::wire::ServerValue::ParticipantTransportRejected(value.clone()),
    ) else {
        return Err("dead aggregate must refuse inbound value");
    };
    assert_eq!(refusal.reason(), ClientInboundRefusalReason::AlreadyDead);
    assert_eq!(
        refusal.into_parts().1,
        crate::wire::ServerValue::ParticipantTransportRejected(value)
    );
    Ok(())
}
