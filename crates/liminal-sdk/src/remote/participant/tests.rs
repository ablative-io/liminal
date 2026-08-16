mod replay;
mod support;

use std::error::Error;
use std::io;

use liminal_protocol::client::{
    ClientInboundRefusalReason, DetachReplayRefusalReason, LostAuthorityKind,
};
use liminal_protocol::wire::{
    AckCommitted, AckGap, AckNoOp, AckRegression, AttachAttemptToken, AttachBound, AttachSecret,
    BindingEpoch, ClientRequest, ConnectionIncarnation, CredentialAttachRequest, DetachAttemptToken,
    DetachCommitted, DetachRequest, EnrollBound, EnrollmentRequest, EnrollmentToken, Generation,
    ParticipantAck, ParticipantAckEnvelope, ReceiptReplay, RecordAdmission, RecordAdmissionEnvelope,
    RecordCommitted, ServerValue,
};

use super::*;
use support::{Action, Loopback, MemoryStore};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const CONVERSATION: u64 = 41;
const PARTICIPANT: u64 = 42;

fn generation(value: u64) -> Result<Generation, io::Error> {
    Generation::new(value).ok_or_else(|| io::Error::other("generation must be nonzero"))
}

fn epoch(value: u64) -> Result<BindingEpoch, io::Error> {
    Ok(BindingEpoch::new(
        ConnectionIncarnation::new(7, 8),
        generation(value)?,
    ))
}

fn enrollment_request() -> ClientRequest {
    ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: CONVERSATION,
        enrollment_token: EnrollmentToken::new([1; 16]),
    })
}

fn enroll_bound(conversation: u64, token: [u8; 16]) -> Result<ServerValue, io::Error> {
    EnrollBound::new(
        conversation,
        EnrollmentToken::new(token),
        PARTICIPANT,
        AttachSecret::new([2; 32]),
        epoch(1)?,
        100,
        200,
    )
    .map(ServerValue::EnrollBound)
    .ok_or_else(|| io::Error::other("enrollment response fixture must be generation one"))
}

fn recorded(
    outcome: RemoteOperationRecordOutcome,
) -> Result<RemoteParticipantOperation, io::Error> {
    match outcome {
        RemoteOperationRecordOutcome::Recorded(operation)
        | RemoteOperationRecordOutcome::Continuous(operation) => Ok(operation),
        RemoteOperationRecordOutcome::Refused { .. } => {
            Err(io::Error::other("fixture operation was refused"))
        }
    }
}

fn sent(
    outcome: &RemoteParticipantSendOutcome,
) -> Result<ParticipantResponseProvenance, io::Error> {
    match outcome {
        RemoteParticipantSendOutcome::Sent { provenance } => Ok(*provenance),
        RemoteParticipantSendOutcome::TransportLost { .. } => {
            Err(io::Error::other("fixture operation transport was lost"))
        }
    }
}

fn enroll(
    handle: &RemoteParticipantHandle<MemoryStore>,
) -> Result<ParticipantResponseProvenance, Box<dyn Error>> {
    let operation = recorded(handle.record_operation(enrollment_request())?)?;
    Ok(sent(&handle.send_operation(operation)?)?)
}

fn record_committed(token: [u8; 16], delivery_seq: u64) -> TestResult<ServerValue> {
    Ok(ServerValue::RecordCommitted(RecordCommitted::new(
        RecordAdmissionEnvelope {
            conversation_id: CONVERSATION,
            participant_id: PARTICIPANT,
            capability_generation: generation(1)?,
            record_admission_attempt_token:
                liminal_protocol::wire::RecordAdmissionAttemptToken::new(token),
        },
        delivery_seq,
    )))
}

fn assert_d1_mismatch_retains_slot(
    handle: &RemoteParticipantHandle<MemoryStore>,
    successor_request: &RecordAdmission,
) -> TestResult {
    match handle.receive()? {
        RemoteParticipantInbound::Refused {
            reason,
            value: ServerValue::RecordCommitted(_),
            ..
        } => assert_eq!(reason, ClientInboundRefusalReason::AmbiguousResponse),
        _ => return Err(io::Error::other("different D1 token must be refused").into()),
    }
    assert!(matches!(
        handle.record_operation(ClientRequest::RecordAdmission(successor_request.clone()))?,
        RemoteOperationRecordOutcome::Refused { .. }
    ));
    match handle.receive()? {
        RemoteParticipantInbound::Applied {
            value: ServerValue::RecordCommitted(_),
            provenance,
        } => assert_eq!(provenance.connection_id(), 1),
        _ => return Err(io::Error::other("exact D1 record response must apply").into()),
    }
    Ok(())
}

#[test]
fn sent_is_not_receipt_real_receive_releases_exact_d1_slot() -> TestResult {
    let record_request = RecordAdmission {
        conversation_id: CONVERSATION,
        participant_id: PARTICIPANT,
        capability_generation: generation(1)?,
        record_admission_attempt_token: liminal_protocol::wire::RecordAdmissionAttemptToken::new(
            [0xA7; 16],
        ),
        payload: vec![9],
    };
    let successor_request = RecordAdmission {
        conversation_id: CONVERSATION,
        participant_id: PARTICIPANT,
        capability_generation: generation(1)?,
        record_admission_attempt_token: liminal_protocol::wire::RecordAdmissionAttemptToken::new(
            [0xB8; 16],
        ),
        payload: vec![10],
    };
    let loopback = Loopback::spawn(vec![vec![
        Action::Respond(vec![
            enroll_bound(99, [1; 16])?,
            enroll_bound(CONVERSATION, [9; 16])?,
            enroll_bound(CONVERSATION, [1; 16])?,
        ]),
        Action::Respond(vec![
            record_committed([0xC9; 16], 10)?,
            record_committed([0xA7; 16], 10)?,
        ]),
        Action::Respond(vec![record_committed([0xB8; 16], 11)?]),
    ]])?;
    let config = loopback.connected_config()?;
    let store = MemoryStore::default();
    let observed_store = store.clone();
    let handle = RemoteParticipantHandle::new(&config, store)?;

    let send_provenance = enroll(&handle)?;
    assert_eq!(send_provenance.connection_id(), 1);
    assert_eq!(send_provenance.attempt_id(), 1);

    match handle.receive()? {
        RemoteParticipantInbound::Refused {
            reason, provenance, ..
        } => {
            assert_eq!(reason, ClientInboundRefusalReason::ForeignResponse);
            assert_eq!(provenance, send_provenance);
        }
        _ => return Err(io::Error::other("foreign enrollment must be refused").into()),
    }
    match handle.receive()? {
        RemoteParticipantInbound::Refused { reason, .. } => {
            assert_eq!(reason, ClientInboundRefusalReason::DelayedResponse);
        }
        _ => return Err(io::Error::other("older enrollment must be delayed").into()),
    }
    assert!(matches!(
        handle.receive()?,
        RemoteParticipantInbound::Applied {
            value: ServerValue::EnrollBound(_),
            ..
        }
    ));

    let operation =
        recorded(handle.record_operation(ClientRequest::RecordAdmission(record_request))?)?;
    sent(&handle.send_operation(operation)?)?;
    assert!(matches!(
        handle.record_operation(ClientRequest::RecordAdmission(successor_request.clone()))?,
        RemoteOperationRecordOutcome::Refused { .. }
    ));
    assert_d1_mismatch_retains_slot(&handle, &successor_request)?;
    // Only applying the exact-token terminal answer released the cardinality-one
    // write-ahead slot; `Sent` above was never treated as receipt.
    let operation =
        recorded(handle.record_operation(ClientRequest::RecordAdmission(successor_request))?)?;
    sent(&handle.send_operation(operation)?)?;
    assert!(matches!(
        handle.receive()?,
        RemoteParticipantInbound::Applied {
            value: ServerValue::RecordCommitted(_),
            ..
        }
    ));
    let canonical = observed_store.bytes()?;
    liminal_protocol::client::ClientResumeRecord::decode_canonical(&canonical)
        .map_err(|error| io::Error::other(format!("stored LPCR did not decode: {error:?}")))?;
    loopback.finish()?;
    Ok(())
}

#[test]
fn contract_c34_ack_values_cross_the_real_receive_path() -> TestResult {
    let generation = generation(1)?;
    let requests = [9_u64, 10, 12, 14].map(|through_seq| ParticipantAckEnvelope {
        conversation_id: CONVERSATION,
        participant_id: PARTICIPANT,
        capability_generation: generation,
        through_seq,
    });
    let values = vec![
        ServerValue::AckRegression(
            AckRegression::new(requests[0].clone(), 10)
                .ok_or_else(|| io::Error::other("9 must regress below cursor 10"))?,
        ),
        ServerValue::AckNoOp(AckNoOp::participant_ack(requests[1].clone())),
        ServerValue::AckCommitted(AckCommitted::new(requests[2].clone())),
        ServerValue::AckGap(
            AckGap::new(requests[3].clone(), 12)
                .ok_or_else(|| io::Error::other("14 must gap above offered-through 12"))?,
        ),
    ];
    let mut actions = vec![Action::Respond(vec![enroll_bound(CONVERSATION, [1; 16])?])];
    actions.extend(values.into_iter().map(|value| Action::Respond(vec![value])));
    let loopback = Loopback::spawn(vec![actions])?;
    let config = loopback.connected_config()?;
    let handle = RemoteParticipantHandle::new(&config, MemoryStore::default())?;
    enroll(&handle)?;
    assert!(matches!(
        handle.receive()?,
        RemoteParticipantInbound::Applied { .. }
    ));

    for through_seq in [9_u64, 10, 12, 14] {
        let request = ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: CONVERSATION,
            participant_id: PARTICIPANT,
            capability_generation: generation,
            through_seq,
        });
        let operation = recorded(handle.record_operation(request)?)?;
        sent(&handle.send_operation(operation)?)?;
        assert!(matches!(
            handle.receive()?,
            RemoteParticipantInbound::Applied { .. }
        ));
    }
    loopback.finish()?;
    Ok(())
}

#[test]
fn response_loss_reconnects_once_and_replays_exact_detach_token() -> TestResult {
    let detach_token = DetachAttemptToken::new([4; 16]);
    let terminal = ServerValue::DetachCommitted(DetachCommitted::new(
        CONVERSATION,
        PARTICIPANT,
        detach_token,
        epoch(1)?,
        13,
    ));
    let loopback = Loopback::spawn(vec![
        vec![
            Action::Respond(vec![enroll_bound(CONVERSATION, [1; 16])?]),
            Action::DropAfterRequest,
        ],
        vec![Action::Respond(vec![terminal])],
    ])?;
    let config = loopback.connected_config()?;
    let handle = RemoteParticipantHandle::new(&config, MemoryStore::default())?;
    enroll(&handle)?;
    assert!(matches!(
        handle.receive()?,
        RemoteParticipantInbound::Applied { .. }
    ));

    let detach = ClientRequest::Detach(DetachRequest {
        conversation_id: CONVERSATION,
        participant_id: PARTICIPANT,
        capability_generation: generation(1)?,
        detach_attempt_token: detach_token,
    });
    let operation = recorded(handle.record_operation(detach)?)?;
    sent(&handle.send_operation(operation)?)?;
    assert!(matches!(
        handle.receive(),
        Err(RemoteParticipantError::Transport(_))
    ));
    let loss = handle.record_established_transport_loss()?;
    assert_eq!(
        loss.operation_fate,
        RemoteOperationTransportFate::DetachParked
    );
    let RemoteReconnectPermitOutcome::Permitted { permit, .. } = loss.reconnect else {
        return Err(io::Error::other("transport fate must mint reconnect permit").into());
    };
    let RemoteReconnectAttemptOutcome::Connected {
        provenance: reconnect_provenance,
    } = handle.reconnect(permit)?
    else {
        return Err(io::Error::other("real reconnect attempt must connect").into());
    };
    assert_eq!(reconnect_provenance.connection_id(), 2);
    assert_eq!(reconnect_provenance.attempt_id(), 2);
    assert!(matches!(
        handle.replay_detach()?,
        RemoteDetachReplayOutcome::Send(RemoteParticipantSendOutcome::Sent { provenance })
            if provenance == reconnect_provenance
    ));
    assert!(matches!(
        handle.receive()?,
        RemoteParticipantInbound::Applied {
            value: ServerValue::DetachCommitted(_),
            provenance,
        } if provenance == reconnect_provenance
    ));
    loopback.finish()?;
    Ok(())
}

#[test]
fn lpcr_round_trip_recovers_unissued_and_resolves_issued_testimony() -> TestResult {
    let loopback = Loopback::spawn(vec![vec![Action::DropAfterRequest]])?;
    let config = loopback.connected_config()?;
    let store = MemoryStore::default();
    let observed = store.clone();
    let handle = RemoteParticipantHandle::new(&config, store)?;
    let operation = recorded(handle.record_operation(enrollment_request())?)?;
    let unissued = observed.bytes()?;

    let restored = RemoteParticipantHandle::restore(&config, MemoryStore::default(), &unissued)?;
    assert!(matches!(
        restored.recover_expected_operation()?,
        RemoteExpectedOperationRecovery::Recovered(_)
    ));

    sent(&handle.send_operation(operation)?)?;
    let issued = observed.bytes()?;
    let restored = RemoteParticipantHandle::restore(&config, MemoryStore::default(), &issued)?;
    assert_eq!(
        restored.resolve_lost_operation_authority()?,
        RemoteLostOperationResolution::Recorded {
            request: enrollment_request(),
            testimony: LostAuthorityKind::IssuedOperationCorrelation,
        }
    );
    assert!(matches!(
        restored.resolve_lost_operation_authority()?,
        RemoteLostOperationResolution::Refused { .. }
    ));
    loopback.finish()?;
    Ok(())
}

#[test]
fn nonmatching_attach_preserves_inflight_then_matching_attach_supersedes() -> TestResult {
    let loopback = Loopback::spawn(vec![vec![
        Action::Respond(vec![enroll_bound(CONVERSATION, [1; 16])?]),
        Action::DropAfterRequest,
    ]])?;
    let config = loopback.connected_config()?;
    let handle = RemoteParticipantHandle::new(&config, MemoryStore::default())?;
    enroll(&handle)?;
    assert!(matches!(
        handle.receive()?,
        RemoteParticipantInbound::Applied { .. }
    ));
    let operation = recorded(
        handle.record_operation(ClientRequest::Detach(DetachRequest {
            conversation_id: CONVERSATION,
            participant_id: PARTICIPANT,
            capability_generation: generation(1)?,
            detach_attempt_token: DetachAttemptToken::new([7; 16]),
        }))?,
    )?;
    sent(&handle.send_operation(operation)?)?;

    let nonmatching = AttachBound::ordinary(
        999,
        AttachAttemptToken::new([8; 16]),
        PARTICIPANT,
        generation(1)?,
        AttachSecret::new([9; 32]),
        epoch(2)?,
        10,
        100,
        200,
    )
    .ok_or_else(|| io::Error::other("nonmatching attach fixture must construct"))?;
    assert!(matches!(
        handle.apply_attach(nonmatching)?,
        RemoteReplayApplyOutcome::Refused {
            reason: DetachReplayRefusalReason::ForeignInput,
            ..
        }
    ));
    let matching = AttachBound::ordinary(
        CONVERSATION,
        AttachAttemptToken::new([8; 16]),
        PARTICIPANT,
        generation(1)?,
        AttachSecret::new([9; 32]),
        epoch(2)?,
        10,
        100,
        200,
    )
    .ok_or_else(|| io::Error::other("matching attach fixture must construct"))?;
    assert_eq!(
        handle.apply_attach(matching)?,
        RemoteReplayApplyOutcome::Applied
    );
    loopback.finish()?;
    Ok(())
}

#[test]
fn tokenless_restore_surfaces_durable_abandonment_for_rerecord() -> TestResult {
    let loopback = Loopback::spawn(vec![vec![Action::Respond(vec![enroll_bound(
        CONVERSATION,
        [1; 16],
    )?])]])?;
    let config = loopback.connected_config()?;
    let store = MemoryStore::default();
    let observed = store.clone();
    let handle = RemoteParticipantHandle::new(&config, store)?;
    enroll(&handle)?;
    assert!(matches!(
        handle.receive()?,
        RemoteParticipantInbound::Applied { .. }
    ));
    let request =
        ClientRequest::ObserverRecovery(liminal_protocol::wire::ObserverRecoveryHandshake {
            observer_refusals: vec![],
        });
    {
        let operation = recorded(handle.record_operation(request.clone())?)?;
        core::hint::black_box(&operation);
    }
    let canonical = observed.bytes()?;
    core::mem::drop(handle);

    let restored = RemoteParticipantHandle::restore(&config, MemoryStore::default(), &canonical)?;
    let abandonment = restored
        .take_restored_operation_abandonment()?
        .ok_or_else(|| io::Error::other("tokenless restore must surface abandonment"))?;
    assert_eq!(abandonment.request(), &request);
    assert!(!abandonment.was_issued());
    assert!(restored.take_restored_operation_abandonment()?.is_none());
    assert!(matches!(
        restored.record_operation(abandonment.into_request())?,
        RemoteOperationRecordOutcome::Recorded(_)
    ));
    loopback.finish()?;
    Ok(())
}

#[test]
fn restored_issued_reconnect_permit_resolves_take_once_testimony() -> TestResult {
    let loopback = Loopback::spawn(vec![Vec::new()])?;
    let config = loopback.connected_config()?;
    let store = MemoryStore::default();
    let observed = store.clone();
    let handle = RemoteParticipantHandle::new(&config, store)?;
    {
        let RemoteReconnectPermitOutcome::Permitted { permit, .. } =
            handle.record_explicit_reconnect()?
        else {
            return Err(io::Error::other("explicit event must mint a permit").into());
        };
        core::hint::black_box(&permit);
    }
    let canonical = observed.bytes()?;
    core::mem::drop(handle);

    let restored = RemoteParticipantHandle::restore(&config, MemoryStore::default(), &canonical)?;
    assert_eq!(
        restored.resolve_lost_reconnect_authority()?,
        RemoteLostReconnectResolution::Recorded {
            testimony: LostAuthorityKind::ReconnectPermit,
        }
    );
    assert!(matches!(
        restored.resolve_lost_reconnect_authority()?,
        RemoteLostReconnectResolution::Refused { .. }
    ));
    loopback.finish()?;
    Ok(())
}

/// The server's assigned record sequence must be readable from the SDK's own
/// record-admission return path, and it must be the exact number the wire
/// response carried.
///
/// The expected value is read off the `RecordCommitted` the fixture server will
/// actually send rather than restated as a literal, so the assertion compares
/// the surfaced sequence against the wire response itself. A literal would pass
/// just as well against an accessor that returned a number the SDK invented.
///
/// The `None` legs are the other half of the discriminator: an applied response
/// that is not a commit, and a response the crate refused rather than applied,
/// must both stay empty. A refused `RecordCommitted` in particular carries a
/// sequence on the wire while committing the client to nothing, so surfacing it
/// would be a claim the crate declined to make.
#[test]
fn committed_delivery_seq_reaches_the_record_admission_return_path() -> TestResult {
    const SURFACED_SEQ: u64 = 424_242;
    const EXACT_TOKEN: [u8; 16] = [0xD4; 16];
    const FOREIGN_TOKEN: [u8; 16] = [0xE5; 16];

    let request = RecordAdmission {
        conversation_id: CONVERSATION,
        participant_id: PARTICIPANT,
        capability_generation: generation(1)?,
        record_admission_attempt_token: liminal_protocol::wire::RecordAdmissionAttemptToken::new(
            EXACT_TOKEN,
        ),
        payload: vec![11],
    };
    let wire_response = record_committed(EXACT_TOKEN, SURFACED_SEQ)?;
    let ServerValue::RecordCommitted(ref committed) = wire_response else {
        return Err(io::Error::other("fixture value must be a RecordCommitted").into());
    };
    let wire_delivery_seq = committed.delivery_seq();

    let loopback = Loopback::spawn(vec![vec![
        Action::Respond(vec![enroll_bound(CONVERSATION, [1; 16])?]),
        // The foreign-token commit is refused by the crate's conservative
        // ambiguity rule; the exact-token commit is applied.
        Action::Respond(vec![
            record_committed(FOREIGN_TOKEN, SURFACED_SEQ + 1)?,
            wire_response,
        ]),
    ]])?;
    let config = loopback.connected_config()?;
    let handle = RemoteParticipantHandle::new(&config, MemoryStore::default())?;

    enroll(&handle)?;
    let enrolled = handle.receive()?;
    assert!(matches!(
        enrolled,
        RemoteParticipantInbound::Applied {
            value: ServerValue::EnrollBound(_),
            ..
        }
    ));
    assert_eq!(
        enrolled.committed_delivery_seq(),
        None,
        "an applied response that is not a record commit carries no record sequence"
    );

    let operation = recorded(handle.record_operation(ClientRequest::RecordAdmission(request))?)?;
    sent(&handle.send_operation(operation)?)?;

    let refused = handle.receive()?;
    assert!(matches!(
        refused,
        RemoteParticipantInbound::Refused {
            value: ServerValue::RecordCommitted(_),
            reason: ClientInboundRefusalReason::AmbiguousResponse,
            ..
        }
    ));
    assert_eq!(
        refused.committed_delivery_seq(),
        None,
        "a commit the crate refused must not surface a sequence it never applied"
    );

    let applied = handle.receive()?;
    assert!(matches!(
        applied,
        RemoteParticipantInbound::Applied {
            value: ServerValue::RecordCommitted(_),
            ..
        }
    ));
    assert_eq!(
        applied.committed_delivery_seq(),
        Some(wire_delivery_seq),
        "the surfaced record sequence must be the one the wire response carried"
    );
    assert_eq!(wire_delivery_seq, SURFACED_SEQ);

    loopback.finish()?;
    Ok(())
}

/// #195 support: builds the exact killed-mid-attach checkpoint over a loopback.
///
/// The client enrolls, attaches once so a rotation has really happened, then
/// issues a second attach the server never answers. The returned bytes are what
/// the process left behind: an ISSUED credential attach whose response never
/// arrived.
fn killed_mid_attach_checkpoint() -> TestResult<(Vec<u8>, ClientRequest)> {
    let loopback = Loopback::spawn(vec![vec![
        Action::Respond(vec![enroll_bound(CONVERSATION, [1; 16])?]),
        Action::Respond(vec![ServerValue::AttachBound(
            AttachBound::ordinary(
                CONVERSATION,
                AttachAttemptToken::new([0xA1; 16]),
                PARTICIPANT,
                generation(1)?,
                AttachSecret::new([0x22; 32]),
                epoch(2)?,
                0,
                0,
                0,
            )
            .ok_or_else(|| io::Error::other("attach receipt fixture must be a successor"))?,
        )]),
        Action::DropAfterRequest,
    ]])?;
    let config = loopback.connected_config()?;
    let store = MemoryStore::default();
    let observed = store.clone();
    let handle = RemoteParticipantHandle::new(&config, store)?;
    enroll(&handle)?;
    handle.receive()?;

    // First attach: consumed, so the client really is holding a rotated
    // credential (generation 2, secret 0x22) when the kill lands.
    let first = recorded(handle.record_operation(ClientRequest::CredentialAttach(
        CredentialAttachRequest {
            conversation_id: CONVERSATION,
            participant_id: PARTICIPANT,
            capability_generation: generation(1)?,
            attach_secret: AttachSecret::new([2; 32]),
            attach_attempt_token: AttachAttemptToken::new([0xA1; 16]),
            accept_marker_delivery_seq: None,
        },
    ))?)?;
    sent(&handle.send_operation(first)?)?;
    handle.receive()?;

    // Second attach: issued and never answered. This is the retained envelope
    // the driver must re-present, byte for byte.
    let retained = ClientRequest::CredentialAttach(CredentialAttachRequest {
        conversation_id: CONVERSATION,
        participant_id: PARTICIPANT,
        capability_generation: generation(2)?,
        attach_secret: AttachSecret::new([0x22; 32]),
        attach_attempt_token: AttachAttemptToken::new([0xB2; 16]),
        accept_marker_delivery_seq: None,
    });
    let second = recorded(handle.record_operation(retained.clone())?)?;
    sent(&handle.send_operation(second)?)?;
    let checkpoint = observed.bytes()?;
    drop(handle);
    loopback.finish().ok();
    Ok((checkpoint, retained))
}

/// The receipt replay a server sends to heal a same-token re-presentation.
fn healing_receipt_replay() -> TestResult<ServerValue> {
    Ok(ServerValue::Bound(ReceiptReplay::CredentialAttach(
        AttachBound::ordinary(
            CONVERSATION,
            AttachAttemptToken::new([0xB2; 16]),
            PARTICIPANT,
            generation(2)?,
            AttachSecret::new([0x33; 32]),
            epoch(3)?,
            0,
            0,
            0,
        )
        .ok_or_else(|| io::Error::other("replayed receipt fixture must be a successor"))?,
    )))
}

/// #195 PIN (f) — THE A5 NO-EXTENSION PIN.
///
/// The probe is a FIRST ACT. The receipt window is the healing window, it is
/// fixed at the server's commit and never re-opens, so any delay this driver
/// introduced before its first probe would be spent out of the very window the
/// driver exists to spend. A retry discipline must not extend the orphan
/// (participant contract §0.16 condition 2, A5).
///
/// Both halves of "no timer" are measured rather than read off the code: the
/// loopback counts ONE request for the call, so there is no retry loop, and it
/// records WHEN that request landed, so there is no backoff in front of it. A
/// driver that slept before probing, or that probed more than once, fails here
/// and cannot be made to pass by adjusting a constant.
#[test]
fn the_recovery_probe_is_a_first_act_with_no_timer_in_front_of_it() -> TestResult {
    let (checkpoint, _retained) = killed_mid_attach_checkpoint()?;

    let (loopback, observer) =
        Loopback::spawn_observed(vec![vec![Action::Respond(vec![healing_receipt_replay()?])]])?;
    let config = loopback.connected_config()?;
    let restored = RemoteParticipantHandle::restore(&config, MemoryStore::default(), &checkpoint)?;
    assert_eq!(
        observer.request_count()?,
        0,
        "restoring must not probe on its own"
    );

    let started = std::time::Instant::now();
    let recovery = restored.recover_lost_credential_attach()?;
    let probed_at = observer.arrival(0)?;

    assert_eq!(
        observer.request_count()?,
        1,
        "the driver must probe exactly once; a retry loop in front of the window is the defect"
    );
    assert!(
        probed_at.duration_since(started) < core::time::Duration::from_millis(250),
        "the probe must be the call's first act, not something behind a timer: it landed after {:?}",
        probed_at.duration_since(started)
    );
    assert!(
        matches!(
            recovery,
            RemoteCredentialAttachRecovery::HealedFromReceipt { .. }
        ),
        "the probe must heal from the replayed receipt, got {recovery:?}"
    );

    loopback.finish()?;
    Ok(())
}

/// The driver re-presents the retained envelope EXACTLY.
///
/// The whole cure rests on byte-identity: the server matches the re-presentation
/// to its stored receipt by attempt token and verifies it against the receipt's
/// own committed presented secret — the OLD, invalidated one. A driver that
/// minted a fresh token, advanced the generation, or reached for the current
/// credential would form a request the server can match to nothing, which is
/// precisely the orphaned path the base test already shows being refused.
#[test]
fn the_driver_re_presents_the_exact_retained_envelope() -> TestResult {
    let (checkpoint, retained) = killed_mid_attach_checkpoint()?;

    let (loopback, _observer) =
        Loopback::spawn_observed(vec![vec![Action::Respond(vec![healing_receipt_replay()?])]])?;
    let config = loopback.connected_config()?;
    let restored = RemoteParticipantHandle::restore(&config, MemoryStore::default(), &checkpoint)?;

    // The retained envelope, read back through the crate's driver-support seam
    // before anything consumes it.
    let ClientRequest::CredentialAttach(expected) = &retained else {
        return Err(io::Error::other("fixture must retain a credential attach").into());
    };
    assert_eq!(
        restored.peek_lost_credential_attach()?.as_ref(),
        Some(expected),
        "the retained envelope must survive the restore unchanged"
    );

    assert!(matches!(
        restored.recover_lost_credential_attach()?,
        RemoteCredentialAttachRecovery::HealedFromReceipt { .. }
    ));
    loopback.finish()?;
    Ok(())
}
