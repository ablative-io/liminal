use std::io;
use std::sync::Arc;
use std::thread;

use liminal_protocol::client::ClientOperationRecordRefusalReason;
use liminal_protocol::wire::{
    AttachAttemptToken, AttachSecret, AuthorityStateTag, BindingStateTag, ClientDiscriminant,
    ClientRequest, CredentialAttachRequest, DetachAttemptToken, DetachCommitted, DetachInProgress,
    DetachRequest, DetachStaleAuthority, LeaveAttemptToken, LeaveCommitted, ProtocolVersion,
    ServerDiscriminant, ServerValue, StaleAuthority, decode_server_value_body,
};

use super::support::{Action, Loopback, MemoryStore, PausedReconnectLoopback};
use super::{
    CONVERSATION, PARTICIPANT, TestResult, enroll, enroll_bound, epoch, generation, recorded, sent,
};
use crate::connection::ConnectionPoolConfig;
use crate::remote::{RemoteConfig, RemoteParticipantHandle};
use crate::{ParticipantResumeStore, SdkError};

#[test]
fn all_detach_terminal_arms_apply_and_survive_lpcr_restore() -> TestResult {
    let token = DetachAttemptToken::new([4; 16]);
    let values = [
        ServerValue::DetachCommitted(DetachCommitted::new(
            CONVERSATION,
            PARTICIPANT,
            token,
            epoch(1)?,
            13,
        )),
        ServerValue::DetachInProgress(DetachInProgress {
            conversation_id: CONVERSATION,
            participant_id: PARTICIPANT,
            presented_token: token,
            presented_generation: generation(1)?,
            committed_binding_epoch: epoch(1)?,
        }),
        terminalized_detach(token)?,
    ];

    for value in values {
        run_detach_terminal(value, token)?;
    }
    Ok(())
}

#[test]
fn durable_leave_from_real_receive_supersedes_inflight_detach() -> TestResult {
    let leave = LeaveCommitted::new(
        CONVERSATION,
        LeaveAttemptToken::new([6; 16]),
        PARTICIPANT,
        generation(1)?,
        Some(epoch(1)?),
        None,
        14,
    )
    .ok_or_else(|| io::Error::other("leave fixture must be internally consistent"))?;
    let loopback = Loopback::spawn(vec![vec![
        Action::Respond(vec![enroll_bound(CONVERSATION, [1; 16])?]),
        Action::Respond(vec![ServerValue::LeaveCommitted(leave)]),
    ]])?;
    let config = loopback.connected_config()?;
    let store = MemoryStore::default();
    let observed = store.clone();
    let handle = RemoteParticipantHandle::new(&config, store)?;
    enroll_and_receive(&handle)?;
    send_detach(&handle, DetachAttemptToken::new([4; 16]))?;

    let super::RemoteParticipantInbound::Refused {
        value: ServerValue::LeaveCommitted(leave),
        ..
    } = handle.receive()?
    else {
        return Err(io::Error::other("Leave must first preserve detach correlation").into());
    };
    assert_eq!(
        handle.apply_leave_durable(leave)?,
        super::RemoteReplayApplyOutcome::Applied
    );
    assert_replay_inactive(&handle)?;
    let canonical = observed.bytes()?;
    loopback.finish()?;

    let restored = RemoteParticipantHandle::restore(&config, MemoryStore::default(), &canonical)?;
    assert_replay_inactive(&restored)?;
    Ok(())
}

#[test]
fn issued_tokenless_restore_reports_durable_abandonment() -> TestResult {
    let loopback = Loopback::spawn(vec![vec![
        Action::Respond(vec![enroll_bound(CONVERSATION, [1; 16])?]),
        Action::DropAfterRequest,
    ]])?;
    let config = loopback.connected_config()?;
    let store = MemoryStore::default();
    let observed = store.clone();
    let handle = RemoteParticipantHandle::new(&config, store)?;
    enroll_and_receive(&handle)?;
    let request =
        ClientRequest::ObserverRecovery(liminal_protocol::wire::ObserverRecoveryHandshake {
            observer_refusals: vec![],
        });
    let operation = recorded(handle.record_operation(request.clone())?)?;
    sent(&handle.send_operation(operation)?)?;
    let canonical = observed.bytes()?;
    drop(handle);

    let restored = RemoteParticipantHandle::restore(&config, MemoryStore::default(), &canonical)?;
    let abandonment = restored
        .take_restored_operation_abandonment()?
        .ok_or_else(|| {
            io::Error::other("issued tokenless operation must be abandoned on restore")
        })?;
    assert_eq!(abandonment.request(), &request);
    assert!(abandonment.was_issued());
    assert!(restored.take_restored_operation_abandonment()?.is_none());
    loopback.finish()?;
    Ok(())
}

#[test]
fn in_progress_real_reconnect_restores_take_once_testimony() -> TestResult {
    let loopback = PausedReconnectLoopback::spawn()?;
    let config = loopback.connected_config()?;
    let store = MemoryStore::default();
    let observed = store.clone();
    let handle = Arc::new(RemoteParticipantHandle::new(&config, store)?);
    let super::RemoteReconnectPermitOutcome::Permitted { permit, .. } =
        handle.record_explicit_reconnect()?
    else {
        return Err(io::Error::other("explicit event must mint reconnect authority").into());
    };

    let reconnect_handle = Arc::clone(&handle);
    let reconnect = thread::spawn(move || reconnect_handle.reconnect(permit));
    loopback.wait_until_attempt_started()?;
    let canonical_attempt = observed.bytes()?;

    let restored =
        RemoteParticipantHandle::restore(&config, MemoryStore::default(), &canonical_attempt)?;
    assert_eq!(
        restored.resolve_lost_reconnect_authority()?,
        super::RemoteLostReconnectResolution::Recorded {
            testimony: liminal_protocol::client::LostAuthorityKind::ReconnectAttempt,
        }
    );
    assert!(matches!(
        restored.resolve_lost_reconnect_authority()?,
        super::RemoteLostReconnectResolution::Refused { .. }
    ));

    loopback.finish()?;
    let reconnect = reconnect
        .join()
        .map_err(|_| io::Error::other("real reconnect thread panicked"))??;
    assert!(matches!(
        reconnect,
        super::RemoteReconnectAttemptOutcome::Connected { .. }
    ));
    Ok(())
}

#[test]
fn failed_checkpoint_withholds_successor_authority() -> TestResult {
    let config = RemoteConfig::new(
        "participant-checkpoint.invalid:1",
        "participant-tests",
        "participant-tests",
        ConnectionPoolConfig::new(1, 1, 1),
    )?;
    let handle = RemoteParticipantHandle::new(&config, FailSecondWrite::default())?;
    assert!(matches!(
        handle.record_explicit_reconnect(),
        Err(super::RemoteParticipantError::Storage(_))
    ));
    assert!(matches!(
        handle.record_explicit_reconnect(),
        Err(super::RemoteParticipantError::StateUnavailable)
    ));
    Ok(())
}

/// RED AT 8c8adec (P0 #59, leg 2): a PURE-FUNCTION encode refusal destroyed live
/// participant state.
///
/// Every SDK seam is `take_aggregate` -> persist -> re-seat, so a `?` on the
/// persist propagated with the aggregate still owned by the local frame. It was
/// dropped there, `state.aggregate` stayed `None`, and every later call on the
/// handle returned `StateUnavailable` forever.
///
/// `ClientResumeRecordEncodeError` is the one persist failure that carries no
/// durability ambiguity: `resume_record` is a pure function of the aggregate
/// that wrote nothing and released nothing. Losing the handle to it is pure
/// amplification -- a recoverable typed refusal became a permanently dead
/// participant. (A `Storage` failure is the opposite and must keep bricking;
/// `failed_checkpoint_withholds_successor_authority` above pins that.)
///
/// This is a LATENT amplifier found while tracing the 2026-08-10 estate death.
/// It is NOT that death's cause: the torn state in the fatal log held a
/// `CredentialAttach` expected slot, and no `ResumeEncode`, `StateUnavailable`,
/// or detach-replay witness appears anywhere in it.
///
/// The torn state here is built from real seams only -- enroll, record, send,
/// crash, restore, resolve testimony, replay -- and then answered by a rotated
/// broker that refuses the replayed generation's authority outright.
#[test]
fn refused_detach_authority_never_destroys_live_participant_state() -> TestResult {
    let token = DetachAttemptToken::new([9; 16]);
    let (loopback, torn, config) = torn_rotation_state(token)?;
    let restored = RemoteParticipantHandle::restore(&config, MemoryStore::default(), &torn)?;
    replay_into_rotated_broker(&restored)?;

    // The inbound that carries the refusal may itself fail to checkpoint, but it
    // must never be the call that reports the state already destroyed.
    alive(&restored.receive(), "the refusing inbound")?;
    // Whatever it reported, the handle still owns its aggregate. These three
    // probe different seams: no persist, a persist, then a persist again --
    // proving the re-seat holds on the failing path too, not just once.
    alive(
        &restored.recover_expected_operation(),
        "recover_expected_operation",
    )?;
    alive(&restored.record_transport_fate(), "record_transport_fate")?;
    alive(
        &restored.recover_expected_operation(),
        "recover_expected_operation after a failing checkpoint",
    )?;
    loopback.finish()?;
    Ok(())
}

/// RED AT 8c8adec (P0 #59, leg 1): the refusal must CHECKPOINT, and the record
/// it writes must restore into a participant the consumer can still steer.
///
/// Leg 2 keeps the handle alive through the encode refusal, which is the
/// difference between a typed error and a dead kernel -- but it leaves the
/// aggregate decoupled and every checkpoint from then on failing. This asserts
/// the state the SDK is actually left in: the inbound is applied AND persisted,
/// those bytes restore, and the restored participant answers the consumer's
/// re-attach probe with a refusal it can act on rather than an error.
///
/// The probe presents the generation the broker just disclosed. The local
/// binding has no credential at that generation, so the crate refuses with
/// `BindingMismatch` -- the catchable outcome a fresh-state fallback keys on.
/// Note this keys on FIRST-PRESENT refusals only; once the replay is settled, a
/// re-presented detach is terminal by the consumer's own design, which is
/// outside this crate's scope.
///
/// Kept separate from the leg-2 pin on purpose: that one asserts survival, this
/// one asserts the state survived INTO. Either leg can fail alone.
#[test]
fn a_refused_detach_authority_checkpoints_and_restores_steerable() -> TestResult {
    let token = DetachAttemptToken::new([10; 16]);
    let (loopback, torn, config) = torn_rotation_state(token)?;
    let store = MemoryStore::default();
    let observed = store.clone();
    let restored = RemoteParticipantHandle::restore(&config, store, &torn)?;
    replay_into_rotated_broker(&restored)?;

    let inbound = restored.receive().map_err(|error| {
        io::Error::other(format!(
            "#59 REPRODUCED: the refusal could not be checkpointed: {error:?}"
        ))
    })?;
    assert!(
        matches!(
            inbound,
            super::RemoteParticipantInbound::Applied {
                value: ServerValue::StaleAuthority(_),
                ..
            }
        ),
        "the rotated broker's refusal must be applied, not retained"
    );

    let canonical = observed.bytes()?;
    loopback.finish()?;
    let reborn = RemoteParticipantHandle::restore(&config, MemoryStore::default(), &canonical)
        .map_err(|error| {
            io::Error::other(format!(
                "#59 REPRODUCED: the checkpointed refusal does not restore: {error:?}"
            ))
        })?;
    assert!(
        matches!(
            reborn.record_operation(stale_reattach())?,
            super::RemoteOperationRecordOutcome::Refused {
                reason: ClientOperationRecordRefusalReason::BindingMismatch,
                ..
            }
        ),
        "the restored participant must answer the re-attach probe with a catchable refusal"
    );
    Ok(())
}

/// The consumer's fresh-state probe: re-attach at the generation the broker
/// disclosed, which the local binding has no credential for.
fn stale_reattach() -> ClientRequest {
    ClientRequest::CredentialAttach(CredentialAttachRequest {
        conversation_id: CONVERSATION,
        participant_id: PARTICIPANT,
        capability_generation: generation(2).expect("generation two is nonzero"),
        attach_secret: AttachSecret::new([2; 32]),
        attach_attempt_token: AttachAttemptToken::new([11; 16]),
        accept_marker_delivery_seq: None,
    })
}

/// Builds the real torn state: a node whose durable resume record is ahead of
/// its credential, holding an issued gen-1 detach it never got an answer to.
/// Returns the loopback, those bytes, and a config on a fresh session whose
/// broker has rotated past the replayed generation.
fn torn_rotation_state(token: DetachAttemptToken) -> TestResult<(Loopback, Vec<u8>, RemoteConfig)> {
    let loopback = Loopback::spawn(vec![
        vec![
            Action::Respond(vec![enroll_bound(CONVERSATION, [1; 16])?]),
            Action::DropAfterRequest,
        ],
        vec![Action::Respond(vec![rotated_broker_refusal(token)?])],
    ])?;
    let first = loopback.connected_config()?;
    let store = MemoryStore::default();
    let observed = store.clone();
    let handle = RemoteParticipantHandle::new(&first, store)?;
    enroll_and_receive(&handle)?;
    send_detach(&handle, token)?;
    let torn = observed.bytes()?;
    drop(handle);
    let second = loopback.connected_config()?;
    Ok((loopback, torn, second))
}

/// Consumes the crash testimony and puts the retained gen-1 detach back on the
/// wire, where the rotated broker is waiting to refuse it.
fn replay_into_rotated_broker(handle: &RemoteParticipantHandle<MemoryStore>) -> TestResult {
    assert!(matches!(
        handle.resolve_lost_operation_authority()?,
        super::RemoteLostOperationResolution::DetachParked { .. }
    ));
    assert!(matches!(
        handle.replay_detach()?,
        super::RemoteDetachReplayOutcome::Send(super::RemoteParticipantSendOutcome::Sent { .. })
    ));
    Ok(())
}

/// The rotated broker's answer: the presented generation is no longer live, and
/// the reply carries no detach outcome at all -- neither committed, nor pending,
/// nor a terminalized cell.
fn rotated_broker_refusal(token: DetachAttemptToken) -> Result<ServerValue, io::Error> {
    Ok(ServerValue::StaleAuthority(StaleAuthority::Detach(
        DetachStaleAuthority::Live {
            conversation_id: CONVERSATION,
            participant_id: PARTICIPANT,
            capability_generation: generation(1)?,
            detach_attempt_token: token,
            current_generation: generation(2)?,
        },
    )))
}

/// Fails only on the one error that means the handle's state is gone. Any other
/// typed failure is a refusal the caller can still act on.
fn alive<T>(result: &Result<T, super::RemoteParticipantError>, seam: &str) -> TestResult {
    if matches!(result, Err(super::RemoteParticipantError::StateUnavailable)) {
        return Err(io::Error::other(format!(
            "#59 REPRODUCED: {seam} reported destroyed state after a pure encode refusal"
        ))
        .into());
    }
    Ok(())
}

fn run_detach_terminal(value: ServerValue, token: DetachAttemptToken) -> TestResult {
    let loopback = Loopback::spawn(vec![vec![
        Action::Respond(vec![enroll_bound(CONVERSATION, [1; 16])?]),
        Action::Respond(vec![value]),
    ]])?;
    let config = loopback.connected_config()?;
    let store = MemoryStore::default();
    let observed = store.clone();
    let handle = RemoteParticipantHandle::new(&config, store)?;
    enroll_and_receive(&handle)?;
    send_detach(&handle, token)?;
    assert!(matches!(
        handle.receive()?,
        super::RemoteParticipantInbound::Applied { .. }
    ));
    assert_replay_inactive(&handle)?;
    let canonical = observed.bytes()?;
    loopback.finish()?;

    let restored = RemoteParticipantHandle::restore(&config, MemoryStore::default(), &canonical)?;
    assert_replay_inactive(&restored)?;
    Ok(())
}

fn enroll_and_receive(handle: &RemoteParticipantHandle<MemoryStore>) -> TestResult {
    enroll(handle)?;
    assert!(matches!(
        handle.receive()?,
        super::RemoteParticipantInbound::Applied { .. }
    ));
    Ok(())
}

fn send_detach(
    handle: &RemoteParticipantHandle<MemoryStore>,
    token: DetachAttemptToken,
) -> TestResult {
    let operation = recorded(
        handle.record_operation(ClientRequest::Detach(DetachRequest {
            conversation_id: CONVERSATION,
            participant_id: PARTICIPANT,
            capability_generation: generation(1)?,
            detach_attempt_token: token,
        }))?,
    )?;
    sent(&handle.send_operation(operation)?)?;
    Ok(())
}

fn assert_replay_inactive(handle: &RemoteParticipantHandle<MemoryStore>) -> TestResult {
    assert!(matches!(
        handle.replay_detach()?,
        super::RemoteDetachReplayOutcome::Refused {
            reason: liminal_protocol::client::DetachReplayRefusalReason::InvalidStatus,
        }
    ));
    Ok(())
}

fn terminalized_detach(token: DetachAttemptToken) -> Result<ServerValue, io::Error> {
    let mut body = Vec::new();
    put_u16(&mut body, ClientDiscriminant::DetachRequest.wire_value());
    put_u16(
        &mut body,
        AuthorityStateTag::TerminalizedDetachCell.wire_value(),
    );
    put_u64(&mut body, CONVERSATION);
    put_u64(&mut body, PARTICIPANT);
    put_u64(&mut body, generation(1)?.get());
    body.extend_from_slice(token.as_bytes());
    put_u64(&mut body, generation(2)?.get());
    put_u64(&mut body, 7);
    put_u64(&mut body, 8);
    put_u64(&mut body, generation(1)?.get());
    put_u16(&mut body, BindingStateTag::Detached.wire_value());
    decode_server_value_body(
        ServerDiscriminant::StaleAuthority,
        ProtocolVersion::V1,
        &body,
    )
    .map(|(value, _)| value)
    .map_err(|error| io::Error::other(format!("terminalized fixture decode failed: {error:?}")))
}

fn put_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_be_bytes());
}

#[derive(Debug, Default)]
struct FailSecondWrite {
    writes: usize,
}

impl ParticipantResumeStore for FailSecondWrite {
    fn persist(&mut self, canonical_lpcr: &[u8]) -> Result<(), SdkError> {
        self.writes += 1;
        if self.writes == 1 {
            assert!(!canonical_lpcr.is_empty());
            Ok(())
        } else {
            Err(SdkError::Store {
                description: "injected checkpoint failure".to_string(),
            })
        }
    }
}
