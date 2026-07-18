use std::io;
use std::sync::Arc;
use std::thread;

use liminal_protocol::wire::{
    AuthorityStateTag, BindingStateTag, ClientDiscriminant, ClientRequest, DetachAttemptToken,
    DetachCommitted, DetachInProgress, DetachRequest, LeaveAttemptToken, LeaveCommitted,
    ProtocolVersion, ServerDiscriminant, ServerValue, decode_server_value_body,
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
