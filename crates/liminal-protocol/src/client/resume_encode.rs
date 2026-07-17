use alloc::vec::Vec;

use super::{
    ClientBindingState, ClientParticipantAggregate, DetachReplayStatus, DetachReplayTerminal,
    ExpectedOperationState, LostAuthorityKind, LostAuthorityTestimony, ReconnectAggregate,
    ReconnectFreshEvent, RestoredExpectedOperationAbandonment,
    reconnect::ReconnectMachineState,
    replay::DetachReplayState,
    resume::{
        ClientResumeRecordEncodeError, ClientResumeRecordSection, HEADER_LEN, MAGIC, VERSION,
    },
};
use crate::wire::{
    BindingEpoch, DetachStaleAuthority, Generation, ParticipantFrame, ServerValue, StaleAuthority,
    encode, encoded_len,
};

pub(super) fn encode_aggregate(
    aggregate: &ClientParticipantAggregate,
) -> Result<Vec<u8>, ClientResumeRecordEncodeError> {
    let mut payload = Vec::new();
    encode_binding(&aggregate.binding, &mut payload);
    put_u64(&mut payload, aggregate.next_operation_authorization);
    encode_expected(aggregate.expected.as_ref(), &mut payload)?;
    encode_replay(&aggregate.detach_replay.state, &mut payload)?;
    encode_reconnect(&aggregate.reconnect, &mut payload);
    encode_abandonment(aggregate.restored_abandonment.as_ref(), &mut payload)?;
    let payload_len =
        u64::try_from(payload.len()).map_err(|_| ClientResumeRecordEncodeError::LengthOverflow)?;
    let mut output = Vec::with_capacity(HEADER_LEN.saturating_add(payload.len()));
    output.extend_from_slice(&MAGIC);
    put_u16(&mut output, VERSION);
    put_u64(&mut output, payload_len);
    output.extend_from_slice(&payload);
    Ok(output)
}

fn encode_binding(binding: &ClientBindingState, output: &mut Vec<u8>) {
    match binding {
        ClientBindingState::Unbound => output.push(0),
        ClientBindingState::Bound {
            conversation_id,
            participant_id,
            generation,
            attach_secret,
            binding_epoch,
        } => {
            output.push(1);
            put_u64(output, *conversation_id);
            put_u64(output, *participant_id);
            put_u64(output, generation.get());
            output.extend_from_slice(attach_secret.as_bytes());
            encode_epoch(*binding_epoch, output);
        }
        ClientBindingState::Detached {
            conversation_id,
            participant_id,
            generation,
            attach_secret,
        } => {
            encode_terminal_binding(2, *conversation_id, *participant_id, *generation, output);
            output.extend_from_slice(attach_secret.as_bytes());
        }
        ClientBindingState::Left {
            conversation_id,
            participant_id,
            generation,
        } => encode_terminal_binding(3, *conversation_id, *participant_id, *generation, output),
    }
}

fn encode_terminal_binding(
    tag: u8,
    conversation_id: u64,
    participant_id: u64,
    generation: Generation,
    output: &mut Vec<u8>,
) {
    output.push(tag);
    put_u64(output, conversation_id);
    put_u64(output, participant_id);
    put_u64(output, generation.get());
}

fn encode_epoch(epoch: BindingEpoch, output: &mut Vec<u8>) {
    put_u64(output, epoch.connection_incarnation.server_incarnation);
    put_u64(output, epoch.connection_incarnation.connection_ordinal);
    put_u64(output, epoch.capability_generation.get());
}

fn encode_expected(
    expected: Option<&ExpectedOperationState>,
    output: &mut Vec<u8>,
) -> Result<(), ClientResumeRecordEncodeError> {
    let Some(request) = expected else {
        output.push(0);
        return Ok(());
    };
    output.push(1);
    output.push(u8::from(request.issued));
    put_u64(output, request.authorization);
    encode_testimony(request.lost.as_ref(), output);
    let frame = encode_frame(
        &ParticipantFrame::ClientRequest(request.request.clone()),
        ClientResumeRecordSection::ExpectedOperation,
    )?;
    put_blob(output, &frame)?;
    Ok(())
}

/// Serializes a pending lost-authority testimony so that no encode path can
/// silently drop it: the bytes carry the atom (r2, 2026-07-18).
fn encode_testimony(testimony: Option<&LostAuthorityTestimony>, output: &mut Vec<u8>) {
    output.push(match testimony.map(LostAuthorityTestimony::kind) {
        None => 0,
        Some(LostAuthorityKind::IssuedOperationCorrelation) => 1,
        Some(LostAuthorityKind::DetachTransportAttempt) => 2,
        Some(LostAuthorityKind::ReconnectPermit) => 3,
        Some(LostAuthorityKind::ReconnectAttempt) => 4,
    });
}

/// Serializes the pending tokenless abandonment so it survives
/// encode-without-take (r2, 2026-07-18).
fn encode_abandonment(
    abandonment: Option<&RestoredExpectedOperationAbandonment>,
    output: &mut Vec<u8>,
) -> Result<(), ClientResumeRecordEncodeError> {
    let Some(abandonment) = abandonment else {
        output.push(0);
        return Ok(());
    };
    output.push(1);
    output.push(match abandonment.reason {
        super::RestoredExpectedOperationAbandonmentReason::TokenlessAfterCrash => 0,
    });
    output.push(u8::from(abandonment.was_issued));
    let frame = encode_frame(
        &ParticipantFrame::ClientRequest(abandonment.request.clone()),
        ClientResumeRecordSection::Abandonment,
    )?;
    put_blob(output, &frame)?;
    Ok(())
}

fn encode_replay(
    replay: &DetachReplayState,
    output: &mut Vec<u8>,
) -> Result<(), ClientResumeRecordEncodeError> {
    let DetachReplayState::Recorded { request, status } = replay else {
        output.push(0);
        return Ok(());
    };
    let (tag, terminal) = match status {
        DetachReplayStatus::Parked => (1, None),
        DetachReplayStatus::InFlight => (2, None),
        DetachReplayStatus::Superseded => (3, None),
        DetachReplayStatus::LeaveSuperseded => (4, None),
        DetachReplayStatus::Terminal(DetachReplayTerminal::DetachCommitted(value)) => {
            (5, Some(ServerValue::DetachCommitted(value.clone())))
        }
        DetachReplayStatus::Terminal(DetachReplayTerminal::DetachInProgress(value)) => {
            (6, Some(ServerValue::DetachInProgress(value.clone())))
        }
        DetachReplayStatus::Terminal(DetachReplayTerminal::TerminalizedDetachCell(value)) => (
            7,
            Some(ServerValue::StaleAuthority(StaleAuthority::Detach(
                DetachStaleAuthority::TerminalizedDetachCell(value.clone()),
            ))),
        ),
    };
    output.push(tag);
    put_u64(output, request.conversation_id);
    put_u64(output, request.participant_id);
    put_u64(output, request.capability_generation.get());
    output.extend_from_slice(request.detach_attempt_token.as_bytes());
    if let Some(value) = terminal {
        let frame = encode_frame(
            &ParticipantFrame::ServerValue(value),
            ClientResumeRecordSection::DetachReplay,
        )?;
        put_blob(output, &frame)?;
    }
    Ok(())
}

fn encode_reconnect(reconnect: &ReconnectAggregate, output: &mut Vec<u8>) {
    put_u64(output, reconnect.next_authorization);
    match reconnect.state {
        ReconnectMachineState::Parked => output.push(0),
        ReconnectMachineState::Permit {
            authorization,
            event,
            issued,
        } => {
            output.push(1);
            put_u64(output, authorization);
            encode_event(event, output);
            output.push(u8::from(issued));
        }
        ReconnectMachineState::Attempt {
            authorization,
            event,
        } => {
            output.push(2);
            put_u64(output, authorization);
            encode_event(event, output);
        }
        ReconnectMachineState::Online => output.push(3),
    }
    encode_testimony(reconnect.lost.as_ref(), output);
}

fn encode_event(event: ReconnectFreshEvent, output: &mut Vec<u8>) {
    output.push(match event {
        ReconnectFreshEvent::TransportFate(_) => 0,
        ReconnectFreshEvent::OnlineTransition(_) => 1,
        ReconnectFreshEvent::ExplicitCallerAction(_) => 2,
    });
}

fn encode_frame(
    frame: &ParticipantFrame,
    section: ClientResumeRecordSection,
) -> Result<Vec<u8>, ClientResumeRecordEncodeError> {
    let length = encoded_len(frame)
        .map_err(|source| ClientResumeRecordEncodeError::NestedCodec { section, source })?;
    let mut bytes = alloc::vec![0; length];
    let written = encode(frame, &mut bytes)
        .map_err(|source| ClientResumeRecordEncodeError::NestedCodec { section, source })?;
    bytes.truncate(written);
    Ok(bytes)
}

fn put_blob(output: &mut Vec<u8>, bytes: &[u8]) -> Result<(), ClientResumeRecordEncodeError> {
    let length =
        u64::try_from(bytes.len()).map_err(|_| ClientResumeRecordEncodeError::LengthOverflow)?;
    put_u64(output, length);
    output.extend_from_slice(bytes);
    Ok(())
}

fn put_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_be_bytes());
}
