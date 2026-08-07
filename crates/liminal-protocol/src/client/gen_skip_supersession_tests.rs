//! Pins for the generation-skip stranding cluster (field 2026-08-07).
//!
//! The field shape: a detach replay Recorded/InFlight at generation 3 with the
//! expected slot ENTIRELY ABSENT, while the participant's real credential had
//! reached generation 5 through a request-4-granted-5 attach. The old
//! supersession predicate required `attach.request_generation() ==
//! replay.capability_generation`, so a generation-skipping attach never
//! superseded: the replay stayed active forever, `can_replace_with` refused
//! every new detach over it (`DetachReplayIncompatible`), and the persisted
//! aggregate was a record every restore refuses.
//!
//! Three pins were RED at the parent of the fix commit: the aggregate-door
//! supersession, the correlated-inbound-door arc, and the encode refusal.

use super::*;
use crate::wire::{
    AttachAttemptToken, AttachSecret, BindingEpoch, ClientRequest, ConnectionIncarnation,
    CredentialAttachRequest, DetachAttemptToken, DetachEnvelope, DetachRequest, Generation,
    ServerValue,
};

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

fn bound_at(generation_value: u64) -> TestResult<ClientParticipantAggregate> {
    let mut aggregate = ClientParticipantAggregate::new();
    aggregate.binding = ClientBindingState::Bound {
        conversation_id: 141,
        participant_id: 142,
        generation: generation(generation_value)?,
        attach_secret: AttachSecret::new([143; 32]),
        binding_epoch: epoch(generation_value)?,
    };
    Ok(aggregate)
}

fn replay_envelope(generation_value: u64, token: u8) -> TestResult<DetachEnvelope> {
    Ok(DetachEnvelope {
        conversation_id: 141,
        participant_id: 142,
        capability_generation: generation(generation_value)?,
        detach_attempt_token: DetachAttemptToken::new([token; 16]),
    })
}

fn expected_exact_detach(generation_value: u64, token: u8) -> TestResult<ExpectedOperationState> {
    Ok(ExpectedOperationState {
        request: ClientRequest::Detach(DetachRequest {
            conversation_id: 141,
            participant_id: 142,
            capability_generation: generation(generation_value)?,
            detach_attempt_token: DetachAttemptToken::new([token; 16]),
        }),
        issued: true,
        authorization: 1,
        lost: None,
    })
}

/// The exact field grant: requested at 4, granted 5, over a gen-3 replay.
fn skip_attach(token: u8, secret: u8) -> TestResult<crate::wire::AttachBound> {
    crate::wire::AttachBound::ordinary(
        141,
        AttachAttemptToken::new([token; 16]),
        142,
        generation(4)?,
        AttachSecret::new([secret; 32]),
        epoch(5)?,
        0,
        0,
        0,
    )
    .ok_or("skip attach must have successor generation")
}

/// RED AT PARENT: the aggregate-door supersession. A coupled in-flight gen-3
/// replay must be superseded by a request-4-granted-5 attach; the parent
/// refused it as `ForeignInput` and left the replay stranded active.
#[test]
fn generation_skipping_attach_supersedes_active_replay() -> TestResult {
    let mut aggregate = bound_at(3)?;
    aggregate.expected = Some(expected_exact_detach(3, 0x91)?);
    aggregate.next_operation_authorization = 1;
    aggregate.detach_replay.state = replay::DetachReplayState::Recorded {
        request: replay_envelope(3, 0x91)?,
        status: DetachReplayStatus::InFlight,
    };
    let correlation = ClientResponseCorrelation { authorization: 1 };
    let ApplyAttachDecision::Superseded(applied) =
        apply_attach(aggregate, skip_attach(0x92, 143)?, correlation)
    else {
        return Err("#43 REPRODUCED: generation-skipping attach refused, replay stranded active");
    };
    let aggregate = applied.into_aggregate();
    assert!(aggregate.expected.is_none());
    assert!(matches!(
        aggregate.detach_replay.state,
        replay::DetachReplayState::Recorded {
            status: DetachReplayStatus::Superseded,
            ..
        }
    ));
    aggregate
        .resume_record()
        .map_err(|_| "superseded aggregate must stay encodable")?;
    Ok(())
}

/// RED AT PARENT: the correlated-inbound door, end to end. The inbound clear
/// nulls `expected` and the skip-blind arm left the replay in-flight -- the
/// exact decoupled shape decoded from the field store. Now the arm supersedes,
/// the aggregate persists and restores, and a NEW gen-5 detach records: the
/// shutdown-wedge exit.
#[test]
fn inbound_skip_attach_supersedes_and_the_new_detach_records() -> TestResult {
    let mut aggregate = bound_at(4)?;
    aggregate.expected = Some(ExpectedOperationState {
        request: ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: 141,
            participant_id: 142,
            capability_generation: generation(4)?,
            attach_secret: AttachSecret::new([143; 32]),
            attach_attempt_token: AttachAttemptToken::new([0x93; 16]),
            accept_marker_delivery_seq: None,
        }),
        issued: true,
        authorization: 1,
        lost: None,
    });
    aggregate.next_operation_authorization = 1;
    aggregate.detach_replay.state = replay::DetachReplayState::Recorded {
        request: replay_envelope(3, 0x91)?,
        status: DetachReplayStatus::InFlight,
    };
    let correlation = ClientResponseCorrelation { authorization: 1 };
    let ClientCorrelatedInboundDecision::Applied(applied) = decide_correlated_inbound(
        aggregate,
        ServerValue::AttachBound(skip_attach(0x93, 143)?),
        correlation,
    ) else {
        return Err("matching skip attach must apply");
    };
    let (mut aggregate, _) = applied.into_parts();
    assert!(matches!(
        aggregate.detach_replay.state,
        replay::DetachReplayState::Recorded {
            status: DetachReplayStatus::Superseded,
            ..
        }
    ));
    let record = aggregate.resume_record().map_err(|_| {
        "#43 REPRODUCED: inbound consumption minted the decoupled poison aggregate"
    })?;
    record
        .restore()
        .map_err(|_| "superseded aggregate must restore")?;
    aggregate.next_operation_authorization = 1;
    let decision = record_operation(
        aggregate,
        ClientRequest::Detach(DetachRequest {
            conversation_id: 141,
            participant_id: 142,
            capability_generation: generation(5)?,
            detach_attempt_token: DetachAttemptToken::new([0x94; 16]),
        }),
    );
    let ClientOperationRecordDecision::Pending(_) = decision else {
        return Err("#43 REPRODUCED: the wedge -- a new detach cannot record over the replay");
    };
    Ok(())
}

/// RED AT PARENT: the write-side twin. The field-decoded shape (active replay,
/// expected absent) and its converse must refuse to ENCODE instead of minting
/// a record every restore refuses. The parent encoded both happily.
#[test]
fn decoupled_aggregate_refuses_to_encode_in_both_directions() -> TestResult {
    let mut stranded = bound_at(4)?;
    stranded.detach_replay.state = replay::DetachReplayState::Recorded {
        request: replay_envelope(3, 0x91)?,
        status: DetachReplayStatus::InFlight,
    };
    let Err(ClientResumeRecordEncodeError::DecoupledDetachReplay) = stranded.resume_record() else {
        return Err("#43 REPRODUCED: the field's poison shape encoded without refusal");
    };
    let mut converse = bound_at(3)?;
    converse.expected = Some(expected_exact_detach(3, 0x91)?);
    converse.next_operation_authorization = 1;
    let Err(ClientResumeRecordEncodeError::DecoupledDetachReplay) = converse.resume_record() else {
        return Err("expected detach without active replay must refuse to encode");
    };
    Ok(())
}

/// Negative control: the exact lawful coupling still encodes and restores.
#[test]
fn coupled_active_replay_still_encodes_and_restores() -> TestResult {
    let mut aggregate = bound_at(3)?;
    aggregate.expected = Some(expected_exact_detach(3, 0x91)?);
    aggregate.next_operation_authorization = 1;
    aggregate.detach_replay.state = replay::DetachReplayState::Recorded {
        request: replay_envelope(3, 0x91)?,
        status: DetachReplayStatus::InFlight,
    };
    aggregate
        .resume_record()
        .map_err(|_| "coupled aggregate must encode")?
        .restore()
        .map_err(|_| "coupled aggregate must restore")?;
    Ok(())
}
