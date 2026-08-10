//! Pins for the correlated-refusal decoupling door (P0 #59, leg 1).
//!
//! `decide_inbound_inner` clears `aggregate.expected` unconditionally before
//! delegating to `apply_correlated_value`. The comment above that clear claimed
//! every matching arm supersedes or terminalizes the detach replay, so the pair
//! could never decouple. That claim was false.
//!
//! A correlated value reaches the clear only if its own wire identity resolves
//! to the expected request's key, so the values that can strand a detach replay
//! are exactly those whose `response_key` is `RequestKey::Detach`. Three of them
//! answer the detach (committed, in-progress, terminalized cell) and settle the
//! replay. The rest refuse the detach's AUTHORITY without answering it, fall
//! into the do-nothing catch-all arm, and leave the replay `Recorded`/active
//! behind a cleared expected slot -- a shape `resume_record` then refuses with
//! `DecoupledDetachReplay` and no restore accepts.
//!
//! The census below is derived from the wire envelope types, not from a field
//! trace. Two corrections came out of that:
//!
//! * `ConversationOrderExhausted` cannot reach a detach at all. It carries an
//!   `OrderAllocatingEnvelope`, whose only variants are `Enrollment`,
//!   `CredentialAttach`, and `RecordAdmission`. A trace-derived list named it.
//! * `ObserverBackpressure::Detach` DOES reach a detach, carries a
//!   `DetachEnvelope`, and sits in the same catch-all. No trace named it.
//!
//! That asymmetry is the argument for the fix's shape: the settlement observes
//! whether the replay is still active rather than enumerating the arms that
//! leave it so. An enumeration is exactly what was wrong here, twice.

use alloc::vec;
use alloc::vec::Vec;

use super::gen_skip_supersession_tests::{
    TestResult, bound_at, expected_exact_detach, generation, replay_envelope,
};
use super::*;
use crate::wire::{
    BindingRequiredEnvelope, ConnectionConversationCapacityExceeded, DetachCommitted,
    DetachStaleAuthority, NoBinding, ObserverBackpressure, ObserverBackpressureState,
    ParticipantReferenceEnvelope, ParticipantUnknown, ResponseEnvelope, Retired, ServerValue,
    StaleAuthority,
};

const REPLAY_GENERATION: u64 = 3;
const REPLAY_TOKEN: u8 = 0x91;

/// The census, as code: every server value whose wire identity resolves to the
/// retained detach's request key and which refuses that detach's authority
/// instead of answering it.
///
/// `Retired` is included at a generation BELOW the replay's on purpose. That is
/// the only retirement `apply_retired` declines to apply, so it is the only one
/// that reaches the catch-all; a retirement at or above the replay's generation
/// supersedes it and is pinned separately as a negative control.
fn correlated_refusals_of_the_replayed_detach() -> TestResult<Vec<(&'static str, ServerValue)>> {
    let request = replay_envelope(REPLAY_GENERATION, REPLAY_TOKEN)?;
    Ok(vec![
        (
            "StaleAuthority::Detach(Live)",
            ServerValue::StaleAuthority(StaleAuthority::Detach(DetachStaleAuthority::Live {
                conversation_id: request.conversation_id,
                participant_id: request.participant_id,
                capability_generation: request.capability_generation,
                detach_attempt_token: request.detach_attempt_token,
                current_generation: generation(REPLAY_GENERATION + 1)?,
            })),
        ),
        (
            "NoBinding(Detach)",
            ServerValue::NoBinding(NoBinding {
                request: BindingRequiredEnvelope::Detach(request.clone()),
            }),
        ),
        (
            "ParticipantUnknown(Detach)",
            ServerValue::ParticipantUnknown(ParticipantUnknown {
                request: ParticipantReferenceEnvelope::Detach(request.clone()),
            }),
        ),
        (
            "ConnectionConversationCapacityExceeded(SemanticRequest{Detach})",
            ServerValue::ConnectionConversationCapacityExceeded(
                ConnectionConversationCapacityExceeded::SemanticRequest {
                    request: ResponseEnvelope::Detach(request.clone()),
                    limit: 4,
                },
            ),
        ),
        (
            "ObserverBackpressure::Detach",
            ServerValue::ObserverBackpressure(ObserverBackpressure::Detach {
                request: request.clone(),
                committed_binding_epoch: super::gen_skip_supersession_tests::epoch(
                    REPLAY_GENERATION,
                )?,
                state: ObserverBackpressureState::initial(9),
            }),
        ),
        (
            "Retired(Participant{Detach}) below the replay generation",
            ServerValue::Retired(Retired::Participant {
                request: ParticipantReferenceEnvelope::Detach(request),
                retired_generation: generation(REPLAY_GENERATION - 1)?,
            }),
        ),
    ])
}

/// The exact coupled torn state: an issued gen-3 detach in flight, its replay
/// slot holding the same envelope. This is what a node has on the wire when it
/// replays a detach whose generation the broker has already rotated past.
fn coupled_in_flight_detach() -> TestResult<ClientParticipantAggregate> {
    let mut aggregate = bound_at(REPLAY_GENERATION)?;
    aggregate.expected = Some(expected_exact_detach(REPLAY_GENERATION, REPLAY_TOKEN)?);
    aggregate.next_operation_authorization = 1;
    aggregate.detach_replay.state = replay::DetachReplayState::Recorded {
        request: replay_envelope(REPLAY_GENERATION, REPLAY_TOKEN)?,
        status: DetachReplayStatus::InFlight,
    };
    Ok(aggregate)
}

fn deliver(value: ServerValue) -> TestResult<ClientParticipantAggregate> {
    let correlation = ClientResponseCorrelation { authorization: 1 };
    let ClientCorrelatedInboundDecision::Applied(applied) =
        decide_correlated_inbound(coupled_in_flight_detach()?, value, correlation)
    else {
        return Err("a value carrying the exact detach's wire identity must correlate");
    };
    let (aggregate, _) = applied.into_parts();
    Ok(aggregate)
}

/// RED AT 8c8adec: every correlated refusal of the replayed detach cleared the
/// expected slot and left the replay active, minting an aggregate that will not
/// encode and a record no restore accepts.
#[test]
fn every_correlated_refusal_settles_the_replay_and_stays_persistable() -> TestResult {
    for (name, value) in correlated_refusals_of_the_replayed_detach()? {
        let aggregate = deliver(value)?;
        assert!(
            aggregate.expected.is_none(),
            "{name}: the correlated response must retire the expected slot"
        );
        assert!(
            !matches!(
                aggregate.detach_replay.status(),
                Some(DetachReplayStatus::Parked | DetachReplayStatus::InFlight)
            ),
            "#59 REPRODUCED: {name} cleared the expected detach and left the replay active"
        );
        let Ok(record) = aggregate.resume_record() else {
            panic!("#59 REPRODUCED: {name} minted an aggregate that refuses to encode");
        };
        assert!(
            record.restore().is_ok(),
            "#59 REPRODUCED: {name} minted a record that no restore accepts"
        );
    }
    Ok(())
}

/// RED AT 8c8adec: the settled replay must say WHAT refused it, losslessly, and
/// that testimony must survive the canonical round trip.
///
/// A classification would be a projection. The record already nests a canonical
/// wire frame for its other terminals, so the exact refusing value costs nothing
/// to retain and leaves the consumer able to distinguish a rotated generation
/// from a dropped binding from observer backpressure.
#[test]
fn the_settled_terminal_retains_the_exact_refusing_value() -> TestResult {
    for (name, value) in correlated_refusals_of_the_replayed_detach()? {
        let aggregate = deliver(value.clone())?;
        let Some(DetachReplayStatus::Terminal(DetachReplayTerminal::AuthorityRefused(refused))) =
            aggregate.detach_replay.status()
        else {
            panic!("#59 REPRODUCED: {name} did not settle into a typed authority refusal");
        };
        assert_eq!(
            refused.value(),
            &value,
            "{name}: the retained refusal must be the exact value, not a projection"
        );
        let restored = aggregate
            .resume_record()
            .map_err(|_| "a settled replay must encode")?
            .restore()
            .map_err(|_| "a settled replay must restore")?;
        let Some(DetachReplayStatus::Terminal(DetachReplayTerminal::AuthorityRefused(refused))) =
            restored.detach_replay.status()
        else {
            panic!("{name}: the typed refusal must survive the canonical round trip");
        };
        assert_eq!(
            refused.value(),
            &value,
            "{name}: the round trip must not project the retained refusal"
        );
    }
    Ok(())
}

/// Negative control: a value that ANSWERS the detach keeps its own terminal.
/// The settlement must only fill the gap the answering arms leave, never
/// overwrite what they decided.
#[test]
fn an_answered_detach_keeps_its_own_terminal() -> TestResult {
    let request = replay_envelope(REPLAY_GENERATION, REPLAY_TOKEN)?;
    let committed = DetachCommitted::new(
        request.conversation_id,
        request.participant_id,
        request.detach_attempt_token,
        super::gen_skip_supersession_tests::epoch(REPLAY_GENERATION)?,
        13,
    );
    let aggregate = deliver(ServerValue::DetachCommitted(committed.clone()))?;
    assert!(matches!(
        aggregate.detach_replay.status(),
        Some(DetachReplayStatus::Terminal(
            DetachReplayTerminal::DetachCommitted(_)
        ))
    ));
    Ok(())
}

/// Negative control: a retirement AT OR ABOVE the replayed generation still
/// supersedes it. Only the retirement below the replay -- the one
/// `apply_retired` declines -- may reach the new settlement.
#[test]
fn a_retirement_above_the_replay_still_supersedes_it() -> TestResult {
    let request = replay_envelope(REPLAY_GENERATION, REPLAY_TOKEN)?;
    let aggregate = deliver(ServerValue::Retired(Retired::Participant {
        request: ParticipantReferenceEnvelope::Detach(request),
        retired_generation: generation(REPLAY_GENERATION)?,
    }))?;
    assert!(matches!(
        aggregate.detach_replay.status(),
        Some(DetachReplayStatus::LeaveSuperseded)
    ));
    Ok(())
}

/// Negative control: an UNCOUPLED replay is not the settlement's business. A
/// refusal naming one detach must never settle a replay holding a different
/// one -- that would launder a decoupling the fix exists to prevent.
#[test]
fn a_refusal_never_settles_a_replay_it_does_not_name() -> TestResult {
    let mut aggregate = bound_at(REPLAY_GENERATION)?;
    aggregate.expected = Some(expected_exact_detach(REPLAY_GENERATION, REPLAY_TOKEN)?);
    aggregate.next_operation_authorization = 1;
    // A replay holding a DIFFERENT token than the expected detach.
    aggregate.detach_replay.state = replay::DetachReplayState::Recorded {
        request: replay_envelope(REPLAY_GENERATION, 0x77)?,
        status: DetachReplayStatus::InFlight,
    };
    let correlation = ClientResponseCorrelation { authorization: 1 };
    let value = ServerValue::NoBinding(NoBinding {
        request: BindingRequiredEnvelope::Detach(replay_envelope(REPLAY_GENERATION, REPLAY_TOKEN)?),
    });
    let ClientCorrelatedInboundDecision::Applied(applied) =
        decide_correlated_inbound(aggregate, value, correlation)
    else {
        return Err("the refusal still names the expected detach and must correlate");
    };
    let (aggregate, _) = applied.into_parts();
    assert!(
        matches!(
            aggregate.detach_replay.status(),
            Some(DetachReplayStatus::InFlight)
        ),
        "a refusal must not settle a replay whose retained detach it does not name"
    );
    Ok(())
}
