//! Crash-restore residence robustness.
//!
//! During the crash-restore window a participant slot can rest in
//! `PendingFinalization`: its `Died` source row is durable but the specific
//! fate finalizer never appended before the unclean stop. The restored
//! binding terminal is the earliest frontier candidate, so a valid publish
//! from a still-bound peer drives `DrainFirst` into the binding terminal and
//! must be answered with a typed refusal on the wire — never by tearing the
//! dispatch (`DrainFirst selected a binding terminal instead of marker work`).

use std::error::Error;
use std::sync::Arc;

use liminal_protocol::lifecycle::{
    BindingState, ClosureState, ImmutableSequenceCandidate, PendingFinalization,
};
use liminal_protocol::wire::{
    ClientRequest, EnrollmentRequest, EnrollmentToken, Generation, RecordAdmission,
    RecordAdmissionAttemptToken, ServerValue,
};

use crate::server::participant::ParticipantConnectionConversations;

use super::ProductionParticipantHandler;
use super::tests::{dispatch_tracked, test_participant_config};
use super::tests_w1b_pending_died_restart::{PendingRestartFixture, pending_restart_fixture};

/// Cold-restarts the fixture's durable bytes into a fresh production handler,
/// exactly as an unclean server restart replays durable truth on first touch.
///
/// The write-side fixture bound `max_retained_record_rows = 4`; the restarted
/// configuration must present the same shape for replay audits to hold.
fn restarted_handler(
    fixture: &PendingRestartFixture,
) -> Result<ProductionParticipantHandler, Box<dyn Error>> {
    let mut config = test_participant_config();
    config.max_retained_record_rows = 4;
    Ok(ProductionParticipantHandler::new(
        Arc::clone(&fixture.handler.store),
        config,
    )?)
}

#[test]
#[ignore = "fail-first RED pin held at conflict-STOP: the ruled typed-refusal shape has \
no truthful outcome in the frozen R-D1 register for this prestate (closure Clear, no \
observers — every refusal body would be fabricated), while PARTICIPANT-CONTRACT R-A2 \
prescribes a candidate-lane terminal drain instead; awaiting the tear seat's re-ruling"]
fn valid_publish_during_pending_finalization_residence_is_typed_refused()
-> Result<(), Box<dyn Error>> {
    let fixture = pending_restart_fixture()?;
    let restarted = restarted_handler(&fixture)?;

    // Crash-restore residence: cold replay rests the victim in Pending Died.
    let replayed = restarted.replay_aggregate_reference(fixture.conversation_id, &fixture.log)?;
    let victim = replayed
        .slots
        .get(&fixture.participant_id)
        .ok_or("restore omitted the pending participant")?;
    if !matches!(
        victim.binding,
        BindingState::PendingFinalization(PendingFinalization::Died(_))
    ) {
        return Err("restore did not rest the victim in PendingFinalization(Died)".into());
    }

    // A VALID publish from the still-bound peer names the pending participant
    // as a conversation recipient. Ruled behavior: a typed refusal frame
    // reaches the wire; the dispatch must not fail closed.
    let mut peer_conversations = ParticipantConnectionConversations::default();
    let refused = dispatch_tracked(
        &restarted,
        fixture.peer_connection,
        &mut peer_conversations,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: fixture.conversation_id,
            participant_id: fixture.peer_participant_id,
            capability_generation: Generation::ONE,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([0xA7; 16]),
            payload: vec![0xB1, 0xB2],
        }),
    )?;
    if matches!(refused, ServerValue::RecordCommitted(_)) {
        return Err(format!("residence publish must be refused, not committed: {refused:?}").into());
    }

    // The refusal left the server serving: an unrelated enrollment still binds.
    let unrelated_conversation = fixture
        .conversation_id
        .checked_add(1)
        .ok_or("unrelated conversation id overflowed")?;
    let mut fresh_conversations = ParticipantConnectionConversations::default();
    let enrolled = dispatch_tracked(
        &restarted,
        fixture.peer_connection,
        &mut fresh_conversations,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: unrelated_conversation,
            enrollment_token: EnrollmentToken::new([0xA8; 16]),
        }),
    )?;
    if !matches!(enrolled, ServerValue::EnrollBound(_)) {
        return Err(format!("post-refusal enrollment did not bind: {enrolled:?}").into());
    }
    Ok(())
}

/// Reachability census for the residence tear (facts the refusal design must
/// answer to):
///
/// - the restored frontier's SOLE immutable candidate is the victim's pending
///   `BindingTerminal`, so `DrainFirst` selects it ahead of any marker work
///   and the tear fires upstream of `produced()`; and
/// - the restored closure accounting is `Clear`, so every closure-family or
///   observer-backpressure refusal body in the frozen R-D1 register would be
///   fabricated for this prestate — the contract's total simulation admits no
///   truthful refusal here.
#[test]
fn residence_frontier_census_sole_terminal_candidate_closure_clear()
-> Result<(), Box<dyn Error>> {
    let fixture = pending_restart_fixture()?;
    let restarted = restarted_handler(&fixture)?;
    let mut replayed =
        restarted.replay_aggregate_reference(fixture.conversation_id, &fixture.log)?;
    let owner = replayed.take_frontier()?;
    let candidates = owner.frontiers().sequence().immutable_candidates().to_vec();
    let (_, accounting, _, _) = owner.into_parts();
    let [sole] = candidates.as_slice() else {
        return Err(format!("residence frontier candidates drifted: {candidates:?}").into());
    };
    let ImmutableSequenceCandidate::BindingTerminal { owner: terminal, .. } = sole else {
        return Err(format!("residence sole candidate is not a binding terminal: {sole:?}").into());
    };
    if terminal.participant_index != fixture.participant_id {
        return Err(format!(
            "residence terminal owner {} is not the pending participant {}",
            terminal.participant_index, fixture.participant_id
        )
        .into());
    }
    if terminal.binding_epoch != fixture.binding_epoch {
        return Err("residence terminal epoch is not the victim's dead binding epoch".into());
    }
    if !matches!(accounting.state(), ClosureState::Clear) {
        return Err(format!(
            "residence closure accounting drifted from Clear: {:?}",
            accounting.state()
        )
        .into());
    }
    Ok(())
}
