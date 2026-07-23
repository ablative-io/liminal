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

use liminal_protocol::lifecycle::{BindingState, PendingFinalization};
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
