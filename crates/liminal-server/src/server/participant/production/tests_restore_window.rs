//! Crash-restore residence robustness.
//!
//! During the crash-restore window a participant slot can rest in
//! `PendingFinalization`: its `Died` source row is durable but the specific
//! fate finalizer never appended before the unclean stop. The restored
//! binding terminal is the earliest frontier candidate, so a valid publish
//! from a still-bound peer drives `DrainFirst` into the binding terminal.
//! Ruled behavior (PENDING-DRAIN-EMITTER r3, rows S-1/S-2): the server drains
//! that terminal per PARTICIPANT-CONTRACT R-A2 as one durable candidate
//! transaction — terminal-record append, retention transition, candidate
//! deletion, binding-slot release — and the publish then commits.

use std::error::Error;
use std::sync::Arc;

use liminal::durability::bridge::block_on;
use liminal_protocol::lifecycle::{
    BindingState, ClosureState, ImmutableSequenceCandidate, PendingFinalization,
};
use liminal_protocol::wire::{
    ClientRequest, EnrollmentRequest, EnrollmentToken, Generation, RecordAdmission,
    RecordAdmissionAttemptToken, ServerValue,
};

use crate::server::participant::ParticipantConnectionConversations;

use super::ProductionParticipantHandler;
use super::log::{
    DecodedStoredOperation, StoredOperation, StoredTerminalDisposition,
};
use super::outbox_log::{OutboxLog, OutboxRow, ProducedSourceKind};
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

/// Reads every durable operation row at and after `from_sequence`.
fn rows_from(
    fixture: &PendingRestartFixture,
    from_sequence: u64,
) -> Result<Vec<StoredOperation>, Box<dyn Error>> {
    let mut rows = Vec::new();
    let mut sequence = from_sequence;
    while let Some(entry) = block_on(fixture.log.read_at(sequence))?? {
        let DecodedStoredOperation::V3(operation) = entry.operation else {
            return Err(format!("durable row {sequence} is not schema v3").into());
        };
        rows.push(operation);
        sequence = sequence
            .checked_add(1)
            .ok_or("durable log sequence overflowed")?;
    }
    Ok(rows)
}

/// The S-2 redrawn pin: a valid publish that encounters the crash-restored
/// `PendingFinalization(Died)` residence COMMITS after the candidate-lane
/// terminal drain — not a refusal, not a torn connection.
#[test]
fn valid_publish_during_pending_finalization_residence_commits_after_drain()
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

    // (1) A VALID publish from the still-bound peer commits; the dispatch does
    // not fail closed and no refusal is selected.
    let mut peer_conversations = ParticipantConnectionConversations::default();
    let committed = dispatch_tracked(
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
    let ServerValue::RecordCommitted(record) = committed else {
        return Err(format!("residence publish did not commit: {committed:?}").into());
    };

    // (2) Exactly one Died terminal record was appended for the victim, with
    // a committed disposition carrying the drained terminal sequence.
    let appended = rows_from(&fixture, fixture.specific_sequence)?;
    let drained: Vec<_> = appended
        .iter()
        .filter_map(|operation| match operation {
            StoredOperation::Died { row } if row.participant_id == fixture.participant_id => {
                Some(row)
            }
            _ => None,
        })
        .collect();
    let [drain_row] = drained.as_slice() else {
        return Err(format!(
            "expected exactly one drained Died terminal row, found {}",
            drained.len()
        )
        .into());
    };
    let StoredTerminalDisposition::Committed { terminal_seq } = drain_row.disposition else {
        return Err("drained Died terminal row is not committed".into());
    };
    if drain_row.terminal_order != fixture.terminal_order {
        return Err("drained Died terminal row lost its pinned admission order".into());
    }
    if record.delivery_seq() <= terminal_seq {
        return Err("publish did not commit strictly after the drained terminal".into());
    }

    // (2, continued) The candidate is deleted and the binding slot released.
    let drained_state =
        restarted.replay_aggregate_reference(fixture.conversation_id, &fixture.log)?;
    if drained_state.slots.contains_key(&fixture.participant_id) {
        return Err("drain did not release the victim's binding slot".into());
    }
    let candidates = drained_state
        .frontier()
        .ok_or("post-drain replay lost its frontier")?
        .frontiers()
        .sequence()
        .immutable_candidates()
        .to_vec();
    if !candidates.is_empty() {
        return Err(format!("post-drain replay kept residual candidates: {candidates:?}").into());
    }

    // (3) The committed record's recipient set EXCLUDES the drained victim: in
    // this pinned prestate the victim was the sole other participant, so every
    // post-drain produced batch commits with an empty recipient set.
    let outbox = OutboxLog::new(Arc::clone(&fixture.handler.store), fixture.conversation_id);
    let produced_after_drain: Vec<_> = block_on(outbox.read_all())??
        .into_iter()
        .filter_map(|(_, row)| match row {
            OutboxRow::Produced(batch)
                if batch.source_log_sequence() >= fixture.specific_sequence =>
            {
                Some(batch)
            }
            _ => None,
        })
        .collect();
    let publish_batch = produced_after_drain
        .iter()
        .find(|batch| batch.source_kind() == ProducedSourceKind::RecordAdmission)
        .ok_or("committed publish produced no outbox batch")?;
    for projected in publish_batch.ordered_records() {
        if !projected.recipients().is_empty() {
            return Err(format!(
                "post-drain publish batch named recipients: {:?}",
                projected.recipients()
            )
            .into());
        }
        if projected.recipients().contains(&fixture.participant_id) {
            return Err("post-drain publish batch named the drained victim".into());
        }
    }

    // (4) Post-commit durable replay shows no residual candidate (asserted
    // above) and a REPEAT publish commits with no drain work: exactly one new
    // durable row, and no new Died terminal rows.
    let rows_before_repeat = rows_from(&fixture, fixture.specific_sequence)?.len();
    let repeated = dispatch_tracked(
        &restarted,
        fixture.peer_connection,
        &mut peer_conversations,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: fixture.conversation_id,
            participant_id: fixture.peer_participant_id,
            capability_generation: Generation::ONE,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([0xA8; 16]),
            payload: vec![0xB3],
        }),
    )?;
    if !matches!(repeated, ServerValue::RecordCommitted(_)) {
        return Err(format!("repeat publish after drain did not commit: {repeated:?}").into());
    }
    let rows_after_repeat = rows_from(&fixture, fixture.specific_sequence)?;
    if rows_after_repeat.len() != rows_before_repeat.checked_add(1).ok_or("row count overflow")? {
        return Err(format!(
            "repeat publish appended {} rows, expected exactly one",
            rows_after_repeat.len().saturating_sub(rows_before_repeat)
        )
        .into());
    }
    if !matches!(
        rows_after_repeat.last(),
        Some(StoredOperation::RecordAdmission { .. })
    ) {
        return Err("repeat publish's sole appended row is not a RecordAdmission".into());
    }

    // The drain left the server serving: an unrelated enrollment still binds.
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
            enrollment_token: EnrollmentToken::new([0xA9; 16]),
        }),
    )?;
    if !matches!(enrolled, ServerValue::EnrollBound(_)) {
        return Err(format!("post-drain enrollment did not bind: {enrolled:?}").into());
    }
    Ok(())
}

/// Reachability census for the residence drain (facts the drain design
/// answers to):
///
/// - the restored frontier's SOLE immutable candidate is the victim's pending
///   `BindingTerminal`, so `DrainFirst` selects it ahead of any marker work;
///   and
/// - the restored closure accounting is `Clear`, so every closure-family or
///   observer-backpressure refusal body in the frozen R-D1 register would be
///   fabricated for this prestate — the contract's total simulation admits no
///   truthful refusal here.
#[test]
fn residence_frontier_census_sole_terminal_candidate_closure_clear() -> Result<(), Box<dyn Error>> {
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
    let ImmutableSequenceCandidate::BindingTerminal {
        owner: terminal, ..
    } = sole
    else {
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
