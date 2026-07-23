//! Crash-restore residence robustness — the Detached flavor
//! (LIM-DETACHED-PENDING, PENDING-DRAIN-EMITTER §3A, S-16/S-17/S-18).
//!
//! During the crash-restore window a participant slot can rest in
//! `PendingFinalization(Detached(..))`: its Detached source row (a
//! ServerShutdown fate cut at the retention cap, or a blocked explicit
//! detach) is durable but no finalizer appended before the unclean stop.
//! The restored binding terminal is the earliest frontier candidate, so a
//! valid publish from a still-bound peer drives `DrainFirst` into it.
//! Ruled behavior (§3A.3, S-16): the drain is FAITHFUL DETACH FINALIZATION,
//! never Died-style erasure — one durable candidate transaction appending a
//! committed Detached terminal (cause preserved), settling the slot at
//! committed `BindingState::Detached` with slot AND enrollment token
//! PRESERVED, so the victim stays exact-secret resumable and becomes a
//! parked `produced` recipient of the encountering publish.

use std::cell::Cell;
use std::error::Error;
use std::sync::Arc;

use liminal::durability::bridge::block_on;
use liminal_protocol::lifecycle::{
    BindingState, ClosureState, ImmutableSequenceCandidate, PendingFinalization,
};
use liminal_protocol::wire::{
    AttachAttemptToken, ClientRequest, ConnectionIncarnation, CredentialAttachRequest, Generation,
    ParticipantAck, RecordAdmission, RecordAdmissionAttemptToken, ServerValue,
};

use crate::server::participant::{
    ConnectionFateClass, ConnectionFateWorkItem, ParticipantConnectionConversations,
};

use super::ProductionParticipantHandler;
use super::log::{
    DecodedStoredOperation, OperationLog, OperationLogError, StoredDetached, StoredDetachedCause,
    StoredDetachedSource, StoredFinalizerPresentation, StoredOperation, StoredTerminalDisposition,
};
use super::outbox_log::{OutboxLog, OutboxRow, ProducedSourceKind};
use super::state::DurableAppend;
use super::tests::{dispatch_tracked, test_participant_config};
use super::tests_w1b_pending_died_restart::bound_debt_fixture;

struct SourceOnlyAppender<'a> {
    log: &'a OperationLog,
    source_flushed: Cell<bool>,
}

impl DurableAppend for SourceOnlyAppender<'_> {
    fn append(
        &self,
        operation: &StoredOperation,
        expected_sequence: u64,
    ) -> Result<(), OperationLogError> {
        if self.source_flushed.replace(true) {
            return Err(OperationLogError::FateReplayDrift {
                sequence: expected_sequence,
            });
        }
        block_on(self.log.append(operation, expected_sequence))?
    }
}

pub(super) struct PendingDetachedRestartFixture {
    pub(super) handler: ProductionParticipantHandler,
    pub(super) log: OperationLog,
    pub(super) conversation_id: u64,
    pub(super) participant_id: u64,
    pub(super) peer_participant_id: u64,
    pub(super) peer_connection: ConnectionIncarnation,
    pub(super) binding_epoch: liminal_protocol::wire::BindingEpoch,
    pub(super) attach_secret: liminal_protocol::wire::AttachSecret,
    pub(super) detached_source_sequence: u64,
    pub(super) drain_sequence: u64,
    pub(super) terminal_order: u64,
}

/// Mints the crash-shaped prestate: the victim's ServerShutdown connection
/// fate pends its Detached terminal at the retention cap (source row durable,
/// no finalizer), the still-bound peer survives on its own connection.
pub(super) fn pending_detached_restart_fixture()
-> Result<PendingDetachedRestartFixture, Box<dyn Error>> {
    pending_detached_restart_fixture_with_acks(None, false)
}

/// Same prestate, with pre-crash cursors advanced (victim ack through
/// `victim_ack_through_seq`, peer ack through the victim's debt record) so
/// post-drain retention can release through the real floor mechanism — the
/// prune a later record admission performs over the fully-acked prefix.
pub(super) fn pending_detached_restart_fixture_with_acks(
    victim_ack_through_seq: Option<u64>,
    peer_acks_debt_record: bool,
) -> Result<PendingDetachedRestartFixture, Box<dyn Error>> {
    let setup = bound_debt_fixture(
        73,
        ConnectionIncarnation::new(103, 3),
        ConnectionIncarnation::new(103, 4),
        victim_ack_through_seq,
    )?;
    let log = OperationLog::new(Arc::clone(&setup.handler.store), setup.conversation_id);
    if peer_acks_debt_record {
        let mut sequence = 0_u64;
        let mut r0_seq = None;
        while let Some(entry) = block_on(log.read_at(sequence))?? {
            if let DecodedStoredOperation::V3(StoredOperation::RecordAdmission { row }) =
                entry.operation
            {
                if row.request.participant_id == setup.participant_id {
                    r0_seq = Some(row.delivery_seq);
                }
            }
            sequence = sequence
                .checked_add(1)
                .ok_or("durable log sequence overflowed")?;
        }
        let r0_seq = r0_seq.ok_or("victim debt record r0 is absent before the fate cut")?;
        let mut peer_conversations = ParticipantConnectionConversations::default();
        let acked = dispatch_tracked(
            &setup.handler,
            ConnectionIncarnation::new(103, 4),
            &mut peer_conversations,
            ClientRequest::ParticipantAck(ParticipantAck {
                conversation_id: setup.conversation_id,
                participant_id: setup.peer_participant_id,
                capability_generation: Generation::ONE,
                through_seq: r0_seq,
            }),
        )?;
        if !matches!(acked, ServerValue::AckCommitted(_)) {
            return Err(format!("peer pre-crash ack of r0 did not commit: {acked:?}").into());
        }
    }
    let cell = setup.handler.cell(setup.conversation_id)?;
    let (detached_source_sequence, terminal_order) = {
        let mut owner = cell
            .lock()
            .map_err(|_| "pending Detached restart owner lock was poisoned")?;
        let authority = owner
            .as_mut()
            .ok_or("pending Detached restart owner was unavailable")?;
        let source_sequence = authority.next_log_sequence;
        authority
            .prepare_connection_fate_transaction(&ConnectionFateWorkItem {
                open_sequence: 47,
                connection_incarnation: setup.connection,
                class: ConnectionFateClass::ServerShutdown,
                tracked_conversations: setup.conversations.tracked_conversations(),
            })
            .complete(
                authority,
                &SourceOnlyAppender {
                    log: &log,
                    source_flushed: Cell::new(false),
                },
            )?;
        let pending = authority
            .slots
            .get(&setup.participant_id)
            .and_then(|slot| match slot.binding {
                BindingState::PendingFinalization(PendingFinalization::Detached(pending)) => {
                    Some(pending)
                }
                BindingState::PendingFinalization(PendingFinalization::Died(_))
                | BindingState::Bound(_)
                | BindingState::Detached => None,
            })
            .ok_or("ServerShutdown selector did not produce Pending Detached")?;
        let terminal_order = pending.admission_order().transaction_order();
        drop(owner);
        (source_sequence, terminal_order)
    };
    let source = block_on(log.read_at(detached_source_sequence))??
        .ok_or("pending Detached source-only cut omitted the source row")?;
    let DecodedStoredOperation::V3(StoredOperation::Detached { row }) = source.operation else {
        return Err("pending Detached source-only cut appended the wrong row".into());
    };
    if row.cause != StoredDetachedCause::ServerShutdown
        || row.disposition != StoredTerminalDisposition::Pending
    {
        return Err("pending Detached source row lost its cause or disposition".into());
    }
    let drain_sequence = detached_source_sequence
        .checked_add(1)
        .ok_or("pending Detached drain sequence overflow")?;
    if block_on(log.read_at(drain_sequence))??.is_some() {
        return Err("pending Detached fixture appended past the source cut".into());
    }
    Ok(PendingDetachedRestartFixture {
        handler: setup.handler,
        log,
        conversation_id: setup.conversation_id,
        participant_id: setup.participant_id,
        peer_participant_id: setup.peer_participant_id,
        peer_connection: ConnectionIncarnation::new(103, 4),
        binding_epoch: setup.binding_epoch,
        attach_secret: setup.attach_secret,
        detached_source_sequence,
        drain_sequence,
        terminal_order,
    })
}

/// Cold-restarts the fixture's durable bytes into a fresh production handler
/// with the same retention shape the write side bound.
fn restarted_handler(
    fixture: &PendingDetachedRestartFixture,
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
    fixture: &PendingDetachedRestartFixture,
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

fn publish(fixture: &PendingDetachedRestartFixture, token: u8, payload: Vec<u8>) -> ClientRequest {
    ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: fixture.conversation_id,
        participant_id: fixture.peer_participant_id,
        capability_generation: Generation::ONE,
        record_admission_attempt_token: RecordAdmissionAttemptToken::new([token; 16]),
        payload,
    })
}

/// The S-18 red pin (pins 2, 3, 4, 7): a valid publish that encounters the
/// crash-restored `PendingFinalization(Detached)` residence COMMITS after the
/// candidate-lane drain; the victim's slot settles at committed
/// `BindingState::Detached` with its enrollment token still mapped (never
/// `ParticipantUnknown`, never erased); the committed record's recipients
/// INCLUDE the drained-to-Detached victim as a parked recipient; and a repeat
/// publish commits with no further drain work.
#[test]
fn valid_publish_during_pending_detached_residence_commits_after_drain()
-> Result<(), Box<dyn Error>> {
    let fixture = pending_detached_restart_fixture()?;
    let restarted = restarted_handler(&fixture)?;

    // Crash-restore residence: cold replay rests the victim in Pending Detached.
    let replayed = restarted.replay_aggregate_reference(fixture.conversation_id, &fixture.log)?;
    let victim = replayed
        .slots
        .get(&fixture.participant_id)
        .ok_or("restore omitted the pending participant")?;
    if !matches!(
        victim.binding,
        BindingState::PendingFinalization(PendingFinalization::Detached(_))
    ) {
        return Err("restore did not rest the victim in PendingFinalization(Detached)".into());
    }

    // (pin 2) A VALID publish from the still-bound peer commits: RecordCommitted,
    // no refusal, no torn connection.
    let mut peer_conversations = ParticipantConnectionConversations::default();
    let committed = dispatch_tracked(
        &restarted,
        fixture.peer_connection,
        &mut peer_conversations,
        publish(&fixture, 0xC7, vec![0xD1, 0xD2]),
    )?;
    let ServerValue::RecordCommitted(record) = committed else {
        return Err(format!("residence publish did not commit: {committed:?}").into());
    };

    // (pin 3, durable half) Exactly one committed Detached terminal was
    // appended for the victim, cause PRESERVED (ServerShutdown), never a Died
    // row.
    let appended = rows_from(&fixture, fixture.drain_sequence)?;
    if appended.iter().any(|operation| {
        matches!(operation, StoredOperation::Died { row } if row.participant_id == fixture.participant_id)
    }) {
        return Err("Detached drain fabricated a Died row for the victim".into());
    }
    let drained: Vec<&StoredDetached> = appended
        .iter()
        .filter_map(|operation| match operation {
            StoredOperation::Detached { row } if row.participant_id == fixture.participant_id => {
                Some(row)
            }
            _ => None,
        })
        .collect();
    let [drain_row] = drained.as_slice() else {
        return Err(format!(
            "expected exactly one drained Detached terminal row, found {}",
            drained.len()
        )
        .into());
    };
    let StoredTerminalDisposition::Committed { terminal_seq } = drain_row.disposition else {
        return Err("drained Detached terminal row is not committed".into());
    };
    if drain_row.cause != StoredDetachedCause::ServerShutdown {
        return Err("drained Detached terminal row lost its preserved cause".into());
    }
    if drain_row.terminal_order != fixture.terminal_order {
        return Err("drained Detached terminal row lost its pinned admission order".into());
    }
    if record.delivery_seq() <= terminal_seq {
        return Err("publish did not commit strictly after the drained terminal".into());
    }

    // (pin 3, state half) The candidate is deleted; the victim's slot is
    // PRESENT as committed Detached and its enrollment token still maps — the
    // opposite of the Died drain's erasure.
    let drained_state =
        restarted.replay_aggregate_reference(fixture.conversation_id, &fixture.log)?;
    let victim_slot = drained_state
        .slots
        .get(&fixture.participant_id)
        .ok_or("Detached drain erased the victim's binding slot")?;
    if !matches!(victim_slot.binding, BindingState::Detached) {
        return Err(format!(
            "drained victim did not settle at committed Detached: {:?}",
            victim_slot.binding
        )
        .into());
    }
    if !drained_state
        .tokens
        .values()
        .any(|mapped| *mapped == fixture.participant_id)
    {
        return Err(
            "Detached drain unmapped the victim's enrollment token (ParticipantUnknown)".into(),
        );
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

    // (pin 4) The committed record's recipient set INCLUDES the
    // drained-to-Detached victim as a parked recipient (contrast the Died
    // drain's exclusion).
    assert_post_drain_recipients_include_victim(&fixture)?;

    // (pin 7) Post-commit durable replay shows no residual candidate (asserted
    // above) and a REPEAT publish commits with no drain work.
    assert_repeat_publish_commits_without_drain(&fixture, &restarted, &mut peer_conversations)?;
    Ok(())
}

/// S-18 pin 4: the victim was the sole other participant, so the publish's
/// produced batch must name exactly the victim — the parked recipient the
/// committed-`Detached` slot already means (`outbox_projection.rs::produced`).
fn assert_post_drain_recipients_include_victim(
    fixture: &PendingDetachedRestartFixture,
) -> Result<(), Box<dyn Error>> {
    let outbox = OutboxLog::new(Arc::clone(&fixture.handler.store), fixture.conversation_id);
    let publish_batch = block_on(outbox.read_all())??
        .into_iter()
        .filter_map(|(_, row)| match row {
            OutboxRow::Produced(batch)
                if batch.source_log_sequence() >= fixture.drain_sequence
                    && batch.source_kind() == ProducedSourceKind::RecordAdmission =>
            {
                Some(batch)
            }
            _ => None,
        })
        .next()
        .ok_or("committed publish produced no outbox batch")?;
    for projected in publish_batch.ordered_records() {
        if !projected.recipients().contains(&fixture.participant_id) {
            return Err(format!(
                "post-drain publish batch excluded the drained-to-Detached victim: {:?}",
                projected.recipients()
            )
            .into());
        }
    }
    Ok(())
}

/// S-18 pin 7: a repeat publish after the drain commits with no drain work —
/// exactly one new durable row and no new Detached terminal rows.
fn assert_repeat_publish_commits_without_drain(
    fixture: &PendingDetachedRestartFixture,
    restarted: &ProductionParticipantHandler,
    peer_conversations: &mut ParticipantConnectionConversations,
) -> Result<(), Box<dyn Error>> {
    let rows_before_repeat = rows_from(fixture, fixture.drain_sequence)?.len();
    let repeated = dispatch_tracked(
        restarted,
        fixture.peer_connection,
        peer_conversations,
        publish(fixture, 0xC8, vec![0xD3]),
    )?;
    if !matches!(repeated, ServerValue::RecordCommitted(_)) {
        return Err(format!("repeat publish after drain did not commit: {repeated:?}").into());
    }
    let rows_after_repeat = rows_from(fixture, fixture.drain_sequence)?;
    if rows_after_repeat.len()
        != rows_before_repeat
            .checked_add(1)
            .ok_or("row count overflow")?
    {
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
    Ok(())
}

/// The drain transaction's exact durable shape (S-17): one drain-flavored
/// Detached row — committed disposition, source `Drained` keyed to the
/// pending source row and carrying the consumed `PresentEnclosing`
/// presentation, cause preserved — then the caller's `RecordAdmission` and
/// NOTHING else (a Detached drain has no specific fate, so no Ordinary row) —
/// and the whole log double-replays to identical allocation state.
#[test]
fn detached_drain_appends_detach_shaped_finalizer_transaction() -> Result<(), Box<dyn Error>> {
    let fixture = pending_detached_restart_fixture()?;
    let restarted = restarted_handler(&fixture)?;
    let mut peer_conversations = ParticipantConnectionConversations::default();
    let committed = dispatch_tracked(
        &restarted,
        fixture.peer_connection,
        &mut peer_conversations,
        publish(&fixture, 0xD7, vec![0xE1]),
    )?;
    if !matches!(committed, ServerValue::RecordCommitted(_)) {
        return Err(format!("residence publish did not commit: {committed:?}").into());
    }

    let appended = rows_from(&fixture, fixture.drain_sequence)?;
    let [drain, publish_row] = appended.as_slice() else {
        return Err(format!(
            "drain transaction appended {} rows, expected drain + RecordAdmission",
            appended.len()
        )
        .into());
    };
    let StoredOperation::Detached { row: drain_row } = drain else {
        return Err(format!("first appended row is not the drain row: {drain:?}").into());
    };
    let StoredDetachedSource::Drained { drained } = &drain_row.source else {
        return Err(format!(
            "drain row does not carry drained provenance: {:?}",
            drain_row.source
        )
        .into());
    };
    if drained.pending_source_sequence != fixture.detached_source_sequence {
        return Err("drain row does not name its pending Detached source".into());
    }
    if drained.finalizer_presentation != StoredFinalizerPresentation::PresentEnclosing {
        return Err(format!(
            "drain row consumed the wrong presentation: {:?}",
            drained.finalizer_presentation
        )
        .into());
    }
    if drain_row.cause != StoredDetachedCause::ServerShutdown {
        return Err("drain row lost the preserved ServerShutdown cause".into());
    }
    if !matches!(
        drain_row.disposition,
        StoredTerminalDisposition::Committed { .. }
    ) {
        return Err("drain row is not committed".into());
    }
    if !matches!(publish_row, StoredOperation::RecordAdmission { .. }) {
        return Err(format!("second appended row is not the publish: {publish_row:?}").into());
    }

    // The drained log double-replays to identical allocation state.
    let first = restarted.replay_aggregate_reference(fixture.conversation_id, &fixture.log)?;
    let second = restarted.replay_aggregate_reference(fixture.conversation_id, &fixture.log)?;
    if first.next_seq != second.next_seq
        || first.next_order != second.next_order
        || first.next_log_sequence != second.next_log_sequence
    {
        return Err("drained log did not double-replay to identical allocations".into());
    }
    Ok(())
}

/// S-18 pin 5 (unit half — the decisive live replay rides the e2e analog): a
/// post-drain exact-secret attach binds the drained victim again. The slot
/// preserved by the faithful finalization answers `AttachBound`, never
/// `ParticipantUnknown`. The retention window that forced the terminal to
/// pend is first released through the real mechanism — the peer acknowledges
/// its own obligation endpoint — so the attach's record row is admissible
/// without any capacity waiver.
#[test]
fn drained_detached_victim_resumes_with_exact_secret() -> Result<(), Box<dyn Error>> {
    let fixture = pending_detached_restart_fixture_with_acks(Some(4), true)?;
    let restarted = restarted_handler(&fixture)?;
    let mut peer_conversations = ParticipantConnectionConversations::default();
    let committed = dispatch_tracked(
        &restarted,
        fixture.peer_connection,
        &mut peer_conversations,
        publish(&fixture, 0xE7, vec![0xF1]),
    )?;
    if !matches!(committed, ServerValue::RecordCommitted(_)) {
        return Err(format!("residence publish did not commit: {committed:?}").into());
    }

    let mut resumed_conversations = ParticipantConnectionConversations::default();
    let resumed = dispatch_tracked(
        &restarted,
        ConnectionIncarnation::new(103, 7),
        &mut resumed_conversations,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: fixture.conversation_id,
            participant_id: fixture.participant_id,
            capability_generation: fixture.binding_epoch.capability_generation,
            attach_secret: fixture.attach_secret,
            attach_attempt_token: AttachAttemptToken::new([0xE9; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    let ServerValue::AttachBound(_) = resumed else {
        return Err(
            format!("drained victim's exact-secret attach did not bind: {resumed:?}").into(),
        );
    };
    let cell = restarted.cell(fixture.conversation_id)?;
    let owner = cell
        .lock()
        .map_err(|_| "resumed victim owner lock was poisoned")?;
    let authority = owner
        .as_ref()
        .ok_or("resumed victim owner was unavailable")?;
    let slot = authority
        .slots
        .get(&fixture.participant_id)
        .ok_or("resumed victim slot disappeared")?;
    if !matches!(slot.binding, BindingState::Bound(_)) {
        return Err(format!("resumed victim is not Bound: {:?}", slot.binding).into());
    }
    Ok(())
}

/// S-18 pin 6 — the R-D1 census in the Detached prestate (closes §3A.4's
/// UNVERIFIED flag, mirroring the Died census pin): the restored frontier's
/// SOLE immutable candidate is the victim's pending `BindingTerminal` and the
/// restored closure accounting is `Clear`, so every closure-family or
/// observer-backpressure refusal body in the frozen R-D1 register would be
/// fabricated for this prestate — the total simulation admits no truthful
/// refusal and selects SUCCESS.
#[test]
fn detached_residence_census_sole_terminal_candidate_closure_clear() -> Result<(), Box<dyn Error>> {
    let fixture = pending_detached_restart_fixture()?;
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
        return Err("residence terminal epoch is not the victim's ended binding epoch".into());
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

/// A real durable appender for the mixed-flavor fixture's fate cuts.
struct LogAppender<'a> {
    log: &'a OperationLog,
}

impl DurableAppend for LogAppender<'_> {
    fn append(
        &self,
        operation: &StoredOperation,
        expected_sequence: u64,
    ) -> Result<(), OperationLogError> {
        block_on(self.log.append(operation, expected_sequence))?
    }
}

/// S-18 pin 8 — mixed-flavor ordering. At this base the protocol admits AT
/// MOST ONE pending binding-terminal candidate (the structural boundary the
/// Died suite pins for its own flavor), so the pin instantiates strict
/// `admission_order` sequencing the only way the substrate can express it:
///
/// - while the earlier Died candidate is open, the later Detached fate
///   CANNOT join the candidate lane (refused at admission) — the mixed
///   two-candidate prestate is unmintable;
/// - each candidate then drains in strict admission order, one committed
///   transaction each, with its OWN flavor semantics: the Died drain erases
///   its victim (slot and token gone), the Detached drain preserves its
///   victim at committed `BindingState::Detached` with its token mapped;
/// - the durable log carries the two drain transactions in admission order,
///   each keyed to its own pending source row.
#[test]
fn died_then_detached_candidates_drain_strictly_by_admission_order() -> Result<(), Box<dyn Error>> {
    let conversation_id = 79;
    let died_connection = ConnectionIncarnation::new(105, 3);
    let detached_connection = ConnectionIncarnation::new(105, 4);
    let publisher_connection = ConnectionIncarnation::new(105, 5);
    let store: Arc<dyn liminal::durability::DurableStore> =
        Arc::new(liminal::durability::open_ephemeral(1)?);
    let mut config = test_participant_config();
    // One more retained row than the two-party fixtures: three identities
    // enroll (plus A's attach) before the debt record rests retention at the
    // cap, and the cap must bind at BOTH fate cuts.
    config.max_retained_record_rows = 5;
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), config)?;
    let log = OperationLog::new(Arc::clone(&store), conversation_id);

    // Died victim A: enrolls, attaches, and publishes the debt record that
    // rests retention at its cap.
    let mut conversations_a = ParticipantConnectionConversations::default();
    let enrolled_a = dispatch_tracked(
        &handler,
        died_connection,
        &mut conversations_a,
        ClientRequest::Enrollment(liminal_protocol::wire::EnrollmentRequest {
            conversation_id,
            enrollment_token: liminal_protocol::wire::EnrollmentToken::new([0xA1; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(receipt_a) = enrolled_a else {
        return Err(format!("Died victim did not enroll: {enrolled_a:?}").into());
    };
    let attached_a = dispatch_tracked(
        &handler,
        died_connection,
        &mut conversations_a,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id,
            participant_id: receipt_a.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: receipt_a.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([0xA2; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    let ServerValue::AttachBound(attached_a) = attached_a else {
        return Err(format!("Died victim did not attach: {attached_a:?}").into());
    };

    // Detached victim B and publisher C enroll on their own connections.
    let mut conversations_b = ParticipantConnectionConversations::default();
    let enrolled_b = dispatch_tracked(
        &handler,
        detached_connection,
        &mut conversations_b,
        ClientRequest::Enrollment(liminal_protocol::wire::EnrollmentRequest {
            conversation_id,
            enrollment_token: liminal_protocol::wire::EnrollmentToken::new([0xA3; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(receipt_b) = enrolled_b else {
        return Err(format!("Detached victim did not enroll: {enrolled_b:?}").into());
    };
    let mut conversations_c = ParticipantConnectionConversations::default();
    let enrolled_c = dispatch_tracked(
        &handler,
        publisher_connection,
        &mut conversations_c,
        ClientRequest::Enrollment(liminal_protocol::wire::EnrollmentRequest {
            conversation_id,
            enrollment_token: liminal_protocol::wire::EnrollmentToken::new([0xA4; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(receipt_c) = enrolled_c else {
        return Err(format!("publisher did not enroll: {enrolled_c:?}").into());
    };
    let debt = dispatch_tracked(
        &handler,
        died_connection,
        &mut conversations_a,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id,
            participant_id: receipt_a.participant_id(),
            capability_generation: attached_a.origin_binding_epoch().capability_generation,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([0xA5; 16]),
            payload: vec![0xA6],
        }),
    )?;
    if !matches!(debt, ServerValue::RecordCommitted(_)) {
        return Err(format!("debt record did not commit: {debt:?}").into());
    }

    // Fate cut 1: A's connection dies -> pending Died, the sole candidate.
    let cell = handler.cell(conversation_id)?;
    let died_source_sequence = {
        let mut owner = cell
            .lock()
            .map_err(|_| "mixed-flavor owner lock was poisoned")?;
        let authority = owner.as_mut().ok_or("mixed-flavor owner was unavailable")?;
        let source_sequence = authority.next_log_sequence;
        authority
            .prepare_connection_fate_transaction(&ConnectionFateWorkItem {
                open_sequence: 51,
                connection_incarnation: died_connection,
                class: ConnectionFateClass::ConnectionLost,
                tracked_conversations: conversations_a.tracked_conversations(),
            })
            .complete(authority, &LogAppender { log: &log })?;
        if !matches!(
            authority
                .slots
                .get(&receipt_a.participant_id())
                .map(|slot| slot.binding),
            Some(BindingState::PendingFinalization(
                PendingFinalization::Died(_)
            ))
        ) {
            return Err("fate cut 1 did not rest A in Pending Died".into());
        }

        // STRUCTURAL BOUNDARY: while A's candidate is open, B's Detached
        // fate cannot join the candidate lane.
        let refused = authority
            .prepare_connection_fate_transaction(&ConnectionFateWorkItem {
                open_sequence: 52,
                connection_incarnation: detached_connection,
                class: ConnectionFateClass::ServerShutdown,
                tracked_conversations: conversations_b.tracked_conversations(),
            })
            .complete(authority, &LogAppender { log: &log });
        let error = match refused {
            Ok(()) => {
                return Err(
                    "a second (Detached) pending terminal joined the candidate lane — the \
                     mixed-flavor two-candidate prestate has become mintable; extend the \
                     drain coverage to drain both from one lane occupancy"
                        .into(),
                );
            }
            Err(error) => format!("{error:?}"),
        };
        if !error.contains("binding-terminal admission refused") {
            return Err(format!(
                "second (Detached) pending terminal failed for an unexpected reason: {error}"
            )
            .into());
        }
        drop(owner);
        source_sequence
    };

    // Drain 1: the publisher's record drives DrainFirst into A's candidate;
    // Died semantics — identity erased.
    let publish_1 = dispatch_tracked(
        &handler,
        publisher_connection,
        &mut conversations_c,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id,
            participant_id: receipt_c.participant_id(),
            capability_generation: Generation::ONE,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([0xA7; 16]),
            payload: vec![0xA8],
        }),
    )?;
    if !matches!(publish_1, ServerValue::RecordCommitted(_)) {
        return Err(format!("first drain publish did not commit: {publish_1:?}").into());
    }
    let detached_source_sequence = {
        let mut owner = cell
            .lock()
            .map_err(|_| "mixed-flavor owner lock was poisoned")?;
        let authority = owner.as_mut().ok_or("mixed-flavor owner was unavailable")?;
        if authority.slots.contains_key(&receipt_a.participant_id()) {
            return Err("Died drain did not erase A's binding slot".into());
        }
        if authority
            .tokens
            .values()
            .any(|mapped| *mapped == receipt_a.participant_id())
        {
            return Err("Died drain left A's enrollment token mapped".into());
        }

        // Fate cut 2: with A drained, B's ServerShutdown fate pends.
        let source_sequence = authority.next_log_sequence;
        authority
            .prepare_connection_fate_transaction(&ConnectionFateWorkItem {
                open_sequence: 53,
                connection_incarnation: detached_connection,
                class: ConnectionFateClass::ServerShutdown,
                tracked_conversations: conversations_b.tracked_conversations(),
            })
            .complete(authority, &LogAppender { log: &log })?;
        if !matches!(
            authority
                .slots
                .get(&receipt_b.participant_id())
                .map(|slot| slot.binding),
            Some(BindingState::PendingFinalization(
                PendingFinalization::Detached(_)
            ))
        ) {
            return Err("fate cut 2 did not rest B in Pending Detached".into());
        }
        drop(owner);
        source_sequence
    };

    // Drain 2: the next publish drains B's candidate; Detached semantics —
    // slot preserved at committed Detached.
    let publish_2 = dispatch_tracked(
        &handler,
        publisher_connection,
        &mut conversations_c,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id,
            participant_id: receipt_c.participant_id(),
            capability_generation: Generation::ONE,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([0xA9; 16]),
            payload: vec![0xAA],
        }),
    )?;
    if !matches!(publish_2, ServerValue::RecordCommitted(_)) {
        return Err(format!("second drain publish did not commit: {publish_2:?}").into());
    }
    {
        let mut owner = cell
            .lock()
            .map_err(|_| "mixed-flavor owner lock was poisoned")?;
        let authority = owner.as_mut().ok_or("mixed-flavor owner was unavailable")?;
        let slot_b = authority
            .slots
            .get(&receipt_b.participant_id())
            .ok_or("Detached drain erased B's binding slot")?;
        if !matches!(slot_b.binding, BindingState::Detached) {
            return Err(format!(
                "Detached drain did not settle B at committed Detached: {:?}",
                slot_b.binding
            )
            .into());
        }
        if !authority
            .tokens
            .values()
            .any(|mapped| *mapped == receipt_b.participant_id())
        {
            return Err("Detached drain unmapped B's enrollment token".into());
        }
        drop(owner);
    }

    // Durable shape: [Died source(A), Died drain(A), Ordinary(A), publish 1,
    // Detached source(B), Detached drain(B), publish 2] — two drain
    // transactions strictly by admission order, each its own flavor.
    let mut rows = Vec::new();
    let mut sequence = died_source_sequence;
    while let Some(entry) = block_on(log.read_at(sequence))?? {
        let DecodedStoredOperation::V3(operation) = entry.operation else {
            return Err(format!("durable row {sequence} is not schema v3").into());
        };
        rows.push(operation);
        sequence = sequence
            .checked_add(1)
            .ok_or("durable log sequence overflowed")?;
    }
    let [
        StoredOperation::Died { row: died_source },
        StoredOperation::Died { row: died_drain },
        StoredOperation::Ordinary { .. },
        StoredOperation::RecordAdmission { .. },
        StoredOperation::Detached {
            row: detached_source,
        },
        StoredOperation::Detached {
            row: detached_drain,
        },
        StoredOperation::RecordAdmission { .. },
    ] = rows.as_slice()
    else {
        return Err(format!("mixed-flavor drain transactions drifted: {rows:?}").into());
    };
    if died_source.disposition != StoredTerminalDisposition::Pending
        || died_drain
            .drained
            .map(|drained| drained.pending_source_sequence)
            != Some(died_source_sequence)
    {
        return Err("Died drain is not keyed to its pending source".into());
    }
    if detached_source.disposition != StoredTerminalDisposition::Pending
        || !matches!(
            &detached_drain.source,
            StoredDetachedSource::Drained { drained }
                if drained.pending_source_sequence == detached_source_sequence
        )
    {
        return Err("Detached drain is not keyed to its pending source".into());
    }
    if died_drain.terminal_order >= detached_drain.terminal_order {
        return Err(format!(
            "drains are not strictly ordered by admission order: Died {} vs Detached {}",
            died_drain.terminal_order, detached_drain.terminal_order
        )
        .into());
    }
    if detached_drain.cause != StoredDetachedCause::ServerShutdown {
        return Err("Detached drain lost its preserved cause".into());
    }
    Ok(())
}
