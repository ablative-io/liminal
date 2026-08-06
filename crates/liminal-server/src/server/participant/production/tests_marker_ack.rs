use std::error::Error;
use std::sync::Arc;

use liminal::durability::bridge::block_on;
use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, Generation, MarkerAck, ParticipantAck, RecordAdmission,
    RecordAdmissionAttemptToken, ServerValue,
};

use super::ProductionParticipantHandler;
use super::log::OperationLog;
use super::outbox::RetainedAuthorityMeasurements;
use super::outbox_log::{OutboxLog, OutboxRow, StoredMarkerAckCommitted};
use super::tests::dispatch;
use super::tests_marker_ack_fixture::{
    MarkerFixture, marker_fixture_config, marker_protocol_snapshot, prepare_marker_fixture,
};
use super::tests_outbox_owner::assert_live_recipient_obligation_bound_holds_without_mutation_and_owner_continues;
use crate::server::participant::{ParticipantOfferedProgress, ParticipantSemanticHandler};

fn dispatch_marker_ack(
    fixture: &MarkerFixture,
    connection: ConnectionIncarnation,
    generation: Generation,
    marker_delivery_seq: u64,
) -> Result<ServerValue, Box<dyn Error>> {
    dispatch(
        &fixture.handler,
        connection,
        ClientRequest::MarkerAck(MarkerAck {
            conversation_id: fixture.marker_delivery.conversation_id,
            participant_id: fixture.target_participant,
            capability_generation: generation,
            marker_delivery_seq,
        }),
    )
}

pub(super) fn record_exact_marker_offer(fixture: &MarkerFixture) -> Result<(), Box<dyn Error>> {
    let mut offered = None;
    let mut marker_publication = None;
    for _ in 0..8 {
        let publication = fixture
            .handler
            .next_publication(
                fixture.target_connection,
                fixture.marker_delivery.conversation_id,
                offered,
            )?
            .ok_or("marker fixture obligations ended before its marker")?;
        offered = Some(ParticipantOfferedProgress {
            binding_epoch: publication.binding_epoch,
            through_seq: publication.delivery_seq(),
        });
        if publication.delivery == fixture.marker_delivery {
            marker_publication = Some(publication);
            break;
        }
    }
    let publication =
        marker_publication.ok_or("marker was not reached within the signed fixture bound")?;
    fixture.handler.record_publication_offer(&publication)?;
    Ok(())
}

fn assert_marker_refusals(fixture: &MarkerFixture) -> Result<(), Box<dyn Error>> {
    let marker_delivery_seq = fixture.marker_delivery.delivery_seq;
    let before_offer = dispatch_marker_ack(
        fixture,
        fixture.target_connection,
        Generation::ONE,
        marker_delivery_seq,
    )?;
    assert!(
        matches!(
            before_offer,
            ServerValue::MarkerNotDelivered(_) | ServerValue::MarkerMismatch(_)
        ),
        "marker ack committed before exact offer testimony: {before_offer:?}"
    );

    record_exact_marker_offer(fixture)?;
    let wrong_marker = dispatch_marker_ack(
        fixture,
        fixture.target_connection,
        Generation::ONE,
        marker_delivery_seq.saturating_add(1),
    )?;
    assert!(matches!(wrong_marker, ServerValue::MarkerMismatch(_)));

    let generation_two = Generation::new(2).ok_or("generation two was invalid")?;
    let stale_generation = dispatch_marker_ack(
        fixture,
        fixture.target_connection,
        generation_two,
        marker_delivery_seq,
    )?;
    assert!(matches!(stale_generation, ServerValue::StaleAuthority(_)));

    let wrong_connection = ConnectionIncarnation::new(
        fixture.target_connection.server_incarnation,
        fixture
            .target_connection
            .connection_ordinal
            .saturating_add(20),
    );
    let wrong_binding = dispatch_marker_ack(
        fixture,
        wrong_connection,
        Generation::ONE,
        marker_delivery_seq,
    )?;
    assert!(
        matches!(
            wrong_binding,
            ServerValue::NoBinding(_) | ServerValue::StaleAuthority(_)
        ),
        "wrong-binding marker ack was not a typed refusal: {wrong_binding:?}"
    );
    Ok(())
}

pub(super) fn commit_exact_marker_ack(
    fixture: &MarkerFixture,
) -> Result<StoredMarkerAckCommitted, Box<dyn Error>> {
    let conversation_id = fixture.marker_delivery.conversation_id;
    let outbox_log = OutboxLog::new(Arc::clone(&fixture.store), conversation_id);
    let rows_before_commit = block_on(outbox_log.read_all())??;
    let committed = dispatch_marker_ack(
        fixture,
        fixture.target_connection,
        Generation::ONE,
        fixture.marker_delivery.delivery_seq,
    )?;
    if !matches!(committed, ServerValue::MarkerAckCommitted(_)) {
        return Err(format!("exact offered MarkerAck did not commit: {committed:?}").into());
    }

    let live_rows = block_on(outbox_log.read_all())??;
    assert_eq!(live_rows.len(), rows_before_commit.len() + 1);
    let Some((physical_sequence, OutboxRow::MarkerAckCommitted(stored))) = live_rows.last() else {
        return Err("live MarkerAck extension row was absent".into());
    };
    assert_eq!(stored.extension_sequence, *physical_sequence);
    Ok(stored.clone())
}

fn assert_marker_replay(
    live: &MarkerFixture,
    stored: &StoredMarkerAckCommitted,
) -> Result<(), Box<dyn Error>> {
    let conversation_id = live.marker_delivery.conversation_id;
    let live_snapshot =
        marker_protocol_snapshot(&live.handler, conversation_id, live.target_participant)?;
    let replay = prepare_marker_fixture()?;
    assert_eq!(replay.target_participant, live.target_participant);
    assert_eq!(replay.marker_delivery, live.marker_delivery);

    let replay_cell = replay.handler.cell(conversation_id)?;
    let mut replay_owner = replay_cell
        .lock()
        .map_err(|_| "marker replay owner lock was poisoned")?;
    let replay_authority = replay_owner
        .as_mut()
        .ok_or("marker replay owner was absent")?;
    assert_eq!(stored.base_log_head, replay_authority.next_log_sequence);
    replay_authority.replay_marker_ack_extension(stored)?;
    drop(replay_owner);

    let replay_snapshot =
        marker_protocol_snapshot(&replay.handler, conversation_id, replay.target_participant)?;
    // ⛔ WHAT THIS EQUALITY CANNOT EVIDENCE, STATED HERE BECAUSE OF WHERE IT SITS.
    //
    // This asserts that cold replay AGREES WITH live. It does NOT assert that
    // either one is HEALTHY. When `#26` was open, the live and replay paths both
    // failed to progress the sealed binding-fate token, so BOTH SIDES FROZE
    // IDENTICALLY and this assertion was GREEN WITH THE DEFECT — exactly as green
    // as it is now that the defect is fixed. ⇒ AGREEMENT IS NOT HEALTH: two paths
    // sharing a bug agree perfectly.
    //
    // ⚠ AND THIS UNIT COULD NOT BE STRENGTHENED IN PLACE TO CLOSE THAT, WHICH IS
    // WHY IT CARRIES A NOTE RATHER THAN A BETTER ASSERTION. `prepare_marker_fixture`
    // (line above) drives NO CredentialAttach, so it mints NO sealed binding-fate
    // token — there is nothing here for a marker-ack to strand, and no state
    // assertion about fate is even expressible against this fixture. Rewriting it
    // onto `attached_marker_fixture` would not sharpen this test; it would replace
    // it with a different one.
    //
    // The health question is answered by `tests_marker_fate_repro`, which arms on
    // the ATTACHED fixture and discriminates on STATE (does the next ordinary ack
    // COMMIT) rather than on agreement. ⛔ DO NOT CITE THIS UNIT AS EVIDENCE THAT
    // COLD REPLAY IS CORRECT. It pins live/cold parity, which is worth pinning, and
    // that is the whole of what it can say.
    assert_eq!(replay_snapshot, live_snapshot);
    Ok(())
}

#[derive(Clone, Copy)]
enum MarkerBaseInterleaving {
    AckFirst,
    AckBetween,
}

fn dispatch_interleaved_ordinary(
    fixture: &MarkerFixture,
    token: u8,
) -> Result<ServerValue, Box<dyn Error>> {
    dispatch(
        &fixture.handler,
        fixture.record_connection,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: fixture.marker_delivery.conversation_id,
            participant_id: fixture.record_participant,
            capability_generation: Generation::ONE,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([token; 16]),
            payload: vec![token],
        }),
    )
}

fn commit_interleaved_ordinary(fixture: &MarkerFixture, token: u8) -> Result<u64, Box<dyn Error>> {
    let outcome = dispatch_interleaved_ordinary(fixture, token)?;
    if let ServerValue::RecordCommitted(committed) = outcome {
        return Ok(committed.delivery_seq());
    }
    Err(format!("interleaved ordinary admission was not committed: {outcome:?}").into())
}

fn commit_interleaved_catchup(
    fixture: &MarkerFixture,
    through_seq: u64,
) -> Result<(), Box<dyn Error>> {
    let outcome = dispatch(
        &fixture.handler,
        fixture.catchup_connection,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: fixture.marker_delivery.conversation_id,
            participant_id: fixture.catchup_participant,
            capability_generation: Generation::ONE,
            through_seq,
        }),
    )?;
    if !matches!(outcome, ServerValue::AckCommitted(_)) {
        return Err(format!("interleaved catch-up ack did not commit: {outcome:?}").into());
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MarkerAccountingSnapshot {
    live_records: usize,
    live_obligations: u64,
    charged_bytes: u64,
    durable_ack: u64,
    next_live: Option<u64>,
    retained: RetainedAuthorityMeasurements,
}

fn marker_accounting_snapshot(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
    participant_id: u64,
) -> Result<MarkerAccountingSnapshot, Box<dyn Error>> {
    let cell = handler.cell(conversation_id)?;
    let owner = cell
        .lock()
        .map_err(|_| "marker accounting owner lock was poisoned")?;
    let authority = owner.as_ref().ok_or("marker accounting owner was absent")?;
    let outbox = authority
        .outbox
        .as_ref()
        .ok_or("marker accounting outbox was absent")?;
    let snapshot = MarkerAccountingSnapshot {
        live_records: outbox.live_record_count(),
        live_obligations: outbox.live_recipient_obligation_count(),
        charged_bytes: outbox.charged_bytes(),
        durable_ack: outbox.ack_through(participant_id),
        next_live: outbox.next_live(participant_id),
        retained: outbox.retained_authority_measurements()?,
    };
    drop(owner);
    Ok(snapshot)
}

#[derive(Debug, PartialEq, Eq)]
struct CompleteMarkerSnapshot {
    cursor: u64,
    next_order: u64,
    next_seq: u64,
    next_log_sequence: u64,
    observer_progress: u64,
    frontier: String,
    outbox: String,
    /// F8B: how many immutable candidates the owner's lane holds. A cold
    /// restart now drains them (R-BOOT-DRAIN), so this is the one field whose
    /// live and cold readings are designed to differ.
    lane_candidates: usize,
    /// The restored frontier's closure state — the "owner variant" this pin is
    /// named for, read structurally instead of off the debug rendering.
    owner_variant: String,
}

/// The marker pin's own subject — exactly what its name claims and nothing
/// else: the OWNER VARIANT (the restored frontier's closure state) and the
/// DISPATCH CURSOR the `MarkerAck` reconciled, plus the durable observer
/// progress that cursor is measured against. These survive a cold restart
/// untouched whether or not that restart drains a lane.
///
/// The outbox is deliberately NOT here. A marker drain produces its own
/// projection, so a restart that drains adds live records, obligations and
/// charged bytes to the outbox owner — a designed effect of R-BOOT-DRAIN, not
/// a drift. The outbox is still asserted whole on the no-drain interleaving,
/// where a restart must change nothing at all.
#[derive(Debug, PartialEq, Eq)]
struct MarkerPinSubject {
    cursor: u64,
    observer_progress: u64,
    owner_variant: String,
}

impl CompleteMarkerSnapshot {
    fn pin_subject(&self) -> MarkerPinSubject {
        MarkerPinSubject {
            cursor: self.cursor,
            observer_progress: self.observer_progress,
            owner_variant: self.owner_variant.clone(),
        }
    }
}

fn complete_marker_snapshot(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
    participant_id: u64,
) -> Result<CompleteMarkerSnapshot, Box<dyn Error>> {
    let cell = handler.cell(conversation_id)?;
    let owner = cell
        .lock()
        .map_err(|_| "interleaving snapshot owner lock was poisoned")?;
    let authority = owner
        .as_ref()
        .ok_or("interleaving snapshot owner was absent")?;
    let cursor = authority
        .slots
        .get(&participant_id)
        .ok_or("interleaving snapshot participant was absent")?
        .member
        .cursor();
    let snapshot = CompleteMarkerSnapshot {
        cursor,
        next_order: authority.next_order,
        next_seq: authority.next_seq,
        next_log_sequence: authority.next_log_sequence,
        observer_progress: authority.observer_progress,
        frontier: format!("{:?}", authority.obligation_debt_dispatch),
        outbox: format!("{:?}", authority.outbox),
        lane_candidates: authority.frontier().map_or(0, |frontier| {
            frontier.frontiers().sequence().immutable_candidates().len()
        }),
        owner_variant: authority.frontier().map_or_else(
            || "None".to_owned(),
            |frontier| format!("{:?}", frontier.closure_accounting().state()),
        ),
    };
    drop(owner);
    Ok(snapshot)
}

/// Returns how many immutable candidates the cold restart's boot drain
/// consumed, so the caller can prove the two interleavings together exercise
/// the drain at all.
fn assert_marker_base_interleaving(
    interleaving: MarkerBaseInterleaving,
) -> Result<u64, Box<dyn Error>> {
    let fixture = prepare_marker_fixture()?;
    let outbox_log = OutboxLog::new(
        Arc::clone(&fixture.store),
        fixture.marker_delivery.conversation_id,
    );

    let (tied_base_projection, catchup_through_seq) =
        if matches!(interleaving, MarkerBaseInterleaving::AckBetween) {
            let through_seq = commit_interleaved_ordinary(&fixture, 0xB1)?;
            let rows = block_on(outbox_log.read_all())??;
            let (physical_sequence, row) = rows
                .last()
                .ok_or("ack-between ordinary projection row was absent")?;
            if !matches!(row, OutboxRow::Produced(_)) {
                return Err(format!("ack-between ordinary row was not Produced: {row:?}").into());
            }
            record_exact_marker_offer(&fixture)?;
            (Some((*physical_sequence, row.base_log_head())), through_seq)
        } else {
            record_exact_marker_offer(&fixture)?;
            (None, fixture.catchup_through_seq)
        };

    let stored_marker = commit_exact_marker_ack(&fixture)?;
    let rows_after_marker = block_on(outbox_log.read_all())??;
    let (marker_physical_sequence, marker_row) = rows_after_marker
        .last()
        .ok_or("MarkerAckCommitted extension row was absent")?;
    assert!(matches!(marker_row, OutboxRow::MarkerAckCommitted(_)));
    assert_eq!(*marker_physical_sequence, stored_marker.extension_sequence);
    if let Some((projection_sequence, projection_boundary)) = tied_base_projection {
        assert!(projection_sequence < stored_marker.extension_sequence);
        assert_eq!(projection_boundary, Some(stored_marker.base_log_head));
    }

    let immediate = dispatch_interleaved_ordinary(&fixture, 0xB2)?;
    let ordinary_boundary_offset = match immediate {
        ServerValue::RecordCommitted(_) => 1,
        ServerValue::ObserverBackpressure(_) => {
            commit_interleaved_catchup(&fixture, catchup_through_seq)?;
            let rows_after_catchup = block_on(outbox_log.read_all())??;
            let (catchup_physical_sequence, catchup_row) = rows_after_catchup
                .last()
                .ok_or("post-MarkerAck catch-up projection row was absent")?;
            assert!(matches!(catchup_row, OutboxRow::AckAdvanced { .. }));
            assert!(stored_marker.extension_sequence < *catchup_physical_sequence);
            assert_eq!(
                catchup_row.base_log_head(),
                Some(stored_marker.base_log_head + 1)
            );
            commit_interleaved_ordinary(&fixture, 0xB2)?;
            2
        }
        other => {
            return Err(format!(
                "post-MarkerAck admission was neither committed nor the exact typed pressure outcome: {other:?}"
            )
            .into());
        }
    };
    let rows_after_ordinary = block_on(outbox_log.read_all())??;
    let (ordinary_physical_sequence, ordinary_row) = rows_after_ordinary
        .last()
        .ok_or("post-MarkerAck ordinary projection row was absent")?;
    if !matches!(ordinary_row, OutboxRow::Produced(_)) {
        return Err(
            format!("post-MarkerAck ordinary row was not Produced: {ordinary_row:?}").into(),
        );
    }
    assert!(stored_marker.extension_sequence < *ordinary_physical_sequence);
    assert_eq!(
        ordinary_row.base_log_head(),
        Some(stored_marker.base_log_head + ordinary_boundary_offset)
    );

    let conversation_id = fixture.marker_delivery.conversation_id;
    let participant_id = fixture.target_participant;
    let live_snapshot =
        complete_marker_snapshot(&fixture.handler, conversation_id, participant_id)?;
    // The lane a COLD REPLAY of these durable bytes rebuilds — which is the
    // prestate boot drains, and is NOT the live owner's lane: measured here,
    // the live owner rests with an empty lane while its own replay rebuilds a
    // marker candidate. Boot consumes that candidate; the assertions below
    // hold the drain to exactly it.
    let replayed_lane = {
        let log = OperationLog::new(Arc::clone(&fixture.store), conversation_id);
        // `replay_and_repair`, not the frozen aggregate oracle: this is the
        // exact authority `restore_all_conversations` hands the boot drain.
        let replayed = fixture.handler.replay_and_repair(conversation_id, &log)?;
        replayed.frontier().map_or(0, |frontier| {
            frontier.frontiers().sequence().immutable_candidates().len()
        })
    };
    let store = Arc::clone(&fixture.store);
    drop(fixture);

    let reopened = ProductionParticipantHandler::new(store, marker_fixture_config())?;
    let cold_snapshot = complete_marker_snapshot(&reopened, conversation_id, participant_id)?;

    // CONVERTED for F8B R-BOOT-DRAIN (§6.2, authorized in §6.6). A cold restart
    // no longer leaves the marker lane occupied: boot drains it before any
    // retained connection-fate `Open` replays. So the whole-snapshot equality
    // this pin used to assert is no longer the truth about a restart, and
    // asserting it would be asserting that R-BOOT-DRAIN did not happen.
    //
    // The pin's own subject is unchanged and still asserted whole: the
    // delivery cursor the MarkerAck reconciled, the durable observer progress,
    // and the outbox owner all survive the restart byte-identical. What moves
    // is the lane and the allocations the drain of that lane consumes — and
    // that movement is asserted EXACTLY, not merely tolerated.
    assert_eq!(cold_snapshot.pin_subject(), live_snapshot.pin_subject());

    assert_restart_drained_exactly(&live_snapshot, &cold_snapshot, replayed_lane)
}

/// The restart half of the converted marker pin: whatever the cold replay
/// rebuilt in the lane, boot drained exactly that and moved exactly what a
/// drain of it moves.
fn assert_restart_drained_exactly(
    live_snapshot: &CompleteMarkerSnapshot,
    cold_snapshot: &CompleteMarkerSnapshot,
    replayed_lane: usize,
) -> Result<u64, Box<dyn Error>> {
    let drained = u64::try_from(replayed_lane)?;
    assert_eq!(
        cold_snapshot.lane_candidates, 0,
        "boot left the restored marker lane occupied"
    );
    // One durable drain row and one delivery sequence per marker head — N
    // markers need N drains — and the transaction order does not move, because
    // the marker drain consumes the candidate's already-allocated position.
    assert_eq!(
        cold_snapshot.next_log_sequence,
        live_snapshot.next_log_sequence + drained
    );
    assert_eq!(cold_snapshot.next_seq, live_snapshot.next_seq + drained);
    assert_eq!(cold_snapshot.next_order, live_snapshot.next_order);
    if drained == 0 {
        // Nothing to drain: the restart must be a total no-op, asserted whole
        // exactly as this pin asserted it before R-BOOT-DRAIN existed.
        assert_eq!(cold_snapshot, live_snapshot);
    }
    Ok(drained)
}

/// CONVERTED for F8B R-BOOT-DRAIN (§6.2, authorized in §6.6). A cold restart no
/// longer leaves the restored marker lane occupied, so the whole-snapshot
/// live-equals-cold equality this pin asserted is no longer the truth about a
/// restart — asserting it would assert that the boot drain did not happen.
/// The pin's own subject (cursor, observer progress, outbox) is still asserted
/// whole; what the drain moves is asserted exactly, per candidate consumed.
///
/// MEASURED, and the reason the drain count is returned rather than assumed:
/// the two interleavings differ. `AckFirst` restores with an EMPTY lane and its
/// restart is a total no-op — the original equality, still asserted for it.
/// `AckBetween` restores with a marker candidate that boot drains. Neither
/// alone covers both halves of the posture, so the coverage is asserted here,
/// where both are visible.
#[test]
fn marker_ack_preserves_owner_variant_and_reconciles_dispatch_cursor() -> Result<(), Box<dyn Error>>
{
    let ack_first = assert_marker_base_interleaving(MarkerBaseInterleaving::AckFirst)?;
    let ack_between = assert_marker_base_interleaving(MarkerBaseInterleaving::AckBetween)?;
    let drained = ack_first
        .checked_add(ack_between)
        .ok_or("marker drain count overflowed")?;
    assert!(
        drained > 0,
        "neither marker interleaving restored an occupied lane, so this pin no longer \
         exercises the boot drain it was converted for (AckFirst {ack_first}, \
         AckBetween {ack_between})"
    );
    Ok(())
}

#[test]
fn marker_covered_outbox_accounting_stays_bounded_until_real_discharge()
-> Result<(), Box<dyn Error>> {
    let fixture = prepare_marker_fixture()?;
    let conversation_id = fixture.marker_delivery.conversation_id;
    let marker_cursor = fixture.marker_delivery.delivery_seq;
    let target_participant = fixture.target_participant;
    let target_connection = fixture.target_connection;
    let catchup_participant = fixture.catchup_participant;
    let catchup_connection = fixture.catchup_connection;
    let catchup_through_seq = fixture.catchup_through_seq;

    record_exact_marker_offer(&fixture)?;
    let before_marker =
        marker_accounting_snapshot(&fixture.handler, conversation_id, target_participant)?;
    assert!(before_marker.live_records > 0);
    assert!(before_marker.live_obligations > 0);
    assert!(before_marker.charged_bytes > 0);
    commit_exact_marker_ack(&fixture)?;
    let after_marker =
        marker_accounting_snapshot(&fixture.handler, conversation_id, target_participant)?;
    assert_eq!(after_marker, before_marker);

    let (protocol_cursor, _) =
        marker_protocol_snapshot(&fixture.handler, conversation_id, target_participant)?;
    assert_eq!(protocol_cursor, marker_cursor);
    if let Some(publication) =
        fixture
            .handler
            .next_publication(target_connection, conversation_id, None)?
    {
        assert!(publication.delivery_seq() > protocol_cursor);
    }

    let no_op = dispatch(
        &fixture.handler,
        target_connection,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id,
            participant_id: target_participant,
            capability_generation: Generation::ONE,
            through_seq: protocol_cursor,
        }),
    )?;
    assert!(matches!(no_op, ServerValue::AckNoOp(_)));
    assert_eq!(
        marker_accounting_snapshot(&fixture.handler, conversation_id, target_participant)?,
        after_marker
    );

    let idle_before = fixture.handler.obligation_dispatch_work_snapshot();
    assert_eq!(
        marker_accounting_snapshot(&fixture.handler, conversation_id, target_participant)?,
        after_marker
    );
    assert_eq!(
        fixture.handler.obligation_dispatch_work_snapshot(),
        idle_before
    );
    assert_live_recipient_obligation_bound_holds_without_mutation_and_owner_continues()?;

    let store = Arc::clone(&fixture.store);
    drop(fixture);
    let cold = ProductionParticipantHandler::new(store, marker_fixture_config())?;
    let cold_snapshot = marker_accounting_snapshot(&cold, conversation_id, target_participant)?;
    assert_eq!(cold_snapshot, after_marker);
    if let Some(publication) = cold.next_publication(target_connection, conversation_id, None)? {
        assert!(publication.delivery_seq() > protocol_cursor);
    }

    let before_discharge = marker_accounting_snapshot(&cold, conversation_id, catchup_participant)?;
    let advanced = dispatch(
        &cold,
        catchup_connection,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id,
            participant_id: catchup_participant,
            capability_generation: Generation::ONE,
            through_seq: catchup_through_seq,
        }),
    )?;
    assert!(matches!(advanced, ServerValue::AckCommitted(_)));
    let after_discharge = marker_accounting_snapshot(&cold, conversation_id, catchup_participant)?;
    assert!(after_discharge.live_obligations < before_discharge.live_obligations);
    assert!(after_discharge.charged_bytes <= before_discharge.charged_bytes);
    Ok(())
}

#[test]
fn marker_ack_requires_exact_offered_binding_testimony() -> Result<(), Box<dyn Error>> {
    let live = prepare_marker_fixture()?;
    assert_marker_refusals(&live)?;
    let stored = commit_exact_marker_ack(&live)?;
    assert_marker_replay(&live, &stored)
}
