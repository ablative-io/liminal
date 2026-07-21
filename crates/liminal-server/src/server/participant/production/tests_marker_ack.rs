use std::error::Error;
use std::sync::Arc;

use liminal::durability::bridge::block_on;
use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, Generation, MarkerAck, ParticipantAck, RecordAdmission,
    RecordAdmissionAttemptToken, ServerValue,
};

use super::ProductionParticipantHandler;
use super::outbox_log::{OutboxLog, OutboxRow, StoredMarkerAckCommitted};
use super::tests::dispatch;
use super::tests_marker_ack_fixture::{
    MarkerFixture, marker_fixture_config, marker_protocol_snapshot, prepare_marker_fixture,
};
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

#[derive(Debug, PartialEq, Eq)]
struct CompleteMarkerSnapshot {
    cursor: u64,
    next_order: u64,
    next_seq: u64,
    next_log_sequence: u64,
    observer_progress: u64,
    frontier: String,
    outbox: String,
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
    };
    drop(owner);
    Ok(snapshot)
}

fn assert_marker_base_interleaving(
    interleaving: MarkerBaseInterleaving,
) -> Result<(), Box<dyn Error>> {
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
    let store = Arc::clone(&fixture.store);
    drop(fixture);

    let reopened = ProductionParticipantHandler::new(store, marker_fixture_config())?;
    let cold_snapshot = complete_marker_snapshot(&reopened, conversation_id, participant_id)?;
    assert_eq!(cold_snapshot, live_snapshot);
    Ok(())
}

#[test]
fn marker_ack_and_base_row_interleavings_replay_exactly_and_totally() -> Result<(), Box<dyn Error>>
{
    assert_marker_base_interleaving(MarkerBaseInterleaving::AckFirst)?;
    assert_marker_base_interleaving(MarkerBaseInterleaving::AckBetween)
}

#[test]
fn marker_ack_requires_exact_offered_binding_testimony() -> Result<(), Box<dyn Error>> {
    let live = prepare_marker_fixture()?;
    assert_marker_refusals(&live)?;
    let stored = commit_exact_marker_ack(&live)?;
    assert_marker_replay(&live, &stored)
}
