use std::error::Error;
use std::sync::Arc;

use liminal::durability::bridge::block_on;
use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, Generation, MarkerAck, ServerValue,
};

use super::outbox_log::{OutboxLog, OutboxRow, StoredMarkerAckCommitted};
use super::tests::dispatch;
use super::tests_marker_ack_fixture::{
    MarkerFixture, marker_protocol_snapshot, prepare_marker_fixture,
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
    assert!(matches!(committed, ServerValue::MarkerAckCommitted(_)));

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

#[test]
fn marker_ack_requires_exact_offered_binding_testimony() -> Result<(), Box<dyn Error>> {
    let live = prepare_marker_fixture()?;
    assert_marker_refusals(&live)?;
    let stored = commit_exact_marker_ack(&live)?;
    assert_marker_replay(&live, &stored)
}
