use super::*;

fn newest_produced(
    store: Arc<dyn DurableStore>,
    conversation_id: u64,
) -> Result<ProducedBatch, Box<dyn Error>> {
    extension_rows(store, conversation_id)?
        .into_iter()
        .rev()
        .find_map(|(_, row)| match row {
            OutboxRow::Produced(batch) => Some(batch),
            OutboxRow::AckAdvanced { .. } | OutboxRow::MarkerAckCommitted(_) => None,
        })
        .ok_or_else(|| "operation did not append a Produced source batch".into())
}

fn assert_system_marker_includes_live_target() -> Result<(), Box<dyn Error>> {
    let marker = prepare_marker_fixture()?;
    let marker_rows = extension_rows(
        Arc::clone(&marker.store),
        marker.marker_delivery.conversation_id,
    )?;
    let marker_record = marker_rows
        .iter()
        .find_map(|(_, row)| match row {
            OutboxRow::Produced(batch)
                if batch.source_kind() == ProducedSourceKind::MarkerDrained =>
            {
                batch.ordered_records().first()
            }
            _ => None,
        })
        .ok_or("system marker Produced row was absent")?;
    assert_eq!(marker_record.sender(), None);
    assert!(
        marker_record
            .recipients()
            .contains(&marker.target_participant)
    );
    assert!(
        marker_record
            .recipients()
            .windows(2)
            .all(|pair| pair[0] < pair[1])
    );
    Ok(())
}
/// The recipient snapshot is postcommit `Bound ∪ slot-present-resumable-Detached`,
/// minus the sender (B1 ruled contract, superseding the prior Bound-only rule).
/// `detached` reaches this state by detach-by-request, which RETAINS its slot at
/// `Detached` and is resumable, so it is now named; `retired` left cleanly, the
/// sole path that removes the slot from `authority.slots`, so it stays absent.
#[test]
fn recipient_snapshot_is_postcommit_bound_and_resumable_detached_minus_sender()
-> Result<(), Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let conversation_id = 0xF0_52;
    let mut config = test_participant_config();
    config.identity_slots = 8;
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), config)?;
    let peer_a = enroll(
        &handler,
        conversation_id,
        ConnectionIncarnation::new(0x52, 1),
        0x21,
    )?;
    let peer_b = enroll(
        &handler,
        conversation_id,
        ConnectionIncarnation::new(0x52, 2),
        0x22,
    )?;
    let detached = enroll(
        &handler,
        conversation_id,
        ConnectionIncarnation::new(0x52, 3),
        0x23,
    )?;
    detach(&handler, conversation_id, detached, 0x24)?;
    let retired = enroll(
        &handler,
        conversation_id,
        ConnectionIncarnation::new(0x52, 4),
        0x25,
    )?;
    leave(&handler, conversation_id, retired, 0x26)?;

    let mut sender = enroll(
        &handler,
        conversation_id,
        ConnectionIncarnation::new(0x52, 5),
        0x27,
    )?;
    let mut persisted = vec![newest_produced(Arc::clone(&store), conversation_id)?];
    detach(&handler, conversation_id, sender, 0x28)?;
    persisted.push(newest_produced(Arc::clone(&store), conversation_id)?);
    sender = attach(
        &handler,
        conversation_id,
        sender,
        ConnectionIncarnation::new(0x52, 6),
        0x29,
    )?;
    persisted.push(newest_produced(Arc::clone(&store), conversation_id)?);
    sender = attach(
        &handler,
        conversation_id,
        sender,
        ConnectionIncarnation::new(0x52, 7),
        0x2A,
    )?;
    persisted.push(newest_produced(Arc::clone(&store), conversation_id)?);
    let _ = admit(
        &handler,
        conversation_id,
        sender,
        0x2B,
        vec![0, 0xFE, 0xFF, 0],
    )?;
    persisted.push(newest_produced(Arc::clone(&store), conversation_id)?);
    leave(&handler, conversation_id, sender, 0x2C)?;
    persisted.push(newest_produced(Arc::clone(&store), conversation_id)?);

    // Bound peers plus the resumable-Detached member, sorted ascending; the
    // cleanly-departed identity is absent by construction (its slot is gone).
    let expected = vec![
        peer_a.participant_id,
        peer_b.participant_id,
        detached.participant_id,
    ];
    for batch in &persisted {
        for record in batch.ordered_records() {
            assert_eq!(record.recipients(), expected);
            assert!(record.recipients().windows(2).all(|pair| pair[0] < pair[1]));
            assert_eq!(record.sender(), Some(sender.participant_id));
            assert!(record.recipients().contains(&detached.participant_id));
            assert!(!record.recipients().contains(&retired.participant_id));
        }
    }

    // Mutating today's bindings cannot rewrite any already-persisted source row.
    let exact_before = extension_rows(Arc::clone(&store), conversation_id)?;
    detach(&handler, conversation_id, peer_b, 0x2D)?;
    let _ = attach(
        &handler,
        conversation_id,
        peer_b,
        ConnectionIncarnation::new(0x52, 8),
        0x2E,
    )?;
    let exact_after = extension_rows(Arc::clone(&store), conversation_id)?;
    assert_eq!(&exact_after[..exact_before.len()], exact_before.as_slice());

    assert_system_marker_includes_live_target()
}
