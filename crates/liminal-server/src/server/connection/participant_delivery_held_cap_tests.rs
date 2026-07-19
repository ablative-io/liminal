use super::*;

#[test]
fn combined_held_heads_refuse_at_signed_cap_without_close_or_loss()
-> Result<(), Box<dyn std::error::Error>> {
    const HELD_LIMIT: u64 = 1;
    let expected_delivery = delivery(1, 1);
    let source = Arc::new(FixtureSource::with_conversation_limit(
        [expected_delivery.clone()],
        HELD_LIMIT,
    ));
    let service = service(Arc::clone(&source))?;
    let mut state = state(&service, &[expected_delivery.conversation_id])?;
    let mut sink = RecordingSink::new(4096);
    sink.fill_current_room();
    let budget = usize::try_from(HELD_LIMIT)?;

    assert_eq!(
        service_participant_publications(&mut state, &service, &mut sink, budget)?,
        0
    );
    assert_eq!(state.held_pushes.participant_len(), budget);
    assert_eq!(state.held_pushes.observer_len(), 0);

    let observer = ObserverPublication {
        conversation_id: expected_delivery.conversation_id,
        refused_epoch: 7,
        observer_progress: 9,
    };
    state
        .participant_publication
        .as_ref()
        .ok_or("missing publication inbox")?
        .requeue_observers([observer])?;

    match service_participant_publications(&mut state, &service, &mut sink, budget) {
        Err(ParticipantPumpError::Publication(ParticipantPublicationError::InboxCapacity {
            limit,
        })) => assert_eq!(limit, HELD_LIMIT),
        other => {
            return Err(format!(
                "combined held-head edge was not typed capacity refusal: {other:?}"
            )
            .into());
        }
    }
    let held_count = state
        .held_pushes
        .participant_len()
        .checked_add(state.held_pushes.observer_len())
        .ok_or("combined held push count overflowed")?;
    assert_eq!(u64::try_from(held_count)?, HELD_LIMIT);
    assert!(state.held_pushes.capacity_refused());
    assert!(
        state
            .participant_publication
            .as_ref()
            .ok_or("missing publication inbox")?
            .has_pending()?,
        "the refused observer and durable participant work must remain pending"
    );

    sink.writable();
    let resumed_budget = budget
        .checked_add(budget)
        .ok_or("combined held push resume budget overflowed")?;
    assert_eq!(
        service_participant_publications(&mut state, &service, &mut sink, resumed_budget)?,
        resumed_budget
    );
    let pushes = sink
        .frames
        .iter()
        .map(decode_push)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        pushes,
        vec![
            observer.into_server_push(),
            ServerPush::ParticipantDelivery(expected_delivery)
        ]
    );
    assert!(state.held_pushes.is_empty());
    assert!(!state.held_pushes.capacity_refused());
    assert_eq!(source.offered(), vec![(1, 1)]);
    Ok(())
}
