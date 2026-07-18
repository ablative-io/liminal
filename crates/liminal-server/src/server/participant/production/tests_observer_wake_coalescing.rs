use super::*;

#[test]
fn stale_arm_reaches_progressed_after_wake_coalesced() -> Result<(), Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let service = installed(store)?;
    let conversation_id = 930;
    let (ack_incarnation, participant_id) = enroll_two(&service, conversation_id, 0x81)?;
    let third_incarnation = ConnectionIncarnation::new(0xF0, conversation_id * 2 + 2);
    let third = apply(
        &service,
        third_incarnation,
        &mut ParticipantConnectionConversations::default(),
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0x83; 16]),
        }),
    )?;
    assert!(matches!(third, ServerValue::EnrollBound(_)));

    let observer_incarnation = ConnectionIncarnation::new(0xF5, 1);
    let wake_count = Arc::new(AtomicU64::new(0));
    let inbox = register(&service, observer_incarnation, Arc::clone(&wake_count))?;
    let mut observer_conversations = ParticipantConnectionConversations::default();
    let older = recover(
        &service,
        observer_incarnation,
        &mut observer_conversations,
        conversation_id,
        0,
    )?;
    assert!(older.armed);
    assert!(!older.progressed);

    assert!(matches!(
        ack_through(
            &service,
            ack_incarnation,
            conversation_id,
            participant_id,
            2,
        )?,
        ServerValue::AckCommitted(_)
    ));
    assert!(inbox.has_pending()?);
    assert_eq!(wake_count.load(Ordering::SeqCst), 1);

    // Do not drain the older fired payload. Arm the same conversation at the
    // now-current, different refused epoch while the READY edge remains pending.
    let newer = recover(
        &service,
        observer_incarnation,
        &mut observer_conversations,
        conversation_id,
        2,
    )?;
    assert!(newer.armed);
    assert!(!newer.progressed);
    assert_eq!(newer.current_observer_progress, 2);

    assert!(matches!(
        ack_through(
            &service,
            ack_incarnation,
            conversation_id,
            participant_id,
            3,
        )?,
        ServerValue::AckCommitted(_)
    ));
    assert_eq!(
        wake_count.load(Ordering::SeqCst),
        1,
        "latest-per-conversation replacement must not emit a duplicate READY edge"
    );

    let ready = inbox.take_ready()?;
    assert!(ready.conversations.is_empty());
    let [publication] = ready.observer_progressed.as_slice() else {
        return Err("coalesced observer inbox did not contain exactly one latest payload".into());
    };
    assert_eq!(publication.conversation_id, conversation_id);
    assert_eq!(publication.refused_epoch, 2);
    assert_eq!(publication.observer_progress, 3);
    let drained = inbox.take_ready()?;
    assert!(drained.conversations.is_empty());
    assert!(drained.observer_progressed.is_empty());

    // The displaced older arm cannot hang: its ordinary recovery handshake is
    // a deterministic progressed result because durable progress exceeds zero.
    let recovered_older = recover(
        &service,
        observer_incarnation,
        &mut observer_conversations,
        conversation_id,
        0,
    )?;
    assert!(!recovered_older.armed);
    assert!(recovered_older.progressed);
    assert_eq!(recovered_older.current_observer_progress, 3);
    assert!(!inbox.has_pending()?);
    assert_eq!(wake_count.load(Ordering::SeqCst), 1);
    Ok(())
}
