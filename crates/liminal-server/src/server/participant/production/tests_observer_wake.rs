//! Acceptance 25: exact live observer wake targeting across both durable barriers.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use liminal::durability::bridge::block_on;
use liminal::durability::{DurableStore, open_ephemeral};
use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, EnrollmentRequest, EnrollmentToken, Generation,
    ObserverRecoveryHandshake, ObserverRefusal, ParticipantAck, ServerValue,
};

use crate::server::connection::ReadyWaker;
use crate::server::participant::{
    InstalledParticipantService, ParticipantConnectionContext, ParticipantConnectionConversations,
    ParticipantPublicationInbox, ParticipantSemanticHandler,
};

use super::ProductionParticipantHandler;
use super::tests::{open_disk_store_for_tests, test_participant_config};
use super::tests_observer_wake_fixture::{BarrierKind, ObserverBarrierStore};

const OBSERVER_STREAM_KEY: &str = "liminal:participant-observer-recovery";

fn installed(store: Arc<dyn DurableStore>) -> Result<InstalledParticipantService, Box<dyn Error>> {
    let config = test_participant_config();
    let handler = Arc::new(ProductionParticipantHandler::new(
        Arc::clone(&store),
        config,
    )?);
    InstalledParticipantService::new(handler, store, config.wire_frame_limit)
        .map_err(|error| format!("participant service configuration failed: {error:?}").into())
}

fn apply(
    service: &InstalledParticipantService,
    incarnation: ConnectionIncarnation,
    conversations: &mut ParticipantConnectionConversations,
    request: ClientRequest,
) -> Result<ServerValue, Box<dyn Error>> {
    service
        .handle(
            ParticipantConnectionContext::new(incarnation),
            conversations,
            request,
        )
        .map_err(Into::into)
}

fn enroll_two(
    service: &InstalledParticipantService,
    conversation_id: u64,
    token_seed: u8,
) -> Result<(ConnectionIncarnation, u64), Box<dyn Error>> {
    let incarnation_a = ConnectionIncarnation::new(0xF0, conversation_id * 2);
    let incarnation_b = ConnectionIncarnation::new(0xF0, conversation_id * 2 + 1);
    let first = apply(
        service,
        incarnation_a,
        &mut ParticipantConnectionConversations::default(),
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([token_seed; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(first) = first else {
        return Err(format!("first enrollment did not bind: {first:?}").into());
    };
    let second = apply(
        service,
        incarnation_b,
        &mut ParticipantConnectionConversations::default(),
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([token_seed.wrapping_add(1); 16]),
        }),
    )?;
    if !matches!(second, ServerValue::EnrollBound(_)) {
        return Err(format!("second enrollment did not bind: {second:?}").into());
    }
    Ok((incarnation_a, first.participant_id()))
}

fn register(
    service: &InstalledParticipantService,
    incarnation: ConnectionIncarnation,
    wake_count: Arc<AtomicU64>,
) -> Result<ParticipantPublicationInbox, Box<dyn Error>> {
    let inbox = service.new_publication_inbox();
    service.publication_registry().register(
        incarnation,
        &inbox,
        ReadyWaker::for_test(wake_count),
    )?;
    Ok(inbox)
}

fn recover(
    service: &InstalledParticipantService,
    incarnation: ConnectionIncarnation,
    conversations: &mut ParticipantConnectionConversations,
    conversation_id: u64,
    refused_epoch: u64,
) -> Result<liminal_protocol::wire::ObserverProgressStatus, Box<dyn Error>> {
    let value = apply(
        service,
        incarnation,
        conversations,
        ClientRequest::ObserverRecovery(ObserverRecoveryHandshake {
            observer_refusals: vec![ObserverRefusal {
                conversation_id,
                refused_epoch,
            }],
        }),
    )?;
    let ServerValue::ObserverRecoveryAccepted(accepted) = value else {
        return Err(format!("observer recovery was not accepted: {value:?}").into());
    };
    accepted
        .statuses
        .into_iter()
        .next()
        .ok_or_else(|| "observer recovery omitted its status".into())
}

fn ack(
    service: &InstalledParticipantService,
    incarnation: ConnectionIncarnation,
    conversation_id: u64,
    participant_id: u64,
) -> Result<ServerValue, Box<dyn Error>> {
    apply(
        service,
        incarnation,
        &mut ParticipantConnectionConversations::default(),
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id,
            participant_id,
            capability_generation: Generation::ONE,
            through_seq: 2,
        }),
    )
}

fn assert_one_wake(
    inbox: &ParticipantPublicationInbox,
    conversation_id: u64,
) -> Result<u64, Box<dyn Error>> {
    let ready = inbox.take_ready()?;
    assert!(ready.conversations.is_empty());
    assert_eq!(ready.observer_progressed.len(), 1);
    let publication = ready
        .observer_progressed
        .into_iter()
        .next()
        .ok_or("observer wake disappeared")?;
    assert_eq!(publication.conversation_id, conversation_id);
    assert_eq!(publication.refused_epoch, 0);
    assert!(publication.observer_progress > 0);
    let second = inbox.take_ready()?;
    assert!(second.conversations.is_empty());
    assert!(second.observer_progressed.is_empty());
    Ok(publication.observer_progress)
}

fn assert_progressed(
    service: &InstalledParticipantService,
    incarnation: ConnectionIncarnation,
    conversation_id: u64,
    expected_progress: Option<u64>,
) -> Result<(), Box<dyn Error>> {
    let status = recover(
        service,
        incarnation,
        &mut ParticipantConnectionConversations::default(),
        conversation_id,
        0,
    )?;
    assert!(!status.armed);
    assert!(status.progressed);
    if let Some(expected) = expected_progress {
        assert_eq!(status.current_observer_progress, expected);
    }
    Ok(())
}

fn observer_advance_rows(store: &Arc<dyn DurableStore>) -> Result<usize, Box<dyn Error>> {
    Ok(block_on(store.read_from(OBSERVER_STREAM_KEY, 0, 128))??
        .into_iter()
        .filter(|entry| {
            entry
                .payload
                .windows(b"\"row\":\"advance\"".len())
                .any(|window| window == b"\"row\":\"advance\"")
        })
        .count())
}

#[test]
fn observer_progressed_fires_after_source_and_advance_flushes() -> Result<(), Box<dyn Error>> {
    // Gate both ordered barriers in one real acknowledgement. Neither the
    // source append nor the uncommitted Advance append can publish; releasing
    // the Advance flush transfers exactly one payload and one READY edge.
    let inner: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let barriers = Arc::new(ObserverBarrierStore::new(inner));
    let store: Arc<dyn DurableStore> = barriers.clone();
    let service = Arc::new(installed(store)?);
    let conversation_id = 901;
    let (ack_incarnation, participant_id) = enroll_two(&service, conversation_id, 0x31)?;
    let observer_incarnation = ConnectionIncarnation::new(0xF1, 1);
    let wake_count = Arc::new(AtomicU64::new(0));
    let inbox = register(&service, observer_incarnation, Arc::clone(&wake_count))?;
    let mut observer_conversations = ParticipantConnectionConversations::default();
    let armed = recover(
        &service,
        observer_incarnation,
        &mut observer_conversations,
        conversation_id,
        0,
    )?;
    assert!(armed.armed);
    assert!(!armed.progressed);

    barriers.arm([BarrierKind::Source, BarrierKind::Advance])?;
    let ack_service = Arc::clone(&service);
    let ack_thread = std::thread::spawn(move || {
        ack(
            &ack_service,
            ack_incarnation,
            conversation_id,
            participant_id,
        )
        .map_err(|error| error.to_string())
    });
    barriers.wait_for(BarrierKind::Source)?;
    assert!(!inbox.has_pending()?);
    assert_eq!(wake_count.load(Ordering::SeqCst), 0);
    barriers.release(BarrierKind::Source)?;
    barriers.wait_for(BarrierKind::Advance)?;
    assert!(!inbox.has_pending()?);
    assert_eq!(wake_count.load(Ordering::SeqCst), 0);
    barriers.release(BarrierKind::Advance)?;
    let acked = ack_thread
        .join()
        .map_err(|_| "acknowledgement thread panicked")??;
    assert!(matches!(acked, ServerValue::AckCommitted(_)));
    let progress = assert_one_wake(&inbox, conversation_id)?;
    assert_eq!(wake_count.load(Ordering::SeqCst), 1);

    // Advance-first: a normal reattach observes durable progress in its
    // handshake and installs no arm or wake.
    let advance_first_incarnation = ConnectionIncarnation::new(0xF1, 2);
    let advance_first_inbox = register(
        &service,
        advance_first_incarnation,
        Arc::new(AtomicU64::new(0)),
    )?;
    assert_progressed(
        &service,
        advance_first_incarnation,
        conversation_id,
        Some(progress),
    )?;
    assert!(!advance_first_inbox.has_pending()?);

    // Reattach-first: the second accepted handshake replaces the old weak arm
    // owner under the observer mutex, and only that exact target receives the
    // next conversation's fired payload.
    let replace_conversation = 902;
    let (replace_ack_incarnation, replace_participant) =
        enroll_two(&service, replace_conversation, 0x41)?;
    let old_incarnation = ConnectionIncarnation::new(0xF1, 3);
    let new_incarnation = ConnectionIncarnation::new(0xF1, 4);
    let old_inbox = register(&service, old_incarnation, Arc::new(AtomicU64::new(0)))?;
    let new_inbox = register(&service, new_incarnation, Arc::new(AtomicU64::new(0)))?;
    assert!(
        recover(
            &service,
            old_incarnation,
            &mut ParticipantConnectionConversations::default(),
            replace_conversation,
            0,
        )?
        .armed
    );
    assert!(
        recover(
            &service,
            new_incarnation,
            &mut ParticipantConnectionConversations::default(),
            replace_conversation,
            0,
        )?
        .armed
    );
    assert!(matches!(
        ack(
            &service,
            replace_ack_incarnation,
            replace_conversation,
            replace_participant,
        )?,
        ServerValue::AckCommitted(_)
    ));
    assert!(!old_inbox.has_pending()?);
    assert_one_wake(&new_inbox, replace_conversation)?;

    // A dead arm owner is not replaced by a default target and is never
    // broadcast. Reattach recovers only through the ordinary progressed
    // handshake.
    let dead_conversation = 903;
    let (dead_ack_incarnation, dead_participant) = enroll_two(&service, dead_conversation, 0x51)?;
    let dead_incarnation = ConnectionIncarnation::new(0xF1, 5);
    let dead_inbox = register(&service, dead_incarnation, Arc::new(AtomicU64::new(0)))?;
    assert!(
        recover(
            &service,
            dead_incarnation,
            &mut ParticipantConnectionConversations::default(),
            dead_conversation,
            0,
        )?
        .armed
    );
    service.publication_registry().deregister(dead_incarnation);
    drop(dead_inbox);
    let bystander_incarnation = ConnectionIncarnation::new(0xF1, 6);
    let bystander = register(&service, bystander_incarnation, Arc::new(AtomicU64::new(0)))?;
    assert!(matches!(
        ack(
            &service,
            dead_ack_incarnation,
            dead_conversation,
            dead_participant,
        )?,
        ServerValue::AckCommitted(_)
    ));
    assert!(!bystander.has_pending()?);
    assert_progressed(&service, bystander_incarnation, dead_conversation, None)?;

    // Both flush-fault cuts publish nothing live. Dropping every live owner and
    // reopening the disk repairs/accepts the committed source and makes the
    // normal recovery handshake report progressed before any target is armed.
    for (index, fault) in [BarrierKind::Source, BarrierKind::Advance]
        .into_iter()
        .enumerate()
    {
        let home = tempfile::tempdir()?;
        let data_dir = home.path().join("durability");
        let fault_conversation = 910 + u64::try_from(index)?;
        {
            let inner = open_disk_store_for_tests(&data_dir)?;
            let fault_store = Arc::new(ObserverBarrierStore::new(inner));
            let store: Arc<dyn DurableStore> = fault_store.clone();
            let fault_service = installed(Arc::clone(&store))?;
            let (fault_ack_incarnation, fault_participant) = enroll_two(
                &fault_service,
                fault_conversation,
                0x61 + u8::try_from(index)?,
            )?;
            let fault_observer = ConnectionIncarnation::new(0xF2, u64::try_from(index)? + 1);
            let fault_inbox =
                register(&fault_service, fault_observer, Arc::new(AtomicU64::new(0)))?;
            assert!(
                recover(
                    &fault_service,
                    fault_observer,
                    &mut ParticipantConnectionConversations::default(),
                    fault_conversation,
                    0,
                )?
                .armed
            );
            let advances_before = observer_advance_rows(&store)?;
            fault_store.fail_next(fault)?;
            assert!(
                ack(
                    &fault_service,
                    fault_ack_incarnation,
                    fault_conversation,
                    fault_participant,
                )
                .is_err()
            );
            assert!(!fault_inbox.has_pending()?);
            if fault == BarrierKind::Source {
                assert_eq!(observer_advance_rows(&store)?, advances_before);
            }
        }
        let reopened_store = open_disk_store_for_tests(&data_dir)?;
        let reopened = installed(reopened_store)?;
        let reattach_incarnation = ConnectionIncarnation::new(0xF3, u64::try_from(index)? + 1);
        let reattach_inbox =
            register(&reopened, reattach_incarnation, Arc::new(AtomicU64::new(0)))?;
        assert_progressed(&reopened, reattach_incarnation, fault_conversation, None)?;
        assert!(!reattach_inbox.has_pending()?);
    }

    // Successful Advance flush followed by a process cut before pump handoff:
    // the queued weak target dies, while cold reopen retains durable progress.
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let cut_conversation = 920;
    {
        let store = open_disk_store_for_tests(&data_dir)?;
        let cut_service = installed(store)?;
        let (cut_ack_incarnation, cut_participant) =
            enroll_two(&cut_service, cut_conversation, 0x71)?;
        let cut_observer = ConnectionIncarnation::new(0xF4, 1);
        let cut_inbox = register(&cut_service, cut_observer, Arc::new(AtomicU64::new(0)))?;
        assert!(
            recover(
                &cut_service,
                cut_observer,
                &mut ParticipantConnectionConversations::default(),
                cut_conversation,
                0,
            )?
            .armed
        );
        assert!(matches!(
            ack(
                &cut_service,
                cut_ack_incarnation,
                cut_conversation,
                cut_participant,
            )?,
            ServerValue::AckCommitted(_)
        ));
        assert!(cut_inbox.has_pending()?);
    }
    let reopened_store = open_disk_store_for_tests(&data_dir)?;
    let reopened = installed(reopened_store)?;
    let cut_reattach = ConnectionIncarnation::new(0xF4, 2);
    let cut_reattach_inbox = register(&reopened, cut_reattach, Arc::new(AtomicU64::new(0)))?;
    assert_progressed(&reopened, cut_reattach, cut_conversation, None)?;
    assert!(!cut_reattach_inbox.has_pending()?);
    Ok(())
}
