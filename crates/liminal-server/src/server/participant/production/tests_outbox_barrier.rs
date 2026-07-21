use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};

use liminal::durability::bridge::block_on;
use liminal::durability::{DurableStore, open_ephemeral};
use liminal::protocol::{encode, encoded_len};
use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, EnrollmentRequest, EnrollmentToken, Generation,
    ParticipantAck, RecordAdmission, RecordAdmissionAttemptToken, ServerPush, ServerValue,
};

use crate::server::connection::ReadyWaker;
use crate::server::participant::{
    InstalledParticipantService, ParticipantConnectionContext, ParticipantConnectionConversations,
    ParticipantSemanticHandler, encode_server_push,
};

use super::ProductionParticipantHandler;
use super::log::STREAM_PREFIX;
use super::outbox_log::OUTBOX_STREAM_PREFIX;
use super::tests::test_participant_config;
use super::tests_outbox_barrier_fixture::{OutboxBarrierKind, OutboxBarrierStore};

const CONVERSATION: u64 = 0xF0_70;

fn apply(
    service: &InstalledParticipantService,
    incarnation: ConnectionIncarnation,
    request: ClientRequest,
) -> Result<ServerValue, Box<dyn Error>> {
    service
        .handle(
            ParticipantConnectionContext::new(incarnation),
            &mut ParticipantConnectionConversations::default(),
            request,
        )
        .map_err(Into::into)
}

fn enroll(
    service: &InstalledParticipantService,
    incarnation: ConnectionIncarnation,
    token: u8,
) -> Result<u64, Box<dyn Error>> {
    let value = apply(
        service,
        incarnation,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new([token; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(bound) = value else {
        return Err(format!("barrier enrollment did not bind: {value:?}").into());
    };
    Ok(bound.participant_id())
}

fn stream_count(store: &Arc<dyn DurableStore>, stream_key: &str) -> Result<usize, Box<dyn Error>> {
    Ok(block_on(store.read_from(stream_key, 0, 64))??.len())
}

fn assert_blocked_before_publication(
    inbox: &crate::server::participant::ParticipantPublicationInbox,
    wake_count: &AtomicU64,
    responses: &mpsc::Receiver<Result<ServerValue, String>>,
) -> Result<(), Box<dyn Error>> {
    assert!(!inbox.has_pending()?);
    assert_eq!(wake_count.load(Ordering::SeqCst), 0);
    assert!(matches!(
        responses.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    Ok(())
}

struct BarrierHarness {
    inner: Arc<dyn DurableStore>,
    barriers: Arc<OutboxBarrierStore>,
    service: Arc<InstalledParticipantService>,
    sender_incarnation: ConnectionIncarnation,
    peer_incarnation: ConnectionIncarnation,
    sender: u64,
    inbox: crate::server::participant::ParticipantPublicationInbox,
    wake_count: Arc<AtomicU64>,
    base_key: String,
    outbox_key: String,
    base_before: usize,
    outbox_before: usize,
}

fn prepare_barrier_harness() -> Result<BarrierHarness, Box<dyn Error>> {
    let inner: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let barriers = Arc::new(OutboxBarrierStore::new(Arc::clone(&inner)));
    let store: Arc<dyn DurableStore> = barriers.clone();
    let config = test_participant_config();
    let handler = Arc::new(ProductionParticipantHandler::new(
        Arc::clone(&store),
        config,
    )?);
    let semantic: Arc<dyn ParticipantSemanticHandler> = handler;
    let service = Arc::new(
        InstalledParticipantService::new(semantic, Arc::clone(&store), config.wire_frame_limit)
            .map_err(|error| format!("barrier service configuration failed: {error:?}"))?,
    );
    let sender_incarnation = ConnectionIncarnation::new(0x70, 1);
    let peer_incarnation = ConnectionIncarnation::new(0x70, 2);
    let sender = enroll(&service, sender_incarnation, 0x71)?;
    let peer = enroll(&service, peer_incarnation, 0x72)?;
    assert_ne!(sender, peer);
    let acked = apply(
        &service,
        sender_incarnation,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: CONVERSATION,
            participant_id: sender,
            capability_generation: Generation::ONE,
            through_seq: 2,
        }),
    )?;
    assert!(matches!(acked, ServerValue::AckCommitted(_)));
    let wake_count = Arc::new(AtomicU64::new(0));
    let inbox = service.new_publication_inbox();
    service.publication_registry().register(
        peer_incarnation,
        &inbox,
        ReadyWaker::for_test(Arc::clone(&wake_count)),
    )?;
    assert!(
        service
            .next_publication(peer_incarnation, CONVERSATION, None)?
            .is_none()
    );
    let base_key = format!("{STREAM_PREFIX}{CONVERSATION}");
    let outbox_key = format!("{OUTBOX_STREAM_PREFIX}{CONVERSATION}");
    let base_before = stream_count(&inner, &base_key)?;
    let outbox_before = stream_count(&inner, &outbox_key)?;
    barriers.arm([
        OutboxBarrierKind::OperationAppend,
        OutboxBarrierKind::OperationFlush,
        OutboxBarrierKind::OutboxAppend,
        OutboxBarrierKind::OutboxFlush,
    ])?;
    Ok(BarrierHarness {
        inner,
        barriers,
        service,
        sender_incarnation,
        peer_incarnation,
        sender,
        inbox,
        wake_count,
        base_key,
        outbox_key,
        base_before,
        outbox_before,
    })
}

#[test]
fn published_obligation_tells_exact_live_dispatch_once() -> Result<(), Box<dyn Error>> {
    let BarrierHarness {
        inner,
        barriers,
        service,
        sender_incarnation,
        peer_incarnation,
        sender,
        inbox,
        wake_count,
        base_key,
        outbox_key,
        base_before,
        outbox_before,
    } = prepare_barrier_harness()?;
    let (responses, received_responses) = mpsc::channel();
    let operation_service = Arc::clone(&service);
    let operation = std::thread::spawn(move || {
        let result = apply(
            &operation_service,
            sender_incarnation,
            ClientRequest::RecordAdmission(RecordAdmission {
                conversation_id: CONVERSATION,
                participant_id: sender,
                capability_generation: Generation::ONE,
                record_admission_attempt_token: RecordAdmissionAttemptToken::new([0x73; 16]),
                payload: vec![0, 0xFF, 0xA5, 0],
            }),
        )
        .map_err(|error| error.to_string());
        let _ = responses.send(result);
    });

    barriers.wait_for(OutboxBarrierKind::OperationAppend)?;
    assert_eq!(stream_count(&inner, &base_key)?, base_before);
    assert_eq!(stream_count(&inner, &outbox_key)?, outbox_before);
    assert_blocked_before_publication(&inbox, &wake_count, &received_responses)?;

    barriers.release(OutboxBarrierKind::OperationAppend)?;
    barriers.wait_for(OutboxBarrierKind::OperationFlush)?;
    assert_eq!(stream_count(&inner, &base_key)?, base_before + 1);
    assert_eq!(stream_count(&inner, &outbox_key)?, outbox_before);
    assert_blocked_before_publication(&inbox, &wake_count, &received_responses)?;

    barriers.release(OutboxBarrierKind::OperationFlush)?;
    barriers.wait_for(OutboxBarrierKind::OutboxAppend)?;
    assert_eq!(stream_count(&inner, &outbox_key)?, outbox_before);
    assert_blocked_before_publication(&inbox, &wake_count, &received_responses)?;

    barriers.release(OutboxBarrierKind::OutboxAppend)?;
    barriers.wait_for(OutboxBarrierKind::OutboxFlush)?;
    assert_eq!(stream_count(&inner, &outbox_key)?, outbox_before + 1);
    assert_blocked_before_publication(&inbox, &wake_count, &received_responses)?;

    barriers.release(OutboxBarrierKind::OutboxFlush)?;
    operation
        .join()
        .map_err(|_| "barrier operation thread panicked")?;
    let result = received_responses
        .recv()
        .map_err(|_| "barrier response channel closed")??;
    let ServerValue::RecordCommitted(committed) = result else {
        return Err(
            format!("barrier operation did not return its terminal answer: {result:?}").into(),
        );
    };
    assert_eq!(wake_count.load(Ordering::SeqCst), 1);
    let ready = inbox.take_ready()?;
    assert_eq!(ready.conversations, vec![CONVERSATION]);
    assert!(ready.observer_progressed.is_empty());

    let publication = service
        .next_publication(peer_incarnation, CONVERSATION, None)?
        .ok_or("outbox flush did not make the push eligible")?;
    assert_eq!(publication.delivery_seq(), committed.delivery_seq());
    let frame = encode_server_push(ServerPush::ParticipantDelivery(publication.delivery))
        .map_err(|error| format!("postflush push encoding failed: {error:?}"))?;
    let mut bytes = vec![0; encoded_len(&frame)?];
    let written = encode(&frame, &mut bytes)?;
    assert_eq!(written, bytes.len());
    Ok(())
}
