//! Extension/base restore, two-barrier crash cuts, and pre-publication refusal.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use liminal::durability::bridge::block_on;
use liminal::durability::{DurabilityError, DurableStore, StoredEntry, open_ephemeral};
use liminal_protocol::wire::{
    AttachAttemptToken, ClientRequest, ConnectionIncarnation, CredentialAttachRequest, EnrollBound,
    EnrollmentRequest, EnrollmentToken, ReceiptReplay, ServerValue,
};

use crate::server::connection::ReadyWaker;
use crate::server::participant::{
    InstalledParticipantService, ParticipantConnectionContext, ParticipantConnectionConversations,
    ParticipantOfferedProgress, ParticipantSemanticHandler,
};

use super::ProductionParticipantHandler;
use super::log::{OperationLog, OperationSchemaPhase};
use super::outbox_log::{OUTBOX_STREAM_PREFIX, OutboxLog, OutboxRow};
use super::tests::{dispatch, test_participant_config};

const CONVERSATION: u64 = 0xF0_C4;

#[derive(Debug)]
struct OutboxFaultStore {
    inner: Arc<dyn DurableStore>,
    fail_append: AtomicBool,
    fail_flush: AtomicBool,
    outbox_flush_pending: AtomicBool,
}

impl OutboxFaultStore {
    fn new(inner: Arc<dyn DurableStore>) -> Self {
        Self {
            inner,
            fail_append: AtomicBool::new(false),
            fail_flush: AtomicBool::new(false),
            outbox_flush_pending: AtomicBool::new(false),
        }
    }
}

#[async_trait::async_trait]
impl DurableStore for OutboxFaultStore {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        let outbox = stream_key.starts_with(OUTBOX_STREAM_PREFIX);
        if outbox && self.fail_append.load(Ordering::SeqCst) {
            return Err(DurabilityError::SequenceConflict {
                expected: expected_seq,
                actual: u64::MAX,
            });
        }
        let assigned = self.inner.append(stream_key, payload, expected_seq).await?;
        self.outbox_flush_pending.store(outbox, Ordering::SeqCst);
        Ok(assigned)
    }

    async fn read_from(
        &self,
        stream_key: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, DurabilityError> {
        self.inner.read_from(stream_key, offset, limit).await
    }

    async fn cas(&self, key: &str, old_value: u64, new_value: u64) -> Result<(), DurabilityError> {
        self.inner.cas(key, old_value, new_value).await
    }

    async fn read_value(&self, key: &str) -> Result<Option<u64>, DurabilityError> {
        self.inner.read_value(key).await
    }

    async fn scan(&self, prefix: &str) -> Result<Vec<StoredEntry>, DurabilityError> {
        self.inner.scan(prefix).await
    }

    async fn flush(&self) -> Result<(), DurabilityError> {
        let outbox_pending = self.outbox_flush_pending.swap(false, Ordering::SeqCst);
        if outbox_pending && self.fail_flush.load(Ordering::SeqCst) {
            return Err(DurabilityError::SequenceConflict {
                expected: 0,
                actual: u64::MAX,
            });
        }
        self.inner.flush().await
    }
}

fn enrollment() -> ClientRequest {
    ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: CONVERSATION,
        enrollment_token: EnrollmentToken::new([0xC4; 16]),
    })
}

fn extension_count(store: &Arc<dyn DurableStore>) -> Result<usize, Box<dyn Error>> {
    let key = format!("{OUTBOX_STREAM_PREFIX}{CONVERSATION}");
    Ok(block_on(store.read_from(&key, 0, 16))??.len())
}

fn exercise_second_barrier_cut(
    fail_append: bool,
    fail_flush: bool,
    expected_rows_after_fault: usize,
) -> Result<(), Box<dyn Error>> {
    let inner: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let faults = Arc::new(OutboxFaultStore::new(Arc::clone(&inner)));
    faults.fail_append.store(fail_append, Ordering::SeqCst);
    faults.fail_flush.store(fail_flush, Ordering::SeqCst);
    let store: Arc<dyn DurableStore> = faults.clone();
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), test_participant_config())?;
    let result = dispatch(&handler, ConnectionIncarnation::new(90, 1), enrollment());
    assert!(result.is_err(), "second-barrier fault published a response");

    let base = OperationLog::new(Arc::clone(&store), CONVERSATION);
    assert_eq!(
        block_on(base.read_page(0, OperationSchemaPhase::V2Prefix))??
            .rows
            .len(),
        2
    );
    assert_eq!(extension_count(&store)?, expected_rows_after_fault);
    drop(handler);

    faults.fail_append.store(false, Ordering::SeqCst);
    faults.fail_flush.store(false, Ordering::SeqCst);
    let repaired =
        ProductionParticipantHandler::new(Arc::clone(&store), test_participant_config())?;
    assert_eq!(extension_count(&store)?, 1);
    drop(repaired);

    let restored_again =
        ProductionParticipantHandler::new(Arc::clone(&store), test_participant_config())?;
    assert_eq!(extension_count(&store)?, 1);
    drop(restored_again);
    Ok(())
}

#[test]
fn postcommit_outbox_append_failure_repairs_before_publication() -> Result<(), Box<dyn Error>> {
    exercise_second_barrier_cut(true, false, 0)
}

#[test]
fn uncertain_outbox_flush_accepts_exact_existing_without_duplicate() -> Result<(), Box<dyn Error>> {
    exercise_second_barrier_cut(false, true, 1)
}

#[test]
fn malformed_unknown_and_mixed_extension_refuses_before_publication() -> Result<(), Box<dyn Error>>
{
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), test_participant_config())?;
    let enrolled = dispatch(&handler, ConnectionIncarnation::new(91, 1), enrollment())?;
    assert!(matches!(
        enrolled,
        liminal_protocol::wire::ServerValue::EnrollBound(_)
    ));
    drop(handler);

    let key = format!("{OUTBOX_STREAM_PREFIX}{CONVERSATION}");
    let assigned = block_on(store.append(&key, vec![2], 1))??;
    assert_eq!(assigned, 1);
    block_on(store.flush())??;
    let result = ProductionParticipantHandler::new(store, test_participant_config());
    let Err(error) = result else {
        return Err("mixed extension stream unexpectedly published authority".into());
    };
    assert!(
        error
            .to_string()
            .contains("mixed Unit 2 extension schema versions")
    );
    Ok(())
}

#[test]
fn impossible_extension_boundary_refuses_before_publication() -> Result<(), Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), test_participant_config())?;
    let enrolled = dispatch(&handler, ConnectionIncarnation::new(92, 1), enrollment())?;
    assert!(matches!(
        enrolled,
        liminal_protocol::wire::ServerValue::EnrollBound(_)
    ));
    drop(handler);

    let outbox = OutboxLog::new(Arc::clone(&store), CONVERSATION);
    block_on(outbox.append(
        &OutboxRow::AckAdvanced {
            source_log_sequence: 99,
            participant_id: 0,
            through_seq: 1,
        },
        1,
    ))??;
    let result = ProductionParticipantHandler::new(store, test_participant_config());
    let Err(error) = result else {
        return Err("impossible extension boundary unexpectedly published authority".into());
    };
    assert!(error.to_string().contains("impossible future boundary"));
    Ok(())
}

fn enrollment_with(token: u8) -> ClientRequest {
    ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: CONVERSATION,
        enrollment_token: EnrollmentToken::new([token; 16]),
    })
}

fn raw_extension_payloads(store: &Arc<dyn DurableStore>) -> Result<Vec<Vec<u8>>, Box<dyn Error>> {
    let key = format!("{OUTBOX_STREAM_PREFIX}{CONVERSATION}");
    Ok(block_on(store.read_from(&key, 0, 16))??
        .into_iter()
        .map(|entry| entry.payload)
        .collect())
}

fn installed_service(
    handler: Arc<ProductionParticipantHandler>,
    store: Arc<dyn DurableStore>,
) -> Result<InstalledParticipantService, Box<dyn Error>> {
    let config = test_participant_config();
    let semantic: Arc<dyn ParticipantSemanticHandler> = handler;
    InstalledParticipantService::new(semantic, store, config.wire_frame_limit)
        .map_err(|error| format!("repair service configuration failed: {error:?}").into())
}

fn apply_service(
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

fn expected_second_enrollment_payload() -> Result<Vec<u8>, Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), test_participant_config())?;
    let first = dispatch(
        &handler,
        ConnectionIncarnation::new(0xC4, 1),
        enrollment_with(0xC1),
    )?;
    assert!(matches!(first, ServerValue::EnrollBound(_)));
    let second = dispatch(
        &handler,
        ConnectionIncarnation::new(0xC4, 2),
        enrollment_with(0xC2),
    )?;
    assert!(matches!(second, ServerValue::EnrollBound(_)));
    raw_extension_payloads(&store)?
        .into_iter()
        .nth(1)
        .ok_or_else(|| "healthy second enrollment omitted its outbox row".into())
}

fn durable_ack_through(
    handler: &ProductionParticipantHandler,
    participant_id: u64,
) -> Result<u64, Box<dyn Error>> {
    let cell = handler.cell(CONVERSATION)?;
    let owner = cell
        .lock()
        .map_err(|_| "repaired conversation owner lock was poisoned")?;
    let authority = owner
        .as_ref()
        .ok_or("repaired conversation owner was absent")?;
    let outbox = authority
        .outbox
        .as_ref()
        .ok_or("repaired conversation outbox was absent")?;
    let cursor = outbox.durable_ack_through(participant_id);
    drop(owner);
    Ok(cursor)
}

fn replay_then_commit_rebind(
    repaired: &InstalledParticipantService,
    repaired_handler: &ProductionParticipantHandler,
    first_bound: &EnrollBound,
    first_incarnation: ConnectionIncarnation,
    second_incarnation: ConnectionIncarnation,
    exact_request: ClientRequest,
) -> Result<(), Box<dyn Error>> {
    // Construction returns only after cold first-touch reconciliation. Neither
    // startup, registration, nor an exact-token replay commits an impact or tell.
    let repaired_wakes = Arc::new(AtomicU64::new(u64::from(false)));
    let repaired_inbox = repaired.new_publication_inbox();
    repaired.publication_registry().register(
        first_incarnation,
        &repaired_inbox,
        ReadyWaker::for_test(Arc::clone(&repaired_wakes)),
    )?;
    assert!(!repaired_inbox.has_pending()?);
    assert_eq!(repaired_wakes.load(Ordering::SeqCst), u64::from(false));
    let retry = apply_service(repaired, second_incarnation, exact_request)?;
    let ServerValue::Bound(ReceiptReplay::Enrollment(bound)) = retry else {
        return Err(format!("exact-token retry lost its terminal answer: {retry:?}").into());
    };
    assert_eq!(bound.token(), EnrollmentToken::new([0xC2; 16]));
    assert_eq!(bound.conversation_id(), CONVERSATION);
    assert!(!repaired_inbox.has_pending()?);
    assert_eq!(repaired_wakes.load(Ordering::SeqCst), u64::from(false));

    let reconciled_cursor = durable_ack_through(repaired_handler, first_bound.participant_id())?;
    let rebound_incarnation = ConnectionIncarnation::new(0xC4, 3);
    let rebound_inbox = repaired.new_publication_inbox();
    repaired.publication_registry().register(
        rebound_incarnation,
        &rebound_inbox,
        ReadyWaker::for_test(Arc::clone(&repaired_wakes)),
    )?;
    assert!(!rebound_inbox.has_pending()?);
    assert_eq!(repaired_wakes.load(Ordering::SeqCst), u64::from(false));

    let rebound = apply_service(
        repaired,
        rebound_incarnation,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: CONVERSATION,
            participant_id: first_bound.participant_id(),
            capability_generation: first_bound.capability_generation(),
            attach_secret: first_bound.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([0xC3; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    if !matches!(rebound, ServerValue::AttachBound(_)) {
        return Err(format!("committed rebind did not bind: {rebound:?}").into());
    }
    assert_eq!(repaired_wakes.load(Ordering::SeqCst), u64::from(true));
    assert!(!repaired_inbox.has_pending()?);
    let ready = rebound_inbox.take_ready()?;
    assert_eq!(ready.conversations, vec![CONVERSATION]);
    assert!(ready.observer_progressed.is_empty());

    let repaired_obligation = repaired
        .next_publication(rebound_incarnation, CONVERSATION, None)?
        .ok_or("committed rebind did not select the repaired obligation")?;
    assert_eq!(
        repaired_obligation.participant_id,
        first_bound.participant_id()
    );
    assert!(repaired_obligation.delivery_seq() > reconciled_cursor);
    let offered = ParticipantOfferedProgress {
        binding_epoch: repaired_obligation.binding_epoch,
        through_seq: repaired_obligation.delivery_seq(),
    };
    assert!(
        repaired
            .next_publication(rebound_incarnation, CONVERSATION, Some(offered))?
            .is_none(),
        "repaired obligation was not dispatched exactly once"
    );
    Ok(())
}

fn exercise_repair_and_retry(
    fail_append: bool,
    fail_flush: bool,
    expected_rows_after_fault: usize,
    expected_second_payload: &[u8],
) -> Result<(), Box<dyn Error>> {
    let inner: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let faults = Arc::new(OutboxFaultStore::new(Arc::clone(&inner)));
    let store: Arc<dyn DurableStore> = faults.clone();
    let config = test_participant_config();
    let handler = Arc::new(ProductionParticipantHandler::new(
        Arc::clone(&store),
        config,
    )?);
    let service = installed_service(handler, Arc::clone(&store))?;
    let first_incarnation = ConnectionIncarnation::new(0xC4, 1);
    let second_incarnation = ConnectionIncarnation::new(0xC4, 2);
    let first = apply_service(&service, first_incarnation, enrollment_with(0xC1))?;
    let ServerValue::EnrollBound(first_bound) = first else {
        return Err(format!("first enrollment did not bind: {first:?}").into());
    };

    let wake_count = Arc::new(AtomicU64::new(0));
    let inbox = service.new_publication_inbox();
    service.publication_registry().register(
        first_incarnation,
        &inbox,
        ReadyWaker::for_test(Arc::clone(&wake_count)),
    )?;
    faults.fail_append.store(fail_append, Ordering::SeqCst);
    faults.fail_flush.store(fail_flush, Ordering::SeqCst);
    let exact_request = enrollment_with(0xC2);
    assert!(
        apply_service(&service, second_incarnation, exact_request.clone()).is_err(),
        "barrier-2 fault fabricated a successful terminal answer"
    );
    assert!(!inbox.has_pending()?);
    assert_eq!(wake_count.load(Ordering::SeqCst), 0);

    let base = OperationLog::new(Arc::clone(&store), CONVERSATION);
    assert_eq!(
        block_on(base.read_page(0, OperationSchemaPhase::V2Prefix))??
            .rows
            .len(),
        3,
        "both enrollments plus genesis remain durable after barrier-2 failure"
    );
    assert_eq!(
        raw_extension_payloads(&store)?.len(),
        expected_rows_after_fault
    );
    drop(service);

    faults.fail_append.store(false, Ordering::SeqCst);
    faults.fail_flush.store(false, Ordering::SeqCst);
    let repaired_handler = Arc::new(ProductionParticipantHandler::new(
        Arc::clone(&store),
        config,
    )?);
    let repaired_payloads = raw_extension_payloads(&store)?;
    assert_eq!(repaired_payloads.len(), 2);
    assert_eq!(repaired_payloads[1], expected_second_payload);

    let repaired = installed_service(Arc::clone(&repaired_handler), Arc::clone(&store))?;
    replay_then_commit_rebind(
        &repaired,
        &repaired_handler,
        &first_bound,
        first_incarnation,
        second_incarnation,
        exact_request,
    )?;

    let payloads_after_rebind = raw_extension_payloads(&store)?;
    drop(repaired);
    let restored_again =
        ProductionParticipantHandler::new(Arc::clone(&store), test_participant_config())?;
    assert_eq!(raw_extension_payloads(&store)?, payloads_after_rebind);
    drop(restored_again);
    Ok(())
}

#[test]
fn postcommit_outbox_failure_is_repaired_not_rolled_back() -> Result<(), Box<dyn Error>> {
    let expected = expected_second_enrollment_payload()?;
    exercise_repair_and_retry(true, false, 1, &expected)?;
    exercise_repair_and_retry(false, true, 2, &expected)
}
