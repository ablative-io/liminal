//! Extension/base restore, two-barrier crash cuts, and pre-publication refusal.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use liminal::durability::bridge::block_on;
use liminal::durability::{DurabilityError, DurableStore, StoredEntry, open_ephemeral};
use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, EnrollmentRequest, EnrollmentToken,
};

use super::ProductionParticipantHandler;
use super::log::OperationLog;
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
    assert_eq!(block_on(base.read_page(0))??.len(), 2);
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
