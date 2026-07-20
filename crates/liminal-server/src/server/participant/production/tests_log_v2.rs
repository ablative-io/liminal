//! Participant production-log v2 migration contract.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use liminal::durability::bridge::block_on;
use liminal::durability::{DurabilityError, DurableStore, StoredEntry, open_ephemeral};
use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, EnrollmentRequest, EnrollmentToken, Generation,
    LeaveAttemptToken, LeaveRequest, RecordAdmission, RecordAdmissionAttemptToken, ServerValue,
};

use super::ProductionParticipantHandler;
use super::log::{
    DecodedStoredOperation, OperationLog, OperationLogError, OperationSchemaPhase, SCHEMA_VERSION,
    STREAM_PREFIX, StoredOperation,
};
use super::tests::{dispatch, open_disk_store_for_tests, test_participant_config};

const CONVERSATION: u64 = 0xF0C4;

#[derive(Debug)]
struct FaultStore {
    inner: Arc<dyn DurableStore>,
    fail_append: AtomicBool,
    fail_flush: AtomicBool,
}

#[async_trait::async_trait]
impl DurableStore for FaultStore {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        if self.fail_append.load(Ordering::SeqCst) && stream_key.starts_with(STREAM_PREFIX) {
            return Err(DurabilityError::SequenceConflict {
                expected: expected_seq,
                actual: u64::MAX,
            });
        }
        self.inner.append(stream_key, payload, expected_seq).await
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
        if self.fail_flush.load(Ordering::SeqCst) {
            return Err(DurabilityError::SequenceConflict {
                expected: 0,
                actual: u64::MAX,
            });
        }
        self.inner.flush().await
    }
}

fn store() -> Result<Arc<dyn DurableStore>, Box<dyn Error>> {
    Ok(Arc::new(open_ephemeral(1)?))
}

fn append_literal(
    store: &Arc<dyn DurableStore>,
    conversation_id: u64,
    sequence: u64,
    bytes: &[u8],
) -> Result<(), Box<dyn Error>> {
    let stream_key = format!("{STREAM_PREFIX}{conversation_id}");
    let assigned = block_on(store.append(&stream_key, bytes.to_vec(), sequence))??;
    assert_eq!(assigned, sequence);
    block_on(store.flush())??;
    Ok(())
}

#[test]
fn empty_stream_and_all_v2_stream_restore() -> Result<(), Box<dyn Error>> {
    let store = store()?;
    let log = OperationLog::new(Arc::clone(&store), CONVERSATION);
    assert!(
        block_on(log.read_page(0, OperationSchemaPhase::V2Prefix))??
            .rows
            .is_empty()
    );

    block_on(log.append(&StoredOperation::Genesis { event: Vec::new() }, 0))??;
    let page = block_on(log.read_page(0, OperationSchemaPhase::V2Prefix))??;
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0].sequence, 0);
    assert!(matches!(
        page.rows[0].operation,
        DecodedStoredOperation::V3(StoredOperation::Genesis { .. })
    ));

    let raw = block_on(store.read_from(&format!("{STREAM_PREFIX}{CONVERSATION}"), 0, 1))??;
    let json: serde_json::Value = serde_json::from_slice(&raw[0].payload)?;
    assert_eq!(json["schema_version"], SCHEMA_VERSION);
    Ok(())
}

#[test]
fn literal_v1_row_is_rejected_as_schema_version_one() -> Result<(), Box<dyn Error>> {
    let store = store()?;
    append_literal(
        &store,
        CONVERSATION,
        0,
        br#"{"schema_version":1,"operation":{"operation":"genesis","event":[]}}"#,
    )?;
    let log = OperationLog::new(store, CONVERSATION);
    let result = block_on(log.read_page(0, OperationSchemaPhase::V2Prefix))?;
    assert!(matches!(result, Err(OperationLogError::SchemaVersion(1))));
    Ok(())
}

#[test]
fn mixed_v2_then_v1_page_is_rejected_before_returning_any_rows() -> Result<(), Box<dyn Error>> {
    let store = store()?;
    let log = OperationLog::new(Arc::clone(&store), CONVERSATION);
    block_on(log.append(&StoredOperation::Genesis { event: Vec::new() }, 0))??;
    append_literal(
        &store,
        CONVERSATION,
        1,
        br#"{"schema_version":1,"operation":{"operation":"genesis","event":[]}}"#,
    )?;

    let result = block_on(log.read_page(0, OperationSchemaPhase::V2Prefix))?;
    assert!(matches!(result, Err(OperationLogError::SchemaVersion(1))));
    Ok(())
}

#[test]
fn mixed_stream_fails_startup_without_publishing_an_authority() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let conversation_id = 0xF0C5;
    {
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler =
            ProductionParticipantHandler::new(Arc::clone(&store), test_participant_config())?;
        let enrolled = dispatch(
            &handler,
            ConnectionIncarnation::new(44, 1),
            ClientRequest::Enrollment(EnrollmentRequest {
                conversation_id,
                enrollment_token: EnrollmentToken::new([44; 16]),
            }),
        )?;
        assert!(matches!(enrolled, ServerValue::EnrollBound(_)));
        drop(handler);
        append_literal(
            &store,
            conversation_id,
            2,
            br#"{"schema_version":1,"operation":{"operation":"genesis","event":[]}}"#,
        )?;
    }

    let store = open_disk_store_for_tests(&data_dir)?;
    let result = ProductionParticipantHandler::new(store, test_participant_config());
    let Err(error) = result else {
        return Err("mixed v2/v1 stream unexpectedly published an authority".into());
    };
    assert!(
        error.to_string().contains("schema version 1"),
        "startup reported the wrong migration failure: {error}"
    );
    Ok(())
}

fn record(token: u8) -> ClientRequest {
    ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: CONVERSATION,
        participant_id: 0,
        capability_generation: Generation::ONE,
        record_admission_attempt_token: RecordAdmissionAttemptToken::new([token; 16]),
        payload: vec![1, 2, 3, token],
    })
}

fn enrolled_fault_handler(
    fault: &Arc<FaultStore>,
    incarnation: ConnectionIncarnation,
) -> Result<ProductionParticipantHandler, Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = fault.clone();
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    let value = dispatch(
        &handler,
        incarnation,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new([0xF4; 16]),
        }),
    )?;
    assert!(matches!(value, ServerValue::EnrollBound(_)));
    Ok(handler)
}

fn enrolled_fault_leave_handler(
    fault: &Arc<FaultStore>,
    incarnation: ConnectionIncarnation,
) -> Result<(ProductionParticipantHandler, LeaveRequest), Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = fault.clone();
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    let value = dispatch(
        &handler,
        incarnation,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new([0xF5; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(receipt) = value else {
        return Err(format!("fault-test enrollment did not bind: {value:?}").into());
    };
    let request = LeaveRequest {
        conversation_id: CONVERSATION,
        participant_id: receipt.participant_id(),
        capability_generation: Generation::ONE,
        attach_secret: receipt.attach_secret(),
        leave_attempt_token: LeaveAttemptToken::new([0xF6; 16]),
    };
    Ok((handler, request))
}

#[test]
fn record_append_failure_publishes_no_response_or_poststate() -> Result<(), Box<dyn Error>> {
    let inner: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let fault = Arc::new(FaultStore {
        inner: Arc::clone(&inner),
        fail_append: AtomicBool::new(false),
        fail_flush: AtomicBool::new(false),
    });
    let incarnation = ConnectionIncarnation::new(45, 1);
    let handler = enrolled_fault_handler(&fault, incarnation)?;
    fault.fail_append.store(true, Ordering::SeqCst);

    let failed = dispatch(&handler, incarnation, record(0xA1));
    assert!(
        failed.is_err(),
        "append fault returned a response: {failed:?}"
    );
    let cell = handler.cell(CONVERSATION)?;
    assert!(
        cell.lock()
            .map_err(|_| "test conversation owner lock poisoned")?
            .is_none(),
        "append fault published in-memory poststate"
    );
    let rows = block_on(inner.read_from(&format!("{STREAM_PREFIX}{CONVERSATION}"), 0, 8))??;
    assert_eq!(rows.len(), 2, "append fault persisted a record row");

    fault.fail_append.store(false, Ordering::SeqCst);
    let committed = dispatch(&handler, incarnation, record(0xA1))?;
    assert!(matches!(committed, ServerValue::RecordCommitted(_)));
    Ok(())
}

#[test]
fn record_flush_failure_replays_only_a_complete_row() -> Result<(), Box<dyn Error>> {
    let inner: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let fault = Arc::new(FaultStore {
        inner: Arc::clone(&inner),
        fail_append: AtomicBool::new(false),
        fail_flush: AtomicBool::new(false),
    });
    let incarnation = ConnectionIncarnation::new(46, 1);
    let handler = enrolled_fault_handler(&fault, incarnation)?;
    fault.fail_flush.store(true, Ordering::SeqCst);

    let failed = dispatch(&handler, incarnation, record(0xB1));
    assert!(
        failed.is_err(),
        "flush fault returned a response: {failed:?}"
    );
    let cell = handler.cell(CONVERSATION)?;
    assert!(
        cell.lock()
            .map_err(|_| "test conversation owner lock poisoned")?
            .is_none(),
        "flush fault published in-memory poststate"
    );
    let rows = block_on(inner.read_from(&format!("{STREAM_PREFIX}{CONVERSATION}"), 0, 8))??;
    assert_eq!(
        rows.len(),
        3,
        "the test backend exposes the complete uncertain append, never a partial row"
    );
    let stored: serde_json::Value = serde_json::from_slice(&rows[2].payload)?;
    assert_eq!(stored["schema_version"], SCHEMA_VERSION);
    assert_eq!(stored["operation"]["operation"], "record_admission");

    fault.fail_flush.store(false, Ordering::SeqCst);
    drop(handler);
    let store: Arc<dyn DurableStore> = fault;
    let reopened = ProductionParticipantHandler::new(store, test_participant_config())?;
    let committed = dispatch(&reopened, incarnation, record(0xB2))?;
    let ServerValue::RecordCommitted(committed) = committed else {
        return Err("post-fault cold replay did not admit the next record".into());
    };
    assert_eq!(committed.delivery_seq(), 3);
    Ok(())
}

#[test]
fn leave_append_failure_publishes_no_response_or_tombstone() -> Result<(), Box<dyn Error>> {
    let inner: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let fault = Arc::new(FaultStore {
        inner: Arc::clone(&inner),
        fail_append: AtomicBool::new(false),
        fail_flush: AtomicBool::new(false),
    });
    let incarnation = ConnectionIncarnation::new(47, 1);
    let (handler, request) = enrolled_fault_leave_handler(&fault, incarnation)?;
    fault.fail_append.store(true, Ordering::SeqCst);

    let failed = dispatch(&handler, incarnation, ClientRequest::Leave(request.clone()));
    assert!(
        failed.is_err(),
        "Leave append fault returned a response: {failed:?}"
    );
    let cell = handler.cell(CONVERSATION)?;
    assert!(
        cell.lock()
            .map_err(|_| "test conversation owner lock poisoned")?
            .is_none(),
        "Leave append fault published in-memory tombstone state"
    );
    let rows = block_on(inner.read_from(&format!("{STREAM_PREFIX}{CONVERSATION}"), 0, 8))??;
    assert_eq!(rows.len(), 2, "Leave append fault persisted a Left row");

    fault.fail_append.store(false, Ordering::SeqCst);
    let committed = dispatch(&handler, incarnation, ClientRequest::Leave(request))?;
    assert!(matches!(committed, ServerValue::LeaveCommitted(_)));
    Ok(())
}

#[test]
fn leave_flush_failure_replays_only_a_complete_tombstone() -> Result<(), Box<dyn Error>> {
    let inner: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let fault = Arc::new(FaultStore {
        inner: Arc::clone(&inner),
        fail_append: AtomicBool::new(false),
        fail_flush: AtomicBool::new(false),
    });
    let incarnation = ConnectionIncarnation::new(48, 1);
    let (handler, request) = enrolled_fault_leave_handler(&fault, incarnation)?;
    fault.fail_flush.store(true, Ordering::SeqCst);

    let failed = dispatch(&handler, incarnation, ClientRequest::Leave(request.clone()));
    assert!(
        failed.is_err(),
        "Leave flush fault returned a response: {failed:?}"
    );
    let cell = handler.cell(CONVERSATION)?;
    assert!(
        cell.lock()
            .map_err(|_| "test conversation owner lock poisoned")?
            .is_none(),
        "Leave flush fault published in-memory tombstone state"
    );
    let rows = block_on(inner.read_from(&format!("{STREAM_PREFIX}{CONVERSATION}"), 0, 8))??;
    assert_eq!(
        rows.len(),
        3,
        "the backend exposes one complete uncertain Left row, never a partial row"
    );
    let stored: serde_json::Value = serde_json::from_slice(&rows[2].payload)?;
    assert_eq!(stored["schema_version"], SCHEMA_VERSION);
    assert_eq!(stored["operation"]["operation"], "left");

    fault.fail_flush.store(false, Ordering::SeqCst);
    drop(handler);
    let store: Arc<dyn DurableStore> = fault;
    let reopened = ProductionParticipantHandler::new(store, test_participant_config())?;
    let replayed = dispatch(&reopened, incarnation, ClientRequest::Leave(request))?;
    assert!(matches!(replayed, ServerValue::LeaveCommitted(_)));
    Ok(())
}
