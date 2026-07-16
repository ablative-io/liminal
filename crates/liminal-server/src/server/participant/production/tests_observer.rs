//! Observer-recovery crash-window and durable-occupancy production tests.
//!
//! Two reachable states the observer aggregate must classify from durable
//! reality rather than from lucky in-memory ordering: (1) an enrollment whose
//! durable observer `Track` row was lost to a crash between the conversation
//! append and the tracking append, and (2) a connection-conversation
//! occupancy count taken while a conversation's in-memory owner had been
//! discarded by another operation's failure.

use std::error::Error;
use std::sync::Arc;

use liminal::durability::{DurabilityError, DurableStore, StoredEntry};
use liminal_protocol::wire::{
    ClientRequest, ConnectionConversationCapacityExceeded, ConnectionIncarnation,
    EnrollmentRequest, EnrollmentToken, Generation, InvalidObserverEpoch,
    ObserverRecoveryHandshake, ObserverRefusal, RecordAdmission, ServerValue,
};

use super::ProductionParticipantHandler;
use super::tests::{dispatch, open_disk_store_for_tests, test_participant_config};

/// Stream key of the server-wide observer row log (mirrors `observer.rs`).
const OBSERVER_STREAM_KEY: &str = "liminal:participant-observer-recovery";

/// Store wrapper that fails every append to the observer row stream,
/// simulating a crash in the window between the enrollment append and the
/// observer `Track` append.
#[derive(Debug)]
struct ObserverAppendFailingStore {
    inner: Arc<dyn DurableStore>,
}

#[async_trait::async_trait]
impl DurableStore for ObserverAppendFailingStore {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        if stream_key == OBSERVER_STREAM_KEY {
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
        self.inner.flush().await
    }
}

/// Crash window between the enrollment append and the observer Track append:
/// durable reality holds the `Enrolled` entry and no Track row. After a cold
/// reopen, an observer-recovery handshake naming that conversation must
/// CLASSIFY (the Track registration is recovered from the conversation log
/// itself), never refuse the conversation as unknown forever.
#[test]
fn enrolled_conversation_without_track_row_recovers_classification() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let conversation_id = 801;

    {
        let disk = open_disk_store_for_tests(&data_dir)?;
        let failing: Arc<dyn DurableStore> = Arc::new(ObserverAppendFailingStore { inner: disk });
        let handler = ProductionParticipantHandler::new(failing, test_participant_config());
        // The enrollment's conversation append succeeds; the Track append
        // fails — exactly the crash window. The request itself fails loudly.
        let result = dispatch(
            &handler,
            ConnectionIncarnation::new(81, 1),
            ClientRequest::Enrollment(EnrollmentRequest {
                conversation_id,
                enrollment_token: EnrollmentToken::new([91; 16]),
            }),
        );
        assert!(
            result.is_err(),
            "a lost Track append must fail the enrollment request loudly: {result:?}"
        );
    }

    // Cold reopen over the plain store: the handshake is the FIRST touch of
    // this conversation — no prior conversation request repairs it.
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(store, test_participant_config());
    let value = dispatch(
        &handler,
        ConnectionIncarnation::new(82, 1),
        ClientRequest::ObserverRecovery(ObserverRecoveryHandshake {
            observer_refusals: vec![ObserverRefusal {
                conversation_id,
                refused_epoch: 1,
            }],
        }),
    )?;
    // Contract-derived row for a tracked conversation at progress 0 with
    // refused_epoch 1: the epoch is AHEAD of durable progress.
    let ServerValue::InvalidObserverEpoch(InvalidObserverEpoch::EpochAhead {
        conversation_id: refused_conversation,
        presented_epoch,
        current_observer_progress,
    }) = value
    else {
        return Err(format!(
            "recovered tracking must classify the epoch against durable progress: {value:?}"
        )
        .into());
    };
    assert_eq!(refused_conversation, conversation_id);
    assert_eq!(presented_epoch, 1);
    assert_eq!(current_observer_progress, 0);
    Ok(())
}

/// Occupancy for the connection-conversation limit is derived from durable
/// binding authority: discarding a conversation's in-memory owner (any failed
/// operation does) must not open capacity the durable bindings still occupy.
#[test]
fn capacity_check_counts_durably_bound_conversation_after_owner_discard()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(83, 1);
    let bound_conversation = 802;
    let fresh_conversation = 803;
    let mut config = test_participant_config();
    config.max_semantic_conversations_per_connection = 1;

    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(store, config);
    let enrolled = dispatch(
        &handler,
        incarnation,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: bound_conversation,
            enrollment_token: EnrollmentToken::new([92; 16]),
        }),
    )?;
    assert!(matches!(enrolled, ServerValue::EnrollBound(_)));

    // Discard the bound conversation's in-memory owner through a failing
    // operation (record admission fails closed by design).
    let failed = dispatch(
        &handler,
        incarnation,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: bound_conversation,
            participant_id: 0,
            capability_generation: Generation::ONE,
            payload: vec![1, 2, 3],
        }),
    );
    assert!(
        failed.is_err(),
        "record admission must fail closed: {failed:?}"
    );

    // The recovery batch names a FRESH conversation. Durable reality still
    // binds this connection to `bound_conversation`, so the configured limit
    // of one is already occupied and the batch must refuse on capacity.
    let value = dispatch(
        &handler,
        incarnation,
        ClientRequest::ObserverRecovery(ObserverRecoveryHandshake {
            observer_refusals: vec![ObserverRefusal {
                conversation_id: fresh_conversation,
                refused_epoch: 0,
            }],
        }),
    )?;
    let ServerValue::ConnectionConversationCapacityExceeded(
        ConnectionConversationCapacityExceeded::ObserverRecovery {
            conversation_id: refused_conversation,
            limit,
        },
    ) = value
    else {
        return Err(format!(
            "durably bound conversation must occupy the configured limit: {value:?}"
        )
        .into());
    };
    assert_eq!(refused_conversation, fresh_conversation);
    assert_eq!(limit, 1);
    Ok(())
}
