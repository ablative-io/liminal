//! Cold-replay regression for the terminalized detach-cell binding.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use haematite::{Database, DatabaseConfig, EventStore};
use liminal::durability::{DurabilityError, DurableStore, HaematiteStore, StoredEntry};
use liminal_protocol::lifecycle::{AttachSecretProof, ParticipantSlotAllocatorProof};
use liminal_protocol::wire::{
    AttachAttemptToken, AttachSecret, BindingEpoch, BindingStateView, ConnectionIncarnation,
    CredentialAttachRequest, DetachAttemptToken, DetachRequest, EnrollmentRequest, EnrollmentToken,
    Generation,
};

use super::detach_repository::{
    DetachAllocation, EnrollmentAllocation, OrdinaryAttachAllocation, ParticipantDetachRepository,
    ParticipantDetachRepositoryError,
};

#[derive(Debug)]
struct FailFirstFlush {
    inner: Arc<dyn DurableStore>,
    fail_flush: AtomicBool,
}

impl FailFirstFlush {
    fn new(inner: Arc<dyn DurableStore>) -> Self {
        Self {
            inner,
            fail_flush: AtomicBool::new(true),
        }
    }
}

#[async_trait::async_trait]
impl DurableStore for FailFirstFlush {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
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
        if self.fail_flush.swap(false, Ordering::SeqCst) {
            return Err(DurabilityError::ConfigError(
                "injected participant detach flush failure".into(),
            ));
        }
        self.inner.flush().await
    }
}

#[derive(Clone, Copy, Debug)]
struct TestParticipantSlot {
    conversation_id: u64,
    participant_id: u64,
    identity_limit: u64,
}

impl ParticipantSlotAllocatorProof for TestParticipantSlot {
    fn conversation_id(&self) -> u64 {
        self.conversation_id
    }

    fn participant_index(&self) -> u64 {
        self.participant_id
    }

    fn identity_limit(&self) -> u64 {
        self.identity_limit
    }
}

fn generation(value: u64) -> Generation {
    Generation::new(value).expect("test generation is nonzero")
}

fn create_store(
    data_dir: &std::path::Path,
) -> Result<Arc<dyn DurableStore>, Box<dyn std::error::Error>> {
    let database = Database::create(DatabaseConfig {
        data_dir: data_dir.to_path_buf(),
        shard_count: 2,
        distributed: None,
    })?;
    Ok(Arc::new(HaematiteStore::new(Arc::new(EventStore::new(
        database,
    )))))
}

fn reopen_store(
    data_dir: &std::path::Path,
) -> Result<Arc<dyn DurableStore>, Box<dyn std::error::Error>> {
    let database = Database::open(data_dir)?;
    Ok(Arc::new(HaematiteStore::new(Arc::new(EventStore::new(
        database,
    )))))
}

struct DetachFixture {
    conversation_id: u64,
    participant_id: u64,
    old_epoch: BindingEpoch,
    new_epoch: BindingEpoch,
    original_attach_secret: AttachSecret,
    detach_verifier: [u8; 32],
    detach_request: DetachRequest,
}

fn detach_fixture() -> DetachFixture {
    let conversation_id = 29;
    let participant_id = 3;
    DetachFixture {
        conversation_id,
        participant_id,
        old_epoch: BindingEpoch::new(ConnectionIncarnation::new(7, 11), Generation::ONE),
        new_epoch: BindingEpoch::new(ConnectionIncarnation::new(7, 12), generation(2)),
        original_attach_secret: AttachSecret::new([0x44; 32]),
        detach_verifier: [0xA5; 32],
        detach_request: DetachRequest {
            conversation_id,
            participant_id,
            capability_generation: Generation::ONE,
            detach_attempt_token: DetachAttemptToken::new([0xD3; 16]),
        },
    }
}

fn write_history(
    data_dir: &std::path::Path,
    fixture: &DetachFixture,
) -> Result<String, Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let store = create_store(data_dir)?;
    let repository = ParticipantDetachRepository::new(Arc::clone(&store), fixture.conversation_id);
    let stream_key = repository.stream_key().to_owned();
    let enrollment = runtime.block_on(repository.commit_enrollment(
        EnrollmentRequest {
            conversation_id: fixture.conversation_id,
            enrollment_token: EnrollmentToken::new([0xE1; 16]),
        },
        TestParticipantSlot {
            conversation_id: fixture.conversation_id,
            participant_id: fixture.participant_id,
            identity_limit: 8,
        },
        EnrollmentAllocation {
            attach_secret: fixture.original_attach_secret,
            origin_binding_epoch: fixture.old_epoch,
            attached_transaction_order: 1,
            attached_delivery_seq: 40,
            receipt_expires_at: 1_000,
            provenance_expires_at: 2_000,
            enrollment_fingerprint: [0x91; 32],
        },
    ))?;
    assert_eq!(enrollment.participant_id(), fixture.participant_id);
    assert_eq!(enrollment.origin_binding_epoch(), fixture.old_epoch);

    let detached = runtime.block_on(repository.commit_detach(
        fixture.detach_request.clone(),
        DetachAllocation {
            request_verifier: fixture.detach_verifier,
            receiving_binding_epoch: fixture.old_epoch,
            terminal_transaction_order: 2,
            terminal_delivery_seq: 44,
        },
    ))?;
    assert_eq!(detached.committed_binding_epoch(), fixture.old_epoch);
    assert_eq!(detached.detached_delivery_seq(), 44);

    let attached = runtime.block_on(repository.commit_ordinary_attach(
        CredentialAttachRequest {
            conversation_id: fixture.conversation_id,
            participant_id: fixture.participant_id,
            capability_generation: Generation::ONE,
            attach_secret: fixture.original_attach_secret,
            attach_attempt_token: AttachAttemptToken::new([0xA7; 16]),
            accept_marker_delivery_seq: None,
        },
        AttachSecretProof::Verified,
        OrdinaryAttachAllocation {
            binding_epoch: fixture.new_epoch,
            attach_secret: AttachSecret::new([0x55; 32]),
            attached_transaction_order: 3,
            attached_delivery_seq: 45,
            receipt_expires_at: 3_000,
            provenance_expires_at: 4_000,
        },
    ))?;
    assert_eq!(attached.origin_binding_epoch(), fixture.new_epoch);

    let entries = runtime.block_on(store.read_from(repository.stream_key(), 0, 8))?;
    assert_eq!(entries.len(), 3, "one durable event per atomic transition");
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    drop(repository);
    drop(store);
    drop(runtime);
    Ok(stream_key)
}

#[test]
fn failed_first_flush_never_publishes_enrollment_outcome() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    let data_dir = directory.path().join("participant-db");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let inner = create_store(&data_dir)?;
    let failing: Arc<dyn DurableStore> = Arc::new(FailFirstFlush::new(Arc::clone(&inner)));
    let repository = ParticipantDetachRepository::new(failing, 29);

    let result = runtime.block_on(repository.commit_enrollment(
        EnrollmentRequest {
            conversation_id: 29,
            enrollment_token: EnrollmentToken::new([0xE1; 16]),
        },
        TestParticipantSlot {
            conversation_id: 29,
            participant_id: 3,
            identity_limit: 8,
        },
        EnrollmentAllocation {
            attach_secret: AttachSecret::new([0x44; 32]),
            origin_binding_epoch: BindingEpoch::new(
                ConnectionIncarnation::new(7, 11),
                Generation::ONE,
            ),
            attached_transaction_order: 1,
            attached_delivery_seq: 40,
            receipt_expires_at: 1_000,
            provenance_expires_at: 2_000,
            enrollment_fingerprint: [0x91; 32],
        },
    ));
    assert!(matches!(
        result,
        Err(ParticipantDetachRepositoryError::DurableStore(
            DurabilityError::ConfigError(message)
        )) if message == "injected participant detach flush failure"
    ));

    let entries = runtime.block_on(inner.read_from(repository.stream_key(), 0, 8))?;
    assert_eq!(
        entries.len(),
        1,
        "the failed durability barrier must not cause an internal append retry"
    );
    Ok(())
}

fn assert_reopened_history(
    data_dir: &std::path::Path,
    stream_key: &str,
    fixture: &DetachFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let store = reopen_store(data_dir)?;
    let reopened = ParticipantDetachRepository::new(Arc::clone(&store), fixture.conversation_id);
    let replayed = runtime.block_on(reopened.exact_terminalized_detach_lookup(
        &fixture.detach_request,
        fixture.detach_verifier,
        Some(fixture.new_epoch),
        0,
    ))?;

    assert_eq!(replayed.conversation_id(), fixture.conversation_id);
    assert_eq!(replayed.participant_id(), fixture.participant_id);
    assert_eq!(replayed.capability_generation(), Generation::ONE);
    assert_eq!(
        replayed.detach_attempt_token(),
        fixture.detach_request.detach_attempt_token
    );
    assert_eq!(replayed.current_generation(), generation(2));
    assert_eq!(replayed.committed_binding_epoch(), fixture.old_epoch);
    assert_eq!(
        replayed.binding_state(),
        BindingStateView::Bound {
            current_binding_epoch: fixture.new_epoch,
        }
    );
    let reopened_entries = runtime.block_on(store.read_from(stream_key, 0, 8))?;
    assert_eq!(reopened_entries.len(), 3);
    Ok(())
}

#[test]
fn cold_reopen_replays_terminalized_detach_with_old_epoch() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    let data_dir = directory.path().join("participant-db");
    let fixture = detach_fixture();
    let stream_key = write_history(&data_dir, &fixture)?;

    // Every repository, store, EventStore, Database, and runtime handle from
    // the writer lifetime has been dropped. Reopen the same Haematite path and
    // derive the terminalized cell only by replaying its three durable events.
    assert_reopened_history(&data_dir, &stream_key, &fixture)
}
