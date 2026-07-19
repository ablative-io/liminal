//! Deterministic stores and history builders for the W3 restore oracles.

use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use liminal::durability::bridge::block_on;
use liminal::durability::{DurabilityError, DurableStore, StoredEntry, open_ephemeral};
use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, EnrollmentRequest, EnrollmentToken, ServerValue,
};

use super::ProductionParticipantHandler;
use super::log::STREAM_PREFIX;
use super::outbox_log::{OUTBOX_STREAM_PREFIX, UNIT2_OUTBOX_RESTORE_BATCH_ROWS};
use super::tests::{dispatch, test_participant_config};

pub(super) const CONVERSATION: u64 = 0xF0_C7;

pub(super) fn new_store() -> Result<Arc<dyn DurableStore>, Box<dyn Error>> {
    Ok(Arc::new(open_ephemeral(1)?))
}

pub(super) fn enrollment(token: u8) -> ClientRequest {
    ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: CONVERSATION,
        enrollment_token: EnrollmentToken::new([token; 16]),
    })
}

pub(super) fn seed_enrollment(
    store: &Arc<dyn DurableStore>,
) -> Result<ProductionParticipantHandler, Box<dyn Error>> {
    let handler = ProductionParticipantHandler::new(Arc::clone(store), test_participant_config())?;
    let value = dispatch(
        &handler,
        ConnectionIncarnation::new(CONVERSATION, 1),
        enrollment(1),
    )?;
    if !matches!(value, ServerValue::EnrollBound(_)) {
        return Err(format!("seed enrollment returned {value:?}").into());
    }
    Ok(handler)
}

pub(super) fn stream_payloads(
    store: &Arc<dyn DurableStore>,
    stream_key: &str,
) -> Result<Vec<Vec<u8>>, Box<dyn Error>> {
    let mut offset = 0_u64;
    let mut payloads = Vec::new();
    loop {
        let entries =
            block_on(store.read_from(stream_key, offset, UNIT2_OUTBOX_RESTORE_BATCH_ROWS))??;
        if entries.is_empty() {
            break;
        }
        for entry in entries {
            if entry.sequence != offset {
                return Err(format!(
                    "fixture stream expected sequence {offset}, got {}",
                    entry.sequence
                )
                .into());
            }
            payloads.push(entry.payload);
            offset = offset
                .checked_add(1)
                .ok_or("fixture stream offset overflowed")?;
        }
    }
    Ok(payloads)
}

pub(super) fn extension_key() -> String {
    format!("{OUTBOX_STREAM_PREFIX}{CONVERSATION}")
}

pub(super) fn operation_key() -> String {
    format!("{STREAM_PREFIX}{CONVERSATION}")
}

pub(super) fn append_payload(
    store: &Arc<dyn DurableStore>,
    stream_key: &str,
    payload: Vec<u8>,
    sequence: u64,
) -> Result<(), Box<dyn Error>> {
    let assigned = block_on(store.append(stream_key, payload, sequence))??;
    if assigned != sequence {
        return Err(format!("fixture append expected {sequence}, got {assigned}").into());
    }
    block_on(store.flush())??;
    Ok(())
}

pub(super) fn duplicate_extension_to(
    store: &Arc<dyn DurableStore>,
    target_rows: usize,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let key = extension_key();
    let existing = stream_payloads(store, &key)?;
    let payload = existing
        .first()
        .cloned()
        .ok_or("seed enrollment produced no extension row")?;
    let mut sequence = u64::try_from(existing.len())?;
    let target = u64::try_from(target_rows)?;
    while sequence < target {
        append_payload(store, &key, payload.clone(), sequence)?;
        sequence = sequence
            .checked_add(1)
            .ok_or("duplicate extension sequence overflowed")?;
    }
    Ok(payload)
}

#[derive(Debug)]
pub(super) struct OutboxAppendFaultStore {
    inner: Arc<dyn DurableStore>,
    fail: AtomicBool,
}

impl OutboxAppendFaultStore {
    pub(super) fn new(inner: Arc<dyn DurableStore>) -> Self {
        Self {
            inner,
            fail: AtomicBool::new(false),
        }
    }

    pub(super) fn set_fail(&self, fail: bool) {
        self.fail.store(fail, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl DurableStore for OutboxAppendFaultStore {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        if stream_key.starts_with(OUTBOX_STREAM_PREFIX) && self.fail.load(Ordering::SeqCst) {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CursorFaultArm {
    PassTwoNonEof,
    PassTwoEmptyEof,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RunKind {
    Reference,
    W3,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum TraversalPhase {
    #[default]
    NotStarted,
    Validation,
    ValidationComplete,
    Application,
}

#[derive(Debug)]
struct PhaseControl {
    run: RunKind,
    phase: TraversalPhase,
    arm: CursorFaultArm,
}

#[derive(Debug)]
pub(super) struct PhaseAwareStore {
    inner: Arc<dyn DurableStore>,
    control: Mutex<PhaseControl>,
}

impl PhaseAwareStore {
    pub(super) fn new(inner: Arc<dyn DurableStore>, arm: CursorFaultArm) -> Self {
        Self {
            inner,
            control: Mutex::new(PhaseControl {
                run: RunKind::Reference,
                phase: TraversalPhase::NotStarted,
                arm,
            }),
        }
    }

    /// Reset independently before the aggregate reference traversal.
    pub(super) fn reset_reference(&self, arm: CursorFaultArm) -> Result<(), Box<dyn Error>> {
        let mut control = self.control.lock().map_err(|_| "phase control poisoned")?;
        *control = PhaseControl {
            run: RunKind::Reference,
            phase: TraversalPhase::NotStarted,
            arm,
        };
        drop(control);
        Ok(())
    }

    /// Reset independently before W3; faults are selected by traversal phase.
    pub(super) fn reset_w3(&self, arm: CursorFaultArm) -> Result<(), Box<dyn Error>> {
        let mut control = self.control.lock().map_err(|_| "phase control poisoned")?;
        *control = PhaseControl {
            run: RunKind::W3,
            phase: TraversalPhase::NotStarted,
            arm,
        };
        drop(control);
        Ok(())
    }
}

#[async_trait::async_trait]
impl DurableStore for PhaseAwareStore {
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
        if !stream_key.starts_with(OUTBOX_STREAM_PREFIX) {
            return self.inner.read_from(stream_key, offset, limit).await;
        }
        let (run, phase, arm) = {
            let mut control =
                self.control
                    .lock()
                    .map_err(|_| DurabilityError::SequenceConflict {
                        expected: offset,
                        actual: u64::MAX,
                    })?;
            if offset == 0 {
                control.phase = match control.phase {
                    TraversalPhase::NotStarted => TraversalPhase::Validation,
                    TraversalPhase::ValidationComplete => TraversalPhase::Application,
                    phase => phase,
                };
            }
            (control.run, control.phase, control.arm)
        };
        if run == RunKind::W3
            && phase == TraversalPhase::Application
            && arm == CursorFaultArm::PassTwoNonEof
        {
            return Err(DurabilityError::SequenceConflict {
                expected: offset,
                actual: u64::MAX,
            });
        }
        let entries = self.inner.read_from(stream_key, offset, limit).await?;
        if entries.is_empty() {
            let mut control =
                self.control
                    .lock()
                    .map_err(|_| DurabilityError::SequenceConflict {
                        expected: offset,
                        actual: u64::MAX,
                    })?;
            if control.phase == TraversalPhase::Validation {
                control.phase = TraversalPhase::ValidationComplete;
            }
            drop(control);
            if run == RunKind::W3
                && phase == TraversalPhase::Application
                && arm == CursorFaultArm::PassTwoEmptyEof
            {
                return Err(DurabilityError::SequenceConflict {
                    expected: offset,
                    actual: u64::MAX,
                });
            }
        }
        Ok(entries)
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

#[derive(Debug, Default)]
struct ShortReadState {
    phase: TraversalPhase,
    validation_empty_reads: usize,
    application_empty_reads: usize,
}

#[derive(Debug)]
pub(super) struct ShortPageStore {
    inner: Arc<dyn DurableStore>,
    state: Mutex<ShortReadState>,
}

impl ShortPageStore {
    pub(super) fn new(inner: Arc<dyn DurableStore>) -> Self {
        Self {
            inner,
            state: Mutex::new(ShortReadState::default()),
        }
    }

    pub(super) fn empty_reads(&self) -> Result<(usize, usize), Box<dyn Error>> {
        let state = self.state.lock().map_err(|_| "short-read state poisoned")?;
        Ok((state.validation_empty_reads, state.application_empty_reads))
    }
}

#[async_trait::async_trait]
impl DurableStore for ShortPageStore {
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
        if !stream_key.starts_with(OUTBOX_STREAM_PREFIX) {
            return self.inner.read_from(stream_key, offset, limit).await;
        }
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| DurabilityError::SequenceConflict {
                    expected: offset,
                    actual: u64::MAX,
                })?;
            if offset == 0 {
                state.phase = match state.phase {
                    TraversalPhase::NotStarted => TraversalPhase::Validation,
                    TraversalPhase::ValidationComplete => TraversalPhase::Application,
                    phase => phase,
                };
            }
        }
        let short_limit = limit
            .checked_sub(1)
            .filter(|value| *value != 0)
            .unwrap_or(limit);
        let entries = self
            .inner
            .read_from(stream_key, offset, short_limit)
            .await?;
        if entries.is_empty() {
            let mut state = self
                .state
                .lock()
                .map_err(|_| DurabilityError::SequenceConflict {
                    expected: offset,
                    actual: u64::MAX,
                })?;
            match state.phase {
                TraversalPhase::Validation => {
                    state.validation_empty_reads = state.validation_empty_reads.saturating_add(1);
                    state.phase = TraversalPhase::ValidationComplete;
                }
                TraversalPhase::Application => {
                    state.application_empty_reads = state.application_empty_reads.saturating_add(1);
                }
                TraversalPhase::NotStarted | TraversalPhase::ValidationComplete => {}
            }
        }
        Ok(entries)
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

#[derive(Debug)]
pub(super) struct CutPageStore {
    inner: Arc<dyn DurableStore>,
    cut_offset: u64,
    first_page_rows: usize,
}

impl CutPageStore {
    pub(super) fn new(
        inner: Arc<dyn DurableStore>,
        cut_offset: u64,
        first_page_rows: usize,
    ) -> Self {
        Self {
            inner,
            cut_offset,
            first_page_rows,
        }
    }
}

#[async_trait::async_trait]
impl DurableStore for CutPageStore {
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
        let selected = if !stream_key.starts_with(OUTBOX_STREAM_PREFIX) {
            limit
        } else if offset == 0 && self.cut_offset != 0 {
            usize::try_from(self.cut_offset)
                .map_err(|_| DurabilityError::SequenceConflict {
                    expected: self.cut_offset,
                    actual: u64::MAX,
                })?
                .min(limit)
        } else if offset == self.cut_offset {
            self.first_page_rows.min(limit)
        } else {
            limit
        };
        self.inner.read_from(stream_key, offset, selected).await
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
