//! Deterministic independent append/flush gates for both Unit 1 and Unit 2 streams.

use std::collections::VecDeque;
use std::error::Error;
use std::sync::{Arc, Condvar, Mutex};

use liminal::durability::{DurabilityError, DurableStore, StoredEntry};

use super::log::STREAM_PREFIX;
use super::outbox_log::OUTBOX_STREAM_PREFIX;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OutboxBarrierKind {
    OperationAppend,
    OperationFlush,
    OutboxAppend,
    OutboxFlush,
}

#[derive(Debug, Default)]
struct BarrierState {
    gates: VecDeque<OutboxBarrierKind>,
    fail_next: Option<OutboxBarrierKind>,
    reached: Option<OutboxBarrierKind>,
    released: bool,
    pending_flush: Option<OutboxBarrierKind>,
}

#[derive(Debug)]
pub(super) struct OutboxBarrierStore {
    inner: Arc<dyn DurableStore>,
    state: Mutex<BarrierState>,
    changed: Condvar,
}

impl OutboxBarrierStore {
    pub(super) fn new(inner: Arc<dyn DurableStore>) -> Self {
        Self {
            inner,
            state: Mutex::new(BarrierState::default()),
            changed: Condvar::new(),
        }
    }

    pub(super) fn arm(
        &self,
        gates: impl IntoIterator<Item = OutboxBarrierKind>,
    ) -> Result<(), Box<dyn Error>> {
        let mut state = self.state.lock().map_err(|_| "outbox barrier poisoned")?;
        state.gates = gates.into_iter().collect();
        state.reached = None;
        state.released = false;
        state.pending_flush = None;
        drop(state);
        Ok(())
    }

    pub(super) fn fail_next(&self, kind: OutboxBarrierKind) -> Result<(), Box<dyn Error>> {
        let mut state = self.state.lock().map_err(|_| "outbox barrier poisoned")?;
        state.fail_next = Some(kind);
        drop(state);
        Ok(())
    }

    pub(super) fn wait_for(&self, expected: OutboxBarrierKind) -> Result<(), Box<dyn Error>> {
        let mut state = self.state.lock().map_err(|_| "outbox barrier poisoned")?;
        while state.reached != Some(expected) {
            state = self
                .changed
                .wait(state)
                .map_err(|_| "outbox barrier poisoned while waiting")?;
        }
        drop(state);
        Ok(())
    }

    pub(super) fn release(&self, expected: OutboxBarrierKind) -> Result<(), Box<dyn Error>> {
        let mut state = self.state.lock().map_err(|_| "outbox barrier poisoned")?;
        if state.reached != Some(expected) {
            return Err(format!(
                "attempted to release {expected:?} while {:?} was reached",
                state.reached
            )
            .into());
        }
        state.released = true;
        drop(state);
        self.changed.notify_all();
        Ok(())
    }

    fn cross(&self, kind: OutboxBarrierKind) -> Result<(), DurabilityError> {
        let mut state = self.state.lock().map_err(|_| barrier_fault())?;
        if state.fail_next == Some(kind) {
            state.fail_next = None;
            return Err(barrier_fault());
        }
        if state.gates.front().copied() == Some(kind) {
            state.reached = Some(kind);
            state.released = false;
            self.changed.notify_all();
            while !state.released {
                state = self.changed.wait(state).map_err(|_| barrier_fault())?;
            }
            state.reached = None;
            state.released = false;
            state.gates.pop_front();
            self.changed.notify_all();
        }
        drop(state);
        Ok(())
    }

    fn set_pending_flush(&self, kind: OutboxBarrierKind) -> Result<(), DurabilityError> {
        let mut state = self.state.lock().map_err(|_| barrier_fault())?;
        state.pending_flush = Some(kind);
        drop(state);
        Ok(())
    }

    fn take_pending_flush(&self) -> Result<Option<OutboxBarrierKind>, DurabilityError> {
        let mut state = self.state.lock().map_err(|_| barrier_fault())?;
        let pending = state.pending_flush.take();
        drop(state);
        Ok(pending)
    }
}

#[async_trait::async_trait]
impl DurableStore for OutboxBarrierStore {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        let (append_gate, flush_gate) = if stream_key.starts_with(STREAM_PREFIX) {
            (
                Some(OutboxBarrierKind::OperationAppend),
                Some(OutboxBarrierKind::OperationFlush),
            )
        } else if stream_key.starts_with(OUTBOX_STREAM_PREFIX) {
            (
                Some(OutboxBarrierKind::OutboxAppend),
                Some(OutboxBarrierKind::OutboxFlush),
            )
        } else {
            (None, None)
        };
        if let Some(gate) = append_gate {
            self.cross(gate)?;
        }
        let assigned = self.inner.append(stream_key, payload, expected_seq).await?;
        if let Some(gate) = flush_gate {
            self.set_pending_flush(gate)?;
        }
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
        if let Some(gate) = self.take_pending_flush()? {
            self.cross(gate)?;
        }
        self.inner.flush().await
    }
}

const fn barrier_fault() -> DurabilityError {
    DurabilityError::SequenceConflict {
        expected: 0,
        actual: u64::MAX,
    }
}
