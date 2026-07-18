//! Deterministic participant-source and observer-Advance durability barriers.

use std::collections::VecDeque;
use std::error::Error;
use std::sync::{Arc, Condvar, Mutex};

use liminal::durability::{DurabilityError, DurableStore, StoredEntry};

use super::log::STREAM_PREFIX;

const OBSERVER_STREAM_KEY: &str = "liminal:participant-observer-recovery";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BarrierKind {
    Source,
    Advance,
}

#[derive(Debug, Default)]
struct BarrierState {
    pending: Option<BarrierKind>,
    gates: VecDeque<BarrierKind>,
    reached: Option<BarrierKind>,
    released: bool,
    fail_next: Option<BarrierKind>,
}

#[derive(Debug)]
pub(super) struct ObserverBarrierStore {
    inner: Arc<dyn DurableStore>,
    state: Mutex<BarrierState>,
    changed: Condvar,
}

impl ObserverBarrierStore {
    pub(super) fn new(inner: Arc<dyn DurableStore>) -> Self {
        Self {
            inner,
            state: Mutex::new(BarrierState::default()),
            changed: Condvar::new(),
        }
    }

    pub(super) fn arm(
        &self,
        gates: impl IntoIterator<Item = BarrierKind>,
    ) -> Result<(), Box<dyn Error>> {
        let mut state = self.state.lock().map_err(|_| "barrier state poisoned")?;
        state.gates = gates.into_iter().collect();
        state.reached = None;
        state.released = false;
        Ok(())
    }

    pub(super) fn wait_for(&self, expected: BarrierKind) -> Result<(), Box<dyn Error>> {
        let mut state = self.state.lock().map_err(|_| "barrier state poisoned")?;
        while state.reached != Some(expected) {
            state = self
                .changed
                .wait(state)
                .map_err(|_| "barrier state poisoned while waiting")?;
        }
        Ok(())
    }

    pub(super) fn release(&self, expected: BarrierKind) -> Result<(), Box<dyn Error>> {
        let mut state = self.state.lock().map_err(|_| "barrier state poisoned")?;
        if state.reached != Some(expected) {
            return Err(format!(
                "attempted to release {expected:?} while {:?} was reached",
                state.reached
            )
            .into());
        }
        state.released = true;
        self.changed.notify_all();
        Ok(())
    }

    pub(super) fn fail_next(&self, kind: BarrierKind) -> Result<(), Box<dyn Error>> {
        let mut state = self.state.lock().map_err(|_| "barrier state poisoned")?;
        state.fail_next = Some(kind);
        Ok(())
    }
}

#[async_trait::async_trait]
impl DurableStore for ObserverBarrierStore {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        let kind = if stream_key.starts_with(STREAM_PREFIX) {
            Some(BarrierKind::Source)
        } else if stream_key == OBSERVER_STREAM_KEY
            && payload
                .windows(b"\"row\":\"advance\"".len())
                .any(|window| window == b"\"row\":\"advance\"")
        {
            Some(BarrierKind::Advance)
        } else {
            None
        };
        let assigned = self.inner.append(stream_key, payload, expected_seq).await?;
        let mut state = self.state.lock().map_err(|_| barrier_fault())?;
        state.pending = kind;
        drop(state);
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
        let fail = {
            let mut state = self.state.lock().map_err(|_| barrier_fault())?;
            let pending = state.pending.take();
            if state.fail_next == pending && pending.is_some() {
                state.fail_next = None;
                true
            } else {
                if state.gates.front().copied() == pending && pending.is_some() {
                    state.reached = pending;
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
                false
            }
        };
        if fail {
            return Err(barrier_fault());
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
