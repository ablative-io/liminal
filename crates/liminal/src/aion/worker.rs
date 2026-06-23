#![allow(clippy::module_name_repetitions)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};

use super::channels::{ChannelName, dispatch_channel};
use super::codec::dispatch_request_schema;
use super::dispatch::{DispatchWorker, DispatchWorkerPool};
use super::error::AionSurfaceError;
use super::types::{ActivityRequest, WorkerCapacity};
use crate::channel::{ChannelConfig, ChannelHandle, ChannelMode, SubscriptionHandle};
use crate::conversation::{ConversationSupervisor, ParticipantPid};
use crate::routing::{ConsumerId, ConsumerStateView};

mod link;
use link::WorkerLinkMonitor;

#[derive(Clone)]
pub struct WorkerContext {
    inner: Arc<WorkerContextInner>,
}

impl WorkerContext {
    /// Create a worker context backed by an embedded, lazily-created
    /// [`ConversationSupervisor`] per dispatch channel.
    ///
    /// Note: the embedded supervisor runs on a single-thread beamr scheduler, so
    /// all worker link monitors for a channel share one scheduler thread. This is
    /// appropriate for the single-process embedded case; deployments that register
    /// very large worker pools should front them with an externally-constructed,
    /// multi-threaded supervisor rather than relying on this default.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(WorkerContextInner::default()),
        }
    }

    /// # Errors
    /// Returns when channel resolution, channel creation, or subscription opening fails.
    pub fn register_worker(
        &self,
        namespace: &str,
        task_queue: &str,
        capacity: WorkerCapacity,
    ) -> Result<WorkerRegistration, AionSurfaceError> {
        let channel_name = dispatch_channel(namespace, task_queue)?;
        let sequence = self.next_sequence();
        let participant = link::spawn_worker_process(self, &channel_name)?;
        self.register_worker_on_channel(
            &channel_name,
            format!("worker-{sequence}"),
            participant,
            capacity,
            Some(participant),
        )
    }

    /// # Errors
    /// Returns when the dispatch channel cannot be resolved or subscribed.
    pub fn register_worker_with_participant(
        &self,
        namespace: &str,
        task_queue: &str,
        worker_id: impl Into<String>,
        participant: ParticipantPid,
        capacity: WorkerCapacity,
    ) -> Result<WorkerRegistration, AionSurfaceError> {
        let channel_name = dispatch_channel(namespace, task_queue)?;
        self.register_worker_on_channel(
            &channel_name,
            worker_id.into(),
            participant,
            capacity,
            None,
        )
    }

    fn register_worker_on_channel(
        &self,
        channel_name: &ChannelName,
        worker_id: String,
        participant: ParticipantPid,
        capacity: WorkerCapacity,
        owned_participant: Option<ParticipantPid>,
    ) -> Result<WorkerRegistration, AionSurfaceError> {
        let session = self.session_for(channel_name)?;
        let subscription = session
            .handle
            .subscribe()
            .map_err(|error| lifecycle_failed(channel_name, error))?;
        let subscription = Mutex::new(Some(subscription));
        let entry = Arc::new(WorkerEntry {
            channel_name: channel_name.clone(),
            worker_id,
            participant,
            capacity,
            subscription,
            current_in_flight: AtomicU32::new(0),
            active: AtomicBool::new(true),
        });
        // Arm crash detection BEFORE the entry is inserted into the pool, so a
        // worker is never dispatch-eligible without a live link monitor. If the
        // participant dies in the window before insert, the listener deactivates
        // the entry and the next `retain_active` prunes it — the pool never
        // exposes a worker whose crash would go unnoticed.
        let monitor = link::monitor_worker_process(
            self.clone(),
            channel_name,
            participant,
            Arc::downgrade(&entry),
            owned_participant,
        )?;
        self.insert_entry(channel_name, &entry)?;
        Ok(WorkerRegistration::new(self.clone(), entry, monitor))
    }

    /// # Errors
    /// Returns when the worker pool cannot be read.
    pub fn workers_for_channel(
        &self,
        channel_name: &ChannelName,
        request: &ActivityRequest,
    ) -> Result<Vec<DispatchWorker>, AionSurfaceError> {
        <Self as DispatchWorkerPool>::workers_for(self, channel_name, request)
    }

    fn next_sequence(&self) -> u64 {
        self.inner
            .next_worker
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1)
    }

    fn session_for(&self, channel_name: &ChannelName) -> Result<ChannelSession, AionSurfaceError> {
        if let Some(session) = self.lookup_session(channel_name)? {
            return Ok(session);
        }

        let schema =
            dispatch_request_schema().map_err(|error| lifecycle_failed(channel_name, error))?;
        let handle = ChannelHandle::new(ChannelConfig::new(
            channel_name.as_str().to_owned(),
            schema,
            ChannelMode::Ephemeral,
        ));
        let session = ChannelSession { handle };
        self.insert_or_reuse_session(channel_name, session)
    }

    fn lookup_session(
        &self,
        channel_name: &ChannelName,
    ) -> Result<Option<ChannelSession>, AionSurfaceError> {
        let session = {
            let channels = self.lock_channels(channel_name)?;
            channels
                .get(channel_name.as_str())
                .map(|state| state.session.clone())
        };
        Ok(session)
    }

    fn insert_or_reuse_session(
        &self,
        channel_name: &ChannelName,
        session: ChannelSession,
    ) -> Result<ChannelSession, AionSurfaceError> {
        let mut channels = self.lock_channels(channel_name)?;
        let state = channels
            .entry(channel_name.as_str().to_owned())
            .or_insert_with(|| ChannelState::new(session));
        let stored = state.session.clone();
        drop(channels);
        Ok(stored)
    }

    fn insert_entry(
        &self,
        channel_name: &ChannelName,
        entry: &Arc<WorkerEntry>,
    ) -> Result<(), AionSurfaceError> {
        let mut channels = self.lock_channels(channel_name)?;
        let state = channels
            .get_mut(channel_name.as_str())
            .ok_or_else(|| lifecycle_failed(channel_name, "dispatch channel missing"))?;
        state.entries.push(Arc::downgrade(entry));
        drop(channels);
        Ok(())
    }

    fn remove_inactive(&self, channel_name: &ChannelName) -> Result<(), AionSurfaceError> {
        let mut channels = self.lock_channels(channel_name)?;
        if let Some(state) = channels.get_mut(channel_name.as_str()) {
            state.retain_active();
        }
        drop(channels);
        Ok(())
    }

    fn snapshot(
        &self,
        channel_name: &ChannelName,
    ) -> Result<Vec<DispatchWorker>, AionSurfaceError> {
        let mut channels = self.lock_channels(channel_name)?;
        let Some(state) = channels.get_mut(channel_name.as_str()) else {
            return Ok(Vec::new());
        };
        state.retain_active();
        let workers = state
            .entries
            .iter()
            .filter_map(Weak::upgrade)
            .map(|entry| entry.to_dispatch_worker())
            .collect();
        drop(channels);
        Ok(workers)
    }

    fn lock_channels(
        &self,
        channel_name: &ChannelName,
    ) -> Result<MutexGuard<'_, BTreeMap<String, ChannelState>>, AionSurfaceError> {
        self.inner
            .channels
            .lock()
            .map_err(|error| lifecycle_failed(channel_name, error))
    }

    fn supervisor_for(
        &self,
        channel_name: &ChannelName,
    ) -> Result<ConversationSupervisor, AionSurfaceError> {
        if let Some(supervisor) = self.inner.supervisor.get() {
            return Ok(supervisor.clone());
        }
        let supervisor =
            ConversationSupervisor::new().map_err(|error| lifecycle_failed(channel_name, error))?;
        if self.inner.supervisor.set(supervisor.clone()).is_ok() {
            Ok(supervisor)
        } else {
            self.inner.supervisor.get().cloned().ok_or_else(|| {
                lifecycle_failed(channel_name, "worker process supervisor unavailable")
            })
        }
    }
}

impl Default for WorkerContext {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for WorkerContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkerContext")
            .finish_non_exhaustive()
    }
}

impl DispatchWorkerPool for WorkerContext {
    fn workers_for(
        &self,
        channel_name: &ChannelName,
        request: &ActivityRequest,
    ) -> Result<Vec<DispatchWorker>, AionSurfaceError> {
        let _ = request;
        self.snapshot(channel_name)
    }
}

pub type WorkerPool = WorkerContext;

#[derive(Debug)]
pub struct WorkerRegistration {
    context: WorkerContext,
    entry: Arc<WorkerEntry>,
    link_monitor: Option<WorkerLinkMonitor>,
}

impl WorkerRegistration {
    const fn new(
        context: WorkerContext,
        entry: Arc<WorkerEntry>,
        link_monitor: WorkerLinkMonitor,
    ) -> Self {
        Self {
            context,
            entry,
            link_monitor: Some(link_monitor),
        }
    }

    #[must_use]
    pub fn worker_id(&self) -> &str {
        self.entry.worker_id.as_str()
    }

    #[must_use]
    pub fn channel_name(&self) -> &ChannelName {
        &self.entry.channel_name
    }

    #[must_use]
    pub fn participant(&self) -> ParticipantPid {
        self.entry.participant
    }

    #[must_use]
    pub fn capacity(&self) -> &WorkerCapacity {
        &self.entry.capacity
    }

    #[must_use]
    pub fn current_in_flight(&self) -> u32 {
        self.entry.current_in_flight.load(Ordering::Acquire)
    }

    pub fn set_in_flight(&self, count: u32) {
        self.entry.current_in_flight.store(count, Ordering::Release);
    }

    /// # Errors
    /// Returns when the subscription inbox cannot be read.
    pub fn try_next(&self) -> Result<Option<crate::envelope::Envelope>, AionSurfaceError> {
        let subscription = self
            .entry
            .subscription
            .lock()
            .map_err(|error| lifecycle_failed(&self.entry.channel_name, error))?;
        subscription.as_ref().map_or(Ok(None), |subscription| {
            subscription
                .try_next()
                .map_err(|error| lifecycle_failed(&self.entry.channel_name, error))
        })
    }

    /// # Errors
    /// Returns when the worker pool cannot be updated.
    pub fn unregister(mut self) -> Result<(), AionSurfaceError> {
        self.deactivate()?;
        self.entry.drop_subscription();
        if let Some(mut monitor) = self.link_monitor.take() {
            monitor.shutdown();
        }
        Ok(())
    }

    fn deactivate(&self) -> Result<(), AionSurfaceError> {
        self.entry.active.store(false, Ordering::Release);
        self.context.remove_inactive(&self.entry.channel_name)
    }
}

impl Drop for WorkerRegistration {
    fn drop(&mut self) {
        self.entry.active.store(false, Ordering::Release);
        let _ = self.context.remove_inactive(&self.entry.channel_name);
        self.entry.drop_subscription();
        if let Some(mut monitor) = self.link_monitor.take() {
            monitor.shutdown();
        }
    }
}

#[derive(Debug, Default)]
struct WorkerContextInner {
    channels: Mutex<BTreeMap<String, ChannelState>>,
    next_worker: AtomicU64,
    supervisor: OnceLock<ConversationSupervisor>,
}

#[derive(Clone, Debug)]
struct ChannelSession {
    handle: ChannelHandle,
}

#[derive(Debug)]
struct ChannelState {
    session: ChannelSession,
    entries: Vec<Weak<WorkerEntry>>,
}

impl ChannelState {
    const fn new(session: ChannelSession) -> Self {
        Self {
            session,
            entries: Vec::new(),
        }
    }

    fn retain_active(&mut self) {
        self.entries.retain(|entry| {
            entry
                .upgrade()
                .is_some_and(|entry| entry.active.load(Ordering::Acquire))
        });
    }
}

#[derive(Debug)]
struct WorkerEntry {
    channel_name: ChannelName,
    worker_id: String,
    participant: ParticipantPid,
    capacity: WorkerCapacity,
    subscription: Mutex<Option<SubscriptionHandle>>,
    current_in_flight: AtomicU32,
    active: AtomicBool,
}

impl WorkerEntry {
    pub(super) fn drop_subscription(&self) {
        if let Ok(mut subscription) = self.subscription.lock() {
            subscription.take();
        }
    }

    fn to_dispatch_worker(&self) -> DispatchWorker {
        let max_in_flight =
            u32::try_from(self.capacity.max_concurrent).map_or(u32::MAX, |value| value);
        let consumer_state = ConsumerStateView::new(
            ConsumerId::new(self.worker_id.clone()),
            self.current_in_flight.load(Ordering::Acquire),
            max_in_flight,
            0,
            self.capacity.activity_types.clone(),
        );
        DispatchWorker::with_consumer_state(
            self.worker_id.clone(),
            self.participant,
            consumer_state,
        )
    }
}

#[must_use]
pub fn default_worker_context() -> &'static WorkerContext {
    static DEFAULT_CONTEXT: OnceLock<WorkerContext> = OnceLock::new();
    DEFAULT_CONTEXT.get_or_init(WorkerContext::new)
}

/// # Errors
/// Returns when the worker cannot subscribe to its dispatch channel.
pub fn register_worker(
    namespace: &str,
    task_queue: &str,
    capacity: WorkerCapacity,
) -> Result<WorkerRegistration, AionSurfaceError> {
    default_worker_context().register_worker(namespace, task_queue, capacity)
}

fn lifecycle_failed(
    channel_name: &ChannelName,
    message: impl std::fmt::Display,
) -> AionSurfaceError {
    AionSurfaceError::ChannelLifecycleError {
        channel_name: String::from(channel_name.clone()),
        message: message.to_string(),
    }
}

#[cfg(test)]
mod tests;
