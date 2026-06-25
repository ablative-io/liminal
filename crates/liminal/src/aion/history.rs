use std::sync::{Arc, Mutex, MutexGuard};

use super::channels::{ChannelName, history_channel};
use super::error::AionSurfaceError;
use super::types::HistoryEvent;
use crate::channel::{ChannelConfig, ChannelHandle, ChannelMode};

#[path = "history/codec.rs"]
mod codec;
#[path = "history/projection.rs"]
mod projection;
#[cfg(test)]
#[path = "history/tests.rs"]
mod tests;
#[path = "history/types.rs"]
mod types;

pub use types::{
    HistoryChannel, HistoryPublishFailure, HistoryPublishReporter, HistorySubscription,
};

use codec::{encode_history_event, history_schema};
use projection::{HistoryProjection, InMemoryHistoryProjection, NoopHistoryReporter};
use types::{HistoryRegistry, HistorySession};

/// Dependencies and registry state used by workflow history streaming.
#[derive(Clone)]
pub struct HistoryContext {
    registry: Arc<Mutex<HistoryRegistry>>,
    projection: Arc<dyn HistoryProjection>,
    reporter: Arc<dyn HistoryPublishReporter>,
}

impl HistoryContext {
    /// Creates a history context with an in-memory durable projection and no-op failure reporter.
    #[must_use]
    pub fn new() -> Self {
        Self::with_reporter(Arc::new(NoopHistoryReporter))
    }

    /// Creates a history context with an explicit non-fatal publish failure reporter.
    #[must_use]
    pub fn with_reporter(reporter: Arc<dyn HistoryPublishReporter>) -> Self {
        Self::with_parts(Arc::new(InMemoryHistoryProjection::default()), reporter)
    }

    #[cfg(test)]
    fn with_projection(projection: Arc<dyn HistoryProjection>) -> Self {
        Self::with_parts(projection, Arc::new(NoopHistoryReporter))
    }

    #[cfg(test)]
    fn with_projection_and_reporter(
        projection: Arc<dyn HistoryProjection>,
        reporter: Arc<dyn HistoryPublishReporter>,
    ) -> Self {
        Self::with_parts(projection, reporter)
    }

    fn with_parts(
        projection: Arc<dyn HistoryProjection>,
        reporter: Arc<dyn HistoryPublishReporter>,
    ) -> Self {
        Self {
            registry: Arc::new(Mutex::new(HistoryRegistry::default())),
            projection,
            reporter,
        }
    }

    /// Starts durable history streaming for a workflow, creating or reusing its channel.
    ///
    /// # Errors
    ///
    /// Returns [`AionSurfaceError`] when the channel name is invalid or the channel cannot start.
    pub fn start_workflow_history(
        &self,
        namespace: &str,
        workflow_id: &str,
    ) -> Result<HistoryChannel, AionSurfaceError> {
        let channel_name = history_channel(namespace, workflow_id)?;
        Ok(self.session_for(&channel_name)?.to_channel())
    }

    /// Runs a successful durable-log append callback, then publishes the returned event.
    ///
    /// # Errors
    ///
    /// Returns the callback error only when the durable-log `append` itself fails. Once `append`
    /// succeeds, every history-streaming-side failure (channel resolution, session lifecycle,
    /// encoding, projection, and live fan-out) is reported through [`HistoryPublishReporter`] and
    /// never propagated, so a returned [`Ok`] guarantees the durable append succeeded.
    pub fn publish_after_record<Append>(
        &self,
        namespace: &str,
        workflow_id: &str,
        append: Append,
    ) -> Result<HistoryEvent, AionSurfaceError>
    where
        Append: FnOnce() -> Result<HistoryEvent, AionSurfaceError>,
    {
        let event = append()?;
        self.publish_recorded_event(namespace, workflow_id, event.clone())?;
        Ok(event)
    }

    /// Publishes a workflow event that has already been written to the durable workflow log.
    ///
    /// # Errors
    ///
    /// This method never returns an error: the durable workflow log is authoritative, so every
    /// history-streaming-side failure (channel resolution, session lifecycle, encoding, projection,
    /// and live fan-out) is reported through [`HistoryPublishReporter`] and swallowed.
    pub fn publish_recorded_event(
        &self,
        namespace: &str,
        workflow_id: &str,
        event: HistoryEvent,
    ) -> Result<(), AionSurfaceError> {
        let sequence = event.sequence;
        let channel_name = match history_channel(namespace, workflow_id) {
            Ok(channel_name) => channel_name,
            Err(error) => {
                self.report_publish_failure("", workflow_id, sequence, error);
                return Ok(());
            }
        };
        let session = match self.session_for(&channel_name) {
            Ok(session) => session,
            Err(error) => {
                self.report_publish_failure(channel_name.as_str(), workflow_id, sequence, error);
                return Ok(());
            }
        };
        let encoded = match encode_history_event(&channel_name, workflow_id, &event) {
            Ok(encoded) => encoded,
            Err(error) => {
                self.report_publish_failure(channel_name.as_str(), workflow_id, sequence, error);
                return Ok(());
            }
        };

        if let Err(error) = self.projection.append(channel_name.as_str(), event) {
            self.report_publish_failure(channel_name.as_str(), workflow_id, sequence, error);
            return Ok(());
        }
        if let Err(error) = session.handle.publish(encoded) {
            self.report_publish_failure(channel_name.as_str(), workflow_id, sequence, error);
        }
        Ok(())
    }

    /// Subscribes to typed workflow history events, replaying from the supplied sequence cursor.
    ///
    /// # Errors
    ///
    /// Returns [`AionSurfaceError`] when the channel cannot be resolved, subscribed, or replayed.
    pub fn subscribe_history(
        &self,
        namespace: &str,
        workflow_id: &str,
        cursor: Option<u64>,
    ) -> Result<HistorySubscription, AionSurfaceError> {
        let channel_name = history_channel(namespace, workflow_id)?;
        let session = self.session_for(&channel_name)?;
        let live = session
            .handle
            .subscribe()
            .map_err(|error| streaming_failed(&channel_name, workflow_id, error))?;
        let start_after = cursor.unwrap_or(0);
        let replay = self
            .projection
            .read_after(channel_name.as_str(), start_after)
            .map_err(|error| streaming_failed(&channel_name, workflow_id, error))?;
        Ok(HistorySubscription::new(
            channel_name,
            workflow_id.to_owned(),
            replay,
            live,
            start_after,
        ))
    }

    fn session_for(&self, channel_name: &ChannelName) -> Result<HistorySession, AionSurfaceError> {
        if let Some(session) = self.lookup_session(channel_name)? {
            return Ok(session);
        }

        self.projection
            .ensure_channel(channel_name.as_str())
            .map_err(|error| lifecycle_failed(channel_name, error))?;
        let schema = history_schema(channel_name)?;
        let handle = ChannelHandle::new(ChannelConfig::new(
            channel_name.as_str().to_owned(),
            schema,
            ChannelMode::Durable,
        ));
        self.insert_or_reuse(
            channel_name,
            HistorySession::new(channel_name.clone(), handle),
        )
    }

    fn lookup_session(
        &self,
        channel_name: &ChannelName,
    ) -> Result<Option<HistorySession>, AionSurfaceError> {
        let session = {
            let registry = self.lock_registry(channel_name)?;
            registry.active.get(channel_name.as_str()).cloned()
        };
        Ok(session)
    }

    fn insert_or_reuse(
        &self,
        channel_name: &ChannelName,
        session: HistorySession,
    ) -> Result<HistorySession, AionSurfaceError> {
        let stored = {
            let mut registry = self.lock_registry(channel_name)?;
            let key = channel_name.as_str().to_owned();
            registry.active.entry(key).or_insert(session).clone()
        };
        Ok(stored)
    }

    fn lock_registry(
        &self,
        channel_name: &ChannelName,
    ) -> Result<MutexGuard<'_, HistoryRegistry>, AionSurfaceError> {
        self.registry
            .lock()
            .map_err(|error| lifecycle_failed(channel_name, error))
    }

    fn report_publish_failure(
        &self,
        channel_name: impl Into<String>,
        workflow_id: &str,
        sequence: u64,
        error: impl std::fmt::Display,
    ) {
        let error = error.to_string();
        let failure = HistoryPublishFailure {
            channel_name: channel_name.into(),
            workflow_id: workflow_id.to_owned(),
            sequence,
            error,
        };
        tracing::warn!(
            channel_name = failure.channel_name.as_str(),
            workflow_id = failure.workflow_id.as_str(),
            sequence = failure.sequence,
            error = failure.error.as_str(),
            "history channel publish failed after event store write"
        );
        self.reporter.publish_failed(&failure);
    }
}

impl Default for HistoryContext {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for HistoryContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HistoryContext")
            .finish_non_exhaustive()
    }
}

/// Starts durable history streaming for a workflow using an explicit context.
///
/// # Errors
///
/// Returns [`AionSurfaceError`] when the channel name is invalid or the channel cannot start.
pub fn start_workflow_history(
    context: &HistoryContext,
    namespace: &str,
    workflow_id: &str,
) -> Result<HistoryChannel, AionSurfaceError> {
    context.start_workflow_history(namespace, workflow_id)
}

/// Runs a successful durable-log append callback, then publishes the returned history event.
///
/// # Errors
///
/// Returns the callback error when the durable-log append fails.
pub fn publish_history_after_record<Append>(
    context: &HistoryContext,
    namespace: &str,
    workflow_id: &str,
    append: Append,
) -> Result<HistoryEvent, AionSurfaceError>
where
    Append: FnOnce() -> Result<HistoryEvent, AionSurfaceError>,
{
    context.publish_after_record(namespace, workflow_id, append)
}

/// Publishes a workflow event already written to the durable workflow log.
///
/// # Errors
///
/// This function never returns an error: all history-streaming-side failures are reported through
/// [`HistoryPublishReporter`] and swallowed because the durable workflow log is authoritative.
pub fn publish_recorded_event(
    context: &HistoryContext,
    namespace: &str,
    workflow_id: &str,
    event: HistoryEvent,
) -> Result<(), AionSurfaceError> {
    context.publish_recorded_event(namespace, workflow_id, event)
}

/// Subscribes to typed workflow history events using an explicit context.
///
/// # Errors
///
/// Returns [`AionSurfaceError`] when the channel cannot be resolved, subscribed, or replayed.
pub fn subscribe_history(
    context: &HistoryContext,
    namespace: &str,
    workflow_id: &str,
    cursor: Option<u64>,
) -> Result<HistorySubscription, AionSurfaceError> {
    context.subscribe_history(namespace, workflow_id, cursor)
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

fn streaming_failed(
    channel_name: &ChannelName,
    workflow_id: &str,
    message: impl std::fmt::Display,
) -> AionSurfaceError {
    AionSurfaceError::StreamingFailed {
        channel_name: String::from(channel_name.clone()),
        workflow_id: workflow_id.to_owned(),
        message: message.to_string(),
    }
}
