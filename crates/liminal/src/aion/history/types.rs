use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use super::codec::decode_history_event;
use super::streaming_failed;
use crate::aion::channels::ChannelName;
use crate::aion::error::AionSurfaceError;
use crate::aion::types::HistoryEvent;
use crate::channel::{ChannelHandle, ChannelMode, SubscriptionHandle};

/// Active durable workflow history channel returned when a workflow starts.
#[derive(Clone, Debug)]
pub struct HistoryChannel {
    /// Validated channel name following `aion.history.{namespace}.{workflow_id}`.
    pub channel_name: ChannelName,
    /// History channels are always durable.
    pub mode: ChannelMode,
}

/// Diagnostic emitted when history projection publish fails after the durable workflow log wrote.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error(
    "history publish failed for workflow '{workflow_id}' sequence {sequence} on channel '{channel_name}': {error}"
)]
pub struct HistoryPublishFailure {
    /// History channel that failed to publish or fan out an event.
    pub channel_name: String,
    /// Workflow whose history projection failed.
    pub workflow_id: String,
    /// Workflow-history sequence number that failed to publish.
    pub sequence: u64,
    /// Human-readable failure detail.
    pub error: String,
}

/// Observer used to report non-fatal history projection publish failures.
pub trait HistoryPublishReporter: std::fmt::Debug + Send + Sync {
    /// Records a non-fatal publish failure with workflow and sequence context.
    fn publish_failed(&self, failure: &HistoryPublishFailure);
}

/// Typed subscription returned to workflow history consumers.
#[derive(Clone, Debug)]
pub struct HistorySubscription {
    channel_name: ChannelName,
    workflow_id: String,
    live: SubscriptionHandle,
    state: Arc<Mutex<HistorySubscriptionState>>,
}

impl HistorySubscription {
    pub(super) fn new(
        channel_name: ChannelName,
        workflow_id: String,
        replay: Vec<HistoryEvent>,
        live: SubscriptionHandle,
        last_sequence: u64,
    ) -> Self {
        Self {
            channel_name,
            workflow_id,
            live,
            state: Arc::new(Mutex::new(HistorySubscriptionState {
                replay: replay.into(),
                last_sequence,
            })),
        }
    }

    /// Returns the history channel name this subscription reads from.
    #[must_use]
    pub const fn channel_name(&self) -> &ChannelName {
        &self.channel_name
    }

    /// Returns the workflow id this subscription reads from.
    #[must_use]
    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    /// Attempts to receive the next typed history event without blocking.
    ///
    /// # Errors
    ///
    /// Returns [`AionSurfaceError`] when replay state, live subscription, or decoding fails.
    pub fn try_next(&self) -> Result<Option<HistoryEvent>, AionSurfaceError> {
        if let Some(event) = self.next_replay_event()? {
            return Ok(Some(event));
        }

        loop {
            let Some(envelope) = self.live.try_next().map_err(|error| {
                streaming_failed(&self.channel_name, self.workflow_id.as_str(), error)
            })?
            else {
                return Ok(None);
            };
            let event = decode_history_event(
                &self.channel_name,
                self.workflow_id.as_str(),
                &envelope.payload,
            )?;
            if self.mark_delivered(&event)? {
                return Ok(Some(event));
            }
        }
    }

    fn next_replay_event(&self) -> Result<Option<HistoryEvent>, AionSurfaceError> {
        let mut state = self.state.lock().map_err(|error| {
            streaming_failed(&self.channel_name, self.workflow_id.as_str(), error)
        })?;
        while let Some(event) = state.replay.pop_front() {
            if event.sequence > state.last_sequence {
                state.last_sequence = event.sequence;
                drop(state);
                return Ok(Some(event));
            }
        }
        drop(state);
        Ok(None)
    }

    fn mark_delivered(&self, event: &HistoryEvent) -> Result<bool, AionSurfaceError> {
        let mut state = self.state.lock().map_err(|error| {
            streaming_failed(&self.channel_name, self.workflow_id.as_str(), error)
        })?;
        if event.sequence <= state.last_sequence {
            drop(state);
            return Ok(false);
        }
        state.last_sequence = event.sequence;
        drop(state);
        Ok(true)
    }
}

#[derive(Clone, Debug)]
pub(super) struct HistorySession {
    channel_name: ChannelName,
    pub(super) handle: ChannelHandle,
    mode: ChannelMode,
}

impl HistorySession {
    pub(super) const fn new(channel_name: ChannelName, handle: ChannelHandle) -> Self {
        Self {
            channel_name,
            handle,
            mode: ChannelMode::Durable,
        }
    }

    pub(super) fn to_channel(&self) -> HistoryChannel {
        HistoryChannel {
            channel_name: self.channel_name.clone(),
            mode: self.mode,
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct HistoryRegistry {
    pub(super) active: BTreeMap<String, HistorySession>,
}

#[derive(Debug)]
struct HistorySubscriptionState {
    replay: VecDeque<HistoryEvent>,
    last_sequence: u64,
}
