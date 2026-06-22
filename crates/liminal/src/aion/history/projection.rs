use std::collections::BTreeMap;
use std::sync::Mutex;

use super::types::{HistoryPublishFailure, HistoryPublishReporter};
use crate::aion::types::HistoryEvent;

pub(super) trait HistoryProjection: std::fmt::Debug + Send + Sync {
    fn ensure_channel(&self, channel_name: &str) -> Result<(), HistoryProjectionError>;
    fn append(&self, channel_name: &str, event: HistoryEvent)
    -> Result<(), HistoryProjectionError>;
    fn read_after(
        &self,
        channel_name: &str,
        cursor: u64,
    ) -> Result<Vec<HistoryEvent>, HistoryProjectionError>;
}

#[derive(Debug, Default)]
pub(super) struct InMemoryHistoryProjection {
    streams: Mutex<BTreeMap<String, Vec<HistoryEvent>>>,
}

impl HistoryProjection for InMemoryHistoryProjection {
    fn ensure_channel(&self, channel_name: &str) -> Result<(), HistoryProjectionError> {
        self.streams
            .lock()
            .map_err(lock_projection_error)?
            .entry(channel_name.to_owned())
            .or_default();
        Ok(())
    }

    fn append(
        &self,
        channel_name: &str,
        event: HistoryEvent,
    ) -> Result<(), HistoryProjectionError> {
        self.streams
            .lock()
            .map_err(lock_projection_error)?
            .entry(channel_name.to_owned())
            .or_default()
            .push(event);
        Ok(())
    }

    fn read_after(
        &self,
        channel_name: &str,
        cursor: u64,
    ) -> Result<Vec<HistoryEvent>, HistoryProjectionError> {
        let events = {
            let streams = self.streams.lock().map_err(lock_projection_error)?;
            streams.get(channel_name).map_or_else(Vec::new, |events| {
                events
                    .iter()
                    .filter(|event| event.sequence > cursor)
                    .cloned()
                    .collect()
            })
        };
        Ok(events)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub(super) struct HistoryProjectionError {
    message: String,
}

impl HistoryProjectionError {
    pub(super) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct NoopHistoryReporter;

impl HistoryPublishReporter for NoopHistoryReporter {
    fn publish_failed(&self, failure: &HistoryPublishFailure) {
        std::hint::black_box(failure);
    }
}

fn lock_projection_error<T>(error: std::sync::PoisonError<T>) -> HistoryProjectionError {
    let message = format!("history projection unavailable: {error}");
    drop(error);
    HistoryProjectionError::new(message)
}
