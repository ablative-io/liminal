use std::time::{Duration, SystemTime};

use super::error::AionSurfaceError;

/// Type-erased data exchanged across Aion activity, signal, and history surfaces.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Payload {
    /// Opaque payload bytes.
    pub data: Vec<u8>,
    /// Content type tag describing the encoded payload.
    pub content_type: String,
}

/// Activity dispatch request sent through an Aion dispatch channel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivityRequest {
    /// Activity type name requested by the workflow.
    pub activity_type: String,
    /// Type-erased activity input payload.
    pub input: Payload,
    /// Task queue that should receive the activity request.
    pub task_queue: String,
    /// Maximum time from scheduling to completion.
    pub schedule_to_close_timeout: Option<Duration>,
    /// Maximum time from worker start to completion.
    pub start_to_close_timeout: Option<Duration>,
}

/// Activity dispatch result returned by an Aion dispatch conversation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActivityResult {
    /// Activity completed with a type-erased output payload.
    Completed { output: Payload },
    /// Activity failed with integration-surface diagnostic context.
    Failed { error: AionSurfaceError },
}

/// Signal payload delivered through a workflow signal channel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignalPayload {
    /// Workflow-declared signal name.
    pub signal_name: String,
    /// Type-erased signal bytes and content type.
    pub payload: Payload,
}

/// Worker capacity declaration used by later dispatch routing briefs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerCapacity {
    /// Maximum number of activities the worker can run concurrently.
    pub max_concurrent: usize,
    /// Activity types accepted by this worker.
    pub activity_types: Vec<String>,
}

/// Workflow history event envelope published to a history channel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryEvent {
    /// Monotonic workflow-history sequence number.
    pub sequence: u64,
    /// Workflow event type name.
    pub event_type: String,
    /// Timestamp assigned to the history event.
    pub timestamp: SystemTime,
    /// Type-erased event payload.
    pub payload: Payload,
}

#[cfg(test)]
mod tests {
    use super::Payload;

    #[test]
    fn payload_default_is_empty() {
        let payload = Payload::default();

        assert!(payload.data.is_empty());
        assert!(payload.content_type.is_empty());
    }
}
