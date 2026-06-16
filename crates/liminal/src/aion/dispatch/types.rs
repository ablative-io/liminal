use super::super::channels::ChannelName;
use super::super::codec::{DispatchRequest, DispatchResponse};
use super::super::error::AionSurfaceError;
use super::super::types::{ActivityRequest, ActivityResult};
use crate::conversation::ParticipantPid;
use crate::routing::{ConsumerId, ConsumerStateView};

/// Worker process visible to one dispatch routing decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DispatchWorker {
    /// Stable worker identity used by routing decisions and recorded events.
    pub worker_id: String,
    /// Beamr process identifier linked into the dispatch conversation.
    pub participant: ParticipantPid,
    /// Capacity and affinity view presented to the configured routing function.
    pub consumer_state: ConsumerStateView,
}

impl DispatchWorker {
    /// Creates a worker with a one-slot capacity view.
    #[must_use]
    pub fn new(worker_id: impl Into<String>, participant: ParticipantPid) -> Self {
        let worker_id = worker_id.into();
        let consumer_state =
            ConsumerStateView::new(ConsumerId::new(worker_id.clone()), 0, 1, 0, Vec::new());
        Self {
            worker_id,
            participant,
            consumer_state,
        }
    }

    /// Creates a worker with an explicit routing view.
    #[must_use]
    pub fn with_consumer_state(
        worker_id: impl Into<String>,
        participant: ParticipantPid,
        consumer_state: ConsumerStateView,
    ) -> Self {
        Self {
            worker_id: worker_id.into(),
            participant,
            consumer_state,
        }
    }
}

/// Aion activity state emitted as a recorder-visible channel operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActivityDispatchState {
    /// Dispatch conversation opened and the activity became scheduled.
    ActivityScheduled,
    /// A worker was selected and linked, so the activity started.
    ActivityStarted,
    /// The worker returned a completed activity result.
    ActivityCompleted,
    /// The worker failed or its linked process exited.
    ActivityFailed { retry_eligible: bool },
}

/// Recorder-visible dispatch channel operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DispatchOperationKind {
    /// The dispatch conversation boundary opened on the task queue channel.
    ConversationOpened,
    /// The configured routing function selected a worker.
    WorkerSelected,
    /// The activity request was sent through the conversation.
    MessageSent,
    /// A worker response was received through the conversation.
    MessageReceived,
    /// A linked worker process exited while the conversation was waiting.
    WorkerExited,
    /// The dispatch conversation boundary closed.
    ConversationClosed,
}

/// Structured event recorded for each non-deterministic dispatch operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DispatchOperation {
    /// Operation kind.
    pub kind: DispatchOperationKind,
    /// Conversation correlation identifier.
    pub conversation_id: String,
    /// Dispatch channel name.
    pub channel_name: String,
    /// Worker associated with the operation, when one has been selected.
    pub worker_id: Option<String>,
    /// Aion activity state mapped from the conversation lifecycle.
    pub activity_state: Option<ActivityDispatchState>,
    /// Activity result carried by receive operations.
    pub result: Option<ActivityResult>,
    /// Diagnostic detail for failure operations.
    pub message: Option<String>,
}

impl DispatchOperation {
    pub(crate) fn new(
        kind: DispatchOperationKind,
        conversation_id: &str,
        channel_name: &ChannelName,
    ) -> Self {
        Self {
            kind,
            conversation_id: conversation_id.to_owned(),
            channel_name: String::from(channel_name.clone()),
            worker_id: None,
            activity_state: None,
            result: None,
            message: None,
        }
    }

    pub(crate) fn worker(mut self, worker_id: impl Into<String>) -> Self {
        self.worker_id = Some(worker_id.into());
        self
    }

    pub(crate) const fn state(mut self, state: ActivityDispatchState) -> Self {
        self.activity_state = Some(state);
        self
    }

    pub(crate) fn result(mut self, result: ActivityResult) -> Self {
        self.result = Some(result);
        self
    }

    pub(crate) fn message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }
}

/// Recorded final dispatch outcome used by Aion's resolver during replay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordedDispatchOutcome {
    /// Final recorded dispatch result.
    pub result: Result<ActivityResult, AionSurfaceError>,
}

impl RecordedDispatchOutcome {
    /// Creates a replayable outcome from a dispatch result.
    #[must_use]
    pub const fn new(result: Result<ActivityResult, AionSurfaceError>) -> Self {
        Self { result }
    }

    pub(crate) fn into_result(self) -> Result<ActivityResult, AionSurfaceError> {
        self.result
    }
}

/// Recorder seam implemented by Aion's existing record/replay layer.
pub trait DispatchRecorder: std::fmt::Debug + Send + Sync {
    /// Returns a recorded outcome during replay, before any live conversation is opened.
    ///
    /// # Errors
    ///
    /// Returns an error if the recorder cannot read replay data.
    fn replay_outcome(
        &self,
        channel_name: &str,
        request: &ActivityRequest,
    ) -> Result<Option<RecordedDispatchOutcome>, AionSurfaceError>;

    /// Records one dispatch channel operation.
    ///
    /// # Errors
    ///
    /// Returns an error if the recorder cannot append the operation.
    fn record(&self, operation: DispatchOperation) -> Result<(), AionSurfaceError>;
}

/// Supplies the current task queue subscriber snapshot.
pub trait DispatchWorkerPool: std::fmt::Debug + Send + Sync {
    /// Returns workers currently subscribed to the dispatch channel.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscriber snapshot cannot be loaded.
    fn workers_for(
        &self,
        channel_name: &ChannelName,
        request: &ActivityRequest,
    ) -> Result<Vec<DispatchWorker>, AionSurfaceError>;
}

/// Selects a worker by invoking the channel's configured routing function.
pub trait DispatchRouter: std::fmt::Debug + Send + Sync {
    /// Selects one worker from the supplied snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if routing execution fails.
    fn select_worker(
        &self,
        workflow_id: &str,
        channel_name: &ChannelName,
        request: &ActivityRequest,
        candidates: &[DispatchWorker],
        excluded_worker_ids: &[String],
    ) -> Result<Option<DispatchWorker>, AionSurfaceError>;
}

/// Typed event returned while waiting on a dispatch conversation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DispatchConversationEvent {
    /// Worker returned an activity result.
    Response(DispatchResponse),
    /// Linked worker process exited before returning a result.
    WorkerExited { worker_id: String, message: String },
}

/// Open dispatch conversation with typed zero-hop request and response methods.
pub trait DispatchConversation: std::fmt::Debug + Send {
    /// Links a selected worker process before the request is sent.
    ///
    /// # Errors
    ///
    /// Returns an error if the process link cannot be established.
    fn link_worker(&mut self, worker: &DispatchWorker) -> Result<(), AionSurfaceError>;

    /// Sends a typed dispatch request through the conversation.
    ///
    /// # Errors
    ///
    /// Returns an error if the conversation cannot accept the request.
    fn send(&mut self, request: DispatchRequest) -> Result<(), AionSurfaceError>;

    /// Receives either a worker response or a linked-process exit event.
    ///
    /// # Errors
    ///
    /// Returns an error if the conversation cannot receive an event.
    fn receive(&mut self) -> Result<DispatchConversationEvent, AionSurfaceError>;

    /// Closes the conversation boundary.
    ///
    /// # Errors
    ///
    /// Returns an error if the conversation cannot close normally.
    fn close(&mut self) -> Result<(), AionSurfaceError>;
}

/// Opens dispatch conversations on task queue channels.
pub trait DispatchConversationFactory: std::fmt::Debug + Send + Sync {
    /// Opens a dispatch conversation boundary for `channel_name`.
    ///
    /// # Errors
    ///
    /// Returns an error if the conversation cannot be opened.
    fn open(
        &self,
        workflow_id: &str,
        channel_name: &ChannelName,
        conversation_id: &str,
    ) -> Result<Box<dyn DispatchConversation>, AionSurfaceError>;
}

/// Generates conversation correlation identifiers at the conversation-open boundary.
pub trait ConversationIdProvider: std::fmt::Debug + Send + Sync {
    /// Returns the next conversation identifier.
    fn next_conversation_id(&self) -> String;
}
