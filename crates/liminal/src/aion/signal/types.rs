use super::super::channels::ChannelName;
use super::super::error::AionSurfaceError;
use super::super::types::{Payload, SignalPayload};
use crate::channel::{ChannelHandle, ChannelMode, Schema};
use crate::conversation::ParticipantPid;

/// Workflow-declared signal type supplied by Aion's workflow definition.
#[derive(Clone, Debug)]
pub struct SignalDeclaration {
    /// Workflow-declared signal name.
    pub signal_name: String,
    /// Required payload content type for this signal.
    pub content_type: String,
    /// Optional schema used to validate the signal payload bytes before publish.
    pub payload_schema: Option<Schema>,
}

impl SignalDeclaration {
    /// Creates a declaration that validates signal name and content type.
    #[must_use]
    pub fn new(signal_name: impl Into<String>, content_type: impl Into<String>) -> Self {
        Self {
            signal_name: signal_name.into(),
            content_type: content_type.into(),
            payload_schema: None,
        }
    }

    /// Creates a declaration that also validates payload bytes against a schema.
    #[must_use]
    pub fn with_payload_schema(
        signal_name: impl Into<String>,
        content_type: impl Into<String>,
        payload_schema: Schema,
    ) -> Self {
        Self {
            signal_name: signal_name.into(),
            content_type: content_type.into(),
            payload_schema: Some(payload_schema),
        }
    }
}

/// Signal channel configuration for one workflow instance.
#[derive(Clone, Debug)]
pub struct SignalWorkflowConfig {
    /// Aion namespace that owns the workflow.
    pub namespace: String,
    /// Workflow identifier used in the signal channel name.
    pub workflow_id: String,
    /// Workflow process that receives delivered signals in its normal mailbox.
    pub workflow_pid: ParticipantPid,
    /// Signal declarations supplied by the workflow definition.
    pub declarations: Vec<SignalDeclaration>,
    /// Per-workflow signal channel durability mode.
    pub mode: ChannelMode,
}

impl SignalWorkflowConfig {
    /// Creates an ephemeral signal workflow configuration.
    #[must_use]
    pub fn new(
        namespace: impl Into<String>,
        id: impl Into<String>,
        pid: ParticipantPid,
        declarations: Vec<SignalDeclaration>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            workflow_id: id.into(),
            workflow_pid: pid,
            declarations,
            mode: ChannelMode::Ephemeral,
        }
    }

    /// Sets the per-workflow durability mode.
    #[must_use]
    pub const fn with_mode(mut self, mode: ChannelMode) -> Self {
        self.mode = mode;
        self
    }
}

/// Active signal channel session returned when a workflow declares signal handlers.
#[derive(Clone, Debug)]
pub struct SignalChannel {
    /// Validated channel name following `aion.signal.{namespace}.{workflow_id}`.
    pub channel_name: ChannelName,
    /// Typed channel handle used for publish-time schema validation and fan-out.
    pub handle: ChannelHandle,
    /// Signal declarations used to type the channel.
    pub declarations: Vec<SignalDeclaration>,
    /// Per-workflow durability mode.
    pub mode: ChannelMode,
}

/// Terminal workflow states that tear down signal delivery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkflowTerminalStatus {
    /// Workflow completed normally.
    Completed,
    /// Workflow failed.
    Failed,
    /// Workflow was cancelled.
    Cancelled,
    /// Workflow timed out.
    TimedOut,
}

impl WorkflowTerminalStatus {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "Completed",
            Self::Failed => "Failed",
            Self::Cancelled => "Cancelled",
            Self::TimedOut => "TimedOut",
        }
    }
}

/// Recorder-visible signal channel operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SignalOperationKind {
    /// A validated signal was delivered to the workflow mailbox.
    SignalDelivered,
}

/// Structured event recorded for durable signal delivery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignalOperation {
    /// Operation kind.
    pub kind: SignalOperationKind,
    /// Signal channel name.
    pub channel_name: String,
    /// Workflow that received the signal.
    pub workflow_id: String,
    /// Delivered signal name.
    pub signal_name: String,
    /// Delivered signal payload.
    pub payload: Payload,
    /// Per-workflow channel mode at delivery time.
    pub mode: ChannelMode,
}

impl SignalOperation {
    pub(super) fn delivered(
        channel_name: &ChannelName,
        workflow_id: &str,
        signal: &SignalPayload,
        mode: ChannelMode,
    ) -> Self {
        Self {
            kind: SignalOperationKind::SignalDelivered,
            channel_name: String::from(channel_name.clone()),
            workflow_id: workflow_id.to_owned(),
            signal_name: signal.signal_name.clone(),
            payload: signal.payload.clone(),
            mode,
        }
    }
}

/// Recorded signal delivery returned to Aion's resolver during replay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordedSignalDelivery {
    /// Signal channel name that originally delivered the signal.
    pub channel_name: String,
    /// Workflow that originally received the signal.
    pub workflow_id: String,
    /// Recorded signal payload.
    pub signal: SignalPayload,
}

impl RecordedSignalDelivery {
    /// Creates a replayable signal delivery event.
    #[must_use]
    pub const fn new(channel_name: String, workflow_id: String, signal: SignalPayload) -> Self {
        Self {
            channel_name,
            workflow_id,
            signal,
        }
    }
}

/// Sends validated signal payloads into the workflow process mailbox.
pub trait SignalDeliverer: std::fmt::Debug + Send + Sync {
    /// Delivers one standard workflow mailbox message.
    ///
    /// # Errors
    ///
    /// Returns [`AionSurfaceError`] when the workflow process cannot accept the signal.
    fn deliver(
        &self,
        workflow_pid: ParticipantPid,
        signal: SignalPayload,
    ) -> Result<(), AionSurfaceError>;
}

/// Recorder seam implemented by Aion's durable event log and replay resolver.
pub trait SignalRecorder: std::fmt::Debug + Send + Sync {
    /// Returns recorded signal deliveries during replay without touching live channels.
    ///
    /// # Errors
    ///
    /// Returns an error if replay data cannot be read.
    fn replay_deliveries(
        &self,
        channel_name: &str,
        workflow_id: &str,
    ) -> Result<Vec<RecordedSignalDelivery>, AionSurfaceError>;

    /// Records one durable signal delivery operation.
    ///
    /// # Errors
    ///
    /// Returns an error if the event log cannot append the operation.
    fn record(&self, operation: SignalOperation) -> Result<(), AionSurfaceError>;
}
