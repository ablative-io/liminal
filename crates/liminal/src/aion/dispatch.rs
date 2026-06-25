use std::sync::Arc;

mod defaults;
mod router;
#[cfg(test)]
mod tests;
mod types;

use defaults::{
    EmptyWorkerPool, NoopConversationFactory, NoopRecorder, NoopRouter, UuidConversationIds,
};
pub use router::RoutingFunctionDispatchRouter;
pub use types::{
    ActivityDispatchState, ConversationIdProvider, DispatchConversation, DispatchConversationEvent,
    DispatchConversationFactory, DispatchOperation, DispatchOperationKind, DispatchRecorder,
    DispatchRouter, DispatchWorker, DispatchWorkerPool, RecordedDispatchOutcome,
};

use super::channels::{ChannelName, dispatch_channel};
use super::codec::{DispatchRequest, DispatchResponse};
use super::error::AionSurfaceError;
use super::types::{ActivityRequest, ActivityResult};

const DEFAULT_WORKFLOW_ID: &str = "aion-dispatch";

/// Dependencies used by activity dispatch.
#[derive(Clone)]
pub struct DispatchContext {
    workflow_id: String,
    worker_pool: Arc<dyn DispatchWorkerPool>,
    router: Arc<dyn DispatchRouter>,
    conversations: Arc<dyn DispatchConversationFactory>,
    recorder: Arc<dyn DispatchRecorder>,
    ids: Arc<dyn ConversationIdProvider>,
}

impl DispatchContext {
    /// Creates a dispatch context from explicit integration dependencies.
    #[must_use]
    pub fn new(
        workflow_id: impl Into<String>,
        worker_pool: Arc<dyn DispatchWorkerPool>,
        router: Arc<dyn DispatchRouter>,
        conversations: Arc<dyn DispatchConversationFactory>,
        recorder: Arc<dyn DispatchRecorder>,
        ids: Arc<dyn ConversationIdProvider>,
    ) -> Self {
        Self {
            workflow_id: workflow_id.into(),
            worker_pool,
            router,
            conversations,
            recorder,
            ids,
        }
    }

    /// Creates an embedded in-process context with no registered workers yet.
    #[must_use]
    pub fn embedded_no_workers(workflow_id: impl Into<String>) -> Self {
        Self::new(
            workflow_id,
            Arc::new(EmptyWorkerPool),
            Arc::new(NoopRouter),
            Arc::new(NoopConversationFactory),
            Arc::new(NoopRecorder),
            Arc::new(UuidConversationIds),
        )
    }

    fn workflow_id(&self) -> &str {
        if self.workflow_id.is_empty() {
            DEFAULT_WORKFLOW_ID
        } else {
            self.workflow_id.as_str()
        }
    }
}

impl Default for DispatchContext {
    fn default() -> Self {
        Self::embedded_no_workers(DEFAULT_WORKFLOW_ID)
    }
}

impl std::fmt::Debug for DispatchContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DispatchContext")
            .field("workflow_id", &self.workflow_id())
            .finish_non_exhaustive()
    }
}

/// Dispatches an activity using the default embedded context.
///
/// # Errors
///
/// Returns [`AionSurfaceError`] when the channel name is invalid, no worker is available, the
/// conversation fails, or the worker reports an activity failure.
pub fn dispatch_activity(
    namespace: &str,
    task_queue: &str,
    request: ActivityRequest,
) -> Result<ActivityResult, AionSurfaceError> {
    dispatch_activity_with_context(&DispatchContext::default(), namespace, task_queue, request)
}

/// Dispatches an activity through a recorder-compatible liminal conversation.
///
/// # Errors
///
/// Returns [`AionSurfaceError`] when the channel name is invalid, no worker is available, the
/// conversation fails, or the worker reports an activity failure.
pub fn dispatch_activity_with_context(
    context: &DispatchContext,
    namespace: &str,
    task_queue: &str,
    request: ActivityRequest,
) -> Result<ActivityResult, AionSurfaceError> {
    let request = (request,);
    dispatch_activity_ref(context, namespace, task_queue, &request.0)
}

fn dispatch_activity_ref(
    context: &DispatchContext,
    namespace: &str,
    task_queue: &str,
    request: &ActivityRequest,
) -> Result<ActivityResult, AionSurfaceError> {
    let channel_name = dispatch_channel(namespace, task_queue)?;
    if let Some(outcome) = context
        .recorder
        .replay_outcome(channel_name.as_str(), request)?
    {
        return outcome.into_result();
    }

    let conversation_id = context.ids.next_conversation_id();
    let mut conversation = context.conversations.open(
        context.workflow_id(),
        &channel_name,
        conversation_id.as_str(),
    )?;
    record_operation(
        context,
        DispatchOperation::new(
            DispatchOperationKind::ConversationOpened,
            conversation_id.as_str(),
            &channel_name,
        )
        .state(ActivityDispatchState::ActivityScheduled),
    )?;

    let mut excluded_workers = Vec::new();
    let outcome = dispatch_attempts(
        context,
        &channel_name,
        conversation_id.as_str(),
        request,
        &mut *conversation,
        &mut excluded_workers,
    );
    let close_result = close_conversation(
        context,
        &channel_name,
        conversation_id.as_str(),
        &mut *conversation,
    );

    match (outcome, close_result) {
        (Ok(result), Ok(())) => Ok(result),
        (Err(error), Ok(())) | (_, Err(error)) => Err(error),
    }
}

fn dispatch_attempts(
    context: &DispatchContext,
    channel_name: &ChannelName,
    conversation_id: &str,
    request: &ActivityRequest,
    conversation: &mut dyn DispatchConversation,
    excluded_workers: &mut Vec<String>,
) -> Result<ActivityResult, AionSurfaceError> {
    loop {
        let worker = select_worker(context, channel_name, request, excluded_workers)?;
        start_activity(
            context,
            channel_name,
            conversation_id,
            conversation,
            &worker,
        )?;
        send_request(
            context,
            channel_name,
            conversation_id,
            request,
            conversation,
            &worker,
        )?;

        match conversation.receive()? {
            DispatchConversationEvent::Response(response) => {
                record_response(context, channel_name, conversation_id, &response)?;
                return map_activity_result(context, channel_name, response);
            }
            DispatchConversationEvent::WorkerExited { worker_id, message } => {
                record_worker_exit(context, channel_name, conversation_id, &worker_id, message)?;
                excluded_workers.push(worker_id);
            }
        }
    }
}

fn start_activity(
    context: &DispatchContext,
    channel_name: &ChannelName,
    conversation_id: &str,
    conversation: &mut dyn DispatchConversation,
    worker: &DispatchWorker,
) -> Result<(), AionSurfaceError> {
    conversation.link_worker(worker)?;
    record_operation(
        context,
        DispatchOperation::new(
            DispatchOperationKind::WorkerSelected,
            conversation_id,
            channel_name,
        )
        .worker(worker.worker_id.clone())
        .state(ActivityDispatchState::ActivityStarted),
    )
}

fn send_request(
    context: &DispatchContext,
    channel_name: &ChannelName,
    conversation_id: &str,
    request: &ActivityRequest,
    conversation: &mut dyn DispatchConversation,
    worker: &DispatchWorker,
) -> Result<(), AionSurfaceError> {
    let dispatch_request = DispatchRequest::new(conversation_id.to_owned(), request.clone());
    conversation.send(dispatch_request)?;
    record_operation(
        context,
        DispatchOperation::new(
            DispatchOperationKind::MessageSent,
            conversation_id,
            channel_name,
        )
        .worker(worker.worker_id.clone()),
    )
}

fn record_response(
    context: &DispatchContext,
    channel_name: &ChannelName,
    conversation_id: &str,
    response: &DispatchResponse,
) -> Result<(), AionSurfaceError> {
    let result = response.result.clone();
    let mut operation = DispatchOperation::new(
        DispatchOperationKind::MessageReceived,
        conversation_id,
        channel_name,
    )
    .worker(response.worker_id.clone())
    .state(result_state(&result))
    .result(result.clone());
    if let Some(message) = result_message(&result) {
        operation = operation.message(message);
    }
    record_operation(context, operation)
}

fn record_worker_exit(
    context: &DispatchContext,
    channel_name: &ChannelName,
    conversation_id: &str,
    worker_id: &str,
    message: String,
) -> Result<(), AionSurfaceError> {
    record_operation(
        context,
        DispatchOperation::new(
            DispatchOperationKind::WorkerExited,
            conversation_id,
            channel_name,
        )
        .worker(worker_id.to_owned())
        .state(ActivityDispatchState::ActivityFailed {
            retry_eligible: true,
        })
        .message(message),
    )
}

fn select_worker(
    context: &DispatchContext,
    channel_name: &ChannelName,
    request: &ActivityRequest,
    excluded_workers: &[String],
) -> Result<DispatchWorker, AionSurfaceError> {
    let candidates = context.worker_pool.workers_for(channel_name, request)?;
    context
        .router
        .select_worker(
            context.workflow_id(),
            channel_name,
            request,
            &candidates,
            excluded_workers,
        )?
        .ok_or_else(|| {
            dispatch_failed(
                channel_name,
                context.workflow_id(),
                "NoWorkersAvailable: no dispatch workers are available",
            )
        })
}

fn close_conversation(
    context: &DispatchContext,
    channel_name: &ChannelName,
    conversation_id: &str,
    conversation: &mut dyn DispatchConversation,
) -> Result<(), AionSurfaceError> {
    conversation.close()?;
    record_operation(
        context,
        DispatchOperation::new(
            DispatchOperationKind::ConversationClosed,
            conversation_id,
            channel_name,
        ),
    )
}

fn map_activity_result(
    context: &DispatchContext,
    channel_name: &ChannelName,
    response: DispatchResponse,
) -> Result<ActivityResult, AionSurfaceError> {
    match response.result {
        ActivityResult::Completed { output } => Ok(ActivityResult::Completed { output }),
        ActivityResult::Failed { error } => Err(dispatch_failed(
            channel_name,
            context.workflow_id(),
            format!("worker '{}' failed activity: {error}", response.worker_id),
        )),
    }
}

const fn result_state(result: &ActivityResult) -> ActivityDispatchState {
    match result {
        ActivityResult::Completed { .. } => ActivityDispatchState::ActivityCompleted,
        ActivityResult::Failed { .. } => ActivityDispatchState::ActivityFailed {
            retry_eligible: false,
        },
    }
}

fn result_message(result: &ActivityResult) -> Option<String> {
    match result {
        ActivityResult::Completed { .. } => None,
        ActivityResult::Failed { error } => Some(error.to_string()),
    }
}

fn record_operation(
    context: &DispatchContext,
    operation: DispatchOperation,
) -> Result<(), AionSurfaceError> {
    context.recorder.record(operation)
}

fn dispatch_failed(
    channel_name: &ChannelName,
    workflow_id: &str,
    message: impl Into<String>,
) -> AionSurfaceError {
    AionSurfaceError::DispatchFailed {
        channel_name: String::from(channel_name.clone()),
        workflow_id: workflow_id.to_owned(),
        message: message.into(),
    }
}
