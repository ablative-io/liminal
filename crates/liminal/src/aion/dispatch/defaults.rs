use uuid::Uuid;

use super::super::channels::ChannelName;
use super::super::codec::DispatchRequest;
use super::super::error::AionSurfaceError;
use super::super::types::ActivityRequest;
use super::{
    ConversationIdProvider, DispatchConversation, DispatchConversationEvent,
    DispatchConversationFactory, DispatchRecorder, DispatchWorker, DispatchWorkerPool,
    RecordedDispatchOutcome, dispatch_failed,
};
#[derive(Debug)]
pub(super) struct EmptyWorkerPool;

impl DispatchWorkerPool for EmptyWorkerPool {
    fn workers_for(
        &self,
        channel_name: &ChannelName,
        request: &ActivityRequest,
    ) -> Result<Vec<DispatchWorker>, AionSurfaceError> {
        let _ = (channel_name, request);
        Ok(Vec::new())
    }
}

#[derive(Debug)]
pub(super) struct NoopRouter;

impl super::DispatchRouter for NoopRouter {
    fn select_worker(
        &self,
        workflow_id: &str,
        channel_name: &ChannelName,
        request: &ActivityRequest,
        candidates: &[DispatchWorker],
        excluded_worker_ids: &[String],
    ) -> Result<Option<DispatchWorker>, AionSurfaceError> {
        let _ = (
            workflow_id,
            channel_name,
            request,
            candidates,
            excluded_worker_ids,
        );
        Ok(None)
    }
}

#[derive(Debug)]
pub(super) struct NoopConversationFactory;

impl DispatchConversationFactory for NoopConversationFactory {
    fn open(
        &self,
        workflow_id: &str,
        channel_name: &ChannelName,
        conversation_id: &str,
    ) -> Result<Box<dyn DispatchConversation>, AionSurfaceError> {
        let _ = conversation_id;
        Ok(Box::new(NoopConversation {
            workflow_id: workflow_id.to_owned(),
            channel_name: channel_name.clone(),
        }))
    }
}

#[derive(Debug)]
struct NoopConversation {
    workflow_id: String,
    channel_name: ChannelName,
}

impl DispatchConversation for NoopConversation {
    fn link_worker(&mut self, worker: &DispatchWorker) -> Result<(), AionSurfaceError> {
        let _ = worker;
        Ok(())
    }

    fn send(&mut self, request: DispatchRequest) -> Result<(), AionSurfaceError> {
        let _ = request;
        Ok(())
    }

    fn receive(&mut self) -> Result<DispatchConversationEvent, AionSurfaceError> {
        Err(dispatch_failed(
            &self.channel_name,
            self.workflow_id.as_str(),
            "NoWorkersAvailable: no response can be received without a worker",
        ))
    }

    fn close(&mut self) -> Result<(), AionSurfaceError> {
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct NoopRecorder;

impl DispatchRecorder for NoopRecorder {
    fn replay_outcome(
        &self,
        channel_name: &str,
        request: &ActivityRequest,
    ) -> Result<Option<RecordedDispatchOutcome>, AionSurfaceError> {
        let _ = (channel_name, request);
        Ok(None)
    }

    fn record(&self, operation: super::DispatchOperation) -> Result<(), AionSurfaceError> {
        let _ = operation;
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct UuidConversationIds;

impl ConversationIdProvider for UuidConversationIds {
    fn next_conversation_id(&self) -> String {
        Uuid::new_v4().to_string()
    }
}
