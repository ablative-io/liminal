use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};

use super::*;
use crate::aion::codec::DispatchResponse;
use crate::aion::{ActivityRequest, Payload, dispatch_channel};
use crate::conversation::ParticipantPid;
use crate::routing::{ConsumerId, ConsumerStateView};

#[derive(Clone)]
pub(super) struct TestSetup {
    pub(super) pool: Arc<TestWorkerPool>,
    pub(super) router: Arc<RecordingRouter>,
    pub(super) factory: Arc<TestConversationFactory>,
    pub(super) conversation: Arc<TestConversationState>,
    pub(super) recorder: Arc<TestRecorder>,
}

impl TestSetup {
    pub(super) fn new(
        workers: Vec<DispatchWorker>,
        events: Vec<DispatchConversationEvent>,
    ) -> Self {
        Self::with_recorder(workers, events, TestRecorder::recording())
    }

    pub(super) fn replaying(outcome: RecordedDispatchOutcome) -> Self {
        Self::with_recorder(Vec::new(), Vec::new(), TestRecorder::replaying(outcome))
    }

    fn with_recorder(
        workers: Vec<DispatchWorker>,
        events: Vec<DispatchConversationEvent>,
        recorder: TestRecorder,
    ) -> Self {
        let conversation = Arc::new(TestConversationState::new(events));
        Self {
            pool: Arc::new(TestWorkerPool::new(workers)),
            router: Arc::new(RecordingRouter::new()),
            factory: Arc::new(TestConversationFactory::new(Arc::clone(&conversation))),
            conversation,
            recorder: Arc::new(recorder),
        }
    }

    pub(super) fn context(&self) -> DispatchContext {
        DispatchContext::new(
            "wf-1",
            self.pool.clone(),
            self.router.clone(),
            self.factory.clone(),
            self.recorder.clone(),
            Arc::new(FixedIds),
        )
    }
}

#[derive(Debug)]
pub(super) struct TestWorkerPool {
    workers: Mutex<Vec<DispatchWorker>>,
    calls: Mutex<usize>,
}

impl TestWorkerPool {
    fn new(workers: Vec<DispatchWorker>) -> Self {
        Self {
            workers: Mutex::new(workers),
            calls: Mutex::new(0),
        }
    }

    pub(super) fn set_workers(&self, workers: Vec<DispatchWorker>) -> Result<(), AionSurfaceError> {
        *lock(&self.workers)? = workers;
        Ok(())
    }

    pub(super) fn calls(&self) -> Result<usize, AionSurfaceError> {
        Ok(*lock(&self.calls)?)
    }
}

impl DispatchWorkerPool for TestWorkerPool {
    fn workers_for(
        &self,
        channel_name: &ChannelName,
        request: &ActivityRequest,
    ) -> Result<Vec<DispatchWorker>, AionSurfaceError> {
        let _ = (channel_name, request);
        *lock(&self.calls)? += 1;
        Ok(lock(&self.workers)?.clone())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RouterCall {
    pub(super) candidates: Vec<String>,
    pub(super) excluded: Vec<String>,
}

#[derive(Debug)]
pub(super) struct RecordingRouter {
    calls: Mutex<Vec<RouterCall>>,
    decisions: Mutex<VecDeque<Option<String>>>,
}

impl RecordingRouter {
    fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            decisions: Mutex::new(VecDeque::new()),
        }
    }

    pub(super) fn push_decision(&self, decision: Option<&str>) -> Result<(), AionSurfaceError> {
        lock(&self.decisions)?.push_back(decision.map(str::to_owned));
        Ok(())
    }

    pub(super) fn calls(&self) -> Result<Vec<RouterCall>, AionSurfaceError> {
        Ok(lock(&self.calls)?.clone())
    }
}

impl DispatchRouter for RecordingRouter {
    fn select_worker(
        &self,
        workflow_id: &str,
        channel_name: &ChannelName,
        request: &ActivityRequest,
        candidates: &[DispatchWorker],
        excluded_worker_ids: &[String],
    ) -> Result<Option<DispatchWorker>, AionSurfaceError> {
        let _ = (workflow_id, channel_name, request);
        lock(&self.calls)?.push(RouterCall {
            candidates: candidates
                .iter()
                .map(|worker| worker.worker_id.clone())
                .collect(),
            excluded: excluded_worker_ids.to_vec(),
        });
        let selected = lock(&self.decisions)?.pop_front().flatten();
        Ok(candidates
            .iter()
            .find(|worker| {
                let requested = selected.as_ref().is_none_or(|id| id == &worker.worker_id);
                requested && !excluded_worker_ids.iter().any(|id| id == &worker.worker_id)
            })
            .cloned())
    }
}

#[derive(Debug)]
pub(super) struct TestConversationFactory {
    state: Arc<TestConversationState>,
    opens: Mutex<Vec<String>>,
}

impl TestConversationFactory {
    fn new(state: Arc<TestConversationState>) -> Self {
        Self {
            state,
            opens: Mutex::new(Vec::new()),
        }
    }

    pub(super) fn opens(&self) -> Result<Vec<String>, AionSurfaceError> {
        Ok(lock(&self.opens)?.clone())
    }
}

impl DispatchConversationFactory for TestConversationFactory {
    fn open(
        &self,
        workflow_id: &str,
        channel_name: &ChannelName,
        conversation_id: &str,
    ) -> Result<Box<dyn DispatchConversation>, AionSurfaceError> {
        let _ = (workflow_id, conversation_id);
        lock(&self.opens)?.push(String::from(channel_name.clone()));
        Ok(Box::new(TestConversation {
            state: Arc::clone(&self.state),
        }))
    }
}

#[derive(Debug)]
pub(super) struct TestConversationState {
    events: Mutex<VecDeque<DispatchConversationEvent>>,
    links: Mutex<Vec<String>>,
    sends: Mutex<Vec<DispatchRequest>>,
    closes: Mutex<usize>,
}

impl TestConversationState {
    fn new(events: Vec<DispatchConversationEvent>) -> Self {
        Self {
            events: Mutex::new(events.into()),
            links: Mutex::new(Vec::new()),
            sends: Mutex::new(Vec::new()),
            closes: Mutex::new(0),
        }
    }

    pub(super) fn links(&self) -> Result<Vec<String>, AionSurfaceError> {
        Ok(lock(&self.links)?.clone())
    }

    pub(super) fn sent_requests(&self) -> Result<Vec<DispatchRequest>, AionSurfaceError> {
        Ok(lock(&self.sends)?.clone())
    }

    pub(super) fn closes(&self) -> Result<usize, AionSurfaceError> {
        Ok(*lock(&self.closes)?)
    }
}

#[derive(Debug)]
struct TestConversation {
    state: Arc<TestConversationState>,
}

impl DispatchConversation for TestConversation {
    fn link_worker(&mut self, worker: &DispatchWorker) -> Result<(), AionSurfaceError> {
        lock(&self.state.links)?.push(worker.worker_id.clone());
        Ok(())
    }

    fn send(&mut self, request: DispatchRequest) -> Result<(), AionSurfaceError> {
        lock(&self.state.sends)?.push(request);
        Ok(())
    }

    fn receive(&mut self) -> Result<DispatchConversationEvent, AionSurfaceError> {
        lock(&self.state.events)?
            .pop_front()
            .ok_or_else(|| test_error("no scripted conversation event"))
    }

    fn close(&mut self) -> Result<(), AionSurfaceError> {
        *lock(&self.state.closes)? += 1;
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct TestRecorder {
    replay: Option<RecordedDispatchOutcome>,
    operations: Mutex<Vec<DispatchOperation>>,
}

impl TestRecorder {
    fn recording() -> Self {
        Self {
            replay: None,
            operations: Mutex::new(Vec::new()),
        }
    }

    fn replaying(outcome: RecordedDispatchOutcome) -> Self {
        Self {
            replay: Some(outcome),
            operations: Mutex::new(Vec::new()),
        }
    }

    pub(super) fn operations(&self) -> Result<Vec<DispatchOperation>, AionSurfaceError> {
        Ok(lock(&self.operations)?.clone())
    }

    pub(super) fn kinds(&self) -> Result<Vec<DispatchOperationKind>, AionSurfaceError> {
        Ok(self
            .operations()?
            .into_iter()
            .map(|operation| operation.kind)
            .collect())
    }

    pub(super) fn states(&self) -> Result<Vec<ActivityDispatchState>, AionSurfaceError> {
        Ok(self
            .operations()?
            .into_iter()
            .filter_map(|operation| operation.activity_state)
            .collect())
    }
}

impl DispatchRecorder for TestRecorder {
    fn replay_outcome(
        &self,
        channel_name: &str,
        request: &ActivityRequest,
    ) -> Result<Option<RecordedDispatchOutcome>, AionSurfaceError> {
        let _ = (channel_name, request);
        Ok(self.replay.clone())
    }

    fn record(&self, operation: DispatchOperation) -> Result<(), AionSurfaceError> {
        lock(&self.operations)?.push(operation);
        Ok(())
    }
}

#[derive(Debug)]
struct FixedIds;

impl ConversationIdProvider for FixedIds {
    fn next_conversation_id(&self) -> String {
        "conversation-1".to_owned()
    }
}

pub(super) fn request(task_queue: &str) -> ActivityRequest {
    ActivityRequest {
        activity_type: "send-email".to_owned(),
        input: Payload {
            data: b"input".to_vec(),
            content_type: "application/json".to_owned(),
        },
        task_queue: task_queue.to_owned(),
        schedule_to_close_timeout: None,
        start_to_close_timeout: None,
    }
}

pub(super) fn worker(worker_id: &str, pid: u64) -> DispatchWorker {
    let consumer = ConsumerStateView::new(ConsumerId::new(worker_id), 0, 1, 0, Vec::new());
    DispatchWorker::with_consumer_state(worker_id, ParticipantPid::new(pid), consumer)
}

pub(super) fn response(worker_id: &str, result: ActivityResult) -> DispatchConversationEvent {
    DispatchConversationEvent::Response(DispatchResponse::new(worker_id.to_owned(), result))
}

pub(super) fn completed(data: &[u8]) -> ActivityResult {
    ActivityResult::Completed {
        output: Payload {
            data: data.to_vec(),
            content_type: "application/octet-stream".to_owned(),
        },
    }
}

pub(super) fn test_error(message: &str) -> AionSurfaceError {
    let channel_name =
        dispatch_channel("prod", "email").map_or_else(|error| error.to_string(), String::from);
    AionSurfaceError::DispatchFailed {
        channel_name,
        workflow_id: "wf-1".to_owned(),
        message: message.to_owned(),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>, AionSurfaceError> {
    mutex.lock().map_err(|error| test_error(&error.to_string()))
}
