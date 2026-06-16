use std::sync::Arc;

use super::super::channels::ChannelName;
use super::super::error::AionSurfaceError;
use super::super::types::ActivityRequest;
use super::{DispatchRouter, DispatchWorker, dispatch_failed};
use crate::routing::function::RoutingMessage;
use crate::routing::{FieldValue, RoutingSlot, SupervisedExecutor};

/// Router adapter backed by the active liminal routing function slot.
#[derive(Debug)]
pub struct RoutingFunctionDispatchRouter {
    slot: Arc<RoutingSlot>,
    executor: SupervisedExecutor,
}

impl RoutingFunctionDispatchRouter {
    /// Creates a router using the supplied routing slot and supervised executor.
    #[must_use]
    pub const fn new(slot: Arc<RoutingSlot>, executor: SupervisedExecutor) -> Self {
        Self { slot, executor }
    }
}

impl DispatchRouter for RoutingFunctionDispatchRouter {
    fn select_worker(
        &self,
        workflow_id: &str,
        channel_name: &ChannelName,
        request: &ActivityRequest,
        candidates: &[DispatchWorker],
        excluded_worker_ids: &[String],
    ) -> Result<Option<DispatchWorker>, AionSurfaceError> {
        let eligible = eligible_workers(candidates, excluded_worker_ids);
        let views = eligible
            .iter()
            .map(|worker| worker.consumer_state.clone())
            .collect();
        let decision = self
            .executor
            .execute(&self.slot.current(), routing_message(request), views)
            .map_err(|error| dispatch_failed(channel_name, workflow_id, error.to_string()))?;

        let Some(selected) = decision.selected() else {
            return Ok(None);
        };
        let selected_id = selected.as_str();
        eligible
            .into_iter()
            .find(|worker| worker.worker_id == selected_id)
            .map_or_else(
                || {
                    Err(dispatch_failed(
                        channel_name,
                        workflow_id,
                        format!("routing selected unknown worker '{selected_id}'"),
                    ))
                },
                |worker| Ok(Some(worker)),
            )
    }
}

fn eligible_workers(
    candidates: &[DispatchWorker],
    excluded_worker_ids: &[String],
) -> Vec<DispatchWorker> {
    candidates
        .iter()
        .filter(|worker| !excluded_worker_ids.iter().any(|id| id == &worker.worker_id))
        .cloned()
        .collect()
}

fn routing_message(request: &ActivityRequest) -> RoutingMessage {
    RoutingMessage::new()
        .with(
            "activity_type",
            FieldValue::Text(request.activity_type.clone()),
        )
        .with("task_queue", FieldValue::Text(request.task_queue.clone()))
        .with(
            "input_content_type",
            FieldValue::Text(request.input.content_type.clone()),
        )
}
