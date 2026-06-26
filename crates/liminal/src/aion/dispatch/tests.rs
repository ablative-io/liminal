use std::error::Error;
use std::sync::Arc;

use super::*;
use crate::conversation::ConversationSupervisor;
use crate::routing::{
    FieldValue, ModuleLoader, RoutingDecision, RoutingModule, RoutingSlot, SupervisedExecutor,
};

mod support;

use support::{TestSetup, completed, request, response, test_error, worker};

#[test]
fn successful_dispatch_records_conversation_lifecycle() -> Result<(), Box<dyn Error>> {
    let setup = TestSetup::new(
        vec![worker("worker-a", 11), worker("worker-b", 12)],
        vec![response("worker-b", completed(b"ok"))],
    );
    setup.router.push_decision(Some("worker-b"))?;

    let result =
        dispatch_activity_with_context(&setup.context(), "prod", "email", request("email"))?;

    assert_eq!(result, completed(b"ok"));
    let expected_channel = String::from(dispatch_channel("prod", "email")?);
    assert_eq!(setup.factory.opens()?, vec![expected_channel]);
    assert_eq!(setup.conversation.links()?, vec!["worker-b"]);
    assert_eq!(
        setup.conversation.sent_requests()?[0].request,
        request("email")
    );
    assert_eq!(setup.router.calls()?.len(), 1);
    assert_eq!(
        setup.recorder.kinds()?,
        vec![
            DispatchOperationKind::ConversationOpened,
            DispatchOperationKind::WorkerSelected,
            DispatchOperationKind::MessageSent,
            DispatchOperationKind::MessageReceived,
            DispatchOperationKind::ConversationClosed,
        ]
    );
    assert_eq!(
        setup.recorder.states()?,
        vec![
            ActivityDispatchState::ActivityScheduled,
            ActivityDispatchState::ActivityStarted,
            ActivityDispatchState::ActivityCompleted,
        ]
    );
    Ok(())
}

#[test]
fn failed_activity_maps_to_dispatch_failed() -> Result<(), Box<dyn Error>> {
    let setup = TestSetup::new(
        vec![worker("worker-a", 11)],
        vec![response(
            "worker-a",
            ActivityResult::Failed {
                error: test_error("activity exploded"),
            },
        )],
    );

    let result =
        dispatch_activity_with_context(&setup.context(), "prod", "email", request("email"));

    assert!(matches!(
        result,
        Err(AionSurfaceError::DispatchFailed { .. })
    ));
    let error = result
        .err()
        .map_or_else(String::new, |error| error.to_string());
    assert!(error.contains("activity exploded"));
    assert_eq!(
        setup.recorder.states()?,
        vec![
            ActivityDispatchState::ActivityScheduled,
            ActivityDispatchState::ActivityStarted,
            ActivityDispatchState::ActivityFailed {
                retry_eligible: false,
            },
        ]
    );
    assert_eq!(setup.conversation.closes()?, 1);
    Ok(())
}

#[test]
fn linked_worker_exit_reselects_and_resends_same_request() -> Result<(), Box<dyn Error>> {
    let setup = TestSetup::new(
        vec![worker("worker-a", 11), worker("worker-b", 12)],
        vec![
            DispatchConversationEvent::WorkerExited {
                worker_id: "worker-a".to_owned(),
                message: "linked process exited".to_owned(),
            },
            response("worker-b", completed(b"ok")),
        ],
    );

    let result =
        dispatch_activity_with_context(&setup.context(), "prod", "email", request("email"))?;

    assert_eq!(result, completed(b"ok"));
    assert_eq!(setup.conversation.links()?, vec!["worker-a", "worker-b"]);
    let sent = setup.conversation.sent_requests()?;
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0].request, sent[1].request);
    let calls = setup.router.calls()?;
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1].excluded, vec!["worker-a"]);
    assert!(setup.recorder.operations()?.iter().any(|operation| {
        operation.kind == DispatchOperationKind::WorkerExited
            && operation.activity_state
                == Some(ActivityDispatchState::ActivityFailed {
                    retry_eligible: true,
                })
    }));
    Ok(())
}

#[test]
fn no_workers_after_exit_returns_dispatch_failed() -> Result<(), Box<dyn Error>> {
    let setup = TestSetup::new(
        vec![worker("worker-a", 11)],
        vec![DispatchConversationEvent::WorkerExited {
            worker_id: "worker-a".to_owned(),
            message: "linked process exited".to_owned(),
        }],
    );

    let result =
        dispatch_activity_with_context(&setup.context(), "prod", "email", request("email"));

    assert!(matches!(
        result,
        Err(AionSurfaceError::DispatchFailed { .. })
    ));
    let error = result
        .err()
        .map_or_else(String::new, |error| error.to_string());
    assert!(error.contains("NoWorkersAvailable"));
    assert_eq!(setup.router.calls()?.len(), 2);
    assert_eq!(setup.conversation.closes()?, 1);
    Ok(())
}

#[test]
fn replay_returns_recorded_outcome_without_live_conversation() -> Result<(), Box<dyn Error>> {
    let setup = TestSetup::replaying(RecordedDispatchOutcome::new(Ok(completed(b"replayed"))));

    let result =
        dispatch_activity_with_context(&setup.context(), "prod", "email", request("email"))?;

    assert_eq!(result, completed(b"replayed"));
    assert_no_live_dispatch(&setup)?;
    Ok(())
}

#[test]
fn replay_returns_recorded_failure_without_re_dispatch() -> Result<(), Box<dyn Error>> {
    let setup = TestSetup::replaying(RecordedDispatchOutcome::new(Err(test_error(
        "NoWorkersAvailable: recorded crash re-dispatch exhausted workers",
    ))));

    let result =
        dispatch_activity_with_context(&setup.context(), "prod", "email", request("email"));

    assert!(matches!(
        result,
        Err(AionSurfaceError::DispatchFailed { .. })
    ));
    let error = result
        .err()
        .map_or_else(String::new, |error| error.to_string());
    assert!(error.contains("NoWorkersAvailable"));
    assert_no_live_dispatch(&setup)?;
    Ok(())
}

fn assert_no_live_dispatch(setup: &TestSetup) -> Result<(), Box<dyn Error>> {
    assert_eq!(setup.factory.opens()?.len(), 0);
    assert_eq!(setup.router.calls()?.len(), 0);
    assert_eq!(setup.pool.calls()?, 0);
    assert_eq!(setup.recorder.operations()?.len(), 0);
    Ok(())
}

#[test]
fn routing_function_adapter_selects_from_current_candidates() -> Result<(), Box<dyn Error>> {
    let loader = ModuleLoader::new();
    let function = loader.load(RoutingModule::new(
        b"select-email-worker",
        |message, consumers| {
            if message.get("activity_type") == Some(&FieldValue::Text("send-email".to_owned())) {
                consumers
                    .get(1)
                    .map_or_else(RoutingDecision::none, |state| {
                        RoutingDecision::select(state.consumer.clone())
                    })
            } else {
                RoutingDecision::none()
            }
        },
    ));
    let supervisor = ConversationSupervisor::new()?;
    let router = RoutingFunctionDispatchRouter::new(
        Arc::new(RoutingSlot::new(function)),
        SupervisedExecutor::with_default_timeout(supervisor.scheduler()),
    );
    let workers = vec![worker("worker-a", 11), worker("worker-b", 12)];
    let channel_name = dispatch_channel("prod", "email")?;

    let selected = router.select_worker("wf-1", &channel_name, &request("email"), &workers, &[])?;

    assert_eq!(
        selected.map(|worker| worker.worker_id),
        Some("worker-b".to_owned())
    );
    Ok(())
}

#[test]
fn routing_is_called_per_dispatch_not_membership_change() -> Result<(), Box<dyn Error>> {
    let setup = TestSetup::new(
        vec![worker("worker-a", 11)],
        vec![
            response("worker-a", completed(b"one")),
            response("worker-b", completed(b"two")),
        ],
    );
    let first =
        dispatch_activity_with_context(&setup.context(), "prod", "email", request("email"))?;
    setup
        .pool
        .set_workers(vec![worker("worker-a", 11), worker("worker-b", 12)])?;

    let second =
        dispatch_activity_with_context(&setup.context(), "prod", "email", request("email"))?;

    assert_eq!(first, completed(b"one"));
    assert_eq!(second, completed(b"two"));
    assert_eq!(setup.router.calls()?.len(), 2);
    assert_eq!(
        setup.router.calls()?[1].candidates,
        vec!["worker-a", "worker-b"]
    );
    Ok(())
}
