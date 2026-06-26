use std::sync::{Arc, Mutex};

use beamr::process::ExitReason;
use beamr::scheduler::Scheduler;
use beamr::{Actor, ActorContext, spawn_actor};

use super::{ConsumerStateView, RoutingDecision, RoutingMessage};
use crate::routing::function::loader::{ContentHash, RoutingFunction};

const REQUEST_TOKEN: i64 = 1;
const REPLY_COMPLETED: i64 = 0;
const REPLY_CRASHED: i64 = 1;

/// Runs one routing-function invocation inside a beamr native actor process.
pub(super) struct BeamrInvocation {
    scheduler: Arc<Scheduler>,
    timeout: std::time::Duration,
}

impl BeamrInvocation {
    #[must_use]
    pub(super) const fn new(scheduler: Arc<Scheduler>, timeout: std::time::Duration) -> Self {
        Self { scheduler, timeout }
    }

    pub(super) fn execute(
        &self,
        function: RoutingFunction,
        message: RoutingMessage,
        consumers: Vec<ConsumerStateView>,
    ) -> Result<RoutingDecision, InvocationError> {
        let hash = function.content_hash();
        let state = InvocationState::default();
        let actor_state = RoutingInvocationState {
            function,
            message,
            consumers,
            outcome: state.clone(),
        };
        let actor = spawn_actor(&self.scheduler, move || {
            RoutingInvocationActor::new(actor_state.clone())
        })
        .map_err(|error| InvocationError::SpawnFailed(error.to_string()))?;

        match actor.sender.call_timeout(REQUEST_TOKEN, self.timeout) {
            Ok(REPLY_COMPLETED) => match state.take() {
                Some(InvocationOutcome::Completed(decision)) => Ok(decision),
                Some(InvocationOutcome::Crashed) | None => Err(InvocationError::Crashed),
            },
            Ok(REPLY_CRASHED) => Err(InvocationError::Crashed),
            Ok(_unknown) => Err(InvocationError::Crashed),
            Err(beamr::ActorError::Spawn) => Err(InvocationError::SpawnFailed(
                beamr::ActorError::Spawn.to_string(),
            )),
            Err(beamr::ActorError::Timeout) => {
                if self.is_live(actor.pid) {
                    self.scheduler
                        .terminate_process(actor.pid, ExitReason::Kill);
                    Err(InvocationError::TimedOut(hash))
                } else {
                    Err(InvocationError::Crashed)
                }
            }
        }
    }

    fn is_live(&self, pid: u64) -> bool {
        self.scheduler.process_table().get(pid).is_some()
    }
}

impl std::fmt::Debug for BeamrInvocation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BeamrInvocation")
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
struct RoutingInvocationState {
    function: RoutingFunction,
    message: RoutingMessage,
    consumers: Vec<ConsumerStateView>,
    outcome: InvocationState,
}

#[derive(Debug)]
struct RoutingInvocationActor {
    state: RoutingInvocationState,
}

impl RoutingInvocationActor {
    const fn new(state: RoutingInvocationState) -> Self {
        Self { state }
    }
}

impl Actor for RoutingInvocationActor {
    type Call = i64;
    type Reply = i64;
    type Cast = i64;

    fn handle_call(&mut self, request: Self::Call, ctx: &mut ActorContext<'_, '_>) -> Self::Reply {
        let _ = ctx.self_pid();
        if request != REQUEST_TOKEN {
            self.state.outcome.store(InvocationOutcome::Crashed);
            return REPLY_CRASHED;
        }

        let logic = self.state.function.logic();
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            logic(&self.state.message, &self.state.consumers)
        }));
        if let Ok(decision) = outcome {
            self.state
                .outcome
                .store(InvocationOutcome::Completed(decision));
            REPLY_COMPLETED
        } else {
            self.state.outcome.store(InvocationOutcome::Crashed);
            REPLY_CRASHED
        }
    }

    fn handle_cast(&mut self, request: Self::Cast, ctx: &mut ActorContext<'_, '_>) {
        let _ = request;
        let _ = ctx.self_pid();
    }
}

#[derive(Clone, Debug, Default)]
struct InvocationState {
    inner: Arc<Mutex<Option<InvocationOutcome>>>,
}

impl InvocationState {
    fn store(&self, outcome: InvocationOutcome) {
        let mut guard = lock_or_recover(&self.inner);
        *guard = Some(outcome);
    }

    fn take(&self) -> Option<InvocationOutcome> {
        let mut guard = lock_or_recover(&self.inner);
        guard.take()
    }
}

#[derive(Debug)]
enum InvocationOutcome {
    Completed(RoutingDecision),
    Crashed,
}

#[derive(Debug)]
pub(super) enum InvocationError {
    Crashed,
    TimedOut(ContentHash),
    SpawnFailed(String),
}

fn lock_or_recover<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
