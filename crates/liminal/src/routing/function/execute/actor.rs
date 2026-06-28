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

/// WR-9b: the REAL routing [`RoutingInvocationActor`] running on beamr's
/// cooperative (single-threaded / wasm) [`beamr::scheduler::WasmScheduler`],
/// driven through the non-blocking [`beamr::CoopSenderHandle::call_async`] surface.
///
/// The threaded [`BeamrInvocation::execute`] spawns this same `Actor` via
/// `spawn_actor` and drives it with the BLOCKING `call_timeout` — illegal on the
/// wasm main thread. This smoke spawns the GENUINE production
/// [`RoutingInvocationActor`] (with a real [`RoutingFunction`] loaded through the
/// production [`crate::routing::function::loader::ModuleLoader`]) on the
/// cooperative scheduler via `spawn_actor_cooperative`, issues the request with
/// `call_async`, pumps cooperative `run_until_idle` turns while polling the
/// returned [`beamr::CallFuture`] with a no-op waker (the same pattern beamr's own
/// WR-6 tests use), and asserts the routing reply comes back AND the decision
/// the function produced lands in the shared [`InvocationState`].
///
/// The actor runs cooperatively AS-IS: `handle_call` only touches `ctx.self_pid()`,
/// `catch_unwind`, and the pure routing closure — no threads, tokio, sockets, or
/// `SharedState`. The only thing the wasm path changes is the DRIVER: a
/// non-blocking `call_async` + host turn pump replaces the blocking `call_timeout`.
#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod cooperative_smoke {
    use std::cell::RefCell;
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    use beamr::atom::AtomTable;
    use beamr::module::ModuleRegistry;
    use beamr::native::BifRegistryImpl;
    use beamr::scheduler::WasmScheduler;
    use beamr::{ActorError, CallFuture, spawn_actor_cooperative};

    use super::{
        InvocationOutcome, InvocationState, REPLY_COMPLETED, REQUEST_TOKEN, RoutingInvocationActor,
        RoutingInvocationState,
    };
    use crate::routing::function::execute::{
        ConsumerId, ConsumerStateView, RoutingDecision, RoutingMessage,
    };
    use crate::routing::function::loader::{ModuleLoader, RoutingModule};

    /// Build a cooperative scheduler the way a wasm host holds it.
    fn cooperative_scheduler() -> Rc<RefCell<WasmScheduler>> {
        let atom_table = Arc::new(AtomTable::with_common_atoms());
        let modules = Arc::new(ModuleRegistry::new());
        let bifs = Arc::new(BifRegistryImpl::new());
        Rc::new(RefCell::new(WasmScheduler::new(atom_table, modules, bifs)))
    }

    /// A no-op waker built through the safe [`Wake`] trait (liminal forbids
    /// `unsafe`, so the raw-vtable construction beamr's own tests use is not
    /// available here). The host pump — not a waker thread — advances the future,
    /// so the wake is genuinely a no-op; polling needs only a valid `Context`.
    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
        fn wake_by_ref(self: &Arc<Self>) {}
    }

    fn noop_waker() -> Waker {
        Waker::from(Arc::new(NoopWake))
    }

    fn poll_once(future: &mut Pin<Box<CallFuture<i64>>>) -> Poll<Result<i64, ActorError>> {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        future.as_mut().poll(&mut cx)
    }

    fn select_first_with_capacity() -> RoutingModule {
        RoutingModule::new(b"coop-router", |_message, consumers| {
            consumers
                .iter()
                .find(|state| state.has_capacity())
                .map_or_else(RoutingDecision::none, |state| {
                    RoutingDecision::select(state.consumer.clone())
                })
        })
    }

    fn consumer(id: &str, current: u32, max: u32) -> ConsumerStateView {
        ConsumerStateView::new(ConsumerId::new(id), current, max, 0, Vec::new())
    }

    #[test]
    fn real_routing_actor_replies_to_a_cooperative_call_async() {
        let scheduler = cooperative_scheduler();

        // Load the GENUINE production routing function through the real loader.
        let loader = ModuleLoader::new();
        let function = loader.load(select_first_with_capacity());
        let consumers = vec![consumer("saturated", 5, 5), consumer("ready", 1, 4)];

        // The shared outcome slot the actor writes its decision into — the same
        // record the threaded `BeamrInvocation::execute` reads back.
        let outcome = InvocationState::default();
        let actor_state = RoutingInvocationState {
            function,
            message: RoutingMessage::new(),
            consumers,
            outcome: outcome.clone(),
        };

        // Spawn the GENUINE production routing actor cooperatively.
        let actor = spawn_actor_cooperative::<RoutingInvocationActor, _>(&scheduler, move || {
            RoutingInvocationActor::new(actor_state.clone())
        });

        // Issue the request without blocking; nothing has run yet.
        let mut future = Box::pin(actor.sender.call_async(REQUEST_TOKEN));
        assert!(
            matches!(poll_once(&mut future), Poll::Pending),
            "the call future is pending before any turn runs"
        );

        // Pump host turns: the client sends the request, the actor runs the
        // routing function, replies with REPLY_COMPLETED, and the client resolves
        // the future from the ref-matched reply.
        let mut reply = None;
        for _ in 0..16 {
            scheduler.borrow_mut().run_until_idle();
            if let Poll::Ready(result) = poll_once(&mut future) {
                reply = Some(result);
                break;
            }
        }

        assert_eq!(
            reply,
            Some(Ok(REPLY_COMPLETED)),
            "the routing actor replied REPLY_COMPLETED over the cooperative call"
        );

        // The actor's side effect — the routing decision — is observable in the
        // shared slot, proving the real routing logic ran inside the actor.
        match outcome.take() {
            Some(InvocationOutcome::Completed(decision)) => {
                assert_eq!(
                    decision.selected().map(ConsumerId::as_str),
                    Some("ready"),
                    "the routing function selected the consumer with capacity"
                );
            }
            other => panic!("expected a completed routing decision, got {other:?}"),
        }
    }
}
