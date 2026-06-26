//! Supervised, isolated execution of routing functions.
//!
//! Each invocation runs in its own supervised process, never on the calling
//! thread. A panic is contained and surfaced as [`FunctionError::Crashed`]; a
//! function running past the supervision timeout is abandoned and surfaced as
//! [`FunctionError::TimedOut`]. Neither outcome affects any other channel.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use beamr::scheduler::Scheduler;

use crate::routing::FieldValue;
use crate::routing::function::loader::{ContentHash, RoutingFunction};
mod actor;

/// Identifier of a consumer that a routing function may select.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConsumerId(String);

impl ConsumerId {
    /// Creates a consumer identifier from an owned or borrowed string.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the consumer identifier as a borrowed string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// Per-consumer state presented to a routing function during execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerStateView {
    /// Consumer the state describes.
    pub consumer: ConsumerId,
    /// Messages currently in flight to the consumer.
    pub current_in_flight: u32,
    /// Maximum messages the consumer will accept in flight.
    pub max_in_flight: u32,
    /// Depth of the consumer's pending buffer.
    pub buffer_depth: u32,
    /// Affinity tags advertised by the consumer.
    pub affinity_tags: Vec<String>,
}

impl ConsumerStateView {
    /// Creates a consumer state view.
    #[must_use]
    pub const fn new(
        consumer: ConsumerId,
        current_in_flight: u32,
        max_in_flight: u32,
        buffer_depth: u32,
        affinity_tags: Vec<String>,
    ) -> Self {
        Self {
            consumer,
            current_in_flight,
            max_in_flight,
            buffer_depth,
            affinity_tags,
        }
    }

    /// Returns the remaining in-flight capacity of the consumer.
    #[must_use]
    pub const fn available_capacity(&self) -> u32 {
        self.max_in_flight.saturating_sub(self.current_in_flight)
    }

    /// Returns true when the consumer can accept at least one more message.
    #[must_use]
    pub const fn has_capacity(&self) -> bool {
        self.available_capacity() > 0
    }

    /// Returns true when the consumer advertises `tag`.
    #[must_use]
    pub fn has_affinity(&self, tag: &str) -> bool {
        self.affinity_tags
            .iter()
            .any(|advertised| advertised == tag)
    }
}

/// Routing decision produced by a routing function.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RoutingDecision {
    selected: Option<ConsumerId>,
}

impl RoutingDecision {
    /// A decision that selects `consumer`.
    #[must_use]
    pub const fn select(consumer: ConsumerId) -> Self {
        Self {
            selected: Some(consumer),
        }
    }

    /// A decision that selects no consumer.
    #[must_use]
    pub const fn none() -> Self {
        Self { selected: None }
    }

    /// Returns the selected consumer, if any.
    #[must_use]
    pub const fn selected(&self) -> Option<&ConsumerId> {
        self.selected.as_ref()
    }

    /// Returns true when a consumer was selected.
    #[must_use]
    pub const fn is_selected(&self) -> bool {
        self.selected.is_some()
    }
}

/// Owned, supervisor-marshalled view of a message a routing function evaluates.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RoutingMessage {
    fields: BTreeMap<String, FieldValue>,
}

impl RoutingMessage {
    /// Creates an empty routing message.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds or replaces a field, returning the updated message.
    #[must_use]
    pub fn with(mut self, field: impl Into<String>, value: FieldValue) -> Self {
        self.fields.insert(field.into(), value);
        self
    }

    /// Returns the value of `field`, if present.
    #[must_use]
    pub fn get(&self, field: &str) -> Option<&FieldValue> {
        self.fields.get(field)
    }

    /// Iterates the message's fields in deterministic (key-sorted) order.
    pub fn fields(&self) -> impl Iterator<Item = (&str, &FieldValue)> {
        self.fields
            .iter()
            .map(|(name, value)| (name.as_str(), value))
    }
}

/// Failure surfaced when a supervised routing function does not complete.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum FunctionError {
    /// The routing function panicked; the supervisor contained the crash.
    #[error("routing function '{0}' panicked during execution")]
    Crashed(ContentHash),
    /// The routing function exceeded the supervision timeout and was abandoned.
    #[error("routing function '{0}' exceeded the supervision timeout")]
    TimedOut(ContentHash),
    /// The supervised execution process could not be started.
    #[error("routing function execution process could not be started: {0}")]
    SpawnFailed(String),
}

/// Executes routing functions in supervised, isolated processes.
///
/// Each invocation runs in its own supervised process. A panic is contained and
/// returned as [`FunctionError::Crashed`]; a function that runs past the
/// supervision timeout is abandoned and returns [`FunctionError::TimedOut`].
/// Neither outcome affects the execution of any other channel's functions.
#[derive(Clone)]
pub struct SupervisedExecutor {
    scheduler: Arc<Scheduler>,
    timeout: Duration,
}

impl SupervisedExecutor {
    /// Default supervision timeout applied by [`SupervisedExecutor::with_default_timeout`].
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

    /// Creates an executor with the given supervision timeout.
    #[must_use]
    pub const fn new(scheduler: Arc<Scheduler>, timeout: Duration) -> Self {
        Self { scheduler, timeout }
    }

    /// Creates an executor using [`SupervisedExecutor::DEFAULT_TIMEOUT`].
    #[must_use]
    pub const fn with_default_timeout(scheduler: Arc<Scheduler>) -> Self {
        Self::new(scheduler, Self::DEFAULT_TIMEOUT)
    }

    /// Returns the beamr scheduler backing supervised invocations.
    #[must_use]
    pub fn scheduler(&self) -> Arc<Scheduler> {
        Arc::clone(&self.scheduler)
    }

    /// Executes `function` against `message` and `consumers` under supervision.
    ///
    /// The function runs in a dedicated supervised process, never on the calling
    /// thread, and receives the message and per-consumer state views.
    ///
    /// # Errors
    ///
    /// Returns [`FunctionError::Crashed`] if the function panics,
    /// [`FunctionError::TimedOut`] if it exceeds the supervision timeout, and
    /// [`FunctionError::SpawnFailed`] if the supervised process cannot start.
    pub fn execute(
        &self,
        function: &RoutingFunction,
        message: RoutingMessage,
        consumers: Vec<ConsumerStateView>,
    ) -> Result<RoutingDecision, FunctionError> {
        let invocation = actor::BeamrInvocation::new(Arc::clone(&self.scheduler), self.timeout);
        let hash = function.content_hash();
        match invocation.execute(function.clone(), message, consumers) {
            Ok(decision) => Ok(decision),
            Err(actor::InvocationError::Crashed) => Err(FunctionError::Crashed(hash)),
            Err(actor::InvocationError::TimedOut(timed_out_hash)) => {
                Err(FunctionError::TimedOut(timed_out_hash))
            }
            Err(actor::InvocationError::SpawnFailed(message)) => {
                Err(FunctionError::SpawnFailed(message))
            }
        }
    }
}

impl std::fmt::Debug for SupervisedExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SupervisedExecutor")
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    use super::{
        ConsumerId, ConsumerStateView, FunctionError, RoutingDecision, RoutingMessage,
        SupervisedExecutor,
    };
    use crate::conversation::ConversationSupervisor;
    use crate::routing::FieldValue;
    use crate::routing::function::loader::{ModuleLoader, RoutingModule, RoutingSlot};

    fn consumer(id: &str, current: u32, max: u32, tags: &[&str]) -> ConsumerStateView {
        ConsumerStateView::new(
            ConsumerId::new(id),
            current,
            max,
            0,
            tags.iter().map(|tag| (*tag).to_owned()).collect(),
        )
    }

    fn select_first_with_capacity_module(bytecode: &[u8]) -> RoutingModule {
        RoutingModule::new(bytecode, |_message, consumers| {
            consumers
                .iter()
                .find(|state| state.has_capacity())
                .map_or_else(RoutingDecision::none, |state| {
                    RoutingDecision::select(state.consumer.clone())
                })
        })
    }

    fn selected_name(decision: &RoutingDecision) -> Option<&str> {
        decision.selected().map(ConsumerId::as_str)
    }

    fn supervised_executor() -> Result<(ConversationSupervisor, SupervisedExecutor), Box<dyn Error>>
    {
        let supervisor = ConversationSupervisor::new()?;
        let executor = SupervisedExecutor::with_default_timeout(supervisor.scheduler());
        Ok((supervisor, executor))
    }

    #[test]
    fn execution_returns_decision_using_consumer_state_view() -> Result<(), Box<dyn Error>> {
        let loader = ModuleLoader::new();
        let function = loader.load(select_first_with_capacity_module(b"v1"));
        let (_supervisor, executor) = supervised_executor()?;
        let consumers = vec![
            consumer("saturated", 5, 5, &["fast"]),
            consumer("ready", 1, 4, &["fast"]),
        ];

        let decision = executor.execute(&function, RoutingMessage::new(), consumers);

        assert!(matches!(decision, Ok(ref outcome) if selected_name(outcome) == Some("ready")));
        Ok(())
    }

    #[test]
    fn message_fields_are_visible_to_routing_function() -> Result<(), Box<dyn Error>> {
        let loader = ModuleLoader::new();
        let function = loader.load(RoutingModule::new(
            b"amount-router",
            |message, consumers| {
                let high_value = matches!(
                    message.get("amount"),
                    Some(FieldValue::Integer(amount)) if *amount > 1_000
                );
                if high_value {
                    consumers
                        .first()
                        .map_or_else(RoutingDecision::none, |state| {
                            RoutingDecision::select(state.consumer.clone())
                        })
                } else {
                    RoutingDecision::none()
                }
            },
        ));
        let (_supervisor, executor) = supervised_executor()?;
        let message = RoutingMessage::new().with("amount", FieldValue::Integer(5_000));

        let decision = executor.execute(&function, message, vec![consumer("priority", 0, 1, &[])]);

        assert!(matches!(decision, Ok(ref outcome) if selected_name(outcome) == Some("priority")));
        Ok(())
    }

    #[test]
    fn panic_in_function_is_contained_and_other_channels_proceed() -> Result<(), Box<dyn Error>> {
        let loader = ModuleLoader::new();
        let crashing = loader.load(RoutingModule::new(b"channel-a", |_message, _consumers| {
            std::panic::resume_unwind(Box::new(
                "intentional crash for fault-isolation test".to_owned(),
            ))
        }));
        let healthy = loader.load(select_first_with_capacity_module(b"channel-b"));
        let (_supervisor, executor) = supervised_executor()?;

        let crashed = executor.execute(&crashing, RoutingMessage::new(), Vec::new());
        assert_eq!(
            crashed,
            Err(FunctionError::Crashed(crashing.content_hash()))
        );

        let recovered = executor.execute(
            &healthy,
            RoutingMessage::new(),
            vec![consumer("ready", 0, 1, &[])],
        );
        assert!(matches!(recovered, Ok(ref outcome) if selected_name(outcome) == Some("ready")));
        Ok(())
    }

    #[test]
    fn repeated_panics_do_not_poison_the_shared_supervisor() -> Result<(), Box<dyn Error>> {
        // Real beamr supervision (not an ad-hoc panic catch on the calling
        // thread) must keep the shared scheduler healthy under a sustained
        // fault load: every crashing invocation is contained as `Crashed`, and
        // a healthy invocation spawned on the *same* scheduler immediately
        // after each crash still runs to a correct decision. If a panic escaped
        // the supervised actor it would unwind the scheduler worker and the
        // interleaved healthy invocations would hang or fail.
        let loader = ModuleLoader::new();
        let crashing = loader.load(RoutingModule::new(b"flaky", |_message, _consumers| {
            std::panic::resume_unwind(Box::new("repeated intentional crash".to_owned()))
        }));
        let healthy = loader.load(select_first_with_capacity_module(b"steady"));
        let (_supervisor, executor) = supervised_executor()?;

        for _ in 0..16 {
            let crashed = executor.execute(&crashing, RoutingMessage::new(), Vec::new());
            assert_eq!(
                crashed,
                Err(FunctionError::Crashed(crashing.content_hash()))
            );

            let served = executor.execute(
                &healthy,
                RoutingMessage::new(),
                vec![consumer("ready", 0, 1, &[])],
            );
            assert!(
                matches!(served, Ok(ref outcome) if selected_name(outcome) == Some("ready")),
                "scheduler must keep serving healthy invocations after a contained panic"
            );
        }
        Ok(())
    }

    #[test]
    fn function_exceeding_timeout_is_terminated_with_error() -> Result<(), Box<dyn Error>> {
        let loader = ModuleLoader::new();
        let slow = loader.load(RoutingModule::new(b"slow", |_message, _consumers| {
            thread::sleep(Duration::from_millis(200));
            RoutingDecision::none()
        }));
        let supervisor = ConversationSupervisor::new()?;
        let executor = SupervisedExecutor::new(supervisor.scheduler(), Duration::from_millis(20));

        let result = executor.execute(&slow, RoutingMessage::new(), Vec::new());

        assert_eq!(result, Err(FunctionError::TimedOut(slow.content_hash())));
        Ok(())
    }

    #[test]
    fn hot_deploy_does_not_interrupt_in_flight_and_swaps_next_version() -> Result<(), Box<dyn Error>>
    {
        let loader = ModuleLoader::new();
        let entered = Arc::new(AtomicBool::new(false));
        let release = Arc::new(AtomicBool::new(false));
        let entered_for_logic = Arc::clone(&entered);
        let release_for_logic = Arc::clone(&release);

        let old = loader.load(RoutingModule::new(b"v1", move |_message, _consumers| {
            entered_for_logic.store(true, Ordering::SeqCst);
            while !release_for_logic.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(1));
            }
            RoutingDecision::select(ConsumerId::new("old"))
        }));
        let new = loader.load(RoutingModule::new(b"v2", |_message, _consumers| {
            RoutingDecision::select(ConsumerId::new("new"))
        }));
        let old_hash = old.content_hash();
        let new_hash = new.content_hash();

        let slot = Arc::new(RoutingSlot::new(old));
        let (_supervisor, executor) = supervised_executor()?;
        let slot_for_thread = Arc::clone(&slot);
        let executor_for_thread = executor.clone();

        let in_flight = thread::spawn(move || {
            let function = slot_for_thread.current();
            executor_for_thread.execute(&function, RoutingMessage::new(), Vec::new())
        });

        while !entered.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(1));
        }

        slot.deploy(new);
        assert_eq!(slot.active_hash(), new_hash);
        assert!(
            loader.is_loaded(old_hash),
            "old module must remain loaded while in flight"
        );
        assert_eq!(loader.loaded_count(), 2);

        release.store(true, Ordering::SeqCst);

        let in_flight_result = in_flight.join();
        assert!(matches!(
            in_flight_result,
            Ok(Ok(ref outcome)) if selected_name(outcome) == Some("old")
        ));

        let next = executor.execute(&slot.current(), RoutingMessage::new(), Vec::new());
        assert!(matches!(next, Ok(ref outcome) if selected_name(outcome) == Some("new")));
        Ok(())
    }
}
