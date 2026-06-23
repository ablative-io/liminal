//! Supervised, isolated execution of routing functions.
//!
//! Each invocation runs in its own supervised process, never on the calling
//! thread. A panic is contained and surfaced as [`FunctionError::Crashed`]; a
//! function running past the supervision timeout is abandoned and surfaced as
//! [`FunctionError::TimedOut`]. Neither outcome affects any other channel.

use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::mpsc::{RecvTimeoutError, sync_channel};
use std::thread;
use std::time::Duration;

use crate::routing::FieldValue;
use crate::routing::function::loader::{ContentHash, RoutingFunction};

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
#[derive(Clone, Debug)]
pub struct SupervisedExecutor {
    timeout: Duration,
}

impl SupervisedExecutor {
    /// Default supervision timeout applied by [`SupervisedExecutor::with_default_timeout`].
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

    /// Creates an executor with the given supervision timeout.
    #[must_use]
    pub const fn new(timeout: Duration) -> Self {
        Self { timeout }
    }

    /// Creates an executor using [`SupervisedExecutor::DEFAULT_TIMEOUT`].
    #[must_use]
    pub const fn with_default_timeout() -> Self {
        Self::new(Self::DEFAULT_TIMEOUT)
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
        let logic = function.logic();
        let hash = function.content_hash();
        let (sender, receiver) = sync_channel(1);

        let spawned = thread::Builder::new()
            .name(format!("routing-fn-{hash}"))
            .spawn(move || {
                let outcome = catch_unwind(AssertUnwindSafe(|| logic(&message, &consumers)));
                let _ = sender.send(outcome);
            });

        if let Err(error) = spawned {
            return Err(FunctionError::SpawnFailed(error.to_string()));
        }

        match receiver.recv_timeout(self.timeout) {
            Ok(Ok(decision)) => Ok(decision),
            Ok(Err(_panic)) => Err(FunctionError::Crashed(hash)),
            Err(RecvTimeoutError::Timeout) => Err(FunctionError::TimedOut(hash)),
            Err(RecvTimeoutError::Disconnected) => Err(FunctionError::Crashed(hash)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    use super::{
        ConsumerId, ConsumerStateView, FunctionError, RoutingDecision, RoutingMessage,
        SupervisedExecutor,
    };
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

    #[test]
    fn execution_returns_decision_using_consumer_state_view() {
        let loader = ModuleLoader::new();
        let function = loader.load(select_first_with_capacity_module(b"v1"));
        let executor = SupervisedExecutor::with_default_timeout();
        let consumers = vec![
            consumer("saturated", 5, 5, &["fast"]),
            consumer("ready", 1, 4, &["fast"]),
        ];

        let decision = executor.execute(&function, RoutingMessage::new(), consumers);

        assert!(matches!(decision, Ok(ref outcome) if selected_name(outcome) == Some("ready")));
    }

    #[test]
    fn message_fields_are_visible_to_routing_function() {
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
        let executor = SupervisedExecutor::with_default_timeout();
        let message = RoutingMessage::new().with("amount", FieldValue::Integer(5_000));

        let decision = executor.execute(&function, message, vec![consumer("priority", 0, 1, &[])]);

        assert!(matches!(decision, Ok(ref outcome) if selected_name(outcome) == Some("priority")));
    }

    #[test]
    #[allow(clippy::panic)]
    fn panic_in_function_is_contained_and_other_channels_proceed() {
        let loader = ModuleLoader::new();
        let crashing = loader.load(RoutingModule::new(b"channel-a", |_message, _consumers| {
            panic!("intentional crash for fault-isolation test")
        }));
        let healthy = loader.load(select_first_with_capacity_module(b"channel-b"));
        let executor = SupervisedExecutor::with_default_timeout();

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
    }

    #[test]
    fn function_exceeding_timeout_is_terminated_with_error() {
        let loader = ModuleLoader::new();
        let slow = loader.load(RoutingModule::new(b"slow", |_message, _consumers| {
            thread::sleep(Duration::from_secs(30));
            RoutingDecision::none()
        }));
        let executor = SupervisedExecutor::new(Duration::from_millis(20));

        let result = executor.execute(&slow, RoutingMessage::new(), Vec::new());

        assert_eq!(result, Err(FunctionError::TimedOut(slow.content_hash())));
    }

    #[test]
    fn hot_deploy_does_not_interrupt_in_flight_and_swaps_next_version() {
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
        let executor = SupervisedExecutor::with_default_timeout();
        let slot_for_thread = Arc::clone(&slot);

        let in_flight = thread::spawn(move || {
            let function = slot_for_thread.current();
            executor.execute(&function, RoutingMessage::new(), Vec::new())
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

        let next = SupervisedExecutor::with_default_timeout().execute(
            &slot.current(),
            RoutingMessage::new(),
            Vec::new(),
        );
        assert!(matches!(next, Ok(ref outcome) if selected_name(outcome) == Some("new")));
    }
}
