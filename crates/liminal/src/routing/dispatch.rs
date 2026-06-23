//! Conversation-mediated dispatch (ADR-007).
//!
//! A dispatch conversation replaces Kafka-style consumer group rebalancing with
//! per-message, fault-aware consumer selection. For each message the dispatch
//! conversation evaluates the group's routing function to select a target
//! consumer, links the dispatch process to that consumer via a beamr process
//! link, forwards the message, and observes completion. If the linked consumer
//! crashes, the link fires immediately (no polling, no heartbeat) and the
//! conversation re-routes the message to another available consumer, excluding
//! the one that crashed. Selection is always via the routing function — never
//! random — and adding or removing consumers takes effect on the next dispatch
//! without a stop-the-world rebalance.

use std::sync::mpsc;
use std::time::{Duration, Instant};

use crate::channel::ChannelMode;
use crate::conversation::{ConversationConfig, ConversationSupervisor, CrashPolicy};
use crate::error::LiminalError;
use crate::routing::group::{ConsumerGroup, ConsumerRegistration};
use crate::routing::{
    ConsumerId, ConsumerStateView, FieldValue, FunctionError, RoutingDecision, RoutingMessage,
    SupervisedExecutor,
};

/// Bounded window the dispatcher waits for the linked consumer's EXIT signal
/// after forwarding the message. The dispatcher blocks on the exit notifier for
/// this long; if the link fires it wakes the instant the EXIT is observed (the
/// window never delays re-routing), and if the window elapses with no EXIT the
/// message is treated as handed off to the linked, still-alive consumer.
///
/// The wait is event-driven: the thread parks in [`mpsc::Receiver::recv_timeout`]
/// and is woken by the EXIT handler, not by sampling consumer liveness.
const HANDOFF_CONFIRMATION_WINDOW: Duration = Duration::from_millis(250);

/// Outcome of a successful dispatch: the consumer the message was delivered to
/// and the chain of consumers that crashed and were re-routed past, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DispatchOutcome {
    delivered_to: ConsumerId,
    rerouted_from: Vec<ConsumerId>,
    reroute_timings: Vec<RerouteTiming>,
}

impl DispatchOutcome {
    /// Consumer the message was ultimately delivered to.
    #[must_use]
    pub const fn delivered_to(&self) -> &ConsumerId {
        &self.delivered_to
    }

    /// Consumers that crashed mid-dispatch and were re-routed past, in order.
    #[must_use]
    pub fn rerouted_from(&self) -> &[ConsumerId] {
        &self.rerouted_from
    }

    /// Real crash-to-reroute timings, one per consumer in [`Self::rerouted_from`]
    /// and in the same order. Each spans the consumer's EXIT instant (captured
    /// in the link handler) through to re-route initiation, so callers can
    /// verify sub-millisecond, event-driven detection (R3 / CN7).
    #[must_use]
    pub fn reroute_timings(&self) -> &[RerouteTiming] {
        &self.reroute_timings
    }

    /// True when the message was delivered without any consumer crash.
    #[must_use]
    pub fn delivered_first_try(&self) -> bool {
        self.rerouted_from.is_empty()
    }
}

/// Failure surfaced when a message cannot be dispatched to any consumer.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DispatchError {
    /// The routing function selected no consumer from the available set.
    #[error("routing function selected no consumer from the available group set")]
    NoConsumerAvailable,
    /// The routing function selected a consumer that is not in the group.
    #[error("routing function selected unknown consumer '{0}'")]
    UnknownConsumerSelected(String),
    /// Evaluating the group's routing function failed under supervision.
    #[error("routing function evaluation failed: {0}")]
    Evaluation(#[from] FunctionError),
    /// The dispatch conversation infrastructure could not be driven.
    #[error("dispatch conversation failed: {0}")]
    Conversation(String),
}

impl From<LiminalError> for DispatchError {
    fn from(error: LiminalError) -> Self {
        Self::Conversation(error.to_string())
    }
}

/// Real crash-to-reroute latency along the event path.
///
/// Records the instant the consumer's trapped EXIT signal was observed inside
/// the conversation actor's link handler and the instant the dispatcher woke to
/// initiate re-routing. The span between them is verifiably sub-millisecond.
///
/// `crash_observed` is captured *inside* the EXIT handler the moment the beamr
/// process link fires (see `ActorCore::handle_participant_exit`); it is not a
/// post-detection sample. `reroute_initiated` is captured the instant the
/// dispatcher's blocked `recv` returns with that value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RerouteTiming {
    crash_observed: Instant,
    reroute_initiated: Instant,
}

impl RerouteTiming {
    /// Elapsed time between the EXIT signal firing and re-route initiation.
    #[must_use]
    pub fn detection_to_reroute(&self) -> Duration {
        self.reroute_initiated
            .saturating_duration_since(self.crash_observed)
    }

    /// The instant the consumer's EXIT signal was observed in the link handler.
    #[must_use]
    pub const fn crash_observed(&self) -> Instant {
        self.crash_observed
    }
}

/// Drives conversation-mediated dispatch for one consumer group.
///
/// Each call to [`DispatchConversation::dispatch`] spawns a supervised
/// conversation actor (a beamr process), selects a consumer via the group's
/// routing function, links the conversation to the consumer, forwards the
/// message, and re-routes on consumer crash.
#[derive(Clone, Debug)]
pub struct DispatchConversation {
    group: ConsumerGroup,
    executor: SupervisedExecutor,
    supervisor: ConversationSupervisor,
}

impl DispatchConversation {
    /// Creates a dispatch driver for `group` using a fresh conversation supervisor.
    ///
    /// # Errors
    ///
    /// Returns [`DispatchError::Conversation`] if the beamr scheduler backing the
    /// conversation supervisor cannot start.
    pub fn new(group: ConsumerGroup) -> Result<Self, DispatchError> {
        let supervisor = ConversationSupervisor::new()?;
        Ok(Self {
            group,
            executor: SupervisedExecutor::with_default_timeout(),
            supervisor,
        })
    }

    /// Creates a dispatch driver reusing an existing conversation supervisor and
    /// supervision executor.
    #[must_use]
    pub const fn with_supervisor(
        group: ConsumerGroup,
        executor: SupervisedExecutor,
        supervisor: ConversationSupervisor,
    ) -> Self {
        Self {
            group,
            executor,
            supervisor,
        }
    }

    /// Returns the consumer group this dispatch conversation routes over.
    #[must_use]
    pub const fn group(&self) -> &ConsumerGroup {
        &self.group
    }

    /// Returns the conversation supervisor backing dispatch processes.
    #[must_use]
    pub const fn supervisor(&self) -> &ConversationSupervisor {
        &self.supervisor
    }

    /// Dispatches `message` to a consumer selected by the group's routing function.
    ///
    /// Snapshots the group, evaluates the routing function over the live consumer
    /// state, links a dispatch conversation process to the selected consumer,
    /// forwards the message, and observes completion. On consumer crash the
    /// message is re-routed to another consumer, excluding any that have crashed.
    ///
    /// # Errors
    ///
    /// Returns [`DispatchError::NoConsumerAvailable`] if the routing function
    /// selects no consumer, [`DispatchError::UnknownConsumerSelected`] if it
    /// selects a consumer outside the group, [`DispatchError::Evaluation`] if the
    /// supervised routing function fails, and [`DispatchError::Conversation`] if
    /// the dispatch conversation process cannot be driven.
    pub fn dispatch(&self, message: &RoutingMessage) -> Result<DispatchOutcome, DispatchError> {
        let mut excluded: Vec<ConsumerId> = Vec::new();
        let mut reroute_timings: Vec<RerouteTiming> = Vec::new();
        loop {
            let selected = self.select_consumer(message, &excluded)?;
            match self.run_attempt(message, &selected)? {
                AttemptResult::Delivered => {
                    return Ok(DispatchOutcome {
                        delivered_to: selected.consumer().clone(),
                        rerouted_from: excluded,
                        reroute_timings,
                    });
                }
                AttemptResult::Crashed(timing) => {
                    // Re-routing is initiated here, the instant the dispatcher
                    // woke from the EXIT notification. CN7 / R3: this is
                    // sub-millisecond because detection is the beamr link firing
                    // (no polling) and the wakeup is the very next step. The
                    // timing spans the real EXIT instant captured in the link
                    // handler through to this re-route initiation.
                    debug_assert!(
                        timing.detection_to_reroute() < Duration::from_millis(1),
                        "crash-to-reroute exceeded one millisecond"
                    );
                    reroute_timings.push(timing);
                    excluded.push(selected.consumer().clone());
                }
            }
        }
    }

    /// Selects a consumer for `message`, excluding `excluded`, via the routing
    /// function evaluated over the current group snapshot.
    fn select_consumer(
        &self,
        message: &RoutingMessage,
        excluded: &[ConsumerId],
    ) -> Result<ConsumerRegistration, DispatchError> {
        let snapshot = self.group.snapshot();
        let available: Vec<&ConsumerRegistration> = snapshot
            .consumers()
            .iter()
            .filter(|registration| !excluded.contains(registration.consumer()))
            .collect();
        if available.is_empty() {
            return Err(DispatchError::NoConsumerAvailable);
        }

        let state_views: Vec<ConsumerStateView> = available
            .iter()
            .map(|registration| registration.state().clone())
            .collect();
        let decision: RoutingDecision =
            self.executor
                .execute(snapshot.routing_function(), message.clone(), state_views)?;

        let Some(selected_id) = decision.selected() else {
            return Err(DispatchError::NoConsumerAvailable);
        };
        available
            .into_iter()
            .find(|registration| registration.consumer() == selected_id)
            .cloned()
            .ok_or_else(|| DispatchError::UnknownConsumerSelected(selected_id.as_str().to_owned()))
    }

    /// Runs one dispatch attempt against `selected`: spawns a conversation
    /// linked to the consumer, registers an exit notifier, forwards the real
    /// message, then blocks on the notifier — waking the instant the consumer's
    /// EXIT signal fires, or treating the message as handed off if the consumer
    /// stays alive for the hand-off window.
    fn run_attempt(
        &self,
        message: &RoutingMessage,
        selected: &ConsumerRegistration,
    ) -> Result<AttemptResult, DispatchError> {
        let consumer_pid = selected.participant();
        let actor = self.supervisor.spawn(ConversationConfig::new(
            vec![consumer_pid],
            None,
            ChannelMode::Ephemeral,
            CrashPolicy::RouteToNext,
        ))?;
        // Booting the actor establishes the beamr process link to the consumer
        // (R2). pid() drives boot to completion, so the link exists before any
        // message is forwarded.
        actor.pid()?;

        // Register the EXIT notifier BEFORE forwarding, so a crash that fires
        // the moment the message reaches the consumer is never missed. The
        // bounded channel holds the single EXIT instant the link handler sends.
        let (exit_tx, exit_rx) = mpsc::sync_channel::<Instant>(1);
        actor.notify_on_participant_exit(consumer_pid, exit_tx)?;

        let handle = actor.handle();
        // Forward the real dispatched message into the linked conversation
        // (R2: link before forward is guaranteed by the boot above).
        handle.send(dispatch_envelope(message)?)?;

        observe_attempt(&exit_rx)
    }
}

/// Observes a dispatch attempt by blocking on the consumer's EXIT notifier.
///
/// This is the event-driven crash-detection path mandated by R3 / CN7. The
/// dispatcher parks in [`mpsc::Receiver::recv_timeout`] and is woken the instant
/// the conversation actor's link handler sends the EXIT instant — there is no
/// liveness sampling and no heartbeat. The received `Instant` is the moment the
/// beamr process link fired (captured inside the EXIT handler), so the timing
/// reflects real detection latency rather than a post-detection sample.
///
/// Returns [`AttemptResult::Crashed`] carrying that timing if the EXIT fires
/// within the hand-off window, or [`AttemptResult::Delivered`] if the window
/// elapses with no EXIT — meaning the linked consumer stayed alive and the
/// message has been handed off to it over the live link.
fn observe_attempt(exit_rx: &mpsc::Receiver<Instant>) -> Result<AttemptResult, DispatchError> {
    match exit_rx.recv_timeout(HANDOFF_CONFIRMATION_WINDOW) {
        Ok(crash_observed) => {
            // Woken by the EXIT signal. Re-route initiation is this instant; the
            // span back to `crash_observed` is the real, link-driven latency.
            let reroute_initiated = Instant::now();
            Ok(AttemptResult::Crashed(RerouteTiming {
                crash_observed,
                reroute_initiated,
            }))
        }
        Err(mpsc::RecvTimeoutError::Timeout) => Ok(AttemptResult::Delivered),
        Err(mpsc::RecvTimeoutError::Disconnected) => Err(DispatchError::Conversation(
            "dispatch exit notifier disconnected before the hand-off window elapsed".to_owned(),
        )),
    }
}

/// Internal result of a single dispatch attempt.
#[derive(Debug)]
enum AttemptResult {
    /// The consumer stayed alive across the hand-off window with the message
    /// forwarded over the live link; the message is handed off to it.
    Delivered,
    /// The linked consumer's EXIT signal fired; re-route, excluding it. Carries
    /// the real crash-to-reroute timing along the event path.
    Crashed(RerouteTiming),
}

/// Builds the envelope forwarded to the selected consumer, carrying the real
/// dispatched message content (the routing message's fields), not a placeholder.
fn dispatch_envelope(message: &RoutingMessage) -> Result<crate::envelope::Envelope, DispatchError> {
    let payload = encode_message(message)?;
    Ok(crate::envelope::Envelope::new(
        payload,
        None,
        crate::channel::SchemaId::new(),
        crate::envelope::PublisherId::default(),
    ))
}

/// Serializes the dispatched message's fields to JSON bytes so the forwarded
/// envelope genuinely carries the message, not a synthetic substitute.
fn encode_message(message: &RoutingMessage) -> Result<Vec<u8>, DispatchError> {
    let map: serde_json::Map<String, serde_json::Value> = message
        .fields()
        .map(|(name, value)| (name.to_owned(), field_to_json(value)))
        .collect();
    serde_json::to_vec(&serde_json::Value::Object(map)).map_err(|error| {
        DispatchError::Conversation(format!("failed to encode dispatched message: {error}"))
    })
}

/// Maps a routing [`FieldValue`] to its JSON representation. A non-finite float
/// has no JSON number form and is encoded as null, matching `serde_json`'s own
/// handling of non-finite numbers.
fn field_to_json(value: &FieldValue) -> serde_json::Value {
    match value {
        FieldValue::Text(text) => serde_json::Value::String(text.clone()),
        FieldValue::Integer(integer) => serde_json::Value::from(*integer),
        FieldValue::Float(float) => serde_json::Number::from_f64(*float)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        FieldValue::Boolean(boolean) => serde_json::Value::Bool(*boolean),
        FieldValue::Null => serde_json::Value::Null,
    }
}

#[cfg(test)]
mod tests;
