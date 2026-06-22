#![allow(clippy::module_name_repetitions)]

use std::{collections::BTreeMap, time::SystemTime};

use crate::pressure::{
    ChannelPolicyConfig, ChannelPressureSnapshot, ConsumerPressureMetrics, PolicyAction,
    PressureMonitor,
};

/// Scaling signal emitted for external orchestrators when pressure requires more consumers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScaleSignal {
    /// Channel whose pressure triggered the scale signal.
    pub channel_id: String,
    /// Number of consumers currently tracked for the channel.
    pub current_consumer_count: usize,
}

/// Typed pressure policy event emitted by the enforcer.
#[derive(Clone, Debug, PartialEq)]
pub enum PolicyEvent {
    /// A policy action became active for a channel pressure update.
    Action {
        /// Channel whose policy action triggered.
        channel_id: String,
        /// Action selected by the channel policy configuration.
        action: PolicyAction,
        /// Wall-clock time when the enforcer emitted this event.
        triggered_at: SystemTime,
    },
    /// A scale-consumer policy emitted a scaling signal for external orchestration.
    ScaleConsumer {
        /// Signal payload for the external orchestrator.
        signal: ScaleSignal,
        /// Wall-clock time when the enforcer emitted this event.
        triggered_at: SystemTime,
    },
}

/// Result returned from a monitor update after policy enforcement runs.
#[derive(Clone, Debug, PartialEq)]
pub struct EnforcementOutcome {
    /// Snapshot of the pressure state after the monitor update.
    pub snapshot: ChannelPressureSnapshot,
    /// Triggered policy actions for the caller to apply synchronously.
    pub actions: Vec<PolicyAction>,
    /// Typed events consumable by routing, dispatch, or observability subsystems.
    pub events: Vec<PolicyEvent>,
}

/// Synchronously updates pressure metrics and enforces channel policies.
#[derive(Debug, Default)]
pub struct PressureEnforcer {
    monitor: PressureMonitor,
    policies: BTreeMap<String, ChannelPolicyConfig>,
    last_events: Vec<PolicyEvent>,
    last_snapshot: Option<ChannelPressureSnapshot>,
}

impl PressureEnforcer {
    /// Creates an enforcer with an empty monitor and no configured channel policies.
    #[must_use]
    pub fn new() -> Self {
        Self {
            monitor: PressureMonitor::new(),
            policies: BTreeMap::new(),
            last_events: Vec::new(),
            last_snapshot: None,
        }
    }

    /// Creates an enforcer around an existing monitor.
    #[must_use]
    pub const fn with_monitor(monitor: PressureMonitor) -> Self {
        Self {
            monitor,
            policies: BTreeMap::new(),
            last_events: Vec::new(),
            last_snapshot: None,
        }
    }

    /// Returns the monitor used by the enforcer.
    #[must_use]
    pub const fn monitor(&self) -> &PressureMonitor {
        &self.monitor
    }

    /// Returns the latest typed policy events emitted by an automatic update.
    #[must_use]
    pub fn last_events(&self) -> &[PolicyEvent] {
        &self.last_events
    }

    /// Returns the latest pressure snapshot observed by an automatic update.
    #[must_use]
    pub const fn last_snapshot(&self) -> Option<&ChannelPressureSnapshot> {
        self.last_snapshot.as_ref()
    }

    /// Registers or replaces the pressure policy configuration for a channel.
    pub fn set_channel_policy(
        &mut self,
        channel_id: impl Into<String>,
        config: ChannelPolicyConfig,
    ) {
        self.policies.insert(channel_id.into(), config);
    }

    /// Returns the pressure policy configuration for a channel, if one is registered.
    #[must_use]
    pub fn channel_policy(&self, channel_id: &str) -> Option<&ChannelPolicyConfig> {
        self.policies.get(channel_id)
    }

    /// Records consumer metrics and immediately evaluates the channel policy.
    pub fn record_consumer_metrics(
        &mut self,
        channel_id: impl Into<String>,
        consumer_id: impl Into<String>,
        metrics: ConsumerPressureMetrics,
    ) -> Vec<PolicyAction> {
        self.record_consumer_metrics_outcome(channel_id, consumer_id, metrics)
            .actions
    }

    /// Records consumer metrics and returns the full enforcement outcome.
    ///
    /// The primary update path returns the action vector directly; this helper
    /// exposes the same automatic enforcement result with its snapshot and
    /// typed events for observers that need more context.
    pub fn record_consumer_metrics_outcome(
        &mut self,
        channel_id: impl Into<String>,
        consumer_id: impl Into<String>,
        metrics: ConsumerPressureMetrics,
    ) -> EnforcementOutcome {
        let channel_id = channel_id.into();
        let snapshot =
            self.monitor
                .record_consumer_metrics(channel_id.clone(), consumer_id, metrics);
        self.enforce_snapshot(&channel_id, snapshot)
    }

    /// Records an accept decision and immediately evaluates the channel policy.
    pub fn record_accept(
        &mut self,
        channel_id: impl Into<String>,
        consumer_id: impl Into<String>,
    ) -> Vec<PolicyAction> {
        self.record_accept_outcome(channel_id, consumer_id).actions
    }

    /// Records an accept decision and returns the full enforcement outcome.
    ///
    /// Enforcement still runs synchronously as part of this monitor update.
    pub fn record_accept_outcome(
        &mut self,
        channel_id: impl Into<String>,
        consumer_id: impl Into<String>,
    ) -> EnforcementOutcome {
        let channel_id = channel_id.into();
        let snapshot = self.monitor.record_accept(channel_id.clone(), consumer_id);
        self.enforce_snapshot(&channel_id, snapshot)
    }

    /// Records a defer decision and immediately evaluates the channel policy.
    pub fn record_defer(
        &mut self,
        channel_id: impl Into<String>,
        consumer_id: impl Into<String>,
    ) -> Vec<PolicyAction> {
        self.record_defer_outcome(channel_id, consumer_id).actions
    }

    /// Records a defer decision and returns the full enforcement outcome.
    ///
    /// Enforcement still runs synchronously as part of this monitor update.
    pub fn record_defer_outcome(
        &mut self,
        channel_id: impl Into<String>,
        consumer_id: impl Into<String>,
    ) -> EnforcementOutcome {
        let channel_id = channel_id.into();
        let snapshot = self.monitor.record_defer(channel_id.clone(), consumer_id);
        self.enforce_snapshot(&channel_id, snapshot)
    }

    /// Records a reject decision and immediately evaluates the channel policy.
    pub fn record_reject(
        &mut self,
        channel_id: impl Into<String>,
        consumer_id: impl Into<String>,
    ) -> Vec<PolicyAction> {
        self.record_reject_outcome(channel_id, consumer_id).actions
    }

    /// Records a reject decision and returns the full enforcement outcome.
    ///
    /// Enforcement still runs synchronously as part of this monitor update.
    pub fn record_reject_outcome(
        &mut self,
        channel_id: impl Into<String>,
        consumer_id: impl Into<String>,
    ) -> EnforcementOutcome {
        let channel_id = channel_id.into();
        let snapshot = self.monitor.record_reject(channel_id.clone(), consumer_id);
        self.enforce_snapshot(&channel_id, snapshot)
    }

    fn enforce_snapshot(
        &mut self,
        channel_id: &str,
        snapshot: ChannelPressureSnapshot,
    ) -> EnforcementOutcome {
        let actions = self
            .policies
            .get(channel_id)
            .map_or_else(Vec::new, |config| {
                config.actions_for_pressure(snapshot.pressure_score)
            });
        let triggered_at = SystemTime::now();
        let events = Self::events_for_actions(
            channel_id,
            snapshot.consumer_count(),
            &actions,
            triggered_at,
        );
        self.last_events.clone_from(&events);
        self.last_snapshot = Some(snapshot.clone());
        EnforcementOutcome {
            snapshot,
            actions,
            events,
        }
    }

    fn events_for_actions(
        channel_id: &str,
        current_consumer_count: usize,
        actions: &[PolicyAction],
        triggered_at: SystemTime,
    ) -> Vec<PolicyEvent> {
        let mut events = Vec::with_capacity(actions.len());
        for action in actions {
            events.push(PolicyEvent::Action {
                channel_id: channel_id.to_owned(),
                action: action.clone(),
                triggered_at,
            });
            if matches!(action, PolicyAction::ScaleConsumer) {
                events.push(PolicyEvent::ScaleConsumer {
                    signal: ScaleSignal {
                        channel_id: channel_id.to_owned(),
                        current_consumer_count,
                    },
                    triggered_at,
                });
            }
        }
        events
    }
}

#[cfg(test)]
mod tests {
    use super::{PolicyEvent, PressureEnforcer, ScaleSignal};
    use crate::pressure::{
        ChannelPolicyConfig, ConsumerPressureMetrics, PolicyAction, PressureMonitor, PressurePolicy,
    };

    fn slow_policy(threshold: f64) -> PressurePolicy {
        PressurePolicy {
            threshold,
            action: PolicyAction::SlowProducer {
                reduction_factor: 0.5,
            },
        }
    }

    #[test]
    fn policy_enforcement_emits_slow_producer_when_threshold_is_crossed() {
        let mut enforcer = PressureEnforcer::new();
        enforcer.set_channel_policy("orders", ChannelPolicyConfig::new(vec![slow_policy(0.7)]));

        let actions = enforcer.record_consumer_metrics(
            "orders",
            "consumer-a",
            ConsumerPressureMetrics::new(7, 10, 0),
        );

        assert_eq!(
            actions,
            vec![PolicyAction::SlowProducer {
                reduction_factor: 0.5
            }]
        );
        assert!(matches!(
            enforcer.last_events(),
            [PolicyEvent::Action {
                channel_id,
                action: PolicyAction::SlowProducer { reduction_factor },
                triggered_at: _
            }] if channel_id == "orders" && (*reduction_factor - 0.5).abs() < f64::EPSILON
        ));
    }

    #[test]
    fn enforcement_runs_as_part_of_each_monitor_update() {
        let mut enforcer = PressureEnforcer::new();
        enforcer.set_channel_policy("orders", ChannelPolicyConfig::new(vec![slow_policy(0.7)]));

        let below = enforcer.record_consumer_metrics(
            "orders",
            "consumer-a",
            ConsumerPressureMetrics::new(6, 10, 0),
        );
        let above = enforcer.record_consumer_metrics(
            "orders",
            "consumer-a",
            ConsumerPressureMetrics::new(8, 10, 0),
        );

        assert!(below.is_empty());
        assert_eq!(above.len(), 1);
    }

    #[test]
    fn enforcement_returns_no_actions_below_all_thresholds() {
        let mut enforcer = PressureEnforcer::new();
        enforcer.set_channel_policy("orders", ChannelPolicyConfig::new(vec![slow_policy(0.7)]));

        let actions = enforcer.record_consumer_metrics(
            "orders",
            "consumer-a",
            ConsumerPressureMetrics::new(3, 10, 0),
        );

        assert!(actions.is_empty());
        assert!(enforcer.last_events().is_empty());
    }

    #[test]
    fn scale_consumer_policy_emits_scale_signal_with_channel_and_consumer_count() {
        let mut enforcer = PressureEnforcer::new();
        enforcer.set_channel_policy(
            "orders",
            ChannelPolicyConfig::new(vec![PressurePolicy {
                threshold: 0.7,
                action: PolicyAction::ScaleConsumer,
            }]),
        );
        enforcer.record_consumer_metrics(
            "orders",
            "consumer-a",
            ConsumerPressureMetrics::new(6, 10, 0),
        );

        let actions = enforcer.record_consumer_metrics(
            "orders",
            "consumer-b",
            ConsumerPressureMetrics::new(8, 10, 0),
        );

        assert_eq!(actions, vec![PolicyAction::ScaleConsumer]);
        assert!(enforcer.last_events().iter().any(|event| matches!(
            event,
            PolicyEvent::ScaleConsumer {
                signal: ScaleSignal {
                    channel_id,
                    current_consumer_count: 2,
                },
                triggered_at: _
            } if channel_id == "orders"
        )));
    }

    #[test]
    fn custom_monitor_scores_are_enforced_without_manual_evaluate_call() {
        let monitor = PressureMonitor::with_scoring(|_| 1.0);
        let mut enforcer = PressureEnforcer::with_monitor(monitor);
        enforcer.set_channel_policy("orders", ChannelPolicyConfig::new(vec![slow_policy(0.7)]));

        let actions = enforcer.record_accept("orders", "consumer-a");

        assert_eq!(actions.len(), 1);
    }
}
