#![allow(clippy::module_name_repetitions)]

/// Operator-facing severity for pressure alert policy actions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AlertSeverity {
    /// Informational pressure notification.
    Info,
    /// Warning pressure notification.
    Warning,
    /// Error pressure notification.
    Error,
    /// Critical pressure notification.
    Critical,
}

/// Action selected by a channel pressure policy when a threshold is active.
#[derive(Clone, Debug, PartialEq)]
pub enum PolicyAction {
    /// Signal producers to reduce their publish rate by the configured factor.
    SlowProducer {
        /// Multiplicative rate reduction factor the producer should apply.
        reduction_factor: f64,
    },
    /// Reject messages below the configured priority class while overloaded.
    ShedLoad {
        /// Priority threshold below which messages are rejected.
        priority_threshold: u8,
    },
    /// Emit a scale signal for external orchestration systems.
    ScaleConsumer,
    /// Emit an observability event at the configured severity.
    Alert {
        /// Severity attached to the pressure event.
        severity: AlertSeverity,
    },
}

/// One pressure threshold and the action active at or above that threshold.
#[derive(Clone, Debug, PartialEq)]
pub struct PressurePolicy {
    /// Channel pressure threshold, expected to be between 0.0 and 1.0.
    pub threshold: f64,
    /// Action active while channel pressure is at or above the threshold.
    pub action: PolicyAction,
}

/// Ordered pressure escalation policy for a channel.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ChannelPolicyConfig {
    policies: Vec<PressurePolicy>,
}

impl ChannelPolicyConfig {
    /// Creates a channel policy configuration ordered by ascending threshold.
    #[must_use]
    pub fn new(mut policies: Vec<PressurePolicy>) -> Self {
        policies.sort_by(|left, right| left.threshold.total_cmp(&right.threshold));
        Self { policies }
    }

    /// Returns the configured pressure policies in escalation order.
    #[must_use]
    pub fn policies(&self) -> &[PressurePolicy] {
        &self.policies
    }

    /// Returns whether this channel has no configured pressure policies.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.policies.is_empty()
    }

    /// Returns the number of configured pressure policies for this channel.
    #[must_use]
    pub fn len(&self) -> usize {
        self.policies.len()
    }

    /// Returns every action active for the supplied channel pressure score.
    #[must_use]
    pub fn actions_for_pressure(&self, pressure: f64) -> Vec<PolicyAction> {
        let mut actions = Vec::new();
        for policy in &self.policies {
            if policy.threshold <= pressure {
                actions.push(policy.action.clone());
            } else {
                break;
            }
        }
        actions
    }
}

#[cfg(test)]
mod tests {
    use super::{AlertSeverity, ChannelPolicyConfig, PolicyAction, PressurePolicy};

    fn action_name(action: &PolicyAction) -> &'static str {
        match action {
            PolicyAction::SlowProducer { .. } => "slow-producer",
            PolicyAction::ShedLoad { .. } => "shed-load",
            PolicyAction::ScaleConsumer => "scale-consumer",
            PolicyAction::Alert { .. } => "alert",
        }
    }

    fn close_to(actual: f64, expected: f64) -> bool {
        (actual - expected).abs() < f64::EPSILON
    }

    #[test]
    fn policy_action_defines_exact_pressure_actions() {
        let slow = PolicyAction::SlowProducer {
            reduction_factor: 0.5,
        };
        let shed = PolicyAction::ShedLoad {
            priority_threshold: 3,
        };
        let scale = PolicyAction::ScaleConsumer;
        let alert = PolicyAction::Alert {
            severity: AlertSeverity::Warning,
        };

        assert_eq!(action_name(&slow), "slow-producer");
        assert_eq!(action_name(&shed), "shed-load");
        assert_eq!(action_name(&scale), "scale-consumer");
        assert_eq!(action_name(&alert), "alert");
    }

    #[test]
    fn pressure_policy_constructs_with_action_and_threshold() {
        let policy = PressurePolicy {
            threshold: 0.7,
            action: PolicyAction::SlowProducer {
                reduction_factor: 0.25,
            },
        };

        assert!(close_to(policy.threshold, 0.7));
        assert!(matches!(
            policy.action,
            PolicyAction::SlowProducer { reduction_factor } if close_to(reduction_factor, 0.25)
        ));
    }

    #[test]
    fn channel_policy_config_stores_policies_in_threshold_order() {
        let high = PressurePolicy {
            threshold: 0.9,
            action: PolicyAction::ShedLoad {
                priority_threshold: 2,
            },
        };
        let low = PressurePolicy {
            threshold: 0.5,
            action: PolicyAction::SlowProducer {
                reduction_factor: 0.5,
            },
        };

        let config = ChannelPolicyConfig::new(vec![high, low]);

        assert_eq!(config.len(), 2);
        assert!(close_to(config.policies()[0].threshold, 0.5));
        assert!(close_to(config.policies()[1].threshold, 0.9));
    }

    #[test]
    fn policies_compose_in_escalation_sequence() {
        let slow = PressurePolicy {
            threshold: 0.5,
            action: PolicyAction::SlowProducer {
                reduction_factor: 0.5,
            },
        };
        let shed = PressurePolicy {
            threshold: 0.8,
            action: PolicyAction::ShedLoad {
                priority_threshold: 4,
            },
        };
        let config = ChannelPolicyConfig::new(vec![shed, slow]);

        let moderate = config.actions_for_pressure(0.6);
        let high = config.actions_for_pressure(0.9);
        let reduced = config.actions_for_pressure(0.6);
        let clear = config.actions_for_pressure(0.3);

        assert_eq!(moderate.len(), 1);
        assert!(matches!(
            moderate.as_slice(),
            [PolicyAction::SlowProducer { reduction_factor }] if close_to(*reduction_factor, 0.5)
        ));
        assert_eq!(high.len(), 2);
        assert!(matches!(high[0], PolicyAction::SlowProducer { .. }));
        assert!(matches!(high[1], PolicyAction::ShedLoad { .. }));
        assert_eq!(reduced.len(), 1);
        assert!(clear.is_empty());
    }

    #[test]
    fn pressure_root_re_exports_policy_types() {
        use crate::pressure::{AlertSeverity as RootSeverity, PolicyAction as RootAction};

        let action = RootAction::Alert {
            severity: RootSeverity::Critical,
        };

        assert!(matches!(
            action,
            RootAction::Alert {
                severity: RootSeverity::Critical
            }
        ));
    }
}
