#![allow(clippy::module_name_repetitions)]

use alloc::collections::BTreeMap;
use alloc::format;
use alloc::vec::Vec;

use crate::SdkError;

use super::lifecycle::{ConnectionEvent, ConnectionState};

/// Application-visible identifier for an active subscription.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubscriptionId(u64);

impl SubscriptionId {
    /// Creates a subscription identifier from its wire-level numeric value.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the wire-level numeric value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for SubscriptionId {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

/// Resume request produced for an active subscription after reconnecting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResumeRequest {
    /// Subscription to resume.
    pub subscription_id: SubscriptionId,
    /// First sequence number that should be replayed.
    pub from_sequence: u64,
}

impl ResumeRequest {
    /// Creates a resume request for a subscription and starting sequence.
    #[must_use]
    pub const fn new(subscription_id: SubscriptionId, from_sequence: u64) -> Self {
        Self {
            subscription_id,
            from_sequence,
        }
    }
}

/// Tracks active subscriptions and their last acknowledged sequence numbers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SubscriptionRecovery {
    acknowledged: BTreeMap<SubscriptionId, u64>,
    active: Vec<SubscriptionId>,
}

impl SubscriptionRecovery {
    /// Creates an empty subscription recovery tracker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Marks a subscription as active for reconnect recovery.
    pub fn track_subscription(&mut self, subscription_id: SubscriptionId) {
        if !self.active.contains(&subscription_id) {
            self.active.push(subscription_id);
            self.active.sort_unstable();
        }
    }

    /// Records the last acknowledged sequence for an active subscription.
    pub fn acknowledge(&mut self, subscription_id: SubscriptionId, sequence: u64) {
        self.track_subscription(subscription_id);
        self.acknowledged
            .entry(subscription_id)
            .and_modify(|acknowledged| *acknowledged = core::cmp::max(*acknowledged, sequence))
            .or_insert(sequence);
    }

    /// Returns the last acknowledged sequence for a subscription, if any.
    #[must_use]
    pub fn last_acknowledged_sequence(&self, subscription_id: SubscriptionId) -> Option<u64> {
        self.acknowledged.get(&subscription_id).copied()
    }

    /// Computes the next sequence that should be requested for a subscription.
    ///
    /// A subscription with no prior acknowledgement resumes from sequence zero.
    /// A subscription last acknowledged at sequence `N` resumes from `N + 1`.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] if advancing the acknowledged sequence would overflow.
    pub fn resume_sequence(&self, subscription_id: SubscriptionId) -> Result<u64, SdkError> {
        self.acknowledged
            .get(&subscription_id)
            .copied()
            .map_or(Ok(0), |sequence| {
                sequence.checked_add(1).ok_or_else(|| SdkError::Store {
                    description: format!(
                        "cannot resume subscription {} after maximum sequence {sequence}",
                        subscription_id.get()
                    ),
                })
            })
    }

    /// Produces resume requests for every active subscription.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] if any acknowledged sequence cannot be advanced.
    pub fn resume_requests(&self) -> Result<Vec<ResumeRequest>, SdkError> {
        self.resume_requests_for_active()
    }

    /// Produces resume requests when a transition moves from reconnecting to connected.
    ///
    /// Non-recovery transitions produce an empty request list.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] if any acknowledged sequence cannot be advanced.
    pub fn resume_requests_for_transition(
        &self,
        event: &ConnectionEvent,
    ) -> Result<Vec<ResumeRequest>, SdkError> {
        if matches!(event.previous, ConnectionState::Reconnecting { .. })
            && event.current == ConnectionState::Connected
        {
            return self.resume_requests_for_active();
        }

        Ok(Vec::new())
    }

    fn resume_requests_for_active(&self) -> Result<Vec<ResumeRequest>, SdkError> {
        let mut requests = Vec::with_capacity(self.active.len());

        for subscription_id in &self.active {
            requests.push(ResumeRequest::new(
                *subscription_id,
                self.resume_sequence(*subscription_id)?,
            ));
        }

        Ok(requests)
    }

    /// Clears recovery state for an explicitly unsubscribed subscription.
    pub fn unsubscribe(&mut self, subscription_id: SubscriptionId) {
        self.acknowledged.remove(&subscription_id);
        self.active
            .retain(|active_id| *active_id != subscription_id);
    }

    /// Returns true when the subscription is currently tracked as active.
    #[must_use]
    pub fn is_active(&self, subscription_id: SubscriptionId) -> bool {
        self.active.contains(&subscription_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscriptions_without_acknowledgement_resume_from_zero() -> Result<(), SdkError> {
        let mut recovery = SubscriptionRecovery::new();
        let subscription_id = SubscriptionId::new(7);

        recovery.track_subscription(subscription_id);

        assert_eq!(recovery.resume_sequence(subscription_id)?, 0);
        assert_eq!(
            recovery.resume_requests()?,
            vec![ResumeRequest::new(subscription_id, 0)]
        );
        Ok(())
    }

    #[test]
    fn acknowledged_subscriptions_resume_after_last_sequence() -> Result<(), SdkError> {
        let mut recovery = SubscriptionRecovery::new();
        let subscription_id = SubscriptionId::new(9);

        recovery.acknowledge(subscription_id, 41);

        assert_eq!(
            recovery.last_acknowledged_sequence(subscription_id),
            Some(41)
        );
        assert_eq!(recovery.resume_sequence(subscription_id)?, 42);
        assert_eq!(
            recovery.resume_requests()?,
            vec![ResumeRequest::new(subscription_id, 42)]
        );
        Ok(())
    }

    #[test]
    fn unsubscribe_clears_recovery_state() -> Result<(), SdkError> {
        let mut recovery = SubscriptionRecovery::new();
        let subscription_id = SubscriptionId::new(11);

        recovery.acknowledge(subscription_id, 2);
        recovery.unsubscribe(subscription_id);

        assert!(!recovery.is_active(subscription_id));
        assert_eq!(recovery.last_acknowledged_sequence(subscription_id), None);
        assert!(recovery.resume_requests()?.is_empty());
        Ok(())
    }

    #[test]
    fn reconnect_to_connected_transition_builds_resume_requests() -> Result<(), SdkError> {
        let mut recovery = SubscriptionRecovery::new();
        let subscription_id = SubscriptionId::new(13);
        let event = ConnectionEvent::new(
            ConnectionState::Reconnecting { attempt: 2 },
            ConnectionState::Connected,
        );

        recovery.acknowledge(subscription_id, 5);

        assert_eq!(
            recovery.resume_requests_for_transition(&event)?,
            vec![ResumeRequest::new(subscription_id, 6)]
        );
        Ok(())
    }

    #[test]
    fn non_recovery_transition_builds_no_resume_requests() -> Result<(), SdkError> {
        let mut recovery = SubscriptionRecovery::new();
        let event = ConnectionEvent::new(ConnectionState::Connecting, ConnectionState::Connected);

        recovery.acknowledge(SubscriptionId::new(15), 5);

        assert!(recovery.resume_requests_for_transition(&event)?.is_empty());
        Ok(())
    }

    #[test]
    fn maximum_sequence_does_not_wrap_to_zero() {
        let mut recovery = SubscriptionRecovery::new();
        let subscription_id = SubscriptionId::new(17);

        recovery.acknowledge(subscription_id, u64::MAX);

        assert!(recovery.resume_sequence(subscription_id).is_err());
    }
}
