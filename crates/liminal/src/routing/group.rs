use std::collections::BTreeMap;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::conversation::ParticipantPid;
use crate::routing::{ConsumerId, ConsumerStateView, RoutingFunction};

/// One active consumer registered in a routing consumer group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerRegistration {
    consumer: ConsumerId,
    participant: ParticipantPid,
    state: ConsumerStateView,
}

impl ConsumerRegistration {
    /// Creates a consumer registration from the state view exposed to routing functions.
    #[must_use]
    pub fn new(participant: ParticipantPid, state: ConsumerStateView) -> Self {
        Self {
            consumer: state.consumer.clone(),
            participant,
            state,
        }
    }

    /// Creates a consumer with a one-slot default capacity view.
    #[must_use]
    pub fn with_default_state(consumer: ConsumerId, participant: ParticipantPid) -> Self {
        let state = ConsumerStateView::new(consumer, 0, 1, 0, Vec::new());
        Self::new(participant, state)
    }

    /// Stable consumer identifier used by routing decisions.
    #[must_use]
    pub const fn consumer(&self) -> &ConsumerId {
        &self.consumer
    }

    /// Beamr participant process linked by dispatch conversations.
    #[must_use]
    pub const fn participant(&self) -> ParticipantPid {
        self.participant
    }

    /// Per-consumer state presented to routing functions.
    #[must_use]
    pub const fn state(&self) -> &ConsumerStateView {
        &self.state
    }
}

/// Immutable snapshot of a consumer group at a dispatch boundary.
#[derive(Clone, Debug)]
pub struct ConsumerGroupSnapshot {
    routing_function: RoutingFunction,
    consumers: Arc<[ConsumerRegistration]>,
}

impl ConsumerGroupSnapshot {
    /// Returns the routing function active for this snapshot.
    #[must_use]
    pub const fn routing_function(&self) -> &RoutingFunction {
        &self.routing_function
    }

    /// Returns the ordered, deduplicated consumers captured by this snapshot.
    #[must_use]
    pub fn consumers(&self) -> &[ConsumerRegistration] {
        &self.consumers
    }

    /// Returns the ordered consumer identifiers captured by this snapshot.
    #[must_use]
    pub fn consumer_ids(&self) -> Vec<ConsumerId> {
        self.consumers
            .iter()
            .map(|registration| registration.consumer.clone())
            .collect()
    }
}

/// A consumer group modeled as a routing function over a mutable consumer set.
#[derive(Clone, Debug)]
pub struct ConsumerGroup {
    inner: Arc<GroupInner>,
}

impl ConsumerGroup {
    /// Creates a consumer group with no active consumers.
    #[must_use]
    pub fn new(routing_function: RoutingFunction) -> Self {
        Self {
            inner: Arc::new(GroupInner {
                routing_function,
                state: RwLock::new(GroupState::default()),
            }),
        }
    }

    /// Returns the routing function associated with this group.
    #[must_use]
    pub fn routing_function(&self) -> RoutingFunction {
        self.inner.routing_function.clone()
    }

    /// Returns the ordered, deduplicated active consumer identifiers.
    #[must_use]
    pub fn consumers(&self) -> Vec<ConsumerId> {
        read_group_state(&self.inner.state)
            .consumers
            .keys()
            .cloned()
            .collect()
    }

    /// Returns a stable dispatch-boundary snapshot of the group.
    #[must_use]
    pub fn snapshot(&self) -> ConsumerGroupSnapshot {
        let consumers = read_group_state(&self.inner.state)
            .consumers
            .values()
            .cloned()
            .collect::<Vec<_>>();
        ConsumerGroupSnapshot {
            routing_function: self.routing_function(),
            consumers: Arc::from(consumers.into_boxed_slice()),
        }
    }

    /// Adds or updates a consumer, returning true when the id was newly inserted.
    #[must_use = "the boolean reports whether the consumer was newly inserted"]
    pub fn add_consumer(&self, registration: ConsumerRegistration) -> bool {
        write_group_state(&self.inner.state)
            .consumers
            .insert(registration.consumer.clone(), registration)
            .is_none()
    }

    /// Removes a consumer from future group snapshots.
    #[must_use = "the boolean reports whether a consumer was actually removed"]
    pub fn remove_consumer(&self, consumer: &ConsumerId) -> bool {
        write_group_state(&self.inner.state)
            .consumers
            .remove(consumer)
            .is_some()
    }
}

#[derive(Debug)]
struct GroupInner {
    routing_function: RoutingFunction,
    state: RwLock<GroupState>,
}

#[derive(Debug, Default)]
struct GroupState {
    consumers: BTreeMap<ConsumerId, ConsumerRegistration>,
}

fn read_group_state(lock: &RwLock<GroupState>) -> RwLockReadGuard<'_, GroupState> {
    match lock.read() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn write_group_state(lock: &RwLock<GroupState>) -> RwLockWriteGuard<'_, GroupState> {
    match lock.write() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests {
    use super::{ConsumerGroup, ConsumerRegistration};
    use crate::conversation::ParticipantPid;
    use crate::routing::function::loader::{ModuleLoader, RoutingModule};
    use crate::routing::{ConsumerId, ConsumerStateView, RoutingDecision};

    fn function() -> crate::routing::RoutingFunction {
        ModuleLoader::new().load(RoutingModule::new(b"group-test", |_message, consumers| {
            consumers
                .first()
                .map_or_else(RoutingDecision::none, |consumer| {
                    RoutingDecision::select(consumer.consumer.clone())
                })
        }))
    }

    fn registration(id: &str, pid: u64) -> ConsumerRegistration {
        ConsumerRegistration::new(
            ParticipantPid::new(pid),
            ConsumerStateView::new(ConsumerId::new(id), 0, 1, 0, Vec::new()),
        )
    }

    #[test]
    fn new_group_has_routing_function_and_empty_consumer_set() {
        let routing_function = function();
        let group = ConsumerGroup::new(routing_function.clone());

        assert_eq!(
            group.routing_function().content_hash(),
            routing_function.content_hash()
        );
        assert!(group.consumers().is_empty());
        assert!(format!("{group:?}").contains("ConsumerGroup"));
    }

    #[test]
    fn consumer_set_is_ordered_and_deduplicated() {
        let group = ConsumerGroup::new(function());

        assert!(group.add_consumer(registration("B", 2)));
        assert!(group.add_consumer(registration("A", 1)));
        assert!(!group.add_consumer(registration("B", 22)));
        assert!(group.add_consumer(registration("C", 3)));

        assert_eq!(
            ids(group.consumers()),
            vec!["A".to_owned(), "B".to_owned(), "C".to_owned()]
        );
        assert_eq!(
            group.snapshot().consumers()[1].participant(),
            ParticipantPid::new(22)
        );
    }

    #[test]
    fn remove_consumer_affects_future_snapshots_only() {
        let group = ConsumerGroup::new(function());
        let _ = group.add_consumer(registration("A", 1));
        let _ = group.add_consumer(registration("B", 2));
        let _ = group.add_consumer(registration("C", 3));
        let before = group.snapshot();

        assert!(group.remove_consumer(&ConsumerId::new("B")));
        assert!(!group.remove_consumer(&ConsumerId::new("B")));

        assert_eq!(ids(group.consumers()), vec!["A".to_owned(), "C".to_owned()]);
        assert_eq!(ids(before.consumer_ids()), vec!["A", "B", "C"]);
    }

    fn ids(consumers: Vec<ConsumerId>) -> Vec<String> {
        consumers
            .into_iter()
            .map(|consumer| consumer.as_str().to_owned())
            .collect()
    }
}
