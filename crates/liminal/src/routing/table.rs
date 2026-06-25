use std::collections::BTreeMap;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::routing::{FieldAccessor, Predicate, evaluate};

/// Subscriber identity stored with a routing subscription.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SubscriberId(String);

impl SubscriberId {
    /// Creates a subscriber identity from an owned or borrowed string.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the subscriber identity as a borrowed string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// Active predicate subscription for a channel.
#[derive(Clone, Debug, PartialEq)]
pub struct Subscription {
    /// Subscriber identity that owns this subscription.
    pub subscriber: SubscriberId,
    /// Predicate that must match for this subscription to receive a message.
    pub predicate: Predicate,
}

impl Subscription {
    /// Creates a subscription from a subscriber identity and predicate.
    #[must_use]
    pub const fn new(subscriber: SubscriberId, predicate: Predicate) -> Self {
        Self {
            subscriber,
            predicate,
        }
    }
}

/// Concurrent routing table mapping channels to active predicate subscriptions.
#[derive(Clone, Debug)]
pub struct RoutingTable {
    inner: Arc<TableInner>,
}

impl RoutingTable {
    /// Creates an empty routing table.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(TableInner::new()),
        }
    }

    /// Returns true when no channel has any active subscriptions.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        read_table_state(&self.inner.state).channels.is_empty()
    }

    /// Registers `subscription` on `channel` and returns its shared table handle.
    #[must_use]
    pub fn register(
        &self,
        channel: impl Into<String>,
        subscription: Subscription,
    ) -> Arc<Subscription> {
        let channel = channel.into();
        let subscription = Arc::new(subscription);
        let mut state = write_table_state(&self.inner.state);
        let mut subscriptions = state
            .channels
            .get(channel.as_str())
            .map_or_else(Vec::new, |snapshot| {
                snapshot.iter().cloned().collect::<Vec<_>>()
            });

        subscriptions.push(Arc::clone(&subscription));
        state
            .channels
            .insert(channel, Arc::from(subscriptions.into_boxed_slice()));

        subscription
    }

    /// Removes subscriptions for `subscriber` on `channel`.
    #[must_use]
    pub fn remove(&self, channel: &str, subscriber: &SubscriberId) -> bool {
        let mut state = write_table_state(&self.inner.state);
        let Some(snapshot) = state.channels.get(channel).cloned() else {
            return false;
        };

        let mut removed = false;
        let mut subscriptions = Vec::with_capacity(snapshot.len());
        for subscription in snapshot.iter() {
            if subscription.subscriber == *subscriber {
                removed = true;
            } else {
                subscriptions.push(Arc::clone(subscription));
            }
        }

        if removed {
            if subscriptions.is_empty() {
                state.channels.remove(channel);
            } else {
                state.channels.insert(
                    channel.to_owned(),
                    Arc::from(subscriptions.into_boxed_slice()),
                );
            }
        }

        removed
    }

    /// Resolves all subscriptions on `channel` whose predicates match `accessor`.
    #[must_use]
    pub fn resolve(&self, channel: &str, accessor: &dyn FieldAccessor) -> Vec<Arc<Subscription>> {
        let snapshot = {
            let state = read_table_state(&self.inner.state);
            state.channels.get(channel).cloned()
        };

        let Some(subscriptions) = snapshot else {
            return Vec::new();
        };

        subscriptions
            .iter()
            .filter(|subscription| evaluate(&subscription.predicate, accessor))
            .cloned()
            .collect()
    }
}

impl Default for RoutingTable {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct TableInner {
    state: RwLock<TableState>,
}

impl TableInner {
    fn new() -> Self {
        Self {
            state: RwLock::new(TableState::default()),
        }
    }
}

#[derive(Debug, Default)]
struct TableState {
    channels: BTreeMap<String, Arc<[Arc<Subscription>]>>,
}

fn read_table_state(lock: &RwLock<TableState>) -> RwLockReadGuard<'_, TableState> {
    match lock.read() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn write_table_state(lock: &RwLock<TableState>) -> RwLockWriteGuard<'_, TableState> {
    match lock.write() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
    use std::thread;
    use std::time::Duration;

    use super::{RoutingTable, SubscriberId, Subscription};
    use crate::routing::{
        ComparisonOp, FieldAccessor, FieldPath, FieldValue, FieldValueRef, Predicate,
    };

    #[derive(Debug)]
    struct StaticAccessor {
        field: &'static str,
        value: FieldValueRef<'static>,
    }

    impl StaticAccessor {
        const fn new(field: &'static str, value: FieldValueRef<'static>) -> Self {
            Self { field, value }
        }
    }

    impl FieldAccessor for StaticAccessor {
        fn field(&self, path: &FieldPath) -> Option<FieldValueRef<'_>> {
            path.segments().eq([self.field]).then_some(self.value)
        }
    }

    struct BlockingAccessor {
        entered: SyncSender<()>,
        release: Receiver<()>,
    }

    impl std::fmt::Debug for BlockingAccessor {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("BlockingAccessor")
                .finish_non_exhaustive()
        }
    }

    impl BlockingAccessor {
        const fn new(entered: SyncSender<()>, release: Receiver<()>) -> Self {
            Self { entered, release }
        }
    }

    impl FieldAccessor for BlockingAccessor {
        fn field(&self, path: &FieldPath) -> Option<FieldValueRef<'_>> {
            if path.segments().eq(["gate"]) {
                if self.entered.send(()).is_err() {
                    return None;
                }
                if self.release.recv().is_err() {
                    return None;
                }

                Some(FieldValueRef::Boolean(true))
            } else {
                None
            }
        }
    }

    fn assert_send_sync<T: Send + Sync>() {}

    fn amount_greater_than(value: i64) -> Predicate {
        Predicate::Comparison {
            field: FieldPath::new("amount"),
            op: ComparisonOp::Gt,
            value: FieldValue::Integer(value),
        }
    }

    fn gate_predicate() -> Predicate {
        Predicate::Comparison {
            field: FieldPath::new("gate"),
            op: ComparisonOp::Eq,
            value: FieldValue::Boolean(true),
        }
    }

    fn subscription(subscriber: &str, predicate: Predicate) -> Subscription {
        Subscription::new(SubscriberId::new(subscriber), predicate)
    }

    #[test]
    fn new_table_is_empty() {
        let table = RoutingTable::new();

        assert!(table.is_empty());
    }

    #[test]
    fn resolve_returns_matching_subscription() {
        let table = RoutingTable::new();
        let registered = table.register(
            "orders",
            subscription("billing", amount_greater_than(1_000)),
        );
        let accessor = StaticAccessor::new("amount", FieldValueRef::Integer(1_500));

        let matches = table.resolve("orders", &accessor);

        assert_eq!(matches, vec![registered]);
    }

    #[test]
    fn resolve_returns_empty_when_no_subscription_matches() {
        let table = RoutingTable::new();
        let _ = table.register(
            "orders",
            subscription("billing", amount_greater_than(1_000)),
        );
        let accessor = StaticAccessor::new("amount", FieldValueRef::Integer(500));

        assert!(table.resolve("orders", &accessor).is_empty());
    }

    #[test]
    fn multiple_subscriptions_on_channel_are_evaluated_independently() {
        let table = RoutingTable::new();
        let low = table.register("orders", subscription("low", amount_greater_than(100)));
        let high = table.register("orders", subscription("high", amount_greater_than(1_000)));
        let accessor = StaticAccessor::new("amount", FieldValueRef::Integer(500));

        let matches = table.resolve("orders", &accessor);

        assert_eq!(matches, vec![low]);
        assert_ne!(matches, vec![high]);
    }

    #[test]
    fn routing_table_is_send_and_sync() {
        assert_send_sync::<RoutingTable>();
    }

    #[test]
    fn register_does_not_block_active_resolve() {
        let table = RoutingTable::new();
        let _ = table.register("orders", subscription("initial", gate_predicate()));
        let resolver_table = table.clone();
        let (entered_sender, entered_receiver) = sync_channel(0);
        let (release_sender, release_receiver) = sync_channel(0);

        let resolver = thread::spawn(move || {
            let accessor = BlockingAccessor::new(entered_sender, release_receiver);
            resolver_table.resolve("orders", &accessor).len()
        });

        assert!(entered_receiver.recv().is_ok());
        let registered = table.register("orders", subscription("new", gate_predicate()));
        assert_eq!(registered.subscriber.as_str(), "new");
        assert!(release_sender.send(()).is_ok());
        assert!(matches!(resolver.join(), Ok(1)));

        let accessor = StaticAccessor::new("gate", FieldValueRef::Boolean(true));
        assert_eq!(table.resolve("orders", &accessor).len(), 2);
    }

    #[test]
    fn remove_does_not_block_active_resolve() {
        let table = RoutingTable::new();
        let subscriber = SubscriberId::new("initial");
        let _ = table.register(
            "orders",
            Subscription::new(subscriber.clone(), gate_predicate()),
        );
        let resolver_table = table.clone();
        let (entered_sender, entered_receiver) = sync_channel(0);
        let (release_sender, release_receiver) = sync_channel(0);

        let resolver = thread::spawn(move || {
            let accessor = BlockingAccessor::new(entered_sender, release_receiver);
            resolver_table.resolve("orders", &accessor).len()
        });

        assert!(entered_receiver.recv().is_ok());
        assert!(table.remove("orders", &subscriber));
        assert!(release_sender.send(()).is_ok());
        assert!(matches!(resolver.join(), Ok(1)));

        let accessor = StaticAccessor::new("gate", FieldValueRef::Boolean(true));
        assert!(table.resolve("orders", &accessor).is_empty());
    }

    #[test]
    fn register_during_active_resolve_preserves_state() {
        let table = RoutingTable::new();
        let _ = table.register("orders", subscription("initial", gate_predicate()));
        let resolver_table = table.clone();
        let (entered_sender, entered_receiver) = sync_channel(0);
        let (release_sender, release_receiver) = sync_channel(0);

        let resolver = thread::spawn(move || {
            let accessor = BlockingAccessor::new(entered_sender, release_receiver);
            resolver_table.resolve("orders", &accessor).len()
        });

        assert!(entered_receiver.recv().is_ok());
        let _ = table.register("orders", subscription("new", gate_predicate()));
        assert!(release_sender.send(()).is_ok());
        assert!(matches!(resolver.join(), Ok(1)));

        let accessor = StaticAccessor::new("gate", FieldValueRef::Boolean(true));
        let matches = table.resolve("orders", &accessor);

        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].subscriber.as_str(), "initial");
        assert_eq!(matches[1].subscriber.as_str(), "new");
    }

    #[test]
    fn resolve_nonexistent_channel_returns_empty_set() {
        let table = RoutingTable::new();
        let accessor = StaticAccessor::new("amount", FieldValueRef::Integer(1_500));

        assert!(table.resolve("nonexistent-channel", &accessor).is_empty());
    }

    #[test]
    fn removing_last_subscription_makes_channel_resolve_empty() {
        let table = RoutingTable::new();
        let subscriber = SubscriberId::new("billing");
        let _ = table.register(
            "orders",
            Subscription::new(subscriber.clone(), amount_greater_than(1_000)),
        );
        let accessor = StaticAccessor::new("amount", FieldValueRef::Integer(1_500));

        assert!(table.remove("orders", &subscriber));
        assert!(table.resolve("orders", &accessor).is_empty());
    }

    #[test]
    fn updater_completion_is_observed_before_resolve_release() {
        let table = RoutingTable::new();
        let _ = table.register("orders", subscription("initial", gate_predicate()));
        let resolver_table = table.clone();
        let updater_table = table;
        let (entered_sender, entered_receiver) = sync_channel(0);
        let (release_sender, release_receiver) = sync_channel(0);
        let (updated_sender, updated_receiver) = sync_channel(0);

        let resolver = thread::spawn(move || {
            let accessor = BlockingAccessor::new(entered_sender, release_receiver);
            resolver_table.resolve("orders", &accessor).len()
        });

        assert!(entered_receiver.recv().is_ok());
        let updater = thread::spawn(move || {
            let _ = updater_table.register("orders", subscription("new", gate_predicate()));
            updated_sender.send(())
        });
        assert!(
            updated_receiver
                .recv_timeout(Duration::from_secs(1))
                .is_ok()
        );
        assert!(release_sender.send(()).is_ok());
        assert!(matches!(resolver.join(), Ok(1)));
        assert!(matches!(updater.join(), Ok(Ok(()))));
    }
}
