/// Stub handle returned by channel subscriptions.
#[derive(Clone, Debug)]
pub struct SubscriptionHandle;

impl SubscriptionHandle {
    pub(crate) const fn new() -> Self {
        Self
    }
}
