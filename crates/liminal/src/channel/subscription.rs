use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Weak};

use crate::envelope::Envelope;
use crate::error::LiminalError;

pub(crate) type SubscriberInbox = Arc<Mutex<VecDeque<Envelope>>>;
pub(crate) type SubscriberRegistration = Weak<Mutex<VecDeque<Envelope>>>;

/// Handle returned by channel subscriptions for receiving validated envelopes.
#[derive(Clone, Debug)]
pub struct SubscriptionHandle {
    inbox: SubscriberInbox,
}

impl SubscriptionHandle {
    pub(crate) fn new() -> Self {
        Self {
            inbox: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    pub(crate) fn registration(&self) -> SubscriberRegistration {
        Arc::downgrade(&self.inbox)
    }

    /// Attempts to receive the next delivered envelope without blocking.
    ///
    /// # Errors
    ///
    /// Returns [`LiminalError::SubscriptionFailed`] when the subscription inbox cannot be read.
    pub fn try_next(&self) -> Result<Option<Envelope>, LiminalError> {
        let mut messages = self
            .inbox
            .lock()
            .map_err(|error| LiminalError::SubscriptionFailed {
                message: format!("subscription inbox unavailable: {error}"),
            })?;
        Ok(messages.pop_front())
    }
}
