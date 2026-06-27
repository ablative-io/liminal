use alloc::string::String;
use core::time::Duration;

/// Application-visible backpressure result for a publish operation.
///
/// Pressure responses are part of the successful publish path so applications can
/// react to delivery, buffering, and shedding decisions without those signals
/// being hidden inside transport-specific errors.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PressureResponse {
    /// The message was accepted and delivered to the consumer.
    Accept,
    /// The message was accepted into a buffer and will be delivered later.
    Defer {
        /// Estimated delay before the buffered message can be delivered.
        delay: Duration,
    },
    /// The consumer was overwhelmed and the message was shed.
    Reject {
        /// Human-readable reason describing why the message was shed.
        reason: String,
    },
}

/// Genuine delivery ack for a publish, distinct from the backpressure signal.
///
/// [`PressureResponse`] reports the bus's admission decision (accepted / buffered
/// / shed); a `DeliveryAck` reports whether the message was actually received by a
/// subscriber. A caller that must only treat a send as done when a worker genuinely
/// accepted it (for example the aion outbox) inspects [`DeliveryAck::is_accepted`]:
/// `true` means at least one subscriber received the message, `false` means the
/// publish succeeded but reached no subscriber (an empty channel, or a duplicate
/// suppressed by dedup-on-delivery).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliveryAck {
    pressure: PressureResponse,
    accepted: bool,
}

impl DeliveryAck {
    /// Builds a delivery ack from the backpressure response and acceptance flag.
    #[must_use]
    pub const fn new(pressure: PressureResponse, accepted: bool) -> Self {
        Self { pressure, accepted }
    }

    /// Whether the message was genuinely accepted by at least one subscriber.
    #[must_use]
    pub const fn is_accepted(&self) -> bool {
        self.accepted
    }

    /// The backpressure response the bus returned for this publish.
    #[must_use]
    pub const fn pressure(&self) -> &PressureResponse {
        &self.pressure
    }
}
