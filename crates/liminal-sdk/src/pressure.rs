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
