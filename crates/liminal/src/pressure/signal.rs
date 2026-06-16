/// Producer-visible backpressure signal returned during a publish round-trip.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PressureSignal {
    /// Message delivered to the consumer and processing has begun.
    Accept {
        /// Messages already in flight when this delivery decision was made.
        current_in_flight: usize,
        /// Maximum messages the consumer declared it can process concurrently.
        max_in_flight: usize,
    },
    /// Message buffered and will be delivered when consumer capacity frees.
    Defer {
        /// Messages already in flight when this delivery decision was made.
        current_in_flight: usize,
        /// Maximum messages the consumer declared it can process concurrently.
        max_in_flight: usize,
        /// Messages already buffered when this delivery decision was made.
        current_buffer_depth: usize,
        /// Maximum messages the consumer declared can wait for capacity.
        max_buffer_depth: usize,
    },
    /// Consumer is overwhelmed and the message has been shed.
    Reject {
        /// Messages already in flight when this delivery decision was made.
        current_in_flight: usize,
        /// Maximum messages the consumer declared it can process concurrently.
        max_in_flight: usize,
        /// Messages already buffered when this delivery decision was made.
        current_buffer_depth: usize,
        /// Maximum messages the consumer declared can wait for capacity.
        max_buffer_depth: usize,
    },
}

impl PressureSignal {
    /// Creates an accept signal for a consumer with available in-flight capacity.
    #[must_use]
    pub const fn accept(current_in_flight: usize, max_in_flight: usize) -> Self {
        Self::Accept {
            current_in_flight,
            max_in_flight,
        }
    }

    /// Creates a defer signal for a consumer whose in-flight slots are full but buffer has room.
    #[must_use]
    pub const fn defer(
        current_in_flight: usize,
        max_in_flight: usize,
        current_buffer_depth: usize,
        max_buffer_depth: usize,
    ) -> Self {
        Self::Defer {
            current_in_flight,
            max_in_flight,
            current_buffer_depth,
            max_buffer_depth,
        }
    }

    /// Creates a reject signal for a consumer whose in-flight and buffer limits are reached.
    #[must_use]
    pub const fn reject(
        current_in_flight: usize,
        max_in_flight: usize,
        current_buffer_depth: usize,
        max_buffer_depth: usize,
    ) -> Self {
        Self::Reject {
            current_in_flight,
            max_in_flight,
            current_buffer_depth,
            max_buffer_depth,
        }
    }
}

/// Synchronous delivery result returned to a producer before publish returns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliveryResult {
    /// Backpressure signal produced during the same publish round-trip.
    pub signal: PressureSignal,
}

impl DeliveryResult {
    /// Creates a delivery result with the signal returned to the producer.
    #[must_use]
    pub const fn new(signal: PressureSignal) -> Self {
        Self { signal }
    }

    /// Creates an accept delivery result.
    #[must_use]
    pub const fn accepted(current_in_flight: usize, max_in_flight: usize) -> Self {
        Self::new(PressureSignal::accept(current_in_flight, max_in_flight))
    }

    /// Creates a defer delivery result.
    #[must_use]
    pub const fn deferred(
        current_in_flight: usize,
        max_in_flight: usize,
        current_buffer_depth: usize,
        max_buffer_depth: usize,
    ) -> Self {
        Self::new(PressureSignal::defer(
            current_in_flight,
            max_in_flight,
            current_buffer_depth,
            max_buffer_depth,
        ))
    }

    /// Creates a reject delivery result.
    #[must_use]
    pub const fn rejected(
        current_in_flight: usize,
        max_in_flight: usize,
        current_buffer_depth: usize,
        max_buffer_depth: usize,
    ) -> Self {
        Self::new(PressureSignal::reject(
            current_in_flight,
            max_in_flight,
            current_buffer_depth,
            max_buffer_depth,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{DeliveryResult, PressureSignal};

    #[test]
    fn pressure_signal_defines_only_accept_defer_reject_semantics() {
        let accept = PressureSignal::accept(0, 10);
        let defer = PressureSignal::defer(10, 10, 3, 50);
        let reject = PressureSignal::reject(10, 10, 50, 50);

        assert_eq!(
            accept,
            PressureSignal::Accept {
                current_in_flight: 0,
                max_in_flight: 10,
            }
        );
        assert_eq!(
            defer,
            PressureSignal::Defer {
                current_in_flight: 10,
                max_in_flight: 10,
                current_buffer_depth: 3,
                max_buffer_depth: 50,
            }
        );
        assert_eq!(
            reject,
            PressureSignal::Reject {
                current_in_flight: 10,
                max_in_flight: 10,
                current_buffer_depth: 50,
                max_buffer_depth: 50,
            }
        );
    }

    #[test]
    fn delivery_result_carries_signal_in_return_value() {
        let accepted = DeliveryResult::accepted(1, 10);
        let deferred = DeliveryResult::deferred(10, 10, 2, 50);
        let rejected = DeliveryResult::rejected(10, 10, 50, 50);

        assert_eq!(accepted.signal, PressureSignal::accept(1, 10));
        assert_eq!(deferred.signal, PressureSignal::defer(10, 10, 2, 50));
        assert_eq!(rejected.signal, PressureSignal::reject(10, 10, 50, 50));
    }

    #[test]
    fn pressure_root_re_exports_signal_result_types() {
        use crate::pressure::{
            DeliveryResult as RootDeliveryResult, PressureSignal as RootPressureSignal,
        };

        let result = RootDeliveryResult::new(RootPressureSignal::accept(0, 1));

        assert_eq!(result.signal, RootPressureSignal::accept(0, 1));
    }
}
