pub use state::{CapacityError, CapacityTracker, ConsumerCapacity};

mod state {
    use crate::pressure::signal::PressureSignal;

    /// Consumer-declared capacity limits for pressure-aware delivery.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct ConsumerCapacity {
        /// Maximum messages this consumer can process concurrently.
        pub max_in_flight: usize,
        /// Maximum messages that may wait for this consumer's capacity to free.
        pub max_buffer_depth: usize,
    }

    impl ConsumerCapacity {
        /// Creates a capacity declaration after verifying both limits are positive.
        ///
        /// # Errors
        ///
        /// Returns [`CapacityError::InvalidCapacity`] when either declared limit is zero.
        pub const fn new(
            max_in_flight: usize,
            max_buffer_depth: usize,
        ) -> Result<Self, CapacityError> {
            if max_in_flight == 0 || max_buffer_depth == 0 {
                Err(CapacityError::InvalidCapacity {
                    max_in_flight,
                    max_buffer_depth,
                })
            } else {
                Ok(Self {
                    max_in_flight,
                    max_buffer_depth,
                })
            }
        }

        /// Verifies that the declared capacity contains positive limits.
        ///
        /// # Errors
        ///
        /// Returns [`CapacityError::InvalidCapacity`] when either declared limit is zero.
        pub const fn validate(&self) -> Result<(), CapacityError> {
            if self.max_in_flight == 0 || self.max_buffer_depth == 0 {
                Err(CapacityError::InvalidCapacity {
                    max_in_flight: self.max_in_flight,
                    max_buffer_depth: self.max_buffer_depth,
                })
            } else {
                Ok(())
            }
        }
    }

    /// Capacity tracking failures that keep counters from entering invalid states.
    #[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
    pub enum CapacityError {
        /// A capacity declaration used zero for at least one required positive limit.
        #[error("consumer capacity limits must be positive")]
        InvalidCapacity {
            /// Declared maximum in-flight messages.
            max_in_flight: usize,
            /// Declared maximum buffered messages.
            max_buffer_depth: usize,
        },
        /// Processing completion was recorded while no message was in flight.
        #[error("cannot decrement in-flight count below zero")]
        InFlightUnderflow,
        /// Buffer removal was recorded while no message was buffered.
        #[error("cannot decrement buffer depth below zero")]
        BufferUnderflow,
    }

    /// Per-consumer tracker for current in-flight and buffered message counts.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct CapacityTracker {
        capacity: ConsumerCapacity,
        current_in_flight: usize,
        current_buffer_depth: usize,
    }

    impl CapacityTracker {
        /// Creates a tracker for an explicitly declared consumer capacity.
        #[must_use]
        pub const fn new(capacity: ConsumerCapacity) -> Self {
            Self {
                capacity,
                current_in_flight: 0,
                current_buffer_depth: 0,
            }
        }

        /// Returns the consumer capacity declaration this tracker follows.
        #[must_use]
        pub const fn capacity(&self) -> &ConsumerCapacity {
            &self.capacity
        }

        /// Returns the number of messages currently being processed by the consumer.
        #[must_use]
        pub const fn current_in_flight(&self) -> usize {
            self.current_in_flight
        }

        /// Returns the number of messages currently buffered for the consumer.
        #[must_use]
        pub const fn current_buffer_depth(&self) -> usize {
            self.current_buffer_depth
        }

        /// Records that a message was delivered and processing began.
        pub const fn record_delivery(&mut self) {
            if self.current_in_flight < usize::MAX {
                self.current_in_flight += 1;
            }
        }

        /// Records that processing completed for one in-flight message.
        ///
        /// # Errors
        ///
        /// Returns [`CapacityError::InFlightUnderflow`] if no message is currently in flight.
        pub const fn record_completion(&mut self) -> Result<(), CapacityError> {
            if self.current_in_flight == 0 {
                Err(CapacityError::InFlightUnderflow)
            } else {
                self.current_in_flight -= 1;
                Ok(())
            }
        }

        /// Records that a message was buffered pending consumer capacity.
        pub const fn record_buffered(&mut self) {
            if self.current_buffer_depth < usize::MAX {
                self.current_buffer_depth += 1;
            }
        }

        /// Records that one buffered message left the buffer.
        ///
        /// # Errors
        ///
        /// Returns [`CapacityError::BufferUnderflow`] if no message is currently buffered.
        pub const fn record_buffer_drained(&mut self) -> Result<(), CapacityError> {
            if self.current_buffer_depth == 0 {
                Err(CapacityError::BufferUnderflow)
            } else {
                self.current_buffer_depth -= 1;
                Ok(())
            }
        }

        /// Determines the pressure signal for the next message without mutating counters.
        #[must_use]
        pub const fn pressure_signal(&self) -> PressureSignal {
            if self.current_in_flight < self.capacity.max_in_flight {
                PressureSignal::accept(self.current_in_flight, self.capacity.max_in_flight)
            } else if self.current_buffer_depth < self.capacity.max_buffer_depth {
                PressureSignal::defer(
                    self.current_in_flight,
                    self.capacity.max_in_flight,
                    self.current_buffer_depth,
                    self.capacity.max_buffer_depth,
                )
            } else {
                PressureSignal::reject(
                    self.current_in_flight,
                    self.capacity.max_in_flight,
                    self.current_buffer_depth,
                    self.capacity.max_buffer_depth,
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CapacityError, CapacityTracker, ConsumerCapacity};
    use crate::pressure::PressureSignal;

    const fn capacity(max_in_flight: usize, max_buffer_depth: usize) -> ConsumerCapacity {
        ConsumerCapacity {
            max_in_flight,
            max_buffer_depth,
        }
    }

    #[test]
    fn consumer_capacity_constructs_with_public_fields_and_validates_positive_limits() {
        let declaration = ConsumerCapacity {
            max_in_flight: 10,
            max_buffer_depth: 50,
        };

        assert_eq!(declaration.max_in_flight, 10);
        assert_eq!(declaration.max_buffer_depth, 50);
        assert_eq!(declaration.validate(), Ok(()));
        assert_eq!(ConsumerCapacity::new(10, 50), Ok(declaration));
        assert_eq!(
            ConsumerCapacity::new(0, 50),
            Err(CapacityError::InvalidCapacity {
                max_in_flight: 0,
                max_buffer_depth: 50,
            })
        );
    }

    #[test]
    fn capacity_tracker_starts_empty_and_records_counts() {
        let mut tracker = CapacityTracker::new(capacity(10, 50));

        assert_eq!(tracker.current_in_flight(), 0);
        assert_eq!(tracker.current_buffer_depth(), 0);
        assert_eq!(tracker.capacity(), &capacity(10, 50));

        tracker.record_delivery();
        assert_eq!(tracker.current_in_flight(), 1);

        assert_eq!(tracker.record_completion(), Ok(()));
        assert_eq!(tracker.current_in_flight(), 0);

        tracker.record_buffered();
        assert_eq!(tracker.current_buffer_depth(), 1);

        assert_eq!(tracker.record_buffer_drained(), Ok(()));
        assert_eq!(tracker.current_buffer_depth(), 0);
    }

    #[test]
    fn capacity_tracker_reports_underflow_errors_without_negative_counts() {
        let mut tracker = CapacityTracker::new(capacity(10, 50));

        assert_eq!(
            tracker.record_completion(),
            Err(CapacityError::InFlightUnderflow)
        );
        assert_eq!(tracker.current_in_flight(), 0);

        assert_eq!(
            tracker.record_buffer_drained(),
            Err(CapacityError::BufferUnderflow)
        );
        assert_eq!(tracker.current_buffer_depth(), 0);
    }

    #[test]
    fn pressure_signal_accepts_when_in_flight_capacity_is_available() {
        let mut tracker = CapacityTracker::new(capacity(2, 5));
        tracker.record_delivery();

        assert_eq!(tracker.pressure_signal(), PressureSignal::accept(1, 2));
        assert_eq!(tracker.current_in_flight(), 1);
        assert_eq!(tracker.current_buffer_depth(), 0);
    }

    #[test]
    fn pressure_signal_defers_when_in_flight_full_and_buffer_has_capacity() {
        let mut tracker = CapacityTracker::new(capacity(2, 5));
        tracker.record_delivery();
        tracker.record_delivery();
        tracker.record_buffered();
        tracker.record_buffered();
        tracker.record_buffered();

        assert_eq!(tracker.pressure_signal(), PressureSignal::defer(2, 2, 3, 5));
        assert_eq!(tracker.current_in_flight(), 2);
        assert_eq!(tracker.current_buffer_depth(), 3);
    }

    #[test]
    fn pressure_signal_rejects_when_in_flight_and_buffer_limits_are_reached() {
        let mut tracker = CapacityTracker::new(capacity(2, 5));
        tracker.record_delivery();
        tracker.record_delivery();
        tracker.record_buffered();
        tracker.record_buffered();
        tracker.record_buffered();
        tracker.record_buffered();
        tracker.record_buffered();

        assert_eq!(
            tracker.pressure_signal(),
            PressureSignal::reject(2, 2, 5, 5)
        );
        assert_eq!(tracker.current_in_flight(), 2);
        assert_eq!(tracker.current_buffer_depth(), 5);
    }

    #[test]
    fn pressure_signal_accepts_available_in_flight_regardless_of_buffer_state() {
        let mut tracker = CapacityTracker::new(capacity(1, 1));
        tracker.record_buffered();

        assert_eq!(tracker.pressure_signal(), PressureSignal::accept(0, 1));
        assert_eq!(tracker.current_in_flight(), 0);
        assert_eq!(tracker.current_buffer_depth(), 1);
    }

    #[test]
    fn pressure_root_re_exports_capacity_types() {
        use crate::pressure::{
            CapacityError as RootCapacityError, CapacityTracker as RootCapacityTracker,
            ConsumerCapacity as RootConsumerCapacity,
        };

        let mut tracker = RootCapacityTracker::new(RootConsumerCapacity {
            max_in_flight: 1,
            max_buffer_depth: 1,
        });

        assert_eq!(
            tracker.record_completion(),
            Err(RootCapacityError::InFlightUnderflow)
        );
    }
}
