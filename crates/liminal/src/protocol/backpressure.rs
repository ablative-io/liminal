use super::{MessageId, ProtocolError};

/// Payload carried by an in-band Accept backpressure signal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptPayload {
    /// Unique identifier of the message accepted by the consumer.
    pub referenced_message_id: MessageId,
}

/// Payload carried by an in-band Defer backpressure signal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeferPayload {
    /// Unique identifier of the message deferred by the consumer.
    pub referenced_message_id: MessageId,
    /// Optional human-readable deferral reason.
    pub reason: Option<String>,
}

/// Payload carried by an in-band Reject backpressure signal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RejectPayload {
    /// Unique identifier of the message rejected by the consumer.
    pub referenced_message_id: MessageId,
    /// Optional human-readable rejection reason.
    pub reason: Option<String>,
}

/// Per-stream pressure state derived from outstanding message counts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PressureState {
    /// The stream is below its declared in-flight capacity.
    Normal,
    /// The stream is at capacity but still within caller-supplied buffer space.
    Deferred,
    /// The stream has exceeded capacity plus caller-supplied buffer space.
    Rejecting,
}

/// Tracks protocol-level pressure counters for one application stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamPressure {
    /// Messages delivered on this stream but not yet accepted.
    outstanding_count: u32,
    /// Consumer-declared in-flight capacity from the Subscribe frame.
    max_in_flight: u32,
    /// Current pressure state for the stream.
    state: PressureState,
}

impl StreamPressure {
    /// Create pressure tracking for a subscription capacity declaration.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::CodecError`] when `max_in_flight` is zero.
    pub fn new(max_in_flight: u32) -> Result<Self, ProtocolError> {
        if max_in_flight == 0 {
            return Err(ProtocolError::codec(
                "max_in_flight must be greater than zero",
            ));
        }

        Ok(Self {
            outstanding_count: 0,
            max_in_flight,
            state: PressureState::Normal,
        })
    }

    /// Return messages delivered on this stream but not yet accepted.
    #[must_use]
    pub const fn outstanding_count(&self) -> u32 {
        self.outstanding_count
    }

    /// Return consumer-declared in-flight capacity from the Subscribe frame.
    #[must_use]
    pub const fn max_in_flight(&self) -> u32 {
        self.max_in_flight
    }

    /// Return the current pressure state for the stream.
    #[must_use]
    pub const fn state(&self) -> PressureState {
        self.state
    }

    /// Record delivery of one message and recompute state using caller capacity.
    ///
    /// `buffer_capacity` is supplied by the bus/subscription layer; the protocol
    /// state machine does not define or hardcode that threshold.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::CodecError`] when the outstanding counter or
    /// capacity threshold overflows.
    pub fn record_delivery(
        &mut self,
        buffer_capacity: u32,
    ) -> Result<PressureState, ProtocolError> {
        let next_outstanding = self
            .outstanding_count
            .checked_add(1)
            .ok_or_else(|| ProtocolError::codec("outstanding message count overflowed"))?;
        let next_state = Self::state_for(next_outstanding, self.max_in_flight, buffer_capacity)?;

        self.outstanding_count = next_outstanding;
        self.state = next_state;
        Ok(next_state)
    }

    /// Record acceptance of one outstanding message and recompute state using caller capacity.
    ///
    /// `buffer_capacity` is supplied by the bus/subscription layer; the protocol
    /// state machine does not define or hardcode that threshold.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::CodecError`] when no messages are outstanding or
    /// the capacity threshold overflows.
    pub fn record_accept(&mut self, buffer_capacity: u32) -> Result<PressureState, ProtocolError> {
        let next_outstanding = self
            .outstanding_count
            .checked_sub(1)
            .ok_or_else(|| ProtocolError::codec("cannot accept with zero outstanding messages"))?;
        let next_state = Self::state_for(next_outstanding, self.max_in_flight, buffer_capacity)?;

        self.outstanding_count = next_outstanding;
        self.state = next_state;
        Ok(next_state)
    }

    fn state_for(
        outstanding_count: u32,
        max_in_flight: u32,
        buffer_capacity: u32,
    ) -> Result<PressureState, ProtocolError> {
        let reject_threshold = max_in_flight
            .checked_add(buffer_capacity)
            .ok_or_else(|| ProtocolError::codec("pressure buffer threshold overflowed"))?;

        Ok(if outstanding_count < max_in_flight {
            PressureState::Normal
        } else if outstanding_count > reject_threshold {
            PressureState::Rejecting
        } else {
            PressureState::Deferred
        })
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use super::{AcceptPayload, DeferPayload, PressureState, RejectPayload, StreamPressure};
    use crate::protocol::{Frame, MessageId, ProtocolError};

    #[test]
    fn pressure_state_has_exact_required_variants() {
        fn state_name(state: PressureState) -> &'static str {
            match state {
                PressureState::Normal => "normal",
                PressureState::Deferred => "deferred",
                PressureState::Rejecting => "rejecting",
            }
        }

        let variants = [
            PressureState::Normal,
            PressureState::Deferred,
            PressureState::Rejecting,
        ];

        assert_eq!(variants.len(), 3);
        assert_eq!(state_name(PressureState::Normal), "normal");
        assert_eq!(state_name(PressureState::Deferred), "deferred");
        assert_eq!(state_name(PressureState::Rejecting), "rejecting");
    }

    #[test]
    fn public_backpressure_types_implement_debug() {
        fn assert_debug<T: Debug>() {}

        assert_debug::<AcceptPayload>();
        assert_debug::<DeferPayload>();
        assert_debug::<RejectPayload>();
        assert_debug::<PressureState>();
        assert_debug::<StreamPressure>();
    }

    #[test]
    fn payload_structs_carry_referenced_message_ids_and_reasons() {
        let accept = AcceptPayload {
            referenced_message_id: MessageId::from("message-1"),
        };
        let defer = DeferPayload {
            referenced_message_id: MessageId::from("message-2"),
            reason: Some("buffered".to_owned()),
        };
        let reject = RejectPayload {
            referenced_message_id: MessageId::from("message-3"),
            reason: None,
        };

        assert_eq!(accept.referenced_message_id.as_str(), "message-1");
        assert_eq!(defer.reason.as_deref(), Some("buffered"));
        assert_eq!(reject.reason, None);
    }

    #[test]
    fn stream_pressure_rejects_zero_capacity() {
        assert!(matches!(
            StreamPressure::new(0),
            Err(ProtocolError::CodecError { .. })
        ));
    }

    #[test]
    fn stream_pressure_transitions_to_deferred_at_max_in_flight() -> Result<(), ProtocolError> {
        let mut pressure = StreamPressure::new(10)?;

        for _ in 0..9 {
            assert_eq!(pressure.record_delivery(0)?, PressureState::Normal);
        }

        assert_eq!(pressure.record_delivery(0)?, PressureState::Deferred);
        assert_eq!(pressure.outstanding_count(), 10);
        assert_eq!(pressure.max_in_flight(), 10);
        assert_eq!(pressure.state(), PressureState::Deferred);
        Ok(())
    }

    #[test]
    fn stream_pressure_transitions_to_rejecting_beyond_buffer() -> Result<(), ProtocolError> {
        let mut pressure = StreamPressure::new(2)?;

        assert_eq!(pressure.record_delivery(1)?, PressureState::Normal);
        assert_eq!(pressure.record_delivery(1)?, PressureState::Deferred);
        assert_eq!(pressure.record_delivery(1)?, PressureState::Deferred);
        assert_eq!(pressure.record_delivery(1)?, PressureState::Rejecting);
        Ok(())
    }

    #[test]
    fn accept_decrements_outstanding_and_returns_to_normal() -> Result<(), ProtocolError> {
        let mut pressure = StreamPressure::new(10)?;

        for _ in 0..10 {
            pressure.record_delivery(0)?;
        }

        assert_eq!(pressure.record_accept(0)?, PressureState::Normal);
        assert_eq!(pressure.outstanding_count(), 9);
        assert_eq!(pressure.state(), PressureState::Normal);
        Ok(())
    }

    #[test]
    fn accept_preserves_rejecting_when_still_beyond_buffer() -> Result<(), ProtocolError> {
        let mut pressure = StreamPressure::new(2)?;

        for _ in 0..5 {
            pressure.record_delivery(1)?;
        }
        assert_eq!(pressure.state(), PressureState::Rejecting);

        assert_eq!(pressure.record_accept(1)?, PressureState::Rejecting);
        assert_eq!(pressure.outstanding_count(), 4);
        assert_eq!(pressure.state(), PressureState::Rejecting);
        Ok(())
    }

    #[test]
    fn accept_with_zero_outstanding_returns_protocol_error() -> Result<(), ProtocolError> {
        let mut pressure = StreamPressure::new(10)?;

        assert!(matches!(
            pressure.record_accept(0),
            Err(ProtocolError::CodecError { .. })
        ));
        Ok(())
    }

    #[test]
    fn subscribe_capacity_can_create_stream_pressure() -> Result<(), ProtocolError> {
        let subscribe = Frame::Subscribe {
            flags: 0,
            stream_id: 1,
            channel: "orders".to_owned(),
            accepted_schemas: Vec::new(),
            max_in_flight: 100,
        };
        let Frame::Subscribe { max_in_flight, .. } = subscribe else {
            return Err(ProtocolError::codec("test frame was not Subscribe"));
        };
        let pressure = StreamPressure::new(max_in_flight)?;

        assert_eq!(pressure.max_in_flight(), 100);
        assert_eq!(pressure.outstanding_count(), 0);
        assert_eq!(pressure.state(), PressureState::Normal);
        Ok(())
    }
}
